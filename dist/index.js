"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable @typescript-eslint/no-non-null-assertion */
const client_s3_1 = require("@aws-sdk/client-s3");
const prompt_1 = __importDefault(require("prompt"));
class ServerlessS3Cleaner {
    constructor(serverless, _options, logging) {
        this.serverless = serverless;
        this.configSchema = {
            type: 'object',
            properties: {
                prompt: { type: 'boolean', nullable: true, default: false },
                buckets: {
                    type: 'array',
                    uniqueItems: true,
                    items: { type: 'string' },
                    nullable: true,
                },
                bucketsToCleanOnDeploy: {
                    type: 'array',
                    uniqueItems: true,
                    items: { type: 'string' },
                    nullable: true,
                },
            },
            additionalProperties: false,
            anyOf: [{ required: ['buckets'] }, { required: ['bucketsToCleanOnDeploy'] }],
        };
        this.provider = this.serverless.getProvider('aws');
        this.log = logging.log;
        this.s3Client = new client_s3_1.S3Client({ region: this.provider.getRegion() });
        this.serverless.configSchemaHandler.defineCustomProperties({
            type: 'object',
            properties: {
                'serverless-s3-cleaner': this.configSchema,
            },
        });
        this.commands = {
            s3remove: {
                usage: 'Remove all files in S3 buckets',
                lifecycleEvents: ['remove'],
            },
        };
        this.hooks = {
            'before:deploy:deploy': async () => this.remove(true),
            'before:remove:remove': async () => this.remove(false),
            's3remove:remove': async () => this.remove(false),
        };
    }
    async remove(isDeploying) {
        const config = this.loadConfig();
        let bucketsToEmpty = isDeploying ? config.bucketsToCleanOnDeploy : config.buckets;
        if (config.prompt) {
            prompt_1.default.start();
            const bucketPromptResults = await prompt_1.default.get(bucketsToEmpty.map((bucket) => ({
                name: bucket,
                description: `Empty bucket ${bucket}. Are you sure? [yes/no]:`,
                pattern: /(yes|no)/,
                default: 'yes',
                message: 'Must respond yes or no',
            })));
            bucketsToEmpty = [];
            for (const bucket of Object.keys(bucketPromptResults)) {
                const confirmed = bucketPromptResults[bucket].toString() === 'yes';
                if (confirmed) {
                    bucketsToEmpty.push(bucket);
                }
                else {
                    this.log.notice(`${bucket}: remove skipped`);
                }
            }
        }
        const existingBuckets = [];
        for (const bucket of bucketsToEmpty) {
            const exists = await this.bucketExists(bucket);
            if (exists) {
                existingBuckets.push(bucket);
            }
            else {
                this.log.warning(`${bucket} not found or you do not have permissions, skipping...`);
            }
        }
        const removePromises = existingBuckets.map((bucket) => this.listBucketKeys(bucket)
            .then((keys) => this.deleteObjects(bucket, keys))
            .then(() => this.log.success(`${bucket} successfully emptied`))
            .catch((err) => this.log.error(`${bucket} cannot be emptied. ${err}`)));
        await Promise.all(removePromises);
    }
    async bucketExists(bucket) {
        const params = { Bucket: bucket };
        try {
            await this.s3Client.send(new client_s3_1.HeadBucketCommand(params));
            return true;
        }
        catch (_a) {
            return false;
        }
    }
    async deleteObjects(bucket, keys) {
        const maxDeleteKeys = 1000;
        const params = [];
        for (let i = 0; i < keys.length; i += maxDeleteKeys) {
            params.push(new client_s3_1.DeleteObjectsCommand({
                Bucket: bucket,
                Delete: {
                    Objects: keys.slice(i, i + maxDeleteKeys),
                    Quiet: true,
                },
            }));
        }
        const deleteResults = await Promise.all(params.map((param) => this.s3Client.send(param)));
        const firstErrorResult = deleteResults.find((dr) => dr.Errors && dr.Errors.length > 0);
        if (firstErrorResult) {
            const errInfo = firstErrorResult.Errors[0];
            throw new Error(`${errInfo.Key} - ${errInfo.Message}`);
        }
    }
    async listBucketKeys(bucketName) {
        const listParams = { Bucket: bucketName };
        let bucketKeys = [];
        try {
            while (true) {
                const listResult = await this.s3Client.send(new client_s3_1.ListObjectVersionsCommand(listParams));
                if (listResult.Versions) {
                    bucketKeys = bucketKeys.concat(listResult.Versions.map((item) => ({ Key: item.Key, VersionId: item.VersionId })));
                }
                if (listResult.DeleteMarkers) {
                    bucketKeys = bucketKeys.concat(listResult.DeleteMarkers.map((item) => ({ Key: item.Key, VersionId: item.VersionId })));
                }
                if (!listResult.IsTruncated) {
                    break;
                }
                listParams.VersionIdMarker = listResult.NextVersionIdMarker;
                listParams.KeyMarker = listResult.NextKeyMarker;
            }
        }
        catch (error) {
            const err = error;
            if (err.name === 'NotImplemented' && err.message.includes('This bucket does not support Object Versioning')) {
                // Fall back to ListObjectsV2Command if versioning is not supported
                delete listParams.VersionIdMarker; // Clear VersionIdMarker and KeyMarker for ListObjectsV2
                delete listParams.KeyMarker;
                while (true) {
                    const listResult = await this.s3Client.send(new client_s3_1.ListObjectsV2Command(listParams));
                    if (listResult.Contents) {
                        bucketKeys = bucketKeys.concat(listResult.Contents.map((item) => ({ Key: item.Key, VersionId: 'null' })));
                    }
                    if (!listResult.IsTruncated) {
                        break;
                    }
                    listParams.ContinuationToken = listResult.NextContinuationToken;
                }
            }
            else {
                throw error; // Rethrow if it's a different error
            }
        }
        return bucketKeys;
    }
    loadConfig() {
        const providedConfig = this.serverless.service.custom['serverless-s3-cleaner'];
        if (!providedConfig.buckets && !providedConfig.bucketsToCleanOnDeploy) {
            throw new Error('You must configure "buckets" or "bucketsToCleanOnDeploy" parameters in custom > serverless-s3-cleaner section');
        }
        return {
            buckets: providedConfig.buckets || [],
            prompt: providedConfig.prompt || false,
            bucketsToCleanOnDeploy: providedConfig.bucketsToCleanOnDeploy || [],
        };
    }
}
exports.default = ServerlessS3Cleaner;
module.exports = ServerlessS3Cleaner;
