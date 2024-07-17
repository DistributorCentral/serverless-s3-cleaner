/* eslint-disable @typescript-eslint/no-non-null-assertion */
import {
  S3Client,
  HeadBucketCommand,
  DeleteObjectsCommand,
  ListObjectVersionsCommand,
  ObjectIdentifier,
} from '@aws-sdk/client-s3';
import type { JSONSchemaType } from 'ajv';
import prompt from 'prompt';
import Serverless from 'serverless';
import Plugin, { Logging } from 'serverless/classes/Plugin';
import Aws from 'serverless/plugins/aws/provider/awsProvider';

type ObjectIdentifierList = ObjectIdentifier[];

interface ListParams {
  Bucket: string;
  VersionIdMarker?: string;
  KeyMarker?: string;
}

export default class ServerlessS3Cleaner implements Plugin {
  public commands: Plugin.Commands;
  public hooks: Plugin.Hooks;
  public provider: Aws;
  private log: Logging['log'];
  private s3Client: S3Client;

  private configSchema: JSONSchemaType<ServerlessS3CleanerConfig> = {
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

  constructor(private readonly serverless: Serverless, _options, logging: Logging) {
    this.provider = this.serverless.getProvider('aws');
    this.log = logging.log;
    this.s3Client = new S3Client({ region: this.provider.getRegion() });
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

  private async remove(isDeploying: boolean): Promise<void> {
    const config = this.loadConfig();
    let bucketsToEmpty = isDeploying ? config.bucketsToCleanOnDeploy : config.buckets;

    if (config.prompt) {
      prompt.start();
      const bucketPromptResults = await prompt.get(
        bucketsToEmpty.map((bucket) => ({
          name: bucket,
          description: `Empty bucket ${bucket}. Are you sure? [yes/no]:`,
          pattern: /(yes|no)/,
          default: 'yes',
          message: 'Must respond yes or no',
        })),
      );

      bucketsToEmpty = [];
      for (const bucket of Object.keys(bucketPromptResults)) {
        const confirmed = bucketPromptResults[bucket].toString() === 'yes';
        if (confirmed) {
          bucketsToEmpty.push(bucket);
        } else {
          this.log.notice(`${bucket}: remove skipped`);
        }
      }
    }

    const existingBuckets: string[] = [];
    for (const bucket of bucketsToEmpty) {
      const exists = await this.bucketExists(bucket);
      if (exists) {
        existingBuckets.push(bucket);
      } else {
        this.log.warning(`${bucket} not found or you do not have permissions, skipping...`);
      }
    }

    const removePromises = existingBuckets.map((bucket) =>
      this.listBucketKeys(bucket)
        .then((keys) => this.deleteObjects(bucket, keys))
        .then(() => this.log.success(`${bucket} successfully emptied`))
        .catch((err) => this.log.error(`${bucket} cannot be emptied. ${err}`)),
    );

    await Promise.all(removePromises);
  }

  private async bucketExists(bucket: string): Promise<boolean> {
    const params = { Bucket: bucket };
    try {
      await this.s3Client.send(new HeadBucketCommand(params));
      return true;
    } catch {
      return false;
    }
  }

  private async deleteObjects(bucket: string, keys: ObjectIdentifierList): Promise<void> {
    const maxDeleteKeys = 1000;
    const params: DeleteObjectsCommand[] = [];
    for (let i = 0; i < keys.length; i += maxDeleteKeys) {
      params.push(
        new DeleteObjectsCommand({
          Bucket: bucket,
          Delete: {
            Objects: keys.slice(i, i + maxDeleteKeys),
            Quiet: true,
          },
        }),
      );
    }

    const deleteResults = await Promise.all(params.map((param) => this.s3Client.send(param)));

    const firstErrorResult = deleteResults.find((dr) => dr.Errors && dr.Errors.length > 0);
    if (firstErrorResult) {
      const errInfo = firstErrorResult.Errors![0];
      throw new Error(`${errInfo.Key} - ${errInfo.Message}`);
    }
  }

  private async listBucketKeys(bucketName: string): Promise<ObjectIdentifierList> {
    const listParams: ListParams = { Bucket: bucketName };
    let bucketKeys: ObjectIdentifierList = [];

    while (true) {
      const listResult = await this.s3Client.send(new ListObjectVersionsCommand(listParams));
      if (listResult.Versions) {
        bucketKeys = bucketKeys.concat(
          listResult.Versions.map((item) => ({ Key: item.Key!, VersionId: item.VersionId! })),
        );
      }
      if (listResult.DeleteMarkers) {
        bucketKeys = bucketKeys.concat(
          listResult.DeleteMarkers.map((item) => ({ Key: item.Key!, VersionId: item.VersionId! })),
        );
      }

      if (!listResult.IsTruncated) {
        break;
      }
      listParams.VersionIdMarker = listResult.NextVersionIdMarker;
      listParams.KeyMarker = listResult.NextKeyMarker;
    }

    return bucketKeys;
  }

  private loadConfig(): Required<ServerlessS3CleanerConfig> {
    const providedConfig: Partial<ServerlessS3CleanerConfig> = this.serverless.service.custom['serverless-s3-cleaner'];
    if (!providedConfig.buckets && !providedConfig.bucketsToCleanOnDeploy) {
      throw new Error(
        'You must configure "buckets" or "bucketsToCleanOnDeploy" parameters in custom > serverless-s3-cleaner section',
      );
    }

    return {
      buckets: providedConfig.buckets || [],
      prompt: providedConfig.prompt || false,
      bucketsToCleanOnDeploy: providedConfig.bucketsToCleanOnDeploy || [],
    };
  }
}

module.exports = ServerlessS3Cleaner;
