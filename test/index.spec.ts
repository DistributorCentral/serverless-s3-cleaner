/* eslint-disable @typescript-eslint/no-use-before-define */
import { DeleteObjectsRequest, ListObjectVersionsOutput, ObjectIdentifier } from 'aws-sdk/clients/s3';
import prompt from 'prompt';
import Serverless from 'serverless';
import Aws from 'serverless/plugins/aws/provider/awsProvider';
import ServerlessS3Cleaner from '../src/index';

describe('ServerlessS3Cleaner', () => {

  it('should create the plugin', () => {
    const { serverless } = stubServerlessInstance();
    const plugin = new ServerlessS3Cleaner(serverless);
    expect(plugin).toBeTruthy();
  });

  it('should fail when neither buckets nor bucketsToCleanOnDeploy is configured', async () => {
    const { serverless } = stubServerlessInstance({});
    const plugin = new ServerlessS3Cleaner(serverless);
    expect(plugin).toBeTruthy();

    const removeFn = plugin.hooks['before:remove:remove'];
    await expectAsync(removeFn()).toBeRejectedWith(jasmine.objectContaining({
      message: jasmine.stringMatching(/You must configure.+/)
    }));
  });

  describe('before stack removal', () => {
    it('should empty configured buckets', async () => {
      const { requestSpy, serverless } = stubServerlessInstance({
        buckets: ['b1', 'b2'],
      });
      const plugin = new ServerlessS3Cleaner(serverless);

      requestSpy.withArgs('S3', 'listObjectVersions', jasmine.anything()).and.resolveTo({
        Versions: [
          { Key: 'obj1', VersionId: 'v1' },
          { Key: 'obj2', VersionId: 'v2' }
        ]
      } as ListObjectVersionsOutput);

      const removeFn = plugin.hooks['before:remove:remove'];
      await expectAsync(removeFn()).toBeResolved();

      expect(requestSpy).toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.objectContaining<DeleteObjectsRequest>({
        Bucket: 'b1',
        Delete: jasmine.objectContaining({
          Objects: jasmine.arrayContaining<ObjectIdentifier>([
            { Key: 'obj1', VersionId: 'v1' },
            { Key: 'obj2', VersionId: 'v2' },
          ])
        })
      }));
      expect(requestSpy).toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.objectContaining<DeleteObjectsRequest>({
        Bucket: 'b2',
        Delete: jasmine.objectContaining({
          Objects: jasmine.arrayContaining<ObjectIdentifier>([
            { Key: 'obj1', VersionId: 'v1' },
            { Key: 'obj2', VersionId: 'v2' },
          ])
        })
      }));
    });

    it('should include delete markers when emptying buckets', async () => {
      const { requestSpy, serverless } = stubServerlessInstance({
        buckets: ['b1'],
      });
      const plugin = new ServerlessS3Cleaner(serverless);

      requestSpy.withArgs('S3', 'listObjectVersions', jasmine.anything()).and.resolveTo({
        DeleteMarkers: [
          { Key: 'obj1', VersionId: 'v1' },
          { Key: 'obj2', VersionId: 'v2' }
        ]
      } as ListObjectVersionsOutput);

      const removeFn = plugin.hooks['before:remove:remove'];
      await expectAsync(removeFn()).toBeResolved();

      expect(requestSpy).toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.objectContaining<DeleteObjectsRequest>({
        Bucket: 'b1',
        Delete: jasmine.objectContaining({
          Objects: jasmine.arrayContaining<ObjectIdentifier>([
            { Key: 'obj1', VersionId: 'v1' },
            { Key: 'obj2', VersionId: 'v2' },
          ])
        })
      }));
    });

    it('should delete all objects when listObjectVersions returns truncated results', async () => {
      const { requestSpy, serverless } = stubServerlessInstance({
        buckets: ['b1'],
      });
      const plugin = new ServerlessS3Cleaner(serverless);

      let callCount = 0;
      requestSpy.withArgs('S3', 'listObjectVersions', jasmine.anything()).and.callFake(() => ({
        DeleteMarkers: [
          { Key: 'obj', VersionId: `v${callCount}` },
        ],
        IsTruncated: (callCount++ === 0)
      }) as ListObjectVersionsOutput);

      const removeFn = plugin.hooks['before:remove:remove'];
      await expectAsync(removeFn()).toBeResolved();

      expect(requestSpy).toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.objectContaining<DeleteObjectsRequest>({
        Bucket: 'b1',
        Delete: jasmine.objectContaining({
          Objects: jasmine.arrayContaining<ObjectIdentifier>([
            { Key: 'obj', VersionId: 'v0' },
            { Key: 'obj', VersionId: 'v1' },
          ])
        })
      }));
    });

    it('should log a message when emptying an existing bucket fails', async () => {
      const { requestSpy, serverless } = stubServerlessInstance({
        buckets: ['b1', 'b2'],
      });
      const plugin = new ServerlessS3Cleaner(serverless);

      const errorMsg = 'bad object';
      let callCount = 0;
      requestSpy.withArgs('S3', 'listObjectVersions', jasmine.anything()).and.callFake(
        () => (callCount++ > 0
          ? Promise.reject(errorMsg)
          : ({
            DeleteMarkers: [
              { Key: 'obj1', VersionId: 'v1' },
            ]
          }) as ListObjectVersionsOutput)
      );

      const removeFn = plugin.hooks['before:remove:remove'];
      await expectAsync(removeFn()).toBeResolved();

      expect(requestSpy).toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.objectContaining<DeleteObjectsRequest>({
        Bucket: 'b1'
      }));
      expect(requestSpy).not.toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.objectContaining<DeleteObjectsRequest>({
        Bucket: 'b2'
      }));
      expect(serverless.cli.log).toHaveBeenCalledWith(jasmine.stringMatching(`cannot be emptied: ${errorMsg}`));
    });

    it('should skip buckets that do not exist', async () => {
      const { requestSpy, serverless } = stubServerlessInstance({
        buckets: ['b1'],
      });
      const plugin = new ServerlessS3Cleaner(serverless);

      requestSpy.withArgs('S3', 'listObjectVersions', jasmine.anything()).and.resolveTo({});
      requestSpy.withArgs('S3', 'headBucket', jasmine.anything()).and.rejectWith('bad bucket');

      const removeFn = plugin.hooks['before:remove:remove'];
      await expectAsync(removeFn()).toBeResolved();

      expect(requestSpy).not.toHaveBeenCalledWith('S3', 'listObjectVersions', jasmine.anything());
      expect(serverless.cli.log).toHaveBeenCalledWith(jasmine.stringMatching('skipping'));
    });

    it('should skip configured bucketsToCleanOnDeploy', async () => {
      const { requestSpy, serverless } = stubServerlessInstance({
        bucketsToCleanOnDeploy: ['b2']
      });
      const plugin = new ServerlessS3Cleaner(serverless);

      requestSpy.withArgs('S3', 'listObjectVersions', jasmine.anything()).and.resolveTo({
        Versions: [
          { Key: 'obj1', VersionId: 'v1' },
          { Key: 'obj2', VersionId: 'v2' }
        ]
      } as ListObjectVersionsOutput);

      const removeFn = plugin.hooks['before:remove:remove'];
      await expectAsync(removeFn()).toBeResolved();

      expect(requestSpy).not.toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.anything());
    });

    it('should prompt the user for each bucket when configured to do so', async () => {
      const { requestSpy, serverless } = stubServerlessInstance({
        buckets: ['b1', 'b2'],
        prompt: true,
      });
      const plugin = new ServerlessS3Cleaner(serverless);

      requestSpy.withArgs('S3', 'listObjectVersions', jasmine.anything()).and.resolveTo({
        DeleteMarkers: [
          { Key: 'obj1', VersionId: 'v1' },
        ]
      } as ListObjectVersionsOutput);

      spyOn(prompt, 'start');
      spyOn(prompt, 'get').and.resolveTo({
        b1: 'yes',
        b2: 'no',
      });

      const removeFn = plugin.hooks['before:remove:remove'];
      await expectAsync(removeFn()).toBeResolved();
      expect(prompt.start).toHaveBeenCalled();

      expect(serverless.cli.log).not.toHaveBeenCalledWith(jasmine.stringMatching('b1: remove skipped'));
      expect(requestSpy).toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.objectContaining<DeleteObjectsRequest>({
        Bucket: 'b1'
      }));

      expect(serverless.cli.log).toHaveBeenCalledWith(jasmine.stringMatching('b2: remove skipped'));
      expect(requestSpy).not.toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.objectContaining<DeleteObjectsRequest>({
        Bucket: 'b2'
      }));
    });
  });

  describe('when executing s3remove command', () => {
    it('should empty configured buckets', async () => {
      const { requestSpy, serverless } = stubServerlessInstance({
        buckets: ['b1', 'b2']
      });
      const plugin = new ServerlessS3Cleaner(serverless);

      requestSpy.withArgs('S3', 'listObjectVersions', jasmine.anything()).and.resolveTo({
        Versions: [
          { Key: 'obj1', VersionId: 'v1' },
          { Key: 'obj2', VersionId: 'v2' }
        ]
      } as ListObjectVersionsOutput);

      const removeFn = plugin.hooks['s3remove:remove'];
      await expectAsync(removeFn()).toBeResolved();

      expect(requestSpy).toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.objectContaining<DeleteObjectsRequest>({
        Bucket: 'b1'
      }));
      expect(requestSpy).toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.objectContaining<DeleteObjectsRequest>({
        Bucket: 'b2'
      }));
    });

    it('should skip configured bucketsToCleanOnDeploy', async () => {
      const { requestSpy, serverless } = stubServerlessInstance({
        bucketsToCleanOnDeploy: ['b2']
      });
      const plugin = new ServerlessS3Cleaner(serverless);

      requestSpy.withArgs('S3', 'listObjectVersions', jasmine.anything()).and.resolveTo({
        Versions: [
          { Key: 'obj1', VersionId: 'v1' },
          { Key: 'obj2', VersionId: 'v2' }
        ]
      } as ListObjectVersionsOutput);

      const removeFn = plugin.hooks['before:remove:remove'];
      await expectAsync(removeFn()).toBeResolved();

      expect(requestSpy).not.toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.anything());
    });
  });

  describe('before stack deploy', () => {
    it('should not empty configured buckets', async () => {
      const { requestSpy, serverless } = stubServerlessInstance({
        buckets: ['b1', 'b2']
      });
      const plugin = new ServerlessS3Cleaner(serverless);

      requestSpy.withArgs('S3', 'listObjectVersions', jasmine.anything()).and.resolveTo({
        Versions: [
          { Key: 'obj1', VersionId: 'v1' },
          { Key: 'obj2', VersionId: 'v2' }
        ]
      } as ListObjectVersionsOutput);

      const removeFn = plugin.hooks['before:deploy:deploy'];
      await expectAsync(removeFn()).toBeResolved();

      expect(requestSpy).not.toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.anything());
    });

    it('should empty configured bucketsToCleanOnDeploy', async () => {
      const { requestSpy, serverless } = stubServerlessInstance({
        bucketsToCleanOnDeploy: ['b1', 'b2']
      });
      const plugin = new ServerlessS3Cleaner(serverless);

      requestSpy.withArgs('S3', 'listObjectVersions', jasmine.anything()).and.resolveTo({
        Versions: [
          { Key: 'obj1', VersionId: 'v1' },
          { Key: 'obj2', VersionId: 'v2' }
        ]
      } as ListObjectVersionsOutput);

      const removeFn = plugin.hooks['before:deploy:deploy'];
      await expectAsync(removeFn()).toBeResolved();

      expect(requestSpy).toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.objectContaining<DeleteObjectsRequest>({
        Bucket: 'b1'
      }));
      expect(requestSpy).toHaveBeenCalledWith('S3', 'deleteObjects', jasmine.objectContaining<DeleteObjectsRequest>({
        Bucket: 'b2'
      }));
    });
  });

  function stubServerlessInstance(config?: Partial<ServerlessS3CleanerConfig>): { requestSpy: jasmine.Spy; serverless: jasmine.SpyObj<Serverless> } {
    const requestSpy = jasmine.createSpy('request').and.resolveTo({});
    return {
      requestSpy,
      serverless: jasmine.createSpyObj<Serverless>({
        getProvider: ({
          request: requestSpy
        }) as unknown as Aws,
      }, {
        cli: jasmine.createSpyObj(['log']),
        service: jasmine.createSpyObj([], {
          custom: {
            'serverless-s3-cleaner': config
          }
        })
      })
    };
  }
});
