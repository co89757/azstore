using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.DataMovement;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace azstore {
    public class AzureStorage {
        // client side timeout for all requests to azure storage
        public static readonly TimeSpan ClientRequestTimeout = TimeSpan.FromMinutes(5);
        private CloudBlobClient client;
        private BlobRequestOptions blobRequestOptions = new BlobRequestOptions() {
            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 3),
            MaximumExecutionTime = ClientRequestTimeout
        };

        public AzureStorage(CloudStorageAccount account) {
            if (account == null) {
                throw new ArgumentNullException(nameof(account));
            }

            client = account.CreateCloudBlobClient();
            client.DefaultRequestOptions = blobRequestOptions;
        }

        public async Task StoreAsync(StorageKey key, byte[] data, string leastId = null, bool overwrite = true) {
            var blob = GetBlobReference(key);
            AccessCondition condition = AccessCondition.GenerateEmptyCondition();
            if (leastId != null) {
                condition.LeaseId = leastId;
            }

            if (!overwrite) {
                condition.IfNoneMatchETag = "*"; // "doesn't match any etag" means that it doesn't exist
            }

            // upload
            try {
                await blob.UploadFromByteArrayAsync(data, 0, data.Length, condition, blobRequestOptions, null);

            } catch (StorageException e) {
                throw new StorageException(e.RequestInformation, $"error uploading {data.Length:N0} bytes to block blob {blob.Uri}", e);
            }
        }

        public async Task StoreAsync(StorageKey key, string filepath, string leastId = null, bool overwrite = true) {
            var blob = GetBlobReference(key);
            AccessCondition condition = AccessCondition.GenerateEmptyCondition();
            if (leastId != null) {
                condition.LeaseId = leastId;
            }

            if (!overwrite) {
                condition.IfNoneMatchETag = "*"; // "doesn't match any etag" means that it doesn't exist
            }

            // upload
            try {
                await blob.UploadFromFileAsync(filepath, condition, blobRequestOptions, null);

            } catch (StorageException e) {
                throw new StorageException(e.RequestInformation, $"error uploading file {filepath} bytes to block blob {blob.Uri}", e);
            }
        }

        public async Task StoreFastAsync(StorageKey key, string filepath) {
            TransferManager.Configurations.ParallelOperations = Environment.ProcessorCount;
            var destblob = GetBlobReference(key);
            await TransferManager.UploadAsync(filepath, destblob);
        }

        public async Task StoreAsync(StorageKey key, byte[] data, CancellationToken token, string leastId = null, bool overwrite = true) {
            var blob = GetBlobReference(key);
            AccessCondition condition = AccessCondition.GenerateEmptyCondition();
            if (leastId != null) {
                condition.LeaseId = leastId;
            }

            if (!overwrite) {
                condition.IfNoneMatchETag = "*"; // "doesn't match any etag" means that it doesn't exist
            }

            try {
                await blob.UploadFromByteArrayAsync(data, 0, data.Length, condition, null, null, token);
            } catch (StorageException e) {
                throw new StorageException(e.RequestInformation, $"error uploading {data.Length:N0} bytes to block blob {blob.Uri}", e);
            }
        }

        public async Task StoreAsync(StorageKey key, System.IO.Stream source, CancellationToken token, string leastId = null, bool overwrite = true) {
            var blob = GetBlobReference(key);
            AccessCondition condition = AccessCondition.GenerateEmptyCondition();
            if (leastId != null) {
                condition.LeaseId = leastId;
            }

            if (!overwrite) {
                condition.IfNoneMatchETag = "*"; // "doesn't match any etag" means that it doesn't exist
            }

            try {
                await blob.UploadFromStreamAsync(source, condition, blobRequestOptions, null, token);
            } catch (StorageException e) {
                throw new StorageException(e.RequestInformation, $"error uploading stream data to block blob {blob.Uri}", e);
            }
        }

        public async Task CopyAsync(StorageKey target, StorageKey source) {
            var targetblob = GetBlobReference(target);
            var sourceBlob = GetBlobReference(source);
            await TransferManager.CopyAsync(sourceBlob, targetblob, true);
        }
        public async Task DeleteIfExistAsync(StorageKey key) {
            var blob = GetBlobReference(key);
            await blob.DeleteIfExistsAsync();
        }

        public async Task<byte[]> RetrieveAsync(StorageKey key) {
            var blob = GetBlobReference(key);
            var ms = new MemoryStream();
            await blob.DownloadToStreamAsync(ms, AccessCondition.GenerateEmptyCondition(), blobRequestOptions, null);
            ms.Seek(0, SeekOrigin.Begin);
            return ms.ToArray();
        }

        public async Task<bool> ExistAsync(StorageKey key) {
            var b = GetBlobReference(key);
            return await b.ExistsAsync();
        }

        internal CloudBlockBlob GetBlobReference(StorageKey key) {
            var container = NeedContainer(key.ContainerName);
            var b = container.GetBlockBlobReference(key.BlobName);
            return b;
        }

        private CloudBlobContainer NeedContainer(string containerName) {
            var containerReference = this.client.GetContainerReference(containerName);

            Util.PerformAzureOperationWithTimeout<CloudBlobContainer>(
                (azureObject) => {
                    azureObject.CreateIfNotExistsAsync().Wait();
                },
                containerReference);
            return containerReference;
        }
    }
}