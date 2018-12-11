
using System;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure.Storage;
using System.Threading.Tasks;

namespace azstore
{
    public static class BlobMetadataManager
    {
        /// <summary>
        /// Similar to AddRunTag method, but add tags in one batch
        /// </summary>
       
        public static async Task BatchAddMetadata(CloudBlob blob,  IDictionary<string, string> tags)
        {
            if (blob == null)
            {
                throw new ArgumentNullException(nameof(blob));
            }

            if (tags == null)
            {
                throw new ArgumentNullException(nameof(tags));
            }

            const int MaxRetries = 3;
            // Retrieve and update the metadata with retry
            for (int i = 1; i <= MaxRetries; i++)
            {
                try
                {
                    // FetchAttributes() is needed here, otherwise the put operation would wipe out all previously written entries if the
                    // blob is obtained without BloblistingDetails.Metadata option.
                    await blob.FetchAttributesAsync();
                    string etag = blob.Properties.ETag;
                    AccessCondition accessCondition = AccessCondition.GenerateIfMatchCondition(etag);
                    // adds or updates the key with the specified value when condition is met
                    foreach (var kv in tags)
                    {
                        blob.Metadata[kv.Key] = kv.Value;
                    }

                    await blob.SetMetadataAsync(accessCondition, null, null);
                    return;
                }
                catch (StorageException e) when (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                {
                    // conflict, the metadata has been updated by another agent, wait and retry later.
                    Thread.Sleep(1000);
                }
            }

            throw new TimeoutException($"metadata update operation exceeds maximum retries due to concurrency conflict");
        }

        public static async Task<IDictionary<string, string>> RetrieveMetadata(CloudBlob blob)
        {
            if (blob == null)
            {
                throw new ArgumentNullException(nameof(blob));
            }

            await blob.FetchAttributesAsync();
            var tags = new Dictionary<string,string>(blob.Metadata);
            return tags;
        }
    }
}