// // Copyright (c) Microsoft Corporation. All Rights Reserved. 

using System;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure.Storage;

namespace XSpatial.AzureStore
{
    public static class BlobMetadataManager
    {
        /// <summary>
        /// Similar to AddRunTag method, but add tags in one batch
        /// </summary>
       
        public static void BatchAddMetadata(CloudBlob blob,  IDictionary<string, string> tags)
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
                    blob.FetchAttributes();
                    string etag = blob.Properties.ETag;
                    AccessCondition accessCondition = AccessCondition.GenerateIfMatchCondition(etag);
                    // adds or updates the key with the specified value when condition is met
                    foreach (var kv in tags)
                    {
                        blob.Metadata[kv.Key] = kv.Value;
                    }

                    blob.SetMetadata(accessCondition);
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

        public static IDictionary<string, string> RetrieveMetadata(CloudBlob blob)
        {
            if (blob == null)
            {
                throw new ArgumentNullException(nameof(blob));
            }

            blob.FetchAttributes();
            var tags = new Dictionary<string,string>(blob.Metadata);
            return tags;

        }

    }
}