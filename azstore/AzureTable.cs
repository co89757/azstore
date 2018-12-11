using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Linq;
namespace azstore
{
    public enum Result
    {
        /// <summary>
        /// Table operation successfully completed.
        /// </summary>
        Ok = 0,

        /// <summary>
        /// Table operation did not complete because the row was updated by another table instance.
        /// </summary>
        FailedBecauseUpdated = 1,

        /// <summary>
        /// Table operation could not find the row to which it was applied.
        /// </summary>
        NotFound = 2,

        /// <summary>
        /// One or more of values to SetValue call exceeded the column size limitation.
        /// </summary>
        SizeExceeded = 3,
    }

    public class AzureTable<T> where T : IDataEntity, new()
    {
        public string Name { get; set; }

        private EntityBinder<T> binder;
        private CloudTable table;

        public AzureTable(string name, string connectionstring) : this(name, connectionstring, new EntityBinder<T>()) { }
        public AzureTable(string name, string conn, EntityBinder<T> binder)
        {
            this.Name = name;
            var account = CloudStorageAccount.Parse(conn);
            var tblClient = account.CreateCloudTableClient();
            this.table = tblClient.GetTableReference(this.Name);
            table.CreateIfNotExistsAsync().Wait();
            this.binder = binder;
        }

        public async Task<Result> InsertOrReplaceAsync(T data)
        {
            var e = binder.Write(data);
            TableOperation operation = TableOperation.InsertOrReplace(e);
            try
            {
                await table.ExecuteAsync(operation);
                return Result.Ok;
            }
            catch (StorageException ex)
            {

                switch ((HttpStatusCode)ex.RequestInformation.HttpStatusCode)
                {
                    case HttpStatusCode.Conflict:
                        // Returned by Insert if it fails because the row already exists.
                        return  Result.FailedBecauseUpdated;
                    case HttpStatusCode.PreconditionFailed:
                        // Returned by Replace if it fails because the ETAG doesn't match (got modified by someone else).
                        return  Result.FailedBecauseUpdated;
                    default:
                        throw new InvalidOperationException($"table operation failed against {table.Uri}", ex);
                }
            }
        }

        public async Task<Result> InsertAll(IEnumerable<T> data)
        {
            Util.Ensure(data, x => x.Count() < 100, "one batch operation can hold at most 100 entities" );
            if(data.Count() == 0){
                return Result.Ok;
            }
            TableBatchOperation batch = new TableBatchOperation();
            var pk = data.First().GetPartitionKey();
            foreach(var d in data){
                
                var bound = this.binder.Write(d);
                if ( pk != bound.PartitionKey)
                {
                    throw new InvalidOperationException($"Partition key of entity {bound.RowKey} is not equal to other entities in the batch for batch operation");
                }
                batch.InsertOrReplace(bound);
            }
            var res = await table.ExecuteBatchAsync(batch);
            return Result.Ok;
        }

        public async Task<IEnumerable<T>> RetrieveByPartition(string partitionName){

            Util.EnsureNonNull(partitionName, "partitionName");
            TableQuery<DynamicTableEntity> q = new TableQuery<DynamicTableEntity>()
            .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionName));
            var result = new List<T>();
            TableContinuationToken continuationToken = null;
            do
            {
                TableQuerySegment<DynamicTableEntity> segment =
                    await table.ExecuteQuerySegmentedAsync(q, continuationToken);
                continuationToken = segment.ContinuationToken;
                result.AddRange(segment.Results.Select(x => binder.Read(x)));
            } while (continuationToken != null);

            return result;
            
        }

        public async Task<IEnumerable<T>> RetrieveByQuery(string query)
        {
            Util.EnsureNonNull(query, "query");
            TableQuery<DynamicTableEntity> q = new TableQuery<DynamicTableEntity>()
                .Where(query);
            var result = new List<T>();
            TableContinuationToken continuationToken = null;
            do
            {
                TableQuerySegment<DynamicTableEntity> segment =
                    await table.ExecuteQuerySegmentedAsync(q, continuationToken);
                continuationToken = segment.ContinuationToken;
                foreach (var dte in segment.Results)
                {
                    result.Add(binder.Read(dte));
                }
                 
            } while (continuationToken != null);

            return result;
        }
        

        public async Task<Result> DeleteAsync(T element){
            var boundEntity = binder.Write(element);
            TableOperation op = TableOperation.Delete(boundEntity);
            try
            {
                var res = await table.ExecuteAsync(op);
                return Result.Ok;
            }
            catch (StorageException e)
            {
                int statuscode = e.RequestInformation.HttpStatusCode;
                if( statuscode == (int) HttpStatusCode.NotFound)
                    return Result.NotFound;
                if(statuscode == (int) HttpStatusCode.Conflict || (int) HttpStatusCode.PreconditionFailed == statuscode )
                    return Result.FailedBecauseUpdated;
                throw e;
            }
        }

        public async Task<Result> DeleteAsync(string partitionKey, string rowkey)
        {
            var entity = new TableEntity(partitionKey, rowkey) { ETag = "*" };
            try
            {
                await table.ExecuteAsync(TableOperation.Delete(entity));
                return Result.Ok;
            }
            catch (StorageException e)
            {
                int statuscode = e.RequestInformation.HttpStatusCode;
                if (statuscode == (int)HttpStatusCode.NotFound)
                    return Result.NotFound;
                if (statuscode == (int)HttpStatusCode.Conflict || (int)HttpStatusCode.PreconditionFailed == statuscode)
                    return Result.FailedBecauseUpdated;
                throw new Exception($"table delete operation failed against {table.Uri}", e);
            }
         
        }
    }
}
