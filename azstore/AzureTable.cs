using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace azstore {
  public class Result{
    public object Data { get; set; }
    public int HttpStatus { get; set; }
    public bool Ok => HttpStatus <= 299 && HttpStatus >= 200 ;
    public static Result EmptyOk = new Result{HttpStatus = (int) HttpStatusCode.OK };
    public static Result OkWithData(object data) => new Result{Data = data, HttpStatus = (int) HttpStatusCode.OK };

    public static Result Error(HttpStatusCode code){
      return new Result{HttpStatus = (int) code};
    }

    public static Result Error(int code){
      return new Result{HttpStatus = code};
    }
  }

  public class Result<T>{
    public T Data { get; set; }
    public int HttpStatus { get; set; }
    public bool Ok => HttpStatus <= 299 && HttpStatus >= 200 ;
    public static Result<T> OkWithData(T data) => new Result<T>{Data = data, HttpStatus = (int) HttpStatusCode.OK };
  }

  public class AzureTable<T> where T : class, IDataEntity, new() {
    public string Name { get; set; }

    private EntityBinder<T> binder;
    private CloudTable table;

    public AzureTable(string name, string connectionstring): this(name, connectionstring, new EntityBinder<T>()) { }
    public AzureTable(string name, string conn, EntityBinder<T> binder) {
      this.Name = name;
      var account = CloudStorageAccount.Parse(conn);
      var tblClient = account.CreateCloudTableClient();
      this.table = tblClient.GetTableReference(this.Name);
      Util.Syncify(()=> table.CreateIfNotExistsAsync());
      this.binder = binder;
    }

    public async Task<Result> InsertOrReplaceAsync(T data) {
      var e = binder.Write(data);
      TableOperation operation = TableOperation.InsertOrReplace(e);
      try {
        var r = await table.ExecuteAsync(operation);
        return new Result{HttpStatus = r.HttpStatusCode};
      } catch (StorageException ex) {

        switch ((HttpStatusCode)ex.RequestInformation.HttpStatusCode) {
          case HttpStatusCode.Conflict:
            // Returned by Insert if it fails because the row already exists.
          case HttpStatusCode.PreconditionFailed:
            // Returned by Replace if it fails because the ETAG doesn't match (got modified by someone else).
            return Result.Error(ex.RequestInformation.HttpStatusCode);
          default:
            throw new InvalidOperationException($"table operation failed against {table.Uri}", ex);
        }
      }
    }

    public async Task<Result> InsertOrReplaceAll(IEnumerable<T> data) {
      Util.Ensure(data, x => x.Count()< 100, "one batch operation can hold at most 100 entities");
      if (data.Count()== 0) {
        return new Result{HttpStatus = (int) HttpStatusCode.OK};
      }
      TableBatchOperation batch = new TableBatchOperation();
      var pk = data.First().GetPartitionKey();
      foreach (var d in data) {
        var bound = this.binder.Write(d);
        if (pk != bound.PartitionKey) {
          throw new InvalidOperationException($"Partition key of entity {bound.RowKey} is not equal to other entities in the batch for batch operation");
        }
        batch.InsertOrReplace(bound);
      }
      var res = await table.ExecuteBatchAsync(batch);
      return Result.EmptyOk;
    }

    public async Task<Result> MergeOne(T data){
      Util.EnsureNonNull(data, nameof(data));
      var dte = binder.Write(data);
      dte.ETag = "*";
      TableOperation mergeOp = TableOperation.Merge(dte);
      var r = await table.ExecuteAsync(mergeOp);
      return new Result{HttpStatus = r.HttpStatusCode};
    }

    public async Task<Result> MergeOne(string pk, string rk, IDictionary<string, object> props){
      var dte = new DynamicTableEntity(pk, rk){ETag = "*"};
      foreach (var kv in props)
      {
          dte.Properties.Add(kv.Key, EntityProperty.CreateEntityPropertyFromObject(kv.Value));
      }
      var op = TableOperation.Merge(dte);
      try
      {
          var r = await table.ExecuteAsync(op);
          return Result.EmptyOk;
      }
      catch (StorageException ex)
      {
          int status = ex.RequestInformation.HttpStatusCode;
          if(status == (int) HttpStatusCode.PreconditionFailed ||
            status == (int) HttpStatusCode.Conflict
          )
            return Result.Error(status);
          throw;
      }
    }

    public async Task<Result<IEnumerable<T>>> RetrieveByPartition(string partitionName) {

      Util.EnsureNonNull(partitionName, "partitionName");
      TableQuery<DynamicTableEntity> q = new TableQuery<DynamicTableEntity>()
        .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionName));
      var result = new List<T>();
      TableContinuationToken continuationToken = null;
      do {
        TableQuerySegment<DynamicTableEntity> segment =
          await table.ExecuteQuerySegmentedAsync(q, continuationToken);
        
        continuationToken = segment.ContinuationToken;
        result.AddRange(segment.Results.Select(x => binder.Read(x)));
      } while (continuationToken != null);

      return Result<IEnumerable<T>>.OkWithData(result);
    }

    public async Task<Result<IEnumerable<T>>> RetrieveByQuery(string query) {
      Util.EnsureNonNull(query, "query");
      TableQuery<DynamicTableEntity> q = new TableQuery<DynamicTableEntity>()
        .Where(query);
      var result = new List<T>();
      TableContinuationToken continuationToken = null;
      do {
        TableQuerySegment<DynamicTableEntity> segment =
          await table.ExecuteQuerySegmentedAsync(q, continuationToken);
        continuationToken = segment.ContinuationToken;
        foreach (var dte in segment.Results) {
          result.Add(binder.Read(dte));
        }

      } while (continuationToken != null);

      return Result<IEnumerable<T>>.OkWithData(result);
    }

    public async Task<Result<T>> RetrieveOne(string partitionKey, string rowkey){
      Util.EnsureNonNull(partitionKey, nameof(partitionKey));
      Util.EnsureNonNull(rowkey, nameof(rowkey));
      TableOperation op = TableOperation.Retrieve<DynamicTableEntity>(partitionKey, rowkey);
      var res = await table.ExecuteAsync(op);
      var clientEntity = binder.Read( (DynamicTableEntity) res.Result);
      return new Result<T>{Data = clientEntity, HttpStatus = res.HttpStatusCode};
    }

    public static string BuildPointQuery(string partitionKey, string rowkey) {
      string q1 = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
      string q2 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowkey);
      return TableQuery.CombineFilters(q1, TableOperators.And, q2);
    }

    public static string BuildRangeQuery(string partitionKey, string rowkeyUpper, string rowkeyLower) {
      string q1 = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
      string q2 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, rowkeyUpper);
      string q3 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, rowkeyLower);
      var qq = TableQuery.CombineFilters(q1, TableOperators.And, q2);
      var q = TableQuery.CombineFilters(qq, TableOperators.And, q3);
      return q;
    }

    public async Task<Result> DeleteAsync(T element) {
      var boundEntity = binder.Write(element);
      TableOperation op = TableOperation.Delete(boundEntity);
      try {
        var res = await table.ExecuteAsync(op);
        return Result.EmptyOk;
      } catch (StorageException e) {
        int statuscode = e.RequestInformation.HttpStatusCode;
        if(statuscode == (int)HttpStatusCode.NotFound || 
          statuscode == (int)HttpStatusCode.Conflict ||
          statuscode == (int)HttpStatusCode.PreconditionFailed
         )
         return Result.Error(statuscode);
        throw e;
      }
    }

    public async Task<Result> DeleteAsync(string partitionKey, string rowkey) {
      var entity = new TableEntity(partitionKey, rowkey) { ETag = "*" };
      try {
        await table.ExecuteAsync(TableOperation.Delete(entity));
        return Result.EmptyOk;
      } catch (StorageException e) {
        int statuscode = e.RequestInformation.HttpStatusCode;
        if(statuscode == (int)HttpStatusCode.NotFound || 
          statuscode == (int)HttpStatusCode.Conflict ||
          statuscode == (int)HttpStatusCode.PreconditionFailed
         )
         return Result.Error(statuscode);
        throw e;
      }
    }
  }
}