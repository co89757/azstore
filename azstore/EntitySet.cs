﻿using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace azstore
{

    public enum Result
    {
        /// <summary>
        /// Table operation successfully completed.
        /// </summary>
        Succeeded = 0,

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

    public class EntitySet<T> where T : IDataEntity, new()
    {
        public string Name { get; set; }

        private string connectionString;
        private EntityBinder<T> binder;
        private CloudTable table;

        public EntitySet(string name, string connectionstring) : this(name, connectionstring, new EntityBinder<T>()) { }
        public EntitySet(string name, string conn, EntityBinder<T> binder)
        {
            this.Name = name;
            this.connectionString = conn;
            var account = CloudStorageAccount.Parse(conn);
            var tblClient = account.CreateCloudTableClient();
            this.table = tblClient.GetTableReference(this.Name);
            table.CreateIfNotExistsAsync().Wait();
            this.binder = binder;
        }

        public async Task<Result> AddOverrideAsync(T data)
        {
            var e = binder.Write(data);
            TableOperation operation = TableOperation.InsertOrReplace(e);
            try
            {
                await table.ExecuteAsync(operation);
                return Result.Succeeded;
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

        public async Task<Result> AddBatch(IEnumerable<T> data)
        {
            //TODO
            return Result.Succeeded;
        }

    }
}
