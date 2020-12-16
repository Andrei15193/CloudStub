﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace CloudStub.Tests
{
    public abstract class BaseInMemoryCloudTableTests : AzureStorageUnitTest
    {
        /// <summary>A temporary flag to easily switch between in-memory cloud table and actual Azure Storage Table.</summary>
        protected static bool UseInMemory { get; } = true;

        protected BaseInMemoryCloudTableTests()
            => CloudTable = GetCloudTable(TestTableName);

        protected new string TestTableName { get; } = (nameof(BaseInMemoryCloudTableTests) + "TestTable" + Guid.NewGuid().ToString().Replace("-", "")).Substring(0, 63);

        protected CloudTable CloudTable { get; }

        protected IReadOnlyCollection<ITableEntity> GetAllEntities()
            => GetAllEntities(CloudTable);

        protected static IReadOnlyCollection<ITableEntity> GetAllEntities(CloudTable cloudTable)
        {
            var query = new TableQuery();
            var continuationToken = default(TableContinuationToken);
            var entities = new List<ITableEntity>();

            do
            {
                var result = cloudTable.ExecuteQuerySegmented(query, continuationToken);
                continuationToken = result.ContinuationToken;
                entities.AddRange(result);
            } while (continuationToken != null);

            return entities;
        }

        protected Task<IReadOnlyCollection<ITableEntity>> GetAllEntitiesAsync()
            => GetAllEntitiesAsync(CloudTable);

        protected static async Task<IReadOnlyCollection<ITableEntity>> GetAllEntitiesAsync(CloudTable cloudTable)
        {
            var query = new TableQuery();
            var continuationToken = default(TableContinuationToken);
            var entities = new List<ITableEntity>();

            do
            {
                var result = await cloudTable.ExecuteQuerySegmentedAsync(query, continuationToken);
                continuationToken = result.ContinuationToken;
                entities.AddRange(result);
            } while (continuationToken != null);

            return entities;
        }

        protected static CloudTable GetCloudTable(string tableName)
            => UseInMemory ?
                new InMemoryCloudTable(tableName) :
                CloudStorageAccount
                    .Parse(AzureStorageConnectionString)
                    .CreateCloudTableClient()
                    .GetTableReference(tableName);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Task.Run(CloudTable.DeleteIfExistsAsync).Wait();
            base.Dispose(disposing);
        }
    }
}