using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CloudStub.Tests
{
    public class InMemoryCloudTableTests : AzureStorageUnitTest
    {
        /// <summary>A temporary flag to easily switch between in-memory cloud table and actual Azure Storage Table.</summary>
        protected static bool UseInMemory { get; } = true;

        public InMemoryCloudTableTests()
            => CloudTable = GetCloudTable(TestTableName);

        protected new string TestTableName { get; } = (nameof(InMemoryCloudTableTests) + "TestTable" + Guid.NewGuid().ToString().Replace("-", "")).Substring(0, 63);

        protected CloudTable CloudTable { get; }

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

        protected CloudTable GetCloudTable(string tableName)
            => UseInMemory ?
                new InMemoryCloudTable(tableName) :
                CloudStorageAccount
                    .Parse(AzureStorageConnectionString)
                    .CreateCloudTableClient()
                    .GetTableReference(tableName);
    }
}