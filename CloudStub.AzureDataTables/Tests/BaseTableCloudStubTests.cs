using System;
using System.Linq;
using System.Threading;
using Azure.Data.Tables;
using Xunit;

namespace CloudStub.AzureDataTables.Tests
{
    [Collection(nameof(TestRunFixtureCollection))]
    public abstract class BaseTableCloudStubTests
    {
        private static string _TableNamePrefix = "TestTable" + (int)(DateTime.UtcNow - DateTime.UtcNow.Date).TotalSeconds;
        private static int _tableCounter = 0;

        public BaseTableCloudStubTests()
        {
            TableServiceClient = TestRunContext.InMemory
                ? new TableServiceClientStub(TableAccountName)
                : new TableServiceClient(TestRunContext.AzureStorageConnectionString);

            TableName = $"{_TableNamePrefix}{Interlocked.Increment(ref _tableCounter)}";
            TableClient = TableServiceClient.GetTableClient(TableName);
        }

        protected static string TableAccountName { get; }
            = TestRunContext.InMemory
                ? "TestAccount" + Random.Shared.Next(1000, 9999)
                : TestRunContext.AzureStorageConnectionString
                    .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    .Where(part => part.StartsWith("AccountName=", StringComparison.OrdinalIgnoreCase))
                    .Select(part => part.Substring("AccountName=".Length))
                    .DefaultIfEmpty("UnknownAccount")
                    .First();

        protected string TableName { get; }

        protected TableServiceClient TableServiceClient { get; }

        protected TableClient TableClient { get; }
    }
}