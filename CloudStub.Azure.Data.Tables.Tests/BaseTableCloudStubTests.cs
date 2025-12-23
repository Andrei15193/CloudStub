using Azure.Data.Tables;

namespace CloudStub.Azure.Data.Tables.Tests;

[Collection(nameof(TestRunFixtureCollection))]
public abstract class BaseTableCloudStubTests
{
    private static string _TableNamePrefix = "TestTable" + Random.Shared.Next(1000, 9999);
    private static int _tableCounter = 0;

    public BaseTableCloudStubTests()
    {
        TableServiceClient = TestRunContext.InMemory
            ? new TableServiceClientStub()
            : new TableServiceClient(TestRunContext.AzureStorageConnectionString);

        TestTableName = $"{_TableNamePrefix}{Interlocked.Increment(ref _tableCounter)}";
        CloudTable = TableServiceClient.GetTableClient(TestTableName);
    }

    protected string TestTableName { get; }

    protected TableServiceClient TableServiceClient { get; }

    protected TableClient CloudTable { get; }
}