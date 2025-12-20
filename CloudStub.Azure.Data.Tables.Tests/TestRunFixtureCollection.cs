using Azure.Data.Tables;

namespace CloudStub.Azure.Data.Tables.Tests;

[CollectionDefinition(nameof(TestRunFixtureCollection))]
public class TestRunFixtureCollection : ICollectionFixture<TestRunFixture>
{
}

public class TestRunFixture :IDisposable
{
    public TestRunFixture()
        => CleanUpTables();

    public void Dispose()
        => CleanUpTables();

    private void CleanUpTables()
    {
        if (!TestRunContext.InMemory)
        {
            var tableServiceClinet = new TableServiceClient(TestRunContext.AzureStorageConnectionString);
            foreach (var table in tableServiceClinet.Query())
                tableServiceClinet.DeleteTable(table.Name);
        }
    }
}