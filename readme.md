A high-fidelity in-memory implementation for Azure Table Storage useful for testing and local development.

**Setup**

The library provides a complete in-memory implementation for both the table service client and table clients. Replacing the table service client is enough to have have all subsequent table clients replaced with stubs as well.

```c#
public class DataTables
{
    public DataTables(TableServiceClient tableServiceClient)
    {
        Users = tableServiceClient.GetTableClient("Users");
    }

    public TableClient Users { get; }
}

public class MyTestClass
{
    public void MyTestMethod()
    {
        var dataTables = new DataTables(new TableServiceClientStub());

        dataTables.Users.Query<TableEntity>();
    }
}
```

Similar setups can be made for integration tests or local development.

```c#
public class MyWebApplicationFactory : WebApplicationFactory<Startup>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
        => builder.ConfigureTestServices(services =>
        {
            services.AddSingleton<TableServiceClient>(_ => new TableServiceClientStub());
        });
}
```