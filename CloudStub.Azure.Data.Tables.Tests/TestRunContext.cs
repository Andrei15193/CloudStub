using Microsoft.Extensions.Configuration;

namespace CloudStub.Azure.Data.Tables.Tests;

internal static class TestRunContext
{
    private const string InMemoryKey = "InMemory";
    private const string AzureStorageConnectionStringKey = "AzureStorageConnectionString";

    private static readonly IConfigurationRoot _configuration = new ConfigurationBuilder()
        .AddEnvironmentVariables()
        .Build();

    public static bool InMemory
        => bool.TrueString.Equals(_configuration[InMemoryKey], StringComparison.OrdinalIgnoreCase);

    public static string AzureStorageConnectionString
        => _configuration[AzureStorageConnectionStringKey] ?? throw new ArgumentException($"Environment variable '{AzureStorageConnectionStringKey}' is not set.");
}