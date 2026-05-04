using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;

namespace CloudStub.AzureDataTables.Tests
{
    internal static class TestRunContext
    {
        private const string InMemoryKey = "InMemory";
        private const string AzureStorageConnectionStringKey = "AzureStorageConnectionString";

        private static readonly IConfigurationRoot _configuration = new ConfigurationBuilder()
            .AddEnvironmentVariables()

            // For local testing, you can uncomment the following lines and provide values directly.
            // .AddInMemoryCollection(new Dictionary<string, string>
            // {
            //     [InMemoryKey] = bool.FalseString,
            //     [AzureStorageConnectionStringKey] = "get this from somewhere :)"
            // })

            .Build();

        public static bool InMemory
            => bool.TrueString.Equals(_configuration[InMemoryKey], StringComparison.OrdinalIgnoreCase);

        public static string AzureStorageConnectionString
            => _configuration[AzureStorageConnectionStringKey] ?? throw new ArgumentException($"Environment variable '{AzureStorageConnectionStringKey}' is not set.");
    }
}