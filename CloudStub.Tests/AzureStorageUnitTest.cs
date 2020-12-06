using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace CloudStub.Tests
{
    /// <summary>
    /// <para>
    /// A base class for writing unit tests of classes whose implementation is backed by Azure Storage.
    /// </para>
    /// <para>
    /// This class represents a template method for initializing and cleaning up Azure Storage used for testing.
    /// </para>
    /// </summary>
    public abstract class AzureStorageUnitTest : IDisposable
    {
        private const string _testTablePrefix = "testtable";
        private static int _tableCount = 0;

        /// <summary>Gets the connection string for the Azure Storage account used for automated testing.</summary>
        /// <value>The value is retrieved from <c>CUSTOMCONNSTR_TestAzureStorage</c> environment variable. If no such variable is defined then the value is <c>UseDevelopmentStorage=true</c> (Azure Storage Emulator).</value>
        protected static string AzureStorageConnectionString { get; } =
            (from environmentVariableTarget in new[] { EnvironmentVariableTarget.Process, EnvironmentVariableTarget.User, EnvironmentVariableTarget.Machine }
             let connectionString = Environment.GetEnvironmentVariable("CUSTOMCONNSTR_TestAzureStorage", environmentVariableTarget)
             where !string.IsNullOrWhiteSpace(connectionString)
             select connectionString)
            .DefaultIfEmpty("UseDevelopmentStorage=true;")
            .First();

        private readonly Lazy<string> _testTableName;

        /// <summary>Initializes a new instance of the <see cref="AzureStorageUnitTest"/> class.</summary>
        protected AzureStorageUnitTest()
            => _testTableName = new Lazy<string>(() => Task.Run(_GetTestTableNameAsync).Result);

        /// <summary>Gets the test table name. Unique for each test method.</summary>
        protected string TestTableName
            => _testTableName.Value;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>Releases all test data from the test environment.</summary>
        /// <param name="disposing">A parameter indicating from where this method was called. <c>true</c> if it was called from the <see cref="IDisposable.Dispose"/> method, <c>false</c> otherwise.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
                Task.Run(_ClearTestEnvironmentAsync).Wait();
        }

        private Task _ClearTestEnvironmentAsync()
            => _ClearTableAsync();


        private async Task<string> _GetTestTableNameAsync()
        {
            var tableName = $"{_testTablePrefix}{Interlocked.Increment(ref _tableCount)}";
            var table = CloudStorageAccount
                .Parse(AzureStorageConnectionString)
                .CreateCloudTableClient()
                .GetTableReference(tableName);

            if (await table.ExistsAsync().ConfigureAwait(false))
                await _ClearEntitiesAsync(table).ConfigureAwait(false);
            else
                await table.CreateAsync(new TableRequestOptions(), new OperationContext()).ConfigureAwait(false);

            return tableName;
        }

        private static async Task _ClearEntitiesAsync(CloudTable table)
        {
            var deletedEntities = 0;
            do
            {
                var entitiesToDelete = (await table.ExecuteQuerySegmentedAsync(new TableQuery(), null).ConfigureAwait(false)).Results;
                await Task.WhenAll(
                    from entityToDelete in entitiesToDelete
                    select table.ExecuteAsync(
                        TableOperation.Delete(
                            new TableEntity
                            {
                                PartitionKey = entityToDelete.PartitionKey,
                                RowKey = entityToDelete.RowKey,
                                ETag = "*"
                            }
                        )
                    )
                ).ConfigureAwait(false);
                deletedEntities = entitiesToDelete.Count;
            } while (deletedEntities > 0);
        }

        private async Task _ClearTableAsync()
        {
            if (_testTableName.IsValueCreated)
                await CloudStorageAccount
                    .Parse(AzureStorageConnectionString)
                    .CreateCloudTableClient()
                    .GetTableReference(_testTableName.Value)
                    .DeleteIfExistsAsync()
                    .ConfigureAwait(false);
        }
    }
}