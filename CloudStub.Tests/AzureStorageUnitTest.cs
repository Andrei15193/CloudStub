using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
        private const string _testBlobContainerPrefix = "testblobcontainer";
        private static int _blobCount = 0;

        private const string _testQueuePrefix = "testqueue";
        private static int _queueCount = 0;

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

        private readonly Lazy<string> _blobContainerName;
        private readonly Lazy<string> _testQueueName;
        private readonly Lazy<string> _testTableName;

        /// <summary>Initializes a new instance of the <see cref="AzureStorageUnitTest"/> class.</summary>
        protected AzureStorageUnitTest()
        {
            _blobContainerName = new Lazy<string>(() => Task.Run(_GetTestBlobContainerNameAsync).Result);
            _testQueueName = new Lazy<string>(() => Task.Run(_GetTestQueueNameAsync).Result);
            _testTableName = new Lazy<string>(() => Task.Run(_GetTestTableNameAsync).Result);
        }

        /// <summary>Gets the test blob container name. Unique for each test method.</summary>
        protected string TestBlobContainerName
            => _blobContainerName.Value;

        /// <summary>Gets the test queue name. Unique for each test method.</summary>
        protected string TestQueueName
            => _testQueueName.Value;

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
            => Task.WhenAll(
                _ClearTestBlobContainersAsync(),
                _ClearTableAsync(),
                _ClearTestQueuesAsync()
            );

        private static async Task<string> _GetTestBlobContainerNameAsync()
        {
            var blobContainerName = $"{_testBlobContainerPrefix}{Interlocked.Increment(ref _blobCount)}";
            var blobContainer = CloudStorageAccount
                .Parse(AzureStorageConnectionString)
                .CreateCloudBlobClient()
                .GetContainerReference(blobContainerName);
            if (await blobContainer.ExistsAsync().ConfigureAwait(false))
                await _ClearBlobs(blobContainer).ConfigureAwait(false);
            else
                await blobContainer.CreateAsync(BlobContainerPublicAccessType.Off, new BlobRequestOptions(), new OperationContext());

            return blobContainerName;
        }

        private static async Task _ClearBlobs(CloudBlobContainer blobContainer)
        {
            var deletedBlobs = 0;
            do
            {
                var blobsToDelete = (await blobContainer.ListBlobsSegmentedAsync(null).ConfigureAwait(false)).Results.OfType<ICloudBlob>().ToList();

                await Task.WhenAll(
                    from blobToDelete in blobsToDelete
                    select blobToDelete.DeleteIfExistsAsync()
                ).ConfigureAwait(false);
                deletedBlobs = blobsToDelete.Count;
            } while (deletedBlobs > 0);
        }

        private async Task _ClearTestBlobContainersAsync()
        {
            if (_blobContainerName.IsValueCreated)
                await CloudStorageAccount
                    .Parse(AzureStorageConnectionString)
                    .CreateCloudBlobClient()
                    .GetContainerReference(_blobContainerName.Value)
                    .DeleteIfExistsAsync()
                    .ConfigureAwait(false);
        }

        private static async Task<string> _GetTestQueueNameAsync()
        {
            var queueName = $"{_testQueuePrefix}{Interlocked.Increment(ref _queueCount)}";
            var queue = CloudStorageAccount
                .Parse(AzureStorageConnectionString)
                .CreateCloudQueueClient()
                .GetQueueReference(queueName);

            if (await queue.ExistsAsync().ConfigureAwait(false))
                await queue.ClearAsync();

            return queueName;
        }

        private async Task _ClearTestQueuesAsync()
        {
            if (_testQueueName.IsValueCreated)
                await CloudStorageAccount
                    .Parse(AzureStorageConnectionString)
                    .CreateCloudQueueClient()
                    .GetQueueReference(_testQueueName.Value)
                    .DeleteIfExistsAsync()
                    .ConfigureAwait(false);
        }

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