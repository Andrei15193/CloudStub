using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace CloudStub.Tests
{
    public class InMemoryCloudTableTests : AzureStorageUnitTest
    {
        /// <summary>A temporary flag to easily switch between in-memory cloud table and actual Azure Storage Table.</summary>
        private static readonly bool _useInMemory = true;
        private readonly CloudTable _cloudTable;
        private readonly string _testTableName = (nameof(InMemoryCloudTableTests) + "TestTable" + Guid.NewGuid().ToString().Replace("-", "")).Substring(0, 63);

        public InMemoryCloudTableTests()
            => _cloudTable = _GetCloudTable(_testTableName);

        [Fact]
        public void TableName_GetsTheSameNameWhichWasProvided()
        {
            Assert.Equal(_testTableName, _cloudTable.Name);
        }

        [Fact]
        public void CreateAsync_WhenTableNameIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("tableName", () => _GetCloudTable(null));
            Assert.Equal(new ArgumentNullException("tableName").Message, exception.Message);
        }

        [Fact]
        public void CreateAsync_WhenTableNameIsEmpty_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentException>("tableName", () => _GetCloudTable(""));
            Assert.Equal(new ArgumentException("The argument must not be empty string.", "tableName").Message, exception.Message);
        }

        [Fact]
        public async Task ExistsAsync_WhenTableDoesNotExist_ReturnsFalse()
        {
            Assert.False(await _cloudTable.ExistsAsync(null, null));
        }

        [Fact]
        public async Task ExistsAsync_WhenTableExist_ReturnsTrue()
        {
            await _cloudTable.CreateAsync(null, null);

            Assert.True(await _cloudTable.ExistsAsync(null, null));
        }

        [Fact]
        public async Task CreateAsync_WhenTableDoesNotExist_CreatesTable()
        {
            await _cloudTable.CreateAsync(null, null);

            Assert.True(await _cloudTable.ExistsAsync(null, null));
        }

        [Fact]
        public async Task CreateAsync_WhenTableExists_ThrowsException()
        {
            await _cloudTable.CreateAsync(null, null);

            var exception = await Assert.ThrowsAsync<StorageException>(() => _cloudTable.CreateAsync(null, null));

            Assert.Equal("Conflict", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(409, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Conflict", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory]
        [InlineData("tables")]
        [InlineData("invalid_table_name")]
        [InlineData("1nvalid")]
        [InlineData(" ")]
        [InlineData("t")]
        [InlineData("tt")]
        [InlineData("testTableNameHavingALengthOf63CharactersSomeOfThemAreJustExtra1s")]
        public async Task CreateAsync_WhenTableNameIsInvalid_ThrowsException(string tableName)
        {
            var cloudTable = _GetCloudTable(tableName);

            var exception = await Assert.ThrowsAsync<StorageException>(() => cloudTable.CreateAsync(null, null));

            Assert.Equal("Bad Request", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Bad Request", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public async Task CreateIfNotExistsAsync_WhenTableDoesNotExist_ReturnsFalse()
        {
            Assert.True(await _cloudTable.CreateIfNotExistsAsync(null, null));

            Assert.True(await _cloudTable.ExistsAsync(null, null));
        }

        [Fact]
        public async Task CreateIfNotExistsAsync_WhenTableExists_ReturnsFalse()
        {
            await _cloudTable.CreateAsync(null, null);

            Assert.False(await _cloudTable.CreateIfNotExistsAsync(null, null));
        }

        [Fact]
        public async Task DeleteAsync_WhenTableDoesNotExist_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<StorageException>(() => _cloudTable.DeleteAsync(null, null));

            Assert.Equal("Not Found", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(404, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Not Found", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public async Task DeleteAsync_WhenTableExists_DeletesTable()
        {
            await _cloudTable.CreateAsync(null, null);

            await _cloudTable.DeleteAsync(null, null);

            Assert.False(await _cloudTable.ExistsAsync(null, null));
        }

        [Fact]
        public async Task DeleteIfExistsAsync_WhenTableDoesNotExist_ReturnsFalse()
        {
            Assert.False(await _cloudTable.DeleteIfExistsAsync(null, null));
        }

        [Fact]
        public async Task DeleteIfExistsAsync_WhenTableExists_ReturnsTrue()
        {
            await _cloudTable.CreateAsync(null, null);

            Assert.True(await _cloudTable.DeleteIfExistsAsync(null, null));
            Assert.False(await _cloudTable.DeleteIfExistsAsync(null, null));
        }

        [Fact]
        public async Task ExecuteAsync_InsertOperation_InsertsEntity()
        {
            await _cloudTable.CreateAsync();
            var startTime = DateTimeOffset.UtcNow.AddSeconds(-1);

            var tableResult = await _cloudTable.ExecuteAsync(
                TableOperation.Insert(
                    _GetTableEntity(
                        "partition-key",
                        "row-key"
                    )
                )
            );

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Equal(resultEntity.ETag, tableResult.Etag);

            var entities = await _GetAllEntitiesAsync();
            var entity = Assert.Single(entities);
            Assert.Equal("partition-key", entity.PartitionKey);
            Assert.Equal("row-key", entity.RowKey);
            Assert.True(startTime <= entity.Timestamp.ToUniversalTime());

            Assert.Equal(entity.PartitionKey, resultEntity.PartitionKey);
            Assert.Equal(entity.RowKey, resultEntity.RowKey);
            Assert.Equal(entity.ETag, resultEntity.ETag);
            Assert.Equal(entity.Timestamp, resultEntity.Timestamp);
        }

        [Theory]
        [ClassData(typeof(TableKeyTestData))]
        public async Task ExecuteAsync_InsertOperation_ThrowsExceptionForInvalidPartitionKey(string partitionKey)
        {
            await _cloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => _cloudTable.ExecuteAsync(
                    TableOperation.Insert(
                        _GetTableEntity(
                            partitionKey,
                            "row-key"
                        )
                    )
                )
            );

            Assert.Equal("Bad Request", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Bad Request", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory]
        [ClassData(typeof(TableKeyTestData))]
        public async Task ExecuteAsync_InsertOperation_ThrowsExceptionForInvalidRowKey(string rowKey)
        {
            await _cloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => _cloudTable.ExecuteAsync(
                    TableOperation.Insert(
                        _GetTableEntity(
                            "partition-key",
                            rowKey
                        )
                    )
                )
            );

            Assert.Equal("Bad Request", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Bad Request", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public void TableOperation_Insert_ThrowsExceptionWithNullEntity()
        {
            var exception = Assert.Throws<ArgumentNullException>("entity", () => TableOperation.Insert(null));
            Assert.Equal(new ArgumentNullException("entity").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteBatchAsync_Throws_NotImplementedException()
        {
            await Assert.ThrowsAsync<NotImplementedException>(() => _cloudTable.ExecuteBatchAsync(null, null, null));
        }

        [Fact]
        public async Task GetPermissionsAsync_Throws_NotImplementedException()
        {
            await Assert.ThrowsAsync<NotImplementedException>(() => _cloudTable.GetPermissionsAsync(null, null));
        }

        [Fact]
        public async Task SetPermissionsAsync_Throws_NotImplementedException()
        {
            await Assert.ThrowsAsync<NotImplementedException>(() => _cloudTable.SetPermissionsAsync(null, null, null));
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Task.Run(() => _cloudTable.DeleteIfExistsAsync()).Wait();
            base.Dispose(disposing);
        }

        private CloudTable _GetCloudTable(string tableName)
            => _useInMemory ?
                new InMemoryCloudTable(tableName) :
                CloudStorageAccount
                    .Parse(AzureStorageConnectionString)
                    .CreateCloudTableClient()
                    .GetTableReference(tableName);

        private static ITableEntity _GetTableEntity(string partitionKey, string rowKey)
            => new TableEntity(partitionKey, rowKey);

        private static ITableEntity _GetTableEntity<TEntity>(string partitionKey, string rowKey, TEntity entity)
            => new TableEntityAdapter<TEntity>(entity, partitionKey, rowKey);

        private Task<IReadOnlyCollection<ITableEntity>> _GetAllEntitiesAsync()
            => _GetAllEntitiesAsync(_cloudTable);

        private static async Task<IReadOnlyCollection<ITableEntity>> _GetAllEntitiesAsync(CloudTable cloudTable)
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
    }
}