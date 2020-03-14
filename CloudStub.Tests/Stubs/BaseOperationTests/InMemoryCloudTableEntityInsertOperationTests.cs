using System;
using System.Collections;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace CloudStub.Tests.BaseOperationTests
{
    public abstract class InMemoryCloudTableEntityInsertOperationTests : InMemoryCloudTableEntityInsertOperationsTests
    {
        [Fact]
        public virtual async Task ExecuteAsync_InsertOperationWhenPartitionKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => ExecuteAsync(GetOperation(new TableEntity(null, "row-key")))
            );

            AssertExceptionMessageWithFallback("Bad Request", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Equal(ExpectedErrorCode, exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            AssertExceptionMessageWithFallback("Bad Request", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public virtual async Task ExecuteAsync_WhenRowKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => ExecuteAsync(GetOperation(new TableEntity("partition-key", null)))
            );

            AssertExceptionMessageWithFallback("Bad Request", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Equal(ExpectedErrorCode, exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            AssertExceptionMessageWithFallback("Bad Request", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public virtual async Task ExecuteAsync_InsertOperationWhenEntityAlreadyExists_ThrowsException()
        {
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(new TableEntity("partition-key", "row-key")));

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => ExecuteAsync(GetOperation(new TableEntity("partition-key", "row-key")))
            );

            AssertExceptionMessageWithFallback("Conflict", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(409, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Equal(ExpectedErrorCode, exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            AssertExceptionMessageWithFallback("Conflict", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public async Task TableOperation_InsertOperationWhenEchoContentIsFalse_HasNoEffect()
        {
            var startTime = DateTimeOffset.UtcNow;
            await CloudTable.CreateAsync();

            var tableResults = await ExecuteAsync(TableOperation.Insert(new TableEntity("partition-key", "row-key"), echoContent: false));
            var tableResult = Assert.Single(tableResults);

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal("partition-key", resultEntity.PartitionKey);
            Assert.Equal("row-key", resultEntity.RowKey);
            Assert.NotNull(resultEntity.ETag);
            Assert.True(startTime.AddSeconds(-10) <= resultEntity.Timestamp.ToUniversalTime());
        }

        protected sealed override TableOperation GetOperation(ITableEntity entity)
            => TableOperation.Insert(entity);
    }
}