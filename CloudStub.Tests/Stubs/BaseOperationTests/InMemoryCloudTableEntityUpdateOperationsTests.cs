using System;
using System.Collections;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace CloudStub.Tests.BaseOperationTests
{
    public abstract class InMemoryCloudTableEntityUpdateOperationsTests : InMemoryCloudTableEntityEditOperationsTests
    {
        [Theory]
        [ClassData(typeof(TableInvalidStringPropertyTestData))]
        public virtual async Task ExecuteAsync_WhenStringPropertyIsInvalid_ThrowsException(string stringPropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = "*"
            };
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                StringProp = stringPropValue,
                ETag = testEntity.ETag
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            var exception = await Assert.ThrowsAsync<StorageException>(() => CloudTable.ExecuteAsync(GetOperation(updatedTestEntity)));

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

        [Theory]
        [ClassData(typeof(TableInvalidBinaryPropertyTestData))]
        public virtual async Task ExecuteAsync_WhenBinaryPropertyIsInvalid_ThrowsException(byte[] binaryPropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = "*"
            };
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                BinaryProp = binaryPropValue,
                ETag = testEntity.ETag
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            var exception = await Assert.ThrowsAsync<StorageException>(() => CloudTable.ExecuteAsync(GetOperation(updatedTestEntity)));

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

        [Theory]
        [ClassData(typeof(TableInvalidDateTimePropertyTestData))]
        public virtual async Task ExecuteAsync_WhenDateTimePropertyIsInvalid_ThrowsException(DateTime dateTimePropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = "*"
            };
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                DateTimeProp = dateTimePropValue,
                ETag = testEntity.ETag
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            var exception = await Assert.ThrowsAsync<StorageException>(() => CloudTable.ExecuteAsync(GetOperation(updatedTestEntity)));

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
        public virtual async Task ExecuteAsync_WhenEntityDoesNotExist_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = new string('t', 1 << 10 + 1),
                RowKey = new string('t', 1 << 10 + 1),
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(() => CloudTable.ExecuteAsync(GetOperation(testEntity)));

            AssertExceptionMessageWithFallback("Not Found", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(404, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Equal(ExpectedErrorCode, exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            AssertExceptionMessageWithFallback("Not Found", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public virtual async Task ExecuteAsync_WhenETagsMismatch_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            await CloudTable.CreateAsync();
            var result = await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));
            var updatedTestEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = result.Etag
            };
            await CloudTable.ExecuteAsync(TableOperation.InsertOrReplace(testEntity));

            var exception = await Assert.ThrowsAsync<StorageException>(() => CloudTable.ExecuteAsync(GetOperation(updatedTestEntity)));

            AssertExceptionMessageWithFallback("Precondition Failed", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(412, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Equal(ExpectedErrorCode, exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            AssertExceptionMessageWithFallback("Precondition Failed", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }
    }
}