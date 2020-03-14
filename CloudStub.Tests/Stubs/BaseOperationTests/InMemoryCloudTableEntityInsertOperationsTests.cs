using System;
using System.Collections;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace CloudStub.Tests.BaseOperationTests
{
    public abstract class InMemoryCloudTableEntityInsertOperationsTests : InMemoryCloudTableEntityEditOperationsTests
    {
        [Fact]
        public async Task ExecuteAsync_InsertOperation_InsertsEntity()
        {
            var startTime = DateTimeOffset.UtcNow;
            var tableEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            await CloudTable.CreateAsync();

            var tableResult = await CloudTable.ExecuteAsync(TableOperation.Insert(tableEntity));

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Equal(resultEntity.ETag, tableResult.Etag);

            var entities = await GetAllEntitiesAsync();
            var entity = Assert.Single(entities);
            Assert.Equal("partition-key", entity.PartitionKey);
            Assert.Equal("row-key", entity.RowKey);
            Assert.True(startTime.AddSeconds(-10) <= entity.Timestamp.ToUniversalTime());

            Assert.Equal(entity.PartitionKey, resultEntity.PartitionKey);
            Assert.Equal(entity.RowKey, resultEntity.RowKey);
            Assert.Equal(entity.ETag, resultEntity.ETag);
            Assert.Equal(entity.Timestamp, resultEntity.Timestamp);
        }

        [Fact]
        public async Task ExecuteAsync_InserOperationWhenEntityHasOtherProperties_InsertsEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                BinaryProp = new byte[1 << 16],
                BooleanProp = true,
                StringProp = new string('t', 1 << 15),
                Int32Prop = 4,
                Int64Prop = 4,
                DoubleProp = 4,
                DateTimeProp = DateTime.MaxValue.ToUniversalTime(),
                GuidProp = Guid.NewGuid(),
                DecimalProp = 4
            };
            await CloudTable.CreateAsync();

            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            var entities = await GetAllEntitiesAsync();
            var entity = Assert.Single(entities);
            var actualProps = entity.WriteEntity(null);
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.PartitionKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.RowKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.Timestamp)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.ETag)));
            Assert.Equal(new EntityProperty(testEntity.BinaryProp), actualProps[nameof(TestEntity.BinaryProp)]);
            Assert.Equal(new EntityProperty(testEntity.BooleanProp), actualProps[nameof(TestEntity.BooleanProp)]);
            Assert.Equal(new EntityProperty(testEntity.StringProp), actualProps[nameof(TestEntity.StringProp)]);
            Assert.Equal(new EntityProperty(testEntity.Int32Prop), actualProps[nameof(TestEntity.Int32Prop)]);
            Assert.Equal(new EntityProperty(testEntity.Int64Prop), actualProps[nameof(TestEntity.Int64Prop)]);
            Assert.Equal(new EntityProperty(testEntity.DoubleProp), actualProps[nameof(TestEntity.DoubleProp)]);
            Assert.Equal(new EntityProperty(testEntity.DateTimeProp), actualProps[nameof(TestEntity.DateTimeProp)]);
            Assert.Equal(new EntityProperty(testEntity.GuidProp), actualProps[nameof(TestEntity.GuidProp)]);
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.DecimalProp)));
        }

        [Fact]
        public Task ExecuteAsync_WhenPartitionKeyExceedsLimit_ThrowsException()
            => ExecuteAsync_WhenPartitionKeyIsInvalid_ThrowsException(new string('t', 1 << 10 + 1));

        [Fact]
        public Task ExecuteAsync_WhenRowKeyExceedsLimit_ThrowsException()
            => ExecuteAsync_WhenRowKeyIsInvalid_ThrowsException(new string('t', 1 << 10 + 1));

        [Theory]
        [ClassData(typeof(TableInvalidStringPropertyTestData))]
        public async Task ExecuteAsync_WhenStringPropertyIsInvalid_ThrowsException(string stringPropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = stringPropValue,
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(() => ExecuteAsync(GetOperation(testEntity)));

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
        public async Task ExecuteAsync_WhenBinaryPropertyIsInvalid_ThrowsException(byte[] binaryPropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                BinaryProp = binaryPropValue,
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(() => ExecuteAsync(GetOperation(testEntity)));

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
        public async Task ExecuteAsync_WhenDateTimePropertyIsInvalid_ThrowsException(DateTime dateTimePropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                DateTimeProp = dateTimePropValue,
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(() => ExecuteAsync(GetOperation(testEntity)));

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
    }
}