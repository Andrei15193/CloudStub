﻿using System;
using System.Collections;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace CloudStub.Tests
{
    public abstract class InMemoryCloudTableEntityUpdateAndDeleteOperationTests : InMemoryCloudTableEntityOperationTests
    {
        [Theory]
        [ClassData(typeof(TableInvalidStringPropertyTestData))]
        public async Task ExecuteAsync_WhenStringPropertyIsInvalid_ThrowsException(string stringPropValue)
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
        [ClassData(typeof(TableInvalidBinaryPropertyTestData))]
        public async Task ExecuteAsync_WhenBinaryPropertyIsInvalid_ThrowsException(byte[] binaryPropValue)
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
        [ClassData(typeof(TableInvalidDateTimePropertyTestData))]
        public async Task ExecuteAsync_WhenDateTimePropertyIsInvalid_ThrowsException(DateTime dateTimePropValue)
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
        public async Task ExecuteAsync_WhenEntityDoesNotExist_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = new string('t', 1 << 10 + 1),
                RowKey = new string('t', 1 << 10 + 1),
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(() => CloudTable.ExecuteAsync(GetOperation(testEntity)));

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
        public async Task ExecuteAsync_WhenETagsMismatch_ThrowsException()
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

            Assert.Equal("Precondition Failed", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(412, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Precondition Failed", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }
    }

    public class InMemoryCloudTableEntityReplaceOperationTests : InMemoryCloudTableEntityUpdateAndDeleteOperationTests
    {
        [Fact]
        public async Task ExecuteAsync_WhenPartitionKeyIsNull_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = null,
                RowKey = "row-key",
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "Replace requires a valid PartitionKey",
                () => CloudTable.ExecuteAsync(GetOperation(testEntity))
            );

            Assert.Equal(new ArgumentNullException("Replace requires a valid PartitionKey").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteAsync_WhenRowKeyIsNull_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = null,
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "Replace requires a valid RowKey",
                () => CloudTable.ExecuteAsync(GetOperation(testEntity))
            );

            Assert.Equal(new ArgumentNullException("Replace requires a valid RowKey").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteAsync_WhenETagsIsWildcard_ReplacesEntity()
        {
            var startTime = DateTimeOffset.UtcNow;
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string-prop"
            };
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8,
                ETag = "*"
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            var tableResult = await CloudTable.ExecuteAsync(GetOperation(updatedTestEntity));

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Equal(resultEntity.ETag, tableResult.Etag);

            var entities = await GetAllEntitiesAsync();
            var entity = Assert.Single(entities);
            var actualProps = entity.WriteEntity(null);
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.PartitionKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.RowKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.Timestamp)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.ETag)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.StringProp)));
            Assert.Equal(new EntityProperty(updatedTestEntity.Int32Prop), actualProps[nameof(TestEntity.Int32Prop)]);

            Assert.Equal(entity.PartitionKey, resultEntity.PartitionKey);
            Assert.Equal(entity.RowKey, resultEntity.RowKey);
            Assert.Equal(entity.ETag, resultEntity.ETag);
            Assert.Equal(default(DateTimeOffset), resultEntity.Timestamp);
        }

        [Fact]
        public async Task ExecuteAsync_WhenETagsMatch_ReplacesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string-prop"
            };
            await CloudTable.CreateAsync();
            var tableResult = await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8,
                ETag = tableResult.Etag
            };

            tableResult = await CloudTable.ExecuteAsync(GetOperation(updatedTestEntity));

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Equal(resultEntity.ETag, tableResult.Etag);

            var entities = await GetAllEntitiesAsync();
            var entity = Assert.Single(entities);
            var actualProps = entity.WriteEntity(null);
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.PartitionKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.RowKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.Timestamp)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.ETag)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.StringProp)));
            Assert.Equal(new EntityProperty(updatedTestEntity.Int32Prop), actualProps[nameof(TestEntity.Int32Prop)]);

            Assert.Equal(entity.PartitionKey, resultEntity.PartitionKey);
            Assert.Equal(entity.RowKey, resultEntity.RowKey);
            Assert.Equal(entity.ETag, resultEntity.ETag);
            Assert.Equal(default(DateTimeOffset), resultEntity.Timestamp);
        }

        [Fact]
        public void TableOperation_WhenETagIsMissing_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentException>(() => GetOperation(new TableEntity()));
            Assert.Equal(new ArgumentException("Replace requires an ETag (which may be the '*' wildcard).").Message, exception.Message);
        }

        protected override TableOperation GetOperation(ITableEntity entity)
            => TableOperation.Replace(entity);
    }

    public class InMemoryCloudTableEntityMergeOperationTests : InMemoryCloudTableEntityUpdateAndDeleteOperationTests
    {
        [Fact]
        public async Task ExecuteAsync_WhenPartitionKeyIsNull_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = null,
                RowKey = "row-key",
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "Merge requires a valid PartitionKey",
                () => CloudTable.ExecuteAsync(GetOperation(testEntity))
            );

            Assert.Equal(new ArgumentNullException("Merge requires a valid PartitionKey").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteAsync_WhenRowKeyIsNull_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = null,
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "Merge requires a valid RowKey",
                () => CloudTable.ExecuteAsync(GetOperation(testEntity))
            );

            Assert.Equal(new ArgumentNullException("Merge requires a valid RowKey").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteAsync_WhenETagsIsWildcard_MergesEntity()
        {
            var startTime = DateTimeOffset.UtcNow;
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string-prop",
                Int32Prop = 4
            };
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8,
                Int64Prop = 8,
                ETag = "*"
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            var tableResult = await CloudTable.ExecuteAsync(GetOperation(updatedTestEntity));

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Equal(resultEntity.ETag, tableResult.Etag);

            var entities = await GetAllEntitiesAsync();
            var entity = Assert.Single(entities);
            var actualProps = entity.WriteEntity(null);
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.PartitionKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.RowKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.Timestamp)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.ETag)));
            Assert.Equal(new EntityProperty(testEntity.StringProp), actualProps[nameof(TestEntity.StringProp)]);
            Assert.Equal(new EntityProperty(updatedTestEntity.Int32Prop), actualProps[nameof(TestEntity.Int32Prop)]);
            Assert.Equal(new EntityProperty(updatedTestEntity.Int64Prop), actualProps[nameof(TestEntity.Int64Prop)]);

            Assert.Equal(entity.PartitionKey, resultEntity.PartitionKey);
            Assert.Equal(entity.RowKey, resultEntity.RowKey);
            Assert.Equal(entity.ETag, resultEntity.ETag);
            Assert.Equal(default(DateTimeOffset), resultEntity.Timestamp);
        }

        [Fact]
        public async Task ExecuteAsync_WhenETagsMatch_MergesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string-prop",
                Int32Prop = 4
            };
            await CloudTable.CreateAsync();
            var tableResult = await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8,
                Int64Prop = 8,
                ETag = tableResult.Etag
            };

            tableResult = await CloudTable.ExecuteAsync(GetOperation(updatedTestEntity));

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Equal(resultEntity.ETag, tableResult.Etag);

            var entities = await GetAllEntitiesAsync();
            var entity = Assert.Single(entities);
            var actualProps = entity.WriteEntity(null);
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.PartitionKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.RowKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.Timestamp)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.ETag)));
            Assert.Equal(new EntityProperty(testEntity.StringProp), actualProps[nameof(TestEntity.StringProp)]);
            Assert.Equal(new EntityProperty(updatedTestEntity.Int32Prop), actualProps[nameof(TestEntity.Int32Prop)]);
            Assert.Equal(new EntityProperty(updatedTestEntity.Int64Prop), actualProps[nameof(TestEntity.Int64Prop)]);

            Assert.Equal(entity.PartitionKey, resultEntity.PartitionKey);
            Assert.Equal(entity.RowKey, resultEntity.RowKey);
            Assert.Equal(entity.ETag, resultEntity.ETag);
            Assert.Equal(default(DateTimeOffset), resultEntity.Timestamp);
        }

        [Fact]
        public void TableOperation_WhenETagIsMissing_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentException>(() => GetOperation(new TableEntity()));
            Assert.Equal(new ArgumentException("Merge requires an ETag (which may be the '*' wildcard).").Message, exception.Message);
        }

        protected override TableOperation GetOperation(ITableEntity entity)
            => TableOperation.Merge(entity);
    }
}