using System;
using System.Collections;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace CloudStub.Tests
{
    public abstract class InMemoryCloudTableInsertOperationTests : InMemoryCloudTableEntityOperationTests
    {
        [Fact]
        public async Task ExecuteAsync_InsertOperation_InsertsEntity()
        {
            await CloudTable.CreateAsync();
            var startTime = DateTimeOffset.UtcNow;

            var tableResult = await CloudTable.ExecuteAsync(
                TableOperation.Insert(new TableEntity("partition-key", "row-key"))
            );

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
    }

    public class InMemoryCloudTableInsertTests : InMemoryCloudTableInsertOperationTests
    {
        [Fact]
        public async Task ExecuteAsync_InsertOperationWhenPartitionKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteAsync(GetOperation(new TableEntity(null, "row-key")))
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
        public async Task ExecuteAsync_InsertOperationWhenRowKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteAsync(GetOperation(new TableEntity("partition-key", null)))
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
        public async Task ExecuteAsync_InsertOperationWhenEntityAlreadyExists_ThrowsException()
        {
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(GetOperation(new TableEntity("partition-key", "row-key")));

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteAsync(GetOperation(new TableEntity("partition-key", "row-key")))
            );

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

        [Fact]
        public async Task TableOperation_InsertOperationWhenEchoContentIsFalse_HasNoEffect()
        {
            var startTime = DateTimeOffset.UtcNow;
            await CloudTable.CreateAsync();

            var result = await CloudTable.ExecuteAsync(TableOperation.Insert(new TableEntity("partition-key", "row-key"), echoContent: false));

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(result.Result);
            Assert.Equal("partition-key", resultEntity.PartitionKey);
            Assert.Equal("row-key", resultEntity.RowKey);
            Assert.NotNull(resultEntity.ETag);
            Assert.True(startTime.AddSeconds(-10) <= resultEntity.Timestamp.ToUniversalTime());
        }

        protected override TableOperation GetOperation(ITableEntity entity)
            => TableOperation.Insert(entity);
    }

    public class InMemoryCloudTableInsertOrReplaceTests : InMemoryCloudTableInsertOperationTests
    {
        [Fact]
        public async Task ExecuteAsync_InsertOrReplaceOperationWhenPartitionKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "Upserts require a valid PartitionKey",
                () => CloudTable.ExecuteAsync(GetOperation(new TableEntity(null, "row-key")))
            );

            Assert.Equal(new ArgumentNullException("Upserts require a valid PartitionKey").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteAsync_InsertOrReplaceOperationWhenRowKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "Upserts require a valid RowKey",
                () => CloudTable.ExecuteAsync(GetOperation(new TableEntity("partition-key", null)))
            );

            Assert.Equal(new ArgumentNullException("Upserts require a valid RowKey").Message, exception.Message);
        }

        [Fact]
        public async Task TableOperation_InsertOrReplaceOperation_RepleacesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string value",
                Int32Prop = 4
            };
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                ETag = "this is not a vaild e-tag, it's ignored",
                Int32Prop = 8,
                Int64Prop = 8
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            var tableResult = await CloudTable.ExecuteAsync(TableOperation.InsertOrReplace(updatedTestEntity));
            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Equal(resultEntity.ETag, tableResult.Etag);

            var entities = await GetAllEntitiesAsync();
            var entity = Assert.Single(entities);
            var entityProps = entity.WriteEntity(null);
            Assert.False(entityProps.ContainsKey(nameof(TestEntity.StringProp)));
            Assert.Equal(new EntityProperty(updatedTestEntity.Int32Prop), entityProps[nameof(TestEntity.Int32Prop)]);
            Assert.Equal(new EntityProperty(updatedTestEntity.Int64Prop), entityProps[nameof(TestEntity.Int64Prop)]);

            Assert.Equal(entity.PartitionKey, resultEntity.PartitionKey);
            Assert.Equal(entity.RowKey, resultEntity.RowKey);
            Assert.Equal(entity.ETag, resultEntity.ETag);
            Assert.Equal(default(DateTimeOffset), resultEntity.Timestamp);
        }

        protected override TableOperation GetOperation(ITableEntity entity)
            => TableOperation.InsertOrReplace(entity);
    }

    public class InMemoryCloudTableInsertOrMergeTests : InMemoryCloudTableInsertOperationTests
    {
        [Fact]
        public async Task ExecuteAsync_InsertOrMergeOperationWhenPartitionKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "Upserts require a valid PartitionKey",
                () => CloudTable.ExecuteAsync(GetOperation(new TableEntity(null, "row-key")))
            );

            Assert.Equal(new ArgumentNullException("Upserts require a valid PartitionKey").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteAsync_InsertOrMergeOperationWhenRowKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "Upserts require a valid RowKey",
                () => CloudTable.ExecuteAsync(GetOperation(new TableEntity("partition-key", null)))
            );

            Assert.Equal(new ArgumentNullException("Upserts require a valid RowKey").Message, exception.Message);
        }

        [Fact]
        public async Task TableOperation_InsertOrMergeOperation_MergesEntities()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string value",
                Int32Prop = 4
            };
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                ETag = "this is not a vaild e-tag, it's ignored",
                Int32Prop = 8,
                Int64Prop = 8
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            var tableResult = await CloudTable.ExecuteAsync(TableOperation.InsertOrMerge(updatedTestEntity));
            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Equal(resultEntity.ETag, tableResult.Etag);

            var entities = await GetAllEntitiesAsync();
            var entity = Assert.Single(entities);
            var entityProps = entity.WriteEntity(null);
            Assert.Equal(new EntityProperty(testEntity.StringProp), entityProps[nameof(TestEntity.StringProp)]);
            Assert.Equal(new EntityProperty(updatedTestEntity.Int32Prop), entityProps[nameof(TestEntity.Int32Prop)]);
            Assert.Equal(new EntityProperty(updatedTestEntity.Int64Prop), entityProps[nameof(TestEntity.Int64Prop)]);

            Assert.Equal(entity.PartitionKey, resultEntity.PartitionKey);
            Assert.Equal(entity.RowKey, resultEntity.RowKey);
            Assert.Equal(entity.ETag, resultEntity.ETag);
            Assert.Equal(default(DateTimeOffset), resultEntity.Timestamp);
        }

        protected override TableOperation GetOperation(ITableEntity entity)
            => TableOperation.InsertOrMerge(entity);
    }
}