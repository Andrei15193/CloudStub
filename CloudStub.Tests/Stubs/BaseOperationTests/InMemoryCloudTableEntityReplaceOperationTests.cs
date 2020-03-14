using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace CloudStub.Tests.BaseOperationTests
{
    public abstract class InMemoryCloudTableEntityReplaceOperationTests : InMemoryCloudTableEntityUpdateOperationsTests
    {
        [Fact]
        public virtual async Task ExecuteAsync_WhenPartitionKeyIsNull_ThrowsException()
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
                () => ExecuteAsync(GetOperation(testEntity))
            );

            Assert.Equal(new ArgumentNullException("Replace requires a valid PartitionKey").Message, exception.Message);
        }

        [Fact]
        public virtual async Task ExecuteAsync_WhenRowKeyIsNull_ThrowsException()
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
                () => ExecuteAsync(GetOperation(testEntity))
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

            var tableResults = await ExecuteAsync(GetOperation(updatedTestEntity));
            var tableResult = Assert.Single(tableResults);

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

            var tableResults = await ExecuteAsync(GetOperation(updatedTestEntity));
            tableResult = Assert.Single(tableResults);

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

        protected sealed override TableOperation GetOperation(ITableEntity entity)
            => TableOperation.Replace(entity);
    }
}