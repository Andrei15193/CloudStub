using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace CloudStub.Tests.BaseOperationTests
{
    public abstract class InMemoryCloudTableEntityInsertOrReplaceOperationTests : InMemoryCloudTableEntityInsertOperationsTests
    {
        [Fact]
        public virtual async Task ExecuteAsync_InsertOrReplaceOperationWhenPartitionKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "Upserts require a valid PartitionKey",
                () => ExecuteAsync(GetOperation(new TableEntity(null, "row-key")))
            );

            Assert.Equal(new ArgumentNullException("Upserts require a valid PartitionKey").Message, exception.Message);
        }

        [Fact]
        public virtual async Task ExecuteAsync_WhenRowKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "Upserts require a valid RowKey",
                () => ExecuteAsync(GetOperation(new TableEntity("partition-key", null)))
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

            var tableResults = await ExecuteAsync(GetOperation(updatedTestEntity));
            var tableResult = Assert.Single(tableResults);

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

        protected sealed override TableOperation GetOperation(ITableEntity entity)
            => TableOperation.InsertOrReplace(entity);
    }
}