using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CloudStub.Tests.BaseOperationTests;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace CloudStub.Tests.TableBatchOperationTests
{
    public sealed class InMemoryCloudTableEntityRetrieveTableBatchOperationTests : InMemoryCloudTableEntityRetrieveOperationTests
    {
        protected override async Task<IEnumerable<TableResult>> ExecuteAsync(TableOperation tableOperation)
            => await CloudTable.ExecuteBatchAsync(new TableBatchOperation { tableOperation });

        [Fact]
        public override async Task ExecuteAsync_WhenEntityExistsAndOnlySomePropertiesAreSelected_RetrievesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = new string('t', 1 << 15),
                Int32Prop = 4
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            var tableResults = await ExecuteAsync(
                TableOperation.Retrieve(testEntity.PartitionKey, testEntity.RowKey, new List<string> { nameof(TestEntity.StringProp) })
            );
            var tableResult = Assert.Single(tableResults);

            Assert.Equal(200, tableResult.HttpStatusCode);
            Assert.NotNull(tableResult.Etag);

            var entity = (ITableEntity)tableResult.Result;
            Assert.Equal(testEntity.PartitionKey, entity.PartitionKey);
            Assert.Equal(testEntity.RowKey, entity.RowKey);

            var actualProps = entity.WriteEntity(null);
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.PartitionKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.RowKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.Timestamp)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.ETag)));
            Assert.Equal(new EntityProperty(testEntity.StringProp), actualProps[nameof(TestEntity.StringProp)]);
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.BinaryProp)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.BooleanProp)));
            Assert.True(actualProps.ContainsKey(nameof(TestEntity.Int32Prop)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.Int64Prop)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.DoubleProp)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.DateTimeProp)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.GuidProp)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.DecimalProp)));

            Assert.Equal(tableResult.Etag, entity.ETag);
        }

        [Fact]
        public override async Task ExecuteAsync_WhenTypedEntityOperationIsUsedAndOnlySelectedFieldsAreSpecified_RetrievesSpecificEntity()
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

            var tableResults = await ExecuteAsync(
                TableOperation.Retrieve<TestEntity>(testEntity.PartitionKey, testEntity.RowKey, new List<string> { nameof(TestEntity.StringProp) })
            );
            var tableResult = Assert.Single(tableResults);

            Assert.Equal(200, tableResult.HttpStatusCode);
            Assert.NotNull(tableResult.Etag);

            var entity = Assert.IsAssignableFrom<TestEntity>(tableResult.Result);
            Assert.Equal(testEntity.PartitionKey, entity.PartitionKey);
            Assert.Equal(testEntity.RowKey, entity.RowKey);
            Assert.Equal(testEntity.StringProp, entity.StringProp);
            Assert.Equal(testEntity.BinaryProp, entity.BinaryProp);
            Assert.Equal(testEntity.BooleanProp, entity.BooleanProp);
            Assert.Equal(testEntity.Int32Prop, entity.Int32Prop);
            Assert.Equal(testEntity.Int64Prop, entity.Int64Prop);
            Assert.Equal(testEntity.DoubleProp, entity.DoubleProp);
            Assert.Equal(testEntity.DateTimeProp, entity.DateTimeProp);
            Assert.Equal(testEntity.GuidProp, entity.GuidProp);
            Assert.Null(entity.DecimalProp);

            Assert.Equal(tableResult.Etag, entity.ETag);
        }

        [Fact]
        public override async Task ExecuteAsync_WhenResolverIsUsedAndOnlySomeColumnsAreSelected_RetrievesEntityWithSelectedColumns()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = new string('t', 1 << 15),
                Int32Prop = 4
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            var tableResults = await ExecuteAsync(
                TableOperation.Retrieve(
                    testEntity.PartitionKey,
                    testEntity.RowKey,
                    (partitonKey, rowKey, timestamp, properties, etag) => new DynamicTableEntity
                    {
                        PartitionKey = partitonKey,
                        RowKey = rowKey,
                        Timestamp = timestamp,
                        Properties = properties,
                        ETag = etag
                    },
                    new List<string> { nameof(TestEntity.StringProp) }
                )
            );
            var tableResult = Assert.Single(tableResults);

            Assert.Equal(200, tableResult.HttpStatusCode);
            Assert.NotNull(tableResult.Etag);

            var entity = Assert.IsAssignableFrom<DynamicTableEntity>(tableResult.Result);
            Assert.Equal(testEntity.PartitionKey, entity.PartitionKey);
            Assert.Equal(testEntity.RowKey, entity.RowKey);

            var entityProps = entity.Properties;
            Assert.False(entityProps.ContainsKey(nameof(TestEntity.PartitionKey)));
            Assert.False(entityProps.ContainsKey(nameof(TestEntity.RowKey)));
            Assert.False(entityProps.ContainsKey(nameof(TestEntity.Timestamp)));
            Assert.False(entityProps.ContainsKey(nameof(TestEntity.ETag)));
            Assert.Equal(new EntityProperty(testEntity.StringProp), entityProps[nameof(TestEntity.StringProp)]);
            Assert.False(entityProps.ContainsKey(nameof(TestEntity.BinaryProp)));
            Assert.False(entityProps.ContainsKey(nameof(TestEntity.BooleanProp)));
            Assert.Equal(new EntityProperty(testEntity.Int32Prop), entityProps[nameof(TestEntity.Int32Prop)]);
            Assert.False(entityProps.ContainsKey(nameof(TestEntity.Int64Prop)));
            Assert.False(entityProps.ContainsKey(nameof(TestEntity.DoubleProp)));
            Assert.False(entityProps.ContainsKey(nameof(TestEntity.DateTimeProp)));
            Assert.False(entityProps.ContainsKey(nameof(TestEntity.GuidProp)));
            Assert.False(entityProps.ContainsKey(nameof(TestEntity.DecimalProp)));

            Assert.Equal(tableResult.Etag, entity.ETag);
        }
    }
}