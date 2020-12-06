using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Xunit;

namespace CloudStub.Tests.TableBatchOperationTests
{
    public class InMemoryCloudTableEntityInsertOrMergeTableBatchOperationTests : BaseInMemoryCloudTableTests
    {
        [Fact]
        public async Task ExecuteAsync_WhenTableDoesNotExist_ThrowsException()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = "*"
            };

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteBatchAsync(new TableBatchOperation { TableOperation.InsertOrMerge(testEntity) })
            );

            Assert.Matches(
                "^0:The table specified does not exist.\n"
                + $"RequestId:{exception.RequestInformation.ServiceRequestID}\n"
                + @"Time:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7}Z$",
                exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(404, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Null(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("TableNotFound", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^0:The table specified does not exist.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public void TableOperation_WhenEntityIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                "entity",
                () => TableOperation.InsertOrMerge(null)
            );
            Assert.Equal(new ArgumentNullException("entity").Message, exception.Message);
        }

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

            var tableResults = await CloudTable.ExecuteBatchAsync(new TableBatchOperation { TableOperation.InsertOrMerge(updatedTestEntity) });
            var tableResult = Assert.Single(tableResults);

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

        [Fact]
        public async Task ExecuteAsync_WhenPartitionKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "item",
                () => CloudTable.ExecuteBatchAsync(new TableBatchOperation { TableOperation.InsertOrMerge(new TableEntity(null, "row-key")) })
            );

            Assert.Equal(
                new ArgumentNullException("item", "A batch non-retrieve operation requires a non-null partition key and row key.").Message,
                exception.Message
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task ExecuteAsync_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            var testEntity = new TableEntity
            {
                PartitionKey = partitionKey,
                RowKey = "row-key",
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteBatchAsync(new TableBatchOperation { TableOperation.InsertOrMerge(testEntity) })
            );

            Assert.Matches(@"^Element \d+ in the batch returned an unexpected response code.$", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Null(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            switch (partitionKey)
            {
                case "/":
                case "\\":
                    Assert.Equal("InvalidInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
                    Assert.Matches(
                        @$"^0:Bad Request - Error in query syntax.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                        exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
                    );
                    break;

                default:
                    Assert.Equal("OutOfRangeInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
                    Assert.Matches(
                        @$"^0:One of the request inputs is out of range.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                        exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
                    );
                    break;
            }

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public async Task ExecuteAsync_WhenPartitionKeyExceedsLimit_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = new string('t', 1 << 10 + 1),
                RowKey = "row-key",
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteBatchAsync(new TableBatchOperation { TableOperation.InsertOrMerge(testEntity) })
            );

            Assert.Matches(@"^Element \d+ in the batch returned an unexpected response code.$", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Null(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("PropertyValueTooLarge", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The property value exceeds the maximum allowed size \(64KB\). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public async Task ExecuteAsync_WhenRowKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "item",
                () => CloudTable.ExecuteBatchAsync(new TableBatchOperation { TableOperation.InsertOrMerge(new TableEntity("partition-key", null)) })
            );

            Assert.Equal(
                new ArgumentNullException("item", "A batch non-retrieve operation requires a non-null partition key and row key.").Message,
                exception.Message
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task ExecuteAsync_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = rowKey,
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteBatchAsync(new TableBatchOperation { TableOperation.InsertOrMerge(testEntity) })
            );

            Assert.Matches(@"^Element \d+ in the batch returned an unexpected response code.$", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Null(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            switch (rowKey)
            {
                case "/":
                case "\\":
                    Assert.Equal("InvalidInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
                    Assert.Matches(
                        @$"^0:Bad Request - Error in query syntax.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                        exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
                    );
                    break;

                default:
                    Assert.Equal("OutOfRangeInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
                    Assert.Matches(
                        @$"^0:One of the request inputs is out of range.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                        exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
                    );
                    break;
            }

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public async Task ExecuteAsync_WhenRowKeyExceedsLimit_ThrowsException()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = new string('t', 1 << 10 + 1),
                ETag = "*"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteBatchAsync(new TableBatchOperation { TableOperation.InsertOrMerge(testEntity) })
            );

            Assert.Matches(@"^Element \d+ in the batch returned an unexpected response code.$", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Null(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("PropertyValueTooLarge", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The property value exceeds the maximum allowed size \(64KB\). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidStringData), MemberType = typeof(TableOperationTestData))]
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

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteBatchAsync(new TableBatchOperation { TableOperation.InsertOrMerge(testEntity) })
            );

            Assert.Matches(@"^Element \d+ in the batch returned an unexpected response code.$", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Null(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("PropertyValueTooLarge", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The property value exceeds the maximum allowed size \(64KB\). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidBinaryData), MemberType = typeof(TableOperationTestData))]
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

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteBatchAsync(new TableBatchOperation { TableOperation.InsertOrMerge(testEntity) })
            );

            Assert.Matches(@"^Element \d+ in the batch returned an unexpected response code.$", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Null(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("PropertyValueTooLarge", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The property value exceeds the maximum allowed size \(64KB\). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidDateTimeData), MemberType = typeof(TableOperationTestData))]
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

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteBatchAsync(new TableBatchOperation { TableOperation.InsertOrMerge(testEntity) })
            );

            Assert.Matches(@"^Element \d+ in the batch returned an unexpected response code.$", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Null(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("OutOfRangeInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^0:The 'DateTimeProp' parameter of value '{dateTimePropValue:MM/dd/yyyy HH:mm:ss}' is out of range.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        protected async Task<IEnumerable<TableResult>> ExecuteAsync(TableOperation tableOperation)
            => await CloudTable.ExecuteBatchAsync(new TableBatchOperation { tableOperation });
    }
}