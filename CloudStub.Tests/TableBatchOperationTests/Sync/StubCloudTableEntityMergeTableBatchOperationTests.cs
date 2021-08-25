using System;
using System.Collections;
using Microsoft.Azure.Cosmos.Table;
using Xunit;

namespace CloudStub.Tests.TableBatchOperationTests.Sync
{
    public class StubCloudTableEntityMergeTableBatchOperationTests : BaseStubCloudTableTests
    {
        [Fact]
        public void Execute_WhenTableDoesNotExist_ThrowsException()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = "*"
            };

            var exception = Assert.Throws<StorageException>(
                () => CloudTable.ExecuteBatch(new TableBatchOperation { TableOperation.Merge(testEntity) })
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
            var exception = Assert.Throws<ArgumentNullException>("entity", () => TableOperation.Merge(null));
            Assert.Equal(new ArgumentNullException("entity").Message, exception.Message);
        }

        [Fact]
        public void TableOperation_WhenETagIsMissing_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentException>(() => TableOperation.Merge(new TableEntity()));
            Assert.Equal(new ArgumentException("Merge requires an ETag (which may be the '*' wildcard).").Message, exception.Message);
        }

        [Fact]
        public void Execute_WhenETagsIsWildcard_MergesEntity()
        {
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
            CloudTable.Create();
            CloudTable.Execute(TableOperation.Insert(testEntity));

            var tableResults = CloudTable.ExecuteBatch(new TableBatchOperation { TableOperation.Merge(updatedTestEntity) });
            var tableResult = Assert.Single(tableResults);

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Equal(resultEntity.ETag, tableResult.Etag);

            var entities = GetAllEntities();
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
            Assert.Equal(default, resultEntity.Timestamp);
        }

        [Fact]
        public void Execute_WhenETagsMatch_MergesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string-prop",
                Int32Prop = 4
            };
            CloudTable.Create();
            var tableResult = CloudTable.Execute(TableOperation.Insert(testEntity));
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8,
                Int64Prop = 8,
                ETag = tableResult.Etag
            };

            var tableResults = CloudTable.ExecuteBatch(new TableBatchOperation { TableOperation.Merge(updatedTestEntity) });
            tableResult = Assert.Single(tableResults);

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Equal(resultEntity.ETag, tableResult.Etag);

            var entities = GetAllEntities();
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
            Assert.Equal(default, resultEntity.Timestamp);
        }

        [Fact]
        public void Execute_WhenEntityDoesNotExist_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = new string('t', 1 << 10 + 1),
                RowKey = new string('t', 1 << 10 + 1),
                ETag = "*"
            };
            CloudTable.Create();

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.Merge(testEntity)));

            Assert.Equal("Not Found", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(404, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("ResourceNotFound", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The specified resource does not exist.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public void Execute_WhenETagsMismatch_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            CloudTable.Create();
            var result = CloudTable.Execute(TableOperation.Insert(testEntity));
            var updatedTestEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = result.Etag
            };
            CloudTable.Execute(TableOperation.InsertOrReplace(testEntity));

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.Merge(updatedTestEntity)));

            Assert.Equal("Precondition Failed", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(412, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("UpdateConditionNotSatisfied", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The update condition specified in the request was not satisfied.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public void Execute_WhenPartitionKeyIsNull_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = null,
                RowKey = "row-key",
                ETag = "*"
            };
            CloudTable.Create();

            var exception = Assert.Throws<ArgumentNullException>(
                "item",
                () => CloudTable.ExecuteBatch(new TableBatchOperation { TableOperation.Merge(testEntity) })
            );

            Assert.Equal(
                new ArgumentNullException("item", "A batch non-retrieve operation requires a non-null partition key and row key.").Message,
                exception.Message
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public void Execute_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            var testEntity = new TableEntity
            {
                PartitionKey = partitionKey,
                RowKey = "row-key",
                ETag = "*"
            };
            CloudTable.Create();

            var exception = Assert.Throws<StorageException>(
                () => CloudTable.ExecuteBatch(new TableBatchOperation { TableOperation.Merge(testEntity) })
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
        public void Execute_WhenRowKeyIsNull_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = null,
                ETag = "*"
            };
            CloudTable.Create();

            var exception = Assert.Throws<ArgumentNullException>(
                "item",
                () => CloudTable.ExecuteBatch(new TableBatchOperation { TableOperation.Merge(testEntity) })
            );

            Assert.Equal(
                new ArgumentNullException("item", "A batch non-retrieve operation requires a non-null partition key and row key.").Message,
                exception.Message
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public void Execute_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = rowKey,
                ETag = "*"
            };
            CloudTable.Create();

            var exception = Assert.Throws<StorageException>(
                () => CloudTable.ExecuteBatch(new TableBatchOperation { TableOperation.Merge(testEntity) })
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

        [Theory, MemberData(nameof(TableOperationTestData.InvalidStringData), MemberType = typeof(TableOperationTestData))]
        public void Execute_WhenStringPropertyIsInvalid_ThrowsException(string stringPropValue)
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
            CloudTable.Create();
            CloudTable.Execute(TableOperation.Insert(testEntity));

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.Merge(updatedTestEntity)));

            Assert.Equal("The remote server returned an error: (400) Bad Request.", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("PropertyValueTooLarge", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The property value exceeds the maximum allowed size \(64KB\). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidBinaryData), MemberType = typeof(TableOperationTestData))]
        public void Execute_WhenBinaryPropertyIsInvalid_ThrowsException(byte[] binaryPropValue)
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
            CloudTable.Create();
            CloudTable.Execute(TableOperation.Insert(testEntity));

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.Merge(updatedTestEntity)));

            Assert.Equal("The remote server returned an error: (400) Bad Request.", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("PropertyValueTooLarge", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The property value exceeds the maximum allowed size \(64KB\). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidDateTimeData), MemberType = typeof(TableOperationTestData))]
        public void Execute_WhenDateTimePropertyIsInvalid_ThrowsException(DateTime dateTimePropValue)
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
            CloudTable.Create();
            CloudTable.Execute(TableOperation.Insert(testEntity));

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.Merge(updatedTestEntity)));

            Assert.Equal("The remote server returned an error: (400) Bad Request.", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("OutOfRangeInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The 'DateTimeProp' parameter of value '{dateTimePropValue:MM/dd/yyyy HH:mm:ss}' is out of range.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }
    }
}