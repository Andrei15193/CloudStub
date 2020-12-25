using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.Azure.Cosmos.Table;
using Xunit;

namespace CloudStub.Tests.TableOperationTests.Sync
{
    public class InMemoryCloudTableEntityInsertOrMergeTableOperationTests : BaseInMemoryCloudTableTests
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

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.InsertOrMerge(testEntity)));

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

            Assert.Equal("TableNotFound", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The table specified does not exist.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public void TableOperation_WhenEntityIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("entity", () => TableOperation.InsertOrMerge(null));
            Assert.Equal(new ArgumentNullException("entity").Message, exception.Message);
        }

        [Fact]
        public void Execute_InsertOperation_InsertsEntity()
        {
            var startTime = DateTimeOffset.UtcNow;
            var tableEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            CloudTable.Create();

            var tableResult = CloudTable.Execute(TableOperation.Insert(tableEntity));

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Equal(resultEntity.ETag, tableResult.Etag);

            var entities = GetAllEntities();
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
        public void Execute_InserOperationWhenEntityHasOtherProperties_InsertsEntity()
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
            CloudTable.Create();

            CloudTable.Execute(TableOperation.Insert(testEntity));

            var entities = GetAllEntities();
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
        public void TableOperation_InsertOrMergeOperation_MergesEntities()
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
            CloudTable.Create();
            CloudTable.Execute(TableOperation.Insert(testEntity));

            var tableResult = CloudTable.Execute(TableOperation.InsertOrMerge(updatedTestEntity));

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Equal(resultEntity.ETag, tableResult.Etag);

            var entities = GetAllEntities();
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
        public void Execute_WhenDynamicEntityHasNullProperties_TheyAreIgnored()
        {
            CloudTable.Create();

            CloudTable.Execute(TableOperation.InsertOrMerge(new DynamicTableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, EntityProperty>(StringComparer.Ordinal)
                {
                    { nameof(TestEntity.Int32Prop), EntityProperty.CreateEntityPropertyFromObject(null) }
                }
            }));

            var entities = GetAllEntities();
            var entity = Assert.Single(entities);
            var actualProps = entity.WriteEntity(null);
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.PartitionKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.RowKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.Timestamp)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.ETag)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.Int32Prop)));
        }

        [Fact]
        public void Execute_WhenDynamicEntityHasNullProperties_TheyAreIgnoredWhenEntityAlreadyExists()
        {
            CloudTable.Create();
            CloudTable.Execute(TableOperation.Insert(new DynamicTableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, EntityProperty>(StringComparer.Ordinal)
                {
                    { nameof(TestEntity.Int32Prop), EntityProperty.GeneratePropertyForInt(1) }
                }
            }));

            CloudTable.Execute(TableOperation.InsertOrMerge(new DynamicTableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, EntityProperty>(StringComparer.Ordinal)
                {
                    { nameof(TestEntity.Int32Prop), EntityProperty.CreateEntityPropertyFromObject(null) }
                }
            }));

            var entities = GetAllEntities();
            var entity = Assert.Single(entities);
            var actualProps = entity.WriteEntity(null);
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.PartitionKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.RowKey)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.Timestamp)));
            Assert.False(actualProps.ContainsKey(nameof(TestEntity.ETag)));
            Assert.Equal(EntityProperty.GeneratePropertyForInt(1), actualProps[nameof(TestEntity.Int32Prop)]);
        }

        [Fact]
        public void Execute_WhenPartitionKeyIsNull_ThrowsException()
        {
            CloudTable.Create();

            var exception = Assert.Throws<ArgumentNullException>(
                "Upserts require a valid PartitionKey",
                () => CloudTable.Execute(TableOperation.InsertOrMerge(new TableEntity(null, "row-key")))
            );

            Assert.Equal(new ArgumentNullException("Upserts require a valid PartitionKey").Message, exception.Message);
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

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.InsertOrMerge(testEntity)));

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

            switch (partitionKey)
            {
                case "/":
                case "\\":
                    Assert.Equal("InvalidInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
                    Assert.Matches(
                        @$"^Bad Request - Error in query syntax.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                        exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
                    );
                    break;

                case "\u0000":
                case "\u0001":
                case "\u0002":
                case "\u0003":
                case "\u0004":
                case "\u0005":
                case "\u0006":
                case "\u0007":
                case "\u0008":
                case "\u0009":
                case "\u000A":
                case "\u000C":
                case "\u000B":
                case "\u000D":
                case "\u000E":
                case "\u000F":
                case "\u0010":
                case "\u0011":
                case "\u0012":
                case "\u0013":
                case "\u0014":
                case "\u0015":
                case "\u0016":
                case "\u0017":
                case "\u0018":
                case "\u0019":
                case "\u001A":
                case "\u001B":
                case "\u001C":
                case "\u001D":
                case "\u001E":
                case "\u001F":
                case "\u007F":
                case "\u0081":
                case "\u008D":
                case "\u008F":
                case "\u0090":
                case "\u009D":
                    Assert.Null(exception.RequestInformation.ExtendedErrorInformation);
                    break;

                default:
                    Assert.Equal("OutOfRangeInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
                    Assert.Matches(
                        @$"^One of the request inputs is out of range.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                        exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
                    );
                    break;
            }

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public void Execute_WhenPartitionKeyExceedsLimit_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = new string('t', 1 << 10 + 1),
                RowKey = "row-key",
                ETag = "*"
            };
            CloudTable.Create();

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.InsertOrMerge(testEntity)));

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

        [Fact]
        public void Execute_WhenRowKeyIsNull_ThrowsException()
        {
            CloudTable.Create();

            var exception = Assert.Throws<ArgumentNullException>(
                "Upserts require a valid RowKey",
                () => CloudTable.Execute(TableOperation.InsertOrMerge(new TableEntity("partition-key", null)))
            );

            Assert.Equal(new ArgumentNullException("Upserts require a valid RowKey").Message, exception.Message);
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

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.InsertOrMerge(testEntity)));

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

            switch (rowKey)
            {
                case "/":
                case "\\":
                    Assert.Equal("InvalidInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
                    Assert.Matches(
                        @$"^Bad Request - Error in query syntax.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                        exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
                    );
                    break;

                case "\u0000":
                case "\u0001":
                case "\u0002":
                case "\u0003":
                case "\u0004":
                case "\u0005":
                case "\u0006":
                case "\u0007":
                case "\u0008":
                case "\u0009":
                case "\u000A":
                case "\u000C":
                case "\u000B":
                case "\u000D":
                case "\u000E":
                case "\u000F":
                case "\u0010":
                case "\u0011":
                case "\u0012":
                case "\u0013":
                case "\u0014":
                case "\u0015":
                case "\u0016":
                case "\u0017":
                case "\u0018":
                case "\u0019":
                case "\u001A":
                case "\u001B":
                case "\u001C":
                case "\u001D":
                case "\u001E":
                case "\u001F":
                case "\u007F":
                case "\u0081":
                case "\u008D":
                case "\u008F":
                case "\u0090":
                case "\u009D":
                    Assert.Null(exception.RequestInformation.ExtendedErrorInformation);
                    break;

                default:
                    Assert.Equal("OutOfRangeInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
                    Assert.Matches(
                        @$"^One of the request inputs is out of range.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                        exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
                    );
                    break;
            }

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public void Execute_WhenRowKeyExceedsLimit_ThrowsException()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = new string('t', 1 << 10 + 1),
                ETag = "*"
            };
            CloudTable.Create();

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.InsertOrMerge(testEntity)));

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

        [Theory, MemberData(nameof(TableOperationTestData.InvalidStringData), MemberType = typeof(TableOperationTestData))]
        public void Execute_WhenStringPropertyIsInvalid_ThrowsException(string stringPropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = stringPropValue,
                ETag = "*"
            };
            CloudTable.Create();

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.InsertOrMerge(testEntity)));

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
                BinaryProp = binaryPropValue,
                ETag = "*"
            };
            CloudTable.Create();

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.InsertOrMerge(testEntity)));

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
                DateTimeProp = dateTimePropValue,
                ETag = "*"
            };
            CloudTable.Create();

            var exception = Assert.Throws<StorageException>(() => CloudTable.Execute(TableOperation.InsertOrMerge(testEntity)));

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