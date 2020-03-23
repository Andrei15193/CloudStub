using System;
using System.Collections;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace CloudStub.Tests.BaseOperationTests
{
    public abstract class InMemoryCloudTableEntityDeleteOperationTests : InMemoryCloudTableEntityEditOperationsTests
    {
        [Fact]
        public async Task ExecuteAsync_WhenETagsIsWildcard_DeletesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string-prop",
                Int32Prop = 4
            };
            var testEntityToRemove = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8,
                Int64Prop = 8,
                ETag = "*"
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            var tableResults = await ExecuteAsync(GetOperation(testEntityToRemove));
            var tableResult = Assert.Single(tableResults);

            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Null(tableResult.Etag);

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal("partition-key", resultEntity.PartitionKey);
            Assert.Equal("row-key", resultEntity.RowKey);
            Assert.NotEmpty(resultEntity.ETag);
            Assert.Equal(default(DateTimeOffset), resultEntity.Timestamp);
        }

        [Fact]
        public async Task ExecuteAsync_WhenETagsMatch_DeletesEntity()
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
            var testEntityToRemove = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8,
                Int64Prop = 8,
                ETag = tableResult.Etag
            };

            var tableResults = await ExecuteAsync(GetOperation(testEntityToRemove));
            tableResult = Assert.Single(tableResults);

            Assert.Equal(204, tableResult.HttpStatusCode);
            Assert.Null(tableResult.Etag);

            var resultEntity = Assert.IsAssignableFrom<ITableEntity>(tableResult.Result);
            Assert.Equal("partition-key", resultEntity.PartitionKey);
            Assert.Equal("row-key", resultEntity.RowKey);
            Assert.NotEmpty(resultEntity.ETag);
            Assert.Equal(default(DateTimeOffset), resultEntity.Timestamp);
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

            var exception = await Assert.ThrowsAsync<StorageException>(() => ExecuteAsync(GetOperation(testEntity)));

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
            Assert.Equal("ResourceNotFound", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The specified resource does not exist.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );
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
            var testEntityToRemove = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = result.Etag
            };
            await CloudTable.ExecuteAsync(TableOperation.InsertOrReplace(testEntity));

            var exception = await Assert.ThrowsAsync<StorageException>(() => ExecuteAsync(GetOperation(testEntityToRemove)));

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
            Assert.Equal("UpdateConditionNotSatisfied", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The update condition specified in the request was not satisfied.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

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
                "Delete requires a valid PartitionKey",
                () => ExecuteAsync(GetOperation(testEntity))
            );

            Assert.Equal(new ArgumentNullException("Delete requires a valid PartitionKey").Message, exception.Message);
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
                "Delete requires a valid RowKey",
                () => ExecuteAsync(GetOperation(testEntity))
            );

            Assert.Equal(new ArgumentNullException("Delete requires a valid RowKey").Message, exception.Message);
        }

        [Theory]
        [MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public virtual async Task ExecuteAsync_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            var testEntity = new TableEntity
            {
                PartitionKey = partitionKey,
                RowKey = "row-key",
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
            switch (partitionKey)
            {
                case "\u002F":
                case "\u005C":
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
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory]
        [MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public virtual async Task ExecuteAsync_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = rowKey,
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
            switch (rowKey)
            {
                case "\u002F":
                case "\u005C":
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
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory]
        [MemberData(nameof(TableOperationTestData.InvalidStringData), MemberType = typeof(TableOperationTestData))]
        public async Task ExecuteAsync_WhenStringPropertyIsInvalid_DeletesEntity(string stringPropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = "*"
            };
            var testEntityToRemove = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                StringProp = stringPropValue,
                ETag = testEntity.ETag
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            await ExecuteAsync(GetOperation(testEntityToRemove));

            var entities = await GetAllEntitiesAsync();
            Assert.Empty(entities);
        }

        [Theory]
        [MemberData(nameof(TableOperationTestData.InvalidBinaryData), MemberType = typeof(TableOperationTestData))]
        public async Task ExecuteAsync_WhenBinaryPropertyIsInvalid_DeletesEntity(byte[] binaryPropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = "*"
            };
            var testEntityToRemove = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                BinaryProp = binaryPropValue,
                ETag = testEntity.ETag
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            await ExecuteAsync(GetOperation(testEntityToRemove));

            var entities = await GetAllEntitiesAsync();
            Assert.Empty(entities);
        }

        [Theory]
        [MemberData(nameof(TableOperationTestData.InvalidDateTimeData), MemberType = typeof(TableOperationTestData))]
        public async Task ExecuteAsync_WhenDateTimePropertyIsInvalid_DeletesEntity(DateTime dateTimePropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = "*"
            };
            var testEntityToRemove = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                DateTimeProp = dateTimePropValue,
                ETag = testEntity.ETag
            };
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(testEntity));

            await ExecuteAsync(GetOperation(testEntityToRemove));

            var entities = await GetAllEntitiesAsync();
            Assert.Empty(entities);
        }

        [Fact]
        public void TableOperation_WhenETagIsMissing_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentException>(() => GetOperation(new TableEntity()));
            Assert.Equal(new ArgumentException("Delete requires an ETag (which may be the '*' wildcard).").Message, exception.Message);
        }

        protected sealed override TableOperation GetOperation(ITableEntity entity)
            => TableOperation.Delete(entity);
    }
}