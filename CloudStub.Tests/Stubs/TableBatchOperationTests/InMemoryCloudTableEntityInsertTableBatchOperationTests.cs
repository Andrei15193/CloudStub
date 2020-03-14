using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using CloudStub.Tests.BaseOperationTests;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace CloudStub.Tests.TableBatchOperationTests
{
    public sealed class InMemoryCloudTableEntityInsertTableBatchOperationTests : InMemoryCloudTableEntityInsertOperationTests
    {
        protected override string ExpectedErrorMessagePattern
            => @"Element \d+ in the batch returned an unexpected response code.";

        protected override async Task<IEnumerable<TableResult>> ExecuteAsync(TableOperation tableOperation)
            => await CloudTable.ExecuteBatchAsync(new TableBatchOperation { tableOperation });

        [Fact]
        public override async Task ExecuteAsync_InsertOperationWhenPartitionKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(() => ExecuteAsync(GetOperation(new TableEntity(null, "row-key"))));

            Assert.Equal(
                new ArgumentNullException(null, "A batch non-retrieve operation requires a non-null partition key and row key. (Parameter 'item')").Message,
                exception.Message
            );
        }

        [Fact]
        public override async Task ExecuteAsync_WhenRowKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(() => ExecuteAsync(GetOperation(new TableEntity("partition-key", null))));

            Assert.Equal(
                new ArgumentNullException(null, "A batch non-retrieve operation requires a non-null partition key and row key. (Parameter 'item')").Message,
                exception.Message
            );
        }

        [Fact]
        public override async Task ExecuteAsync_InsertOperationWhenEntityAlreadyExists_ThrowsException()
        {
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(new TableEntity("partition-key", "row-key")));

            var exception = await Assert.ThrowsAsync<StorageException>(() => ExecuteAsync(GetOperation(new TableEntity("partition-key", "row-key"))));

            Assert.Matches(
                "^The specified entity already exists.\n"
                + $"RequestId:{exception.RequestInformation.ServiceRequestID}\n"
                + @"Time:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7}Z$",
                exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(409, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Equal(ExpectedErrorCode, exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Matches(
                "^The specified entity already exists.\n"
                + $"RequestId:{exception.RequestInformation.ServiceRequestID}\n"
                + @"Time:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7}Z$",
                exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }
    }
}