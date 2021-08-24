using System;
using System.Collections;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Xunit;

namespace CloudStub.Tests.TableBatchOperationTests.Async
{
    public class StubCloudTableBatchOperationTests : BaseStubCloudTableTests
    {
        [Fact]
        public async Task ExecuteBatchAsync_WhenBatchIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>("batch", () => CloudTable.ExecuteBatchAsync(null));

            Assert.Equal(new ArgumentNullException("batch").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteBatchAsync_WhenBatchIsEmpty_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CloudTable.ExecuteBatchAsync(new TableBatchOperation()));

            Assert.Equal("Cannot execute an empty batch operation", exception.Message);
            Assert.Null(exception.InnerException);
        }

        [Fact]
        public async Task ExecuteBatchAsync_WhenBatchHasOperationsInMultiplePartitions_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentException>(
                () => CloudTable.ExecuteBatchAsync(
                    new TableBatchOperation
                    {
                        TableOperation.Insert(new TableEntity("partition-key-1", "row-key")),
                        TableOperation.Insert(new TableEntity("partition-key-2", "row-key"))
                    }
                )
            );

            Assert.Equal(new ArgumentException("All entities in a given batch must have the same partition key.").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteBatchAsync_WhenBatchHasEditAndRetrieveOperations_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentException>(
                () => CloudTable.ExecuteBatchAsync(
                    new TableBatchOperation
                    {
                        TableOperation.Insert(new TableEntity("partition-key", "row-key")),
                        TableOperation.Retrieve("partition-key", "row-key")
                    }
                )
            );

            Assert.Equal(new ArgumentException("A batch transaction with a retrieve operation cannot contain any other operations.").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteBatchAsync_WhenBatchHasMultipleRetrieveOperations_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentException>(
                () => CloudTable.ExecuteBatchAsync(
                    new TableBatchOperation
                    {
                        TableOperation.Retrieve("partition-key", "row-key"),
                        TableOperation.Retrieve("partition-key", "row-key")
                    }
                )
            );

            Assert.Equal(new ArgumentException("A batch transaction with a retrieve operation cannot contain any other operations.").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteBatchAsync_WithMultipleOperationsOnSameEntity_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteBatchAsync(
                    new TableBatchOperation
                    {
                        TableOperation.InsertOrReplace(new TableEntity("partition-key", "row-key")),
                        TableOperation.InsertOrReplace(new TableEntity("partition-key", "row-key"))
                    }
                )
            );

            Assert.Equal("Element 1 in the batch returned an unexpected response code.", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Null(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("InvalidDuplicateRow", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^1:The batch request contains multiple changes with same row key. An entity can appear only once in a batch request.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public async Task ExecuteBatchAsync_FailingBatchOperation_DoesNotExecutePartially()
        {
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestEntity { PartitionKey = "partition-key", RowKey = "row-key", StringProp = "string prop" }));

            await Assert.ThrowsAsync<StorageException>(
                () => CloudTable.ExecuteBatchAsync(
                    new TableBatchOperation
                    {
                        TableOperation.Replace(new TestEntity{ PartitionKey = "partition-key", RowKey = "row-key", ETag = "*", Int32Prop = 4 }),
                        TableOperation.Replace(new TestEntity{ PartitionKey = "partition-key", RowKey = "row-key", ETag = "*", Int64Prop = 4 })
                    }
                )
            );
            var result = await CloudTable.ExecuteAsync(TableOperation.Retrieve<TestEntity>("partition-key", "row-key"));
            var existingEntity = (TestEntity)result.Result;

            Assert.Equal("partition-key", existingEntity.PartitionKey);
            Assert.Equal("row-key", existingEntity.RowKey);
            Assert.Equal("string prop", existingEntity.StringProp);
            Assert.Null(existingEntity.Int32Prop);
            Assert.Null(existingEntity.Int64Prop);
        }

        [Fact]
        public async Task ExecuteBatchAsync_WhenBatchHasMoreThan100Operations_ThrowsException()
        {
            await CloudTable.CreateAsync();
            var batchOperation = new TableBatchOperation();
            for (var operationIndex = 0; operationIndex < 101; operationIndex++)
                batchOperation.Add(TableOperation.Insert(new TableEntity("partition-key", $"row-key-{operationIndex}")));

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CloudTable.ExecuteBatchAsync(batchOperation));

            Assert.Equal(new InvalidOperationException("The maximum number of operations allowed in one batch has been exceeded.").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteBatchAsync_WhenBatchHas100Operations_ExecutesSuccessfully()
        {
            await CloudTable.CreateAsync();
            var batchOperation = new TableBatchOperation();
            for (var operationIndex = 0; operationIndex < 100; operationIndex++)
                batchOperation.Add(TableOperation.Insert(new TableEntity("partition-key", $"row-key-{operationIndex}")));

            var tableResults = await CloudTable.ExecuteBatchAsync(batchOperation);

            for (var operationIndex = 0; operationIndex < 100; operationIndex++)
            {
                var tableEntity = (ITableEntity)tableResults[operationIndex].Result;
                Assert.Equal("partition-key", tableEntity.PartitionKey);
                Assert.Equal($"row-key-{operationIndex}", tableEntity.RowKey);
            }
        }
    }
}