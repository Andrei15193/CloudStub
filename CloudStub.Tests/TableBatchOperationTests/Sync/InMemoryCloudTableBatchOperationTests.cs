using System;
using System.Collections;
using Microsoft.Azure.Cosmos.Table;
using Xunit;

namespace CloudStub.Tests.TableBatchOperationTests.Sync
{
    public class InMemoryCloudTableBatchOperationTests : BaseInMemoryCloudTableTests
    {
        [Fact]
        public void ExecuteBatch_WhenBatchIsNull_ThrowsException()
        {
            CloudTable.Create();

            var exception = Assert.Throws<ArgumentNullException>("batch", () => CloudTable.ExecuteBatch(null));

            Assert.Equal(new ArgumentNullException("batch").Message, exception.Message);
        }

        [Fact]
        public void ExecuteBatch_WhenBatchIsEmpty_ThrowsException()
        {
            CloudTable.Create();

            var exception = Assert.Throws<InvalidOperationException>(() => CloudTable.ExecuteBatch(new TableBatchOperation()));

            Assert.Equal("Cannot execute an empty batch operation", exception.Message);
            Assert.Null(exception.InnerException);
        }

        [Fact]
        public void ExecuteBatch_WhenBatchHasOperationsInMultiplePartitions_ThrowsException()
        {
            CloudTable.Create();

            var exception = Assert.Throws<ArgumentException>(
                () => CloudTable.ExecuteBatch(
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
        public void ExecuteBatch_WhenBatchHasEditAndRetrieveOperations_ThrowsException()
        {
            CloudTable.Create();

            var exception = Assert.Throws<ArgumentException>(
                () => CloudTable.ExecuteBatch(
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
        public void ExecuteBatch_WhenBatchHasMultipleRetrieveOperations_ThrowsException()
        {
            CloudTable.Create();

            var exception = Assert.Throws<ArgumentException>(
                () => CloudTable.ExecuteBatch(
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
        public void ExecuteBatch_WithMultipleOperationsOnSameEntity_ThrowsException()
        {
            CloudTable.Create();

            var exception = Assert.Throws<StorageException>(
                () => CloudTable.ExecuteBatch(
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
        public void ExecuteBatch_FailingBatchOperation_DoesNotExecutePartially()
        {
            CloudTable.Create();
            CloudTable.Execute(TableOperation.Insert(new TestEntity { PartitionKey = "partition-key", RowKey = "row-key", StringProp = "string prop" }));

            Assert.Throws<StorageException>(
                () => CloudTable.ExecuteBatch(
                    new TableBatchOperation
                    {
                        TableOperation.Replace(new TestEntity{ PartitionKey = "partition-key", RowKey = "row-key", ETag = "*", Int32Prop = 4 }),
                        TableOperation.Replace(new TestEntity{ PartitionKey = "partition-key", RowKey = "row-key", ETag = "*", Int64Prop = 4 })
                    }
                )
            );
            var result = CloudTable.Execute(TableOperation.Retrieve<TestEntity>("partition-key", "row-key"));
            var existingEntity = (TestEntity)result.Result;

            Assert.Equal("partition-key", existingEntity.PartitionKey);
            Assert.Equal("row-key", existingEntity.RowKey);
            Assert.Equal("string prop", existingEntity.StringProp);
            Assert.Null(existingEntity.Int32Prop);
            Assert.Null(existingEntity.Int64Prop);
        }

        [Fact]
        public void ExecuteBatch_WhenBatchHasMoreThan100Operations_ThrowsException()
        {
            CloudTable.Create();
            var batchOperation = new TableBatchOperation();
            for (var operationIndex = 0; operationIndex < 101; operationIndex++)
                batchOperation.Add(TableOperation.Insert(new TableEntity("partition-key", $"row-key-{operationIndex}")));

            var exception = Assert.Throws<InvalidOperationException>(() => CloudTable.ExecuteBatch(batchOperation));

            Assert.Equal(new InvalidOperationException("The maximum number of operations allowed in one batch has been exceeded.").Message, exception.Message);
        }

        [Fact]
        public void ExecuteBatch_WhenBatchHas100Operations_ExecutesSuccessfully()
        {
            CloudTable.Create();
            var batchOperation = new TableBatchOperation();
            for (var operationIndex = 0; operationIndex < 100; operationIndex++)
                batchOperation.Add(TableOperation.Insert(new TableEntity("partition-key", $"row-key-{operationIndex}")));

            var tableResults = CloudTable.ExecuteBatch(batchOperation);

            for (var operationIndex = 0; operationIndex < 100; operationIndex++)
            {
                var tableEntity = (ITableEntity)tableResults[operationIndex].Result;
                Assert.Equal("partition-key", tableEntity.PartitionKey);
                Assert.Equal($"row-key-{operationIndex}", tableEntity.RowKey);
            }
        }
    }
}