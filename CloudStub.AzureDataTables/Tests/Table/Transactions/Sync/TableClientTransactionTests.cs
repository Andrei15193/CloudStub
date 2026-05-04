using System;
using System.Linq;
using System.Net;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Transactions.Sync
{
    public class TableClientTransactionTests : BaseTableCloudStubTests
    {
        [Fact]
        public void SubmitTransaction_WhenTableDoesNotExist_ThrowsException()
        {
            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("partition-key", "row-key"))
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NotFound,
                    ErrorCode = "TableNotFound",
                    ErrorDescription = "0:The table specified does not exist.",
                    FailedEntityIndex = 0
                }
            );
        }

        [Fact]
        public void SubmitTransaction_WhenBatchIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("transactionalBatch", () => CloudTable.SubmitTransaction(null));

            Assert.Multiple(
                () => Assert.Equal(new ArgumentNullException("transactionalBatch").Message, exception.Message),
                () => Assert.Contains(exception.Source, new[] { "Azure.Data.Tables", "System.Private.CoreLib" })
            );
        }

        [Fact]
        public void SubmitTransaction_WhenBatchIsEmpty_ThrowsException()
        {
            var exception = Assert.Throws<InvalidOperationException>(() => CloudTable.SubmitTransaction(Enumerable.Empty<TableTransactionAction>()));

            Assert.Multiple(
                () => Assert.Equal(new InvalidOperationException("The batch contains no entity operations.").Message, exception.Message),
                () => Assert.Null(exception.InnerException),
                () => Assert.Contains(exception.Source, new[] { "Azure.Data.Tables", "System.Private.CoreLib" })
            );
        }

        [Fact]
        public void SubmitTransaction_WhenBatchHasOperationsInMultiplePartitions_ThrowsException()
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("partition-key-1", "row-key")),
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("partition-key-2", "row-key"))
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "CommandsInBatchActOnDifferentPartitions",
                    ErrorDescription = "1:All commands in a batch must operate on same entity group.",
                    FailedEntityIndex = 1
                }
            );
        }

        [Fact]
        public void SubmitTransaction_WithMultipleOperationsOnSameEntity_ThrowsException()
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.UpsertMerge, new TableEntity("partition-key", "row-key")),
                        new TableTransactionAction(TableTransactionActionType.UpsertMerge, new TableEntity("partition-key", "row-key"))
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "InvalidDuplicateRow",
                    ErrorDescription = "1:The batch request contains multiple changes with same row key. An entity can appear only once in a batch request.",
                    FailedEntityIndex = 1
                }
            );
        }

        [Fact]
        public void SubmitTransaction_FailingBatchOperation_DoesNotExecutePartially()
        {
            CloudTable.Create();
            CloudTable.AddEntity(new TestEntity { PartitionKey = "partition-key", RowKey = "row-key", StringProp = "string prop" });

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
                    new[]
                    {
                        new TableTransactionAction(
                            TableTransactionActionType.UpdateReplace,
                            new TestEntity
                            {
                                PartitionKey = "partition-key",
                                RowKey = "row-key",
                                Int32Prop = 4
                            }
                        ),
                        new TableTransactionAction(
                            TableTransactionActionType.UpdateReplace,
                            new TestEntity
                            {
                                PartitionKey = "partition-key",
                                RowKey = "row-key",
                                Int64Prop = 8
                            }
                        )
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "InvalidDuplicateRow",
                    ErrorDescription = "1:The batch request contains multiple changes with same row key. An entity can appear only once in a batch request.",
                    FailedEntityIndex = 1
                }
            );

            var entity = CloudTable.GetEntity<TestEntity>("partition-key", "row-key").Value;

            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
                () => Assert.Equal("string prop", entity.StringProp),
                () => Assert.Null(entity.Int32Prop),
                () => Assert.Null(entity.Int64Prop)
            );
        }

        [Fact]
        public void SubmitTransaction_WhenBatchHasMoreThan100Operations_ThrowsException()
        {
            int? failedTransactionActionIndex = null;
            Assert.Multiple(
                () => Assertions.TransactionJsonResponseThrows(
                    () =>
                    {
                        try
                        {
                            CloudTable.SubmitTransaction(
                                from rowNumber in Enumerable.Range(1, 101)
                                select new TableTransactionAction(
                                    TableTransactionActionType.Add,
                                    new TableEntity("partition-key", $"row-key-${rowNumber}")
                                )
                            );
                        }
                        catch (TableTransactionFailedException exception)
                        {
                            failedTransactionActionIndex = exception.FailedTransactionActionIndex;
                            Assert.Contains(failedTransactionActionIndex, new int?[] { 0, 99 });

                            throw;
                        }
                    },
                    rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.BadRequest,
                        ErrorCode = "InvalidInput",
                        ErrorDescription = $"{failedTransactionActionIndex}:The batch request operation exceeds the maximum 100 changes per change set.",
                        FailedEntityIndex = failedTransactionActionIndex
                    }
                )
            );
        }

        [Fact]
        public void SubmitTransaction_WhenOperationIsNotSupported_ThrowsException()
        {
            CloudTable.Create();

            var exception = Assert.Throws<InvalidOperationException>(
                () => CloudTable.SubmitTransaction(
                    new[]
                    {
                        new TableTransactionAction((TableTransactionActionType)(-1), new TableEntity("partition-key", "row-key"))
                    }
                )
            );
            Assert.Multiple(
                () => Assert.Equal(new InvalidOperationException("Unknown request type.").Message, exception.Message),
                () => Assert.Contains(exception.Source, new[] { "Azure.Data.Tables", "System.Private.CoreLib" })
            );
        }

        [Fact]
        public void SubmitTransaction_WhenBatchHas100Operations_ExecutesSuccessfully()
        {
            CloudTable.Create();

            var result = CloudTable.SubmitTransaction(
                from rowNumber in Enumerable.Range(1, 100)
                select new TableTransactionAction(
                    TableTransactionActionType.Add,
                    new TableEntity("partition-key", $"row-key-{rowNumber}")
                )
            );

            var rawResponse = result.GetRawResponse();

            Assertions.TableTransactionResponse(
                rawResponse,
                new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.Accepted,
                    Headers = new Assertions.DefaultHeaders(rawResponse)
                    {
                        ["Content-Type"] = rawResponse.Headers.ContentType
                    },
                    WithoutDate = true
                },
                result.Value
            );

            var operationNumber = 1;
            foreach (var operationResponse in result.Value)
            {
                Assertions.EmptyResponse(
                    operationResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NoContent,
                        Headers = new Assertions.TableTransactionHeaders(operationResponse, CloudTable.Name, "partition-key", $"row-key-{operationNumber}"),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                );
                operationNumber++;
            }
        }
    }
}