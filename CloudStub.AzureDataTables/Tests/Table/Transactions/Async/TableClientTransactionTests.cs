using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Transactions.Async
{
    public class TableClientTransactionTests : BaseTableCloudStubTests
    {
        [Fact]
        public async Task SubmitTransactionAsync_WhenTableDoesNotExist_ThrowsException()
        {
            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
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
        public async Task SubmitTransactionAsync_WhenBatchIsNull_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<ArgumentNullException>("transactionalBatch", () => CloudTable.SubmitTransactionAsync(null));

            Assert.Multiple(
                () => Assert.Equal(new ArgumentNullException("transactionalBatch").Message, exception.Message),
                () => Assert.Contains(exception.Source, new[] { "Azure.Data.Tables", "System.Private.CoreLib" })
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenBatchIsEmpty_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CloudTable.SubmitTransactionAsync(Enumerable.Empty<TableTransactionAction>()));

            Assert.Multiple(
                () => Assert.Equal(new InvalidOperationException("The batch contains no entity operations.").Message, exception.Message),
                () => Assert.Null(exception.InnerException),
                () => Assert.Contains(exception.Source, new[] { "Azure.Data.Tables", "System.Private.CoreLib" })
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenBatchHasOperationsInMultiplePartitions_ThrowsException()
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
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
        public async Task SubmitTransactionAsync_WithMultipleOperationsOnSameEntity_ThrowsException()
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
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
        public async Task SubmitTransactionAsync_FailingBatchOperation_DoesNotExecutePartially()
        {
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(new TestEntity { PartitionKey = "partition-key", RowKey = "row-key", StringProp = "string prop" });

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
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

            var result = await CloudTable.GetEntityAsync<TestEntity>("partition-key", "row-key");
            var entity = result.Value;

            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
                () => Assert.Equal("string prop", entity.StringProp),
                () => Assert.Null(entity.Int32Prop),
                () => Assert.Null(entity.Int64Prop)
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenBatchHasMoreThan100Operations_ThrowsException()
        {
            int? failedTransactionActionIndex = null;

            await Assertions.TransactionJsonResponseThrowsAsync(
                async () =>
                {
                    try
                    {
                        await CloudTable.SubmitTransactionAsync(
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
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenOperationIsNotSupported_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => CloudTable.SubmitTransactionAsync(
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
        public async Task SubmitTransactionAsync_WhenBatchHas100Operations_ExecutesSuccessfully()
        {
            await CloudTable.CreateAsync();

            var result = await CloudTable.SubmitTransactionAsync(
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
                        Headers = new Assertions.TransactionAddActionHeaders(operationResponse, CloudTable.Name, "partition-key", $"row-key-{operationNumber}"),
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