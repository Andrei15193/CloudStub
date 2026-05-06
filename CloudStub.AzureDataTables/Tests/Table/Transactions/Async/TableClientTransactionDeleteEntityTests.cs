using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Transactions.Async
{
    public class TableClientTransactionDeleteEntityTests : BaseTableCloudStubTests
    {
        [Fact]
        public async Task SubmitTransactionAsync_WhenTableDoesNotExist_ThrowsException()
        {
            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Delete, new TableEntity("partition-key", "row-key"))
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
        public async Task SubmitTransactionAsync_WhenEntityIsNull_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<NullReferenceException>(() => CloudTable.SubmitTransactionAsync(
                new[]
                {
                    new TableTransactionAction(TableTransactionActionType.Delete, null)
                }
            ));

            Assert.Multiple(
                () => Assert.Equal(new NullReferenceException().Message, exception.Message),
                () => Assert.Contains(exception.Source, new[] { "Azure.Data.Tables", "System.Private.CoreLib" })
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenETagsIsNotSpecified_DeletesEntity()
        {
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(
                new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key",
                    StringProp = "string-prop",
                    Int32Prop = 4
                }
            );

            var result = await CloudTable.SubmitTransactionAsync(
                new[]
                {
                    new TableTransactionAction(
                        TableTransactionActionType.Delete,
                        new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            Int32Prop = 8,
                            Int64Prop = 12
                        }
                    )
                }
            );

            var rawResponse = result.GetRawResponse();
            var operationResponse = Assert.Single(result.Value);
            var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();

            Assert.Multiple(
                () => Assert.Empty(entities),
                () => Assertions.TableTransactionResponse(
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
                ),
                () => Assertions.EmptyResponse(
                    operationResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NoContent,
                        Headers = new Assertions.TransactionDeleteActionHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenETagsIsWildcard_DeletesEntity()
        {
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(
                new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key",
                    StringProp = "string-prop",
                    Int32Prop = 4
                }
            );

            var result = await CloudTable.SubmitTransactionAsync(
                new[]
                {
                    new TableTransactionAction(
                        TableTransactionActionType.Delete,
                        new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            Int32Prop = 8,
                            Int64Prop = 12
                        },
                        ETag.All
                    )
                }
            );

            var rawResponse = result.GetRawResponse();
            var operationResponse = Assert.Single(result.Value);
            var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();

            Assert.Multiple(
                () => Assert.Empty(entities),
                () => Assertions.TableTransactionResponse(
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
                ),
                () => Assertions.EmptyResponse(
                    operationResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NoContent,
                        Headers = new Assertions.TransactionDeleteActionHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenETagsMatch_DeletesEntity()
        {
            await CloudTable.CreateAsync();
            var addResult = await CloudTable.AddEntityAsync(
                new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key",
                    StringProp = "string-prop",
                    Int32Prop = 4
                }
            );

            var result = await CloudTable.SubmitTransactionAsync(
                new[]
                {
                    new TableTransactionAction(
                        TableTransactionActionType.Delete,
                        new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            Int32Prop = 8,
                            Int64Prop = 12
                        },
                        addResult.Headers.ETag.Value
                    )
                }
            );

            var rawResponse = result.GetRawResponse();
            var operationResponse = Assert.Single(result.Value);
            var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();

            Assert.Multiple(
                () => Assert.Empty(entities),
                () => Assertions.TableTransactionResponse(
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
                ),
                () => Assertions.EmptyResponse(
                    operationResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NoContent,
                        Headers = new Assertions.TransactionDeleteActionHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenEntityDoesNotExist_ThrowsException()
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(
                            TableTransactionActionType.Delete,
                            new TableEntity
                            {
                                PartitionKey =  new string('t', 1 << 10 + 1),
                                RowKey = new string('t', 1 << 10 + 1)
                            }
                        )
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NotFound,
                    ErrorCode = "ResourceNotFound",
                    ErrorDescription = "The specified resource does not exist."
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenMultipleEntitiesDoNotExist_ThrowsException()
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(
                            TableTransactionActionType.Delete,
                            new TableEntity
                            {
                                PartitionKey =  new string('t', 1 << 10 + 1),
                                RowKey = new string('t', 1 << 10 + 1)
                            }
                        ),
                        new TableTransactionAction(
                            TableTransactionActionType.Delete,
                            new TableEntity
                            {
                                PartitionKey =  new string('t', 1 << 10 + 1),
                                RowKey = "row-key-1"
                            }
                        )
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NotFound,
                    ErrorCode = "ResourceNotFound",
                    ErrorDescription = "0:The specified resource does not exist.",
                    FailedEntityIndex = 0
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WithMultipleEntitiesWhenOneDoesNotExist_ThrowsException()
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(
                            TableTransactionActionType.Delete,
                            new TableEntity
                            {
                                PartitionKey =  new string('t', 1 << 10 + 1),
                                RowKey = new string('t', 1 << 10 + 1)
                            }
                        ),
                        new TableTransactionAction(
                            TableTransactionActionType.Delete,
                            new TableEntity
                            {
                                PartitionKey =  new string('t', 1 << 10 + 1),
                                RowKey = new string("row-key")
                            }
                        )
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NotFound,
                    ErrorCode = "ResourceNotFound",
                    ErrorDescription = "0:The specified resource does not exist.",
                    FailedEntityIndex = 0
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenETagsMismatch_ThrowsException()
        {
            await CloudTable.CreateAsync();
            var addResult = await CloudTable.AddEntityAsync(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );
            await CloudTable.UpdateEntityAsync(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                },
                ETag.All
            );

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(
                            TableTransactionActionType.Delete,
                            new TableEntity
                            {
                                PartitionKey = "partition-key",
                                RowKey = "row-key"
                            },
                            addResult.Headers.ETag.Value
                        )
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.PreconditionFailed,
                    ErrorCode = "UpdateConditionNotSatisfied",
                    ErrorDescription = "The update condition specified in the request was not satisfied."
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WithMultipleEntitiesWhenETagsMismatch_ThrowsException()
        {
            await CloudTable.CreateAsync();
            var addResult = await CloudTable.AddEntityAsync(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );
            await CloudTable.UpdateEntityAsync(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                },
                ETag.All
            );

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(
                            TableTransactionActionType.Delete,
                            new TableEntity
                            {
                                PartitionKey = "partition-key",
                                RowKey = "row-key"
                            },
                            addResult.Headers.ETag.Value
                        ),
                        new TableTransactionAction(
                            TableTransactionActionType.Delete,
                            new TableEntity
                            {
                                PartitionKey = "partition-key",
                                RowKey = "row-key-1"
                            }
                        )
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.PreconditionFailed,
                    ErrorCode = "UpdateConditionNotSatisfied",
                    ErrorDescription = "0:The update condition specified in the request was not satisfied.",
                    FailedEntityIndex = 0
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenPartitionKeyIsNull_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<NullReferenceException>(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Delete, new TableEntity(null, "row-key"))
                    }
                )
            );
            Assert.Multiple(
                () => Assert.Equal(new NullReferenceException().Message, exception.Message),
                () => Assert.Contains(exception.Source, new[] { "Azure.Data.Tables", "System.Private.CoreLib" })
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task SubmitTransactionAsync_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            await CloudTable.CreateAsync();

            switch (partitionKey)
            {
                case "/":
                case "\\":
                    await Assertions.TransactionJsonResponseThrowsAsync(
                        () => CloudTable.SubmitTransactionAsync(
                            new[]
                            {
                                new TableTransactionAction(TableTransactionActionType.Delete, new TableEntity(partitionKey, "row-key"))
                            }
                        ),
                        rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            StatusCode = HttpStatusCode.BadRequest,
                            ErrorCode = "InvalidInput",
                            ErrorDescription = "0:Bad Request - Error in query syntax.",
                            FailedEntityIndex = 0
                        }
                    );
                    break;

                default:
                    await Assertions.TransactionJsonResponseThrowsAsync(
                        () => CloudTable.SubmitTransactionAsync(
                            new[]
                            {
                                new TableTransactionAction(TableTransactionActionType.Delete, new TableEntity(partitionKey, "row-key"))
                            }
                        ),
                        rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            StatusCode = HttpStatusCode.BadRequest,
                            ErrorCode = "OutOfRangeInput",
                            ErrorDescription = "0:One of the request inputs is out of range.",
                            FailedEntityIndex = 0
                        }
                    );
                    break;
            }
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenRowKeyIsNull_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<NullReferenceException>(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Delete, new TableEntity("partition-key", null))
                    }
                )
            );
            Assert.Multiple(
                () => Assert.Equal(new NullReferenceException().Message, exception.Message),
                () => Assert.Contains(exception.Source, new[] { "Azure.Data.Tables", "System.Private.CoreLib" })
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task SubmitTransactionAsync_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            await CloudTable.CreateAsync();

            switch (rowKey)
            {
                case "/":
                case "\\":
                    await Assertions.TransactionJsonResponseThrowsAsync(
                        () => CloudTable.SubmitTransactionAsync(
                            new[]
                            {
                                new TableTransactionAction(TableTransactionActionType.Delete, new TableEntity("partition-key", rowKey))
                            }
                        ),
                        rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            StatusCode = HttpStatusCode.BadRequest,
                            ErrorCode = "InvalidInput",
                            ErrorDescription = "0:Bad Request - Error in query syntax.",
                            FailedEntityIndex = 0
                        }
                    );
                    break;

                default:
                    await Assertions.TransactionJsonResponseThrowsAsync(
                        () => CloudTable.SubmitTransactionAsync(
                            new[]
                            {
                                new TableTransactionAction(TableTransactionActionType.Delete, new TableEntity("partition-key", rowKey))
                            }
                        ),
                        rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            StatusCode = HttpStatusCode.BadRequest,
                            ErrorCode = "OutOfRangeInput",
                            ErrorDescription = "0:One of the request inputs is out of range.",
                            FailedEntityIndex = 0
                        }
                    );
                    break;
            }
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidStringData), MemberType = typeof(TableOperationTestData))]
        public async Task SubmitTransactionAsync_WhenStringPropertyIsInvalid_DeletesEntity(string stringPropValue)
        {
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );

            var result = await CloudTable.SubmitTransactionAsync(
                new[]
                {
                    new TableTransactionAction(
                        TableTransactionActionType.Delete,
                        new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            StringProp = stringPropValue,
                        }
                    )
                }
            );

            var rawResponse = result.GetRawResponse();
            var operationResponse = Assert.Single(result.Value);
            var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();

            Assert.Multiple(
                () => Assert.Empty(entities),
                () => Assertions.TableTransactionResponse(
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
                ),
                () => Assertions.EmptyResponse(
                    operationResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NoContent,
                        Headers = new Assertions.TransactionDeleteActionHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidBinaryData), MemberType = typeof(TableOperationTestData))]
        public async Task SubmitTransactionAsync_WhenBinaryPropertyIsInvalid_DeletesEntity(byte[] binaryPropValue)
        {
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );

            var result = await CloudTable.SubmitTransactionAsync(
                new[]
                {
                    new TableTransactionAction(
                        TableTransactionActionType.Delete,
                        new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            BinaryProp = binaryPropValue,
                        }
                    )
                }
            );

            var rawResponse = result.GetRawResponse();
            var operationResponse = Assert.Single(result.Value);
            var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();

            Assert.Multiple(
                () => Assert.Empty(entities),
                () => Assertions.TableTransactionResponse(
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
                ),
                () => Assertions.EmptyResponse(
                    operationResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NoContent,
                        Headers = new Assertions.TransactionDeleteActionHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidDateTimeData), MemberType = typeof(TableOperationTestData))]
        public async Task SubmitTransactionAsync_WhenDateTimePropertyIsInvalid_DeletesEntity(DateTime dateTimePropValue)
        {
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );

            var result = await CloudTable.SubmitTransactionAsync(
                new[]
                {
                    new TableTransactionAction(
                        TableTransactionActionType.Delete,
                        new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            DateTimeProp = dateTimePropValue,
                        }
                    )
                }
            );

            var rawResponse = result.GetRawResponse();
            var operationResponse = Assert.Single(result.Value);
            var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();

            Assert.Multiple(
                () => Assert.Empty(entities),
                () => Assertions.TableTransactionResponse(
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
                ),
                () => Assertions.EmptyResponse(
                    operationResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NoContent,
                        Headers = new Assertions.TransactionDeleteActionHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenDateTimePropertyIsNotUniversal_DeletesEntity()
        {
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );

            var result = await CloudTable.SubmitTransactionAsync(
                new[]
                {
                    new TableTransactionAction(
                        TableTransactionActionType.Delete,
                        new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            DateTimeProp = DateTime.Now,
                        }
                    )
                }
            );

            var rawResponse = result.GetRawResponse();
            var operationResponse = Assert.Single(result.Value);
            var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();

            Assert.Multiple(
                () => Assert.Empty(entities),
                () => Assertions.TableTransactionResponse(
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
                ),
                () => Assertions.EmptyResponse(
                    operationResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NoContent,
                        Headers = new Assertions.TransactionDeleteActionHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }
    }
}