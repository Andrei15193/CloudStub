using System;
using System.Net;
using Azure;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Transactions.Sync
{
    public class TableClientTransactionDeleteEntityTests : BaseTableCloudStubTests
    {
        [Fact]
        public void SubmitTransaction_WhenTableDoesNotExist_ThrowsException()
        {
            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenEntityIsNull_ThrowsException()
        {
            var exception = Assert.Throws<NullReferenceException>(() => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenETagsIsNotSpecified_DeletesEntity()
        {
            CloudTable.Create();
            CloudTable.AddEntity(
                new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key",
                    StringProp = "string-prop",
                    Int32Prop = 4
                }
            );

            var result = CloudTable.SubmitTransaction(
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
            var entities = CloudTable.Query<TableEntity>();

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
                        Headers = new Assertions.TableTransactionDeleteHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Fact]
        public void SubmitTransaction_WhenETagsIsWildcard_DeletesEntity()
        {
            CloudTable.Create();
            CloudTable.AddEntity(
                new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key",
                    StringProp = "string-prop",
                    Int32Prop = 4
                }
            );

            var result = CloudTable.SubmitTransaction(
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
            var entities = CloudTable.Query<TableEntity>();

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
                        Headers = new Assertions.TableTransactionDeleteHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Fact]
        public void SubmitTransaction_WhenETagsMatch_DeletesEntity()
        {
            CloudTable.Create();
            var addResult = CloudTable.AddEntity(
                new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key",
                    StringProp = "string-prop",
                    Int32Prop = 4
                }
            );

            var result = CloudTable.SubmitTransaction(
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
            var entities = CloudTable.Query<TableEntity>();

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
                        Headers = new Assertions.TableTransactionDeleteHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Fact]
        public void SubmitTransaction_WhenEntityDoesNotExist_ThrowsException()
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WithMultipleEntitiesWhenOneDoesNotExist_ThrowsException()
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenETagsMismatch_ThrowsException()
        {
            CloudTable.Create();
            var addResult = CloudTable.AddEntity(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );
            CloudTable.UpdateEntity(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                },
                ETag.All
            );

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WithMultipleEntitiesWhenETagsMismatch_ThrowsException()
        {
            CloudTable.Create();
            var addResult = CloudTable.AddEntity(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );
            CloudTable.UpdateEntity(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                },
                ETag.All
            );

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenPartitionKeyIsNull_ThrowsException()
        {
            var exception = Assert.Throws<NullReferenceException>(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            CloudTable.Create();

            switch (partitionKey)
            {
                case "/":
                case "\\":
                    Assertions.TransactionJsonResponseThrows(
                        () => CloudTable.SubmitTransaction(
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
                    Assertions.TransactionJsonResponseThrows(
                        () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenRowKeyIsNull_ThrowsException()
        {
            var exception = Assert.Throws<NullReferenceException>(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            CloudTable.Create();

            switch (rowKey)
            {
                case "/":
                case "\\":
                    Assertions.TransactionJsonResponseThrows(
                        () => CloudTable.SubmitTransaction(
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
                    Assertions.TransactionJsonResponseThrows(
                        () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenStringPropertyIsInvalid_DeletesEntity(string stringPropValue)
        {
            CloudTable.Create();
            CloudTable.AddEntity(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );

            var result = CloudTable.SubmitTransaction(
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
            var entities = CloudTable.Query<TableEntity>();

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
                        Headers = new Assertions.TableTransactionDeleteHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidBinaryData), MemberType = typeof(TableOperationTestData))]
        public void SubmitTransaction_WhenBinaryPropertyIsInvalid_DeletesEntity(byte[] binaryPropValue)
        {
            CloudTable.Create();
            CloudTable.AddEntity(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );

            var result = CloudTable.SubmitTransaction(
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
            var entities = CloudTable.Query<TableEntity>();

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
                        Headers = new Assertions.TableTransactionDeleteHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidDateTimeData), MemberType = typeof(TableOperationTestData))]
        public void SubmitTransaction_WhenDateTimePropertyIsInvalid_DeletesEntity(DateTime dateTimePropValue)
        {
            CloudTable.Create();
            CloudTable.AddEntity(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );

            var result = CloudTable.SubmitTransaction(
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
            var entities = CloudTable.Query<TableEntity>();

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
                        Headers = new Assertions.TableTransactionDeleteHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Fact]
        public void SubmitTransaction_WhenDateTimePropertyIsNotUniversal_DeletesEntity()
        {
            CloudTable.Create();
            CloudTable.AddEntity(
                new TableEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );

            var result = CloudTable.SubmitTransaction(
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
            var entities = CloudTable.Query<TableEntity>();

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
                        Headers = new Assertions.TableTransactionDeleteHeaders(),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }
    }
}