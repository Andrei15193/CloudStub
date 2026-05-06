using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Transactions.Async
{
    public class TableClientTransactionAddEntityTests : BaseTableCloudStubTests
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
        public async Task SubmitTransactionAsync_WhenEntityIsNull_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<NullReferenceException>(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, null)
                    }
                )
            );

            Assert.Multiple(
                () => Assert.Equal(new NullReferenceException().Message, exception.Message),
                () => Assert.Contains(exception.Source, new[] { "Azure.Data.Tables", "System.Private.CoreLib" })
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_InsertOperation_InsertsEntity()
        {
            await CloudTable.CreateAsync();

            var result = await CloudTable.SubmitTransactionAsync(
                new[]
                {
                    new TableTransactionAction(
                        TableTransactionActionType.Add,
                        new TableEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key"
                        }
                    )
                }
            );

            var rawResponse = result.GetRawResponse();
            var operationResponse = Assert.Single(result.Value);
            var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();
            var entity = Assert.Single(entities);

            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
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
                        Headers = new Assertions.TransactionAddActionHeaders(operationResponse, CloudTable.Name, "partition-key", "row-key"),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                )
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_InsertOperationWhenEntityAlreadyExists_ThrowsException()
        {
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(new TableEntity("partition-key", "row-key"));

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(
                            TableTransactionActionType.Add,
                            new TableEntity
                            {
                                PartitionKey = "partition-key",
                                RowKey = "row-key"
                            }
                        )
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.Conflict,
                    ErrorCode = "EntityAlreadyExists",
                    ErrorDescription = "The specified entity already exists."
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_MultipleInsertOperationsWhenOneEntityAlreadyExists_ThrowsException()
        {
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(new TableEntity("partition-key", "row-key"));

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(
                            TableTransactionActionType.Add,
                            new TableEntity
                            {
                                PartitionKey = "partition-key",
                                RowKey = "row-key-1"
                            }
                        ),
                        new TableTransactionAction(
                            TableTransactionActionType.Add,
                            new TableEntity
                            {
                                PartitionKey = "partition-key",
                                RowKey = "row-key"
                            }
                        ),
                        new TableTransactionAction(
                            TableTransactionActionType.Add,
                            new TableEntity
                            {
                                PartitionKey = "partition-key",
                                RowKey = "row-key-2"
                            }
                        )
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.Conflict,
                    ErrorCode = "EntityAlreadyExists",
                    ErrorDescription = "1:The specified entity already exists.",
                    FailedEntityIndex = 1
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_InserOperationWhenEntityHasOtherProperties_InsertsEntity()
        {
            var guid = Guid.NewGuid();
            await CloudTable.CreateAsync();

            var result = await CloudTable.SubmitTransactionAsync(
                new[]
                {
                    new TableTransactionAction(
                        TableTransactionActionType.Add,
                        new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            BinaryProp = new byte[1 << 16],
                            BooleanProp = true,
                            StringProp = new string('t', 1 << 15),
                            Int32Prop = 4,
                            Int64Prop = 5,
                            DoubleProp = 6,
                            DateTimeProp = DateTime.MaxValue.ToUniversalTime(),
                            DateTimeOffsetProp = DateTimeOffset.MaxValue.ToUniversalTime(),
                            GuidProp = guid,
                            DecimalProp = 7
                        }
                    )
                }
            );

            var rawResponse = result.GetRawResponse();
            var operationResponse = Assert.Single(result.Value);
            var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();
            var entity = Assert.Single(entities);

            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
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
                        Headers = new Assertions.TransactionAddActionHeaders(operationResponse, CloudTable.Name, "partition-key", "row-key"),
                        WithoutRequestId = true,
                        WithoutClientRequestId = true,
                        WithoutDate = true
                    }
                ),
                () =>
                {
                    Assert.Contains(nameof(TestEntity.PartitionKey), entity);
                    Assert.Contains(nameof(TestEntity.RowKey), entity);
                    Assert.Contains(nameof(TestEntity.Timestamp), entity);
                    Assert.Contains("odata.etag", entity);
                    Assert.Equal(new byte[1 << 16], entity[nameof(TestEntity.BinaryProp)]);
                    Assert.Equal(true, entity[nameof(TestEntity.BooleanProp)]);
                    Assert.Equal(new string('t', 1 << 15), entity[nameof(TestEntity.StringProp)]);
                    Assert.Equal(4, entity[nameof(TestEntity.Int32Prop)]);
                    Assert.Equal(5L, entity[nameof(TestEntity.Int64Prop)]);
                    Assert.Equal(6D, entity[nameof(TestEntity.DoubleProp)]);
                    Assert.Equal((DateTimeOffset)DateTime.MaxValue.ToUniversalTime(), entity[nameof(TestEntity.DateTimeProp)]);
                    Assert.Equal(DateTimeOffset.MaxValue.ToUniversalTime(), entity[nameof(TestEntity.DateTimeOffsetProp)]);
                    Assert.Equal(guid, entity[nameof(TestEntity.GuidProp)]);
                    Assert.Equal(7, entity[nameof(TestEntity.DecimalProp)]);
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenPartitionKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity(null, "row-key"))
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertiesNeedValue",
                    ErrorDescription = "0:The values are not specified for all properties in the entity.",
                    FailedEntityIndex = 0
                }
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task SubmitTransactionAsync_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity(partitionKey, "row-key"))
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "OutOfRangeInput",
                    ErrorDescription = $"0:The 'PartitionKey' parameter of value '{partitionKey}' is out of range.",
                    FailedEntityIndex = 0
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenPartitionKeyExceedsLimit_ThrowsException()
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity(new string('t', 1 << 10 + 1), "row-key"))
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less."
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WithMultipleEntitiesWhenPartitionKeysExceedsLimit_ThrowsException()
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity(new string('t', 1 << 10 + 1), "row-key-1")),
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity(new string('t', 1 << 10 + 1), "row-key-2"))
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "0:The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    FailedEntityIndex = 0
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenRowKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "key",
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("partition-key", null))
                    }
                )
            );
            Assert.Multiple(
                () => Assert.Equal(new ArgumentNullException("key").Message, exception.Message),
                () => Assert.Contains(exception.Source, new[] { "Azure.Data.Tables", "System.Private.CoreLib" })
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task SubmitTransactionAsync_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("partition-key", rowKey))
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "OutOfRangeInput",
                    ErrorDescription = $"0:The 'RowKey' parameter of value '{rowKey}' is out of range.",
                    FailedEntityIndex = 0
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenRowKeyExceedsLimit_ThrowsException()
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("partition-key", new string('t', 1 << 10 + 1)))
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less."
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WithMultipleEntitiesWhenOneRowKeyExceedsLimit_ThrowsException()
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("partition-key", "row-key")),
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("partition-key", new string('t', 1 << 10 + 1)))
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "1:The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    FailedEntityIndex = 1
                }
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidStringData), MemberType = typeof(TableOperationTestData))]
        public async Task SubmitTransactionAsync_WhenStringPropertyIsInvalid_ThrowsException(string stringPropValue)
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            StringProp = stringPropValue
                        })
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less."
                }
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidStringData), MemberType = typeof(TableOperationTestData))]
        public async Task SubmitTransactionAsync_WithMultipleEntitiesWhenOneStringPropertyIsInvalid_ThrowsException(string stringPropValue)
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("partition-key", "row-key-1")),
                        new TableTransactionAction(TableTransactionActionType.Add, new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            StringProp = stringPropValue
                        })
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "1:The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    FailedEntityIndex = 1
                }
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidBinaryData), MemberType = typeof(TableOperationTestData))]
        public async Task SubmitTransactionAsync_WhenBinaryPropertyIsInvalid_ThrowsException(byte[] binaryPropValue)
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            BinaryProp = binaryPropValue,
                        })
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less."
                }
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidBinaryData), MemberType = typeof(TableOperationTestData))]
        public async Task SubmitTransactionAsync_WithMultipleEntitiesWhenOneBinaryPropertyIsInvalid_ThrowsException(byte[] binaryPropValue)
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("partition-key", "row-key-1")),
                        new TableTransactionAction(TableTransactionActionType.Add, new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            BinaryProp = binaryPropValue,
                        })
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "1:The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    FailedEntityIndex = 1
                }
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidDateTimeData), MemberType = typeof(TableOperationTestData))]
        public async Task SubmitTransactionAsync_WhenDateTimePropertyIsInvalid_ThrowsException(DateTime dateTimePropValue)
        {
            await CloudTable.CreateAsync();

            await Assertions.TransactionJsonResponseThrowsAsync(
                () => CloudTable.SubmitTransactionAsync(
                    new[]
                    {
                        new TableTransactionAction(TableTransactionActionType.Add, new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            DateTimeProp = dateTimePropValue,
                        })
                    }
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "OutOfRangeInput",
                    ErrorDescription = $"0:The 'DateTimeProp' parameter of value '{dateTimePropValue:MM/dd/yyyy HH:mm:ss}' is out of range.",
                    FailedEntityIndex = 0
                }
            );
        }

        [Fact]
        public async Task SubmitTransactionAsync_WhenDateTimePropertyIsNotUniversal_ThrowsException()
        {
            var now = DateTime.Now;
            var exception = await Assert.ThrowsAsync<NotSupportedException>(() => CloudTable.SubmitTransactionAsync(
                new[]
                {
                    new TableTransactionAction(
                        TableTransactionActionType.Add,
                        new TestEntity
                        {
                            PartitionKey = "partition-key",
                            RowKey = "row-key",
                            DateTimeProp = now
                        }
                    )
                }
            ));

            Assert.Multiple(
                () => Assert.Equal($"DateTime {now} has a Kind of {now.Kind}. Azure SDK requires it to be UTC. You can call DateTime.SpecifyKind to change Kind property value to DateTimeKind.Utc.", exception.Message),
                () => Assert.Equal("Azure.Data.Tables", exception.Source),
                () => Assert.Empty(exception.Data),
                () => Assert.Null(exception.InnerException),
                () => Assert.Null(exception.HelpLink),
                () => Assert.Equal(-2146233067, exception.HResult)
            );
        }
    }
}