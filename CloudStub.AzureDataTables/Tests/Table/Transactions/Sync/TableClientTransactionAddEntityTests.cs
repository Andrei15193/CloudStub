using System;
using System.Net;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Transactions.Sync
{
    public class TableClientTransactionAddEntityTests : BaseTableCloudStubTests
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
        public void SubmitTransaction_WhenEntityIsNull_ThrowsException()
        {
            var exception = Assert.Throws<NullReferenceException>(() => CloudTable.SubmitTransaction(
                new[]
                {
                    new TableTransactionAction(TableTransactionActionType.Add, null)
                }
            ));

            Assert.Multiple(
                () => Assert.Equal(new NullReferenceException().Message, exception.Message),
                () => Assert.Contains(exception.Source, new[] { "Azure.Data.Tables", "System.Private.CoreLib" })
            );
        }

        [Fact]
        public void SubmitTransaction_InsertOperation_InsertsEntity()
        {
            CloudTable.Create();
            var result = CloudTable.SubmitTransaction(
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

            Assertions.EmptyResponse(
                operationResponse,
                new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NoContent,
                    Headers = new Assertions.TableTransactionHeaders(operationResponse, CloudTable.Name, "partition-key", "row-key"),
                    WithoutRequestId = true,
                    WithoutClientRequestId = true,
                    WithoutDate = true
                }
            );
        }

        [Fact]
        public void SubmitTransaction_InsertOperationWhenEntityAlreadyExists_ThrowsException()
        {
            CloudTable.Create();
            CloudTable.AddEntity(new TableEntity("partition-key", "row-key"));

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_MultipleInsertOperationsWhenOneEntityAlreadyExists_ThrowsException()
        {
            CloudTable.Create();
            CloudTable.AddEntity(new TableEntity("partition-key", "row-key"));

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_InserOperationWhenEntityHasOtherProperties_InsertsEntity()
        {
            var guid = Guid.NewGuid();
            CloudTable.Create();

            CloudTable.SubmitTransaction(
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

            var entity = Assert.Single(CloudTable.Query<TableEntity>());
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

        [Fact]
        public void SubmitTransaction_WhenPartitionKeyIsNull_ThrowsException()
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenPartitionKeyExceedsLimit_ThrowsException()
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WithMultipleEntitiesWhenPartitionKeysExceedsLimit_ThrowsException()
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenRowKeyIsNull_ThrowsException()
        {
            CloudTable.Create();

            var exception = Assert.Throws<ArgumentNullException>(
                "key",
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenRowKeyExceedsLimit_ThrowsException()
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WithMultipleEntitiesWhenOneRowKeyExceedsLimit_ThrowsException()
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenStringPropertyIsInvalid_ThrowsException(string stringPropValue)
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WithMultipleEntitiesWhenOneStringPropertyIsInvalid_ThrowsException(string stringPropValue)
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenBinaryPropertyIsInvalid_ThrowsException(byte[] binaryPropValue)
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WithMultipleEntitiesWhenOneBinaryPropertyIsInvalid_ThrowsException(byte[] binaryPropValue)
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenDateTimePropertyIsInvalid_ThrowsException(DateTime dateTimePropValue)
        {
            CloudTable.Create();

            Assertions.TransactionJsonResponseThrows(
                () => CloudTable.SubmitTransaction(
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
        public void SubmitTransaction_WhenDateTimePropertyIsNotUniversal_ThrowsException()
        {
            var now = DateTime.Now;
            var exception = Assert.Throws<NotSupportedException>(() => CloudTable.SubmitTransaction(
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