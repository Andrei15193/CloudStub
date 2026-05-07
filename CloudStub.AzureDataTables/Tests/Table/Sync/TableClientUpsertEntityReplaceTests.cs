using System;
using System.Collections.Generic;
using System.Net;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Sync
{
    public class TableClientUpsertEntityReplaceTests : BaseTableCloudStubTests
    {
        [Fact]
        public void UpsertEntityReplace_WhenTableDoesNotExist_ThrowsException()
        {
            Assertions.JsonResponseThrows(
                () => TableClient.UpsertEntity(
                    new TestEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key"
                    },
                    TableUpdateMode.Replace
                ),
                rawResponse =>
                {
                    var headers = new Assertions.DefaultHeaders(rawResponse);
                    headers.Remove("Cache-Control");

                    return new Assertions.UnsuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NotFound,
                        ErrorCode = "TableNotFound",
                        ErrorDescription = "The table specified does not exist.",
                        Headers = headers
                    };
                }
            );
        }

        [Fact]
        public void UpsertEntityReplace_WhenEntityIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("entity", () => TableClient.UpsertEntity<TableEntity>(null, TableUpdateMode.Replace));
            Assert.Equal(new ArgumentNullException("entity").Message, exception.Message);
        }

        [Fact]
        public void UpsertEntityReplace_WhenUpdateModeIsNotSupported_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentException>(() => TableClient.UpsertEntity(new TableEntity("partition-key", "row-key"), (TableUpdateMode)(-1)));
            Assert.Equal(new ArgumentException("Unexpected value for mode: -1").Message, exception.Message);
        }

        [Fact]
        public void UpsertEntityReplace_WhenEntityDoesNotExist_InsertsEntity()
        {
            var tableEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            TableClient.Create();

            var response = TableClient.UpsertEntity(tableEntity, TableUpdateMode.Replace);

            var entities = TableClient.Query<TableEntity>();
            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
                () => Assert.Contains(nameof(TestEntity.Timestamp), entity),
                () => Assert.Contains("odata.etag", entity),
                () => Assertions.EmptyResponse(response, new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NoContent,
                    Headers = new Assertions.NoContentHeaders(response)
                    {
                        { "ETag", response.Headers.ETag.ToString() }
                    }
                })
            );
        }

        [Fact]
        public void UpsertEntityReplace_WhenEntityHasOtherProperties_InsertsEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                BinaryProp = new byte[1 << 16],
                BooleanProp = true,
                StringProp = new string('t', 1 << 15),
                Int32Prop = 4,
                Int64Prop = 4,
                DoubleProp = 4,
                DateTimeProp = DateTime.MaxValue.ToUniversalTime(),
                GuidProp = Guid.NewGuid(),
                DecimalProp = 4
            };
            TableClient.Create();

            TableClient.UpsertEntity(testEntity, TableUpdateMode.Replace);

            var entities = TableClient.Query<TableEntity>();
            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
                () => Assert.Contains(nameof(TestEntity.Timestamp), entity),
                () => Assert.Contains("odata.etag", entity),
                () => Assert.Equal(testEntity.BinaryProp, entity[nameof(TestEntity.BinaryProp)]),
                () => Assert.Equal(testEntity.BooleanProp, entity[nameof(TestEntity.BooleanProp)]),
                () => Assert.Equal(testEntity.StringProp, entity[nameof(TestEntity.StringProp)]),
                () => Assert.Equal(testEntity.Int32Prop, entity[nameof(TestEntity.Int32Prop)]),
                () => Assert.Equal(testEntity.Int64Prop, entity[nameof(TestEntity.Int64Prop)]),
                () => Assert.Equal(testEntity.DoubleProp, entity[nameof(TestEntity.DoubleProp)]),
                () => Assert.Equal((DateTimeOffset?)testEntity.DateTimeProp, entity[nameof(TestEntity.DateTimeProp)]),
                () => Assert.Equal(testEntity.GuidProp, entity[nameof(TestEntity.GuidProp)]),
                () => Assert.Contains(nameof(TestEntity.DecimalProp), entity)
            );
        }

        [Fact]
        public void UpsertEntityReplace_InsertOrReplaceOperation_RepleacesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string value",
                Int32Prop = 4
            };
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                Int32Prop = 8,
                Int64Prop = 8
            };
            TableClient.Create();
            TableClient.AddEntity(testEntity);

            var response = TableClient.UpsertEntity(updatedTestEntity, TableUpdateMode.Replace);

            var entities = TableClient.Query<TableEntity>();
            var entity = Assert.Single(entities);

            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
                () => Assert.Contains(nameof(TestEntity.Timestamp), entity),
                () => Assert.Contains("odata.etag", entity),
                () => Assert.DoesNotContain(nameof(TestEntity.StringProp), entity),
                () => Assert.Equal(8, entity[nameof(TestEntity.Int32Prop)]),
                () => Assert.Equal(8L, entity[nameof(TestEntity.Int64Prop)]),
                () => Assertions.EmptyResponse(response, new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NoContent,
                    Headers = new Assertions.NoContentHeaders(response)
                    {
                        { "ETag", response.Headers.ETag.ToString() }
                    }
                })
            );
        }

        [Fact]
        public void UpsertEntityReplace_WhenDynamicEntityHasNullProperties_TheyAreIgnored()
        {
            TableClient.Create();

            TableClient.UpsertEntity(
                new TableEntity(
                    new Dictionary<string, object>
                    {
                        { nameof(TestEntity.Int32Prop), null }
                    }
                )
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                },
                TableUpdateMode.Replace
            );

            var entities = TableClient.Query<TableEntity>();
            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
                () => Assert.Contains(nameof(TestEntity.Timestamp), entity),
                () => Assert.Contains("odata.etag", entity),
                () => Assert.DoesNotContain(nameof(TestEntity.Int32Prop), entity)
            );
        }

        [Fact]
        public void UpsertEntityReplace_WhenDynamicEntityHasNullProperties_TheyAreRemovedWhenEntityAlreadyExists()
        {
            TableClient.Create();
            TableClient.AddEntity(new TableEntity(
                new Dictionary<string, object>
                {
                    { nameof(TestEntity.Int32Prop), 1 }
                }
            )
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            });

            TableClient.UpsertEntity(
                new TableEntity(
                    new Dictionary<string, object>
                    {
                        { nameof(TestEntity.Int32Prop), null }
                    }
                )
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                },
                TableUpdateMode.Replace
            );

            var entities = TableClient.Query<TableEntity>();
            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
                () => Assert.Contains(nameof(TestEntity.Timestamp), entity),
                () => Assert.Contains("odata.etag", entity),
                () => Assert.DoesNotContain(nameof(TestEntity.Int32Prop), entity)
            );
        }

        [Fact]
        public void UpsertEntityReplace_InsertOrReplaceOperationWhenPartitionKeyIsNull_ThrowsException()
        {
            TableClient.Create();

            var exception = Assert.Throws<ArgumentNullException>(
                "PartitionKey",
                () => TableClient.UpsertEntity(
                    new TableEntity(null, "row-key"),
                    TableUpdateMode.Replace
                )
            );

            Assert.Equal(new ArgumentNullException("PartitionKey").Message, exception.Message);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public void UpsertEntityReplace_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            var testEntity = new TableEntity
            {
                PartitionKey = partitionKey,
                RowKey = "row-key"
            };
            TableClient.Create();

            switch (partitionKey)
            {
                case "/":
                case "\\":
                    Assertions.JsonResponseThrows(
                        () => TableClient.UpsertEntity(testEntity, TableUpdateMode.Replace),
                        response => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            StatusCode = HttpStatusCode.BadRequest,
                            Headers = new Assertions.DeletedHeaders(response),
                            ErrorCode = "InvalidInput",
                            ErrorDescription = "Bad Request - Error in query syntax."
                        }
                    );
                    break;

                case "\u0000":
                    Assertions.InvalidUrlThrows(
                        () => TableClient.UpsertEntity(testEntity, TableUpdateMode.Replace),
                        response => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            WithoutRequestId = true,
                            StatusCode = HttpStatusCode.BadRequest,
                            Headers = new Assertions.InvalidUrlHeaders(response)
                            {
                                { "Connection", "close" },
                                { "Content-Length", "324" }
                            },
                            ErrorDescription = "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\">\r\n<HTML><HEAD><TITLE>Bad Request</TITLE>\r\n<META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD>\r\n<BODY><h2>Bad Request - Invalid URL</h2>\r\n<hr><p>HTTP Error 400. The request URL is invalid.</p>\r\n</BODY></HTML>\r\n"
                        }
                    );
                    break;

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
                case "\u000B":
                case "\u000C":
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
                    Assertions.InvalidUrlThrows(
                        () => TableClient.UpsertEntity(testEntity, TableUpdateMode.Replace),
                        response => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            WithoutRequestId = true,
                            StatusCode = HttpStatusCode.BadRequest,
                            Headers = new Assertions.InvalidUrlHeaders(response)
                            {
                                { "Content-Length", "312" }
                            },
                            ErrorDescription = "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\"><HTML><HEAD><TITLE>Bad Request</TITLE><META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD><BODY><h2>Bad Request - Invalid URL</h2><hr><p>HTTP Error 400. The request URL is invalid.</p></BODY></HTML>"
                        }
                    );
                    break;

                default:
                    Assertions.JsonResponseThrows(
                        () => TableClient.UpsertEntity(testEntity, TableUpdateMode.Replace),
                        response => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            StatusCode = HttpStatusCode.BadRequest,
                            Headers = new Assertions.DefaultHeaders(response),
                            ErrorCode = "OutOfRangeInput",
                            ErrorDescription = "One of the request inputs is out of range."
                        }
                    );
                    break;
            }
        }

        [Fact]
        public void UpsertEntityReplace_WhenPartitionKeyExceedsLimit_ThrowsException()
        {
            TableClient.Create();

            Assertions.JsonResponseThrows(
                () => TableClient.UpsertEntity(
                    new TableEntity
                    {
                        PartitionKey = new string('t', 1 << 10 + 1),
                        RowKey = "row-key"
                    },
                    TableUpdateMode.Replace
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    Headers = new Assertions.DefaultHeaders(rawResponse)
                }
            );
        }

        [Fact]
        public void UpsertEntityReplace_WhenRowKeyIsNull_ThrowsException()
        {
            TableClient.Create();

            var exception = Assert.Throws<ArgumentNullException>(
                "RowKey",
                () => TableClient.UpsertEntity(new TableEntity("partition-key", null), TableUpdateMode.Replace)
            );

            Assert.Equal(new ArgumentNullException("RowKey").Message, exception.Message);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public void UpsertEntityReplace_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = rowKey
            };
            TableClient.Create();

            switch (rowKey)
            {
                case "/":
                case "\\":
                    Assertions.JsonResponseThrows(
                        () => TableClient.UpsertEntity(testEntity, TableUpdateMode.Replace),
                        response => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            StatusCode = HttpStatusCode.BadRequest,
                            Headers = new Assertions.DeletedHeaders(response),
                            ErrorCode = "InvalidInput",
                            ErrorDescription = "Bad Request - Error in query syntax."
                        }
                    );
                    break;

                case "\u0000":
                    Assertions.InvalidUrlThrows(
                        () => TableClient.UpsertEntity(testEntity, TableUpdateMode.Replace),
                        response => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            WithoutRequestId = true,
                            StatusCode = HttpStatusCode.BadRequest,
                            Headers = new Assertions.InvalidUrlHeaders(response)
                            {
                                { "Connection", "close" },
                                { "Content-Length", "324" }
                            },
                            ErrorDescription = "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\">\r\n<HTML><HEAD><TITLE>Bad Request</TITLE>\r\n<META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD>\r\n<BODY><h2>Bad Request - Invalid URL</h2>\r\n<hr><p>HTTP Error 400. The request URL is invalid.</p>\r\n</BODY></HTML>\r\n"
                        }
                    );
                    break;

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
                case "\u000B":
                case "\u000C":
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
                    Assertions.InvalidUrlThrows(
                        () => TableClient.UpsertEntity(testEntity, TableUpdateMode.Replace),
                        response => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            WithoutRequestId = true,
                            StatusCode = HttpStatusCode.BadRequest,
                            Headers = new Assertions.InvalidUrlHeaders(response)
                            {
                                { "Content-Length", "312" }
                            },
                            ErrorDescription = "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\"><HTML><HEAD><TITLE>Bad Request</TITLE><META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD><BODY><h2>Bad Request - Invalid URL</h2><hr><p>HTTP Error 400. The request URL is invalid.</p></BODY></HTML>"
                        }
                    );
                    break;

                default:
                    Assertions.JsonResponseThrows(
                        () => TableClient.UpsertEntity(testEntity, TableUpdateMode.Replace),
                        response => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            StatusCode = HttpStatusCode.BadRequest,
                            Headers = new Assertions.DefaultHeaders(response),
                            ErrorCode = "OutOfRangeInput",
                            ErrorDescription = "One of the request inputs is out of range."
                        }
                    );
                    break;
            }
        }

        [Fact]
        public void UpsertEntityReplace_WhenRowKeyExceedsLimit_ThrowsException()
        {
            TableClient.Create();

            Assertions.JsonResponseThrows(
                () => TableClient.UpsertEntity(
                    new TableEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = new string('t', 1 << 10 + 1)
                    },
                    TableUpdateMode.Replace
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    Headers = new Assertions.DefaultHeaders(rawResponse)
                }
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidStringData), MemberType = typeof(TableOperationTestData))]
        public void UpsertEntityReplace_WhenStringPropertyIsInvalid_ThrowsException(string stringPropValue)
        {
            TableClient.Create();

            Assertions.JsonResponseThrows(
                () => TableClient.UpsertEntity(
                    new TestEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key",
                        StringProp = stringPropValue
                    },
                    TableUpdateMode.Replace
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    Headers = new Assertions.DefaultHeaders(rawResponse)
                }
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidBinaryData), MemberType = typeof(TableOperationTestData))]
        public void UpsertEntityReplace_WhenBinaryPropertyIsInvalid_ThrowsException(byte[] binaryPropValue)
        {
            TableClient.Create();

            Assertions.JsonResponseThrows(
                () => TableClient.UpsertEntity(
                    new TestEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key",
                        BinaryProp = binaryPropValue
                    },
                    TableUpdateMode.Replace
                ),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    Headers = new Assertions.DefaultHeaders(rawResponse)
                }
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidDateTimeData), MemberType = typeof(TableOperationTestData))]
        public void UpsertEntityReplace_WhenDateTimePropertyIsInvalid_ThrowsException(DateTime dateTimePropValue)
        {
            TableClient.Create();

            Assertions.JsonResponseThrows(
                () => TableClient.UpsertEntity(
                    new TestEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key",
                        DateTimeProp = dateTimePropValue
                    },
                    TableUpdateMode.Replace
                ),
                rawResponse =>
                {
                    var headers = new Assertions.DefaultHeaders(rawResponse);
                    headers.Remove("Cache-Control");

                    return new Assertions.UnsuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.BadRequest,
                        ErrorCode = "OutOfRangeInput",
                        ErrorDescription = $"The 'DateTimeProp' parameter of value '{dateTimePropValue:MM/dd/yyyy HH:mm:ss}' is out of range.",
                        Headers = headers
                    };
                }
            );
        }

        [Fact]
        public void UpsertEntityReplace_WhenDateTimePropertyIsNotUniversal_ThrowsException()
        {
            var now = DateTime.Now;
            var exception = Assert.Throws<NotSupportedException>(() => TableClient.UpsertEntity(
                new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key",
                    DateTimeProp = now
                },
                TableUpdateMode.Replace
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