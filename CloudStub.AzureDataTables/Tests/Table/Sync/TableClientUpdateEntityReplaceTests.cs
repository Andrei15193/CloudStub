using System;
using System.Collections.Generic;
using System.Net;
using Azure;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Sync
{
    public class TableClientUpdateEntityReplaceTests : BaseTableCloudStubTests
    {
        [Fact]
        public void UpdateEntityReplace_WhenTableDoesNotExist_ThrowsException()
        {
            Assertions.JsonResponseThrows(
                () => CloudTable.UpdateEntity(
                    new TestEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key"
                    },
                    ETag.All,
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
        public void UpdateEntityReplace_WhenEntityIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("entity", () => CloudTable.UpdateEntity<TableEntity>(null, ETag.All, TableUpdateMode.Replace));
            Assert.Equal(new ArgumentNullException("entity").Message, exception.Message);
            Assert.Equal("Azure.Data.Tables", exception.Source);
        }

        [Fact]
        public void UpdateEntityReplace_WhenETagIsDefault_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentException>(
                "ifMatch",
                () => CloudTable.UpdateEntity(
                    new TableEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key"
                    },
                    default,
                    TableUpdateMode.Replace
                )
            );
            Assert.Equal(new ArgumentException("Value cannot be empty.", "ifMatch").Message, exception.Message);
            Assert.Equal("Azure.Data.Tables", exception.Source);
        }

        [Fact]
        public void UpdateEntityReplace_WhenUpdateModeIsNotSupported_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentException>(() => CloudTable.UpdateEntity(new TableEntity("partition-key", "row-key"), ETag.All, (TableUpdateMode)(-1)));
            Assert.Equal(new ArgumentException("Unexpected value for mode: -1").Message, exception.Message);
        }

        [Fact]
        public void UpdateEntityReplace_WhenETagsIsWildcard_ReplacesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string-prop"
            };
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8
            };
            CloudTable.Create();
            CloudTable.AddEntity(testEntity);

            var response = CloudTable.UpdateEntity(updatedTestEntity, ETag.All, TableUpdateMode.Replace);

            var entities = CloudTable.Query<TableEntity>();
            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Contains(nameof(TestEntity.PartitionKey), entity),
                () => Assert.Contains(nameof(TestEntity.RowKey), entity),
                () => Assert.Contains(nameof(TestEntity.Timestamp), entity),
                () => Assert.Contains("odata.etag", entity),
                () => Assert.DoesNotContain(nameof(TestEntity.StringProp), entity),
                () => Assert.DoesNotContain(nameof(TestEntity.Int64Prop), entity),
                () => Assert.Equal(8, entity[nameof(TestEntity.Int32Prop)]),

                () => Assertions.EmptyResponse(
                    response,
                    new Assertions.ResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NoContent,
                        Headers = new Assertions.NoContentHeaders(response)
                        {
                            { "ETag", response.Headers.ETag.ToString() }
                        }
                    }
                )
            );
        }

        [Fact]
        public void UpdateEntityReplace_WhenETagsMatch_ReplacesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string-prop"
            };
            CloudTable.Create();
            var response = CloudTable.AddEntity(testEntity);
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8
            };

            response = CloudTable.UpdateEntity(updatedTestEntity, response.Headers.ETag.Value, TableUpdateMode.Replace);

            var entities = CloudTable.Query<TableEntity>();
            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Contains(nameof(TestEntity.PartitionKey), entity),
                () => Assert.Contains(nameof(TestEntity.RowKey), entity),
                () => Assert.Contains(nameof(TestEntity.Timestamp), entity),
                () => Assert.Contains("odata.etag", entity),
                () => Assert.DoesNotContain(nameof(TestEntity.StringProp), entity),
                () => Assert.DoesNotContain(nameof(TestEntity.Int64Prop), entity),
                () => Assert.Equal(8, entity[nameof(TestEntity.Int32Prop)]),

                () => Assertions.EmptyResponse(
                    response,
                    new Assertions.ResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NoContent,
                        Headers = new Assertions.NoContentHeaders(response)
                        {
                            { "ETag", response.Headers.ETag.ToString() }
                        }
                    }
                )
            );
        }

        [Fact]
        public void UpdateEntityReplace_WhenDynamicEntityHasNullProperties_TheyAreRemoved()
        {
            CloudTable.Create();
            var tableResult = CloudTable.AddEntity(
                new TableEntity(
                    new Dictionary<string, object>
                    {
                        { nameof(TestEntity.Int32Prop), 1 }
                    }
                )
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }
            );

            var response = CloudTable.UpdateEntity(
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
                ETag.All,
                TableUpdateMode.Replace
            );

            var entities = CloudTable.Query<TableEntity>();
            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Contains(nameof(TestEntity.PartitionKey), entity),
                () => Assert.Contains(nameof(TestEntity.RowKey), entity),
                () => Assert.Contains(nameof(TestEntity.Timestamp), entity),
                () => Assert.Contains("odata.etag", entity),
                () => Assert.DoesNotContain(nameof(TestEntity.Int32Prop), entity),

                () => Assertions.EmptyResponse(
                    response,
                    new Assertions.ResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NoContent,
                        Headers = new Assertions.NoContentHeaders(response)
                        {
                            { "ETag", response.Headers.ETag.ToString() }
                        }
                    }
                )
            );
        }

        [Fact]
        public void UpdateEntityReplace_WhenEntityDoesNotExist_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = new string('t', 1 << 10 + 1),
                RowKey = new string('t', 1 << 10 + 1)
            };
            CloudTable.Create();

            Assertions.JsonResponseThrows(
                () => CloudTable.UpdateEntity(testEntity, ETag.All, TableUpdateMode.Replace),
                response => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NotFound,
                    Headers = new Assertions.DefaultHeaders(response),
                    ErrorCode = "ResourceNotFound",
                    ErrorDescription = "The specified resource does not exist."
                }
            );
        }

        [Fact]
        public void UpdateEntityReplace_WhenETagsMismatch_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            CloudTable.Create();
            var response = CloudTable.AddEntity(testEntity);
            var updatedTestEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            CloudTable.UpdateEntity(testEntity, ETag.All, TableUpdateMode.Replace);

            Assertions.JsonResponseThrows(
                () => CloudTable.UpdateEntity(updatedTestEntity, response.Headers.ETag.Value, TableUpdateMode.Replace),
                updateResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.PreconditionFailed,
                    Headers = new Assertions.DefaultHeaders(updateResponse),
                    ErrorCode = "UpdateConditionNotSatisfied",
                    ErrorDescription = "The update condition specified in the request was not satisfied."
                }
            );
        }

        [Fact]
        public void UpdateEntityReplace_WhenPartitionKeyIsNull_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = null,
                RowKey = "row-key"
            };
            CloudTable.Create();

            var exception = Assert.Throws<ArgumentNullException>(
                "PartitionKey",
                () => CloudTable.UpdateEntity(testEntity, ETag.All, TableUpdateMode.Replace)
            );

            Assert.Equal(new ArgumentNullException("PartitionKey").Message, exception.Message);
            Assert.Equal("Azure.Data.Tables", exception.Source);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public void UpdateEntityReplace_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            var testEntity = new TableEntity
            {
                PartitionKey = partitionKey,
                RowKey = "row-key"
            };
            CloudTable.Create();

            switch (partitionKey)
            {
                case "/":
                case "\\":
                    Assertions.JsonResponseThrows(
                        () => CloudTable.UpdateEntity(testEntity, ETag.All, TableUpdateMode.Replace),
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
                        () => CloudTable.UpdateEntity(testEntity, ETag.All, TableUpdateMode.Replace),
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
                        () => CloudTable.UpdateEntity(testEntity, ETag.All, TableUpdateMode.Replace),
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
                        () => CloudTable.UpdateEntity(testEntity, ETag.All, TableUpdateMode.Replace),
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
        public void UpdateEntityReplace_WhenRowKeyIsNull_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = null
            };
            CloudTable.Create();

            var exception = Assert.Throws<ArgumentNullException>(
                "RowKey",
                () => CloudTable.UpdateEntity(testEntity, ETag.All, TableUpdateMode.Replace)
            );

            Assert.Equal(new ArgumentNullException("RowKey").Message, exception.Message);
            Assert.Equal("Azure.Data.Tables", exception.Source);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public void UpdateEntityReplace_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = rowKey
            };
            CloudTable.Create();

            switch (rowKey)
            {
                case "/":
                case "\\":
                    Assertions.JsonResponseThrows(
                        () => CloudTable.UpdateEntity(testEntity, ETag.All, TableUpdateMode.Replace),
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
                        () => CloudTable.UpdateEntity(testEntity, ETag.All, TableUpdateMode.Replace),
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
                        () => CloudTable.UpdateEntity(testEntity, ETag.All, TableUpdateMode.Replace),
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
                        () => CloudTable.UpdateEntity(testEntity, ETag.All, TableUpdateMode.Replace),
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

        [Theory, MemberData(nameof(TableOperationTestData.InvalidStringData), MemberType = typeof(TableOperationTestData))]
        public void UpdateEntityReplace_WhenStringPropertyIsInvalid_ThrowsException(string stringPropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                StringProp = stringPropValue,
                ETag = testEntity.ETag
            };
            CloudTable.Create();
            CloudTable.AddEntity(testEntity);

            Assertions.JsonResponseThrows(
                () => CloudTable.UpdateEntity(
                    new TestEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key",
                        StringProp = stringPropValue
                    },
                    ETag.All,
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
        public void UpdateEntityReplace_WhenBinaryPropertyIsInvalid_ThrowsException(byte[] binaryPropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
            };
            CloudTable.Create();
            CloudTable.AddEntity(testEntity);

            Assertions.JsonResponseThrows(
                () => CloudTable.UpdateEntity(
                    new TestEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key",
                        BinaryProp = binaryPropValue
                    },
                    ETag.All,
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
        public void UpdateEntityReplace_WhenDateTimePropertyIsInvalid_ThrowsException(DateTime dateTimePropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            CloudTable.Create();
            CloudTable.AddEntity(testEntity);

            Assertions.JsonResponseThrows(
                () => CloudTable.UpdateEntity(
                    new TestEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key",
                        DateTimeProp = dateTimePropValue
                    },
                    ETag.All,
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
                });
        }
    }
}