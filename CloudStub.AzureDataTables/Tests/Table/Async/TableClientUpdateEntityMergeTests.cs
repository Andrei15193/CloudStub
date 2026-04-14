using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Async
{
    public class TableClientUpdateEntityMergeTests : BaseTableCloudStubTests
    {
        [Fact]
        public async Task UpdateEntityMergeAsync_WhenTableDoesNotExist_ThrowsException()
        {
            await Assertions.JsonResponseThrowsAsync(
                () => CloudTable.UpdateEntityAsync(
                    new TestEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key"
                    },
                    ETag.All,
                    TableUpdateMode.Merge
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
        public async Task UpdateEntityMergeAsync_WhenEntityIsNull_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<ArgumentNullException>("entity", () => CloudTable.UpdateEntityAsync<TableEntity>(null, ETag.All, TableUpdateMode.Merge));
            Assert.Equal(new ArgumentNullException("entity").Message, exception.Message);
            Assert.Equal("Azure.Data.Tables", exception.Source);
        }

        [Fact]
        public async Task UpdateEntityMergeAsync_WhenETagIsDefault_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<ArgumentException>(
                "ifMatch",
                () => CloudTable.UpdateEntityAsync(
                    new TableEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key"
                    },
                    default,
                    TableUpdateMode.Merge
                )
            );
            Assert.Equal(new ArgumentException("Value cannot be empty.", "ifMatch").Message, exception.Message);
            Assert.Equal("Azure.Data.Tables", exception.Source);
        }

        [Fact]
        public async Task UpdateEntityMergeAsync_WhenUpdateModeIsNotSupported_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<ArgumentException>(() => CloudTable.UpdateEntityAsync(new TableEntity("partition-key", "row-key"), ETag.All, (TableUpdateMode)(-1)));
             Assert.Equal(new ArgumentException("Unexpected value for mode: -1").Message, exception.Message);
        }

        [Fact]
        public async Task UpdateEntityMergeAsync_WhenETagsIsWildcard_MergesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string-prop",
                Int32Prop = 4
            };
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8,
                Int64Prop = 8
            };
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(testEntity);

            var response = await CloudTable.UpdateEntityAsync(updatedTestEntity, ETag.All, TableUpdateMode.Merge);

            var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();
            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Contains(nameof(TestEntity.PartitionKey), entity),
                () => Assert.Contains(nameof(TestEntity.RowKey), entity),
                () => Assert.Contains(nameof(TestEntity.Timestamp), entity),
                () => Assert.Contains("odata.etag", entity),
                () => Assert.Equal("string-prop", entity[nameof(TestEntity.StringProp)]),
                () => Assert.Equal(8, entity[nameof(TestEntity.Int32Prop)]),
                () => Assert.Equal(8L, entity[nameof(TestEntity.Int64Prop)]),

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
        public async Task UpdateEntityMergeAsync_WhenETagsMatch_MergesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string-prop",
                Int32Prop = 4
            };
            await CloudTable.CreateAsync();
            var response = await CloudTable.AddEntityAsync(testEntity);
            var updatedTestEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8,
                Int64Prop = 8
            };

            response = await CloudTable.UpdateEntityAsync(updatedTestEntity, response.Headers.ETag.Value, TableUpdateMode.Merge);

            var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();
            var entity = Assert.Single(entities);

            Assert.Multiple(
                () => Assert.Contains(nameof(TestEntity.PartitionKey), entity),
                () => Assert.Contains(nameof(TestEntity.RowKey), entity),
                () => Assert.Contains(nameof(TestEntity.Timestamp), entity),
                () => Assert.Contains("odata.etag", entity),
                () => Assert.Equal("string-prop", entity[nameof(TestEntity.StringProp)]),
                () => Assert.Equal(8, entity[nameof(TestEntity.Int32Prop)]),
                () => Assert.Equal(8L, entity[nameof(TestEntity.Int64Prop)]),

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
        public async Task UpdateEntityMergeAsync_WhenDynamicEntityHasNullProperties_TheyAreIgnored()
        {
            await CloudTable.CreateAsync();
            var response = await CloudTable.AddEntityAsync(new TableEntity(
                new Dictionary<string, object>
                {
                    { nameof(TestEntity.Int32Prop), 1 }
                }
            )
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            });

            await CloudTable.UpdateEntityAsync(
                new TableEntity(
                    new Dictionary<string, object>
                    {
                        { nameof(TestEntity.Int32Prop), null }
                    }
                )
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key",
                },
                ETag.All,
                TableUpdateMode.Merge
            );

            var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();
            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Contains(nameof(TestEntity.PartitionKey), entity),
                () => Assert.Contains(nameof(TestEntity.RowKey), entity),
                () => Assert.Contains(nameof(TestEntity.Timestamp), entity),
                () => Assert.Contains("odata.etag", entity),
                () => Assert.Equal(1, entity[nameof(TestEntity.Int32Prop)])
            );
        }

        [Fact]
        public async Task UpdateEntityMergeAsync_WhenEntityDoesNotExist_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = new string('t', 1 << 10 + 1),
                RowKey = new string('t', 1 << 10 + 1)
            };
            await CloudTable.CreateAsync();

            await Assertions.JsonResponseThrowsAsync(
                () => CloudTable.UpdateEntityAsync(testEntity, ETag.All, TableUpdateMode.Merge),
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
        public async Task UpdateEntityMergeAsync_WhenETagsMismatch_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            await CloudTable.CreateAsync();
            var response = await CloudTable.AddEntityAsync(testEntity);
            var updatedTestEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            await CloudTable.UpdateEntityAsync(testEntity, ETag.All, TableUpdateMode.Merge);

            await Assertions.JsonResponseThrowsAsync(
                () => CloudTable.UpdateEntityAsync(updatedTestEntity, response.Headers.ETag.Value, TableUpdateMode.Merge),
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
        public async Task UpdateEntityMergeAsync_WhenPartitionKeyIsNull_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = null,
                RowKey = "row-key"
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "PartitionKey",
                () => CloudTable.UpdateEntityAsync(testEntity, ETag.All, TableUpdateMode.Merge)
            );

            Assert.Equal(new ArgumentNullException("PartitionKey").Message, exception.Message);
            Assert.Equal("Azure.Data.Tables", exception.Source);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task UpdateEntityMergeAsync_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            var testEntity = new TableEntity
            {
                PartitionKey = partitionKey,
                RowKey = "row-key"
            };
            await CloudTable.CreateAsync();

            switch (partitionKey)
            {
                case "/":
                case "\\":
                    await Assertions.JsonResponseThrowsAsync(
                        () => CloudTable.UpdateEntityAsync(testEntity, ETag.All, TableUpdateMode.Merge),
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
                    await Assertions.InvalidUrlThrowsAsync(
                        () => CloudTable.UpdateEntityAsync(testEntity, ETag.All, TableUpdateMode.Merge),
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
                    await Assertions.InvalidUrlThrowsAsync(
                        () => CloudTable.UpdateEntityAsync(testEntity, ETag.All, TableUpdateMode.Merge),
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
                    await Assertions.JsonResponseThrowsAsync(
                        () => CloudTable.UpdateEntityAsync(testEntity, ETag.All, TableUpdateMode.Merge),
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
        public async Task UpdateEntityMergeAsync_WhenRowKeyIsNull_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = null
            };
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "RowKey",
                () => CloudTable.UpdateEntityAsync(testEntity, ETag.All, TableUpdateMode.Merge)
            );

            Assert.Equal(new ArgumentNullException("RowKey").Message, exception.Message);
            Assert.Equal("Azure.Data.Tables", exception.Source);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task UpdateEntityMergeAsync_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = rowKey
            };
            await CloudTable.CreateAsync();

            switch (rowKey)
            {
                case "/":
                case "\\":
                    await Assertions.JsonResponseThrowsAsync(
                        () => CloudTable.UpdateEntityAsync(testEntity, ETag.All, TableUpdateMode.Merge),
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
                    await Assertions.InvalidUrlThrowsAsync(
                        () => CloudTable.UpdateEntityAsync(testEntity, ETag.All, TableUpdateMode.Merge),
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
                    await Assertions.InvalidUrlThrowsAsync(
                        () => CloudTable.UpdateEntityAsync(testEntity, ETag.All, TableUpdateMode.Merge),
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
                    await Assertions.JsonResponseThrowsAsync(
                        () => CloudTable.UpdateEntityAsync(testEntity, ETag.All, TableUpdateMode.Merge),
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
        public async Task UpdateEntityMergeAsync_WhenStringPropertyIsInvalid_ThrowsException(string stringPropValue)
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
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(testEntity);

            await Assertions.JsonResponseThrowsAsync(
                () => CloudTable.UpdateEntityAsync(
                    new TestEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key",
                        StringProp = stringPropValue
                    },
                    ETag.All,
                    TableUpdateMode.Merge
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
        public async Task UpdateEntityMergeAsync_WhenBinaryPropertyIsInvalid_ThrowsException(byte[] binaryPropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
            };
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(testEntity);

            await Assertions.JsonResponseThrowsAsync(
                () => CloudTable.UpdateEntityAsync(
                    new TestEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key",
                        BinaryProp = binaryPropValue
                    },
                    ETag.All,
                    TableUpdateMode.Merge
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
        public async Task UpdateEntityMergeAsync_WhenDateTimePropertyIsInvalid_ThrowsException(DateTime dateTimePropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(testEntity);

            await Assertions.JsonResponseThrowsAsync(
                () => CloudTable.UpdateEntityAsync(
                    new TestEntity
                    {
                        PartitionKey = "partition-key",
                        RowKey = "row-key",
                        DateTimeProp = dateTimePropValue
                    },
                    ETag.All,
                    TableUpdateMode.Merge
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