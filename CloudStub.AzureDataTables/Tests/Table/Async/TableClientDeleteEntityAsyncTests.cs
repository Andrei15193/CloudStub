using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Async
{
    public class TableClientDeleteEntityAsyncTests : BaseTableCloudStubTests
    {
        [Fact]
        public async Task DeleteEntityAsync_WhenTableDoesNotExist_ReturnsUnsuccessfulResponse()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };

            var response = await TableClient.DeleteEntityAsync(testEntity);

            Assertions.UnsuccessfulJsonResponse(
                response,
                new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NotFound,
                    Headers = new Assertions.DeletedHeaders(response),
                    ErrorCode = "TableNotFound",
                    ErrorDescription = "The table specified does not exist."
                }
            );
        }

        [Fact]
        public async Task DeleteEntityAsync_WhenEntityIsNull_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<ArgumentNullException>("entity", () => TableClient.DeleteEntityAsync(null));
            Assert.Equal(new ArgumentNullException("entity").Message, exception.Message);
        }

        [Fact]
        public async Task DeleteEntityAsync_WhenEntityDoesNotExist_ReturnsUnsuccessfulResponse()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = new string('t', 1 << 10 + 1),
                RowKey = new string('t', 1 << 10 + 1)
            };
            await TableClient.CreateAsync();

            var response = await TableClient.DeleteEntityAsync(testEntity);

            Assertions.UnsuccessfulJsonResponse(
                response,
                new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NotFound,
                    Headers = new Assertions.DefaultHeaders(response),
                    ErrorCode = "ResourceNotFound",
                    ErrorDescription = "The specified resource does not exist."
                }
            );
        }

        [Fact]
        public async Task DeleteEntityAsync_WhenETagsIsUnspecified_DeletesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string-prop",
                Int32Prop = 4
            };
            var testEntityToRemove = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8,
                Int64Prop = 8
            };
            await TableClient.CreateAsync();
            await TableClient.AddEntityAsync(testEntity);

            var response = await TableClient.DeleteEntityAsync(testEntityToRemove);

            Assertions.EmptyResponse(
                response,
                new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NoContent,
                    Headers = new Assertions.NoContentHeaders(response)
                }
            );
        }

        [Fact]
        public async Task DeleteEntityAsync_WhenETagMatches_DeletesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = "string-prop",
                Int32Prop = 4
            };
            await TableClient.CreateAsync();
            var response = await TableClient.AddEntityAsync(testEntity);
            Assert.NotEqual(ETag.All, response.Headers.ETag.Value);

            var testEntityToRemove = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8,
                Int64Prop = 8
            };

            response = await TableClient.DeleteEntityAsync(testEntityToRemove, response.Headers.ETag.Value);

            Assertions.EmptyResponse(
                response,
                new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NoContent,
                    Headers = new Assertions.NoContentHeaders(response)
                }
            );
        }

        [Fact]
        public async Task DeleteEntityAsync_WhenETagMismatches_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            await TableClient.CreateAsync();
            var response = await TableClient.AddEntityAsync(testEntity);
            Assert.NotEqual(ETag.All, response.Headers.ETag.Value);

            var testEntityToRemove = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Int32Prop = 8,
                Int64Prop = 8
            };
            await TableClient.UpdateEntityAsync(
                new TestEntity
                {
                    PartitionKey = testEntity.PartitionKey,
                    RowKey = testEntity.RowKey,
                    Int32Prop = 16,
                    Int64Prop = 16
                },
                response.Headers.ETag.Value,
                TableUpdateMode.Replace
            );

            await Assertions.JsonResponseThrowsAsync(
                () => TableClient.DeleteEntityAsync(testEntityToRemove, response.Headers.ETag.Value),
                deleteEntityResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.PreconditionFailed,
                    Headers = new Assertions.DefaultHeaders(deleteEntityResponse),
                    ErrorCode = "UpdateConditionNotSatisfied",
                    ErrorDescription = "The update condition specified in the request was not satisfied."
                }
            );
            Assert.Single(await TableClient.QueryAsync<TableEntity>().ToListAsync());
        }

        [Fact]
        public async Task DeleteEntityAsync_WhenPartitionKeyIsNull_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = null,
                RowKey = "row-key"
            };
            await TableClient.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "partitionKey",
                () => TableClient.DeleteEntityAsync(testEntity)
            );

            Assert.Equal(new ArgumentNullException("partitionKey").Message, exception.Message);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task DeleteEntityAsync_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            var testEntity = new TableEntity
            {
                PartitionKey = partitionKey,
                RowKey = "row-key"
            };
            await TableClient.CreateAsync();

            switch (partitionKey)
            {
                case "/":
                case "\\":
                    await Assertions.JsonResponseThrowsAsync(
                        () => TableClient.DeleteEntityAsync(testEntity),
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
                        () => TableClient.DeleteEntityAsync(testEntity),
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
                        () => TableClient.DeleteEntityAsync(testEntity),
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
                        () => TableClient.DeleteEntityAsync(testEntity),
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
        public async Task DeleteEntityAsync_WhenRowKeyIsNull_ThrowsException()
        {
            var testEntity = new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = null
            };
            await TableClient.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>(
                "rowKey",
                () => TableClient.DeleteEntityAsync(testEntity)
            );

            Assert.Equal(new ArgumentNullException("rowKey").Message, exception.Message);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task DeleteEntityAsync_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = rowKey
            };
            await TableClient.CreateAsync();

            switch (rowKey)
            {
                case "/":
                case "\\":
                    await Assertions.JsonResponseThrowsAsync(
                        () => TableClient.DeleteEntityAsync(testEntity),
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
                        () => TableClient.DeleteEntityAsync(testEntity),
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
                        () => TableClient.DeleteEntityAsync(testEntity),
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
                        () => TableClient.DeleteEntityAsync(testEntity),
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
        public async Task DeleteEntityAsync_WhenStringPropertyIsInvalid_DeletesEntity(string stringPropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            var testEntityToRemove = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                StringProp = stringPropValue
            };
            await TableClient.CreateAsync();
            await TableClient.AddEntityAsync(testEntity);

            await TableClient.DeleteEntityAsync(testEntityToRemove);

            Assert.Empty(await TableClient.QueryAsync<TableEntity>().ToListAsync());
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidBinaryData), MemberType = typeof(TableOperationTestData))]
        public async Task DeleteEntityAsync_WhenBinaryPropertyIsInvalid_DeletesEntity(byte[] binaryPropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            var testEntityToRemove = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                BinaryProp = binaryPropValue
            };
            await TableClient.CreateAsync();
            await TableClient.AddEntityAsync(testEntity);

            await TableClient.DeleteEntityAsync(testEntityToRemove);

            Assert.Empty(await TableClient.QueryAsync<TableEntity>().ToListAsync());
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidDateTimeData), MemberType = typeof(TableOperationTestData))]
        public async Task DeleteEntityAsync_WhenDateTimePropertyIsInvalid_DeletesEntity(DateTime dateTimePropValue)
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            };
            var testEntityToRemove = new TestEntity
            {
                PartitionKey = testEntity.PartitionKey,
                RowKey = testEntity.RowKey,
                DateTimeProp = dateTimePropValue
            };
            await TableClient.CreateAsync();
            await TableClient.AddEntityAsync(testEntity);

            await TableClient.DeleteEntityAsync(testEntityToRemove);

            Assert.Empty(await TableClient.QueryAsync<TableEntity>().ToListAsync());
        }
    }
}