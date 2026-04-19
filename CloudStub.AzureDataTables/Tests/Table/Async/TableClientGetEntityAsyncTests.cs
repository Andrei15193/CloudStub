using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Async
{
    public class TableClientGetEntityAsyncTests : BaseTableCloudStubTests
    {
        [Fact]
        public async Task GetEntityAsync_WhenTableDoesNotExist_ThrowsException()
        {
            await Assertions.JsonResponseThrowsAsync(
                () => CloudTable.GetEntityAsync<TestEntity>("partition-key", "row-key"),
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
        public async Task GetEntityAsync_WhenEntityExists_RetrievesEntity()
        {
            var guidValue = Guid.NewGuid();
            var testEntity = new TestEntity
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
                GuidProp = guidValue,
                DecimalProp = 7
            };
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(testEntity);

            var result = await CloudTable.GetEntityAsync<TableEntity>(testEntity.PartitionKey, testEntity.RowKey);
            var entity = result.Value;
            var response = result.GetRawResponse();

            Assert.Multiple(
                () => Assertions.SuccessfulJsonResponse(
                    response,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.DefaultHeaders(response)
                        {
                            { "ETag", response.Headers.ETag.ToString() }
                        },
                        Content =
                        {
                            { "odata.metadata", $"https://cloudstubdev.table.core.windows.net/$metadata#{CloudTable.Name}/@Element" },
                            { "odata.etag", response.Headers.ETag.ToString() },
                            { "PartitionKey", "partition-key" },
                            { "RowKey", "row-key" },
                            { "Timestamp", entity.Timestamp.Value.ToString(Assertions.DateTimeValueFormat) },
                            { "BinaryProp", Convert.ToBase64String(new byte[1 << 16]) },
                            { "BinaryProp@odata.type", "Edm.Binary" },
                            { "BooleanProp", true },
                            { "StringProp", new string('t', 1 << 15) },
                            { "Int32Prop", 4 },
                            { "Int64Prop", "5" },
                            { "Int64Prop@odata.type", "Edm.Int64" },
                            { "DoubleProp", 6D },
                            { "DateTimeProp", DateTime.MaxValue.ToUniversalTime().ToString(Assertions.DateTimeValueFormat) },
                            { "DateTimeProp@odata.type", "Edm.DateTime" },
                            { "DateTimeOffsetProp", DateTimeOffset.MaxValue.ToUniversalTime().ToString(Assertions.DateTimeValueFormat) },
                            { "DateTimeOffsetProp@odata.type", "Edm.DateTime" },
                            { "GuidProp", guidValue.ToString("D") },
                            { "GuidProp@odata.type", "Edm.Guid" },
                            { "DecimalProp", 7 }
                        }
                    }
                ),
                () => Assert.Equal(testEntity.RowKey, entity[nameof(TestEntity.RowKey)]),
                () => Assert.Equal(testEntity.PartitionKey, entity[nameof(TestEntity.PartitionKey)]),
                () => Assert.Contains(nameof(TestEntity.Timestamp), entity),
                () => Assert.Equal(response.Headers.ETag.Value, entity.ETag),
                () => Assert.Equal(testEntity.BinaryProp, entity[nameof(TestEntity.BinaryProp)]),
                () => Assert.Equal(testEntity.BooleanProp, entity[nameof(TestEntity.BooleanProp)]),
                () => Assert.Equal(testEntity.StringProp, entity[nameof(TestEntity.StringProp)]),
                () => Assert.Equal(testEntity.Int32Prop, entity[nameof(TestEntity.Int32Prop)]),
                () => Assert.Equal(testEntity.Int64Prop, entity[nameof(TestEntity.Int64Prop)]),
                () => Assert.Equal(testEntity.DoubleProp, entity[nameof(TestEntity.DoubleProp)]),
                () => Assert.Equal((DateTimeOffset?)testEntity.DateTimeProp, entity[nameof(TestEntity.DateTimeProp)]),
                () => Assert.Equal(testEntity.DateTimeOffsetProp, entity[nameof(TestEntity.DateTimeOffsetProp)]),
                () => Assert.Equal(testEntity.GuidProp, entity[nameof(TestEntity.GuidProp)]),
                () => Assert.Equal((int?)testEntity.DecimalProp, entity[nameof(TestEntity.DecimalProp)])
            );
        }

        [Fact]
        public async Task GetEntityAsync_WhenEntityExistsAndOnlySomePropertiesAreSelected_RetrievesEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                StringProp = new string('t', 1 << 15),
                Int32Prop = 4
            };
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(testEntity);

            var result = await CloudTable.GetEntityAsync<TableEntity>(testEntity.PartitionKey, testEntity.RowKey, new List<string> { nameof(TestEntity.StringProp) });
            var entity = result.Value;
            var response = result.GetRawResponse();

            Assert.Multiple(
                () => Assertions.SuccessfulJsonResponse(
                    response,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.DefaultHeaders(response)
                        {
                            { "ETag", response.Headers.ETag.ToString() }
                        },
                        Content =
                        {
                            { "odata.metadata", $"https://cloudstubdev.table.core.windows.net/$metadata#{CloudTable.Name}/@Element&$select=StringProp" },
                            { "odata.etag", response.Headers.ETag.ToString() },
                            { "StringProp", new string('t', 1 << 15) }
                        }
                    }
                ),
                () => Assert.DoesNotContain(nameof(TestEntity.PartitionKey), entity),
                () => Assert.DoesNotContain(nameof(TestEntity.RowKey), entity),
                () => Assert.DoesNotContain(nameof(TestEntity.Timestamp), entity),
                () => Assert.Equal(response.Headers.ETag.Value, entity.ETag),
                () => Assert.Equal(testEntity.StringProp, entity[nameof(TestEntity.StringProp)]),
                () => Assert.DoesNotContain(nameof(TestEntity.Int32Prop), entity)
            );
        }

        [Fact]
        public async Task GetEntityAsync_WhenSpecificEntityTypeIsUsed_RetrievesSpecificEntity()
        {
            var guidValue = Guid.NewGuid();
            var testEntity = new TestEntity
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
                GuidProp = guidValue,
                DecimalProp = 7
            };
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(testEntity);

            var result = await CloudTable.GetEntityAsync<TestEntity>(testEntity.PartitionKey, testEntity.RowKey);
            var entity = result.Value;
            var response = result.GetRawResponse();

            Assert.Multiple(
                () => Assertions.SuccessfulJsonResponse(
                    response,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.DefaultHeaders(response)
                        {
                            { "ETag", response.Headers.ETag.ToString() }
                        },
                        Content =
                        {
                            { "odata.metadata", $"https://cloudstubdev.table.core.windows.net/$metadata#{CloudTable.Name}/@Element" },
                            { "odata.etag", response.Headers.ETag.ToString() },
                            { "PartitionKey", "partition-key" },
                            { "RowKey", "row-key" },
                            { "Timestamp", entity.Timestamp.Value.ToString(Assertions.DateTimeValueFormat) },
                            { "BinaryProp", Convert.ToBase64String(new byte[1 << 16]) },
                            { "BinaryProp@odata.type", "Edm.Binary" },
                            { "BooleanProp", true },
                            { "StringProp", new string('t', 1 << 15) },
                            { "Int32Prop", 4 },
                            { "Int64Prop", "5" },
                            { "Int64Prop@odata.type", "Edm.Int64" },
                            { "DoubleProp", 6D },
                            { "DateTimeProp", DateTime.MaxValue.ToUniversalTime().ToString(Assertions.DateTimeValueFormat) },
                            { "DateTimeProp@odata.type", "Edm.DateTime" },
                            { "DateTimeOffsetProp", DateTimeOffset.MaxValue.ToUniversalTime().ToString(Assertions.DateTimeValueFormat) },
                            { "DateTimeOffsetProp@odata.type", "Edm.DateTime" },
                            { "GuidProp", guidValue.ToString("D") },
                            { "GuidProp@odata.type", "Edm.Guid" },
                            { "DecimalProp", 7 }
                        }
                    }
                ),
                () => Assert.Equal(testEntity.RowKey, entity.RowKey),
                () => Assert.Equal(testEntity.PartitionKey, entity.PartitionKey),
                () => Assert.NotNull(entity.Timestamp),
                () => Assert.Equal(response.Headers.ETag.Value, entity.ETag),
                () => Assert.Equal(testEntity.BinaryProp, entity.BinaryProp),
                () => Assert.Equal(testEntity.BooleanProp, entity.BooleanProp),
                () => Assert.Equal(testEntity.StringProp, entity.StringProp),
                () => Assert.Equal(testEntity.Int32Prop, entity.Int32Prop),
                () => Assert.Equal(testEntity.Int64Prop, entity.Int64Prop),
                () => Assert.Equal(testEntity.DoubleProp, entity.DoubleProp),
                () => Assert.Equal(testEntity.DateTimeProp, entity.DateTimeProp),
                () => Assert.Equal(testEntity.DateTimeOffsetProp, entity.DateTimeOffsetProp),
                () => Assert.Equal(testEntity.GuidProp, entity.GuidProp),
                () => Assert.Null(entity.DecimalProp)
            );
        }

        [Fact]
        public async Task GetEntityAsync_WhenSpecificEntityTypeIsUsedAndOnlySelectedFieldsAreSpecified_RetrievesSpecificEntity()
        {
            var testEntity = new TestEntity
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
                GuidProp = Guid.NewGuid(),
                DecimalProp = 7
            };
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(testEntity);

            var result = await CloudTable.GetEntityAsync<TestEntity>(testEntity.PartitionKey, testEntity.RowKey, new List<string> { nameof(TestEntity.StringProp), nameof(TestEntity.DecimalProp) });
            var entity = result.Value;
            var response = result.GetRawResponse();

            Assert.Multiple(
                () => Assertions.SuccessfulJsonResponse(
                    response,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.DefaultHeaders(response)
                        {
                            { "ETag", response.Headers.ETag.ToString() }
                        },
                        Content =
                        {
                            { "odata.metadata", $"https://cloudstubdev.table.core.windows.net/$metadata#{CloudTable.Name}/@Element&$select=StringProp,DecimalProp" },
                            { "odata.etag", response.Headers.ETag.ToString() },
                            { "StringProp", new string('t', 1 << 15) },
                            { "DecimalProp", 7 }
                        }
                    }
                ),
                () => Assert.Null(entity.RowKey),
                () => Assert.Null(entity.PartitionKey),
                () => Assert.Null(entity.Timestamp),
                () => Assert.Equal(response.Headers.ETag.Value, entity.ETag),
                () => Assert.Null(entity.BinaryProp),
                () => Assert.Null(entity.BooleanProp),
                () => Assert.Equal(testEntity.StringProp, entity.StringProp),
                () => Assert.Null(entity.Int32Prop),
                () => Assert.Null(entity.Int64Prop),
                () => Assert.Null(entity.DoubleProp),
                () => Assert.Null(entity.DateTimeProp),
                () => Assert.Null(entity.DateTimeOffsetProp),
                () => Assert.Null(entity.GuidProp),
                () => Assert.Null(entity.DecimalProp)
            );
        }

        [Fact]
        public async Task GetEntityAsync_WhenEntityDoesNotExist_ThrowsException()
        {
            await CloudTable.CreateAsync();

            await Assertions.JsonResponseThrowsAsync(
                () => CloudTable.GetEntityAsync<TableEntity>(new string('t', 1 << 10 + 1), new string('t', 1 << 10 + 1)),
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
        public async Task GetEntityAsync_WhenPartitionKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<NullReferenceException>(() => CloudTable.GetEntityAsync<TableEntity>(null, "row-key"));
            Assert.Equal(new NullReferenceException().Message, exception.Message);
            Assert.Equal("Azure.Data.Tables", exception.Source);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task GetEntityAsync_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            await CloudTable.CreateAsync();

            switch (partitionKey)
            {
                case "/":
                case "\\":
                    await Assertions.JsonResponseThrowsAsync(
                        () => CloudTable.GetEntityAsync<TableEntity>(partitionKey, "row-key"),
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
                        () => CloudTable.GetEntityAsync<TableEntity>(partitionKey, "row-key"),
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
                        () => CloudTable.GetEntityAsync<TableEntity>(partitionKey, "row-key"),
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
                        () => CloudTable.GetEntityAsync<TableEntity>(partitionKey, "row-key"),
                        response => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            StatusCode = HttpStatusCode.NotFound,
                            Headers = new Assertions.DefaultHeaders(response),
                            ErrorCode = "ResourceNotFound",
                            ErrorDescription = "The specified resource does not exist."
                        }
                    );
                    break;
            }
        }

        [Fact]
        public async Task GetEntityAsync_WhenRowKeyIsNull_ThrowsException()
        {
            await CloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<NullReferenceException>(() => CloudTable.GetEntityAsync<TableEntity>("partition-key", null));
            Assert.Equal(new NullReferenceException().Message, exception.Message);
            Assert.Equal("Azure.Data.Tables", exception.Source);
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task GetEntityAsync_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            await CloudTable.CreateAsync();

            switch (rowKey)
            {
                case "/":
                case "\\":
                    await Assertions.JsonResponseThrowsAsync(
                        () => CloudTable.GetEntityAsync<TableEntity>("partition-key", rowKey),
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
                        () => CloudTable.GetEntityAsync<TableEntity>("partition-key", rowKey),
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
                        () => CloudTable.GetEntityAsync<TableEntity>("partition-key", rowKey),
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
                        () => CloudTable.GetEntityAsync<TableEntity>("partition-key", rowKey),
                        response => new Assertions.UnsuccessfulResponseAssertOptions
                        {
                            StatusCode = HttpStatusCode.NotFound,
                            Headers = new Assertions.DefaultHeaders(response),
                            ErrorCode = "ResourceNotFound",
                            ErrorDescription = "The specified resource does not exist."
                        }
                    );
                    break;
            }
        }

        [Fact]
        public async Task GetEntityAsync_WhenUsingProperties_SetsValuesForEach()
        {
            var guid = Guid.NewGuid();
            var now = DateTime.UtcNow;
            await CloudTable.CreateAsync();

            await CloudTable.AddEntityAsync(new TableEntity
            {
                { "PartitionKey", "partition-key" },
                { "RowKey", "row-key" },
                { nameof(TestQueryEntity.StringProp), "test" },
                { nameof(TestQueryEntity.BinaryProp), new byte[] { 1, 2, 3 } },
                { nameof(TestQueryEntity.Int32Prop), 3 },
                { nameof(TestQueryEntity.Int64Prop), 3L },
                { nameof(TestQueryEntity.DoubleProp), 3D },
                { nameof(TestQueryEntity.GuidProp), guid },
                { nameof(TestQueryEntity.BoolProp), true },
                { nameof(TestQueryEntity.DateTimeProp), now },
                { nameof(TestQueryEntity.DateTimeOffsetProp), (DateTimeOffset)now }
            });

            var result = await CloudTable.GetEntityAsync<TestQueryEntity>("partition-key", "row-key");
            var entity = result.Value;

            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
                () => Assert.Equal("test", entity.StringProp),
                () => Assert.Equal(new byte[] { 1, 2, 3 }, entity.BinaryProp),
                () => Assert.Equal(3, entity.Int32Prop),
                () => Assert.Equal(3L, entity.Int64Prop),
                () => Assert.Equal(3D, entity.DoubleProp),
                () => Assert.Equal(guid, entity.GuidProp),
                () => Assert.True(entity.BoolProp),
                () => Assert.Equal(now, entity.DateTimeProp),
                () => Assert.Equal((DateTimeOffset)now, entity.DateTimeOffsetProp)
            );
        }

        [Fact]
        public async Task GetEntityAsync_WhenUsingDifferentCasePropertyNames_DoesNotSetValues()
        {
            await CloudTable.CreateAsync();

            await CloudTable.AddEntityAsync(new TableEntity
            {
                { "PartitionKey", "partition-key" },
                { "RowKey", "row-key" },
                { nameof(TestQueryEntity.StringProp).ToUpperInvariant(), "test" },
                { nameof(TestQueryEntity.BinaryProp).ToUpperInvariant(), new byte[] { 1, 2, 3 } },
                { nameof(TestQueryEntity.Int32Prop).ToUpperInvariant(), 3 },
                { nameof(TestQueryEntity.Int64Prop).ToUpperInvariant(), 3L },
                { nameof(TestQueryEntity.DoubleProp).ToUpperInvariant(), 3D },
                { nameof(TestQueryEntity.GuidProp).ToUpperInvariant(), Guid.NewGuid() },
                { nameof(TestQueryEntity.BoolProp).ToUpperInvariant(), true },
                { nameof(TestQueryEntity.DateTimeProp).ToUpperInvariant(), DateTime.UtcNow },
                { nameof(TestQueryEntity.DateTimeOffsetProp).ToUpperInvariant(), DateTimeOffset.UtcNow }
            });

            var result = await CloudTable.GetEntityAsync<TestQueryEntity>("partition-key", "row-key");
            var entity = result.Value;

            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
                () => Assert.Null(entity.StringProp),
                () => Assert.Null(entity.BinaryProp),
                () => Assert.Null(entity.Int32Prop),
                () => Assert.Null(entity.Int64Prop),
                () => Assert.Null(entity.DoubleProp),
                () => Assert.Null(entity.GuidProp),
                () => Assert.Null(entity.BoolProp),
                () => Assert.Null(entity.DateTimeProp),
                () => Assert.Null(entity.DateTimeOffsetProp)
            );
        }

        [Fact]
        public async Task GetEntityAsync_WhenUsingFields_SetsValuesForEach()
        {
            var guid = Guid.NewGuid();
            var now = DateTime.UtcNow;
            await CloudTable.CreateAsync();

            await CloudTable.AddEntityAsync(new TableEntity
            {
                { "PartitionKey", "partition-key" },
                { "RowKey", "row-key" },
                { nameof(TestQueryEntityFields.StringField), "test" },
                { nameof(TestQueryEntityFields.BinaryField), new byte[] { 1, 2, 3 } },
                { nameof(TestQueryEntityFields.Int32Field), 3 },
                { nameof(TestQueryEntityFields.Int64Field), 3L },
                { nameof(TestQueryEntityFields.DoubleField), 3D },
                { nameof(TestQueryEntityFields.GuidField), guid },
                { nameof(TestQueryEntityFields.BoolField), true },
                { nameof(TestQueryEntityFields.DateTimeField), now },
                { nameof(TestQueryEntityFields.DateTimeOffsetField), (DateTimeOffset)now }
            });

            var result = await CloudTable.GetEntityAsync<TestQueryEntityFields>("partition-key", "row-key");
            var entity = result.Value;

            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
                () => Assert.Equal("test", entity.StringField),
                () => Assert.Equal(new byte[] { 1, 2, 3 }, entity.BinaryField),
                () => Assert.Equal(3, entity.Int32Field),
                () => Assert.Equal(3L, entity.Int64Field),
                () => Assert.Equal(3D, entity.DoubleField),
                () => Assert.Equal(guid, entity.GuidField),
                () => Assert.True(entity.BoolField),
                () => Assert.Equal(now, entity.DateTimeField),
                () => Assert.Equal((DateTimeOffset)now, entity.DateTimeOffsetField)
            );
        }

        [Fact]
        public async Task GetEntityAsync_WhenUsingDifferentCaseFieldNames_DoesNotSetValues()
        {
            await CloudTable.CreateAsync();

            await CloudTable.AddEntityAsync(new TableEntity
            {
                { "PartitionKey", "partition-key" },
                { "RowKey", "row-key" },
                { nameof(TestQueryEntityFields.StringField).ToUpperInvariant(), "test" },
                { nameof(TestQueryEntityFields.BinaryField).ToUpperInvariant(), new byte[] { 1, 2, 3 } },
                { nameof(TestQueryEntityFields.Int32Field).ToUpperInvariant(), 3 },
                { nameof(TestQueryEntityFields.Int64Field).ToUpperInvariant(), 3L },
                { nameof(TestQueryEntityFields.DoubleField).ToUpperInvariant(), 3D },
                { nameof(TestQueryEntityFields.GuidField).ToUpperInvariant(), Guid.NewGuid() },
                { nameof(TestQueryEntityFields.BoolField).ToUpperInvariant(), true },
                { nameof(TestQueryEntityFields.DateTimeField).ToUpperInvariant(), DateTime.UtcNow },
                { nameof(TestQueryEntityFields.DateTimeOffsetField).ToUpperInvariant(), DateTimeOffset.UtcNow }
            });

            var result = await CloudTable.GetEntityAsync<TestQueryEntityFields>("partition-key", "row-key");
            var entity = result.Value;

            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),
                () => Assert.Null(entity.StringField),
                () => Assert.Null(entity.BinaryField),
                () => Assert.Null(entity.Int32Field),
                () => Assert.Null(entity.Int64Field),
                () => Assert.Null(entity.DoubleField),
                () => Assert.Null(entity.GuidField),
                () => Assert.Null(entity.BoolField),
                () => Assert.Null(entity.DateTimeField),
                () => Assert.Null(entity.DateTimeOffsetField)
            );
        }

        [Fact]
        public async Task GetEntityAsync_WhenUsingPropertiesAndFieldsWithDifferentAccessModifiers_OnlySetsPublicMembers()
        {
            await CloudTable.CreateAsync();

            await CloudTable.AddEntityAsync(new TableEntity
            {
                { "PartitionKey", "partition-key" },
                { "RowKey", "row-key" },

                { "PublicProp", "public" },
                { "ProtectedProp", "protected" },
                { "InternalProp", "internal" },
                { "PrivateProp", "private" },
                { "ProtectedInternalProp", "protected internal" },
                { "PrivateProtectedProp", "private protected" },

                { "PublicField", "public" },
                { "ProtectedField", "protected" },
                { "InternalField", "internal" },
                { "PrivateField", "private" },
                { "ProtectedInternalField", "protected internal" },
                { "PrivateProtectedField", "private protected" }
            });

            var result = await CloudTable.GetEntityAsync<TestQueryEntityAccessModifier>("partition-key", "row-key");
            var entity = result.Value;

            Assert.Multiple(
                () => Assert.Equal("partition-key", entity.PartitionKey),
                () => Assert.Equal("row-key", entity.RowKey),

                () => Assert.Equal("public", typeof(TestQueryEntityAccessModifier).GetProperty("PublicProp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetProperty("ProtectedProp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetProperty("InternalProp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetProperty("PrivateProp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetProperty("ProtectedInternalProp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetProperty("PrivateProtectedProp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty).GetValue(entity)),

                () => Assert.Equal("public", typeof(TestQueryEntityAccessModifier).GetField("PublicField", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetField("ProtectedField", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetField("InternalField", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetField("PrivateField", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetField("ProtectedInternalField", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetField("PrivateProtectedField", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField).GetValue(entity))
            );
        }

        [Theory]
        [ClassData(typeof(TableDeserializationTestData))]
        public async Task GetEntityAsync_WhenDeserializingProperty_MayParseOrThrowException(string propertyName, object value, object expectedResult)
        {
            await CloudTable.CreateAsync();
            await CloudTable.AddEntityAsync(new TableEntity
            {
                { "PartitionKey", "partition-key" },
                { "RowKey", "row-key" },
                { propertyName, value },
            });

            if (expectedResult is Exception expectedException)
            {
                var exception = await Assert.ThrowsAsync(expectedResult.GetType(), () => CloudTable.GetEntityAsync<TestQueryEntity>("partition-key", "row-key"));
                Assert.Equal(expectedException.Message, exception.Message);
                Assert.Contains(exception.Source, new[] { "System.Private.CoreLib", "Azure.Data.Tables" });
            }
            else
            {
                var result = await CloudTable.GetEntityAsync<TestQueryEntity>("partition-key", "row-key");
                var entity = result.Value;
                var actualResult = typeof(TestQueryEntity)
                    .GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetProperty)
                    .GetValue(entity);
                Assert.Equal(expectedResult, actualResult);
            }
        }
    }
}