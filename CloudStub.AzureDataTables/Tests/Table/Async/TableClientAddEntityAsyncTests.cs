using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Async
{
    public class TableClientAddEntityAsyncTests : BaseTableCloudStubTests
    {
        [Fact]
        public async Task AddEntityAsync_WhenTableDoesNotExist_ThrowsException()
        {
            await Assertions.JsonResponseThrowsAsync(
                () => TableClient.AddEntityAsync(new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key"
                }),
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
        public async Task AddEntityAsync_WhenEntityIsNull_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<ArgumentNullException>("entity", () => TableClient.AddEntityAsync<ITableEntity>(null));

            Assert.Multiple(
                () => Assert.Equal(new ArgumentNullException("entity").Message, exception.Message),
                () => Assert.Equal("Azure.Data.Tables", exception.Source)
            );
        }

        [Fact]
        public async Task AddEntityAsync_WhenEntityDoesNotExist_InsertsEntity()
        {
            await TableClient.CreateAsync();

            var response = await TableClient.AddEntityAsync(new TableEntity
            {
                PartitionKey = "partition-key:1",
                RowKey = "row-key:1"
            });

            Assertions.EmptyResponse(response, new Assertions.SuccessfulResponseAssertOptions
            {
                StatusCode = HttpStatusCode.NoContent,
                Headers = new Assertions.NoContentHeaders(response)
                {
                    { "ETag", response.Headers.ETag.ToString() },
                    { "Location", $"{TableClient.Uri}(PartitionKey='{Uri.EscapeDataString("partition-key:1")}',RowKey='{Uri.EscapeDataString("row-key:1")}')" },
                    { "Preference-Applied", "return-no-content" },
                    { "DataServiceId", $"{TableClient.Uri}(PartitionKey='{Uri.EscapeDataString("partition-key:1")}',RowKey='{Uri.EscapeDataString("row-key:1")}')" }
                }
            });
        }

        [Fact]
        public async Task AddEntityAsync_WhenEntityAlreadyExists_ThrowsException()
        {
            await TableClient.CreateAsync();
            await TableClient.AddEntityAsync(new TableEntity("partition-key:1", "row-key:1"));

            await Assertions.JsonResponseThrowsAsync(
                () => TableClient.AddEntityAsync(new TableEntity("partition-key:1", "row-key:1")),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.Conflict,
                    ErrorCode = "EntityAlreadyExists",
                    ErrorDescription = "The specified entity already exists.",
                    Headers = new Assertions.DefaultHeaders(rawResponse)
                    {
                        { "Preference-Applied", "return-no-content" }
                    }
                }
            );
        }

        [Fact]
        public async Task AddEntityAsync_WhenEntityHasOtherProperties_InsertsEntity()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key:1",
                RowKey = "row-key:1",
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
            await TableClient.CreateAsync();

            var response = await TableClient.AddEntityAsync(testEntity);

            var entities = await TableClient.QueryAsync<TableEntity>().ToListAsync();
            var entity = Assert.Single(entities);

            var expectedProps = new Dictionary<string, object>
            {
                { nameof(TestEntity.PartitionKey), testEntity.PartitionKey },
                { nameof(TestEntity.RowKey), testEntity.RowKey },
                { nameof(TestEntity.Timestamp), DateTimeOffset.ParseExact(Uri.UnescapeDataString(response.Headers.ETag.ToString()), Assertions.ETagDateTimeFormat, CultureInfo.InvariantCulture) },
                { "odata.etag", response.Headers.Single(header => header.Name == "ETag").Value },
                { nameof(TestEntity.BinaryProp), testEntity.BinaryProp },
                { nameof(TestEntity.BooleanProp), testEntity.BooleanProp },
                { nameof(TestEntity.StringProp), testEntity.StringProp },
                { nameof(TestEntity.Int32Prop), testEntity.Int32Prop },
                { nameof(TestEntity.Int64Prop), testEntity.Int64Prop },
                { nameof(TestEntity.DoubleProp), testEntity.DoubleProp },
                { nameof(TestEntity.DateTimeProp), (DateTimeOffset?)testEntity.DateTimeProp },
                { nameof(TestEntity.GuidProp), testEntity.GuidProp },
                { nameof(TestEntity.DecimalProp), (int)testEntity.DecimalProp }
            };

            Assert.Equal(expectedProps.Count, entity.Count);
            Assert.Multiple(
                expectedProps
                    .Select(expectedProp => new Action(() =>
                    {
                        Assert.True(entity.TryGetValue(expectedProp.Key, out var value), $"Expected property '{expectedProp.Key}' was not found in the actual entity.");
                        Assert.Equal(expectedProp.Value, value);
                    }))
                    .ToArray()
            );
        }

        [Fact]
        public async Task AddEntityAsync_WhenSomePropertiesAreNull_TheyAreIgnored()
        {
            await TableClient.CreateAsync();

            await TableClient.AddEntityAsync(new TableEntity
            {
                { nameof(TestEntity.PartitionKey), "partition-key:1" },
                { nameof(TestEntity.RowKey), "row-key:1" },
                { nameof(TestEntity.Int32Prop), (int?)null }
            });

            var entities = await TableClient.QueryAsync<TableEntity>().ToListAsync();
            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.True(entity.ContainsKey(nameof(TestEntity.PartitionKey))),
                () => Assert.True(entity.ContainsKey(nameof(TestEntity.RowKey))),
                () => Assert.True(entity.ContainsKey(nameof(TestEntity.Timestamp))),
                () => Assert.True(entity.ContainsKey("odata.etag")),
                () => Assert.False(entity.ContainsKey(nameof(TestEntity.Int32Prop)))
            );
        }

        [Fact]
        public async Task AddEntityAsync_WhenPartitionKeyIsNull_ThrowsException()
        {
            await TableClient.CreateAsync();

            var exception = await Assert.ThrowsAsync<ArgumentNullException>("PartitionKey", () => TableClient.AddEntityAsync(new TableEntity
            {
                PartitionKey = null,
                RowKey = "row-key"
            }));

            Assert.Multiple(
                () => Assert.Equal(new ArgumentNullException("PartitionKey").Message, exception.Message),
                () => Assert.Equal("PartitionKey", exception.ParamName),
                () => Assert.Equal("Azure.Data.Tables", exception.Source),
                () => Assert.Empty(exception.Data),
                () => Assert.Null(exception.InnerException),
                () => Assert.Null(exception.HelpLink),
                () => Assert.Equal(-2147467261, exception.HResult)
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task AddEntityAsync_WhenPartitionKeyIsInvalid_ThrowsException(string partitionKey)
        {
            await TableClient.CreateAsync();

            var exception = await Assertions.JsonResponseThrowsAsync(
                () => TableClient.AddEntityAsync(new TableEntity
                {
                    PartitionKey = partitionKey,
                    RowKey = "row-key"
                }),
                rawResponse =>
                {
                    var headers = new Assertions.DefaultHeaders(rawResponse);
                    headers.Remove("Cache-Control");

                    return new Assertions.UnsuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.BadRequest,
                        ErrorCode = "OutOfRangeInput",
                        ErrorDescription = $"The 'PartitionKey' parameter of value '{partitionKey}' is out of range.",
                        Headers = headers
                    };
                }
            );
        }

        [Fact]
        public async Task AddEntityAsync_WhenPartitionKeyExceedsLimit_ThrowsException()
        {
            await TableClient.CreateAsync();

            await Assertions.JsonResponseThrowsAsync(
                () => TableClient.AddEntityAsync(new TableEntity
                {
                    PartitionKey = new string('t', 1 << 10 + 1),
                    RowKey = "row-key"
                }),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    Headers = new Assertions.DefaultHeaders(rawResponse)
                    {
                        { "Preference-Applied", "return-no-content" }
                    }
                }
            );
        }

        [Fact]
        public async Task AddEntityAsync_WhenRowKeyIsNull_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<ArgumentNullException>("RowKey", () => TableClient.AddEntityAsync(new TableEntity
            {
                PartitionKey = "partition-key",
                RowKey = null
            }));

            Assert.Multiple(
                () => Assert.Equal(new ArgumentNullException("RowKey").Message, exception.Message),
                () => Assert.Equal("RowKey", exception.ParamName),
                () => Assert.Equal("Azure.Data.Tables", exception.Source),
                () => Assert.Empty(exception.Data),
                () => Assert.Null(exception.InnerException),
                () => Assert.Null(exception.HelpLink),
                () => Assert.Equal(-2147467261, exception.HResult)
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidKeyTestData), MemberType = typeof(TableOperationTestData))]
        public async Task AddEntityAsync_WhenRowKeyIsInvalid_ThrowsException(string rowKey)
        {
            await TableClient.CreateAsync();

            var exception = await Assertions.JsonResponseThrowsAsync(
                () => TableClient.AddEntityAsync(new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = rowKey
                }),
                rawResponse =>
                {
                    var headers = new Assertions.DefaultHeaders(rawResponse);
                    headers.Remove("Cache-Control");

                    return new Assertions.UnsuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.BadRequest,
                        ErrorCode = "OutOfRangeInput",
                        ErrorDescription = $"The 'RowKey' parameter of value '{rowKey}' is out of range.",
                        Headers = headers
                    };
                }
            );
        }

        [Fact]
        public async Task AddEntityAsync_WhenRowKeyExceedsLimit_ThrowsException()
        {
            await TableClient.CreateAsync();

            await Assertions.JsonResponseThrowsAsync(
                () => TableClient.AddEntityAsync(new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = new string('t', 1 << 10 + 1)
                }),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    Headers = new Assertions.DefaultHeaders(rawResponse)
                    {
                        { "Preference-Applied", "return-no-content" }
                    }
                }
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidStringData), MemberType = typeof(TableOperationTestData))]
        public async Task AddEntityAsync_WhenStringPropertyIsInvalid_ThrowsException(string stringPropValue)
        {
            await TableClient.CreateAsync();

            var exception = await Assertions.JsonResponseThrowsAsync(
                () => TableClient.AddEntityAsync(new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key",
                    StringProp = stringPropValue
                }),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    Headers = new Assertions.DefaultHeaders(rawResponse)
                    {
                        { "Preference-Applied", "return-no-content" }
                    }
                }
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidBinaryData), MemberType = typeof(TableOperationTestData))]
        public async Task AddEntityAsync_WhenBinaryPropertyIsInvalid_ThrowsException(byte[] binaryPropValue)
        {
            await TableClient.CreateAsync();

            var exception = await Assertions.JsonResponseThrowsAsync(
                () => TableClient.AddEntityAsync(new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key",
                    BinaryProp = binaryPropValue
                }),
                rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorDescription = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    Headers = new Assertions.DefaultHeaders(rawResponse)
                    {
                        { "Preference-Applied", "return-no-content" }
                    }
                }
            );
        }

        [Theory, MemberData(nameof(TableOperationTestData.InvalidDateTimeData), MemberType = typeof(TableOperationTestData))]
        public async Task AddEntityAsync_WhenDateTimePropertyIsInvalid_ThrowsException(DateTime dateTimePropValue)
        {
            await TableClient.CreateAsync();

            var exception = await Assertions.JsonResponseThrowsAsync(
                () => TableClient.AddEntityAsync(new TestEntity
                {
                    PartitionKey = "partition-key",
                    RowKey = "row-key",
                    DateTimeProp = dateTimePropValue
                }),
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

        [Fact]
        public async Task AddEntityAsync_WhenDateTimePropertyIsNotUniversal_ThrowsException()
        {
            await TableClient.CreateAsync();

            var now = DateTime.Now;
            var exception = await Assert.ThrowsAsync<NotSupportedException>(() => TableClient.AddEntityAsync(new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                DateTimeProp = now,
                DateTimeOffsetProp = DateTimeOffset.Now
            }));

            Assert.Multiple(
                () => Assert.Equal($"DateTime {now} has a Kind of {now.Kind}. Azure SDK requires it to be UTC. You can call DateTime.SpecifyKind to change Kind property value to DateTimeKind.Utc.", exception.Message),
                () => Assert.Equal("Azure.Data.Tables", exception.Source),
                () => Assert.Empty(exception.Data),
                () => Assert.Null(exception.InnerException),
                () => Assert.Null(exception.HelpLink),
                () => Assert.Equal(-2146233067, exception.HResult)
            );
        }

        [Fact]
        public async Task AddEntityAsync_WhenDateTimeOffsetPropertyIsLocal_InsertsEntity()
        {
            await TableClient.CreateAsync();

            var now = DateTimeOffset.Now;
            var response = await TableClient.AddEntityAsync(new TestEntity
            {
                PartitionKey = "partition-key:1",
                RowKey = "row-key:1",
                DateTimeOffsetProp = now
            });

            var entities = await TableClient.QueryAsync<TableEntity>().ToListAsync();
            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Equal(now.UtcDateTime, entity.GetDateTimeOffset(nameof(TestEntity.DateTimeOffsetProp))),
                () => Assert.Equal(TimeSpan.Zero, entity.GetDateTimeOffset(nameof(TestEntity.DateTimeOffsetProp))?.Offset),
                () => Assertions.EmptyResponse(response, new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NoContent,
                    Headers = new Assertions.NoContentHeaders(response)
                    {
                        { "ETag", response.Headers.ETag.ToString() },
                        { "Location", $"{TableClient.Uri}(PartitionKey='{Uri.EscapeDataString("partition-key:1")}',RowKey='{Uri.EscapeDataString("row-key:1")}')" },
                        { "Preference-Applied", "return-no-content" },
                        { "DataServiceId", $"{TableClient.Uri}(PartitionKey='{Uri.EscapeDataString("partition-key:1")}',RowKey='{Uri.EscapeDataString("row-key:1")}')" }
                    }
                })
            );
        }
    }
}