using System.Net;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;

namespace CloudStub.Azure.Data.Tables.Tests.TableService.Async;

public class StubCloudTableTests : BaseTableCloudStubTests
{
    [Fact(Skip = "Include this for CloudTableStub tests")]
    public async Task CreateAsync_WhenTablePreviouslyContainedEntities_IsEmpty()
    {
        await CloudTable.CreateAsync();
        await CloudTable.AddEntityAsync(new TableEntity("partition-key", "row-key"));
        await CloudTable.DeleteAsync();

        if (!TestRunContext.InMemory)
            await Task.Delay(TimeSpan.FromMinutes(1));
        await CloudTable.CreateAsync();

        var entities = await CloudTable.QueryAsync<TableEntity>().ToListAsync();

        Assert.Empty(entities);
    }

    [Fact]
    public void AccountName_GetsTheSameNameWhichWasProvided()
    {
        Assert.Equal(TableAccountName, TableServiceClient.AccountName);
    }

    [Fact]
    public async Task CreateTableAsync_WhenTableDoesNotExist_ReturnsTableItem()
    {
        var response = await TableServiceClient.CreateTableAsync(TestTableName);

        var tableItem = response.Value;
        var rawResponse = response.GetRawResponse();

        Assert.Multiple(
            () => Assert.False(rawResponse.IsError),
            () => Assertions.SuccessfulJsonResponse(
                rawResponse,
                new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.Created,
                    Headers = new Assertions.DefaultHeaders(rawResponse)
                    {
                        { "Location", $"{TableServiceClient.Uri}Tables('{TestTableName}')" }
                    },
                    Content =
                    {
                        { "odata.metadata", $"{TableServiceClient.Uri}$metadata#Tables/@Element" },
                        { "TableName", TestTableName }
                    },
                }
            ),
            () =>
            {
                Assert.NotNull(tableItem);
                Assert.Equal(TestTableName, tableItem.Name);
            }
        );
    }

    [Fact]
    public async Task CreateTableAsync_WhenTableExists_ThrowsException()
    {
        await TableServiceClient.CreateTableAsync(TestTableName);

        await Assertions.JsonResponseThrowsAsync(
            () => TableServiceClient.CreateTableAsync(TestTableName),
            rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
            {
                StatusCode = HttpStatusCode.Conflict,
                Headers = new Assertions.DefaultHeaders(rawResponse),
                ErrorCode = "TableAlreadyExists",
                ErrorDescription = "The table specified already exists."
            }
        );
    }

    [Theory]
    [InlineData("invalid_table_name")]
    [InlineData("1nvalid")]
    public async Task CreateTableAsync_WhenTableNameIsInvalid_ThrowsException(string tableName)
    {
        await Assertions.JsonResponseThrowsAsync(
            () => TableServiceClient.CreateTableAsync(tableName),
            rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
            {
                StatusCode = HttpStatusCode.BadRequest,
                Headers = new Assertions.DefaultHeaders(rawResponse),
                ErrorCode = "InvalidResourceName",
                ErrorDescription = "The specifed resource name contains invalid characters."
            }
        );
    }

    [Theory]
    [InlineData("tables")]
    public async Task CreateTableAsync_WhenTableNameIsReserved_ThrowsException(string tableName)
    {
        await Assertions.JsonResponseThrowsAsync(
            () => TableServiceClient.CreateTableAsync(tableName),
            rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
            {
                StatusCode = HttpStatusCode.BadRequest,
                Headers = new Assertions.DefaultHeaders(rawResponse),
                ErrorCode = "InvalidInput",
                ErrorDescription = "One of the request inputs is not valid."
            }
        );
    }

    [Theory]
    [InlineData("t")]
    [InlineData("tt")]
    [InlineData("testTableNameHavingALengthOf63CharactersSomeOfThemAreJustExtra1s")]
    public async Task CreateTableAsync_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
    {
        await Assertions.JsonResponseThrowsAsync(
            () => TableServiceClient.CreateTableAsync(tableName),
            rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
            {
                StatusCode = HttpStatusCode.BadRequest,
                Headers = new Assertions.DefaultHeaders(rawResponse),
                ErrorCode = "OutOfRangeInput",
                ErrorDescription = "The specified resource name length is not within the permissible limits."
            }
        );
    }

    [Fact]
    public async Task CreateTableIfNotExistsAsync_WhenTableDoesNotExist_ReturnsTableItemWithNoContentResponse()
    {
        var response = await TableServiceClient.CreateTableIfNotExistsAsync(TestTableName);
        var tableItem = response.Value;
        var rawResponse = response.GetRawResponse();

        Assert.Multiple(
            () => Assert.False(rawResponse.IsError),
            () => Assertions.EmptyResponse(
                rawResponse,
                new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NoContent,
                    Headers = new Assertions.NoContentHeaders(rawResponse)
                    {
                        { "Location", $"{TableServiceClient.Uri}Tables('{TestTableName}')" },
                        { "Preference-Applied", "return-no-content" },
                        { "DataServiceId", $"{TableServiceClient.Uri}Tables('{TestTableName}')"}
                    }
                }
            ),
            () =>
            {
                Assert.NotNull(tableItem);
                Assert.Equal(TestTableName, tableItem.Name);
            }
        );
    }

    [Fact]
    public async Task CreateTableIfNotExistsAsync_WhenTableExists_ReturnsTableItemWithConflictResponse()
    {
        TableServiceClient.CreateTable(TestTableName);

        var response = await TableServiceClient.CreateTableIfNotExistsAsync(TestTableName);
        var tableItem = response.Value;
        var rawResponse = response.GetRawResponse();

        Assert.Multiple(
            () => Assert.False(rawResponse.IsError),
            () => Assertions.UnsuccessfulJsonResponse(
                rawResponse,
                new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.Conflict,
                    Headers = new Assertions.DefaultHeaders(rawResponse)
                    {
                        { "Preference-Applied", "return-no-content" }
                    },
                    ErrorCode = "TableAlreadyExists",
                    ErrorDescription = "The table specified already exists."
                }
            ),
            () =>
            {
                Assert.NotNull(tableItem);
                Assert.Equal(TestTableName, tableItem.Name);
            }
        );
    }

    [Theory]
    [InlineData("invalid_table_name")]
    [InlineData("1nvalid")]
    public async Task CreateTableIfNotExistsAsync_WhenTableNameIsInvalid_ThrowsException(string tableName)
    {
        await Assertions.JsonResponseThrowsAsync(
            () => TableServiceClient.CreateTableIfNotExistsAsync(tableName),
            rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
            {
                StatusCode = HttpStatusCode.BadRequest,
                Headers = new Assertions.DefaultHeaders(rawResponse)
                {
                    { "Preference-Applied", "return-no-content" }
                },
                ErrorCode = "InvalidResourceName",
                ErrorDescription = "The specifed resource name contains invalid characters."
            }
        );
    }

    [Theory]
    [InlineData("tables")]
    public async Task CreateTableIfNotExistsAsync_WhenTableNameIsReserved_ThrowsException(string tableName)
    {
        await Assertions.JsonResponseThrowsAsync(
            () => TableServiceClient.CreateTableIfNotExistsAsync(tableName),
            rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
            {
                StatusCode = HttpStatusCode.BadRequest,
                Headers = new Assertions.DefaultHeaders(rawResponse)
                {
                    { "Preference-Applied", "return-no-content" }
                },
                ErrorCode = "InvalidInput",
                ErrorDescription = "One of the request inputs is not valid."
            }
        );
    }

    [Theory]
    [InlineData("t")]
    [InlineData("tt")]
    [InlineData("testTableNameHavingALengthOf63CharactersSomeOfThemAreJustExtra1s")]
    public async Task CreateTableIfNotExistsAsync_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
    {
        await Assertions.JsonResponseThrowsAsync(
            () => TableServiceClient.CreateTableIfNotExistsAsync(tableName),
            rawResponse => new Assertions.UnsuccessfulResponseAssertOptions
            {
                StatusCode = HttpStatusCode.BadRequest,
                Headers = new Assertions.DefaultHeaders(rawResponse)
                {
                    { "Preference-Applied", "return-no-content" }
                },
                ErrorCode = "OutOfRangeInput",
                ErrorDescription = "The specified resource name length is not within the permissible limits."
            }
        );
    }

    [Fact]
    public async Task DeleteTableAsync_WhenTableDoesNotExist_ReturnsSuccessfulResponse()
    {
        var rawResponse = await TableServiceClient.DeleteTableAsync(TestTableName);

        Assert.Multiple(
            () => Assert.False(rawResponse.IsError),
            () => Assertions.UnsuccessfulJsonResponse(
                rawResponse,
                new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NotFound,
                    Headers = new Assertions.DefaultHeaders(rawResponse),
                    ErrorCode = "ResourceNotFound",
                    ErrorDescription = "The specified resource does not exist."
                }
            )
        );
    }

    [Fact]
    public async Task DeleteTableAsync_WhenTableExists_ReturnsSuccessfulResponse()
    {
        await TableServiceClient.CreateTableAsync(TestTableName);

        var rawResponse = await TableServiceClient.DeleteTableAsync(TestTableName);

        Assert.Multiple(
            () => Assert.False(rawResponse.IsError),
            () => Assertions.EmptyResponse(
                rawResponse,
                new Assertions.ResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NoContent,
                    Headers = new Assertions.NoContentHeaders(rawResponse)
                }
            )
        );
    }

    [Theory]
    [InlineData("invalid_table_name")]
    [InlineData("1nvalid")]
    public async Task DeleteTableAsync_WhenTableNameIsInvalid_ReturnsUnsuccessfulResponse(string tableName)
    {
        var rawResponse = await TableServiceClient.DeleteTableAsync(tableName);

        Assert.Multiple(
            () => Assert.False(rawResponse.IsError),
            () => Assertions.UnsuccessfulJsonResponse(
                rawResponse,
                new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NotFound,
                    Headers = new Assertions.DefaultHeaders(rawResponse),
                    ErrorCode = "ResourceNotFound",
                    ErrorDescription = "The specified resource does not exist."
                }
            )
        );
    }

    [Theory]
    [InlineData("tables")]
    public async Task DeleteTableAsync_WhenTableNameIsReserved_ReturnsUnsuccessfulResponse(string tableName)
    {
        var rawResponse = await TableServiceClient.DeleteTableAsync(tableName);

        Assert.Multiple(
            () => Assert.False(rawResponse.IsError),
            () => Assertions.UnsuccessfulJsonResponse(
                rawResponse,
                new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NotFound,
                    Headers = new Assertions.DefaultHeaders(rawResponse),
                    ErrorCode = "ResourceNotFound",
                    ErrorDescription = "The specified resource does not exist."
                }
            )
        );
    }

    [Theory]
    [InlineData("t")]
    [InlineData("tt")]
    [InlineData("testTableNameHavingALengthOf63CharactersSomeOfThemAreJustExtra1s")]
    public async Task DeleteTableAsync_WhenTableNameHasInvalidLength_ReturnsUnsuccessfulResponse(string tableName)
    {
        var rawResponse = await TableServiceClient.DeleteTableAsync(tableName);

        Assert.Multiple(
            () => Assert.False(rawResponse.IsError),
            () => Assertions.UnsuccessfulJsonResponse(
                rawResponse,
                new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NotFound,
                    Headers = new Assertions.DefaultHeaders(rawResponse),
                    ErrorCode = "ResourceNotFound",
                    ErrorDescription = "The specified resource does not exist."
                }
            )
        );
    }

    [Fact]
    public void QueryAsync_WhenThereIsNoMatchingTestTable_ReturnsEmptyResult()
    {
        var result = TableServiceClient.QueryAsync($"TableName eq '{TestTableName}'");

        var page = Assert.Single(result.AsPages());
        Assert.Multiple(
            () => Assert.Empty(page.Values),
            () => Assert.Null(page.ContinuationToken),
            () =>
            {
                var response = page.GetRawResponse();
                Assertions.SuccessfulJsonResponse(response, new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.OK,
                    Headers = new Assertions.DefaultHeaders(response),
                    Content =
                    {
                        { "odata.metadata", $"https://cloudstubdev.table.core.windows.net/$metadata#Tables" },
                        { "value", new List<IReadOnlyDictionary<string, object>>() }
                    }
                });
            }
        );
    }

    [Fact]
    public void QueryAsync_WhenThereIsMatchingTestTable_ReturnsTestTable()
    {
        TableServiceClient.CreateTable(TestTableName);
        var result = TableServiceClient.QueryAsync($"TableName eq '{TestTableName}'");

        var page = Assert.Single(result.AsPages());
        Assert.Multiple(
            () => Assert.Single(page.Values),
            () => Assert.Null(page.ContinuationToken),
            () =>
            {
                var response = page.GetRawResponse();
                Assertions.SuccessfulJsonResponse(response, new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.OK,
                    Headers = new Assertions.DefaultHeaders(response),
                    Content =
                    {
                        { "odata.metadata", $"https://cloudstubdev.table.core.windows.net/$metadata#Tables" },
                        { "value", new List<IReadOnlyDictionary<string, object>>
                            {
                                new Dictionary<string, object>
                                {
                                    { "TableName", TestTableName }
                                }
                            }
                        }
                    }
                });
            }
        );
    }

    [Fact]
    public async Task QueryAsync_WhenUsingZeroPageNumber_ThrowsException()
    {
        TableServiceClient.CreateTable(TestTableName);
        var result = TableServiceClient.QueryAsync($"filter eq not valid", maxPerPage: 0);

        await Assertions.JsonResponseThrowsAsync(
            async () => await result.ToListAsync(),
            response => new Assertions.UnsuccessfulResponseAssertOptions
            {
                StatusCode = HttpStatusCode.NotImplemented,
                ErrorCode = "NotImplemented",
                ErrorDescription = "The requested operation is not implemented on the specified resource.",
                Headers = new Assertions.DefaultHeaders(response)
            }
        );
    }

    [Fact]
    public async Task QueryAsync_WhenUsingNegativePageNumber_ThrowsException()
    {
        var result = TableServiceClient.QueryAsync($"filter eq not valid", maxPerPage: -1);

        await Assertions.JsonResponseThrowsAsync(
            async () => await result.ToListAsync(),
            response => new Assertions.UnsuccessfulResponseAssertOptions
            {
                StatusCode = HttpStatusCode.BadRequest,
                ErrorCode = "InvalidInput",
                ErrorDescription = "One of the request inputs is not valid.",
                Headers = new Assertions.DefaultHeaders(response)
            }
        );
    }

    [Fact]
    public async Task QueryAsync_WhenSpecifyingNonExistentPropertyName_ThrowsException()
    {
        var result = TableServiceClient.QueryAsync($"name eq 'does not exist'");

        await Assertions.JsonResponseThrowsAsync(
            async () => await result.ToListAsync(),
            response => new Assertions.UnsuccessfulResponseAssertOptions
            {
                StatusCode = HttpStatusCode.InternalServerError,
                ErrorCode = "InternalError",
                ErrorDescription = "Server encountered an internal error. Please try again after some time.",
                Headers = new Assertions.DefaultHeaders(response)
            }
        );
    }

    [Fact]
    public async Task GetProperties_WhenCalled_GetsTableStorageProperties()
    {
        var response = await TableServiceClient.GetPropertiesAsync();

        var properties = response.Value;
        var rawResponse = response.GetRawResponse();

        Assert.Multiple(
            () => Assert.False(rawResponse.IsError),
            () => Assertions.SuccessfulXmlResponse(
                rawResponse,
                new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.OK,
                    Headers = new Assertions.XmlContentHeaders(rawResponse),
                    Content = {
                        { "StorageServiceProperties", new Dictionary<string, object> {
                            { "Logging", new Dictionary<string, object> {
                                { "Version", "1.0" },
                                { "Delete", "false" },
                                { "Read", "false" },
                                { "Write", "false" },
                                { "RetentionPolicy", new Dictionary<string, object> {
                                    { "Enabled", "false" }
                                }}
                            }},
                            { "HourMetrics", new Dictionary<string, object> {
                                { "Version", "1.0" },
                                { "Enabled", "false" },
                                { "RetentionPolicy", new Dictionary<string, object> {
                                    { "Enabled", "false" }
                                }}
                            }},
                            { "MinuteMetrics", new Dictionary<string, object> {
                                { "Version", "1.0" },
                                { "Enabled", "false" },
                                { "RetentionPolicy", new Dictionary<string, object> {
                                    { "Enabled", "false" }
                                }}
                            }},
                            { "Cors", string.Empty }
                        }}
                    }
                }
            ),
            () =>
            {
                Assert.NotNull(properties);
                Assert.Multiple(
                    () =>
                    {
                        Assert.NotNull(properties.Logging);
                        Assert.Multiple(
                            () => Assert.Equal("1.0", properties.Logging.Version),
                            () => Assert.False(properties.Logging.Read),
                            () => Assert.False(properties.Logging.Write),
                            () => Assert.False(properties.Logging.Delete),
                            () =>
                            {
                                Assert.NotNull(properties.Logging.RetentionPolicy);
                                Assert.Multiple(
                                    () => Assert.False(properties.Logging.RetentionPolicy.Enabled),
                                    () => Assert.Null(properties.Logging.RetentionPolicy.Days)
                                );
                            }
                        );
                    },
                    () =>
                    {
                        Assert.NotNull(properties.HourMetrics);
                        Assert.Multiple(
                            () => Assert.Equal("1.0", properties.HourMetrics.Version),
                            () => Assert.False(properties.HourMetrics.Enabled),
                            () =>
                            {
                                Assert.NotNull(properties.HourMetrics.RetentionPolicy);
                                Assert.Multiple(
                                    () => Assert.False(properties.HourMetrics.RetentionPolicy.Enabled),
                                    () => Assert.Null(properties.HourMetrics.RetentionPolicy.Days)
                                );
                            }
                        );
                    },
                    () =>
                    {
                        Assert.NotNull(properties.MinuteMetrics);
                        Assert.Multiple(
                            () => Assert.False(properties.MinuteMetrics.RetentionPolicy.Enabled),
                            () => Assert.Null(properties.MinuteMetrics.RetentionPolicy.Days)
                        );
                    },
                    () => Assert.Empty(properties.Cors)
                );
            }
        );
    }

    [Fact]
    public async Task SetPropertiesAsync_WhenCalled_UpdatesTableStorageProperties()
    {
        var response = await TableServiceClient.SetPropertiesAsync(new TableServiceProperties
        {
            Logging = new TableAnalyticsLoggingSettings(
                version: "1.0",
                delete: false,
                read: false,
                write: false,
                retentionPolicy: new TableRetentionPolicy(enabled: false)
            ),
            HourMetrics = new TableMetrics(false)
            {
                Version = "1.0",
                RetentionPolicy = new TableRetentionPolicy(enabled: false)
                {
                    Days = null
                }
            },
            MinuteMetrics = new TableMetrics(false)
            {
                Version = "1.0",
                RetentionPolicy = new TableRetentionPolicy(enabled: false)
                {
                    Days = null
                }
            }
        });

        Assertions.EmptyResponse(
            response,
            new Assertions.ResponseAssertOptions
            {
                StatusCode = HttpStatusCode.Accepted,
                Headers = new Assertions.AcceptedHeaders(response)
            }
        );
    }

    [Fact]
    public async Task SetPropertiesAsync_WhenCalledWithNull_ThrowsException()
    {
        var exception = await Assert.ThrowsAsync<ArgumentNullException>("tableServiceProperties", () => TableServiceClient.SetPropertiesAsync(null));

        Assert.Equal(new ArgumentNullException("tableServiceProperties").Message, exception.Message);
    }

    [Fact]
    public async Task SetPropertiesAsync_WhenCalledWithEmptyProperties_ThrowsException()
    {
        var exception = await Assertions.XmlResponseThrowsAsync(
            () => TableServiceClient.SetPropertiesAsync(new TableServiceProperties()),
            rawResponse =>
            {
                var headers = new Assertions.XmlContentHeaders(rawResponse)
                {
                    { "Content-Length", "327" },
                    { "x-ms-error-code", "InvalidXmlDocument" }
                };
                headers.Remove("Transfer-Encoding");

                return new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    Headers = headers,
                    ErrorCode = "InvalidXmlDocument",
                    ExceptiopnErrorCode = null,
                    ErrorDescription = "XML specified is not syntactically valid.",
                    ErrorPhrase = "XML specified is not syntactically valid.",
                };
            }
        );
    }

    [Fact]
    public async Task GetStatisticsAsync_WhenCalled_ThrowsException()
    {
        await Assert.ThrowsAnyAsync<Exception>(() => TableServiceClient.GetStatisticsAsync());
    }
}