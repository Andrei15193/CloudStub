using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.TableService.Sync
{
    // TODO:
    // Add tests for continuation token (valid and invalid scenarios)
    public class StubCloudTableTests : BaseTableCloudStubTests
    {
        [Fact(Skip = "Include this for CloudTableStub tests")]
        public void Create_WhenTablePreviouslyContainedEntities_IsEmpty()
        {
            CloudTable.Create();
            CloudTable.AddEntity(new TableEntity("partition-key", "row-key"));
            CloudTable.Delete();

            if (!TestRunContext.InMemory)
                Thread.Sleep(TimeSpan.FromMinutes(1));
            CloudTable.Create();

            var entities = CloudTable.Query<TableEntity>();

            Assert.Empty(entities);
        }

        [Fact]
        public void AccountName_GetsTheSameNameWhichWasProvided()
        {
            Assert.Equal(TableAccountName, TableServiceClient.AccountName);
        }

        [Fact]
        public void CreateTable_WhenTableDoesNotExist_ReturnsTableItem()
        {
            var response = TableServiceClient.CreateTable(TestTableName);

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
        public void CreateTable_WhenTableExists_ThrowsException()
        {
            TableServiceClient.CreateTable(TestTableName);

            Assertions.JsonResponseThrows(
                () => TableServiceClient.CreateTable(TestTableName.ToLowerInvariant()),
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
        public void CreateTable_WhenTableNameIsInvalid_ThrowsException(string tableName)
        {
            Assertions.JsonResponseThrows(
                () => TableServiceClient.CreateTable(tableName),
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
        public void CreateTable_WhenTableNameIsReserved_ThrowsException(string tableName)
        {
            Assertions.JsonResponseThrows(
                () => TableServiceClient.CreateTable(tableName),
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
        public void CreateTable_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
        {
            Assertions.JsonResponseThrows(
                () => TableServiceClient.CreateTable(tableName),
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
        public void CreateTableIfNotExists_WhenTableDoesNotExist_ReturnsTableItemWithNoContentResponse()
        {
            var response = TableServiceClient.CreateTableIfNotExists(TestTableName);
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
        public void CreateTableIfNotExists_WhenTableExists_ReturnsTableItemWithConflictResponse()
        {
            TableServiceClient.CreateTable(TestTableName);

            var response = TableServiceClient.CreateTableIfNotExists(TestTableName.ToLowerInvariant());
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
                    Assert.Equal(TestTableName.ToLowerInvariant(), tableItem.Name);
                }
            );
        }

        [Theory]
        [InlineData("invalid_table_name")]
        [InlineData("1nvalid")]
        public void CreateTableIfNotExists_WhenTableNameIsInvalid_ThrowsException(string tableName)
        {
            Assertions.JsonResponseThrows(
                () => TableServiceClient.CreateTableIfNotExists(tableName),
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
        public void CreateTableIfNotExists_WhenTableNameIsReserved_ThrowsException(string tableName)
        {
            Assertions.JsonResponseThrows(
                () => TableServiceClient.CreateTableIfNotExists(tableName),
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
        public void CreateTableIfNotExists_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
        {
            Assertions.JsonResponseThrows(
                () => TableServiceClient.CreateTableIfNotExists(tableName),
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
        public void DeleteTable_WhenTableDoesNotExist_ReturnsSuccessfulResponse()
        {
            var rawResponse = TableServiceClient.DeleteTable(TestTableName);

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
        public void DeleteTable_WhenTableExists_ReturnsSuccessfulResponse()
        {
            TableServiceClient.CreateTable(TestTableName);

            var rawResponse = TableServiceClient.DeleteTable(TestTableName.ToLowerInvariant());

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
        public void DeleteTable_WhenTableNameIsInvalid_ReturnsUnsuccessfulResponse(string tableName)
        {
            var rawResponse = TableServiceClient.DeleteTable(tableName);

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
        public void DeleteTable_WhenTableNameIsReserved_ReturnsUnsuccessfulResponse(string tableName)
        {
            var rawResponse = TableServiceClient.DeleteTable(tableName);

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
        public void DeleteTable_WhenTableNameHasInvalidLength_ReturnsUnsuccessfulResponse(string tableName)
        {
            var rawResponse = TableServiceClient.DeleteTable(tableName);

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
        public void Query_WhenThereIsNoMatchingTestTable_ReturnsEmptyResult()
        {
            var result = TableServiceClient.Query($"TableName eq '{TestTableName}'");

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
                            { "odata.metadata", "https://cloudstubdev.table.core.windows.net/$metadata#Tables" },
                            { "value", new List<IReadOnlyDictionary<string, object>>() }
                        }
                    });
                }
            );
        }

        [Fact]
        public void Query_WhenThereIsMatchingTestTable_ReturnsTestTable()
        {
            TableServiceClient.CreateTable(TestTableName);
            var result = TableServiceClient.Query($"TableName eq '{TestTableName}'");

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
        public void Query_WhenUsingInvalidFilter_ThrowsException()
        {
            var result = TableServiceClient.Query($"filter eq not valid");

            Assertions.JsonResponseThrows(
                () => result.ToList(),
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
        public void Query_WhenUsingZeroPageNumber_ThrowsException()
        {
            TableServiceClient.CreateTable(TestTableName);
            var result = TableServiceClient.Query($"filter eq not valid", maxPerPage: 0);

            Assertions.JsonResponseThrows(
                () => result.ToList(),
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
        public void Query_WhenUsingNegativePageNumber_ThrowsException()
        {
            var result = TableServiceClient.Query($"filter eq not valid", maxPerPage: -1);

            Assertions.JsonResponseThrows(
                () => result.ToList(),
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
        public void Query_WhenSpecifyingNonExistentPropertyName_ThrowsException()
        {
            var result = TableServiceClient.Query($"name eq 'does not exist'");

            Assertions.JsonResponseThrows(
                () => result.ToList(),
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
        public void Query_WhenUsingTakeCount_ReturnsOnlyFirstPage()
        {
            TableServiceClient.CreateTable($"z1zz{TestTableName}");
            TableServiceClient.CreateTable($"z1zzz{TestTableName}");
            TableServiceClient.CreateTable($"z1zzzz{TestTableName}");
            TableServiceClient.CreateTable($"z1zzzzz{TestTableName}");

            var page = TableServiceClient.Query($"TableName eq 'z1zz{TestTableName}' or TableName eq 'z1zzz{TestTableName}' or TableName eq 'z1zzzz{TestTableName}' or TableName eq 'z1zzzzz{TestTableName}'", maxPerPage: 2).AsPages().First();

            Assert.Multiple(
                () => Assert.Equal(2, page.Values.Count),
                () => Assert.Equal($"z1zzzz{TestTableName}".ToLowerInvariant(), ResponseContinuationToken.DecodeTableNameContinuationToken(page.ContinuationToken)),
                () =>
                {
                    var response = page.GetRawResponse();
                    Assertions.SuccessfulJsonResponse(response, new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.DefaultHeaders(response)
                        {
                            { "x-ms-continuation-NextTableName", page.ContinuationToken }
                        },
                        Content =
                        {
                            { "odata.metadata", $"https://cloudstubdev.table.core.windows.net/$metadata#Tables" },
                            { "value", new List<IReadOnlyDictionary<string, object>>
                                {
                                    new Dictionary<string, object>
                                    {
                                        { "TableName", $"z1zz{TestTableName}" }
                                    },
                                    new Dictionary<string, object>
                                    {
                                        { "TableName", $"z1zzz{TestTableName}" }
                                    }
                                }
                            }
                        }
                    });
                }
            );
        }

        [Fact]
        public void Query_WhenUsingTakeCount_ReturnsContinuationTokenContainingPartitionAndRowKeysForNextPage()
        {
            TableServiceClient.CreateTable($"z2zz{TestTableName}");
            TableServiceClient.CreateTable($"z2zzz{TestTableName}");
            TableServiceClient.CreateTable($"z2zzzz{TestTableName}");
            TableServiceClient.CreateTable($"z2zzzzz{TestTableName}");

            var pages = TableServiceClient.Query($"TableName eq 'z2zz{TestTableName}' or TableName eq 'z2zzz{TestTableName}' or TableName eq 'z2zzzz{TestTableName}' or TableName eq 'z2zzzzz{TestTableName}'", maxPerPage: 2).AsPages().ToList();
            var firstPage = pages.First();
            var lastPage = pages.Last();

            Assert.Multiple(
                () => Assert.Equal(2, firstPage.Values.Count),
                () => Assert.Equal($"z2zzzz{TestTableName}".ToLowerInvariant(), ResponseContinuationToken.DecodeTableNameContinuationToken(firstPage.ContinuationToken)),
                () =>
                {
                    var response = firstPage.GetRawResponse();
                    Assertions.SuccessfulJsonResponse(response, new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.DefaultHeaders(response)
                        {
                            { "x-ms-continuation-NextTableName", firstPage.ContinuationToken }
                        },
                        Content =
                        {
                            { "odata.metadata", $"https://cloudstubdev.table.core.windows.net/$metadata#Tables" },
                            { "value", new List<IReadOnlyDictionary<string, object>>
                                {
                                    new Dictionary<string, object>
                                    {
                                        { "TableName", $"z2zz{TestTableName}" }
                                    },
                                    new Dictionary<string, object>
                                    {
                                        { "TableName", $"z2zzz{TestTableName}" }
                                    }
                                }
                            }
                        }
                    });
                },

                () => Assert.Equal(2, lastPage.Values.Count),
                () => Assert.Null(lastPage.ContinuationToken),
                () =>
                {
                    var response = lastPage.GetRawResponse();
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
                                        { "TableName", $"z2zzzz{TestTableName}" }
                                    },
                                    new Dictionary<string, object>
                                    {
                                        { "TableName", $"z2zzzzz{TestTableName}" }
                                    }
                                }
                            }
                        }
                    });
                }
            );
        }

        [Theory]
        [InlineData(TableAccountSasPermissions.All, TableAccountSasResourceTypes.All)]
        [InlineData(TableAccountSasPermissions.Add, TableAccountSasResourceTypes.All)]
        [InlineData(TableAccountSasPermissions.Delete, TableAccountSasResourceTypes.All)]
        [InlineData(TableAccountSasPermissions.List, TableAccountSasResourceTypes.All)]
        [InlineData(TableAccountSasPermissions.Read, TableAccountSasResourceTypes.All)]
        [InlineData(TableAccountSasPermissions.Update, TableAccountSasResourceTypes.All)]
        [InlineData(TableAccountSasPermissions.Write, TableAccountSasResourceTypes.All)]

        [InlineData(TableAccountSasPermissions.All, TableAccountSasResourceTypes.Container)]
        [InlineData(TableAccountSasPermissions.Add, TableAccountSasResourceTypes.Container)]
        [InlineData(TableAccountSasPermissions.Delete, TableAccountSasResourceTypes.Container)]
        [InlineData(TableAccountSasPermissions.List, TableAccountSasResourceTypes.Container)]
        [InlineData(TableAccountSasPermissions.Read, TableAccountSasResourceTypes.Container)]
        [InlineData(TableAccountSasPermissions.Update, TableAccountSasResourceTypes.Container)]
        [InlineData(TableAccountSasPermissions.Write, TableAccountSasResourceTypes.Container)]

        [InlineData(TableAccountSasPermissions.All, TableAccountSasResourceTypes.Object)]
        [InlineData(TableAccountSasPermissions.Add, TableAccountSasResourceTypes.Object)]
        [InlineData(TableAccountSasPermissions.Delete, TableAccountSasResourceTypes.Object)]
        [InlineData(TableAccountSasPermissions.List, TableAccountSasResourceTypes.Object)]
        [InlineData(TableAccountSasPermissions.Read, TableAccountSasResourceTypes.Object)]
        [InlineData(TableAccountSasPermissions.Update, TableAccountSasResourceTypes.Object)]
        [InlineData(TableAccountSasPermissions.Write, TableAccountSasResourceTypes.Object)]

        [InlineData(TableAccountSasPermissions.All, TableAccountSasResourceTypes.Service)]
        [InlineData(TableAccountSasPermissions.Add, TableAccountSasResourceTypes.Service)]
        [InlineData(TableAccountSasPermissions.Delete, TableAccountSasResourceTypes.Service)]
        [InlineData(TableAccountSasPermissions.List, TableAccountSasResourceTypes.Service)]
        [InlineData(TableAccountSasPermissions.Read, TableAccountSasResourceTypes.Service)]
        [InlineData(TableAccountSasPermissions.Update, TableAccountSasResourceTypes.Service)]
        [InlineData(TableAccountSasPermissions.Write, TableAccountSasResourceTypes.Service)]
        public void GenerateSasUri_WhenCalled_GeneratesValidSasUri(TableAccountSasPermissions permissions, TableAccountSasResourceTypes resourceTypes)
        {
            var sasUri = TableServiceClient.GenerateSasUri(permissions, resourceTypes, DateTimeOffset.UtcNow.AddHours(1));

            Assert.NotNull(sasUri);
        }

        [Fact]
        public void GetProperties_WhenCalled_GetsTableStorageProperties()
        {
            var response = TableServiceClient.GetProperties();

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
        public void SetProperties_WhenCalled_UpdatesTableStorageProperties()
        {
            var response = TableServiceClient.SetProperties(new TableServiceProperties
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
        public void SetProperties_WhenCalledWithNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("tableServiceProperties", () => TableServiceClient.SetProperties(null));

            Assert.Equal(new ArgumentNullException("tableServiceProperties").Message, exception.Message);
        }

        [Fact]
        public void SetProperties_WhenCalledWithEmptyProperties_ThrowsException()
        {
            Assertions.XmlResponseThrows(
                () => TableServiceClient.SetProperties(new TableServiceProperties()),
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
                        ExceptionErrorCode = null,
                        ErrorDescription = "XML specified is not syntactically valid.",
                        ErrorPhrase = "XML specified is not syntactically valid.",
                    };
                }
            );
        }

        [Fact]
        public void GetStatistics_WhenCalled_ThrowsException()
        {
            Assert.ThrowsAny<Exception>(() => TableServiceClient.GetStatistics());
        }

        [Fact]
        public void GetTableClient_WhenCalledTwice_ReturnsDifferentInstances()
        {
            var first = TableServiceClient.GetTableClient(TestTableName);
            var second = TableServiceClient.GetTableClient(TestTableName);

            Assert.NotEqual(first, second);
            Assert.NotSame(first, second);
        }
    }
}