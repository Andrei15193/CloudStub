using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Async
{
    public class TableClientOperationsTests : BaseTableCloudStubTests
    {
        [Fact(Skip = "Remove skip after implementing add entity operation")]
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
        public async Task CreateAsync_WhenTableDoesNotExist_ReturnsTableItem()
        {
            var response = await CloudTable.CreateAsync();

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
        public async Task CreateAsync_WhenTableExists_ThrowsException()
        {
            await CloudTable.CreateAsync();

            await Assertions.JsonResponseThrowsAsync(
                () => CloudTable.CreateAsync(),
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
        public async Task CreateAsync_WhenTableNameIsInvalid_ThrowsException(string tableName)
        {
            await Assertions.JsonResponseThrowsAsync(
                 () => TableServiceClient.GetTableClient(tableName).CreateAsync(),
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
        public async Task CreateAsync_WhenTableNameIsReserved_ThrowsException(string tableName)
        {
            await Assertions.JsonResponseThrowsAsync(
                () => TableServiceClient.GetTableClient(tableName).CreateAsync(),
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
        public async Task CreateAsync_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
        {
            await Assertions.JsonResponseThrowsAsync(
                () => TableServiceClient.GetTableClient(tableName).CreateAsync(),
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
        public async Task CreateIfNotExistsAsync_WhenTableDoesNotExist_ReturnsTableItemWithNoContentResponse()
        {
            var response = await CloudTable.CreateIfNotExistsAsync();
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
        public async Task CreateIfNotExistsAsync_WhenTableExists_ReturnsTableItemWithConflictResponse()
        {
            await CloudTable.CreateIfNotExistsAsync();

            var response = await CloudTable.CreateIfNotExistsAsync();
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
        public async Task CreateIfNotExistsAsync_WhenTableNameIsInvalid_ThrowsException(string tableName)
        {
            await Assertions.JsonResponseThrowsAsync(
                () => TableServiceClient.GetTableClient(tableName).CreateIfNotExistsAsync(),
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
        public async Task CreateIfNotExistsAsync_WhenTableNameIsReserved_ThrowsException(string tableName)
        {
            await Assertions.JsonResponseThrowsAsync(
                () => TableServiceClient.GetTableClient(tableName).CreateIfNotExistsAsync(),
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
        public async Task CreateIfNotExistsAsync_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
        {
            await Assertions.JsonResponseThrowsAsync(
                () => TableServiceClient.GetTableClient(tableName).CreateIfNotExistsAsync(),
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
        public async Task DeleteAsync_WhenTableDoesNotExist_ReturnsSuccessfulResponse()
        {
            var rawResponse = await CloudTable.DeleteAsync();

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
        public async Task DeleteAsync_WhenTableExists_ReturnsSuccessfulResponse()
        {
            await CloudTable.CreateAsync();

            var rawResponse = await CloudTable.DeleteAsync();

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
        public async Task DeleteAsync_WhenTableNameIsInvalid_ReturnsUnsuccessfulResponse(string tableName)
        {
            var rawResponse = await TableServiceClient.GetTableClient(tableName).DeleteAsync();

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
        public async Task DeleteAsync_WhenTableNameIsReserved_ReturnsUnsuccessfulResponse(string tableName)
        {
            var rawResponse = await TableServiceClient.GetTableClient(tableName).DeleteAsync();

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
        public async Task DeleteAsync_WhenTableNameHasInvalidLength_ReturnsUnsuccessfulResponse(string tableName)
        {
            var rawResponse = await TableServiceClient.GetTableClient(tableName).DeleteAsync();

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
        public async Task GetAccessPoliciesAsync_WhenCalled_GetsTableAccessPolicies()
        {
            await CloudTable.CreateAsync();

            var response = await CloudTable.GetAccessPoliciesAsync();
            var accessPolicies = response.Value;

            var rawResponse = response.GetRawResponse();

            Assert.Multiple(
                () => Assert.False(rawResponse.IsError),
                () => Assertions.SuccessfulXmlResponse(
                    rawResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.XmlContentHeaders(rawResponse),
                        Content =
                        {
                            { "SignedIdentifiers", string.Empty }
                        }
                    }
                ),
                () => Assert.Empty(accessPolicies)
            );
        }

        [Fact]
        public async Task GetAccessPoliciesAsync_WhenTableDoesNotExist_ThrowsException()
        {
            await Assertions.XmlResponseThrowsAsync(
                () => CloudTable.GetAccessPoliciesAsync(),
                rawResponse =>
                {
                    var headers = new Assertions.XmlContentHeaders(rawResponse)
                    {
                        { "x-ms-error-code", "TableNotFound" },
                        { "Content-Length", "316" }
                    };
                    headers.Remove("Transfer-Encoding");

                    return new Assertions.UnsuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NotFound,
                        Headers = headers,
                        ErrorCode = "TableNotFound",
                        ExceptionErrorCode = null,
                        ErrorDescription = "The table specified does not exist.",
                        ErrorPhrase = "The table specified does not exist."
                    };
                }
            );
        }

        [Fact]
        public async Task SetAccessPolicyAsync_WhenCalled_SetsTableAccessPolicies()
        {
            await CloudTable.CreateAsync();

            var response = await CloudTable.SetAccessPolicyAsync(Enumerable.Empty<TableSignedIdentifier>());

            Assert.Multiple(
                () => Assert.False(response.IsError),
                () =>
                {
                    var headers = new Assertions.NoContentHeaders(response);
                    headers.Remove("Cache-Control");
                    headers.Remove("X-Content-Type-Options");

                    Assertions.EmptyResponse(
                        response,
                        new Assertions.SuccessfulResponseAssertOptions
                        {
                            StatusCode = HttpStatusCode.NoContent,
                            Headers = headers
                        }
                    );
                }
            );
        }

        [Fact]
        public async Task SetAccessPolicyAsync_WhenTableDoesNotExist_ThrowsException()
        {
            await Assertions.XmlResponseThrowsAsync(
                () => CloudTable.SetAccessPolicyAsync(Enumerable.Empty<TableSignedIdentifier>()),
                rawResponse =>
                {
                    var headers = new Assertions.XmlContentHeaders(rawResponse)
                    {
                        { "x-ms-error-code", "TableNotFound" },
                        { "Content-Length", "316" }
                    };
                    headers.Remove("Transfer-Encoding");

                    return new Assertions.UnsuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.NotFound,
                        Headers = headers,
                        ErrorCode = "TableNotFound",
                        ExceptionErrorCode = null,
                        ErrorDescription = "The table specified does not exist.",
                        ErrorPhrase = "The table specified does not exist."
                    };
                }
            );
        }

        [Fact]
        public async Task GetAccessPoliciesAsync_WhenPoliciesHaveBeenSet_ReturnsThem()
        {
            var utcNow = DateTimeOffset.UtcNow;
            await CloudTable.CreateAsync();
            await CloudTable.SetAccessPolicyAsync(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(utcNow, utcNow.AddHours(1), "raud")) });

            var response = await CloudTable.GetAccessPoliciesAsync();
            var accessPolicies = response.Value;

            var rawResponse = response.GetRawResponse();

            Assert.Multiple(
                () => Assert.False(rawResponse.IsError),
                () => Assertions.SuccessfulXmlResponse(
                    rawResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.XmlContentHeaders(rawResponse),
                        Content =
                        {
                            { "SignedIdentifiers",
                                new Dictionary<string, object> {
                                    {
                                        "SignedIdentifier",
                                        new Dictionary<string, object> {
                                            { "Id", "access-policy-id" },
                                            {
                                                "AccessPolicy", new Dictionary<string, object> {
                                                    { "Start", utcNow.ToString(Assertions.DateTimeFormat) },
                                                    { "Expiry", utcNow.AddHours(1).ToString(Assertions.DateTimeFormat) },
                                                    { "Permission", "raud" }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                ),
                () => Assert.Equal(
                    accessPolicies.Select(accessPolicy => new { accessPolicy.Id, accessPolicy.AccessPolicy.StartsOn, accessPolicy.AccessPolicy.ExpiresOn, accessPolicy.AccessPolicy.Permission }),
                    new[] { new { Id = "access-policy-id", StartsOn = utcNow as DateTimeOffset?, ExpiresOn = utcNow.AddHours(1) as DateTimeOffset?, Permission = "raud" } })
            );
        }

        [Fact]
        public async Task GetAccessPoliciesAsync_WhenPoliciesHaveExpired_ReturnsThem()
        {
            var utcNow = DateTimeOffset.UtcNow;
            await CloudTable.CreateAsync();
            await CloudTable.SetAccessPolicyAsync(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(utcNow, utcNow.AddSeconds(1), "raud")) });
            Thread.Sleep(TimeSpan.FromSeconds(2));

            var response = await CloudTable.GetAccessPoliciesAsync();
            var accessPolicies = response.Value;

            var rawResponse = response.GetRawResponse();

            Assert.Multiple(
                () => Assert.False(rawResponse.IsError),
                () => Assertions.SuccessfulXmlResponse(
                    rawResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.XmlContentHeaders(rawResponse),
                        Content =
                        {
                            { "SignedIdentifiers",
                                new Dictionary<string, object> {
                                    {
                                        "SignedIdentifier",
                                        new Dictionary<string, object> {
                                            { "Id", "access-policy-id" },
                                            {
                                                "AccessPolicy", new Dictionary<string, object> {
                                                    { "Start", utcNow.ToString(Assertions.DateTimeFormat) },
                                                    { "Expiry", utcNow.AddSeconds(1).ToString(Assertions.DateTimeFormat) },
                                                    { "Permission", "raud" }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                ),
                () => Assert.Equal(
                    accessPolicies.Select(accessPolicy => new { accessPolicy.Id, accessPolicy.AccessPolicy.StartsOn, accessPolicy.AccessPolicy.ExpiresOn, accessPolicy.AccessPolicy.Permission }),
                    new[] { new { Id = "access-policy-id", StartsOn = utcNow as DateTimeOffset?, ExpiresOn = utcNow.AddSeconds(1) as DateTimeOffset?, Permission = "raud" } })
            );
        }

        [Fact]
        public async Task SetAccessPolicyAsync_WhenPoliciesHaveAlreadyBeenSet_OverwritesPreviousListCompletely()
        {
            var utcNow = DateTimeOffset.UtcNow;
            await CloudTable.CreateAsync();
            await CloudTable.SetAccessPolicyAsync(new[] { new TableSignedIdentifier("access-policy-id-1", new TableAccessPolicy(utcNow, utcNow.AddHours(1), "raud")) });

            await CloudTable.SetAccessPolicyAsync(new[] { new TableSignedIdentifier("access-policy-id-2", new TableAccessPolicy(utcNow, utcNow.AddHours(1), "raud")) });

            var response = await CloudTable.GetAccessPoliciesAsync();
            var accessPolicies = response.Value;
            var rawResponse = response.GetRawResponse();
            Assert.Multiple(
                () => Assert.False(rawResponse.IsError),
                () => Assertions.SuccessfulXmlResponse(
                    rawResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.XmlContentHeaders(rawResponse),
                        Content =
                        {
                            { "SignedIdentifiers",
                                new Dictionary<string, object> {
                                    {
                                        "SignedIdentifier",
                                        new Dictionary<string, object> {
                                            { "Id", "access-policy-id-2" },
                                            {
                                                "AccessPolicy", new Dictionary<string, object> {
                                                    { "Start", utcNow.ToString(Assertions.DateTimeFormat) },
                                                    { "Expiry", utcNow.AddHours(1).ToString(Assertions.DateTimeFormat) },
                                                    { "Permission", "raud" }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                ),
                () => Assert.Equal(
                    accessPolicies.Select(accessPolicy => new { accessPolicy.Id, accessPolicy.AccessPolicy.StartsOn, accessPolicy.AccessPolicy.ExpiresOn, accessPolicy.AccessPolicy.Permission }),
                    new[] { new { Id = "access-policy-id-2", StartsOn = utcNow as DateTimeOffset?, ExpiresOn = utcNow.AddHours(1) as DateTimeOffset?, Permission = "raud" } })
            );
        }

        [Fact]
        public async Task SetAccessPolicyAsync_WhenPolicyHasNullAccessPolicy_DoesNotSetIt()
        {
            var utcNow = DateTimeOffset.UtcNow;
            await CloudTable.CreateAsync();

            await CloudTable.SetAccessPolicyAsync(new[] { new TableSignedIdentifier("access-policy-id", null) });

            var response = await CloudTable.GetAccessPoliciesAsync();
            var accessPolicies = response.Value;
            var rawResponse = response.GetRawResponse();
            Assert.Multiple(
                () => Assert.False(rawResponse.IsError),
                () => Assertions.SuccessfulXmlResponse(
                    rawResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.XmlContentHeaders(rawResponse),
                        Content =
                        {
                            { "SignedIdentifiers",
                                new Dictionary<string, object> {
                                    {
                                        "SignedIdentifier",
                                        new Dictionary<string, object> {
                                            { "Id", "access-policy-id" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                ),
                () => Assert.Equal(
                    accessPolicies.Select(accessPolicy => new { accessPolicy.Id, accessPolicy.AccessPolicy }),
                    new[] { new { Id = "access-policy-id", AccessPolicy = default(TableAccessPolicy) } })
            );
        }

        [Fact]
        public async Task SetAccessPolicyAsync_WhenPoliciesAreNull_ClearsThem()
        {
            var utcNow = DateTimeOffset.UtcNow;
            await CloudTable.CreateAsync();
            await CloudTable.SetAccessPolicyAsync(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(utcNow, utcNow.AddHours(1), "raud")) });

            await CloudTable.SetAccessPolicyAsync(null);

            var response = await CloudTable.GetAccessPoliciesAsync();
            var accessPolicies = response.Value;
            var rawResponse = response.GetRawResponse();
            Assert.Multiple(
                () => Assert.False(rawResponse.IsError),
                () => Assertions.SuccessfulXmlResponse(
                    rawResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.XmlContentHeaders(rawResponse),
                        Content =
                        {
                            { "SignedIdentifiers", string.Empty }
                        }
                    }
                ),
                () => Assert.Empty(accessPolicies)
            );
        }

        [Fact]
        public async Task SetAccessPolicyAsync_WhenPolicyHasNullStartsOn_DoesNotSetIt()
        {
            var utcNow = DateTimeOffset.UtcNow;
            await CloudTable.CreateAsync();

            await CloudTable.SetAccessPolicyAsync(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(null, utcNow.AddHours(1), "raud")) });

            var response = await CloudTable.GetAccessPoliciesAsync();
            var accessPolicies = response.Value;
            var rawResponse = response.GetRawResponse();
            Assert.Multiple(
                () => Assert.False(rawResponse.IsError),
                () => Assertions.SuccessfulXmlResponse(
                    rawResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.XmlContentHeaders(rawResponse),
                        Content =
                        {
                            { "SignedIdentifiers",
                                new Dictionary<string, object> {
                                    {
                                        "SignedIdentifier",
                                        new Dictionary<string, object> {
                                            { "Id", "access-policy-id" },
                                            {
                                                "AccessPolicy", new Dictionary<string, object> {
                                                    { "Expiry", utcNow.AddHours(1).ToString(Assertions.DateTimeFormat) },
                                                    { "Permission", "raud" }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                ),
                () => Assert.Equal(
                    accessPolicies.Select(accessPolicy => new { accessPolicy.Id, accessPolicy.AccessPolicy.StartsOn, accessPolicy.AccessPolicy.ExpiresOn, accessPolicy.AccessPolicy.Permission }),
                    new[] { new { Id = "access-policy-id", StartsOn = default(DateTimeOffset?), ExpiresOn = utcNow.AddHours(1) as DateTimeOffset?, Permission = "raud" } })
            );
        }

        [Fact]
        public async Task SetAccessPolicyAsync_WhenPolicyHasNullExpiresOn_DoesNotSetIt()
        {
            var utcNow = DateTimeOffset.UtcNow;
            await CloudTable.CreateAsync();

            await CloudTable.SetAccessPolicyAsync(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(utcNow, null, "raud")) });

            var response = await CloudTable.GetAccessPoliciesAsync();
            var accessPolicies = response.Value;
            var rawResponse = response.GetRawResponse();
            Assert.Multiple(
                () => Assert.False(rawResponse.IsError),
                () => Assertions.SuccessfulXmlResponse(
                    rawResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.XmlContentHeaders(rawResponse),
                        Content =
                        {
                            { "SignedIdentifiers",
                                new Dictionary<string, object> {
                                    {
                                        "SignedIdentifier",
                                        new Dictionary<string, object> {
                                            { "Id", "access-policy-id" },
                                            {
                                                "AccessPolicy", new Dictionary<string, object> {
                                                    { "Start", utcNow.ToString(Assertions.DateTimeFormat) },
                                                    { "Permission", "raud" }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                ),
                () => Assert.Equal(
                    accessPolicies.Select(accessPolicy => new { accessPolicy.Id, accessPolicy.AccessPolicy.StartsOn, accessPolicy.AccessPolicy.ExpiresOn, accessPolicy.AccessPolicy.Permission }),
                    new[] { new { Id = "access-policy-id", StartsOn = utcNow as DateTimeOffset?, ExpiresOn = default(DateTimeOffset?), Permission = "raud" } })
            );
        }

        [Fact]
        public async Task SetAccessPolicyAsync_WhenPolicyHasNullPermissions_DoesNotSetThem()
        {
            var utcNow = DateTimeOffset.UtcNow;
            await CloudTable.CreateAsync();

            await CloudTable.SetAccessPolicyAsync(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(utcNow, utcNow.AddHours(1), null)) });

            var response = await CloudTable.GetAccessPoliciesAsync();
            var accessPolicies = response.Value;
            var rawResponse = response.GetRawResponse();
            Assert.Multiple(
                () => Assert.False(rawResponse.IsError),
                () => Assertions.SuccessfulXmlResponse(
                    rawResponse,
                    new Assertions.SuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.OK,
                        Headers = new Assertions.XmlContentHeaders(rawResponse),
                        Content =
                        {
                            { "SignedIdentifiers",
                                new Dictionary<string, object> {
                                    {
                                        "SignedIdentifier",
                                        new Dictionary<string, object> {
                                            { "Id", "access-policy-id" },
                                            {
                                                "AccessPolicy", new Dictionary<string, object> {
                                                    { "Start", utcNow.ToString(Assertions.DateTimeFormat) },
                                                    { "Expiry", utcNow.AddHours(1).ToString(Assertions.DateTimeFormat) }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                ),
                () => Assert.Equal(
                    accessPolicies.Select(accessPolicy => new { accessPolicy.Id, accessPolicy.AccessPolicy.StartsOn, accessPolicy.AccessPolicy.ExpiresOn, accessPolicy.AccessPolicy.Permission }),
                    new[] { new { Id = "access-policy-id", StartsOn = utcNow as DateTimeOffset?, ExpiresOn = utcNow.AddHours(1) as DateTimeOffset?, Permission = default(string) } })
            );
        }
    }
}