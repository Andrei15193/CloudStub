using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Sync
{
    public class TableClientTests : BaseTableCloudStubTests
    {
        [Fact]
        public void Name_GetsTheSameNameWhichWasProvided()
        {
            Assert.Equal(TableName, TableClient.Name);
        }

        [Fact]
        public void AccountName_GetsTheSameNameWhichWasProvided()
        {
            Assert.Equal(TableAccountName, TableClient.AccountName);
        }

        [Fact]
        public void Create_WhenTablePreviouslyContainedEntities_IsEmpty()
        {
            TableClient.Create();
            TableClient.AddEntity(new TableEntity("partition-key", "row-key"));
            TableClient.Delete();

            if (!TestRunContext.InMemory)
                Thread.Sleep(TimeSpan.FromMinutes(1));
            TableClient.Create();

            var entities = TableClient.Query<TableEntity>();

            Assert.Empty(entities);
        }

        [Fact]
        public void Create_WhenTableDoesNotExist_ReturnsTableItem()
        {
            var response = TableClient.Create();

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
                            { "Location", $"{TableServiceClient.Uri}Tables('{TableName}')" }
                        },
                        Content =
                        {
                            { "odata.metadata", $"{TableServiceClient.Uri}$metadata#Tables/@Element" },
                            { "TableName", TableName }
                        }
                    }
                ),
                () =>
                {
                    Assert.NotNull(tableItem);
                    Assert.Equal(TableName, tableItem.Name);
                }
            );
        }

        [Fact]
        public void Create_WhenTableExists_ThrowsException()
        {
            TableClient.Create();

            Assertions.JsonResponseThrows(
                () => TableClient.Create(),
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
        public void Create_WhenTableNameIsInvalid_ThrowsException(string tableName)
        {
            Assertions.JsonResponseThrows(
                () => TableServiceClient.GetTableClient(tableName).Create(),
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
        public void Create_WhenTableNameIsReserved_ThrowsException(string tableName)
        {
            Assertions.JsonResponseThrows(
                () => TableServiceClient.GetTableClient(tableName).Create(),
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
        public void Create_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
        {
            Assertions.JsonResponseThrows(
                () => TableServiceClient.GetTableClient(tableName).Create(),
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
        public void CreateIfNotExists_WhenTableDoesNotExist_ReturnsTableItemWithNoContentResponse()
        {
            var response = TableClient.CreateIfNotExists();
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
                            { "Location", $"{TableServiceClient.Uri}Tables('{TableName}')" },
                            { "Preference-Applied", "return-no-content" },
                            { "DataServiceId", $"{TableServiceClient.Uri}Tables('{TableName}')"}
                        }
                    }
                ),
                () =>
                {
                    Assert.NotNull(tableItem);
                    Assert.Equal(TableName, tableItem.Name);
                }
            );
        }

        [Fact]
        public void CreateIfNotExists_WhenTableExists_ReturnsTableItemWithConflictResponse()
        {
            TableClient.CreateIfNotExists();

            var response = TableClient.CreateIfNotExists();
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
                    Assert.Equal(TableName, tableItem.Name);
                }
            );
        }

        [Theory]
        [InlineData("invalid_table_name")]
        [InlineData("1nvalid")]
        public void CreateIfNotExists_WhenTableNameIsInvalid_ThrowsException(string tableName)
        {
            Assertions.JsonResponseThrows(
                () => TableServiceClient.GetTableClient(tableName).CreateIfNotExists(),
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
        public void CreateIfNotExists_WhenTableNameIsReserved_ThrowsException(string tableName)
        {
            Assertions.JsonResponseThrows(
                () => TableServiceClient.GetTableClient(tableName).CreateIfNotExists(),
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
        public void CreateIfNotExists_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
        {
            Assertions.JsonResponseThrows(
                () => TableServiceClient.GetTableClient(tableName).CreateIfNotExists(),
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
        public void Delete_WhenTableDoesNotExist_ReturnsSuccessfulResponse()
        {
            var rawResponse = TableClient.Delete();

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
        public void Delete_WhenTableExists_ReturnsSuccessfulResponse()
        {
            TableClient.Create();

            var rawResponse = TableClient.Delete();

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
        public void Delete_WhenTableNameIsInvalid_ReturnsUnsuccessfulResponse(string tableName)
        {
            var rawResponse = TableServiceClient.GetTableClient(tableName).Delete();

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
        public void Delete_WhenTableNameIsReserved_ReturnsUnsuccessfulResponse(string tableName)
        {
            var rawResponse = TableServiceClient.GetTableClient(tableName).Delete();

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
        public void Delete_WhenTableNameHasInvalidLength_ReturnsUnsuccessfulResponse(string tableName)
        {
            var rawResponse = TableServiceClient.GetTableClient(tableName).Delete();

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
        [InlineData(TableSasPermissions.Read)]
        [InlineData(TableSasPermissions.Add)]
        [InlineData(TableSasPermissions.Update)]
        [InlineData(TableSasPermissions.Delete)]
        [InlineData(TableSasPermissions.All)]
        public void GenerateSasUri_WhenCalled_GeneratesValidSasUri(TableSasPermissions permissions)
        {
            var sasUri = TableClient.GenerateSasUri(permissions, DateTimeOffset.UtcNow.AddHours(1));

            Assert.NotNull(sasUri);
        }

        [Fact]
        public void GetAccessPolicies_WhenCalled_GetsTableAccessPolicies()
        {
            TableClient.Create();

            var response = TableClient.GetAccessPolicies();
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
        public void GetAccessPolicies_WhenTableDoesNotExist_ThrowsException()
        {
            Assertions.XmlResponseThrows(
                () => TableClient.GetAccessPolicies(),
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
        public void SetAccessPolicy_WhenCalled_SetsTableAccessPolicies()
        {
            TableClient.Create();

            var response = TableClient.SetAccessPolicy(Enumerable.Empty<TableSignedIdentifier>());

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
        public void SetAccessPolicy_WhenTableDoesNotExist_ThrowsException()
        {
            Assertions.XmlResponseThrows(
                () => TableClient.SetAccessPolicy(Enumerable.Empty<TableSignedIdentifier>()),
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
        public void GetAccessPolicies_WhenPoliciesHaveBeenSet_ReturnsThem()
        {
            var utcNow = DateTimeOffset.UtcNow;
            TableClient.Create();
            TableClient.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(utcNow, utcNow.AddHours(1), "raud")) });

            var response = TableClient.GetAccessPolicies();
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
        public void GetAccessPolicies_WhenPoliciesHaveExpired_ReturnsThem()
        {
            var utcNow = DateTimeOffset.UtcNow;
            TableClient.Create();
            TableClient.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(utcNow, utcNow.AddSeconds(1), "raud")) });
            Thread.Sleep(TimeSpan.FromSeconds(2));

            var response = TableClient.GetAccessPolicies();
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
        public void SetAccessPolicy_WhenPoliciesHaveAlreadyBeenSet_OverwritesPreviousListCompletely()
        {
            var utcNow = DateTimeOffset.UtcNow;
            TableClient.Create();
            TableClient.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id-1", new TableAccessPolicy(utcNow, utcNow.AddHours(1), "raud")) });

            TableClient.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id-2", new TableAccessPolicy(utcNow, utcNow.AddHours(1), "raud")) });

            var response = TableClient.GetAccessPolicies();
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
        public void SetAccessPolicy_WhenPolicyHasNullAccessPolicy_DoesNotSetIt()
        {
            var utcNow = DateTimeOffset.UtcNow;
            TableClient.Create();

            TableClient.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id", null) });

            var response = TableClient.GetAccessPolicies();
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
        public void SetAccessPolicy_WhenPoliciesAreNull_ClearsThem()
        {
            var utcNow = DateTimeOffset.UtcNow;
            TableClient.Create();
            TableClient.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(utcNow, utcNow.AddHours(1), "raud")) });

            TableClient.SetAccessPolicy(null);

            var response = TableClient.GetAccessPolicies();
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
        public void SetAccessPolicy_WhenPolicyHasNullStartsOn_DoesNotSetIt()
        {
            var utcNow = DateTimeOffset.UtcNow;
            TableClient.Create();

            TableClient.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(null, utcNow.AddHours(1), "raud")) });

            var response = TableClient.GetAccessPolicies();
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
        public void SetAccessPolicy_WhenPolicyHasNullExpiresOn_DoesNotSetIt()
        {
            var utcNow = DateTimeOffset.UtcNow;
            TableClient.Create();

            TableClient.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(utcNow, null, "raud")) });

            var response = TableClient.GetAccessPolicies();
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
        public void SetAccessPolicy_WhenPolicyHasNullPermissions_DoesNotSetThem()
        {
            var utcNow = DateTimeOffset.UtcNow;
            TableClient.Create();

            TableClient.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(utcNow, utcNow.AddHours(1), null)) });

            var response = TableClient.GetAccessPolicies();
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