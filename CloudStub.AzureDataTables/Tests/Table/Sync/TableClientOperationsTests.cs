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
    public class TableClientOperationsTests : BaseTableCloudStubTests
    {
        [Fact]
        public void Name_GetsTheSameNameWhichWasProvided()
        {
            Assert.Equal(TestTableName, CloudTable.Name);
        }

        [Fact]
        public void AccountName_GetsTheSameNameWhichWasProvided()
        {
            Assert.Equal(TableAccountName, CloudTable.AccountName);
        }

        [Fact(Skip = "Remove skip after implementing add entity operation")]
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
        public void Create_WhenTableDoesNotExist_ReturnsTableItem()
        {
            var response = CloudTable.Create();

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
        public void Create_WhenTableExists_ThrowsException()
        {
            CloudTable.Create();

            Assertions.JsonResponseThrows(
                () => CloudTable.Create(),
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
            var response = CloudTable.CreateIfNotExists();
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
        public void CreateIfNotExists_WhenTableExists_ReturnsTableItemWithConflictResponse()
        {
            CloudTable.CreateIfNotExists();

            var response = CloudTable.CreateIfNotExists();
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
            var rawResponse = CloudTable.Delete();

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
            CloudTable.Create();

            var rawResponse = CloudTable.Delete();

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
            var sasUri = CloudTable.GenerateSasUri(permissions, DateTimeOffset.UtcNow.AddHours(1));

            Assert.NotNull(sasUri);
        }

        [Fact]
        public void GetAccessPolicies_WhenCalled_GetsTableAccessPolicies()
        {
            CloudTable.Create();

            var response = CloudTable.GetAccessPolicies();
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
                () => CloudTable.GetAccessPolicies(),
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
            CloudTable.Create();

            var response = CloudTable.SetAccessPolicy(Enumerable.Empty<TableSignedIdentifier>());

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
                () => CloudTable.SetAccessPolicy(Enumerable.Empty<TableSignedIdentifier>()),
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
            CloudTable.Create();
            CloudTable.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(utcNow, utcNow.AddHours(1), "raud")) });

            var response = CloudTable.GetAccessPolicies();
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
            CloudTable.Create();
            CloudTable.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id", new TableAccessPolicy(utcNow, utcNow.AddSeconds(1), "raud")) });
            Thread.Sleep(TimeSpan.FromSeconds(2));

            var response = CloudTable.GetAccessPolicies();
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
            CloudTable.Create();
            CloudTable.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id-1", new TableAccessPolicy(utcNow, utcNow.AddHours(1), "raud")) });

            CloudTable.SetAccessPolicy(new[] { new TableSignedIdentifier("access-policy-id-2", new TableAccessPolicy(utcNow, utcNow.AddHours(1), "raud")) });

            var response = CloudTable.GetAccessPolicies();
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
    }
}