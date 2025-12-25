using System.Net;
using Azure.Data.Tables.Sas;

namespace CloudStub.Azure.Data.Tables.Tests.TableService.Sync;

public class StubCloudTableTests : BaseTableCloudStubTests
{
    [Fact(Skip = "Include this for CloudTableStub tests")]
    public void TableName_GetsTheSameNameWhichWasProvided()
    {
        Assert.Equal(TestTableName, CloudTable.Name);
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
    public void CreateTable_WhenTableExists_ThrowsException()
    {
        TableServiceClient.CreateTable(TestTableName);

        Assertions.Throws(
            () => TableServiceClient.CreateTable(TestTableName),
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
        Assertions.Throws(
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
        Assertions.Throws(
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
        Assertions.Throws(
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
    public void CreateTableIfNotExists_WhenTableDoesNotExist_ReturnsTableItem()
    {
        TableServiceClient.CreateTable(TestTableName);

        var response = TableServiceClient.CreateTableIfNotExists(TestTableName);
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

    [Fact]
    public void CreateTableIfNotExists_WhenTableExists_ThrowsException()
    {
        TableServiceClient.CreateTable(TestTableName);

        var response = TableServiceClient.CreateTableIfNotExists(TestTableName);
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
    public void CreateTableIfNotExists_WhenTableNameIsInvalid_ThrowsException(string tableName)
    {
        Assertions.Throws(
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
        Assertions.Throws(
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
        Assertions.Throws(
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

        var rawResponse = TableServiceClient.DeleteTable(TestTableName);

        Assert.Multiple(
            () => Assert.False(rawResponse.IsError),
            () => Assertions.SuccessfulJsonResponse(
                rawResponse,
                new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NoContent,
                    Headers = new Assertions.NoContentHeaders(rawResponse),
                    Content =
                    {
                        { "odata.metadata", $"{TableServiceClient.Uri}$metadata#Tables/@Element" },
                        { "TableName", TestTableName }
                    },
                }
            )
        );
    }

    [Theory]
    [InlineData("invalid_table_name")]
    [InlineData("1nvalid")]
    public void DeleteTable_WhenTableNameIsInvalid_ThrowsException(string tableName)
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
    public void DeleteTable_WhenTableNameIsReserved_ThrowsException(string tableName)
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
    public void DeleteTable_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
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
    public void GetProperties_WhenCalled_GeneratesValidSasUri()
    {
        var response = TableServiceClient.GetProperties();

        var properties = response.Value;
        var rawResponse = response.GetRawResponse();

        Assert.Multiple(
            () => Assert.False(rawResponse.IsError),
            () => Assertions.SuccessfulJsonResponse(
                rawResponse,
                new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.OK,
                    Headers = new Assertions.XmlContentHeaders(rawResponse)
                }
            ),
            () =>
            {
                Assert.NotNull(properties);
                // Assert.Equal(TestTableName, properties.Cors);
            }
        );
    }

    // [Fact]
    // public void Create_WhenTablePreviouslyContainedEntities_IsEmpty()
    // {
    //     CloudTable.Create();
    //     CloudTable.Execute(TableOperation.Insert(new TableEntity("partition-key", "row-key")));
    //     CloudTable.Delete();

    //     if (!UseInMemory)
    //         Thread.Sleep(TimeSpan.FromMinutes(1));

    //     CloudTable.Create();

    //     var entities = GetAllEntities();
    //     Assert.Empty(entities);
    // }

    // [Fact]
    // public void GetPermissions_WhenNotImplemented_ThrowsException()
    // {
    //     Assert.Throws<NotImplementedException>(() => CloudTable.GetPermissions(null, null));
    // }

    // [Fact]
    // public void SetPermissions_WhenNotImplemented_ThrowsException()
    // {
    //     Assert.Throws<NotImplementedException>(() => CloudTable.SetPermissions(null, null, null));
    // }
}