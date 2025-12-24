using System.Net;

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
        var tableItem = Assertions.SuccessfulRequest(
            () => TableServiceClient.CreateTable(TestTableName),
            HttpStatusCode.Created,
            new Dictionary<string, string>
            {
                { "odata.metadata", $"{TableServiceClient.Uri}$metadata#Tables/@Element" },
                { "TableName", TestTableName }
            },
            new Dictionary<string, string?>
            {
                { "Location", $"{TableServiceClient.Uri}Tables('{TestTableName}')" }
            }
        );

        Assert.NotNull(tableItem);
        Assert.Equal(TestTableName, tableItem.Name);
    }

    [Fact]
    public void CreateTable_WhenTableExists_ThrowsException()
    {
        TableServiceClient.CreateTable(TestTableName);

        Assertions.Throws(
            () => TableServiceClient.CreateTable(TestTableName),
            HttpStatusCode.Conflict,
            "TableAlreadyExists",
            "The table specified already exists."
        );
    }

    [Theory]
    [InlineData("invalid_table_name")]
    [InlineData("1nvalid")]
    public void CreateTable_WhenTableNameIsInvalid_ThrowsException(string tableName)
    {
        Assertions.Throws(
            () => TableServiceClient.CreateTable(tableName),
            HttpStatusCode.BadRequest,
            "InvalidResourceName",
            "The specifed resource name contains invalid characters."
        );
    }

    [Theory]
    [InlineData("tables")]
    public void CreateTable_WhenTableNameIsReserved_ThrowsException(string tableName)
    {
        Assertions.Throws(
            () => TableServiceClient.CreateTable(tableName),
            HttpStatusCode.BadRequest,
            "InvalidInput",
            "One of the request inputs is not valid."
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
            HttpStatusCode.BadRequest,
            "OutOfRangeInput",
            "The specified resource name length is not within the permissible limits."
        );
    }

    [Fact]
    public void CreateTableIfNotExists_WhenTableDoesNotExist_ReturnsTableItem()
    {
        TableServiceClient.CreateTable(TestTableName);

        var tableItem = Assertions.UnsuccessfulRequest(
            () => TableServiceClient.CreateTableIfNotExists(TestTableName),
            HttpStatusCode.Conflict,
            "TableAlreadyExists",
            "The table specified already exists.",
            new Dictionary<string, string?>
            {
                { "Preference-Applied", "return-no-content" }
            }
        );
        Assert.NotNull(tableItem);
        Assert.Equal(TestTableName, tableItem.Name);
    }

    [Fact]
    public void CreateTableIfNotExists_WhenTableExists_ThrowsException()
    {
        TableServiceClient.CreateTable(TestTableName);

        var tableItem = Assertions.UnsuccessfulRequest(
            () => TableServiceClient.CreateTableIfNotExists(TestTableName),
            HttpStatusCode.Conflict,
            "TableAlreadyExists",
            "The table specified already exists.",
            new Dictionary<string, string?>
            {
                { "Preference-Applied", "return-no-content" }
            }
        );
        Assert.NotNull(tableItem);
        Assert.Equal(TestTableName, tableItem.Name);
    }

    [Theory]
    [InlineData("invalid_table_name")]
    [InlineData("1nvalid")]
    public void CreateTableIfNotExists_WhenTableNameIsInvalid_ThrowsException(string tableName)
    {
        Assertions.Throws(
            () => TableServiceClient.CreateTableIfNotExists(tableName),
            HttpStatusCode.BadRequest,
            "InvalidResourceName",
            "The specifed resource name contains invalid characters.",
            new Dictionary<string, string?>
            {
                { "Preference-Applied", "return-no-content" }
            }
        );
    }

    [Theory]
    [InlineData("tables")]
    public void CreateTableIfNotExists_WhenTableNameIsReserved_ThrowsException(string tableName)
    {
        Assertions.Throws(
            () => TableServiceClient.CreateTableIfNotExists(tableName),
            HttpStatusCode.BadRequest,
            "InvalidInput",
            "One of the request inputs is not valid.",
            new Dictionary<string, string?>
            {
                { "Preference-Applied", "return-no-content" }
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
            HttpStatusCode.BadRequest,
            "OutOfRangeInput",
            "The specified resource name length is not within the permissible limits.",
            new Dictionary<string, string?>
            {
                { "Preference-Applied", "return-no-content" }
            }
        );
    }

    [Fact]
    public void DeleteTable_WhenTableDoesNotExist_ReturnsSuccessfulResponse()
    {
        Assertions.UnsuccessfulRequest(
            () => TableServiceClient.DeleteTable(TestTableName),
            HttpStatusCode.NotFound,
            "ResourceNotFound",
            "The specified resource does not exist."
        );
    }

    [Fact]
    public void DeleteTable_WhenTableExists_ReturnsSuccessfulResponse()
    {
        TableServiceClient.CreateTable(TestTableName);

        Assertions.SuccessfulRequest(
            () => TableServiceClient.DeleteTable(TestTableName),
            HttpStatusCode.NoContent,
            new Dictionary<string, string>
            {
                { "odata.metadata", $"{TableServiceClient.Uri}$metadata#Tables/@Element" },
                { "TableName", TestTableName }
            }
        );
    }

    [Theory]
    [InlineData("invalid_table_name")]
    [InlineData("1nvalid")]
    public void DeleteTable_WhenTableNameIsInvalid_ThrowsException(string tableName)
    {
        Assertions.UnsuccessfulRequest(
            () => TableServiceClient.DeleteTable(tableName),
            HttpStatusCode.NotFound,
            "ResourceNotFound",
            "The specified resource does not exist."
        );
    }

    [Theory]
    [InlineData("tables")]
    public void DeleteTable_WhenTableNameIsReserved_ThrowsException(string tableName)
    {
        Assertions.UnsuccessfulRequest(
            () => TableServiceClient.DeleteTable(tableName),
            HttpStatusCode.NotFound,
            "ResourceNotFound",
            "The specified resource does not exist."
        );
    }

    [Theory]
    [InlineData("t")]
    [InlineData("tt")]
    [InlineData("testTableNameHavingALengthOf63CharactersSomeOfThemAreJustExtra1s")]
    public void DeleteTable_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
    {
        Assertions.UnsuccessfulRequest(
            () => TableServiceClient.DeleteTable(tableName),
            HttpStatusCode.NotFound,
            "ResourceNotFound",
            "The specified resource does not exist."
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