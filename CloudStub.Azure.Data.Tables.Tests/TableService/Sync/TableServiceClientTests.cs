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

    // [Fact]
    // public void Delete_WhenTableDoesNotExist_ThrowsException()
    // {
    //     var exception = Assert.Throws<StorageException>(() => CloudTable.Delete(null, null));

    //     Assert.Equal("Not Found", exception.Message);
    //     Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
    //     Assert.Null(exception.HelpLink);
    //     Assert.Equal(-2146233088, exception.HResult);
    //     Assert.Null(exception.InnerException);
    //     Assert.IsAssignableFrom<IDictionary>(exception.Data);

    //     Assert.Equal(404, exception.RequestInformation.HttpStatusCode);
    //     Assert.Null(exception.RequestInformation.ContentMd5);
    //     Assert.Empty(exception.RequestInformation.ErrorCode);
    //     Assert.Null(exception.RequestInformation.Etag);

    //     Assert.Equal("ResourceNotFound", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
    //     Assert.Matches(
    //         @$"^The specified resource does not exist.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
    //         exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
    //     );

    //     Assert.Same(exception, exception.RequestInformation.Exception);
    // }

    // [Fact(Skip = "CloudTable.Exists cannot be overridden.")]
    // public void Delete_WhenTableExists_DeletesTable()
    // {
    //     CloudTable.Create(null, null, null, null, null);

    //     CloudTable.Delete(null, null);

    //     Assert.False(CloudTable.Exists(null, null));
    // }

    // [Fact]
    // public void DeleteIfExists_WhenTableDoesNotExist_ReturnsFalse()
    // {
    //     Assert.False(CloudTable.DeleteIfExists(null, null));
    // }

    // [Fact]
    // public void DeleteIfExists_WhenTableExists_ReturnsTrue()
    // {
    //     CloudTable.Create(null, null, null, null, null);

    //     Assert.True(CloudTable.DeleteIfExists(null, null));
    //     Assert.False(CloudTable.DeleteIfExists(null, null));
    // }

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