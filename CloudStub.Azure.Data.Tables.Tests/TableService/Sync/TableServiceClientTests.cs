using System.Collections;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Azure;

namespace CloudStub.Azure.Data.Tables.Tests.TableService.Sync;

public class StubCloudTableTests : BaseTableCloudStubTests
{
    [Fact]
    public void TableName_GetsTheSameNameWhichWasProvided()
    {
        Assert.Equal(TestTableName, CloudTable.Name);
    }

    [Fact]
    public void Create_WhenTableExists_ThrowsException()
    {
        CloudTable.Create();

        var exception = Assert.Throws<RequestFailedException>(() => CloudTable.Create());

        Assert.Equal("Azure.Data.Tables", exception.Source);
        Assert.Null(exception.HelpLink);
        Assert.Equal(-2146233088, exception.HResult);
        Assert.Null(exception.InnerException);
        Assert.IsAssignableFrom<IDictionary>(exception.Data);

        Assert.Equal("TableAlreadyExists", exception.ErrorCode);
        Assert.Equal(409, exception.Status);
        Assert.NotNull(exception.GetRawResponse());

        var rawResponse = exception.GetRawResponse()!;
        Assert.Equal(409, rawResponse.Status);
        Assert.True(rawResponse.IsError);
        Assert.Equal("Conflict", rawResponse.ReasonPhrase);

        Assert.NotNull(rawResponse.ClientRequestId);
        Assert.True(Guid.TryParseExact(rawResponse.ClientRequestId, "D", out _));

        Assert.NotEmpty(rawResponse.Headers);

        Assert.Null(rawResponse.Headers.ContentLength);
        Assert.Null(rawResponse.Headers.ContentLengthLong);
        Assert.Equal("application/json;odata=minimalmetadata;streaming=true;charset=utf-8", rawResponse.Headers.ContentType);

        Assert.NotNull(rawResponse.Headers.Date);
        Assert.InRange(rawResponse.Headers.Date!.Value, TestStart.AddSeconds(-3), TestStart.AddMinutes(1));

        Assert.Null(rawResponse.Headers.ETag);
        Assert.NotNull(rawResponse.Headers.RequestId);
        Assert.True(Guid.TryParseExact(rawResponse.Headers.RequestId, "D", out _));

        var headersDictionary = rawResponse.Headers.ToDictionary(h => h.Name, h => h.Value);
        Assert.Equal(9, headersDictionary.Count);
        Assert.Equal("no-cache", headersDictionary["Cache-Control"]);
        Assert.Equal("chunked", headersDictionary["Transfer-Encoding"]);
        Assert.Equal("Windows-Azure-Table/1.0 Microsoft-HTTPAPI/2.0", headersDictionary["Server"]);
        Assert.Equal(rawResponse.Headers.RequestId, headersDictionary["x-ms-request-id"]);
        Assert.Equal(rawResponse.ClientRequestId, headersDictionary["x-ms-client-request-id"]);
        Assert.Equal("2020-12-06", headersDictionary["x-ms-version"]);
        Assert.Equal("nosniff", headersDictionary["X-Content-Type-Options"]);
        Assert.Equal(rawResponse.Headers.Date.Value.ToString("R"), headersDictionary["Date"]);
        Assert.Equal("application/json;odata=minimalmetadata;streaming=true;charset=utf-8", headersDictionary["Content-Type"]);

        Assert.NotNull(rawResponse.Content);
        Assert.NotNull(rawResponse.ContentStream);
        var content = rawResponse.Content.ToString();
        var jsonContent = JsonSerializer.Deserialize<JsonObject>(content)!;
        Assert.Single(jsonContent);
        Assert.Contains("odata.error", jsonContent);

        var jsonContentOdataError = jsonContent["odata.error"]!.AsObject();
        Assert.Equal(2, jsonContentOdataError.Count);
        Assert.Contains("code", jsonContentOdataError);
        Assert.Equal("TableAlreadyExists", jsonContentOdataError["code"]!.GetValue<string>());

        Assert.Contains("message", jsonContentOdataError);
        var jsonContentOdataErrorMessage = jsonContentOdataError["message"]!.AsObject();
        Assert.Equal(2, jsonContentOdataErrorMessage.Count);
        Assert.Contains("lang", jsonContentOdataErrorMessage);
        Assert.Equal("en-US", jsonContentOdataErrorMessage["lang"]!.GetValue<string>());
        Assert.Contains("value", jsonContentOdataErrorMessage);
        var jsonContentOdataErrorMessageValue = jsonContentOdataErrorMessage["value"]!.GetValue<string>();
        Assert.Equal(
            $"The table specified already exists.\nRequestId:{rawResponse.Headers.RequestId}\nTime:",
            jsonContentOdataErrorMessageValue[..^DateTimeFormat.Length]
        );
        var jsonContentOdataErrorMessageTime = DateTimeOffset.ParseExact(jsonContentOdataErrorMessageValue[^DateTimeFormat.Length..], DateTimeFormat, CultureInfo.InvariantCulture);
        Assert.True(rawResponse.Headers.Date <= jsonContentOdataErrorMessageTime);

        Assert.Equal(
            $@"The table specified already exists.
RequestId:{rawResponse.Headers.RequestId}
Time:{jsonContentOdataErrorMessageTime.ToString(DateTimeFormat)}
Status: 409 (Conflict)
ErrorCode: TableAlreadyExists

Content:
{content}

Headers:
Cache-Control: no-cache
Transfer-Encoding: chunked
Server: Windows-Azure-Table/1.0 Microsoft-HTTPAPI/2.0
x-ms-request-id: {rawResponse.Headers.RequestId}
x-ms-client-request-id: {rawResponse.ClientRequestId}
x-ms-version: REDACTED
X-Content-Type-Options: REDACTED
Date: {rawResponse.Headers.Date:R}
Content-Type: application/json;odata=minimalmetadata;streaming=true;charset=utf-8
",
            exception.Message
        );
    }

    // [Theory]
    // [InlineData("invalid_table_name")]
    // [InlineData("1nvalid")]
    // public void Create_WhenTableNameIsInvalid_ThrowsException(string tableName)
    // {
    //     var cloudTable = GetCloudTable(tableName);

    //     var exception = Assert.Throws<StorageException>(() => cloudTable.Create(null, null, null, null, null));

    //     Assert.Equal("The remote server returned an error: (400) Bad Request.", exception.Message);
    //     Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
    //     Assert.Null(exception.HelpLink);
    //     Assert.Equal(-2146233088, exception.HResult);
    //     Assert.Null(exception.InnerException);
    //     Assert.IsAssignableFrom<IDictionary>(exception.Data);

    //     Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
    //     Assert.Null(exception.RequestInformation.ContentMd5);
    //     Assert.Empty(exception.RequestInformation.ErrorCode);
    //     Assert.Null(exception.RequestInformation.Etag);

    //     Assert.Equal("InvalidResourceName", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
    //     Assert.Matches(
    //         @$"^The specifed resource name contains invalid characters.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
    //         exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
    //     );

    //     Assert.Same(exception, exception.RequestInformation.Exception);
    // }

    // [Theory]
    // [InlineData("tables")]
    // public void Create_WhenTableNameIsReserved_ThrowsException(string tableName)
    // {
    //     var cloudTable = GetCloudTable(tableName);

    //     var exception = Assert.Throws<StorageException>(() => cloudTable.Create(null, null, null, null, null));

    //     Assert.Equal("The remote server returned an error: (400) Bad Request.", exception.Message);
    //     Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
    //     Assert.Null(exception.HelpLink);
    //     Assert.Equal(-2146233088, exception.HResult);
    //     Assert.Null(exception.InnerException);
    //     Assert.IsAssignableFrom<IDictionary>(exception.Data);

    //     Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
    //     Assert.Null(exception.RequestInformation.ContentMd5);
    //     Assert.Empty(exception.RequestInformation.ErrorCode);
    //     Assert.Null(exception.RequestInformation.Etag);

    //     Assert.Equal("InvalidInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
    //     Assert.Matches(
    //         @$"^One of the request inputs is not valid.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
    //         exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
    //     );

    //     Assert.Same(exception, exception.RequestInformation.Exception);
    // }

    // [Theory]
    // [InlineData("t")]
    // [InlineData("tt")]
    // [InlineData("testTableNameHavingALengthOf63CharactersSomeOfThemAreJustExtra1s")]
    // public void Create_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
    // {
    //     var cloudTable = GetCloudTable(tableName);

    //     var exception = Assert.Throws<StorageException>(() => cloudTable.Create(null, null, null, null, null));

    //     Assert.Equal("The remote server returned an error: (400) Bad Request.", exception.Message);
    //     Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
    //     Assert.Null(exception.HelpLink);
    //     Assert.Equal(-2146233088, exception.HResult);
    //     Assert.Null(exception.InnerException);
    //     Assert.IsAssignableFrom<IDictionary>(exception.Data);

    //     Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
    //     Assert.Null(exception.RequestInformation.ContentMd5);
    //     Assert.Empty(exception.RequestInformation.ErrorCode);
    //     Assert.Null(exception.RequestInformation.Etag);

    //     Assert.Equal("OutOfRangeInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
    //     Assert.Matches(
    //         @$"^The specified resource name length is not within the permissible limits.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
    //         exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
    //     );

    //     Assert.Same(exception, exception.RequestInformation.Exception);
    // }

    // [Fact(Skip = "CloudTable.Exists cannot be overridden.")]
    // public void CreateIfNotExists_WhenTableDoesNotExist_ReturnsFalse()
    // {
    //     Assert.True(CloudTable.CreateIfNotExists(null, null));

    //     Assert.True(CloudTable.Exists(null, null));
    // }

    // [Fact]
    // public void CreateIfNotExists_WhenTableExists_ReturnsFalse()
    // {
    //     CloudTable.Create(null, null, null, null, null);

    //     Assert.False(CloudTable.CreateIfNotExists(null, null));
    // }

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