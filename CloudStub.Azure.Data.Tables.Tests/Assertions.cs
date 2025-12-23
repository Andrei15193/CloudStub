using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Azure;

namespace CloudStub.Azure.Data.Tables.Tests;

internal static class Assertions
{
    private const string DateTimeFormat = "yyyy-MM-ddTHH:mm:ss.fffffffZ";

    public static RequestFailedException Throws(Action action, HttpStatusCode httpStatusCode, string errorCode, string errorDescription, IReadOnlyDictionary<string, string?>? additionalHeaders = null)
    {
        var utcNow = DateTimeOffset.UtcNow;
        var spelledOutStatusCode = Regex.Replace(httpStatusCode.ToString(), "(?<=.)[A-Z]", " $0");

        var exception = Assert.Throws<RequestFailedException>(action);

        Assert.Equal("Azure.Data.Tables", exception.Source);
        Assert.Null(exception.HelpLink);
        Assert.Equal(-2146233088, exception.HResult);
        Assert.Null(exception.InnerException);
        Assert.Empty(exception.Data);

        Assert.Equal(errorCode, exception.ErrorCode);
        Assert.Equal((int)httpStatusCode, exception.Status);
        Assert.NotNull(exception.GetRawResponse());

        var (response, content, jsonContent) = UnsuccessfulResponse(exception.GetRawResponse(), httpStatusCode, errorCode, errorDescription, additionalHeaders);

        var jsonContentOdataErrorMessageValue = jsonContent["odata.error"]!["message"]!["value"]!.GetValue<string>();
        var jsonContentOdataErrorMessageTime = DateTimeOffset.ParseExact(jsonContentOdataErrorMessageValue[^DateTimeFormat.Length..], DateTimeFormat, CultureInfo.InvariantCulture);

        Assert.Equal(
            $@"{errorDescription}
RequestId:{response.Headers.RequestId}
Time:{jsonContentOdataErrorMessageTime.ToString(DateTimeFormat, CultureInfo.InvariantCulture)}",
            jsonContentOdataErrorMessageValue
        );
        Assert.True(response.Headers.Date <= jsonContentOdataErrorMessageTime);
        Assert.InRange(jsonContentOdataErrorMessageTime, utcNow.AddSeconds(-3), utcNow.AddMinutes(1));

        Assert.Equal(
            $@"{errorDescription}
RequestId:{response.Headers.RequestId}
Time:{jsonContentOdataErrorMessageTime.ToString(DateTimeFormat, CultureInfo.InvariantCulture)}
Status: {httpStatusCode:D} ({spelledOutStatusCode})
ErrorCode: {errorCode}

Content:
{content}

Headers:
Cache-Control: no-cache
Transfer-Encoding: chunked
Server: Windows-Azure-Table/1.0 Microsoft-HTTPAPI/2.0
x-ms-request-id: {response.Headers.RequestId}
x-ms-client-request-id: {response.ClientRequestId}
x-ms-version: REDACTED
X-Content-Type-Options: REDACTED{(additionalHeaders is null ? "" : "\n" + string.Join("\n", additionalHeaders.Select(h => $"{h.Key}: REDACTED")))}
Date: {response.Headers.Date:R}
Content-Type: application/json;odata=minimalmetadata;streaming=true;charset=utf-8
",
            exception.Message
        );

        return exception;
    }

    public static T SuccessfulRequest<T>(Func<Response<T>> action, HttpStatusCode httpStatusCode, IReadOnlyDictionary<string, string> metadataResponse, IReadOnlyDictionary<string, string?>? additionalHeaders = null)
    {
        var utcNow = DateTimeOffset.UtcNow;
        var spelledOutStatusCode = Regex.Replace(httpStatusCode.ToString(), "(?<=.)[A-Z]", " $0");

        var response = action();

        var rawResponse = response.GetRawResponse();
        Assert.False(rawResponse.IsError);
        Assert.Equal((int)httpStatusCode, rawResponse.Status);
        Assert.False(rawResponse.IsError);
        Assert.Equal(spelledOutStatusCode, rawResponse.ReasonPhrase);

        Assert.NotNull(rawResponse.ClientRequestId);
        Assert.True(Guid.TryParseExact(rawResponse.ClientRequestId, "D", out _));

        Assert.NotEmpty(rawResponse.Headers);

        Assert.Null(rawResponse.Headers.ContentLength);
        Assert.Null(rawResponse.Headers.ContentLengthLong);
        Assert.Equal("application/json;odata=minimalmetadata;streaming=true;charset=utf-8", rawResponse.Headers.ContentType);

        Assert.NotNull(rawResponse.Headers.Date);
        Assert.InRange(rawResponse.Headers.Date!.Value, utcNow.AddSeconds(-3), utcNow.AddMinutes(1));

        Assert.Null(rawResponse.Headers.ETag);
        Assert.NotNull(rawResponse.Headers.RequestId);
        Assert.True(Guid.TryParseExact(rawResponse.Headers.RequestId, "D", out _));

       Headers(rawResponse, additionalHeaders);

        Assert.NotNull(rawResponse.Content);
        Assert.NotNull(rawResponse.ContentStream);
        var content = rawResponse.Content.ToString();
        var jsonContent = JsonSerializer.Deserialize<JsonObject>(content)!;

        Assert.Equal(metadataResponse.Count, jsonContent.Count);
        foreach (var metadata in metadataResponse)
            Assert.Equal(metadata.Value, jsonContent[metadata.Key]!.GetValue<string>());

        return response;
    }

    public static T UnsuccessfulRequest<T>(Func<Response<T>> action, HttpStatusCode httpStatusCode, string errorCode, string errorDescription, IReadOnlyDictionary<string, string?>? additionalHeaders = null)
    {
        var response = action();

        var (rawResponse, _, _) = UnsuccessfulResponse(response.GetRawResponse(), httpStatusCode, errorCode, errorDescription, additionalHeaders);
        Assert.False(rawResponse.IsError);

        return response;
    }

    public static T FailedRequest<T>(Func<Response<T>> action, HttpStatusCode httpStatusCode, string errorCode, string errorDescription, IReadOnlyDictionary<string, string?>? additionalHeaders = null)
    {
        var response = action();

        var (rawResponse, _, _) = UnsuccessfulResponse(response.GetRawResponse(), httpStatusCode, errorCode, errorDescription, additionalHeaders);
        Assert.True(rawResponse.IsError);

        return response;
    }

    private static (Response Response, string Content, JsonObject JsonContent) UnsuccessfulResponse(Response? response, HttpStatusCode httpStatusCode, string errorCode, string errorDescription, IReadOnlyDictionary<string, string?>? additionalHeaders = null)
    {
        var utcNow = DateTimeOffset.UtcNow;
        var spelledOutStatusCode = Regex.Replace(httpStatusCode.ToString(), "(?<=.)[A-Z]", " $0");

        Assert.NotNull(response);

        Assert.Equal((int)httpStatusCode, response.Status);
        Assert.Equal(spelledOutStatusCode, response.ReasonPhrase);

        Assert.NotNull(response.ClientRequestId);
        Assert.True(Guid.TryParseExact(response.ClientRequestId, "D", out _));

        Assert.NotEmpty(response.Headers);

        Assert.Null(response.Headers.ContentLength);
        Assert.Null(response.Headers.ContentLengthLong);
        Assert.Equal("application/json;odata=minimalmetadata;streaming=true;charset=utf-8", response.Headers.ContentType);

        Assert.NotNull(response.Headers.Date);
        Assert.InRange(response.Headers.Date!.Value, utcNow.AddSeconds(-3), utcNow.AddMinutes(1));

        Assert.Null(response.Headers.ETag);
        Assert.NotNull(response.Headers.RequestId);
        Assert.True(Guid.TryParseExact(response.Headers.RequestId, "D", out _));

       Headers(response, additionalHeaders);

        Assert.NotNull(response.Content);
        Assert.NotNull(response.ContentStream);
        var content = response.Content.ToString();
        var jsonContent = JsonSerializer.Deserialize<JsonObject>(content)!;
        Assert.Single(jsonContent);
        Assert.Contains("odata.error", jsonContent);

        var jsonContentOdataError = jsonContent["odata.error"]!.AsObject();
        Assert.Equal(2, jsonContentOdataError.Count);
        Assert.Contains("code", jsonContentOdataError);
        Assert.Equal(errorCode, jsonContentOdataError["code"]!.GetValue<string>());

        Assert.Contains("message", jsonContentOdataError);
        var jsonContentOdataErrorMessage = jsonContentOdataError["message"]!.AsObject();
        Assert.Equal(2, jsonContentOdataErrorMessage.Count);
        Assert.Contains("lang", jsonContentOdataErrorMessage);
        Assert.Equal("en-US", jsonContentOdataErrorMessage["lang"]!.GetValue<string>());
        Assert.Contains("value", jsonContentOdataErrorMessage);
        var jsonContentOdataErrorMessageValue = jsonContentOdataErrorMessage["value"]!.GetValue<string>();
        Assert.Equal(
            $"{errorDescription}\nRequestId:{response.Headers.RequestId}\nTime:",
            jsonContentOdataErrorMessageValue[..^DateTimeFormat.Length]
        );
        var jsonContentOdataErrorMessageTime = DateTimeOffset.ParseExact(jsonContentOdataErrorMessageValue[^DateTimeFormat.Length..], DateTimeFormat, CultureInfo.InvariantCulture);
        Assert.True(response.Headers.Date <= jsonContentOdataErrorMessageTime);

        return (
            Response: response,
            Content: content,
            JsonContent: jsonContent
        );
    }

    private static void Headers(Response rawResponse, IReadOnlyDictionary<string, string?>? additionalHeaders)
    {
        var headersDictionary = rawResponse.Headers.ToDictionary(header => header.Name, header => header.Value);

        Assert.Equal(9 + (additionalHeaders?.Count ?? 0), headersDictionary.Count);
        Assert.Equal("no-cache", headersDictionary["Cache-Control"]);
        Assert.Equal("chunked", headersDictionary["Transfer-Encoding"]);
        Assert.Equal("Windows-Azure-Table/1.0 Microsoft-HTTPAPI/2.0", headersDictionary["Server"]);
        Assert.Equal(rawResponse.Headers.RequestId, headersDictionary["x-ms-request-id"]);
        Assert.Equal(rawResponse.ClientRequestId, headersDictionary["x-ms-client-request-id"]);
        Assert.Equal("2020-12-06", headersDictionary["x-ms-version"]);
        Assert.Equal("nosniff", headersDictionary["X-Content-Type-Options"]);
        Assert.Equal(rawResponse.Headers.Date!.Value.ToString("R"), headersDictionary["Date"]);
        Assert.Equal("application/json;odata=minimalmetadata;streaming=true;charset=utf-8", headersDictionary["Content-Type"]);
        if (additionalHeaders is not null)
            foreach (var additionalHeader in additionalHeaders)
                Assert.Equal(additionalHeader.Value, headersDictionary[additionalHeader.Key]);
    }
}