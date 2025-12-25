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
    private static readonly ICollection<string> _nonRedactedHeaderNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "Cache-Control",
        "Transfer-Encoding",
        "Server",
        "x-ms-request-id",
        "x-ms-client-request-id",
        "Date",
        "Content-Type"
    };

    public static Response SuccessfulJsonResponse(Response? response, SuccessfulResponseAssertOptions responseAssertOptions)
    {
        Assert.NotNull(response);
        Assert.Multiple(
            () => AssertInfo(response, responseAssertOptions),
            () => AssertHeaders(response, responseAssertOptions),
            () => AssertSuccessfulJsonContent(response, responseAssertOptions)
        );

        return response;
    }

    public static Response UnsuccessfulJsonResponse(Response? response, UnsuccessfulResponseAssertOptions responseAssertOptions)
    {
        Assert.NotNull(response);
        Assert.Multiple(
            () => AssertInfo(response, responseAssertOptions),
            () => AssertHeaders(response, responseAssertOptions),
            () => AssertUnsuccessfulJsonContent(response, responseAssertOptions)
        );

        return response;
    }

    public static RequestFailedException Throws(Action action, Func<Response?, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
    {
        var exception = Assert.Throws<RequestFailedException>(action);

        var responseAssertOptions = responseAssertOptionsFactory(exception.GetRawResponse());

        Assert.Multiple(
            () => AssertException(exception, responseAssertOptions),
            () => UnsuccessfulJsonResponse(exception.GetRawResponse(), responseAssertOptions)
        );

        return exception;
    }

    private static void AssertInfo(Response response, ResponseAssertOptions responseAssertOptions)
    {
        var spelledOutStatusCode = Regex.Replace(responseAssertOptions.StatusCode.ToString(), "(?<=[a-z])[A-Z]", " $0");

        Assert.Multiple(
            () => Assert.Equal((int)responseAssertOptions.StatusCode, response.Status),
            () => Assert.Equal(spelledOutStatusCode, response.ReasonPhrase),
            () =>
            {
                Assert.NotNull(response.ClientRequestId);
                Assert.True(Guid.TryParseExact(response.ClientRequestId, "D", out _), "Expected ClientRequestId to be a valid GUID.");
            }
        );
    }

    private static void AssertHeaders(Response response, ResponseAssertOptions responseAssertOptions)
    {
        var utcNow = DateTimeOffset.UtcNow;

        Assert.Multiple(
            () => Assert.Equal(responseAssertOptions.Headers.Count, response.Headers.Count()),
            () => Assert.Multiple([
                ..response.Headers.Select(header => new Action(() =>
                {
                    Assert.Contains(header.Name, responseAssertOptions.Headers);
                    Assert.Equal(responseAssertOptions.Headers[header.Name], header.Value);
                }))
            ]),
            () => Assert.NotNull(response.Headers.Date),
            () => Assert.InRange(response.Headers.Date!.Value, utcNow.AddSeconds(-3), utcNow.AddMinutes(1)),

            () => Assert.NotNull(response.Headers.RequestId),
            () => Assert.True(Guid.TryParseExact(response.Headers.RequestId, "D", out _))
        );
    }

    private static void AssertSuccessfulJsonContent(Response response, SuccessfulResponseAssertOptions responseAssertOptions)
    {
        Assert.NotNull(response.Content);
        Assert.NotNull(response.ContentStream);
        var contentReader = new StreamReader(response.Content.ToStream());
        var content = contentReader.ReadToEnd();

        if (responseAssertOptions.StatusCode == HttpStatusCode.NoContent)
            Assert.Empty(content);
        else
        {
            var jsonContent = JsonSerializer.Deserialize<JsonObject>(content)!;

            Assert.Equal(responseAssertOptions.Content.Count, jsonContent.Count);
            foreach (var jsonContentProperty in jsonContent)
            {
                Assert.Contains(jsonContentProperty.Key, responseAssertOptions.Content);
                Assert.Equal(responseAssertOptions.Content[jsonContentProperty.Key], jsonContentProperty.Value?.GetValue<string>());
            }
        }
    }

    private static void AssertUnsuccessfulJsonContent(Response response, UnsuccessfulResponseAssertOptions responseAssertOptions)
    {
        Assert.Multiple(
            () => Assert.NotNull(response.Content),
            () => Assert.NotNull(response.ContentStream)
        );

        var contentStreamReader = new StreamReader(response.Content.ToStream());
        var content = contentStreamReader.ReadToEnd();
        Assert.NotEmpty(content);

        var jsonContent = JsonSerializer.Deserialize<JsonObject>(content)!;
        Assert.Single(jsonContent);
        Assert.Contains("odata.error", jsonContent);

        var jsonContentOdataError = jsonContent["odata.error"]!.AsObject();
        Assert.Equal(2, jsonContentOdataError.Count);
        Assert.Contains("code", jsonContentOdataError);
        Assert.Equal(responseAssertOptions.ErrorCode, jsonContentOdataError["code"]!.GetValue<string>());

        Assert.Contains("message", jsonContentOdataError);
        var jsonContentOdataErrorMessage = jsonContentOdataError["message"]!.AsObject();
        Assert.Equal(2, jsonContentOdataErrorMessage.Count);
        Assert.Contains("lang", jsonContentOdataErrorMessage);
        Assert.Equal("en-US", jsonContentOdataErrorMessage["lang"]!.GetValue<string>());
        Assert.Contains("value", jsonContentOdataErrorMessage);
        var jsonContentOdataErrorMessageValue = jsonContentOdataErrorMessage["value"]!.GetValue<string>();
        Assert.Equal(
            $"{responseAssertOptions.ErrorDescription}\nRequestId:{response.Headers.RequestId}\nTime:",
            jsonContentOdataErrorMessageValue[..^DateTimeFormat.Length]
        );
        var jsonContentOdataErrorMessageTime = DateTimeOffset.ParseExact(jsonContentOdataErrorMessageValue[^DateTimeFormat.Length..], DateTimeFormat, CultureInfo.InvariantCulture);
        Assert.True(response.Headers.Date <= jsonContentOdataErrorMessageTime);
    }

    private static void AssertException(RequestFailedException exception, UnsuccessfulResponseAssertOptions responseAssertOptions)
    {
        Assert.Multiple(
            () => Assert.Equal("Azure.Data.Tables", exception.Source),
            () => Assert.Null(exception.HelpLink),
            () => Assert.Equal(-2146233088, exception.HResult),
            () => Assert.Null(exception.InnerException),
            () => Assert.Empty(exception.Data),
            () => Assert.Equal(responseAssertOptions.ErrorCode, exception.ErrorCode),
            () => Assert.Equal(responseAssertOptions.StatusCode, (HttpStatusCode)exception.Status),
            () => AssertExceptionMessage(exception, responseAssertOptions)
        );
    }

    private static void AssertExceptionMessage(RequestFailedException exception, UnsuccessfulResponseAssertOptions responseAssertOptions)
    {
        var utcNow = DateTimeOffset.UtcNow;
        var spelledOutStatusCode = Regex.Replace(responseAssertOptions.StatusCode.ToString(), "(?<=[a-z])[A-Z]", " $0");
        var rawResponse = exception.GetRawResponse()!;

        var contentStreamReader = new StreamReader(rawResponse.Content.ToStream());
        JsonObject jsonContent = JsonSerializer.Deserialize<JsonObject>(contentStreamReader.ReadToEnd())!;

        var jsonContentOdataErrorMessageValue = jsonContent["odata.error"]!["message"]!["value"]!.GetValue<string>();
        var jsonContentOdataErrorMessageTime = DateTimeOffset.ParseExact(jsonContentOdataErrorMessageValue[^DateTimeFormat.Length..], DateTimeFormat, CultureInfo.InvariantCulture);

        Assert.Multiple(
            () => Assert.Equal(
                $@"{responseAssertOptions.ErrorDescription}
RequestId:{rawResponse.Headers.RequestId}
Time:{jsonContentOdataErrorMessageTime.ToString(DateTimeFormat, CultureInfo.InvariantCulture)}",
                jsonContentOdataErrorMessageValue
            ),
            () => Assert.True(rawResponse.Headers.Date <= jsonContentOdataErrorMessageTime),
            () => Assert.InRange(jsonContentOdataErrorMessageTime, utcNow.AddSeconds(-3), utcNow.AddMinutes(1)),

            () => Assert.Equal(
                $@"{responseAssertOptions.ErrorDescription}
RequestId:{rawResponse.Headers.RequestId}
Time:{jsonContentOdataErrorMessageTime.ToString(DateTimeFormat, CultureInfo.InvariantCulture)}
Status: {responseAssertOptions.StatusCode:D} ({spelledOutStatusCode})
ErrorCode: {responseAssertOptions.ErrorCode}

Content:
{JsonSerializer.Serialize(jsonContent, new JsonSerializerOptions { WriteIndented = false })}

Headers:
{string.Join("\n",
    from header in rawResponse!.Headers
    let headerValue = (_nonRedactedHeaderNames.Contains(header.Name) ? header.Value : "REDACTED")
    select $"{header.Name}: {headerValue}"
)}
".Replace("\r", string.Empty),
                exception.Message
            )
        );
    }

    public abstract class ResponseAssertOptions
    {
        public required HttpStatusCode StatusCode { get; init; }
        public required IDictionary<string, string?> Headers { get; init; }
    }

    public class SuccessfulResponseAssertOptions : ResponseAssertOptions
    {
        public IDictionary<string, string> Content { get; } = new Dictionary<string, string>();
    }

    public class UnsuccessfulResponseAssertOptions : ResponseAssertOptions
    {
        public required string ErrorCode { get; init; }
        public required string ErrorDescription { get; init; }
    }

    public class DefaultHeaders : Dictionary<string, string?>
    {
        public DefaultHeaders(Response? response)
        {
            Add("Cache-Control", "no-cache");
            Add("Server", "Windows-Azure-Table/1.0 Microsoft-HTTPAPI/2.0");
            Add("x-ms-version", "2020-12-06");
            Add("X-Content-Type-Options", "nosniff");
            Add("x-ms-request-id", response?.Headers.RequestId);
            Add("x-ms-client-request-id", response?.ClientRequestId);
            Add("Date", response?.Headers.Date?.ToString("R"));

            Add("Transfer-Encoding", "chunked");
            Add("Content-Type", "application/json;odata=minimalmetadata;streaming=true;charset=utf-8");
        }
    }

    public class XmlContentHeaders : DefaultHeaders
    {
        public XmlContentHeaders(Response response) : base(response)
        {
            this["Content-Type"] = "application/xml";

            Remove("Cache-Control");
            Remove("X-Content-Type-Options");
        }
    }

    public class NoContentHeaders : DefaultHeaders
    {
        public NoContentHeaders(Response response) : base(response)
        {
            Add("Content-Length", "0");

            Remove("Transfer-Encoding");
            Remove("Content-Type");
        }
    }
}