using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;
using Azure;

namespace CloudStub.AzureDataTables.Tests;

internal static class Assertions
{
    private const string DateTimeFormat = "yyyy-MM-ddTHH:mm:ss.fffffffZ";
    private const string DateTimeValueFormat = "yyyy-MM-ddTHH:mm:ss.FFFFFFFZ";
    private static readonly IReadOnlyCollection<string> _nonRedactedHeaderNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "Cache-Control",
        "Transfer-Encoding",
        "Server",
        "x-ms-request-id",
        "x-ms-client-request-id",
        "Date",
        "Content-Type",
        "Content-Length"
    };

    public static Response EmptyResponse(Response? response, ResponseAssertOptions responseAssertOptions)
    {
        Assert.NotNull(response);
        Assert.Multiple(
            () => AssertInfo(response, responseAssertOptions),
            () => AssertHeaders(response, responseAssertOptions),
            () => AssertEmptyContent(response)
        );

        return response;
    }

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
            () => AssertInfo(response, responseAssertOptions, responseAssertOptions.ErrorPhrase),
            () => AssertHeaders(response, responseAssertOptions),
            () => AssertUnsuccessfulJsonContent(response, responseAssertOptions)
        );

        return response;
    }

    public static Response SuccessfulXmlResponse(Response? response, SuccessfulResponseAssertOptions responseAssertOptions)
    {
        Assert.NotNull(response);
        Assert.Multiple(
            () => AssertInfo(response, responseAssertOptions),
            () => AssertHeaders(response, responseAssertOptions),
            () => AssertSuccessfulXmlContent(response, responseAssertOptions)
        );

        return response;
    }

    public static Response UnsuccessfulXmlResponse(Response? response, UnsuccessfulResponseAssertOptions responseAssertOptions)
    {
        Assert.NotNull(response);
        Assert.Multiple(
            () => AssertInfo(response, responseAssertOptions, responseAssertOptions.ErrorPhrase),
            () => AssertHeaders(response, responseAssertOptions),
            () => AssertUnsuccessfulXmlContent(response, responseAssertOptions)
        );

        return response;
    }

    public static async Task<RequestFailedException> JsonResponseThrowsAsync(Func<Task> action, Func<Response?, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
    {
        var exception = await Assert.ThrowsAsync<RequestFailedException>(action);
        var rawResponse = exception.GetRawResponse();

        var responseAssertOptions = responseAssertOptionsFactory(exception.GetRawResponse());

        Assert.Multiple(
            () => AssertExceptionInfo(exception, responseAssertOptions),
            () => AssertJsonExceptionMessage(exception, responseAssertOptions),
            () => Assert.True(rawResponse?.IsError),
            () => UnsuccessfulJsonResponse(rawResponse, responseAssertOptions)
        );

        return exception;
    }

    public static RequestFailedException JsonResponseThrows(Action action, Func<Response?, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
    {
        var exception = Assert.Throws<RequestFailedException>(action);
        var rawResponse = exception.GetRawResponse();

        var responseAssertOptions = responseAssertOptionsFactory(exception.GetRawResponse());

        Assert.Multiple(
            () => AssertExceptionInfo(exception, responseAssertOptions),
            () => AssertJsonExceptionMessage(exception, responseAssertOptions),
            () => Assert.True(rawResponse?.IsError),
            () => UnsuccessfulJsonResponse(rawResponse, responseAssertOptions)
        );

        return exception;
    }

    public static async Task<RequestFailedException> XmlResponseThrowsAsync(Func<Task> action, Func<Response?, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
    {
        var exception = await Assert.ThrowsAsync<RequestFailedException>(action);
        var rawResponse = exception.GetRawResponse();

        var responseAssertOptions = responseAssertOptionsFactory(exception.GetRawResponse());

        Assert.Multiple(
            () => AssertExceptionInfo(exception, responseAssertOptions),
            () => AssertXmlExceptionMessage(exception, responseAssertOptions),
            () => Assert.True(rawResponse?.IsError),
            () => UnsuccessfulXmlResponse(rawResponse, responseAssertOptions)
        );

        return exception;
    }

    public static RequestFailedException XmlResponseThrows(Action action, Func<Response?, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
    {
        var exception = Assert.Throws<RequestFailedException>(action);
        var rawResponse = exception.GetRawResponse();

        var responseAssertOptions = responseAssertOptionsFactory(exception.GetRawResponse());

        Assert.Multiple(
            () => AssertExceptionInfo(exception, responseAssertOptions),
            () => AssertXmlExceptionMessage(exception, responseAssertOptions),
            () => Assert.True(rawResponse?.IsError),
            () => UnsuccessfulXmlResponse(rawResponse, responseAssertOptions)
        );

        return exception;
    }

    private static void AssertInfo(Response response, ResponseAssertOptions responseAssertOptions, string? reasonPhrase = null)
    {
        var spelledOutStatusCode = Regex.Replace(responseAssertOptions.StatusCode.ToString(), "(?<=[a-z])[A-Z]", " $0");

        Assert.Multiple(
            () => Assert.Equal((int)responseAssertOptions.StatusCode, response.Status),
            () => Assert.Equal(reasonPhrase ?? spelledOutStatusCode, response.ReasonPhrase),
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
            () =>
            {
                Assert.False(response.Headers.TryGetValue("non-existent-header", out var value));
                Assert.Null(value);
            },
            () =>
            {
                Assert.False(response.Headers.TryGetValues("non-existent-header", out var values));
                Assert.Null(values);
            },
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

    private static void AssertEmptyContent(Response response)
    {
        Assert.NotNull(response.Content);
        Assert.NotNull(response.ContentStream);
        using var contentReader = new StreamReader(response.Content.ToStream());
        var content = contentReader.ReadToEnd();

        Assert.Empty(content);
    }

    private static void AssertSuccessfulJsonContent(Response response, SuccessfulResponseAssertOptions responseAssertOptions)
    {
        Assert.NotNull(response.Content);
        Assert.NotNull(response.ContentStream);
        using var contentReader = new StreamReader(response.Content.ToStream());
        var content = contentReader.ReadToEnd();
        Assert.NotEmpty(content);

        var jsonContent = JsonSerializer.Deserialize<JsonObject>(content)!;

        var toCheck = new Queue<(JsonObject Element, IReadOnlyDictionary<string, object>)>([(jsonContent, responseAssertOptions.Content)]);
        do
        {
            var (jsonObject, expectedItem) = toCheck.Dequeue();

            Assert.Equal(expectedItem.Count, jsonObject.Count());
            foreach (var expectedChild in expectedItem)
            {
                var propertyValue = jsonObject[expectedChild.Key];

                if (expectedChild.Value is IReadOnlyDictionary<string, object> expectedElementChildren)
                    toCheck.Enqueue((propertyValue!.AsObject(), expectedElementChildren));
                else if (expectedChild.Value is IEnumerable<object> expectedList)
                {
                    Assert.Equal(expectedList.Count(), propertyValue!.AsArray().Count);

                    foreach (var (listItem, expectedListItem) in propertyValue.AsArray().Zip(expectedList, (listItem, expectedListItem) => (listItem, expectedListItem)))
                        toCheck.Enqueue((listItem!.AsObject(), (IReadOnlyDictionary<string, object>)expectedListItem));
                }
                else if (expectedChild.Value is bool @bool)
                    Assert.Equal(@bool, propertyValue?.GetValue<bool>());
                else if (expectedChild.Value is Guid guid)
                    Assert.Equal(guid, propertyValue?.GetValue<Guid>());

                else if (expectedChild.Value is DateTime dateTime)
                    Assert.Equal(dateTime.ToString(DateTimeValueFormat), propertyValue?.GetValue<string>());
                else if (expectedChild.Value is DateTimeOffset dateTimeOffset)
                    Assert.Equal(dateTimeOffset.ToString(DateTimeValueFormat), propertyValue?.GetValue<string>());

                else if (expectedChild.Value is int @int)
                    Assert.Equal(@int, propertyValue?.GetValue<int>());
                else if (expectedChild.Value is long @long)
                    Assert.Equal(@long.ToString(), propertyValue?.GetValue<string>());
                else if (expectedChild.Value is float @float)
                    Assert.Equal(@float, propertyValue?.GetValue<float>());
                else if (expectedChild.Value is double @double)
                    Assert.Equal(@double, propertyValue?.GetValue<double>());

                else if (expectedChild.Value is byte[] byteArray)
                    Assert.Equal(Convert.ToBase64String(byteArray), propertyValue?.GetValue<string>());

                else if (expectedChild.Value is ETag eTag)
                    Assert.Equal(eTag.ToString(), propertyValue?.GetValue<string>());

                else
                    Assert.Equal(expectedChild.Value, propertyValue?.GetValue<string>());
            }
        } while (toCheck.Count > 0);
    }

    private static void AssertUnsuccessfulJsonContent(Response response, UnsuccessfulResponseAssertOptions responseAssertOptions)
    {
        Assert.Multiple(
            () => Assert.NotNull(response.Content),
            () => Assert.NotNull(response.ContentStream)
        );

        using var contentStreamReader = new StreamReader(response.Content.ToStream());
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

    private static void AssertSuccessfulXmlContent(Response response, SuccessfulResponseAssertOptions responseAssertOptions)
    {
        Assert.NotNull(response.Content);
        Assert.NotNull(response.ContentStream);
        using var contentReader = new StreamReader(response.Content.ToStream());
        var content = contentReader.ReadToEnd();
        Assert.NotEmpty(content);

        using var xmlReader = XmlReader.Create(new StringReader(content));
        var xmlContent = XDocument.Load(xmlReader);

        var toCheck = new Queue<(XContainer Element, IReadOnlyDictionary<string, object>)>([(xmlContent, responseAssertOptions.Content)]);
        do
        {
            var (xmlElement, expectedItem) = toCheck.Dequeue();

            Assert.Equal(expectedItem.Count, xmlElement.Elements().Count());
            foreach (var expectedChild in expectedItem)
            {
                var childXmlElement = xmlElement.Element(expectedChild.Key);
                Assert.NotNull(childXmlElement);

                if (expectedChild.Value is IReadOnlyDictionary<string, object> expectedElementChildren)
                    toCheck.Enqueue((childXmlElement, expectedElementChildren));
                else
                    Assert.Equal(expectedChild.Value, childXmlElement.Value);
            }
        } while (toCheck.Count > 0);
    }

    private static void AssertUnsuccessfulXmlContent(Response response, UnsuccessfulResponseAssertOptions responseAssertOptions)
    {
        Assert.Multiple(
            () => Assert.NotNull(response.Content),
            () => Assert.NotNull(response.ContentStream)
        );

        using var contentStreamReader = new StreamReader(response.Content.ToStream());
        var content = contentStreamReader.ReadToEnd();
        Assert.NotEmpty(content);

        using var xmlReader = XmlReader.Create(new StringReader(content));
        var xmlContent = XDocument.Load(xmlReader);

        Assert.NotNull(xmlContent.Root);
        Assert.Equal("http://schemas.microsoft.com/ado/2007/08/dataservices/metadata", xmlContent.Root.GetNamespaceOfPrefix("m"));

        Assert.Multiple(
            () =>
            {
                var codeXmlElement = xmlContent.Root.Element(XName.Get("code", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"));

                Assert.NotNull(codeXmlElement);
                Assert.Equal(responseAssertOptions.ErrorCode, codeXmlElement.Value);
            },
            () =>
            {
                var errorMessageXmlElement = xmlContent.Root.Element(XName.Get("message", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"));

                Assert.NotNull(errorMessageXmlElement);
                Assert.Multiple(
                    () =>
                    {
                        var langAttr = errorMessageXmlElement.Attribute(XName.Get("lang", "http://www.w3.org/XML/1998/namespace"));
                        Assert.NotNull(langAttr);
                        Assert.Equal("en-US", langAttr.Value);
                    },
                    () =>
                    {
                        Assert.Equal(
                            $"{responseAssertOptions.ErrorDescription}\nRequestId:{response.Headers.RequestId}\nTime:",
                            errorMessageXmlElement.Value[..^DateTimeFormat.Length]
                        );

                        var errorMessageTime = DateTimeOffset.ParseExact(errorMessageXmlElement.Value[^DateTimeFormat.Length..], DateTimeFormat, CultureInfo.InvariantCulture);
                        Assert.True(response.Headers.Date <= errorMessageTime);
                    }
                );
            }
        );
    }

    private static void AssertExceptionInfo(RequestFailedException exception, UnsuccessfulResponseAssertOptions responseAssertOptions)
    {
        Assert.Multiple(
            () => Assert.Equal("Azure.Data.Tables", exception.Source),
            () => Assert.Null(exception.HelpLink),
            () => Assert.Equal(-2146233088, exception.HResult),
            () => Assert.Null(exception.InnerException),
            () => Assert.Empty(exception.Data),
            () => Assert.Equal(responseAssertOptions.ExceptiopnErrorCode, exception.ErrorCode),
            () => Assert.Equal(responseAssertOptions.StatusCode, (HttpStatusCode)exception.Status)
        );
    }

    private static void AssertJsonExceptionMessage(RequestFailedException exception, UnsuccessfulResponseAssertOptions responseAssertOptions)
    {
        var utcNow = DateTimeOffset.UtcNow;
        var spelledOutStatusCode = Regex.Replace(responseAssertOptions.StatusCode.ToString(), "(?<=[a-z])[A-Z]", " $0");
        var rawResponse = exception.GetRawResponse()!;

        var contentStreamReader = new StreamReader(rawResponse.Content.ToStream());
        var content = contentStreamReader.ReadToEnd();
        Assert.NotEmpty(content);

        var jsonContent = JsonSerializer.Deserialize<JsonObject>(content);
        Assert.NotNull(jsonContent);

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
{content}

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

    private static void AssertXmlExceptionMessage(RequestFailedException exception, UnsuccessfulResponseAssertOptions responseAssertOptions)
    {
        var utcNow = DateTimeOffset.UtcNow;
        var spelledOutStatusCode = Regex.Replace(responseAssertOptions.StatusCode.ToString(), "(?<=[a-z])[A-Z]", " $0");
        var rawResponse = exception.GetRawResponse()!;

        using var contentStreamReader = new StreamReader(rawResponse.Content.ToStream());
        var content = contentStreamReader.ReadToEnd();
        Assert.NotEmpty(content);

        using var xmlReader = XmlReader.Create(new StringReader(content));
        var xmlContent = XDocument.Load(xmlReader);
        Assert.NotNull(xmlContent.Root);

        var xmlErrorMessageElement = xmlContent.Root.Element(XName.Get("message", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"));
        Assert.NotNull(xmlErrorMessageElement);

        var xmlErrorMessage = xmlErrorMessageElement.Value;
        var xmlErrorMessageTime = DateTimeOffset.ParseExact(xmlErrorMessage[^DateTimeFormat.Length..], DateTimeFormat, CultureInfo.InvariantCulture);

        Assert.Multiple(
            () => Assert.Equal(
                $@"{responseAssertOptions.ErrorDescription}
RequestId:{rawResponse.Headers.RequestId}
Time:{xmlErrorMessageTime.ToString(DateTimeFormat, CultureInfo.InvariantCulture)}",
                xmlErrorMessage
            ),
            () => Assert.True(rawResponse.Headers.Date <= xmlErrorMessageTime),
            () => Assert.InRange(xmlErrorMessageTime, utcNow.AddSeconds(-3), utcNow.AddMinutes(1)),

            () => Assert.Equal(
                $@"Service request failed.
Status: {responseAssertOptions.StatusCode:D} ({responseAssertOptions.ErrorPhrase ?? spelledOutStatusCode})

Content:
{content}

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

    public class ResponseAssertOptions
    {
        public required HttpStatusCode StatusCode { get; init; }
        public required IDictionary<string, string?> Headers { get; init; }
    }

    public class SuccessfulResponseAssertOptions : ResponseAssertOptions
    {
        public Dictionary<string, object> Content { get; } = [];
    }

    public class UnsuccessfulResponseAssertOptions : ResponseAssertOptions
    {
        private bool _exceptionErrorCodeSet = false;

        public required string ErrorCode { get; init; }

        public string? ExceptiopnErrorCode
        {
            get => _exceptionErrorCodeSet ? field : ErrorCode;
            set
            {
                field = value;
                _exceptionErrorCodeSet = true;
            }
        }

        public required string ErrorDescription { get; init; }

        public string? ErrorPhrase { get; set; }
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
        public XmlContentHeaders(Response? response) : base(response)
        {
            this["Content-Type"] = "application/xml";

            Remove("Cache-Control");
            Remove("X-Content-Type-Options");
        }
    }

    public class NoContentHeaders : DefaultHeaders
    {
        public NoContentHeaders(Response? response) : base(response)
        {
            Add("Content-Length", "0");

            Remove("Transfer-Encoding");
            Remove("Content-Type");
        }
    }

    public class AcceptedHeaders : DefaultHeaders
    {
        public AcceptedHeaders(Response? response) : base(response)
        {
            Remove("Cache-Control");
            Remove("X-Content-Type-Options");
            Remove("Content-Type");
        }
    }
}