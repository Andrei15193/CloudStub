using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Azure;
using Azure.Data.Tables;
using Xunit;

namespace CloudStub.AzureDataTables.Tests
{
    internal static class Assertions
    {
        public const string DateTimeFormat = "yyyy-MM-ddTHH:mm:ss.fffffffZ";
        public const string DateTimeValueFormat = "yyyy-MM-ddTHH:mm:ss.FFFFFFFZ";
        public const string ETagDateTimeFormat = @"'W/""datetime\''" + DateTimeValueFormat + @"'\'""'";
        private static readonly IReadOnlyCollection<string> _nonRedactedHeaderNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "Connection",
            "Cache-Control",
            "Transfer-Encoding",
            "Server",
            "x-ms-request-id",
            "x-ms-client-request-id",
            "Date",
            "Content-Type",
            "Content-Length"
        };

        public static Response EmptyResponse(Response response, ResponseAssertOptions responseAssertOptions)
        {
            Assert.NotNull(response);
            Assert.Multiple(
                () => AssertInfo(response, responseAssertOptions),
                () => AssertHeaders(response, responseAssertOptions),
                () => AssertEmptyContent(response)
            );

            return response;
        }

        public static Response TableTransactionResponse(Response response, ResponseAssertOptions responseAssertOptions, IEnumerable<Response> operationResponses)
        {
            Assert.NotNull(response);
            Assert.Multiple(
                () => AssertInfo(response, responseAssertOptions),
                () => Assert.Multiple(
                    () => AssertHeaders(response, responseAssertOptions),
                    () => Assert.StartsWith("multipart/mixed; boundary=batchresponse_", response.Headers.ContentType),
                    () => Assert.True(Guid.TryParseExact(response.Headers.ContentType.Substring("multipart/mixed; boundary=batchresponse_".Length), "D", out var _))
                ),
                () => AssertTableTransactionContent(response, operationResponses)
            );

            return response;
        }

        public static Response SuccessfulJsonResponse(Response response, SuccessfulResponseAssertOptions responseAssertOptions)
        {
            Assert.NotNull(response);
            Assert.Multiple(
                () => AssertInfo(response, responseAssertOptions),
                () => AssertHeaders(response, responseAssertOptions),
                () => AssertSuccessfulJsonContent(response, responseAssertOptions)
            );

            return response;
        }

        public static Response UnsuccessfulJsonResponse(Response response, UnsuccessfulResponseAssertOptions responseAssertOptions)
        {
            Assert.NotNull(response);
            Assert.Multiple(
                () => AssertInfo(response, responseAssertOptions, responseAssertOptions.ErrorPhrase),
                () => AssertHeaders(response, responseAssertOptions),
                () => AssertUnsuccessfulJsonContent(response, responseAssertOptions)
            );

            return response;
        }

        public static Response SuccessfulXmlResponse(Response response, SuccessfulResponseAssertOptions responseAssertOptions)
        {
            Assert.NotNull(response);
            Assert.Multiple(
                () => AssertInfo(response, responseAssertOptions),
                () => AssertHeaders(response, responseAssertOptions),
                () => AssertSuccessfulXmlContent(response, responseAssertOptions)
            );

            return response;
        }

        public static Response UnsuccessfulXmlResponse(Response response, UnsuccessfulResponseAssertOptions responseAssertOptions)
        {
            Assert.NotNull(response);
            Assert.Multiple(
                () => AssertInfo(response, responseAssertOptions, responseAssertOptions.ErrorPhrase),
                () => AssertHeaders(response, responseAssertOptions),
                () => AssertUnsuccessfulXmlContent(response, responseAssertOptions)
            );

            return response;
        }

        public static Response InvalidUrlResponse(Response response, UnsuccessfulResponseAssertOptions responseAssertOptions)
        {
            Assert.NotNull(response);
            Assert.Multiple(
                () => AssertInfo(response, responseAssertOptions, responseAssertOptions.ErrorPhrase),
                () => AssertHeaders(response, responseAssertOptions),
                () => AssertInvalidUrlContent(response, responseAssertOptions)
            );

            return response;
        }

        public static async Task<RequestFailedException> JsonResponseThrowsAsync(Func<Task> action, Func<Response, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
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

        public static async Task<TableTransactionFailedException> TransactionJsonResponseThrowsAsync(Func<Task> action, Func<Response, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
        {
            var exception = await Assert.ThrowsAsync<TableTransactionFailedException>(action);
            var rawResponse = exception.GetRawResponse();

            var responseAssertOptions = responseAssertOptionsFactory(exception.GetRawResponse());

            Assert.Multiple(
                () => AssertExceptionInfo(exception, responseAssertOptions),
                () => AssertTransactionExceptionInfo(exception, responseAssertOptions),
                () => Assert.Null(rawResponse),
                () => Assert.Equal(responseAssertOptions.FailedEntityIndex, exception.FailedTransactionActionIndex)
            );

            return exception;
        }

        public static RequestFailedException JsonResponseThrows(Action action, Func<Response, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
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

        public static TableTransactionFailedException TransactionJsonResponseThrows(Action action, Func<Response, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
        {
            var exception = Assert.Throws<TableTransactionFailedException>(action);
            var rawResponse = exception.GetRawResponse();

            var responseAssertOptions = responseAssertOptionsFactory(exception.GetRawResponse());

            Assert.Multiple(
                () => AssertExceptionInfo(exception, responseAssertOptions),
                () => AssertTransactionExceptionInfo(exception, responseAssertOptions),
                () => Assert.Null(rawResponse),
                () => Assert.Equal(responseAssertOptions.FailedEntityIndex, exception.FailedTransactionActionIndex)
            );

            return exception;
        }

        public static async Task<RequestFailedException> XmlResponseThrowsAsync(Func<Task> action, Func<Response, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
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

        public static RequestFailedException XmlResponseThrows(Action action, Func<Response, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
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

        public static async Task<RequestFailedException> InvalidUrlThrowsAsync(Func<Task> action, Func<Response, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
        {
            var exception = await Assert.ThrowsAsync<RequestFailedException>(action);
            var rawResponse = exception.GetRawResponse();

            var responseAssertOptions = responseAssertOptionsFactory(exception.GetRawResponse());

            Assert.Multiple(
                () => AssertExceptionInfo(exception, responseAssertOptions),
                () => AssertInvalidUrlExceptionMessage(exception, responseAssertOptions),
                () => Assert.True(rawResponse?.IsError),
                () => InvalidUrlResponse(rawResponse, responseAssertOptions)
            );

            return exception;
        }

        public static RequestFailedException InvalidUrlThrows(Action action, Func<Response, UnsuccessfulResponseAssertOptions> responseAssertOptionsFactory)
        {
            var exception = Assert.Throws<RequestFailedException>(action);
            var rawResponse = exception.GetRawResponse();

            var responseAssertOptions = responseAssertOptionsFactory(exception.GetRawResponse());

            Assert.Multiple(
                () => AssertExceptionInfo(exception, responseAssertOptions),
                () => AssertInvalidUrlExceptionMessage(exception, responseAssertOptions),
                () => Assert.True(rawResponse?.IsError),
                () => InvalidUrlResponse(rawResponse, responseAssertOptions)
            );

            return exception;
        }

        private static void AssertInfo(Response response, ResponseAssertOptions responseAssertOptions, string reasonPhrase = null)
        {
            var spelledOutStatusCode = Regex.Replace(responseAssertOptions.StatusCode.ToString(), "(?<=[a-z])[A-Z]", " $0");

            Assert.Multiple(
                () => Assert.Equal(responseAssertOptions.StatusCode, (HttpStatusCode)response.Status),
                () => Assert.Equal(reasonPhrase ?? spelledOutStatusCode, response.ReasonPhrase),
                () =>
                {
                    if (responseAssertOptions.WithoutClientRequestId)
                        Assert.Null(response.ClientRequestId);
                    else
                    {
                        Assert.NotNull(response.ClientRequestId);
                        Assert.True(Guid.TryParseExact(response.ClientRequestId, "D", out _), "Expected ClientRequestId to be a valid GUID.");
                    }
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
                () => Assert.Multiple(
                    response
                        .Headers
                        .Select(header => new Action(() =>
                        {
                            Assert.Contains(header.Name, responseAssertOptions.Headers);
                            Assert.Equal(responseAssertOptions.Headers[header.Name], header.Value);
                        }))
                        .ToArray()
                ),
                () =>
                {
                    if (!responseAssertOptions.WithoutDate)
                        Assert.NotNull(response.Headers.Date);
                },
                () =>
                {
                    if (!responseAssertOptions.WithoutDate)
                        Assert.InRange(response.Headers.Date.Value, utcNow.AddSeconds(-3), utcNow.AddMinutes(1));
                },

                () =>
                {
                    if (response.Headers.TryGetValue("ETag", out var eTag))
                    {
                        Assert.True(
                            DateTime.TryParseExact(
                                Uri.UnescapeDataString(eTag),
                                ETagDateTimeFormat,
                                CultureInfo.InvariantCulture,
                                DateTimeStyles.None,
                                out var eTagDateTime
                            ),
                            "Expected ETag header value to be in the format " + ETagDateTimeFormat);

                        Assert.Multiple(
                            () => Assert.InRange(eTagDateTime, utcNow.AddSeconds(-3), utcNow.AddMinutes(1)),
                            () => Assert.NotEqual(eTagDateTime, response.Headers.Date)
                        );
                    }
                },

                () =>
                {
                    if (responseAssertOptions.WithoutRequestId)
                        Assert.Null(response.Headers.RequestId);
                    else
                        Assert.NotNull(response.Headers.RequestId);
                },
                () =>
                {
                    if (!responseAssertOptions.WithoutRequestId)
                        Assert.True(Guid.TryParseExact(response.Headers.RequestId, "D", out _));
                }
            );
        }

        private static void AssertEmptyContent(Response response)
        {
            Assert.NotNull(response.Content);
            Assert.NotNull(response.ContentStream);

            using (var contentReader = new StreamReader(response.Content.ToStream()))
            {
                var content = contentReader.ReadToEnd();
                Assert.Empty(content);
            }
        }

        private static void AssertTableTransactionContent(Response response, IEnumerable<Response> operationResponses)
        {
            Assert.NotNull(response.Content);
            Assert.NotNull(response.ContentStream);

            using (var contentReader = new StreamReader(response.Content.ToStream()))
            {
                var content = contentReader.ReadToEnd();
                var contentLines = content.Split("\r\n");

                Assert.Multiple(
                    () =>
                    {
                        var batchBoundaryTag = response.Headers.ContentType.Substring("multipart/mixed; boundary=".Length);
                        Assert.Equal($"--{batchBoundaryTag}", contentLines.First());

                        Assert.Equal($"--{batchBoundaryTag}--", contentLines.ElementAt(contentLines.Length - 2));
                        Assert.Empty(contentLines.Last());
                    },
                    () =>
                    {
                        Assert.StartsWith("Content-Type: multipart/mixed; boundary=changesetresponse_", contentLines.ElementAt(1));
                        Assert.True(Guid.TryParseExact(contentLines.ElementAt(1).Substring("Content-Type: multipart/mixed; boundary=changesetresponse_".Length), "D", out var _));
                        Assert.Empty(contentLines.ElementAt(2));

                        var operationBoundaryTag = contentLines.ElementAt(1).Substring("Content-Type: multipart/mixed; boundary=".Length);
                        Assert.Equal($"--{operationBoundaryTag}", contentLines.ElementAt(3));
                        Assert.Equal($"--{operationBoundaryTag}--", contentLines.ElementAt(contentLines.Length - 3));

                        var operationsContents = content
                            .Split($"--{operationBoundaryTag}--")
                            .First()
                            .Split($"--{operationBoundaryTag}")
                            .Skip(1);

                        Assert.Equal(operationResponses.Count(), operationsContents.Count());
                        foreach (var (operationResponse, actualOperationContent) in operationResponses.Zip(operationsContents, (operationResponse, operationContent) => (operationResponse, operationContent)))
                        {
                            var expectedOperationContent = string.Join(
                                "\r\n",
                                new[]
                                {
                                    string.Empty,
                                    "Content-Type: application/http",
                                    "Content-Transfer-Encoding: binary",
                                    string.Empty,
                                    "HTTP/1.1 204 No Content"
                                }
                                .Concat(operationResponse.Headers.Select(header => $"{header.Name}: {header.Value}"))
                                .Concat(new[]
                                {
                                    string.Empty,
                                    string.Empty,
                                    string.Empty
                                })
                            );
                            Assert.Equal(expectedOperationContent, actualOperationContent);
                        }
                    }
                );
            }
        }

        private static void AssertSuccessfulJsonContent(Response response, SuccessfulResponseAssertOptions responseAssertOptions)
        {
            Assert.NotNull(response.Content);
            Assert.NotNull(response.ContentStream);

            string content;
            using (var contentReader = new StreamReader(response.Content.ToStream()))
                content = contentReader.ReadToEnd();
            Assert.NotEmpty(content);

            var jsonContent = JsonSerializer.Deserialize<JsonObject>(content);

            var toCheck = new Queue<(JsonObject Element, IReadOnlyDictionary<string, object>)>();
            toCheck.Enqueue((jsonContent, responseAssertOptions.Content));

            do
            {
                var (jsonObject, expectedItem) = toCheck.Dequeue();

                Assert.Equal(expectedItem.Count, jsonObject.Count());
                foreach (var expectedChild in expectedItem)
                {
                    var propertyValue = jsonObject[expectedChild.Key];

                    if (expectedChild.Value is IReadOnlyDictionary<string, object> expectedElementChildren)
                        toCheck.Enqueue((propertyValue.AsObject(), expectedElementChildren));
                    else if (expectedChild.Value is IEnumerable<object> expectedList)
                    {
                        Assert.Equal(expectedList.Count(), propertyValue.AsArray().Count);

                        foreach (var (listItem, expectedListItem) in propertyValue.AsArray().Zip(expectedList, (listItem, expectedListItem) => (listItem, expectedListItem)))
                            toCheck.Enqueue((listItem.AsObject(), (IReadOnlyDictionary<string, object>)expectedListItem));
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
                        Assert.Equal(@long, long.Parse(propertyValue?.GetValue<string>(), NumberStyles.Integer));
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

            string content;
            using (var contentStreamReader = new StreamReader(response.Content.ToStream()))
                content = contentStreamReader.ReadToEnd();
            Assert.NotEmpty(content);

            var jsonContent = JsonSerializer.Deserialize<JsonObject>(content);
            Assert.Single(jsonContent);
            Assert.Contains("odata.error", jsonContent);

            var jsonContentOdataError = jsonContent["odata.error"].AsObject();
            Assert.Equal(2, jsonContentOdataError.Count);
            Assert.Contains("code", jsonContentOdataError);
            Assert.Equal(responseAssertOptions.ErrorCode, jsonContentOdataError["code"].GetValue<string>());

            Assert.Contains("message", jsonContentOdataError);
            var jsonContentOdataErrorMessage = jsonContentOdataError["message"].AsObject();
            Assert.Equal(2, jsonContentOdataErrorMessage.Count);
            Assert.Contains("lang", jsonContentOdataErrorMessage);
            Assert.Equal("en-US", jsonContentOdataErrorMessage["lang"].GetValue<string>());
            Assert.Contains("value", jsonContentOdataErrorMessage);
            var jsonContentOdataErrorMessageValue = jsonContentOdataErrorMessage["value"].GetValue<string>();
            Assert.Equal(
                $"{responseAssertOptions.ErrorDescription}\nRequestId:{response.Headers.RequestId}\nTime:",
                jsonContentOdataErrorMessageValue.Substring(0, jsonContentOdataErrorMessageValue.Length - DateTimeFormat.Length)
            );
            var jsonContentOdataErrorMessageTime = DateTimeOffset.ParseExact(jsonContentOdataErrorMessageValue.Substring(jsonContentOdataErrorMessageValue.Length - DateTimeFormat.Length), DateTimeFormat, CultureInfo.InvariantCulture);
            Assert.True(response.Headers.Date <= jsonContentOdataErrorMessageTime);
        }

        private static void AssertSuccessfulXmlContent(Response response, SuccessfulResponseAssertOptions responseAssertOptions)
        {
            Assert.NotNull(response.Content);
            Assert.NotNull(response.ContentStream);

            string content;
            using (var contentReader = new StreamReader(response.Content.ToStream()))
                content = contentReader.ReadToEnd();
            Assert.NotEmpty(content);

            using (var xmlReader = XmlReader.Create(new StringReader(content)))
            {
                var xmlContent = XDocument.Load(xmlReader);

                var toCheck = new Queue<(XContainer Element, IReadOnlyDictionary<string, object>)>();
                toCheck.Enqueue((xmlContent, responseAssertOptions.Content));
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
        }

        private static void AssertUnsuccessfulXmlContent(Response response, UnsuccessfulResponseAssertOptions responseAssertOptions)
        {
            Assert.Multiple(
                () => Assert.NotNull(response.Content),
                () => Assert.NotNull(response.ContentStream)
            );

            string content;
            using (var contentStreamReader = new StreamReader(response.Content.ToStream()))
                content = contentStreamReader.ReadToEnd();
            Assert.NotEmpty(content);

            using (var xmlReader = XmlReader.Create(new StringReader(content)))
            {
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
                                    errorMessageXmlElement.Value.Substring(0, errorMessageXmlElement.Value.Length - DateTimeFormat.Length)
                                );

                                var errorMessageTime = DateTimeOffset.ParseExact(errorMessageXmlElement.Value.Substring(errorMessageXmlElement.Value.Length - DateTimeFormat.Length), DateTimeFormat, CultureInfo.InvariantCulture);
                                Assert.True(response.Headers.Date <= errorMessageTime);
                            }
                        );
                    }
                );
            }
        }

        private static void AssertInvalidUrlContent(Response response, UnsuccessfulResponseAssertOptions responseAssertOptions)
        {
            string content;
            using (var contentStreamReader = new StreamReader(response.Content.ToStream()))
                content = contentStreamReader.ReadToEnd();

            Assert.Equal(
                responseAssertOptions.ErrorDescription,
                content
            );
        }

        private static void AssertExceptionInfo(RequestFailedException exception, UnsuccessfulResponseAssertOptions responseAssertOptions)
        {
            Assert.Multiple(
                () => Assert.Equal("Azure.Data.Tables", exception.Source),
                () => Assert.Null(exception.HelpLink),
                () => Assert.Equal(-2146233088, exception.HResult),
                () => Assert.Null(exception.InnerException),
                () =>
                {
                    if (responseAssertOptions.FailedEntityIndex.HasValue)
                        Assert.Multiple(
                            () => Assert.Single(exception.Data),
                            () => Assert.Equal(responseAssertOptions.FailedEntityIndex.ToString(), exception.Data["FailedEntity"])
                        );
                    else
                        Assert.Empty(exception.Data);
                },
                () => Assert.Equal(responseAssertOptions.ExceptionErrorCode, exception.ErrorCode),
                () => Assert.Equal(responseAssertOptions.StatusCode, (HttpStatusCode)exception.Status)
            );
        }

        private static void AssertTransactionExceptionInfo(TableTransactionFailedException exception, UnsuccessfulResponseAssertOptions responseAssertOptions)
        {
            var spelledOutStatusCode = Regex.Replace(responseAssertOptions.StatusCode.ToString(), "(?<=[a-z])[A-Z]", " $0");

            var exceptionMessageLines = exception.Message.Split('\n').AsEnumerable();
            if (exceptionMessageLines.ElementAt(0).EndsWith("'") && exceptionMessageLines.ElementAt(1).StartsWith("'"))
            {
                exceptionMessageLines = Enumerable
                    .Repeat(exceptionMessageLines.ElementAt(0) + "\n" + exceptionMessageLines.ElementAt(1), 1)
                    .Concat(exceptionMessageLines.Skip(2));
            }
            exceptionMessageLines = exceptionMessageLines.Take(3).Concat(Enumerable.Repeat(string.Join('\n', exceptionMessageLines.Skip(3)), 1));

            Assert.Collection(
                exceptionMessageLines,
                errorDescription => Assert.Equal(responseAssertOptions.ErrorDescription, errorDescription),
                requestIdInformation =>
                {
                    Assert.StartsWith("RequestId:", requestIdInformation);
                    Assert.True(Guid.TryParseExact(requestIdInformation.Substring("RequestId:".Length), "D", out var _));
                },
                requestTimeInformation =>
                {
                    Assert.StartsWith("Time:", requestTimeInformation);
                    Assert.True(DateTimeOffset.TryParseExact(requestTimeInformation.Substring("Time:".Length), DateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var _));
                },
                errorDetails =>
                {
                    if (responseAssertOptions.FailedEntityIndex.HasValue)
                        Assert.Equal(
                            string.Join(
                                '\n',
                                $" The index of the entity that caused the error can be found in FailedTransactionActionIndex.",
                                $"Status: {responseAssertOptions.StatusCode:D} ({spelledOutStatusCode})",
                                $"ErrorCode: {responseAssertOptions.ErrorCode}",
                                string.Empty,
                                "Additional Information:",
                                $"FailedEntity: {responseAssertOptions.FailedEntityIndex}",
                                string.Empty,
                                "Service request succeeded. Response content and headers are not included to avoid logging sensitive data.",
                                string.Empty
                            ),
                            errorDetails
                        );
                    else
                        Assert.Equal(
                            string.Join(
                                '\n',
                                $"Status: {responseAssertOptions.StatusCode:D} ({spelledOutStatusCode})",
                                $"ErrorCode: {responseAssertOptions.ErrorCode}",
                                string.Empty,
                                "Service request succeeded. Response content and headers are not included to avoid logging sensitive data.",
                                string.Empty
                            ),
                            errorDetails
                        );
                }
            );
        }

        private static void AssertJsonExceptionMessage(RequestFailedException exception, UnsuccessfulResponseAssertOptions responseAssertOptions)
        {
            var utcNow = DateTimeOffset.UtcNow;
            var spelledOutStatusCode = Regex.Replace(responseAssertOptions.StatusCode.ToString(), "(?<=[a-z])[A-Z]", " $0");
            var rawResponse = exception.GetRawResponse();

            var contentStreamReader = new StreamReader(rawResponse.Content.ToStream());
            var content = contentStreamReader.ReadToEnd();
            Assert.NotEmpty(content);

            var jsonContent = JsonSerializer.Deserialize<JsonObject>(content);
            Assert.NotNull(jsonContent);

            var jsonContentOdataErrorMessageValue = jsonContent["odata.error"]["message"]["value"].GetValue<string>();
            var jsonContentOdataErrorMessageTime = DateTimeOffset.ParseExact(jsonContentOdataErrorMessageValue.Substring(jsonContentOdataErrorMessageValue.Length - DateTimeFormat.Length), DateTimeFormat, CultureInfo.InvariantCulture);

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
                        responseAssertOptions.ErrorDescription + $@"
RequestId:{rawResponse.Headers.RequestId}
Time:{jsonContentOdataErrorMessageTime.ToString(DateTimeFormat, CultureInfo.InvariantCulture)}
Status: {responseAssertOptions.StatusCode:D} ({spelledOutStatusCode})
ErrorCode: {responseAssertOptions.ErrorCode}

Content:
{content}

Headers:
{string.Join("\n", from header in rawResponse.Headers
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
            var rawResponse = exception.GetRawResponse();

            string content;
            using (var contentStreamReader = new StreamReader(rawResponse.Content.ToStream()))
                content = contentStreamReader.ReadToEnd();
            Assert.NotEmpty(content);

            using (var xmlReader = XmlReader.Create(new StringReader(content)))
            {
                var xmlContent = XDocument.Load(xmlReader);
                Assert.NotNull(xmlContent.Root);

                var xmlErrorMessageElement = xmlContent.Root.Element(XName.Get("message", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"));
                Assert.NotNull(xmlErrorMessageElement);

                var xmlErrorMessage = xmlErrorMessageElement.Value;
                var xmlErrorMessageTime = DateTimeOffset.ParseExact(xmlErrorMessage.Substring(xmlErrorMessage.Length - DateTimeFormat.Length), DateTimeFormat, CultureInfo.InvariantCulture);

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
    from header in rawResponse.Headers
    let headerValue = (_nonRedactedHeaderNames.Contains(header.Name) ? header.Value : "REDACTED")
    select $"{header.Name}: {headerValue}"
)}
".Replace("\r", string.Empty),
                        exception.Message
                    )
                );
            }
        }

        private static void AssertInvalidUrlExceptionMessage(RequestFailedException exception, UnsuccessfulResponseAssertOptions responseAssertOptions)
        {
            var utcNow = DateTimeOffset.UtcNow;
            var spelledOutStatusCode = Regex.Replace(responseAssertOptions.StatusCode.ToString(), "(?<=[a-z])[A-Z]", " $0");
            var rawResponse = exception.GetRawResponse();

            string content;
            using (var contentStreamReader = new StreamReader(rawResponse.Content.ToStream()))
                content = contentStreamReader.ReadToEnd();
            Assert.NotEmpty(content);

            Assert.Equal(
                $@"Service request failed.
Status: {responseAssertOptions.StatusCode:D} ({responseAssertOptions.ErrorPhrase ?? spelledOutStatusCode})

Content:
".Replace("\r", string.Empty)
+ content
+ $@"

Headers:
{string.Join("\n",
from header in rawResponse.Headers
let headerValue = (_nonRedactedHeaderNames.Contains(header.Name) ? header.Value : "REDACTED")
select $"{header.Name}: {headerValue}"
)}
".Replace("\r", string.Empty),
                exception.Message
            );
        }

        public class ResponseAssertOptions
        {
            public bool WithoutRequestId { get; set; }
            public bool WithoutClientRequestId { get; set; }
            public bool WithoutDate { get; set; }
            public HttpStatusCode StatusCode { get; set; }
            public IDictionary<string, string> Headers { get; set; }
        }

        public class SuccessfulResponseAssertOptions : ResponseAssertOptions
        {
            public Dictionary<string, object> Content { get; } = new Dictionary<string, object>();
        }

        public class UnsuccessfulResponseAssertOptions : ResponseAssertOptions
        {
            private bool _exceptionErrorCodeSet = false;
            private string _exceptionErrorCode;

            public string ErrorCode { get; set; }

            public string ExceptionErrorCode
            {
                get => _exceptionErrorCodeSet ? _exceptionErrorCode : ErrorCode;
                set
                {
                    _exceptionErrorCode = value;
                    _exceptionErrorCodeSet = true;
                }
            }

            public string ErrorDescription { get; set; }

            public string ErrorPhrase { get; set; }
            public int? FailedEntityIndex { get; set; }
        }

        public class DefaultHeaders : Dictionary<string, string>
        {
            public DefaultHeaders(Response response)
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

        public class TableTransactionHeaders : Dictionary<string, string>
        {
            public TableTransactionHeaders(Response response, string tableName, string partitionKey, string rowKey)
            {
                Add("X-Content-Type-Options", "nosniff");
                Add("Cache-Control", "no-cache");
                Add("Preference-Applied", "return-no-content");
                Add("DataServiceVersion", "3.0;");
                Add("Location", $"https://cloudstubdev.table.core.windows.net/{tableName}(PartitionKey='{partitionKey}',RowKey='{rowKey}')");
                Add("DataServiceId", $"https://cloudstubdev.table.core.windows.net/{tableName}(PartitionKey='{partitionKey}',RowKey='{rowKey}')");
                Add("ETag", response.Headers.ETag.ToString());
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

        public class AcceptedHeaders : DefaultHeaders
        {
            public AcceptedHeaders(Response response) : base(response)
            {
                Remove("Cache-Control");
                Remove("X-Content-Type-Options");
                Remove("Content-Type");
            }
        }

        public class DeletedHeaders : DefaultHeaders
        {
            public DeletedHeaders(Response response) : base(response)
            {
                Remove("Cache-Control");
            }
        }

        public class InvalidUrlHeaders : DefaultHeaders
        {
            public InvalidUrlHeaders(Response response) : base(response)
            {
                this["Server"] = "Microsoft-HTTPAPI/2.0";
                this["Content-Type"] = "text/html; charset=us-ascii";

                Remove("Cache-Control");
                Remove("x-ms-version");
                Remove("X-Content-Type-Options");
                Remove("x-ms-request-id");
                Remove("x-ms-client-request-id");
                Remove("Transfer-Encoding");
            }
        }
    }
}