using System;
using System.Collections.Generic;
using System.Net;
using System.Text.RegularExpressions;
using Azure;
using Azure.Core;
using CloudStub.AzureDataTables.Serializers;

namespace CloudStub.AzureDataTables
{
    internal static class TableStubResponseFactory
    {
        public static ResponseStub EntityResponse(ResponseHeaders headers, string metadata, string etag, IReadOnlyDictionary<string, object> entity, IEnumerable<string> selectedProperties = null)
            => new ResponseStub(
                HttpStatusCode.OK,
                "OK",
                JsonSeriaizer.SerializeEntity(metadata, etag, entity, selectedProperties),
                headers
            );

        public static ResponseStub EntitiesResponse(ResponseHeaders headers, string metadata, IEnumerable<IReadOnlyDictionary<string, object>> entities, IEnumerable<string> selectedProperties = null)
            => new ResponseStub(
                HttpStatusCode.OK,
                "OK",
                JsonSeriaizer.SerializeEntities(metadata, entities, selectedProperties),
                headers
            );

        public static ResponseStub NoContentResponse()
            => NoContentResponse(new NoContentResponseHeaders());

        public static ResponseStub NoContentResponse(ResponseHeaders headers)
            => new ResponseStub(
                HttpStatusCode.NoContent,
                "No Content",
                string.Empty,
                headers
            );

        public static ResponseStub AcceptedResponse()
            => new ResponseStub(
                HttpStatusCode.Accepted,
                "Accepted",
                string.Empty,
                new AcceptedResponseHeaders()
            );

        public static ResponseStub TableCreatedResponse(Uri accountUri, string tableName)
            => new ResponseStub(
                HttpStatusCode.Created,
                "Created",
                JsonSeriaizer.TableCreatedMetadata(accountUri, tableName),
                new DefaultResponseHeaders
                {
                    { "Location", $"{accountUri}Tables('{tableName}')" }
                }
            );

        public static ResponseStub SuccessfulXmlResponse(string xmlContent)
            => new ResponseStub(
                HttpStatusCode.OK,
                "OK",
                xmlContent,
                new XmlResponseHeaders()
            );

        public static ResponseStub UnsuccessfulJsonResponse(HttpStatusCode statusCode, string errorCode, string errorDescription)
            => UnsuccessfulJsonResponse(statusCode, errorCode, errorDescription, new DefaultResponseHeaders());

        public static ResponseStub UnsuccessfulJsonResponse(HttpStatusCode statusCode, string errorCode, string errorDescription, ResponseHeaders headers)
            => new ResponseStub(
                statusCode,
                Regex.Replace(statusCode.ToString(), "(?<=[a-z])[A-Z]", " $0"),
                JsonSeriaizer.SerializeError(errorCode, $"{errorDescription}\nRequestId:{headers.RequestId:D}\nTime:{DateTime.UtcNow:yyyy-MM-ddTHH:mm:ss.fffffffZ}"),
                headers
            );

        public static RequestFailedException JsonRequestFailedException(HttpStatusCode statusCode, string errorCode, string errorDescription)
            => JsonRequestFailedException(statusCode, errorCode, errorDescription, new DefaultResponseHeaders());

        public static RequestFailedException JsonRequestFailedException(HttpStatusCode statusCode, string errorCode, string errorDescription, ResponseHeaders headers)
        {
            var errorMessage = $"{errorDescription}\nRequestId:{headers.RequestId:D}\nTime:{DateTime.UtcNow:yyyy-MM-ddTHH:mm:ss.fffffffZ}";

            return new RequestFailedException(
                new ResponseStub(
                    statusCode,
                    Regex.Replace(statusCode.ToString(), "(?<=[a-z])[A-Z]", " $0"),
                    JsonSeriaizer.SerializeError(errorCode, errorMessage),
                    headers
                )
                {
                    IsError = true
                },
                innerException: null,
                detailsParser: new RequestFailedDetailsParserStub(errorCode, errorMessage))
            {
                Source = "Azure.Data.Tables"
            };
        }

        public static RequestFailedException XmlRequestFailedException(HttpStatusCode statusCode, string errorCode, string errorDescription)
        {
            var headers = new XmlResponseHeaders
            {
                { "Content-Length", "327" },
                { "x-ms-error-code", "InvalidXmlDocument" },
            };
            headers.Remove("Transfer-Encoding");

            return XmlRequestFailedException(statusCode, errorCode, errorDescription, headers);
        }

        public static RequestFailedException XmlRequestFailedException(HttpStatusCode statusCode, string errorCode, string errorDescription, ResponseHeaders headers)
            => new RequestFailedException(
                    new ResponseStub(
                        statusCode,
                        errorDescription,
                        XmlSeriaizer.SerializeError(errorCode, $"{errorDescription}\nRequestId:{headers.RequestId:D}\nTime:{DateTime.UtcNow:yyyy-MM-ddTHH:mm:ss.fffffffZ}"),
                        headers
                    )
                    {
                        IsError = true
                    },
                    innerException: null,
                    detailsParser: new RequestFailedDetailsParserStub(null, "Service request failed."))
            {
                Source = "Azure.Data.Tables"
            };

        public static RequestFailedException InvalidUriException(HttpStatusCode statusCode, string htmlErrorDescription, ResponseHeaders headers)
        {
            return new RequestFailedException(
                new ResponseStub(
                    statusCode,
                    Regex.Replace(statusCode.ToString(), "(?<=[a-z])[A-Z]", " $0"),
                    htmlErrorDescription,
                    headers
                )
                {
                    IsError = true
                }
            )
            {
                Source = "Azure.Data.Tables"
            };
        }

        private class RequestFailedDetailsParserStub : RequestFailedDetailsParser
        {
            private readonly string _errorCode;
            private readonly string _errorMessage;

            public RequestFailedDetailsParserStub(string errorCode, string errorMessage)
            {
                _errorCode = errorCode;
                _errorMessage = errorMessage;
            }

            public override bool TryParse(Response response, out ResponseError error, out IDictionary<string, string> data)
            {
                error = new ResponseError(_errorCode, _errorMessage.Replace("\\n", "\n"));
                data = null;
                return true;
            }
        }
    }
}