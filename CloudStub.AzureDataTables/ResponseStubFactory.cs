using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Azure;
using Azure.Core;
using Azure.Data.Tables;
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

        public static ResponseStub<IReadOnlyList<Response>> TransactionResponse(IReadOnlyList<Response> operationResponses)
        {
            var batchBoundaryTag = $"batchresponse_{Guid.NewGuid():D}";
            var operationBoundaryTag = $"changesetresponse_{Guid.NewGuid():D}";

            var responseContentBuilder = new StringBuilder()
                .AppendFormat("--{0}\r\n", batchBoundaryTag)
                .AppendFormat("Content-Type: multipart/mixed; boundary={0}\r\n", operationBoundaryTag)
                .Append("\r\n");

            foreach (var operationResponse in operationResponses)
            {
                responseContentBuilder
                    .AppendFormat("--{0}\r\n", operationBoundaryTag)
                    .Append("Content-Type: application/http\r\n")
                    .Append("Content-Transfer-Encoding: binary\r\n")
                    .Append("\r\n")
                    .AppendFormat("HTTP/1.1 {0} {1}\r\n", operationResponse.Status, Regex.Replace(((HttpStatusCode)operationResponse.Status).ToString(), "(?<=[a-z])[A-Z]", " $0"));

                foreach (var header in operationResponse.Headers)
                    responseContentBuilder
                        .AppendFormat("{0}: {1}\r\n", header.Name, header.Value);

                responseContentBuilder
                    .Append("\r\n")
                    .Append("\r\n");
            }

            responseContentBuilder
                .AppendFormat("--{0}--\r\n", operationBoundaryTag)
                .AppendFormat("--{0}--\r\n", batchBoundaryTag);

            return new ResponseStub<IReadOnlyList<Response>>(
                new ResponseStub(
                    HttpStatusCode.Accepted,
                    "Accepted",
                    responseContentBuilder.ToString(),
                    new DefaultResponseHeaders
                    {
                        ["Content-Type"] = $"multipart/mixed; boundary={batchBoundaryTag}"
                    }
                ),
                operationResponses
            );
        }

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

        public static TableTransactionFailedException TransactionJsonRequestFailedException(HttpStatusCode statusCode, string errorCode, string errorDescription, int? errorIndex)
        {
            var errorMessageBuilder = new StringBuilder();

            if (errorIndex.HasValue)
                errorMessageBuilder
                    .AppendFormat("{0}:", errorIndex);

            errorMessageBuilder
                .AppendFormat("{0}\n", errorDescription)
                .AppendFormat("RequestId:{0:D}\n", Guid.NewGuid())
                .AppendFormat("Time:{0:yyyy-MM-ddTHH:mm:ss.fffffffZ}\n", DateTime.UtcNow);

            if (errorIndex.HasValue)
                errorMessageBuilder
                    .Append(" The index of the entity that caused the error can be found in FailedTransactionActionIndex.\n");

            errorMessageBuilder
                .AppendFormat("Status: {0:D} ({1})\n", statusCode, Regex.Replace(statusCode.ToString(), "(?<=[a-z])[A-Z]", " $0"))
                .AppendFormat("ErrorCode: {0}\n\n", errorCode);

            if (errorIndex.HasValue)
                errorMessageBuilder
                    .Append("Additional Information:\n")
                    .AppendFormat("FailedEntity: {0}\n\n", errorIndex);

            errorMessageBuilder
                .Append("Service request succeeded. Response content and headers are not included to avoid logging sensitive data.\n");

            var exception = new TableTransactionFailedException(
                new RequestFailedException(
                    status: (int)statusCode,
                    errorCode: errorCode,
                    message: errorMessageBuilder.ToString(),
                    innerException: null
                )
                {
                    Source = "Azure.Data.Tables"
                }
            )
            {
                Source = "Azure.Data.Tables"
            };

            if (errorIndex.HasValue)
            {
                exception.Data["FailedEntity"] = errorIndex.ToString();
                typeof(TableTransactionFailedException)
                    .GetProperty(nameof(TableTransactionFailedException.FailedTransactionActionIndex), BindingFlags.Instance | BindingFlags.Public | BindingFlags.SetProperty)
                    .SetValue(exception, errorIndex);
            }

            return exception;
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