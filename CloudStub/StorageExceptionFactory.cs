using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using Microsoft.WindowsAzure.Storage;

namespace CloudStub
{
    internal static class StorageExceptionFactory
    {
        public static StorageException InvalidTableNameLengthException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "OutOfRangeInput",
                    ErrorDetailsMessage = "The specified resource name length is not within the permissible limits."
                }
            );

        public static StorageException TableDoesNotExistException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 404,
                    HttpStatusName = "Not Found",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "TableNotFound",
                    ErrorDetailsMessage = "The table specified does not exist."
                }
            );

        public static StorageException TableAlreadyExistsException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 409,
                    HttpStatusName = "Conflict",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "TableAlreadyExists",
                    ErrorDetailsMessage = "The table specified already exists."
                }
            );

        public static StorageException InvalidInputException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "InvalidInput",
                    ErrorDetailsMessage = "One of the request inputs is not valid."
                }
            );

        public static StorageException InvalidResourceNameException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "InvalidResourceName",
                    ErrorDetailsMessage = "The specifed resource name contains invalid characters."
                }
            );

        public static StorageException ResourceNotFoundException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 404,
                    HttpStatusName = "Not Found",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "ResourceNotFound",
                    ErrorDetailsMessage = "The specified resource does not exist."
                }
            );

        public static StorageException PropertiesWithoutValueException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "PropertiesNeedValue",
                    ErrorDetailsMessage = "The values are not specified for all properties in the entity."
                }
            );

        public static StorageException PropertyValueTooLargeException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "PropertyValueTooLarge",
                    ErrorDetailsMessage = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less."
                }
            );

        public static StorageException InvalidPartitionKeyException(string partitionKey)
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "OutOfRangeInput",
                    ErrorDetailsMessage = $"The 'PartitionKey' parameter of value '{_Escape(partitionKey)}' is out of range."
                }
            );

        public static StorageException InvalidRowKeyException(string rowKey)
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "OutOfRangeInput",
                    ErrorDetailsMessage = $"The 'RowKey' parameter of value '{_Escape(rowKey)}' is out of range."
                }
            );

        public static StorageException InvalidDateTimePropertyException(string propertyName, DateTime value)
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "OutOfRangeInput",
                    ErrorDetailsMessage = $"The '{propertyName}' parameter of value '{value:MM/dd/yyyy HH:mm:ss}' is out of range."
                }
            );

        public static StorageException EntityAlreadyExistsException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 409,
                    HttpStatusName = "Conflict",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "EntityAlreadyExists",
                    ErrorDetailsMessage = "The specified entity already exists."
                }
            );

        public static StorageException PreconditionFailedException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 412,
                    HttpStatusName = "Precondition Failed",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = "UpdateConditionNotSatisfied",
                    ErrorDetailsMessage = "The update condition specified in the request was not satisfied."
                }
            );

        public static StorageException InvalidDuplicateRowException(int operationIndex)
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorCode = null,
                    ErrorDetailsCode = "InvalidDuplicateRow",
                    ErrorDetailsMessage = $"Element {operationIndex} in the batch returned an unexpected response code."
                }
            );

        public static StorageException InvalidOperationInBatchException(int operationIndex, StorageException operationException)
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = operationException.RequestInformation.HttpStatusCode,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorCode = string.Empty,
                    ErrorDetailsCode = operationException.RequestInformation.ErrorCode,
                    ErrorDetailsMessage = $"Element {operationIndex} in the batch returned an unexpected response code."
                }
            );

        public static StorageException InvalidOperationInBatchExceptionWithoutErrorCode(int operationIndex, StorageException operationException)
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = operationException.RequestInformation.HttpStatusCode,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorCode = null,
                    ErrorDetailsCode = operationException.RequestInformation.ErrorCode,
                    ErrorDetailsMessage = $"Element {operationIndex} in the batch returned an unexpected response code."
                }
            );

        public static StorageException InvalidOperationInBatchExceptionWithDetailedMessage(StorageException operationException)
        {
            var extendedErrorMessage = operationException.RequestInformation.ExtendedErrorInformation.ErrorMessage;
            var errorMessageEndIndex = extendedErrorMessage.IndexOf('\n');
            var errorMessage = errorMessageEndIndex >= 0 ? extendedErrorMessage.Substring(0, errorMessageEndIndex) : extendedErrorMessage;
            return _FromTemplate(
                new Template
                {
                    DetailedExceptionMessage = true,
                    HttpStatusCode = operationException.RequestInformation.HttpStatusCode,
                    HttpStatusName = errorMessage,
                    ErrorCode = null,
                    ErrorDetailsCode = operationException.RequestInformation.ErrorCode,
                    ErrorDetailsMessage = errorMessage
                }
            );
        }

        public static StorageException InvalidOperationInBatchExceptionWithDetailedMessage(int operationIndex, StorageException operationException)
        {
            var extendedErrorMessage = operationException.RequestInformation.ExtendedErrorInformation.ErrorMessage;
            var errorMessageEndIndex = extendedErrorMessage.IndexOf('\n');
            var errorMessage = $"{operationIndex}:{(errorMessageEndIndex >= 0 ? extendedErrorMessage.Substring(0, errorMessageEndIndex) : extendedErrorMessage)}";
            return _FromTemplate(
                new Template
                {
                    DetailedExceptionMessage = true,
                    HttpStatusCode = operationException.RequestInformation.HttpStatusCode,
                    HttpStatusName = errorMessage,
                    ErrorCode = null,
                    ErrorDetailsCode = operationException.RequestInformation.ErrorCode,
                    ErrorDetailsMessage = errorMessage
                }
            );
        }

        private static StorageException _FromTemplate(Template template)
            => _FromTemplate(template, null);

        private static StorageException _FromTemplate(Template template, Exception innerException)
        {
            var requestId = Guid.NewGuid();
            var requestTimestamp = DateTimeOffset.UtcNow;

            var exceptionMessage = template.DetailedExceptionMessage
                ? $"{template.ErrorDetailsMessage}\nRequestId:{requestId}\nTime:{requestTimestamp:yyyy-MM-dd'T'HH':'mm':'ss'.'fffffff'Z'}"
                : template.HttpStatusName;

            return _FromXml(
                exceptionMessage,
                $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<!--An exception has occurred. For more information please deserialize this message via RequestResult.TranslateFromExceptionMessage.-->
<RequestResult>
   <HTTPStatusCode>{template.HttpStatusCode}</HTTPStatusCode>
   <HttpStatusMessage>{template.HttpStatusName}</HttpStatusMessage>
   <TargetLocation>Primary</TargetLocation>
   <ServiceRequestID>{requestId}</ServiceRequestID>
   <ContentMd5 />
   <Etag />
   <RequestDate>{requestTimestamp:ddd, d MMM yyy HH':'mm':'ss 'GMT'}</RequestDate>
   {(template.ErrorCode != null ? $"<ErrorCode>{template.ErrorCode}</ErrorCode>" : "")}
   <StartTime>{requestTimestamp:ddd, d MMM yyy HH':'mm':'ss 'GMT'}</StartTime>
   <EndTime>{requestTimestamp:ddd, d MMM yyy HH':'mm':'ss 'GMT'}</EndTime>
   <Error>
      <Code>{template.ErrorDetailsCode}</Code>
      <Message>{template.ErrorDetailsMessage}
RequestId:{requestId}
Time:{requestTimestamp:yyyy-MM-dd'T'HH':'mm':'ss'.'fffffff'Z'}</Message>
   </Error>
</RequestResult>",
                innerException);
        }

        private static StorageException _FromXml(string message, string xmlContent, Exception innerException)
        {
            var requestResult = new RequestResult();
            using (var stringReader = new StringReader(xmlContent))
            using (var xmlReader = XmlReader.Create(stringReader))
                requestResult.ReadXml(xmlReader);

            var storageException = new StorageException(requestResult, message, innerException)
            {
                Source = "Microsoft.WindowsAzure.Storage",
            };
            requestResult.Exception = storageException;

            return storageException;
        }

        private static string _Escape(string value)
        {
            if (!value.Any(char.IsControl))
                return value;

            var builder = new StringBuilder(value.Length * 2);
            foreach (var @char in value)
                if (char.IsControl(@char))
                    builder.Append('%').Append(((short)@char).ToString("x2"));
                else
                    builder.Append(@char);

            return builder.ToString();
        }

        private sealed class Template
        {
            public bool DetailedExceptionMessage { get; set; }

            public int HttpStatusCode { get; set; }

            public string HttpStatusName { get; set; }

            public string ErrorCode { get; set; }

            public string ErrorDetailsCode { get; set; }

            public string ErrorDetailsMessage { get; set; }
        }
    }
}