using Microsoft.WindowsAzure.Storage;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;

namespace CloudStub
{
    internal static class StorageExceptionFactory
    {
        private static readonly PropertyInfo _extendedErrorInformationProperty = typeof(RequestResult).GetRuntimeProperty("ExtendedErrorInformation");

        public static StorageException InvalidTableNameLengthException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "OutOfRangeInput",
                        Message = "The specified resource name length is not within the permissible limits."
                    }
                }
            );

        public static StorageException TableDoesNotExistException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 404,
                    HttpStatusName = "Not Found",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "TableNotFound",
                        Message = "The table specified does not exist."
                    }
                }
            );

        public static StorageException TableDoesNotExistForBatchException(int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 404,
                    HttpStatusName = $"{operationIndex}:The table specified does not exist.",
                    DetailedExceptionMessage = true,
                    ErrorDetails =
                    {
                        Code = "TableNotFound",
                        Message = $"{operationIndex}:The table specified does not exist."
                    }
                }
            );

        public static StorageException TableDoesNotExistForBatchInsertException(int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 404,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorDetails =
                    {
                        Code = "TableNotFound",
                        Message = $"{operationIndex}:The table specified does not exist."
                    }
                }
            );

        public static StorageException TableAlreadyExistsException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 409,
                    HttpStatusName = "Conflict",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "TableAlreadyExists",
                        Message = "The table specified already exists."
                    }
                }
            );

        public static StorageException InvalidInputException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "InvalidInput",
                        Message = "One of the request inputs is not valid."
                    }
                }
            );

        public static StorageException BadRequestException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty
                }
            );

        public static StorageException BadRequestForBatchException(int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorDetails =
                    {
                        Code = "InvalidInput",
                        Message = $"{operationIndex}:Bad Request - Error in query syntax."
                    }
                }
            );

        public static StorageException InvalidResourceNameException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "InvalidResourceName",
                        Message = "The specifed resource name contains invalid characters."
                    }
                }
            );

        public static StorageException ResourceNotFoundException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 404,
                    HttpStatusName = "Not Found",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "ResourceNotFound",
                        Message = "The specified resource does not exist."
                    }
                }
            );

        public static StorageException ResourceNotFoundForBatchException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 404,
                    HttpStatusName = "The specified resource does not exist.",
                    DetailedExceptionMessage = true,
                    ErrorDetails =
                    {
                        Code = "ResourceNotFound",
                        Message = "The specified resource does not exist."
                    }
                }
            );

        public static StorageException PropertiesWithoutValueException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "PropertiesNeedValue",
                        Message = "The values are not specified for all properties in the entity."
                    }
                }
            );

        public static StorageException PropertyValueTooLargeException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "PropertyValueTooLarge",
                        Message = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less."
                    }
                }
            );

        public static StorageException PropertyValueTooLargeForBatchException(int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorDetails =
                    {
                        Code = "PropertyValueTooLarge",
                        Message = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less."
                    }
                }
            );

        public static StorageException ErrorInQuerySyntaxException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "InvalidInput",
                        Message = "Bad Request - Error in query syntax."
                    }
                }
            );

        public static StorageException InputOutOfRangeException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "OutOfRangeInput",
                        Message = $"One of the request inputs is out of range."
                    }
                }
            );

        public static StorageException InputOutOfRangeForBatchException(int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorDetails =
                    {
                        Code = "OutOfRangeInput",
                        Message = $"{operationIndex}:One of the request inputs is out of range."
                    }
                }
            );

        public static StorageException InvalidPartitionKeyException(string partitionKey)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "OutOfRangeInput",
                        Message = $"The 'PartitionKey' parameter of value '{_Escape(partitionKey)}' is out of range."
                    }
                }
            );

        public static StorageException InvalidPartitionKeyForBatchException(string partitionKey, int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorDetails =
                    {
                        Code = "OutOfRangeInput",
                        Message = $"{operationIndex}:The 'PartitionKey' parameter of value '{_Escape(partitionKey)}' is out of range."
                    }
                }
            );

        public static StorageException InvalidRowKeyException(string rowKey)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "OutOfRangeInput",
                        Message = $"The 'RowKey' parameter of value '{_Escape(rowKey)}' is out of range."
                    }
                }
            );

        public static StorageException InvalidRowKeyForBatchException(string partitionKey, int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorDetails =
                    {
                        Code = "OutOfRangeInput",
                        Message = $"{operationIndex}:The 'RowKey' parameter of value '{_Escape(partitionKey)}' is out of range."
                    }
                }
            );

        public static StorageException InvalidDateTimePropertyException(string propertyName, DateTime value)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "OutOfRangeInput",
                        Message = $"The '{propertyName}' parameter of value '{value:MM/dd/yyyy HH:mm:ss}' is out of range."
                    }
                }
            );

        public static StorageException InvalidDateTimePropertyForBatchException(string propertyName, DateTime value, int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorDetails =
                    {
                        Code = "OutOfRangeInput",
                        Message = $"{operationIndex}:The '{propertyName}' parameter of value '{value:MM/dd/yyyy HH:mm:ss}' is out of range."
                    }
                }
            );

        public static StorageException EntityAlreadyExistsException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 409,
                    HttpStatusName = "Conflict",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "EntityAlreadyExists",
                        Message = "The specified entity already exists."
                    }
                }
            );

        public static StorageException EntityAlreadyExistsForBatchException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 409,
                    HttpStatusName = "The specified entity already exists.",
                    DetailedExceptionMessage = true,
                    ErrorDetails =
                    {
                        Code = "EntityAlreadyExists",
                        Message = "The specified entity already exists."
                    }
                }
            );

        public static StorageException PreconditionFailedException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 412,
                    HttpStatusName = "Precondition Failed",
                    ErrorCode = string.Empty,
                    ErrorDetails =
                    {
                        Code = "UpdateConditionNotSatisfied",
                        Message = "The update condition specified in the request was not satisfied."
                    }
                }
            );

        public static StorageException PreconditionFailedForBatchException(int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 412,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorDetails =
                    {
                        Code = "UpdateConditionNotSatisfied",
                        Message = "The update condition specified in the request was not satisfied."
                    }
                }
            );

        public static StorageException MultipleOperationsChangeSameEntityException(int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorCode = null,
                    ErrorDetails =
                    {
                            Code = "InvalidDuplicateRow",
                            Message = $"{operationIndex}:The batch request contains multiple changes with same row key. An entity can appear only once in a batch request."
                    }
                }
            );

        private static StorageException _FromTemplate(StorageExceptionTemplate template)
            => _FromTemplate(template, null);

        private static StorageException _FromTemplate(StorageExceptionTemplate template, Exception innerException)
        {
            var requestId = Guid.NewGuid();
            var requestTimestamp = DateTimeOffset.UtcNow;

            var exceptionMessage = template.DetailedExceptionMessage
                ? $"{template.HttpStatusName}\nRequestId:{requestId}\nTime:{requestTimestamp:yyyy-MM-dd'T'HH':'mm':'ss'.'fffffff'Z'}"
                : template.HttpStatusName;

            return _FromXml(
                exceptionMessage,
                $@"<?xml version=""1.0"" encoding=""UTF-8""?>
<!--An exception has occurred. For more information please deserialize this message via RequestResult.TranslateFromExceptionMessage.-->
<RequestResult>
    <HTTPStatusCode>{template.HttpStatusCode}</HTTPStatusCode>
    <HttpStatusMessage>{_Escape(template.HttpStatusName)}</HttpStatusMessage>
    <TargetLocation>Primary</TargetLocation>
    <ServiceRequestID>{requestId}</ServiceRequestID>
    <ContentMd5 />
    <Etag />
    <RequestDate>{requestTimestamp:ddd, d MMM yyy HH':'mm':'ss 'GMT'}</RequestDate>
    {(template.ErrorCode != null ? $"<ErrorCode>{_Escape(template.ErrorCode)}</ErrorCode>" : "")}
    <StartTime>{requestTimestamp:ddd, d MMM yyy HH':'mm':'ss 'GMT'}</StartTime>
    <EndTime>{requestTimestamp:ddd, d MMM yyy HH':'mm':'ss 'GMT'}</EndTime>
    <Error>
        <Code>{_Escape(template.ErrorDetails.Code)}</Code>
        <Message>{_Escape(template.ErrorDetails.Message)}
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
            using (var xmlReader = XmlReader.Create(stringReader, new XmlReaderSettings { CheckCharacters = false }))
                requestResult.ReadXml(xmlReader);

            if (string.IsNullOrEmpty(requestResult.ExtendedErrorInformation.ErrorCode))
                _extendedErrorInformationProperty.SetValue(requestResult, null);

            var storageException = new StorageException(requestResult, message, innerException)
            {
                Source = "Microsoft.WindowsAzure.Storage",
            };
            requestResult.Exception = storageException;

            return storageException;
        }

        private static string _Escape(string value)
            => value != null && value.Any(char.IsControl)
                ? value
                    .Aggregate(
                        new StringBuilder(),
                        (builder, @char) => char.IsControl(@char)
                            ? builder.Append("&#x").Append(((short)@char).ToString("X4")).Append(';')
                            : builder.Append(@char))
                    .ToString()
                : value;

        private class StorageExceptionTemplate
        {
            public bool DetailedExceptionMessage { get; set; }

            public int HttpStatusCode { get; set; }

            public string HttpStatusName { get; set; }

            public string ErrorCode { get; set; }

            public StorageExceptionErrorDetailTemplate ErrorDetails { get; } = new StorageExceptionErrorDetailTemplate();
        }

        private class StorageExceptionErrorDetailTemplate
        {
            public string Code { get; set; }

            public string Message { get; set; }
        }
    }
}