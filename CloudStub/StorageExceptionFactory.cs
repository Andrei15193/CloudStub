using System;
using Microsoft.Azure.Cosmos.Table;

namespace CloudStub
{
    internal static class StorageExceptionFactory
    {
        public static StorageException InvalidTableNameLengthException()
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "The remote server returned an error: (400) Bad Request.",
                    ErrorCode = string.Empty,
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    HttpStatusName = "The remote server returned an error: (400) Bad Request.",
                    ErrorCode = string.Empty,
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    HttpStatusName = "The remote server returned an error: (400) Bad Request.",
                    ErrorCode = string.Empty
                }
            );

        public static StorageException BadRequestForBatchException(int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    HttpStatusName = "The remote server returned an error: (400) Bad Request.",
                    ErrorCode = string.Empty,
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    HttpStatusName = "The remote server returned an error: (400) Bad Request.",
                    ErrorCode = string.Empty,
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    HttpStatusName = "The remote server returned an error: (400) Bad Request.",
                    ErrorCode = string.Empty,
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    HttpStatusName = "The remote server returned an error: (400) Bad Request.",
                    ErrorCode = string.Empty,
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    HttpStatusName = "The remote server returned an error: (400) Bad Request.",
                    ErrorCode = string.Empty,
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    HttpStatusName = "The remote server returned an error: (400) Bad Request.",
                    ErrorCode = string.Empty,
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
                    {
                        Code = "OutOfRangeInput",
                        Message = $"The 'PartitionKey' parameter of value '{partitionKey}' is out of range."
                    }
                }
            );

        public static StorageException InvalidPartitionKeyForBatchException(string partitionKey, int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
                    {
                        Code = "OutOfRangeInput",
                        Message = $"{operationIndex}:The 'PartitionKey' parameter of value '{partitionKey}' is out of range."
                    }
                }
            );

        public static StorageException InvalidRowKeyException(string rowKey)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "The remote server returned an error: (400) Bad Request.",
                    ErrorCode = string.Empty,
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
                    {
                        Code = "OutOfRangeInput",
                        Message = $"The 'RowKey' parameter of value '{rowKey}' is out of range."
                    }
                }
            );

        public static StorageException InvalidRowKeyForBatchException(string partitionKey, int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
                    {
                        Code = "OutOfRangeInput",
                        Message = $"{operationIndex}:The 'RowKey' parameter of value '{partitionKey}' is out of range."
                    }
                }
            );

        public static StorageException InvalidDateTimePropertyException(string propertyName, DateTimeOffset value)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "The remote server returned an error: (400) Bad Request.",
                    ErrorCode = string.Empty,
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
                    {
                        Code = "OutOfRangeInput",
                        Message = $"The '{propertyName}' parameter of value '{value:MM/dd/yyyy HH:mm:ss}' is out of range."
                    }
                }
            );

        public static StorageException InvalidDateTimePropertyForBatchException(string propertyName, DateTimeOffset value, int operationIndex)
            => _FromTemplate(
                new StorageExceptionTemplate
                {
                    HttpStatusCode = 400,
                    HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
                    ErrorDetails = new StorageExceptionErrorDetailTemplate
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
            var requestId = Guid.NewGuid().ToString();
            var requestTimestamp = DateTimeOffset.UtcNow;

            var exceptionMessage = template.DetailedExceptionMessage
                ? $"{template.HttpStatusName}\nRequestId:{requestId}\nTime:{requestTimestamp:yyyy-MM-dd'T'HH':'mm':'ss'.'fffffff'Z'}"
                : template.HttpStatusName;

            var extendedErrorInformation = default(StorageExtendedErrorInformation);
            if (template.ErrorDetails is object)
            {
                extendedErrorInformation = new StorageExtendedErrorInformation();
                typeof(StorageExtendedErrorInformation).GetProperty(nameof(StorageExtendedErrorInformation.ErrorCode)).SetValue(extendedErrorInformation, template.ErrorDetails.Code);
                typeof(StorageExtendedErrorInformation).GetProperty(nameof(StorageExtendedErrorInformation.ErrorMessage)).SetValue(extendedErrorInformation, $"{template.ErrorDetails.Message}\nRequestId:{requestId}\nTime:{requestTimestamp:yyyy-MM-dd'T'HH':'mm':'ss'.'fffffff'Z'}");
            }

            var requestResult = new RequestResult
            {
                HttpStatusCode = template.HttpStatusCode
            };
            typeof(RequestResult).GetProperty(nameof(RequestResult.HttpStatusMessage)).SetValue(requestResult, template.HttpStatusName);
            typeof(RequestResult).GetProperty(nameof(RequestResult.ServiceRequestID)).SetValue(requestResult, requestId);
            typeof(RequestResult).GetProperty(nameof(RequestResult.ContentMd5)).SetValue(requestResult, null);
            typeof(RequestResult).GetProperty(nameof(RequestResult.Etag)).SetValue(requestResult, null);
            typeof(RequestResult).GetProperty(nameof(RequestResult.RequestDate)).SetValue(requestResult, requestTimestamp.ToString("ddd, d MMM yyy HH':'mm':'ss 'GMT'"));
            typeof(RequestResult).GetProperty(nameof(RequestResult.TargetLocation)).SetValue(requestResult, StorageLocation.Primary);

            typeof(RequestResult).GetProperty(nameof(RequestResult.ExtendedErrorInformation)).SetValue(requestResult, extendedErrorInformation);
            typeof(RequestResult).GetProperty(nameof(RequestResult.ErrorCode)).SetValue(requestResult, template.ErrorCode);
            typeof(RequestResult).GetProperty(nameof(RequestResult.IsRequestServerEncrypted)).SetValue(requestResult, true);
            typeof(RequestResult).GetProperty(nameof(RequestResult.StartTime)).SetValue(requestResult, requestTimestamp);
            typeof(RequestResult).GetProperty(nameof(RequestResult.EndTime)).SetValue(requestResult, requestTimestamp);
            typeof(RequestResult).GetProperty(nameof(RequestResult.RequestCharge)).SetValue(requestResult, default(decimal?));

            var storageException = new StorageException(requestResult, exceptionMessage, innerException)
            {
                Source = "Microsoft.Azure.Cosmos.Table",
            };
            requestResult.Exception = storageException;

            return storageException;
        }

        private class StorageExceptionTemplate
        {
            public bool DetailedExceptionMessage { get; set; }

            public int HttpStatusCode { get; set; }

            public string HttpStatusName { get; set; }

            public string ErrorCode { get; set; }

            public StorageExceptionErrorDetailTemplate ErrorDetails { get; set; }
        }

        private class StorageExceptionErrorDetailTemplate
        {
            public string Code { get; set; }

            public string Message { get; set; }
        }
    }
}