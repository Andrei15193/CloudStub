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
                    ErrorCode = "OutOfRangeInput",
                    ErrorMessage = "The specified resource name length is not within the permissible limits."
                }
            );

        public static StorageException TableDoesNotExistException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 404,
                    HttpStatusName = "Not Found",
                    ErrorCode = "TableNotFound",
                    ErrorMessage = "The table specified does not exist."
                }
            );

        public static StorageException TableAlreadyExistsException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 409,
                    HttpStatusName = "Conflict",
                    ErrorCode = "TableAlreadyExists",
                    ErrorMessage = "The table specified already exists."
                }
            );

        public static StorageException InvalidInputException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = "InvalidInput",
                    ErrorMessage = "One of the request inputs is not valid."
                }
            );

        public static StorageException InvalidResourceNameException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = "InvalidResourceName",
                    ErrorMessage = "The specifed resource name contains invalid characters."
                }
            );

        public static StorageException ResourceNotFoundException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 404,
                    HttpStatusName = "Not Found",
                    ErrorCode = "ResourceNotFound",
                    ErrorMessage = "The specified resource does not exist."
                }
            );

        public static StorageException PropertiesWithoutValueException()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = "PropertiesNeedValue",
                    ErrorMessage = "The values are not specified for all properties in the entity."
                }
            );

        public static StorageException PropertyValueTooLarge()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = "PropertyValueTooLarge",
                    ErrorMessage = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less."
                }
            );

        public static StorageException InvalidPartitionKeyException(string partitionKey)
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = "OutOfRangeInput",
                    ErrorMessage = $"The 'PartitionKey' parameter of value '{_Escape(partitionKey)}' is out of range."
                }
            );

        public static StorageException InvalidRowKeyException(string rowKey)
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 400,
                    HttpStatusName = "Bad Request",
                    ErrorCode = "OutOfRangeInput",
                    ErrorMessage = $"The 'RowKey' parameter of value '{_Escape(rowKey)}' is out of range."
                }
            );

        public static StorageException EntityAlreadyExists()
            => _FromTemplate(
                new Template
                {
                    HttpStatusCode = 409,
                    HttpStatusName = "Conflict",
                    ErrorCode = "EntityAlreadyExists",
                    ErrorMessage = "The specified entity already exists."
                }
            );

        private static StorageException _FromTemplate(Template template)
            => _FromTemplate(template, null);

        private static StorageException _FromTemplate(Template template, Exception innerException)
        {
            var requestId = Guid.NewGuid();
            var requestTimestamp = DateTimeOffset.UtcNow;

            return _FromXml(
                template.HttpStatusName,
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
   <ErrorCode></ErrorCode>
   <StartTime>{requestTimestamp:ddd, d MMM yyy HH':'mm':'ss 'GMT'}</StartTime>
   <EndTime>{requestTimestamp:ddd, d MMM yyy HH':'mm':'ss 'GMT'}</EndTime>
   <Error>
      <Code>{template.ErrorCode}</Code>
      <Message>{template.ErrorMessage}
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
            public int HttpStatusCode { get; set; }

            public string HttpStatusName { get; set; }

            public string ErrorCode { get; set; }

            public string ErrorMessage { get; set; }
        }
    }
}