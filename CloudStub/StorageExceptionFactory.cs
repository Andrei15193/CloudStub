using System;
using System.Globalization;
using System.IO;
using System.Xml;
using Microsoft.WindowsAzure.Storage;

namespace CloudStub
{
    internal static class StorageExceptionFactory
    {
        public static StorageException TableAlreadyExistsException()
            => FromXml(
                "Conflict",
                null,
                @"<?xml version=""1.0"" encoding=""UTF-8""?>
<!--An exception has occurred. For more information please deserialize this message via RequestResult.TranslateFromExceptionMessage.-->
<RequestResult>
   <HTTPStatusCode>409</HTTPStatusCode>
   <HttpStatusMessage>Conflict</HttpStatusMessage>
   <TargetLocation>Primary</TargetLocation>
   <ServiceRequestID>{0}</ServiceRequestID>
   <ContentMd5 />
   <Etag />
   <RequestDate>{1}</RequestDate>
   <ErrorCode></ErrorCode>
   <StartTime>{1}</StartTime>
   <EndTime>{1}</EndTime>
   <Error>
      <Code>TableAlreadyExists</Code>
      <Message>The table specified already exists.
RequestId:{0}
Time:{1}</Message>
   </Error>
</RequestResult>",
                Guid.NewGuid(),
                DateTime.UtcNow
            );

        public static StorageException InvalidInputException()
            => FromXml(
                "Bad Request",
                null,
                @"<?xml version=""1.0"" encoding=""UTF-8""?>
<!--An exception has occurred. For more information please deserialize this message via RequestResult.TranslateFromExceptionMessage.-->
<RequestResult>
<HTTPStatusCode>400</HTTPStatusCode>
<HttpStatusMessage>Bad Request</HttpStatusMessage>
<TargetLocation>Primary</TargetLocation>
<ServiceRequestID>{0}</ServiceRequestID>
<ContentMd5 />
<Etag />
<RequestDate>{1}</RequestDate>
<ErrorCode></ErrorCode>
<StartTime>{1}</StartTime>
<EndTime>{1}</EndTime>
<Error>
    <Code>InvalidInput</Code>
    <Message>One of the request inputs is not valid.
RequestId:{0}
Time:{1}</Message>
</Error>
</RequestResult>",
                Guid.NewGuid(),
                DateTime.UtcNow
            );

        public static StorageException InvalidResourceNameException()
            => FromXml(
                "Bad Request",
                null,
                @"<?xml version=""1.0"" encoding=""UTF-8""?>
<!--An exception has occurred. For more information please deserialize this message via RequestResult.TranslateFromExceptionMessage.-->
<RequestResult>
<HTTPStatusCode>400</HTTPStatusCode>
<HttpStatusMessage>Bad Request</HttpStatusMessage>
<TargetLocation>Primary</TargetLocation>
<ServiceRequestID>{0}</ServiceRequestID>
<ContentMd5 />
<Etag />
<RequestDate>{1}</RequestDate>
<ErrorCode></ErrorCode>
<StartTime>{1}</StartTime>
<EndTime>{1}</EndTime>
<Error>
    <Code>InvalidResourceName</Code>
    <Message>The specifed resource name contains invalid characters.
RequestId:{0}
Time:{1}</Message>
</Error>
</RequestResult>",
                Guid.NewGuid(),
                DateTime.UtcNow
            );

        public static StorageException ResourceNotFoundException()
            => FromXml(
                "Not Found",
                null,
                @"<?xml version=""1.0"" encoding=""UTF-8""?>
<!--An exception has occurred. For more information please deserialize this message via RequestResult.TranslateFromExceptionMessage.-->
<RequestResult>
<HTTPStatusCode>404</HTTPStatusCode>
<HttpStatusMessage>Not Found</HttpStatusMessage>
<TargetLocation>Primary</TargetLocation>
<ServiceRequestID>{0}</ServiceRequestID>
<ContentMd5 />
<Etag />
<RequestDate>{1}</RequestDate>
<ErrorCode></ErrorCode>
<StartTime>{1}</StartTime>
<EndTime>{1}</EndTime>
<Error>
    <Code>ResourceNotFound</Code>
    <Message>The specified resource does not exist.
RequestId:{0}
Time:{1}</Message>
</Error>
</RequestResult>",
                Guid.NewGuid(),
                DateTime.UtcNow
            );

        public static StorageException FromXml(string message, Exception innerException, string xmlFormat, params object[] args)
        {
            var requestResult = new RequestResult();
            using (var stringReader = new StringReader(string.Format(CultureInfo.InvariantCulture, xmlFormat, args)))
            using (var xmlReader = XmlReader.Create(stringReader))
                requestResult.ReadXml(xmlReader);

            var storageException = new StorageException(requestResult, message, innerException)
            {
                Source = "Microsoft.WindowsAzure.Storage",
            };
            requestResult.Exception = storageException;

            return storageException;
        }
    }
}