using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace CloudStub
{
    public class StubCloudTableClient : CloudTableClient
    {
        public StubCloudTableClient(StorageUri storageUri, StorageCredentials credentials)
            : this(storageUri, credentials, null)
        {
        }

        public StubCloudTableClient(Uri baseUri, StorageCredentials credentials, TableClientConfiguration configuration = null)
            : this(new StorageUri(baseUri), credentials, configuration)
        {
        }

        public StubCloudTableClient(StorageUri storageUri, StorageCredentials credentials, TableClientConfiguration configuration = null)
            : base(storageUri, credentials, configuration)
        {
        }

        public override void SetServiceProperties(ServiceProperties properties, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public override Task SetServicePropertiesAsync(ServiceProperties properties)
        {
            SetServiceProperties(properties, default, default);
            return Task.CompletedTask;
        }

        public override Task SetServicePropertiesAsync(ServiceProperties properties, CancellationToken cancellationToken)
        {
            SetServiceProperties(properties, default, default);
            return Task.CompletedTask;
        }

        public override Task SetServicePropertiesAsync(ServiceProperties properties, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            SetServiceProperties(properties, requestOptions, operationContext);
            return Task.CompletedTask;
        }

        public override Task SetServicePropertiesAsync(ServiceProperties properties, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            SetServiceProperties(properties, requestOptions, operationContext);
            return Task.CompletedTask;
        }
    }
}