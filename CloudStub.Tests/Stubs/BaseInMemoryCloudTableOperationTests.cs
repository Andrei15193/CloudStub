using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.Tests
{
    public abstract class BaseInMemoryCloudTableOperationTests : BaseInMemoryCloudTableTests
    {
        protected abstract Task<IEnumerable<TableResult>> ExecuteAsync(TableOperation tableOperation);

        protected abstract TableOperation GetOperation(ITableEntity tableEntity);
    }
}