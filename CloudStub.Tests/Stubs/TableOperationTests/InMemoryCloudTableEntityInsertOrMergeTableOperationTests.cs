using System.Collections.Generic;
using System.Threading.Tasks;
using CloudStub.Tests.BaseOperationTests;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.Tests.TableOperationTests
{
    public sealed class InMemoryCloudTableEntityInsertOrMergeTableOperationTests : InMemoryCloudTableEntityInsertOrMergeOperationTests
    {
        protected override string ExpectedErrorCode
            => string.Empty;

        protected override async Task<IEnumerable<TableResult>> ExecuteAsync(TableOperation tableOperation)
            => new[] { await CloudTable.ExecuteAsync(tableOperation) };
    }
}