using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;

namespace CloudStub.TableOperations
{
    internal interface ITableOperationExecutorContext
    {
        bool TableExists { get; }

        IDictionary<string, IDictionary<string, DynamicTableEntity>> Entities { get; }
    }
}