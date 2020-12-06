using System.Collections.Generic;
using Microsoft.Azure.Cosmos.Table;

namespace CloudStub.TableOperations
{
    internal interface ITableOperationExecutorContext
    {
        bool TableExists { get; }

        IDictionary<string, IDictionary<string, DynamicTableEntity>> Entities { get; }
    }
}