
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace CloudStub.Azure.Data.Tables
{
    internal class TableSet : ConcurrentDictionary<string, TablePartitionSet>
    {
        public TableSet()
            : base(StringComparer.OrdinalIgnoreCase)
        {
        }
    }

    internal class TablePartitionSet : ConcurrentDictionary<string, TableRowSet>
    {
    }

    internal class TableRowSet : Dictionary<string, PropertySet>
    {
    }

    internal class PropertySet : Dictionary<string, object>
    {
    }
}