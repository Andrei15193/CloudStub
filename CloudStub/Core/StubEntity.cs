using System;
using System.Collections.Generic;

namespace CloudStub.Core
{
    public class StubEntity
    {
        public StubEntity()
        {
        }

        internal StubEntity(StubEntity source)
        {
            PartitionKey = source.PartitionKey;
            RowKey = source.RowKey;
            Timestamp = source.Timestamp;
            ETag = source.ETag;
            Properties = source.Properties;
        }

        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTime Timestamp { get; set; }

        public string ETag { get; set; }

        public IReadOnlyDictionary<string, StubEntityProperty> Properties { get; set; }
    }
}