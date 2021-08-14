using System;
using System.Collections.Generic;

namespace CloudStub.Core
{
    public class StubEntity
    {
        private static readonly IReadOnlyDictionary<string, StubEntityProperty> _emptyProperties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal);
        private IReadOnlyDictionary<string, StubEntityProperty> _properties = _emptyProperties;

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

        public IReadOnlyDictionary<string, StubEntityProperty> Properties
        {
            get => _properties;
            set => _properties = value ?? _emptyProperties;
        }
    }
}