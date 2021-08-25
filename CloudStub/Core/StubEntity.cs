using System;
using System.Collections.Generic;

namespace CloudStub.Core
{
    public class StubEntity
    {
        public StubEntity(string partitionKey, string rowKey)
            : this(partitionKey, rowKey, default, default)
        {
        }

        public StubEntity(string partitionKey, string rowKey, string etag)
            : this(partitionKey, rowKey, default, etag)
        {
        }

        internal StubEntity(string partitionKey, string rowKey, DateTimeOffset timestamp, string etag)
        {
            if (string.IsNullOrWhiteSpace(partitionKey))
                throw new ArgumentException("The partition key cannot be null, empty or white space.", nameof(partitionKey));
            if (string.IsNullOrWhiteSpace(rowKey))
                throw new ArgumentException("The row key cannot be null, empty or white space.", nameof(rowKey));

            PartitionKey = partitionKey;
            RowKey = rowKey;
            Timestamp = timestamp;
            ETag = etag;
        }

        public string PartitionKey { get; }

        public string RowKey { get; }

        public DateTimeOffset? Timestamp { get; }

        public string ETag { get; }

        public IDictionary<string, StubEntityProperty> Properties { get; } = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal);
    }
}