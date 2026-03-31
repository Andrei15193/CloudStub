using System;
using Azure;
using Azure.Data.Tables;

namespace CloudStub.AzureDataTables.Tests.Data
{
    public sealed class TestQueryEntityFields : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public ETag ETag { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public string StringField;

        public byte[] BinaryField;

        public int? Int32Field;

        public long? Int64Field;

        public double? DoubleField;

        public Guid? GuidField;

        public bool? BoolField;

        public DateTime? DateTimeField;

        public DateTimeOffset? DateTimeOffsetField;
    }
}