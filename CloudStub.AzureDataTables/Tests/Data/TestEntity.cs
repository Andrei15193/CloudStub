using System;
using Azure;
using Azure.Data.Tables;

namespace CloudStub.AzureDataTables.Tests.Data
{
    public class TestEntity : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public ETag ETag { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public byte[] BinaryProp { get; set; }

        public bool? BooleanProp { get; set; }

        public string StringProp { get; set; }

        public int? Int32Prop { get; set; }

        public long? Int64Prop { get; set; }

        public double? DoubleProp { get; set; }

        public DateTime? DateTimeProp { get; set; }

        public DateTimeOffset? DateTimeOffsetProp { get; set; }

        public Guid? GuidProp { get; set; }

        public decimal? DecimalProp { get; set; }
    }
}