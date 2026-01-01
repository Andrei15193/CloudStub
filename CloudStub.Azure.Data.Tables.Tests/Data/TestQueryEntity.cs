using Azure;
using Azure.Data.Tables;

namespace CloudStub.Azure.Data.Tables.Tests.Data
{
    public sealed class TestQueryEntity : ITableEntity
    {
        public required string PartitionKey { get; set; }

        public required string RowKey { get; set; }

        public ETag ETag { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public string? StringProp { get; set; }

        public byte[]? BinaryProp { get; set; }

        public int? Int32Prop { get; set; }

        public long? Int64Prop { get; set; }

        public double? DoubleProp { get; set; }

        public Guid? GuidProp { get; set; }

        public bool? BoolProp { get; set; }

        public DateTime? DateTimeProp { get; set; }

        public DateTimeOffset? DateTimeOffsetProp { get; set; }
    }
}