using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace CloudStub.Tests
{
    public sealed class TestQueryEntity : TableEntity
    {
        public string StringProp { get; set; }

        public byte[] BinaryProp { get; set; }

        public int? Int32Prop { get; set; }

        public long? Int64Prop { get; set; }

        public double? DoubleProp { get; set; }

        public Guid? GuidProp { get; set; }

        public bool? BoolProp { get; set; }

        public DateTime? DateTimeProp { get; set; }

        public DateTimeOffset? DateTimeOffsetProp { get; set; }
    }
}