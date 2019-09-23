using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.Tests
{
    public class TestEntity : TableEntity
    {
        public byte[] BinaryProp { get; set; }

        public bool? BooleanProp { get; set; }

        public string StringProp { get; set; }

        public int? Int32Prop { get; set; }

        public long? Int64Prop { get; set; }

        public double? DoubleProp { get; set; }

        public DateTime? DateTimeProp { get; set; }

        public Guid? GuidProp { get; set; }

        public decimal? DecimalProp { get; set; }
    }
}