using System;
using Azure;
using Azure.Data.Tables;

namespace CloudStub.AzureDataTables.Tests.Data
{
    public class TestQueryEntityAccessModifier : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public ETag ETag { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public string PublicProp { get; set; }

        protected string ProtectedProp { get; set; }

        internal string InternalProp { get; set; }

        private string PrivateProp { get; set; }

        protected internal string ProtectedInternalProp { get; set; }

        private protected string PrivateProtectedProp { get; set; }

        public string PublicField;

        protected string ProtectedField;

        internal string InternalField = null;

        private string PrivateField = null;

        protected internal string ProtectedInternalField;

        private protected string PrivateProtectedField;

        public void SetPrivateMembers()
        {
            PrivateProp = null;
            PrivateField = null;
        }

        public string GetPrivateProp()
            => PrivateProp;

        public string GetPrivateField()
            => PrivateField;
    }
}