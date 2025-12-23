using System;
using System.Collections.Generic;
using Azure.Data.Tables;

namespace CloudStub.Azure.Data.Tables
{
    public class TableServiceClientStub : TableServiceClient
    {
        private readonly string _accountName;
        private readonly IDictionary<string, TableClientStub> _tableClientStubs;

        public TableServiceClientStub(string accountName)
        {
            if (accountName == null)
                throw new ArgumentNullException(nameof(accountName));
            if (string.IsNullOrWhiteSpace(accountName))
                throw new ArgumentException("Account name cannot be empty or whitespace.", nameof(accountName));

            _accountName = accountName;
            _tableClientStubs = new Dictionary<string, TableClientStub>(StringComparer.Ordinal);
        }

        public TableServiceClientStub()
            : this("StubStorageAccount")
        {
        }

        public override string AccountName
            => _accountName;

        public override Uri Uri
            => new Uri($"http://{_accountName}.cloud.stub");

        public override TableClient GetTableClient(string tableName)
        {
            if (!_tableClientStubs.TryGetValue(tableName, out var tableClientStub))
            {
                tableClientStub = new TableClientStub(this, tableName);
                _tableClientStubs[tableName] = tableClientStub;
            }

            return tableClientStub;
        }
    }
}