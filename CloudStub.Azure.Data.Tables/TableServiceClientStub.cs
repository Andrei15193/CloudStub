using System;
using Azure.Data.Tables;

namespace CloudStub.Azure.Data.Tables
{
    public class TableServiceClientStub : TableServiceClient
    {
        private readonly string _accountName;

        public TableServiceClientStub(string accountName)
        {
            if (accountName == null)
                throw new ArgumentNullException(nameof(accountName));
            if (string.IsNullOrWhiteSpace(accountName))
                throw new ArgumentException("Account name cannot be empty or whitespace.", nameof(accountName));

            _accountName = accountName;
        }

        public TableServiceClientStub()
            : this("StubStorageAccount")
        {
        }

        public override string AccountName
            => _accountName;

        public override Uri Uri
            => new Uri($"http://{_accountName}.cloud.stub");
    }
}