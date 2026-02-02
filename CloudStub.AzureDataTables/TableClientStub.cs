using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;

namespace CloudStub.AzureDataTables
{
    public class TableClientStub : TableClient
    {
        private readonly TableServiceClientStub _tableServiceClientStub;
        private readonly string _tableName;

        public TableClientStub(TableServiceClientStub tableServiceClientStub, string tableName)
            : base()
        {
            _tableServiceClientStub = tableServiceClientStub;
            _tableName = tableName;
        }

        public override string Name
            => _tableName;

        public override string AccountName
            => _tableServiceClientStub.AccountName;

        public override Uri Uri
            => new UriBuilder(_tableServiceClientStub.Uri) { Path = _tableName }.Uri;

        public override Response<TableItem> Create(CancellationToken cancellationToken = default)
            => _tableServiceClientStub.CreateTable(_tableName, cancellationToken);

        public override Task<Response<TableItem>> CreateAsync(CancellationToken cancellationToken = default)
            => _tableServiceClientStub.CreateTableAsync(_tableName, cancellationToken);

        public override Response<TableItem> CreateIfNotExists(CancellationToken cancellationToken = default)
            => _tableServiceClientStub.CreateTableIfNotExists(_tableName, cancellationToken);

        public override Task<Response<TableItem>> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
            => _tableServiceClientStub.CreateTableIfNotExistsAsync(_tableName, cancellationToken);

        public override Response Delete(CancellationToken cancellationToken = default)
            => _tableServiceClientStub.DeleteTable(_tableName, cancellationToken);

        public override Task<Response> DeleteAsync(CancellationToken cancellationToken = default)
            => _tableServiceClientStub.DeleteTableAsync(_tableName, cancellationToken);

        public override Uri GenerateSasUri(TableSasBuilder builder)
            => new UriBuilder(Uri) { Query = "st=stub-sas-token" }.Uri;

        public override Uri GenerateSasUri(TableSasPermissions permissions, DateTimeOffset expiresOn)
            => GenerateSasUri(GetSasBuilder(permissions, expiresOn));

        public override Response<IReadOnlyList<TableSignedIdentifier>> GetAccessPolicies(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Response SetAccessPolicy(IEnumerable<TableSignedIdentifier> tableAcl, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }
    }
}