using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;
using CloudStub.AzureDataTables.Serializers;

namespace CloudStub.AzureDataTables
{
    public class TableClientStub : TableClient
    {
        private readonly TableServiceClientStub _tableServiceClientStub;
        private readonly string _tableName;

        internal TableClientStub(TableServiceClientStub tableServiceClientStub, string tableName)
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

        public override Response AddEntity<T>(T entity, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotImplementedException();
        }

        public override async Task<Response> AddEntityAsync<T>(T entity, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return AddEntity(entity, cancellationToken);
        }

        public override Response UpsertEntity<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotImplementedException();
        }

        public override async Task<Response> UpsertEntityAsync<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return UpsertEntity(entity, mode, cancellationToken);
        }

        public override Response UpdateEntity<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotImplementedException();
        }

        public override async Task<Response> UpdateEntityAsync<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return UpdateEntity(entity, ifMatch, mode, cancellationToken);
        }

        public override Response DeleteEntity(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotImplementedException();
        }

        public override async Task<Response> DeleteEntityAsync(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return DeleteEntity(partitionKey, rowKey, ifMatch, cancellationToken);
        }

        public override Response DeleteEntity(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotImplementedException();
        }

        public override async Task<Response> DeleteEntityAsync(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return DeleteEntity(entity, ifMatch, cancellationToken);
        }

        public override Response<T> GetEntity<T>(string partitionKey, string rowKey, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotImplementedException();
        }

        public override async Task<Response<T>> GetEntityAsync<T>(string partitionKey, string rowKey, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return GetEntity<T>(partitionKey, rowKey, select, cancellationToken);
        }

        public override NullableResponse<T> GetEntityIfExists<T>(string partitionKey, string rowKey, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotImplementedException();
        }

        public override async Task<NullableResponse<T>> GetEntityIfExistsAsync<T>(string partitionKey, string rowKey, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return GetEntityIfExists<T>(partitionKey, rowKey, select, cancellationToken);
        }

        public override Response<IReadOnlyList<TableSignedIdentifier>> GetAccessPolicies(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            IReadOnlyList<TableSignedIdentifier> signedIdentifiersCopy;
            using (_tableServiceClientStub.Tables.ReadLock())
            {
                if (!_tableServiceClientStub.Tables.TryGetValue(_tableName, out var table))
                {
                    var responseHeaders = new XmlResponseHeaders
                    {
                        { "x-ms-error-code", "TableNotFound" },
                        { "Content-Length", "316" }
                    };
                    responseHeaders.Remove("Transfer-Encoding");

                    throw TableStubResponseFactory.XmlRequestFailedException(HttpStatusCode.NotFound, "TableNotFound", "The table specified does not exist.", responseHeaders);
                }

                signedIdentifiersCopy = table
                    .SignedIdentifiers
                    .Select(signedIdentifier => new TableSignedIdentifier(signedIdentifier.Id, signedIdentifier.AccessPolicy == null ? null : new TableAccessPolicy(signedIdentifier.AccessPolicy.StartsOn, signedIdentifier.AccessPolicy.ExpiresOn, signedIdentifier.AccessPolicy.Permission)))
                    .ToList();
            }

            return Response.FromValue(signedIdentifiersCopy, TableStubResponseFactory.SuccessfulXmlResponse(XmlSeriaizer.Serialize(signedIdentifiersCopy)));
        }

        public override async Task<Response<IReadOnlyList<TableSignedIdentifier>>> GetAccessPoliciesAsync(CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return GetAccessPolicies(cancellationToken);
        }

        public override Response SetAccessPolicy(IEnumerable<TableSignedIdentifier> tableAcl, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using (_tableServiceClientStub.Tables.UpgradableReadLock())
            {
                if (!_tableServiceClientStub.Tables.TryGetValue(_tableName, out var table))
                {
                    var responseHeaders = new XmlResponseHeaders
                    {
                        { "x-ms-error-code", "TableNotFound" },
                        { "Content-Length", "316" }
                    };
                    responseHeaders.Remove("Transfer-Encoding");

                    throw TableStubResponseFactory.XmlRequestFailedException(HttpStatusCode.NotFound, "TableNotFound", "The table specified does not exist.", responseHeaders);
                }

                using (_tableServiceClientStub.Tables.WriteLock())
                    table.SignedIdentifiers =
                        tableAcl
                        ?.Select(signedIdentifier => new TableSignedIdentifier(signedIdentifier.Id, signedIdentifier.AccessPolicy == null ? null : new TableAccessPolicy(signedIdentifier.AccessPolicy.StartsOn, signedIdentifier.AccessPolicy.ExpiresOn, signedIdentifier.AccessPolicy.Permission)))
                        ?.ToList()
                        ?? Array.Empty<TableSignedIdentifier>() as IReadOnlyList<TableSignedIdentifier>;
            }

            var headers = new NoContentResponseHeaders();
            headers.Remove("Cache-Control");
            headers.Remove("X-Content-Type-Options");
            return TableStubResponseFactory.NoContentResponse(headers);
        }

        public override async Task<Response> SetAccessPolicyAsync(IEnumerable<TableSignedIdentifier> tableAcl, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return SetAccessPolicy(tableAcl, cancellationToken);
        }
    }
}