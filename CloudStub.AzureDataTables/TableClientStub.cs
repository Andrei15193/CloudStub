using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;
using CloudStub.AzureDataTables.Filters;
using CloudStub.AzureDataTables.Filters.Nodes;
using CloudStub.AzureDataTables.Pageables;
using CloudStub.AzureDataTables.Serializers;

namespace CloudStub.AzureDataTables
{
    public class TableClientStub : TableClient
    {
        private static readonly char[] ContinuationTokenSeparator = new[] { ' ' };

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
            if (entity == null)
                throw new ArgumentNullException(nameof(entity))
                {
                    Source = "Azure.Data.Tables"
                };
            if (entity.PartitionKey == null)
                throw new ArgumentNullException("PartitionKey")
                {
                    Source = "Azure.Data.Tables"
                };
            if (entity.RowKey == null)
                throw new ArgumentNullException("RowKey")
                {
                    Source = "Azure.Data.Tables"
                };

            var mappedEntity = new ValidatedTableRowStub<T>(entity);
            if (mappedEntity.NotSupportedDateTimeValue != null)
                throw new NotSupportedException($"DateTime {mappedEntity.NotSupportedDateTimeValue} has a Kind of {mappedEntity.NotSupportedDateTimeValue?.Kind}. Azure SDK requires it to be UTC. You can call DateTime.SpecifyKind to change Kind property value to DateTimeKind.Utc.")
                {
                    Source = "Azure.Data.Tables"
                };

            cancellationToken.ThrowIfCancellationRequested();

            if (mappedEntity.IsPartitionKeyInvalid)
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "OutOfRangeInput",
                    $"The 'PartitionKey' parameter of value '{entity.PartitionKey}' is out of range.",
                    new DefaultResponseHeaders(headers => headers.Remove("Cache-Control"))
                );
            if (mappedEntity.IsRowKeyInvalid)
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "OutOfRangeInput",
                    $"The 'RowKey' parameter of value '{entity.RowKey}' is out of range.",
                    new DefaultResponseHeaders(headers => headers.Remove("Cache-Control"))
                );

            if (mappedEntity.IsPartitionKeyExceedingMaxLength || mappedEntity.IsRowKeyExceedingMaxLength || mappedEntity.IsStringPropertyExceedingMaxLength || mappedEntity.IsBinaryPropertyExceedingMaxLength)
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "PropertyValueTooLarge",
                    "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    new DefaultResponseHeaders
                    {
                        { "Preference-Applied", "return-no-content" }
                    }
                );
            if (mappedEntity.InvalidDateTimeProperty != null)
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "OutOfRangeInput",
                    $"The '{mappedEntity.InvalidDateTimeProperty?.Key}' parameter of value '{mappedEntity.InvalidDateTimeProperty?.Value:MM/dd/yyyy HH:mm:ss}' is out of range.",
                    new DefaultResponseHeaders(headers => headers.Remove("Cache-Control"))
                );

            using (_tableServiceClientStub.Tables.ReadLock())
            {
                if (!_tableServiceClientStub.Tables.TryGetValue(_tableName, out var tableItem))
                    throw TableStubResponseFactory.JsonRequestFailedException(
                        HttpStatusCode.NotFound,
                        "TableNotFound",
                        "The table specified does not exist.",
                         new DefaultResponseHeaders(headers => headers.Remove("Cache-Control"))
                    );

                if (!tableItem.TryGetValue(entity.PartitionKey, out var tablePartition))
                {
                    tablePartition = new TablePartitionStub();
                    tableItem.Add(entity.PartitionKey, tablePartition);
                }

                if (tablePartition.ContainsKey(entity.RowKey))
                    throw TableStubResponseFactory.JsonRequestFailedException(
                        HttpStatusCode.Conflict,
                        "EntityAlreadyExists",
                        "The specified entity already exists.",
                        new DefaultResponseHeaders
                        {
                            { "Preference-Applied", "return-no-content" }
                        }
                    );

                tablePartition.Add(entity.RowKey, mappedEntity);

                return TableStubResponseFactory.NoContentResponse(
                    new NoContentResponseHeaders()
                    {
                        { "ETag", mappedEntity.ETag },
                        { "Location", $"{Uri}(PartitionKey='{Uri.EscapeDataString(entity.PartitionKey)}',RowKey='{Uri.EscapeDataString(entity.RowKey)}')" },
                        { "Preference-Applied", "return-no-content" },
                        { "DataServiceId", $"{Uri}(PartitionKey='{Uri.EscapeDataString(entity.PartitionKey)}',RowKey='{Uri.EscapeDataString(entity.RowKey)}')" }
                    }

                );
            }
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

        public override Pageable<T> Query<T>(string filter = null, int? maxPerPage = null, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new PageableStub<T>(_GetTableItemsPageFactory<T>(FilterParser.Parse(FilterScanner.Scan(filter)), select, cancellationToken), maxPerPage);
        }

        public override Pageable<T> Query<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
            => Query<T>(CreateQueryFilter(filter), maxPerPage, select, cancellationToken);

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

        private PageFactory<T> _GetTableItemsPageFactory<T>(Filter filter, IEnumerable<string> selectedProperties, CancellationToken cancellationToken)
            => (continuationToken, pageSize) =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (pageSize < 0 || 1000 < pageSize)
                    throw TableStubResponseFactory.JsonRequestFailedException(
                        HttpStatusCode.BadRequest,
                        "InvalidInput",
                        "One of the request inputs is not valid."
                    );
                if (filter is InvalidFilter invalidFilter)
                    switch (invalidFilter.Type)
                    {
                        case InvalidFilterType.NotImplemented:
                            throw TableStubResponseFactory.JsonRequestFailedException(
                                HttpStatusCode.NotImplemented,
                                "NotImplemented",
                                "The requested operation is not implemented on the specified resource."
                            );

                        case InvalidFilterType.SyntaxError:
                            throw TableStubResponseFactory.JsonRequestFailedException(
                                HttpStatusCode.BadRequest,
                                "InvalidInput",
                                invalidFilter.ErrorMessage,
                                new DefaultResponseHeaders(headers => headers.Remove("Cache-Control"))
                            );

                        default:
                            throw new InvalidOperationException($"Unhandled invalid filter type {invalidFilter.Type}.");
                    }

                var rows = new List<TableRowStub>(pageSize + 1);
                var continuationTokenParts = continuationToken?.Split(ContinuationTokenSeparator, 2);
                var continuationTokenPartitionKey = continuationTokenParts?.First();
                var continuationTokenRowKey = continuationTokenParts?.Last();

                using (_tableServiceClientStub.Tables.ReadLock())
                    if (_tableServiceClientStub.Tables.TryGetValue(_tableName, out var tableItem))
                        using (tableItem.ReadLock())
                            rows.AddRange(
                                tableItem
                                    .SkipWhile(tablePartition => string.Compare(tablePartition.Key, continuationTokenPartitionKey, StringComparison.Ordinal) < 0)
                                    .SelectMany(tablePartition => tablePartition.Value)
                                    .SkipWhile(tableRow => string.Compare(tableRow.Key, continuationTokenRowKey, StringComparison.Ordinal) < 0)
                                    .Select(tableRow => tableRow.Value)
                                    .Where(tableRow => filter == null || filter.Apply(tableRow))
                                    .Take(pageSize + 1)
                            );

                var nextContinuationToken = default(string);
                if (rows.Count > pageSize)
                {
                    rows.RemoveAt(rows.Count - 1);
                    if (rows.Count > 0)
                    {
                        var lastEntity = rows.Last();
                        nextContinuationToken = $"{lastEntity[nameof(ITableEntity.PartitionKey)]} {lastEntity[nameof(ITableEntity.RowKey)]}";
                    }
                }

                var entities = new List<T>(rows.Count);
                entities.AddRange(rows.Select(row => row.MapToEntity<T>(selectedProperties)));

                var page = Page<T>.FromValues(
                    entities,
                    nextContinuationToken,
                    TableStubResponseFactory.EntitiesResponse(
                        new DefaultResponseHeaders(headers =>
                        {
                            if (nextContinuationToken != null)
                            {
                                var lastRow = rows[rows.Count - 1];
                                headers.Add("x-ms-continuation-NextPartitionKey", (string)lastRow[nameof(ITableEntity.PartitionKey)]);
                                headers.Add("x-ms-continuation-NextRowKey", (string)lastRow[nameof(ITableEntity.RowKey)]);
                            }
                        }),
                        $"https://cloudstubdev.table.core.windows.net/$metadata#{_tableName}{(selectedProperties?.Any() ?? false ? "&$select=" + string.Join(",", selectedProperties) : string.Empty)}",
                        rows,
                        selectedProperties
                    )
                );
                return page;
            };
    }
}