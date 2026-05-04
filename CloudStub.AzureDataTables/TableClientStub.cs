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

            var mappedEntity = new ValidatedTableRowStub(entity);
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

                using (tableItem.UpgradableReadLock())
                {
                    var partitionExists = tableItem.TryGetValue(entity.PartitionKey, out var tablePartition);

                    if (partitionExists && tablePartition.ContainsKey(entity.RowKey))
                        throw TableStubResponseFactory.JsonRequestFailedException(
                            HttpStatusCode.Conflict,
                            "EntityAlreadyExists",
                            "The specified entity already exists.",
                            new DefaultResponseHeaders
                            {
                                { "Preference-Applied", "return-no-content" }
                            }
                        );

                    using (tableItem.WriteLock())
                    {
                        if (!partitionExists)
                        {
                            tablePartition = new TablePartitionStub();
                            tableItem.Add(entity.PartitionKey, tablePartition);
                        }

                        tablePartition.Add(entity.RowKey, mappedEntity);
                    }
                }
            }

            return TableStubResponseFactory.NoContentResponse(
                new NoContentResponseHeaders()
                {
                    { "ETag", mappedEntity.ETag.ToString() },
                    { "Location", $"{Uri}(PartitionKey='{Uri.EscapeDataString(entity.PartitionKey)}',RowKey='{Uri.EscapeDataString(entity.RowKey)}')" },
                    { "Preference-Applied", "return-no-content" },
                    { "DataServiceId", $"{Uri}(PartitionKey='{Uri.EscapeDataString(entity.PartitionKey)}',RowKey='{Uri.EscapeDataString(entity.RowKey)}')" }
                }
            );
        }

        public override async Task<Response> AddEntityAsync<T>(T entity, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return AddEntity(entity, cancellationToken);
        }

        public override Response UpsertEntity<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        {
            if (entity == null)
                throw new ArgumentNullException("entity")
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
            if (!Enum.IsDefined(typeof(TableUpdateMode), mode))
                throw new ArgumentException($"Unexpected value for mode: {mode}")
                {
                    Source = "Azure.Data.Tables"
                };

            cancellationToken.ThrowIfCancellationRequested();

            if (entity.PartitionKey.Contains((char)0) || entity.RowKey.Contains((char)0))
                throw TableStubResponseFactory.InvalidUriException(
                    HttpStatusCode.BadRequest,
                    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\">\r\n<HTML><HEAD><TITLE>Bad Request</TITLE>\r\n<META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD>\r\n<BODY><h2>Bad Request - Invalid URL</h2>\r\n<hr><p>HTTP Error 400. The request URL is invalid.</p>\r\n</BODY></HTML>\r\n",
                    new InvlaidUrlResponseHeaders
                    {
                        { "Connection", "close" },
                        { "Content-Length", "324" }
                    }
                );

            if (entity.PartitionKey.Contains('/') || entity.PartitionKey.Contains('\\') || entity.RowKey.Contains('/') || entity.RowKey.Contains('\\'))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "InvalidInput",
                    "Bad Request - Error in query syntax.",
                    new DefaultResponseHeaders(headers => headers.Remove("Cache-Control"))
                );

            if (entity.PartitionKey.Any(_IsInvalidUriCharacter) || entity.RowKey.Any(_IsInvalidUriCharacter))
                throw TableStubResponseFactory.InvalidUriException(
                    HttpStatusCode.BadRequest,
                    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\"><HTML><HEAD><TITLE>Bad Request</TITLE><META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD><BODY><h2>Bad Request - Invalid URL</h2><hr><p>HTTP Error 400. The request URL is invalid.</p></BODY></HTML>",
                    new InvlaidUrlResponseHeaders
                    {
                        { "Content-Length", "312" }
                    }
                );

            if (entity.PartitionKey.Any(TableRowStub.IsReservedKeyCharacter) || entity.RowKey.Any(TableRowStub.IsReservedKeyCharacter))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "OutOfRangeInput",
                    "One of the request inputs is out of range."
                );

            var mappedEntity = new ValidatedTableRowStub(entity);
            if (mappedEntity.NotSupportedDateTimeValue != null)
                throw new NotSupportedException($"DateTime {mappedEntity.NotSupportedDateTimeValue} has a Kind of {mappedEntity.NotSupportedDateTimeValue?.Kind}. Azure SDK requires it to be UTC. You can call DateTime.SpecifyKind to change Kind property value to DateTimeKind.Utc.")
                {
                    Source = "Azure.Data.Tables"
                };

            if (mappedEntity.IsPartitionKeyExceedingMaxLength || mappedEntity.IsRowKeyExceedingMaxLength || mappedEntity.IsStringPropertyExceedingMaxLength || mappedEntity.IsBinaryPropertyExceedingMaxLength)
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "PropertyValueTooLarge",
                    "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    new DefaultResponseHeaders()
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

                using (tableItem.WriteLock())
                {
                    if (!tableItem.TryGetValue(entity.PartitionKey, out var tablePartition))
                    {
                        tablePartition = new TablePartitionStub();
                        tableItem.Add(entity.PartitionKey, tablePartition);
                    }

                    if (!tablePartition.TryGetValue(entity.RowKey, out var existingEntity))
                        tablePartition.Add(entity.RowKey, mappedEntity);
                    else
                        switch (mode)
                        {
                            case TableUpdateMode.Merge:
                                foreach (var existingEntityProperty in existingEntity)
                                    if (!mappedEntity.ContainsKey(existingEntityProperty.Key))
                                        mappedEntity.Add(existingEntityProperty.Key, existingEntityProperty.Value);

                                tablePartition[entity.RowKey] = mappedEntity;
                                break;

                            case TableUpdateMode.Replace:
                                tablePartition[entity.RowKey] = mappedEntity;
                                break;

                            default:
                                throw new InvalidOperationException($"Unhandled '{mode}' mode.");
                        }
                }
            }

            return TableStubResponseFactory.NoContentResponse(
                new NoContentResponseHeaders()
                {
                    { "ETag", mappedEntity.ETag.ToString() }
                }
            );
        }

        public override async Task<Response> UpsertEntityAsync<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return UpsertEntity(entity, mode, cancellationToken);
        }

        public override Response UpdateEntity<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        {
            if (entity == null)
                throw new ArgumentNullException("entity")
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
            if (ifMatch == default)
                throw new ArgumentException("Value cannot be empty.", "ifMatch")
                {
                    Source = "Azure.Data.Tables"
                };
            if (!Enum.IsDefined(typeof(TableUpdateMode), mode))
                throw new ArgumentException($"Unexpected value for mode: {mode}")
                {
                    Source = "Azure.Data.Tables"
                };

            cancellationToken.ThrowIfCancellationRequested();

            if (entity.PartitionKey.Contains((char)0) || entity.RowKey.Contains((char)0))
                throw TableStubResponseFactory.InvalidUriException(
                    HttpStatusCode.BadRequest,
                    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\">\r\n<HTML><HEAD><TITLE>Bad Request</TITLE>\r\n<META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD>\r\n<BODY><h2>Bad Request - Invalid URL</h2>\r\n<hr><p>HTTP Error 400. The request URL is invalid.</p>\r\n</BODY></HTML>\r\n",
                    new InvlaidUrlResponseHeaders
                    {
                        { "Connection", "close" },
                        { "Content-Length", "324" }
                    }
                );

            if (entity.PartitionKey.Contains('/') || entity.PartitionKey.Contains('\\') || entity.RowKey.Contains('/') || entity.RowKey.Contains('\\'))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "InvalidInput",
                    "Bad Request - Error in query syntax.",
                    new DefaultResponseHeaders(headers => headers.Remove("Cache-Control"))
                );

            if (entity.PartitionKey.Any(_IsInvalidUriCharacter) || entity.RowKey.Any(_IsInvalidUriCharacter))
                throw TableStubResponseFactory.InvalidUriException(
                    HttpStatusCode.BadRequest,
                    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\"><HTML><HEAD><TITLE>Bad Request</TITLE><META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD><BODY><h2>Bad Request - Invalid URL</h2><hr><p>HTTP Error 400. The request URL is invalid.</p></BODY></HTML>",
                    new InvlaidUrlResponseHeaders
                    {
                        { "Content-Length", "312" }
                    }
                );

            if (entity.PartitionKey.Any(TableRowStub.IsReservedKeyCharacter) || entity.RowKey.Any(TableRowStub.IsReservedKeyCharacter))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "OutOfRangeInput",
                    "One of the request inputs is out of range."
                );

            var mappedEntity = new ValidatedTableRowStub(entity);
            if (mappedEntity.NotSupportedDateTimeValue != null)
                throw new NotSupportedException($"DateTime {mappedEntity.NotSupportedDateTimeValue} has a Kind of {mappedEntity.NotSupportedDateTimeValue?.Kind}. Azure SDK requires it to be UTC. You can call DateTime.SpecifyKind to change Kind property value to DateTimeKind.Utc.")
                {
                    Source = "Azure.Data.Tables"
                };

            if (mappedEntity.IsStringPropertyExceedingMaxLength || mappedEntity.IsBinaryPropertyExceedingMaxLength)
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "PropertyValueTooLarge",
                    "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                    new DefaultResponseHeaders()
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

                using (tableItem.UpgradableReadLock())
                {
                    if (!tableItem.TryGetValue(entity.PartitionKey, out var tablePartition) || !tablePartition.TryGetValue(entity.RowKey, out var tableEntity))
                        throw TableStubResponseFactory.JsonRequestFailedException(
                            HttpStatusCode.NotFound,
                            "ResourceNotFound",
                            "The specified resource does not exist.",
                            new DefaultResponseHeaders()
                        );

                    if (ifMatch != default && ifMatch != ETag.All && ifMatch != tableEntity.ETag)
                        throw TableStubResponseFactory.JsonRequestFailedException(
                            HttpStatusCode.PreconditionFailed,
                            "UpdateConditionNotSatisfied",
                            "The update condition specified in the request was not satisfied."
                        );

                    using (tableItem.WriteLock())
                        switch (mode)
                        {
                            case TableUpdateMode.Merge:
                                foreach (var existingEntityProperty in tableEntity)
                                    if (!mappedEntity.ContainsKey(existingEntityProperty.Key))
                                        mappedEntity.Add(existingEntityProperty.Key, existingEntityProperty.Value);

                                tablePartition[entity.RowKey] = mappedEntity;
                                break;

                            case TableUpdateMode.Replace:
                                tablePartition[entity.RowKey] = mappedEntity;
                                break;

                            default:
                                throw new InvalidOperationException($"Unhandled '{mode}' mode.");
                        }
                }
            }

            return TableStubResponseFactory.NoContentResponse(
                new NoContentResponseHeaders()
                {
                    { "ETag", mappedEntity.ETag.ToString() }
                }
            );
        }

        public override async Task<Response> UpdateEntityAsync<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return UpdateEntity(entity, ifMatch, mode, cancellationToken);
        }

        public override Response DeleteEntity(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
        {
            if (partitionKey == null)
                throw new ArgumentNullException("partitionKey")
                {
                    Source = "Azure.Data.Tables"
                };
            if (rowKey == null)
                throw new ArgumentNullException("rowKey")
                {
                    Source = "Azure.Data.Tables"
                };

            cancellationToken.ThrowIfCancellationRequested();

            if (partitionKey.Contains((char)0) || rowKey.Contains((char)0))
                throw TableStubResponseFactory.InvalidUriException(
                    HttpStatusCode.BadRequest,
                    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\">\r\n<HTML><HEAD><TITLE>Bad Request</TITLE>\r\n<META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD>\r\n<BODY><h2>Bad Request - Invalid URL</h2>\r\n<hr><p>HTTP Error 400. The request URL is invalid.</p>\r\n</BODY></HTML>\r\n",
                    new InvlaidUrlResponseHeaders
                    {
                        { "Connection", "close" },
                        { "Content-Length", "324" }
                    }
                );

            if (partitionKey.Contains('/') || partitionKey.Contains('\\') || rowKey.Contains('/') || rowKey.Contains('\\'))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "InvalidInput",
                    "Bad Request - Error in query syntax.",
                    new DefaultResponseHeaders(headers => headers.Remove("Cache-Control"))
                );

            if (partitionKey.Any(_IsInvalidUriCharacter) || rowKey.Any(_IsInvalidUriCharacter))
                throw TableStubResponseFactory.InvalidUriException(
                    HttpStatusCode.BadRequest,
                    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\"><HTML><HEAD><TITLE>Bad Request</TITLE><META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD><BODY><h2>Bad Request - Invalid URL</h2><hr><p>HTTP Error 400. The request URL is invalid.</p></BODY></HTML>",
                    new InvlaidUrlResponseHeaders
                    {
                        { "Content-Length", "312" }
                    }
                );

            if (partitionKey.Any(TableRowStub.IsReservedKeyCharacter) || rowKey.Any(TableRowStub.IsReservedKeyCharacter))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "OutOfRangeInput",
                    "One of the request inputs is out of range."
                );

            using (_tableServiceClientStub.Tables.ReadLock())
            {
                if (!_tableServiceClientStub.Tables.TryGetValue(_tableName, out var tableItem))
                    return TableStubResponseFactory.UnsuccessfulJsonResponse(
                        HttpStatusCode.NotFound,
                        "TableNotFound",
                        "The table specified does not exist.",
                         new DefaultResponseHeaders(headers => headers.Remove("Cache-Control"))
                    );

                using (tableItem.UpgradableReadLock())
                {
                    if (!tableItem.TryGetValue(partitionKey, out var tablePartition) || !tablePartition.TryGetValue(rowKey, out var tableEntity))
                        return TableStubResponseFactory.UnsuccessfulJsonResponse(
                            HttpStatusCode.NotFound,
                            "ResourceNotFound",
                            "The specified resource does not exist."
                        );

                    if (ifMatch != default && ifMatch != ETag.All && ifMatch != tableEntity.ETag)
                        throw TableStubResponseFactory.JsonRequestFailedException(
                            HttpStatusCode.PreconditionFailed,
                            "UpdateConditionNotSatisfied",
                            "The update condition specified in the request was not satisfied."
                        );

                    using (tableItem.WriteLock())
                        tablePartition.Remove(rowKey);
                }
            }

            return TableStubResponseFactory.NoContentResponse();
        }

        public override async Task<Response> DeleteEntityAsync(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return DeleteEntity(partitionKey, rowKey, ifMatch, cancellationToken);
        }

        public override Response DeleteEntity(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity))
                {
                    Source = "Azure.Data.Tables"
                };

            return DeleteEntity(entity.PartitionKey, entity.RowKey, ifMatch, cancellationToken);
        }

        public override async Task<Response> DeleteEntityAsync(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return DeleteEntity(entity, ifMatch, cancellationToken);
        }

        public override Response<T> GetEntity<T>(string partitionKey, string rowKey, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
        {
            if (partitionKey == null)
                throw new NullReferenceException()
                {
                    Source = "Azure.Data.Tables"
                };
            if (rowKey == null)
                throw new NullReferenceException()
                {
                    Source = "Azure.Data.Tables"
                };

            cancellationToken.ThrowIfCancellationRequested();

            if (partitionKey.Contains((char)0) || rowKey.Contains((char)0))
                throw TableStubResponseFactory.InvalidUriException(
                    HttpStatusCode.BadRequest,
                    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\">\r\n<HTML><HEAD><TITLE>Bad Request</TITLE>\r\n<META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD>\r\n<BODY><h2>Bad Request - Invalid URL</h2>\r\n<hr><p>HTTP Error 400. The request URL is invalid.</p>\r\n</BODY></HTML>\r\n",
                    new InvlaidUrlResponseHeaders
                    {
                        { "Connection", "close" },
                        { "Content-Length", "324" }
                    }
                );

            if (partitionKey.Contains('/') || partitionKey.Contains('\\') || rowKey.Contains('/') || rowKey.Contains('\\'))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "InvalidInput",
                    "Bad Request - Error in query syntax.",
                    new DefaultResponseHeaders(headers => headers.Remove("Cache-Control"))
                );

            if (partitionKey.Any(_IsInvalidUriCharacter) || rowKey.Any(_IsInvalidUriCharacter))
                throw TableStubResponseFactory.InvalidUriException(
                    HttpStatusCode.BadRequest,
                    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\"><HTML><HEAD><TITLE>Bad Request</TITLE><META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD><BODY><h2>Bad Request - Invalid URL</h2><hr><p>HTTP Error 400. The request URL is invalid.</p></BODY></HTML>",
                    new InvlaidUrlResponseHeaders
                    {
                        { "Content-Length", "312" }
                    }
                );

            if (partitionKey.Any(TableRowStub.IsReservedKeyCharacter) || rowKey.Any(TableRowStub.IsReservedKeyCharacter))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.NotFound,
                    "ResourceNotFound",
                    "The specified resource does not exist.",
                    new DefaultResponseHeaders()
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

                using (tableItem.ReadLock())
                {
                    if (!tableItem.TryGetValue(partitionKey, out var tablePartition) || !tablePartition.TryGetValue(rowKey, out var tableEntity))
                        throw TableStubResponseFactory.JsonRequestFailedException(
                            HttpStatusCode.NotFound,
                            "ResourceNotFound",
                            "The specified resource does not exist.",
                            new DefaultResponseHeaders()
                        );

                    var resultEntity = tableEntity.MapToEntity<T>(select?.Concat(Enumerable.Repeat("odata.etag", 1)));
                    return new ResponseStub<T>(
                        TableStubResponseFactory.EntityResponse(
                            new DefaultResponseHeaders()
                            {
                                { "ETag", tableEntity.ETag.ToString() }
                            },
                            $"https://cloudstubdev.table.core.windows.net/$metadata#{_tableName}/@Element{(select?.Any() ?? false ? "&$select=" + string.Join(",", select) : string.Empty)}",
                            tableEntity.ETag.ToString(),
                            tableEntity,
                            select
                        ),
                        resultEntity
                    );
                }
            }
        }

        public override async Task<Response<T>> GetEntityAsync<T>(string partitionKey, string rowKey, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return GetEntity<T>(partitionKey, rowKey, select, cancellationToken);
        }

        public override NullableResponse<T> GetEntityIfExists<T>(string partitionKey, string rowKey, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
        {
            if (partitionKey == null)
                throw new NullReferenceException()
                {
                    Source = "Azure.Data.Tables"
                };
            if (rowKey == null)
                throw new NullReferenceException()
                {
                    Source = "Azure.Data.Tables"
                };

            cancellationToken.ThrowIfCancellationRequested();

            if (partitionKey.Contains((char)0) || rowKey.Contains((char)0))
                throw TableStubResponseFactory.InvalidUriException(
                    HttpStatusCode.BadRequest,
                    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\">\r\n<HTML><HEAD><TITLE>Bad Request</TITLE>\r\n<META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD>\r\n<BODY><h2>Bad Request - Invalid URL</h2>\r\n<hr><p>HTTP Error 400. The request URL is invalid.</p>\r\n</BODY></HTML>\r\n",
                    new InvlaidUrlResponseHeaders
                    {
                        { "Connection", "close" },
                        { "Content-Length", "324" }
                    }
                );

            if (partitionKey.Contains('/') || partitionKey.Contains('\\') || rowKey.Contains('/') || rowKey.Contains('\\'))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "InvalidInput",
                    "Bad Request - Error in query syntax.",
                    new DefaultResponseHeaders(headers => headers.Remove("Cache-Control"))
                );

            if (partitionKey.Any(_IsInvalidUriCharacter) || rowKey.Any(_IsInvalidUriCharacter))
                throw TableStubResponseFactory.InvalidUriException(
                    HttpStatusCode.BadRequest,
                    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"\"http://www.w3.org/TR/html4/strict.dtd\"><HTML><HEAD><TITLE>Bad Request</TITLE><META HTTP-EQUIV=\"Content-Type\" Content=\"text/html; charset=us-ascii\"></HEAD><BODY><h2>Bad Request - Invalid URL</h2><hr><p>HTTP Error 400. The request URL is invalid.</p></BODY></HTML>",
                    new InvlaidUrlResponseHeaders
                    {
                        { "Content-Length", "312" }
                    }
                );

            if (partitionKey.Any(TableRowStub.IsReservedKeyCharacter) || rowKey.Any(TableRowStub.IsReservedKeyCharacter))
                return new ResponseStub<T>(
                    TableStubResponseFactory.UnsuccessfulJsonResponse(
                        HttpStatusCode.NotFound,
                        "ResourceNotFound",
                        "The specified resource does not exist.",
                        new DefaultResponseHeaders()
                    )
                );

            using (_tableServiceClientStub.Tables.ReadLock())
            {
                if (!_tableServiceClientStub.Tables.TryGetValue(_tableName, out var tableItem))
                    return new ResponseStub<T>(
                        TableStubResponseFactory.UnsuccessfulJsonResponse(
                            HttpStatusCode.NotFound,
                            "TableNotFound",
                            "The table specified does not exist.",
                            new DefaultResponseHeaders(headers => headers.Remove("Cache-Control"))
                        )
                    );

                using (tableItem.ReadLock())
                {
                    if (!tableItem.TryGetValue(partitionKey, out var tablePartition) || !tablePartition.TryGetValue(rowKey, out var tableEntity))
                        return new ResponseStub<T>(
                            TableStubResponseFactory.UnsuccessfulJsonResponse(
                                HttpStatusCode.NotFound,
                                "ResourceNotFound",
                                "The specified resource does not exist.",
                                new DefaultResponseHeaders()
                            )
                        );

                    var resultEntity = tableEntity.MapToEntity<T>(select?.Concat(Enumerable.Repeat("odata.etag", 1)));
                    return new ResponseStub<T>(
                        TableStubResponseFactory.EntityResponse(
                            new DefaultResponseHeaders()
                            {
                                { "ETag", tableEntity.ETag.ToString() }
                            },
                            $"https://cloudstubdev.table.core.windows.net/$metadata#{_tableName}/@Element{(select?.Any() ?? false ? "&$select=" + string.Join(",", select) : string.Empty)}",
                            tableEntity.ETag.ToString(),
                            tableEntity,
                            select
                        ),
                        resultEntity
                    );
                }
            }
        }

        public override async Task<NullableResponse<T>> GetEntityIfExistsAsync<T>(string partitionKey, string rowKey, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return GetEntityIfExists<T>(partitionKey, rowKey, select, cancellationToken);
        }

        public override Response<IReadOnlyList<Response>> SubmitTransaction(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
        {
            if (transactionActions == null)
                throw new ArgumentNullException("transactionalBatch") { Source = "Azure.Data.Tables" };

            var mappedTransactionActions = new List<TableTransactionAction>(101);
            mappedTransactionActions.AddRange(transactionActions.Select((tableTransactionAction, tableTransactionActionIndex) =>
            {
                if (tableTransactionActionIndex > 99)
                    throw TableStubResponseFactory.TransactionJsonRequestFailedException(
                        HttpStatusCode.BadRequest,
                        "InvalidInput",
                        "The batch request operation exceeds the maximum 100 changes per change set.",
                        99
                    );

                if (!Enum.IsDefined(typeof(TableTransactionActionType), tableTransactionAction.ActionType))
                    throw new InvalidOperationException("Unknown request type.") { Source = "Azure.Data.Tables" };

                if (tableTransactionAction.Entity == null)
                    throw new NullReferenceException { Source = "Azure.Data.Tables" };

                var validatedEntity = new ValidatedTableRowStub(tableTransactionAction.Entity);
                if (validatedEntity.NotSupportedDateTimeValue != null)
                    throw new NotSupportedException($"DateTime {validatedEntity.NotSupportedDateTimeValue} has a Kind of {validatedEntity.NotSupportedDateTimeValue?.Kind}. Azure SDK requires it to be UTC. You can call DateTime.SpecifyKind to change Kind property value to DateTimeKind.Utc.") { Source = "Azure.Data.Tables" };

                return new TableTransactionAction(
                    tableTransactionAction.ActionType,
                    validatedEntity,
                    tableTransactionAction.ETag
                );
            }));

            if (mappedTransactionActions.Count == 0)
                throw new InvalidOperationException("The batch contains no entity operations.") { Source = "Azure.Data.Tables" };
            if (mappedTransactionActions.Any(mappedTransactionAction => mappedTransactionAction.Entity.RowKey == null))
                throw new ArgumentNullException("key") { Source = "Azure.Data.Tables" };

            cancellationToken.ThrowIfCancellationRequested();

            using (_tableServiceClientStub.Tables.ReadLock())
            {
                if (!_tableServiceClientStub.Tables.TryGetValue(_tableName, out var tableItem))
                    throw TableStubResponseFactory.TransactionJsonRequestFailedException(
                        HttpStatusCode.NotFound,
                        "TableNotFound",
                        "The table specified does not exist.",
                        errorIndex: 0
                    );

                using (tableItem.UpgradableReadLock())
                {
                    var partitionKey = mappedTransactionActions[0].Entity.PartitionKey;
                    var rowKeys = new HashSet<string>(((ValidatedTableRowStub)mappedTransactionActions[0].Entity).Comparer);
                    for (var transactionActionIndex = 0; transactionActionIndex < mappedTransactionActions.Count; transactionActionIndex++)
                    {
                        var transactionAction = mappedTransactionActions[transactionActionIndex];
                        var transactionActionEntity = (ValidatedTableRowStub)transactionAction.Entity;

                        if (transactionActionEntity.PartitionKey == null)
                            throw TableStubResponseFactory.TransactionJsonRequestFailedException(
                                HttpStatusCode.BadRequest,
                                "PropertiesNeedValue",
                                "The values are not specified for all properties in the entity.",
                                transactionActionIndex
                            );

                        if (transactionActionEntity.IsPartitionKeyInvalid)
                            throw TableStubResponseFactory.TransactionJsonRequestFailedException(
                                HttpStatusCode.BadRequest,
                                "OutOfRangeInput",
                                $"The 'PartitionKey' parameter of value '{transactionActionEntity.PartitionKey}' is out of range.",
                                transactionActionIndex
                            );

                        if (transactionActionEntity.IsRowKeyInvalid)
                            throw TableStubResponseFactory.TransactionJsonRequestFailedException(
                                HttpStatusCode.BadRequest,
                                "OutOfRangeInput",
                                $"The 'RowKey' parameter of value '{transactionActionEntity.RowKey}' is out of range.",
                                transactionActionIndex
                            );

                        if (transactionActionEntity.IsPartitionKeyExceedingMaxLength || transactionActionEntity.IsRowKeyExceedingMaxLength || transactionActionEntity.IsStringPropertyExceedingMaxLength || transactionActionEntity.IsBinaryPropertyExceedingMaxLength)
                            throw TableStubResponseFactory.TransactionJsonRequestFailedException(
                                HttpStatusCode.BadRequest,
                                "PropertyValueTooLarge",
                                "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less.",
                                mappedTransactionActions.Count > 1 ? transactionActionIndex : (int?)null
                            );

                        if (transactionActionEntity.InvalidDateTimeProperty != null)
                            throw TableStubResponseFactory.TransactionJsonRequestFailedException(
                                HttpStatusCode.BadRequest,
                                "OutOfRangeInput",
                                $"The '{transactionActionEntity.InvalidDateTimeProperty.Value.Key}' parameter of value '{transactionActionEntity.InvalidDateTimeProperty.Value.Value:MM/dd/yyyy HH:mm:ss}' is out of range.",
                                transactionActionIndex
                            );

                        if (transactionActionEntity.PartitionKey != partitionKey)
                            throw TableStubResponseFactory.TransactionJsonRequestFailedException(
                                HttpStatusCode.BadRequest,
                                "CommandsInBatchActOnDifferentPartitions",
                                "All commands in a batch must operate on same entity group.",
                                transactionActionIndex
                            );
                        if (!rowKeys.Add(transactionActionEntity.RowKey))
                            throw TableStubResponseFactory.TransactionJsonRequestFailedException(
                                HttpStatusCode.BadRequest,
                                "InvalidDuplicateRow",
                                "The batch request contains multiple changes with same row key. An entity can appear only once in a batch request.",
                                transactionActionIndex
                            );

                        if (
                            transactionAction.ActionType == TableTransactionActionType.Add
                            && tableItem.TryGetValue(transactionActionEntity.PartitionKey, out var tablePartition)
                            && tablePartition.ContainsKey(transactionActionEntity.RowKey)
                        )
                            throw TableStubResponseFactory.TransactionJsonRequestFailedException(
                                HttpStatusCode.Conflict,
                                "EntityAlreadyExists",
                                "The specified entity already exists.",
                                mappedTransactionActions.Count > 1 ? transactionActionIndex : (int?)null
                            );
                    }

                    using (tableItem.WriteLock())
                        return _ApplyTransaction(tableItem, mappedTransactionActions);
                }
            }
        }

        public override async Task<Response<IReadOnlyList<Response>>> SubmitTransactionAsync(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return SubmitTransaction(transactionActions, cancellationToken);
        }

        public override Pageable<T> Query<T>(string filter = null, int? maxPerPage = null, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
            => new PageableStub<T>(_GetEntityPageFactory<T>(FilterParser.Parse(FilterScanner.Scan(filter)), select, cancellationToken), maxPerPage);

        public override Pageable<T> Query<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
            => Query<T>(CreateQueryFilter(filter), maxPerPage, select, cancellationToken);

        public override AsyncPageable<T> QueryAsync<T>(string filter = null, int? maxPerPage = null, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
            => new AsyncPageableStub<T>(_GetEntityPageFactory<T>(FilterParser.Parse(FilterScanner.Scan(filter)), select, cancellationToken), maxPerPage);

        public override AsyncPageable<T> QueryAsync<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
            => QueryAsync<T>(CreateQueryFilter(filter), maxPerPage, select, cancellationToken);

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

            return new ResponseStub<IReadOnlyList<TableSignedIdentifier>>(TableStubResponseFactory.SuccessfulXmlResponse(XmlSeriaizer.Serialize(signedIdentifiersCopy)), signedIdentifiersCopy);
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

        private Response<IReadOnlyList<Response>> _ApplyTransaction(TableItemStub tableItem, IReadOnlyList<TableTransactionAction> transactionActions)
        {
            var partitionKey = transactionActions[0].Entity.PartitionKey;
            if (!tableItem.TryGetValue(partitionKey, out var tablePartition))
            {
                tablePartition = new TablePartitionStub();
                tableItem.Add(partitionKey, tablePartition);
            }

            var responses = new List<Response>(transactionActions.Count);
            foreach (var transactionAction in transactionActions)
            {
                ResponseStub response;
                var transactionActionEntity = (ValidatedTableRowStub)transactionAction.Entity;
                switch (transactionAction.ActionType)
                {
                    case TableTransactionActionType.Add:
                        tablePartition.Add(transactionActionEntity.RowKey, transactionActionEntity);
                        response = TableStubResponseFactory.NoContentResponse(new TransactionActionResponseHeaders(tableItem.TableName, transactionActionEntity.PartitionKey, transactionActionEntity.RowKey, transactionActionEntity.ETag.ToString()));
                        break;

                    default:
                        throw new NotImplementedException();
                }

                response.ClientRequestId = null;
                responses.Add(response);
            }

            return TableStubResponseFactory.TransactionResponse(responses);
        }

        private PageFactory<T> _GetEntityPageFactory<T>(Filter filter, IEnumerable<string> selectedProperties, CancellationToken cancellationToken)
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

                        case InvalidFilterType.NotSupported:
                            throw TableStubResponseFactory.JsonRequestFailedException(
                                HttpStatusCode.BadRequest,
                                "InvalidInput",
                                invalidFilter.ErrorMessage,
                                new DefaultResponseHeaders()
                            );

                        default:
                            throw new InvalidOperationException($"Unhandled invalid filter type {invalidFilter.Type}.");
                    }

                var rows = new List<TableRowStub>(pageSize + 1);
                var (continuationTokenPartitionKey, continuationTokenRowKey) = ResponseContinuationToken.DecodeRowContinuationToken(continuationToken);

                using (_tableServiceClientStub.Tables.ReadLock())
                    if (_tableServiceClientStub.Tables.TryGetValue(_tableName, out var tableItem))
                        using (tableItem.ReadLock())
                            rows.AddRange(_GetEntitiesPage(tableItem, continuationTokenPartitionKey, continuationTokenRowKey, filter, pageSize + 1));

                var nextContinuationTokenPartitionKey = default(string);
                var nextContinuationTokenRowKey = default(string);
                var nextContinuationToken = default(string);
                if (rows.Count > pageSize)
                {
                    var nextEntity = rows.Last();
                    rows.RemoveAt(rows.Count - 1);

                    if (rows.Count > 0)
                    {
                        nextContinuationTokenPartitionKey = ResponseContinuationToken.EncodeContinuationToken((string)nextEntity[nameof(ITableEntity.PartitionKey)]);
                        nextContinuationTokenRowKey = ResponseContinuationToken.EncodeContinuationToken((string)nextEntity[nameof(ITableEntity.RowKey)]);
                        nextContinuationToken = $"{nextContinuationTokenPartitionKey} {nextContinuationTokenRowKey}";
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
                                headers.Add("x-ms-continuation-NextPartitionKey", nextContinuationTokenPartitionKey);
                                headers.Add("x-ms-continuation-NextRowKey", nextContinuationTokenRowKey);
                            }
                        }),
                        $"https://cloudstubdev.table.core.windows.net/$metadata#{_tableName}{(selectedProperties?.Any() ?? false ? "&$select=" + string.Join(",", selectedProperties) : string.Empty)}",
                        rows,
                        selectedProperties
                    )
                );
                return page;
            };

        private static IEnumerable<TableRowStub> _GetEntitiesPage(TableItemStub tableItem, string partitionKeyStart, string rowKeyStart, Filter filter, int pageSize)
        {
            var partitionIndex = ResponseContinuationToken.SuccessorSearch(tableItem.Keys, partitionKeyStart, tableItem.Comparer);
            var count = 0;

            if (partitionIndex < tableItem.Count && count < pageSize)
            {
                var partition = tableItem[tableItem.Keys[partitionIndex]];
                var rowIndex = ResponseContinuationToken.SuccessorSearch(partition.Keys, rowKeyStart, partition.Comparer);
                while (rowIndex < partition.Count && count < pageSize)
                {
                    var row = partition.Values[rowIndex];
                    if (filter.Apply(row))
                    {
                        count++;
                        yield return row;
                    }
                    rowIndex++;
                }
                partitionIndex++;
            }

            while (partitionIndex < tableItem.Count && count < pageSize)
            {
                var partition = tableItem[tableItem.Keys[partitionIndex]];
                var rowIndex = 0;
                while (rowIndex < partition.Count && count < pageSize)
                {
                    var row = partition.Values[rowIndex];
                    if (filter == null || filter.Apply(row))
                    {
                        count++;
                        yield return row;
                    }

                    rowIndex++;
                }
                partitionIndex++;
            }
        }

        private static bool _IsInvalidUriCharacter(char @char)
            => (
                (0x00 <= @char && @char <= 0x001F)
                || @char == 0x007F
                || @char == 0x0081
                || @char == 0x008D
                || @char == 0x008F
                || @char == 0x0090
                || @char == 0x009D
            );
    }
}