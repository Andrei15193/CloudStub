using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text.RegularExpressions;
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
    public class TableServiceClientStub : TableServiceClient
    {
        private static readonly IReadOnlyCollection<string> _reservedTableNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "tables" };
        private readonly string _accountName;
        private volatile TableServiceProperties _tableServiceProperties = new TableServiceProperties
        {
            Logging = new TableAnalyticsLoggingSettings("1.0", false, false, false, new TableRetentionPolicy(false)),
            HourMetrics = new TableMetrics(false)
            {
                Version = "1.0"
            },
            MinuteMetrics = new TableMetrics(false)
            {
                Version = "1.0"
            }
        };

        public TableServiceClientStub(string accountName)
        {
            if (accountName == null)
                throw new ArgumentNullException(nameof(accountName));
            if (string.IsNullOrWhiteSpace(accountName))
                throw new ArgumentException("Account name cannot be empty or whitespace.", nameof(accountName));

            _accountName = accountName;
            Tables = new TableCollectionStub();
        }

        public TableServiceClientStub()
            : this("StubStorageAccount")
        {
        }

        internal TableCollectionStub Tables { get; }

        public override string AccountName
            => _accountName;

        public override Uri Uri
            => new Uri($"http://{_accountName}.cloud.stub");

        public override TableClient GetTableClient(string tableName)
            => new TableClientStub(this, tableName);

        public override Response<TableItem> CreateTable(string tableName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!(3 <= tableName.Length && tableName.Length <= 63))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "OutOfRangeInput",
                    "The specified resource name length is not within the permissible limits."
                );

            if (_reservedTableNames.Contains(tableName))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "InvalidInput",
                    "One of the request inputs is not valid."
                );

            if (!Regex.IsMatch(tableName, "^[a-z][a-z0-9]{2,62}$", RegexOptions.IgnoreCase))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "InvalidResourceName",
                    "The specifed resource name contains invalid characters."
                );

            using (Tables.UpgradableReadLock())
            {
                if (Tables.ContainsKey(tableName))
                    throw TableStubResponseFactory.JsonRequestFailedException(
                        HttpStatusCode.Conflict,
                        "TableAlreadyExists",
                        "The table specified already exists."
                    );

                using (Tables.WriteLock())
                    Tables.Add(tableName, new TableItemStub());

                return Response.FromValue(TableModelFactory.TableItem(tableName), TableStubResponseFactory.TableCreatedResponse(Uri, tableName));
            }
        }

        public override async Task<Response<TableItem>> CreateTableAsync(string tableName, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return CreateTable(tableName, cancellationToken);
        }

        public override Response<TableItem> CreateTableIfNotExists(string tableName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!(3 <= tableName.Length && tableName.Length <= 63))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "OutOfRangeInput",
                    "The specified resource name length is not within the permissible limits.",
                    new DefaultResponseHeaders
                    {
                        { "Preference-Applied", "return-no-content" }
                    }
                );

            if (_reservedTableNames.Contains(tableName))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "InvalidInput",
                    "One of the request inputs is not valid.",
                    new DefaultResponseHeaders
                    {
                        { "Preference-Applied", "return-no-content" }
                    }
                );

            if (!Regex.IsMatch(tableName, "^[a-z][a-z0-9]{2,62}$", RegexOptions.IgnoreCase))
                throw TableStubResponseFactory.JsonRequestFailedException(
                    HttpStatusCode.BadRequest,
                    "InvalidResourceName",
                    "The specifed resource name contains invalid characters.",
                    new DefaultResponseHeaders
                    {
                        { "Preference-Applied", "return-no-content" }
                    }
                );

            using (Tables.UpgradableReadLock())
            {
                if (Tables.ContainsKey(tableName))
                    return Response.FromValue(
                        TableModelFactory.TableItem(tableName),
                        TableStubResponseFactory.UnsuccessfulJsonResponse(
                            HttpStatusCode.Conflict,
                            "TableAlreadyExists",
                            "The table specified already exists.",
                            new DefaultResponseHeaders
                            {
                                { "Preference-Applied", "return-no-content" }
                            }
                        )
                    );

                using (Tables.WriteLock())
                    Tables.Add(tableName, new TableItemStub());

                return Response.FromValue(
                    TableModelFactory.TableItem(tableName),
                    TableStubResponseFactory.NoContentResponse(
                        new NoContentResponseHeaders
                        {
                            { "Location", $"{Uri}Tables('{tableName}')" },
                            { "Preference-Applied", "return-no-content" },
                            { "DataServiceId", $"{Uri}Tables('{tableName}')"}
                        }
                    )
                );
            }
        }

        public override async Task<Response<TableItem>> CreateTableIfNotExistsAsync(string tableName, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return CreateTableIfNotExists(tableName, cancellationToken);
        }

        public override Response DeleteTable(string tableName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using (Tables.WriteLock())
                if (!Tables.Remove(tableName))
                    return TableStubResponseFactory.UnsuccessfulJsonResponse(HttpStatusCode.NotFound, "ResourceNotFound", "The specified resource does not exist.");
                else
                    return TableStubResponseFactory.NoContentResponse();
        }

        public override async Task<Response> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return DeleteTable(tableName, cancellationToken);
        }

        public override Pageable<TableItem> Query(string filter = null, int? maxPerPage = null, CancellationToken cancellationToken = default)
            => new PageableStub<TableItem>(_GetTableItemsPageFactory(FilterParser.Parse(FilterScanner.Scan(filter)), cancellationToken), maxPerPage);

        public override Pageable<TableItem> Query(FormattableString filter, int? maxPerPage = null, CancellationToken cancellationToken = default)
            => Query(CreateQueryFilter(filter), maxPerPage, cancellationToken);

        public override Pageable<TableItem> Query(Expression<Func<TableItem, bool>> filter, int? maxPerPage = null, CancellationToken cancellationToken = default)
            => Query(CreateQueryFilter(filter), maxPerPage, cancellationToken);

        public override AsyncPageable<TableItem> QueryAsync(string filter = null, int? maxPerPage = null, CancellationToken cancellationToken = default)
            => new AsyncPageableStub<TableItem>(_GetTableItemsPageFactory(FilterParser.Parse(FilterScanner.Scan(filter)), cancellationToken), maxPerPage);

        public override AsyncPageable<TableItem> QueryAsync(FormattableString filter, int? maxPerPage = null, CancellationToken cancellationToken = default)
            => QueryAsync(CreateQueryFilter(filter), maxPerPage, cancellationToken);

        public override AsyncPageable<TableItem> QueryAsync(Expression<Func<TableItem, bool>> filter, int? maxPerPage = null, CancellationToken cancellationToken = default)
            => QueryAsync(CreateQueryFilter(filter), maxPerPage, cancellationToken);

        public override Response<TableServiceProperties> GetProperties(CancellationToken cancellationToken = default)
        {
            TableServiceProperties tableServiceProperties;
            using (Tables.ReadLock())
                tableServiceProperties = _CopyTableServiceProperties(_tableServiceProperties);

            return Response.FromValue(tableServiceProperties, TableStubResponseFactory.SuccessfulXmlResponse(XmlSeriaizer.Serialize(tableServiceProperties)));
        }

        public override async Task<Response<TableServiceProperties>> GetPropertiesAsync(CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return GetProperties(cancellationToken);
        }

        public override Response SetProperties(TableServiceProperties properties, CancellationToken cancellationToken = default)
        {
            if (properties == null)
                throw new ArgumentNullException("tableServiceProperties")
                {
                    Source = "Azure.Data.Tables"
                };

            if (properties.Logging == null && properties.HourMetrics == null && properties.MinuteMetrics == null)
                throw TableStubResponseFactory.XmlRequestFailedException(HttpStatusCode.BadRequest, "InvalidXmlDocument", "XML specified is not syntactically valid.");

            using (Tables.WriteLock())
                _tableServiceProperties = _CopyTableServiceProperties(properties);

            return TableStubResponseFactory.AcceptedResponse();
        }

        public override async Task<Response> SetPropertiesAsync(TableServiceProperties properties, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return SetProperties(properties, cancellationToken);
        }

        public override Response<TableServiceStatistics> GetStatistics(CancellationToken cancellationToken = default)
            => throw new NotImplementedException("CloudStub does not simulate Geo replication, similar to storage accounts without this setting the method throws an exception.");

        public override async Task<Response<TableServiceStatistics>> GetStatisticsAsync(CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return GetStatistics(cancellationToken);
        }

        public override Uri GenerateSasUri(TableAccountSasBuilder builder)
            => new UriBuilder(Uri) { Query = "st=stub-sas-token" }.Uri;

        public override Uri GenerateSasUri(TableAccountSasPermissions permissions, TableAccountSasResourceTypes resourceTypes, DateTimeOffset expiresOn)
            => GenerateSasUri(GetSasBuilder(permissions, resourceTypes, expiresOn));

        private PageFactory<TableItem> _GetTableItemsPageFactory(Filter filter, CancellationToken cancellationToken)
            => (continuationToken, pageSize) =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (pageSize < 0)
                    throw TableStubResponseFactory.JsonRequestFailedException(
                        HttpStatusCode.BadRequest,
                        "InvalidInput",
                        "One of the request inputs is not valid."
                    );
                if (filter is InvalidFilter invalidFilter)
                    throw TableStubResponseFactory.JsonRequestFailedException(
                        HttpStatusCode.NotImplemented,
                        "NotImplemented",
                        "The requested operation is not implemented on the specified resource."
                    );
                if (pageSize == 0 || filter.FilteredProperties.Any(propertyName => !string.Equals(propertyName, "TableName", StringComparison.OrdinalIgnoreCase)))
                    throw TableStubResponseFactory.JsonRequestFailedException(
                        HttpStatusCode.InternalServerError,
                        "InternalError",
                        "Server encountered an internal error. Please try again after some time."
                    );

                var tables = new List<IReadOnlyDictionary<string, object>>(pageSize + 1);
                using (Tables.ReadLock())
                    tables.AddRange(
                        Tables
                            .Keys
                            .SkipWhile(tableName => continuationToken != null && string.Compare(tableName, continuationToken, StringComparison.OrdinalIgnoreCase) <= 0)
                            .Select(tableName => new Dictionary<string, object> { { "TableName", tableName } })
                            .Where(table => filter.Apply(table))
                            .Take(pageSize + 1)
                    );

                var nextContinuationToken = default(string);
                if (tables.Count > pageSize)
                {
                    tables.RemoveAt(tables.Count - 1);
                    nextContinuationToken = (string)tables.Last()["TableName"];
                }

                var tableItems = new List<TableItem>(tables.Count);
                tableItems.AddRange(tables.Select(table => TableModelFactory.TableItem((string)table["TableName"])));

                var page = Page<TableItem>.FromValues(
                    tableItems,
                    nextContinuationToken,
                    TableStubResponseFactory.EntitiesResponse(
                        new DefaultResponseHeaders(),
                        "https://cloudstubdev.table.core.windows.net/$metadata#Tables",
                        tables
                    )
                );
                return page;
            };

        private static TableServiceProperties _CopyTableServiceProperties(TableServiceProperties tableServiceProperties)
        {
            var tableServicePropertiesCopy = new TableServiceProperties
            {
                Logging = new TableAnalyticsLoggingSettings(
                    tableServiceProperties.Logging.Version,
                    tableServiceProperties.Logging.Delete,
                    tableServiceProperties.Logging.Read,
                    tableServiceProperties.Logging.Write,
                    new TableRetentionPolicy(tableServiceProperties.Logging.RetentionPolicy.Enabled)
                    {
                        Days = tableServiceProperties.Logging.RetentionPolicy.Days
                    }
                ),
                HourMetrics = new TableMetrics(tableServiceProperties.HourMetrics.Enabled)
                {
                    Version = tableServiceProperties.HourMetrics.Version,
                    RetentionPolicy = new TableRetentionPolicy(tableServiceProperties.HourMetrics.RetentionPolicy?.Enabled ?? false)
                    {
                        Days = tableServiceProperties.HourMetrics.RetentionPolicy?.Days
                    }
                },
                MinuteMetrics = new TableMetrics(tableServiceProperties.MinuteMetrics.Enabled)
                {
                    Version = tableServiceProperties.MinuteMetrics.Version,
                    RetentionPolicy = new TableRetentionPolicy(tableServiceProperties.MinuteMetrics.RetentionPolicy?.Enabled ?? false)
                    {
                        Days = tableServiceProperties.HourMetrics.RetentionPolicy?.Days
                    }
                }
            };

            foreach (var rule in tableServiceProperties.Cors)
                tableServicePropertiesCopy.Cors.Add(new TableCorsRule(
                    rule.AllowedOrigins,
                    rule.AllowedMethods,
                    rule.AllowedHeaders,
                    rule.ExposedHeaders,
                    rule.MaxAgeInSeconds
                ));

            return tableServicePropertiesCopy;
        }
    }
}