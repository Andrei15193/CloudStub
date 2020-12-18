using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using CloudStub.FilterParser;
using CloudStub.TableOperations;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub
{
    public class InMemoryCloudTable : CloudTable
    {
        private static readonly ConstructorInfo _tableQuerySegmentConstructor = typeof(TableQuerySegment<DynamicTableEntity>)
            .GetTypeInfo()
            .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.CreateInstance)
            .Single(constructor => constructor.GetParameters().Select(parameter => parameter.ParameterType).SequenceEqual(new[] { typeof(List<DynamicTableEntity>) }));
        private static readonly PropertyInfo _continuationTokenProperty = typeof(TableQuerySegment<DynamicTableEntity>)
            .GetRuntimeProperty(nameof(TableQuerySegment<DynamicTableEntity>.ContinuationToken));

        private static class TableQuerySegmentInfo<TElement>
        {
            public static ConstructorInfo TableQuerySegmentConstructor { get; } = typeof(TableQuerySegment<TElement>)
                .GetTypeInfo()
                .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.CreateInstance)
                .First(constructor => constructor.GetParameters().Select(parameter => parameter.ParameterType).SequenceEqual(new[] { typeof(List<TElement>) }));

            public static PropertyInfo ContinuationTokenProperty { get; } = typeof(TableQuerySegment<TElement>)
                .GetRuntimeProperty(nameof(TableQuerySegment<TElement>.ContinuationToken));
        }

        private readonly object _locker;
        private bool _tableExists;
        private readonly IReadOnlyDictionary<TableOperationType, TableOperationExecutor> _tableOperationExecutors;
        private readonly IDictionary<string, IDictionary<string, DynamicTableEntity>> _entitiesByPartitionKey;

        public InMemoryCloudTable(string tableName)
            : base(new Uri($"https://unit.test/{tableName}"))
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));
            else if (tableName.Length == 0)
                throw new ArgumentException("The argument must not be empty string.", nameof(tableName));

            _locker = new object();
            _tableExists = false;
            _entitiesByPartitionKey = new SortedDictionary<string, IDictionary<string, DynamicTableEntity>>(StringComparer.Ordinal);

            var context = new TableOperationExecutorContext(this);
            _tableOperationExecutors = new Dictionary<TableOperationType, TableOperationExecutor>
            {
                { TableOperationType.Insert, new InsertTableOperationExecutor(context) },
                { TableOperationType.InsertOrReplace, new InsertOrReplaceTableOperationExecutor(context) },
                { TableOperationType.InsertOrMerge, new InsertOrMergeTableOperationExecutor(context) },
                { TableOperationType.Replace, new ReplaceTableOperationExecutor(context) },
                { TableOperationType.Merge, new MergeTableOperationExecutor(context) },
                { TableOperationType.Delete, new DeleteTableOperationExecutor(context) },
                { TableOperationType.Retrieve, new RetrieveTableOperationExecutor(context) }
            };
        }

        public override void Create(IndexingMode? indexingMode, int? throughput = null, int? defaultTimeToLive = null)
            => Create(null, null, null, throughput, defaultTimeToLive);

        public override void Create(TableRequestOptions requestOptions = null, OperationContext operationContext = null, string serializedIndexingPolicy = null, int? throughput = null, int? defaultTimeToLive = null)
        {
            if (Name.Length < 3 || Name.Length > 63)
                throw InvalidTableNameLengthException();

            var reservedTableNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "tables" };
            if (reservedTableNames.Contains(Name))
                throw InvalidInputException();

            if (!Regex.IsMatch(Name, "^[A-Za-z][A-Za-z0-9]{2,62}$"))
                throw InvalidResourceNameException();

            lock (_locker)
                if (_tableExists)
                    throw TableAlreadyExistsException();
                else
                    _tableExists = true;
        }

        public override Task CreateAsync()
            => CreateAsync(null, null, null, null, null, CancellationToken.None);

        public override Task CreateAsync(CancellationToken cancellationToken)
            => CreateAsync(null, null, null, null, null, cancellationToken);

        public override Task CreateAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => CreateAsync(requestOptions, operationContext, null, null, null, CancellationToken.None);

        public override Task CreateAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => CreateAsync(requestOptions, operationContext, null, null, null, cancellationToken);

        public override Task CreateAsync(int? throughput, IndexingMode indexingMode, int? defaultTimeToLive, CancellationToken cancellationToken)
            => CreateAsync(null, null, null, throughput, defaultTimeToLive, cancellationToken);

        public override Task CreateAsync(int? throughput, string serializedIndexingPolicy, int? defaultTimeToLive, CancellationToken cancellationToken)
            => CreateAsync(null, null, serializedIndexingPolicy, throughput, defaultTimeToLive, cancellationToken);

        public override Task CreateAsync(TableRequestOptions requestOptions, OperationContext operationContext, string serializedIndexingPolicy, int? throughput, int? defaultTimeToLive, CancellationToken cancellationToken)
        {
            if (Name.Length < 3 || Name.Length > 63)
                return Task.FromException(InvalidTableNameLengthException());

            var reservedTableNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "tables" };
            if (reservedTableNames.Contains(Name))
                return Task.FromException(InvalidInputException());

            if (!Regex.IsMatch(Name, "^[A-Za-z][A-Za-z0-9]{2,62}$"))
                return Task.FromException(InvalidResourceNameException());

            lock (_locker)
                if (_tableExists)
                    return Task.FromException(TableAlreadyExistsException());
                else
                    _tableExists = true;

            return Task.CompletedTask;
        }

        public override bool CreateIfNotExists(IndexingMode indexingMode, int? throughput = null, int? defaultTimeToLive = null)
            => CreateIfNotExists(null, null, null, throughput, defaultTimeToLive);

        public override bool CreateIfNotExists(TableRequestOptions requestOptions = null, OperationContext operationContext = null, string serializedIndexingPolicy = null, int? throughput = null, int? defaultTimeToLive = null)
        {
            lock (_locker)
            {
                var result = !_tableExists;
                _tableExists = true;
                return result;
            }
        }

        public override Task<bool> CreateIfNotExistsAsync()
            => CreateIfNotExistsAsync(null, null, null, null, null, CancellationToken.None);

        public override Task<bool> CreateIfNotExistsAsync(CancellationToken cancellationToken)
            => CreateIfNotExistsAsync(null, null, null, null, null, cancellationToken);

        public override Task<bool> CreateIfNotExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => CreateIfNotExistsAsync(requestOptions, operationContext, null, null, null, CancellationToken.None);

        public override Task<bool> CreateIfNotExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => CreateIfNotExistsAsync(requestOptions, operationContext, null, null, null, cancellationToken);

        public override Task<bool> CreateIfNotExistsAsync(IndexingMode indexingMode, int? throughput, int? defaultTimeToLive, CancellationToken cancellationToken)
            => CreateIfNotExistsAsync(null, null, null, throughput, defaultTimeToLive, cancellationToken);

        public override Task<bool> CreateIfNotExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, string serializedIndexingPolicy, int? throughput, int? defaultTimeToLive, CancellationToken cancellationToken)
        {
            lock (_locker)
            {
                var result = Task.FromResult(!_tableExists);
                _tableExists = true;
                return result;
            }
        }

        public override Task<bool> ExistsAsync()
            => ExistsAsync(null, null, CancellationToken.None);

        public override Task<bool> ExistsAsync(CancellationToken cancellationToken)
            => ExistsAsync(null, null, cancellationToken);

        public override Task<bool> ExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => ExistsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<bool> ExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => Task.FromResult(_tableExists);

        public override void Delete(TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            lock (_locker)
            {
                if (!_tableExists)
                    throw ResourceNotFoundException();

                _tableExists = false;
                _entitiesByPartitionKey.Clear();
            }
        }

        public override Task DeleteAsync()
            => DeleteAsync(null, null, CancellationToken.None);

        public override Task DeleteAsync(CancellationToken cancellationToken)
            => DeleteAsync(null, null, cancellationToken);

        public override Task DeleteAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => DeleteAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task DeleteAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            lock (_locker)
            {
                if (!_tableExists)
                    return Task.FromException(ResourceNotFoundException());

                _tableExists = false;
                _entitiesByPartitionKey.Clear();
                return Task.CompletedTask;
            }
        }

        public override bool DeleteIfExists(TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            lock (_locker)
            {
                var result = _tableExists;
                _tableExists = false;
                return result;
            }
        }

        public override Task<bool> DeleteIfExistsAsync()
            => DeleteIfExistsAsync(null, null, CancellationToken.None);

        public override Task<bool> DeleteIfExistsAsync(CancellationToken cancellationToken)
            => DeleteIfExistsAsync(null, null, cancellationToken);

        public override Task<bool> DeleteIfExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => DeleteIfExistsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<bool> DeleteIfExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            lock (_locker)
            {
                var result = Task.FromResult(_tableExists);
                _tableExists = false;
                return result;
            }
        }

        public override TableResult Execute(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            if (operation is null)
                throw new ArgumentNullException(nameof(operation));

            if (_tableOperationExecutors.TryGetValue(operation.OperationType, out var tableOperationExecutor))
                lock (_locker)
                {
                    var exception = tableOperationExecutor.Validate(operation, operationContext);
                    if (exception != null)
                        throw exception;
                    return tableOperationExecutor.Execute(operation, operationContext);
                }

            throw new NotImplementedException();
        }

        public override Task<TableResult> ExecuteAsync(TableOperation operation)
            => ExecuteAsync(operation, null, null, CancellationToken.None);

        public override Task<TableResult> ExecuteAsync(TableOperation operation, CancellationToken cancellationToken)
            => ExecuteAsync(operation, null, null, cancellationToken);

        public override Task<TableResult> ExecuteAsync(TableOperation operation, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteAsync(operation, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableResult> ExecuteAsync(TableOperation operation, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            if (_tableOperationExecutors.TryGetValue(operation.OperationType, out var tableOperationExecutor))
                lock (_locker)
                {
                    var exception = tableOperationExecutor.Validate(operation, operationContext);
                    if (exception != null)
                        return Task.FromException<TableResult>(exception);
                    return Task.FromResult(tableOperationExecutor.Execute(operation, operationContext));
                }

            return Task.FromException<TableResult>(new NotImplementedException());
        }

        public override TableBatchResult ExecuteBatch(TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            if (batch == null)
                throw new ArgumentNullException(nameof(batch));

            lock (_locker)
            {
                var batchOperationException = _ValidateBatchOperation(batch) ?? _ValidateOperationsInBatch(batch, operationContext);
                if (batchOperationException != null)
                    throw batchOperationException;

                var result = new TableBatchResult();
                result.AddRange(
                    batch
                        .Select(
                            tableOperation => _tableOperationExecutors.TryGetValue(tableOperation.OperationType, out var tableOperationExecutor)
                                ? tableOperationExecutor.Execute(
                                    tableOperation.OperationType == TableOperationType.Retrieve ? _WithoutSelectionList(tableOperation) : tableOperation,
                                    operationContext
                                )
                                : null
                        )
                );

                return result;
            }
        }

        public override Task<TableBatchResult> ExecuteBatchAsync(TableBatchOperation batch)
            => ExecuteBatchAsync(batch, null, null, CancellationToken.None);

        public override Task<TableBatchResult> ExecuteBatchAsync(TableBatchOperation batch, CancellationToken cancellationToken)
            => ExecuteBatchAsync(batch, null, null, cancellationToken);

        public override Task<TableBatchResult> ExecuteBatchAsync(TableBatchOperation batch, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteBatchAsync(batch, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableBatchResult> ExecuteBatchAsync(TableBatchOperation batch, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            if (batch == null)
                throw new ArgumentNullException(nameof(batch));

            lock (_locker)
            {
                var batchOperationException = _ValidateBatchOperation(batch) ?? _ValidateOperationsInBatch(batch, operationContext);
                if (batchOperationException != null)
                    return Task.FromException<TableBatchResult>(batchOperationException);

                var result = new TableBatchResult();
                result.AddRange(
                    batch
                        .Select(
                            tableOperation => _tableOperationExecutors.TryGetValue(tableOperation.OperationType, out var tableOperationExecutor)
                                ? tableOperationExecutor.Execute(
                                    tableOperation.OperationType == TableOperationType.Retrieve ? _WithoutSelectionList(tableOperation) : tableOperation,
                                    operationContext
                                )
                                : null
                        )
                );

                return Task.FromResult(result);
            }
        }

        private static Exception _ValidateBatchOperation(TableBatchOperation batch)
        {
            if (batch.Count == 0)
                return new InvalidOperationException("Cannot execute an empty batch operation");
            if (batch.Count > 100)
                return new InvalidOperationException("The maximum number of operations allowed in one batch has been exceeded.");
            if (batch.Count > 1 && batch.Where(operation => operation.OperationType == TableOperationType.Retrieve).Skip(1).Any())
                return new ArgumentException("A batch transaction with a retrieve operation cannot contain any other operations.");
            if (batch.GroupBy(operation => operation.GetPartitionKey()).Skip(1).Any())
                return new ArgumentException("All entities in a given batch must have the same partition key.");

            int? duplicateIndex = null;
            var operationIndex = 0;
            var addedRowKeys = new HashSet<string>(StringComparer.Ordinal);
            while (operationIndex < batch.Count && duplicateIndex == null)
                if (batch[operationIndex].Entity != null && !addedRowKeys.Add(batch[operationIndex].Entity.RowKey))
                    duplicateIndex = operationIndex;
                else
                    operationIndex++;
            if (duplicateIndex != null)
                return MultipleOperationsChangeSameEntityException(duplicateIndex.Value);

            return null;
        }

        private Exception _ValidateOperationsInBatch(TableBatchOperation batch, OperationContext operationContext)
            => batch
                .Select((tableOperation, tableOperationIndex) =>
                    _tableOperationExecutors.TryGetValue(tableOperation.OperationType, out var tableOperationExecutor)
                        ? tableOperationExecutor.ValidateForBatch(tableOperation, operationContext, tableOperationIndex)
                        : null
                )
                .FirstOrDefault(exception => exception != null);

        private TableOperation _WithoutSelectionList(TableOperation tableOperation)
            => TableOperation.Retrieve(tableOperation.GetPartitionKey(), tableOperation.GetRowKey(), tableOperation.GetEntityResolver<object>());

        public override IEnumerable<DynamicTableEntity> ExecuteQuery(TableQuery query, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            lock (_locker)
                return _QueryAllEntities(query.FilterString, query.TakeCount, query.SelectColumns).ToList();
        }

        public override IEnumerable<TResult> ExecuteQuery<TResult>(TableQuery query, EntityResolver<TResult> resolver, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            TResult GetConcreteEntity(DynamicTableEntity existingEntity) =>
                resolver(
                    existingEntity.PartitionKey,
                    existingEntity.RowKey,
                    existingEntity.Timestamp,
                    existingEntity.Properties,
                    existingEntity.ETag
                );

            lock (_locker)
            {
                var result = _QueryAllEntities(query.FilterString, query.TakeCount, query.SelectColumns);
                return result.Select(GetConcreteEntity).ToList();
            }
        }

        public override IEnumerable<TElement> ExecuteQuery<TElement>(TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            => ExecuteQuery(
                new TableQuery().Where(query.FilterString).Take(query.TakeCount).Select(query.SelectColumns),
                TableOperation.Retrieve<TElement>(string.Empty, string.Empty).GetEntityResolver<TElement>(),
                requestOptions,
                operationContext
            );

        public override IEnumerable<TResult> ExecuteQuery<TElement, TResult>(TableQuery<TElement> query, EntityResolver<TResult> resolver, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            => ExecuteQuery(
                new TableQuery().Where(query.FilterString).Take(query.TakeCount).Select(query.SelectColumns),
                resolver,
                requestOptions,
                operationContext
            );

        public override TableQuerySegment<DynamicTableEntity> ExecuteQuerySegmented(TableQuery query, TableContinuationToken token, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            lock (_locker)
            {
                var result = _QueryEntitiesSegmented(query.FilterString, query.TakeCount, query.SelectColumns, token);
                return _CreateTableQuerySegment(result);
            }
        }

        public override TableQuerySegment<TElement> ExecuteQuerySegmented<TElement>(TableQuery<TElement> query, TableContinuationToken token, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            => ExecuteQuerySegmented(
                new TableQuery().Where(query.FilterString).Take(query.TakeCount).Select(query.SelectColumns),
                TableOperation.Retrieve<TElement>(string.Empty, string.Empty).GetEntityResolver<TElement>(),
                token,
                requestOptions,
                operationContext
            );

        public override TableQuerySegment<TResult> ExecuteQuerySegmented<TResult>(TableQuery query, EntityResolver<TResult> resolver, TableContinuationToken token, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            TResult GetConcreteEntity(DynamicTableEntity existingEntity) =>
                resolver(
                    existingEntity.PartitionKey,
                    existingEntity.RowKey,
                    existingEntity.Timestamp,
                    existingEntity.Properties,
                    existingEntity.ETag
                );

            lock (_locker)
            {
                var result = _QueryEntitiesSegmented(query.FilterString, query.TakeCount, query.SelectColumns, token);
                var typedResult = new QueryResult<TResult>(result.Entities.Select(GetConcreteEntity).ToList(), result.ContinuationToken);
                return _CreateTableQuerySegment(typedResult);
            }
        }

        public override TableQuerySegment<TResult> ExecuteQuerySegmented<TElement, TResult>(TableQuery<TElement> query, EntityResolver<TResult> resolver, TableContinuationToken token, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            => ExecuteQuerySegmented(
                new TableQuery().Where(query.FilterString).Take(query.TakeCount).Select(query.SelectColumns),
                resolver,
                token,
                requestOptions,
                operationContext
            );

        public override Task<TableQuerySegment<DynamicTableEntity>> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token)
            => ExecuteQuerySegmentedAsync(query, token, null, null, CancellationToken.None);

        public override Task<TableQuerySegment<DynamicTableEntity>> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token, CancellationToken cancellationToken)
            => ExecuteQuerySegmentedAsync(query, token, null, null, cancellationToken);

        public override Task<TableQuerySegment<DynamicTableEntity>> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteQuerySegmentedAsync(query, token, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableQuerySegment<DynamicTableEntity>> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            lock (_locker)
            {
                var result = _QueryEntitiesSegmented(query.FilterString, query.TakeCount, query.SelectColumns, token);
                return Task.FromResult(_CreateTableQuerySegment(result));
            }
        }

        public override Task<TableQuerySegment<TElement>> ExecuteQuerySegmentedAsync<TElement>(TableQuery<TElement> query, TableContinuationToken token)
            => ExecuteQuerySegmentedAsync(query, token, null, null, CancellationToken.None);

        public override Task<TableQuerySegment<TElement>> ExecuteQuerySegmentedAsync<TElement>(TableQuery<TElement> query, TableContinuationToken token, CancellationToken cancellationToken)
            => ExecuteQuerySegmentedAsync(query, token, null, null, cancellationToken);

        public override Task<TableQuerySegment<TElement>> ExecuteQuerySegmentedAsync<TElement>(TableQuery<TElement> query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteQuerySegmentedAsync(query, token, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableQuerySegment<TElement>> ExecuteQuerySegmentedAsync<TElement>(TableQuery<TElement> query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => ExecuteQuerySegmentedAsync(
                new TableQuery().Where(query.FilterString).Take(query.TakeCount).Select(query.SelectColumns),
                TableOperation.Retrieve<TElement>(string.Empty, string.Empty).GetEntityResolver<TElement>(),
                token,
                requestOptions,
                operationContext,
                cancellationToken
            );

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TResult>(TableQuery query, EntityResolver<TResult> resolver, TableContinuationToken token)
            => ExecuteQuerySegmentedAsync(query, resolver, token, null, null, CancellationToken.None);

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TResult>(TableQuery query, EntityResolver<TResult> resolver, TableContinuationToken token, CancellationToken cancellationToken)
            => ExecuteQuerySegmentedAsync(query, resolver, token, null, null, cancellationToken);

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TResult>(TableQuery query, EntityResolver<TResult> resolver, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteQuerySegmentedAsync(query, resolver, token, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TResult>(TableQuery query, EntityResolver<TResult> resolver, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            TResult GetConcreteEntity(DynamicTableEntity existingEntity) =>
                resolver(
                    existingEntity.PartitionKey,
                    existingEntity.RowKey,
                    existingEntity.Timestamp,
                    existingEntity.Properties,
                    existingEntity.ETag
                );

            lock (_locker)
            {
                var result = _QueryEntitiesSegmented(query.FilterString, query.TakeCount, query.SelectColumns, token);
                var typedResult = new QueryResult<TResult>(result.Entities.Select(GetConcreteEntity).ToList(), result.ContinuationToken);
                return Task.FromResult(_CreateTableQuerySegment(typedResult));
            }
        }

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TElement, TResult>(TableQuery<TElement> query, EntityResolver<TResult> resolver, TableContinuationToken token)
            => ExecuteQuerySegmentedAsync(query, resolver, token, null, null, CancellationToken.None);

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TElement, TResult>(TableQuery<TElement> query, EntityResolver<TResult> resolver, TableContinuationToken token, CancellationToken cancellationToken)
            => ExecuteQuerySegmentedAsync(query, resolver, token, null, null, cancellationToken);

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TElement, TResult>(TableQuery<TElement> query, EntityResolver<TResult> resolver, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteQuerySegmentedAsync(query, resolver, token, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TElement, TResult>(TableQuery<TElement> query, EntityResolver<TResult> resolver, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => ExecuteQuerySegmentedAsync(
                new TableQuery().Where(query.FilterString).Take(query.TakeCount).Select(query.SelectColumns),
                resolver,
                token,
                requestOptions,
                operationContext,
                cancellationToken
            );

        public override TablePermissions GetPermissions(TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            => throw new NotImplementedException();

        public override Task<TablePermissions> GetPermissionsAsync()
            => GetPermissionsAsync(null, null, CancellationToken.None);

        public override Task<TablePermissions> GetPermissionsAsync(CancellationToken cancellationToken)
            => GetPermissionsAsync(null, null, cancellationToken);

        public override Task<TablePermissions> GetPermissionsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => GetPermissionsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<TablePermissions> GetPermissionsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => throw new NotImplementedException();

        public override void SetPermissions(TablePermissions permissions, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            => throw new NotImplementedException();

        public override Task SetPermissionsAsync(TablePermissions permissions)
            => SetPermissionsAsync(permissions, null, null, CancellationToken.None);

        public override Task SetPermissionsAsync(TablePermissions permissions, CancellationToken cancellationToken)
            => SetPermissionsAsync(permissions, null, null, cancellationToken);

        public override Task SetPermissionsAsync(TablePermissions permissions, TableRequestOptions requestOptions, OperationContext operationContext)
            => SetPermissionsAsync(permissions, requestOptions, operationContext, CancellationToken.None);

        public override Task SetPermissionsAsync(TablePermissions permissions, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => throw new NotImplementedException();

        private QueryResult<DynamicTableEntity> _QueryEntitiesSegmented(string filterString, int? takeCount, IEnumerable<string> selectColumns, TableContinuationToken continuationToken)
        {
            const int defaultPageSize = 1000;

            var projection = selectColumns != null && selectColumns.Any()
                ? entity => entity.Clone(selectColumns)
                : new Func<DynamicTableEntity, DynamicTableEntity>(CloudTableExtensions.Clone);

            var allEntities = _entitiesByPartitionKey.Values.SelectMany(partitionedEntities => partitionedEntities.Values);
            var filteredEntities = _ApplyFilter(allEntities, filterString).Select(projection);
            var remainingEntities = continuationToken != null
                    ? filteredEntities
                        .SkipWhile(
                            entity => string.Compare(entity.PartitionKey, continuationToken.NextPartitionKey, StringComparison.Ordinal) <= 0
                                && string.Compare(entity.RowKey, continuationToken.NextRowKey, StringComparison.Ordinal) < 0
                        )
                    : filteredEntities;

            var pageSize = takeCount != null ? Math.Min(defaultPageSize, takeCount.Value) : defaultPageSize;
            var entitiesPage = remainingEntities.Take(pageSize + 1).ToList();
            var lastEntity = default(DynamicTableEntity);
            if (entitiesPage.Count > pageSize)
            {
                lastEntity = entitiesPage[pageSize];
                entitiesPage.RemoveAt(pageSize);
            }
            var nextContinuationToken = lastEntity != null
                ? new TableContinuationToken
                {
                    NextPartitionKey = lastEntity.PartitionKey,
                    NextRowKey = lastEntity.RowKey,
                    TargetLocation = StorageLocation.Primary
                }
                : null;

            return new QueryResult<DynamicTableEntity>(entitiesPage, nextContinuationToken);
        }

        private IEnumerable<DynamicTableEntity> _QueryAllEntities(string filterString, int? takeCount, IEnumerable<string> selectColumns)
        {
            var projection = selectColumns != null && selectColumns.Any()
                ? entity => entity.Clone(selectColumns)
                : new Func<DynamicTableEntity, DynamicTableEntity>(CloudTableExtensions.Clone);

            var allEntities = _entitiesByPartitionKey.Values.SelectMany(partitionedEntities => partitionedEntities.Values);
            var filteredEntities = _ApplyFilter(allEntities, filterString).Select(projection);
            var entitiesPage = takeCount.HasValue ? filteredEntities.Take(takeCount.Value) : filteredEntities;

            return entitiesPage;
        }

        private IEnumerable<DynamicTableEntity> _ApplyFilter(IEnumerable<DynamicTableEntity> entities, string filterString)
        {
            var scanner = new FilterTokenScanner();
            var tokens = scanner.Scan(filterString ?? string.Empty);
            var parser = new FilterTokenParser();
            var predicate = parser.Parse(tokens);
            var result = entities.Where(predicate);
            return result;
        }

        private static TableQuerySegment<DynamicTableEntity> _CreateTableQuerySegment(QueryResult<DynamicTableEntity> result)
        {
            var resultSegment = (TableQuerySegment<DynamicTableEntity>)_tableQuerySegmentConstructor.Invoke(new[] { result.Entities });

            if (result.ContinuationToken != null)
                _continuationTokenProperty.SetValue(resultSegment, result.ContinuationToken);

            return resultSegment;
        }

        private static TableQuerySegment<TElement> _CreateTableQuerySegment<TElement>(QueryResult<TElement> result)
        {
            var resultSegment = (TableQuerySegment<TElement>)TableQuerySegmentInfo<TElement>.TableQuerySegmentConstructor.Invoke(new[] { result.Entities });

            if (result.ContinuationToken != null)
                TableQuerySegmentInfo<TElement>.ContinuationTokenProperty.SetValue(resultSegment, result.ContinuationToken);

            return resultSegment;
        }

        private sealed class TableOperationExecutorContext : ITableOperationExecutorContext
        {
            private readonly InMemoryCloudTable _inMemoryCloudTable;

            public TableOperationExecutorContext(InMemoryCloudTable inMemoryCloudTable)
                => _inMemoryCloudTable = inMemoryCloudTable;

            public bool TableExists
                => _inMemoryCloudTable._tableExists;

            public IDictionary<string, IDictionary<string, DynamicTableEntity>> Entities
                => _inMemoryCloudTable._entitiesByPartitionKey;
        }

        private sealed class QueryResult<TResult>
        {
            public QueryResult(IReadOnlyList<TResult> entities, TableContinuationToken continuationToken)
            {
                Entities = entities;
                ContinuationToken = continuationToken;
            }

            public IReadOnlyList<TResult> Entities { get; }

            public TableContinuationToken ContinuationToken { get; }
        }
    }
}