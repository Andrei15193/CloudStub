using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub
{
    public class InMemoryCloudTable : CloudTable
    {
        private static IReadOnlyCollection<string> _reservedTableNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "tables"
        };

        private bool _tableExists;
        private readonly IReadOnlyDictionary<TableOperationType, Func<ITableEntity, IDictionary<string, DynamicTableEntity>, OperationContext, TableResult>> _operationHandlers;
        private readonly IDictionary<string, IDictionary<string, DynamicTableEntity>> _entitiesByPartitionKey;
        private readonly object _locker;

        public InMemoryCloudTable(string tableName)
            : base(new Uri($"https://unit.test/{tableName}"))
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));
            else if (tableName.Length == 0)
                throw new ArgumentException("The argument must not be empty string.", nameof(tableName));

            _tableExists = false;
            _operationHandlers = new Dictionary<TableOperationType, Func<ITableEntity, IDictionary<string, DynamicTableEntity>, OperationContext, TableResult>>
            {
                { TableOperationType.Insert, _InsertEntity }
            };
            _entitiesByPartitionKey = new SortedList<string, IDictionary<string, DynamicTableEntity>>(StringComparer.Ordinal);
            _locker = new object();
        }

        public override Task CreateAsync()
        {
            if (Name.Length < 3 || Name.Length > 63)
                return Task.FromException(InvalidTableNameLengthException());
            if (_reservedTableNames.Contains(Name))
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

        public override Task CreateAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => CreateAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task CreateAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => CreateAsync();

        public override Task<bool> CreateIfNotExistsAsync()
        {
            lock (_locker)
            {
                var result = Task.FromResult(!_tableExists);
                _tableExists = true;
                return result;
            }
        }

        public override Task<bool> CreateIfNotExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => CreateIfNotExistsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<bool> CreateIfNotExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => CreateIfNotExistsAsync();

        public override Task<bool> ExistsAsync()
            => Task.FromResult(_tableExists);

        public override Task<bool> ExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => ExistsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<bool> ExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => ExistsAsync();

        public override Task DeleteAsync()
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

        public override Task DeleteAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => DeleteAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task DeleteAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => DeleteAsync();

        public override Task<bool> DeleteIfExistsAsync()
        {
            lock (_locker)
            {
                var result = Task.FromResult(_tableExists);
                _tableExists = false;
                return result;
            }
        }

        public override Task<bool> DeleteIfExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => DeleteIfExistsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<bool> DeleteIfExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => DeleteIfExistsAsync();

        public override Task<IList<TableResult>> ExecuteBatchAsync(TableBatchOperation batch)
            => throw new NotImplementedException();

        public override Task<IList<TableResult>> ExecuteBatchAsync(TableBatchOperation batch, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteBatchAsync(batch, requestOptions, operationContext, CancellationToken.None);

        public override Task<IList<TableResult>> ExecuteBatchAsync(TableBatchOperation batch, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => ExecuteBatchAsync(batch);

        public override Task<TableResult> ExecuteAsync(TableOperation operation)
            => ExecuteAsync(operation, null, null, CancellationToken.None);

        public override Task<TableResult> ExecuteAsync(TableOperation operation, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteAsync(operation, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableResult> ExecuteAsync(TableOperation operation, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            if (_operationHandlers.TryGetValue(operation.OperationType, out var operationHandler))
                lock (_locker)
                {
                    if (!_tableExists)
                        return Task.FromException<TableResult>(TableDoesNotExistException());

                    var entity = operation.Entity;
                    if (entity.PartitionKey == null)
                        return Task.FromException<TableResult>(PropertiesWithoutValueException());
                    if (entity.PartitionKey.Length > 1024)
                        return Task.FromException<TableResult>(PropertyValueTooLarge());
                    if (!entity.PartitionKey.All(_IsValidKeyCharacter))
                        return Task.FromException<TableResult>(InvalidPartitionKeyException(entity.PartitionKey));

                    if (entity.RowKey == null)
                        return Task.FromException<TableResult>(PropertiesWithoutValueException());
                    if (entity.RowKey.Length > 1024)
                        return Task.FromException<TableResult>(PropertyValueTooLarge());
                    if (!entity.RowKey.All(_IsValidKeyCharacter))
                        return Task.FromException<TableResult>(InvalidRowKeyException(entity.RowKey));
                    var partition = _GetPartition(entity);

                    if (partition.ContainsKey(entity.RowKey))
                        return Task.FromException<TableResult>(EntityAlreadyExists());

                    return Task.FromResult(operationHandler(entity, partition, operationContext));
                }

            return Task.FromException<TableResult>(new NotImplementedException());
        }

        public override Task<TableQuerySegment> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token)
            => ExecuteQuerySegmentedAsync(query, token, null, null);

        public override Task<TableQuerySegment> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteQuerySegmentedAsync(query, token, null, null, CancellationToken.None);

        public override Task<TableQuerySegment> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => Task.FromResult(_CreateTableQuerySegment(_entitiesByPartitionKey.Values.SelectMany(partitionedEntities => partitionedEntities.Values)));

        public override Task<TablePermissions> GetPermissionsAsync()
            => throw new NotImplementedException();

        public override Task<TablePermissions> GetPermissionsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => GetPermissionsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<TablePermissions> GetPermissionsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => GetPermissionsAsync();

        public override Task SetPermissionsAsync(TablePermissions permissions)
            => throw new NotImplementedException();

        public override Task SetPermissionsAsync(TablePermissions permissions, TableRequestOptions requestOptions, OperationContext operationContext)
            => SetPermissionsAsync(permissions, requestOptions, operationContext, CancellationToken.None);

        public override Task SetPermissionsAsync(TablePermissions permissions, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => SetPermissionsAsync(permissions);

        private TableResult _InsertEntity(ITableEntity entity, IDictionary<string, DynamicTableEntity> partition, OperationContext operationContext)
        {
            var timestamp = DateTimeOffset.UtcNow;
            var etag = $"{timestamp:o}-{Guid.NewGuid()}";
            partition.Add(
                entity.RowKey,
                new DynamicTableEntity
                {
                    PartitionKey = entity.PartitionKey,
                    RowKey = entity.RowKey,
                    ETag = etag,
                    Timestamp = timestamp,
                    Properties = TableEntity.Flatten(entity, operationContext)
                }
            );

            return new TableResult
            {
                Etag = etag,
                HttpStatusCode = 204,
                Result = new TableEntity
                {
                    PartitionKey = entity.PartitionKey,
                    RowKey = entity.RowKey,
                    ETag = etag,
                    Timestamp = timestamp
                }
            };
        }

        private IDictionary<string, DynamicTableEntity> _GetPartition(ITableEntity entity)
        {
            if (!_entitiesByPartitionKey.TryGetValue(entity.PartitionKey, out var entitiesByRowKey))
            {
                entitiesByRowKey = new SortedList<string, DynamicTableEntity>(StringComparer.Ordinal);
                _entitiesByPartitionKey.Add(entity.PartitionKey, entitiesByRowKey);
            }

            return entitiesByRowKey;
        }

        private static bool _IsValidKeyCharacter(char @char)
            => !char.IsControl(@char)
                && @char != '/'
                && @char != '\\'
                && @char != '#'
                && @char != '?'
                && @char != '\t'
                && @char != '\n'
                && @char != '\r';

        private static TableQuerySegment _CreateTableQuerySegment(IEnumerable<DynamicTableEntity> entities)
        {
            return (TableQuerySegment)typeof(TableQuerySegment)
                .GetTypeInfo()
                .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.CreateInstance)
                .Single(constructor => constructor.GetParameters().Select(parameter => parameter.ParameterType).SequenceEqual(new[] { typeof(List<DynamicTableEntity>) }))
                .Invoke(new[] { entities.Select(_Clone).ToList() });

            DynamicTableEntity _Clone(DynamicTableEntity entity)
                => new DynamicTableEntity
                {
                    PartitionKey = entity.PartitionKey,
                    RowKey = entity.RowKey,
                    ETag = entity.ETag,
                    Timestamp = entity.Timestamp,
                    Properties = new Dictionary<string, EntityProperty>(entity.Properties)
                };
        }
    }
}