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
        private delegate Task<TableResult> OperationHandler(ITableEntity entity, OperationContext operationContext);

        private static IReadOnlyCollection<string> _reservedTableNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "tables"
        };

        private bool _tableExists;
        private readonly IReadOnlyDictionary<TableOperationType, OperationHandler> _operationHandlers;
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
            _operationHandlers = new Dictionary<TableOperationType, OperationHandler>
            {
                { TableOperationType.Insert, _InsertEntity },
                { TableOperationType.InsertOrReplace, _InsertOrReplaceEntity },
                { TableOperationType.InsertOrMerge, _InsertOrMergeEntity }
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

                    return operationHandler(operation.Entity, operationContext);
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

        private Task<TableResult> _InsertEntity(ITableEntity entity, OperationContext operationContext)
        {
            var entityException = _ValidateEntity(entity);
            if (entityException != null)
                return Task.FromException<TableResult>(entityException);

            var dynamicEntity = _GetDynamicEntity(entity, operationContext);
            var dynamicEntityException = dynamicEntity
                .Properties
                .Select(property => _ValidateEntityProperty(property.Key, property.Value))
                .FirstOrDefault(exception => exception != null);
            if (dynamicEntityException != null)
                return Task.FromException<TableResult>(dynamicEntityException);

            var partition = _GetPartition(dynamicEntity);

            if (partition.ContainsKey(dynamicEntity.RowKey))
                return Task.FromException<TableResult>(EntityAlreadyExists());

            partition.Add(dynamicEntity.RowKey, dynamicEntity);

            return Task.FromResult(
                new TableResult
                {
                    Etag = dynamicEntity.ETag,
                    HttpStatusCode = 204,
                    Result = new TableEntity
                    {
                        PartitionKey = dynamicEntity.PartitionKey,
                        RowKey = dynamicEntity.RowKey,
                        ETag = dynamicEntity.ETag,
                        Timestamp = dynamicEntity.Timestamp
                    }
                }
            );
        }

        private Task<TableResult> _InsertOrReplaceEntity(ITableEntity entity, OperationContext operationContext)
        {
            if (entity.PartitionKey == null)
                return Task.FromException<TableResult>(new ArgumentNullException("Upserts require a valid PartitionKey"));
            if (entity.RowKey == null)
                return Task.FromException<TableResult>(new ArgumentNullException("Upserts require a valid RowKey"));

            var entityException = _ValidateEntity(entity);
            if (entityException != null)
                return Task.FromException<TableResult>(entityException);

            var dynamicEntity = _GetDynamicEntity(entity, operationContext);
            var dynamicEntityException = dynamicEntity
                .Properties
                .Select(property => _ValidateEntityProperty(property.Key, property.Value))
                .FirstOrDefault(exception => exception != null);
            if (dynamicEntityException != null)
                return Task.FromException<TableResult>(dynamicEntityException);

            var partition = _GetPartition(dynamicEntity);

            partition[entity.RowKey] = dynamicEntity;

            return Task.FromResult(
                new TableResult
                {
                    Etag = dynamicEntity.ETag,
                    HttpStatusCode = 204,
                    Result = new TableEntity
                    {
                        PartitionKey = dynamicEntity.PartitionKey,
                        RowKey = dynamicEntity.RowKey,
                        ETag = dynamicEntity.ETag,
                        Timestamp = default(DateTimeOffset)
                    }
                }
            );
        }

        private Task<TableResult> _InsertOrMergeEntity(ITableEntity entity, OperationContext operationContext)
        {
            if (entity.PartitionKey == null)
                return Task.FromException<TableResult>(new ArgumentNullException("Upserts require a valid PartitionKey"));
            if (entity.RowKey == null)
                return Task.FromException<TableResult>(new ArgumentNullException("Upserts require a valid RowKey"));

            var entityException = _ValidateEntity(entity);
            if (entityException != null)
                return Task.FromException<TableResult>(entityException);

            var dynamicEntity = _GetDynamicEntity(entity, operationContext);
            var dynamicEntityException = dynamicEntity
                .Properties
                .Select(property => _ValidateEntityProperty(property.Key, property.Value))
                .FirstOrDefault(exception => exception != null);
            if (dynamicEntityException != null)
                return Task.FromException<TableResult>(dynamicEntityException);

            var partition = _GetPartition(dynamicEntity);

            if (partition.TryGetValue(entity.RowKey, out var existingEntity))
                foreach (var property in existingEntity.Properties)
                    if (!dynamicEntity.Properties.ContainsKey(property.Key))
                        dynamicEntity.Properties.Add(property);
            partition[entity.RowKey] = dynamicEntity;

            return Task.FromResult(
                new TableResult
                {
                    Etag = dynamicEntity.ETag,
                    HttpStatusCode = 204,
                    Result = new TableEntity
                    {
                        PartitionKey = dynamicEntity.PartitionKey,
                        RowKey = dynamicEntity.RowKey,
                        ETag = dynamicEntity.ETag,
                        Timestamp = default(DateTimeOffset)
                    }
                }
            );
        }

        private static StorageException _ValidateEntity(ITableEntity entity)
        {
            if (entity.PartitionKey == null)
                return PropertiesWithoutValueException();
            if (entity.PartitionKey.Length > (1 << 10))
                return PropertyValueTooLarge();
            if (!entity.PartitionKey.All(_IsValidKeyCharacter))
                return InvalidPartitionKeyException(entity.PartitionKey);

            if (entity.RowKey == null)
                return PropertiesWithoutValueException();
            if (entity.RowKey.Length > (1 << 10))
                return PropertyValueTooLarge();
            if (!entity.RowKey.All(_IsValidKeyCharacter))
                return InvalidRowKeyException(entity.RowKey);

            return null;
        }

        private static StorageException _ValidateEntityProperty(string name, EntityProperty property)
        {
            switch (property.PropertyType)
            {
                case EdmType.String when property.StringValue.Length > (1 << 15):
                case EdmType.Binary when property.BinaryValue.Length > (1 << 16):
                    return PropertyValueTooLarge();

                case EdmType.DateTime when property.DateTime != null && property.DateTime < new DateTime(1601, 1, 1, 0, 0, 0, DateTimeKind.Utc):
                    return InvalidDateTimePropertyException(name, property.DateTime.Value);

                default:
                    return null;
            }
        }

        private static DynamicTableEntity _GetDynamicEntity(ITableEntity entity, OperationContext operationContext)
        {
            var timestamp = DateTimeOffset.UtcNow;
            var properties = TableEntity.Flatten(entity, operationContext);
            properties.Remove(nameof(TableEntity.PartitionKey));
            properties.Remove(nameof(TableEntity.RowKey));
            properties.Remove(nameof(TableEntity.Timestamp));
            properties.Remove(nameof(TableEntity.ETag));

            return new DynamicTableEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = $"{timestamp:o}-{Guid.NewGuid()}",
                Timestamp = timestamp,
                Properties = properties
            };
        }

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