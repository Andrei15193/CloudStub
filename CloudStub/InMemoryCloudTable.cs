using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using CloudStub.FilterParser;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub
{
    public class InMemoryCloudTable : CloudTable
    {
        private delegate Task<TableResult> OperationHandler(TableOperation tableOperation, OperationContext operationContext);

        private readonly object _locker;
        private bool _tableExists;
        private readonly IReadOnlyDictionary<TableOperationType, OperationHandler> _operationHandlers;
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
            _operationHandlers = new Dictionary<TableOperationType, OperationHandler>
            {
                { TableOperationType.Insert, _InsertEntity },
                { TableOperationType.InsertOrReplace, _InsertOrReplaceEntity },
                { TableOperationType.InsertOrMerge, _InsertOrMergeEntity },
                { TableOperationType.Replace, _ReplaceEntity },
                { TableOperationType.Merge, _MergeEntity },
                { TableOperationType.Delete, _DeleteEntity },
                { TableOperationType.Retrieve, _RetrieveEntity }
            };
            _entitiesByPartitionKey = new SortedList<string, IDictionary<string, DynamicTableEntity>>(StringComparer.Ordinal);
        }

        public override Task CreateAsync()
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
                    return operationHandler(operation, operationContext);

            return Task.FromException<TableResult>(new NotImplementedException());
        }

        public override Task<TableQuerySegment> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token)
            => ExecuteQuerySegmentedAsync(query, token, null, null);

        public override Task<TableQuerySegment> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteQuerySegmentedAsync(query, token, null, null, CancellationToken.None);

        public override Task<TableQuerySegment> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            lock (_locker)
            {
                var entities = _QueryEntities(query.FilterString, query.SelectColumns);
                return Task.FromResult(_CreateTableQuerySegment(entities));
            }
        }

        public override Task<TableQuerySegment<T>> ExecuteQuerySegmentedAsync<T>(TableQuery<T> query, TableContinuationToken token)
            => ExecuteQuerySegmentedAsync(query, token, null, null);

        public override Task<TableQuerySegment<T>> ExecuteQuerySegmentedAsync<T>(TableQuery<T> query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteQuerySegmentedAsync(query, token, null, null, CancellationToken.None);

        public override Task<TableQuerySegment<T>> ExecuteQuerySegmentedAsync<T>(TableQuery<T> query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            var entityResolver = _GetEntityResolver(TableOperation.Retrieve<T>(string.Empty, string.Empty));
            T GetConcreteEntity(DynamicTableEntity existingEntity) =>
                (T)entityResolver(
                    existingEntity.PartitionKey,
                    existingEntity.RowKey,
                    existingEntity.Timestamp,
                    existingEntity.Properties,
                    existingEntity.ETag
                );

            lock (_locker)
            {
                var entities = _QueryEntities(query.FilterString, query.SelectColumns);
                return Task.FromResult(_CreateTableQuerySegment(entities.Select(GetConcreteEntity)));

            }
        }

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

        private IEnumerable<DynamicTableEntity> _QueryEntities(string filterString, IEnumerable<string> selectColumns)
        {
            var allEntities = _entitiesByPartitionKey.Values.SelectMany(partitionedEntities => partitionedEntities.Values);
            var filteredEntities = _ApplyFilter(allEntities, filterString);

            return filteredEntities.Select(_Clone);
        }

        private IEnumerable<DynamicTableEntity> _ApplyFilter(IEnumerable<DynamicTableEntity> entities, string filterString)
        {
            var scanner = new FilterTokenScanner();
            var tokens = scanner.Scan(filterString ?? string.Empty);
            var parser = new FilterTokenParser();
            var predicate = parser.Parse(tokens);
            var result = entities.Where(predicate).ToList();
            return result;
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

        private Task<TableResult> _InsertEntity(TableOperation tableOperation, OperationContext operationContext)
        {
            if (!_tableExists)
                return Task.FromException<TableResult>(TableDoesNotExistException());

            var entity = tableOperation.Entity;
            var entityException = _ValidateEntityForInsert(entity);
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
                return Task.FromException<TableResult>(EntityAlreadyExistsException());

            partition.Add(dynamicEntity.RowKey, dynamicEntity);

            return Task.FromResult(
                new TableResult
                {
                    HttpStatusCode = 204,
                    Etag = dynamicEntity.ETag,
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

        private Task<TableResult> _InsertOrReplaceEntity(TableOperation tableOperation, OperationContext operationContext)
        {
            if (!_tableExists)
                return Task.FromException<TableResult>(TableDoesNotExistException());

            var entity = tableOperation.Entity;
            var entityException = _ValidateEntityForUpsert(entity);
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
                    HttpStatusCode = 204,
                    Etag = dynamicEntity.ETag,
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

        private Task<TableResult> _InsertOrMergeEntity(TableOperation tableOperation, OperationContext operationContext)
        {
            if (!_tableExists)
                return Task.FromException<TableResult>(TableDoesNotExistException());

            var entity = tableOperation.Entity;
            var entityException = _ValidateEntityForUpsert(entity);
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
                    HttpStatusCode = 204,
                    Etag = dynamicEntity.ETag,
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

        private Task<TableResult> _ReplaceEntity(TableOperation tableOperation, OperationContext operationContext)
        {
            if (!_tableExists)
                return Task.FromException<TableResult>(TableDoesNotExistException());

            var entity = tableOperation.Entity;
            var entityException = _ValidateEntityForReplace(entity);
            if (entityException != null)
                return Task.FromException<TableResult>(entityException);

            if (!_entitiesByPartitionKey.TryGetValue(entity.PartitionKey, out var partition)
                || !partition.TryGetValue(entity.RowKey, out var existingEntity))
                return Task.FromException<TableResult>(ResourceNotFoundException());

            var dynamicEntity = _GetDynamicEntity(entity, operationContext);
            var dynamicEntityException = dynamicEntity
                .Properties
                .Select(property => _ValidateEntityProperty(property.Key, property.Value))
                .FirstOrDefault(exception => exception != null);
            if (dynamicEntityException != null)
                return Task.FromException<TableResult>(dynamicEntityException);

            if (entity.ETag != "*" && !StringComparer.OrdinalIgnoreCase.Equals(entity.ETag, existingEntity.ETag))
                return Task.FromException<TableResult>(PreconditionFailedException());

            partition = _GetPartition(dynamicEntity);
            partition[entity.RowKey] = dynamicEntity;

            return Task.FromResult(
                new TableResult
                {
                    HttpStatusCode = 204,
                    Etag = dynamicEntity.ETag,
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

        private Task<TableResult> _MergeEntity(TableOperation tableOperation, OperationContext operationContext)
        {
            if (!_tableExists)
                return Task.FromException<TableResult>(TableDoesNotExistException());

            var entity = tableOperation.Entity;
            var entityException = _ValidateEntityForMerge(entity);
            if (entityException != null)
                return Task.FromException<TableResult>(entityException);

            if (!_entitiesByPartitionKey.TryGetValue(entity.PartitionKey, out var partition)
                || !partition.TryGetValue(entity.RowKey, out var existingEntity))
                return Task.FromException<TableResult>(ResourceNotFoundException());

            var dynamicEntity = _GetDynamicEntity(entity, operationContext);
            var dynamicEntityException = dynamicEntity
                .Properties
                .Select(property => _ValidateEntityProperty(property.Key, property.Value))
                .FirstOrDefault(exception => exception != null);
            if (dynamicEntityException != null)
                return Task.FromException<TableResult>(dynamicEntityException);

            if (entity.ETag != "*" && !StringComparer.OrdinalIgnoreCase.Equals(entity.ETag, existingEntity.ETag))
                return Task.FromException<TableResult>(PreconditionFailedException());

            partition = _GetPartition(dynamicEntity);
            foreach (var property in existingEntity.Properties)
                if (!dynamicEntity.Properties.ContainsKey(property.Key))
                    dynamicEntity.Properties.Add(property);
            partition[entity.RowKey] = dynamicEntity;

            return Task.FromResult(
                new TableResult
                {
                    HttpStatusCode = 204,
                    Etag = dynamicEntity.ETag,
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

        private Task<TableResult> _DeleteEntity(TableOperation tableOperation, OperationContext operationContext)
        {
            if (!_tableExists)
                return Task.FromException<TableResult>(TableDoesNotExistException());

            var entity = tableOperation.Entity;
            var entityException = _ValidateEntityForDelete(entity);
            if (entityException != null)
                return Task.FromException<TableResult>(entityException);

            if (!_entitiesByPartitionKey.TryGetValue(entity.PartitionKey, out var partition)
                || !partition.TryGetValue(entity.RowKey, out var existingEntity))
                return Task.FromException<TableResult>(ResourceNotFoundException());

            if (entity.ETag != "*" && !StringComparer.OrdinalIgnoreCase.Equals(entity.ETag, existingEntity.ETag))
                return Task.FromException<TableResult>(PreconditionFailedException());

            partition.Remove(entity.RowKey);

            return Task.FromResult(
                new TableResult
                {
                    HttpStatusCode = 204,
                    Etag = null,
                    Result = new TableEntity
                    {
                        PartitionKey = existingEntity.PartitionKey,
                        RowKey = existingEntity.RowKey,
                        ETag = existingEntity.ETag,
                        Timestamp = default(DateTimeOffset)
                    }
                }
            );
        }

        private Task<TableResult> _RetrieveEntity(TableOperation tableOperation, OperationContext operationContext)
        {
            var entity = _GetEntityForRetrieve(tableOperation);
            var entityException = _ValidateEntityForRetrieve(entity);
            if (entityException != null)
                return Task.FromException<TableResult>(entityException);

            if (_entitiesByPartitionKey.TryGetValue(entity.PartitionKey, out var partition)
                && partition.TryGetValue(entity.RowKey, out var existingEntity))
                return Task.FromResult(
                    new TableResult
                    {
                        HttpStatusCode = 200,
                        Etag = existingEntity.ETag,
                        Result = _GetEntityRetrieveResult(existingEntity, tableOperation)
                    }
                );

            return Task.FromResult(
                new TableResult
                {
                    HttpStatusCode = 404,
                    Etag = null,
                    Result = null
                }
            );
        }

        private static object _GetEntityRetrieveResult(DynamicTableEntity existingEntity, TableOperation tableOperation)
        {
            var selectColumns = _GetSelectColumns(tableOperation);
            var entityProperties = selectColumns == null ?
                existingEntity.Properties :
                existingEntity.Properties.Where(property => selectColumns.Contains(property.Key, StringComparer.Ordinal));

            var entityResolver = _GetEntityResolver(tableOperation);
            var entityResult = entityResolver(
                existingEntity.PartitionKey,
                existingEntity.RowKey,
                existingEntity.Timestamp,
                entityProperties.ToDictionary(entityProperty => entityProperty.Key, entityProperty => entityProperty.Value, StringComparer.Ordinal),
                existingEntity.ETag
            );
            return entityResult;
        }

        private static Exception _ValidateEntityForInsert(ITableEntity entity)
        {
            if (entity.PartitionKey == null)
                return PropertiesWithoutValueException();
            if (entity.PartitionKey.Length > (1 << 10))
                return PropertyValueTooLargeException();
            if (!entity.PartitionKey.All(_IsValidKeyCharacter))
                return InvalidPartitionKeyException(entity.PartitionKey);

            if (entity.RowKey == null)
                return PropertiesWithoutValueException();
            if (entity.RowKey.Length > (1 << 10))
                return PropertyValueTooLargeException();
            if (!entity.RowKey.All(_IsValidKeyCharacter))
                return InvalidRowKeyException(entity.RowKey);

            return null;
        }

        private static Exception _ValidateEntityForUpsert(ITableEntity entity)
        {
            if (entity.PartitionKey == null)
                return new ArgumentNullException("Upserts require a valid PartitionKey");
            if (entity.PartitionKey.Length > (1 << 10))
                return PropertyValueTooLargeException();
            if (!entity.PartitionKey.All(_IsValidKeyCharacter))
                return InvalidPartitionKeyException(entity.PartitionKey);

            if (entity.RowKey == null)
                return new ArgumentNullException("Upserts require a valid RowKey");
            if (entity.RowKey.Length > (1 << 10))
                return PropertyValueTooLargeException();
            if (!entity.RowKey.All(_IsValidKeyCharacter))
                return InvalidRowKeyException(entity.RowKey);

            return null;
        }

        private static Exception _ValidateEntityForReplace(ITableEntity entity)
        {
            if (entity.PartitionKey == null)
                return new ArgumentNullException("Replace requires a valid PartitionKey");
            if (!entity.PartitionKey.All(_IsValidKeyCharacter))
                return InvalidPartitionKeyException(entity.PartitionKey);

            if (entity.RowKey == null)
                return new ArgumentNullException("Replace requires a valid RowKey");
            if (!entity.RowKey.All(_IsValidKeyCharacter))
                return InvalidRowKeyException(entity.RowKey);

            return null;
        }

        private static Exception _ValidateEntityForMerge(ITableEntity entity)
        {
            if (entity.PartitionKey == null)
                return new ArgumentNullException("Merge requires a valid PartitionKey");
            if (!entity.PartitionKey.All(_IsValidKeyCharacter))
                return InvalidPartitionKeyException(entity.PartitionKey);

            if (entity.RowKey == null)
                return new ArgumentNullException("Merge requires a valid RowKey");
            if (!entity.RowKey.All(_IsValidKeyCharacter))
                return InvalidRowKeyException(entity.RowKey);

            return null;
        }

        private static Exception _ValidateEntityForDelete(ITableEntity entity)
        {
            if (entity.PartitionKey == null)
                return new ArgumentNullException("Delete requires a valid PartitionKey");
            if (!entity.PartitionKey.All(_IsValidKeyCharacter))
                return InvalidPartitionKeyException(entity.PartitionKey);

            if (entity.RowKey == null)
                return new ArgumentNullException("Delete requires a valid RowKey");
            if (!entity.RowKey.All(_IsValidKeyCharacter))
                return InvalidRowKeyException(entity.RowKey);

            return null;
        }

        private static Exception _ValidateEntityForRetrieve(ITableEntity entity)
        {
            if (entity.PartitionKey == null)
                return new ArgumentNullException("partitionKey");

            if (entity.RowKey == null)
                return new ArgumentNullException("rowkey");

            return null;
        }

        private static StorageException _ValidateEntityProperty(string name, EntityProperty property)
        {
            switch (property.PropertyType)
            {
                case EdmType.String when property.StringValue.Length > (1 << 15):
                case EdmType.Binary when property.BinaryValue.Length > (1 << 16):
                    return PropertyValueTooLargeException();

                case EdmType.DateTime when property.DateTime != null && property.DateTime < new DateTime(1601, 1, 1, 0, 0, 0, DateTimeKind.Utc):
                    return InvalidDateTimePropertyException(name, property.DateTime.Value);

                default:
                    return null;
            }
        }

        private static DynamicTableEntity _GetDynamicEntity(ITableEntity entity, OperationContext operationContext)
        {
            var timestamp = DateTimeOffset.UtcNow;
            var properties = entity is DynamicTableEntity dynamicTableEntity
                ? new Dictionary<string, EntityProperty>(dynamicTableEntity.Properties, StringComparer.Ordinal)
                : TableEntity.Flatten(entity, operationContext);
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

        private static ITableEntity _GetEntityForRetrieve(TableOperation tableOperation)
        {
            var partitionKeyPropertyInfo = typeof(TableOperation)
                .GetTypeInfo()
                .GetProperty("PartitionKey", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.GetProperty);
            var rowKeyPropertyInfo = typeof(TableOperation)
                .GetTypeInfo()
                .GetProperty("RowKey", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.GetProperty);
            return new TableEntity
            {
                PartitionKey = (string)partitionKeyPropertyInfo.GetValue(tableOperation),
                RowKey = (string)rowKeyPropertyInfo.GetValue(tableOperation)
            };
        }

        private static IEnumerable<string> _GetSelectColumns(TableOperation tableOperation)
        {
            var selectColumnsPropertyInfo = typeof(TableOperation)
                .GetTypeInfo()
                .GetProperty("SelectColumns", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.GetProperty);
            return (IEnumerable<string>)selectColumnsPropertyInfo.GetValue(tableOperation);
        }

        private static Func<string, string, DateTimeOffset, IDictionary<string, EntityProperty>, string, object> _GetEntityResolver(TableOperation tableOperation)
        {
            var retrieveResolverPropertyInfo = typeof(TableOperation)
                .GetTypeInfo()
                .GetProperty("RetrieveResolver", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.GetProperty);
            return (Func<string, string, DateTimeOffset, IDictionary<string, EntityProperty>, string, object>)retrieveResolverPropertyInfo.GetValue(tableOperation);
        }

        private static TableQuerySegment _CreateTableQuerySegment(IEnumerable<DynamicTableEntity> entities)
            => (TableQuerySegment)typeof(TableQuerySegment)
                .GetTypeInfo()
                .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.CreateInstance)
                .Single(constructor => constructor.GetParameters().Select(parameter => parameter.ParameterType).SequenceEqual(new[] { typeof(List<DynamicTableEntity>) }))
                .Invoke(new[] { entities.ToList() });

        private static TableQuerySegment<TElement> _CreateTableQuerySegment<TElement>(IEnumerable<TElement> entities)
            => (TableQuerySegment<TElement>)typeof(TableQuerySegment<TElement>)
                .GetTypeInfo()
                .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.CreateInstance)
                .Single(constructor => constructor.GetParameters().Select(parameter => parameter.ParameterType).SequenceEqual(new[] { typeof(List<TElement>) }))
                .Invoke(new[] { entities.ToList() });

        private static DynamicTableEntity _Clone(DynamicTableEntity entity)
            => new DynamicTableEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp,
                Properties = new Dictionary<string, EntityProperty>(entity.Properties, StringComparer.Ordinal)
            };
    }
}