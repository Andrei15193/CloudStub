using System;
using System.Collections.Generic;
using System.Linq;

namespace CloudStub.Core
{
    public class StubTable
    {
        private static readonly StubEntityJsonSerializer _entityJsonSerializer = new StubEntityJsonSerializer();
        private readonly ITableStorageHandler _tableStorageHandler;

        public StubTable(string name, ITableStorageHandler tableStorageHandler)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            _tableStorageHandler = tableStorageHandler ?? throw new ArgumentNullException(nameof(tableStorageHandler));
        }

        public string Name { get; }

        public bool Exists
            => _tableStorageHandler.Exists(Name);

        public StubTableCreateResult Create()
            => _tableStorageHandler.Create(Name)
            ? StubTableCreateResult.Success
            : StubTableCreateResult.TableAlreadyExists;

        public StubTableDeleteResult Delete()
            => _tableStorageHandler.Delete(Name)
            ? StubTableDeleteResult.Success
            : StubTableDeleteResult.TableDoesNotExist;

        public StubTableInsertResult Insert(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTableInsertResult result;
            try
            {
                using (_tableStorageHandler.AquirePartitionClusterLock(Name, entity.PartitionKey))
                {
                    var partitionCluster = _ReadPartitionCluster(entity.PartitionKey);

                    result = StubTableOperation.Insert(entity, partitionCluster);

                    if (result == StubTableInsertResult.Success)
                        _WritePartitionCluster(entity.PartitionKey, partitionCluster);
                }
            }
            catch (KeyNotFoundException)
            {
                result = StubTableInsertResult.TableDoesNotExist;
            }

            return result;
        }

        public StubTableInsertOrMergeResult InsertOrMerge(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTableInsertOrMergeResult result;
            try
            {
                using (_tableStorageHandler.AquirePartitionClusterLock(Name, entity.PartitionKey))
                {
                    var partitionCluster = _ReadPartitionCluster(entity.PartitionKey);

                    result = StubTableOperation.InsertOrMerge(entity, partitionCluster);

                    if (result == StubTableInsertOrMergeResult.Success)
                        _WritePartitionCluster(entity.PartitionKey, partitionCluster);
                }
            }
            catch (KeyNotFoundException)
            {
                result = StubTableInsertOrMergeResult.TableDoesNotExist;
            }

            return result;
        }

        public StubTableInsertOrReplaceResult InsertOrReplace(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTableInsertOrReplaceResult result;

            try
            {
                using (_tableStorageHandler.AquirePartitionClusterLock(Name, entity.PartitionKey))
                {
                    var partitionCluster = _ReadPartitionCluster(entity.PartitionKey);

                    result = StubTableOperation.InsertOrReplace(entity, partitionCluster);

                    if (result == StubTableInsertOrReplaceResult.Success)
                        _WritePartitionCluster(entity.PartitionKey, partitionCluster);
                }
            }
            catch (KeyNotFoundException)
            {
                result = StubTableInsertOrReplaceResult.TableDoesNotExist;
            }

            return result;
        }

        public StubTableMergeResult Merge(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTableMergeResult result;

            try
            {
                using (_tableStorageHandler.AquirePartitionClusterLock(Name, entity.PartitionKey))
                {
                    var partitionCluster = _ReadPartitionCluster(entity.PartitionKey);

                    result = StubTableOperation.Merge(entity, partitionCluster);

                    if (result == StubTableMergeResult.Success)
                        _WritePartitionCluster(entity.PartitionKey, partitionCluster);
                }
            }
            catch (KeyNotFoundException)
            {
                result = StubTableMergeResult.TableDoesNotExist;
            }

            return result;
        }

        public StubTableReplaceResult Replace(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTableReplaceResult result;

            try
            {
                using (_tableStorageHandler.AquirePartitionClusterLock(Name, entity.PartitionKey))
                {
                    var partitionCluster = _ReadPartitionCluster(entity.PartitionKey);

                    result = StubTableOperation.Replace(entity, partitionCluster);

                    if (result == StubTableReplaceResult.Success)
                        _WritePartitionCluster(entity.PartitionKey, partitionCluster);
                }
            }
            catch (KeyNotFoundException)
            {
                result = StubTableReplaceResult.TableDoesNotExist;
            }

            return result;
        }

        public StubTablDeleteResult Delete(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTablDeleteResult result;

            try
            {
                using (_tableStorageHandler.AquirePartitionClusterLock(Name, entity.PartitionKey))
                {
                    var partitionCluster = _ReadPartitionCluster(entity.PartitionKey);

                    result = StubTableOperation.Delete(entity, partitionCluster);

                    if (result == StubTablDeleteResult.Success)
                        _WritePartitionCluster(entity.PartitionKey, partitionCluster);
                }
            }
            catch (KeyNotFoundException)
            {
                result = StubTablDeleteResult.TableDoesNotExist;
            }

            return result;
        }

        public StubTableRetrieveDataResult Retrieve(string partitionKey, string rowKey)
            => Retrieve(partitionKey, rowKey, default);

        public StubTableRetrieveDataResult Retrieve(string partitionKey, string rowKey, IEnumerable<string> selectedProperties)
        {
            if (partitionKey is null)
                throw new ArgumentNullException(nameof(partitionKey));
            if (rowKey is null)
                throw new ArgumentNullException(nameof(rowKey));

            StubTableRetrieveDataResult result;

            try
            {
                using (_tableStorageHandler.AquirePartitionClusterLock(Name, partitionKey))
                {
                    var partitionCluster = _ReadPartitionCluster(partitionKey);

                    result = StubTableOperation.Retrieve(partitionKey, rowKey, selectedProperties, partitionCluster);
                }
            }
            catch (KeyNotFoundException)
            {
                result = new StubTableRetrieveDataResult();
            }

            return result;
        }

        public StubTableQueryDataResult Query(StubTableQuery query, StubTableQueryContinuationToken continuationToken)
        {
            StubTableQueryDataResult result;

            try
            {
                using (_tableStorageHandler.AquireTableLock(Name))
                {
                    var entites = _ReadEntities().Where(entity => entity.Timestamp is object || entity.ETag is object);

                    var remainingEntities = continuationToken is object
                        ? entites.SkipWhile(entity => string.CompareOrdinal(entity.PartitionKey, continuationToken.LastPartitionKey) <= 0 && string.CompareOrdinal(entity.RowKey, continuationToken.LastRowKey) <= 0)
                        : entites;
                    var filteredEntities = query?.Filter is object ? remainingEntities.Where(query.Filter) : remainingEntities;
                    var pageSize = Math.Min(query?.PageSize ?? 1000, 1000);
                    var pagedEntitiesPlusOne = filteredEntities.Take(pageSize + 1);

                    var resultEntities = new List<StubEntity>(pageSize + 1);
                    resultEntities.AddRange(pagedEntitiesPlusOne);
                    var resultContinuationToken = default(StubTableQueryContinuationToken);
                    var hasContinuation = resultEntities.Count > pageSize;
                    if (hasContinuation)
                    {
                        resultEntities.RemoveAt(resultEntities.Count - 1);
                        resultContinuationToken = new StubTableQueryContinuationToken(resultEntities[resultEntities.Count - 1]);
                    }

                    if (query is object && query.SelectedProperties is object)
                        for (var index = 0; index < resultEntities.Count; index++)
                            resultEntities[index] = _SelectPropertiesFromEntity(resultEntities[index], query.SelectedProperties);

                    result = new StubTableQueryDataResult(resultEntities, resultContinuationToken);
                }
            }
            catch (KeyNotFoundException)
            {
                result = new StubTableQueryDataResult();
            }

            return result;
        }

        public StubTableBulkOperation BulkOperation()
            => new StubTableBulkOperation(Name, _tableStorageHandler);

        private StubEntity _SelectPropertiesFromEntity(StubEntity entity, IReadOnlyCollection<string> selectedPropertyNames)
        {
            var projectEntity = new StubEntity(entity.PartitionKey, entity.RowKey, entity.Timestamp.Value, entity.ETag);
            if (selectedPropertyNames is null)
                foreach (var property in entity.Properties)
                    projectEntity.Properties.Add(property);
            else
                foreach (var propertyName in selectedPropertyNames)
                    if (entity.Properties.TryGetValue(propertyName, out var propertyValue))
                        projectEntity.Properties.Add(propertyName, propertyValue);

            return projectEntity;
        }

        private IEnumerable<StubEntity> _ReadEntities()
        {
            var partitionsResults = new List<IEnumerable<StubEntity>>();
            foreach (var readerProvider in _tableStorageHandler.GetPartitionClustersTextReaderProviders(Name))
                using (var reader = readerProvider())
                {
                    var entities = _entityJsonSerializer.Deserialize(reader);
                    if (entities.Count > 0)
                        partitionsResults.Add(entities);
                }

            return partitionsResults
                .OrderBy(partitionResults => partitionResults.First().PartitionKey, StringComparer.Ordinal)
                .ThenBy(partitionResults => partitionResults.First().RowKey, StringComparer.Ordinal)
                .SelectMany(Enumerable.AsEnumerable);
        }

        private List<StubEntity> _ReadPartitionCluster(string partitionKey)
        {
            using (var reader = _tableStorageHandler.GetPartitionClusterTextReader(Name, partitionKey))
                return _entityJsonSerializer.Deserialize(reader);
        }

        private void _WritePartitionCluster(string partitionKey, IEnumerable<StubEntity> entities)
        {
            using (var writer = _tableStorageHandler.GetPartitionClusterTextWriter(Name, partitionKey))
                _entityJsonSerializer.Serialize(writer, entities);
        }
    }
}