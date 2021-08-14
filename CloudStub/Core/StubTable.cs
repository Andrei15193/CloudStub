using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace CloudStub.Core
{
    public class StubTable
    {
        private static StubEntityJsonSerializer _entityJsonSerializer = new StubEntityJsonSerializer();
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
                    IReadOnlyList<StubEntity> entities;
                    using (var reader = _tableStorageHandler.GetPartitionClusterTextReader(Name, entity.PartitionKey))
                        entities = _entityJsonSerializer.Deserialize(reader);

                    var insertIndex = _FindInsertIndex(entity, entities, out var found);

                    if (found)
                        result = StubTableInsertResult.EntityAlreadyExists;
                    else
                    {
                        var now = DateTime.UtcNow;
                        var updatedEntities = entities
                            .Take(insertIndex)
                            .Concat(Enumerable.Repeat(new StubEntity(entity) { Timestamp = now, ETag = $"etag/{now:o}".ToString(CultureInfo.InvariantCulture) }, 1))
                            .Concat(entities.Skip(insertIndex));
                        _WriteEntities(entity.PartitionKey, updatedEntities);

                        result = StubTableInsertResult.Success;
                    }
                }
            }
            catch (KeyNotFoundException)
            {
                result = StubTableInsertResult.TableDoesNotExist;
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
                    IReadOnlyList<StubEntity> entities;
                    using (var reader = _tableStorageHandler.GetPartitionClusterTextReader(Name, entity.PartitionKey))
                        entities = _entityJsonSerializer.Deserialize(reader);

                    var entityIndex = _FindInsertIndex(entity, entities, out var found);

                    if (!found)
                        result = StubTableMergeResult.EntityDoesNotExists;
                    else if (entity.ETag == "*" || entity.ETag == entities[entityIndex].ETag)
                    {
                        var now = DateTime.UtcNow;
                        var updatedEntity = new StubEntity(entities[entityIndex])
                        {
                            Timestamp = now,
                            ETag = $"etag/{now:o}".ToString(CultureInfo.InvariantCulture)
                        };
                        updatedEntity.Properties = _Merge(updatedEntity.Properties, entity.Properties);

                        var updatedEntities = entities
                            .Take(entityIndex)
                            .Concat(Enumerable.Repeat(updatedEntity, 1))
                            .Concat(entities.Skip(entityIndex + 1));
                        _WriteEntities(entity.PartitionKey, updatedEntities);

                        result = StubTableMergeResult.Success;
                    }
                    else
                        result = StubTableMergeResult.EtagsDoNotMatch;
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
                    IReadOnlyList<StubEntity> entities;
                    using (var reader = _tableStorageHandler.GetPartitionClusterTextReader(Name, entity.PartitionKey))
                        entities = _entityJsonSerializer.Deserialize(reader);

                    var entityIndex = _FindInsertIndex(entity, entities, out var found);

                    if (!found)
                        result = StubTableReplaceResult.EntityDoesNotExists;
                    else if (entity.ETag == "*" || entity.ETag == entities[entityIndex].ETag)
                    {
                        var now = DateTime.UtcNow;
                        var updatedEntities = entities
                            .Take(entityIndex)
                            .Concat(Enumerable.Repeat(new StubEntity(entity) { Timestamp = now, ETag = $"etag/{now:o}".ToString(CultureInfo.InvariantCulture) }, 1))
                            .Concat(entities.Skip(entityIndex + 1));
                        _WriteEntities(entity.PartitionKey, updatedEntities);

                        result = StubTableReplaceResult.Success;
                    }
                    else
                        result = StubTableReplaceResult.EtagsDoNotMatch;
                }
            }
            catch (KeyNotFoundException)
            {
                result = StubTableReplaceResult.TableDoesNotExist;
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
                    var entites = _ReadEntities();

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

        private StubEntity _SelectPropertiesFromEntity(StubEntity entity, IReadOnlyCollection<string> selectedPropertyNames)
        {
            if (selectedPropertyNames is null)
                return entity;
            else
            {
                var selectedProperties = new Dictionary<string, StubEntityProperty>(selectedPropertyNames.Count, StringComparer.Ordinal);
                foreach (var propertyName in selectedPropertyNames)
                    if (entity.Properties.TryGetValue(propertyName, out var propertyValue))
                        selectedProperties.Add(propertyName, propertyValue);
                return new StubEntity(entity)
                {
                    Properties = selectedProperties
                };
            }
        }

        private static IReadOnlyDictionary<string, StubEntityProperty> _Merge(IReadOnlyDictionary<string, StubEntityProperty> left, IReadOnlyDictionary<string, StubEntityProperty> right)
        {
            var result = new Dictionary<string, StubEntityProperty>(StringComparer.OrdinalIgnoreCase);
            foreach (var pair in left.Concat(right))
                result[pair.Key] = pair.Value;
            return result;
        }

        private static int _FindInsertIndex(StubEntity entity, IReadOnlyList<StubEntity> entities, out bool found)
        {
            var startIndex = 0;
            var endIndex = entities.Count;
            var insertIndex = 0;
            found = false;
            while (!found && startIndex < endIndex)
            {
                insertIndex = (startIndex + endIndex) / 2;
                var partitionKeyComparison = string.CompareOrdinal(entity.PartitionKey, entities[insertIndex].PartitionKey);
                if (partitionKeyComparison == 0)
                {
                    var rowKeyCompartison = string.CompareOrdinal(entity.RowKey, entities[insertIndex].RowKey);
                    if (rowKeyCompartison == 0)
                        found = true;
                    else if (rowKeyCompartison < 0)
                        endIndex = insertIndex;
                    else
                        startIndex = insertIndex + 1;
                }
                else if (partitionKeyComparison < 0)
                    endIndex = insertIndex;
                else
                    startIndex = insertIndex + 1;
            }
            if (!found && insertIndex < entities.Count)
            {
                var partitionKeyComparison = string.CompareOrdinal(entity.PartitionKey, entities[insertIndex].PartitionKey);
                if (partitionKeyComparison > 0)
                    insertIndex++;
                else if (partitionKeyComparison == 0)
                {
                    var rowKeyCompartison = string.CompareOrdinal(entity.RowKey, entities[insertIndex].RowKey);
                    if (rowKeyCompartison > 0)
                        insertIndex++;
                }
            }

            return insertIndex;
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

        private void _WriteEntities(string partitionKey, IEnumerable<StubEntity> entities)
        {
            using (var writer = _tableStorageHandler.GetPartitionClusterTextWriter(Name, partitionKey))
                _entityJsonSerializer.Serialize(writer, entities);
        }
    }
}