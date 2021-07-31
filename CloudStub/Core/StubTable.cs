using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace CloudStub.Core
{
    public class StubTable
    {
        private static StubEntityJsonSerializer _entityJsonSerializer = new StubEntityJsonSerializer();
        private readonly object _tableLock = new object();
        private readonly TableStorageHandler _tableStorageHandler;

        public StubTable(string name, TableStorageHandler tableStorageHandler)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            _tableStorageHandler = tableStorageHandler ?? throw new ArgumentNullException(nameof(tableStorageHandler));
            Exists = _tableStorageHandler.Exists(Name);
        }

        public string Name { get; }

        public bool Exists { get; private set; }

        public StubTableCreateResult Create()
        {
            StubTableCreateResult result;

            lock (_tableLock)
                if (Exists)
                    result = StubTableCreateResult.TableAlreadyExists;
                else
                {
                    Exists = true;
                    result = StubTableCreateResult.Success;
                }

            return result;
        }

        public StubTableDeleteResult Delete()
        {
            StubTableDeleteResult result;

            lock (_tableLock)
                if (!Exists)
                    result = StubTableDeleteResult.TableDoesNotExist;
                else
                {
                    _tableStorageHandler.Delete(Name);
                    Exists = false;
                    result = StubTableDeleteResult.Success;
                }

            return result;
        }

        public StubTableInsertResult Insert(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTableInsertResult result;

            lock (_tableLock)
            {
                if (!Exists)
                    result = StubTableInsertResult.TableDoesNotExist;
                else
                    using (var reader = _tableStorageHandler.GetTextReader(Name, entity.PartitionKey))
                    {
                        var entities = _entityJsonSerializer.Deserialize(reader);
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

            return result;
        }

        public StubTableQueryDataResult Query(StubTableQuery query, StubTableQueryContinuationToken continuationToken)
        {
            StubTableQueryDataResult result;

            lock (_tableLock)
                if (!Exists)
                    result = new StubTableQueryDataResult();
                else
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

        private List<StubEntity> _ReadEntities()
        {
            var partitionsResults = new List<IEnumerable<StubEntity>>();
            foreach (var reader in _tableStorageHandler.GetPartitionTextReaders(Name))
                using (reader)
                {
                    var entities = _entityJsonSerializer.Deserialize(reader);
                    if (entities.Count > 0)
                        partitionsResults.Add(entities);
                }

            return partitionsResults
                .OrderBy(partitionResults => partitionResults.First().PartitionKey, StringComparer.Ordinal)
                .ThenBy(partitionResults => partitionResults.First().RowKey, StringComparer.Ordinal)
                .SelectMany(Enumerable.AsEnumerable)
                .ToList();
        }

        private void _WriteEntities(string partitionKey, IEnumerable<StubEntity> entities)
        {
            using (var writer = _tableStorageHandler.GetTextWriter(Name, partitionKey))
                _entityJsonSerializer.Serialize(writer, entities);
        }
    }
}