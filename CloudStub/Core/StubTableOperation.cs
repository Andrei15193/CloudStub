using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace CloudStub.Core
{
    internal static class StubTableOperation
    {
        internal static StubTableInsertDataResult Insert(StubEntity entity, List<StubEntity> partitionCluster)
        {
            var entityIndex = _FindEntityIndex(entity.PartitionKey, entity.RowKey, partitionCluster, out var found);

            if (found)
                return new StubTableInsertDataResult(StubTableInsertResult.EntityAlreadyExists);
            else
            {
                var now = DateTime.UtcNow;
                var insertEntity = new StubEntity(entity.PartitionKey, entity.RowKey, now, $"etag/{now:o}");
                foreach (var property in entity.Properties)
                    insertEntity.Properties.Add(property);

                partitionCluster.Insert(entityIndex, insertEntity);
                return new StubTableInsertDataResult(StubTableInsertResult.Success, insertEntity);
            }
        }

        internal static StubTableInsertOrMergeDataResult InsertOrMerge(StubEntity entity, List<StubEntity> partitionCluster)
        {
            var entityIndex = _FindEntityIndex(entity.PartitionKey, entity.RowKey, partitionCluster, out var found);

            var now = DateTime.UtcNow;
            StubEntity insertOrMergeEntity;
            if (found)
            {
                insertOrMergeEntity = new StubEntity(partitionCluster[entityIndex].PartitionKey, partitionCluster[entityIndex].RowKey, now, $"etag/{now:o}".ToString(CultureInfo.InvariantCulture));
                foreach (var property in partitionCluster[entityIndex].Properties.Concat(entity.Properties))
                    insertOrMergeEntity.Properties[property.Key] = property.Value;

                partitionCluster[entityIndex] = insertOrMergeEntity;
            }
            else
            {
                insertOrMergeEntity = new StubEntity(entity.PartitionKey, entity.RowKey, now, $"etag/{now:o}".ToString(CultureInfo.InvariantCulture));
                foreach (var property in entity.Properties)
                    insertOrMergeEntity.Properties.Add(property);

                partitionCluster.Insert(entityIndex, insertOrMergeEntity);
            }

            return new StubTableInsertOrMergeDataResult(StubTableInsertOrMergeResult.Success, insertOrMergeEntity);
        }

        internal static StubTableInsertOrReplaceResult InsertOrReplace(StubEntity entity, List<StubEntity> partitionCluster)
        {
            var entityIndex = _FindEntityIndex(entity.PartitionKey, entity.RowKey, partitionCluster, out var found);

            var now = DateTime.UtcNow;
            var insertOrReplaceEntity = new StubEntity(entity.PartitionKey, entity.RowKey, now, $"etag/{now:o}".ToString(CultureInfo.InvariantCulture));
            foreach (var property in entity.Properties)
                insertOrReplaceEntity.Properties.Add(property);

            if (found)
                partitionCluster[entityIndex] = insertOrReplaceEntity;
            else
                partitionCluster.Insert(entityIndex, insertOrReplaceEntity);

            return StubTableInsertOrReplaceResult.Success;
        }

        internal static StubTableMergeResult Merge(StubEntity entity, List<StubEntity> partitionCluster)
        {
            var entityIndex = _FindEntityIndex(entity.PartitionKey, entity.RowKey, partitionCluster, out var found);

            if (!found)
                return StubTableMergeResult.EntityDoesNotExists;
            else if (entity.ETag == "*" || entity.ETag == partitionCluster[entityIndex].ETag)
            {
                var now = DateTime.UtcNow;
                var mergeEntity = new StubEntity(partitionCluster[entityIndex].PartitionKey, partitionCluster[entityIndex].RowKey, now, $"etag/{now:o}".ToString(CultureInfo.InvariantCulture));
                foreach (var property in partitionCluster[entityIndex].Properties.Concat(entity.Properties))
                    mergeEntity.Properties[property.Key] = property.Value;

                partitionCluster[entityIndex] = mergeEntity;

                return StubTableMergeResult.Success;
            }
            else
                return StubTableMergeResult.EtagsDoNotMatch;
        }

        internal static StubTableReplaceResult Replace(StubEntity entity, List<StubEntity> partitionCluster)
        {
            var entityIndex = _FindEntityIndex(entity.PartitionKey, entity.RowKey, partitionCluster, out var found);

            if (!found)
                return StubTableReplaceResult.EntityDoesNotExists;
            else if (entity.ETag == "*" || entity.ETag == partitionCluster[entityIndex].ETag)
            {
                var now = DateTime.UtcNow;
                var replaceEntity = new StubEntity(entity.PartitionKey, entity.RowKey, now, $"etag/{now:o}".ToString(CultureInfo.InvariantCulture));
                foreach (var property in entity.Properties)
                    replaceEntity.Properties.Add(property);

                partitionCluster[entityIndex] = replaceEntity;

                return StubTableReplaceResult.Success;
            }
            else
                return StubTableReplaceResult.EtagsDoNotMatch;
        }

        internal static StubTableDeleteResult Delete(StubEntity entity, List<StubEntity> partitionCluster)
        {
            var entityIndex = _FindEntityIndex(entity.PartitionKey, entity.RowKey, partitionCluster, out var found);

            if (!found)
                return StubTableDeleteResult.EntityDoesNotExists;
            else if (entity.ETag == "*" || entity.ETag == partitionCluster[entityIndex].ETag)
            {
                partitionCluster.RemoveAt(entityIndex);

                return StubTableDeleteResult.Success;
            }
            else
                return StubTableDeleteResult.EtagsDoNotMatch;
        }

        internal static StubTableRetrieveDataResult Retrieve(string partitionKey, string rowKey, IEnumerable<string> selectedProperties, IReadOnlyList<StubEntity> partitionCluster)
        {
            var entityIndex = _FindEntityIndex(partitionKey, rowKey, partitionCluster, out var found);

            if (!found || partitionCluster[entityIndex].Timestamp is null || partitionCluster[entityIndex].ETag is null)
                return new StubTableRetrieveDataResult(null);
            else
            {
                var entity = partitionCluster[entityIndex];

                var retrieveEntity = new StubEntity(entity.PartitionKey, entity.RowKey, entity.Timestamp.Value, entity.ETag);
                if (selectedProperties is null)
                    foreach (var property in entity.Properties)
                        retrieveEntity.Properties.Add(property);
                else if (selectedProperties.Any())
                    foreach (var propertyName in selectedProperties)
                        if (entity.Properties.TryGetValue(propertyName, out var propertyValue))
                            retrieveEntity.Properties.Add(propertyName, propertyValue);

                return new StubTableRetrieveDataResult(retrieveEntity);
            }
        }

        private static int _FindEntityIndex(string partitionKey, string rowKey, IReadOnlyList<StubEntity> entities, out bool found)
        {
            var startIndex = 0;
            var endIndex = entities.Count;
            var insertIndex = 0;
            found = false;
            while (!found && startIndex < endIndex)
            {
                insertIndex = (startIndex + endIndex) / 2;
                var partitionKeyComparison = string.CompareOrdinal(partitionKey, entities[insertIndex].PartitionKey);
                if (partitionKeyComparison == 0)
                {
                    var rowKeyCompartison = string.CompareOrdinal(rowKey, entities[insertIndex].RowKey);
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
                var partitionKeyComparison = string.CompareOrdinal(partitionKey, entities[insertIndex].PartitionKey);
                if (partitionKeyComparison > 0)
                    insertIndex++;
                else if (partitionKeyComparison == 0)
                {
                    var rowKeyCompartison = string.CompareOrdinal(rowKey, entities[insertIndex].RowKey);
                    if (rowKeyCompartison > 0)
                        insertIndex++;
                }
            }

            return insertIndex;
        }

        private static IReadOnlyDictionary<string, StubEntityProperty> _Merge(IReadOnlyDictionary<string, StubEntityProperty> left, IReadOnlyDictionary<string, StubEntityProperty> right)
        {
            var result = new Dictionary<string, StubEntityProperty>(StringComparer.OrdinalIgnoreCase);
            foreach (var property in left.Concat(right))
                result[property.Key] = property.Value;
            return result;
        }
    }
}