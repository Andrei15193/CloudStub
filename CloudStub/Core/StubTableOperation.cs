using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace CloudStub.Core
{
    internal static class StubTableOperation
    {
        internal static StubTableInsertResult Insert(StubEntity entity, List<StubEntity> partitionCluster)
        {
            var entityIndex = _FindEntityIndex(entity.PartitionKey, entity.RowKey, partitionCluster, out var found);

            if (found)
                return StubTableInsertResult.EntityAlreadyExists;
            else
            {
                var now = DateTime.UtcNow;
                partitionCluster.Insert(entityIndex, new StubEntity(entity) { Timestamp = now, ETag = $"etag/{now:o}".ToString(CultureInfo.InvariantCulture) });
                return StubTableInsertResult.Success;
            }
        }

        internal static StubTableInsertOrMergeResult InsertOrMerge(StubEntity entity, List<StubEntity> partitionCluster)
        {
            var entityIndex = _FindEntityIndex(entity.PartitionKey, entity.RowKey, partitionCluster, out var found);

            var now = DateTime.UtcNow;
            if (found)
            {
                var updatedEntity = new StubEntity(partitionCluster[entityIndex])
                {
                    Timestamp = now,
                    ETag = $"etag/{now:o}".ToString(CultureInfo.InvariantCulture),
                    Properties = _Merge(partitionCluster[entityIndex].Properties, entity.Properties)
                };

                partitionCluster[entityIndex] = updatedEntity;
            }
            else
                partitionCluster.Insert(entityIndex, new StubEntity(entity) { Timestamp = now, ETag = $"etag/{now:o}".ToString(CultureInfo.InvariantCulture) });

            return StubTableInsertOrMergeResult.Success;
        }

        internal static StubTableInsertOrReplaceResult InsertOrReplace(StubEntity entity, List<StubEntity> partitionCluster)
        {
            var entityIndex = _FindEntityIndex(entity.PartitionKey, entity.RowKey, partitionCluster, out var found);

            var now = DateTime.UtcNow;
            if (found)
                partitionCluster[entityIndex] = new StubEntity(entity)
                {
                    Timestamp = now,
                    ETag = $"etag/{now:o}".ToString(CultureInfo.InvariantCulture)
                };
            else
                partitionCluster.Insert(entityIndex, new StubEntity(entity)
                {
                    Timestamp = now,
                    ETag = $"etag/{now:o}".ToString(CultureInfo.InvariantCulture)
                });

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
                var updatedEntity = new StubEntity(partitionCluster[entityIndex])
                {
                    Timestamp = now,
                    ETag = $"etag/{now:o}".ToString(CultureInfo.InvariantCulture),
                    Properties = _Merge(partitionCluster[entityIndex].Properties, entity.Properties)
                };

                partitionCluster[entityIndex] = updatedEntity;

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
                partitionCluster[entityIndex] = new StubEntity(entity)
                {
                    Timestamp = now,
                    ETag = $"etag/{now:o}".ToString(CultureInfo.InvariantCulture)
                };

                return StubTableReplaceResult.Success;
            }
            else
                return StubTableReplaceResult.EtagsDoNotMatch;
        }

        internal static StubTablDeleteResult Delete(StubEntity entity, List<StubEntity> partitionCluster)
        {
            var entityIndex = _FindEntityIndex(entity.PartitionKey, entity.RowKey, partitionCluster, out var found);

            if (!found)
                return StubTablDeleteResult.EntityDoesNotExists;
            else if (entity.ETag == "*" || entity.ETag == partitionCluster[entityIndex].ETag)
            {
                partitionCluster.RemoveAt(entityIndex);

                return StubTablDeleteResult.Success;
            }
            else
                return StubTablDeleteResult.EtagsDoNotMatch;
        }

        internal static StubTableRetrieveDataResult Retrieve(string partitionKey, string rowKey, IEnumerable<string> selectedProperties, IReadOnlyList<StubEntity> partitionCluster)
        {
            var entityIndex = _FindEntityIndex(partitionKey, rowKey, partitionCluster, out var found);

            if (!found)
                return new StubTableRetrieveDataResult(null);
            else
            {
                var entity = partitionCluster[entityIndex];
                var resultEntity = new StubEntity
                {
                    PartitionKey = entity.PartitionKey,
                    RowKey = entity.RowKey,
                    ETag = entity.ETag,
                    Timestamp = entity.Timestamp
                };
                if (selectedProperties is null)
                    resultEntity.Properties = entity.Properties;
                else if (selectedProperties.Any())
                    resultEntity.Properties = entity
                        .Properties
                        .Where(property => selectedProperties.Contains(property.Key))
                        .ToDictionary(
                            property => property.Key,
                            property => property.Value,
                            StringComparer.Ordinal
                        );

                return new StubTableRetrieveDataResult(resultEntity);
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
            foreach (var pair in left.Concat(right))
                result[pair.Key] = pair.Value;
            return result;
        }
    }
}