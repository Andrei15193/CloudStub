using System;
using System.Collections.Generic;

namespace CloudStub.Core
{
    public class StubTableBulkOperation
    {
        private static readonly StubEntityJsonSerializer _entityJsonSerializer = new StubEntityJsonSerializer();
        private string _partitionKey;
        private readonly string _tableName;
        private readonly ITableStorageHandler _tableStorageHandler;
        private readonly ICollection<Func<List<StubEntity>, StubTableBulkOperationResult>> _operations = new List<Func<List<StubEntity>, StubTableBulkOperationResult>>();

        internal StubTableBulkOperation(string tableName, ITableStorageHandler tableStorageHandler)
            => (_tableName, _tableStorageHandler) = (tableName, tableStorageHandler);

        public StubTableBulkOperation Insert(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Bulk operations can be carried out only on the same partition.", nameof(entity));

            _operations.Add(partitionCluster =>
            {
                switch (StubTableOperation.Insert(entity, partitionCluster))
                {
                    case StubTableInsertResult.Success:
                        return StubTableBulkOperationResult.Success;

                    case StubTableInsertResult.TableDoesNotExist:
                        return StubTableBulkOperationResult.TableDoesNotExist;

                    case StubTableInsertResult.EntityAlreadyExists:
                        return StubTableBulkOperationResult.EntityAlreadyExist;

                    default:
                        return StubTableBulkOperationResult.Failed;
                }
            });
            return this;
        }

        public StubTableBulkOperation InsertOrMerge(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Bulk operations can be carried out only on the same partition.", nameof(entity));

            _operations.Add(partitionCluster =>
            {
                switch (StubTableOperation.InsertOrMerge(entity, partitionCluster))
                {
                    case StubTableInsertOrMergeResult.Success:
                        return StubTableBulkOperationResult.Success;

                    case StubTableInsertOrMergeResult.TableDoesNotExist:
                        return StubTableBulkOperationResult.TableDoesNotExist;

                    default:
                        return StubTableBulkOperationResult.Failed;
                }
            });
            return this;
        }

        public StubTableBulkOperation InsertOrReplace(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Bulk operations can be carried out only on the same partition.", nameof(entity));

            _operations.Add(partitionCluster =>
            {
                switch (StubTableOperation.InsertOrReplace(entity, partitionCluster))
                {
                    case StubTableInsertOrReplaceResult.Success:
                        return StubTableBulkOperationResult.Success;

                    case StubTableInsertOrReplaceResult.TableDoesNotExist:
                        return StubTableBulkOperationResult.TableDoesNotExist;

                    default:
                        return StubTableBulkOperationResult.Failed;
                }
            });
            return this;
        }

        public StubTableBulkOperation Merge(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Bulk operations can be carried out only on the same partition.", nameof(entity));

            _operations.Add(partitionCluster =>
            {
                switch (StubTableOperation.Merge(entity, partitionCluster))
                {
                    case StubTableMergeResult.Success:
                        return StubTableBulkOperationResult.Success;

                    case StubTableMergeResult.TableDoesNotExist:
                        return StubTableBulkOperationResult.TableDoesNotExist;

                    case StubTableMergeResult.EntityDoesNotExists:
                        return StubTableBulkOperationResult.EntityDoesNotExist;

                    case StubTableMergeResult.EtagsDoNotMatch:
                        return StubTableBulkOperationResult.EtagsDoNotMatch;

                    default:
                        return StubTableBulkOperationResult.Failed;
                }
            });
            return this;
        }

        public StubTableBulkOperation Replace(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Bulk operations can be carried out only on the same partition.", nameof(entity));

            _operations.Add(partitionCluster =>
            {
                switch (StubTableOperation.Replace(entity, partitionCluster))
                {
                    case StubTableReplaceResult.Success:
                        return StubTableBulkOperationResult.Success;

                    case StubTableReplaceResult.TableDoesNotExist:
                        return StubTableBulkOperationResult.TableDoesNotExist;

                    case StubTableReplaceResult.EntityDoesNotExists:
                        return StubTableBulkOperationResult.EntityDoesNotExist;

                    case StubTableReplaceResult.EtagsDoNotMatch:
                        return StubTableBulkOperationResult.EtagsDoNotMatch;

                    default:
                        return StubTableBulkOperationResult.Failed;
                }
            });
            return this;
        }

        public StubTableBulkOperation Delete(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Bulk operations can be carried out only on the same partition.", nameof(entity));

            _operations.Add(partitionCluster =>
            {
                switch (StubTableOperation.Delete(entity, partitionCluster))
                {
                    case StubTablDeleteResult.Success:
                        return StubTableBulkOperationResult.Success;

                    case StubTablDeleteResult.TableDoesNotExist:
                        return StubTableBulkOperationResult.TableDoesNotExist;

                    case StubTablDeleteResult.EntityDoesNotExists:
                        return StubTableBulkOperationResult.EntityDoesNotExist;

                    case StubTablDeleteResult.EtagsDoNotMatch:
                        return StubTableBulkOperationResult.EtagsDoNotMatch;

                    default:
                        return StubTableBulkOperationResult.Failed;
                }
            });
            return this;
        }

        public StubTableBulkOperationDataResult Execute()
        {
            var result = new StubTableBulkOperationDataResult(StubTableBulkOperationResult.Success);
            if (_partitionKey is object)
                try
                {
                    using (_tableStorageHandler.AquirePartitionClusterLock(_tableName, _partitionKey))
                    {
                        List<StubEntity> partitionCluster;
                        using (var reader = _tableStorageHandler.GetPartitionClusterTextReader(_tableName, _partitionKey))
                            partitionCluster = _entityJsonSerializer.Deserialize(reader);

                        var index = 0;
                        using (var operation = _operations.GetEnumerator())
                            while (result.BulkOperationResult == StubTableBulkOperationResult.Success && operation.MoveNext())
                            {
                                var operationResult = operation.Current(partitionCluster);
                                if (operationResult != StubTableBulkOperationResult.Success)
                                    result = new StubTableBulkOperationDataResult(operationResult, index);
                                index++;
                            }

                        if (result.BulkOperationResult == StubTableBulkOperationResult.Success)
                            using (var writer = _tableStorageHandler.GetPartitionClusterTextWriter(_tableName, _partitionKey))
                                _entityJsonSerializer.Serialize(writer, partitionCluster);
                    }
                }
                catch (KeyNotFoundException)
                {
                    result = new StubTableBulkOperationDataResult(StubTableBulkOperationResult.TableDoesNotExist);
                }

            return result;
        }
    }
}