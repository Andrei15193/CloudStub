using System;
using System.Collections.Generic;

namespace CloudStub.Core
{
    public class StubTableBatchOperation
    {
        private string _partitionKey;
        private readonly Func<string, IEnumerable<Func<List<StubEntity>, StubTableBatchOperationResult>>, StubTableBatchOperationDataResult> _executeCallback;
        private readonly ICollection<Func<List<StubEntity>, StubTableBatchOperationResult>> _operations = new List<Func<List<StubEntity>, StubTableBatchOperationResult>>();

        internal StubTableBatchOperation(Func<string, IEnumerable<Func<List<StubEntity>, StubTableBatchOperationResult>>, StubTableBatchOperationDataResult> executeCallback)
            => _executeCallback = executeCallback;

        public StubTableBatchOperation Insert(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Batch operations can be performed only on the same partition.", nameof(entity));

            _operations.Add(partitionCluster =>
            {
                switch (StubTableOperation.Insert(entity, partitionCluster).OperationResult)
                {
                    case StubTableInsertOperationResult.Success:
                        return StubTableBatchOperationResult.Success;

                    case StubTableInsertOperationResult.TableDoesNotExist:
                        return StubTableBatchOperationResult.TableDoesNotExist;

                    case StubTableInsertOperationResult.EntityAlreadyExists:
                        return StubTableBatchOperationResult.EntityAlreadyExist;

                    default:
                        return StubTableBatchOperationResult.Failed;
                }
            });
            return this;
        }

        public StubTableBatchOperation InsertOrMerge(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Batch operations can be performed only on the same partition.", nameof(entity));

            _operations.Add(partitionCluster =>
            {
                switch (StubTableOperation.InsertOrMerge(entity, partitionCluster).OperationResult)
                {
                    case StubTableInsertOrMergeOperationResult.Success:
                        return StubTableBatchOperationResult.Success;

                    case StubTableInsertOrMergeOperationResult.TableDoesNotExist:
                        return StubTableBatchOperationResult.TableDoesNotExist;

                    default:
                        return StubTableBatchOperationResult.Failed;
                }
            });
            return this;
        }

        public StubTableBatchOperation InsertOrReplace(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Batch operations can be performed only on the same partition.", nameof(entity));

            _operations.Add(partitionCluster =>
            {
                switch (StubTableOperation.InsertOrReplace(entity, partitionCluster).OperationResult)
                {
                    case StubTableInsertOrReplaceOperationResult.Success:
                        return StubTableBatchOperationResult.Success;

                    case StubTableInsertOrReplaceOperationResult.TableDoesNotExist:
                        return StubTableBatchOperationResult.TableDoesNotExist;

                    default:
                        return StubTableBatchOperationResult.Failed;
                }
            });
            return this;
        }

        public StubTableBatchOperation Merge(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Batch operations can be performed only on the same partition.", nameof(entity));

            _operations.Add(partitionCluster =>
            {
                switch (StubTableOperation.Merge(entity, partitionCluster))
                {
                    case StubTableMergeOperationResult.Success:
                        return StubTableBatchOperationResult.Success;

                    case StubTableMergeOperationResult.TableDoesNotExist:
                        return StubTableBatchOperationResult.TableDoesNotExist;

                    case StubTableMergeOperationResult.EntityDoesNotExists:
                        return StubTableBatchOperationResult.EntityDoesNotExist;

                    case StubTableMergeOperationResult.EtagsDoNotMatch:
                        return StubTableBatchOperationResult.EtagsDoNotMatch;

                    default:
                        return StubTableBatchOperationResult.Failed;
                }
            });
            return this;
        }

        public StubTableBatchOperation Replace(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Batch operations can be performed only on the same partition.", nameof(entity));

            _operations.Add(partitionCluster =>
            {
                switch (StubTableOperation.Replace(entity, partitionCluster))
                {
                    case StubTableReplaceOperationResult.Success:
                        return StubTableBatchOperationResult.Success;

                    case StubTableReplaceOperationResult.TableDoesNotExist:
                        return StubTableBatchOperationResult.TableDoesNotExist;

                    case StubTableReplaceOperationResult.EntityDoesNotExists:
                        return StubTableBatchOperationResult.EntityDoesNotExist;

                    case StubTableReplaceOperationResult.EtagsDoNotMatch:
                        return StubTableBatchOperationResult.EtagsDoNotMatch;

                    default:
                        return StubTableBatchOperationResult.Failed;
                }
            });
            return this;
        }

        public StubTableBatchOperation Delete(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Batch operations can be performed only on the same partition.", nameof(entity));

            _operations.Add(partitionCluster =>
            {
                switch (StubTableOperation.Delete(entity, partitionCluster))
                {
                    case StubTableDeleteOperationResult.Success:
                        return StubTableBatchOperationResult.Success;

                    case StubTableDeleteOperationResult.TableDoesNotExist:
                        return StubTableBatchOperationResult.TableDoesNotExist;

                    case StubTableDeleteOperationResult.EntityDoesNotExists:
                        return StubTableBatchOperationResult.EntityDoesNotExist;

                    case StubTableDeleteOperationResult.EtagsDoNotMatch:
                        return StubTableBatchOperationResult.EtagsDoNotMatch;

                    default:
                        return StubTableBatchOperationResult.Failed;
                }
            });
            return this;
        }

        public StubTableBatchOperationDataResult Execute()
            => _executeCallback(_partitionKey, _operations);
    }
}