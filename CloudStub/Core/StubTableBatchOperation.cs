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
                    case StubTableInsertResult.Success:
                        return StubTableBatchOperationResult.Success;

                    case StubTableInsertResult.TableDoesNotExist:
                        return StubTableBatchOperationResult.TableDoesNotExist;

                    case StubTableInsertResult.EntityAlreadyExists:
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
                    case StubTableInsertOrMergeResult.Success:
                        return StubTableBatchOperationResult.Success;

                    case StubTableInsertOrMergeResult.TableDoesNotExist:
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
                switch (StubTableOperation.InsertOrReplace(entity, partitionCluster))
                {
                    case StubTableInsertOrReplaceResult.Success:
                        return StubTableBatchOperationResult.Success;

                    case StubTableInsertOrReplaceResult.TableDoesNotExist:
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
                    case StubTableMergeResult.Success:
                        return StubTableBatchOperationResult.Success;

                    case StubTableMergeResult.TableDoesNotExist:
                        return StubTableBatchOperationResult.TableDoesNotExist;

                    case StubTableMergeResult.EntityDoesNotExists:
                        return StubTableBatchOperationResult.EntityDoesNotExist;

                    case StubTableMergeResult.EtagsDoNotMatch:
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
                    case StubTableReplaceResult.Success:
                        return StubTableBatchOperationResult.Success;

                    case StubTableReplaceResult.TableDoesNotExist:
                        return StubTableBatchOperationResult.TableDoesNotExist;

                    case StubTableReplaceResult.EntityDoesNotExists:
                        return StubTableBatchOperationResult.EntityDoesNotExist;

                    case StubTableReplaceResult.EtagsDoNotMatch:
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
                    case StubTableDeleteResult.Success:
                        return StubTableBatchOperationResult.Success;

                    case StubTableDeleteResult.TableDoesNotExist:
                        return StubTableBatchOperationResult.TableDoesNotExist;

                    case StubTableDeleteResult.EntityDoesNotExists:
                        return StubTableBatchOperationResult.EntityDoesNotExist;

                    case StubTableDeleteResult.EtagsDoNotMatch:
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