using System;
using System.Collections.Generic;

namespace CloudStub.Core.Operations
{
    public class StubTableBatchOperation
    {
        private string _partitionKey;
        private readonly Func<string, IEnumerable<Func<List<StubEntity>, IStubTableOperationDataResult>>, StubTableBatchOperationDataResult> _executeCallback;
        private readonly ICollection<Func<List<StubEntity>, IStubTableOperationDataResult>> _operationCallbacks = new List<Func<List<StubEntity>, IStubTableOperationDataResult>>();

        internal StubTableBatchOperation(Func<string, IEnumerable<Func<List<StubEntity>, IStubTableOperationDataResult>>, StubTableBatchOperationDataResult> executeCallback)
            => _executeCallback = executeCallback;

        public StubTableBatchOperation Insert(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Batch operations can be performed only on the same partition.", nameof(entity));

            _operationCallbacks.Add(partitionCluster => StubTableOperation.Insert(entity, partitionCluster));
            return this;
        }

        public StubTableBatchOperation InsertOrMerge(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Batch operations can be performed only on the same partition.", nameof(entity));

            _operationCallbacks.Add(partitionCluster => StubTableOperation.InsertOrMerge(entity, partitionCluster));
            return this;
        }

        public StubTableBatchOperation InsertOrReplace(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Batch operations can be performed only on the same partition.", nameof(entity));

            _operationCallbacks.Add(partitionCluster => StubTableOperation.InsertOrReplace(entity, partitionCluster));
            return this;
        }

        public StubTableBatchOperation Merge(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Batch operations can be performed only on the same partition.", nameof(entity));

            _operationCallbacks.Add(partitionCluster => StubTableOperation.Merge(entity, partitionCluster));
            return this;
        }

        public StubTableBatchOperation Replace(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Batch operations can be performed only on the same partition.", nameof(entity));

            _operationCallbacks.Add(partitionCluster => StubTableOperation.Replace(entity, partitionCluster));
            return this;
        }

        public StubTableBatchOperation Delete(StubEntity entity)
        {
            if (_partitionKey is null)
                _partitionKey = entity.PartitionKey;
            else if (!string.Equals(_partitionKey, entity.PartitionKey, StringComparison.Ordinal))
                throw new ArgumentException("Batch operations can be performed only on the same partition.", nameof(entity));

            _operationCallbacks.Add(partitionCluster => StubTableOperation.Delete(entity, partitionCluster));
            return this;
        }

        public StubTableBatchOperationDataResult Execute()
            => _executeCallback(_partitionKey, _operationCallbacks);
    }
}