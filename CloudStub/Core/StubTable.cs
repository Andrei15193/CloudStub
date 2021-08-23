using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using CloudStub.Core.OperationResults;
using CloudStub.Core.StorageHandlers;

namespace CloudStub.Core
{
    public class StubTable
    {
        private static readonly StubEntityJsonSerializer _entityJsonSerializer = new StubEntityJsonSerializer();
        private readonly ITableStorageHandler _tableStorageHandler;

        private readonly ReaderWriterLockSlim _tableReaderWriterLock = new ReaderWriterLockSlim();
        private readonly IDictionary<string, ReaderWriterLockSlim> _partitionClustersReaderWriterLocks = new Dictionary<string, ReaderWriterLockSlim>(StringComparer.Ordinal);

        public StubTable(string name, ITableStorageHandler tableStorageHandler)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("The table name cannot be null, empty or white space.", nameof(name));

            Name = name;
            _tableStorageHandler = tableStorageHandler ?? throw new ArgumentNullException(nameof(tableStorageHandler));
        }

        public string Name { get; }

        public StubTableCreateResult Create()
        {
            _tableReaderWriterLock.EnterWriteLock();
            try
            {
                return _tableStorageHandler.Create(Name) ? StubTableCreateResult.Success : StubTableCreateResult.TableAlreadyExists;
            }
            finally
            {
                _tableReaderWriterLock.ExitWriteLock();
            }
        }

        public bool Exists()
        {
            _tableReaderWriterLock.EnterReadLock();
            try
            {
                return _tableStorageHandler.Exists(Name);
            }
            finally
            {
                _tableReaderWriterLock.ExitReadLock();
            }
        }

        public StubTableDeleteResult Delete()
        {
            _tableReaderWriterLock.EnterWriteLock();
            try
            {
                return _tableStorageHandler.Delete(Name) ? StubTableDeleteResult.Success : StubTableDeleteResult.TableDoesNotExist;
            }
            finally
            {
                _tableReaderWriterLock.ExitWriteLock();
            }
        }

        public StubTableInsertOperationDataResult Insert(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTableInsertOperationDataResult result;
            _tableReaderWriterLock.EnterReadLock();
            try
            {
                var partitionClusterStorageHandler = _tableStorageHandler.GetPartitionClusterStorageHandler(Name, entity.PartitionKey);
                var partitionClusterReaderWriterLock = _GetPartitionClusterReaderWriterLock(partitionClusterStorageHandler);

                partitionClusterReaderWriterLock.EnterWriteLock();
                try
                {
                    var partitionCluster = _ReadPartitionCluster(partitionClusterStorageHandler);

                    result = StubTableOperation.Insert(entity, partitionCluster);

                    if (result.OperationResult == StubTableInsertOperationResult.Success)
                        _WritePartitionCluster(partitionClusterStorageHandler, partitionCluster);
                }
                finally
                {
                    partitionClusterReaderWriterLock.ExitWriteLock();
                }
            }
            catch (KeyNotFoundException)
            {
                result = new StubTableInsertOperationDataResult(StubTableInsertOperationResult.TableDoesNotExist);
            }
            finally
            {
                _tableReaderWriterLock.ExitReadLock();
            }

            return result;
        }

        public StubTableInsertOrMergeOperationDataResult InsertOrMerge(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTableInsertOrMergeOperationDataResult result;
            _tableReaderWriterLock.EnterReadLock();
            try
            {
                var partitionClusterStorageHandler = _tableStorageHandler.GetPartitionClusterStorageHandler(Name, entity.PartitionKey);
                var partitionClusterReaderWriterLock = _GetPartitionClusterReaderWriterLock(partitionClusterStorageHandler);

                partitionClusterReaderWriterLock.EnterWriteLock();
                try
                {
                    var partitionCluster = _ReadPartitionCluster(partitionClusterStorageHandler);

                    result = StubTableOperation.InsertOrMerge(entity, partitionCluster);

                    if (result.OperationResult == StubTableInsertOrMergeOperationResult.Success)
                        _WritePartitionCluster(partitionClusterStorageHandler, partitionCluster);
                }
                finally
                {
                    partitionClusterReaderWriterLock.ExitWriteLock();
                }
            }
            catch (KeyNotFoundException)
            {
                result = new StubTableInsertOrMergeOperationDataResult(StubTableInsertOrMergeOperationResult.TableDoesNotExist);
            }
            finally
            {
                _tableReaderWriterLock.ExitReadLock();
            }

            return result;
        }

        public StubTableInsertOrReplaceOperationDataResult InsertOrReplace(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTableInsertOrReplaceOperationDataResult result;
            _tableReaderWriterLock.EnterReadLock();
            try
            {
                var partitionClusterStorageHandler = _tableStorageHandler.GetPartitionClusterStorageHandler(Name, entity.PartitionKey);
                var partitionClusterReaderWriterLock = _GetPartitionClusterReaderWriterLock(partitionClusterStorageHandler);

                partitionClusterReaderWriterLock.EnterWriteLock();
                try
                {
                    var partitionCluster = _ReadPartitionCluster(partitionClusterStorageHandler);

                    result = StubTableOperation.InsertOrReplace(entity, partitionCluster);

                    if (result.OperationResult == StubTableInsertOrReplaceOperationResult.Success)
                        _WritePartitionCluster(partitionClusterStorageHandler, partitionCluster);
                }
                finally
                {
                    partitionClusterReaderWriterLock.ExitWriteLock();
                }
            }
            catch (KeyNotFoundException)
            {
                result = new StubTableInsertOrReplaceOperationDataResult(StubTableInsertOrReplaceOperationResult.TableDoesNotExist);
            }
            finally
            {
                _tableReaderWriterLock.ExitReadLock();
            }

            return result;
        }

        public StubTableMergeOperationDataResult Merge(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTableMergeOperationDataResult result;
            _tableReaderWriterLock.EnterReadLock();
            try
            {
                var partitionClusterStorageHandler = _tableStorageHandler.GetPartitionClusterStorageHandler(Name, entity.PartitionKey);
                var partitionClusterReaderWriterLock = _GetPartitionClusterReaderWriterLock(partitionClusterStorageHandler);

                partitionClusterReaderWriterLock.EnterWriteLock();
                try
                {
                    var partitionCluster = _ReadPartitionCluster(partitionClusterStorageHandler);

                    result = StubTableOperation.Merge(entity, partitionCluster);

                    if (result.OperationResult == StubTableMergeOperationResult.Success)
                        _WritePartitionCluster(partitionClusterStorageHandler, partitionCluster);
                }
                finally
                {
                    partitionClusterReaderWriterLock.ExitWriteLock();
                }
            }
            catch (KeyNotFoundException)
            {
                result = new StubTableMergeOperationDataResult(StubTableMergeOperationResult.TableDoesNotExist);
            }
            finally
            {
                _tableReaderWriterLock.ExitReadLock();
            }

            return result;
        }

        public StubTableReplaceOperationDataResult Replace(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTableReplaceOperationDataResult result;
            _tableReaderWriterLock.EnterReadLock();
            try
            {
                var partitionClusterStorageHandler = _tableStorageHandler.GetPartitionClusterStorageHandler(Name, entity.PartitionKey);
                var partitionClusterReaderWriterLock = _GetPartitionClusterReaderWriterLock(partitionClusterStorageHandler);

                partitionClusterReaderWriterLock.EnterWriteLock();
                try
                {
                    var partitionCluster = _ReadPartitionCluster(partitionClusterStorageHandler);

                    result = StubTableOperation.Replace(entity, partitionCluster);

                    if (result.OperationResult == StubTableReplaceOperationResult.Success)
                        _WritePartitionCluster(partitionClusterStorageHandler, partitionCluster);
                }
                finally
                {
                    partitionClusterReaderWriterLock.ExitWriteLock();
                }
            }
            catch (KeyNotFoundException)
            {
                result = new StubTableReplaceOperationDataResult(StubTableReplaceOperationResult.TableDoesNotExist);
            }
            finally
            {
                _tableReaderWriterLock.ExitReadLock();
            }

            return result;
        }

        public StubTableDeleteOperationDataResult Delete(StubEntity entity)
        {
            if (entity is null)
                throw new ArgumentNullException(nameof(entity));

            StubTableDeleteOperationDataResult result;
            _tableReaderWriterLock.EnterReadLock();
            try
            {
                var partitionClusterStorageHandler = _tableStorageHandler.GetPartitionClusterStorageHandler(Name, entity.PartitionKey);
                var partitionClusterReaderWriterLock = _GetPartitionClusterReaderWriterLock(partitionClusterStorageHandler);

                partitionClusterReaderWriterLock.EnterWriteLock();
                try
                {
                    var partitionCluster = _ReadPartitionCluster(partitionClusterStorageHandler);

                    result = StubTableOperation.Delete(entity, partitionCluster);

                    if (result.OperationResult == StubTableDeleteOperationResult.Success)
                        _WritePartitionCluster(partitionClusterStorageHandler, partitionCluster);
                }
                finally
                {
                    partitionClusterReaderWriterLock.ExitWriteLock();
                }
            }
            catch (KeyNotFoundException)
            {
                result = new StubTableDeleteOperationDataResult(StubTableDeleteOperationResult.TableDoesNotExist);
            }
            finally
            {
                _tableReaderWriterLock.ExitReadLock();
            }

            return result;
        }

        public StubTableRetrieveOperationDataResult Retrieve(string partitionKey, string rowKey)
            => Retrieve(partitionKey, rowKey, default);

        public StubTableRetrieveOperationDataResult Retrieve(string partitionKey, string rowKey, IEnumerable<string> selectedProperties)
        {
            if (string.IsNullOrWhiteSpace(partitionKey))
                throw new ArgumentException("The partition key cannot be null, empty or white space.", nameof(partitionKey));
            if (string.IsNullOrWhiteSpace(rowKey))
                throw new ArgumentException("The row key cannot be null, empty or white space.", nameof(rowKey));

            StubTableRetrieveOperationDataResult result;
            _tableReaderWriterLock.EnterReadLock();
            try
            {
                var partitionClusterStorageHandler = _tableStorageHandler.GetPartitionClusterStorageHandler(Name, partitionKey);
                var partitionClusterReaderWriterLock = _GetPartitionClusterReaderWriterLock(partitionClusterStorageHandler);

                partitionClusterReaderWriterLock.EnterReadLock();
                try
                {
                    var partitionCluster = _ReadPartitionCluster(partitionClusterStorageHandler);

                    result = StubTableOperation.Retrieve(partitionKey, rowKey, selectedProperties, partitionCluster);
                }
                finally
                {
                    partitionClusterReaderWriterLock.ExitReadLock();
                }
            }
            catch (KeyNotFoundException)
            {
                result = new StubTableRetrieveOperationDataResult();
            }
            finally
            {
                _tableReaderWriterLock.ExitReadLock();
            }

            return result;
        }

        public StubTableQueryDataResult Query(StubTableQuery query, StubTableQueryContinuationToken continuationToken)
        {
            StubTableQueryDataResult result;

            _tableReaderWriterLock.EnterReadLock();
            try
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
            catch (KeyNotFoundException)
            {
                result = new StubTableQueryDataResult();
            }
            finally
            {
                _tableReaderWriterLock.ExitReadLock();
            }

            return result;
        }

        public StubTableBatchOperation BatchOperation()
            => new StubTableBatchOperation((partitionKey, applyOperationCallbacks) =>
            {
                var result = default(StubTableBatchOperationDataResult);
                var operationResults = new List<IStubTableOperationDataResult>();

                if (partitionKey is object && applyOperationCallbacks.Any())
                {
                    _tableReaderWriterLock.EnterReadLock();
                    try
                    {
                        var partitionClusterStorageHandler = _tableStorageHandler.GetPartitionClusterStorageHandler(Name, partitionKey);
                        var partitionClusterReaderWriterLock = _GetPartitionClusterReaderWriterLock(partitionClusterStorageHandler);

                        partitionClusterReaderWriterLock.EnterWriteLock();
                        try
                        {
                            var partitionCluster = _ReadPartitionCluster(partitionClusterStorageHandler);

                            using (var applyOperationCallback = applyOperationCallbacks.GetEnumerator())
                                while (result is null && applyOperationCallback.MoveNext())
                                {
                                    var operationResult = applyOperationCallback.Current(partitionCluster);
                                    operationResults.Add(operationResult);
                                    if (!operationResult.IsSuccessful)
                                        result = new StubTableBatchOperationDataResult(StubTableBatchOperationResult.Failed, operationResults);
                                }

                            if (result is null)
                            {
                                result = new StubTableBatchOperationDataResult(StubTableBatchOperationResult.Success, operationResults);
                                _WritePartitionCluster(partitionClusterStorageHandler, partitionCluster);
                            }
                        }
                        finally
                        {
                            partitionClusterReaderWriterLock.ExitWriteLock();
                        }
                    }
                    catch (KeyNotFoundException)
                    {
                        result = new StubTableBatchOperationDataResult(StubTableBatchOperationResult.TableDoesNotExist);
                    }
                    finally
                    {
                        _tableReaderWriterLock.ExitReadLock();
                    }
                }
                else
                    result = new StubTableBatchOperationDataResult(Exists() ? StubTableBatchOperationResult.Success : StubTableBatchOperationResult.TableDoesNotExist);

                return result;
            }
            );

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
            foreach (var partitionClusterStorageHandler in _tableStorageHandler.GetPartitionClusterStorageHandlers(Name))
            {
                var partitionClusterReaderWriterLock = _GetPartitionClusterReaderWriterLock(partitionClusterStorageHandler);

                partitionClusterReaderWriterLock.EnterReadLock();
                try
                {
                    using (var reader = partitionClusterStorageHandler.OpenRead())
                    {
                        var entities = _entityJsonSerializer.Deserialize(reader);
                        if (entities.Count > 0)
                            partitionsResults.Add(entities);
                    }
                }
                finally
                {
                    partitionClusterReaderWriterLock.ExitReadLock();
                }
            }

            return partitionsResults
                .OrderBy(partitionResults => partitionResults.First().PartitionKey, StringComparer.Ordinal)
                .ThenBy(partitionResults => partitionResults.First().RowKey, StringComparer.Ordinal)
                .SelectMany(Enumerable.AsEnumerable)
                .Where(entity => entity.Timestamp is object || entity.ETag is object);
        }

        private List<StubEntity> _ReadPartitionCluster(IPartitionClusterStorageHandler partitionClusterStorageHandler)
        {
            using (var reader = partitionClusterStorageHandler.OpenRead())
                return _entityJsonSerializer.Deserialize(reader);
        }

        private void _WritePartitionCluster(IPartitionClusterStorageHandler partitionClusterStorageHandler, IEnumerable<StubEntity> entities)
        {
            using (var writer = partitionClusterStorageHandler.OpenWrite())
                _entityJsonSerializer.Serialize(writer, entities);
        }

        private ReaderWriterLockSlim _GetPartitionClusterReaderWriterLock(IPartitionClusterStorageHandler partitionClusterStorageHandler)
        {
            if (!_partitionClustersReaderWriterLocks.TryGetValue(partitionClusterStorageHandler.Key, out var partitionClusterReaderWriterLock))
            {
                partitionClusterReaderWriterLock = new ReaderWriterLockSlim();
                _partitionClustersReaderWriterLocks.Add(partitionClusterStorageHandler.Key, partitionClusterReaderWriterLock);
            }

            return partitionClusterReaderWriterLock;
        }
    }
}