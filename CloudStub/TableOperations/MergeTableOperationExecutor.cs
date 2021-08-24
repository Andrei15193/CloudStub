using System;
using CloudStub.Core;
using CloudStub.Core.OperationResults;
using Microsoft.Azure.Cosmos.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal sealed class MergeTableOperationExecutor : TableOperationExecutor
    {
        public MergeTableOperationExecutor(StubTable stubTable)
            : base(stubTable)
        {
        }

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            if (tableOperation.Entity.PartitionKey == null)
                throw new ArgumentNullException("Merge requires a valid PartitionKey");
            var partitionKeyException = ValidateKeyProperty(tableOperation.Entity.PartitionKey);
            if (partitionKeyException != null)
                throw partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                throw new ArgumentNullException("Merge requires a valid RowKey");
            var rowKeyException = ValidateKeyProperty(tableOperation.Entity.RowKey);
            if (rowKeyException != null)
                throw rowKeyException;

            var entityPropertyException = ValidateEntityProperties(tableOperation.Entity);
            if (entityPropertyException != null)
                throw entityPropertyException;

            var result = StubTable.Merge(GetStubEntity(tableOperation.Entity));
            switch (result.OperationResult)
            {
                case StubTableMergeOperationResult.Success:
                    return _GetTableResult(result);

                case StubTableMergeOperationResult.TableDoesNotExist:
                    throw TableDoesNotExistException();

                case StubTableMergeOperationResult.EntityDoesNotExists:
                    throw ResourceNotFoundException();

                case StubTableMergeOperationResult.EtagsDoNotMatch:
                    throw PreconditionFailedException();

                default:
                    throw new InvalidOperationException($"Operation result {result.OperationResult} not handled.");
            }
        }

        public override Func<IStubTableOperationDataResult, TableResult> BatchCallback(StubTableBatchOperation batchOperation, TableOperation tableOperation, OperationContext operationContext, int operationIndex)
        {
            if (tableOperation.Entity.PartitionKey == null)
                throw new ArgumentNullException("Merge requires a valid PartitionKey");
            var partitionKeyException = ValidateBatckKeyProperty(tableOperation.Entity.PartitionKey, operationIndex);
            if (partitionKeyException != null)
                throw partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                throw new ArgumentNullException("Merge requires a valid RowKey");
            var rowKeyException = ValidateBatckKeyProperty(tableOperation.Entity.RowKey, operationIndex);
            if (rowKeyException != null)
                throw rowKeyException;

            var entityPropertyException = ValidateEntityProperties(tableOperation.Entity);
            if (entityPropertyException != null)
                throw entityPropertyException;

            batchOperation.Merge(GetStubEntity(tableOperation.Entity));
            return operationResult =>
            {
                var result = (StubTableMergeOperationDataResult)operationResult;
                switch (result.OperationResult)
                {
                    case StubTableMergeOperationResult.Success:
                        return _GetTableResult(result);

                    case StubTableMergeOperationResult.TableDoesNotExist:
                        throw TableDoesNotExistForBatchException(operationIndex);

                    case StubTableMergeOperationResult.EntityDoesNotExists:
                        throw ResourceNotFoundException();

                    case StubTableMergeOperationResult.EtagsDoNotMatch:
                        throw PreconditionFailedException();

                    default:
                        throw new InvalidOperationException($"Operation result {result.OperationResult} not handled.");
                }
            };
        }

        private static TableResult _GetTableResult(StubTableMergeOperationDataResult result)
            => new TableResult
            {
                HttpStatusCode = 204,
                Etag = result.Entity.ETag,
                Result = new TableEntity
                {
                    PartitionKey = result.Entity.PartitionKey,
                    RowKey = result.Entity.RowKey,
                    ETag = result.Entity.ETag,
                    Timestamp = default(DateTimeOffset)
                }
            };
    }
}