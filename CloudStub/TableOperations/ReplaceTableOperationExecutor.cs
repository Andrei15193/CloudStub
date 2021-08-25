using System;
using CloudStub.Core;
using CloudStub.Core.OperationResults;
using Microsoft.Azure.Cosmos.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal sealed class ReplaceTableOperationExecutor : TableOperationExecutor
    {
        public ReplaceTableOperationExecutor(StubTable stubTable)
            : base(stubTable)
        {
        }

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            if (tableOperation.Entity.PartitionKey == null)
                throw new ArgumentNullException("Replace requires a valid PartitionKey");
            var partitionKeyException = ValidateKeyProperty(tableOperation.Entity.PartitionKey);
            if (partitionKeyException != null)
                throw partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                throw new ArgumentNullException("Replace requires a valid RowKey");
            var rowKeyException = ValidateKeyProperty(tableOperation.Entity.RowKey);
            if (rowKeyException != null)
                throw rowKeyException;

            var entityPropertyException = ValidateEntityProperties(tableOperation.Entity);
            if (entityPropertyException != null)
                throw entityPropertyException;

            var result = StubTable.Replace(GetStubEntity(tableOperation.Entity));
            switch (result.OperationResult)
            {
                case StubTableReplaceOperationResult.Success:
                    return _GetTableResult(result);

                case StubTableReplaceOperationResult.TableDoesNotExist:
                    throw TableDoesNotExistException();

                case StubTableReplaceOperationResult.EntityDoesNotExists:
                    throw ResourceNotFoundException();

                case StubTableReplaceOperationResult.EtagsDoNotMatch:
                    throw PreconditionFailedException();

                default:
                    throw new InvalidOperationException($"Operation result {result.OperationResult} not handled.");
            }
        }

        public override Func<IStubTableOperationDataResult, TableResult> BatchCallback(StubTableBatchOperation batchOperation, TableOperation tableOperation, OperationContext operationContext, int operationIndex)
        {
            if (tableOperation.Entity.PartitionKey == null)
                throw new ArgumentNullException("Replace requires a valid PartitionKey");
            var partitionKeyException = ValidateBatckKeyProperty(tableOperation.Entity.PartitionKey, operationIndex);
            if (partitionKeyException != null)
                throw partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                throw new ArgumentNullException("Replace requires a valid RowKey");
            var rowKeyException = ValidateBatckKeyProperty(tableOperation.Entity.RowKey, operationIndex);
            if (rowKeyException != null)
                throw rowKeyException;

            var entityPropertyException = ValidateEntityProperties(tableOperation.Entity);
            if (entityPropertyException != null)
                throw entityPropertyException;

            batchOperation.Replace(GetStubEntity(tableOperation.Entity));
            return operationResult =>
            {
                var result = (StubTableReplaceOperationDataResult)operationResult;
                switch (result.OperationResult)
                {
                    case StubTableReplaceOperationResult.Success:
                        return _GetTableResult(result);

                    case StubTableReplaceOperationResult.TableDoesNotExist:
                        throw TableDoesNotExistForBatchException(operationIndex);

                    case StubTableReplaceOperationResult.EntityDoesNotExists:
                        throw ResourceNotFoundException();

                    case StubTableReplaceOperationResult.EtagsDoNotMatch:
                        throw PreconditionFailedException();

                    default:
                        throw new InvalidOperationException($"Operation result {result.OperationResult} not handled.");
                }
            };
        }

        private static TableResult _GetTableResult(StubTableReplaceOperationDataResult result)
            => new TableResult
            {
                HttpStatusCode = 204,
                Etag = result.Entity.ETag,
                Result = new TableEntity
                {
                    PartitionKey = result.Entity.PartitionKey,
                    RowKey = result.Entity.RowKey,
                    ETag = result.Entity.ETag,
                    Timestamp = default
                }
            };
    }
}