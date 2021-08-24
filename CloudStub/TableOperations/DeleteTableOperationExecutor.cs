using System;
using CloudStub.Core;
using CloudStub.Core.OperationResults;
using Microsoft.Azure.Cosmos.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal sealed class DeleteTableOperationExecutor : TableOperationExecutor
    {
        public DeleteTableOperationExecutor(StubTable stubTable)
            : base(stubTable)
        {
        }

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            if (tableOperation.Entity.PartitionKey == null)
                throw new ArgumentNullException("Delete requires a valid PartitionKey");
            var partitionKeyException = ValidateKeyProperty(tableOperation.Entity.PartitionKey);
            if (partitionKeyException != null)
                throw partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                throw new ArgumentNullException("Delete requires a valid RowKey");
            var rowKeyException = ValidateKeyProperty(tableOperation.Entity.RowKey);
            if (rowKeyException != null)
                throw rowKeyException;

            var result = StubTable.Delete(GetStubEntity(tableOperation.Entity));
            switch (result.OperationResult)
            {
                case StubTableDeleteOperationResult.Success:
                    return _GetTableResult(result);

                case StubTableDeleteOperationResult.TableDoesNotExist:
                    throw TableDoesNotExistException();

                case StubTableDeleteOperationResult.EntityDoesNotExists:
                    throw ResourceNotFoundException();

                case StubTableDeleteOperationResult.EtagsDoNotMatch:
                    throw PreconditionFailedException();

                default:
                    throw new InvalidOperationException($"Operation result {result.OperationResult} not handled.");
            }
        }

        public override Func<IStubTableOperationDataResult, TableResult> BatchCallback(StubTableBatchOperation batchOperation, TableOperation tableOperation, OperationContext operationContext, int operationIndex)
        {
            if (tableOperation.Entity.PartitionKey == null)
                throw new ArgumentNullException("Delete requires a valid PartitionKey");
            var partitionKeyException = ValidateBatckKeyProperty(tableOperation.Entity.PartitionKey, operationIndex);
            if (partitionKeyException != null)
                throw partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                throw new ArgumentNullException("Delete requires a valid RowKey");
            var rowKeyException = ValidateBatckKeyProperty(tableOperation.Entity.RowKey, operationIndex);
            if (rowKeyException != null)
                throw rowKeyException;

            batchOperation.Delete(GetStubEntity(tableOperation.Entity));
            return operationResult =>
            {
                var result = (StubTableDeleteOperationDataResult)operationResult;
                switch (result.OperationResult)
                {
                    case StubTableDeleteOperationResult.Success:
                        return _GetTableResult(result);

                    case StubTableDeleteOperationResult.TableDoesNotExist:
                        throw TableDoesNotExistForBatchException(operationIndex);

                    case StubTableDeleteOperationResult.EntityDoesNotExists:
                        throw ResourceNotFoundForBatchException();

                    case StubTableDeleteOperationResult.EtagsDoNotMatch:
                        throw PreconditionFailedForBatchException(operationIndex);

                    default:
                        throw new InvalidOperationException($"Operation result {result.OperationResult} not handled.");
                }
            };
        }

        private static TableResult _GetTableResult(StubTableDeleteOperationDataResult result)
            => new TableResult
            {
                HttpStatusCode = 204,
                Etag = null,
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