using System;
using CloudStub.Core;
using CloudStub.Core.OperationResults;
using Microsoft.Azure.Cosmos.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal sealed class InsertOrReplaceTableOperationExecutor : TableOperationExecutor
    {
        public InsertOrReplaceTableOperationExecutor(StubTable stubTable)
            : base(stubTable)
        {
        }

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            if (tableOperation.Entity.PartitionKey == null)
                throw new ArgumentNullException("Upserts require a valid PartitionKey");
            if (tableOperation.Entity.PartitionKey.Length > (1 << 10))
                throw PropertyValueTooLargeException();
            var partitionKeyException = ValidateKeyProperty(tableOperation.Entity.PartitionKey);
            if (partitionKeyException != null)
                throw partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                throw new ArgumentNullException("Upserts require a valid RowKey");
            if (tableOperation.Entity.RowKey.Length > (1 << 10))
                throw PropertyValueTooLargeException();
            var rowKeyException = ValidateKeyProperty(tableOperation.Entity.RowKey);
            if (rowKeyException != null)
                throw rowKeyException;

            var entityPropertyException = ValidateEntityProperties(tableOperation.Entity);
            if (entityPropertyException != null)
                throw entityPropertyException;

            var result = StubTable.InsertOrReplace(GetStubEntity(tableOperation.Entity));
            switch (result.OperationResult)
            {
                case StubTableInsertOrReplaceOperationResult.Success:
                    return _GetTableResult(result);

                case StubTableInsertOrReplaceOperationResult.TableDoesNotExist:
                    throw TableDoesNotExistException();

                default:
                    throw new InvalidOperationException($"Operation result {result.OperationResult} not handled.");
            }
        }

        public override Func<IStubTableOperationDataResult, TableResult> BatchCallback(StubTableBatchOperation batchOperation, TableOperation tableOperation, OperationContext operationContext, int operationIndex)
        {
            if (tableOperation.Entity.PartitionKey == null)
                throw new ArgumentNullException("Upserts require a valid PartitionKey");
            if (tableOperation.Entity.PartitionKey.Length > (1 << 10))
                throw PropertyValueTooLargeForBatchException(operationIndex);
            var partitionKeyException = ValidateBatckKeyProperty(tableOperation.Entity.PartitionKey, operationIndex);
            if (partitionKeyException != null)
                throw partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                throw new ArgumentNullException("Upserts require a valid RowKey");
            if (tableOperation.Entity.RowKey.Length > (1 << 10))
                throw PropertyValueTooLargeForBatchException(operationIndex);
            var rowKeyException = ValidateBatckKeyProperty(tableOperation.Entity.RowKey, operationIndex);
            if (rowKeyException != null)
                throw rowKeyException;

            var entityPropertyException = ValidateEntityPropertiesForBatch(tableOperation.Entity, operationContext, operationIndex);
            if (entityPropertyException != null)
                throw entityPropertyException;

            batchOperation.InsertOrReplace(GetStubEntity(tableOperation.Entity));
            return operationResult =>
            {
                var result = (StubTableInsertOrReplaceOperationDataResult)operationResult;
                switch (result.OperationResult)
                {
                    case StubTableInsertOrReplaceOperationResult.Success:
                        return _GetTableResult(result);

                    case StubTableInsertOrReplaceOperationResult.TableDoesNotExist:
                        throw TableDoesNotExistForBatchException(operationIndex);

                    default:
                        throw new InvalidOperationException($"Operation result {result.OperationResult} not handled.");
                }
            };
        }

        private static TableResult _GetTableResult(StubTableInsertOrReplaceOperationDataResult result)
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