using System;
using System.Linq;
using CloudStub.Core;
using CloudStub.Core.OperationResults;
using Microsoft.Azure.Cosmos.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal sealed class InsertTableOperationExecutor : TableOperationExecutor
    {
        public InsertTableOperationExecutor(StubTable stubTable)
            : base(stubTable)
        {
        }

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            if (tableOperation.Entity.PartitionKey == null)
                throw PropertiesWithoutValueException();
            if (tableOperation.Entity.PartitionKey.Length > (1 << 10))
                throw PropertyValueTooLargeException();
            if (!tableOperation.Entity.PartitionKey.All(IsValidKeyCharacter))
                throw InvalidPartitionKeyException(tableOperation.Entity.PartitionKey);

            if (tableOperation.Entity.RowKey == null)
                throw PropertiesWithoutValueException();
            if (tableOperation.Entity.RowKey.Length > (1 << 10))
                throw PropertyValueTooLargeException();
            if (!tableOperation.Entity.RowKey.All(IsValidKeyCharacter))
                throw InvalidRowKeyException(tableOperation.Entity.RowKey);

            var entityPropertyException = ValidateEntityProperties(tableOperation.Entity);
            if (entityPropertyException != null)
                throw entityPropertyException;

            var result = StubTable.Insert(GetStubEntity(tableOperation.Entity));
            switch (result.OperationResult)
            {
                case StubTableInsertOperationResult.Success:
                    return _GetTableResult(result);

                case StubTableInsertOperationResult.TableDoesNotExist:
                    throw TableDoesNotExistException();

                case StubTableInsertOperationResult.EntityAlreadyExists:
                    throw EntityAlreadyExistsException();

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
            var partitionKeyException = _ValidateBatckPartitionKeyProperty(tableOperation.Entity.PartitionKey, operationIndex);
            if (partitionKeyException != null)
                throw partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                throw new ArgumentNullException("Upserts require a valid RowKey");
            if (tableOperation.Entity.RowKey.Length > (1 << 10))
                throw PropertyValueTooLargeForBatchException(operationIndex);
            var rowKeyException = _ValidateBatckRowKeyProperty(tableOperation.Entity.RowKey, operationIndex);
            if (rowKeyException != null)
                throw rowKeyException;

            var entityPropertyException = ValidateEntityPropertiesForBatch(tableOperation.Entity, operationContext, operationIndex);
            if (entityPropertyException != null)
                throw entityPropertyException;

            batchOperation.Insert(GetStubEntity(tableOperation.Entity));
            return operationResult =>
            {
                var result = (StubTableInsertOperationDataResult)operationResult;
                switch (result.OperationResult)
                {
                    case StubTableInsertOperationResult.Success:
                        return _GetTableResult(result);

                    case StubTableInsertOperationResult.TableDoesNotExist:
                        throw TableDoesNotExistForBatchInsertException(operationIndex);

                    case StubTableInsertOperationResult.EntityAlreadyExists:
                        throw EntityAlreadyExistsForBatchException();

                    default:
                        throw new InvalidOperationException($"Operation result {result.OperationResult} not handled.");
                }
            };
        }

        private StorageException _ValidateBatckPartitionKeyProperty(string value, int operationIndex)
            => value
                .Select(@char => !IsValidKeyCharacter(@char) ? InvalidPartitionKeyForBatchException(value, operationIndex) : null)
                .FirstOrDefault(exception => exception != null);

        private StorageException _ValidateBatckRowKeyProperty(string value, int operationIndex)
            => value
                .Select(@char => !IsValidKeyCharacter(@char) ? InvalidRowKeyForBatchException(value, operationIndex) : null)
                .FirstOrDefault(exception => exception != null);

        private static TableResult _GetTableResult(StubTableInsertOperationDataResult result)
            => new TableResult
            {
                HttpStatusCode = 204,
                Etag = result.Entity.ETag,
                Result = new TableEntity
                {
                    PartitionKey = result.Entity.PartitionKey,
                    RowKey = result.Entity.RowKey,
                    ETag = result.Entity.ETag,
                    Timestamp = result.Entity.Timestamp.Value
                }
            };
    }
}