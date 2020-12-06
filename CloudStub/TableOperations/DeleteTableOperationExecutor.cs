using System;
using Microsoft.Azure.Cosmos.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal sealed class DeleteTableOperationExecutor : TableOperationExecutor
    {
        public DeleteTableOperationExecutor(ITableOperationExecutorContext context)
            : base(context)
        {
        }

        public override Exception Validate(TableOperation tableOperation, OperationContext operationContext)
        {
            if (!Context.TableExists)
                return TableDoesNotExistException();

            if (tableOperation.Entity.PartitionKey == null)
                return new ArgumentNullException("Delete requires a valid PartitionKey");
            var partitionKeyException = ValidateKeyProperty(tableOperation.Entity.PartitionKey);
            if (partitionKeyException != null)
                return partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                return new ArgumentNullException("Delete requires a valid RowKey");
            var rowKeyException = ValidateKeyProperty(tableOperation.Entity.RowKey);
            if (rowKeyException != null)
                return rowKeyException;

            if (!Context.Entities.TryGetValue(tableOperation.Entity.PartitionKey, out var partition)
                || !partition.TryGetValue(tableOperation.Entity.RowKey, out var existingEntity))
                return ResourceNotFoundException();

            if (tableOperation.Entity.ETag != "*" && !StringComparer.OrdinalIgnoreCase.Equals(tableOperation.Entity.ETag, existingEntity.ETag))
                return PreconditionFailedException();

            return null;
        }

        public override Exception ValidateForBatch(TableOperation tableOperation, OperationContext operationContext, int operationIndex)
        {
            if (!Context.TableExists)
                return TableDoesNotExistForBatchException(operationIndex);

            if (tableOperation.Entity.PartitionKey == null)
                return new ArgumentNullException("Delete requires a valid PartitionKey");
            var partitionKeyException = ValidateBatckKeyProperty(tableOperation.Entity.PartitionKey, operationIndex);
            if (partitionKeyException != null)
                return partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                return new ArgumentNullException("Delete requires a valid RowKey");
            var rowKeyException = ValidateBatckKeyProperty(tableOperation.Entity.RowKey, operationIndex);
            if (rowKeyException != null)
                return rowKeyException;

            if (!Context.Entities.TryGetValue(tableOperation.Entity.PartitionKey, out var partition)
                || !partition.TryGetValue(tableOperation.Entity.RowKey, out var existingEntity))
                return ResourceNotFoundForBatchException();

            if (tableOperation.Entity.ETag != "*" && !StringComparer.OrdinalIgnoreCase.Equals(tableOperation.Entity.ETag, existingEntity.ETag))
                return PreconditionFailedForBatchException(operationIndex);

            return null;
        }

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            var partition = Context.Entities[tableOperation.Entity.PartitionKey];
            var existingEntity = partition[tableOperation.Entity.RowKey];
            partition.Remove(tableOperation.Entity.RowKey);

            return new TableResult
            {
                HttpStatusCode = 204,
                Etag = null,
                Result = new TableEntity
                {
                    PartitionKey = existingEntity.PartitionKey,
                    RowKey = existingEntity.RowKey,
                    ETag = existingEntity.ETag,
                    Timestamp = default(DateTimeOffset)
                }
            };
        }
    }
}