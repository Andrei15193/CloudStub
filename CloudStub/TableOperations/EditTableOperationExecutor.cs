using System;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal abstract class EditTableOperationExecutor : TableOperationExecutor
    {
        protected EditTableOperationExecutor(ITableOperationExecutorContext context)
            : base(context)
        {
        }

        protected abstract string PartitionKeyErrorMessage { get; }

        protected abstract string RowKeyErrorMessage { get; }

        public override Exception Validate(TableOperation tableOperation, OperationContext operationContext)
        {
            if (!Context.TableExists)
                return TableDoesNotExistException();

            if (tableOperation.Entity.PartitionKey == null)
                return new ArgumentNullException(PartitionKeyErrorMessage);
            if (!tableOperation.Entity.PartitionKey.All(IsValidKeyCharacter))
                return InvalidPartitionKeyException(tableOperation.Entity.PartitionKey);

            if (tableOperation.Entity.RowKey == null)
                return new ArgumentNullException(RowKeyErrorMessage);
            if (!tableOperation.Entity.RowKey.All(IsValidKeyCharacter))
                return InvalidRowKeyException(tableOperation.Entity.RowKey);

            var entityPropertyException = ValidateEntityProperties(tableOperation.Entity, operationContext);
            if (entityPropertyException != null)
                return entityPropertyException;

            if (!Context.Entities.TryGetValue(tableOperation.Entity.PartitionKey, out var partition)
                || !partition.TryGetValue(tableOperation.Entity.RowKey, out var existingEntity))
                return ResourceNotFoundException();

            if (tableOperation.Entity.ETag != "*" && !StringComparer.OrdinalIgnoreCase.Equals(tableOperation.Entity.ETag, existingEntity.ETag))
                return PreconditionFailedException();

            return null;
        }
    }
}