using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal abstract class AddTableOperationExecutor : TableOperationExecutor
    {
        protected AddTableOperationExecutor(ITableOperationExecutorContext context)
            : base(context)
        {
        }

        protected abstract Exception GetMissingPartitionKeyException();

        protected abstract Exception GetMissingRowKeyException();

        public override Exception Validate(TableOperation tableOperation, OperationContext operationContext)
        {
            if (!Context.TableExists)
                return TableDoesNotExistException();

            var entity = tableOperation.Entity;

            if (entity.PartitionKey == null)
                return GetMissingPartitionKeyException();
            if (entity.PartitionKey.Length > (1 << 10))
                return PropertyValueTooLargeException();
            if (!entity.PartitionKey.All(IsValidKeyCharacter))
                return InvalidPartitionKeyException(entity.PartitionKey);

            if (entity.RowKey == null)
                return GetMissingRowKeyException();
            if (entity.RowKey.Length > (1 << 10))
                return PropertyValueTooLargeException();
            if (!entity.RowKey.All(IsValidKeyCharacter))
                return InvalidRowKeyException(entity.RowKey);

            var entityPropertyException = ValidateEntityProperties(entity);
            if (entityPropertyException != null)
                return entityPropertyException;

            return null;
        }

        protected IDictionary<string, DynamicTableEntity> GetPartition(ITableEntity entity)
        {
            if (!Context.Entities.TryGetValue(entity.PartitionKey, out var entitiesByRowKey))
            {
                entitiesByRowKey = new SortedList<string, DynamicTableEntity>(StringComparer.Ordinal);
                Context.Entities.Add(entity.PartitionKey, entitiesByRowKey);
            }

            return entitiesByRowKey;
        }
    }
}