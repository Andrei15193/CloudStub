using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal sealed class InsertTableOperationExecutor : TableOperationExecutor
    {
        public InsertTableOperationExecutor(ITableOperationExecutorContext context)
            : base(context)
        {
        }

        public override Exception Validate(TableOperation tableOperation, OperationContext operationContext)
        {
            if (!Context.TableExists)
                return TableDoesNotExistException();

            if (tableOperation.Entity.PartitionKey == null)
                return PropertiesWithoutValueException();
            if (tableOperation.Entity.PartitionKey.Length > (1 << 10))
                return PropertyValueTooLargeException();
            if (!tableOperation.Entity.PartitionKey.All(IsValidKeyCharacter))
                return InvalidPartitionKeyException(tableOperation.Entity.PartitionKey);

            if (tableOperation.Entity.RowKey == null)
                return PropertiesWithoutValueException();
            if (tableOperation.Entity.RowKey.Length > (1 << 10))
                return PropertyValueTooLargeException();
            if (!tableOperation.Entity.RowKey.All(IsValidKeyCharacter))
                return InvalidRowKeyException(tableOperation.Entity.RowKey);

            var entityPropertyException = ValidateEntityProperties(tableOperation.Entity);
            if (entityPropertyException != null)
                return entityPropertyException;

            if (Context.Entities.TryGetValue(tableOperation.Entity.PartitionKey, out var partition) && partition.ContainsKey(tableOperation.Entity.RowKey))
                return EntityAlreadyExistsException();

            return null;
        }

        public override Exception ValidateForBatch(TableOperation tableOperation, OperationContext operationContext, int operationIndex)
        {
            if (!Context.TableExists)
                return TableDoesNotExistForBatchInsertException(operationIndex);

            if (tableOperation.Entity.PartitionKey == null)
                return new ArgumentNullException("Upserts require a valid PartitionKey");
            if (tableOperation.Entity.PartitionKey.Length > (1 << 10))
                return PropertyValueTooLargeForBatchException(operationIndex);
            var partitionKeyException = _ValidateBatckPartitionKeyProperty(tableOperation.Entity.PartitionKey, operationIndex);
            if (partitionKeyException != null)
                return partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                return new ArgumentNullException("Upserts require a valid RowKey");
            if (tableOperation.Entity.RowKey.Length > (1 << 10))
                return PropertyValueTooLargeForBatchException(operationIndex);
            var rowKeyException = _ValidateBatckRowKeyProperty(tableOperation.Entity.RowKey, operationIndex);
            if (rowKeyException != null)
                return rowKeyException;

            var entityPropertyException = ValidateEntityPropertiesForBatch(tableOperation.Entity, operationContext, operationIndex);
            if (entityPropertyException != null)
                return entityPropertyException;

            if (Context.Entities.TryGetValue(tableOperation.Entity.PartitionKey, out var partition) && partition.ContainsKey(tableOperation.Entity.RowKey))
                return EntityAlreadyExistsForBatchException();

            return null;
        }

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            var dynamicEntity = GetDynamicEntity(tableOperation.Entity);
            var partition = _GetPartition(dynamicEntity);
            partition.Add(dynamicEntity.RowKey, dynamicEntity);

            return new TableResult
            {
                HttpStatusCode = 204,
                Etag = dynamicEntity.ETag,
                Result = new TableEntity
                {
                    PartitionKey = dynamicEntity.PartitionKey,
                    RowKey = dynamicEntity.RowKey,
                    ETag = dynamicEntity.ETag,
                    Timestamp = dynamicEntity.Timestamp
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

        private IDictionary<string, DynamicTableEntity> _GetPartition(ITableEntity entity)
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