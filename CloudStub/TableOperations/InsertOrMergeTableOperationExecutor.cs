using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal sealed class InsertOrMergeTableOperationExecutor : TableOperationExecutor
    {
        public InsertOrMergeTableOperationExecutor(ITableOperationExecutorContext context)
            : base(context)
        {
        }

        public override Exception Validate(TableOperation tableOperation, OperationContext operationContext)
        {
            if (!Context.TableExists)
                return TableDoesNotExistException();

            if (tableOperation.Entity.PartitionKey == null)
                return new ArgumentNullException("Upserts require a valid PartitionKey");
            if (tableOperation.Entity.PartitionKey.Length > (1 << 10))
                return PropertyValueTooLargeException();
            var partitionKeyException = ValidateKeyProperty(tableOperation.Entity.PartitionKey);
            if (partitionKeyException != null)
                return partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                return new ArgumentNullException("Upserts require a valid RowKey");
            if (tableOperation.Entity.RowKey.Length > (1 << 10))
                return PropertyValueTooLargeException();
            var rowKeyException = ValidateKeyProperty(tableOperation.Entity.RowKey);
            if (rowKeyException != null)
                return rowKeyException;

            var entityPropertyException = ValidateEntityProperties(tableOperation.Entity, operationContext);
            if (entityPropertyException != null)
                return entityPropertyException;

            return null;
        }

        public override Exception ValidateForBatch(TableOperation tableOperation, OperationContext operationContext, int operationIndex)
        {
            if (!Context.TableExists)

                return TableDoesNotExistForBatchException(operationIndex);

            if (tableOperation.Entity.PartitionKey == null)
                return new ArgumentNullException("Upserts require a valid PartitionKey");
            if (tableOperation.Entity.PartitionKey.Length > (1 << 10))
                return PropertyValueTooLargeForBatchException(operationIndex);
            var partitionKeyException = ValidateBatckKeyProperty(tableOperation.Entity.PartitionKey, operationIndex);
            if (partitionKeyException != null)
                return partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                return new ArgumentNullException("Upserts require a valid RowKey");
            if (tableOperation.Entity.RowKey.Length > (1 << 10))
                return PropertyValueTooLargeForBatchException(operationIndex);
            var rowKeyException = ValidateBatckKeyProperty(tableOperation.Entity.RowKey, operationIndex);
            if (rowKeyException != null)
                return rowKeyException;

            var entityPropertyException = ValidateEntityPropertiesForBatch(tableOperation.Entity, operationContext, operationIndex);
            if (entityPropertyException != null)
                return entityPropertyException;

            return null;
        }

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            var dynamicEntity = GetDynamicEntity(tableOperation.Entity, operationContext);
            var partition = _GetPartition(dynamicEntity);

            if (partition.TryGetValue(dynamicEntity.RowKey, out var existingEntity))
                foreach (var property in existingEntity.Properties)
                    if (!dynamicEntity.Properties.ContainsKey(property.Key))
                        dynamicEntity.Properties.Add(property);
            partition[dynamicEntity.RowKey] = dynamicEntity;

            return new TableResult
            {
                HttpStatusCode = 204,
                Etag = dynamicEntity.ETag,
                Result = new TableEntity
                {
                    PartitionKey = dynamicEntity.PartitionKey,
                    RowKey = dynamicEntity.RowKey,
                    ETag = dynamicEntity.ETag,
                    Timestamp = default(DateTimeOffset)
                }
            };
        }

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