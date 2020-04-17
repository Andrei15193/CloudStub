using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq;

namespace CloudStub.TableOperations
{
    internal sealed class RetrieveTableOperationExecutor : TableOperationExecutor
    {
        public RetrieveTableOperationExecutor(ITableOperationExecutorContext context)
            : base(context)
        {
        }

        public override Exception Validate(TableOperation tableOperation, OperationContext operationContext)
        {
            if (tableOperation.GetPartitionKey() == null)
                return new ArgumentNullException("partitionKey");
            if (tableOperation.GetRowKey() == null)
                return new ArgumentNullException("rowkey");

            return null;
        }

        public override Exception ValidateForBatch(TableOperation tableOperation, OperationContext operationContext, int operationIndex)
            => Validate(tableOperation, operationContext);

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            if (Context.Entities.TryGetValue(tableOperation.GetPartitionKey(), out var partition)
                && partition.TryGetValue(tableOperation.GetRowKey(), out var existingEntity))
                return new TableResult
                {
                    HttpStatusCode = 200,
                    Etag = existingEntity.ETag,
                    Result = _GetEntityRetrieveResult(existingEntity, tableOperation)
                };

            return new TableResult
            {
                HttpStatusCode = 404,
                Etag = null,
                Result = null
            };
        }

        private static object _GetEntityRetrieveResult(DynamicTableEntity existingEntity, TableOperation tableOperation)
        {
            var selectColumns = tableOperation.GetSelectColumns();
            var entityProperties = selectColumns == null ?
                existingEntity.Properties :
                existingEntity.Properties.Where(property => selectColumns.Contains(property.Key, StringComparer.Ordinal));

            var entityResolver = tableOperation.GetEntityResolver<object>();
            var entityResult = entityResolver(
                existingEntity.PartitionKey,
                existingEntity.RowKey,
                existingEntity.Timestamp,
                entityProperties.ToDictionary(entityProperty => entityProperty.Key, entityProperty => entityProperty.Value, StringComparer.Ordinal),
                existingEntity.ETag
            );
            return entityResult;
        }
    }
}