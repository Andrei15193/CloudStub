using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal sealed class InsertTableOperationExecutor : AddTableOperationExecutor
    {
        public InsertTableOperationExecutor(ITableOperationExecutorContext context)
            : base(context)
        {
        }

        public override Exception Validate(TableOperation tableOperation, OperationContext operationContext)
        {
            var exception = base.Validate(tableOperation, operationContext);
            if (exception != null)
                return exception;

            if (Context.Entities.TryGetValue(tableOperation.Entity.PartitionKey, out var partition) && partition.ContainsKey(tableOperation.Entity.RowKey))
                return EntityAlreadyExistsException();

            return null;
        }

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            var dynamicEntity = GetDynamicEntity(tableOperation.Entity, operationContext);
            var partition = GetPartition(dynamicEntity);
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

        protected override Exception GetMissingPartitionKeyException()
            => PropertiesWithoutValueException();

        protected override Exception GetMissingRowKeyException()
            => PropertiesWithoutValueException();
    }
}