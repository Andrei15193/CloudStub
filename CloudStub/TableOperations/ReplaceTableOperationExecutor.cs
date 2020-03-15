using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.TableOperations
{
    internal sealed class ReplaceTableOperationExecutor : EditTableOperationExecutor
    {
        public ReplaceTableOperationExecutor(ITableOperationExecutorContext context)
            : base(context)
        {
        }

        protected override string PartitionKeyErrorMessage
            => "Replace requires a valid PartitionKey";

        protected override string RowKeyErrorMessage
            => "Replace requires a valid RowKey";

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            var dynamicEntity = GetDynamicEntity(tableOperation.Entity, operationContext);
            Context.Entities[tableOperation.Entity.PartitionKey][tableOperation.Entity.RowKey] = dynamicEntity;

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
    }
}