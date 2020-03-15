using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.TableOperations
{
    internal sealed class MergeTableOperationExecutor : EditTableOperationExecutor
    {
        public MergeTableOperationExecutor(ITableOperationExecutorContext context)
            : base(context)
        {
        }

        protected override string PartitionKeyErrorMessage
            => "Merge requires a valid PartitionKey";

        protected override string RowKeyErrorMessage
            => "Merge requires a valid RowKey";

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            var partition = Context.Entities[tableOperation.Entity.PartitionKey];
            var existingEntity = partition[tableOperation.Entity.RowKey];

            var dynamicEntity = GetDynamicEntity(tableOperation.Entity, operationContext);
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
    }
}