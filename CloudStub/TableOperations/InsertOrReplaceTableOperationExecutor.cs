using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.TableOperations
{
    internal sealed class InsertOrReplaceTableOperationExecutor : AddTableOperationExecutor
    {
        public InsertOrReplaceTableOperationExecutor(ITableOperationExecutorContext context)
            : base(context)
        {
        }

        protected override Exception GetMissingPartitionKeyException()
            => new ArgumentNullException("Upserts require a valid PartitionKey");

        protected override Exception GetMissingRowKeyException()
            => new ArgumentNullException("Upserts require a valid RowKey");

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            var dynamicEntity = GetDynamicEntity(tableOperation.Entity, operationContext);
            var partition = GetPartition(dynamicEntity);
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