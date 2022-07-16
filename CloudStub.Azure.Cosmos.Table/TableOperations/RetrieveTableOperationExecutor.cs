using System;
using CloudStub.OperationResults;
using Microsoft.Azure.Cosmos.Table;

namespace CloudStub.TableOperations
{
    internal sealed class RetrieveTableOperationExecutor : TableOperationExecutor
    {
        public RetrieveTableOperationExecutor(StubTable stubTable)
            : base(stubTable)
        {
        }

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            var partitionKey = tableOperation.GetPartitionKey();
            var rowKey = tableOperation.GetRowKey();
            var selectedProperties = tableOperation.GetSelectColumns();

            if (partitionKey is null)
                throw new ArgumentNullException("partitionKey");
            if (rowKey is null)
                throw new ArgumentNullException("rowkey");

            var result = StubTable.Retrieve(partitionKey, rowKey, selectedProperties);
            switch (result.OperationResult)
            {
                case StubTableRetrieveOperationResult.Success:
                    return new TableResult
                    {
                        HttpStatusCode = 200,
                        Etag = result.Entity.ETag,
                        Result = _GetEntityRetrieveResult(result.Entity, tableOperation)
                    };

                case StubTableRetrieveOperationResult.TableDoesNotExist:
                case StubTableRetrieveOperationResult.EntityDoesNotExists:
                    return new TableResult
                    {
                        HttpStatusCode = 404,
                        Etag = null,
                        Result = null
                    };

                default:
                    throw new InvalidOperationException($"Operation result {result.OperationResult} not handled.");
            }
        }

        public override Func<IStubTableOperationDataResult, TableResult> BatchCallback(StubTableBatchOperation batchOperation, TableOperation tableOperation, OperationContext operationContext, int operationIndex)
        {
            var partitionKey = tableOperation.GetPartitionKey();
            var rowKey = tableOperation.GetRowKey();

            if (partitionKey is null)
                throw new ArgumentNullException("partitionKey");
            if (rowKey is null)
                throw new ArgumentNullException("rowkey");

            batchOperation.Retrieve(partitionKey, rowKey);
            return operationResult =>
            {
                var result = (StubTableRetrieveOperationDataResult)operationResult;
                switch (result.OperationResult)
                {
                    case StubTableRetrieveOperationResult.Success:
                        return new TableResult
                        {
                            HttpStatusCode = 200,
                            Etag = result.Entity.ETag,
                            Result = _GetEntityRetrieveResult(result.Entity, tableOperation)
                        };

                    case StubTableRetrieveOperationResult.TableDoesNotExist:
                    case StubTableRetrieveOperationResult.EntityDoesNotExists:
                        return new TableResult
                        {
                            HttpStatusCode = 404,
                            Etag = null,
                            Result = null
                        };

                    default:
                        throw new InvalidOperationException($"Operation result {result.OperationResult} not handled.");
                }
            };
        }

        private static object _GetEntityRetrieveResult(StubEntity stubEntity, TableOperation tableOperation)
        {
            var entityResolver = tableOperation.GetEntityResolver<object>();
            var entityResult = entityResolver(
                stubEntity.PartitionKey,
                stubEntity.RowKey,
                stubEntity.Timestamp.Value,
                GetEntityProperties(stubEntity.Properties),
                stubEntity.ETag
            );
            return entityResult;
        }
    }
}