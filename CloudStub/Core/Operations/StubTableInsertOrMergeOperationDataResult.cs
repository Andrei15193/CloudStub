namespace CloudStub.Core.Operations
{
    public class StubTableInsertOrMergeOperationDataResult : IStubTableOperationDataResult<StubTableInsertOrMergeOperationResult>
    {
        internal StubTableInsertOrMergeOperationDataResult(StubTableInsertOrMergeOperationResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableInsertOrMergeOperationDataResult(StubTableInsertOrMergeOperationResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableOperationType OperationType
            => StubTableOperationType.InsertOrMerge;

        public StubTableInsertOrMergeOperationResult OperationResult { get; }

        int IStubTableOperationDataResult.OperationResult
            => (int)OperationResult;

        public bool IsSuccessful
            => OperationResult == StubTableInsertOrMergeOperationResult.Success;

        public StubEntity Entity { get; }
    }
}