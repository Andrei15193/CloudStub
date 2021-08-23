namespace CloudStub.Core.Operations
{
    public class StubTableMergeOperationDataResult : IStubTableOperationDataResult<StubTableMergeOperationResult>
    {
        internal StubTableMergeOperationDataResult(StubTableMergeOperationResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableMergeOperationDataResult(StubTableMergeOperationResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableOperationType OperationType
            => StubTableOperationType.Merge;

        public StubTableMergeOperationResult OperationResult { get; }

        int IStubTableOperationDataResult.OperationResult
            => (int)OperationResult;

        public bool IsSuccessful
            => OperationResult == StubTableMergeOperationResult.Success;

        public StubEntity Entity { get; }
    }
}