namespace CloudStub.Core
{
    public class StubTableMergeOperationDataResult
    {
        internal StubTableMergeOperationDataResult(StubTableMergeOperationResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableMergeOperationDataResult(StubTableMergeOperationResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableMergeOperationResult OperationResult { get; }

        public StubEntity Entity { get; }
    }
}