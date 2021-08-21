namespace CloudStub.Core
{
    public class StubTableInsertOrMergeOperationDataResult
    {
        internal StubTableInsertOrMergeOperationDataResult(StubTableInsertOrMergeOperationResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableInsertOrMergeOperationDataResult(StubTableInsertOrMergeOperationResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableInsertOrMergeOperationResult OperationResult { get; }

        public StubEntity Entity { get; }
    }
}