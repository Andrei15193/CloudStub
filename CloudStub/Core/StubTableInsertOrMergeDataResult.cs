namespace CloudStub.Core
{
    public class StubTableInsertOrMergeDataResult
    {
        internal StubTableInsertOrMergeDataResult(StubTableInsertOrMergeResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableInsertOrMergeDataResult(StubTableInsertOrMergeResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableInsertOrMergeResult OperationResult { get; }

        public StubEntity Entity { get; }
    }
}