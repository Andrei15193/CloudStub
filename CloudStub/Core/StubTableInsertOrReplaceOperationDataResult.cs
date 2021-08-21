namespace CloudStub.Core
{
    public class StubTableInsertOrReplaceOperationDataResult
    {
        internal StubTableInsertOrReplaceOperationDataResult(StubTableInsertOrReplaceOperationResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableInsertOrReplaceOperationDataResult(StubTableInsertOrReplaceOperationResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableInsertOrReplaceOperationResult OperationResult { get; }

        public StubEntity Entity { get; }
    }
}