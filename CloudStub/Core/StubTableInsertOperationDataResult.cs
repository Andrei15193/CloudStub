namespace CloudStub.Core
{
    public class StubTableInsertOperationDataResult
    {
        internal StubTableInsertOperationDataResult(StubTableInsertOperationResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableInsertOperationDataResult(StubTableInsertOperationResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableInsertOperationResult OperationResult { get; }

        public StubEntity Entity { get; }
    }
}