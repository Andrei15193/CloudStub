namespace CloudStub.Core
{
    public class StubTableInsertDataResult
    {
        internal StubTableInsertDataResult(StubTableInsertResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableInsertDataResult(StubTableInsertResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableInsertResult OperationResult { get; }

        public StubEntity Entity { get; }
    }
}