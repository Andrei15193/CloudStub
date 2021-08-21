namespace CloudStub.Core
{
    public class StubTableDeleteOperationDataResult
    {
        internal StubTableDeleteOperationDataResult(StubTableDeleteOperationResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableDeleteOperationDataResult(StubTableDeleteOperationResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableDeleteOperationResult OperationResult { get; }

        public StubEntity Entity { get; }
    }
}