namespace CloudStub.Core
{
    public class StubTableDeleteOperationDataResult : IStubTableOperationDataResult<StubTableDeleteOperationResult>
    {
        internal StubTableDeleteOperationDataResult(StubTableDeleteOperationResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableDeleteOperationDataResult(StubTableDeleteOperationResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableOperationType OperationType
            => StubTableOperationType.Delete;

        public StubTableDeleteOperationResult OperationResult { get; }

        int IStubTableOperationDataResult.OperationResult
            => (int)OperationResult;

        public StubEntity Entity { get; }
    }
}