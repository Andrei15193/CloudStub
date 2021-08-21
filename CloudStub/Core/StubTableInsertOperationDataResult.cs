namespace CloudStub.Core
{
    public class StubTableInsertOperationDataResult : IStubTableOperationDataResult<StubTableInsertOperationResult>
    {
        internal StubTableInsertOperationDataResult(StubTableInsertOperationResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableInsertOperationDataResult(StubTableInsertOperationResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableOperationType OperationType
            => StubTableOperationType.Insert;

        public StubTableInsertOperationResult OperationResult { get; }

        int IStubTableOperationDataResult.OperationResult
            => (int)OperationResult;

        public StubEntity Entity { get; }
    }
}