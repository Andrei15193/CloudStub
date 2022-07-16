namespace CloudStub.OperationResults
{
    public class StubTableReplaceOperationDataResult : IStubTableOperationDataResult<StubTableReplaceOperationResult>
    {
        internal StubTableReplaceOperationDataResult(StubTableReplaceOperationResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableReplaceOperationDataResult(StubTableReplaceOperationResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableOperationType OperationType
            => StubTableOperationType.Replace;

        public StubTableReplaceOperationResult OperationResult { get; }

        int IStubTableOperationDataResult.OperationResult
            => (int)OperationResult;

        public bool IsSuccessful
            => OperationResult == StubTableReplaceOperationResult.Success;

        public StubEntity Entity { get; }
    }
}