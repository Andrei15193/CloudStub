namespace CloudStub.Core.OperationResults
{
    public class StubTableInsertOrReplaceOperationDataResult : IStubTableOperationDataResult<StubTableInsertOrReplaceOperationResult>
    {
        internal StubTableInsertOrReplaceOperationDataResult(StubTableInsertOrReplaceOperationResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        internal StubTableInsertOrReplaceOperationDataResult(StubTableInsertOrReplaceOperationResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableOperationType OperationType
            => StubTableOperationType.InsertOrReplace;

        public StubTableInsertOrReplaceOperationResult OperationResult { get; }

        int IStubTableOperationDataResult.OperationResult
            => (int)OperationResult;

        public bool IsSuccessful
            => OperationResult == StubTableInsertOrReplaceOperationResult.Success;

        public StubEntity Entity { get; }
    }
}