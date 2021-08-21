namespace CloudStub.Core
{
    public class StubTableReplaceOperationDataResult
    {
        public StubTableReplaceOperationDataResult(StubTableReplaceOperationResult operationResult)
            => (OperationResult, Entity) = (operationResult, default);

        public StubTableReplaceOperationDataResult(StubTableReplaceOperationResult operationResult, StubEntity entity)
            => (OperationResult, Entity) = (operationResult, entity);

        public StubTableReplaceOperationResult OperationResult { get; }

        public StubEntity Entity { get; }
    }
}