namespace CloudStub.Core
{
    public class StubTableBatchOperationDataResult
    {
        internal StubTableBatchOperationDataResult(StubTableBatchOperationResult batchOperationResult)
            => (OperationResult, Index) = (batchOperationResult, default);

        internal StubTableBatchOperationDataResult(StubTableBatchOperationResult batchOperationResult, int? index)
            => (OperationResult, Index) = (batchOperationResult, index);

        public StubTableBatchOperationResult OperationResult { get; }

        public int? Index { get; }
    }
}