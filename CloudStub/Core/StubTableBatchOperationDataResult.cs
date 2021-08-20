namespace CloudStub.Core
{
    public class StubTableBatchOperationDataResult
    {
        internal StubTableBatchOperationDataResult(StubTableBatchOperationResult batchOperationResult)
            => (BatchOperationResult, Index) = (batchOperationResult, default);

        internal StubTableBatchOperationDataResult(StubTableBatchOperationResult batchOperationResult, int? index)
            => (BatchOperationResult, Index) = (batchOperationResult, index);

        public StubTableBatchOperationResult BatchOperationResult { get; }

        public int? Index { get; }
    }
}