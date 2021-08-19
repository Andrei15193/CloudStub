namespace CloudStub.Core
{
    public class StubTableBulkOperationDataResult
    {
        internal StubTableBulkOperationDataResult(StubTableBulkOperationResult bulkOperationResult)
            => (BulkOperationResult, Index) = (bulkOperationResult, default);

        internal StubTableBulkOperationDataResult(StubTableBulkOperationResult bulkOperationResult, int? index)
            => (BulkOperationResult, Index) = (bulkOperationResult, index);

        public StubTableBulkOperationResult BulkOperationResult { get; }

        public int? Index { get; }
    }
}