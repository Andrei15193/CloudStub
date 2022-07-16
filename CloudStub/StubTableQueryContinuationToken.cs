namespace CloudStub
{
    public class StubTableQueryContinuationToken
    {
        public StubTableQueryContinuationToken(StubEntity lastStubEntity)
            => (LastPartitionKey, LastRowKey) = (lastStubEntity.PartitionKey, lastStubEntity.RowKey);

        public StubTableQueryContinuationToken(string lastPartitionKey, string lastRowKey)
            => (LastPartitionKey, LastRowKey) = (lastPartitionKey, lastRowKey);

        public string LastPartitionKey { get; }
        public string LastRowKey { get; }
    }
}