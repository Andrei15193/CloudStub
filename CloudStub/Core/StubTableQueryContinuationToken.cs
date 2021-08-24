namespace CloudStub.Core
{
    public class StubTableQueryContinuationToken
    {
        internal StubTableQueryContinuationToken(StubEntity lastStubEntity)
            => (LastPartitionKey, LastRowKey) = (lastStubEntity.PartitionKey, lastStubEntity.RowKey);

        internal StubTableQueryContinuationToken(string lastPartitionKey, string lastRowKey)
            => (LastPartitionKey, LastRowKey) = (lastPartitionKey, lastRowKey);

        public string LastPartitionKey { get; }
        public string LastRowKey { get; }
    }
}