namespace CloudStub.Core
{
    public class StubTableQueryContinuationToken
    {
        internal StubTableQueryContinuationToken(StubEntity lastStubEntity)
            => (LastPartitionKey, LastRowKey) = (lastStubEntity.PartitionKey, lastStubEntity.RowKey);

        public string LastPartitionKey { get; }
        public string LastRowKey { get; }
    }
}