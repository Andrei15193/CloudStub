namespace CloudStub.Core
{
    public class StubTableRetrieveDataResult
    {
        internal StubTableRetrieveDataResult()
            => (RetrieveResult, Entity) = (StubTableRetrieveResult.TableDoesNotExist, null);

        internal StubTableRetrieveDataResult(StubEntity stubEntity)
            => (RetrieveResult, Entity) = (stubEntity is object ? StubTableRetrieveResult.Success : StubTableRetrieveResult.EntityDoesNotExists, stubEntity);

        public StubTableRetrieveResult RetrieveResult { get; }

        public StubEntity Entity { get; }
    }
}