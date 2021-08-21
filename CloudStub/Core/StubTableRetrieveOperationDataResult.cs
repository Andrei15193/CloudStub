namespace CloudStub.Core
{
    public class StubTableRetrieveOperationDataResult
    {
        internal StubTableRetrieveOperationDataResult()
            => (RetrieveResult, Entity) = (StubTableRetrieveOperationResult.TableDoesNotExist, null);

        internal StubTableRetrieveOperationDataResult(StubEntity stubEntity)
            => (RetrieveResult, Entity) = (stubEntity is object ? StubTableRetrieveOperationResult.Success : StubTableRetrieveOperationResult.EntityDoesNotExists, stubEntity);

        public StubTableRetrieveOperationResult RetrieveResult { get; }

        public StubEntity Entity { get; }
    }
}