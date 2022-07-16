namespace CloudStub.OperationResults
{
    public class StubTableRetrieveOperationDataResult : IStubTableOperationDataResult<StubTableRetrieveOperationResult>
    {
        internal StubTableRetrieveOperationDataResult()
            => (OperationResult, Entity) = (StubTableRetrieveOperationResult.TableDoesNotExist, null);

        internal StubTableRetrieveOperationDataResult(StubEntity stubEntity)
            => (OperationResult, Entity) = (stubEntity is object ? StubTableRetrieveOperationResult.Success : StubTableRetrieveOperationResult.EntityDoesNotExists, stubEntity);

        public StubTableOperationType OperationType
            => StubTableOperationType.Retrieve;

        public StubTableRetrieveOperationResult OperationResult { get; }

        int IStubTableOperationDataResult.OperationResult
            => (int)OperationResult;

        public bool IsSuccessful
            => OperationResult == StubTableRetrieveOperationResult.Success;

        public StubEntity Entity { get; }
    }
}