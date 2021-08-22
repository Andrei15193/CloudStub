using System;
using System.Collections.Generic;

namespace CloudStub.Core
{
    public class StubTableBatchOperationDataResult
    {
        internal StubTableBatchOperationDataResult(StubTableBatchOperationResult operationResult)
            => (OperationResult, IndividualOperationResults) = (operationResult, Array.Empty<IStubTableOperationDataResult>());

        internal StubTableBatchOperationDataResult(StubTableBatchOperationResult operationResult, IReadOnlyList<IStubTableOperationDataResult> operationResults)
            => (OperationResult, IndividualOperationResults) = (operationResult, operationResults);

        public StubTableBatchOperationResult OperationResult { get; }

        public bool IsSuccessful
            => OperationResult == StubTableBatchOperationResult.Success;

        public IReadOnlyList<IStubTableOperationDataResult> IndividualOperationResults { get; }
    }
}