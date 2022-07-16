using System;
using System.Collections.Generic;

namespace CloudStub.OperationResults
{
    public class StubTableQueryDataResult
    {
        internal StubTableQueryDataResult()
            => (OperationResult, Entities, ContinuationToken) = (StubTableQueryResult.TableDoesNotExist, null, null);

        internal StubTableQueryDataResult(IReadOnlyCollection<StubEntity> entities, StubTableQueryContinuationToken continuationToken)
            => (OperationResult, Entities, ContinuationToken) = (StubTableQueryResult.Success, entities ?? Array.Empty<StubEntity>(), continuationToken);

        public StubTableQueryResult OperationResult { get; }

        public IReadOnlyCollection<StubEntity> Entities { get; }

        public StubTableQueryContinuationToken ContinuationToken { get; }
    }
}