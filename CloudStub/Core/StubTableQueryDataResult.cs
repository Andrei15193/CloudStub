using System;
using System.Collections.Generic;

namespace CloudStub.Core
{
    public class StubTableQueryDataResult
    {
        internal StubTableQueryDataResult()
        {
            OperationResult = StubTableQueryResult.TableDoesNotExist;
            Entities = null;
            ContinuationToken = null;
        }

        internal StubTableQueryDataResult(IReadOnlyCollection<StubEntity> entities, StubTableQueryContinuationToken continuationToken)
        {
            OperationResult = StubTableQueryResult.Success;
            Entities = entities ?? Array.Empty<StubEntity>();
            ContinuationToken = continuationToken;
        }

        public StubTableQueryResult OperationResult { get; }

        public IReadOnlyCollection<StubEntity> Entities { get; }

        public StubTableQueryContinuationToken ContinuationToken { get; }
    }
}