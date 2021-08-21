﻿namespace CloudStub.Core
{
    public interface IStubTableOperationDataResult
    {
        StubTableOperationType OperationType { get; }

        int OperationResult { get; }

        bool IsSuccessful { get; }

        StubEntity Entity { get; }
    }

    public interface IStubTableOperationDataResult<TOperationResult> : IStubTableOperationDataResult
    {
        new TOperationResult OperationResult { get; }
    }
}