using System;

namespace CloudStub.StorageHandlers
{
    internal sealed class FileLockOptions
    {
        public TimeSpan LockIntervalCheck { get; set; } = TimeSpan.FromSeconds(0.5);

        public int? LockRetryCount { get; set; }
    }
}