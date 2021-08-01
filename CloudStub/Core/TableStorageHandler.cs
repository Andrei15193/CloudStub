using System;

namespace CloudStub.Core
{

    internal sealed class CallbackDisposable : IDisposable
    {
        private readonly Action<bool> _disposeCallback;

        public CallbackDisposable(Action<bool> disposeCallback)
            => _disposeCallback = disposeCallback;

        public CallbackDisposable(Action disposeCallback)
            => _disposeCallback = (disposing => disposeCallback());

        ~CallbackDisposable()
            => _disposeCallback(false);

        public void Dispose()
        {
            _disposeCallback(true);
            GC.SuppressFinalize(this);
        }
    }
}