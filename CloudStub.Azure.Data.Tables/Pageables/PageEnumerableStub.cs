using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure;

namespace CloudStub.Azure.Data.Tables.Pageables
{
    internal delegate Page<T> PageFactory<T>(string continuationToken, int pageSize);

    internal class PageEnumerableStub<T> : IEnumerable<Page<T>>, IAsyncEnumerable<Page<T>>
    {
        private readonly PageFactory<T> _pageFactory;
        private readonly string _continuationToken;
        private readonly int _pageSize;

        public PageEnumerableStub(PageFactory<T> pageFactory, string continuationToken, int pageSize)
            => (_pageFactory, _continuationToken, _pageSize) = (pageFactory, continuationToken, pageSize);

        public IEnumerator<Page<T>> GetEnumerator()
            => new PageEnumeratorStub(_pageFactory, _continuationToken, _pageSize, default);

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();

        public IAsyncEnumerator<Page<T>> GetAsyncEnumerator(CancellationToken cancellationToken)
            => new PageEnumeratorStub(_pageFactory, _continuationToken, _pageSize, cancellationToken);

        private class PageEnumeratorStub : IEnumerator<Page<T>>, IAsyncEnumerator<Page<T>>
        {
            private bool _hasCompleted = false;
            private string _continuationToken;
            private readonly PageFactory<T> _pageFactory;
            private readonly int _pageSize;
            private readonly CancellationToken _cancellationToken;

            public PageEnumeratorStub(PageFactory<T> pageFactory, string continuationToken, int pageSize, CancellationToken cancellationToken)
                => (_pageFactory, _continuationToken, _pageSize, _cancellationToken) = (pageFactory, continuationToken, pageSize, cancellationToken);

            public Page<T> Current { get; private set; }

            object IEnumerator.Current
                => Current;

            public bool MoveNext()
            {
                if (_hasCompleted)
                    return false;

                Current = _pageFactory(_continuationToken, _pageSize);
                _continuationToken = Current.ContinuationToken;
                _hasCompleted = _continuationToken == null;
                return true;
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                await Task.Yield();
                _cancellationToken.ThrowIfCancellationRequested();

                return MoveNext();
            }

            void IEnumerator.Reset()
            {
                _hasCompleted = false;
                _continuationToken = null;
            }

            void IDisposable.Dispose()
            {
            }

            ValueTask IAsyncDisposable.DisposeAsync()
                => new ValueTask();
        }
    }
}