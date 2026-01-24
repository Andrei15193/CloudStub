using System.Collections.Generic;
using Azure;

namespace CloudStub.Azure.Data.Tables.Pageables
{
    internal class AsyncPageableStub<T> : AsyncPageable<T>
    {
        private readonly int _defaultPageSize;
        private readonly PageFactory<T> _pageFactory;

        public AsyncPageableStub(PageFactory<T> pageFactory, int? defaultPageSize = 1000)
            => (_pageFactory, _defaultPageSize) = (pageFactory, defaultPageSize ?? 1000);

        public override IAsyncEnumerable<Page<T>> AsPages(string continuationToken = null, int? pageSizeHint = null)
            => new PageEnumerableStub<T>(_pageFactory, continuationToken, pageSizeHint.GetValueOrDefault(_defaultPageSize));
    }
}