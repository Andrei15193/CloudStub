using System.Collections;
using System.Collections.Generic;

namespace CloudStub
{
    internal static class Extensions
    {
        public static IReadOnlyList<T> Subrange<T>(this IReadOnlyList<T> collection, int start)
            => collection.Subrange(start, collection.Count - start);

        public static IReadOnlyList<T> Subrange<T>(this IReadOnlyList<T> collection, int start, int length)
        {
            if (collection is ReadOnlySubrange<T> subrange)
                return subrange.Subrange(start, length);
            else
                return new ReadOnlySubrange<T>(collection, start, length);
        }

        private class ReadOnlySubrange<T> : IReadOnlyList<T>
        {
            private readonly IReadOnlyList<T> _collection;
            private readonly int _start;

            public ReadOnlySubrange(IReadOnlyList<T> collection, int start, int length)
            {
                _collection = collection;
                _start = start;
                Count = length;
            }

            public T this[int index]
                => _collection[_start + index];

            public int Count { get; }

            public IEnumerator<T> GetEnumerator()
            {
                for (var index = 0; index < Count; index++)
                    yield return _collection[_start + index];
            }

            IEnumerator IEnumerable.GetEnumerator()
                => GetEnumerator();

            public ReadOnlySubrange<T> Subrange(int start, int length)
                => new ReadOnlySubrange<T>(_collection, _start + start, length);
        }
    }
}