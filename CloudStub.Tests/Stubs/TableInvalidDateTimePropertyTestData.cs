using System;
using System.Collections;
using System.Collections.Generic;

namespace CloudStub.Tests
{
    public class TableInvalidDateTimePropertyTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { new DateTime() };
            yield return new object[] { new DateTime(1601, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(-1) };
        }

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
    }
}