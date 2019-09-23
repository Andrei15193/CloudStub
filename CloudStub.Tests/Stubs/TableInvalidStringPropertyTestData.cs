using System.Collections;
using System.Collections.Generic;

namespace CloudStub.Tests
{
    public class TableInvalidStringPropertyTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { new string('t', 1 << 15 + 1) };
        }

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
    }
}