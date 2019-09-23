using System.Collections;
using System.Collections.Generic;

namespace CloudStub.Tests
{
    public class TableInvalidBinaryPropertyTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { new byte[1 << 16 + 1] };
        }

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
    }
}