using System.Collections;
using System.Collections.Generic;

namespace CloudStub.Tests
{
    public class TableInvalidKeyTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { "/" };
            yield return new object[] { "\\" };
            yield return new object[] { "#" };
            yield return new object[] { "?" };
            yield return new object[] { "\t" };
            yield return new object[] { "\n" };
            yield return new object[] { "\r" };

            for (var controlChar = (char)0x0000; controlChar < 0x001F; controlChar++)
                yield return new object[] { new string(controlChar, 1) };
            for (var controlChar = (char)0x007F; controlChar < 0x009F; controlChar++)
                yield return new object[] { new string(controlChar, 1) };
        }

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
    }
}