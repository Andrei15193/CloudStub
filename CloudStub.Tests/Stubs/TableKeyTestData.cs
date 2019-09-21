using System.Collections;
using System.Collections.Generic;

namespace CloudStub.Tests
{
    public class TableKeyTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { null };
            yield return new object[] { "/" };
            yield return new object[] { "\\" };
            yield return new object[] { "#" };
            yield return new object[] { "?" };
            yield return new object[] { "\t" };
            yield return new object[] { "\n" };
            yield return new object[] { "\r" };
            yield return new object[] { new string('t', 1025) };

            for (var controlChar = (char)0x0000; controlChar < 0x001F; controlChar++)
                yield return new object[] { controlChar };
            for (var controlChar = (char)0x007F; controlChar < 0x009F; controlChar++)
                yield return new object[] { controlChar };
        }

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
    }
}