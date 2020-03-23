using System;
using System.Collections.Generic;

namespace CloudStub.Tests
{
    public static class TableOperationTestData
    {
        public static IEnumerable<object[]> InvalidKeyTestData
        {
            get
            {
                yield return new object[] { "#" };
                yield return new object[] { "?" };
                yield return new object[] { "\t" };
                yield return new object[] { "\n" };
                yield return new object[] { "\r" };
                yield return new object[] { "/" };
                yield return new object[] { "\\" };

                for (var controlChar = (char)0x0000; controlChar <= 0x001F; controlChar++)
                    yield return new object[] { new string(controlChar, 1) };

                for (var controlChar = (char)0x007F; controlChar <= 0x009F; controlChar++)
                    yield return new object[] { new string(controlChar, 1) };
            }
        }

        public static IEnumerable<object[]> InvalidStringData
        {
            get
            {
                yield return new object[] { new string('t', 1 << 15 + 1) };
            }
        }

        public static IEnumerable<object[]> InvalidBinaryData
        {
            get
            {
                yield return new object[] { new byte[1 << 16 + 1] };
            }
        }

        public static IEnumerable<object[]> InvalidDateTimeData
        {
            get
            {
                yield return new object[] { new DateTime() };
                yield return new object[] { new DateTime(1601, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(-1) };
            }
        }
    }
}