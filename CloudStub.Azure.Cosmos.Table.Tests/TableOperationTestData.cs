using System;
using System.Collections.Generic;
using System.Linq;

namespace CloudStub.Azure.Cosmos.Table.Tests
{
    public static class TableOperationTestData
    {
        public static IEnumerable<object[]> InvalidKeyTestData
        {
            get
            {
                var reservedCharacters = new[] { '#', '?', '\t', '\n', '\r', '/', '\\' };
                foreach (var reservedCharacter in reservedCharacters)
                    yield return new object[] { new string(reservedCharacter, 1) };

                for (var controlChar = (char)0x0000; controlChar <= 0x001F; controlChar++)
                    if (!reservedCharacters.Contains(controlChar))
                        yield return new object[] { new string(controlChar, 1) };

                for (var controlChar = (char)0x007F; controlChar <= 0x009F; controlChar++)
                    if (!reservedCharacters.Contains(controlChar))
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