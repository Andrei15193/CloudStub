using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace CloudStub.AzureDataTables.Tests.Data
{
    public class TableDeserializationTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
            => Int32MismatchingTypeDeserializationTestData
            .Concat(Int64MismatchingTypeDeserializationTestData)
            .Concat(DoubleMismatchingTypeDeserializationTestData)
            .Concat(BoolMismatchingTypeDeserializationTestData)
            .Concat(DateTimeMismatchingTypeDeserializationTestData)
            .Concat(DateTimeOffsetMismatchingTypeDeserializationTestData)
            .Concat(GuidMismatchingTypeDeserializationTestData)
            .Concat(BinaryMismatchingTypeDeserializationTestData)
            .Concat(StringMismatchingTypeDeserializationTestData)
            .GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();

        private static IEnumerable<object[]> Int32MismatchingTypeDeserializationTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, 3);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3L, new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3F, 3);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3.1F, new InvalidCastException("Unable to cast object of type 'System.Double' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3D, new InvalidCastException("Unable to cast object of type 'System.Double' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3.1D, new InvalidCastException("Unable to cast object of type 'System.Double' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3M, 3);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), (decimal)int.MaxValue + 1, new InvalidCastException("Unable to cast object of type 'System.Double' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3.1M, new InvalidCastException("Unable to cast object of type 'System.Double' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), "", new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), "3", new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), true, new InvalidCastException("Unable to cast object of type 'System.Boolean' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), false, new InvalidCastException("Unable to cast object of type 'System.Boolean' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), DateTime.UtcNow, new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), DateTimeOffset.UtcNow, new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), Guid.NewGuid(), new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Int32]'."));
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), new byte[] { 1, 2, 3 }, new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Int32]'."));
            }
        }

        private static IEnumerable<object[]> Int64MismatchingTypeDeserializationTestData
        {
            get
            {
                var now = DateTime.UtcNow;
                var nowOffset = DateTimeOffset.UtcNow;
                var guid = Guid.NewGuid();

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, 3L);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3F, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3.1F, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3D, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3.1D, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3M, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), (decimal)int.MaxValue + 1, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), (decimal)long.MaxValue + 1, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3.1M, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), "", new FormatException("The input string '' was not in a correct format."));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), "3", 3L);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), "3.0", new FormatException("The input string '3.0' was not in a correct format."));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), "-3", -3L);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), " 3", 3L);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), "3 ", 3L);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), " 3 ", 3L);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), true, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), false, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), now, new FormatException($"The input string '{now:yyyy-MM-ddTHH:mm:ss.FFFFFFFZ}' was not in a correct format."));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), nowOffset, new FormatException($"The input string '{nowOffset:yyyy-MM-ddTHH:mm:ss.FFFFFFFZ}' was not in a correct format."));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), guid, new FormatException($"The input string '{guid}' was not in a correct format."));
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), new byte[] { 1, 2, 3 }, new FormatException("The input string 'AQID' was not in a correct format."));
            }
        }

        private static IEnumerable<object[]> DoubleMismatchingTypeDeserializationTestData
        {
            get
            {
                var now = DateTime.UtcNow;
                var nowOffset = DateTimeOffset.UtcNow;
                var guid = Guid.NewGuid();

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3, 3D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3L, 3D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3F, 3D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3.1F, 3.1D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, 3D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3.1D, 3.1D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3M, 3D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), (decimal)int.MaxValue + 1, (double)int.MaxValue + 1);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), (decimal)long.MaxValue + 1, (double)long.MaxValue + 1);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3.1M, 3.1D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), "", new FormatException("The input string '' was not in a correct format."));
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), "3", 3D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), "3.0", 3D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), "3.1", 3.1D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), "-3", -3D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), " 3", 3D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), "3 ", 3D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), " 3 ", 3D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), true, 1D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), false, 0D);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), now, new FormatException($"The input string '{now:yyyy-MM-ddTHH:mm:ss.FFFFFFFZ}' was not in a correct format."));
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), nowOffset, new FormatException($"The input string '{nowOffset:yyyy-MM-ddTHH:mm:ss.FFFFFFFZ}' was not in a correct format."));
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), guid, new FormatException($"The input string '{guid}' was not in a correct format."));
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), new byte[] { 1, 2, 3 }, new FormatException("The input string 'AQID' was not in a correct format."));
            }
        }

        private static IEnumerable<object[]> BoolMismatchingTypeDeserializationTestData
        {
            get
            {
                var now = DateTime.UtcNow;
                var nowOffset = DateTimeOffset.UtcNow;
                var guid = Guid.NewGuid();

                yield return new TestData(nameof(TestQueryEntity.BoolProp), 3, new InvalidCastException("Unable to cast object of type 'System.Int32' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), 3L, new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), 3F, new InvalidCastException("Unable to cast object of type 'System.Int32' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), 3.1F, new InvalidCastException("Unable to cast object of type 'System.Double' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), 3D, new InvalidCastException("Unable to cast object of type 'System.Double' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), 3.1D, new InvalidCastException("Unable to cast object of type 'System.Double' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), 3M, new InvalidCastException("Unable to cast object of type 'System.Int32' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), 3.1M, new InvalidCastException("Unable to cast object of type 'System.Double' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), "", new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), "true", new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), "false", new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), false, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), now, new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), nowOffset, new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), guid, new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Boolean]'."));
                yield return new TestData(nameof(TestQueryEntity.BoolProp), new byte[] { 1, 2, 3 }, new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Boolean]'."));
            }
        }

        private static IEnumerable<object[]> DateTimeMismatchingTypeDeserializationTestData
        {
            get
            {
                var now = DateTime.UtcNow;
                var nowOffset = DateTimeOffset.UtcNow;
                var guid = new Guid("01234567-89ab-cdef-0123-456789abcdef");

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), 3, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), 3L, new FormatException("String '3' was not recognized as a valid DateTime."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), 3F, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), 3.1F, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), 3D, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), 3.1D, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), 3M, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), 3.1M, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "", new FormatException("String '' was not recognized as a valid DateTime."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "123", new FormatException("String '123' was not recognized as a valid DateTime."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "123T", new FormatException("String '123T' was not recognized as a valid DateTime."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "123Z", new FormatException("String '123Z' was not recognized as a valid DateTime."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "123-", new FormatException("String '123-' was not recognized as a valid DateTime."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "123A", new FormatException("The string '123A' was not recognized as a valid DateTime. There is an unknown word starting at index '3'."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "2026-03-31", new DateTime(2026, 3, 31, 0, 0, 0, DateTimeKind.Local));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), true, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), false, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), now, (DateTime?)now);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), nowOffset, (DateTime?)nowOffset.DateTime);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), guid, new FormatException($"The string '{guid}' was not recognized as a valid DateTime. There is an unknown word starting at index '11'."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), new byte[] { 1, 2, 3 }, new FormatException("The string 'AQID' was not recognized as a valid DateTime. There is an unknown word starting at index '0'."));
            }
        }

        private static IEnumerable<object[]> DateTimeOffsetMismatchingTypeDeserializationTestData
        {
            get
            {
                var now = DateTime.UtcNow;
                var nowOffset = DateTimeOffset.UtcNow;
                var guid = new Guid("01234567-89ab-cdef-0123-456789abcdef");

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), 3, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), 3L, new FormatException("String '3' was not recognized as a valid DateTime."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), 3F, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), 3.1F, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), 3D, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), 3.1D, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), 3M, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), 3.1M, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "", new FormatException("String '' was not recognized as a valid DateTime."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "123", new FormatException("String '123' was not recognized as a valid DateTime."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "123T", new FormatException("String '123T' was not recognized as a valid DateTime."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "123Z", new FormatException("String '123Z' was not recognized as a valid DateTime."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "123-", new FormatException("String '123-' was not recognized as a valid DateTime."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "123A", new FormatException("The string '123A' was not recognized as a valid DateTime. There is an unknown word starting at index '3'."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "2026-03-31", (DateTimeOffset)new DateTime(2026, 3, 31, 0, 0, 0, DateTimeKind.Local));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), true, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), false, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), now, (DateTimeOffset?)now);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), nowOffset, (DateTimeOffset?)nowOffset);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), guid, new FormatException($"The string '{guid}' was not recognized as a valid DateTime. There is an unknown word starting at index '11'."));
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), new byte[] { 1, 2, 3 }, new FormatException("The string 'AQID' was not recognized as a valid DateTime. There is an unknown word starting at index '0'."));
            }
        }

        private static IEnumerable<object[]> GuidMismatchingTypeDeserializationTestData
        {
            get
            {
                var now = DateTime.UtcNow;
                var nowOffset = DateTimeOffset.UtcNow;
                var guid = Guid.NewGuid();

                yield return new TestData(nameof(TestQueryEntity.GuidProp), 3, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), 3L, new FormatException("Unrecognized Guid format."));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), 3F, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), 3.1F, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), 3D, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), 3.1D, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), 3M, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), 3.1M, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "", new FormatException("Unrecognized Guid format."));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), true, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), false, new ArgumentNullException("input"));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), now, new FormatException("Unrecognized Guid format."));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), nowOffset, new FormatException("Unrecognized Guid format."));
                yield return new TestData(nameof(TestQueryEntity.GuidProp), guid, guid);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), new byte[] { 1, 2, 3 }, new FormatException("Unrecognized Guid format."));
            }
        }

        private static IEnumerable<object[]> BinaryMismatchingTypeDeserializationTestData
        {
            get
            {
                var now = DateTime.UtcNow;
                var nowOffset = DateTimeOffset.UtcNow;
                var guid = new Guid("01234567-89ab-cdef-0123-456789abcdef");

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), 3, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), 3L, new FormatException("The input is not a valid Base-64 string as it contains a non-base 64 character, more than two padding characters, or an illegal character among the padding characters."));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), 3F, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), 3F, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), 3.1F, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), 3D, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), 3.1D, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), 3M, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), 3.1M, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), "", new byte[0]);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), "123", new FormatException("The input is not a valid Base-64 string as it contains a non-base 64 character, more than two padding characters, or an illegal character among the padding characters."));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), true, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), false, new ArgumentNullException("s"));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), now, new FormatException("The input is not a valid Base-64 string as it contains a non-base 64 character, more than two padding characters, or an illegal character among the padding characters."));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), nowOffset, new FormatException("The input is not a valid Base-64 string as it contains a non-base 64 character, more than two padding characters, or an illegal character among the padding characters."));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), guid, new FormatException("The input is not a valid Base-64 string as it contains a non-base 64 character, more than two padding characters, or an illegal character among the padding characters."));
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 });
            }
        }

        private static IEnumerable<object[]> StringMismatchingTypeDeserializationTestData
        {
            get
            {
                var now = DateTime.UtcNow;
                var nowOffset = DateTimeOffset.UtcNow;
                var guid = new Guid("01234567-89ab-cdef-0123-456789abcdef");

                yield return new TestData(nameof(TestQueryEntity.StringProp), 3, null);
                yield return new TestData(nameof(TestQueryEntity.StringProp), 3L, "3");
                yield return new TestData(nameof(TestQueryEntity.StringProp), 3F, null);
                yield return new TestData(nameof(TestQueryEntity.StringProp), 3.1F, null);
                yield return new TestData(nameof(TestQueryEntity.StringProp), 3D, null);
                yield return new TestData(nameof(TestQueryEntity.StringProp), 3.1D, null);
                yield return new TestData(nameof(TestQueryEntity.StringProp), 3M, null);
                yield return new TestData(nameof(TestQueryEntity.StringProp), 3.1M, null);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "", "");
                yield return new TestData(nameof(TestQueryEntity.StringProp), "123", "123");
                yield return new TestData(nameof(TestQueryEntity.StringProp), true, null);
                yield return new TestData(nameof(TestQueryEntity.StringProp), false, null);
                yield return new TestData(nameof(TestQueryEntity.StringProp), now, now.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFFZ"));
                yield return new TestData(nameof(TestQueryEntity.StringProp), nowOffset, nowOffset.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFFZ"));
                yield return new TestData(nameof(TestQueryEntity.StringProp), guid, guid.ToString("D"));
                yield return new TestData(nameof(TestQueryEntity.StringProp), new byte[] { 1, 2, 3 }, "AQID");
            }
        }

        private readonly struct TestData
        {
            private readonly object[] _data;

            public static implicit operator object[](TestData testData)
                => testData._data;

            public TestData(string propertyName, object propertyValue, object expectedResult)
                => _data = new[] { propertyName, propertyValue, expectedResult };
        }
    }
}