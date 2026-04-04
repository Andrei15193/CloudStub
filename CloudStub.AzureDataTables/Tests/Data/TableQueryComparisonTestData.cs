using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace CloudStub.AzureDataTables.Tests.Data
{
    public class TableQueryComparisonTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
            => MatchingTypeComparisonTestData
            .Concat(MismatchingTypeComparisonTestData)
            .GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();

        private static IEnumerable<object[]> MatchingTypeComparisonTestData
            => Int32MatchingTypeComparisonTestData
            .Concat(Int64MatchingTypeComparisonTestData)
            .Concat(DoubleMatchingTypeComparisonTestData)
            .Concat(BoolMatchingTypeComparisonTestData)
            .Concat(DateTimeMatchingTypeComparisonTestData)
            .Concat(DateTimeOffsetMatchingTypeComparisonTestData)
            .Concat(GuidMatchingTypeComparisonTestData)
            .Concat(BinaryMatchingTypeComparisonTestData)
            .Concat(StringMatchingTypeComparisonTestData);

        private static IEnumerable<object[]> MismatchingTypeComparisonTestData
            => Int32MismatchingTypeComparisonTestData
            .Concat(Int64MismatchingTypeComparisonTestData)
            .Concat(DoubleMismatchingTypeComparisonTestData)
            .Concat(BoolMismatchingTypeComparisonTestData)
            .Concat(DateTimeMismatchingTypeComparisonTestData)
            .Concat(DateTimeOffsetMismatchingTypeComparisonTestData)
            .Concat(GuidMismatchingTypeComparisonTestData)
            .Concat(BinaryMismatchingTypeComparisonTestData)
            .Concat(StringMismatchingTypeComparisonTestData);

        private static IEnumerable<object[]> Int32MatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "eq", 2, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "eq", 3, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "eq", 4, false);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ne", 2, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ne", 3, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ne", 4, true);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "lt", 2, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "lt", 3, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "lt", 4, true);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "le", 2, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "le", 3, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "le", 4, true);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "gt", 2, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "gt", 3, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "gt", 4, false);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ge", 2, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ge", 3, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ge", 4, false);
            }
        }

        private static IEnumerable<object[]> Int64MatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "eq", 2L, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "eq", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "eq", 4L, false);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ne", 2L, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ne", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ne", 4L, true);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "lt", 2L, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "lt", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "lt", 4L, true);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "le", 2L, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "le", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "le", 4L, true);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "gt", 2L, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "gt", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "gt", 4L, false);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ge", 2L, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ge", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ge", 4L, false);
            }
        }

        private static IEnumerable<object[]> DoubleMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "eq", 2D, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "eq", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "eq", 4D, false);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ne", 2D, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ne", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ne", 4D, true);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "lt", 2D, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "lt", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "lt", 4D, true);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "le", 2D, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "le", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "le", 4D, true);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "gt", 2D, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "gt", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "gt", 4D, false);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ge", 2D, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ge", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ge", 4D, false);
            }
        }

        private static IEnumerable<object[]> BoolMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "eq", true, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "eq", false, false);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ne", true, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ne", false, true);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "lt", true, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "lt", false, false);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "le", true, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "le", false, false);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "gt", true, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "gt", false, true);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ge", true, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ge", false, true);
            }
        }

        private static IEnumerable<object[]> DateTimeMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "eq", "datetime-2019-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "eq", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "eq", "datetime-2021-01-22T00:00:00Z", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ne", "datetime-2019-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ne", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ne", "datetime-2021-01-22T00:00:00Z", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "lt", "datetime-2019-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "lt", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "lt", "datetime-2021-01-22T00:00:00Z", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "le", "datetime-2019-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "le", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "le", "datetime-2021-01-22T00:00:00Z", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "gt", "datetime-2019-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "gt", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "gt", "datetime-2021-01-22T00:00:00Z", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ge", "datetime-2019-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ge", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ge", "datetime-2021-01-22T00:00:00Z", false);
            }
        }

        private static IEnumerable<object[]> DateTimeOffsetMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "eq", "datetimeoffset-2019-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "eq", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "eq", "datetimeoffset-2021-01-22T00:00:00Z", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ne", "datetimeoffset-2019-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ne", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ne", "datetimeoffset-2021-01-22T00:00:00Z", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "lt", "datetimeoffset-2019-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "lt", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "lt", "datetimeoffset-2021-01-22T00:00:00Z", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "le", "datetimeoffset-2019-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "le", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "le", "datetimeoffset-2021-01-22T00:00:00Z", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "gt", "datetimeoffset-2019-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "gt", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "gt", "datetimeoffset-2021-01-22T00:00:00Z", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ge", "datetimeoffset-2019-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ge", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ge", "datetimeoffset-2021-01-22T00:00:00Z", false);
            }
        }

        private static IEnumerable<object[]> GuidMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "eq", "guid-58260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "eq", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "eq", "guid-78260b3f-beab-45e2-b900-33e2b442e724", false);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ne", "guid-58260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ne", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ne", "guid-78260b3f-beab-45e2-b900-33e2b442e724", true);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "lt", "guid-58260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "lt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "lt", "guid-78260b3f-beab-45e2-b900-33e2b442e724", true);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "le", "guid-58260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "le", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "le", "guid-78260b3f-beab-45e2-b900-33e2b442e724", true);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "gt", "guid-58260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "gt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "gt", "guid-78260b3f-beab-45e2-b900-33e2b442e724", false);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ge", "guid-58260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ge", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ge", "guid-78260b3f-beab-45e2-b900-33e2b442e724", false);
            }
        }

        private static IEnumerable<object[]> BinaryMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "eq", $"binary-{ToBase64(2)}", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "eq", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "eq", $"binary-{ToBase64(4)}", false);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ne", $"binary-{ToBase64(2)}", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ne", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ne", $"binary-{ToBase64(3, 1)}", true);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "lt", $"binary-{ToBase64(2)}", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "lt", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "lt", $"binary-{ToBase64(3, 1)}", true);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "le", $"binary-{ToBase64(2)}", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "le", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "le", $"binary-{ToBase64(3, 1)}", true);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "gt", $"binary-{ToBase64(2)}", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "gt", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "gt", $"binary-{ToBase64(3, 1)}", false);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ge", $"binary-{ToBase64(2)}", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ge", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ge", $"binary-{ToBase64(3, 1)}", false);
            }
        }

        private static IEnumerable<object[]> StringMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "eq", "B", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "eq", "b", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "eq", "bB", false);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "ne", "B", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "ne", "b", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "ne", "bB", true);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "lt", "B", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "lt", "b", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "lt", "bB", true);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "le", "B", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "le", "b", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "le", "bB", true);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "gt", "B", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "gt", "b", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "gt", "bB", false);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "ge", "B", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "ge", "b", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", "ge", "bB", false);
            }
        }

        private static IEnumerable<object[]> Int32MismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "eq", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "eq", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "eq", true, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "eq", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "eq", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "eq", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "eq", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "eq", "3", false);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ne", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ne", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ne", true, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ne", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ne", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ne", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ne", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ne", "3", true);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "lt", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "lt", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "lt", true, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "lt", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "lt", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "lt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "lt", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "lt", "3", true);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "le", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "le", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "le", true, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "le", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "le", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "le", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "le", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "le", "3", true);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "gt", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "gt", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "gt", true, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "gt", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "gt", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "gt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "gt", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "gt", "3", false);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ge", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ge", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ge", true, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ge", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ge", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ge", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ge", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ge", "3", false);
            }
        }

        private static IEnumerable<object[]> Int64MismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "eq", 3, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "eq", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "eq", true, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "eq", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "eq", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "eq", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "eq", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "eq", "3", false);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ne", 3, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ne", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ne", true, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ne", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ne", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ne", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ne", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ne", "3", true);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "lt", 3, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "lt", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "lt", true, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "lt", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "lt", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "lt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "lt", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "lt", "3", true);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "le", 3, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "le", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "le", true, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "le", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "le", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "le", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "le", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "le", "3", true);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "gt", 3, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "gt", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "gt", true, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "gt", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "gt", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "gt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "gt", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "gt", "3", false);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ge", 3, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ge", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ge", true, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ge", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ge", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ge", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ge", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ge", "3", false);
            }
        }

        private static IEnumerable<object[]> DoubleMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "eq", 3, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "eq", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "eq", true, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "eq", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "eq", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "eq", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "eq", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "eq", "3", false);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ne", 3, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ne", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ne", true, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ne", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ne", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ne", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ne", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ne", "3", true);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "lt", 3, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "lt", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "lt", true, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "lt", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "lt", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "lt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "lt", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "lt", "3", true);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "le", 3, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "le", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "le", true, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "le", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "le", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "le", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "le", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "le", "3", true);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "gt", 3, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "gt", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "gt", true, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "gt", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "gt", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "gt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "gt", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "gt", "3", false);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ge", 3, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ge", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ge", true, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ge", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ge", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ge", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ge", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ge", "3", false);
            }
        }

        private static IEnumerable<object[]> BoolMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "eq", 3, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "eq", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "eq", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "eq", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "eq", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "eq", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "eq", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "eq", "3", false);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ne", 3, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ne", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ne", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ne", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ne", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ne", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ne", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ne", "3", true);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "lt", 3, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "lt", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "lt", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "lt", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "lt", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "lt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "lt", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "lt", "3", true);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "le", 3, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "le", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "le", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "le", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "le", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "le", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "le", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "le", "3", true);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "gt", 3, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "gt", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "gt", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "gt", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "gt", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "gt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "gt", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "gt", "3", false);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ge", 3, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ge", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ge", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ge", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ge", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ge", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ge", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ge", "3", false);
            }
        }

        private static IEnumerable<object[]> DateTimeMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "eq", 3, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "eq", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "eq", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "eq", true, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "eq", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "eq", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "eq", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "eq", "3", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ne", 3, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ne", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ne", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ne", true, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ne", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ne", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ne", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ne", "3", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "lt", 3, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "lt", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "lt", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "lt", true, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "lt", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "lt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "lt", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "lt", "3", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "le", 3, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "le", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "le", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "le", true, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "le", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "le", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "le", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "le", "3", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "gt", 3, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "gt", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "gt", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "gt", true, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "gt", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "gt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "gt", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "gt", "3", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ge", 3, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ge", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ge", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ge", true, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ge", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ge", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ge", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", "ge", "3", false);
            }
        }

        private static IEnumerable<object[]> DateTimeOffsetMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "eq", 3, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "eq", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "eq", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "eq", true, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "eq", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "eq", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "eq", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "eq", "3", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ne", 3, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ne", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ne", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ne", true, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ne", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ne", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ne", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ne", "3", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "lt", 3, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "lt", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "lt", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "lt", true, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "lt", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "lt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "lt", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "lt", "3", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "le", 3, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "le", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "le", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "le", true, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "le", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "le", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "le", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "le", "3", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "gt", 3, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "gt", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "gt", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "gt", true, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "gt", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "gt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "gt", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "gt", "3", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ge", 3, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ge", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ge", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ge", true, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ge", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ge", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ge", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", "ge", "3", false);
            }
        }

        private static IEnumerable<object[]> GuidMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "eq", 3, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "eq", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "eq", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "eq", true, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "eq", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "eq", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "eq", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "eq", "3", false);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ne", 3, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ne", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ne", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ne", true, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ne", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ne", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ne", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ne", "3", true);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "lt", 3, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "lt", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "lt", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "lt", true, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "lt", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "lt", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "lt", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "lt", "3", true);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "le", 3, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "le", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "le", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "le", true, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "le", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "le", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "le", $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "le", "3", true);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "gt", 3, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "gt", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "gt", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "gt", true, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "gt", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "gt", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "gt", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "gt", "3", false);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ge", 3, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ge", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ge", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ge", true, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ge", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ge", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ge", $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", "ge", "3", false);
            }
        }

        private static IEnumerable<object[]> BinaryMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "eq", 3, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "eq", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "eq", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "eq", true, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "eq", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "eq", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "eq", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "eq", "3", false);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ne", 3, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ne", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ne", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ne", true, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ne", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ne", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ne", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ne", "3", true);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "lt", 3, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "lt", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "lt", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "lt", true, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "lt", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "lt", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "lt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "lt", "3", true);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "le", 3, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "le", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "le", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "le", true, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "le", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "le", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "le", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "le", "3", true);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "gt", 3, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "gt", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "gt", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "gt", true, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "gt", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "gt", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "gt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "gt", "3", false);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ge", 3, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ge", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ge", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ge", true, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ge", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ge", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ge", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", "ge", "3", false);
            }
        }

        private static IEnumerable<object[]> StringMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "eq", 3, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "eq", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "eq", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "eq", true, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "eq", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "eq", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "eq", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "eq", $"binary-{ToBase64(3)}", false);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ne", 3, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ne", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ne", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ne", true, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ne", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ne", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ne", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ne", $"binary-{ToBase64(3)}", true);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "lt", 3, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "lt", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "lt", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "lt", true, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "lt", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "lt", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "lt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "lt", $"binary-{ToBase64(3)}", true);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "le", 3, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "le", 3L, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "le", 3D, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "le", true, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "le", "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "le", "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "le", "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "le", $"binary-{ToBase64(3)}", true);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "gt", 3, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "gt", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "gt", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "gt", true, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "gt", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "gt", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "gt", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "gt", $"binary-{ToBase64(3)}", false);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ge", 3, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ge", 3L, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ge", 3D, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ge", true, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ge", "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ge", "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ge", "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", "ge", $"binary-{ToBase64(3)}", false);
            }
        }

        private static string ToBase64(params byte[] bytes)
            => Convert.ToBase64String(bytes);

        private sealed class TestData
        {
            private readonly object[] _data;

            public static implicit operator object[](TestData testData)
                => testData._data;

            public TestData(string propertyName, object propertyValue, string filterOperator, object filterValue, bool returnsEntity)
                => _data = new[] { propertyName, propertyValue, filterOperator, filterValue, returnsEntity };
        }
    }
}