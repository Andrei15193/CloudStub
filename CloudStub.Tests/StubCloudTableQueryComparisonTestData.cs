using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;

namespace CloudStub.Tests
{
    public class StubCloudTableQueryComparisonTestData : IEnumerable<object[]>
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
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, 2, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, 3, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, 4, false);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, 2, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, 3, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, 4, true);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, 2, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, 3, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, 4, true);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, 2, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, 4, true);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, 2, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, 3, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, 4, false);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, 2, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, 4, false);
            }
        }

        private static IEnumerable<object[]> Int64MatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, 2L, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, 4L, false);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, 2L, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, 4L, true);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, 2L, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, 4L, true);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, 2L, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, 4L, true);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, 2L, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, 4L, false);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, 2L, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, 4L, false);
            }
        }

        private static IEnumerable<object[]> DoubleMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, 2D, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, 4D, false);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, 2D, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, 4D, true);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, 2D, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, 4D, true);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, 2D, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, 4D, true);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, 2D, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, 4D, false);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, 2D, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, 4D, false);
            }
        }

        private static IEnumerable<object[]> BoolMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.Equal, true, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.Equal, false, false);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.NotEqual, true, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.NotEqual, false, true);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThan, true, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThan, false, false);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThanOrEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThanOrEqual, false, false);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThan, true, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThan, false, true);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThanOrEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThanOrEqual, false, true);
            }
        }

        private static IEnumerable<object[]> DateTimeMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetime-2019-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetime-2021-01-22T00:00:00Z", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetime-2019-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetime-2021-01-22T00:00:00Z", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetime-2019-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetime-2021-01-22T00:00:00Z", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetime-2019-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetime-2021-01-22T00:00:00Z", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetime-2019-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetime-2021-01-22T00:00:00Z", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetime-2019-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetime-2021-01-22T00:00:00Z", false);
            }
        }

        private static IEnumerable<object[]> DateTimeOffsetMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetimeoffset-2019-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetimeoffset-2021-01-22T00:00:00Z", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetimeoffset-2019-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetimeoffset-2021-01-22T00:00:00Z", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetimeoffset-2019-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetimeoffset-2021-01-22T00:00:00Z", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetimeoffset-2019-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetimeoffset-2021-01-22T00:00:00Z", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetimeoffset-2019-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetimeoffset-2021-01-22T00:00:00Z", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetimeoffset-2019-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetimeoffset-2021-01-22T00:00:00Z", false);
            }
        }

        private static IEnumerable<object[]> GuidMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, "guid-58260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, "guid-78260b3f-beab-45e2-b900-33e2b442e724", false);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, "guid-58260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, "guid-78260b3f-beab-45e2-b900-33e2b442e724", true);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, "guid-58260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, "guid-78260b3f-beab-45e2-b900-33e2b442e724", true);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, "guid-58260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, "guid-78260b3f-beab-45e2-b900-33e2b442e724", true);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, "guid-58260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, "guid-78260b3f-beab-45e2-b900-33e2b442e724", false);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, "guid-58260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, "guid-78260b3f-beab-45e2-b900-33e2b442e724", false);
            }
        }

        private static IEnumerable<object[]> BinaryMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.Equal, $"binary-{ToBase64(2)}", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.Equal, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.Equal, $"binary-{ToBase64(4)}", false);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.NotEqual, $"binary-{ToBase64(2)}", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.NotEqual, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.NotEqual, $"binary-{ToBase64(3, 1)}", true);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThan, $"binary-{ToBase64(2)}", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThan, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThan, $"binary-{ToBase64(3, 1)}", true);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThanOrEqual, $"binary-{ToBase64(2)}", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThanOrEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThanOrEqual, $"binary-{ToBase64(3, 1)}", true);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThan, $"binary-{ToBase64(2)}", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThan, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThan, $"binary-{ToBase64(3, 1)}", false);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThanOrEqual, $"binary-{ToBase64(2)}", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThanOrEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThanOrEqual, $"binary-{ToBase64(3, 1)}", false);
            }
        }

        private static IEnumerable<object[]> StringMatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.Equal, "B", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.Equal, "b", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.Equal, "bB", false);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.NotEqual, "B", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.NotEqual, "b", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.NotEqual, "bB", true);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.LessThan, "B", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.LessThan, "b", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.LessThan, "bB", true);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.LessThanOrEqual, "B", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.LessThanOrEqual, "b", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.LessThanOrEqual, "bB", true);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.GreaterThan, "B", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.GreaterThan, "b", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.GreaterThan, "bB", false);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.GreaterThanOrEqual, "B", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.GreaterThanOrEqual, "b", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.GreaterThanOrEqual, "bB", false);
            }
        }

        private static IEnumerable<object[]> Int32MismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, true, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, "3", false);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, true, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, "3", true);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, true, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, "3", false);

                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, true, false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, "3", false);
            }
        }

        private static IEnumerable<object[]> Int64MismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, 3, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, true, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, "3", false);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, 3, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, 3, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, true, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, "3", true);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, 3, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, true, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, "3", false);

                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, true, false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, "3", false);
            }
        }

        private static IEnumerable<object[]> DoubleMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, 3, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, true, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, "3", false);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, 3, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, 3, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, true, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, "3", true);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, 3, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, true, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, "3", false);

                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, true, false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, "3", false);
            }
        }

        private static IEnumerable<object[]> BoolMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.Equal, 3, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.Equal, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.Equal, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.Equal, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.Equal, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.Equal, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.Equal, "3", false);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.NotEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.NotEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.NotEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.NotEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.NotEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.NotEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.NotEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThan, 3, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThan, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThan, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThan, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThan, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThan, "3", true);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThanOrEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThanOrEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThanOrEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThanOrEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThanOrEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThan, 3, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThan, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThan, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThan, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThan, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThan, "3", false);

                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThanOrEqual, 3, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThanOrEqual, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThanOrEqual, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThanOrEqual, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThanOrEqual, "3", false);
            }
        }

        private static IEnumerable<object[]> DateTimeMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, 3, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, true, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, "3", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, 3, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, true, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "3", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, 3, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, true, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "3", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, 3, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, true, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "3", false);
            }
        }

        private static IEnumerable<object[]> DateTimeOffsetMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.Equal, 3, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.Equal, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.Equal, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.Equal, true, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.Equal, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.Equal, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.Equal, "3", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThan, 3, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThan, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThan, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThan, true, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThan, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "3", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, 3, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, true, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "3", false);

                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, 3, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, true, false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetimeoffset-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "3", false);
            }
        }

        private static IEnumerable<object[]> GuidMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, 3, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, true, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, "3", false);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, 3, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, true, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, "3", true);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, $"binary-{ToBase64(3)}", true);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, 3, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, true, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, "3", false);

                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, 3, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, true, false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, $"binary-{ToBase64(3)}", false);
                yield return new TestData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, "3", false);
            }
        }

        private static IEnumerable<object[]> BinaryMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.Equal, 3, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.Equal, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.Equal, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.Equal, true, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.Equal, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.Equal, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.Equal, "3", false);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.NotEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.NotEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.NotEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.NotEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.NotEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.NotEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.NotEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThan, 3, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThan, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThan, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThan, true, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThan, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThan, "3", true);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThanOrEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThanOrEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThanOrEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThanOrEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.LessThanOrEqual, "3", true);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThan, 3, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThan, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThan, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThan, true, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThan, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThan, "3", false);

                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThanOrEqual, 3, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThanOrEqual, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThanOrEqual, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThanOrEqual, true, false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), $"binary-{ToBase64(3)}", QueryComparisons.GreaterThanOrEqual, "3", false);
            }
        }

        private static IEnumerable<object[]> StringMismatchingTypeComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.Equal, 3, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.Equal, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.Equal, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.Equal, true, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.Equal, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.Equal, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.Equal, $"binary-{ToBase64(3)}", false);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.NotEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.NotEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.NotEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.NotEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.NotEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.NotEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.NotEqual, $"binary-{ToBase64(3)}", true);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThan, 3, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThan, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThan, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThan, true, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThan, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThan, $"binary-{ToBase64(3)}", true);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThanOrEqual, 3, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThanOrEqual, 3L, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThanOrEqual, 3D, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThanOrEqual, true, true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.LessThanOrEqual, $"binary-{ToBase64(3)}", true);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThan, 3, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThan, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThan, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThan, true, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThan, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThan, $"binary-{ToBase64(3)}", false);

                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThanOrEqual, 3, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThanOrEqual, 3L, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThanOrEqual, 3D, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThanOrEqual, true, false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThanOrEqual, "datetimeoffset-2020-01-22T00:00:00Z", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false);
                yield return new TestData(nameof(TestQueryEntity.StringProp), "3", QueryComparisons.GreaterThanOrEqual, $"binary-{ToBase64(3)}", false);
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
            {
                _data = new[] { propertyName, propertyValue, filterOperator, filterValue, returnsEntity };
            }
        }
    }
}