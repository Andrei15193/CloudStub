
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace CloudStub.AzureDataTables.Tests.Data
{
    public class TableQueryNullComparisonTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
            => EqualsComparisonTestData
            .Concat(NotEqualsComparisonTestData)
            .Concat(LessThanComparisonTestData)
            .Concat(LessThanOrEqualComparisonTestData)
            .Concat(GreaterThanComparisonTestData)
            .Concat(GreaterThanOrEqualComparisonTestData)
            .GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();

        private static IEnumerable<object[]> EqualsComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "eq", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "eq", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "eq", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "eq", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), DateTime.UtcNow, "eq", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), DateTimeOffset.UtcNow, "eq", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.GuidProp), Guid.NewGuid(), "eq", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), new byte[] { 1, 2, 3 }, "eq", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.StringProp), "test", "eq", "One of the request inputs is not valid.");
            }
        }

        private static IEnumerable<object[]> NotEqualsComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ne", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ne", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ne", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ne", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), DateTime.UtcNow, "ne", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), DateTimeOffset.UtcNow, "ne", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.GuidProp), Guid.NewGuid(), "ne", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), new byte[] { 1, 2, 3 }, "ne", "One of the request inputs is not valid.");
                yield return new TestData(nameof(TestQueryEntity.StringProp), "test", "ne", "One of the request inputs is not valid.");
            }
        }

        private static IEnumerable<object[]> LessThanComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "lt", "The operator 'LessThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "lt", "The operator 'LessThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "lt", "The operator 'LessThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "lt", "The operator 'LessThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), DateTime.UtcNow, "lt", "The operator 'LessThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), DateTimeOffset.UtcNow, "lt", "The operator 'LessThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.GuidProp), Guid.NewGuid(), "lt", "The operator 'LessThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), new byte[] { 1, 2, 3 }, "lt", "The operator 'LessThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.StringProp), "test", "lt", "The operator 'LessThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
            }
        }

        private static IEnumerable<object[]> LessThanOrEqualComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "le", "The operator 'LessThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "le", "The operator 'LessThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "le", "The operator 'LessThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "le", "The operator 'LessThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), DateTime.UtcNow, "le", "The operator 'LessThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), DateTimeOffset.UtcNow, "le", "The operator 'LessThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.GuidProp), Guid.NewGuid(), "le", "The operator 'LessThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), new byte[] { 1, 2, 3 }, "le", "The operator 'LessThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.StringProp), "test", "le", "The operator 'LessThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
            }
        }

        private static IEnumerable<object[]> GreaterThanComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "gt", "The operator 'GreaterThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "gt", "The operator 'GreaterThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "gt", "The operator 'GreaterThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "gt", "The operator 'GreaterThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), DateTime.UtcNow, "gt", "The operator 'GreaterThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), DateTimeOffset.UtcNow, "gt", "The operator 'GreaterThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.GuidProp), Guid.NewGuid(), "gt", "The operator 'GreaterThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), new byte[] { 1, 2, 3 }, "gt", "The operator 'GreaterThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.StringProp), "test", "gt", "The operator 'GreaterThan' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
            }
        }

        private static IEnumerable<object[]> GreaterThanOrEqualComparisonTestData
        {
            get
            {
                yield return new TestData(nameof(TestQueryEntity.Int32Prop), 3, "ge", "The operator 'GreaterThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.Int64Prop), 3L, "ge", "The operator 'GreaterThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.DoubleProp), 3D, "ge", "The operator 'GreaterThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.BoolProp), true, "ge", "The operator 'GreaterThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.DateTimeProp), DateTime.UtcNow, "ge", "The operator 'GreaterThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.DateTimeOffsetProp), DateTimeOffset.UtcNow, "ge", "The operator 'GreaterThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.GuidProp), Guid.NewGuid(), "ge", "The operator 'GreaterThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.BinaryProp), new byte[] { 1, 2, 3 }, "ge", "The operator 'GreaterThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
                yield return new TestData(nameof(TestQueryEntity.StringProp), "test", "ge", "The operator 'GreaterThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.", new[] { "Cache-Control" });
            }
        }

        private sealed class TestData
        {
            private readonly object[] _data;

            public static implicit operator object[](TestData testData)
                => testData._data;

            public TestData(string propertyName, object propertyValue, string filterOperator, string expectedErrorMessage)
                : this(propertyName, propertyValue, filterOperator, expectedErrorMessage, Array.Empty<string>())
            {
            }

            public TestData(string propertyName, object propertyValue, string filterOperator, string expectedErrorMessage, IEnumerable<string> removedHeaders)
                => _data = new[] { propertyName, propertyValue, filterOperator, expectedErrorMessage, removedHeaders };
        }
    }
}