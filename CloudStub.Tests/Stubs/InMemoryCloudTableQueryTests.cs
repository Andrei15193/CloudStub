using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace CloudStub.Tests
{
    public class InMemoryCloudTableQueryTests : InMemoryCloudTableTests
    {
        [Fact]
        public async Task ExecuteQuerySegmentedAsync_WhenThereAreNoFilters_ReturnsAllItems()
        {
            await _AddTestData();
            var query = new TableQuery();

            var entities = await _GetAllAsync(query);

            _AssertResult(
                entities,
                ("partition-1", "row-1"),
                ("partition-10", "row-10"),
                ("partition-2", "row-2"),
                ("partition-3", "row-3"),
                ("partition-4", "row-4"),
                ("partition-5", "row-5"),
                ("partition-6", "row-6"),
                ("partition-7", "row-7"),
                ("partition-8", "row-8"),
                ("partition-9", "row-9")
            );
        }

        [Fact]
        public async Task ExecuteQuerySegmentedAsync_WhenThereAreFiltersWithOr_ReturnsMatchingEntitiesWithDefinedRelatedProperties()
        {
            await _AddTestData();
            var query = new TableQuery()
                .Where(
                    new[]
                    {
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.StringProp), QueryComparisons.Equal, "test"),
                        TableQuery.GenerateFilterConditionForInt(nameof(TestQueryEntity.Int32Prop), QueryComparisons.Equal, 3),
                        TableQuery.GenerateFilterConditionForLong(nameof(TestQueryEntity.Int64Prop), QueryComparisons.Equal, 3),
                        TableQuery.GenerateFilterConditionForDouble(nameof(TestQueryEntity.DoubleProp), QueryComparisons.Equal, 3),
                        TableQuery.GenerateFilterConditionForDate(nameof(TestQueryEntity.DateTimeProp), QueryComparisons.Equal, new DateTime(2020, 1, 4, 0,0,0, DateTimeKind.Utc)),
                        TableQuery.GenerateFilterConditionForDate(nameof(TestQueryEntity.DateTimeOffsetProp), QueryComparisons.Equal, new DateTimeOffset(2020, 1, 4, 0,0,0, TimeSpan.Zero)),
                        TableQuery.GenerateFilterConditionForGuid(nameof(TestQueryEntity.GuidProp), QueryComparisons.Equal, Guid.Parse("f32e99d4-05c7-4ed4-b75a-66d47e9d9e63")),
                        nameof(TestQueryEntity.BoolProp),
                        TableQuery.GenerateFilterConditionForBinary(nameof(TestQueryEntity.BinaryProp), QueryComparisons.Equal, Enumerable.Range(0, byte.MaxValue).Select(value => (byte)value).ToArray())
                    }
                    .Aggregate((result, condition) => TableQuery.CombineFilters(result, TableOperators.Or, condition))
                    .Replace("(", "")
                    .Replace(")", "")
                );

            var entities = await _GetAllAsync(query);

            _AssertResult(
                entities,
                ("partition-1", "row-1"),
                ("partition-2", "row-2"),
                ("partition-3", "row-3"),
                ("partition-4", "row-4"),
                ("partition-5", "row-5"),
                ("partition-6", "row-6"),
                ("partition-7", "row-7"),
                ("partition-8", "row-8"),
                ("partition-9", "row-9")
            );
        }

        [Fact]
        public async Task ExecuteQuerySegmentedAsync_WhenUsingInvertedOrFilter_ReturnsMatchingEntitiesAndTheOnesWithoutDefinedRelatedProperties()
        {
            await _AddTestData();
            var query = new TableQuery()
                .Where(
                    TableOperators.Not + "(" +
                    new[]
                    {
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.StringProp), QueryComparisons.NotEqual, "test"),
                        TableQuery.GenerateFilterConditionForInt(nameof(TestQueryEntity.Int32Prop), QueryComparisons.NotEqual, 3),
                        TableQuery.GenerateFilterConditionForLong(nameof(TestQueryEntity.Int64Prop), QueryComparisons.NotEqual, 3),
                        TableQuery.GenerateFilterConditionForDouble(nameof(TestQueryEntity.DoubleProp), QueryComparisons.NotEqual, 3),
                        TableQuery.GenerateFilterConditionForDate(nameof(TestQueryEntity.DateTimeProp), QueryComparisons.NotEqual, new DateTime(2020, 1, 4, 0,0,0, DateTimeKind.Utc)),
                        TableQuery.GenerateFilterConditionForDate(nameof(TestQueryEntity.DateTimeOffsetProp), QueryComparisons.NotEqual, new DateTimeOffset(2020, 1, 4, 0,0,0, TimeSpan.Zero)),
                        TableQuery.GenerateFilterConditionForGuid(nameof(TestQueryEntity.GuidProp), QueryComparisons.NotEqual, Guid.Parse("f32e99d4-05c7-4ed4-b75a-66d47e9d9e63")),
                        $"{TableOperators.Not} {nameof(TestQueryEntity.BoolProp)}",
                        TableQuery.GenerateFilterConditionForBinary(nameof(TestQueryEntity.BinaryProp), QueryComparisons.NotEqual, Enumerable.Range(0, byte.MaxValue).Select(value => (byte)value).ToArray())
                    }
                    .Aggregate((result, condition) => TableQuery.CombineFilters(result, TableOperators.And, condition))
                    .Replace("(", "")
                    .Replace(")", "")
                    + ")"
                );

            var entities = await _GetAllAsync(query);

            _AssertResult(
                entities,
                ("partition-1", "row-1"),
                ("partition-10", "row-10"),
                ("partition-2", "row-2"),
                ("partition-3", "row-3"),
                ("partition-4", "row-4"),
                ("partition-5", "row-5"),
                ("partition-6", "row-6"),
                ("partition-7", "row-7"),
                ("partition-8", "row-8"),
                ("partition-9", "row-9")
            );
        }

        [Fact]
        public async Task ExecuteQuerySegmentedAsync_WhenUsingOrFilterFollowedByAndFilter_ReturnsEntitiesWhereEitherSideOfTheOrOperandsAreTrue()
        {
            await _AddTestData();
            var query = new TableQuery()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.StringProp), QueryComparisons.Equal, "test"),
                        TableOperators.Or,
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterConditionForInt(nameof(TestQueryEntity.Int32Prop), QueryComparisons.Equal, 3),
                            TableOperators.And,
                            TableQuery.GenerateFilterConditionForLong(nameof(TestQueryEntity.Int64Prop), QueryComparisons.Equal, 3)
                        )
                    )
                    .Replace("(", "")
                    .Replace(")", "")
                );

            var entities = await _GetAllAsync(query);

            _AssertResult(
                entities,
                ("partition-1", "row-1")
            );
        }

        [Fact]
        public async Task ExecuteQuerySegmentedAsync_WhenUsingAndFilterFollowedByOrFilter_ReturnsEntitiesWhereEitherSidedOfTheOrOperandsAreTrue()
        {
            await _AddTestData();
            var query = new TableQuery()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.StringProp), QueryComparisons.Equal, "test"),
                        TableOperators.And,
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterConditionForInt(nameof(TestQueryEntity.Int32Prop), QueryComparisons.Equal, 3),
                            TableOperators.Or,
                            TableQuery.GenerateFilterConditionForLong(nameof(TestQueryEntity.Int64Prop), QueryComparisons.Equal, 3)
                        )
                    )
                    .Replace("(", "")
                    .Replace(")", "")
                );

            var entities = await _GetAllAsync(query);

            _AssertResult(
                entities,
                ("partition-3", "row-3")
            );
        }

        [Theory]
        [InlineData(nameof(TestQueryEntity.Int32Prop), QueryComparisons.Equal, 3)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), QueryComparisons.NotEqual, 3)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), QueryComparisons.LessThan, 3)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), QueryComparisons.LessThanOrEqual, 3)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), QueryComparisons.GreaterThan, 3)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), QueryComparisons.GreaterThanOrEqual, 3)]

        [InlineData(nameof(TestQueryEntity.Int64Prop), QueryComparisons.Equal, 3L)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), QueryComparisons.NotEqual, 3L)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), QueryComparisons.LessThan, 3L)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), QueryComparisons.LessThanOrEqual, 3L)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), QueryComparisons.GreaterThan, 3L)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), QueryComparisons.GreaterThanOrEqual, 3L)]

        [InlineData(nameof(TestQueryEntity.DoubleProp), QueryComparisons.Equal, 3D)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), QueryComparisons.NotEqual, 3D)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), QueryComparisons.LessThan, 3D)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), QueryComparisons.LessThanOrEqual, 3D)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), QueryComparisons.GreaterThan, 3D)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), QueryComparisons.GreaterThanOrEqual, 3D)]

        [InlineData(nameof(TestQueryEntity.BoolProp), QueryComparisons.Equal, true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), QueryComparisons.NotEqual, true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), QueryComparisons.LessThan, true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), QueryComparisons.LessThanOrEqual, true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), QueryComparisons.GreaterThan, true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), QueryComparisons.GreaterThanOrEqual, true)]

        [InlineData(nameof(TestQueryEntity.BoolProp), QueryComparisons.Equal, false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), QueryComparisons.NotEqual, false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), QueryComparisons.LessThan, false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), QueryComparisons.LessThanOrEqual, false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), QueryComparisons.GreaterThan, false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), QueryComparisons.GreaterThanOrEqual, false)]

        [InlineData(nameof(TestQueryEntity.DateTimeProp), QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z")]

        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z")]

        [InlineData(nameof(TestQueryEntity.GuidProp), QueryComparisons.Equal, "guid-68260b3f-beab-45e2-b900-33e2b442e724")]
        [InlineData(nameof(TestQueryEntity.GuidProp), QueryComparisons.NotEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724")]
        [InlineData(nameof(TestQueryEntity.GuidProp), QueryComparisons.LessThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724")]
        [InlineData(nameof(TestQueryEntity.GuidProp), QueryComparisons.LessThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724")]
        [InlineData(nameof(TestQueryEntity.GuidProp), QueryComparisons.GreaterThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724")]
        [InlineData(nameof(TestQueryEntity.GuidProp), QueryComparisons.GreaterThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724")]

        [InlineData(nameof(TestQueryEntity.BinaryProp), QueryComparisons.Equal, "binary-AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn8=")]
        [InlineData(nameof(TestQueryEntity.BinaryProp), QueryComparisons.NotEqual, "binary-AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn8=")]
        [InlineData(nameof(TestQueryEntity.BinaryProp), QueryComparisons.LessThan, "binary-AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn8=")]
        [InlineData(nameof(TestQueryEntity.BinaryProp), QueryComparisons.LessThanOrEqual, "binary-AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn8=")]
        [InlineData(nameof(TestQueryEntity.BinaryProp), QueryComparisons.GreaterThan, "binary-AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn8=")]
        [InlineData(nameof(TestQueryEntity.BinaryProp), QueryComparisons.GreaterThanOrEqual, "binary-AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn8=")]

        [InlineData(nameof(TestQueryEntity.StringProp), QueryComparisons.Equal, "3")]
        [InlineData(nameof(TestQueryEntity.StringProp), QueryComparisons.NotEqual, "3")]
        [InlineData(nameof(TestQueryEntity.StringProp), QueryComparisons.LessThan, "3")]
        [InlineData(nameof(TestQueryEntity.StringProp), QueryComparisons.LessThanOrEqual, "3")]
        [InlineData(nameof(TestQueryEntity.StringProp), QueryComparisons.GreaterThan, "3")]
        [InlineData(nameof(TestQueryEntity.StringProp), QueryComparisons.GreaterThanOrEqual, "3")]
        public async Task ExecuteQuerySegmentedAsync_WhenUsingFilterOnNonExistantProperty_ReturnsNoEntities(string propertyName, string filterOperator, object filterValue)
        {
            await CloudTable.CreateIfNotExistsAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition",
                RowKey = "row"
            }));
            var query = new TableQuery().Where(_GetFilter(propertyName, filterOperator, filterValue));

            var entities = await _GetAllAsync(query);

            Assert.Empty(entities);
        }

        [Theory]
        [InlineData(nameof(TestQueryEntity.Int32Prop))]
        [InlineData(nameof(TestQueryEntity.Int64Prop))]
        [InlineData(nameof(TestQueryEntity.DoubleProp))]
        [InlineData(nameof(TestQueryEntity.BoolProp))]
        [InlineData(nameof(TestQueryEntity.DateTimeProp))]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp))]
        [InlineData(nameof(TestQueryEntity.GuidProp))]
        [InlineData(nameof(TestQueryEntity.BinaryProp))]
        [InlineData(nameof(TestQueryEntity.StringProp))]
        public async Task ExecuteQuerySegmentedAsync_WhenUsingPropertyNameFilterOnNonExistantProperty_ReturnsNoEntities(string propertyName)
        {
            await CloudTable.CreateIfNotExistsAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition",
                RowKey = "row"
            }));
            var query = new TableQuery().Where(propertyName);

            var entities = await _GetAllAsync(query);

            Assert.Empty(entities);
        }

        [Theory]
        [InlineData(nameof(TestQueryEntity.Int32Prop))]
        [InlineData(nameof(TestQueryEntity.Int64Prop))]
        [InlineData(nameof(TestQueryEntity.DoubleProp))]
        [InlineData(nameof(TestQueryEntity.BoolProp))]
        [InlineData(nameof(TestQueryEntity.DateTimeProp))]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp))]
        [InlineData(nameof(TestQueryEntity.GuidProp))]
        [InlineData(nameof(TestQueryEntity.BinaryProp))]
        [InlineData(nameof(TestQueryEntity.StringProp))]
        public async Task ExecuteQuerySegmentedAsync_WhenUsingPropertyNameFilterOnExistantProperty_ReturnsNotEntities(string propertyName)
        {
            await CloudTable.CreateIfNotExistsAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition",
                RowKey = "row",
                Int32Prop = 3,
                Int64Prop = 3,
                DoubleProp = 3,
                BoolProp = false,
                DateTimeProp = DateTime.Now,
                DateTimeOffsetProp = DateTimeOffset.Now,
                GuidProp = Guid.NewGuid(),
                BinaryProp = new byte[] { 1, 2, 3 },
                StringProp = "3"
            }));
            var query = new TableQuery().Where(propertyName);

            var entities = await _GetAllAsync(query);

            Assert.Empty(entities);
        }

        [Theory]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, 2, false)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, 3, true)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.Equal, 4, false)]

        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, 2, true)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, 3, false)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.NotEqual, 4, true)]

        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, 2, false)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, 3, false)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThan, 4, true)]

        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, 2, false)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, 3, true)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.LessThanOrEqual, 4, true)]

        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, 2, true)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, 3, false)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThan, 4, false)]

        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, 2, true)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, 3, true)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), 3, QueryComparisons.GreaterThanOrEqual, 4, false)]


        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, 2L, false)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, 3L, true)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.Equal, 4L, false)]

        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, 2L, true)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, 3L, false)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.NotEqual, 4L, true)]

        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, 2L, false)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, 3L, false)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThan, 4L, true)]

        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, 2L, false)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, 3L, true)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.LessThanOrEqual, 4L, true)]

        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, 2L, true)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, 3L, false)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThan, 4L, false)]

        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, 2L, true)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, 3L, true)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), 3L, QueryComparisons.GreaterThanOrEqual, 4L, false)]


        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, 2D, false)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, 3D, true)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.Equal, 4D, false)]

        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, 2D, true)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, 3D, false)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.NotEqual, 4D, true)]

        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, 2D, false)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, 3D, false)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThan, 4D, true)]

        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, 2D, false)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, 3D, true)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.LessThanOrEqual, 4D, true)]

        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, 2D, true)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, 3D, false)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThan, 4D, false)]

        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, 2D, true)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, 3D, true)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), 3D, QueryComparisons.GreaterThanOrEqual, 4D, false)]


        [InlineData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.Equal, true, true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.Equal, false, false)]

        [InlineData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.NotEqual, true, false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.NotEqual, false, true)]

        [InlineData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThan, true, false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThan, false, false)]

        [InlineData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThanOrEqual, true, true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.LessThanOrEqual, false, false)]

        [InlineData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThan, true, false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThan, false, true)]

        [InlineData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThanOrEqual, true, true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), true, QueryComparisons.GreaterThanOrEqual, false, true)]


        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetime-2019-01-22T00:00:00Z", false)]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z", true)]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetime-2021-01-22T00:00:00Z", false)]

        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetime-2019-01-22T00:00:00Z", true)]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z", false)]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetime-2021-01-22T00:00:00Z", true)]

        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetime-2019-01-22T00:00:00Z", false)]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z", false)]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetime-2021-01-22T00:00:00Z", true)]

        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetime-2019-01-22T00:00:00Z", false)]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z", true)]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetime-2021-01-22T00:00:00Z", true)]

        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetime-2019-01-22T00:00:00Z", true)]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z", false)]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetime-2021-01-22T00:00:00Z", false)]

        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetime-2019-01-22T00:00:00Z", true)]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z", true)]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetime-2021-01-22T00:00:00Z", false)]


        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetime-2019-01-22T00:00:00Z", false)]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetime-2020-01-22T00:00:00Z", true)]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.Equal, "datetime-2021-01-22T00:00:00Z", false)]

        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetime-2019-01-22T00:00:00Z", true)]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetime-2020-01-22T00:00:00Z", false)]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.NotEqual, "datetime-2021-01-22T00:00:00Z", true)]

        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetime-2019-01-22T00:00:00Z", false)]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetime-2020-01-22T00:00:00Z", false)]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThan, "datetime-2021-01-22T00:00:00Z", true)]

        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetime-2019-01-22T00:00:00Z", false)]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetime-2020-01-22T00:00:00Z", true)]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.LessThanOrEqual, "datetime-2021-01-22T00:00:00Z", true)]

        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetime-2019-01-22T00:00:00Z", true)]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetime-2020-01-22T00:00:00Z", false)]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThan, "datetime-2021-01-22T00:00:00Z", false)]

        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetime-2019-01-22T00:00:00Z", true)]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetime-2020-01-22T00:00:00Z", true)]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "datetime-2020-01-22T00:00:00Z", QueryComparisons.GreaterThanOrEqual, "datetime-2021-01-22T00:00:00Z", false)]


        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, "guid-58260b3f-beab-45e2-b900-33e2b442e724", false)]
        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true)]
        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.Equal, "guid-78260b3f-beab-45e2-b900-33e2b442e724", false)]

        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, "guid-58260b3f-beab-45e2-b900-33e2b442e724", true)]
        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false)]
        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.NotEqual, "guid-78260b3f-beab-45e2-b900-33e2b442e724", true)]

        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, "guid-58260b3f-beab-45e2-b900-33e2b442e724", false)]
        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false)]
        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThan, "guid-78260b3f-beab-45e2-b900-33e2b442e724", true)]

        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, "guid-58260b3f-beab-45e2-b900-33e2b442e724", false)]
        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true)]
        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.LessThanOrEqual, "guid-78260b3f-beab-45e2-b900-33e2b442e724", true)]

        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, "guid-58260b3f-beab-45e2-b900-33e2b442e724", true)]
        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, "guid-68260b3f-beab-45e2-b900-33e2b442e724", false)]
        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThan, "guid-78260b3f-beab-45e2-b900-33e2b442e724", false)]

        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, "guid-58260b3f-beab-45e2-b900-33e2b442e724", true)]
        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, "guid-68260b3f-beab-45e2-b900-33e2b442e724", true)]
        [InlineData(nameof(TestQueryEntity.GuidProp), "guid-68260b3f-beab-45e2-b900-33e2b442e724", QueryComparisons.GreaterThanOrEqual, "guid-78260b3f-beab-45e2-b900-33e2b442e724", false)]


        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.Equal, "binary-Ag==", false)]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.Equal, "binary-Aw==", true)]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.Equal, "binary-AwE=", false)]

        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.NotEqual, "binary-Ag==", true)]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.NotEqual, "binary-Aw==", false)]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.NotEqual, "binary-AwE=", true)]

        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.LessThan, "binary-Ag==", false)]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.LessThan, "binary-Aw==", false)]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.LessThan, "binary-AwE=", true)]

        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.LessThanOrEqual, "binary-Ag==", false)]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.LessThanOrEqual, "binary-Aw==", true)]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.LessThanOrEqual, "binary-AwE=", true)]

        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.GreaterThan, "binary-Ag==", true)]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.GreaterThan, "binary-Aw==", false)]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.GreaterThan, "binary-AwE=", false)]

        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.GreaterThanOrEqual, "binary-Ag==", true)]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.GreaterThanOrEqual, "binary-Aw==", true)]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "binary-Aw==", QueryComparisons.GreaterThanOrEqual, "binary-AwE=", false)]


        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.Equal, "B", false)]
        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.Equal, "b", true)]
        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.Equal, "bB", false)]

        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.NotEqual, "B", true)]
        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.NotEqual, "b", false)]
        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.NotEqual, "bB", true)]

        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.LessThan, "B", false)]
        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.LessThan, "b", false)]
        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.LessThan, "bB", true)]

        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.LessThanOrEqual, "B", false)]
        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.LessThanOrEqual, "b", true)]
        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.LessThanOrEqual, "bB", true)]

        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.GreaterThan, "B", true)]
        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.GreaterThan, "b", false)]
        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.GreaterThan, "bB", false)]

        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.GreaterThanOrEqual, "B", true)]
        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.GreaterThanOrEqual, "b", true)]
        [InlineData(nameof(TestQueryEntity.StringProp), "b", QueryComparisons.GreaterThanOrEqual, "bB", false)]
        public async Task ExecuteQuerySegmentedAsync_WhenUsingComparisonFilteOperator_MayReturnEntities(string propertyName, object propertyValue, string filterOperator, object filterValue, bool returnsEntity)
        {
            await CloudTable.CreateIfNotExistsAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(new DynamicTableEntity
            {
                PartitionKey = "partition",
                RowKey = "row",
                Properties =
                {
                    { propertyName, EntityProperty.CreateEntityPropertyFromObject(_GetFilterValue(propertyValue)) }
                }
            }));
            var query = new TableQuery().Where(_GetFilter(propertyName, filterOperator, filterValue));

            var entities = await _GetAllAsync(query);

            if (returnsEntity)
                _AssertResult(entities, ("partition", "row"));
            else
                Assert.Empty(entities);
        }

        private static object _GetFilterValue(object filterValue)
        {
            switch (filterValue)
            {
                case int intFilterValue:
                    return intFilterValue;

                case long longFilterValue:
                    return longFilterValue;

                case double doubleFilterValue:
                    return doubleFilterValue;

                case bool boolFilterValue:
                    return boolFilterValue;

                case string stringFilterValue when stringFilterValue.StartsWith("guid-", StringComparison.OrdinalIgnoreCase):
                    return Guid.Parse(stringFilterValue.Substring("guid-".Length));

                case string stringFilterValue when stringFilterValue.StartsWith("datetime-", StringComparison.OrdinalIgnoreCase):
                    return DateTimeOffset.Parse(stringFilterValue.Substring("datetime-".Length));

                case string stringFilterValue when stringFilterValue.StartsWith("binary-", StringComparison.OrdinalIgnoreCase):
                    return Convert.FromBase64String(stringFilterValue.Substring("binary-".Length));

                default:
                    return Convert.ToString(filterValue);
            }
        }

        private static string _GetFilter(string propertyName, string filterOperator, object filterValue)
        {
            switch (filterValue)
            {
                case int intFilterValue:
                    return TableQuery.GenerateFilterConditionForInt(propertyName, filterOperator, intFilterValue);

                case long longFilterValue:
                    return TableQuery.GenerateFilterConditionForLong(propertyName, filterOperator, longFilterValue);

                case double doubleFilterValue:
                    return TableQuery.GenerateFilterConditionForDouble(propertyName, filterOperator, doubleFilterValue);

                case bool boolFilterValue:
                    return TableQuery.GenerateFilterConditionForBool(propertyName, filterOperator, boolFilterValue);

                case string stringFilterValue when stringFilterValue.StartsWith("guid-", StringComparison.OrdinalIgnoreCase):
                    return TableQuery.GenerateFilterConditionForGuid(propertyName, filterOperator, Guid.Parse(stringFilterValue.Substring("guid-".Length)));

                case string stringFilterValue when stringFilterValue.StartsWith("datetime-", StringComparison.OrdinalIgnoreCase):
                    return TableQuery.GenerateFilterConditionForDate(propertyName, filterOperator, DateTimeOffset.Parse(stringFilterValue.Substring("datetime-".Length)));

                case string stringFilterValue when stringFilterValue.StartsWith("binary-", StringComparison.OrdinalIgnoreCase):
                    return TableQuery.GenerateFilterConditionForBinary(propertyName, filterOperator, Convert.FromBase64String(stringFilterValue.Substring("binary-".Length)));

                default:
                    return TableQuery.GenerateFilterCondition(propertyName, filterOperator, Convert.ToString(filterValue));
            }
        }

        private void _AssertResult(IEnumerable<ITableEntity> entities, params (string, string)[] expectedItems)
            => Assert.Equal(expectedItems, entities.Select(entity => (entity.PartitionKey, entity.RowKey)));

        private async Task<IEnumerable<ITableEntity>> _GetAllAsync(TableQuery query)
        {
            var continuationToken = default(TableContinuationToken);
            var entities = new List<ITableEntity>();

            do
            {
                var result = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken);
                continuationToken = result.ContinuationToken;
                entities.AddRange(result);
            } while (continuationToken != null);

            return entities;
        }

        private async Task<IEnumerable<TEntity>> _GetAllAsync<TEntity>(TableQuery<TEntity> query)
                where TEntity : ITableEntity, new()
        {
            var continuationToken = default(TableContinuationToken);
            var entities = new List<TEntity>();

            do
            {
                var result = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken);
                continuationToken = result.ContinuationToken;
                entities.AddRange(result);
            } while (continuationToken != null);

            return entities;
        }

        private async Task _AddTestData()
        {
            await CloudTable.CreateIfNotExistsAsync();

            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-1",
                RowKey = "row-1",
                StringProp = "test"
            }));

            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-2",
                RowKey = "row-2",
                Int32Prop = 3
            }));

            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-3",
                RowKey = "row-3",
                Int64Prop = 3
            }));

            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-4",
                RowKey = "row-4",
                DoubleProp = 3
            }));

            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-5",
                RowKey = "row-5",
                BinaryProp = Enumerable.Range(0, byte.MaxValue).Select(value => (byte)value).ToArray()
            }));

            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-6",
                RowKey = "row-6",
                DateTimeProp = new DateTime(2020, 1, 4, 0, 0, 0, DateTimeKind.Utc)
            }));

            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-7",
                RowKey = "row-7",
                DateTimeOffsetProp = new DateTimeOffset(2020, 1, 4, 0, 0, 0, TimeSpan.Zero)
            }));

            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-8",
                RowKey = "row-8",
                GuidProp = Guid.Parse("f32e99d4-05c7-4ed4-b75a-66d47e9d9e63")
            }));

            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-9",
                RowKey = "row-9",
                BoolProp = true
            }));

            await CloudTable.ExecuteAsync(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-10",
                RowKey = "row-10"
            }));
        }

        private sealed class TestQueryEntity : TableEntity
        {
            public string StringProp { get; set; }

            public byte[] BinaryProp { get; set; }

            public int? Int32Prop { get; set; }

            public long? Int64Prop { get; set; }

            public double? DoubleProp { get; set; }

            public Guid? GuidProp { get; set; }

            public bool? BoolProp { get; set; }

            public DateTime? DateTimeProp { get; set; }

            public DateTimeOffset? DateTimeOffsetProp { get; set; }
        }
    }
}