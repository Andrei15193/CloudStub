using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Xunit;

namespace CloudStub.Tests.TableTests.Async
{
    public class InMemoryCloudTableQueryTests : BaseInMemoryCloudTableTests
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
        [ClassData(typeof(InMemoryCloudTableQueryComparisonTestData))]
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

        [Fact]
        public async Task ExecuteQuerySegmentedAsync_WhenUsingTakeCount_ReturnsAllEntities()
        {
            await _AddTestData();
            var query = new TableQuery().Take(5);

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
        public async Task ExecuteQuerySegmentedAsync_WhenUsingZeroTakeCount_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<ArgumentException>(() => _GetAllAsync(new TableQuery().Take(0)));

            Assert.Equal(new ArgumentException("Take count must be positive and greater than 0.").Message, exception.Message);
        }

        [Fact]
        public async Task ExecuteQuerySegmentedAsync_TakeCountGreaterThan1000_ReturnsEntitiesInPagesOf1000()
        {
            await CloudTable.CreateAsync();
            for (var batchIndex = 0; batchIndex < 20; batchIndex++)
            {
                var batchOperation = new TableBatchOperation();
                for (var index = 1; index <= 100; index++)
                    batchOperation.Add(TableOperation.Insert(new TableEntity("partition", $"row-{index + batchIndex * 100}")));

                await CloudTable.ExecuteBatchAsync(batchOperation);
            }

            var continuationToken = default(TableContinuationToken);
            var query = new TableQuery().Take(1001);
            do
            {
                var result = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken);
                continuationToken = result.ContinuationToken;
                Assert.Equal(1000, result.Count());
            } while (continuationToken != null);
        }

        [Theory]
        [InlineData(5)]
        [InlineData(10)]
        [InlineData(15)]
        [InlineData(20)]
        [InlineData(25)]
        public async Task ExecuteQuerySegmentedAsync_TakeLessThan1000_ReturnsEntitiesInSpecifiedPageSizes(int takeCount)
        {
            await CloudTable.CreateAsync();
            var batchOperation = new TableBatchOperation();
            for (var index = 0; index < takeCount * 4; index++)
                batchOperation.Add(TableOperation.Insert(new TableEntity("partition", $"row-{index:000}")));

            await CloudTable.ExecuteBatchAsync(batchOperation);

            var pageIndex = 0;
            var continuationToken = default(TableContinuationToken);
            var query = new TableQuery().Take(takeCount);
            do
            {
                var result = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken);
                continuationToken = result.ContinuationToken;
                Assert.Equal(takeCount, result.Count());

                for (var index = 0; index < takeCount; index++)
                    Assert.Equal($"row-{index + pageIndex * takeCount:000}", result.ElementAt(index).RowKey);
                pageIndex++;

                if (continuationToken != null)
                {
                    Assert.NotNull(continuationToken.NextPartitionKey);
                    Assert.NotNull(continuationToken.NextRowKey);
                    Assert.Null(continuationToken.NextTableName);
                    Assert.NotNull(continuationToken.TargetLocation);
                }
            } while (continuationToken != null);
        }

        [Fact]
        public async Task ExecuteQuerySegmentedAsync_WhenUsingSelectColumns_ReturnsEntitiesWithSpecifiedColumns()
        {
            await _AddTestData();
            var query = new TableQuery()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.PartitionKey), QueryComparisons.Equal, "partition-1"),
                        TableOperators.Or,
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.PartitionKey), QueryComparisons.Equal, "partition-2")
                    )
                )
                .Select(new[] { nameof(TestQueryEntity.Int32Prop) });

            var entities = await _GetAllAsync(query);

            _AssertResult(entities, ("partition-1", "row-1"), ("partition-2", "row-2"));
            {
                var firstEntity = entities.Cast<DynamicTableEntity>().ElementAt(0);
                Assert.Equal(EdmType.String, firstEntity.Properties[nameof(TestQueryEntity.Int32Prop)].PropertyType);
                Assert.Null(firstEntity.Properties[nameof(TestQueryEntity.Int32Prop)].Int32Value);
                Assert.DoesNotContain(nameof(TestQueryEntity.StringProp), firstEntity.Properties);
            }
            {
                var secondEntity = entities.Cast<DynamicTableEntity>().ElementAt(1);
                Assert.Equal(EdmType.Int32, secondEntity.Properties[nameof(TestQueryEntity.Int32Prop)].PropertyType);
                Assert.Equal(3, secondEntity.Properties[nameof(TestQueryEntity.Int32Prop)].Int32Value);
                Assert.DoesNotContain(nameof(TestQueryEntity.StringProp), secondEntity.Properties);
            }
        }

        [Fact]
        public async Task ExecuteQuerySegmentedAsync_WhenUsingStronglyTypedEntities_ReturnsAllEntities()
        {
            await _AddTestData();
            var query = new TableQuery<TestQueryEntity>();

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
        public async Task ExecuteQuerySegmentedAsync_WhenUsingEntityResolverWithProjection_ReturnsEntitiesWithSelectedProperties()
        {
            await _AddTestData();
            var query = new TableQuery()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.PartitionKey), QueryComparisons.Equal, "partition-1"),
                        TableOperators.Or,
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.PartitionKey), QueryComparisons.Equal, "partition-2")
                    )
                )
                .Select(new[] { nameof(TestQueryEntity.Int32Prop) });

            var entities = await _GetAllAsync(
                query,
                (partitionKey, rowKey, timestamp, properties, etag) => new DynamicTableEntity(partitionKey, rowKey, etag, properties)
                {
                    Timestamp = timestamp
                }
            );

            _AssertResult(entities, ("partition-1", "row-1"), ("partition-2", "row-2"));
            {
                var firstEntity = entities.Cast<DynamicTableEntity>().ElementAt(0);
                Assert.Equal(EdmType.String, firstEntity.Properties[nameof(TestQueryEntity.Int32Prop)].PropertyType);
                Assert.Null(firstEntity.Properties[nameof(TestQueryEntity.Int32Prop)].Int32Value);
                Assert.DoesNotContain(nameof(TestQueryEntity.StringProp), firstEntity.Properties);
            }
            {
                var secondEntity = entities.Cast<DynamicTableEntity>().ElementAt(1);
                Assert.Equal(EdmType.Int32, secondEntity.Properties[nameof(TestQueryEntity.Int32Prop)].PropertyType);
                Assert.Equal(3, secondEntity.Properties[nameof(TestQueryEntity.Int32Prop)].Int32Value);
                Assert.DoesNotContain(nameof(TestQueryEntity.StringProp), secondEntity.Properties);
            }
        }

        [Fact]
        public async Task ExecuteQuerySegmentedAsync_WhenUsingStronglyTypedQueryWithEntityResolverAndProjection_ReturnsEntitiesWithSelectedProperties()
        {
            await _AddTestData();
            var query = new TableQuery<TableEntity>()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.PartitionKey), QueryComparisons.Equal, "partition-1"),
                        TableOperators.Or,
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.PartitionKey), QueryComparisons.Equal, "partition-2")
                    )
                )
                .Select(new[] { nameof(TestQueryEntity.Int32Prop) });

            var entities = await _GetAllAsync(
                query,
                (partitionKey, rowKey, timestamp, properties, etag) => new DynamicTableEntity(partitionKey, rowKey, etag, properties)
                {
                    Timestamp = timestamp
                }
            );

            _AssertResult(entities, ("partition-1", "row-1"), ("partition-2", "row-2"));
            {
                var firstEntity = entities.Cast<DynamicTableEntity>().ElementAt(0);
                Assert.Equal(EdmType.String, firstEntity.Properties[nameof(TestQueryEntity.Int32Prop)].PropertyType);
                Assert.Null(firstEntity.Properties[nameof(TestQueryEntity.Int32Prop)].Int32Value);
                Assert.DoesNotContain(nameof(TestQueryEntity.StringProp), firstEntity.Properties);
            }
            {
                var secondEntity = entities.Cast<DynamicTableEntity>().ElementAt(1);
                Assert.Equal(EdmType.Int32, secondEntity.Properties[nameof(TestQueryEntity.Int32Prop)].PropertyType);
                Assert.Equal(3, secondEntity.Properties[nameof(TestQueryEntity.Int32Prop)].Int32Value);
                Assert.DoesNotContain(nameof(TestQueryEntity.StringProp), secondEntity.Properties);
            }
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
                    return DateTime.Parse(stringFilterValue.Substring("datetime-".Length));

                case string stringFilterValue when stringFilterValue.StartsWith("datetimeoffset-", StringComparison.OrdinalIgnoreCase):
                    return DateTimeOffset.Parse(stringFilterValue.Substring("datetimeoffset-".Length));

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
                    return TableQuery.GenerateFilterConditionForDate(propertyName, filterOperator, DateTime.Parse(stringFilterValue.Substring("datetime-".Length)));

                case string stringFilterValue when stringFilterValue.StartsWith("datetimeoffset-", StringComparison.OrdinalIgnoreCase):
                    return TableQuery.GenerateFilterConditionForDate(propertyName, filterOperator, DateTimeOffset.Parse(stringFilterValue.Substring("datetimeoffset-".Length)));

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

        private async Task<IEnumerable<TResult>> _GetAllAsync<TResult>(TableQuery query, EntityResolver<TResult> entityResolver)
        {
            var continuationToken = default(TableContinuationToken);
            var entities = new List<TResult>();

            do
            {
                var result = await CloudTable.ExecuteQuerySegmentedAsync(query, entityResolver, continuationToken, null, null);
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

        private async Task<IEnumerable<TResult>> _GetAllAsync<TEntity, TResult>(TableQuery<TEntity> query, EntityResolver<TResult> entityResolver)
            where TEntity : ITableEntity, new()
        {
            var continuationToken = default(TableContinuationToken);
            var entities = new List<TResult>();

            do
            {
                var result = await CloudTable.ExecuteQuerySegmentedAsync(query, entityResolver, continuationToken, null, null);
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
    }
}