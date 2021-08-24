using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;
using Xunit;

namespace CloudStub.Tests.TableTests.Sync
{
    public class StubCloudTableQueryTests : BaseStubCloudTableTests
    {
        [Fact]
        public void ExecuteQuery_WhenThereAreNoFilters_ReturnsAllItems()
        {
            _AddTestData();
            var query = new TableQuery();

            var entities = CloudTable.ExecuteQuery(query);

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
        public void ExecuteQuery_WhenThereAreFiltersWithOr_ReturnsMatchingEntitiesWithDefinedRelatedProperties()
        {
            _AddTestData();
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

            var entities = CloudTable.ExecuteQuery(query);

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
        public void ExecuteQuery_WhenUsingInvertedOrFilter_ReturnsMatchingEntitiesAndTheOnesWithoutDefinedRelatedProperties()
        {
            _AddTestData();
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

            var entities = CloudTable.ExecuteQuery(query);

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
        public void ExecuteQuery_WhenUsingOrFilterFollowedByAndFilter_ReturnsEntitiesWhereEitherSideOfTheOrOperandsAreTrue()
        {
            _AddTestData();
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

            var entities = CloudTable.ExecuteQuery(query);

            _AssertResult(
                entities,
                ("partition-1", "row-1")
            );
        }

        [Fact]
        public void ExecuteQuery_WhenUsingAndFilterFollowedByOrFilter_ReturnsEntitiesWhereEitherSidedOfTheOrOperandsAreTrue()
        {
            _AddTestData();
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

            var entities = CloudTable.ExecuteQuery(query);

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
        public void ExecuteQuery_WhenUsingFilterOnNonExistantProperty_ReturnsNoEntities(string propertyName, string filterOperator, object filterValue)
        {
            CloudTable.CreateIfNotExists();
            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition",
                RowKey = "row"
            }));
            var query = new TableQuery().Where(_GetFilter(propertyName, filterOperator, filterValue));

            var entities = CloudTable.ExecuteQuery(query);

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
        public void ExecuteQuery_WhenUsingPropertyNameFilterOnNonExistantProperty_ReturnsNoEntities(string propertyName)
        {
            CloudTable.CreateIfNotExists();
            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition",
                RowKey = "row"
            }));
            var query = new TableQuery().Where(propertyName);

            var entities = CloudTable.ExecuteQuery(query);

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
        public void ExecuteQuery_WhenUsingPropertyNameFilterOnExistantProperty_ReturnsNotEntities(string propertyName)
        {
            CloudTable.CreateIfNotExists();
            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
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

            var entities = CloudTable.ExecuteQuery(query);

            Assert.Empty(entities);
        }

        [Theory]
        [ClassData(typeof(StubCloudTableQueryComparisonTestData))]
        public void ExecuteQuery_WhenUsingComparisonFilterOperator_MayReturnEntities(string propertyName, object propertyValue, string filterOperator, object filterValue, bool returnsEntity)
        {
            CloudTable.CreateIfNotExists();
            CloudTable.Execute(TableOperation.Insert(new DynamicTableEntity
            {
                PartitionKey = "partition",
                RowKey = "row",
                Properties =
                {
                    { propertyName, EntityProperty.CreateEntityPropertyFromObject(_GetFilterValue(propertyValue)) }
                }
            }));
            var query = new TableQuery().Where(_GetFilter(propertyName, filterOperator, filterValue));

            var entities = CloudTable.ExecuteQuery(query);

            if (returnsEntity)
                _AssertResult(entities, ("partition", "row"));
            else
                Assert.Empty(entities);
        }

        [Fact]
        public void ExecuteQuery_WhenUsingTakeCount_ReturnsOnlyFirstPage()
        {
            _AddTestData();
            var query = new TableQuery().Take(5);

            var entities = CloudTable.ExecuteQuery(query);

            _AssertResult(
                entities,
                ("partition-1", "row-1"),
                ("partition-10", "row-10"),
                ("partition-2", "row-2"),
                ("partition-3", "row-3"),
                ("partition-4", "row-4")
            );
        }

        [Fact]
        public void ExecuteQuery_WhenUsingZeroTakeCount_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentException>(() => CloudTable.ExecuteQuery(new TableQuery().Take(0)));

            Assert.Equal(new ArgumentException("Take count must be positive and greater than 0.").Message, exception.Message);
        }

        [Fact]
        public void ExecuteQuery_TakeCountGreaterThan1000_ReturnsSpecifiedNumberOfEntities()
        {
            CloudTable.Create();
            for (var batchIndex = 0; batchIndex < 20; batchIndex++)
            {
                var batchOperation = new TableBatchOperation();
                for (var index = 1; index <= 100; index++)
                    batchOperation.Add(TableOperation.Insert(new TableEntity("partition", $"row-{index + batchIndex * 100}")));

                CloudTable.ExecuteBatch(batchOperation);
            }

            var query = new TableQuery().Take(1001);
            var result = CloudTable.ExecuteQuery(query);
            Assert.Equal(1001, result.Count());
        }

        [Fact]
        public void ExecuteQuery_WhenUsingSelectColumns_ReturnsEntitiesWithSpecifiedColumns()
        {
            _AddTestData();
            var query = new TableQuery()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.PartitionKey), QueryComparisons.Equal, "partition-1"),
                        TableOperators.Or,
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.PartitionKey), QueryComparisons.Equal, "partition-2")
                    )
                )
                .Select(new[] { nameof(TestQueryEntity.Int32Prop) });

            var entities = CloudTable.ExecuteQuery(query);

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
        public void ExecuteQuery_WhenUsingStronglyTypedEntities_ReturnsAllEntities()
        {
            _AddTestData();
            var query = new TableQuery<TestQueryEntity>();

            var entities = CloudTable.ExecuteQuery(query);

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
        public void ExecuteQuery_WhenUsingEntityResolverWithProjection_ReturnsEntitiesWithSelectedProperties()
        {
            _AddTestData();
            var query = new TableQuery()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.PartitionKey), QueryComparisons.Equal, "partition-1"),
                        TableOperators.Or,
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.PartitionKey), QueryComparisons.Equal, "partition-2")
                    )
                )
                .Select(new[] { nameof(TestQueryEntity.Int32Prop) });

            var entities = CloudTable.ExecuteQuery(
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
        public void ExecuteQuery_WhenUsingStronglyTypedQueryWithEntityResolverAndProjection_ReturnsEntitiesWithSelectedProperties()
        {
            _AddTestData();
            var query = new TableQuery<TableEntity>()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.PartitionKey), QueryComparisons.Equal, "partition-1"),
                        TableOperators.Or,
                        TableQuery.GenerateFilterCondition(nameof(TestQueryEntity.PartitionKey), QueryComparisons.Equal, "partition-2")
                    )
                )
                .Select(new[] { nameof(TestQueryEntity.Int32Prop) });

            var entities = CloudTable.ExecuteQuery(
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

        private void _AddTestData()
        {
            CloudTable.CreateIfNotExists();

            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-1",
                RowKey = "row-1",
                StringProp = "test"
            }));

            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-2",
                RowKey = "row-2",
                Int32Prop = 3
            }));

            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-3",
                RowKey = "row-3",
                Int64Prop = 3
            }));

            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-4",
                RowKey = "row-4",
                DoubleProp = 3
            }));

            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-5",
                RowKey = "row-5",
                BinaryProp = Enumerable.Range(0, byte.MaxValue).Select(value => (byte)value).ToArray()
            }));

            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-6",
                RowKey = "row-6",
                DateTimeProp = new DateTime(2020, 1, 4, 0, 0, 0, DateTimeKind.Utc)
            }));

            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-7",
                RowKey = "row-7",
                DateTimeOffsetProp = new DateTimeOffset(2020, 1, 4, 0, 0, 0, TimeSpan.Zero)
            }));

            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-8",
                RowKey = "row-8",
                GuidProp = Guid.Parse("f32e99d4-05c7-4ed4-b75a-66d47e9d9e63")
            }));

            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-9",
                RowKey = "row-9",
                BoolProp = true
            }));

            CloudTable.Execute(TableOperation.Insert(new TestQueryEntity
            {
                PartitionKey = "partition-10",
                RowKey = "row-10"
            }));
        }
    }
}