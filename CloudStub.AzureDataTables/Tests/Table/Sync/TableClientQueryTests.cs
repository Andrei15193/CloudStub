using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Azure;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Sync
{
    public class TableClientQueryTests : BaseTableCloudStubTests
    {
        // Include continuation token tests
        // Partition and row key of last returned entity make up the continuation token
        //
        // Add pagination tests,
        // Split a query result into 2 pages, fetch the 1st one, delete the last entity from it and then fetch the 2nd page
        //
        // Include mapping fields, include case sensitivity checks, include mismatching types for properties
        //
        // Include discrete operation count tests (max 15 according to docs, iirc)
        //
        // Include null comparison tests

        [Fact]
        public void Query_WhenThereAreNoFilters_ReturnsAllItems()
        {
            _AddTestData();

            var entities = CloudTable.Query<TableEntity>();

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
        public void Query_WhenThereAreFiltersWithOr_ReturnsMatchingEntitiesWithDefinedRelatedProperties()
        {
            _AddTestData();

            var dateTimeValue = new DateTime(2020, 1, 4, 0, 0, 0, DateTimeKind.Utc);
            var dateTimeOffsetValue = new DateTimeOffset(2020, 1, 4, 0, 0, 0, TimeSpan.Zero);
            var binaryValue = Enumerable.Range(0, byte.MaxValue).Select(value => (byte)value).ToArray();
            var query = string.Join(" or ", new[]
            {
                TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.StringProp == "test"),
                TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.Int32Prop == 3),
                TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.Int64Prop == 3),
                TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.DoubleProp == 3),
                TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.DateTimeProp == dateTimeValue),
                TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.DateTimeOffsetProp == dateTimeOffsetValue),
                TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.GuidProp == Guid.Parse("f32e99d4-05c7-4ed4-b75a-66d47e9d9e63")),
                nameof(TestQueryEntity.BoolProp),
                TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.BinaryProp == binaryValue)
            });

            var entities = CloudTable.Query<TableEntity>(query);

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
        public void Query_WhenUsingInvertedOrFilter_ReturnsMatchingEntitiesAndTheOnesWithoutDefinedRelatedProperties()
        {
            _AddTestData();

            var dateTimeValue = new DateTime(2020, 1, 4, 0, 0, 0, DateTimeKind.Utc);
            var dateTimeOffsetValue = new DateTimeOffset(2020, 1, 4, 0, 0, 0, TimeSpan.Zero);
            var binaryValue = Enumerable.Range(0, byte.MaxValue).Select(value => (byte)value).ToArray();
            var query = $@"not ({string.Join(" and ", new[]
                {
                    TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.StringProp != "test"),
                    TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.Int32Prop != 3),
                    TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.Int64Prop != 3),
                    TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.DoubleProp != 3),
                    TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.DateTimeProp != dateTimeValue),
                    TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.DateTimeOffsetProp != dateTimeOffsetValue),
                    TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.GuidProp != Guid.Parse("f32e99d4-05c7-4ed4-b75a-66d47e9d9e63")),
                    $"not {nameof(TestQueryEntity.BoolProp)}",
                    TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.BinaryProp != binaryValue)
                })})";

            var entities = CloudTable.Query<TableEntity>(query);

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
        public void Query_WhenUsingOrFilterFollowedByAndFilter_ReturnsEntitiesWhereEitherSideOfTheOrOperandsAreTrue()
        {
            _AddTestData();
            var query = $"{TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.StringProp == "test")} or {TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.Int32Prop == 3)} and {TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.Int64Prop == 3)}";

            var entities = CloudTable.Query<TableEntity>(query);

            _AssertResult(
                entities,
                ("partition-1", "row-1")
            );
        }

        [Fact]
        public void Query_WhenUsingAndFilterFollowedByOrFilter_ReturnsEntitiesWhereEitherSidedOfTheOrOperandsAreTrue()
        {
            _AddTestData();
            var query = $"{TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.StringProp == "test")} and {TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.Int32Prop == 3)} or {TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.Int64Prop == 3)}";

            var entities = CloudTable.Query<TableEntity>(query);

            _AssertResult(
                entities,
                ("partition-3", "row-3")
            );
        }

        [Theory]
        [InlineData(nameof(TestQueryEntity.Int32Prop), "eq", 3)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), "ne", 3)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), "lt", 3)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), "le", 3)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), "gt", 3)]
        [InlineData(nameof(TestQueryEntity.Int32Prop), "ge", 3)]

        [InlineData(nameof(TestQueryEntity.Int64Prop), "eq", 3L)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), "ne", 3L)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), "lt", 3L)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), "le", 3L)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), "gt", 3L)]
        [InlineData(nameof(TestQueryEntity.Int64Prop), "ge", 3L)]

        [InlineData(nameof(TestQueryEntity.DoubleProp), "eq", 3D)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), "ne", 3D)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), "lt", 3D)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), "le", 3D)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), "gt", 3D)]
        [InlineData(nameof(TestQueryEntity.DoubleProp), "ge", 3D)]

        [InlineData(nameof(TestQueryEntity.BoolProp), "eq", true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), "ne", true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), "lt", true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), "le", true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), "gt", true)]
        [InlineData(nameof(TestQueryEntity.BoolProp), "ge", true)]

        [InlineData(nameof(TestQueryEntity.BoolProp), "eq", false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), "ne", false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), "lt", false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), "le", false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), "gt", false)]
        [InlineData(nameof(TestQueryEntity.BoolProp), "ge", false)]

        [InlineData(nameof(TestQueryEntity.DateTimeProp), "eq", "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "ne", "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "lt", "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "le", "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "gt", "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeProp), "ge", "datetime-2020-01-22T00:00:00Z")]

        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "eq", "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "ne", "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "lt", "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "le", "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "gt", "datetime-2020-01-22T00:00:00Z")]
        [InlineData(nameof(TestQueryEntity.DateTimeOffsetProp), "ge", "datetime-2020-01-22T00:00:00Z")]

        [InlineData(nameof(TestQueryEntity.GuidProp), "eq", "guid-68260b3f-beab-45e2-b900-33e2b442e724")]
        [InlineData(nameof(TestQueryEntity.GuidProp), "ne", "guid-68260b3f-beab-45e2-b900-33e2b442e724")]
        [InlineData(nameof(TestQueryEntity.GuidProp), "lt", "guid-68260b3f-beab-45e2-b900-33e2b442e724")]
        [InlineData(nameof(TestQueryEntity.GuidProp), "le", "guid-68260b3f-beab-45e2-b900-33e2b442e724")]
        [InlineData(nameof(TestQueryEntity.GuidProp), "gt", "guid-68260b3f-beab-45e2-b900-33e2b442e724")]
        [InlineData(nameof(TestQueryEntity.GuidProp), "ge", "guid-68260b3f-beab-45e2-b900-33e2b442e724")]

        [InlineData(nameof(TestQueryEntity.BinaryProp), "eq", "binary-AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn8=")]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "ne", "binary-AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn8=")]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "lt", "binary-AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn8=")]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "le", "binary-AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn8=")]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "gt", "binary-AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn8=")]
        [InlineData(nameof(TestQueryEntity.BinaryProp), "ge", "binary-AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn8=")]

        [InlineData(nameof(TestQueryEntity.StringProp), "eq", "3")]
        [InlineData(nameof(TestQueryEntity.StringProp), "ne", "3")]
        [InlineData(nameof(TestQueryEntity.StringProp), "lt", "3")]
        [InlineData(nameof(TestQueryEntity.StringProp), "le", "3")]
        [InlineData(nameof(TestQueryEntity.StringProp), "gt", "3")]
        [InlineData(nameof(TestQueryEntity.StringProp), "ge", "3")]
        public void Query_WhenUsingFilterOnNonExistentProperty_ReturnsNoEntities(string propertyName, string filterOperator, object filterValue)
        {
            CloudTable.CreateIfNotExists();
            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition",
                RowKey = "row"
            });

            var entities = CloudTable.Query<TableEntity>(_GetFilter(propertyName, filterOperator, filterValue));

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
        public void Query_WhenUsingPropertyNameFilterOnNonExistentProperty_ReturnsNoEntities(string propertyName)
        {
            CloudTable.CreateIfNotExists();
            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition",
                RowKey = "row"
            });
            var query = propertyName;

            var entities = CloudTable.Query<TableEntity>(query);

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
        public void Query_WhenUsingPropertyNameFilterOnExistentProperty_ReturnsNotEntities(string propertyName)
        {
            CloudTable.CreateIfNotExists();
            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition",
                RowKey = "row",
                Int32Prop = 3,
                Int64Prop = 3,
                DoubleProp = 3,
                BoolProp = false,
                DateTimeProp = DateTime.UtcNow,
                DateTimeOffsetProp = DateTimeOffset.UtcNow,
                GuidProp = Guid.NewGuid(),
                BinaryProp = new byte[] { 1, 2, 3 },
                StringProp = "3"
            });
            var query = propertyName;

            var entities = CloudTable.Query<TableEntity>(query);

            Assert.Empty(entities);
        }

        [Theory]
        [ClassData(typeof(TableQueryComparisonTestData))]
        public void Query_WhenUsingComparisonFilterOperator_MayReturnEntities(string propertyName, object propertyValue, string filterOperator, object filterValue, bool returnsEntity)
        {
            CloudTable.CreateIfNotExists();
            CloudTable.AddEntity(new TableEntity(new Dictionary<string, object> { { propertyName, _GetFilterValue(propertyValue) } })
            {
                PartitionKey = "partition",
                RowKey = "row",
            });
            var query = _GetFilter(propertyName, filterOperator, filterValue);

            var entities = CloudTable.Query<TableEntity>(query);

            if (returnsEntity)
                _AssertResult(entities, ("partition", "row"));
            else
                Assert.Empty(entities);
        }

        [Fact]
        public void Query_WhenUsingAnInvalidFilter_ThrowsException()
        {
            CloudTable.Create();

            var result = CloudTable.Query<TableEntity>("property1 eq 1'invalid'").AsPages();

            Assertions.JsonResponseThrows(
                () => result.First(),
                response =>
                {
                    var headers = new Assertions.DefaultHeaders(response);
                    headers.Remove("Cache-Control");

                    return new Assertions.UnsuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.BadRequest,
                        ErrorCode = "InvalidInput",
                        ErrorDescription = "Syntax error at position 23 in 'property1 eq 1'invalid''.",
                        Headers = headers
                    };
                }
            );
        }

        [Theory]
        [InlineData("property1 eq propery2")]
        [InlineData("1 eq 2")]
        public void Query_WhenUsingUnsupportedFilter_ThrowsException(string filter)
        {
            CloudTable.Create();

            var result = CloudTable.Query<TableEntity>(filter).AsPages();

            Assertions.JsonResponseThrows(
                () => result.First(),
                response => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.NotImplemented,
                    ErrorCode = "NotImplemented",
                    ErrorDescription = "The requested operation is not implemented on the specified resource.",
                    Headers = new Assertions.DefaultHeaders(response)
                }
            );
        }

        [Fact]
        public void Query_WhenUsingTakeCount_ReturnsOnlyFirstPage()
        {
            _AddTestData();

            var entities = CloudTable.Query<TableEntity>(maxPerPage: 5);

            _AssertResult(
                entities.AsPages().First(),
                ("partition-1", "row-1"),
                ("partition-10", "row-10"),
                ("partition-2", "row-2"),
                ("partition-3", "row-3"),
                ("partition-4", "row-4")
            );
        }

        [Fact]
        public void Query_WhenUsingZeroTakeCount_ReturnsNoEntities()
        {
            _AddTestData();

            var entities = CloudTable.Query<TableEntity>(maxPerPage: 0);

            Assert.Empty(entities);
        }

        [Fact(Skip = "Incomplete implementation")]
        public void Query_TakeCountEqualTo1000_ReturnsSpecifiedNumberOfEntitiesInOnePage()
        {
            CloudTable.Create();
            for (var transactionIndex = 0; transactionIndex < 20; transactionIndex++)
            {
                var transactionActions = new List<TableTransactionAction>(100);
                for (var index = 1; index <= 100; index++)
                    transactionActions.Add(new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("partition", $"row-{index + transactionIndex * 100}")));

                CloudTable.SubmitTransaction(transactionActions);
            }

            var entities = CloudTable.Query<TableEntity>(maxPerPage: 1000);

            Assert.Equal(1000, entities.AsPages().First().Values.Count);
        }

        [Fact]
        public void Query_TakeCountLessThan0_ThrowsException()
        {
            CloudTable.Create();

            Assertions.JsonResponseThrows(
                () => CloudTable.Query<TableEntity>(maxPerPage: -1).AsPages().First(),
                response => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "InvalidInput",
                    ErrorDescription = "One of the request inputs is not valid.",
                    Headers = new Assertions.DefaultHeaders(response)
                }
            );
        }

        [Fact]
        public void Query_TakeCountGreaterThan1000_ThrowsException()
        {
            CloudTable.Create();

            Assertions.JsonResponseThrows(
                () => CloudTable.Query<TableEntity>(maxPerPage: 1001).AsPages().First(),
                response => new Assertions.UnsuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    ErrorCode = "InvalidInput",
                    ErrorDescription = "One of the request inputs is not valid.",
                    Headers = new Assertions.DefaultHeaders(response)
                }
            );
        }

        [Fact]
        public void Query_WhenUsingSelectColumns_ReturnsEntitiesWithSpecifiedColumns()
        {
            _AddTestData();
            var query = TableClient.CreateQueryFilter<TableEntity>(tableEntity => tableEntity.PartitionKey == "partition-1" || tableEntity.PartitionKey == "partition-2");

            var entities = CloudTable.Query<TableEntity>(query, select: new[]
            {
                nameof(TestQueryEntity.PartitionKey),
                nameof(TestQueryEntity.RowKey),
                nameof(TestQueryEntity.Int32Prop)
            });

            _AssertResult(
                entities,
                new string[] { nameof(TestQueryEntity.PartitionKey), nameof(TestQueryEntity.RowKey), nameof(TestQueryEntity.Int32Prop) },
                ("partition-1", "row-1"), ("partition-2", "row-2")
            );
            Assert.Collection(
                entities,
                firstEntity =>
                {
                    Assert.True(firstEntity.TryGetValue(nameof(TestQueryEntity.Int32Prop), out var numberProperty));
                    Assert.Null(numberProperty);
                    Assert.Null(firstEntity.GetInt32(nameof(TestQueryEntity.Int32Prop)));
                    Assert.DoesNotContain(nameof(TestQueryEntity.StringProp), firstEntity);
                },
                secondEntity =>
                {
                    Assert.True(secondEntity.TryGetValue(nameof(TestQueryEntity.Int32Prop), out var numberProperty));
                    Assert.Equal(3, numberProperty);
                    Assert.Equal(3, secondEntity.GetInt32(nameof(TestQueryEntity.Int32Prop)));
                    Assert.DoesNotContain(nameof(TestQueryEntity.StringProp), secondEntity);
                }
            );
        }

        [Fact]
        public void Query_WhenUsingStronglyTypedEntities_ReturnsAllEntities()
        {
            _AddTestData();

            var entities = CloudTable.Query<TestQueryEntity>();

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

        private static object _GetFilterValue(object filterValue)
        {
            if (filterValue is int intFilterValue)
                return intFilterValue;

            if (filterValue is long longFilterValue)
                return longFilterValue;

            if (filterValue is double doubleFilterValue)
                return doubleFilterValue;

            if (filterValue is bool boolFilterValue)
                return boolFilterValue;

            if (filterValue is string stringFilterValue)
                if (stringFilterValue.StartsWith("guid-", StringComparison.OrdinalIgnoreCase))
                    return Guid.Parse(stringFilterValue.Substring("guid-".Length));
                else if (stringFilterValue.StartsWith("datetime-", StringComparison.OrdinalIgnoreCase))
                    return DateTime.Parse(stringFilterValue.Substring("datetime-".Length)).ToUniversalTime();
                else if (stringFilterValue.StartsWith("datetimeoffset-", StringComparison.OrdinalIgnoreCase))
                    return DateTimeOffset.Parse(stringFilterValue.Substring("datetimeoffset-".Length)).ToUniversalTime();
                else if (stringFilterValue.StartsWith("binary-", StringComparison.OrdinalIgnoreCase))
                    return Convert.FromBase64String(stringFilterValue.Substring("binary-".Length));

            return Convert.ToString(filterValue);
        }

        /// <remarks>
        /// Filter keywords such as <c>true</c> and <c>false</c> are case sensitive.
        /// </remarks>
        /// <seealso href="https://learn.microsoft.com/rest/api/storageservices/querying-tables-and-entities"/>
        private static string _GetFilter(string propertyName, string filterOperator, object filterValue)
        {
            if (filterValue is int intFilterValue)
                return $"{propertyName} {filterOperator} {intFilterValue}";

            if (filterValue is long longFilterValue)
                return $"{propertyName} {filterOperator} {longFilterValue}L";

            if (filterValue is double doubleFilterValue)
                return $"{propertyName} {filterOperator} {doubleFilterValue}D";

            if (filterValue is bool boolFilterValue)
                return $"{propertyName} {filterOperator} {(boolFilterValue ? bool.TrueString : bool.FalseString).ToLowerInvariant()}";

            if (filterValue is string stringFilterValue)
                if (stringFilterValue.StartsWith("guid-", StringComparison.OrdinalIgnoreCase))
                    return $"{propertyName} {filterOperator} guid'{stringFilterValue.Substring("guid-".Length)}'";
                else if (stringFilterValue.StartsWith("datetime-", StringComparison.OrdinalIgnoreCase))
                    return $"{propertyName} {filterOperator} datetime'{stringFilterValue.Substring("datetime-".Length)}'";
                else if (stringFilterValue.StartsWith("datetimeoffset-", StringComparison.OrdinalIgnoreCase))
                    return $"{propertyName} {filterOperator} datetime'{stringFilterValue.Substring("datetimeoffset-".Length)}'";
                else if (stringFilterValue.StartsWith("binary-", StringComparison.OrdinalIgnoreCase))
                    return $"{propertyName} {filterOperator} x'{Convert.FromBase64String(stringFilterValue.Substring("binary-".Length)).Aggregate(new StringBuilder(), (result, @byte) => result.AppendFormat("{0:X2}", @byte))}'";

            return $"{propertyName} {filterOperator} '{Convert.ToString(filterValue)}'";
        }

        private void _AssertResult<T>(Pageable<T> entities, params (string, string)[] expectedItems)
            where T : ITableEntity
            => _AssertResult(entities, Array.Empty<string>(), expectedItems);

        private void _AssertResult<T>(Pageable<T> entities, IEnumerable<string> selectedProperties, params (string, string)[] expectedItems)
            where T : ITableEntity
        {
            var processedItemsCount = 0;

            Assert.Multiple(
                entities
                .AsPages()
                .Select(page => new Action(() =>
                {
                    _AssertResult(page, selectedProperties, expectedItems.Skip(processedItemsCount).Take(page.Values.Count).ToArray());
                    processedItemsCount += page.Values.Count;
                }))
                .ToArray()
            );
        }

        private void _AssertResult<T>(Page<T> entities, params (string, string)[] expectedItems)
            where T : ITableEntity
            => _AssertResult(entities, Array.Empty<string>(), expectedItems);

        private void _AssertResult<T>(Page<T> entities, IEnumerable<string> selectedProperties, params (string, string)[] expectedItems)
            where T : ITableEntity
        {
            var resposne = entities.GetRawResponse();

            Assert.Multiple(
                () => Assert.Equal(expectedItems, entities.Values.Select(entity => (entity.PartitionKey, entity.RowKey))),
                () => Assertions.SuccessfulJsonResponse(resposne, new Assertions.SuccessfulResponseAssertOptions
                {
                    StatusCode = HttpStatusCode.OK,
                    Headers = entities.ContinuationToken is null
                        ? new Assertions.DefaultHeaders(resposne)
                        : new Assertions.DefaultHeaders(resposne)
                        {
                            { "x-ms-continuation-NextPartitionKey", entities.ContinuationToken.Split(' ').First() },
                            { "x-ms-continuation-NextRowKey", entities.ContinuationToken.Split(' ').Last() }
                        },
                    Content =
                    {
                        { "odata.metadata", $"https://cloudstubdev.table.core.windows.net/$metadata#{TestTableName}{(selectedProperties.Any() ? "&$select=" + string.Join(',', selectedProperties) : "")}" },
                        { "value", entities.Values.Select(entity => _MapEntityToDictionary(entity)).ToList() }
                    }
                })
            );
        }

        private IReadOnlyDictionary<string, object> _MapEntityToDictionary(ITableEntity entity)
        {
            if (entity is TableEntity tableEntity)
                return _MapEntityToDictionary(tableEntity);
            if (entity is TestQueryEntity testQueryEntity)
                return _MapEntityToDictionary(testQueryEntity);

            throw new ArgumentException(nameof(entity), $"Unhandled '{entity.GetType().Name}' entity type.");
        }

        private IReadOnlyDictionary<string, object> _MapEntityToDictionary(TestQueryEntity entity)
            => _MapEntityToDictionary(new TableEntity(typeof(TestQueryEntity).GetProperties().Where(property => property.GetValue(entity) != null).ToDictionary(
                property => property.Name == nameof(ITableEntity.ETag) ? "odata.etag" : property.Name,
                property =>
                {
                    if (property.GetValue(entity) is DateTime dateTime)
                        return dateTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'FFFFFFFZ");

                    if (property.GetValue(entity) is DateTimeOffset dateTimeOffset)
                        return dateTimeOffset.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'FFFFFFFZ");
                    if (property.GetValue(entity) is Guid guid)
                        return guid.ToString("D");

                    return property.GetValue(entity);
                }
            )));

        private IReadOnlyDictionary<string, object> _MapEntityToDictionary(TableEntity entity)
        {
            var result = new Dictionary<string, object>(entity);

            foreach (var pair in entity)
                switch (pair.Key)
                {
                    case nameof(TestQueryEntity.Int64Prop):
                        result.Add($"{pair.Key}@odata.type", "Edm.Int64");
                        break;

                    case nameof(TestQueryEntity.DateTimeProp):
                        result.Add($"{pair.Key}@odata.type", "Edm.DateTime");
                        break;

                    case nameof(TestQueryEntity.DateTimeOffsetProp):
                        result.Add($"{pair.Key}@odata.type", "Edm.DateTime");
                        break;

                    case nameof(TestQueryEntity.GuidProp):
                        result.Add($"{pair.Key}@odata.type", "Edm.Guid");
                        break;

                    case nameof(TestQueryEntity.BinaryProp):
                        result.Add($"{pair.Key}@odata.type", "Edm.Binary");
                        break;
                }

            return result;
        }

        private void _AddTestData()
        {
            CloudTable.CreateIfNotExists();

            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition-1",
                RowKey = "row-1",
                StringProp = "test"
            });

            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition-2",
                RowKey = "row-2",
                Int32Prop = 3
            });

            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition-3",
                RowKey = "row-3",
                Int64Prop = 3
            });

            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition-4",
                RowKey = "row-4",
                DoubleProp = 3
            });

            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition-5",
                RowKey = "row-5",
                BinaryProp = Enumerable.Range(0, byte.MaxValue).Select(value => (byte)value).ToArray()
            });

            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition-6",
                RowKey = "row-6",
                DateTimeProp = new DateTime(2020, 1, 4, 0, 0, 0, DateTimeKind.Utc)
            });

            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition-7",
                RowKey = "row-7",
                DateTimeOffsetProp = new DateTimeOffset(2020, 1, 4, 0, 0, 0, TimeSpan.Zero)
            });

            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition-8",
                RowKey = "row-8",
                GuidProp = Guid.Parse("f32e99d4-05c7-4ed4-b75a-66d47e9d9e63")
            });

            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition-9",
                RowKey = "row-9",
                BoolProp = true
            });

            CloudTable.AddEntity(new TestQueryEntity
            {
                PartitionKey = "partition-10",
                RowKey = "row-10"
            });
        }
    }
}