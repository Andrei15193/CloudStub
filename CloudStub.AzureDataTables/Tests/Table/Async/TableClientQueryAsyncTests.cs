using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using CloudStub.AzureDataTables.Tests.Data;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Table.Async
{
    public class TableClientQueryAsyncTests : BaseTableCloudStubTests
    {
        [Fact]
        public async Task QueryAsync_WhenThereAreNoFilters_ReturnsAllItems()
        {
            await _AddTestDataAsync();

            var entities = TableClient.QueryAsync<TableEntity>();

            await _AssertResultAsync(
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
        public async Task QueryAsync_WhenThereAreFiltersWithOr_ReturnsMatchingEntitiesWithDefinedRelatedProperties()
        {
            await _AddTestDataAsync();

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

            var entities = TableClient.QueryAsync<TableEntity>(query);

            await _AssertResultAsync(
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
        public async Task QueryAsync_WhenUsingInvertedOrFilter_ReturnsMatchingEntitiesAndTheOnesWithoutDefinedRelatedProperties()
        {
            await _AddTestDataAsync();

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

            var entities = TableClient.QueryAsync<TableEntity>(query);

            await _AssertResultAsync(
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
        public async Task QueryAsync_WhenUsingOrFilterFollowedByAndFilter_ReturnsEntitiesWhereEitherSideOfTheOrOperandsAreTrue()
        {
            await _AddTestDataAsync();
            var query = $"{TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.StringProp == "test")} or {TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.Int32Prop == 3)} and {TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.Int64Prop == 3)}";

            var entities = TableClient.QueryAsync<TableEntity>(query);

            await _AssertResultAsync(
                entities,
                ("partition-1", "row-1")
            );
        }

        [Fact]
        public async Task QueryAsync_WhenUsingAndFilterFollowedByOrFilter_ReturnsEntitiesWhereEitherSidedOfTheOrOperandsAreTrue()
        {
            await _AddTestDataAsync();
            var query = $"{TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.StringProp == "test")} and {TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.Int32Prop == 3)} or {TableClient.CreateQueryFilter<TestQueryEntity>(tableEntity => tableEntity.Int64Prop == 3)}";

            var entities = TableClient.QueryAsync<TableEntity>(query);

            await _AssertResultAsync(
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
        public async Task QueryAsync_WhenUsingFilterOnNonExistentProperty_ReturnsNoEntities(string propertyName, string filterOperator, object filterValue)
        {
            await TableClient.CreateIfNotExistsAsync();
            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition",
                RowKey = "row"
            });

            var entities = await TableClient.QueryAsync<TableEntity>(_GetFilter(propertyName, filterOperator, filterValue)).ToListAsync();

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
        public async Task QueryAsync_WhenUsingPropertyNameFilterOnNonExistentProperty_ReturnsNoEntities(string propertyName)
        {
            await TableClient.CreateIfNotExistsAsync();
            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition",
                RowKey = "row"
            });
            var query = propertyName;

            var entities = await TableClient.QueryAsync<TableEntity>(query).ToListAsync();

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
        public async Task QueryAsync_WhenUsingPropertyNameFilterOnExistentProperty_ReturnsNotEntities(string propertyName)
        {
            await TableClient.CreateIfNotExistsAsync();
            await TableClient.AddEntityAsync(new TestQueryEntity
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

            var entities = await TableClient.QueryAsync<TableEntity>(query).ToListAsync();

            Assert.Empty(entities);
        }

        [Theory]
        [ClassData(typeof(TableQueryComparisonTestData))]
        public async Task QueryAsync_WhenUsingComparisonFilterOperator_MayReturnEntities(string propertyName, object propertyValue, string filterOperator, object filterValue, bool returnsEntity)
        {
            await TableClient.CreateIfNotExistsAsync();
            await TableClient.AddEntityAsync(new TableEntity(new Dictionary<string, object> { { propertyName, _GetFilterValue(propertyValue) } })
            {
                PartitionKey = "partition",
                RowKey = "row",
            });
            var query = _GetFilter(propertyName, filterOperator, filterValue);

            var entities = TableClient.QueryAsync<TableEntity>(query);

            if (returnsEntity)
                await _AssertResultAsync(entities, ("partition", "row"));
            else
                Assert.Empty(await entities.ToListAsync());
        }

        [Theory]
        [ClassData(typeof(TableQueryNullComparisonTestData))]
        public async Task QueryAsync_WhenUsingComparisonFilterOperatorWithNullValue_ThrowsException(string propertyName, object propertyValue, string filterOperator, string expectedErrorMessage, IEnumerable<string> removedHeaders)
        {
            await TableClient.CreateIfNotExistsAsync();
            await TableClient.AddEntityAsync(new TableEntity(new Dictionary<string, object> { { propertyName, _GetFilterValue(propertyValue) } })
            {
                PartitionKey = "partition",
                RowKey = "row",
            });

            var query = _GetFilter(propertyName, filterOperator, null);

            await Assertions.JsonResponseThrowsAsync(
                async () => await TableClient.QueryAsync<TableEntity>(query).ToListAsync(),
                response =>
                {
                    var headers = new Assertions.DefaultHeaders(response);
                    foreach (var remvoedHeader in removedHeaders)
                        headers.Remove(remvoedHeader);

                    return new Assertions.UnsuccessfulResponseAssertOptions
                    {
                        StatusCode = HttpStatusCode.BadRequest,
                        ErrorCode = "InvalidInput",
                        ErrorDescription = expectedErrorMessage,
                        Headers = headers
                    };
                }
            );
        }

        [Fact]
        public async Task QueryAsync_WhenUsingAnInvalidFilter_ThrowsException()
        {
            await TableClient.CreateAsync();

            var result = TableClient.QueryAsync<TableEntity>("property1 eq 1'invalid'").AsPages();

            await Assertions.JsonResponseThrowsAsync(
                async () => await result.FirstAsync(),
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
        public async Task QueryAsync_WhenUsingUnsupportedFilter_ThrowsException(string filter)
        {
            await TableClient.CreateAsync();

            var result = TableClient.QueryAsync<TableEntity>(filter).AsPages();

            await Assertions.JsonResponseThrowsAsync(
                async () => await result.FirstAsync(),
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
        public async Task QueryAsync_WhenUsingMoreThan50DiscreteFilterOperators_ReturnsMatchingEntities()
        {
            await _AddTestDataAsync();

            var result = TableClient.QueryAsync<TableEntity>(
                "PartitionKey ge 'partition-1' or ("
                + string.Join(" and ", Enumerable.Range(1, 50).Select(number => $"property{number} eq 'value{number}'"))
                + ")"
            );

            await _AssertResultAsync(
                result,
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
        public async Task QueryAsync_WhenUsingTakeCount_ReturnsOnlyFirstPage()
        {
            await _AddTestDataAsync();

            var entities = TableClient.QueryAsync<TableEntity>(maxPerPage: 5);

            _AssertResult(
                await entities.AsPages().FirstAsync(),
                ("partition-1", "row-1"),
                ("partition-10", "row-10"),
                ("partition-2", "row-2"),
                ("partition-3", "row-3"),
                ("partition-4", "row-4")
            );
        }

        [Fact]
        public async Task QueryAsync_WhenUsingTakeCount_ReturnsContinuationTokenContainingPartitionAndRowKeysForNextPage()
        {
            await _AddTestDataAsync();

            var entities = TableClient.QueryAsync<TableEntity>(maxPerPage: 5);

            await _AssertResultAsync(
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
        public async Task QueryAsync_WhenUsingTakeCount_ReturnsContinuationTokenUsingBase64UrlEncoding()
        {
            await TableClient.CreateIfNotExistsAsync();

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "<",
                RowKey = "row"
            });

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "<.>",
                RowKey = "test"
            });
            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "<.>",
                RowKey = "ÿÿÿ"
            });

            var entities = TableClient.QueryAsync<TableEntity>(maxPerPage: 1);

            await _AssertResultAsync(
                entities,
                ("<", "row"),
                ("<.>", "test"),
                ("<.>", "ÿÿÿ")
            );
        }

        [Fact]
        public async Task QueryAsync_WhenUsingTakeCountAndFilter_ReturnsContinuationTokenForNextItemSatisfyingTheFilter()
        {
            await TableClient.CreateIfNotExistsAsync();

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-1",
                RowKey = "row-1"
            });
            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-2",
                RowKey = "row-2"
            });
            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-3",
                RowKey = "row-3"
            });
            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-4",
                RowKey = "row-4"
            });

            var pages = await TableClient
                .QueryAsync<TableEntity>(
                    entity => entity.PartitionKey == "partition-1" || entity.PartitionKey == "partition-3",
                    maxPerPage: 1
                )
                .AsPages()
                .ToListAsync();

            Assert.Collection(
                pages,
                firstPage => _AssertResult(
                    firstPage,
                    ("partition-1", "row-1")
                ),
                lastPage => _AssertResult(
                    lastPage,
                    $"{ResponseContinuationToken.EncodeContinuationToken("partition-3")} {ResponseContinuationToken.EncodeContinuationToken("row-3")}",
                    ("partition-3", "row-3")
                )
            );
        }

        [Fact]
        public async Task QueryAsync_WhenUsingZeroTakeCount_ReturnsNoEntities()
        {
            await _AddTestDataAsync();

            var entities = await TableClient.QueryAsync<TableEntity>(maxPerPage: 0).ToListAsync();

            Assert.Empty(entities);
        }

        [Fact]
        public async Task QueryAsync_TakeCountEqualTo1000_ReturnsSpecifiedNumberOfEntitiesInOnePage()
        {
            await TableClient.CreateIfNotExistsAsync();
            for (var transactionIndex = 0; transactionIndex < 20; transactionIndex++)
            {
                var transactionActions = new List<TableTransactionAction>(100);
                for (var index = 1; index <= 100; index++)
                    transactionActions.Add(new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("partition", $"row-{index + transactionIndex * 100}")));

                await TableClient.SubmitTransactionAsync(transactionActions);
            }

            var entities = TableClient.QueryAsync<TableEntity>(maxPerPage: 1000);

            Assert.Equal(1000, (await entities.AsPages().FirstAsync()).Values.Count);
        }

        [Fact]
        public async Task QueryAsync_TakeCountLessThan0_ThrowsException()
        {
            await TableClient.CreateIfNotExistsAsync();

            await Assertions.JsonResponseThrowsAsync(
                async () => await TableClient.QueryAsync<TableEntity>(maxPerPage: -1).AsPages().FirstAsync(),
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
        public async Task QueryAsync_TakeCountGreaterThan1000_ThrowsException()
        {
            await TableClient.CreateIfNotExistsAsync();

            await Assertions.JsonResponseThrowsAsync(
                async () => await TableClient.QueryAsync<TableEntity>(maxPerPage: 1001).AsPages().FirstAsync(),
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
        public async Task QueryAsync_WhenUsingSelectColumns_ReturnsEntitiesWithSpecifiedColumns()
        {
            await _AddTestDataAsync();
            var query = TableClient.CreateQueryFilter<TableEntity>(tableEntity => tableEntity.PartitionKey == "partition-1" || tableEntity.PartitionKey == "partition-2");

            var entities = TableClient.QueryAsync<TableEntity>(query, select: new[]
            {
                nameof(TestQueryEntity.PartitionKey),
                nameof(TestQueryEntity.RowKey),
                nameof(TestQueryEntity.Int32Prop)
            });

            await _AssertResultAsync(
                entities,
                new string[] { nameof(TestQueryEntity.PartitionKey), nameof(TestQueryEntity.RowKey), nameof(TestQueryEntity.Int32Prop) },
                ("partition-1", "row-1"), ("partition-2", "row-2")
            );
            Assert.Collection(
                await entities.ToListAsync(),
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
        public async Task QueryAsync_WhenUsingStronglyTypedEntities_ReturnsAllEntities()
        {
            await _AddTestDataAsync();

            var entities = TableClient.QueryAsync<TestQueryEntity>();

            await _AssertResultAsync(
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
        public async Task QueryAsync_WhenUsingProperties_SetsValuesForEach()
        {
            var guid = Guid.NewGuid();
            var now = DateTime.UtcNow;
            await TableClient.CreateAsync();

            await TableClient.AddEntityAsync(new TableEntity
            {
                { "PartitionKey", "partition-1" },
                { "RowKey", "row-1" },
                { nameof(TestQueryEntity.StringProp), "test" },
                { nameof(TestQueryEntity.BinaryProp), new byte[] { 1, 2, 3 } },
                { nameof(TestQueryEntity.Int32Prop), 3 },
                { nameof(TestQueryEntity.Int64Prop), 3L },
                { nameof(TestQueryEntity.DoubleProp), 3D },
                { nameof(TestQueryEntity.GuidProp), guid },
                { nameof(TestQueryEntity.BoolProp), true },
                { nameof(TestQueryEntity.DateTimeProp), now },
                { nameof(TestQueryEntity.DateTimeOffsetProp), (DateTimeOffset)now }
            });

            var entities = await TableClient.QueryAsync<TestQueryEntity>().ToListAsync();

            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Equal("partition-1", entity.PartitionKey),
                () => Assert.Equal("row-1", entity.RowKey),
                () => Assert.Equal("test", entity.StringProp),
                () => Assert.Equal(new byte[] { 1, 2, 3 }, entity.BinaryProp),
                () => Assert.Equal(3, entity.Int32Prop),
                () => Assert.Equal(3L, entity.Int64Prop),
                () => Assert.Equal(3D, entity.DoubleProp),
                () => Assert.Equal(guid, entity.GuidProp),
                () => Assert.True(entity.BoolProp),
                () => Assert.Equal(now, entity.DateTimeProp),
                () => Assert.Equal((DateTimeOffset)now, entity.DateTimeOffsetProp)
            );
        }

        [Fact]
        public async Task QueryAsync_WhenUsingDifferentCasePropertyNames_DoesNotSetValues()
        {
            await TableClient.CreateAsync();

            await TableClient.AddEntityAsync(new TableEntity
            {
                { "PartitionKey", "partition-1" },
                { "RowKey", "row-1" },
                { nameof(TestQueryEntity.StringProp).ToUpperInvariant(), "test" },
                { nameof(TestQueryEntity.BinaryProp).ToUpperInvariant(), new byte[] { 1, 2, 3 } },
                { nameof(TestQueryEntity.Int32Prop).ToUpperInvariant(), 3 },
                { nameof(TestQueryEntity.Int64Prop).ToUpperInvariant(), 3L },
                { nameof(TestQueryEntity.DoubleProp).ToUpperInvariant(), 3D },
                { nameof(TestQueryEntity.GuidProp).ToUpperInvariant(), Guid.NewGuid() },
                { nameof(TestQueryEntity.BoolProp).ToUpperInvariant(), true },
                { nameof(TestQueryEntity.DateTimeProp).ToUpperInvariant(), DateTime.UtcNow },
                { nameof(TestQueryEntity.DateTimeOffsetProp).ToUpperInvariant(), DateTimeOffset.UtcNow }
            });

            var entities = await TableClient.QueryAsync<TestQueryEntity>().ToListAsync();

            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Equal("partition-1", entity.PartitionKey),
                () => Assert.Equal("row-1", entity.RowKey),
                () => Assert.Null(entity.StringProp),
                () => Assert.Null(entity.BinaryProp),
                () => Assert.Null(entity.Int32Prop),
                () => Assert.Null(entity.Int64Prop),
                () => Assert.Null(entity.DoubleProp),
                () => Assert.Null(entity.GuidProp),
                () => Assert.Null(entity.BoolProp),
                () => Assert.Null(entity.DateTimeProp),
                () => Assert.Null(entity.DateTimeOffsetProp)
            );
        }

        [Fact]
        public async Task QueryAsync_WhenUsingFields_SetsValuesForEach()
        {
            var guid = Guid.NewGuid();
            var now = DateTime.UtcNow;
            await TableClient.CreateAsync();

            await TableClient.AddEntityAsync(new TableEntity
            {
                { "PartitionKey", "partition-1" },
                { "RowKey", "row-1" },
                { nameof(TestQueryEntityFields.StringField), "test" },
                { nameof(TestQueryEntityFields.BinaryField), new byte[] { 1, 2, 3 } },
                { nameof(TestQueryEntityFields.Int32Field), 3 },
                { nameof(TestQueryEntityFields.Int64Field), 3L },
                { nameof(TestQueryEntityFields.DoubleField), 3D },
                { nameof(TestQueryEntityFields.GuidField), guid },
                { nameof(TestQueryEntityFields.BoolField), true },
                { nameof(TestQueryEntityFields.DateTimeField), now },
                { nameof(TestQueryEntityFields.DateTimeOffsetField), (DateTimeOffset)now }
            });

            var entities = await TableClient.QueryAsync<TestQueryEntityFields>().ToListAsync();

            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Equal("partition-1", entity.PartitionKey),
                () => Assert.Equal("row-1", entity.RowKey),
                () => Assert.Equal("test", entity.StringField),
                () => Assert.Equal(new byte[] { 1, 2, 3 }, entity.BinaryField),
                () => Assert.Equal(3, entity.Int32Field),
                () => Assert.Equal(3L, entity.Int64Field),
                () => Assert.Equal(3D, entity.DoubleField),
                () => Assert.Equal(guid, entity.GuidField),
                () => Assert.True(entity.BoolField),
                () => Assert.Equal(now, entity.DateTimeField),
                () => Assert.Equal((DateTimeOffset)now, entity.DateTimeOffsetField)
            );
        }

        [Fact]
        public async Task QueryAsync_WhenUsingDifferentCaseFieldNames_DoesNotSetValues()
        {
            await TableClient.CreateAsync();

            await TableClient.AddEntityAsync(new TableEntity
            {
                { "PartitionKey", "partition-1" },
                { "RowKey", "row-1" },
                { nameof(TestQueryEntityFields.StringField).ToUpperInvariant(), "test" },
                { nameof(TestQueryEntityFields.BinaryField).ToUpperInvariant(), new byte[] { 1, 2, 3 } },
                { nameof(TestQueryEntityFields.Int32Field).ToUpperInvariant(), 3 },
                { nameof(TestQueryEntityFields.Int64Field).ToUpperInvariant(), 3L },
                { nameof(TestQueryEntityFields.DoubleField).ToUpperInvariant(), 3D },
                { nameof(TestQueryEntityFields.GuidField).ToUpperInvariant(), Guid.NewGuid() },
                { nameof(TestQueryEntityFields.BoolField).ToUpperInvariant(), true },
                { nameof(TestQueryEntityFields.DateTimeField).ToUpperInvariant(), DateTime.UtcNow },
                { nameof(TestQueryEntityFields.DateTimeOffsetField).ToUpperInvariant(), DateTimeOffset.UtcNow }
            });

            var entities = await TableClient.QueryAsync<TestQueryEntityFields>().ToListAsync();

            var entity = Assert.Single(entities);
            Assert.Multiple(
                () => Assert.Equal("partition-1", entity.PartitionKey),
                () => Assert.Equal("row-1", entity.RowKey),
                () => Assert.Null(entity.StringField),
                () => Assert.Null(entity.BinaryField),
                () => Assert.Null(entity.Int32Field),
                () => Assert.Null(entity.Int64Field),
                () => Assert.Null(entity.DoubleField),
                () => Assert.Null(entity.GuidField),
                () => Assert.Null(entity.BoolField),
                () => Assert.Null(entity.DateTimeField),
                () => Assert.Null(entity.DateTimeOffsetField)
            );
        }

        [Fact]
        public async Task QueryAsync_WhenUsingPropertiesAndFieldsWithDifferentAccessModifiers_OnlySetsPublicMembers()
        {
            await TableClient.CreateAsync();

            await TableClient.AddEntityAsync(new TableEntity
            {
                { "PartitionKey", "partition-1" },
                { "RowKey", "row-1" },

                { "PublicProp", "public" },
                { "ProtectedProp", "protected" },
                { "InternalProp", "internal" },
                { "PrivateProp", "private" },
                { "ProtectedInternalProp", "protected internal" },
                { "PrivateProtectedProp", "private protected" },

                { "PublicField", "public" },
                { "ProtectedField", "protected" },
                { "InternalField", "internal" },
                { "PrivateField", "private" },
                { "ProtectedInternalField", "protected internal" },
                { "PrivateProtectedField", "private protected" }
            });

            var entities = await TableClient.QueryAsync<TestQueryEntityAccessModifier>().ToListAsync();

            var entity = Assert.Single(entities);

            Assert.Multiple(
                () => Assert.Equal("partition-1", entity.PartitionKey),
                () => Assert.Equal("row-1", entity.RowKey),

                () => Assert.Equal("public", typeof(TestQueryEntityAccessModifier).GetProperty("PublicProp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetProperty("ProtectedProp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetProperty("InternalProp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetProperty("PrivateProp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetProperty("ProtectedInternalProp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetProperty("PrivateProtectedProp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty).GetValue(entity)),

                () => Assert.Equal("public", typeof(TestQueryEntityAccessModifier).GetField("PublicField", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetField("ProtectedField", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetField("InternalField", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetField("PrivateField", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetField("ProtectedInternalField", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField).GetValue(entity)),
                () => Assert.Null(typeof(TestQueryEntityAccessModifier).GetField("PrivateProtectedField", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField).GetValue(entity))
            );
        }

        [Theory]
        [ClassData(typeof(TableDeserializationTestData))]
        public async Task QueryAsync_WhenDeserializingProperty_MayParseOrThrowException(string propertyName, object value, object expectedResult)
        {
            await TableClient.CreateAsync();
            await TableClient.AddEntityAsync(new TableEntity
            {
                { "PartitionKey", $"partition" },
                { "RowKey", "row" },
                { propertyName, value },
            });

            if (expectedResult is Exception expectedException)
            {
                var exception = await Assert.ThrowsAsync(expectedResult.GetType(), async () => await TableClient.QueryAsync<TestQueryEntity>().ToListAsync());
                Assert.Equal(expectedException.Message, exception.Message);
                Assert.Contains(exception.Source, new[] { "System.Private.CoreLib", "Azure.Data.Tables" });
            }
            else
            {
                var entities = await TableClient.QueryAsync<TestQueryEntity>().ToListAsync();
                var entity = Assert.Single(entities);
                var actualResult = typeof(TestQueryEntity)
                    .GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetProperty)
                    .GetValue(entity);
                Assert.Equal(expectedResult, actualResult);
            }
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

            if (!(filterValue is object))
                return $"{propertyName} {filterOperator} null";

            return $"{propertyName} {filterOperator} '{Convert.ToString(filterValue)}'";
        }

        private Task _AssertResultAsync<T>(AsyncPageable<T> entities, params (string, string)[] expectedItems)
            where T : ITableEntity
            => _AssertResultAsync(entities, Array.Empty<string>(), expectedItems);

        private async Task _AssertResultAsync<T>(AsyncPageable<T> entities, IEnumerable<string> selectedProperties, params (string, string)[] expectedItems)
            where T : ITableEntity
        {
            var processedItemsCount = 0;
            var previousContinuationToken = default(string);

            Assert.Multiple(
                await entities
                .AsPages()
                .Select(page => new Action(() =>
                {
                    _AssertResult(page, selectedProperties, previousContinuationToken, expectedItems.Skip(processedItemsCount).Take(page.Values.Count).ToArray());
                    processedItemsCount += page.Values.Count;
                    previousContinuationToken = page.ContinuationToken;
                }))
                .ToArrayAsync()
            );
        }

        private void _AssertResult<T>(Page<T> entities, params (string, string)[] expectedItems)
            where T : ITableEntity
            => _AssertResult(entities, Array.Empty<string>(), null, expectedItems);

        private void _AssertResult<T>(Page<T> entities, string continuationToken, params (string, string)[] expectedItems)
            where T : ITableEntity
            => _AssertResult(entities, Array.Empty<string>(), continuationToken, expectedItems);

        private void _AssertResult<T>(Page<T> entities, IEnumerable<string> selectedProperties, string previousContinuationToken, params (string, string)[] expectedItems)
            where T : ITableEntity
        {
            var resposne = entities.GetRawResponse();

            Assert.Multiple(
                () => Assert.Equal(expectedItems, entities.Values.Select(entity => (entity.PartitionKey, entity.RowKey))),
                () =>
                {
                    if (previousContinuationToken is object)
                    {
                        var firstEntity = entities.Values.First();

                        _AssertContinuationTokenPart(firstEntity.PartitionKey, previousContinuationToken.Split(' ').First());
                        _AssertContinuationTokenPart(firstEntity.RowKey, previousContinuationToken.Split(' ').Last());
                    }
                },
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
                        { "odata.metadata", $"https://cloudstubdev.table.core.windows.net/$metadata#{TableName}{(selectedProperties.Any() ? "&$select=" + string.Join(',', selectedProperties) : "")}" },
                        { "value", entities.Values.Select(entity => _MapEntityToDictionary(entity)).ToList() }
                    }
                })
            );
        }

        private static void _AssertContinuationTokenPart(string key, string tokenPart)
        {
            var encodedTokenParts = tokenPart.Split('!', 3);
            var version = encodedTokenParts[0];
            var encodedKeyLength = int.Parse(encodedTokenParts[1], NumberStyles.None, CultureInfo.InvariantCulture);
            var encodedKey = encodedTokenParts[2];

            var decodedKey = Encoding.UTF8.GetString(
                Convert.FromBase64String(
                    encodedKey.Replace("*", "+").Replace("-", "=").Replace("_", "/") + new string('=', encodedKey.Length % 4)
                )
            );

            Assert.Multiple(
                () => Assert.Equal("1", version),
                () => Assert.Equal(encodedKey.Length, encodedKeyLength),
                () => Assert.Equal(key, decodedKey)
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

        private async Task _AddTestDataAsync()
        {
            await TableClient.CreateIfNotExistsAsync();

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-1",
                RowKey = "row-1",
                StringProp = "test"
            });

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-2",
                RowKey = "row-2",
                Int32Prop = 3
            });

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-3",
                RowKey = "row-3",
                Int64Prop = 3
            });

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-4",
                RowKey = "row-4",
                DoubleProp = 3
            });

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-5",
                RowKey = "row-5",
                BinaryProp = Enumerable.Range(0, byte.MaxValue).Select(value => (byte)value).ToArray()
            });

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-6",
                RowKey = "row-6",
                DateTimeProp = new DateTime(2020, 1, 4, 0, 0, 0, DateTimeKind.Utc)
            });

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-7",
                RowKey = "row-7",
                DateTimeOffsetProp = new DateTimeOffset(2020, 1, 4, 0, 0, 0, TimeSpan.Zero)
            });

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-8",
                RowKey = "row-8",
                GuidProp = Guid.Parse("f32e99d4-05c7-4ed4-b75a-66d47e9d9e63")
            });

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-9",
                RowKey = "row-9",
                BoolProp = true
            });

            await TableClient.AddEntityAsync(new TestQueryEntity
            {
                PartitionKey = "partition-10",
                RowKey = "row-10"
            });
        }
    }
}