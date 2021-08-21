using System;
using System.Linq;
using CloudStub.Core;
using Xunit;

namespace CloudStub.Tests.Core
{
    public class StubTableTests
    {
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData("\t")]
        [InlineData("\n")]
        [InlineData("\r")]
        public void Initialize_WhenNameIsNullEmptyOrWhiteSpace_ThrowsException(string tableName)
        {
            var exception = Assert.Throws<ArgumentException>("name", () => new StubTable(tableName, null));
            Assert.Equal(new ArgumentException("The table name cannot be null, empty or white space.", "name").Message, exception.Message);
        }

        [Fact]
        public void Initialize_WhenTableStorageHandlerIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("tableStorageHandler", () => new StubTable("table-name", null));
            Assert.Equal(new ArgumentNullException("tableStorageHandler").Message, exception.Message);
        }

        [Fact]
        public void Initialize_InitializesName_HasNameSet()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            Assert.Equal("table-name", stubTable.Name);
        }

        [Fact]
        public void Initialize_WithoutRunningAnyOperations_DoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            Assert.False(stubTable.Exists());
        }

        [Fact]
        public void Initialize_WithPreviouslyCreatedTable_Exist()
        {
            var tableDataHandler = new InMemoryTableStorageHandler();
            tableDataHandler.Create("table-name");

            var stubTable = new StubTable("table-name", tableDataHandler);

            Assert.True(stubTable.Exists());
        }

        [Fact]
        public void Create_WhenTableDoesNotExist_ExecutesSuccessfully()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var createResult = stubTable.Create();

            Assert.Equal(StubTableCreateResult.Success, createResult);
            Assert.True(stubTable.Exists());
        }

        [Fact]
        public void Create_WhenTableExist_ReturnsTableAlreadyExists()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var createResult = stubTable.Create();

            Assert.Equal(StubTableCreateResult.TableAlreadyExists, createResult);
            Assert.True(stubTable.Exists());
        }

        [Fact]
        public void Delete_WhenTableDoesNotExists_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var deleteResult = stubTable.Delete();

            Assert.Equal(StubTableDeleteResult.TableDoesNotExist, deleteResult);
        }

        [Fact]
        public void Delete_WhenTableExists_ExecutesSuccessfully()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var deleteResult = stubTable.Delete();

            Assert.Equal(StubTableDeleteResult.Success, deleteResult);
            Assert.False(stubTable.Exists());
        }

        [Fact]
        public void Insert_WhenEntityIsNull_ThrowsException()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var exception = Assert.Throws<ArgumentNullException>("entity", () => stubTable.Insert(null));
            Assert.Equal(new ArgumentNullException("entity").Message, exception.Message);
        }

        [Fact]
        public void Insert_WhenTableDoesNotExist_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var insertResult = stubTable.Insert(new StubEntity("partition-key", "row-key"));

            Assert.Equal(StubTableInsertOperationResult.TableDoesNotExist, insertResult.OperationResult);
            Assert.Null(insertResult.Entity);
        }

        [Fact]
        public void Insert_WhenTableExists_InsertsTheEntitySuccessfully()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var insertResult = stubTable.Insert(new StubEntity("partition-key", "row-key"));

            Assert.Equal(StubTableInsertOperationResult.Success, insertResult.OperationResult);
            var queryResult = stubTable.Query(null, null);
            var entity = Assert.Single(queryResult.Entities);
            Assert.Equal("partition-key", entity.PartitionKey);
            Assert.Equal("row-key", entity.RowKey);

            Assert.NotNull(insertResult.Entity);
            Assert.Equal(entity.PartitionKey, insertResult.Entity.PartitionKey);
            Assert.Equal(entity.RowKey, insertResult.Entity.RowKey);
            Assert.Equal(entity.ETag, insertResult.Entity.ETag);
            Assert.Equal(entity.Timestamp, insertResult.Entity.Timestamp);
            Assert.Empty(insertResult.Entity.Properties);
        }

        [Fact]
        public void Insert_WhenEntityAlreadyExists_ReturnsEntityAlreadyExists()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key"));

            var insertResult = stubTable.Insert(new StubEntity("partition-key", "row-key"));

            Assert.Equal(StubTableInsertOperationResult.EntityAlreadyExists, insertResult.OperationResult);
            Assert.Null(insertResult.Entity);
        }

        [Fact]
        public void InsertOrMerge_WhenTableDoesNotExist_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var result = stubTable.InsertOrMerge(new StubEntity("partition-key", "row-key"));

            Assert.Equal(StubTableInsertOrMergeOperationResult.TableDoesNotExist, result.OperationResult);
            Assert.Null(result.Entity);
        }

        [Fact]
        public void InsertOrMerge_WhenEntityDoesNotExist_InsertsEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var result = stubTable.InsertOrMerge(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            Assert.Equal(StubTableInsertOrMergeOperationResult.Success, result.OperationResult);
            var insertedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("property-1", insertedEntity.Properties["property1"].Value);
            Assert.Equal("property-2", insertedEntity.Properties["property2"].Value);

            Assert.NotNull(result.Entity);
            Assert.Equal(insertedEntity.PartitionKey, result.Entity.PartitionKey);
            Assert.Equal(insertedEntity.RowKey, result.Entity.RowKey);
            Assert.Equal(insertedEntity.ETag, result.Entity.ETag);
            Assert.Equal(insertedEntity.Timestamp, result.Entity.Timestamp);

            Assert.Equal(insertedEntity.Properties.Count, result.Entity.Properties.Count);
            Assert.Equal(insertedEntity.Properties["property1"].Value, result.Entity.Properties["property1"].Value);
            Assert.Equal(insertedEntity.Properties["property2"].Value, result.Entity.Properties["property2"].Value);
        }

        [Fact]
        public void InsertOrMerge_WhenEntityExists_MergesTheEntities()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.InsertOrMerge(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property2", new StubEntityProperty("property-2-merge") },
                    { "property3", new StubEntityProperty("property-3-merge") }
                }
            });

            Assert.Equal(StubTableInsertOrMergeOperationResult.Success, result.OperationResult);
            var insertedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("property-1", insertedEntity.Properties["property1"].Value);
            Assert.Equal("property-2-merge", insertedEntity.Properties["property2"].Value);
            Assert.Equal("property-3-merge", insertedEntity.Properties["property3"].Value);

            Assert.NotNull(result.Entity);
            Assert.Equal(insertedEntity.PartitionKey, result.Entity.PartitionKey);
            Assert.Equal(insertedEntity.RowKey, result.Entity.RowKey);
            Assert.Equal(insertedEntity.ETag, result.Entity.ETag);
            Assert.Equal(insertedEntity.Timestamp, result.Entity.Timestamp);

            Assert.Equal(insertedEntity.Properties.Count, result.Entity.Properties.Count);
            Assert.Equal(insertedEntity.Properties["property1"].Value, result.Entity.Properties["property1"].Value);
            Assert.Equal(insertedEntity.Properties["property2"].Value, result.Entity.Properties["property2"].Value);
            Assert.Equal(insertedEntity.Properties["property3"].Value, result.Entity.Properties["property3"].Value);
        }

        [Fact]
        public void InsertOrReplace_WhenTableDoesNotExist_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var result = stubTable.InsertOrReplace(new StubEntity("partition-key", "row-key"));

            Assert.Equal(StubTableInsertOrReplaceOperationResult.TableDoesNotExist, result.OperationResult);
            Assert.Null(result.Entity);
        }

        [Fact]
        public void InsertOrReplace_WhenEntityDoesNotExist_InsertsEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var result = stubTable.InsertOrReplace(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            Assert.Equal(StubTableInsertOrReplaceOperationResult.Success, result.OperationResult);
            var insertedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("property-1", insertedEntity.Properties["property1"].Value);
            Assert.Equal("property-2", insertedEntity.Properties["property2"].Value);

            Assert.NotNull(result.Entity);
            Assert.Equal(insertedEntity.PartitionKey, result.Entity.PartitionKey);
            Assert.Equal(insertedEntity.RowKey, result.Entity.RowKey);
            Assert.Equal(insertedEntity.ETag, result.Entity.ETag);
            Assert.Equal(insertedEntity.Timestamp, result.Entity.Timestamp);

            Assert.Equal(insertedEntity.Properties.Count, result.Entity.Properties.Count);
            Assert.Equal(insertedEntity.Properties["property1"].Value, result.Entity.Properties["property1"].Value);
            Assert.Equal(insertedEntity.Properties["property2"].Value, result.Entity.Properties["property2"].Value);
        }

        [Fact]
        public void InsertOrReplace_WhenEntityExists_ReplacesTheExistingEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.InsertOrReplace(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property2", new StubEntityProperty("property-2-replaced") },
                    { "property3", new StubEntityProperty("property-3-replaced") }
                }
            });

            Assert.Equal(StubTableInsertOrReplaceOperationResult.Success, result.OperationResult);
            var insertedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.False(insertedEntity.Properties.ContainsKey("property1"));
            Assert.Equal("property-2-replaced", insertedEntity.Properties["property2"].Value);
            Assert.Equal("property-3-replaced", insertedEntity.Properties["property3"].Value);

            Assert.NotNull(result.Entity);
            Assert.Equal(insertedEntity.PartitionKey, result.Entity.PartitionKey);
            Assert.Equal(insertedEntity.RowKey, result.Entity.RowKey);
            Assert.Equal(insertedEntity.ETag, result.Entity.ETag);
            Assert.Equal(insertedEntity.Timestamp, result.Entity.Timestamp);

            Assert.Equal(insertedEntity.Properties.Count, result.Entity.Properties.Count);
            Assert.Equal(insertedEntity.Properties["property2"].Value, result.Entity.Properties["property2"].Value);
            Assert.Equal(insertedEntity.Properties["property3"].Value, result.Entity.Properties["property3"].Value);
        }

        [Fact]
        public void Merge_WhenTableDoesNotExist_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var result = stubTable.Merge(new StubEntity("partition-key", "row-key"));

            Assert.Equal(StubTableMergeOperationResult.TableDoesNotExist, result.OperationResult);
            Assert.Null(result.Entity);
        }

        [Fact]
        public void Merge_WhenEntityDoesNotExist_ReturnsEntityDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var result = stubTable.Merge(new StubEntity("partition-key", "row-key"));

            Assert.Equal(StubTableMergeOperationResult.EntityDoesNotExists, result.OperationResult);
            Assert.Null(result.Entity);
        }

        [Fact]
        public void Merge_WhenEntityDoesNotHaveMatchingEtag_ReturnsEtagsDoNotMatch()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key"));

            var result = stubTable.Merge(new StubEntity("partition-key", "row-key", "unmatching-etag"));

            Assert.Equal(StubTableMergeOperationResult.EtagsDoNotMatch, result.OperationResult);
            Assert.Null(result.Entity);
        }

        [Fact]
        public void Merge_WhenEntityUsesGenericEtag_MergesTheEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.Merge(new StubEntity("partition-key", "row-key", "*")
            {
                Properties =
                {
                    { "property2", new StubEntityProperty("property-2-merge") },
                    { "property3", new StubEntityProperty("property-3-merge") }
                }
            });

            Assert.Equal(StubTableMergeOperationResult.Success, result.OperationResult);
            var mergedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("partition-key", mergedEntity.PartitionKey);
            Assert.Equal("row-key", mergedEntity.RowKey);
            Assert.NotEmpty(mergedEntity.ETag);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) <= mergedEntity.Timestamp && mergedEntity.Timestamp <= DateTime.UtcNow.AddMinutes(1));
            Assert.Equal("property-1", (string)mergedEntity.Properties["property1"].Value);
            Assert.Equal("property-2-merge", (string)mergedEntity.Properties["property2"].Value);
            Assert.Equal("property-3-merge", (string)mergedEntity.Properties["property3"].Value);

            Assert.NotNull(result.Entity);
            Assert.Equal(mergedEntity.PartitionKey, result.Entity.PartitionKey);
            Assert.Equal(mergedEntity.RowKey, result.Entity.RowKey);
            Assert.Equal(mergedEntity.ETag, result.Entity.ETag);
            Assert.Equal(mergedEntity.Timestamp, result.Entity.Timestamp);

            Assert.Equal(mergedEntity.Properties.Count, result.Entity.Properties.Count);
            Assert.Equal(mergedEntity.Properties["property1"].Value, result.Entity.Properties["property1"].Value);
            Assert.Equal(mergedEntity.Properties["property2"].Value, result.Entity.Properties["property2"].Value);
            Assert.Equal(mergedEntity.Properties["property3"].Value, result.Entity.Properties["property3"].Value);
        }

        [Fact]
        public void Merge_WhenEntityUsesMatchingEtag_MergesTheEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });
            var etag = stubTable.Query(new StubTableQuery(), default).Entities.Single().ETag;

            var result = stubTable.Merge(new StubEntity("partition-key", "row-key", etag)
            {
                Properties =
                {
                    { "property2", new StubEntityProperty("property-2-merge") },
                    { "property3", new StubEntityProperty("property-3-merge") }
                }
            });

            Assert.Equal(StubTableMergeOperationResult.Success, result.OperationResult);
            var mergedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("partition-key", mergedEntity.PartitionKey);
            Assert.Equal("row-key", mergedEntity.RowKey);
            Assert.NotEmpty(mergedEntity.ETag);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) <= mergedEntity.Timestamp && mergedEntity.Timestamp <= DateTime.UtcNow.AddMinutes(1));
            Assert.Equal("property-1", (string)mergedEntity.Properties["property1"].Value);
            Assert.Equal("property-2-merge", (string)mergedEntity.Properties["property2"].Value);
            Assert.Equal("property-3-merge", (string)mergedEntity.Properties["property3"].Value);

            Assert.NotNull(result.Entity);
            Assert.Equal(mergedEntity.PartitionKey, result.Entity.PartitionKey);
            Assert.Equal(mergedEntity.RowKey, result.Entity.RowKey);
            Assert.Equal(mergedEntity.ETag, result.Entity.ETag);
            Assert.Equal(mergedEntity.Timestamp, result.Entity.Timestamp);

            Assert.Equal(mergedEntity.Properties.Count, result.Entity.Properties.Count);
            Assert.Equal(mergedEntity.Properties["property1"].Value, result.Entity.Properties["property1"].Value);
            Assert.Equal(mergedEntity.Properties["property2"].Value, result.Entity.Properties["property2"].Value);
            Assert.Equal(mergedEntity.Properties["property3"].Value, result.Entity.Properties["property3"].Value);
        }

        [Fact]
        public void Replace_WhenTableDoesNotExist_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var result = stubTable.Replace(new StubEntity("partition-key", "row-key"));

            Assert.Equal(StubTableReplaceOperationResult.TableDoesNotExist, result);
        }

        [Fact]
        public void Replace_WhenEntityDoesNotExist_ReturnsEntityDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var result = stubTable.Replace(new StubEntity("partition-key", "row-key"));

            Assert.Equal(StubTableReplaceOperationResult.EntityDoesNotExists, result);
        }

        [Fact]
        public void Replace_WhenEntityDoesNotHaveMatchingEtag_ReturnsEtagsDoNotMatch()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key"));

            var result = stubTable.Replace(new StubEntity("partition-key", "row-key", "unmatching-etag"));

            Assert.Equal(StubTableReplaceOperationResult.EtagsDoNotMatch, result);
        }

        [Fact]
        public void Replace_WhenEntityUsesGenericEtag_ReplacesTheEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.Replace(new StubEntity("partition-key", "row-key", "*")
            {
                Properties =
                {
                    { "property2", new StubEntityProperty("property-2-replaced") },
                    { "property3", new StubEntityProperty("property-3-replaced") }
                }
            });

            Assert.Equal(StubTableReplaceOperationResult.Success, result);
            var replacedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("partition-key", replacedEntity.PartitionKey);
            Assert.Equal("row-key", replacedEntity.RowKey);
            Assert.NotEmpty(replacedEntity.ETag);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) <= replacedEntity.Timestamp && replacedEntity.Timestamp <= DateTime.UtcNow.AddMinutes(1));
            Assert.False(replacedEntity.Properties.ContainsKey("property1"));
            Assert.Equal("property-2-replaced", (string)replacedEntity.Properties["property2"].Value);
            Assert.Equal("property-3-replaced", (string)replacedEntity.Properties["property3"].Value);
        }

        [Fact]
        public void Replace_WhenEntityUsesMatchingEtag_ReplacesTheEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });
            var etag = stubTable.Query(new StubTableQuery(), default).Entities.Single().ETag;

            var result = stubTable.Replace(new StubEntity("partition-key", "row-key", etag)
            {
                Properties =
                {
                    { "property2", new StubEntityProperty("property-2-replaced") },
                    { "property3", new StubEntityProperty("property-3-replaced") }
                }
            });

            Assert.Equal(StubTableReplaceOperationResult.Success, result);
            var replacedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("partition-key", replacedEntity.PartitionKey);
            Assert.Equal("row-key", replacedEntity.RowKey);
            Assert.NotEmpty(replacedEntity.ETag);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) <= replacedEntity.Timestamp && replacedEntity.Timestamp <= DateTime.UtcNow.AddMinutes(1));
            Assert.False(replacedEntity.Properties.ContainsKey("property1"));
            Assert.Equal("property-2-replaced", (string)replacedEntity.Properties["property2"].Value);
            Assert.Equal("property-3-replaced", (string)replacedEntity.Properties["property3"].Value);
        }

        [Fact]
        public void Delete_WhenTableDoesNotExist_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var result = stubTable.Delete(new StubEntity("partition-key", "row-key"));

            Assert.Equal(StubTableDeleteOperationResult.TableDoesNotExist, result);
        }

        [Fact]
        public void Delete_WhenEntityDoesNotExist_ReturnsEntityDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var result = stubTable.Delete(new StubEntity("partition-key", "row-key"));

            Assert.Equal(StubTableDeleteOperationResult.EntityDoesNotExists, result);
        }

        [Fact]
        public void Delete_WhenEntityDoesNotHaveMatchingEtag_ReturnsEtagsDoNotMatch()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key"));

            var result = stubTable.Delete(new StubEntity("partition-key", "row-key", "unmatching-etag"));

            Assert.Equal(StubTableDeleteOperationResult.EtagsDoNotMatch, result);
        }

        [Fact]
        public void Delete_WhenEntityUsesGenericEtag_DeletesTheEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key"));

            var result = stubTable.Delete(new StubEntity("partition-key", "row-key", "*"));

            Assert.Equal(StubTableDeleteOperationResult.Success, result);
            Assert.Empty(stubTable.Query(new StubTableQuery(), default).Entities);
        }

        [Fact]
        public void Delete_WhenEntityUsesMatchingEtag_DeletesTheEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key"));
            var etag = stubTable.Query(new StubTableQuery(), default).Entities.Single().ETag;

            var result = stubTable.Delete(new StubEntity("partition-key", "row-key", etag));

            Assert.Equal(StubTableDeleteOperationResult.Success, result);
            Assert.Empty(stubTable.Query(new StubTableQuery(), default).Entities);
        }

        [Fact]
        public void Delete_WhenEntityUsesMatchingEtag_DeletesJustThatEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key", "row-key-2"));

            var result = stubTable.Delete(new StubEntity("partition-key", "row-key-1", "*"));

            Assert.Equal(StubTableDeleteOperationResult.Success, result);
            var remainingEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("partition-key", remainingEntity.PartitionKey);
            Assert.Equal("row-key-2", remainingEntity.RowKey);
        }

        [Fact]
        public void Retrieve_WhenTableDoesNotExist_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var result = stubTable.Retrieve("partition-key", "row-key");

            Assert.Equal(StubTableRetrieveOperationResult.TableDoesNotExist, result.RetrieveResult);
            Assert.Null(result.Entity);
        }

        [Fact]
        public void Retrieve_WhenEntityDoesNotExist_ReturnsEntityDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var result = stubTable.Retrieve("partition-key", "row-key");

            Assert.Equal(StubTableRetrieveOperationResult.EntityDoesNotExists, result.RetrieveResult);
            Assert.Null(result.Entity);
        }

        [Fact]
        public void Retrieve_WhenEntityExists_ReturnsMatchedEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.Retrieve("partition-key", "row-key");

            Assert.Equal(StubTableRetrieveOperationResult.Success, result.RetrieveResult);
            Assert.NotNull(result.Entity);
            Assert.Equal("partition-key", result.Entity.PartitionKey);
            Assert.Equal("row-key", result.Entity.RowKey);
            Assert.Equal("property-1", result.Entity.Properties["property1"].Value);
            Assert.Equal("property-2", result.Entity.Properties["property2"].Value);
        }

        [Fact]
        public void Retrieve_WhenEntityExists_ReturnsMatchedEntityOnlyWithSelectedProperties()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.Retrieve("partition-key", "row-key", new[] { "property2" });

            Assert.Equal(StubTableRetrieveOperationResult.Success, result.RetrieveResult);
            Assert.NotNull(result.Entity);
            Assert.Equal("partition-key", result.Entity.PartitionKey);
            Assert.Equal("row-key", result.Entity.RowKey);
            Assert.False(result.Entity.Properties.ContainsKey("property1"));
            Assert.Equal("property-2", result.Entity.Properties["property2"].Value);
        }

        [Fact]
        public void Retrieve_WhenEntityExists_ReturnsMatchedEntityWithCorePropertiesWhenNoneAreSelected()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.Retrieve("partition-key", "row-key", Enumerable.Empty<string>());

            Assert.Equal(StubTableRetrieveOperationResult.Success, result.RetrieveResult);
            Assert.NotNull(result.Entity);
            Assert.Equal("partition-key", result.Entity.PartitionKey);
            Assert.Equal("row-key", result.Entity.RowKey);
            Assert.False(result.Entity.Properties.ContainsKey("property1"));
            Assert.False(result.Entity.Properties.ContainsKey("property2"));
        }

        [Fact]
        public void Query_WhenTableDoesNotExist_ReturnsTableDoesNotExists()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var queryResult = stubTable.Query(null, null);

            Assert.Equal(StubTableQueryResult.TableDoesNotExist, queryResult.OperationResult);
            Assert.Null(queryResult.Entities);
            Assert.Null(queryResult.ContinuationToken);
        }

        [Fact]
        public void Query_WhenTableExistsAndHasNoEntities_ReturnsEmptyResult()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var queryResult = stubTable.Query(null, null);

            Assert.Equal(StubTableQueryResult.Success, queryResult.OperationResult);
            Assert.Empty(queryResult.Entities);
            Assert.Null(queryResult.ContinuationToken);
        }

        [Fact]
        public void Query_WhenTableContainsEntityWithProperties_ReturnsTheEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "byte-property", new StubEntityProperty(new byte[] { 0x00, 0x01, 0x02 }) },
                    { "boolean-property", new StubEntityProperty(true) },
                    { "int32-property", new StubEntityProperty(default(int)) },
                    { "int64-property", new StubEntityProperty(default(long)) },
                    { "double-property", new StubEntityProperty(default(double)) },
                    { "guid-property", new StubEntityProperty(default(Guid)) },
                    { "dateTime-property", new StubEntityProperty(new DateTime(2021, 5, 15, 0, 0, 0, DateTimeKind.Utc)) },
                    { "string-property", new StubEntityProperty("string") }
                }
            });

            var queryResult = stubTable.Query(null, null);

            Assert.Equal(StubTableQueryResult.Success, queryResult.OperationResult);
            var entity = Assert.Single(queryResult.Entities);
            Assert.Null(queryResult.ContinuationToken);
            Assert.Equal("partition-key", entity.PartitionKey);
            Assert.Equal("row-key", entity.RowKey);
            Assert.NotEmpty(entity.ETag);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) <= entity.Timestamp && entity.Timestamp <= DateTime.UtcNow.AddMinutes(1));
            Assert.Equal(new byte[] { 0x00, 0x01, 0x02 }, (byte[])entity.Properties["byte-property"].Value);
            Assert.True((bool)entity.Properties["boolean-property"].Value);
            Assert.Equal(0, (int)entity.Properties["int32-property"].Value);
            Assert.Equal(0L, (long)entity.Properties["int64-property"].Value);
            Assert.Equal(0.0, (double)entity.Properties["double-property"].Value);
            Assert.Equal(default(Guid), (Guid)entity.Properties["guid-property"].Value);
            Assert.Equal(new DateTime(2021, 5, 15, 0, 0, 0, DateTimeKind.Utc), (DateTime)entity.Properties["dateTime-property"].Value);
            Assert.Equal("string", (string)entity.Properties["string-property"].Value);
        }

        [Fact]
        public void Query_WhenTableContainsMultipleEntities_ReturnsThemSortedByPartitionKeyThenByRowKey()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key-5", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-5", "row-key-2"));
            stubTable.Insert(new StubEntity("partition-key-3", "row-key-2"));
            stubTable.Insert(new StubEntity("partition-key-3", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-1", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-1", "row-key-2"));
            stubTable.Insert(new StubEntity("partition-key-2", "row-key-2"));
            stubTable.Insert(new StubEntity("partition-key-2", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-4", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-4", "row-key-2"));

            var queryResult = stubTable.Query(null, null);

            Assert.Equal(StubTableQueryResult.Success, queryResult.OperationResult);
            Assert.Null(queryResult.ContinuationToken);
            Assert.Equal(
                Enumerable
                    .Range(1, 5)
                    .SelectMany(partitionNumber => new[]
                    {
                        new { PartitionKey = $"partition-key-{partitionNumber}", RowKey = "row-key-1" },
                        new { PartitionKey = $"partition-key-{partitionNumber}", RowKey = "row-key-2" }
                    })
                    .ToArray(),
                queryResult
                    .Entities
                    .Select(entity => new { entity.PartitionKey, entity.RowKey })
                    .ToArray()
            );
        }

        [Theory]
        [InlineData("")]
        [InlineData("int32-property")]
        [InlineData("byte-property,double-property")]
        public void Query_WhenTableContainsEntityWithProperties_ReturnsOnlySelectedProperties(string selectedProperties)
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "byte-property", new StubEntityProperty(new byte[] { 0x00, 0x01, 0x02 }) },
                    { "boolean-property", new StubEntityProperty(true) },
                    { "int32-property", new StubEntityProperty(default(int)) },
                    { "int64-property", new StubEntityProperty(default(long)) },
                    { "double-property", new StubEntityProperty(default(double)) },
                    { "guid-property", new StubEntityProperty(default(Guid)) },
                    { "dateTime-property", new StubEntityProperty(new DateTime(2021, 5, 15, 0, 0, 0, DateTimeKind.Utc)) },
                    { "string-property", new StubEntityProperty("string") }
                }
            });

            var queryResult = stubTable.Query(new StubTableQuery { SelectedProperties = selectedProperties.Split(',', StringSplitOptions.RemoveEmptyEntries) }, null);

            Assert.Equal(StubTableQueryResult.Success, queryResult.OperationResult);
            var entity = Assert.Single(queryResult.Entities);
            Assert.Null(queryResult.ContinuationToken);
            Assert.Equal("partition-key", entity.PartitionKey);
            Assert.Equal("row-key", entity.RowKey);
            Assert.NotEmpty(entity.ETag);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) <= entity.Timestamp && entity.Timestamp <= DateTime.UtcNow.AddMinutes(1));
            Assert.Equal(selectedProperties.Split(',', StringSplitOptions.RemoveEmptyEntries), entity.Properties.Keys.ToArray());
        }

        [Fact]
        public void Query_WhenTableContainsMultipleEntities_ReturnsThemPaginated()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key-5", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-5", "row-key-2"));
            stubTable.Insert(new StubEntity("partition-key-3", "row-key-2"));
            stubTable.Insert(new StubEntity("partition-key-3", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-1", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-1", "row-key-2"));
            stubTable.Insert(new StubEntity("partition-key-2", "row-key-2"));
            stubTable.Insert(new StubEntity("partition-key-2", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-4", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-4", "row-key-2"));

            var iterationCount = 0;
            var query = new StubTableQuery { PageSize = 2 };
            StubTableQueryContinuationToken continuationToken = null;
            foreach (var partitionNumber in Enumerable.Range(1, 5))
            {
                var queryResult = stubTable.Query(query, continuationToken);

                continuationToken = queryResult.ContinuationToken;
                Assert.Equal(StubTableQueryResult.Success, queryResult.OperationResult);
                Assert.Equal(
                    new[]
                    {
                        new { PartitionKey = $"partition-key-{partitionNumber}", RowKey = "row-key-1" },
                        new { PartitionKey = $"partition-key-{partitionNumber}", RowKey = "row-key-2" }
                    },
                    queryResult
                        .Entities
                        .Select(entity => new { entity.PartitionKey, entity.RowKey })
                        .ToArray()
                );

                iterationCount++;
            }
            Assert.Null(continuationToken);
            Assert.Equal(5, iterationCount);
        }

        [Fact]
        public void Query_WhenTableContainsMultipleEntities_ReturnsThemFiltered()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key-5", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-5", "row-key-2"));
            stubTable.Insert(new StubEntity("partition-key-3", "row-key-2"));
            stubTable.Insert(new StubEntity("partition-key-3", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-1", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-1", "row-key-2"));
            stubTable.Insert(new StubEntity("partition-key-2", "row-key-2"));
            stubTable.Insert(new StubEntity("partition-key-2", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-4", "row-key-1"));
            stubTable.Insert(new StubEntity("partition-key-4", "row-key-2"));

            var queryResult = stubTable.Query(new StubTableQuery { Filter = entity => entity.RowKey == "row-key-1" }, null);

            Assert.Equal(StubTableQueryResult.Success, queryResult.OperationResult);
            Assert.Null(queryResult.ContinuationToken);
            Assert.Equal(
                Enumerable
                    .Range(1, 5)
                    .Select(partitionNumber => new { PartitionKey = $"partition-key-{partitionNumber}", RowKey = "row-key-1" })
                    .ToArray(),
                queryResult
                    .Entities
                    .Select(entity => new { entity.PartitionKey, entity.RowKey })
                    .ToArray()
            );
        }

        [Fact]
        public void BatchOperation_WhenHavingNoOperations_IsSuccessful()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var batchOperationResult = stubTable.BatchOperation().Execute();

            Assert.Equal(StubTableBatchOperationResult.Success, batchOperationResult.OperationResult);
            Assert.Null(batchOperationResult.Index);
        }

        [Fact]
        public void BatchOperation_WhenTableDoesNotExist_ReturnsTableDoesNotExistResult()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var batchOperationResult = stubTable.BatchOperation().Insert(new StubEntity("partition-key", "row-key")).Execute();

            Assert.Equal(StubTableBatchOperationResult.TableDoesNotExist, batchOperationResult.OperationResult);
            Assert.Null(batchOperationResult.Index);
        }

        [Fact]
        public void BatchOperation_WhenTwoOperationsHaveTwoDifferentPartitionKeys_ThrowsException()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var exception = Assert.Throws<ArgumentException>(() => stubTable
                .BatchOperation()
                .Insert(new StubEntity("partition-key-1", "row-key"))
                .Insert(new StubEntity("partition-key-2", "row-key"))
                .Execute()
            );

            Assert.Equal(new ArgumentException("Batch operations can be performed only on the same partition.", "entity").Message, exception.Message);
        }

        [Fact]
        public void BatchOperation_WhenOneOperationFails_TheEntireBatchOperationFails()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var batchOperationResult = stubTable
                .BatchOperation()
                .Insert(new StubEntity("partition-key", "row-key-2"))
                .Delete(new StubEntity("partition-key", "row-key-1"))
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.EntityDoesNotExist, batchOperationResult.OperationResult);
            Assert.Equal(1, batchOperationResult.Index);
            Assert.Empty(stubTable.Query(new StubTableQuery(), default).Entities);
        }

        [Fact]
        public void BatchOperation_WhenAllOperationsSucceed_TheBatchOperationSucceeds()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var batchOperationResult = stubTable
                .BatchOperation()
                .Insert(new StubEntity("partition-key", "row-key-1"))
                .Insert(new StubEntity("partition-key", "row-key-2"))
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.Success, batchOperationResult.OperationResult);
            Assert.Null(batchOperationResult.Index);
            var entities = stubTable.Query(new StubTableQuery(), default).Entities;
            Assert.Equal(2, entities.Count);
            Assert.Single(entities, entity => entity.PartitionKey == "partition-key" && entity.RowKey == "row-key-1");
            Assert.Single(entities, entity => entity.PartitionKey == "partition-key" && entity.RowKey == "row-key-2");
        }

        [Fact]
        public void BatchOperation_WhenInsertingEntity_TheBatchOperationSucceeds()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var batchOperationResult = stubTable
                .BatchOperation()
                .Insert(new StubEntity("partition-key", "row-key"))
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.Success, batchOperationResult.OperationResult);
            Assert.Null(batchOperationResult.Index);
            var entities = stubTable.Query(new StubTableQuery(), default).Entities;
            Assert.Single(entities, entity => entity.PartitionKey == "partition-key" && entity.RowKey == "row-key");
        }

        [Fact]
        public void BatchOperation_WhenInsertingExistingEntity_TheBatchOperationFails()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key"));

            var batchOperationResult = stubTable
                .BatchOperation()
                .Insert(new StubEntity("partition-key", "row-key"))
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.EntityAlreadyExist, batchOperationResult.OperationResult);
            Assert.Equal(0, batchOperationResult.Index);
            var entities = stubTable.Query(new StubTableQuery(), default).Entities;
            Assert.Single(entities, entity => entity.PartitionKey == "partition-key" && entity.RowKey == "row-key");
        }

        [Fact]
        public void BatchOperation_WhenInsertingOrMergingEntityThatDoesNotExists_TheBatchOperationSucceeds()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key"));

            var batchOperationResult = stubTable
                .BatchOperation()
                .InsertOrMerge(new StubEntity("partition-key", "row-key"))
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.Success, batchOperationResult.OperationResult);
            Assert.Null(batchOperationResult.Index);
            var entities = stubTable.Query(new StubTableQuery(), default).Entities;
            Assert.Single(entities, entity => entity.PartitionKey == "partition-key" && entity.RowKey == "row-key");
        }

        [Fact]
        public void BatchOperation_WhenInsertingOrMergingEntityThatExists_TheBatchOperationSucceeds()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var batchOperationResult = stubTable
                .BatchOperation()
                .InsertOrMerge(new StubEntity("partition-key", "row-key")
                {
                    Properties =
                    {
                        { "property2", new StubEntityProperty("property-2-merge") },
                        { "property3", new StubEntityProperty("property-3-merge") }
                    }
                })
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.Success, batchOperationResult.OperationResult);
            Assert.Null(batchOperationResult.Index);
            var entities = stubTable.Query(new StubTableQuery(), default).Entities;
            var entity = Assert.Single(entities, entity => entity.PartitionKey == "partition-key" && entity.RowKey == "row-key");
            Assert.Equal("property-1", entity.Properties["property1"].Value);
            Assert.Equal("property-2-merge", entity.Properties["property2"].Value);
            Assert.Equal("property-3-merge", entity.Properties["property3"].Value);
        }

        [Fact]
        public void BatchOperation_WhenInsertingOrReplacingEntityThatDoesNotExists_TheBatchOperationSucceeds()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key"));

            var batchOperationResult = stubTable
                .BatchOperation()
                .InsertOrReplace(new StubEntity("partition-key", "row-key"))
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.Success, batchOperationResult.OperationResult);
            Assert.Null(batchOperationResult.Index);
            var entities = stubTable.Query(new StubTableQuery(), default).Entities;
            Assert.Single(entities, entity => entity.PartitionKey == "partition-key" && entity.RowKey == "row-key");
        }

        [Fact]
        public void BatchOperation_WhenInsertingOrReplacingEntityThatExists_TheBatchOperationSucceeds()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity("partition-key", "row-key")
            {
                Properties =
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var batchOperationResult = stubTable
                .BatchOperation()
                .InsertOrReplace(new StubEntity("partition-key", "row-key")
                {
                    Properties =
                    {
                        { "property2", new StubEntityProperty("property-2-replace") },
                        { "property3", new StubEntityProperty("property-3-replace") }
                    }
                })
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.Success, batchOperationResult.OperationResult);
            Assert.Null(batchOperationResult.Index);
            var entities = stubTable.Query(new StubTableQuery(), default).Entities;
            var entity = Assert.Single(entities, entity => entity.PartitionKey == "partition-key" && entity.RowKey == "row-key");
            Assert.False(entity.Properties.ContainsKey("property1"));
            Assert.Equal("property-2-replace", entity.Properties["property2"].Value);
            Assert.Equal("property-3-replace", entity.Properties["property3"].Value);
        }

        [Fact]
        public void BatchOperation_WhenMergingEntityThatDoesNotExist_TheBatchOperationFails()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var batchOperationResult = stubTable
                .BatchOperation()
                .Merge(new StubEntity("partition-key", "row-key"))
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.EntityDoesNotExist, batchOperationResult.OperationResult);
            Assert.Equal(0, batchOperationResult.Index);
            Assert.Empty(stubTable.Query(new StubTableQuery(), default).Entities);
        }

        [Fact]
        public void BatchOperation_WhenMergingExistingEntity_TheBatchOperationSucceeds()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var batchOperationResult = stubTable
                .BatchOperation()
                .Insert(new StubEntity("partition-key", "row-key")
                {
                    Properties =
                    {
                        { "property1", new StubEntityProperty("property-1") },
                        { "property2", new StubEntityProperty("property-2") }
                    }
                })
                .Merge(new StubEntity("partition-key", "row-key", "*")
                {
                    Properties =
                    {
                        { "property2", new StubEntityProperty("property-2-merge") },
                        { "property3", new StubEntityProperty("property-3-merge") }
                    }
                })
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.Success, batchOperationResult.OperationResult);
            Assert.Null(batchOperationResult.Index);
            var entities = stubTable.Query(new StubTableQuery(), default).Entities;
            var entity = Assert.Single(entities, entity => entity.PartitionKey == "partition-key" && entity.RowKey == "row-key");
            Assert.Equal("property-1", entity.Properties["property1"].Value);
            Assert.Equal("property-2-merge", entity.Properties["property2"].Value);
            Assert.Equal("property-3-merge", entity.Properties["property3"].Value);
        }

        [Fact]
        public void BatchOperation_WhenReplacingEntityThatDoesNotExist_TheBatchOperationFails()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var batchOperationResult = stubTable
                .BatchOperation()
                .Replace(new StubEntity("partition-key", "row-key"))
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.EntityDoesNotExist, batchOperationResult.OperationResult);
            Assert.Equal(0, batchOperationResult.Index);
            Assert.Empty(stubTable.Query(new StubTableQuery(), default).Entities);
        }

        [Fact]
        public void BatchOperation_WhenReplacingExistingEntity_TheBatchOperationSucceeds()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var batchOperationResult = stubTable
                .BatchOperation()
                .Insert(new StubEntity("partition-key", "row-key")
                {
                    Properties =
                    {
                        { "property1", new StubEntityProperty("property-1") },
                        { "property2", new StubEntityProperty("property-2") }
                    }
                })
                .Replace(new StubEntity("partition-key", "row-key", "*")
                {
                    Properties =
                    {
                        { "property2", new StubEntityProperty("property-2-replace") },
                        { "property3", new StubEntityProperty("property-3-replace") }
                    }
                })
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.Success, batchOperationResult.OperationResult);
            Assert.Null(batchOperationResult.Index);
            var entities = stubTable.Query(new StubTableQuery(), default).Entities;
            var entity = Assert.Single(entities, entity => entity.PartitionKey == "partition-key" && entity.RowKey == "row-key");
            Assert.False(entity.Properties.ContainsKey("property1"));
            Assert.Equal("property-2-replace", entity.Properties["property2"].Value);
            Assert.Equal("property-3-replace", entity.Properties["property3"].Value);
        }

        [Fact]
        public void BatchOperation_WhenDeletingEntityThatDoesNotExist_TheBatchOperationFails()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var batchOperationResult = stubTable
                .BatchOperation()
                .Delete(new StubEntity("partition-key", "row-key"))
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.EntityDoesNotExist, batchOperationResult.OperationResult);
            Assert.Equal(0, batchOperationResult.Index);
            Assert.Empty(stubTable.Query(new StubTableQuery(), default).Entities);
        }

        [Fact]
        public void BatchOperation_WhenDeletingExistingEntity_TheBatchOperationSucceeds()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var batchOperationResult = stubTable
                .BatchOperation()
                .Insert(new StubEntity("partition-key", "row-key"))
                .Delete(new StubEntity("partition-key", "row-key", "*"))
                .Execute();

            Assert.Equal(StubTableBatchOperationResult.Success, batchOperationResult.OperationResult);
            Assert.Null(batchOperationResult.Index);
            Assert.Empty(stubTable.Query(new StubTableQuery(), default).Entities);
        }
    }
}