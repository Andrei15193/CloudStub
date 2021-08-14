using System;
using System.Collections.Generic;
using System.Linq;
using CloudStub.Core;
using Xunit;

namespace CloudStub.Tests.Core
{
    public class StubTableTests
    {
        [Fact]
        public void NewTable_WhenNameIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("name", () => new StubTable(null, null));
            Assert.Equal(new ArgumentNullException("name").Message, exception.Message);
        }

        [Fact]
        public void NewTable_WhenTableStorageHandlerIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("tableStorageHandler", () => new StubTable("", null));
            Assert.Equal(new ArgumentNullException("tableStorageHandler").Message, exception.Message);
        }

        [Fact]
        public void NewTable_InitializesName_HasNameSet()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            Assert.Equal("table-name", stubTable.Name);
        }

        [Fact]
        public void NewTable_WithoutRunningAnyOperations_DoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            Assert.False(stubTable.Exists);
        }

        [Fact]
        public void NewTable_WithPreviouslyCreatedTable_Exist()
        {
            var tableDataHandler = new InMemoryTableStorageHandler();
            tableDataHandler.Create("table-name");

            var stubTable = new StubTable("table-name", tableDataHandler);

            Assert.True(stubTable.Exists);
        }

        [Fact]
        public void Create_WhenTableDoesNotExist_ExecutesSuccessfully()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var createResult = stubTable.Create();

            Assert.Equal(StubTableCreateResult.Success, createResult);
            Assert.True(stubTable.Exists);
        }

        [Fact]
        public void Create_WhenTableExist_ReturnsTableAlreadyExists()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var createResult = stubTable.Create();

            Assert.Equal(StubTableCreateResult.TableAlreadyExists, createResult);
            Assert.True(stubTable.Exists);
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
            Assert.False(stubTable.Exists);
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

            var insertResult = stubTable.Insert(new StubEntity());

            Assert.Equal(StubTableInsertResult.TableDoesNotExist, insertResult);
        }

        [Fact]
        public void Insert_WhenTableExists_InsertsTheEntitySuccessfully()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var insertResult = stubTable.Insert(new StubEntity { PartitionKey = "partition-key", RowKey = "row-key" });

            Assert.Equal(StubTableInsertResult.Success, insertResult);
            var queryResult = stubTable.Query(null, null);
            var entity = Assert.Single(queryResult.Entities);
            Assert.Equal("partition-key", entity.PartitionKey);
            Assert.Equal("row-key", entity.RowKey);
        }

        [Fact]
        public void Insert_WhenEntityAlreadyExists_ReturnsEntityAlreadyExists()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key", RowKey = "row-key" });

            var insertResult = stubTable.Insert(new StubEntity { PartitionKey = "partition-key", RowKey = "row-key" });

            Assert.Equal(StubTableInsertResult.EntityAlreadyExists, insertResult);
        }

        [Fact]
        public void InsertOrMerge_WhenTableDoesNotExist_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var result = stubTable.InsertOrMerge(new StubEntity());

            Assert.Equal(StubTableInsertOrMergeResult.TableDoesNotExist, result);
        }

        [Fact]
        public void InsertOrMerge_WhenEntityDoesNotExist_InsertsEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var result = stubTable.InsertOrMerge(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            Assert.Equal(StubTableInsertOrMergeResult.Success, result);
            var insertedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("property-1", insertedEntity.Properties["property1"].Value);
            Assert.Equal("property-2", insertedEntity.Properties["property2"].Value);
        }

        [Fact]
        public void InsertOrMerge_WhenEntityExists_MergesTheEntities()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.InsertOrMerge(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property2", new StubEntityProperty("property-2-merge") },
                    { "property3", new StubEntityProperty("property-3-merge") }
                }
            });

            Assert.Equal(StubTableInsertOrMergeResult.Success, result);
            var insertedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("property-1", insertedEntity.Properties["property1"].Value);
            Assert.Equal("property-2-merge", insertedEntity.Properties["property2"].Value);
            Assert.Equal("property-3-merge", insertedEntity.Properties["property3"].Value);
        }

        [Fact]
        public void InsertOrReplace_WhenTableDoesNotExist_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var result = stubTable.InsertOrReplace(new StubEntity());

            Assert.Equal(StubTableInsertOrReplaceResult.TableDoesNotExist, result);
        }

        [Fact]
        public void InsertOrReplace_WhenEntityDoesNotExist_InsertsEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var result = stubTable.InsertOrReplace(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            Assert.Equal(StubTableInsertOrReplaceResult.Success, result);
            var insertedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("property-1", insertedEntity.Properties["property1"].Value);
            Assert.Equal("property-2", insertedEntity.Properties["property2"].Value);
        }

        [Fact]
        public void InsertOrReplace_WhenEntityExists_ReplacesTheExistingEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.InsertOrReplace(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property2", new StubEntityProperty("property-2-replaced") },
                    { "property3", new StubEntityProperty("property-3-replaced") }
                }
            });

            Assert.Equal(StubTableInsertOrReplaceResult.Success, result);
            var insertedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.False(insertedEntity.Properties.ContainsKey("property1"));
            Assert.Equal("property-2-replaced", insertedEntity.Properties["property2"].Value);
            Assert.Equal("property-3-replaced", insertedEntity.Properties["property3"].Value);
        }

        [Fact]
        public void Merge_WhenTableDoesNotExist_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var result = stubTable.Merge(new StubEntity());

            Assert.Equal(StubTableMergeResult.TableDoesNotExist, result);
        }

        [Fact]
        public void Merge_WhenEntityDoesNotExist_ReturnsEntityDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var result = stubTable.Merge(new StubEntity { PartitionKey = "partition-key", RowKey = "row-key" });

            Assert.Equal(StubTableMergeResult.EntityDoesNotExists, result);
        }

        [Fact]
        public void Merge_WhenEntityDoesNotHaveMatchingEtag_ReturnsEtagsDoNotMatch()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key", RowKey = "row-key" });

            var result = stubTable.Merge(new StubEntity { PartitionKey = "partition-key", RowKey = "row-key", ETag = "unmatching-etag" });

            Assert.Equal(StubTableMergeResult.EtagsDoNotMatch, result);
        }

        [Fact]
        public void Merge_WhenEntityUsesGenericEtag_MergesTheEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.Merge(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = "*",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property2", new StubEntityProperty("property-2-merge") },
                    { "property3", new StubEntityProperty("property-3-merge") }
                }
            });

            Assert.Equal(StubTableMergeResult.Success, result);
            var mergedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("partition-key", mergedEntity.PartitionKey);
            Assert.Equal("row-key", mergedEntity.RowKey);
            Assert.NotEmpty(mergedEntity.ETag);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) <= mergedEntity.Timestamp && mergedEntity.Timestamp <= DateTime.UtcNow.AddMinutes(1));
            Assert.Equal("property-1", (string)mergedEntity.Properties["property1"].Value);
            Assert.Equal("property-2-merge", (string)mergedEntity.Properties["property2"].Value);
            Assert.Equal("property-3-merge", (string)mergedEntity.Properties["property3"].Value);
        }

        [Fact]
        public void Merge_WhenEntityUsesMatchingEtag_MergesTheEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });
            var etag = stubTable.Query(new StubTableQuery(), default).Entities.Single().ETag;

            var result = stubTable.Merge(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = etag,
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property2", new StubEntityProperty("property-2-merge") },
                    { "property3", new StubEntityProperty("property-3-merge") }
                }
            });

            Assert.Equal(StubTableMergeResult.Success, result);
            var mergedEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("partition-key", mergedEntity.PartitionKey);
            Assert.Equal("row-key", mergedEntity.RowKey);
            Assert.NotEmpty(mergedEntity.ETag);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) <= mergedEntity.Timestamp && mergedEntity.Timestamp <= DateTime.UtcNow.AddMinutes(1));
            Assert.Equal("property-1", (string)mergedEntity.Properties["property1"].Value);
            Assert.Equal("property-2-merge", (string)mergedEntity.Properties["property2"].Value);
            Assert.Equal("property-3-merge", (string)mergedEntity.Properties["property3"].Value);
        }

        [Fact]
        public void Replace_WhenTableDoesNotExist_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var result = stubTable.Replace(new StubEntity());

            Assert.Equal(StubTableReplaceResult.TableDoesNotExist, result);
        }

        [Fact]
        public void Replace_WhenEntityDoesNotExist_ReturnsEntityDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var result = stubTable.Replace(new StubEntity { PartitionKey = "partition-key", RowKey = "row-key" });

            Assert.Equal(StubTableReplaceResult.EntityDoesNotExists, result);
        }

        [Fact]
        public void Replace_WhenEntityDoesNotHaveMatchingEtag_ReturnsEtagsDoNotMatch()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key", RowKey = "row-key" });

            var result = stubTable.Replace(new StubEntity { PartitionKey = "partition-key", RowKey = "row-key", ETag = "unmatching-etag" });

            Assert.Equal(StubTableReplaceResult.EtagsDoNotMatch, result);
        }

        [Fact]
        public void Replace_WhenEntityUsesGenericEtag_ReplacesTheEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.Replace(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = "*",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property2", new StubEntityProperty("property-2-replaced") },
                    { "property3", new StubEntityProperty("property-3-replaced") }
                }
            });

            Assert.Equal(StubTableReplaceResult.Success, result);
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
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });
            var etag = stubTable.Query(new StubTableQuery(), default).Entities.Single().ETag;

            var result = stubTable.Replace(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = etag,
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property2", new StubEntityProperty("property-2-replaced") },
                    { "property3", new StubEntityProperty("property-3-replaced") }
                }
            });

            Assert.Equal(StubTableReplaceResult.Success, result);
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

            var result = stubTable.Delete(new StubEntity());

            Assert.Equal(StubTablDeleteResult.TableDoesNotExist, result);
        }

        [Fact]
        public void Delete_WhenEntityDoesNotExist_ReturnsEntityDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var result = stubTable.Delete(new StubEntity { PartitionKey = "partition-key", RowKey = "row-key" });

            Assert.Equal(StubTablDeleteResult.EntityDoesNotExists, result);
        }

        [Fact]
        public void Delete_WhenEntityDoesNotHaveMatchingEtag_ReturnsEtagsDoNotMatch()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key", RowKey = "row-key" });

            var result = stubTable.Delete(new StubEntity { PartitionKey = "partition-key", RowKey = "row-key", ETag = "unmatching-etag" });

            Assert.Equal(StubTablDeleteResult.EtagsDoNotMatch, result);
        }

        [Fact]
        public void Delete_WhenEntityUsesGenericEtag_DeletesTheEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            });

            var result = stubTable.Delete(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = "*"
            });

            Assert.Equal(StubTablDeleteResult.Success, result);
            Assert.Empty(stubTable.Query(new StubTableQuery(), default).Entities);
        }

        [Fact]
        public void Delete_WhenEntityUsesMatchingEtag_DeletesTheEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key"
            });
            var etag = stubTable.Query(new StubTableQuery(), default).Entities.Single().ETag;

            var result = stubTable.Delete(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = etag
            });

            Assert.Equal(StubTablDeleteResult.Success, result);
            Assert.Empty(stubTable.Query(new StubTableQuery(), default).Entities);
        }

        [Fact]
        public void Delete_WhenEntityUsesMatchingEtag_DeletesJustThatEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key-1"
            });
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key-2"
            });

            var result = stubTable.Delete(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key-1",
                ETag = "*"
            });

            Assert.Equal(StubTablDeleteResult.Success, result);
            var remainingEntity = Assert.Single(stubTable.Query(new StubTableQuery(), default).Entities);
            Assert.Equal("partition-key", remainingEntity.PartitionKey);
            Assert.Equal("row-key-2", remainingEntity.RowKey);
        }

        [Fact]
        public void Retrieve_WhenTableDoesNotExist_ReturnsTableDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());

            var result = stubTable.Retrieve("partition-key", "row-key");

            Assert.Equal(StubTableRetrieveResult.TableDoesNotExist, result.RetrieveResult);
            Assert.Null(result.Entity);
        }

        [Fact]
        public void Retrieve_WhenEntityDoesNotExist_ReturnsEntityDoesNotExist()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();

            var result = stubTable.Retrieve("partition-key", "row-key");

            Assert.Equal(StubTableRetrieveResult.EntityDoesNotExists, result.RetrieveResult);
            Assert.Null(result.Entity);
        }

        [Fact]
        public void Retrieve_WhenEntityExists_ReturnsMatchedEntity()
        {
            var stubTable = new StubTable("table-name", new InMemoryTableStorageHandler());
            stubTable.Create();
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.Retrieve("partition-key", "row-key");

            Assert.Equal(StubTableRetrieveResult.Success, result.RetrieveResult);
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
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.Retrieve("partition-key", "row-key", new[] { "property2" });

            Assert.Equal(StubTableRetrieveResult.Success, result.RetrieveResult);
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
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal)
                {
                    { "property1", new StubEntityProperty("property-1") },
                    { "property2", new StubEntityProperty("property-2") }
                }
            });

            var result = stubTable.Retrieve("partition-key", "row-key", Enumerable.Empty<string>());

            Assert.Equal(StubTableRetrieveResult.Success, result.RetrieveResult);
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
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>
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
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-5", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-5", RowKey = "row-key-2" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-3", RowKey = "row-key-2" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-3", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-1", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-1", RowKey = "row-key-2" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-2", RowKey = "row-key-2" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-2", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-4", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-4", RowKey = "row-key-2" });

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
            stubTable.Insert(new StubEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                Properties = new Dictionary<string, StubEntityProperty>
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
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-5", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-5", RowKey = "row-key-2" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-3", RowKey = "row-key-2" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-3", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-1", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-1", RowKey = "row-key-2" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-2", RowKey = "row-key-2" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-2", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-4", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-4", RowKey = "row-key-2" });

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
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-5", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-5", RowKey = "row-key-2" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-3", RowKey = "row-key-2" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-3", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-1", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-1", RowKey = "row-key-2" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-2", RowKey = "row-key-2" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-2", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-4", RowKey = "row-key-1" });
            stubTable.Insert(new StubEntity { PartitionKey = "partition-key-4", RowKey = "row-key-2" });

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
    }
}