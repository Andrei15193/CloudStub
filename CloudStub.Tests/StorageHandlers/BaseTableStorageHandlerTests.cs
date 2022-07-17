using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CloudStub.StorageHandlers;
using Xunit;

namespace CloudStub.Tests.StorageHandlers
{
    public abstract class BaseTableStorageHandlerTests<TTableStorageHandler>
        where TTableStorageHandler : ITableStorageHandler
    {
        private readonly Lazy<TTableStorageHandler> _tableStorageHandler;

        public BaseTableStorageHandlerTests()
            => _tableStorageHandler = new Lazy<TTableStorageHandler>(CreateTableStorageHandler);

        protected TTableStorageHandler TableStorageHandler
            => _tableStorageHandler.Value;

        protected abstract TTableStorageHandler CreateTableStorageHandler();

        [Fact]
        public void Create_WhenTableDoesNotExist_ReturnsTrue()
            => Assert.True(TableStorageHandler.Create("table-name"));

        [Fact]
        public void Create_WhenTableExists_ReturnsFalse()
        {
            TableStorageHandler.Create("table-name");

            Assert.False(TableStorageHandler.Create("table-name"));
        }

        [Fact]
        public void Delete_WhenTableDoesNotExist_ReturnsFalse()
            => Assert.False(TableStorageHandler.Delete("table-name"));

        [Fact]
        public void Delete_WhenTableExists_ReturnsTrue()
        {
            TableStorageHandler.Create("table-name");

            Assert.True(TableStorageHandler.Delete("table-name"));
        }

        [Fact]
        public void Delete_WhenTableWasDeleted_ReturnsTrue()
        {
            TableStorageHandler.Create("table-name");
            TableStorageHandler.Delete("table-name");

            Assert.False(TableStorageHandler.Delete("table-name"));
        }

        [Fact]
        public void Exists_WhenTableDoesNotExist_ReturnsFalse()
            => Assert.False(TableStorageHandler.Exists("table-name"));

        [Fact]
        public void Exists_WhenTableExists_ReturnsTrue()
        {
            TableStorageHandler.Create("table-name");

            Assert.True(TableStorageHandler.Exists("table-name"));
        }

        [Fact]
        public void Exists_WhenTableWasDeleted_ReturnsTrue()
        {
            TableStorageHandler.Create("table-name");
            TableStorageHandler.Delete("table-name");

            Assert.False(TableStorageHandler.Exists("table-name"));
        }

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenTableDoesNotExist_ThrowsException()
            => Assert.Throws<KeyNotFoundException>(() => TableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key"));

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenOnlyOtherTableExists_ThrowsException()
        {
            TableStorageHandler.Create("other-table-name");

            Assert.Throws<KeyNotFoundException>(() => TableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key"));
        }

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenTableContainsNoData_ReturnsEmptyReader()
        {
            TableStorageHandler.Create("table-name");

            var partitionClusterStorageHandler = TableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key");
            using (var reader = partitionClusterStorageHandler.OpenRead())
                Assert.Empty(reader.ReadToEnd());
            Assert.False(string.IsNullOrWhiteSpace(partitionClusterStorageHandler.Key));
        }

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenPartitionClusterContainsData_ReturnsReaderWithData()
        {
            TableStorageHandler.Create("table-name");
            using (var writer = TableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenWrite())
                writer.Write("content");

            using (var reader = TableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenRead())
                Assert.Equal("content", reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenOnlyOtherTableContainsData_ReturnsEmptyReader()
        {
            TableStorageHandler.Create("table-name");
            TableStorageHandler.Create("other-table-name");
            using (var writer = TableStorageHandler.GetPartitionClusterStorageHandler("other-table-name", "partition-key").OpenWrite())
                writer.Write("content");

            using (var reader = TableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenRead())
                Assert.Empty(reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenPartitionClusterContainsData_ReturnsReaderWithSameDataOnMultipleReads()
        {
            TableStorageHandler.Create("table-name");
            using (var writer = TableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenWrite())
                writer.Write("content");

            using (var reader = TableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenRead())
                Assert.Equal("content", reader.ReadToEnd());
            using (var reader = TableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenRead())
                Assert.Equal("content", reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenTableHasData_OverwritesExistingData()
        {
            TableStorageHandler.Create("table-name");
            using (var writer = TableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenWrite())
                writer.Write("content");

            using (var writer = TableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenWrite())
                writer.Write("new");

            using (var reader = TableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenRead())
                Assert.Equal("new", reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClusterStorageHandlers_WhenTableDoesNotExist_ThrowsException()
            => Assert.Throws<KeyNotFoundException>(() => TableStorageHandler.GetPartitionClusterStorageHandlers("table-name"));

        [Fact]
        public void GetPartitionClusterStorageHandlers_WhenOnlyOtherTableExists_ThrowsException()
        {
            TableStorageHandler.Create("other-table-name");

            Assert.Throws<KeyNotFoundException>(() => TableStorageHandler.GetPartitionClusterStorageHandlers("table-name"));
        }

        [Fact]
        public void GetPartitionClusterStorageHandlers_WhenTableContainsMultiplePartitions_ReturnsReadersForAll()
        {
            TableStorageHandler.Create("table-name");

            foreach (var partitionNumber in Enumerable.Range(1, 5))
                using (var writer = TableStorageHandler.GetPartitionClusterStorageHandler("table-name", $"partition-key-{partitionNumber}").OpenWrite())
                    writer.Write("content");

            Assert.Equal(
                string.Join(string.Empty, Enumerable.Repeat("content", 5)),
                TableStorageHandler
                    .GetPartitionClusterStorageHandlers("table-name")
                    .Aggregate(
                        new StringBuilder(),
                        (builder, partitonClusterStorageHandler) =>
                        {
                            using (var reader = partitonClusterStorageHandler.OpenRead())
                                return builder.Append(reader.ReadToEnd());
                        }
                    )
                    .ToString()
            );
        }
    }
}