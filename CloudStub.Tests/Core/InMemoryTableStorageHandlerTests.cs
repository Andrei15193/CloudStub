using System.Collections.Generic;
using System.Linq;
using System.Text;
using CloudStub.Core;
using Xunit;

namespace CloudStub.Tests.Core
{
    public class InMemoryTableStorageHandlerTests
    {
        private readonly ITableStorageHandler _tableStorageHandler = new InMemoryTableStorageHandler();

        [Fact]
        public void Create_WhenTableDoesNotExist_ReturnsTrue()
            => Assert.True(_tableStorageHandler.Create("table-name"));

        [Fact]
        public void Create_WhenTableExists_ReturnsFalse()
        {
            _tableStorageHandler.Create("table-name");

            Assert.False(_tableStorageHandler.Create("table-name"));
        }

        [Fact]
        public void Delete_WhenTableDoesNotExist_ReturnsFalse()
            => Assert.False(_tableStorageHandler.Delete("table-name"));

        [Fact]
        public void Delete_WhenTableExists_ReturnsTrue()
        {
            _tableStorageHandler.Create("table-name");

            Assert.True(_tableStorageHandler.Delete("table-name"));
        }

        [Fact]
        public void Delete_WhenTableWasDeleted_ReturnsTrue()
        {
            _tableStorageHandler.Create("table-name");
            _tableStorageHandler.Delete("table-name");

            Assert.False(_tableStorageHandler.Delete("table-name"));
        }

        [Fact]
        public void Exists_WhenTableDoesNotExist_ReturnsFalse()
            => Assert.False(_tableStorageHandler.Exists("table-name"));

        [Fact]
        public void Exists_WhenTableExists_ReturnsTrue()
        {
            _tableStorageHandler.Create("table-name");

            Assert.True(_tableStorageHandler.Exists("table-name"));
        }

        [Fact]
        public void Exists_WhenTableWasDeleted_ReturnsTrue()
        {
            _tableStorageHandler.Create("table-name");
            _tableStorageHandler.Delete("table-name");

            Assert.False(_tableStorageHandler.Exists("table-name"));
        }

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenTableDoesNotExist_ThrowsException()
            => Assert.Throws<KeyNotFoundException>(() => _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key"));

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenOnlyOtherTableExists_ThrowsException()
        {
            _tableStorageHandler.Create("other-table-name");

            Assert.Throws<KeyNotFoundException>(() => _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key"));
        }

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenTableContainsNoData_ReturnsEmptyReader()
        {
            _tableStorageHandler.Create("table-name");

            var partitionClusterStorageHandler = _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key");
            using (var reader = partitionClusterStorageHandler.OpenRead())
                Assert.Empty(reader.ReadToEnd());
            Assert.False(string.IsNullOrWhiteSpace(partitionClusterStorageHandler.Key));
        }

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenPartitionClusterContainsData_ReturnsReaderWithData()
        {
            _tableStorageHandler.Create("table-name");
            using (var writer = _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenWrite())
                writer.Write("content");

            using (var reader = _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenRead())
                Assert.Equal("content", reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenOnlyOtherTableContainsData_ReturnsEmptyReader()
        {
            _tableStorageHandler.Create("table-name");
            _tableStorageHandler.Create("other-table-name");
            using (var writer = _tableStorageHandler.GetPartitionClusterStorageHandler("other-table-name", "partition-key").OpenWrite())
                writer.Write("content");

            using (var reader = _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenRead())
                Assert.Empty(reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenPartitionClusterContainsData_ReturnsReaderWithSameDataOnMultipleReads()
        {
            _tableStorageHandler.Create("table-name");
            using (var writer = _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenWrite())
                writer.Write("content");

            using (var reader = _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenRead())
                Assert.Equal("content", reader.ReadToEnd());
            using (var reader = _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenRead())
                Assert.Equal("content", reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClusterStorageHandler_WhenTableHasData_OverwritesExistingData()
        {
            _tableStorageHandler.Create("table-name");
            using (var writer = _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenWrite())
                writer.Write("content");

            using (var writer = _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenWrite())
                writer.Write("new");

            using (var reader = _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", "partition-key").OpenRead())
                Assert.Equal("new", reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClusterStorageHandlers_WhenTableDoesNotExist_ThrowsException()
            => Assert.Throws<KeyNotFoundException>(() => _tableStorageHandler.GetPartitionClusterStorageHandlers("table-name"));

        [Fact]
        public void GetPartitionClusterStorageHandlers_WhenOnlyOtherTableExists_ThrowsException()
        {
            _tableStorageHandler.Create("other-table-name");

            Assert.Throws<KeyNotFoundException>(() => _tableStorageHandler.GetPartitionClusterStorageHandlers("table-name"));
        }

        [Fact]
        public void GetPartitionClusterStorageHandlers_WhenTableContainsMultiplePartitions_ReturnsReadersForAll()
        {
            _tableStorageHandler.Create("table-name");

            foreach (var partitionNumber in Enumerable.Range(1, 5))
                using (var writer = _tableStorageHandler.GetPartitionClusterStorageHandler("table-name", $"partition-key-{partitionNumber}").OpenWrite())
                    writer.Write("content");

            Assert.Equal(
                string.Join(string.Empty, Enumerable.Repeat("content", 5)),
                _tableStorageHandler
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