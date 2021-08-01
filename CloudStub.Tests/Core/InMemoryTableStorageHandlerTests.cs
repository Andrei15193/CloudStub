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
        public void GetPartitionClusterTextReader_WhenTableDoesNotExist_ThrowsException()
            => Assert.Throws<KeyNotFoundException>(() => _tableStorageHandler.GetPartitionClusterTextReader("table-name", "partition-key"));

        [Fact]
        public void GetPartitionClusterTextReader_WhenOnlyOtherTableExists_ThrowsException()
        {
            _tableStorageHandler.Create("other-table-name");

            Assert.Throws<KeyNotFoundException>(() => _tableStorageHandler.GetPartitionClusterTextReader("table-name", "partition-key"));
        }

        [Fact]
        public void GetPartitionClusterTextReader_WhenTableContainsNoData_ReturnsEmptyReader()
        {
            _tableStorageHandler.Create("table-name");

            using (var reader = _tableStorageHandler.GetPartitionClusterTextReader("table-name", "partition-key"))
                Assert.Empty(reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClusterTextReader_WhenPartitionClusterContainsData_ReturnsReaderWithData()
        {
            _tableStorageHandler.Create("table-name");
            using (var writer = _tableStorageHandler.GetPartitionClusterTextWriter("table-name", "partition-key"))
                writer.Write("content");

            using (var reader = _tableStorageHandler.GetPartitionClusterTextReader("table-name", "partition-key"))
                Assert.Equal("content", reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClusterTextReader_WhenOnlyOtherTableContainsData_ReturnsEmptyReader()
        {
            _tableStorageHandler.Create("table-name");
            _tableStorageHandler.Create("other-table-name");
            using (var writer = _tableStorageHandler.GetPartitionClusterTextWriter("other-table-name", "partition-key"))
                writer.Write("content");

            using (var reader = _tableStorageHandler.GetPartitionClusterTextReader("table-name", "partition-key"))
                Assert.Empty(reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClusterTextReader_WhenPartitionClusterContainsData_ReturnsReaderWithSameDataOnMultipleReads()
        {
            _tableStorageHandler.Create("table-name");
            using (var writer = _tableStorageHandler.GetPartitionClusterTextWriter("table-name", "partition-key"))
                writer.Write("content");

            using (var reader = _tableStorageHandler.GetPartitionClusterTextReader("table-name", "partition-key"))
                Assert.Equal("content", reader.ReadToEnd());
            using (var reader = _tableStorageHandler.GetPartitionClusterTextReader("table-name", "partition-key"))
                Assert.Equal("content", reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClusterTextWriter_WhenTableDoesNotExist_ThrowsException()
            => Assert.Throws<KeyNotFoundException>(() => _tableStorageHandler.GetPartitionClusterTextWriter("table-name", "partition-key"));

        [Fact]
        public void GetPartitionClusterTextWriter_WhenOnlyOtherTableExists_ThrowsException()
        {
            _tableStorageHandler.Create("other-table-name");

            Assert.Throws<KeyNotFoundException>(() => _tableStorageHandler.GetPartitionClusterTextWriter("table-name", "partition-key"));
        }

        [Fact]
        public void GetPartitionClusterTextWriter_WhenTableHasData_OverwritesExistingData()
        {
            _tableStorageHandler.Create("table-name");
            using (var writer = _tableStorageHandler.GetPartitionClusterTextWriter("table-name", "partition-key"))
                writer.Write("content");

            using (var writer = _tableStorageHandler.GetPartitionClusterTextWriter("table-name", "partition-key"))
                writer.Write("new");

            using (var reader = _tableStorageHandler.GetPartitionClusterTextReader("table-name", "partition-key"))
                Assert.Equal("new", reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionClustersTextReaders_WhenTableDoesNotExist_ThrowsException()
            => Assert.Throws<KeyNotFoundException>(() => _tableStorageHandler.GetPartitionClustersTextReaderProviders("table-name"));

        [Fact]
        public void GetPartitionClustersTextReaders_WhenOnlyOtherTableExists_ThrowsException()
        {
            _tableStorageHandler.Create("other-table-name");

            Assert.Throws<KeyNotFoundException>(() => _tableStorageHandler.GetPartitionClustersTextReaderProviders("table-name"));
        }

        [Fact]
        public void GetPartitionClustersTextReaders_WhenTableContainsMultiplePartitions_ReturnsReadersForAll()
        {
            _tableStorageHandler.Create("table-name");

            foreach (var partitionNumber in Enumerable.Range(1, 5))
                using (var writer = _tableStorageHandler.GetPartitionClusterTextWriter("table-name", $"partition-key-{partitionNumber}"))
                    writer.Write("content");

            Assert.Equal(
                string.Join(string.Empty, Enumerable.Repeat("content", 5)),
                _tableStorageHandler
                    .GetPartitionClustersTextReaderProviders("table-name")
                    .Aggregate(
                        new StringBuilder(),
                        (builder, readerProvider) =>
                        {
                            using (var reader = readerProvider())
                                return builder.Append(reader.ReadToEnd());
                        }
                    )
                    .ToString()
            );
        }
    }
}