using System.Text;
using CloudStub.Core;
using Xunit;

namespace CloudStub.Tests.Core
{
    public class InMemoryTableStorageHandlerTests
    {
        private readonly TableStorageHandler _tablePartitionStreamProvider = new InMemoryTableStorageHandler();

        [Fact]
        public void GetTextReader_WhenThereIsNoTableData_ReturnsEmptyReader()
        {
            using (var reader = _tablePartitionStreamProvider.GetTextReader("table-name", "partition-key"))
                Assert.Empty(reader.ReadToEnd());
        }

        [Fact]
        public void Exists_WhenThereIsNoTableData_ReturnsFalse()
        {
            using (var reader = _tablePartitionStreamProvider.GetTextReader("table-name", "partition-key"))
            { }

            Assert.False(_tablePartitionStreamProvider.Exists("table-name"));
        }

        [Fact]
        public void Exists_WhenThereIsNoTableDataEvenThoughWriterWasRequested_ReturnsFalse()
        {
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name", "partition-key"))
            { }

            Assert.False(_tablePartitionStreamProvider.Exists("table-name"));
        }

        [Fact]
        public void Exists_WhenThereIsTableData_ReturnsTrue()
        {
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name", "partition-key"))
                writer.Write("test");

            Assert.True(_tablePartitionStreamProvider.Exists("table-name"));
        }

        [Fact]
        public void Exists_WhenTableDataWasDeleted_ReturnsFalse()
        {
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name", "partition-key"))
                writer.Write("content");
            _tablePartitionStreamProvider.Delete("table-name");

            Assert.False(_tablePartitionStreamProvider.Exists("table-name"));
        }

        [Fact]
        public void GetTextReader_WhenThereIsTableData_ReturnsTheSameContent()
        {
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name", "partition-key"))
                writer.Write("test-content");

            using (var reader = _tablePartitionStreamProvider.GetTextReader("table-name", "partition-key"))
                Assert.Equal("test-content", reader.ReadToEnd());
        }

        [Fact]
        public void GetTextReader_WhenThereAreMultiplePartitions_DoesNotReadContentFromOtherPartition()
        {
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name", "partition-key-1"))
                writer.Write("test-content-1");
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name", "partition-key-2"))
                writer.Write("test-content-2");

            using (var reader = _tablePartitionStreamProvider.GetTextReader("table-name", "partition-key-1"))
                Assert.Equal("test-content-1", reader.ReadToEnd());
            using (var reader = _tablePartitionStreamProvider.GetTextReader("table-name", "partition-key-2"))
                Assert.Equal("test-content-2", reader.ReadToEnd());
        }

        [Fact]
        public void GetPartitionTextReaders_WhenThereIsTableData_ReturnsTheSameContent()
        {
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name", "partition-key-1"))
                writer.Write("test-content");
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name", "partition-key-2"))
                writer.Write("test-content");

            var contents = new StringBuilder();
            foreach (var reader in _tablePartitionStreamProvider.GetPartitionTextReaders("table-name"))
                using (reader)
                    contents.Append(reader.ReadToEnd());

            Assert.Equal("test-contenttest-content", contents.ToString());
        }

        [Fact]
        public void GetTextReader_WhenTableDataWasDeleted_ReturnsEmptyContent()
        {
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name", "partition-key"))
                writer.Write("test-content");
            _tablePartitionStreamProvider.Delete("table-name");

            using (var reader = _tablePartitionStreamProvider.GetTextReader("table-name", "partition-key"))
                Assert.Empty(reader.ReadToEnd());
        }

        [Fact]
        public void GetTextReader_WhenNonExistentTableDataIsDeleted_ReturnsEmptyContent()
        {
            _tablePartitionStreamProvider.Delete("table-name");

            using (var reader = _tablePartitionStreamProvider.GetTextReader("table-name", "partition-key"))
                Assert.Empty(reader.ReadToEnd());
        }
    }
}