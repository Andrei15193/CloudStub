using CloudStub.Core;
using Xunit;

namespace CloudStub.Tests.Core
{
    public class InMemoryTableStorageHandlerTests
    {
        private readonly ITableStorageHandler _tablePartitionStreamProvider = new InMemoryTableStorageHandler();

        [Fact]
        public void GetTextReader_WhenThereIsNoTableData_ReturnsEmptyReader()
        {
            using (var reader = _tablePartitionStreamProvider.GetTextReader("table-name"))
                Assert.Empty(reader.ReadToEnd());
        }

        [Fact]
        public void Exists_WhenThereIsNoTableData_ReturnsFalse()
        {
            using (var reader = _tablePartitionStreamProvider.GetTextReader("table-name"))
            { }

            Assert.False(_tablePartitionStreamProvider.Exists("table-name"));
        }

        [Fact]
        public void Exists_WhenThereIsTableData_ReturnsTrue()
        {
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name"))
            { }

            Assert.True(_tablePartitionStreamProvider.Exists("table-name"));
        }

        [Fact]
        public void Exists_WhenTableDataWasDeleted_ReturnsFalse()
        {
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name"))
            { }
            _tablePartitionStreamProvider.Delete("table-name");

            Assert.False(_tablePartitionStreamProvider.Exists("table-name"));
        }

        [Fact]
        public void GetTextReader_WhenThereIsTableData_ReturnsTheSameContent()
        {
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name"))
                writer.Write("test-content");

            using (var reader = _tablePartitionStreamProvider.GetTextReader("table-name"))
                Assert.Equal("test-content", reader.ReadToEnd());
        }

        [Fact]
        public void GetTextReader_WhenTableDataWasDeleted_ReturnsEmptyContent()
        {
            using (var writer = _tablePartitionStreamProvider.GetTextWriter("table-name"))
                writer.Write("test-content");
            _tablePartitionStreamProvider.Delete("table-name");

            using (var reader = _tablePartitionStreamProvider.GetTextReader("table-name"))
                Assert.Empty(reader.ReadToEnd());
        }

        [Fact]
        public void GetTextReader_WhenNonExistentTableDataIsDeleted_ReturnsEmptyContent()
        {
            _tablePartitionStreamProvider.Delete("table-name");

            using (var reader = _tablePartitionStreamProvider.GetTextReader("table-name"))
                Assert.Empty(reader.ReadToEnd());
        }
    }
}