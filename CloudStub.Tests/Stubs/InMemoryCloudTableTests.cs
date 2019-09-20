using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections;
using System.Threading.Tasks;
using Xunit;

namespace CloudStub.Tests
{
    public class InMemoryCloudTableTests : AzureStorageUnitTest
    {
        /// <summary>A temporary flag to easily switch between in-memory cloud table and actual Azure Storage Table.</summary>
        private static readonly bool _useInMemory = true;
        private readonly CloudTable _cloudTable;

        public InMemoryCloudTableTests()
            => _cloudTable = _GetCloudTable(nameof(InMemoryCloudTableTests) + "TestTable");

        [Fact]
        public void TableName_GetsTheSameNameWhichWasProvided()
        {
            Assert.Equal(nameof(InMemoryCloudTableTests) + "TestTable", _cloudTable.Name);
        }

        [Fact]
        public void CreateAsync_WhenTableNameIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("tableName", () => _GetCloudTable(null));
            Assert.Equal(new ArgumentNullException("tableName").Message, exception.Message);
        }

        [Fact]
        public void CreateAsync_WhenTableNameIsEmpty_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentException>("tableName", () => _GetCloudTable(""));
            Assert.Equal(new ArgumentException("The argument must not be empty string.", "tableName").Message, exception.Message);
        }

        [Fact]
        public async Task ExistsAsync_WhenTableDoesNotExist_ReturnsFalse()
        {
            Assert.False(await _cloudTable.ExistsAsync());
        }

        [Fact]
        public async Task ExistsAsync_WhenTableExist_ReturnsTrue()
        {
            await _cloudTable.CreateAsync();

            Assert.True(await _cloudTable.ExistsAsync());
        }

        [Fact]
        public async Task CreateAsync_WhenTableDoesNotExist_CreatesTable()
        {
            await _cloudTable.CreateAsync();

            Assert.True(await _cloudTable.ExistsAsync());
        }

        [Fact]
        public async Task CreateAsync_WhenTableExists_ThrowsException()
        {
            await _cloudTable.CreateAsync();

            var exception = await Assert.ThrowsAsync<StorageException>(() => _cloudTable.CreateAsync());
            Assert.Equal("Conflict", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(409, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Conflict", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory]
        [InlineData("tables")]
        [InlineData("invalid_table_name")]
        [InlineData("1nvalid")]
        [InlineData(" ")]
        [InlineData("t")]
        [InlineData("tt")]
        [InlineData("testTableNameHavingALengthOf63CharactersSomeOfThemAreJustExtra1s")]
        public async Task CreateAsync_WhenTableNameIsInvalid_ThrowsException(string tableName)
        {
            var cloudTable = _GetCloudTable(tableName);

            var exception = await Assert.ThrowsAsync<StorageException>(() => cloudTable.CreateAsync());
            Assert.Equal("Bad Request", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Bad Request", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public async Task CreateIfNotExistsAsync_WhenTableDoesNotExist_ReturnsFalse()
        {
            Assert.True(await _cloudTable.CreateIfNotExistsAsync());

            Assert.True(await _cloudTable.ExistsAsync());
        }

        [Fact]
        public async Task CreateIfNotExistsAsync_WhenTableExists_ReturnsFalse()
        {
            await _cloudTable.CreateAsync();

            Assert.False(await _cloudTable.CreateIfNotExistsAsync());
        }

        [Fact]
        public async Task DeleteAsync_WhenTableDoesNotExist_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<StorageException>(() => _cloudTable.DeleteAsync());

            Assert.Equal("Not Found", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(404, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Not Found", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public async Task DeleteAsync_WhenTableExists_DeletesTable()
        {
            await _cloudTable.CreateAsync();

            await _cloudTable.DeleteAsync();

            Assert.False(await _cloudTable.ExistsAsync());
        }

        [Fact]
        public async Task DeleteIfExistsAsync_WhenTableDoesNotExist_ReturnsFalse()
        {
            Assert.False(await _cloudTable.DeleteIfExistsAsync());
        }

        [Fact]
        public async Task DeleteIfExistsAsync_WhenTableExists_ReturnsTrue()
        {
            await _cloudTable.CreateAsync();

            Assert.True(await _cloudTable.DeleteIfExistsAsync());
            Assert.False(await _cloudTable.DeleteIfExistsAsync());
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Task.Run(() => _cloudTable.DeleteIfExistsAsync()).Wait();
            base.Dispose(disposing);
        }

        private CloudTable _GetCloudTable(string tableName)
            => _useInMemory ?
                new InMemoryCloudTable(tableName) :
                CloudStorageAccount
                    .Parse(AzureStorageConnectionString)
                    .CreateCloudTableClient()
                    .GetTableReference(tableName);
    }
}