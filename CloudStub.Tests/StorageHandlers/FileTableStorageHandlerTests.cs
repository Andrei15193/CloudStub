using CloudStub.StorageHandlers;
using System;
using System.IO;
using Xunit;

namespace CloudStub.Tests.StorageHandlers
{
    public class FileTableStorageHandlerTests : BaseTableStorageHandlerTests<FileTableStorageHandler>, IDisposable
    {
        private readonly string _tablesDirectoryPath = Path.Combine(Environment.CurrentDirectory, $"Tables {Guid.NewGuid()}");

        ~FileTableStorageHandlerTests()
            => Dispose(false);

        [Fact]
        public void Initialize_WhenStorageDirectoryIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("storageDirectory", () => new FileTableStorageHandler(storageDirectory: null));
            Assert.Equal(new ArgumentNullException("storageDirectory").Message, exception.Message);
        }

        [Fact]
        public void Initialize_WhenStorageDirectoryPathIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("path", () => new FileTableStorageHandler(storageDirectoryPath: null));
            Assert.Equal(new ArgumentNullException("path").Message, exception.Message);
        }

        [Fact]
        public void Initialize_WhenStorageDirectoryPathIsInvalid_ThrowsException()
            => Assert.Throws<IOException>(() => new FileTableStorageHandler(storageDirectoryPath: "invalid:path:yes:yes"));

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected override FileTableStorageHandler CreateTableStorageHandler()
            => new(_tablesDirectoryPath)
            {
                LockRetryCount = 0
            };

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
                TableStorageHandler.StorageDirectory.Delete(true);
            else if (Directory.Exists(_tablesDirectoryPath))
                Directory.Delete(_tablesDirectoryPath, true);
        }
    }
}