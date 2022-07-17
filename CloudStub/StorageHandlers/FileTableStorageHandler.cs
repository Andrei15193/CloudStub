using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace CloudStub.StorageHandlers
{
    public class FileTableStorageHandler : ITableStorageHandler
    {
        private readonly FileLockOptions _fileLockOptions = new FileLockOptions();
        private readonly HashAlgorithm _md5HashAlgorithm = MD5.Create();

        public FileTableStorageHandler(DirectoryInfo storageDirectory)
        {
            StorageDirectory = storageDirectory ?? throw new ArgumentNullException(nameof(storageDirectory));
            StorageDirectory.Create();
        }

        public FileTableStorageHandler(string storageDirectoryPath)
            : this(new DirectoryInfo(storageDirectoryPath))
        {
        }

        public DirectoryInfo StorageDirectory { get; }

        public TimeSpan LockIntervalCheck
        {
            get => _fileLockOptions.LockIntervalCheck;
            set => _fileLockOptions.LockIntervalCheck = value;
        }

        public int? LockRetryCount
        {
            get => _fileLockOptions.LockRetryCount;
            set => _fileLockOptions.LockRetryCount = value;
        }

        public bool Create(string tableName)
        {
            DirectoryInfo tableDirectory = _GetTableDirectory(tableName);

            using (FileLock.Exclusive(tableDirectory, _fileLockOptions))
            {
                if (tableDirectory.Exists)
                    return false;
                else
                {
                    tableDirectory.Create();
                    return true;
                }
            }
        }

        public bool Delete(string tableName)
        {
            DirectoryInfo tableDirectory = _GetTableDirectory(tableName);

            using (FileLock.Exclusive(tableDirectory, _fileLockOptions))
            {
                if (tableDirectory.Exists)
                {
                    tableDirectory.Delete(true);
                    return true;
                }
                else
                    return false;
            }
        }

        public bool Exists(string tableName)
            => _GetTableDirectory(tableName).Exists;

        public IPartitionClusterStorageHandler GetPartitionClusterStorageHandler(string tableName, string partitionKey)
        {
            if (!Exists(tableName))
                throw new KeyNotFoundException();

            var tableDirectory = _GetTableDirectory(tableName);
            var partitionClusterFileName = _md5HashAlgorithm
                .ComputeHash(Encoding.UTF8.GetBytes(partitionKey))
                .Aggregate(new StringBuilder(), (stringBuilder, @byte) => stringBuilder.AppendFormat("{0:X2}", @byte))
                .Append(".json")
                .ToString();
            return new FilePartitionClusterStorageHandler(new FileInfo(Path.Combine(tableDirectory.FullName, partitionClusterFileName)), _fileLockOptions);
        }

        public IEnumerable<IPartitionClusterStorageHandler> GetPartitionClusterStorageHandlers(string tableName)
        {
            if (!Exists(tableName))
                throw new KeyNotFoundException();

            var tableDirectory = _GetTableDirectory(tableName);
            return tableDirectory
                .EnumerateFiles()
                .Select(partitionClusterFile => new FilePartitionClusterStorageHandler(partitionClusterFile, _fileLockOptions))
                .ToArray();
        }

        private DirectoryInfo _GetTableDirectory(string tableName)
            => new DirectoryInfo(Path.Combine(StorageDirectory.FullName, tableName));
    }
}