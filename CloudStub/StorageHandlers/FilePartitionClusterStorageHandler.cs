using System.IO;

namespace CloudStub.StorageHandlers
{
    internal class FilePartitionClusterStorageHandler : IPartitionClusterStorageHandler
    {
        private readonly FileInfo _partitionClusterFile;
        private readonly FileLockOptions _fileLockOptions;

        public FilePartitionClusterStorageHandler(FileInfo partitionClusterFile, FileLockOptions fileLockOptions)
            => (_partitionClusterFile, _fileLockOptions) = (partitionClusterFile, fileLockOptions);

        public string Key
            => _partitionClusterFile.Name.Substring(0, _partitionClusterFile.Name.Length - _partitionClusterFile.Extension.Length);

        public TextReader OpenRead()
            => FileLock.OpenSharedRead(_partitionClusterFile, _fileLockOptions);

        public TextWriter OpenWrite()
            => FileLock.OpenExclusiveWrite(_partitionClusterFile, _fileLockOptions);
    }
}