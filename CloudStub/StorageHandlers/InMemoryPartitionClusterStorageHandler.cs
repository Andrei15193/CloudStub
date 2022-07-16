using System.IO;
using System.Text;

namespace CloudStub.StorageHandlers
{
    internal class InMemoryPartitionClusterStorageHandler : IPartitionClusterStorageHandler
    {
        private readonly MemoryStream _partitionCluster = new MemoryStream();

        public InMemoryPartitionClusterStorageHandler(string key)
            => Key = key;

        public string Key { get; }

        public TextReader OpenRead()
        {
            _partitionCluster.Seek(0, SeekOrigin.Begin);
            return new StreamReader(_partitionCluster, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 1 << 11, leaveOpen: true);
        }

        public TextWriter OpenWrite()
        {
            _partitionCluster.Seek(0, SeekOrigin.Begin);
            _partitionCluster.SetLength(0);
            return new StreamWriter(_partitionCluster, Encoding.UTF8, bufferSize: 1 << 11, leaveOpen: true);
        }
    }
}