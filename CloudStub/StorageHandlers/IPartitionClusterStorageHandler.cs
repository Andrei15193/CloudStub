using System.IO;

namespace CloudStub.StorageHandlers
{
    public interface IPartitionClusterStorageHandler
    {
        string Key { get; }

        TextReader OpenRead();

        TextWriter OpenWrite();
    }
}