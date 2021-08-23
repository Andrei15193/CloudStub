using System.IO;

namespace CloudStub.Core.StorageHandlers
{
    public interface IPartitionClusterStorageHandler
    {
        string Key { get; }

        TextReader OpenRead();

        TextWriter OpenWrite();
    }
}