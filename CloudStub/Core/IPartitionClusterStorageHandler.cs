using System.IO;

namespace CloudStub.Core
{
    public interface IPartitionClusterStorageHandler
    {
        string Key { get; }

        TextReader OpenRead();

        TextWriter OpenWrite();
    }
}