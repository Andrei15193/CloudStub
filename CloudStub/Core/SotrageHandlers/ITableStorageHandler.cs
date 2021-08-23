using System.Collections.Generic;

namespace CloudStub.Core.StorageHandlers
{
    public interface ITableStorageHandler
    {
        bool Create(string tableName);

        bool Delete(string tableName);

        bool Exists(string tableName);

        IEnumerable<IPartitionClusterStorageHandler> GetPartitionClusterStorageHandlers(string tableName);

        IPartitionClusterStorageHandler GetPartitionClusterStorageHandler(string tableName, string partitionKey);
    }
}