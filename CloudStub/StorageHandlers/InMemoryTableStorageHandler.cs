using System;
using System.Collections.Generic;

namespace CloudStub.StorageHandlers
{
    public class InMemoryTableStorageHandler : ITableStorageHandler
    {
        private readonly IDictionary<string, IDictionary<string, InMemoryPartitionClusterStorageHandler>> _tables;

        public InMemoryTableStorageHandler()
            => _tables = new Dictionary<string, IDictionary<string, InMemoryPartitionClusterStorageHandler>>(StringComparer.Ordinal);

        public bool Create(string tableName)
        {
            if (!_tables.ContainsKey(tableName))
            {
                _tables.Add(tableName, new Dictionary<string, InMemoryPartitionClusterStorageHandler>(StringComparer.Ordinal));
                return true;
            }
            else
                return false;
        }

        public bool Exists(string tableName)
            => _tables.ContainsKey(tableName);

        public bool Delete(string tableName)
            => _tables.Remove(tableName);

        public IEnumerable<IPartitionClusterStorageHandler> GetPartitionClusterStorageHandlers(string tableName)
            => _tables[tableName].Values;

        public IPartitionClusterStorageHandler GetPartitionClusterStorageHandler(string tableName, string partitionKey)
        {
            var table = _tables[tableName];
            if (!table.TryGetValue(partitionKey, out var partitionClusterStorageHandler))
            {
                partitionClusterStorageHandler = new InMemoryPartitionClusterStorageHandler(partitionKey);
                table.Add(partitionKey, partitionClusterStorageHandler);
            }
            return partitionClusterStorageHandler;
        }
    }
}