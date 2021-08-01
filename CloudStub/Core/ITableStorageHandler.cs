using System;
using System.Collections.Generic;
using System.IO;

namespace CloudStub.Core
{
    public interface ITableStorageHandler
    {
        IDisposable AquirePartitionClusterLock(string tableName, string partitionKey);
        IDisposable AquireTableLock(string tableName);
        bool Create(string tableName);
        bool Delete(string tableName);
        bool Exists(string tableName);
        IEnumerable<Func<TextReader>> GetPartitionClustersTextReaderProviders(string tableName);
        TextReader GetPartitionClusterTextReader(string tableName, string partitionKey);
        TextWriter GetPartitionClusterTextWriter(string tableName, string partitionKey);
    }
}