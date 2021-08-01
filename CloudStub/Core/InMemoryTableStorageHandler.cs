using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace CloudStub.Core
{
    public class InMemoryTableStorageHandler : ITableStorageHandler
    {
        private readonly Dictionary<string, IDictionary<string, MemoryStream>> _tables;

        public InMemoryTableStorageHandler()
            => _tables = new Dictionary<string, IDictionary<string, MemoryStream>>();

        public IDisposable AquireTableLock(string tableName)
        {
            lock (_tables)
            {
                var table = _tables[tableName];

                var lockTaken = false;
                try
                {
                    Monitor.Enter(table, ref lockTaken);
                    return new CallbackDisposable(() => Monitor.Exit(table));
                }
                catch
                {
                    if (lockTaken)
                        Monitor.Exit(table);
                    throw;
                }
            }
        }

        public IDisposable AquirePartitionClusterLock(string tableName, string partitionKey)
        {
            lock (_tables)
            {
                var table = _tables[tableName];
                if (!table.TryGetValue(partitionKey, out var partition))
                {
                    partition = new MemoryStream();
                    table.Add(partitionKey, partition);
                }

                var lockTaken = false;
                try
                {
                    Monitor.Enter(partition, ref lockTaken);
                    return new CallbackDisposable(() => Monitor.Exit(partition));
                }
                catch
                {
                    if (lockTaken)
                        Monitor.Exit(partition);
                    throw;
                }
            }
        }

        public bool Create(string tableName)
        {
            lock (_tables)
                if (!_tables.ContainsKey(tableName))
                {
                    _tables.Add(tableName, new Dictionary<string, MemoryStream>());
                    return true;
                }
                else
                    return false;
        }

        public bool Exists(string tableName)
        {
            lock (_tables)
                return _tables.ContainsKey(tableName);
        }

        public bool Delete(string tableName)
        {
            lock (_tables)
                return _tables.Remove(tableName);
        }

        public IEnumerable<Func<TextReader>> GetPartitionClustersTextReaderProviders(string tableName)
        {
            lock (_tables)
                return _tables[tableName]
                    .Values
                    .Select(partition => new Func<TextReader>(() =>
                    {
                        partition.Seek(0, SeekOrigin.Begin);
                        return new StreamReader(partition, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 1 << 11, leaveOpen: true);
                    }))
                    .ToList();
        }

        public TextReader GetPartitionClusterTextReader(string tableName, string partitionKey)
        {
            lock (_tables)
            {
                var table = _tables[tableName];
                if (table.TryGetValue(partitionKey, out var partition))
                {
                    partition.Seek(0, SeekOrigin.Begin);
                    return new StreamReader(partition, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 1 << 11, leaveOpen: true);
                }
                else
                    return new StringReader(string.Empty);
            }
        }

        public TextWriter GetPartitionClusterTextWriter(string tableName, string partitionKey)
        {
            lock (_tables)
            {
                var table = _tables[tableName];
                if (!table.TryGetValue(partitionKey, out var partition))
                {
                    partition = new MemoryStream();
                    table.Add(partitionKey, partition);
                }
                else
                {
                    partition.Seek(0, SeekOrigin.Begin);
                    partition.SetLength(0);
                }
                return new StreamWriter(partition, Encoding.UTF8, bufferSize: 1 << 11, leaveOpen: true);
            }
        }
    }
}