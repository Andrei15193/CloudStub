using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace CloudStub.Core
{
    public class InMemoryTableStorageHandler : TableStorageHandler
    {
        private readonly IDictionary<(string TableName, string PartitionKey), (object SyncObject, MemoryStream Content)> _tablePartitionStreams;

        public InMemoryTableStorageHandler()
            => _tablePartitionStreams = new Dictionary<(string TableName, string PartitionKey), (object SyncObject, MemoryStream Content)>();

        public override bool Exists(string tableName)
        {
            lock (_tablePartitionStreams)
                return _tablePartitionStreams.Keys.Any(key =>
                {
                    if (key.TableName == tableName)
                        using (var reader = GetTextReader(tableName, key.PartitionKey))
                            return reader.Read() != -1;
                    else
                        return false;
                });
        }

        public override void Delete(string tableName)
        {
            lock (_tablePartitionStreams)
                foreach (var keyToRemove in _tablePartitionStreams.Keys.Where(key => key.TableName == tableName).ToList())
                    _tablePartitionStreams.Remove(keyToRemove);
        }

        public override IEnumerable<TextReader> GetPartitionTextReaders(string tableName)
        {
            lock (_tablePartitionStreams)
                return _tablePartitionStreams
                    .Where(pair => pair.Key.TableName == tableName)
                    .Select(pair =>
                    {
                        var partition = pair.Value;
                        partition.Content.Seek(0, SeekOrigin.Begin);
                        return Synchronize(
                            new StreamReader(partition.Content, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 1 << 11, leaveOpen: true),
                            partition.SyncObject
                        );
                    })
                    .ToList();
        }

        public override TextReader GetTextReader(string tableName, string partitionKey)
        {
            lock (_tablePartitionStreams)
                if (_tablePartitionStreams.TryGetValue((tableName, partitionKey), out var partition))
                {
                    partition.Content.Seek(0, SeekOrigin.Begin);
                    return Synchronize(
                        new StreamReader(partition.Content, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 1 << 11, leaveOpen: true),
                        partition.SyncObject
                    );
                }
                else
                    return new StringReader(string.Empty);
        }

        public override TextWriter GetTextWriter(string tableName, string partitionKey)
        {
            lock (_tablePartitionStreams)
            {
                if (!_tablePartitionStreams.TryGetValue((tableName, partitionKey), out var partition))
                {
                    partition = (new object(), new MemoryStream());
                    _tablePartitionStreams.Add((tableName, partitionKey), partition);
                }
                partition.Content.Seek(0, SeekOrigin.Begin);
                partition.Content.SetLength(0);
                return Synchronize(
                    new StreamWriter(partition.Content, Encoding.UTF8, bufferSize: 1 << 11, leaveOpen: true),
                    partition.SyncObject
                );
            }
        }
    }
}