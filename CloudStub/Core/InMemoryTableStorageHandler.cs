using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CloudStub.Core
{
    public class InMemoryTableStorageHandler : ITableStorageHandler
    {
        private readonly IDictionary<string, MemoryStream> _tableStreams;

        public InMemoryTableStorageHandler()
            => _tableStreams = new Dictionary<string, MemoryStream>(StringComparer.Ordinal);

        public bool Exists(string tableName)
            => _tableStreams.ContainsKey(tableName);

        public TextReader GetTextReader(string tableName)
        {
            if (_tableStreams.TryGetValue(tableName, out var partitionStream))
            {
                partitionStream.Seek(0, SeekOrigin.Begin);
                return new StreamReader(partitionStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 1 << 11, leaveOpen: true);
            }
            else
                return new StringReader(string.Empty);
        }

        public TextWriter GetTextWriter(string tableName)
        {
            if (!_tableStreams.TryGetValue(tableName, out var partitionStream))
            {
                partitionStream = new MemoryStream();
                _tableStreams.Add(tableName, partitionStream);
            }

            partitionStream.Seek(0, SeekOrigin.Begin);
            partitionStream.SetLength(0);
            return new StreamWriter(partitionStream, Encoding.UTF8, bufferSize: 1 << 11, leaveOpen: true);
        }

        public void Delete(string tableName)
            => _tableStreams.Remove(tableName);

        private Stream _GetTableStream(string tableName)
        {
            if (!_tableStreams.TryGetValue(tableName, out var partitionStream))
            {
                partitionStream = new MemoryStream();
                _tableStreams.Add(tableName, partitionStream);
            }
            return partitionStream;
        }
    }
}