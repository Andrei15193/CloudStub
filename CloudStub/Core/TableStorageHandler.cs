using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CloudStub.Core
{
    public abstract class TableStorageHandler
    {
        public abstract bool Exists(string tableName);

        public abstract void Delete(string tableName);

        public abstract IEnumerable<TextReader> GetPartitionTextReaders(string tableName);

        public abstract TextReader GetTextReader(string tableName, string partitionKey);

        public abstract TextWriter GetTextWriter(string tableName, string partitionKey);

        protected static TextReader Synchronize(TextReader textReader, object syncObject)
        {
            var lockTaken = false;
            try
            {
                Monitor.Enter(syncObject, ref lockTaken);
                return new SynchronizedTextReader(textReader, syncObject);
            }
            catch
            {
                if (lockTaken)
                    Monitor.Exit(syncObject);
                throw;
            }
        }

        protected static TextWriter Synchronize(TextWriter textWriter, object syncObject)
        {
            var lockTaken = false;
            try
            {
                Monitor.Enter(syncObject, ref lockTaken);
                return new SynchronizedTextWriter(textWriter, syncObject);
            }
            catch
            {
                if (lockTaken)
                    Monitor.Exit(syncObject);
                throw;
            }
        }

        private class SynchronizedTextReader : TextReader
        {
            private volatile bool _disposed = false;
            private readonly TextReader _textReader;
            private readonly object _syncObject;

            public SynchronizedTextReader(TextReader textReader, object syncObject)
                => (_textReader, _syncObject) = (textReader, syncObject);

            public override void Close()
                => _textReader.Close();

            public override object InitializeLifetimeService()
                => _textReader.InitializeLifetimeService();

            public override int Peek()
                => _textReader.Peek();

            public override int Read()
                => _textReader.Read();

            public override int Read(char[] buffer, int index, int count)
                => _textReader.Read(buffer, index, count);

            public override Task<int> ReadAsync(char[] buffer, int index, int count)
                => _textReader.ReadAsync(buffer, index, count);

            public override int ReadBlock(char[] buffer, int index, int count)
                => _textReader.ReadBlock(buffer, index, count);

            public override Task<int> ReadBlockAsync(char[] buffer, int index, int count)
                => _textReader.ReadBlockAsync(buffer, index, count);

            public override string ReadLine()
                => _textReader.ReadLine();

            public override Task<string> ReadLineAsync()
                => _textReader.ReadLineAsync();

            public override string ReadToEnd()
                => _textReader.ReadToEnd();

            public override Task<string> ReadToEndAsync()
                => _textReader.ReadToEndAsync();

            protected override void Dispose(bool disposing)
            {
                try
                {
                    if (!_disposed)
                    {
                        Monitor.Exit(_syncObject);
                        _disposed = true;
                    }
                }
                finally
                {
                    if (disposing)
                        _textReader.Dispose();
                }
            }
        }

        private class SynchronizedTextWriter : TextWriter
        {
            private volatile bool _disposed = false;
            private readonly TextWriter _textWriter;
            private readonly object _syncObject;

            public SynchronizedTextWriter(TextWriter textWriter, object syncObject)
                => (_textWriter, _syncObject) = (textWriter, syncObject);

            public override Encoding Encoding
                => _textWriter.Encoding;

            public override IFormatProvider FormatProvider
                => _textWriter.FormatProvider;

            public override string NewLine
            {
                get => _textWriter.NewLine;
                set => _textWriter.NewLine = value;
            }

            public override void Close()
                => _textWriter.Close();

            public override void Flush()
                => _textWriter.Flush();

            public override Task FlushAsync()
                => _textWriter.FlushAsync();

            public override object InitializeLifetimeService()
                => _textWriter.InitializeLifetimeService();

            public override void Write(bool value)
                => _textWriter.Write(value);

            public override void Write(char value)
                => _textWriter.Write(value);

            public override void Write(char[] buffer)
                => _textWriter.Write(buffer);

            public override void Write(char[] buffer, int index, int count)
                => _textWriter.Write(buffer, index, count);

            public override void Write(decimal value)
                => _textWriter.Write(value);

            public override void Write(double value)
                => _textWriter.Write(value);

            public override void Write(int value)
                => _textWriter.Write(value);

            public override void Write(long value)
                => _textWriter.Write(value);

            public override void Write(object value)
                => _textWriter.Write(value);

            public override void Write(float value)
                => _textWriter.Write(value);

            public override void Write(string value)
                => _textWriter.Write(value);

            public override void Write(string format, object arg0)
                => _textWriter.Write(format, arg0);

            public override void Write(string format, object arg0, object arg1)
                => _textWriter.Write(format, arg0, arg1);

            public override void Write(string format, object arg0, object arg1, object arg2)
                => _textWriter.Write(format, arg0, arg1, arg2);

            public override void Write(string format, params object[] arg)
                => _textWriter.Write(format, arg);

            public override void Write(uint value)
                => _textWriter.Write(value);

            public override void Write(ulong value)
                => _textWriter.Write(value);

            public override Task WriteAsync(char value)
                => _textWriter.WriteAsync(value);

            public override Task WriteAsync(char[] buffer, int index, int count)
                => _textWriter.WriteAsync(buffer, index, count);

            public override Task WriteAsync(string value)
                => _textWriter.WriteAsync(value);

            public override void WriteLine()
                => _textWriter.WriteLine();

            public override void WriteLine(bool value)
                => _textWriter.WriteLine(value);

            public override void WriteLine(char value)
                => _textWriter.WriteLine(value);

            public override void WriteLine(char[] buffer)
                => _textWriter.WriteLine(buffer);

            public override void WriteLine(char[] buffer, int index, int count)
                => _textWriter.WriteLine(buffer, index, count);

            public override void WriteLine(decimal value)
                => _textWriter.WriteLine(value);

            public override void WriteLine(double value)
                => _textWriter.WriteLine(value);

            public override void WriteLine(int value)
                => _textWriter.WriteLine(value);

            public override void WriteLine(long value)
                => _textWriter.WriteLine(value);

            public override void WriteLine(object value)
                => _textWriter.WriteLine(value);

            public override void WriteLine(float value)
                => _textWriter.WriteLine(value);

            public override void WriteLine(string value)
                => _textWriter.WriteLine(value);

            public override void WriteLine(string format, object arg0)
                => _textWriter.WriteLine(format, arg0);

            public override void WriteLine(string format, object arg0, object arg1)
                => _textWriter.WriteLine(format, arg0, arg1);

            public override void WriteLine(string format, object arg0, object arg1, object arg2)
                => _textWriter.WriteLine(format, arg0, arg1, arg2);

            public override void WriteLine(string format, params object[] arg)
                => _textWriter.WriteLine(format, arg);

            public override void WriteLine(uint value)
                => _textWriter.WriteLine(value);

            public override void WriteLine(ulong value)
                => _textWriter.WriteLine(value);

            public override Task WriteLineAsync()
                => _textWriter.WriteLineAsync();

            public override Task WriteLineAsync(char value)
                => _textWriter.WriteLineAsync(value);

            public override Task WriteLineAsync(char[] buffer, int index, int count)
                => _textWriter.WriteLineAsync(buffer, index, count);

            public override Task WriteLineAsync(string value)
                => _textWriter.WriteLineAsync(value);

            protected override void Dispose(bool disposing)
            {
                try
                {
                    if (!_disposed)
                    {
                        Monitor.Exit(_syncObject);
                        _disposed = true;
                    }
                }
                finally
                {
                    if (disposing)
                        _textWriter.Dispose();
                }
            }
        }
    }
}