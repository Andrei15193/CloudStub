
using System;
using System.Collections.Generic;
using System.Threading;

namespace CloudStub.AzureDataTables
{
    internal class TableCollectionStub : SortedList<string, TableItemStub>
    {
        private readonly ReaderWriterLockSlim _tablesLock = new ReaderWriterLockSlim();

        public TableCollectionStub()
            : base(StringComparer.OrdinalIgnoreCase)
        {
        }

        public IDisposable ReadLock()
            => new ReadLock(_tablesLock);

        public IDisposable WriteLock()
            => new WriteLock(_tablesLock);

        public IDisposable UpgradableReadLock()
            => new UpgradableReadLock(_tablesLock);
    }

    internal class TableItemStub : SortedList<string, TablePartitionStub>
    {
        private readonly ReaderWriterLockSlim _tableLock = new ReaderWriterLockSlim();

        public TableItemStub()
            : base(StringComparer.Ordinal)
        {
        }

        public IDisposable ReadLock()
            => new ReadLock(_tableLock);

        public IDisposable WriteLock()
            => new WriteLock(_tableLock);

        public IDisposable UpgradableReadLock()
            => new UpgradableReadLock(_tableLock);
    }

    internal class TablePartitionStub : SortedList<string, TableRowStub>
    {
        public TablePartitionStub()
            : base(StringComparer.Ordinal)
        {
        }
    }

    internal class TableRowStub : Dictionary<string, object>
    {
    }

    internal class ReadLock : IDisposable
    {
        private readonly ReaderWriterLockSlim _readerWriterLockSlim;

        public ReadLock(ReaderWriterLockSlim readerWriterLockSlim)
        {
            _readerWriterLockSlim = readerWriterLockSlim;
            _readerWriterLockSlim.EnterReadLock();
        }

        public void Dispose()
            => _readerWriterLockSlim.ExitReadLock();
    }

    internal class WriteLock : IDisposable
    {
        private readonly ReaderWriterLockSlim _readerWriterLockSlim;

        public WriteLock(ReaderWriterLockSlim readerWriterLockSlim)
        {
            _readerWriterLockSlim = readerWriterLockSlim;
            _readerWriterLockSlim.EnterWriteLock();
        }

        public void Dispose()
            => _readerWriterLockSlim.ExitWriteLock();
    }

    internal class UpgradableReadLock : IDisposable
    {
        private readonly ReaderWriterLockSlim _readerWriterLockSlim;

        public UpgradableReadLock(ReaderWriterLockSlim readerWriterLockSlim)
        {
            _readerWriterLockSlim = readerWriterLockSlim;
            _readerWriterLockSlim.EnterUpgradeableReadLock();
        }

        public void Dispose()
            => _readerWriterLockSlim.ExitUpgradeableReadLock();
    }
}