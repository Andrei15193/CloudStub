using System;
using System.IO;
using System.Threading;

namespace CloudStub.StorageHandlers
{
    internal class FileLock : StreamReader, IDisposable
    {
        public static IDisposable Exclusive(FileSystemInfo file, FileLockOptions options)
        {
            var lockFile = _GetLockFile(file);
            var lockFileStream = default(FileStream);
            var lockRetryCounter = 0;
            do
                try
                {
                    lockFileStream = new FileStream(lockFile.FullName, FileMode.CreateNew, FileAccess.Write, FileShare.None, 0, FileOptions.DeleteOnClose);
                }
                catch (IOException) when (options.LockRetryCount == null || lockRetryCounter < options.LockRetryCount)
                {
                    if (options.LockIntervalCheck <= TimeSpan.Zero)
                        Thread.Yield();
                    else
                        Thread.Sleep(options.LockIntervalCheck);
                    lockRetryCounter++;
                }
            while (lockFileStream is null);

            return lockFileStream;
        }

        public static TextWriter OpenExclusiveWrite(FileInfo file, FileLockOptions options)
        {
            using (Exclusive(file, options))
            {
                var fileStream = default(FileStream);
                var lockRetryCounter = 0;
                do
                    try
                    {
                        fileStream = new FileStream(file.FullName, FileMode.Create, FileAccess.Write, FileShare.None);
                    }
                    catch (IOException) when (options.LockRetryCount == null || lockRetryCounter < options.LockRetryCount)
                    {
                        if (options.LockIntervalCheck <= TimeSpan.Zero)
                            Thread.Yield();
                        else
                            Thread.Sleep(options.LockIntervalCheck);
                        lockRetryCounter++;
                    }
                while (fileStream is null);

                return new StreamWriter(fileStream);
            }
        }

        public static TextReader OpenSharedRead(FileInfo file, FileLockOptions options)
        {
            var lockFile = _GetLockFile(file);
            var lockRetryCounter = 0;

            while (lockFile.Exists && (options.LockRetryCount == null || lockRetryCounter < options.LockRetryCount))
            {
                if (options.LockIntervalCheck <= TimeSpan.Zero)
                    Thread.Yield();
                else
                    Thread.Sleep(options.LockIntervalCheck);
                lockRetryCounter++;
            }

            if (options.LockRetryCount > 0 && lockRetryCounter >= options.LockRetryCount)
                throw new IOException($"Unable to aquire read lock on \"{file.FullName}\".");

            var fileStream = default(FileStream);
            lockRetryCounter = 0;
            do
                try
                {
                    fileStream = new FileStream(file.FullName, FileMode.OpenOrCreate, FileAccess.Read, FileShare.Read);
                }
                catch (IOException) when (options.LockRetryCount == null || lockRetryCounter < options.LockRetryCount)
                {
                    if (options.LockIntervalCheck <= TimeSpan.Zero)
                        Thread.Yield();
                    else
                        Thread.Sleep(options.LockIntervalCheck);
                    lockRetryCounter++;
                }
            while (fileStream is null);

            return new StreamReader(fileStream);
        }

        private static FileInfo _GetLockFile(FileSystemInfo file)
            => new FileInfo($"{file.FullName}.lock");

        //private readonly FileStream _lock;

        private FileLock(Stream stream)
            : base(stream)
        {

        }

        //public FileLock(FileSystemInfo file, FileLockOptions options)
        //{
        //    var lockFile = new FileInfo($"{file.FullName}.lock");
        //    var lockRetryCounter = 0;
        //    do
        //        try
        //        {
        //            _lock = new FileStream(lockFile.FullName, FileMode.CreateNew, FileAccess.Write, FileShare.None, 0, FileOptions.DeleteOnClose);
        //        }
        //        catch (IOException) when (options.LockRetryCount == null || lockRetryCounter < options.LockRetryCount)
        //        {
        //            if (options.LockIntervalCheck <= TimeSpan.Zero)
        //                Thread.Yield();
        //            else
        //                Thread.Sleep(options.LockIntervalCheck);
        //            lockRetryCounter++;
        //        }
        //    while (_lock is null);
        //}

        //protected override void Dispose(bool disposing)
        //{
        //    if (disposing)
        //        _lock?.Dispose();
        //}
    }
}