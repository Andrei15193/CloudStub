
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;

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

        public IReadOnlyList<TableSignedIdentifier> SignedIdentifiers { get; set; } = Array.Empty<TableSignedIdentifier>();

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
        // selected fields
        public T MapToEntity<T>()
        {
            if (typeof(T) == typeof(TableEntity))
                return (T)(new TableEntity(this) as object);
            else if (typeof(IDictionary<string, object>).IsAssignableFrom(typeof(T)))
            {
                var entity = (IDictionary<string, object>)Activator.CreateInstance<T>();
                foreach (var property in this)
                    entity[property.Key] = property.Value;

                return (T)entity;
            }
            else
            {
                var entity = Activator.CreateInstance<T>();

                foreach (var field in typeof(T).GetFields())
                {
                    var isPrimitive = (
                        field.FieldType == typeof(string)
                        || field.FieldType == typeof(DateTimeOffset)
                        || field.FieldType == typeof(Guid)
                        || field.FieldType == typeof(byte[])
                        || field.FieldType.IsPrimitive
                    );

                    if (isPrimitive && TryGetValue(field.Name, out var value))
                        if (field.FieldType == typeof(byte[]) && value?.GetType() == typeof(byte[]))
                        {
                            var binaryArrayCopy = new byte[((byte[])value).Length];
                            Array.Copy((byte[])value, binaryArrayCopy, binaryArrayCopy.Length);
                            field.SetValue(entity, binaryArrayCopy);
                        }
                        else
                            field.SetValue(entity, Convert.ChangeType(value, field.FieldType));
                }

                foreach (var property in typeof(T).GetProperties())
                {
                    var isPrimitive = (
                        property.PropertyType == typeof(string)
                        || property.PropertyType == typeof(DateTimeOffset)
                        || property.PropertyType == typeof(Guid)
                        || property.PropertyType == typeof(byte[])
                        || property.PropertyType.IsPrimitive
                    );

                    if (property.CanWrite && isPrimitive && TryGetValue(property.Name, out var value))
                        if (property.PropertyType == typeof(byte[]) && value?.GetType() == typeof(byte[]))
                        {
                            var binaryArrayCopy = new byte[((byte[])value).Length];
                            Array.Copy((byte[])value, binaryArrayCopy, binaryArrayCopy.Length);
                            property.SetValue(entity, binaryArrayCopy);
                        }
                        else
                            property.SetValue(entity, Convert.ChangeType(value, property.PropertyType));
                }

                return entity;
            }
        }
    }

    internal class ValidatedTableRowStub<T> : TableRowStub
    {
        private const int MaximumKeyLength = 1 << 10 + 1;
        private const int MaximumStringLength = 1 << 15 + 1;
        private const int MaximumBinaryLength = 1 << 16 + 1;
        private static DateTimeOffset MinimumDateTimeOffset = new DateTimeOffset(1601, 1, 1, 0, 0, 0, TimeSpan.Zero);
        private static readonly IReadOnlyCollection<char> ReservedKeyCharacters = new HashSet<char> { '#', '?', '\t', '\n', '\r', '/', '\\' };

        public ValidatedTableRowStub(T entity)
        {
            if (typeof(T) == typeof(TableEntity))
            {
                var tableEntity = (TableEntity)(entity as object);
                foreach (var row in tableEntity)
                    _TrySetValue(row.Key, row.Value);
            }
            else
            {
                foreach (var field in typeof(T).GetFields())
                    _TrySetValue(field.Name, field.GetValue(entity));

                foreach (var property in typeof(T).GetProperties())
                    if (property.GetIndexParameters().Length == 0)
                        _TrySetValue(property.Name, property.GetValue(entity));
            }

            this["odata.etag"] = ETag = _GenerateETag(out var timestamp);
            this["Timestamp"] = Timestamp = timestamp;
        }

        public string ETag { get; private set; }
        public DateTimeOffset Timestamp { get; private set; }

        public bool IsPartitionKeyInvalid { get; private set; }
        public bool IsRowKeyInvalid { get; private set; }
        public bool IsPartitionKeyExceedingMaxLength { get; private set; }
        public bool IsRowKeyExceedingMaxLength { get; private set; }
        public bool IsStringPropertyExceedingMaxLength { get; private set; }
        public bool IsBinaryPropertyExceedingMaxLength { get; private set; }
        public DateTime? NotSupportedDateTimeValue { get; private set; }
        public KeyValuePair<string, object>? InvalidDateTimeProperty { get; private set; }

        private void _TrySetValue(string propertyName, object value)
        {
            if (value == null)
                return;

            if (
              value is bool
              || value is int
              || value is long
              || value is double
              || value is Guid
          )
                this[propertyName] = value;

            else if (value is float floatValue)
                this[propertyName] = (double)floatValue;

            else if (value is string stringValue)
            {
                this[propertyName] = value;

                if (propertyName == "PartitionKey")
                {
                    IsPartitionKeyInvalid = _KeyContainsInvalidCharacters(stringValue);
                    if (stringValue.Length >= MaximumKeyLength)
                        IsPartitionKeyExceedingMaxLength = true;
                }
                else if (propertyName == "RowKey")
                {
                    IsRowKeyInvalid = _KeyContainsInvalidCharacters(stringValue);
                    if (stringValue.Length >= MaximumKeyLength)
                        IsRowKeyExceedingMaxLength = true;
                }
                else
                    IsStringPropertyExceedingMaxLength = IsStringPropertyExceedingMaxLength || stringValue.Length >= MaximumStringLength;
            }

            else if (value is DateTimeOffset dateTimeOffsetValue)
            {
                this[propertyName] = dateTimeOffsetValue.Offset != TimeSpan.Zero ? dateTimeOffsetValue.ToUniversalTime() : value;

                if (dateTimeOffsetValue < MinimumDateTimeOffset)
                    InvalidDateTimeProperty = InvalidDateTimeProperty ?? new KeyValuePair<string, object>(propertyName, value);
            }
            else if (value is DateTime dateTimeValue)
            {
                this[propertyName] = new DateTimeOffset(dateTimeValue);

                if (dateTimeValue.Kind != DateTimeKind.Utc)
                    NotSupportedDateTimeValue = dateTimeValue;
                if (dateTimeValue < MinimumDateTimeOffset)
                    InvalidDateTimeProperty = InvalidDateTimeProperty ?? new KeyValuePair<string, object>(propertyName, value);
            }

            else if (value is byte[] binaryValue)
            {
                var binaryValueCopy = new byte[binaryValue.Length];
                Array.Copy(binaryValue, binaryValueCopy, binaryValueCopy.Length);
                this[propertyName] = binaryValueCopy;

                if (binaryValueCopy.Length >= MaximumBinaryLength)
                    IsBinaryPropertyExceedingMaxLength = true;
            }

            else if (value is decimal decimalValue)
                if (decimal.Truncate(decimalValue) == decimalValue)
                    if (int.MinValue <= decimalValue && decimalValue <= int.MaxValue)
                        this[propertyName] = (int)decimalValue;
                    else if (long.MinValue <= decimalValue && decimalValue <= long.MaxValue)
                        this[propertyName] = (long)decimalValue;
                    else
                        this[propertyName] = (double)decimalValue;
                else
                    this[propertyName] = (double)decimalValue;
        }

        private static bool _KeyContainsInvalidCharacters(string keyValue)
            => keyValue.Any(@char =>
                (0x0000 <= @char && @char <= 0x001F)
                || ((char)0x007F <= @char && @char <= 0x009F)
                || ReservedKeyCharacters.Contains(@char)
            );

        private static string _GenerateETag(out DateTimeOffset timestamp)
        {
            timestamp = DateTimeOffset.UtcNow;
            return $@"W/""datetime'{timestamp:yyyy-MM-ddTHH:mm:ss.FFFFFFFZ}'""";
        }
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