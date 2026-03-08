
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Azure;
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
        public T MapToEntity<T>(IEnumerable<string> selectedProperties)
        {
            if (typeof(T) == typeof(TableEntity) && !(selectedProperties?.Any() ?? false))
                return (T)(new TableEntity(this) as object);
            else if (typeof(IDictionary<string, object>).IsAssignableFrom(typeof(T)))
            {
                var entity = (IDictionary<string, object>)Activator.CreateInstance<T>();
                foreach (var property in this)
                    if (selectedProperties == null || selectedProperties.Contains(property.Key, StringComparer.OrdinalIgnoreCase))
                        entity[property.Key] = property.Value;

                if (selectedProperties != null)
                    foreach (var property in selectedProperties)
                        if (!entity.ContainsKey(property))
                            entity[property] = null;

                return (T)entity;
            }
            else
            {
                var entity = Activator.CreateInstance<T>();

                foreach (var field in typeof(T).GetFields())
                    if (selectedProperties == null || selectedProperties.Contains(field.Name, StringComparer.OrdinalIgnoreCase))
                        _TrySetProperty(field.Name, field.FieldType, value => field.SetValue(entity, value));

                foreach (var property in typeof(T).GetProperties())
                    if (property.CanWrite && (selectedProperties == null || selectedProperties.Contains(property.Name, StringComparer.OrdinalIgnoreCase)))
                        _TrySetProperty(property.Name, property.PropertyType, value => property.SetValue(entity, value));

                return entity;
            }
        }

        private void _TrySetProperty(string targetName, Type targetType, Action<object> setValueAction)
        {
            var entityPropertyName = targetName == nameof(ITableEntity.ETag) ? "odata.etag" : targetName;
            var isNullableType = Nullable.GetUnderlyingType(targetType) != null;
            var resolvedTargetType = Nullable.GetUnderlyingType(targetType) ?? targetType;

            if (TryGetValue(entityPropertyName, out var sourceValue))
                if (resolvedTargetType == typeof(ETag) && sourceValue is string stringValueForETag)
                {
                    var etag = new ETag(stringValueForETag);
                    setValueAction(isNullableType ? (ETag?)etag : etag);
                }

                else if (resolvedTargetType == typeof(string) && sourceValue is string stringValue)
                    setValueAction(stringValue);

                else if (resolvedTargetType == typeof(bool) && sourceValue is bool boolValue)
                    setValueAction(isNullableType ? (bool?)boolValue : boolValue);

                else if (resolvedTargetType == typeof(int) && sourceValue is int intForIntValue)
                    setValueAction(isNullableType ? (int?)intForIntValue : intForIntValue);

                else if (resolvedTargetType == typeof(long) && sourceValue is int intForLongValue)
                    setValueAction(isNullableType ? (long?)intForLongValue : (long)intForLongValue);
                else if (resolvedTargetType == typeof(long) && sourceValue is long longForLongValue)
                    setValueAction(isNullableType ? (long?)longForLongValue : longForLongValue);

                else if (resolvedTargetType == typeof(float) && sourceValue is int intForFloatValue)
                    setValueAction(isNullableType ? (float?)intForFloatValue : (float)intForFloatValue);
                else if (resolvedTargetType == typeof(float) && sourceValue is long longForFloatValue)
                    setValueAction(isNullableType ? (float?)longForFloatValue : (float)longForFloatValue);

                else if (resolvedTargetType == typeof(double) && sourceValue is int intForDoubleValue)
                    setValueAction(isNullableType ? (double?)intForDoubleValue : (double)intForDoubleValue);
                else if (resolvedTargetType == typeof(double) && sourceValue is long longForDoubleValue)
                    setValueAction(isNullableType ? (double?)longForDoubleValue : (double)longForDoubleValue);
                else if (resolvedTargetType == typeof(double) && sourceValue is double doubleForDoubleValue)
                    setValueAction(isNullableType ? (double?)doubleForDoubleValue : doubleForDoubleValue);

                else if (resolvedTargetType == typeof(decimal) && sourceValue is int intForDecimalValue)
                    setValueAction(isNullableType ? (decimal?)intForDecimalValue : (decimal)intForDecimalValue);
                else if (resolvedTargetType == typeof(decimal) && sourceValue is long longForDecimalValue)
                    setValueAction(isNullableType ? (decimal?)longForDecimalValue : (decimal)longForDecimalValue);
                else if (resolvedTargetType == typeof(decimal) && sourceValue is double doubleForDecimalValue)
                    setValueAction(isNullableType ? (decimal?)doubleForDecimalValue : (decimal)doubleForDecimalValue);

                else if (resolvedTargetType == typeof(DateTime) && sourceValue is DateTimeOffset dateTimeOffsetForDateTimeValue)
                    setValueAction(isNullableType ? (DateTime?)dateTimeOffsetForDateTimeValue.UtcDateTime : dateTimeOffsetForDateTimeValue.UtcDateTime);
                else if (resolvedTargetType == typeof(DateTimeOffset) && sourceValue is DateTimeOffset dateTimeOffsetForDateTimeOffsetValue)
                    setValueAction(isNullableType ? (DateTimeOffset?)dateTimeOffsetForDateTimeOffsetValue : dateTimeOffsetForDateTimeOffsetValue);

                else if (resolvedTargetType == typeof(Guid) && sourceValue is Guid guidValue)
                    setValueAction(isNullableType ? (Guid?)guidValue : guidValue);

                else if (resolvedTargetType == typeof(byte[]) && sourceValue is byte[] byteArrayValue)
                {
                    var binaryArrayCopy = new byte[byteArrayValue.Length];
                    Array.Copy(byteArrayValue, binaryArrayCopy, binaryArrayCopy.Length);
                    setValueAction(binaryArrayCopy);
                }

                else
                    setValueAction(Convert.ChangeType(sourceValue, targetType));
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