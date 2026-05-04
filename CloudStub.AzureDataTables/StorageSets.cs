
using System;
using System.Collections.Generic;
using System.Globalization;
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

        public TableItemStub(string tableName)
            : base(StringComparer.Ordinal)
        {
            TableName = tableName;
        }

        public string TableName { get; }

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

    internal class TableRowStub : Dictionary<string, object>, ITableEntity
    {
        private static readonly IReadOnlyCollection<char> _reservedKeyCharacters = new HashSet<char> { '#', '?', '\t', '\n', '\r', '/', '\\' };

        public static bool IsReservedKeyCharacter(char character)
            => (
                (0x0000 <= character && character <= 0x001F)
                || (0x007F <= character && character <= 0x009F)
                || _reservedKeyCharacters.Contains(character)
            );

        private static string _GenerateETag(out DateTimeOffset timestamp)
        {
            timestamp = DateTimeOffset.UtcNow;
            return $@"W/""datetime'{timestamp:yyyy-MM-ddTHH:mm:ss.FFFFFFFZ}'""";
        }

        public TableRowStub()
        {
            this["odata.etag"] = _GenerateETag(out var timestamp);
            this["Timestamp"] = timestamp;
        }

        public string PartitionKey
        {
            get => TryGetValue(nameof(PartitionKey), out var partitionKey) ? (string)partitionKey : null;
            set => this[nameof(PartitionKey)] = value;
        }

        public string RowKey
        {
            get => TryGetValue(nameof(RowKey), out var rowKey) ? (string)rowKey : null;
            set => this[nameof(RowKey)] = value;
        }

        public DateTimeOffset? Timestamp
        {
            get => TryGetValue(nameof(RowKey), out var rowKey) ? (DateTimeOffset)rowKey : (DateTimeOffset?)null;
            set => this[nameof(Timestamp)] = value;
        }

        public ETag ETag
        {
            get => TryGetValue("odata.etag", out var etag) ? new ETag((string)etag) : default;
            set => this["odata.etag"] = value.ToString();
        }

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
                    if (
                        selectedProperties == null
                        || selectedProperties.Contains(field.Name, StringComparer.OrdinalIgnoreCase)
                        || (field.Name == nameof(ITableEntity.ETag) && selectedProperties.Contains("odata.etag", StringComparer.OrdinalIgnoreCase))
                    )
                        _TrySetProperty(field.Name, field.FieldType, value => field.SetValue(entity, value));

                foreach (var property in typeof(T).GetProperties())
                    if (
                        property.CanWrite && (
                            selectedProperties == null
                            || selectedProperties.Contains(property.Name, StringComparer.OrdinalIgnoreCase))
                            || (property.Name == nameof(ITableEntity.ETag) && selectedProperties.Contains("odata.etag", StringComparer.OrdinalIgnoreCase))
                        )
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

                else if (resolvedTargetType == typeof(int))
                    _SetInt32Value(resolvedTargetType, isNullableType, sourceValue, setValueAction);

                else if (resolvedTargetType == typeof(long))
                    _SetInt64Value(resolvedTargetType, isNullableType, sourceValue, setValueAction);

                else if (resolvedTargetType == typeof(double))
                    _SetDoubleValue(resolvedTargetType, isNullableType, sourceValue, setValueAction);

                else if (resolvedTargetType == typeof(bool))
                    _SetBooleanValue(resolvedTargetType, isNullableType, sourceValue, setValueAction);

                else if (resolvedTargetType == typeof(DateTime))
                    _SetDateTimeValue(resolvedTargetType, isNullableType, sourceValue, setValueAction);

                else if (resolvedTargetType == typeof(DateTimeOffset))
                    _SetDateTimeOffsetValue(resolvedTargetType, isNullableType, sourceValue, setValueAction);

                else if (resolvedTargetType == typeof(Guid))
                    _SetGuidValue(resolvedTargetType, isNullableType, sourceValue, setValueAction);

                else if (resolvedTargetType == typeof(byte[]))
                    _SetBinaryValue(resolvedTargetType, sourceValue, setValueAction);

                else if (resolvedTargetType == typeof(string))
                    _SetStringValue(resolvedTargetType, sourceValue, setValueAction);
        }

        private static void _SetInt32Value(Type resolvedTargetType, bool isNullableType, object sourceValue, Action<object> setValueAction)
        {
            if (sourceValue is int intValue)
                setValueAction(isNullableType ? (int?)intValue : intValue);
            else if (sourceValue is double)
                throw new InvalidCastException(
                    isNullableType
                    ? "Unable to cast object of type 'System.Double' to type 'System.Nullable`1[System.Int32]'."
                    : "Unable to cast object of type 'System.Double' to type 'System.Int32'.")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (sourceValue is bool)
                throw new InvalidCastException(
                    isNullableType
                    ? "Unable to cast object of type 'System.Boolean' to type 'System.Nullable`1[System.Int32]'."
                    : "Unable to cast object of type 'System.Boolean' to type 'System.Int32'.")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (
                sourceValue is long
                || sourceValue is string
                || sourceValue is DateTimeOffset
                || sourceValue is Guid
                || sourceValue is byte[]
            )
                throw new InvalidCastException(
                    isNullableType
                    ? "Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Int32]'."
                    : "Unable to cast object of type 'System.String' to type 'System.Int32'.")
                {
                    Source = "Azure.Data.Tables"
                };
            else
                throw new InvalidOperationException($"Unhanled {resolvedTargetType} target type for int32 deserialization.");
        }

        private static void _SetInt64Value(Type resolvedTargetType, bool isNullableType, object sourceValue, Action<object> setValueAction)
        {
            if (sourceValue is long longValue)
                setValueAction(isNullableType ? (long?)longValue : longValue);
            else if (
                sourceValue is int
                || sourceValue is double
                || sourceValue is bool
            )
                throw new ArgumentNullException("s")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (sourceValue is string stringSourceValue)
                if (long.TryParse(stringSourceValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var longParsedSourceValue))
                    setValueAction(isNullableType ? (long?)longParsedSourceValue : longParsedSourceValue);
                else
                    throw new FormatException($"The input string '{stringSourceValue}' was not in a correct format.")
                    {
                        Source = "Azure.Data.Tables"
                    };
            else if (sourceValue is DateTimeOffset dateTimeOffsetSourceValue)
                throw new FormatException($"The input string '{dateTimeOffsetSourceValue:yyyy-MM-ddTHH:mm:ss.FFFFFFFZ}' was not in a correct format.")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (sourceValue is Guid guidSourceValue)
                throw new FormatException($"The input string '{guidSourceValue:D}' was not in a correct format.")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (sourceValue is byte[] binarySourceValue)
                throw new FormatException($"The input string '{Convert.ToBase64String(binarySourceValue)}' was not in a correct format.")
                {
                    Source = "Azure.Data.Tables"
                };
            else
                throw new InvalidOperationException($"Unhanled {resolvedTargetType} target type for int64 deserialization.");
        }

        private static void _SetDoubleValue(Type resolvedTargetType, bool isNullableType, object sourceValue, Action<object> setValueAction)
        {
            if (sourceValue is int intSourceValue)
                setValueAction(isNullableType ? (double?)intSourceValue : intSourceValue);
            else if (sourceValue is long longSourceValue)
                setValueAction(isNullableType ? (double?)longSourceValue : longSourceValue);
            else if (sourceValue is double doubleSourceValue)
                setValueAction(isNullableType ? (double?)doubleSourceValue : doubleSourceValue);
            else if (sourceValue is bool booleanSourceValue)
            {
                var boolValue = booleanSourceValue ? 1D : 0D;
                setValueAction(isNullableType ? (double?)boolValue : (double)boolValue);
            }
            else if (sourceValue is string stringSourceValue)
                if (double.TryParse(stringSourceValue, NumberStyles.Float, CultureInfo.InvariantCulture, out var doubleParsedSourceValue))
                    setValueAction(isNullableType ? (double?)doubleParsedSourceValue : doubleParsedSourceValue);
                else
                    throw new FormatException($"The input string '{stringSourceValue}' was not in a correct format.")
                    {
                        Source = "Azure.Data.Tables"
                    };
            else if (sourceValue is DateTimeOffset dateTimeOffsetSourceValue)
                throw new FormatException($"The input string '{dateTimeOffsetSourceValue:yyyy-MM-ddTHH:mm:ss.FFFFFFFZ}' was not in a correct format.")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (sourceValue is Guid guidSourceValue)
                throw new FormatException($"The input string '{guidSourceValue:D}' was not in a correct format.")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (sourceValue is byte[] binarySourceValue)
                throw new FormatException($"The input string '{Convert.ToBase64String(binarySourceValue)}' was not in a correct format.")
                {
                    Source = "Azure.Data.Tables"
                };
            else
                throw new InvalidOperationException($"Unhanled {resolvedTargetType} target type for double deserialization.");
        }

        private static void _SetBooleanValue(Type resolvedTargetType, bool isNullableType, object sourceValue, Action<object> setValueAction)
        {
            if (sourceValue is bool boolValue)
                setValueAction(isNullableType ? (bool?)boolValue : boolValue);
            else if (sourceValue is int)
                throw new InvalidCastException(
                    isNullableType
                    ? "Unable to cast object of type 'System.Int32' to type 'System.Nullable`1[System.Boolean]'."
                    : "Unable to cast object of type 'System.Int32' to type 'System.Boolean'.")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (sourceValue is double)
                throw new InvalidCastException(
                    isNullableType
                    ? "Unable to cast object of type 'System.Double' to type 'System.Nullable`1[System.Boolean]'."
                    : "Unable to cast object of type 'System.Double' to type 'System.Boolean'.")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (
                sourceValue is long
                || sourceValue is string
                || sourceValue is DateTimeOffset
                || sourceValue is Guid
                || sourceValue is byte[]
            )
                throw new InvalidCastException(
                    isNullableType
                    ? "Unable to cast object of type 'System.String' to type 'System.Nullable`1[System.Boolean]'."
                    : "Unable to cast object of type 'System.String' to type 'System.Boolean'.")
                {
                    Source = "Azure.Data.Tables"
                };
            else
                throw new InvalidOperationException($"Unhanled {resolvedTargetType} target type for boolean deserialization.");
        }

        private static void _SetDateTimeValue(Type resolvedTargetType, bool isNullableType, object sourceValue, Action<object> setValueAction)
        {
            if (
                sourceValue is int
                || sourceValue is double
                || sourceValue is bool
            )
                throw new ArgumentNullException("s")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (sourceValue is long)
                throw new FormatException("String '3' was not recognized as a valid DateTime.")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (sourceValue is DateTimeOffset dateTimeOffsetSourceValue)
                setValueAction(isNullableType ? (DateTime?)dateTimeOffsetSourceValue.DateTime : dateTimeOffsetSourceValue.DateTime);
            else
            {
                string dateTimeStringValue;
                if (sourceValue is string stringSourceValue)
                    dateTimeStringValue = stringSourceValue;
                else if (sourceValue is Guid guidSourceValue)
                    dateTimeStringValue = guidSourceValue.ToString("D");
                else if (sourceValue is byte[] binarySourceValue)
                    dateTimeStringValue = Convert.ToBase64String(binarySourceValue);
                else
                    throw new InvalidOperationException($"Unhanled {resolvedTargetType} target type for date time deserialization.");

                for (var index = 0; index < dateTimeStringValue.Length; index++)
                    if (
                        !char.IsDigit(dateTimeStringValue[index])
                        && dateTimeStringValue[index] != '-'
                        && dateTimeStringValue[index] != ':'
                        && dateTimeStringValue[index] != 'T'
                        && dateTimeStringValue[index] != 'Z'
                    )
                        throw new FormatException($"The string '{dateTimeStringValue}' was not recognized as a valid DateTime. There is an unknown word starting at index '{index}'.")
                        {
                            Source = "Azure.Data.Tables"
                        };

                if (DateTime.TryParseExact(dateTimeStringValue, "yyyy-MM-ddTHH:mm:ss.FFFFFFFZ", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeParsedSourceValue))
                    setValueAction(isNullableType ? (DateTime?)dateTimeParsedSourceValue : dateTimeParsedSourceValue);
                else if (DateTime.TryParseExact(dateTimeStringValue, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTimeParsedSourceValue))
                    setValueAction(isNullableType ? (DateTime?)dateTimeParsedSourceValue : dateTimeParsedSourceValue);
                else
                    throw new FormatException($"String '{dateTimeStringValue}' was not recognized as a valid DateTime.")
                    {
                        Source = "Azure.Data.Tables"
                    };
            }
        }

        private static void _SetDateTimeOffsetValue(Type resolvedTargetType, bool isNullableType, object sourceValue, Action<object> setValueAction)
        {
            if (
                sourceValue is int
                || sourceValue is double
                || sourceValue is bool
            )
                throw new ArgumentNullException("input")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (sourceValue is long)
                throw new FormatException("String '3' was not recognized as a valid DateTime.")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (sourceValue is DateTimeOffset dateTimeOffsetSourceValue)
                setValueAction(isNullableType ? (DateTimeOffset?)dateTimeOffsetSourceValue : dateTimeOffsetSourceValue);
            else
            {
                string dateTimeStringValue;
                if (sourceValue is string stringSourceValue)
                    dateTimeStringValue = stringSourceValue;
                else if (sourceValue is Guid guidSourceValue)
                    dateTimeStringValue = guidSourceValue.ToString("D");
                else if (sourceValue is byte[] binarySourceValue)
                    dateTimeStringValue = Convert.ToBase64String(binarySourceValue);
                else
                    throw new InvalidOperationException($"Unhanled {resolvedTargetType} target type for date time deserialization.");

                for (var index = 0; index < dateTimeStringValue.Length; index++)
                    if (
                        !char.IsDigit(dateTimeStringValue[index])
                        && dateTimeStringValue[index] != '-'
                        && dateTimeStringValue[index] != ':'
                        && dateTimeStringValue[index] != 'T'
                        && dateTimeStringValue[index] != 'Z'
                    )
                        throw new FormatException($"The string '{dateTimeStringValue}' was not recognized as a valid DateTime. There is an unknown word starting at index '{index}'.")
                        {
                            Source = "Azure.Data.Tables"
                        };

                if (DateTimeOffset.TryParseExact(dateTimeStringValue, "yyyy-MM-ddTHH:mm:ss.FFFFFFFZ", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeOffsetParsedSourceValue))
                    setValueAction(isNullableType ? (DateTimeOffset?)dateTimeOffsetParsedSourceValue : dateTimeOffsetParsedSourceValue);
                else if (DateTimeOffset.TryParseExact(dateTimeStringValue, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTimeOffsetParsedSourceValue))
                    setValueAction(isNullableType ? (DateTimeOffset?)dateTimeOffsetParsedSourceValue : dateTimeOffsetParsedSourceValue);
                else
                    throw new FormatException($"String '{dateTimeStringValue}' was not recognized as a valid DateTime.")
                    {
                        Source = "Azure.Data.Tables"
                    };
            }
        }

        private static void _SetGuidValue(Type resolvedTargetType, bool isNullableType, object sourceValue, Action<object> setValueAction)
        {
            if (
                sourceValue is int
                || sourceValue is double
                || sourceValue is bool
            )
                throw new ArgumentNullException("input")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (sourceValue is Guid guidValue)
                setValueAction(isNullableType ? (Guid?)guidValue : guidValue);
            else if (sourceValue is string stringSourceValue)
                if (Guid.TryParse(stringSourceValue, out var guidParsedSourceValue))
                    setValueAction(isNullableType ? (Guid?)guidParsedSourceValue : guidParsedSourceValue);
                else
                    throw new FormatException("Unrecognized Guid format.")
                    {
                        Source = "Azure.Data.Tables"
                    };
            else if (
                sourceValue is long
                || sourceValue is DateTimeOffset
                || sourceValue is byte[]
            )
                throw new FormatException("Unrecognized Guid format.")
                {
                    Source = "Azure.Data.Tables"
                };
            else
                throw new InvalidOperationException($"Unhanled {resolvedTargetType} target type for date time deserialization.");
        }

        private static void _SetBinaryValue(Type resolvedTargetType, object sourceValue, Action<object> setValueAction)
        {
            if (sourceValue is byte[] binarySourceValue)
            {
                var binaryResultValue = new byte[binarySourceValue.Length];
                Array.Copy(binarySourceValue, binaryResultValue, binaryResultValue.Length);
                setValueAction(binaryResultValue);
            }
            else if (
                sourceValue is int
                || sourceValue is double
                || sourceValue is bool
            )
                throw new ArgumentNullException("s")
                {
                    Source = "Azure.Data.Tables"
                };
            else if (
                sourceValue is Guid
                || sourceValue is DateTimeOffset
            )
                throw new FormatException("The input is not a valid Base-64 string as it contains a non-base 64 character, more than two padding characters, or an illegal character among the padding characters.")
                {
                    Source = "Azure.Data.Tables"
                };
            else
            {
                string binaryStringValue;
                if (sourceValue is string stringSourceValue)
                    binaryStringValue = stringSourceValue;
                else if (sourceValue is long longSourceValue)
                    binaryStringValue = longSourceValue.ToString(CultureInfo.InvariantCulture);
                else
                    throw new InvalidOperationException($"Unhanled {resolvedTargetType} target type for date time deserialization.");

                byte[] binaryParsedValue;
                try
                {
                    binaryParsedValue = Convert.FromBase64String(binaryStringValue);
                }
                catch
                {
                    throw new FormatException("The input is not a valid Base-64 string as it contains a non-base 64 character, more than two padding characters, or an illegal character among the padding characters.")
                    {
                        Source = "Azure.Data.Tables"
                    };
                }
                setValueAction(binaryParsedValue);
            }
        }

        private static void _SetStringValue(Type resolvedTargetType, object sourceValue, Action<object> setValueAction)
        {
            if (sourceValue is string stringSourceValue)
                setValueAction(stringSourceValue);
            else if (
                sourceValue is int
                || sourceValue is double
                || sourceValue is bool
            )
                setValueAction(null);
            else if (sourceValue is long longSourceValue)
                setValueAction(longSourceValue.ToString(CultureInfo.InvariantCulture));
            else if (sourceValue is Guid guidSourceValue)
                setValueAction(guidSourceValue.ToString("D"));
            else if (sourceValue is DateTimeOffset dateTimeOffsetSourceValue)
                setValueAction(dateTimeOffsetSourceValue.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFFZ", CultureInfo.InvariantCulture));
            else if (sourceValue is byte[] binarySourceValue)
                setValueAction(Convert.ToBase64String(binarySourceValue));
            else
                throw new InvalidOperationException($"Unhanled {resolvedTargetType} target type for string deserialization.");
        }
    }

    internal class ValidatedTableRowStub : TableRowStub
    {
        private const int MaximumKeyLength = 1 << 10 + 1;
        private const int MaximumStringLength = 1 << 15 + 1;
        private const int MaximumBinaryLength = 1 << 16 + 1;
        private static DateTimeOffset MinimumDateTimeOffset = new DateTimeOffset(1601, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public ValidatedTableRowStub(ITableEntity entity)
        {
            var entityType = entity.GetType();
            if (entityType == typeof(TableEntity))
            {
                var tableEntity = (TableEntity)(entity as object);
                foreach (var row in tableEntity)
                    _TrySetValue(row.Key, row.Value);
            }
            else
            {
                foreach (var field in entityType.GetFields())
                    _TrySetValue(field.Name, field.GetValue(entity));

                foreach (var property in entityType.GetProperties())
                    if (property.GetIndexParameters().Length == 0)
                        _TrySetValue(property.Name, property.GetValue(entity));
            }
        }

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
            if (value == null || propertyName == "ETag" || propertyName == "odata.etag" || propertyName == "Timestamp")
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
                this[propertyName] = (
                    int.MinValue <= floatValue && floatValue <= int.MaxValue && floatValue == Math.Truncate(floatValue)
                        ? (int)floatValue
                        : double.Parse(floatValue.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture)
                    as object
                );

            else if (value is decimal decimalValue)
                this[propertyName] = (
                    int.MinValue <= decimalValue && decimalValue <= int.MaxValue && decimalValue == decimal.Truncate(decimalValue)
                        ? (int)decimalValue
                        : double.Parse(decimalValue.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture)
                    as object
                );

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
        }

        private static bool _KeyContainsInvalidCharacters(string keyValue)
            => keyValue.Any(IsReservedKeyCharacter);
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