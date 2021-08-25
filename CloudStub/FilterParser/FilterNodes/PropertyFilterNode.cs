using System;
using CloudStub.Core;

namespace CloudStub.FilterParser.FilterNodes
{
    internal abstract class PropertyFilterNode : FilterNode
    {
        private const int MismatchingTypeComparisonResult = -1;

        public PropertyFilterNode(string propertyName, StubEntityProperty filterValue)
        {
            PropertyName = propertyName;
            FilterValue = filterValue;
        }

        public string PropertyName { get; }

        public StubEntityProperty FilterValue { get; }

        protected StubEntityProperty GetValueFromEntity(StubEntity entity)
        {
            StubEntityProperty entityProperty;

            if (string.Equals(PropertyName, nameof(entity.PartitionKey), StringComparison.Ordinal))
                entityProperty = new StubEntityProperty(entity.PartitionKey);
            else if (string.Equals(PropertyName, nameof(entity.RowKey), StringComparison.Ordinal))
                entityProperty = new StubEntityProperty(entity.RowKey);
            else if (string.Equals(PropertyName, nameof(entity.ETag), StringComparison.Ordinal))
                entityProperty = new StubEntityProperty(entity.ETag);
            else if (string.Equals(PropertyName, nameof(entity.Timestamp), StringComparison.Ordinal))
                entityProperty = new StubEntityProperty(entity.Timestamp.Value);
            else if (!entity.Properties.TryGetValue(PropertyName, out entityProperty))
                entityProperty = null;

            return entityProperty;
        }

        protected static int? Compare(StubEntityProperty propertyValue, StubEntityProperty filterValue)
        {
            if (propertyValue is null || filterValue is null)
                return null;
            else if (propertyValue.Value is null && filterValue.Value is null)
                return 0;
            else if (propertyValue.Value is null || filterValue.Value is null)
                return null;
            else
                switch (propertyValue.Type)
                {
                    case StubEntityPropertyType.Int32:
                        return _CompareInt32((int)propertyValue.Value, filterValue);

                    case StubEntityPropertyType.Int64:
                        return _CompareInt64((long)propertyValue.Value, filterValue);

                    case StubEntityPropertyType.Double:
                        return _CompareDouble((double)propertyValue.Value, filterValue);

                    case StubEntityPropertyType.Boolean:
                        return _CompareBoolean((bool)propertyValue.Value, filterValue);

                    case StubEntityPropertyType.DateTime:
                        return _CompareDateTimeOffset((DateTimeOffset)propertyValue.Value, filterValue);

                    case StubEntityPropertyType.Guid:
                        return _CompareGuid((Guid)propertyValue.Value, filterValue);

                    case StubEntityPropertyType.Binary:
                        return _CompareBinary((byte[])propertyValue.Value, filterValue);

                    case StubEntityPropertyType.String:
                        return _CompareString((string)propertyValue.Value, filterValue);

                    default:
                        return null;
                }
        }

        private static int _CompareInt32(int propertyValue, StubEntityProperty filterValue)
            => filterValue.Type == StubEntityPropertyType.Int32 ? propertyValue.CompareTo((int)filterValue.Value) : MismatchingTypeComparisonResult;

        private static int _CompareInt64(long propertyValue, StubEntityProperty filterValue)
        {
            switch (filterValue.Type)
            {
                case StubEntityPropertyType.Int32:
                    return propertyValue.CompareTo((int)filterValue.Value);

                case StubEntityPropertyType.Int64:
                    return propertyValue.CompareTo((long)filterValue.Value);

                default:
                    return MismatchingTypeComparisonResult;
            }
        }

        private static int _CompareDouble(double propertyValue, StubEntityProperty filterValue)
        {
            switch (filterValue.Type)
            {
                case StubEntityPropertyType.Int32:
                    return propertyValue.CompareTo((int)filterValue.Value);

                case StubEntityPropertyType.Int64:
                    return propertyValue.CompareTo((long)filterValue.Value);

                case StubEntityPropertyType.Double:
                    return propertyValue.CompareTo((double)filterValue.Value);

                default:
                    return MismatchingTypeComparisonResult;
            }
        }

        private static int _CompareBoolean(bool propertyValue, StubEntityProperty filterValue)
            => filterValue.Type == StubEntityPropertyType.Boolean ? propertyValue.CompareTo((bool)filterValue.Value) : MismatchingTypeComparisonResult;

        private static int _CompareDateTimeOffset(DateTimeOffset propertyValue, StubEntityProperty filterValue)
            => filterValue.Type == StubEntityPropertyType.DateTime ? propertyValue.UtcDateTime.CompareTo(((DateTimeOffset)filterValue.Value).UtcDateTime) : MismatchingTypeComparisonResult;

        private static int _CompareGuid(Guid propertyValue, StubEntityProperty filterValue)
            => filterValue.Type == StubEntityPropertyType.Guid ? propertyValue.CompareTo((Guid)filterValue.Value) : MismatchingTypeComparisonResult;

        private static int _CompareBinary(byte[] propertyValue, StubEntityProperty filterValue)
        {
            if (filterValue.Type == StubEntityPropertyType.Binary)
            {
                var compareValue = (byte[])filterValue.Value;

                var index = 0;
                var comparisonResult = 0;
                while (comparisonResult == 0 && index < propertyValue.Length && index < compareValue.Length)
                {
                    comparisonResult = propertyValue[index].CompareTo(compareValue[index]);
                    index++;
                }

                if (index < propertyValue.Length)
                    return 1;
                else if (index < compareValue.Length)
                    return -1;
                else
                    return comparisonResult;
            }
            else
                return MismatchingTypeComparisonResult;
        }

        private static int _CompareString(string propertyValue, StubEntityProperty filterValue)
            => filterValue.Type == StubEntityPropertyType.String ? StringComparer.Ordinal.Compare(propertyValue, (string)filterValue.Value) : MismatchingTypeComparisonResult;
    }
}