using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.FilterParser.FilterNodes
{
    internal abstract class PropertyFilterNode : FilterNode
    {
        private const int MismatchingTypeComparisonResult = -1;

        public PropertyFilterNode(string propertyName, EntityProperty filterValue)
        {
            PropertyName = propertyName;
            FilterValue = filterValue;
        }

        public string PropertyName { get; }

        public EntityProperty FilterValue { get; }

        protected EntityProperty GetValueFromEntity(DynamicTableEntity entity)
        {
            EntityProperty entityProperty;

            if (string.Equals(PropertyName, nameof(entity.PartitionKey), StringComparison.Ordinal))
                entityProperty = EntityProperty.GeneratePropertyForString(entity.PartitionKey);
            else if (string.Equals(PropertyName, nameof(entity.RowKey), StringComparison.Ordinal))
                entityProperty = EntityProperty.GeneratePropertyForString(entity.PartitionKey);
            else if (string.Equals(PropertyName, nameof(entity.ETag), StringComparison.Ordinal))
                entityProperty = EntityProperty.GeneratePropertyForString(entity.ETag);
            else if (string.Equals(PropertyName, nameof(entity.Timestamp), StringComparison.Ordinal))
                entityProperty = EntityProperty.GeneratePropertyForDateTimeOffset(entity.Timestamp);
            else if (!entity.Properties.TryGetValue(PropertyName, out entityProperty))
                entityProperty = null;

            return entityProperty;
        }

        protected static int? Compare(EntityProperty propertyValue, EntityProperty filterValue)
        {
            if (propertyValue == null || filterValue == null)
                return null;
            else if (propertyValue.PropertyAsObject == null && filterValue.PropertyAsObject == null)
                return 0;
            else if (propertyValue.PropertyAsObject == null || filterValue.PropertyAsObject == null)
                return null;
            else
                switch (propertyValue.PropertyType)
                {
                    case EdmType.Int32:
                        return _CompareInt32(propertyValue.Int32Value.Value, filterValue);

                    case EdmType.Int64:
                        return _CompareInt64(propertyValue.Int64Value.Value, filterValue);

                    case EdmType.Double:
                        return _CompareDouble(propertyValue.DoubleValue.Value, filterValue);

                    case EdmType.Boolean:
                        return _CompareBoolean(propertyValue.BooleanValue.Value, filterValue);

                    case EdmType.DateTime:
                        return _CompareDateTimeOffset(propertyValue.DateTimeOffsetValue.Value, filterValue);

                    case EdmType.Guid:
                        return _CompareGuid(propertyValue.GuidValue.Value, filterValue);

                    case EdmType.Binary:
                        return _CompareBinary(propertyValue.BinaryValue, filterValue);

                    case EdmType.String:
                        return _CompareString(propertyValue.StringValue, filterValue);

                    default:
                        return null;
                }
        }

        private static int _CompareInt32(int propertyValue, EntityProperty filterValue)
            => filterValue.PropertyType == EdmType.Int32 ? propertyValue.CompareTo(filterValue.Int32Value.Value) : MismatchingTypeComparisonResult;

        private static int _CompareInt64(long propertyValue, EntityProperty filterValue)
        {
            switch (filterValue.PropertyType)
            {
                case EdmType.Int32:
                    return propertyValue.CompareTo(filterValue.Int32Value.Value);

                case EdmType.Int64:
                    return propertyValue.CompareTo(filterValue.Int64Value.Value);

                default:
                    return MismatchingTypeComparisonResult;
            }
        }

        private static int _CompareDouble(double propertyValue, EntityProperty filterValue)
        {
            switch (filterValue.PropertyType)
            {
                case EdmType.Int32:
                    return propertyValue.CompareTo(filterValue.Int32Value.Value);

                case EdmType.Int64:
                    return propertyValue.CompareTo(filterValue.Int64Value.Value);

                case EdmType.Double:
                    return propertyValue.CompareTo(filterValue.DoubleValue.Value);

                default:
                    return MismatchingTypeComparisonResult;
            }
        }

        private static int _CompareBoolean(bool propertyValue, EntityProperty filterValue)
            => filterValue.PropertyType == EdmType.Boolean ? propertyValue.CompareTo(filterValue.BooleanValue.Value) : MismatchingTypeComparisonResult;

        private static int _CompareDateTimeOffset(DateTimeOffset propertyValue, EntityProperty filterValue)
            => filterValue.PropertyType == EdmType.DateTime ? propertyValue.CompareTo(filterValue.DateTimeOffsetValue.Value) : MismatchingTypeComparisonResult;

        private static int _CompareGuid(Guid propertyValue, EntityProperty filterValue)
            => filterValue.PropertyType == EdmType.Guid ? propertyValue.CompareTo(filterValue.GuidValue.Value) : MismatchingTypeComparisonResult;

        private static int _CompareBinary(byte[] propertyValue, EntityProperty filterValue)
        {
            if (filterValue.PropertyType == EdmType.Binary)
            {
                var index = 0;
                var comparisonResult = 0;
                while (comparisonResult == 0 && index < propertyValue.Length && index < filterValue.BinaryValue.Length)
                {
                    comparisonResult = propertyValue[index].CompareTo(filterValue.BinaryValue[index]);
                    index++;
                }

                if (index < propertyValue.Length)
                    return 1;
                else if (index < filterValue.BinaryValue.Length)
                    return -1;
                else
                    return comparisonResult;
            }
            else
                return MismatchingTypeComparisonResult;
        }

        private static int _CompareString(string propertyValue, EntityProperty filterValue)
            => filterValue.PropertyType == EdmType.String ? StringComparer.Ordinal.Compare(propertyValue, filterValue.StringValue) : MismatchingTypeComparisonResult;
    }
}