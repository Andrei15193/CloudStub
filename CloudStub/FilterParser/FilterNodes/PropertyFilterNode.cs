using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.FilterParser.FilterNodes
{
    internal abstract class PropertyFilterNode : FilterNode
    {
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

            if (string.Equals(PropertyName, nameof(entity.PartitionKey), StringComparison.OrdinalIgnoreCase))
                entityProperty = EntityProperty.GeneratePropertyForString(entity.PartitionKey);
            else if (string.Equals(PropertyName, nameof(entity.RowKey), StringComparison.OrdinalIgnoreCase))
                entityProperty = EntityProperty.GeneratePropertyForString(entity.PartitionKey);
            else if (string.Equals(PropertyName, nameof(entity.ETag), StringComparison.OrdinalIgnoreCase))
                entityProperty = EntityProperty.GeneratePropertyForString(entity.ETag);
            else if (string.Equals(PropertyName, nameof(entity.Timestamp), StringComparison.OrdinalIgnoreCase))
                entityProperty = EntityProperty.GeneratePropertyForDateTimeOffset(entity.Timestamp);
            else if (!entity.Properties.TryGetValue(PropertyName, out entityProperty))
                entityProperty = null;

            return entityProperty;
        }

        protected static int Compare(EntityProperty propertyValue, EntityProperty filterValue)
        {
            if (propertyValue.PropertyType == EdmType.Binary && filterValue.PropertyType == EdmType.Binary)
            {
                var index = 0;
                var comparisonResult = 0;
                while (comparisonResult == 0 && index < propertyValue.BinaryValue.Length && index < filterValue.BinaryValue.Length)
                {
                    comparisonResult = propertyValue.BinaryValue[index].CompareTo(filterValue.BinaryValue[index]);
                    index++;
                }

                if (index < propertyValue.BinaryValue.Length)
                    return 1;
                else if (index < filterValue.BinaryValue.Length)
                    return -1;
                else
                    return comparisonResult;
            }
            else if (propertyValue.PropertyType == EdmType.String && filterValue.PropertyType == EdmType.String)
                return StringComparer.Ordinal.Compare(propertyValue.StringValue, filterValue.StringValue);
            else
                return (propertyValue.PropertyAsObject as IComparable).CompareTo(filterValue.PropertyAsObject);
        }
    }
}