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

        protected static int Compare(EntityProperty filterValue, EntityProperty propertyValue)
            => (filterValue.PropertyAsObject as IComparable).CompareTo(propertyValue.PropertyAsObject);
    }
}