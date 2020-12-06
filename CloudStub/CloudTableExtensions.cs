using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Azure.Cosmos.Table;

namespace CloudStub
{
    internal static class CloudTableExtensions
    {
        private static readonly PropertyInfo _partitionKeyProperty = typeof(TableOperation)
            .GetRuntimeProperties()
            .Single(property => property.Name == "PartitionKey");
        private static readonly PropertyInfo _rowKeyProperty = typeof(TableOperation)
            .GetRuntimeProperties()
            .Single(property => property.Name == "RowKey");
        private static readonly PropertyInfo _selectColumnsProperty = typeof(TableOperation)
            .GetRuntimeProperties()
            .Single(property => property.Name == "SelectColumns");
        private static readonly PropertyInfo _retrieveResolverProperty = typeof(TableOperation)
            .GetRuntimeProperties()
            .Single(property => property.Name == "RetrieveResolver");

        public static DynamicTableEntity Clone(this DynamicTableEntity entity)
            => new DynamicTableEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp,
                Properties = _CloneProperties(entity.Properties)
            };

        public static DynamicTableEntity Clone(this DynamicTableEntity entity, IEnumerable<string> selectColumns)
            => new DynamicTableEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp,
                Properties = _CloneProperties(entity.Properties, selectColumns)
            };

        public static string GetPartitionKey(this TableOperation tableOperation)
            => (string)_partitionKeyProperty.GetValue(tableOperation);

        public static string GetRowKey(this TableOperation tableOperation)
            => (string)_rowKeyProperty.GetValue(tableOperation);

        public static IEnumerable<string> GetSelectColumns(this TableOperation tableOperation)
            => (IEnumerable<string>)_selectColumnsProperty.GetValue(tableOperation);

        public static EntityResolver<T> GetEntityResolver<T>(this TableOperation tableOperation)
        {
            var defaultEntityResolver = (Func<string, string, DateTimeOffset, IDictionary<string, EntityProperty>, string, object>)_retrieveResolverProperty.GetValue(tableOperation);
            return (partitionKey, rowKey, timestamp, properties, etag) => (T)defaultEntityResolver(partitionKey, rowKey, timestamp, properties, etag);
        }

        private static IDictionary<string, EntityProperty> _CloneProperties(IDictionary<string, EntityProperty> properties, IEnumerable<string> selectColumns)
            => selectColumns.ToDictionary(
                column => column,
                column => properties.TryGetValue(column, out var property)
                    ? EntityProperty.CreateEntityPropertyFromObject(property.PropertyAsObject)
                    : EntityProperty.GeneratePropertyForString(null),
                StringComparer.Ordinal
            );

        private static IDictionary<string, EntityProperty> _CloneProperties(IDictionary<string, EntityProperty> properties)
            => properties.ToDictionary(
                pair => pair.Key,
                pair => EntityProperty.CreateEntityPropertyFromObject(pair.Value.PropertyAsObject),
                StringComparer.Ordinal
            );
    }
}