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
    }
}