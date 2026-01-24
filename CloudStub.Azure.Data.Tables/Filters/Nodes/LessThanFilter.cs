using System.Collections.Generic;

namespace CloudStub.Azure.Data.Tables.Filters.Nodes
{
    internal class LessThanFilter : ComparisonFilter
    {
        public LessThanFilter(string propertyName, object value)
            : base(propertyName, value)
        {
        }

        public override bool Apply(IReadOnlyDictionary<string, object> entity)
            => entity.TryGetValue(PropertyName, out var entityPropertyValue) && Compare(entityPropertyValue, Value) < 0;
    }
}