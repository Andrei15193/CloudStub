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
        {
            if (entity.TryGetValue(PropertyName, out var entityPropertyValue))
                return Compare(entityPropertyValue, Value) < 0;
            else
                return false;
        }
    }
}