using System.Collections.Generic;

namespace CloudStub.AzureDataTables.Filters.Nodes
{
    internal class LessThanOrEqualFilter : ComparisonFilter
    {
        public LessThanOrEqualFilter(string propertyName, object value)
            : base(propertyName, value)
        {
        }

        public override bool Apply(IReadOnlyDictionary<string, object> entity)
        {
            if (entity.TryGetValue(PropertyName, out var entityPropertyValue))
            {
                var compareResult = Compare(entityPropertyValue, Value);
                return compareResult.HasValue && compareResult.Value <= 0;
            }
            else
                return false;
        }
    }
}