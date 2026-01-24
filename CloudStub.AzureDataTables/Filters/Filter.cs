using System.Collections.Generic;

namespace CloudStub.AzureDataTables.Filters
{
    public abstract class Filter
    {
        public abstract int DiscreteFiltersCount { get; }

        public abstract IEnumerable<string> FilteredProperties { get; }

        public abstract bool Apply(IReadOnlyDictionary<string, object> entity);
    }
}