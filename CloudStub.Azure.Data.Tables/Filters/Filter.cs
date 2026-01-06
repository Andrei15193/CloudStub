using System.Collections.Generic;

namespace CloudStub.Azure.Data.Tables.Filters
{
    public abstract class Filter
    {
        public abstract int DiscreteFiltersCount { get; }

        public abstract bool Apply(IReadOnlyDictionary<string, object> entity);
    }
}