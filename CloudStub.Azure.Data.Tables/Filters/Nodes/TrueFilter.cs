using System.Collections.Generic;

namespace CloudStub.Azure.Data.Tables.Filters.Nodes
{
    internal class TrueFilter : Filter
    {
        public override int DiscreteFiltersCount
            => 0;

        public override bool Apply(IReadOnlyDictionary<string, object> entity)
            => true;
    }
}