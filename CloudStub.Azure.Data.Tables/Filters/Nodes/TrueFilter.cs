using System;
using System.Collections.Generic;

namespace CloudStub.Azure.Data.Tables.Filters.Nodes
{
    internal class TrueFilter : Filter
    {
        public override int DiscreteFiltersCount
            => 0;

        public override IEnumerable<string> FilteredProperties
            => Array.Empty<string>();

        public override bool Apply(IReadOnlyDictionary<string, object> entity)
            => true;
    }
}