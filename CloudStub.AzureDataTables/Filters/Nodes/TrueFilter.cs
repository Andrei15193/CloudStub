using System;
using System.Collections.Generic;

namespace CloudStub.AzureDataTables.Filters.Nodes
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