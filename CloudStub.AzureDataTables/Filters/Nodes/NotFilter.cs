using System.Collections.Generic;

namespace CloudStub.AzureDataTables.Filters.Nodes
{
    internal class NotFilter : Filter
    {
        private readonly Filter _fitler;

        public NotFilter(Filter filter)
            => _fitler = filter;

        public override IEnumerable<string> FilteredProperties
            => _fitler.FilteredProperties;

        public override bool Apply(IReadOnlyDictionary<string, object> entity)
            => !_fitler.Apply(entity);
    }
}