using System.Collections.Generic;
using System.Linq;

namespace CloudStub.AzureDataTables.Filters.Nodes
{
    internal class OrFilter : Filter
    {
        private readonly Filter _left;
        private readonly Filter _right;

        public OrFilter(Filter left, Filter right)
            => (_left, _right) = (left, right);

        public override IEnumerable<string> FilteredProperties
            => _left.FilteredProperties.Concat(_right.FilteredProperties);

        public override bool Apply(IReadOnlyDictionary<string, object> entity)
            => _left.Apply(entity) || _right.Apply(entity);
    }
}