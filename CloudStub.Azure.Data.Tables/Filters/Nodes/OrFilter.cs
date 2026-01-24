using System.Collections.Generic;
using System.Linq;

namespace CloudStub.Azure.Data.Tables.Filters.Nodes
{
    internal class OrFilter : Filter
    {
        private readonly Filter _left;
        private readonly Filter _right;

        public OrFilter(Filter left, Filter right)
        {
            _left = left;
            _right = right;
        }

        public override int DiscreteFiltersCount
            => 1 + _left.DiscreteFiltersCount + _right.DiscreteFiltersCount;

        public override IEnumerable<string> FilteredProperties
            => _left.FilteredProperties.Concat(_right.FilteredProperties);

        public override bool Apply(IReadOnlyDictionary<string, object> entity)
            => _left.Apply(entity) || _right.Apply(entity);
    }
}