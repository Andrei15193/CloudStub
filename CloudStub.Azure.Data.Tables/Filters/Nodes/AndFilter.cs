using System.Collections.Generic;

namespace CloudStub.Azure.Data.Tables.Filters.Nodes
{
    internal class AndFilter : Filter
    {
        private readonly Filter _left;
        private readonly Filter _right;

        public AndFilter(Filter left, Filter right)
        {
            _left = left;
            _right = right;
        }

        public override int DiscreteFiltersCount
            => 1 + _left.DiscreteFiltersCount + _right.DiscreteFiltersCount;

        public override bool Apply(IReadOnlyDictionary<string, object> entity)
            => _left.Apply(entity) && _right.Apply(entity);
    }
}