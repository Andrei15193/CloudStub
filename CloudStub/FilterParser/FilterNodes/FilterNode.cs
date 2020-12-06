using Microsoft.Azure.Cosmos.Table;

namespace CloudStub.FilterParser.FilterNodes
{
    internal abstract class FilterNode
    {
        public abstract bool Apply(DynamicTableEntity entity);
    }
}