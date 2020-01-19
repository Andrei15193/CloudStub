using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.FilterParser.FilterNodes
{
    internal abstract class FilterNode
    {
        public abstract bool Apply(DynamicTableEntity entity);
    }
}