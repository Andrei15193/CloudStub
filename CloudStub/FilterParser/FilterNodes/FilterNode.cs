using CloudStub.Core;

namespace CloudStub.FilterParser.FilterNodes
{
    internal abstract class FilterNode
    {
        public abstract bool Apply(StubEntity entity);
    }
}