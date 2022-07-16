using System.Collections.Generic;
using CloudStub.FilterParser.FilterNodes;

namespace CloudStub.FilterParser.FilterNodeFactories
{
    internal class AndFilterNodeFactory : LogicalFilterNodeFactory
    {
        public AndFilterNodeFactory()
            : base(FilterTokenCode.And)
        {
        }

        protected override FilterNode GetNode(IEnumerable<FilterNode> operands)
            => new AndFilterNode(operands);
    }
}