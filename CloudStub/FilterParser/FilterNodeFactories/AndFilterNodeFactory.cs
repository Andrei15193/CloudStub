using CloudStub.FilterParser.FilterNodes;
using System.Collections.Generic;

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