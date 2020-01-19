using System.Collections.Generic;
using CloudStub.FilterParser.FilterNodes;

namespace CloudStub.FilterParser.FilterNodeFactories
{
    internal class OrFilterNodeFactory : LogicalFilterNodeFactory
    {
        public OrFilterNodeFactory()
            : base(FilterTokenCode.Or)
        {
        }

        protected override FilterNode GetNode(IEnumerable<FilterNode> operands)
            => new OrFilterNode(operands);
    }
}