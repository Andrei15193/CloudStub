using System.Collections.Generic;
using System.Linq;
using CloudStub.Core;

namespace CloudStub.FilterParser.FilterNodes
{
    internal class OrFilterNode : FilterNode
    {
        public OrFilterNode(IEnumerable<FilterNode> operands)
            => Operands = operands;

        public IEnumerable<FilterNode> Operands { get; }

        public override bool Apply(StubEntity entity)
            => Operands.Any(operand => operand.Apply(entity));
    }
}