using System.Collections.Generic;
using System.Linq;
using CloudStub.Core;

namespace CloudStub.FilterParser.FilterNodes
{
    internal class AndFilterNode : FilterNode
    {
        public AndFilterNode(IEnumerable<FilterNode> operands)
            => Operands = operands;

        public IEnumerable<FilterNode> Operands { get; }

        public override bool Apply(StubEntity entity)
        {
            var result = Operands.All(operand => operand.Apply(entity));
            return result;
        }
    }
}