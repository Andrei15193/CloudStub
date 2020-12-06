using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;

namespace CloudStub.FilterParser.FilterNodes
{
    internal class AndFilterNode : FilterNode
    {
        public AndFilterNode(IEnumerable<FilterNode> operands)
            => Operands = operands;

        public IEnumerable<FilterNode> Operands { get; }

        public override bool Apply(DynamicTableEntity entity)
        {
            var result = Operands.All(operand => operand.Apply(entity));
            return result;
        }
    }
}