using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;

namespace CloudStub.FilterParser.FilterNodes
{
    internal class OrFilterNode : FilterNode
    {
        public OrFilterNode(IEnumerable<FilterNode> operands)
            => Operands = operands;

        public IEnumerable<FilterNode> Operands { get; }

        public override bool Apply(DynamicTableEntity entity)
        {
            var result = Operands.Any(operand => operand.Apply(entity));
            return result;
        }
    }
}