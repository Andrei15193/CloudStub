using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.FilterParser.FilterNodes
{
    internal class NotFilterNode : FilterNode
    {
        public NotFilterNode(FilterNode operand)
            => Operand = operand;

        public FilterNode Operand { get; set; }

        public override bool Apply(DynamicTableEntity entity)
        {
            var operandResult = Operand.Apply(entity);
            var result = !operandResult;
            return result;
        }
    }
}