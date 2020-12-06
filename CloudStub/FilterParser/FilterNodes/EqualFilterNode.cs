using Microsoft.Azure.Cosmos.Table;

namespace CloudStub.FilterParser.FilterNodes
{
    internal class EqualFilterNode : PropertyFilterNode
    {
        public EqualFilterNode(string propertyName, EntityProperty filterValue)
            : base(propertyName, filterValue)
        {
        }

        public override bool Apply(DynamicTableEntity entity)
        {
            var compareResult = Compare(GetValueFromEntity(entity), FilterValue);
            var result = compareResult == 0;
            return result;
        }
    }
}