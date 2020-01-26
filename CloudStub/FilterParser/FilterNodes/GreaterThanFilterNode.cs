using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.FilterParser.FilterNodes
{
    internal class GreaterThanFilterNode : PropertyFilterNode
    {
        public GreaterThanFilterNode(string propertyName, EntityProperty filterValue)
            : base(propertyName, filterValue)
        {
        }

        public override bool Apply(DynamicTableEntity entity)
        {
            var result = false;
            var entityProperty = GetValueFromEntity(entity);
            if (entityProperty != null)
            {
                var compareResult = Compare(entityProperty, FilterValue);
                result = compareResult > 0;
            }
            return result;
        }
    }
}