using Microsoft.WindowsAzure.Storage.Table;

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
            var entityProperty = GetValueFromEntity(entity);
            var result = entityProperty != null && FilterValue.Equals(entityProperty);
            return result;
        }
    }
}