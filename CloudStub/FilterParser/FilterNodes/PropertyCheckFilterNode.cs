using Microsoft.Azure.Cosmos.Table;

namespace CloudStub.FilterParser.FilterNodes
{
    internal class PropertyCheckFilterNode : EqualFilterNode
    {
        public PropertyCheckFilterNode(string propertyName)
            : base(propertyName, EntityProperty.GeneratePropertyForBool(true))
        {
        }
    }
}