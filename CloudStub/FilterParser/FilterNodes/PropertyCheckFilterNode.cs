using CloudStub.Core;

namespace CloudStub.FilterParser.FilterNodes
{
    internal class PropertyCheckFilterNode : EqualFilterNode
    {
        public PropertyCheckFilterNode(string propertyName)
            : base(propertyName, new StubEntityProperty(true))
        {
        }
    }
}