using CloudStub.Core;

namespace CloudStub.FilterParser.FilterNodes
{
    internal class NotEqualFilterNode : PropertyFilterNode
    {
        public NotEqualFilterNode(string propertyName, StubEntityProperty filterValue)
            : base(propertyName, filterValue)
        {
        }

        public override bool Apply(StubEntity entity)
        {
            var compareResult = Compare(GetValueFromEntity(entity), FilterValue);
            var result = compareResult != null && compareResult != 0;
            return result;
        }
    }
}