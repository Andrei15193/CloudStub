namespace CloudStub.FilterParser.FilterNodes
{
    internal class GreaterThanOrEqualFilterNode : PropertyFilterNode
    {
        public GreaterThanOrEqualFilterNode(string propertyName, StubEntityProperty filterValue)
            : base(propertyName, filterValue)
        {
        }

        public override bool Apply(StubEntity entity)
        {
            var compareResult = Compare(GetValueFromEntity(entity), FilterValue);
            var result = compareResult >= 0;
            return result;
        }
    }
}