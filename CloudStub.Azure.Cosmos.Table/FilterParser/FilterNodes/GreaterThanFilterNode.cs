namespace CloudStub.FilterParser.FilterNodes
{
    internal class GreaterThanFilterNode : PropertyFilterNode
    {
        public GreaterThanFilterNode(string propertyName, StubEntityProperty filterValue)
            : base(propertyName, filterValue)
        {
        }

        public override bool Apply(StubEntity entity)
        {
            var compareResult = Compare(GetValueFromEntity(entity), FilterValue);
            var result = compareResult > 0;
            return result;
        }
    }
}