namespace CloudStub.FilterParser
{
    internal enum FilterTokenCode
    {
        Text,
        And,
        Or,
        Not,
        Equal,
        NotEqual,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual,
        OpenGroup,
        CloseGroup
    }
}