namespace CloudStub.AzureDataTables.Filters
{
    internal enum FilterTokenType : byte
    {
        Unknown,
        Identifier,
        Boolean,
        Int32,
        Int64,
        Double,
        DateTime,
        Guid,
        Binary,
        String,
        Null,
        GroupOpen,
        GroupClose,
        Equals,
        NotEquals,
        LessThan,
        LessThanOrEqualTo,
        GreaterThan,
        GreaterThanOrEqualTo,
        And,
        Or,
        Not
    }
}