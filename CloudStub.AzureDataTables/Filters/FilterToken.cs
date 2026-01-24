namespace CloudStub.AzureDataTables.Filters
{
    public readonly struct FilterToken
    {
        public FilterToken(FilterTokenType tokenType, string filter, int start, int end)
             : this(tokenType, filter, start, end, null)
        {
        }

        public FilterToken(FilterTokenType tokenType, string filter, int start, int end, object value)
        {
            Type = tokenType;
            Filter = filter;
            Start = start;
            End = end;
            Value = value;
        }

        public FilterTokenType Type { get; }
        public string Filter { get; }
        public int Start { get; }
        public int End { get; }
        public object Value { get; }
    }
}