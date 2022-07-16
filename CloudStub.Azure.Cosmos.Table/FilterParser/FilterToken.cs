namespace CloudStub.FilterParser
{
    internal struct FilterToken
    {
        public FilterToken(FilterTokenCode code, string value)
        {
            Code = code;
            Value = value;
        }

        public FilterTokenCode Code { get; }

        public string Value { get; }
    }
}