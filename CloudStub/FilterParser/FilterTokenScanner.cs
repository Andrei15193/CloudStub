using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;

namespace CloudStub.FilterParser
{
    internal class FilterTokenScanner
    {
        private readonly IReadOnlyDictionary<string, FilterTokenCode> _mappings = new Dictionary<string, FilterTokenCode>(StringComparer.OrdinalIgnoreCase)
        {
            { TableOperators.And, FilterTokenCode.And },
            { TableOperators.Or, FilterTokenCode.Or },
            { TableOperators.Not, FilterTokenCode.Not },
            { QueryComparisons.Equal, FilterTokenCode.Equal },
            { QueryComparisons.NotEqual, FilterTokenCode.NotEqual },
            { QueryComparisons.LessThan, FilterTokenCode.LessThan },
            { QueryComparisons.LessThanOrEqual, FilterTokenCode.LessThanOrEqual },
            { QueryComparisons.GreaterThan, FilterTokenCode.GreaterThan },
            { QueryComparisons.GreaterThanOrEqual, FilterTokenCode.GreaterThanOrEqual }
        };

        public IReadOnlyList<FilterToken> Scan(string filter)
        {
            var result = new List<FilterToken>();

            var start = 0;
            var isQuoted = false;
            var isEscaped = false;
            for (var index = 0; index < filter.Length; index++)
                if (filter[index] == '\\')
                    isEscaped = true;
                else
                {
                    if (char.IsWhiteSpace(filter, index))
                    {
                        if (start < index)
                            result.Add(_GetToken(filter.Substring(start, index - start)));
                        start = index + 1;
                    }
                    else if (filter[index] == '\'' && !isEscaped)
                        isQuoted = !isQuoted;
                    else if (filter[index] == '(' && !isQuoted)
                    {
                        if (start < index)
                            result.Add(_GetToken(filter.Substring(start, index - start)));
                        start = index + 1;
                        result.Add(new FilterToken(FilterTokenCode.OpenGroup, "("));
                    }
                    else if (filter[index] == ')' && !isQuoted)
                    {
                        if (start < index)
                            result.Add(_GetToken(filter.Substring(start, index - start)));
                        start = index + 1;
                        result.Add(new FilterToken(FilterTokenCode.CloseGroup, ")"));
                    }
                    isEscaped = false;
                }

            if (start < filter.Length)
                result.Add(_GetToken(filter.Substring(start)));

            return result;
        }

        private FilterToken _GetToken(string tokenText)
            => _mappings.TryGetValue(tokenText, out var tokenCode) ? new FilterToken(tokenCode, tokenText) : new FilterToken(FilterTokenCode.Text, tokenText);
    }
}