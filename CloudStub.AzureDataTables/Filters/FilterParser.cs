using System.Collections.Generic;
using System.Linq;
using CloudStub.AzureDataTables.Filters.Nodes;

namespace CloudStub.AzureDataTables.Filters
{
    internal static class FilterParser
    {
        public static Filter Parse(IReadOnlyList<FilterToken> tokens)
        {
            var unknownToken = tokens.FirstOrDefault(token => token.Type == FilterTokenType.Unknown);
            if (unknownToken.Filter != null)
                return new InvalidFilter(InvalidFilterType.SyntaxError, $"Syntax error at position {unknownToken.End} in '{unknownToken.Filter}'.");

            return _Parse(tokens, 0, tokens.Count);
        }

        private static Filter _Parse(IReadOnlyList<FilterToken> tokens, int start, int end)
        {
            if (start == end)
                return new TrueFilter();

            if (tokens[start].Type == FilterTokenType.Not && end - start > 1)
                return new NotFilter(_Parse(tokens, start + 1, end));

            var logicalFilter = _TryParseLogicalFilter(tokens, start, end);
            if (logicalFilter != null)
                return logicalFilter;

            if (tokens[start].Type == FilterTokenType.GroupOpen && tokens[end - 1].Type == FilterTokenType.GroupClose)
                return _ParseGroup(tokens, start, end);

            return _ParseDiscreteFilter(tokens, start, end);
        }

        private static Filter _ParseDiscreteFilter(IReadOnlyList<FilterToken> tokens, int start, int end)
        {
            switch (end - start)
            {
                case 0:
                    return new TrueFilter();

                case 1:
                    if (tokens[start].Type != FilterTokenType.Identifier)
                        return new InvalidFilter(InvalidFilterType.SyntaxError, $"Syntax error at position {tokens[start].End} in '{tokens[start].Filter}'.");
                    else
                        return new EqualsFilter((string)tokens[start].Value, true);

                case 3:
                    if (tokens[start].Type == FilterTokenType.Identifier && _IsValueTokenType(tokens[end - 1].Type))
                    {
                        var propertyName = (string)tokens[start].Value;
                        var value = tokens[end - 1].Value;

                        switch (tokens[start + 1].Type)
                        {
                            case FilterTokenType.Equals:
                                if (value == null)
                                    return new InvalidFilter(InvalidFilterType.NotSupported, "One of the request inputs is not valid.");
                                else
                                    return new EqualsFilter(propertyName, value);

                            case FilterTokenType.NotEquals:
                                if (value == null)
                                    return new InvalidFilter(InvalidFilterType.NotSupported, "One of the request inputs is not valid.");
                                else
                                    return new NotEqualsFilter(propertyName, value);

                            case FilterTokenType.LessThan:
                                if (value == null)
                                    return new InvalidFilter(InvalidFilterType.SyntaxError, "The operator 'LessThan' is not supported for the 'null' literal; only equality checks are supported.");
                                else
                                    return new LessThanFilter(propertyName, value);

                            case FilterTokenType.LessThanOrEqualTo:
                                if (value == null)
                                    return new InvalidFilter(InvalidFilterType.SyntaxError, "The operator 'LessThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.");
                                else
                                    return new LessThanOrEqualFilter(propertyName, value);

                            case FilterTokenType.GreaterThan:
                                if (value == null)
                                    return new InvalidFilter(InvalidFilterType.SyntaxError, "The operator 'GreaterThan' is not supported for the 'null' literal; only equality checks are supported.");
                                else
                                    return new GreaterThanFilter(propertyName, value);

                            case FilterTokenType.GreaterThanOrEqualTo:
                                if (value == null)
                                    return new InvalidFilter(InvalidFilterType.SyntaxError, "The operator 'GreaterThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.");
                                else
                                    return new GreaterThanOrEqualFilter(propertyName, value);

                            default:
                                return new InvalidFilter(InvalidFilterType.SyntaxError, $"Syntax error at position {tokens[end - 1].End} in '{tokens[end - 1].Filter}'.");
                        }
                    }
                    else if (_IsValueTokenType(tokens[start].Type) && tokens[end - 1].Type == FilterTokenType.Identifier)
                    {
                        var value = tokens[start].Value;
                        var propertyName = (string)tokens[end - 1].Value;

                        switch (tokens[start + 1].Type)
                        {
                            case FilterTokenType.Equals:
                                if (value == null)
                                    return new InvalidFilter(InvalidFilterType.NotSupported, "One of the request inputs is not valid.");
                                else
                                    return new EqualsFilter(propertyName, value);

                            case FilterTokenType.NotEquals:
                                if (value == null)
                                    return new InvalidFilter(InvalidFilterType.NotSupported, "One of the request inputs is not valid.");
                                else
                                    return new NotEqualsFilter(propertyName, value);

                            case FilterTokenType.LessThan:
                                if (value == null)
                                    return new InvalidFilter(InvalidFilterType.SyntaxError, "The operator 'LessThan' is not supported for the 'null' literal; only equality checks are supported.");
                                else
                                    return new GreaterThanFilter(propertyName, value);

                            case FilterTokenType.LessThanOrEqualTo:
                                if (value == null)
                                    return new InvalidFilter(InvalidFilterType.SyntaxError, "The operator 'LessThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.");
                                else
                                    return new GreaterThanOrEqualFilter(propertyName, value);

                            case FilterTokenType.GreaterThan:
                                if (value == null)
                                    return new InvalidFilter(InvalidFilterType.SyntaxError, "The operator 'GreaterThan' is not supported for the 'null' literal; only equality checks are supported.");
                                else
                                    return new LessThanFilter(propertyName, value);

                            case FilterTokenType.GreaterThanOrEqualTo:
                                if (value == null)
                                    return new InvalidFilter(InvalidFilterType.SyntaxError, "The operator 'GreaterThanOrEqual' is not supported for the 'null' literal; only equality checks are supported.");
                                else
                                    return new LessThanOrEqualFilter(propertyName, value);

                            default:
                                return new InvalidFilter(InvalidFilterType.SyntaxError, $"Syntax error at position {tokens[end - 1].End} in '{tokens[end - 1].Filter}'.");
                        }
                    }
                    else
                        return new InvalidFilter(InvalidFilterType.NotImplemented, "The requested operation is not implemented on the specified resource.");

                default:
                    return new InvalidFilter(InvalidFilterType.SyntaxError, $"Syntax error at position {tokens[end - 1].End} in '{tokens[end - 1].Filter}'.");
            }
        }

        private static Filter _TryParseLogicalFilter(IReadOnlyList<FilterToken> tokens, int start, int end)
        {
            var index = start;
            var nestingLevel = 0;
            var foundLogicalOperator = false;
            while (index < end && !foundLogicalOperator)
                switch (tokens[index].Type)
                {
                    case FilterTokenType.GroupOpen:
                        nestingLevel++;
                        index++;
                        break;

                    case FilterTokenType.GroupClose:
                        nestingLevel--;
                        index++;
                        break;

                    case FilterTokenType.Or when nestingLevel == 0:
                    case FilterTokenType.And when nestingLevel == 0:
                        foundLogicalOperator = true;
                        break;

                    default:
                        index++;
                        break;
                }

            if (index == end)
                return null;

            if (start == index || index + 1 == end)
                return new InvalidFilter(InvalidFilterType.SyntaxError, $"Syntax error at position {tokens[index].End} in '{tokens[index].Filter}'.");

            var leftFilter = _Parse(tokens, start, index);
            if (leftFilter is InvalidFilter)
                return leftFilter;

            var rightFilter = _Parse(tokens, index + 1, end);
            if (rightFilter is InvalidFilter)
                return rightFilter;

            if (tokens[index].Type == FilterTokenType.And)
                return new AndFilter(leftFilter, rightFilter);
            else
                return new OrFilter(leftFilter, rightFilter);
        }

        private static Filter _ParseGroup(IReadOnlyList<FilterToken> tokens, int start, int end)
        {
            var index = start + 1;
            var nestingLevel = 1;
            while (index < end && nestingLevel > 0)
            {
                switch (tokens[index].Type)
                {
                    case FilterTokenType.GroupOpen:
                        nestingLevel++;
                        break;

                    case FilterTokenType.GroupClose:
                        nestingLevel--;
                        break;
                }

                index++;
            }

            if (index != end)
                return new InvalidFilter(InvalidFilterType.SyntaxError, $"Syntax error at position {tokens[index - 1].End} in '{tokens[index - 1].Filter}'.");
            else
                return _Parse(tokens, start + 1, end - 1);
        }

        private static bool _IsValueTokenType(FilterTokenType filterTokenType)
        {
            switch (filterTokenType)
            {
                case FilterTokenType.Boolean:
                case FilterTokenType.Int32:
                case FilterTokenType.Int64:
                case FilterTokenType.Double:
                case FilterTokenType.DateTime:
                case FilterTokenType.Guid:
                case FilterTokenType.Binary:
                case FilterTokenType.String:
                case FilterTokenType.Null:
                    return true;

                default:
                    return false;
            }
        }
    }
}