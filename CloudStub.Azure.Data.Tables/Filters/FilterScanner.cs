using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace CloudStub.Azure.Data.Tables.Filters
{
    public static class FilterScanner
    {
        public static IReadOnlyList<FilterToken> Scan(string filter)
        {
            if (string.IsNullOrWhiteSpace(filter))
                return new FilterToken[0];

            var tokens = new FilterToken[_GetTokenCount(filter)];

            var tokenStart = 0;
            var tokenIndex = 0;
            var isInString = false;
            for (var index = 0; index < filter.Length; index++)
            {
                if (!isInString && char.IsWhiteSpace(filter, index))
                {
                    if (tokenStart < index)
                    {
                        tokens[tokenIndex] = _GetToken(filter, tokenStart, index);
                        tokenIndex++;
                        tokenStart = index;
                    }
                    tokenStart++;
                }
                else if (!isInString && (filter[index] == '(' || filter[index] == ')'))
                {
                    if (tokenStart < index)
                    {
                        tokens[tokenIndex] = _GetToken(filter, tokenStart, index);
                        tokenIndex++;
                        tokenStart = index;
                    }

                    tokens[tokenIndex] = _GetToken(filter, tokenStart, index + 1);
                    tokenIndex++;
                    tokenStart = index + 1;
                }
                else if (filter[index] == '\'')
                    if (!isInString)
                        isInString = true;
                    else if (index < filter.Length - 1 && filter[index + 1] == '\'')
                        index++;
                    else
                    {
                        tokens[tokenIndex] = _GetToken(filter, tokenStart, index + 1);
                        tokenIndex++;

                        tokenStart = index + 1;
                        isInString = false;
                    }
            }
            if (tokenStart < filter.Length)
                tokens[tokenIndex] = _GetToken(filter, tokenStart, filter.Length);

            return tokens;
        }

        private static int _GetTokenCount(string filter)
        {
            var tokenCount = 0;

            var isInString = false;
            var tokenStart = 0;
            for (var index = 0; index < filter.Length; index++)
                switch (filter[index])
                {
                    case '\'':
                        if (!isInString)
                            isInString = true;
                        else if (index < filter.Length - 1 && filter[index + 1] == '\'')
                            index++;
                        else
                        {
                            tokenStart = index + 1;
                            tokenCount++;
                            isInString = false;
                        }
                        break;

                    case '(' when !isInString:
                    case ')' when !isInString:
                        if (tokenStart < index)
                            tokenCount++;

                        tokenStart = index + 1;
                        tokenCount++;
                        break;

                    default:
                        if (!isInString && char.IsWhiteSpace(filter, index))
                        {
                            if (tokenStart < index)
                                tokenCount++;

                            tokenStart = index + 1;
                        }
                        break;
                }
            if (tokenStart < filter.Length)
                tokenCount++;

            return tokenCount;
        }

        private static FilterToken _GetToken(string filter, int start, int end)
        {
            if (_IsEqualToCaseSensitive("(", filter, start, end))
                return new FilterToken(FilterTokenType.GroupOpen, filter, start, end);
            else if (_IsEqualToCaseSensitive(")", filter, start, end))
                return new FilterToken(FilterTokenType.GroupClose, filter, start, end);

            else if (_IsEqualToCaseSensitive("true", filter, start, end))
                return new FilterToken(FilterTokenType.Boolean, filter, start, end, true);
            else if (_IsEqualToCaseSensitive("false", filter, start, end))
                return new FilterToken(FilterTokenType.Boolean, filter, start, end, false);

            else if (_IsEqualToCaseSensitive("eq", filter, start, end))
                return new FilterToken(FilterTokenType.Equals, filter, start, end);
            else if (_IsEqualToCaseSensitive("ne", filter, start, end))
                return new FilterToken(FilterTokenType.NotEquals, filter, start, end);
            else if (_IsEqualToCaseSensitive("lt", filter, start, end))
                return new FilterToken(FilterTokenType.LessThan, filter, start, end);
            else if (_IsEqualToCaseSensitive("le", filter, start, end))
                return new FilterToken(FilterTokenType.LessThanOrEqualTo, filter, start, end);
            else if (_IsEqualToCaseSensitive("gt", filter, start, end))
                return new FilterToken(FilterTokenType.GreaterThan, filter, start, end);
            else if (_IsEqualToCaseSensitive("ge", filter, start, end))
                return new FilterToken(FilterTokenType.GreaterThanOrEqualTo, filter, start, end);

            else if (_IsEqualToCaseSensitive("or", filter, start, end))
                return new FilterToken(FilterTokenType.Or, filter, start, end);
            else if (_IsEqualToCaseSensitive("and", filter, start, end))
                return new FilterToken(FilterTokenType.And, filter, start, end);

            else if (_IsEqualToCaseSensitive("not", filter, start, end))
                return new FilterToken(FilterTokenType.Not, filter, start, end);

            else if ('\'' == filter[start] && '\'' == filter[end - 1])
                return new FilterToken(FilterTokenType.String, filter, start, end, _GetString(filter, start, end));

            else if (_IsInt32(filter, start, end) && int.TryParse(filter.Substring(start, end - start), NumberStyles.Integer, CultureInfo.InvariantCulture, out var int32))
                return new FilterToken(FilterTokenType.Int32, filter, start, end, int32);
            else if (_IsInt64(filter, start, end) && long.TryParse(filter.Substring(start, end - start - (char.ToUpperInvariant(filter[end - 1]) == 'L' ? 1 : 0)), NumberStyles.Integer, CultureInfo.InvariantCulture, out var int64))
                return new FilterToken(FilterTokenType.Int64, filter, start, end, int64);
            else if (_IsDouble(filter, start, end) && double.TryParse(filter.Substring(start, end - start - (char.ToUpperInvariant(filter[end - 1]) == 'D' ? 1 : 0)), NumberStyles.Float, CultureInfo.InvariantCulture, out var @double))
                return new FilterToken(FilterTokenType.Double, filter, start, end, @double);

            else if (_StartsWith("datetime'", filter, start, end) && filter[end - 1] == '\'')
            {
                var valueStart = "datetime'".Length;
                var valueEnd = end - 1;
                var dateTimeString = filter.Substring(valueStart, valueEnd - valueStart);

                if (DateTimeOffset.TryParse(dateTimeString, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dateTimeOffset))
                    return new FilterToken(FilterTokenType.DateTime, filter, start, end, dateTimeOffset.ToUniversalTime());
                else
                    return new FilterToken(FilterTokenType.Unknown, filter, start, end);
            }

            else if (_StartsWith("guid'", filter, start, end) && filter[end - 1] == '\'')
            {
                var valueStart = "guid'".Length;
                var valueEnd = end - 1;
                var guidString = filter.Substring(valueStart, valueEnd - valueStart);

                if (Guid.TryParseExact(guidString, "D", out var guid))
                    return new FilterToken(FilterTokenType.Guid, filter, start, end, guid);
                else
                    return new FilterToken(FilterTokenType.Unknown, filter, start, end);
            }

            else if (_StartsWith("x'", filter, start, end) && filter[end - 1] == '\'')
            {
                var valueStart = "x'".Length;
                var valueEnd = end - 1;

                if (_IsHexString(filter, valueStart, valueEnd))
                {
                    var bytes = new byte[(valueEnd - valueStart) / 2];
                    for (int index = valueStart, byteIndex = 0; index < valueEnd; index += 2, byteIndex++)
                        bytes[byteIndex] = (byte)(_GetByte(filter[index]) << 4 | _GetByte(filter[index + 1]));

                    return new FilterToken(FilterTokenType.Binary, filter, start, end, bytes);
                }
                else
                    return new FilterToken(FilterTokenType.Unknown, filter, start, end);
            }

            else if (_IsIdentifier(filter, start, end))
                return new FilterToken(FilterTokenType.Identifier, filter, start, end, filter.Substring(start, end - start));

            else
                return new FilterToken(FilterTokenType.Unknown, filter, start, end);
        }

        private static string _GetString(string filter, int start, int end)
        {
            var builder = new StringBuilder(end - start - 2);

            for (var index = start + 1; index < end - 1; index++)
            {
                builder.Append(filter[index]);
                if (filter[index] == '\'')
                    index++;
            }

            return builder.ToString();
        }

        private static byte _GetByte(char hexChar)
        {
            if (_IsDigit(hexChar))
                return (byte)(hexChar - '0');
            else if ('a' <= hexChar && hexChar <= 'f')
                return (byte)(hexChar - 'a' + 10);
            else if ('A' <= hexChar && hexChar <= 'F')
                return (byte)(hexChar - 'A' + 10);
            else
                throw new ArgumentException($"Unknown '{hexChar}' hex char.", nameof(hexChar));
        }

        private static bool _IsIdentifier(string filter, int start, int end)
        {
            var index = start;
            if (start < end && _IsLeterOrUnderscore(filter[start]))
            {
                index++;
                while (index < end && _IsLeterUnderscoreOrDigit(filter[index]))
                    index++;
            }

            return index == end;
        }

        private static bool _IsHexString(string filter, int start, int end)
        {
            var index = start;
            if (start < end && (end - start) % 2 == 0)
                while (index < end && _IsHexDigit(filter[index]))
                    index++;

            return index == end;
        }

        private static bool _IsInt32(string filter, int start, int end)
        {
            var index = start;
            if (start < end)
            {
                if (filter[start] == '-')
                    index++;

                while (index < end && _IsDigit(filter[index]))
                    index++;

                return index == end;
            }
            else
                return false;
        }

        private static bool _IsInt64(string filter, int start, int end)
        {
            var index = start;
            if (start < end)
            {
                if (filter[start] == '-')
                    index++;

                while (index < end && _IsDigit(filter[index]))
                    index++;

                if (index == end - 1 && char.ToUpperInvariant(filter[index]) == 'L')
                    index++;

                return index == end;
            }
            else
                return false;
        }

        private static bool _IsDouble(string filter, int start, int end)
        {
            var index = start;
            if (start < end)
            {
                if (filter[start] == '-')
                    index++;

                while (index < end && _IsDigit(filter[index]))
                    index++;

                if (index < end - 1 && filter[index] == '.')
                    index++;

                while (index < end && _IsDigit(filter[index]))
                    index++;

                if (index == end - 1 && char.ToUpperInvariant(filter[index]) == 'D')
                    index++;

                return index == end;
            }
            else
                return false;
        }

        private static bool _IsLeterUnderscoreOrDigit(char @char)
            => _IsLeterOrUnderscore(@char) || _IsDigit(@char);

        private static bool _IsLeterOrUnderscore(char @char)
            => @char == '_' || ('a' <= @char && @char <= 'z') || ('A' <= @char && @char <= 'Z');

        private static bool _IsHexDigit(char @char)
            => (
                ('0' <= @char && @char <= '9')
                || ('a' <= @char && @char <= 'f')
                || ('A' <= @char && @char <= 'F')
            );

        private static bool _IsDigit(char @char)
            => '0' <= @char && @char <= '9';

        private static bool _StartsWith(string value, string filter, int start, int end)
        {
            var isMatch = true;
            var index = 0;
            var length = end - start;
            var compareLength = value.Length > length ? length : value.Length;

            while (isMatch && index < compareLength)
            {
                isMatch = char.ToLowerInvariant(value[index]) == char.ToLowerInvariant(filter[start + index]);
                index++;
            }

            return isMatch;
        }

        private static bool _IsEqualToCaseSensitive(string value, string filter, int start, int end)
        {
            var index = 0;
            var length = end - start;

            if (value.Length == length)
            {
                while (index < value.Length && value[index] == filter[start + index])
                    index++;

                return index == value.Length;
            }
            else
                return false;
        }
    }
}