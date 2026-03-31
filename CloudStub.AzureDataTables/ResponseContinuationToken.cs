using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace CloudStub.AzureDataTables
{
    internal static class ResponseContinuationToken
    {
        private const string _continuationTokenTableNamePadding = "\u000101dcb0e52a153536";
        private const char _tableNameContinuationTokenPaddingSeparator = '\u0001';
        private static readonly Regex _continuationTokenValueRegex = new Regex(@"^1!(?<length>\d+)!(?<value>.*)$", RegexOptions.Compiled);

        public static string EncodeContinuationToken(string value)
        {
            var encodedValue = Convert
                .ToBase64String(Encoding.UTF8.GetBytes(value))
                .Replace("+", "*")
                .Replace("=", "-")
                .Replace("/", "_");

            return $"1!{encodedValue.Length}!{encodedValue}";
        }

        public static string EncodeTableNameContinuationToken(string value)
        {
            var continuationToken = value.ToLowerInvariant() + _continuationTokenTableNamePadding + Guid.NewGuid().ToString("N");

            return EncodeContinuationToken(continuationToken.Substring(0, Math.Min(value.Length, TableServiceClientStub.TableNameMaximumLength)));
        }

        public static string DecodeTableNameContinuationToken(string value)
            => DecodeContinuationToken(value)[0].Split(_tableNameContinuationTokenPaddingSeparator)[0];

        public static (string PartitionKey, string RowKey) DecodeRowContinuationToken(string continuationToken)
        {
            if (string.IsNullOrWhiteSpace(continuationToken))
                return (null, null);

            var parts = DecodeContinuationToken(continuationToken);
            if (parts?.Count != 2)
                return (null, null);

            return (parts[0], parts[1]);
        }

        public static IReadOnlyList<string> DecodeContinuationToken(string continuationToken)
            => continuationToken
                ?.Split(' ')
                .Select(continuationTokenPart =>
                {
                    var match = _continuationTokenValueRegex.Match(continuationTokenPart);

                    if (!match.Success)
                        return null;

                    var length = int.Parse(match.Groups["length"].Value, NumberStyles.None, CultureInfo.InvariantCulture);
                    var value = match.Groups["value"].Value;

                    if (value.Length != length)
                        return null;

                    return Encoding.UTF8.GetString(Convert.FromBase64String(value.Replace("*", "+").Replace("-", "=").Replace("_", "/")));
                })
                .Where(value => value != null)
                .ToList();
    }
}