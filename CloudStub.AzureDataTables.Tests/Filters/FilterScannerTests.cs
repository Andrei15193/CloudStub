using System;
using System.Globalization;
using CloudStub.AzureDataTables.Filters;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Filters
{
    public class FilterScannerTests
    {
        [Theory]
        [InlineData(null, 0)]
        [InlineData("", 0)]
        [InlineData("  ", 0)]
        [InlineData(" \t ", 0)]
        [InlineData("a", 1)]
        [InlineData("a b", 2)]
        [InlineData("a(b)", 4)]
        [InlineData("a(b)c", 5)]
        [InlineData("a()c", 4)]
        [InlineData("word'value'another", 2)]
        [InlineData("''''", 1)]
        [InlineData("(", 1)]
        [InlineData(")", 1)]
        [InlineData("()", 2)]
        [InlineData("() ", 2)]
        [InlineData(" ()", 2)]
        [InlineData("( )", 2)]
        [InlineData(" ( )", 2)]
        [InlineData("( ) ", 2)]
        [InlineData(" ( ) ", 2)]
        public void Scan_WhenPassedAFilter_ReturnsExpectedNumberOfTokens(string filter, int expectedTokenCount)
        {
            var tokens = FilterScanner.Scan(filter);

            Assert.Equal(expectedTokenCount, tokens.Count);
        }

        [Theory]
        [InlineData("identifier", FilterTokenType.Identifier)]
        [InlineData("eq", FilterTokenType.Equals)]
        [InlineData("ne", FilterTokenType.NotEquals)]
        [InlineData("lt", FilterTokenType.LessThan)]
        [InlineData("le", FilterTokenType.LessThanOrEqualTo)]
        [InlineData("gt", FilterTokenType.GreaterThan)]
        [InlineData("ge", FilterTokenType.GreaterThanOrEqualTo)]
        [InlineData("or", FilterTokenType.Or)]
        [InlineData("and", FilterTokenType.And)]
        [InlineData("not", FilterTokenType.Not)]
        [InlineData("true", FilterTokenType.Boolean)]
        [InlineData("false", FilterTokenType.Boolean)]
        [InlineData("1", FilterTokenType.Int32)]
        [InlineData("1L", FilterTokenType.Int64)]
        [InlineData("3D", FilterTokenType.Double)]
        [InlineData("3.0", FilterTokenType.Double)]
        [InlineData("-1", FilterTokenType.Int32)]
        [InlineData("-1L", FilterTokenType.Int64)]
        [InlineData("-3D", FilterTokenType.Double)]
        [InlineData("-3.0", FilterTokenType.Double)]
        [InlineData("'test'", FilterTokenType.String)]
        [InlineData("'test''escaped'", FilterTokenType.String)]
        [InlineData("'test ( this ) is () not grouped'", FilterTokenType.String)]
        [InlineData("datetime'2020-01-01T00:00:00'", FilterTokenType.DateTime)]
        [InlineData("datetime'invalid'", FilterTokenType.Unknown)]
        [InlineData("guid'eaffcc56-5569-4683-90d5-4c14955ed7dd'", FilterTokenType.Guid)]
        [InlineData("x''", FilterTokenType.Binary)]
        [InlineData("x'010203'", FilterTokenType.Binary)]
        [InlineData("(", FilterTokenType.GroupOpen)]
        [InlineData(")", FilterTokenType.GroupClose)]
        [InlineData("unknown'type'", FilterTokenType.Unknown)]
        [InlineData("-", FilterTokenType.Unknown)]
        public void Scan_WhenPassedAFilter_ReturnsExpectedTokenType(string filter, FilterTokenType expectedFilterTokenType)
        {
            var tokens = FilterScanner.Scan(filter);

            var token = Assert.Single(tokens);
            Assert.Equal(expectedFilterTokenType, token.Type);
        }

        [Fact]
        public void Scan_WhenPassedStringFollowedByValue_SeparatesTheTwo()
        {
            var tokens = FilterScanner.Scan("'text'identifier");

            Assert.Collection(
                tokens,
                firstToken => Assert.Equal(FilterTokenType.String, firstToken.Type),
                secondToken => Assert.Equal(FilterTokenType.Identifier, secondToken.Type)
            );
        }

        [Theory]
        [InlineData("'test''escaped'", "test'escaped")]
        [InlineData("''''", "'")]
        [InlineData("'test ( this ) is () not grouped'", "test ( this ) is () not grouped")]
        public void Scan_WhenPassedTextValue_StoresNormalizedStringInValue(string filter, string value)
        {
            var tokens = FilterScanner.Scan(filter);

            var token = Assert.Single(tokens);
            Assert.Multiple(
                () => Assert.Equal(FilterTokenType.String, token.Type),
                () => Assert.Equal(value, token.Value)
            );
        }

        [Theory]
        [InlineData("3", 3)]
        [InlineData("-3", -3)]
        [InlineData("100", 100)]
        [InlineData("2147483647", 2147483647)]
        public void Scan_WhenPassedInt32Value_StoresParsedNumber(string filter, int value)
        {
            var tokens = FilterScanner.Scan(filter);

            var token = Assert.Single(tokens);
            Assert.Multiple(
                () => Assert.Equal(FilterTokenType.Int32, token.Type),
                () => Assert.Equal(value, token.Value)
            );
        }

        [Theory]
        [InlineData("3L", 3)]
        [InlineData("3l", 3)]
        [InlineData("-3L", -3)]
        [InlineData("-3l", -3)]
        [InlineData("100L", 100)]
        [InlineData("100l", 100)]
        [InlineData("2147483648", 2147483648)]
        public void Scan_WhenPassedInt64Value_StoresParsedNumber(string filter, long value)
        {
            var tokens = FilterScanner.Scan(filter);

            var token = Assert.Single(tokens);
            Assert.Multiple(
                () => Assert.Equal(FilterTokenType.Int64, token.Type),
                () => Assert.Equal(value, token.Value)
            );
        }

        [Theory]
        [InlineData("3D", 3)]
        [InlineData("3d", 3)]
        [InlineData("3.0", 3)]
        [InlineData("3.123", 3.123)]
        [InlineData("-3D", -3)]
        [InlineData("-3d", -3)]
        [InlineData("100D", 100)]
        [InlineData("100d", 100)]
        [InlineData("9223372036854775808", 9223372036854775808)]
        public void Scan_WhenPassedDoubleValue_StoresParsedNumber(string filter, double value)
        {
            var tokens = FilterScanner.Scan(filter);

            var token = Assert.Single(tokens);
            Assert.Multiple(
                () => Assert.Equal(FilterTokenType.Double, token.Type),
                () => Assert.Equal(value, token.Value)
            );
        }

        [Theory]
        [InlineData("datetime'2020-01-01T00:00'", "2020-01-01T00:00:00.0000000Z")]
        [InlineData("datetime'2020-01-01T02:00+02:00'", "2020-01-01T00:00:00.0000000Z")]
        [InlineData("datetime'2020-01-01T00:00Z'", "2020-01-01T00:00:00.0000000Z")]
        public void Scan_WhenPassedDateTimeValue_StoresParsedDateTimeOffset(string filter, string value)
        {
            var tokens = FilterScanner.Scan(filter);

            var token = Assert.Single(tokens);
            Assert.Multiple(
                () => Assert.Equal(FilterTokenType.DateTime, token.Type),
                () => Assert.Equal(DateTimeOffset.ParseExact(value, "O", CultureInfo.InvariantCulture), token.Value)
            );
        }

        [Theory]
        [InlineData("guid'21070a37-b095-4618-8911-2b9963fb1d40'", "21070a37-b095-4618-8911-2b9963fb1d40")]
        [InlineData("guid'21070A37-B095-4618-8911-2B9963FB1D40'", "21070a37-b095-4618-8911-2b9963fb1d40")]
        public void Scan_WhenPassedGuidValue_StoresParsedGuid(string filter, string value)
        {
            var tokens = FilterScanner.Scan(filter);

            var token = Assert.Single(tokens);
            Assert.Multiple(
                () => Assert.Equal(FilterTokenType.Guid, token.Type),
                () => Assert.Equal(Guid.ParseExact(value, "D"), token.Value)
            );
        }

        [Theory]
        [InlineData("x'00'", "AA==")]
        [InlineData("x'0F'", "Dw==")]
        [InlineData("x'F0'", "8A==")]
        [InlineData("x'FF'", "/w==")]
        [InlineData("x'0f'", "Dw==")]
        [InlineData("x'f0'", "8A==")]
        [InlineData("x'ff'", "/w==")]
        [InlineData("x'fF'", "/w==")]
        [InlineData("x'Ff'", "/w==")]
        [InlineData("x'000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F404142434445464748494A4B4C4D4E4F505152535455565758595A5B5C5D5E5F606162636465666768696A6B6C6D6E6F707172737475767778797A7B7C7D7E7F808182838485868788898A8B8C8D8E8F909192939495969798999A9B9C9D9E9FA0A1A2A3A4A5A6A7A8A9AAABACADAEAFB0B1B2B3B4B5B6B7B8B9BABBBCBDBEBFC0C1C2C3C4C5C6C7C8C9CACBCCCDCECFD0D1D2D3D4D5D6D7D8D9DADBDCDDDEDFE0E1E2E3E4E5E6E7E8E9EAEBECEDEEEFF0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF'", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w==")]
        [InlineData("x'000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff'", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w==")]
        [InlineData("X'00'", "AA==")]
        [InlineData("X'0F'", "Dw==")]
        [InlineData("X'F0'", "8A==")]
        [InlineData("X'FF'", "/w==")]
        [InlineData("X'0f'", "Dw==")]
        [InlineData("X'f0'", "8A==")]
        [InlineData("X'ff'", "/w==")]
        [InlineData("X'fF'", "/w==")]
        [InlineData("X'Ff'", "/w==")]
        [InlineData("X'000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F404142434445464748494A4B4C4D4E4F505152535455565758595A5B5C5D5E5F606162636465666768696A6B6C6D6E6F707172737475767778797A7B7C7D7E7F808182838485868788898A8B8C8D8E8F909192939495969798999A9B9C9D9E9FA0A1A2A3A4A5A6A7A8A9AAABACADAEAFB0B1B2B3B4B5B6B7B8B9BABBBCBDBEBFC0C1C2C3C4C5C6C7C8C9CACBCCCDCECFD0D1D2D3D4D5D6D7D8D9DADBDCDDDEDFE0E1E2E3E4E5E6E7E8E9EAEBECEDEEEFF0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF'", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w==")]
        [InlineData("X'000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff'", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w==")]
        public void Scan_WhenPassedBinaryValue_StoresParsedByteArray(string filter, string value)
        {
            var tokens = FilterScanner.Scan(filter);

            var token = Assert.Single(tokens);
            Assert.Multiple(
                () => Assert.Equal(FilterTokenType.Binary, token.Type),
                () => Assert.Equal(Convert.FromBase64String(value), token.Value)
            );
        }

        [Theory]
        [InlineData("true", true)]
        [InlineData("false", false)]
        public void Scan_WhenPassedBooleanValue_StoresParsedBoolean(string filter, bool value)
        {
            var tokens = FilterScanner.Scan(filter);

            var token = Assert.Single(tokens);
            Assert.Multiple(
                () => Assert.Equal(FilterTokenType.Boolean, token.Type),
                () => Assert.Equal(value, token.Value)
            );
        }

        [Theory]
        [InlineData(" True ")]
        [InlineData(" False ")]
        [InlineData(" property ")]
        [InlineData(" _property ")]
        [InlineData(" property1 ")]
        [InlineData(" _property1 ")]
        [InlineData(" Eq ")]
        [InlineData(" Ne ")]
        [InlineData(" Lt ")]
        [InlineData(" Le ")]
        [InlineData(" Gt ")]
        [InlineData(" Ge ")]
        [InlineData(" Or ")]
        [InlineData(" And ")]
        [InlineData(" Not ")]
        public void Scan_WhenPassedIdentifierValue_DoesNotStoreValue(string filter)
        {
            var tokens = FilterScanner.Scan(filter);

            var token = Assert.Single(tokens);
            Assert.Multiple(
                () => Assert.Equal(FilterTokenType.Identifier, token.Type),
                () => Assert.Equal(filter.Trim(), token.Value)
            );
        }
    }
}