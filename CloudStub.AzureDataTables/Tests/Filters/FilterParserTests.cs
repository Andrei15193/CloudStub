using System.Collections.Generic;
using CloudStub.AzureDataTables.Filters;
using CloudStub.AzureDataTables.Filters.Nodes;
using Xunit;

namespace CloudStub.AzureDataTables.Tests.Filters
{
    public class FilterParserTests
    {
        private static readonly IReadOnlyDictionary<string, object> _testEntity = new Dictionary<string, object>
        {
            { "property1", 1 },
            { "boolProp", true },
            { "textProp", "text" }
        };

        [Theory]
        [InlineData("", true)]
        [InlineData("boolProp", true)]
        [InlineData("property1", false)]
        [InlineData("property2", false)]
        [InlineData("property2 eq true", false)]
        [InlineData("property2 eq false", false)]

        [InlineData("property1 eq 0", false)]
        [InlineData("property1 eq 1", true)]
        [InlineData("property1 eq 2", false)]
        [InlineData("property1 ne 0", true)]
        [InlineData("property1 ne 1", false)]
        [InlineData("property1 ne 2", true)]
        [InlineData("property1 lt 0", false)]
        [InlineData("property1 lt 1", false)]
        [InlineData("property1 lt 2", true)]
        [InlineData("property1 le 0", false)]
        [InlineData("property1 le 1", true)]
        [InlineData("property1 le 2", true)]
        [InlineData("property1 gt 0", true)]
        [InlineData("property1 gt 1", false)]
        [InlineData("property1 gt 2", false)]
        [InlineData("property1 ge 0", true)]
        [InlineData("property1 ge 1", true)]
        [InlineData("property1 ge 2", false)]

        [InlineData("0 eq property1", false)]
        [InlineData("1 eq property1", true)]
        [InlineData("2 eq property1", false)]
        [InlineData("0 ne property1", true)]
        [InlineData("1 ne property1", false)]
        [InlineData("2 ne property1", true)]
        [InlineData("0 lt property1", true)]
        [InlineData("1 lt property1", false)]
        [InlineData("2 lt property1", false)]
        [InlineData("0 le property1", true)]
        [InlineData("1 le property1", true)]
        [InlineData("2 le property1", false)]
        [InlineData("0 gt property1", false)]
        [InlineData("1 gt property1", false)]
        [InlineData("2 gt property1", true)]
        [InlineData("0 ge property1", false)]
        [InlineData("1 ge property1", true)]
        [InlineData("2 ge property1", true)]

        [InlineData("property1 eq '0'", false)]
        [InlineData("property1 eq '1'", false)]
        [InlineData("property1 eq '2'", false)]
        [InlineData("property1 ne '0'", false)]
        [InlineData("property1 ne '1'", false)]
        [InlineData("property1 ne '2'", false)]
        [InlineData("property1 lt '0'", false)]
        [InlineData("property1 lt '1'", false)]
        [InlineData("property1 lt '2'", false)]
        [InlineData("property1 le '0'", false)]
        [InlineData("property1 le '1'", false)]
        [InlineData("property1 le '2'", false)]
        [InlineData("property1 gt '0'", false)]
        [InlineData("property1 gt '1'", false)]
        [InlineData("property1 gt '2'", false)]
        [InlineData("property1 ge '0'", false)]
        [InlineData("property1 ge '1'", false)]
        [InlineData("property1 ge '2'", false)]

        [InlineData("'0' eq property1", false)]
        [InlineData("'1' eq property1", false)]
        [InlineData("'2' eq property1", false)]
        [InlineData("'0' ne property1", false)]
        [InlineData("'1' ne property1", false)]
        [InlineData("'2' ne property1", false)]
        [InlineData("'0' lt property1", false)]
        [InlineData("'1' lt property1", false)]
        [InlineData("'2' lt property1", false)]
        [InlineData("'0' le property1", false)]
        [InlineData("'1' le property1", false)]
        [InlineData("'2' le property1", false)]
        [InlineData("'0' gt property1", false)]
        [InlineData("'1' gt property1", false)]
        [InlineData("'2' gt property1", false)]
        [InlineData("'0' ge property1", false)]
        [InlineData("'1' ge property1", false)]
        [InlineData("'2' ge property1", false)]

        [InlineData("not property1 eq 1", false)]
        [InlineData("not property1 eq 2", true)]

        [InlineData("(property1 eq 1)", true)]
        [InlineData("noProp eq 1", false)]
        [InlineData("noProp eq 1 or property1 eq 2", false)]
        [InlineData("property1 eq 1 or property1 eq 1", true)]
        [InlineData("property1 eq 1 or property1 eq 2", true)]
        [InlineData("property1 eq 1 and property1 eq 1", true)]
        [InlineData("property1 eq 1 and property1 eq 2", false)]
        [InlineData("noProp eq 1 and property1 eq 1", false)]
        public void Parse_WhenParsingFilter_ProvidesApplicableFilterNode(string filterString, bool expectedResult)
        {
            var tokens = FilterScanner.Scan(filterString);
            var filter = FilterParser.Parse(tokens);

            Assert.Equal(expectedResult, filter.Apply(_testEntity));
        }

        [Theory]
        [InlineData("property1 eq property1", "The requested operation is not implemented on the specified resource.")]
        [InlineData("1 eq 1", "The requested operation is not implemented on the specified resource.")]
        [InlineData("and", "Syntax error at position 3 in 'and'.")]
        [InlineData("or", "Syntax error at position 2 in 'or'.")]
        [InlineData("not", "Syntax error at position 3 in 'not'.")]
        [InlineData("property1 eq 'test'error", "Syntax error at position 24 in 'property1 eq 'test'error'.")]
        public void Parse_WhenParsingInvalidFilter_ProvidesInvalidFilter(string filterString, string errorMEssage)
        {
            var tokens = FilterScanner.Scan(filterString);
            var filter = FilterParser.Parse(tokens);

            var invalidFilter = Assert.IsType<InvalidFilter>(filter);
            Assert.Equal(errorMEssage, invalidFilter.ErrorMessage);
        }
    }
}