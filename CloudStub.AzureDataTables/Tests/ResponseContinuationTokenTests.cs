using System;
using Xunit;

namespace CloudStub.AzureDataTables.Tests
{
    public class ResponseContinuationTokenTests
    {
        [Theory]
        [InlineData("00", "00,01,02,03", 0)]
        [InlineData("01", "00,01,02,03", 1)]
        [InlineData("02", "00,01,02,03", 2)]
        [InlineData("03", "00,01,02,03", 3)]
        [InlineData("04", "00,01,02,03", 4)]
        [InlineData("00", "00,02", 0)]
        [InlineData("01", "00,02", 1)]
        [InlineData("02", "00,02", 1)]
        [InlineData("03", "00,02", 2)]
        [InlineData("00", "00,02,05", 0)]
        [InlineData("01", "00,02,05", 1)]
        [InlineData("02", "00,02,05", 1)]
        [InlineData("03", "00,02,05", 2)]
        [InlineData("04", "00,02,05", 2)]
        [InlineData("05", "00,02,05", 2)]
        [InlineData("06", "00,02,05", 3)]
        public void SuccessorSearch(string target, string items, int expectedIndex)
        {
            var sortedItems = items.Split(',');

            var index = ResponseContinuationToken.SuccessorSearch(sortedItems, target, StringComparer.Ordinal);

            Assert.Equal(expectedIndex, index);
        }
    }
}