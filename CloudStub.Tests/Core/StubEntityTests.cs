using System;
using CloudStub.Core;
using Xunit;

namespace CloudStub.Tests.Core
{
    public class StubEntityTests
    {
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData("\t")]
        [InlineData("\n")]
        [InlineData("\r")]
        public void Initialize_WhenPartitionKeyIsNull_ThrowsException(string partitionKey)
        {
            var exception = Assert.Throws<ArgumentException>("partitionKey", () => new StubEntity(partitionKey, "row-key"));
            Assert.Equal(new ArgumentException("The partition key cannot be null, empty or white space.", "partitionKey").Message, exception.Message);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData("\t")]
        [InlineData("\n")]
        [InlineData("\r")]
        public void Initialize_WhenRowKeyIsNull_ThrowsException(string rowKey)
        {
            var exception = Assert.Throws<ArgumentException>("rowKey", () => new StubEntity("partition-key", rowKey));
            Assert.Equal(new ArgumentException("The row key cannot be null, empty or white space.", "rowKey").Message, exception.Message);
        }
    }
}