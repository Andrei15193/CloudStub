using System;
using Xunit;

namespace CloudStub.Tests.TableOperationTests.Sync
{
    public class InMemoryCloudTableOperationTests : BaseInMemoryCloudTableTests
    {
        [Fact]
        public void Execute_WhenOperationIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("operation", () => CloudTable.Execute(null));

            Assert.Equal(new ArgumentNullException("operation").Message, exception.Message);
        }
    }
}