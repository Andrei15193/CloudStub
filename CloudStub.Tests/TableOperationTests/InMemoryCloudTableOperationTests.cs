using System;
using System.Threading.Tasks;
using Xunit;

namespace CloudStub.Tests.TableOperationTests
{
    public class InMemoryCloudTableOperationTests : BaseInMemoryCloudTableTests
    {
        [Fact]
        public async Task ExecuteAsync_WhenOperationIsNull_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<ArgumentNullException>("operation", () => CloudTable.ExecuteAsync(null));

            Assert.Equal(new ArgumentNullException("operation").Message, exception.Message);
        }
    }
}