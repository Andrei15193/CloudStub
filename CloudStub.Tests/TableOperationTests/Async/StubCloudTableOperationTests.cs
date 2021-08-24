using System;
using System.Threading.Tasks;
using Xunit;

namespace CloudStub.Tests.TableOperationTests.Async
{
    public class StubCloudTableOperationTests : BaseStubCloudTableTests
    {
        [Fact]
        public async Task ExecuteAsync_WhenOperationIsNull_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<NullReferenceException>(() => CloudTable.ExecuteAsync(null));

            Assert.Equal(new NullReferenceException().Message, exception.Message);
        }
    }
}