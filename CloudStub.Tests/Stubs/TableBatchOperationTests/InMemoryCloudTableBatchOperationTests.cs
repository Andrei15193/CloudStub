using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;

namespace CloudStub.Tests.TableBatchOperationTests
{
    public class InMemoryCloudTableBatchOperationTests : BaseInMemoryCloudTableTests
    {
        [Fact]
        public async Task ExecuteBatchAsync_WhenBatchIsNull_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<ArgumentNullException>("batch", () => CloudTable.ExecuteBatchAsync(null));

            Assert.Equal(new ArgumentNullException("batch").Message, exception.Message);
        }
    }
}