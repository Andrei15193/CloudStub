using System;
using System.Collections;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Xunit;

namespace CloudStub.Tests.BaseOperationTests
{
    public abstract class InMemoryCloudTableEntityEditOperationsTests : BaseInMemoryCloudTableOperationTests
    {
        [Fact]
        public virtual async Task ExecuteAsync_WhenTableDoesNotExist_ThrowsException()
        {
            var testEntity = new TestEntity
            {
                PartitionKey = "partition-key",
                RowKey = "row-key",
                ETag = "*"
            };

            var exception = await Assert.ThrowsAsync<StorageException>(() => ExecuteAsync(GetOperation(testEntity)));

            AssertExceptionMessageWithFallback("Not Found", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(404, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Equal(ExpectedErrorCode, exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            AssertExceptionMessageWithFallback("Not Found", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Equal("TableNotFound", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The table specified does not exist.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public void TableOperation_WhenEntityIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("entity", () => GetOperation(null));
            Assert.Equal(new ArgumentNullException("entity").Message, exception.Message);
        }

        protected virtual string ExpectedErrorMessagePattern
            => null;

        protected virtual string ExpectedErrorCode
            => null;

        protected void AssertExceptionMessageWithFallback(string expected, string message)
        {
            if (ExpectedErrorMessagePattern == null)
                Assert.Equal(expected, message);
            else
                Assert.Matches($"^{ExpectedErrorMessagePattern}$", message);
        }
    }
}