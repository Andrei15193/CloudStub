using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections;
using System.Threading.Tasks;
using Xunit;

namespace CloudStub.Tests
{
    public class InMemoryCloudTableTests : BaseInMemoryCloudTableTests
    {
        [Fact]
        public void TableName_GetsTheSameNameWhichWasProvided()
        {
            Assert.Equal(TestTableName, CloudTable.Name);
        }

        [Fact]
        public void CreateAsync_WhenTableNameIsNull_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentNullException>("tableName", () => GetCloudTable(null));
            Assert.Equal(new ArgumentNullException("tableName").Message, exception.Message);
        }

        [Fact]
        public void CreateAsync_WhenTableNameIsEmpty_ThrowsException()
        {
            var exception = Assert.Throws<ArgumentException>("tableName", () => GetCloudTable(""));
            Assert.Equal(new ArgumentException("The argument must not be empty string.", "tableName").Message, exception.Message);
        }

        [Fact]
        public async Task ExistsAsync_WhenTableDoesNotExist_ReturnsFalse()
        {
            Assert.False(await CloudTable.ExistsAsync(null, null));
        }

        [Fact]
        public async Task ExistsAsync_WhenTableExist_ReturnsTrue()
        {
            await CloudTable.CreateAsync(null, null);

            Assert.True(await CloudTable.ExistsAsync(null, null));
        }

        [Fact]
        public async Task CreateAsync_WhenTableDoesNotExist_CreatesTable()
        {
            await CloudTable.CreateAsync(null, null);

            Assert.True(await CloudTable.ExistsAsync(null, null));
        }

        [Fact]
        public async Task CreateAsync_WhenTableExists_ThrowsException()
        {
            await CloudTable.CreateAsync(null, null);

            var exception = await Assert.ThrowsAsync<StorageException>(() => CloudTable.CreateAsync(null, null));

            Assert.Equal("Conflict", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(409, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Conflict", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Equal("TableAlreadyExists", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The table specified already exists.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory]
        [InlineData("invalid_table_name")]
        [InlineData("1nvalid")]
        public async Task CreateAsync_WhenTableNameIsInvalid_ThrowsException(string tableName)
        {
            var cloudTable = GetCloudTable(tableName);

            var exception = await Assert.ThrowsAsync<StorageException>(() => cloudTable.CreateAsync(null, null));

            Assert.Equal("Bad Request", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Bad Request", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Equal("InvalidResourceName", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The specifed resource name contains invalid characters.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory]
        [InlineData("tables")]
        public async Task CreateAsync_WhenTableNameIsReserved_ThrowsException(string tableName)
        {
            var cloudTable = GetCloudTable(tableName);

            var exception = await Assert.ThrowsAsync<StorageException>(() => cloudTable.CreateAsync(null, null));

            Assert.Equal("Bad Request", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Bad Request", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Equal("InvalidInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^One of the request inputs is not valid.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory]
        [InlineData(" ")]
        [InlineData("t")]
        [InlineData("tt")]
        [InlineData("testTableNameHavingALengthOf63CharactersSomeOfThemAreJustExtra1s")]
        public async Task CreateAsync_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
        {
            var cloudTable = GetCloudTable(tableName);

            var exception = await Assert.ThrowsAsync<StorageException>(() => cloudTable.CreateAsync(null, null));

            Assert.Equal("Bad Request", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Bad Request", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Equal("OutOfRangeInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The specified resource name length is not within the permissible limits.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public async Task CreateIfNotExistsAsync_WhenTableDoesNotExist_ReturnsFalse()
        {
            Assert.True(await CloudTable.CreateIfNotExistsAsync(null, null));

            Assert.True(await CloudTable.ExistsAsync(null, null));
        }

        [Fact]
        public async Task CreateIfNotExistsAsync_WhenTableExists_ReturnsFalse()
        {
            await CloudTable.CreateAsync(null, null);

            Assert.False(await CloudTable.CreateIfNotExistsAsync(null, null));
        }

        [Fact]
        public async Task DeleteAsync_WhenTableDoesNotExist_ThrowsException()
        {
            var exception = await Assert.ThrowsAsync<StorageException>(() => CloudTable.DeleteAsync(null, null));

            Assert.Equal("Not Found", exception.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(404, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("StorageException", exception.RequestInformation.ExceptionInfo.Type);
            Assert.Equal("Not Found", exception.RequestInformation.ExceptionInfo.Message);
            Assert.Equal("Microsoft.WindowsAzure.Storage", exception.RequestInformation.ExceptionInfo.Source);
            Assert.Equal("ResourceNotFound", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The specified resource does not exist.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );
            Assert.Null(exception.RequestInformation.ExceptionInfo.InnerExceptionInfo);

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact]
        public async Task DeleteAsync_WhenTableExists_DeletesTable()
        {
            await CloudTable.CreateAsync(null, null);

            await CloudTable.DeleteAsync(null, null);

            Assert.False(await CloudTable.ExistsAsync(null, null));
        }

        [Fact]
        public async Task DeleteIfExistsAsync_WhenTableDoesNotExist_ReturnsFalse()
        {
            Assert.False(await CloudTable.DeleteIfExistsAsync(null, null));
        }

        [Fact]
        public async Task DeleteIfExistsAsync_WhenTableExists_ReturnsTrue()
        {
            await CloudTable.CreateAsync(null, null);

            Assert.True(await CloudTable.DeleteIfExistsAsync(null, null));
            Assert.False(await CloudTable.DeleteIfExistsAsync(null, null));
        }

        [Fact]
        public async Task CreateAsync_WhenTablePreviouslyContainedEntities_IsEmpty()
        {
            await CloudTable.CreateAsync();
            await CloudTable.ExecuteAsync(TableOperation.Insert(new TableEntity("partition-key", "row-key")));
            await CloudTable.DeleteAsync();

            if (!UseInMemory)
                await Task.Delay(TimeSpan.FromMinutes(1));

            await CloudTable.CreateAsync();

            var entities = await GetAllEntitiesAsync();
            Assert.Empty(entities);
        }

        [Fact]
        public async Task GetPermissionsAsync_WhenNotImplemented_ThrowsException()
        {
            await Assert.ThrowsAsync<NotImplementedException>(() => CloudTable.GetPermissionsAsync(null, null));
        }

        [Fact]
        public async Task SetPermissionsAsync_WhenNotImplemented_ThrowsException()
        {
            await Assert.ThrowsAsync<NotImplementedException>(() => CloudTable.SetPermissionsAsync(null, null, null));
        }
    }
}