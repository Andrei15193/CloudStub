using System;
using System.Collections;
using System.Threading;
using Microsoft.Azure.Cosmos.Table;
using Xunit;

namespace CloudStub.Tests.TableTests.Sync
{
    public class StubCloudTableTests : BaseStubCloudTableTests
    {
        [Fact]
        public void TableName_GetsTheSameNameWhichWasProvided()
        {
            Assert.Equal(TestTableName, CloudTable.Name);
        }

        [Fact(Skip = "CloudTable.Exists cannot be overridden.")]
        public void Exists_WhenTableDoesNotExist_ReturnsFalse()
        {
            Assert.False(CloudTable.Exists(null, null));
        }

        [Fact(Skip = "CloudTable.Exists cannot be overridden.")]
        public void Exists_WhenTableExist_ReturnsTrue()
        {
            CloudTable.Create(null, null, null, null, null);

            Assert.True(CloudTable.Exists(null, null));
        }

        [Fact(Skip = "CloudTable.Exists cannot be overridden.")]
        public void Create_WhenTableDoesNotExist_CreatesTable()
        {
            CloudTable.Create(null, null, null, null, null);

            Assert.True(CloudTable.Exists(null, null));
        }

        [Fact]
        public void Create_WhenTableExists_ThrowsException()
        {
            CloudTable.Create(null, null, null, null, null);

            var exception = Assert.Throws<StorageException>(() => CloudTable.Create(null, null, null, null, null));

            Assert.Equal("Conflict", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(409, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("TableAlreadyExists", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The table specified already exists.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory]
        [InlineData("invalid_table_name")]
        [InlineData("1nvalid")]
        public void Create_WhenTableNameIsInvalid_ThrowsException(string tableName)
        {
            var cloudTable = GetCloudTable(tableName);

            var exception = Assert.Throws<StorageException>(() => cloudTable.Create(null, null, null, null, null));

            Assert.Equal("The remote server returned an error: (400) Bad Request.", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("InvalidResourceName", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The specifed resource name contains invalid characters.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory]
        [InlineData("tables")]
        public void Create_WhenTableNameIsReserved_ThrowsException(string tableName)
        {
            var cloudTable = GetCloudTable(tableName);

            var exception = Assert.Throws<StorageException>(() => cloudTable.Create(null, null, null, null, null));

            Assert.Equal("The remote server returned an error: (400) Bad Request.", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("InvalidInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^One of the request inputs is not valid.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Theory]
        [InlineData("t")]
        [InlineData("tt")]
        [InlineData("testTableNameHavingALengthOf63CharactersSomeOfThemAreJustExtra1s")]
        public void Create_WhenTableNameHasInvalidLength_ThrowsException(string tableName)
        {
            var cloudTable = GetCloudTable(tableName);

            var exception = Assert.Throws<StorageException>(() => cloudTable.Create(null, null, null, null, null));

            Assert.Equal("The remote server returned an error: (400) Bad Request.", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(400, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("OutOfRangeInput", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The specified resource name length is not within the permissible limits.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact(Skip = "CloudTable.Exists cannot be overridden.")]
        public void CreateIfNotExists_WhenTableDoesNotExist_ReturnsFalse()
        {
            Assert.True(CloudTable.CreateIfNotExists(null, null));

            Assert.True(CloudTable.Exists(null, null));
        }

        [Fact]
        public void CreateIfNotExists_WhenTableExists_ReturnsFalse()
        {
            CloudTable.Create(null, null, null, null, null);

            Assert.False(CloudTable.CreateIfNotExists(null, null));
        }

        [Fact]
        public void Delete_WhenTableDoesNotExist_ThrowsException()
        {
            var exception = Assert.Throws<StorageException>(() => CloudTable.Delete(null, null));

            Assert.Equal("Not Found", exception.Message);
            Assert.Equal("Microsoft.Azure.Cosmos.Table", exception.Source);
            Assert.Null(exception.HelpLink);
            Assert.Equal(-2146233088, exception.HResult);
            Assert.Null(exception.InnerException);
            Assert.IsAssignableFrom<IDictionary>(exception.Data);

            Assert.Equal(404, exception.RequestInformation.HttpStatusCode);
            Assert.Null(exception.RequestInformation.ContentMd5);
            Assert.Empty(exception.RequestInformation.ErrorCode);
            Assert.Null(exception.RequestInformation.Etag);

            Assert.Equal("ResourceNotFound", exception.RequestInformation.ExtendedErrorInformation.ErrorCode);
            Assert.Matches(
                @$"^The specified resource does not exist.\nRequestId:{exception.RequestInformation.ServiceRequestID}\nTime:\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}.\d{{7}}Z$",
                exception.RequestInformation.ExtendedErrorInformation.ErrorMessage
            );

            Assert.Same(exception, exception.RequestInformation.Exception);
        }

        [Fact(Skip = "CloudTable.Exists cannot be overridden.")]
        public void Delete_WhenTableExists_DeletesTable()
        {
            CloudTable.Create(null, null, null, null, null);

            CloudTable.Delete(null, null);

            Assert.False(CloudTable.Exists(null, null));
        }

        [Fact]
        public void DeleteIfExists_WhenTableDoesNotExist_ReturnsFalse()
        {
            Assert.False(CloudTable.DeleteIfExists(null, null));
        }

        [Fact]
        public void DeleteIfExists_WhenTableExists_ReturnsTrue()
        {
            CloudTable.Create(null, null, null, null, null);

            Assert.True(CloudTable.DeleteIfExists(null, null));
            Assert.False(CloudTable.DeleteIfExists(null, null));
        }

        [Fact]
        public void Create_WhenTablePreviouslyContainedEntities_IsEmpty()
        {
            CloudTable.Create();
            CloudTable.Execute(TableOperation.Insert(new TableEntity("partition-key", "row-key")));
            CloudTable.Delete();

            if (!UseInMemory)
                Thread.Sleep(TimeSpan.FromMinutes(1));

            CloudTable.Create();

            var entities = GetAllEntities();
            Assert.Empty(entities);
        }

        [Fact]
        public void GetPermissions_WhenNotImplemented_ThrowsException()
        {
            Assert.Throws<NotImplementedException>(() => CloudTable.GetPermissions(null, null));
        }

        [Fact]
        public void SetPermissions_WhenNotImplemented_ThrowsException()
        {
            Assert.Throws<NotImplementedException>(() => CloudTable.SetPermissions(null, null, null));
        }
    }
}