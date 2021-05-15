using System.IO;

namespace CloudStub.Core
{
    public interface ITableStorageHandler
    {
        bool Exists(string tableName);

        TextReader GetTextReader(string tableName);

        TextWriter GetTextWriter(string tableName);

        void Delete(string tableName);
    }
}