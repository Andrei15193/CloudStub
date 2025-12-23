using System.Threading;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;

namespace CloudStub.Azure.Data.Tables
{
    public class TableClientStub : TableClient
    {
        private readonly TableServiceClientStub _tableServiceClientStub;
        private readonly string _tableName;

        public TableClientStub(TableServiceClientStub tableServiceClientStub, string tableName)
            : base()
        {
            _tableServiceClientStub = tableServiceClientStub;
            _tableName = tableName;
        }

        public override string Name
            => _tableName;

        public override Response<TableItem> Create(CancellationToken cancellationToken = default)
            => _tableServiceClientStub.CreateTable(_tableName, cancellationToken);
    }
}