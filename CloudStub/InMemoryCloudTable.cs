using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub
{
    public class InMemoryCloudTable : CloudTable
    {
        private static IReadOnlyCollection<string> _reservedTableNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "tables"
        };
        private const int _tableDoesNotExistState = 0;
        private const int _tableExistsState = 1;
        private int _tableState;
        private readonly IDictionary<string, IDictionary<string, ITableEntity>> _items;

        public InMemoryCloudTable(string tableName)
            : base(new Uri($"https://unit.test/{tableName}"))
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));
            else if (tableName.Length == 0)
                throw new ArgumentException("The argument must not be empty string.", nameof(tableName));

            _tableState = _tableDoesNotExistState;
            _items = new SortedList<string, IDictionary<string, ITableEntity>>(StringComparer.Ordinal);
        }

        public override Task CreateAsync()
        {
            if (_reservedTableNames.Contains(Name))
                throw InvalidInputException();
            if (!Regex.IsMatch(Name, "^[A-Za-z][A-Za-z0-9]{2,62}$"))
                throw InvalidResourceNameException();
            if (Interlocked.CompareExchange(ref _tableState, _tableExistsState, _tableDoesNotExistState) == _tableExistsState)
                throw TableAlreadyExistsException();

            return Task.CompletedTask;
        }

        public override Task CreateAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => CreateAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task CreateAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => throw new NotImplementedException();

        public override Task<bool> ExistsAsync()
            => Task.FromResult(_tableState == _tableExistsState);

        public override Task<bool> ExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => ExistsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<bool> ExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => throw new NotImplementedException();

        public override Task DeleteAsync()
        {
            if (Interlocked.CompareExchange(ref _tableState, _tableDoesNotExistState, _tableExistsState) == _tableDoesNotExistState)
                throw ResourceNotFoundException();

            return Task.CompletedTask;
        }

        public override Task DeleteAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => DeleteAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task DeleteAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => throw new NotImplementedException();

        public override Task<bool> DeleteIfExistsAsync()
        {
            var result = Task.FromResult(Interlocked.CompareExchange(ref _tableState, _tableDoesNotExistState, _tableExistsState) == _tableExistsState);
            return result;
        }

        public override Task<bool> DeleteIfExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => DeleteIfExistsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<bool> DeleteIfExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => throw new NotImplementedException();

        public override Task<bool> CreateIfNotExistsAsync()
        {
            var result = Task.FromResult(Interlocked.CompareExchange(ref _tableState, _tableExistsState, _tableDoesNotExistState) == _tableDoesNotExistState);
            return result;
        }

        public override Task<bool> CreateIfNotExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => CreateIfNotExistsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<bool> CreateIfNotExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => throw new NotImplementedException();
    }
}