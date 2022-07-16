using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using CloudStub.OperationResults;
using CloudStub.FilterParser;
using CloudStub.TableOperations;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub
{
    public class StubCloudTable : CloudTable
    {
        private const int DefaultPageSize = 1000;

        private static class TableQuerySegmentInfo
        {
            public static readonly ConstructorInfo TableQuerySegmentConstructor = typeof(TableQuerySegment<DynamicTableEntity>)
                .GetTypeInfo()
                .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.CreateInstance)
                .Single(constructor => constructor.GetParameters().Select(parameter => parameter.ParameterType).SequenceEqual(new[] { typeof(List<DynamicTableEntity>) }));
            public static readonly PropertyInfo ContinuationTokenProperty = typeof(TableQuerySegment<DynamicTableEntity>)
                .GetRuntimeProperty(nameof(TableQuerySegment<DynamicTableEntity>.ContinuationToken));
        }

        private static class TableQuerySegmentInfo<TElement>
        {
            public static ConstructorInfo TableQuerySegmentConstructor { get; } = typeof(TableQuerySegment<TElement>)
                .GetTypeInfo()
                .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.CreateInstance)
                .First(constructor => constructor.GetParameters().Select(parameter => parameter.ParameterType).SequenceEqual(new[] { typeof(List<TElement>) }));

            public static PropertyInfo ContinuationTokenProperty { get; } = typeof(TableQuerySegment<TElement>)
                .GetRuntimeProperty(nameof(TableQuerySegment<TElement>.ContinuationToken));
        }

        private readonly StubTable _stubTable;
        private readonly IReadOnlyDictionary<TableOperationType, TableOperationExecutor> _tableOperationExecutors;

        public StubCloudTable(StubTable stubTable)
            : base(new Uri($"https://unit.test/{stubTable.Name}"))
            => (_stubTable, _tableOperationExecutors) = (stubTable, new Dictionary<TableOperationType, TableOperationExecutor>
            {
                { TableOperationType.Insert, new InsertTableOperationExecutor(stubTable) },
                { TableOperationType.InsertOrReplace, new InsertOrReplaceTableOperationExecutor(stubTable) },
                { TableOperationType.InsertOrMerge, new InsertOrMergeTableOperationExecutor(stubTable) },
                { TableOperationType.Replace, new ReplaceTableOperationExecutor(stubTable) },
                { TableOperationType.Merge, new MergeTableOperationExecutor(stubTable) },
                { TableOperationType.Delete, new DeleteTableOperationExecutor(stubTable) },
                { TableOperationType.Retrieve, new RetrieveTableOperationExecutor(stubTable) }
            });

        public override void Create(IndexingMode? indexingMode, int? throughput = null, int? defaultTimeToLive = null)
            => Create(null, null, null, throughput, defaultTimeToLive);

        public override void Create(TableRequestOptions requestOptions = null, OperationContext operationContext = null, string serializedIndexingPolicy = null, int? throughput = null, int? defaultTimeToLive = null)
        {
            if (Name.Length < 3 || Name.Length > 63)
                throw InvalidTableNameLengthException();

            var reservedTableNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "tables" };
            if (reservedTableNames.Contains(Name))
                throw InvalidInputException();

            if (!Regex.IsMatch(Name, "^[A-Za-z][A-Za-z0-9]{2,62}$"))
                throw InvalidResourceNameException();

            var result = _stubTable.Create();
            switch (result)
            {
                case StubTableCreateResult.Success:
                    break;

                case StubTableCreateResult.TableAlreadyExists:
                    throw TableAlreadyExistsException();

                default:
                    throw new InvalidOperationException($"Operation result {result} not handled.");
            }
        }

        public override Task CreateAsync()
            => CreateAsync(null, null, null, null, null, CancellationToken.None);

        public override Task CreateAsync(CancellationToken cancellationToken)
            => CreateAsync(null, null, null, null, null, cancellationToken);

        public override Task CreateAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => CreateAsync(requestOptions, operationContext, null, null, null, CancellationToken.None);

        public override Task CreateAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => CreateAsync(requestOptions, operationContext, null, null, null, cancellationToken);

        public override Task CreateAsync(int? throughput, IndexingMode indexingMode, int? defaultTimeToLive, CancellationToken cancellationToken)
            => CreateAsync(null, null, null, throughput, defaultTimeToLive, cancellationToken);

        public override Task CreateAsync(int? throughput, string serializedIndexingPolicy, int? defaultTimeToLive, CancellationToken cancellationToken)
            => CreateAsync(null, null, serializedIndexingPolicy, throughput, defaultTimeToLive, cancellationToken);

        public override Task CreateAsync(TableRequestOptions requestOptions, OperationContext operationContext, string serializedIndexingPolicy, int? throughput, int? defaultTimeToLive, CancellationToken cancellationToken)
        {
            if (Name.Length < 3 || Name.Length > 63)
                return Task.FromException(InvalidTableNameLengthException());

            var reservedTableNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "tables" };
            if (reservedTableNames.Contains(Name))
                return Task.FromException(InvalidInputException());

            if (!Regex.IsMatch(Name, "^[A-Za-z][A-Za-z0-9]{2,62}$"))
                return Task.FromException(InvalidResourceNameException());

            var result = _stubTable.Create();
            switch (result)
            {
                case StubTableCreateResult.Success:
                    return Task.CompletedTask;

                case StubTableCreateResult.TableAlreadyExists:
                    return Task.FromException(TableAlreadyExistsException());

                default:
                    return Task.FromException(new InvalidOperationException($"Operation result {result} not handled."));
            }
        }

        public override bool CreateIfNotExists(IndexingMode indexingMode, int? throughput = null, int? defaultTimeToLive = null)
            => CreateIfNotExists(null, null, null, throughput, defaultTimeToLive);

        public override bool CreateIfNotExists(TableRequestOptions requestOptions = null, OperationContext operationContext = null, string serializedIndexingPolicy = null, int? throughput = null, int? defaultTimeToLive = null)
        {
            var result = _stubTable.Create();
            switch (result)
            {
                case StubTableCreateResult.Success:
                    return true;

                case StubTableCreateResult.TableAlreadyExists:
                    return false;

                default:
                    throw new InvalidOperationException($"Operation result {result} not handled.");
            }
        }

        public override Task<bool> CreateIfNotExistsAsync()
            => CreateIfNotExistsAsync(null, null, null, null, null, CancellationToken.None);

        public override Task<bool> CreateIfNotExistsAsync(CancellationToken cancellationToken)
            => CreateIfNotExistsAsync(null, null, null, null, null, cancellationToken);

        public override Task<bool> CreateIfNotExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => CreateIfNotExistsAsync(requestOptions, operationContext, null, null, null, CancellationToken.None);

        public override Task<bool> CreateIfNotExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => CreateIfNotExistsAsync(requestOptions, operationContext, null, null, null, cancellationToken);

        public override Task<bool> CreateIfNotExistsAsync(IndexingMode indexingMode, int? throughput, int? defaultTimeToLive, CancellationToken cancellationToken)
            => CreateIfNotExistsAsync(null, null, null, throughput, defaultTimeToLive, cancellationToken);

        public override Task<bool> CreateIfNotExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, string serializedIndexingPolicy, int? throughput, int? defaultTimeToLive, CancellationToken cancellationToken)
        {
            var result = _stubTable.Create();
            switch (result)
            {
                case StubTableCreateResult.Success:
                    return Task.FromResult(true);

                case StubTableCreateResult.TableAlreadyExists:
                    return Task.FromResult(false);

                default:
                    return Task.FromException<bool>(new InvalidOperationException($"Operation result {result} not handled."));
            }
        }

        public override Task<bool> ExistsAsync()
            => ExistsAsync(null, null, CancellationToken.None);

        public override Task<bool> ExistsAsync(CancellationToken cancellationToken)
            => ExistsAsync(null, null, cancellationToken);

        public override Task<bool> ExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => ExistsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<bool> ExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => Task.FromResult(_stubTable.Exists());

        public override void Delete(TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            var result = _stubTable.Delete();
            switch (result)
            {
                case StubTableDeleteResult.Success:
                    break;

                case StubTableDeleteResult.TableDoesNotExist:
                    throw ResourceNotFoundException();

                default:
                    throw new InvalidOperationException($"Operation result {result} not handled.");
            }
        }

        public override Task DeleteAsync()
            => DeleteAsync(null, null, CancellationToken.None);

        public override Task DeleteAsync(CancellationToken cancellationToken)
            => DeleteAsync(null, null, cancellationToken);

        public override Task DeleteAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => DeleteAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task DeleteAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            var result = _stubTable.Delete();
            switch (result)
            {
                case StubTableDeleteResult.Success:
                    return Task.CompletedTask;

                case StubTableDeleteResult.TableDoesNotExist:
                    return Task.FromException(ResourceNotFoundException());

                default:
                    return Task.FromException(new InvalidOperationException($"Operation result {result} not handled."));
            }
        }

        public override bool DeleteIfExists(TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            var result = _stubTable.Delete();
            switch (result)
            {
                case StubTableDeleteResult.Success:
                    return true;

                case StubTableDeleteResult.TableDoesNotExist:
                    return false;

                default:
                    throw new InvalidOperationException($"Operation result {result} not handled.");
            }
        }

        public override Task<bool> DeleteIfExistsAsync()
            => DeleteIfExistsAsync(null, null, CancellationToken.None);

        public override Task<bool> DeleteIfExistsAsync(CancellationToken cancellationToken)
            => DeleteIfExistsAsync(null, null, cancellationToken);

        public override Task<bool> DeleteIfExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => DeleteIfExistsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<bool> DeleteIfExistsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            var result = _stubTable.Delete();
            switch (result)
            {
                case StubTableDeleteResult.Success:
                    return Task.FromResult(true);

                case StubTableDeleteResult.TableDoesNotExist:
                    return Task.FromResult(false);

                default:
                    return Task.FromException<bool>(new InvalidOperationException($"Operation result {result} not handled."));
            }
        }

        public override TableResult Execute(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            if (operation is null)
                throw new ArgumentNullException(nameof(operation));

            if (_tableOperationExecutors.TryGetValue(operation.OperationType, out var tableOperationExecutor))
                return tableOperationExecutor.Execute(operation, operationContext);
            else
                throw new InvalidOperationException($"Operation type {operation.OperationType} not handled.");
        }

        public override Task<TableResult> ExecuteAsync(TableOperation operation)
            => ExecuteAsync(operation, null, null, CancellationToken.None);

        public override Task<TableResult> ExecuteAsync(TableOperation operation, CancellationToken cancellationToken)
            => ExecuteAsync(operation, null, null, cancellationToken);

        public override Task<TableResult> ExecuteAsync(TableOperation operation, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteAsync(operation, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableResult> ExecuteAsync(TableOperation operation, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            if (_tableOperationExecutors.TryGetValue(operation.OperationType, out var tableOperationExecutor))
                try
                {
                    return Task.FromResult(tableOperationExecutor.Execute(operation, operationContext));
                }
                catch (StorageException storageException)
                {
                    return Task.FromException<TableResult>(storageException);
                }
                catch (InvalidOperationException invalidOperationException)
                {
                    return Task.FromException<TableResult>(invalidOperationException);
                }
                catch (ArgumentException argumentException)
                {
                    return Task.FromException<TableResult>(argumentException);
                }
            else
                throw new NotImplementedException($"Operation type {operation.OperationType} not handled.");
        }

        public override TableBatchResult ExecuteBatch(TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            if (batch == null)
                throw new ArgumentNullException(nameof(batch));

            var batchOperationException = _ValidateBatchOperation(batch);
            if (batchOperationException is object)
                throw batchOperationException;

            var stubTableBatchOperation = _stubTable.BatchOperation();
            var callbacks = batch
                .Select((operation, operationIndex) =>
                {
                    if (_tableOperationExecutors.TryGetValue(operation.OperationType, out var operationExecutor))
                        return operationExecutor.BatchCallback(stubTableBatchOperation, operation, operationContext, operationIndex);
                    else
                        throw new NotImplementedException($"Operation type {operation.OperationType} not handled.");
                })
                .ToList();
            var result = stubTableBatchOperation.Execute();

            switch (result.OperationResult)
            {
                case StubTableBatchOperationResult.Success:
                case StubTableBatchOperationResult.Failed:
                    var tableBatchResult = new TableBatchResult();
                    tableBatchResult.AddRange(callbacks.Select((callback, callbackIndex) => callback(result.IndividualOperationResults[callbackIndex])));
                    return tableBatchResult;

                case StubTableBatchOperationResult.TableDoesNotExist when batch.All(tableOperation => tableOperation.OperationType == TableOperationType.Retrieve):
                    return new TableBatchResult
                    {
                        new TableResult
                        {
                            HttpStatusCode = 404,
                            Etag = null,
                            Result = null
                        }
                    };
                case StubTableBatchOperationResult.TableDoesNotExist when batch.First().OperationType == TableOperationType.Insert:
                    throw TableDoesNotExistForBatchInsertException(0);
                case StubTableBatchOperationResult.TableDoesNotExist:
                    throw TableDoesNotExistForBatchException(0);

                default:
                    throw new NotImplementedException($"Operation type {result.OperationResult} not handled.");
            }
        }

        public override Task<TableBatchResult> ExecuteBatchAsync(TableBatchOperation batch)
            => ExecuteBatchAsync(batch, null, null, CancellationToken.None);

        public override Task<TableBatchResult> ExecuteBatchAsync(TableBatchOperation batch, CancellationToken cancellationToken)
            => ExecuteBatchAsync(batch, null, null, cancellationToken);

        public override Task<TableBatchResult> ExecuteBatchAsync(TableBatchOperation batch, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteBatchAsync(batch, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableBatchResult> ExecuteBatchAsync(TableBatchOperation batch, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            if (batch == null)
                throw new ArgumentNullException(nameof(batch));

            var batchOperationException = _ValidateBatchOperation(batch);
            if (batchOperationException is object)
                return Task.FromException<TableBatchResult>(batchOperationException);

            var stubTableBatchOperation = _stubTable.BatchOperation();
            var callbacks = batch
                .Select((operation, operationIndex) =>
                {
                    if (_tableOperationExecutors.TryGetValue(operation.OperationType, out var operationExecutor))
                        return operationExecutor.BatchCallback(stubTableBatchOperation, operation, operationContext, operationIndex);
                    else
                        throw new NotImplementedException($"Operation type {operation.OperationType} not handled.");
                })
                .ToList();
            var result = stubTableBatchOperation.Execute();

            switch (result.OperationResult)
            {
                case StubTableBatchOperationResult.Success:
                case StubTableBatchOperationResult.Failed:
                    var tableBatchResult = new TableBatchResult();
                    tableBatchResult.AddRange(callbacks.Select((callback, callbackIndex) => callback(result.IndividualOperationResults[callbackIndex])));
                    return Task.FromResult(tableBatchResult);

                case StubTableBatchOperationResult.TableDoesNotExist when batch.All(tableOperation => tableOperation.OperationType == TableOperationType.Retrieve):
                    return Task.FromResult(new TableBatchResult
                    {
                        new TableResult
                        {
                            HttpStatusCode = 404,
                            Etag = null,
                            Result = null
                        }
                    });
                case StubTableBatchOperationResult.TableDoesNotExist when batch.First().OperationType == TableOperationType.Insert:
                    return Task.FromException<TableBatchResult>(TableDoesNotExistForBatchInsertException(0));
                case StubTableBatchOperationResult.TableDoesNotExist:
                    return Task.FromException<TableBatchResult>(TableDoesNotExistForBatchException(0));

                default:
                    throw new NotImplementedException($"Operation type {result.OperationResult} not handled.");
            }
        }

        public override IEnumerable<DynamicTableEntity> ExecuteQuery(TableQuery query, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            var stubTableQuery = new StubTableQuery
            {
                Filter = _GetFilter(query.FilterString),
                PageSize = query.TakeCount ?? DefaultPageSize,
                SelectedProperties = query.SelectColumns
            };
            var continuationToken = default(StubTableQueryContinuationToken);

            var entities = new List<DynamicTableEntity>();
            do
            {
                var result = _stubTable.Query(stubTableQuery, continuationToken);
                switch (result.OperationResult)
                {
                    case StubTableQueryResult.Success:
                        continuationToken = result.ContinuationToken;
                        entities.AddRange(result.Entities.Select(stubEntity =>
                        {
                            var dynamicEntity = _GetDynamicEntity(stubEntity);
                            _EnsurePropertiesExist(dynamicEntity.Properties, query.SelectColumns);
                            return dynamicEntity;
                        }));
                        break;

                    default:
                        throw new NotImplementedException($"Operation type {result.OperationResult} not handled.");

                }
            } while (continuationToken is object && (stubTableQuery.PageSize is null || entities.Count < stubTableQuery.PageSize));
            if (stubTableQuery.PageSize is object && entities.Count > stubTableQuery.PageSize)
                entities.RemoveRange(stubTableQuery.PageSize.Value, entities.Count - stubTableQuery.PageSize.Value);

            return entities;
        }

        public override IEnumerable<TResult> ExecuteQuery<TResult>(TableQuery query, EntityResolver<TResult> resolver, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            TResult GetConcreteEntity(StubEntity stubEntity) =>
                resolver(
                    stubEntity.PartitionKey,
                    stubEntity.RowKey,
                    stubEntity.Timestamp.Value,
                    _EnsurePropertiesExist(_GetEntityProperties(stubEntity.Properties), query.SelectColumns),
                    stubEntity.ETag
                );

            var stubTableQuery = new StubTableQuery
            {
                Filter = _GetFilter(query.FilterString),
                PageSize = query.TakeCount ?? DefaultPageSize,
                SelectedProperties = query.SelectColumns
            };
            var continuationToken = default(StubTableQueryContinuationToken);

            var entities = new List<TResult>();
            do
            {
                var result = _stubTable.Query(stubTableQuery, continuationToken);
                switch (result.OperationResult)
                {
                    case StubTableQueryResult.Success:
                        continuationToken = result.ContinuationToken;
                        entities.AddRange(result.Entities.Select(GetConcreteEntity));
                        break;

                    default:
                        throw new NotImplementedException($"Operation type {result.OperationResult} not handled.");
                }
            } while (continuationToken is object);

            return entities;
        }

        public override IEnumerable<TElement> ExecuteQuery<TElement>(TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            => ExecuteQuery(
                new TableQuery().Where(query.FilterString).Take(query.TakeCount).Select(query.SelectColumns),
                TableOperation.Retrieve<TElement>(string.Empty, string.Empty).GetEntityResolver<TElement>(),
                requestOptions,
                operationContext
            );

        public override IEnumerable<TResult> ExecuteQuery<TElement, TResult>(TableQuery<TElement> query, EntityResolver<TResult> resolver, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            => ExecuteQuery(
                new TableQuery().Where(query.FilterString).Take(query.TakeCount).Select(query.SelectColumns),
                resolver,
                requestOptions,
                operationContext
            );

        public override TableQuerySegment<DynamicTableEntity> ExecuteQuerySegmented(TableQuery query, TableContinuationToken token, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            var stubTableQuery = new StubTableQuery
            {
                Filter = _GetFilter(query.FilterString),
                PageSize = query.TakeCount ?? DefaultPageSize,
                SelectedProperties = query.SelectColumns
            };
            var continuationToken = token is null ? default : new StubTableQueryContinuationToken(token.NextPartitionKey, token.NextRowKey);

            var result = _stubTable.Query(stubTableQuery, continuationToken);
            switch (result.OperationResult)
            {
                case StubTableQueryResult.Success:
                    var resultSegment = (TableQuerySegment<DynamicTableEntity>)TableQuerySegmentInfo.TableQuerySegmentConstructor.Invoke(
                        new[]
                        {
                            result
                                .Entities
                                .Select(stubEntity =>
                                {
                                    var dynamicEntity =_GetDynamicEntity(stubEntity);
                                    _EnsurePropertiesExist(dynamicEntity.Properties, query.SelectColumns);
                                    return dynamicEntity;
                                })
                                .ToList()
                        }
                    );

                    if (result.ContinuationToken is object)
                        TableQuerySegmentInfo.ContinuationTokenProperty.SetValue(resultSegment, new TableContinuationToken
                        {
                            NextPartitionKey = result.ContinuationToken.LastPartitionKey,
                            NextRowKey = result.ContinuationToken.LastRowKey,
                            TargetLocation = StorageLocation.Primary
                        });

                    return resultSegment;

                default:
                    throw new NotImplementedException($"Operation type {result.OperationResult} not handled.");
            }
        }

        public override TableQuerySegment<TElement> ExecuteQuerySegmented<TElement>(TableQuery<TElement> query, TableContinuationToken token, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            => ExecuteQuerySegmented(
                new TableQuery().Where(query.FilterString).Take(query.TakeCount).Select(query.SelectColumns),
                TableOperation.Retrieve<TElement>(string.Empty, string.Empty).GetEntityResolver<TElement>(),
                token,
                requestOptions,
                operationContext
            );

        public override TableQuerySegment<TResult> ExecuteQuerySegmented<TResult>(TableQuery query, EntityResolver<TResult> resolver, TableContinuationToken token, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            TResult GetConcreteEntity(StubEntity stubEntity) =>
                resolver(
                    stubEntity.PartitionKey,
                    stubEntity.RowKey,
                    stubEntity.Timestamp.Value,
                    _EnsurePropertiesExist(_GetEntityProperties(stubEntity.Properties), query.SelectColumns),
                    stubEntity.ETag
                );

            var stubTableQuery = new StubTableQuery
            {
                Filter = _GetFilter(query.FilterString),
                PageSize = query.TakeCount ?? DefaultPageSize,
                SelectedProperties = query.SelectColumns
            };
            var continuationToken = token is null ? default : new StubTableQueryContinuationToken(token.NextPartitionKey, token.NextRowKey);

            var result = _stubTable.Query(stubTableQuery, continuationToken);
            switch (result.OperationResult)
            {
                case StubTableQueryResult.Success:
                    var resultSegment = (TableQuerySegment<TResult>)TableQuerySegmentInfo<TResult>.TableQuerySegmentConstructor.Invoke(new[] { result.Entities.Select(GetConcreteEntity).ToList() });

                    if (result.ContinuationToken is object)
                        TableQuerySegmentInfo<TResult>.ContinuationTokenProperty.SetValue(resultSegment, new TableContinuationToken
                        {
                            NextPartitionKey = result.ContinuationToken.LastPartitionKey,
                            NextRowKey = result.ContinuationToken.LastRowKey,
                            TargetLocation = StorageLocation.Primary
                        });

                    return resultSegment;

                default:
                    throw new NotImplementedException($"Operation type {result.OperationResult} not handled.");
            }
        }

        public override TableQuerySegment<TResult> ExecuteQuerySegmented<TElement, TResult>(TableQuery<TElement> query, EntityResolver<TResult> resolver, TableContinuationToken token, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            => ExecuteQuerySegmented(
                new TableQuery().Where(query.FilterString).Take(query.TakeCount).Select(query.SelectColumns),
                resolver,
                token,
                requestOptions,
                operationContext
            );

        public override Task<TableQuerySegment<DynamicTableEntity>> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token)
            => ExecuteQuerySegmentedAsync(query, token, null, null, CancellationToken.None);

        public override Task<TableQuerySegment<DynamicTableEntity>> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token, CancellationToken cancellationToken)
            => ExecuteQuerySegmentedAsync(query, token, null, null, cancellationToken);

        public override Task<TableQuerySegment<DynamicTableEntity>> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteQuerySegmentedAsync(query, token, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableQuerySegment<DynamicTableEntity>> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            var stubTableQuery = new StubTableQuery
            {
                Filter = _GetFilter(query.FilterString),
                PageSize = query.TakeCount ?? DefaultPageSize,
                SelectedProperties = query.SelectColumns
            };
            var continuationToken = token is null ? default : new StubTableQueryContinuationToken(token.NextPartitionKey, token.NextRowKey);

            var result = _stubTable.Query(stubTableQuery, continuationToken);
            switch (result.OperationResult)
            {
                case StubTableQueryResult.Success:
                    var resultSegment = (TableQuerySegment<DynamicTableEntity>)TableQuerySegmentInfo.TableQuerySegmentConstructor.Invoke(
                        new[]
                        {
                            result
                                .Entities
                                .Select(stubEntity =>
                                {
                                    var dynamicEntity = _GetDynamicEntity(stubEntity);
                                    _EnsurePropertiesExist(dynamicEntity.Properties, query.SelectColumns);
                                    return dynamicEntity;
                                })
                                .ToList()
                        }
                    );

                    if (result.ContinuationToken is object)
                        TableQuerySegmentInfo.ContinuationTokenProperty.SetValue(resultSegment, new TableContinuationToken
                        {
                            NextPartitionKey = result.ContinuationToken.LastPartitionKey,
                            NextRowKey = result.ContinuationToken.LastRowKey,
                            TargetLocation = StorageLocation.Primary
                        });

                    return Task.FromResult(resultSegment);

                default:
                    throw new NotImplementedException($"Operation type {result.OperationResult} not handled.");
            }
        }

        public override Task<TableQuerySegment<TElement>> ExecuteQuerySegmentedAsync<TElement>(TableQuery<TElement> query, TableContinuationToken token)
            => ExecuteQuerySegmentedAsync(query, token, null, null, CancellationToken.None);

        public override Task<TableQuerySegment<TElement>> ExecuteQuerySegmentedAsync<TElement>(TableQuery<TElement> query, TableContinuationToken token, CancellationToken cancellationToken)
            => ExecuteQuerySegmentedAsync(query, token, null, null, cancellationToken);

        public override Task<TableQuerySegment<TElement>> ExecuteQuerySegmentedAsync<TElement>(TableQuery<TElement> query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteQuerySegmentedAsync(query, token, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableQuerySegment<TElement>> ExecuteQuerySegmentedAsync<TElement>(TableQuery<TElement> query, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => ExecuteQuerySegmentedAsync(
                new TableQuery().Where(query.FilterString).Take(query.TakeCount).Select(query.SelectColumns),
                TableOperation.Retrieve<TElement>(string.Empty, string.Empty).GetEntityResolver<TElement>(),
                token,
                requestOptions,
                operationContext,
                cancellationToken
            );

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TResult>(TableQuery query, EntityResolver<TResult> resolver, TableContinuationToken token)
            => ExecuteQuerySegmentedAsync(query, resolver, token, null, null, CancellationToken.None);

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TResult>(TableQuery query, EntityResolver<TResult> resolver, TableContinuationToken token, CancellationToken cancellationToken)
            => ExecuteQuerySegmentedAsync(query, resolver, token, null, null, cancellationToken);

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TResult>(TableQuery query, EntityResolver<TResult> resolver, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteQuerySegmentedAsync(query, resolver, token, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TResult>(TableQuery query, EntityResolver<TResult> resolver, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            TResult GetConcreteEntity(StubEntity stubEntity) =>
                resolver(
                    stubEntity.PartitionKey,
                    stubEntity.RowKey,
                    stubEntity.Timestamp.Value,
                    _EnsurePropertiesExist(_GetEntityProperties(stubEntity.Properties), query.SelectColumns),
                    stubEntity.ETag
                );

            var stubTableQuery = new StubTableQuery
            {
                Filter = _GetFilter(query.FilterString),
                PageSize = query.TakeCount ?? DefaultPageSize,
                SelectedProperties = query.SelectColumns
            };
            var continuationToken = token is null ? default : new StubTableQueryContinuationToken(token.NextPartitionKey, token.NextRowKey);

            var result = _stubTable.Query(stubTableQuery, continuationToken);
            switch (result.OperationResult)
            {
                case StubTableQueryResult.Success:
                    var resultSegment = (TableQuerySegment<TResult>)TableQuerySegmentInfo<TResult>.TableQuerySegmentConstructor.Invoke(new[] { result.Entities.Select(GetConcreteEntity).ToList() });

                    if (result.ContinuationToken is object)
                        TableQuerySegmentInfo<TResult>.ContinuationTokenProperty.SetValue(resultSegment, new TableContinuationToken
                        {
                            NextPartitionKey = result.ContinuationToken.LastPartitionKey,
                            NextRowKey = result.ContinuationToken.LastRowKey,
                            TargetLocation = StorageLocation.Primary
                        });

                    return Task.FromResult(resultSegment);

                default:
                    throw new NotImplementedException($"Operation type {result.OperationResult} not handled.");
            }
        }

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TElement, TResult>(TableQuery<TElement> query, EntityResolver<TResult> resolver, TableContinuationToken token)
            => ExecuteQuerySegmentedAsync(query, resolver, token, null, null, CancellationToken.None);

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TElement, TResult>(TableQuery<TElement> query, EntityResolver<TResult> resolver, TableContinuationToken token, CancellationToken cancellationToken)
            => ExecuteQuerySegmentedAsync(query, resolver, token, null, null, cancellationToken);

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TElement, TResult>(TableQuery<TElement> query, EntityResolver<TResult> resolver, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext)
            => ExecuteQuerySegmentedAsync(query, resolver, token, requestOptions, operationContext, CancellationToken.None);

        public override Task<TableQuerySegment<TResult>> ExecuteQuerySegmentedAsync<TElement, TResult>(TableQuery<TElement> query, EntityResolver<TResult> resolver, TableContinuationToken token, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => ExecuteQuerySegmentedAsync(
                new TableQuery().Where(query.FilterString).Take(query.TakeCount).Select(query.SelectColumns),
                resolver,
                token,
                requestOptions,
                operationContext,
                cancellationToken
            );

        public override TablePermissions GetPermissions(TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            => throw new NotImplementedException();

        public override Task<TablePermissions> GetPermissionsAsync()
            => GetPermissionsAsync(null, null, CancellationToken.None);

        public override Task<TablePermissions> GetPermissionsAsync(CancellationToken cancellationToken)
            => GetPermissionsAsync(null, null, cancellationToken);

        public override Task<TablePermissions> GetPermissionsAsync(TableRequestOptions requestOptions, OperationContext operationContext)
            => GetPermissionsAsync(requestOptions, operationContext, CancellationToken.None);

        public override Task<TablePermissions> GetPermissionsAsync(TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => throw new NotImplementedException();

        public override void SetPermissions(TablePermissions permissions, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            => throw new NotImplementedException();

        public override Task SetPermissionsAsync(TablePermissions permissions)
            => SetPermissionsAsync(permissions, null, null, CancellationToken.None);

        public override Task SetPermissionsAsync(TablePermissions permissions, CancellationToken cancellationToken)
            => SetPermissionsAsync(permissions, null, null, cancellationToken);

        public override Task SetPermissionsAsync(TablePermissions permissions, TableRequestOptions requestOptions, OperationContext operationContext)
            => SetPermissionsAsync(permissions, requestOptions, operationContext, CancellationToken.None);

        public override Task SetPermissionsAsync(TablePermissions permissions, TableRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
            => throw new NotImplementedException();

        private static DynamicTableEntity _GetDynamicEntity(StubEntity stubEntity)
            => new DynamicTableEntity
            {
                PartitionKey = stubEntity.PartitionKey,
                RowKey = stubEntity.RowKey,
                ETag = stubEntity.ETag,
                Timestamp = stubEntity.Timestamp.Value,
                Properties = _GetEntityProperties(stubEntity.Properties)
            };

        private static IDictionary<string, EntityProperty> _GetEntityProperties(IEnumerable<KeyValuePair<string, StubEntityProperty>> properties)
        {
            return properties.ToDictionary(pair => pair.Key, pair => _GetEntityProperty(pair.Value), StringComparer.Ordinal);

            EntityProperty _GetEntityProperty(StubEntityProperty property)
            {
                switch (property.Type)
                {
                    case StubEntityPropertyType.Binary:
                        return EntityProperty.GeneratePropertyForByteArray((byte[])property.Value);

                    case StubEntityPropertyType.Boolean:
                        return EntityProperty.GeneratePropertyForBool((bool)property.Value);

                    case StubEntityPropertyType.Int32:
                        return EntityProperty.GeneratePropertyForInt((int)property.Value);

                    case StubEntityPropertyType.Int64:
                        return EntityProperty.GeneratePropertyForLong((long)property.Value);

                    case StubEntityPropertyType.Double:
                        return EntityProperty.GeneratePropertyForDouble((double)property.Value);

                    case StubEntityPropertyType.Guid:
                        return EntityProperty.GeneratePropertyForGuid((Guid)property.Value);

                    case StubEntityPropertyType.DateTime:
                        return EntityProperty.GeneratePropertyForDateTimeOffset((DateTimeOffset)property.Value);

                    case StubEntityPropertyType.String:
                        return EntityProperty.GeneratePropertyForString((string)property.Value);

                    default:
                        throw new NotImplementedException($"Operation type {property.Type} not handled.");
                }
            }
        }

        private static IDictionary<string, EntityProperty> _EnsurePropertiesExist(IDictionary<string, EntityProperty> properties, IEnumerable<string> selectedProperties)
        {
            if (selectedProperties is object)
                foreach (var selectedProperty in selectedProperties)
                    if (!properties.ContainsKey(selectedProperty))
                        properties.Add(selectedProperty, EntityProperty.CreateEntityPropertyFromObject(null));
            return properties;
        }

        private static Exception _ValidateBatchOperation(TableBatchOperation batch)
        {
            if (batch.Count == 0)
                return new InvalidOperationException("Cannot execute an empty batch operation");
            if (batch.Count > 100)
                return new InvalidOperationException("The maximum number of operations allowed in one batch has been exceeded.");
            if (batch.Count > 1 && batch.Where(operation => operation.OperationType == TableOperationType.Retrieve).Skip(1).Any())
                return new ArgumentException("A batch transaction with a retrieve operation cannot contain any other operations.");
            if (batch.GroupBy(operation => operation.GetPartitionKey()).Skip(1).Any())
                return new ArgumentException("All entities in a given batch must have the same partition key.");

            int? duplicateIndex = null;
            var operationIndex = 0;
            var addedRowKeys = new HashSet<string>(StringComparer.Ordinal);
            while (operationIndex < batch.Count && duplicateIndex == null)
                if (batch[operationIndex].Entity != null && !addedRowKeys.Add(batch[operationIndex].Entity.RowKey))
                    duplicateIndex = operationIndex;
                else
                    operationIndex++;
            if (duplicateIndex != null)
                return MultipleOperationsChangeSameEntityException(duplicateIndex.Value);

            return null;
        }

        private static Func<StubEntity, bool> _GetFilter(string filterString)
        {
            var scanner = new FilterTokenScanner();
            var tokens = scanner.Scan(filterString ?? string.Empty);
            var parser = new FilterTokenParser();
            return parser.Parse(tokens);
        }
    }
}