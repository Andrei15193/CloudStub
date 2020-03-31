using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal sealed class InsertTableOperationExecutor : TableOperationExecutor
    {
        public InsertTableOperationExecutor(ITableOperationExecutorContext context)
            : base(context)
        {
        }

        public override Exception Validate(TableOperation tableOperation, OperationContext operationContext)
        {
            if (!Context.TableExists)
                return TableDoesNotExistException();

            if (tableOperation.Entity.PartitionKey == null)
                return PropertiesWithoutValueException();
            if (tableOperation.Entity.PartitionKey.Length > (1 << 10))
                return PropertyValueTooLargeException();
            if (!tableOperation.Entity.PartitionKey.All(IsValidKeyCharacter))
                return InvalidPartitionKeyException(tableOperation.Entity.PartitionKey);

            if (tableOperation.Entity.RowKey == null)
                return PropertiesWithoutValueException();
            if (tableOperation.Entity.RowKey.Length > (1 << 10))
                return PropertyValueTooLargeException();
            if (!tableOperation.Entity.RowKey.All(IsValidKeyCharacter))
                return InvalidRowKeyException(tableOperation.Entity.RowKey);

            var entityPropertyException = ValidateEntityProperties(tableOperation.Entity, operationContext);
            if (entityPropertyException != null)
                return entityPropertyException;

            if (Context.Entities.TryGetValue(tableOperation.Entity.PartitionKey, out var partition) && partition.ContainsKey(tableOperation.Entity.RowKey))
                return EntityAlreadyExistsException();

            return null;
        }

        public override Exception ValidateForBatch(TableOperation tableOperation, OperationContext operationContext, int operationIndex)
        {
            if (!Context.TableExists)
                return FromTemplate(
                    new StorageExceptionTemplate
                    {
                        HttpStatusCode = 404,
                        HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                        ErrorDetails =
                        {
                            Code = "TableNotFound",
                            Message = $"{operationIndex}:The table specified does not exist."
                        }
                    }
                );

            if (tableOperation.Entity.PartitionKey == null)
                return new ArgumentNullException("Upserts require a valid PartitionKey");
            if (tableOperation.Entity.PartitionKey.Length > (1 << 10))
                return FromTemplate(
                    new StorageExceptionTemplate
                    {
                        HttpStatusCode = 400,
                        HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                        ErrorDetails =
                        {
                            Code = "PropertyValueTooLarge",
                            Message = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less."
                        }
                    }
                );
            var partitionKeyException = _ValidateBatckKeyProperty(nameof(ITableEntity.PartitionKey), tableOperation.Entity.PartitionKey, operationIndex);
            if (partitionKeyException != null)
                return partitionKeyException;

            if (tableOperation.Entity.RowKey == null)
                return new ArgumentNullException("Upserts require a valid RowKey");
            if (tableOperation.Entity.RowKey.Length > (1 << 10))
                return FromTemplate(
                    new StorageExceptionTemplate
                    {
                        HttpStatusCode = 400,
                        HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                        ErrorDetails =
                        {
                            Code = "PropertyValueTooLarge",
                            Message = "The property value exceeds the maximum allowed size (64KB). If the property value is a string, it is UTF-16 encoded and the maximum number of characters should be 32K or less."
                        }
                    }
                );
            var rowKeyException = _ValidateBatckKeyProperty(nameof(ITableEntity.RowKey), tableOperation.Entity.RowKey, operationIndex);
            if (rowKeyException != null)
                return rowKeyException;

            var entityPropertyException = ValidateEntityPropertiesForBatch(tableOperation.Entity, operationContext, operationIndex);
            if (entityPropertyException != null)
                return entityPropertyException;

            if (Context.Entities.TryGetValue(tableOperation.Entity.PartitionKey, out var partition) && partition.ContainsKey(tableOperation.Entity.RowKey))
                return FromTemplate(
                    new StorageExceptionTemplate
                    {
                        HttpStatusCode = 409,
                        HttpStatusName = "The specified entity already exists.",
                        DetailedExceptionMessage = true,
                        ErrorDetails =
                        {
                            Code = "EntityAlreadyExists",
                            Message = "The specified entity already exists."
                        }
                    }
                );

            return null;
        }

        public override TableResult Execute(TableOperation tableOperation, OperationContext operationContext)
        {
            var dynamicEntity = GetDynamicEntity(tableOperation.Entity, operationContext);
            var partition = _GetPartition(dynamicEntity);
            partition.Add(dynamicEntity.RowKey, dynamicEntity);

            return new TableResult
            {
                HttpStatusCode = 204,
                Etag = dynamicEntity.ETag,
                Result = new TableEntity
                {
                    PartitionKey = dynamicEntity.PartitionKey,
                    RowKey = dynamicEntity.RowKey,
                    ETag = dynamicEntity.ETag,
                    Timestamp = dynamicEntity.Timestamp
                }
            };
        }

        private StorageException _ValidateBatckKeyProperty(string keyName, string value, int operationIndex)
            => value
                .Select(
                    @char =>
                    {
                        switch (@char)
                        {
                            case '/':
                            case '\\':
                                return FromTemplate(
                                    new StorageExceptionTemplate
                                    {
                                        HttpStatusCode = 400,
                                        HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                                        ErrorDetails =
                                        {
                                            Code = "OutOfRangeInput",
                                            Message = $"{operationIndex}:The '{keyName}' parameter of value '" + value + "' is out of range."
                                        }
                                    }
                                );

                            default:
                                return !IsValidKeyCharacter(@char)
                                    ? FromTemplate(
                                        new StorageExceptionTemplate
                                        {
                                            HttpStatusCode = 400,
                                            HttpStatusName = $"Element {operationIndex} in the batch returned an unexpected response code.",
                                            ErrorDetails =
                                            {
                                                Code = "OutOfRangeInput",
                                                Message = $"{operationIndex}:The '{keyName}' parameter of value '" + value + "' is out of range."
                                            }
                                        }
                                    )
                                    : null;
                        }
                    }
                )
                .FirstOrDefault(exception => exception != null);

        private IDictionary<string, DynamicTableEntity> _GetPartition(ITableEntity entity)
        {
            if (!Context.Entities.TryGetValue(entity.PartitionKey, out var entitiesByRowKey))
            {
                entitiesByRowKey = new SortedList<string, DynamicTableEntity>(StringComparer.Ordinal);
                Context.Entities.Add(entity.PartitionKey, entitiesByRowKey);
            }

            return entitiesByRowKey;
        }
    }
}