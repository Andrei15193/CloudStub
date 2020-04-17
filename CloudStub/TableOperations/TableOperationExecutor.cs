﻿using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal abstract class TableOperationExecutor
    {
        protected TableOperationExecutor(ITableOperationExecutorContext context)
            => Context = context;

        protected ITableOperationExecutorContext Context { get; }

        public abstract Exception Validate(TableOperation tableOperation, OperationContext operationContext);

        public abstract Exception ValidateForBatch(TableOperation tableOperation, OperationContext operationContext, int operationIndex);

        public abstract TableResult Execute(TableOperation tableOperation, OperationContext operationContext);

        protected static DynamicTableEntity GetDynamicEntity(ITableEntity entity, OperationContext operationContext)
        {
            var timestamp = DateTimeOffset.UtcNow;
            var properties = entity is DynamicTableEntity dynamicTableEntity
                ? dynamicTableEntity.Clone().Properties
                : TableEntity.Flatten(entity, operationContext);
            properties.Remove(nameof(TableEntity.PartitionKey));
            properties.Remove(nameof(TableEntity.RowKey));
            properties.Remove(nameof(TableEntity.Timestamp));
            properties.Remove(nameof(TableEntity.ETag));

            return new DynamicTableEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = $"{timestamp:o}-{Guid.NewGuid()}",
                Timestamp = timestamp,
                Properties = properties
            };
        }

        protected static bool IsValidKeyCharacter(char @char)
            => !char.IsControl(@char)
                && @char != '/'
                && @char != '\\'
                && @char != '#'
                && @char != '?'
                && @char != '\t'
                && @char != '\n'
                && @char != '\r';

        protected static Exception ValidateEntityProperties(ITableEntity entity, OperationContext operationContext)
            => (from property in (entity is DynamicTableEntity dynamicTableEntity ? dynamicTableEntity.Properties : TableEntity.Flatten(entity, operationContext))
                where property.Key != nameof(TableEntity.PartitionKey)
                    && property.Key != nameof(TableEntity.RowKey)
                    && property.Key != nameof(TableEntity.Timestamp)
                    && property.Key != nameof(TableEntity.ETag)
                let exception = _ValidateEntityProperty(property.Key, property.Value)
                where exception != null
                select exception
            ).FirstOrDefault();

        protected StorageException ValidateKeyProperty(string value)
            => value
                .Select(
                    @char =>
                    {
                        switch (@char)
                        {
                            case '/':
                            case '\\':
                                return ErrorInQuerySyntaxException();

                            case '\u007F':
                            case '\u0081':
                            case '\u008D':
                            case '\u008F':
                            case '\u0090':
                            case '\u009D':
                                return BadRequestException();

                            default:
                                if ('\u0000' <= @char && @char <= '\u001F')
                                    return BadRequestException();

                                return !IsValidKeyCharacter(@char) ? InputOutOfRangeException() : null;
                        }
                    }
                )
                .FirstOrDefault(exception => exception != null);

        protected StorageException ValidateBatckKeyProperty(string value, int operationIndex)
            => value
                .Select(
                    @char =>
                    {
                        switch (@char)
                        {
                            case '/':
                            case '\\':
                                return BadRequestForBatchException(operationIndex);

                            default:
                                return !IsValidKeyCharacter(@char) ? InputOutOfRangeForBatchException(operationIndex) : null;
                        }
                    }
                )
                .FirstOrDefault(exception => exception != null);

        private static StorageException _ValidateEntityProperty(string name, EntityProperty property)
        {
            switch (property.PropertyType)
            {
                case EdmType.String when property.StringValue.Length > (1 << 15):
                case EdmType.Binary when property.BinaryValue.Length > (1 << 16):
                    return PropertyValueTooLargeException();

                case EdmType.DateTime when property.DateTime != null && property.DateTime < new DateTime(1601, 1, 1, 0, 0, 0, DateTimeKind.Utc):
                    return InvalidDateTimePropertyException(name, property.DateTime.Value);

                default:
                    return null;
            }
        }

        protected static Exception ValidateEntityPropertiesForBatch(ITableEntity entity, OperationContext operationContext, int operationIndex)
            => (from property in (entity is DynamicTableEntity dynamicTableEntity ? dynamicTableEntity.Properties : TableEntity.Flatten(entity, operationContext))
                where property.Key != nameof(TableEntity.PartitionKey)
                    && property.Key != nameof(TableEntity.RowKey)
                    && property.Key != nameof(TableEntity.Timestamp)
                    && property.Key != nameof(TableEntity.ETag)
                let exception = _ValidateEntityPropertyForBatch(property.Key, property.Value, operationIndex)
                where exception != null
                select exception
            ).FirstOrDefault();

        private static StorageException _ValidateEntityPropertyForBatch(string name, EntityProperty property, int operationIndex)
        {
            switch (property.PropertyType)
            {
                case EdmType.String when property.StringValue.Length > (1 << 15):
                case EdmType.Binary when property.BinaryValue.Length > (1 << 16):
                    return PropertyValueTooLargeForBatchException(operationIndex);

                case EdmType.DateTime when property.DateTime != null && property.DateTime < new DateTime(1601, 1, 1, 0, 0, 0, DateTimeKind.Utc):
                    return InvalidDateTimePropertyForBatchException(name, property.DateTime.Value, operationIndex);

                default:
                    return null;
            }
        }
    }
}