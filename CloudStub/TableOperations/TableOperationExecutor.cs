using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;
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
                : _GetProperties(entity);
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

        protected static Exception ValidateEntityProperties(ITableEntity entity)
            => (from property in (entity is DynamicTableEntity dynamicTableEntity ? dynamicTableEntity.Properties : _GetProperties(entity))
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
            => (from property in (entity is DynamicTableEntity dynamicTableEntity ? dynamicTableEntity.Properties : _GetProperties(entity))
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

        private static IDictionary<string, EntityProperty> _GetProperties(ITableEntity entity)
        {
            var entityPropertyFactories = new Func<Type, object, EntityProperty>[]
            {
                (type, value) => type == typeof(byte[]) && value is byte[] byteArray ?  EntityProperty.GeneratePropertyForByteArray(byteArray) : null,
                (type, value) => type == typeof(bool) ? EntityProperty.GeneratePropertyForBool((bool)value) : null,
                (type, value) => type == typeof(bool?) && value is bool @bool ?  EntityProperty.GeneratePropertyForBool(@bool) : null,
                (type, value) => type == typeof(DateTime) && value is DateTime dateTime ?  EntityProperty.GeneratePropertyForDateTimeOffset(dateTime == default ? default(DateTimeOffset) : dateTime) : null,
                (type, value) => type == typeof(DateTime?) && value is DateTime dateTime ?  EntityProperty.GeneratePropertyForDateTimeOffset(dateTime == default ? default(DateTimeOffset) : dateTime) : null,
                (type, value) => type == typeof(DateTimeOffset) ?  EntityProperty.GeneratePropertyForDateTimeOffset((DateTimeOffset)value) : null,
                (type, value) => type == typeof(DateTimeOffset?) && value is DateTimeOffset dateTimeOffset ?  EntityProperty.GeneratePropertyForDateTimeOffset(dateTimeOffset) : null,
                (type, value) => type == typeof(int) ?  EntityProperty.GeneratePropertyForInt((int)value) : null,
                (type, value) => type == typeof(int?) && value is int @int ?  EntityProperty.GeneratePropertyForInt(@int) : null,
                (type, value) => type == typeof(long) ?  EntityProperty.GeneratePropertyForLong((long)value) : null,
                (type, value) => type == typeof(long?) && value is long @long ?  EntityProperty.GeneratePropertyForLong(@long) : null,
                (type, value) => type == typeof(double) ?  EntityProperty.GeneratePropertyForDouble((double)value) : null,
                (type, value) => type == typeof(double?) && value is double @double ?  EntityProperty.GeneratePropertyForDouble(@double) : null,
                (type, value) => type == typeof(Guid) ? EntityProperty.GeneratePropertyForGuid((Guid)value) : null,
                (type, value) => type == typeof(Guid?) && value is Guid guid ?  EntityProperty.GeneratePropertyForGuid(guid) : null,
                (type, value) => type == typeof(string) && value is string @string ?  EntityProperty.GeneratePropertyForString(@string) : null,
            };
            return (
                from property in entity.GetType().GetProperties()
                from entityPropertyFactory in entityPropertyFactories
                let entityProperty = entityPropertyFactory(property.PropertyType, property.GetValue(entity))
                where entityProperty is object
                select (Key: property.Name, Value: entityProperty)
            )
            .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
        }
    }
}