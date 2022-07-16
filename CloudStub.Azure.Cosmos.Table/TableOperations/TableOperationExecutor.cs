using System;
using System.Collections.Generic;
using System.Linq;
using CloudStub.OperationResults;
using Microsoft.Azure.Cosmos.Table;
using static CloudStub.StorageExceptionFactory;

namespace CloudStub.TableOperations
{
    internal abstract class TableOperationExecutor
    {
        protected TableOperationExecutor(StubTable stubTable)
            => StubTable = stubTable;

        protected StubTable StubTable { get; }

        public abstract TableResult Execute(TableOperation tableOperation, OperationContext operationContext);

        public abstract Func<IStubTableOperationDataResult, TableResult> BatchCallback(StubTableBatchOperation batchOperation, TableOperation tableOperation, OperationContext operationContext, int operationIndex);

        protected static StubEntity GetStubEntity(ITableEntity entity)
        {
            var timestamp = DateTimeOffset.UtcNow;
            var properties = entity is DynamicTableEntity dynamicTableEntity
                ? dynamicTableEntity.Properties.Where(pair => pair.Value.PropertyAsObject is object).ToDictionary(pair => pair.Key, pair => EntityProperty.CreateEntityPropertyFromObject(pair.Value.PropertyAsObject), StringComparer.Ordinal)
                : _GetProperties(entity);
            properties.Remove(nameof(TableEntity.PartitionKey));
            properties.Remove(nameof(TableEntity.RowKey));
            properties.Remove(nameof(TableEntity.Timestamp));
            properties.Remove(nameof(TableEntity.ETag));

            var stubEntity = new StubEntity(entity.PartitionKey, entity.RowKey, entity.ETag);
            foreach (var property in properties)
                switch (property.Value.PropertyType)
                {
                    case EdmType.Binary:
                        stubEntity.Properties.Add(property.Key, new StubEntityProperty(property.Value.BinaryValue));
                        break;

                    case EdmType.Boolean:
                        stubEntity.Properties.Add(property.Key, new StubEntityProperty(property.Value.BooleanValue.Value));
                        break;

                    case EdmType.Int32:
                        stubEntity.Properties.Add(property.Key, new StubEntityProperty(property.Value.Int32Value.Value));
                        break;

                    case EdmType.Int64:
                        stubEntity.Properties.Add(property.Key, new StubEntityProperty(property.Value.Int64Value.Value));
                        break;

                    case EdmType.Double:
                        stubEntity.Properties.Add(property.Key, new StubEntityProperty(property.Value.DoubleValue.Value));
                        break;

                    case EdmType.Guid:
                        stubEntity.Properties.Add(property.Key, new StubEntityProperty(property.Value.GuidValue.Value));
                        break;

                    case EdmType.DateTime:
                        stubEntity.Properties.Add(property.Key, new StubEntityProperty(property.Value.DateTimeOffsetValue.Value));
                        break;

                    case EdmType.String:
                        stubEntity.Properties.Add(property.Key, new StubEntityProperty(property.Value.StringValue));
                        break;
                }

            return stubEntity;
        }

        protected static IDictionary<string, EntityProperty> GetEntityProperties(IEnumerable<KeyValuePair<string, StubEntityProperty>> stubEntityProperties)
        {
            var properties = new Dictionary<string, EntityProperty>(StringComparer.Ordinal);

            foreach (var stubEntityProperty in stubEntityProperties)
            {
                switch (stubEntityProperty.Value.Type)
                {
                    case StubEntityPropertyType.Binary:
                        properties.Add(stubEntityProperty.Key, new EntityProperty((byte[])stubEntityProperty.Value.Value));
                        break;

                    case StubEntityPropertyType.Boolean:
                        properties.Add(stubEntityProperty.Key, new EntityProperty((bool)stubEntityProperty.Value.Value));
                        break;

                    case StubEntityPropertyType.Int32:
                        properties.Add(stubEntityProperty.Key, new EntityProperty((int)stubEntityProperty.Value.Value));
                        break;

                    case StubEntityPropertyType.Int64:
                        properties.Add(stubEntityProperty.Key, new EntityProperty((long)stubEntityProperty.Value.Value));
                        break;

                    case StubEntityPropertyType.Double:
                        properties.Add(stubEntityProperty.Key, new EntityProperty((double)stubEntityProperty.Value.Value));
                        break;

                    case StubEntityPropertyType.Guid:
                        properties.Add(stubEntityProperty.Key, new EntityProperty((Guid)stubEntityProperty.Value.Value));
                        break;

                    case StubEntityPropertyType.DateTime:
                        properties.Add(stubEntityProperty.Key, new EntityProperty((DateTimeOffset)stubEntityProperty.Value.Value));
                        break;

                    case StubEntityPropertyType.String:
                        properties.Add(stubEntityProperty.Key, new EntityProperty((string)stubEntityProperty.Value.Value));
                        break;
                }
            }

            return properties;
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
                    && property.Value.PropertyAsObject != null
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

                case EdmType.DateTime when property.DateTimeOffsetValue != null && property.DateTimeOffsetValue.Value < new DateTimeOffset(1601, 1, 1, 0, 0, 0, TimeSpan.Zero):
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
                    && property.Value.PropertyAsObject != null
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

                case EdmType.DateTime when property.DateTimeOffsetValue != null && property.DateTimeOffsetValue < new DateTimeOffset(1601, 1, 1, 0, 0, 0, TimeSpan.Zero):
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