using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Newtonsoft.Json;

namespace CloudStub
{
    internal class StubEntityJsonSerializer
    {
        public List<StubEntity> Deserialize(TextReader textReader)
        {
            var entites = new List<StubEntity>();

            using (var reader = new JsonTextReader(textReader)
            {
                Culture = CultureInfo.InvariantCulture,
                FloatParseHandling = FloatParseHandling.Double,
                DateParseHandling = DateParseHandling.None
            })
                if (reader.Read())
                    switch (reader.TokenType)
                    {
                        case JsonToken.StartArray:
                            while (reader.Read())
                                switch (reader.TokenType)
                                {
                                    case JsonToken.StartObject:
                                        var entity = _TryReadEntity(reader);
                                        if (entity is object)
                                            entites.Add(entity);
                                        break;

                                    case JsonToken.EndArray:
                                        break;

                                    default:
                                        throw _GetInvalidJsonException();
                                }
                            break;

                        default:
                            throw _GetInvalidJsonException();
                    }

            return entites;
        }

        public void Serialize(TextWriter textWriter, IEnumerable<StubEntity> entities)
        {
            using (var writer = new JsonTextWriter(textWriter)
            {
                Culture = CultureInfo.InvariantCulture,
                Formatting = Formatting.None
            })
            {
                writer.WriteStartArray();

                if (entities is object)
                    foreach (var entity in entities)
                        _WriteEntity(writer, entity);

                writer.WriteEndArray();
            }
        }

        private StubEntity _TryReadEntity(JsonReader reader)
        {
            var partitionKey = default(string);
            var rowKey = default(string);
            var etag = default(string);
            var timestamp = default(DateTimeOffset?);
            var entityProperties = new Dictionary<string, StubEntityProperty>(StringComparer.Ordinal);

            while (reader.Read() && reader.TokenType != JsonToken.EndObject)
            {
                switch (reader.TokenType)
                {
                    case JsonToken.PropertyName:
                        var propertyName = (string)reader.Value;
                        switch (propertyName)
                        {
                            case "partitionKey":
                                partitionKey = _ReadEntityKeyProperty(reader);
                                break;

                            case "rowKey":
                                rowKey = _ReadEntityKeyProperty(reader);
                                break;

                            case "etag":
                                etag = _ReadEntityETagProperty(reader);
                                break;

                            case "timestamp":
                                timestamp = _ReadEntityTimestampProperty(reader);
                                break;

                            default:
                                entityProperties[propertyName] = _ReadEntityProperty(reader);
                                break;
                        }
                        break;

                    default:
                        throw _GetInvalidJsonException();
                }
            }

            if (partitionKey is object && rowKey is object && timestamp is object && etag is object)
            {
                var entity = new StubEntity(partitionKey, rowKey, timestamp.Value, etag);
                foreach (var property in entityProperties)
                    entity.Properties.Add(property);
                return entity;
            }
            else
                return null;
        }

        private string _ReadEntityKeyProperty(JsonReader reader)
        {
            if (!reader.Read() || reader.TokenType != JsonToken.String)
                throw _GetInvalidJsonException();

            return (string)reader.Value;
        }

        private string _ReadEntityETagProperty(JsonReader reader)
        {
            if (!reader.Read() || reader.TokenType != JsonToken.String)
                throw _GetInvalidJsonException();

            return (string)reader.Value;
        }

        private DateTimeOffset _ReadEntityTimestampProperty(JsonReader reader)
        {
            if (!reader.Read() || reader.TokenType != JsonToken.String)
                throw _GetInvalidJsonException();

            try
            {
                return DateTimeOffset.ParseExact((string)reader.Value, "O", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
            }
            catch (FormatException formatException)
            {
                throw _GetInvalidJsonException(formatException);
            }
            catch (ArgumentOutOfRangeException argumentOutOfRangeException)
            {
                throw _GetInvalidJsonException(argumentOutOfRangeException);
            }
        }

        private StubEntityProperty _ReadEntityProperty(JsonReader reader)
        {
            if (!reader.Read() || reader.TokenType != JsonToken.StartObject)
                throw _GetInvalidJsonException();

            if (!reader.Read() || reader.TokenType != JsonToken.PropertyName || !string.Equals((string)reader.Value, "value", StringComparison.Ordinal))
                throw _GetInvalidJsonException();

            if (!reader.Read() || reader.TokenType != JsonToken.Integer && reader.TokenType != JsonToken.Float && reader.TokenType != JsonToken.Boolean && reader.TokenType != JsonToken.String)
                throw _GetInvalidJsonException();

            var value = reader.Value;

            if (!reader.Read() || reader.TokenType != JsonToken.PropertyName || !string.Equals((string)reader.Value, "type", StringComparison.Ordinal))
                throw _GetInvalidJsonException();

            if (!reader.Read() || reader.TokenType != JsonToken.String)
                throw _GetInvalidJsonException();

            var type = reader.Value;

            if (!reader.Read() || reader.TokenType != JsonToken.EndObject)
                throw _GetInvalidJsonException();

            try
            {
                switch (type)
                {
                    case "binary":
                        return new StubEntityProperty(Convert.FromBase64String((string)value));

                    case "boolean":
                        return new StubEntityProperty((bool)value);

                    case "int32":
                        return new StubEntityProperty((int)(long)value);

                    case "int64":
                        return new StubEntityProperty((long)value);

                    case "double":
                        return new StubEntityProperty((double)value);

                    case "dateTime":
                        return new StubEntityProperty(DateTimeOffset.ParseExact((string)value, "O", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal));

                    case "guid":
                        return new StubEntityProperty(Guid.ParseExact((string)value, "N"));

                    case "string":
                        return new StubEntityProperty((string)value);

                    default:
                        throw _GetInvalidJsonException();
                }
            }
            catch (InvalidCastException invalidCastException)
            {
                throw _GetInvalidJsonException(invalidCastException);
            }
            catch (FormatException formatException)
            {
                throw _GetInvalidJsonException(formatException);
            }
            catch (ArgumentOutOfRangeException argumentOutOfRangeException)
            {
                throw _GetInvalidJsonException(argumentOutOfRangeException);
            }
        }

        public void _WriteEntity(JsonWriter writer, StubEntity entity)
        {
            writer.WriteStartObject();

            writer.WritePropertyName("partitionKey");
            writer.WriteValue(entity.PartitionKey);
            writer.WritePropertyName("rowKey");
            writer.WriteValue(entity.RowKey);
            writer.WritePropertyName("etag");
            writer.WriteValue(entity.ETag);
            writer.WritePropertyName("timestamp");
            writer.WriteValue(entity.Timestamp?.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));

            if (entity.Properties is object)
                foreach (var pair in entity.Properties)
                {
                    var propertyName = pair.Key;
                    if (!string.Equals(propertyName, "partitionKey", StringComparison.Ordinal)
                        && !string.Equals(propertyName, "rowKey", StringComparison.Ordinal)
                        && !string.Equals(propertyName, "etag", StringComparison.Ordinal)
                        && !string.Equals(propertyName, "timestamp", StringComparison.Ordinal))
                    {
                        writer.WritePropertyName(propertyName);
                        _WriteEntityProperty(writer, pair.Value);
                    }
                }

            writer.WriteEndObject();
        }

        private void _WriteEntityProperty(JsonWriter writer, StubEntityProperty entityProperty)
        {
            writer.WriteStartObject();

            switch (entityProperty.Type)
            {
                case StubEntityPropertyType.Binary when entityProperty.Value is object:
                    writer.WritePropertyName("value");
                    writer.WriteValue(Convert.ToBase64String((byte[])entityProperty.Value));
                    writer.WritePropertyName("type");
                    writer.WriteValue("binary");
                    break;

                case StubEntityPropertyType.Boolean:
                    writer.WritePropertyName("value");
                    writer.WriteValue((bool)entityProperty.Value);
                    writer.WritePropertyName("type");
                    writer.WriteValue("boolean");
                    break;

                case StubEntityPropertyType.Int32:
                    writer.WritePropertyName("value");
                    writer.WriteValue((int)entityProperty.Value);
                    writer.WritePropertyName("type");
                    writer.WriteValue("int32");
                    break;

                case StubEntityPropertyType.Int64:
                    writer.WritePropertyName("value");
                    writer.WriteValue((long)entityProperty.Value);
                    writer.WritePropertyName("type");
                    writer.WriteValue("int64");
                    break;

                case StubEntityPropertyType.Double:
                    writer.WritePropertyName("value");
                    writer.WriteValue((double)entityProperty.Value);
                    writer.WritePropertyName("type");
                    writer.WriteValue("double");
                    break;

                case StubEntityPropertyType.DateTime:
                    writer.WritePropertyName("value");
                    writer.WriteValue(((DateTimeOffset)entityProperty.Value).ToString("O", CultureInfo.InvariantCulture));
                    writer.WritePropertyName("type");
                    writer.WriteValue("dateTime");
                    break;

                case StubEntityPropertyType.Guid:
                    writer.WritePropertyName("value");
                    writer.WriteValue(((Guid)entityProperty.Value).ToString("N"));
                    writer.WritePropertyName("type");
                    writer.WriteValue("guid");
                    break;

                case StubEntityPropertyType.String:
                    writer.WritePropertyName("value");
                    writer.WriteValue((string)entityProperty.Value);
                    writer.WritePropertyName("type");
                    writer.WriteValue("string");
                    break;
            }

            writer.WriteEndObject();
        }

        private static InvalidOperationException _GetInvalidJsonException()
            => _GetInvalidJsonException(null);

        private static InvalidOperationException _GetInvalidJsonException(Exception innerException)
            => new InvalidOperationException("The expected JSON is in an invalid format, please do not manually change the contents of these files unless you know what you are doing. For more information about the expected structure refer to the project site.", innerException);
    }
}