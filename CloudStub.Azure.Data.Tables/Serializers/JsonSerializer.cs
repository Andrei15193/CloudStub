using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace CloudStub.Azure.Data.Tables.Serializers
{
    internal static class JsonSeriaizer
    {
        public static string TableCreatedMetadata(Uri accountUri, string tableName)
        {
            var stream = new MemoryStream();
            using (var jsonWriter = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = false }))
            {
                jsonWriter.WriteStartObject();

                jsonWriter.WriteString("odata.metadata", $"{accountUri}$metadata#Tables/@Element");
                jsonWriter.WriteString("TableName", tableName);

                jsonWriter.WriteEndObject();
            }

            stream.Seek(0L, SeekOrigin.Begin);
            using (var streamReader = new StreamReader(stream))
                return streamReader.ReadToEnd();
        }

        public static string SerializeError(string errorCode, string errorDescription)
        {
            var stream = new MemoryStream();
            using (var jsonWriter = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = false }))
            {
                jsonWriter.WriteStartObject();

                jsonWriter.WritePropertyName("odata.error");
                jsonWriter.WriteStartObject();

                jsonWriter.WriteString("code", errorCode);

                jsonWriter.WritePropertyName("message");
                jsonWriter.WriteStartObject();
                jsonWriter.WriteString("lang", "en-US");
                jsonWriter.WriteString("value", errorDescription);
                jsonWriter.WriteEndObject();

                jsonWriter.WriteEndObject();


                jsonWriter.WriteEndObject();
            }

            stream.Seek(0L, SeekOrigin.Begin);
            using (var streamReader = new StreamReader(stream))
                return streamReader.ReadToEnd();
        }

        internal static string SerializeEntities(string metadata, IEnumerable<IReadOnlyDictionary<string, object>> entities)
        {
            var stream = new MemoryStream();
            using (var jsonWriter = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = false }))
            {
                jsonWriter.WriteStartObject();

                jsonWriter.WriteString("odata.metadata", metadata);
                jsonWriter.WritePropertyName("value");
                jsonWriter.WriteStartArray();

                foreach (var entity in entities)
                {
                    jsonWriter.WriteStartObject();

                    foreach (var property in entity)
                        if (property.Value == null)
                            jsonWriter.WriteNull(property.Key);
                        else if (property.Value is bool boolValue)
                            jsonWriter.WriteBoolean(property.Key, boolValue);
                        else if (property.Value is int intValue)
                            jsonWriter.WriteNumber(property.Key, intValue);
                        else if (property.Value is long longValue)
                            jsonWriter.WriteNumber(property.Key, longValue);
                        else if (property.Value is double doubleValue)
                            jsonWriter.WriteNumber(property.Key, doubleValue);
                        else if (property.Value is DateTimeOffset dateTimeValue)
                            jsonWriter.WriteString(property.Key, dateTimeValue.ToString("o"));
                        else if (property.Value is Guid guidValue)
                            jsonWriter.WriteString(property.Key, guidValue.ToString("D"));
                        else if (property.Value is byte[] binaryValue)
                            jsonWriter.WriteString(property.Key, Convert.ToBase64String(binaryValue));
                        else
                            jsonWriter.WriteString(property.Key, (string)property.Value);

                    jsonWriter.WriteEndObject();
                }

                jsonWriter.WriteEndArray();
                jsonWriter.WriteEndObject();
            }

            stream.Seek(0L, SeekOrigin.Begin);
            using (var streamReader = new StreamReader(stream))
                return streamReader.ReadToEnd();
        }
    }
}