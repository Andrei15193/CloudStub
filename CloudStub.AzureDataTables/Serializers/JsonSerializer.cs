using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace CloudStub.AzureDataTables.Serializers
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

        internal static string SerializeEntities(string metadata, IEnumerable<IReadOnlyDictionary<string, object>> entities, IEnumerable<string> selectedProperties = null)
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

                    foreach (var property in entity.Where(property => selectedProperties?.Contains(property.Key, StringComparer.OrdinalIgnoreCase) ?? true))
                        if (property.Value == null)
                            jsonWriter.WriteNull(property.Key);
                        else if (property.Value is bool boolValue)
                            jsonWriter.WriteBoolean(property.Key, boolValue);
                        else if (property.Value is int intValue)
                            jsonWriter.WriteNumber(property.Key, intValue);
                        else if (property.Value is long longValue)
                        {
                            jsonWriter.WriteString(property.Key, longValue.ToString("0", CultureInfo.InvariantCulture));
                            jsonWriter.WriteString($"{property.Key}@odata.type", "Edm.Int64");
                        }
                        else if (property.Value is double doubleValue)
                            jsonWriter.WriteNumber(property.Key, doubleValue);
                        else if (property.Value is DateTime dateTime)
                        {
                            jsonWriter.WriteString(property.Key, dateTime.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFFZ"));
                            if (!property.Key.Equals("Timestamp", StringComparison.OrdinalIgnoreCase))
                                jsonWriter.WriteString($"{property.Key}@odata.type", "Edm.DateTime");
                        }
                        else if (property.Value is DateTimeOffset dateTimeValue)
                        {
                            jsonWriter.WriteString(property.Key, dateTimeValue.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFFZ"));
                            if (!property.Key.Equals("Timestamp", StringComparison.OrdinalIgnoreCase))
                                jsonWriter.WriteString($"{property.Key}@odata.type", "Edm.DateTime");
                        }
                        else if (property.Value is Guid guidValue)
                        {
                            jsonWriter.WriteString(property.Key, guidValue.ToString("D"));
                            jsonWriter.WriteString($"{property.Key}@odata.type", "Edm.Guid");
                        }
                        else if (property.Value is byte[] binaryValue)
                        {
                            jsonWriter.WriteString(property.Key, Convert.ToBase64String(binaryValue));
                            jsonWriter.WriteString($"{property.Key}@odata.type", "Edm.Binary");
                        }
                        else
                            jsonWriter.WriteString(property.Key, (string)property.Value);

                    if (selectedProperties != null)
                        foreach (var property in selectedProperties)
                            if (!entity.ContainsKey(property))
                                jsonWriter.WriteNull(property);

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