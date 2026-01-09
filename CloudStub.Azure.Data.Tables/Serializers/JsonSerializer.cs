using System;
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
    }
}