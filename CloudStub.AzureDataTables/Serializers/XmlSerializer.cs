using System.IO;
using System.Text;
using System.Xml;
using Azure.Data.Tables.Models;

namespace CloudStub.AzureDataTables.Serializers
{
    internal static class XmlSeriaizer
    {
        public static string SerializeError(string errorCode, string errorDescription)
        {
            var stream = new MemoryStream();
            using (var xmlWriter = XmlWriter.Create(stream, new XmlWriterSettings { Encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false) }))
            {
                xmlWriter.WriteStartElement("m", "StorageServiceProperties", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata");

                xmlWriter.WriteStartElement("code", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata");
                xmlWriter.WriteString(errorCode);
                xmlWriter.WriteEndElement();

                xmlWriter.WriteStartElement("message", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata");
                xmlWriter.WriteStartAttribute("xml", "lang", null);
                xmlWriter.WriteString("en-US");
                xmlWriter.WriteEndAttribute();

                xmlWriter.WriteString(errorDescription);
                xmlWriter.WriteEndElement();

                xmlWriter.WriteEndElement();
            }

            stream.Seek(0L, SeekOrigin.Begin);
            using (var streamReader = new StreamReader(stream))
                return streamReader.ReadToEnd();
        }

        public static string Serialize(TableServiceProperties tableServiceProperties)
        {
            var stream = new MemoryStream();
            using (var xmlWriter = XmlWriter.Create(stream, new XmlWriterSettings { Encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false) }))
            {
                xmlWriter.WriteStartElement("StorageServiceProperties");

                if (tableServiceProperties.Logging != null)
                {
                    xmlWriter.WriteStartElement("Logging");

                    if (tableServiceProperties.Logging.Version != null)
                    {
                        xmlWriter.WriteStartElement("Version");
                        xmlWriter.WriteString(tableServiceProperties.Logging.Version);
                        xmlWriter.WriteEndElement();
                    }

                    xmlWriter.WriteStartElement("Delete");
                    xmlWriter.WriteString(tableServiceProperties.Logging.Delete ? "true" : "false");
                    xmlWriter.WriteEndElement();

                    xmlWriter.WriteStartElement("Read");
                    xmlWriter.WriteString(tableServiceProperties.Logging.Read ? "true" : "false");
                    xmlWriter.WriteEndElement();

                    xmlWriter.WriteStartElement("Write");
                    xmlWriter.WriteString(tableServiceProperties.Logging.Write ? "true" : "false");
                    xmlWriter.WriteEndElement();

                    if (tableServiceProperties.Logging.RetentionPolicy != null)
                    {
                        xmlWriter.WriteStartElement("RetentionPolicy");

                        xmlWriter.WriteStartElement("Enabled");
                        xmlWriter.WriteString(tableServiceProperties.Logging.RetentionPolicy.Enabled ? "true" : "false");
                        xmlWriter.WriteEndElement();

                        if (tableServiceProperties.Logging.RetentionPolicy.Days != null)
                        {
                            xmlWriter.WriteStartElement("Days");
                            xmlWriter.WriteString(tableServiceProperties.Logging.RetentionPolicy.Days?.ToString());
                            xmlWriter.WriteEndElement();
                        }

                        xmlWriter.WriteEndElement();
                    }

                    xmlWriter.WriteEndElement();
                }

                if (tableServiceProperties.HourMetrics != null)
                {
                    xmlWriter.WriteStartElement("HourMetrics");

                    if (tableServiceProperties.HourMetrics.Version != null)
                    {
                        xmlWriter.WriteStartElement("Version");
                        xmlWriter.WriteString(tableServiceProperties.HourMetrics.Version);
                        xmlWriter.WriteEndElement();
                    }

                    xmlWriter.WriteStartElement("Enabled");
                    xmlWriter.WriteString(tableServiceProperties.HourMetrics.Enabled ? "true" : "false");
                    xmlWriter.WriteEndElement();

                    if (tableServiceProperties.HourMetrics.IncludeApis.HasValue)
                    {
                        xmlWriter.WriteStartElement("IncludeApis");
                        xmlWriter.WriteString(tableServiceProperties.HourMetrics.IncludeApis.Value ? "true" : "false");
                        xmlWriter.WriteEndElement();
                    }

                    if (tableServiceProperties.HourMetrics.RetentionPolicy != null)
                    {
                        xmlWriter.WriteStartElement("RetentionPolicy");

                        xmlWriter.WriteStartElement("Enabled");
                        xmlWriter.WriteString(tableServiceProperties.HourMetrics.RetentionPolicy.Enabled ? "true" : "false");
                        xmlWriter.WriteEndElement();

                        if (tableServiceProperties.HourMetrics.RetentionPolicy.Days != null)
                        {
                            xmlWriter.WriteStartElement("Days");
                            xmlWriter.WriteString(tableServiceProperties.HourMetrics.RetentionPolicy.Days?.ToString());
                            xmlWriter.WriteEndElement();
                        }

                        xmlWriter.WriteEndElement();
                    }

                    xmlWriter.WriteEndElement();
                }

                if (tableServiceProperties.MinuteMetrics != null)
                {
                    xmlWriter.WriteStartElement("MinuteMetrics");

                    if (tableServiceProperties.MinuteMetrics.Version != null)
                    {
                        xmlWriter.WriteStartElement("Version");
                        xmlWriter.WriteString(tableServiceProperties.MinuteMetrics.Version);
                        xmlWriter.WriteEndElement();
                    }

                    xmlWriter.WriteStartElement("Enabled");
                    xmlWriter.WriteString(tableServiceProperties.MinuteMetrics.Enabled ? "true" : "false");
                    xmlWriter.WriteEndElement();

                    if (tableServiceProperties.MinuteMetrics.IncludeApis.HasValue)
                    {
                        xmlWriter.WriteStartElement("IncludeApis");
                        xmlWriter.WriteString(tableServiceProperties.MinuteMetrics.IncludeApis.Value ? "true" : "false");
                        xmlWriter.WriteEndElement();
                    }

                    if (tableServiceProperties.MinuteMetrics.RetentionPolicy != null)
                    {
                        xmlWriter.WriteStartElement("RetentionPolicy");

                        xmlWriter.WriteStartElement("Enabled");
                        xmlWriter.WriteString(tableServiceProperties.MinuteMetrics.RetentionPolicy.Enabled ? "true" : "false");
                        xmlWriter.WriteEndElement();

                        if (tableServiceProperties.MinuteMetrics.RetentionPolicy.Days != null)
                        {
                            xmlWriter.WriteStartElement("Days");
                            xmlWriter.WriteString(tableServiceProperties.MinuteMetrics.RetentionPolicy.Days?.ToString());
                            xmlWriter.WriteEndElement();
                        }

                        xmlWriter.WriteEndElement();
                    }

                    xmlWriter.WriteEndElement();
                }

                if (tableServiceProperties.Cors != null)
                {
                    xmlWriter.WriteStartElement("Cors");

                    foreach (var rule in tableServiceProperties.Cors)
                    {
                        xmlWriter.WriteStartElement("Rule");

                        if (rule.AllowedHeaders != null)
                        {
                            xmlWriter.WriteStartElement("AllowedHeaders");
                            xmlWriter.WriteString(rule.AllowedHeaders);
                            xmlWriter.WriteEndElement();
                        }

                        if (rule.AllowedMethods != null)
                        {
                            xmlWriter.WriteStartElement("AllowedMethods");
                            xmlWriter.WriteString(rule.AllowedMethods);
                            xmlWriter.WriteEndElement();
                        }

                        if (rule.AllowedOrigins != null)
                        {
                            xmlWriter.WriteStartElement("AllowedOrigins");
                            xmlWriter.WriteString(rule.AllowedOrigins);
                            xmlWriter.WriteEndElement();
                        }

                        if (rule.ExposedHeaders != null)
                        {
                            xmlWriter.WriteStartElement("ExposedHeaders");
                            xmlWriter.WriteString(rule.ExposedHeaders);
                            xmlWriter.WriteEndElement();
                        }

                        xmlWriter.WriteStartElement("MaxAgeInSeconds");
                        xmlWriter.WriteString(rule.MaxAgeInSeconds.ToString());
                        xmlWriter.WriteEndElement();

                        xmlWriter.WriteEndElement();
                    }

                    xmlWriter.WriteEndElement();
                }

                xmlWriter.WriteEndElement();
            }

            stream.Seek(0L, SeekOrigin.Begin);
            using (var streamReader = new StreamReader(stream))
                return streamReader.ReadToEnd();
        }
    }
}