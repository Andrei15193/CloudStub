using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using Azure;
using Azure.Core;

namespace CloudStub.AzureDataTables
{
    internal class ResponseStub : Response
    {
        /// <remarks>
        /// <para>
        /// Unfortunately the sanitizer property is not exposed in any way and implicitly the default one is used.
        /// This does not work as several HTTP headers are redacted when added to the error message of an exception.
        /// </para>
        /// <para>
        /// This works around the issue as it references the related property and creates a sanitizer with similar
        /// settings so it can be easily set and produce the same outcome.
        /// </para>
        /// </remarks>
        private static readonly PropertyInfo _sanitizerProperty = typeof(Response).GetProperty("Sanitizer", BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.SetProperty);
        private static readonly object _responseSanitizer = Activator.CreateInstance(
            _sanitizerProperty.PropertyType,
            new object[]
            {
                Array.Empty<string>(),
                new string[]
                {
                    "Cache-Control",
                    "Transfer-Encoding",
                    "Server",
                    "x-ms-request-id",
                    "x-ms-client-request-id",
                    "Date",
                    "Content-Type",
                    "Content-Length"
                },
                "REDACTED"
            }
        );

        private readonly MemoryStream _contentStream;
        private readonly IReadOnlyDictionary<string, string> _headers;

        public ResponseStub(HttpStatusCode statusCode, string reasonPhrase, string content, ResponseHeaders headers)
        {
            Status = (int)statusCode;
            ReasonPhrase = reasonPhrase;
            ClientRequestId = headers.ClientRequestId.ToString("D", CultureInfo.InvariantCulture);
            _headers = headers;

            _contentStream = new MemoryStream();
            using (var streamWriter = new StreamWriter(_contentStream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1024 * 10, leaveOpen: true))
                streamWriter.Write(content);
            _contentStream.Seek(0, SeekOrigin.Begin);

            _sanitizerProperty.SetValue(this, _responseSanitizer);
        }

        public override int Status { get; }

        public override string ReasonPhrase { get; }

        public override Stream ContentStream
        {
            get
            {
                var resultStream = new MemoryStream();
                _contentStream.CopyTo(resultStream);
                _contentStream.Seek(0, SeekOrigin.Begin);

                return resultStream;
            }
            set
            {
                _contentStream.Seek(0, SeekOrigin.Begin);
                _contentStream.SetLength(value.Length);
                value.CopyTo(_contentStream);
                _contentStream.Seek(0, SeekOrigin.Begin);
            }
        }

        public override string ClientRequestId { get; set; }

        public new bool IsError
        {
            get => base.IsError;
            set => typeof(Response).GetProperty(nameof(IsError))?.SetValue(this, value);
        }

        public override void Dispose()
            => _contentStream.Dispose();

        protected override bool ContainsHeader(string name)
            => _headers.ContainsKey(name);

        protected override IEnumerable<HttpHeader> EnumerateHeaders()
            => _headers.Select(header => new HttpHeader(header.Key, header.Value));

        protected override bool TryGetHeader(string name, out string value)
            => _headers.TryGetValue(name, out value);

        protected override bool TryGetHeaderValues(string name, out IEnumerable<string> values)
        {
            if (_headers.TryGetValue(name, out var value))
            {
                values = Enumerable.Repeat(value, 1);
                return true;
            }

            values = null;
            return false;
        }
    }
}