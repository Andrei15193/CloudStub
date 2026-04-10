using System;
using System.Collections.Generic;
using System.Globalization;

namespace CloudStub.AzureDataTables
{
    internal abstract class ResponseHeaders : Dictionary<string, string>
    {
        public ResponseHeaders()
            : base(StringComparer.OrdinalIgnoreCase)
        {
        }

        public Guid RequestId { get; } = Guid.NewGuid();
        public Guid ClientRequestId { get; } = Guid.NewGuid();
    }

    internal class DefaultResponseHeaders : ResponseHeaders
    {
        public DefaultResponseHeaders(Action<ResponseHeaders> otherConfig = null)
        {
            Add("Cache-Control", "no-cache");
            Add("Server", "Windows-Azure-Table/1.0 Microsoft-HTTPAPI/2.0");
            Add("x-ms-version", "2020-12-06");
            Add("X-Content-Type-Options", "nosniff");
            Add("x-ms-request-id", RequestId.ToString("D", CultureInfo.InvariantCulture));
            Add("x-ms-client-request-id", ClientRequestId.ToString("D", CultureInfo.InvariantCulture));
            Add("Date", DateTime.UtcNow.ToString("R"));

            Add("Transfer-Encoding", "chunked");
            Add("Content-Type", "application/json;odata=minimalmetadata;streaming=true;charset=utf-8");

            otherConfig?.Invoke(this);
        }
    }

    internal class AcceptedResponseHeaders : DefaultResponseHeaders
    {
        public AcceptedResponseHeaders()
        {
            Remove("Cache-Control");
            Remove("X-Content-Type-Options");
            Remove("Content-Type");
        }
    }

    internal class NoContentResponseHeaders : DefaultResponseHeaders
    {
        public NoContentResponseHeaders()
        {
            Add("Content-Length", "0");

            Remove("Transfer-Encoding");
            Remove("Content-Type");
        }
    }

    internal class XmlResponseHeaders : DefaultResponseHeaders
    {
        public XmlResponseHeaders()
        {
            this["Content-Type"] = "application/xml";

            Remove("Cache-Control");
            Remove("X-Content-Type-Options");
        }
    }

    internal class InvlaidUrlResponseHeaders : DefaultResponseHeaders
    {
        public InvlaidUrlResponseHeaders()
        {
            this["Server"] = "Microsoft-HTTPAPI/2.0";
            this["Content-Type"] = "text/html; charset=us-ascii";

            Remove("Cache-Control");
            Remove("x-ms-version");
            Remove("X-Content-Type-Options");
            Remove("x-ms-request-id");
            Remove("x-ms-client-request-id");
            Remove("Transfer-Encoding");
        }
    }
}