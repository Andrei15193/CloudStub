using System;

namespace CloudStub.Core
{
    public class StubEntityProperty
    {
        public StubEntityProperty(byte[] binary)
            => (Value, Type) = (binary, StubEntityPropertyType.Binary);

        public StubEntityProperty(bool boolean)
            => (Value, Type) = (boolean, StubEntityPropertyType.Boolean);

        public StubEntityProperty(int int32)
            => (Value, Type) = (int32, StubEntityPropertyType.Int32);

        public StubEntityProperty(long int64)
            => (Value, Type) = (int64, StubEntityPropertyType.Int64);

        public StubEntityProperty(double @double)
            => (Value, Type) = (@double, StubEntityPropertyType.Double);

        public StubEntityProperty(Guid guid)
            => (Value, Type) = (guid, StubEntityPropertyType.Guid);

        public StubEntityProperty(DateTime dateTime)
            => (Value, Type) = (dateTime, StubEntityPropertyType.DateTime);

        public StubEntityProperty(string @string)
            => (Value, Type) = (@string, StubEntityPropertyType.String);

        public object Value { get; }

        public StubEntityPropertyType Type { get; }
    }
}