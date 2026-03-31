using System;
using System.Collections.Generic;
using System.Linq;

namespace CloudStub.AzureDataTables.Filters.Nodes
{
    internal abstract class ComparisonFilter : Filter
    {
        public ComparisonFilter(string propertyName, object value)
            => (PropertyName, Value) = (propertyName, value);

        protected string PropertyName { get; }
        protected object Value { get; }

        public sealed override IEnumerable<string> FilteredProperties
            => Enumerable.Repeat(PropertyName, 1);

        protected int? Compare(object propertyValue, object value)
        {
            if ((propertyValue == null || value == null) && propertyValue != value)
                return null;

            if (propertyValue == null && value == null)
                return 0;

            else if (propertyValue is string stringPropertyValue && value is string stringValue)
                return string.CompareOrdinal(stringPropertyValue, stringValue);

            else if (propertyValue.GetType() == value.GetType() && propertyValue is IComparable comparablePropertyValue)
                return comparablePropertyValue.CompareTo(value);

            else if (propertyValue is byte[] binaryPropertyValue && value is byte[] binaryValue)
            {
                var index = 0;
                var compareResult = 0;
                while (compareResult == 0 && index < binaryPropertyValue.Length && index < binaryValue.Length)
                {
                    compareResult = binaryPropertyValue[index].CompareTo(binaryValue[index]);
                    index++;
                }
                if (compareResult == 0)
                    compareResult = binaryPropertyValue.Length.CompareTo(binaryValue.Length);

                return compareResult;
            }
            else
                return null;
        }
    }
}