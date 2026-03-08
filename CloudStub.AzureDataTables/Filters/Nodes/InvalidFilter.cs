using System;
using System.Collections.Generic;

namespace CloudStub.AzureDataTables.Filters.Nodes
{
    internal enum InvalidFilterType
    {
        SyntaxError,
        NotImplemented
    }

    internal sealed class InvalidFilter : Filter
    {
        public InvalidFilter(InvalidFilterType type, string errorMessage)
            => (Type, ErrorMessage) = (type, errorMessage);

        public InvalidFilterType Type { get; }
        public string ErrorMessage { get; }

        public override int DiscreteFiltersCount
            => 0;

        public sealed override IEnumerable<string> FilteredProperties
            => Array.Empty<string>();

        public override bool Apply(IReadOnlyDictionary<string, object> entity)
            => throw new InvalidOperationException("The filter expression is invalid. " + ErrorMessage);
    }
}