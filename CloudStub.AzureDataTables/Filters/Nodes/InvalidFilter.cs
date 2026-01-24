using System;
using System.Collections.Generic;

namespace CloudStub.AzureDataTables.Filters.Nodes
{
    internal sealed class InvalidFilter : Filter
    {
        public InvalidFilter(string errorMessage)
            => ErrorMessage = errorMessage;

        public string ErrorMessage { get; }

        public override int DiscreteFiltersCount
            => 0;

        public sealed override IEnumerable<string> FilteredProperties
            => Array.Empty<string>();

        public override bool Apply(IReadOnlyDictionary<string, object> entity)
            => throw new InvalidOperationException("The filter expression is invalid. " + ErrorMessage);
    }
}