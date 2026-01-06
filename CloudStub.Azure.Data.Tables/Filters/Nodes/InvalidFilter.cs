using System;
using System.Collections.Generic;

namespace CloudStub.Azure.Data.Tables.Filters.Nodes
{
    public class InvalidFilter : Filter
    {
        public InvalidFilter(string errorMessage)
            => ErrorMessage = errorMessage;

        public string ErrorMessage { get; }

        public override int DiscreteFiltersCount
            => 0;

        public override bool Apply(IReadOnlyDictionary<string, object> entity)
            => throw new InvalidOperationException("The filter expression is invalid. " + ErrorMessage);
    }
}