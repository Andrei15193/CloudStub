using System.Collections.Generic;
using CloudStub.FilterParser.FilterNodes;

namespace CloudStub.FilterParser.FilterNodeFactories
{
    internal interface IFilterNodeFactory
    {
        FilterNode Create(IReadOnlyList<FilterToken> tokens);
    }
}