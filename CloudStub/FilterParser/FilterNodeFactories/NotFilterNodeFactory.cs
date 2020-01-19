using System.Collections.Generic;
using CloudStub.FilterParser.FilterNodes;

namespace CloudStub.FilterParser.FilterNodeFactories
{
    internal class NotFilterNodeFactory : IChainedNodeFactory
    {
        public IFilterNodeFactory Next { get; set; }

        public FilterNode Create(IReadOnlyList<FilterToken> tokens)
        {
            if (tokens.Count > 0 && tokens[0].Code == FilterTokenCode.Not)
                return new NotFilterNode(Next.Create(tokens.Subrange(1)));
            else
                return Next.Create(tokens);
        }
    }
}