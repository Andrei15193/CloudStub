﻿using System.Collections.Generic;
using CloudStub.FilterParser.FilterNodes;

namespace CloudStub.FilterParser.FilterNodeFactories
{
    internal class NodeGroupFactory : IChainedNodeFactory
    {
        private readonly IFilterNodeFactory _rootFilterNodeFactory;

        public NodeGroupFactory(IFilterNodeFactory rootFilterNodeFactory)
            => _rootFilterNodeFactory = rootFilterNodeFactory;

        public IFilterNodeFactory Next { get; set; }

        public FilterNode Create(IReadOnlyList<FilterToken> tokens)
        {
            if (tokens[0].Code == FilterTokenCode.OpenGroup && tokens[tokens.Count - 1].Code == FilterTokenCode.CloseGroup)
                return _rootFilterNodeFactory.Create(tokens.Subrange(1, tokens.Count - 2));
            else
                return Next.Create(tokens);
        }
    }
}