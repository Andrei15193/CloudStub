using System;
using System.Collections.Generic;
using System.Linq;
using CloudStub.FilterParser.FilterNodeFactories;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.FilterParser
{
    internal class FilterTokenParser
    {
        private readonly IFilterNodeFactory _rootFilterNodeFactory;

        public FilterTokenParser()
        {
            var rootFilterNodeFactory = new OrFilterNodeFactory();
            rootFilterNodeFactory.Next = new AndFilterNodeFactory
            {
                Next = new NotFilterNodeFactory
                {
                    Next = new NodeGroupFactory(rootFilterNodeFactory)
                    {
                        Next = new PropertyFilterNodeFactory()
                    }
                }
            };
            _rootFilterNodeFactory = rootFilterNodeFactory;
        }

        public Func<DynamicTableEntity, bool> Parse(IReadOnlyList<FilterToken> tokens)
        {
            if (tokens.Any())
            {
                var rootFilterNode = _rootFilterNodeFactory.Create(tokens);
                return rootFilterNode.Apply;
            }
            else
                return delegate { return true; };
        }
    }
}