using System;
using System.Collections.Generic;
using System.Linq;
using CloudStub.FilterParser.FilterNodes;

namespace CloudStub.FilterParser.FilterNodeFactories
{
    internal abstract class LogicalFilterNodeFactory : IChainedNodeFactory
    {
        private readonly FilterTokenCode _tokenCode;

        public LogicalFilterNodeFactory(FilterTokenCode tokenCode)
            => _tokenCode = tokenCode;

        public IFilterNodeFactory Next { get; set; }

        public FilterNode Create(IReadOnlyList<FilterToken> tokens)
        {
            var operands = _GetOperands(tokens).ToList();
            if (operands.Count > 1)
                return GetNode(operands);
            else
                return Next.Create(tokens);
        }

        protected abstract FilterNode GetNode(IEnumerable<FilterNode> operands);

        private IEnumerable<FilterNode> _GetOperands(IReadOnlyList<FilterToken> tokens)
        {
            var start = 0;
            int operatorIndex;
            do
            {
                operatorIndex = _FindOperatorIndex(tokens, start);
                if (operatorIndex >= 0)
                {
                    yield return Next.Create(tokens.Subrange(start, operatorIndex - start));
                    start = operatorIndex + 1;
                }
            } while (operatorIndex >= 0);

            yield return Next.Create(tokens.Subrange(start));
        }

        private int _FindOperatorIndex(IReadOnlyList<FilterToken> tokens, int start)
        {
            var depth = 0;
            var index = start;
            var operatorIndex = -1;
            while (operatorIndex == -1 && index < tokens.Count)
            {
                switch (tokens[index].Code)
                {
                    case FilterTokenCode.OpenGroup:
                        depth++;
                        break;

                    case FilterTokenCode.CloseGroup:
                        depth--;
                        if (depth < 0)
                            throw new ArgumentException("Unexpected ')' token");
                        break;

                    default:
                        if (tokens[index].Code == _tokenCode && depth == 0)
                            operatorIndex = index;
                        break;
                }
                index++;
            }

            return operatorIndex;
        }
    }
}