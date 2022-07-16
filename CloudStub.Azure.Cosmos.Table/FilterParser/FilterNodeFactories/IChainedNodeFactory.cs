namespace CloudStub.FilterParser.FilterNodeFactories
{
    internal interface IChainedNodeFactory : IFilterNodeFactory
    {
        IFilterNodeFactory Next { get; set; }
    }
}