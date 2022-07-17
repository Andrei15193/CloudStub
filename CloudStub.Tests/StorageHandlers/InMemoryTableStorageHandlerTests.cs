using CloudStub.StorageHandlers;

namespace CloudStub.Tests.StorageHandlers
{
    public class InMemoryTableStorageHandlerTests : BaseTableStorageHandlerTests<InMemoryTableStorageHandler>
    {
        protected override InMemoryTableStorageHandler CreateTableStorageHandler()
            => new();
    }
}