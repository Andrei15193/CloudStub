using System;
using System.Collections.Generic;

namespace CloudStub
{
    public class StubTableQuery
    {
        public Func<StubEntity, bool> Filter { get; set; }

        public IEnumerable<string> SelectedProperties { get; set; }

        public int? PageSize { get; set; }
    }
}