﻿using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStub.FilterParser.FilterNodes
{
    internal class LessThanFilterNode : PropertyFilterNode
    {
        public LessThanFilterNode(string propertyName, EntityProperty filterValue)
            : base(propertyName, filterValue)
        {
        }

        public override bool Apply(DynamicTableEntity entity)
        {
            var compareResult = Compare(FilterValue, GetValueFromEntity(entity));
            var result = compareResult < 0;
            return result;
        }
    }
}