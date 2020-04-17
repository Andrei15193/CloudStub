﻿using CloudStub.FilterParser.FilterNodes;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace CloudStub.FilterParser.FilterNodeFactories
{
    internal class PropertyFilterNodeFactory : IFilterNodeFactory
    {
        public FilterNode Create(IReadOnlyList<FilterToken> tokens)
        {
            switch (tokens.Count)
            {
                case 1:
                    return new PropertyCheckFilterNode(tokens[0].Value);

                case 3:
                    return _GetOperatorNode(tokens[0].Value, tokens[1], _GetEntityProperty(tokens[2].Value));

                default:
                    throw new ArgumentException($"Unexpected '{string.Join(" ", tokens.Select(token => token.Value))}' tokens.");
            }
        }

        private static EntityProperty _GetEntityProperty(string value)
        {
            EntityProperty result;

            if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var intResult))
                result = EntityProperty.GeneratePropertyForInt(intResult);
            else if (value.EndsWith("L", StringComparison.OrdinalIgnoreCase) && long.TryParse(value.Substring(0, value.Length - 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out var longResult))
                result = EntityProperty.GeneratePropertyForLong(longResult);
            else if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var doubleValue))
                result = EntityProperty.GeneratePropertyForDouble(doubleValue);
            else if (bool.TryParse(value, out var boolResult))
                result = EntityProperty.GeneratePropertyForBool(boolResult);
            else if (value.StartsWith("x'", StringComparison.OrdinalIgnoreCase) && value.EndsWith("'", StringComparison.OrdinalIgnoreCase))
            {
                var binaryValue = value.Substring("x'".Length, value.Length - "x'".Length - "'".Length);
                result = EntityProperty.GeneratePropertyForByteArray(
                    Enumerable
                        .Range(0, binaryValue.Length / 2)
                        .Select(arrayIndex => Convert.ToByte(binaryValue.Substring(arrayIndex * 2, 2), 16))
                        .ToArray()
                );
            }
            else if (value.StartsWith("guid'", StringComparison.OrdinalIgnoreCase) && value.EndsWith("'", StringComparison.OrdinalIgnoreCase))
            {
                var guidValue = value.Substring("guid'".Length, value.Length - "guid'".Length - "'".Length);
                result = EntityProperty.GeneratePropertyForGuid(Guid.Parse(guidValue));
            }
            else if (value.StartsWith("datetime'", StringComparison.OrdinalIgnoreCase) && value.EndsWith("'", StringComparison.OrdinalIgnoreCase))
            {
                var dateTimeValue = value.Substring("datetime'".Length, value.Length - "datetime'".Length - "'".Length);
                result = EntityProperty.GeneratePropertyForDateTimeOffset(DateTimeOffset.Parse(dateTimeValue, CultureInfo.InvariantCulture));
            }
            else if (value.StartsWith("'", StringComparison.OrdinalIgnoreCase) && value.EndsWith("'", StringComparison.OrdinalIgnoreCase))
            {
                var stringValue = value.Substring("'".Length, value.Length - "'".Length - "'".Length);
                result = EntityProperty.GeneratePropertyForString(stringValue);
            }
            else
                result = EntityProperty.GeneratePropertyForString(value);

            return result;
        }

        private static FilterNode _GetOperatorNode(string propertyName, FilterToken @operator, EntityProperty filterValue)
        {
            switch (@operator.Code)
            {
                case FilterTokenCode.Equal:
                    return new EqualFilterNode(propertyName, filterValue);

                case FilterTokenCode.NotEqual:
                    return new NotEqualFilterNode(propertyName, filterValue);

                case FilterTokenCode.LessThan:
                    return new LessThanFilterNode(propertyName, filterValue);

                case FilterTokenCode.LessThanOrEqual:
                    return new LessThanOrEqualFilterNode(propertyName, filterValue);

                case FilterTokenCode.GreaterThan:
                    return new GreaterThanFilterNode(propertyName, filterValue);

                case FilterTokenCode.GreaterThanOrEqual:
                    return new GreaterThanOrEqualFilterNode(propertyName, filterValue);

                default:
                    throw new ArgumentException($"Unexpected '{@operator.Value}' operator");
            }
        }
    }
}