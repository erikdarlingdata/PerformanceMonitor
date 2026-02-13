/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Provides operator-based filtering for DataGrid column filters.
/// </summary>
public static class DataGridFilterService
{
    /// <summary>
    /// Checks if an item matches a ColumnFilterState using operator-based filtering.
    /// </summary>
    public static bool MatchesFilter(object item, ColumnFilterState filter)
    {
        if (item == null || filter == null || !filter.IsActive)
            return true;

        var property = item.GetType().GetProperty(filter.ColumnName);
        if (property == null)
            return true;

        var rawValue = property.GetValue(item);

        /* Handle IsEmpty/IsNotEmpty operators first */
        if (filter.Operator == FilterOperator.IsEmpty)
            return IsValueEmpty(rawValue);
        if (filter.Operator == FilterOperator.IsNotEmpty)
            return !IsValueEmpty(rawValue);

        var stringValue = rawValue?.ToString() ?? string.Empty;
        var filterValue = filter.Value ?? string.Empty;

        /* Handle comparison operators */
        return filter.Operator switch
        {
            FilterOperator.Contains => stringValue.Contains(filterValue, StringComparison.OrdinalIgnoreCase),
            FilterOperator.Equals => CompareValues(rawValue, filterValue, (a, b) => a == b),
            FilterOperator.NotEquals => CompareValues(rawValue, filterValue, (a, b) => a != b),
            FilterOperator.GreaterThan => CompareNumeric(rawValue, filterValue, (a, b) => a > b),
            FilterOperator.GreaterThanOrEqual => CompareNumeric(rawValue, filterValue, (a, b) => a >= b),
            FilterOperator.LessThan => CompareNumeric(rawValue, filterValue, (a, b) => a < b),
            FilterOperator.LessThanOrEqual => CompareNumeric(rawValue, filterValue, (a, b) => a <= b),
            FilterOperator.StartsWith => stringValue.StartsWith(filterValue, StringComparison.OrdinalIgnoreCase),
            FilterOperator.EndsWith => stringValue.EndsWith(filterValue, StringComparison.OrdinalIgnoreCase),
            _ => true
        };
    }

    private static bool IsValueEmpty(object? value)
    {
        if (value == null)
            return true;
        if (value is string str)
            return string.IsNullOrWhiteSpace(str);
        return false;
    }

    private static bool CompareValues(object? rawValue, string filterValue, Func<int, int, bool> comparison)
    {
        if (rawValue == null)
            return comparison(0, 1);

        var stringValue = rawValue.ToString() ?? string.Empty;

        if (TryParseNumeric(stringValue, out var numericValue) && TryParseNumeric(filterValue, out var filterNumeric))
        {
            return comparison(numericValue.CompareTo(filterNumeric), 0);
        }

        return comparison(string.Compare(stringValue, filterValue, StringComparison.OrdinalIgnoreCase), 0);
    }

    private static bool CompareNumeric(object? rawValue, string filterValue, Func<decimal, decimal, bool> comparison)
    {
        if (rawValue == null)
            return false;

        var stringValue = rawValue.ToString() ?? string.Empty;

        if (TryParseNumeric(stringValue, out var numericValue) && TryParseNumeric(filterValue, out var filterNumeric))
        {
            return comparison(numericValue, filterNumeric);
        }

        return false;
    }

    private static bool TryParseNumeric(string value, out decimal result)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            result = 0;
            return false;
        }

        var cleanValue = value.Trim()
            .Replace(",", "")
            .Replace("%", "")
            .Replace("$", "")
            .Replace(" ", "");

        return decimal.TryParse(cleanValue, NumberStyles.Any, CultureInfo.InvariantCulture, out result);
    }
}
