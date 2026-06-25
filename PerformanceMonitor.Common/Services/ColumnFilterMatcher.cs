/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// Operator-based matching for popup column filters, shared by Lite and Dashboard.
/// Pure reflection + value comparison over <see cref="ColumnFilterState"/> /
/// <see cref="FilterOperator"/> — no WPF dependency — so both apps' DataGridFilterService
/// forward here instead of each carrying their own (previously byte-identical) copy.
/// </summary>
public static class ColumnFilterMatcher
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

        // Handle IsEmpty/IsNotEmpty operators first
        if (filter.Operator == FilterOperator.IsEmpty)
            return IsValueEmpty(rawValue);
        if (filter.Operator == FilterOperator.IsNotEmpty)
            return !IsValueEmpty(rawValue);

        var stringValue = rawValue?.ToString() ?? string.Empty;
        var filterValue = filter.Value ?? string.Empty;

        // Split comma-separated values for text operators (e.g., "db1, db2")
        var terms = filterValue.Split(',')
            .Select(t => t.Trim())
            .Where(t => !string.IsNullOrEmpty(t))
            .ToArray();

        if (terms.Length == 0)
            return true;

        // Text operators match ANY term, NotEquals excludes ALL terms
        return filter.Operator switch
        {
            FilterOperator.Contains => terms.Any(t => stringValue.Contains(t, StringComparison.OrdinalIgnoreCase)),
            FilterOperator.Equals => terms.Any(t => CompareValues(rawValue, t, (a, b) => a == b)),
            FilterOperator.NotEquals => terms.All(t => CompareValues(rawValue, t, (a, b) => a != b)),
            FilterOperator.GreaterThan => CompareNumeric(rawValue, filterValue, (a, b) => a > b),
            FilterOperator.GreaterThanOrEqual => CompareNumeric(rawValue, filterValue, (a, b) => a >= b),
            FilterOperator.LessThan => CompareNumeric(rawValue, filterValue, (a, b) => a < b),
            FilterOperator.LessThanOrEqual => CompareNumeric(rawValue, filterValue, (a, b) => a <= b),
            FilterOperator.StartsWith => terms.Any(t => stringValue.StartsWith(t, StringComparison.OrdinalIgnoreCase)),
            FilterOperator.EndsWith => terms.Any(t => stringValue.EndsWith(t, StringComparison.OrdinalIgnoreCase)),
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
            return comparison(0, 1); // null is "less than" any value

        var stringValue = rawValue.ToString() ?? string.Empty;

        // Try numeric comparison first
        if (TryParseNumeric(stringValue, out var numericValue) && TryParseNumeric(filterValue, out var filterNumeric))
        {
            return comparison(numericValue.CompareTo(filterNumeric), 0);
        }

        // Fall back to string comparison
        return comparison(string.Compare(stringValue, filterValue, StringComparison.OrdinalIgnoreCase), 0);
    }

    private static bool CompareNumeric(object? rawValue, string filterValue, Func<decimal, decimal, bool> comparison)
    {
        if (rawValue == null)
            return false;

        var stringValue = rawValue.ToString() ?? string.Empty;

        // Try to parse both values as decimals
        if (TryParseNumeric(stringValue, out var numericValue) && TryParseNumeric(filterValue, out var filterNumeric))
        {
            return comparison(numericValue, filterNumeric);
        }

        // If we can't parse both as numbers, the filter doesn't match
        return false;
    }

    private static bool TryParseNumeric(string value, out decimal result)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            result = 0;
            return false;
        }

        // Remove common formatting characters (commas, percent signs, currency symbols)
        var cleanValue = value.Trim()
            .Replace(",", "")
            .Replace("%", "")
            .Replace("$", "")
            .Replace(" ", "");

        return decimal.TryParse(cleanValue, NumberStyles.Any, CultureInfo.InvariantCulture, out result);
    }
}
