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
    /// Checks if an item matches a ColumnFilterState: it has to pass the text match (operator-based) AND the value
    /// list part (#5565). A part that is not active passes everything.
    /// </summary>
    public static bool MatchesFilter(object item, ColumnFilterState filter)
    {
        if (item == null || filter == null || !filter.IsActive)
            return true;

        var property = item.GetType().GetProperty(filter.ColumnName);
        if (property == null)
            return true;

        var rawValue = property.GetValue(item);

        return (!filter.HasTextMatch || MatchesText(rawValue, filter)) &&
               (!filter.HasValuePart || MatchesValuePart(rawValue, filter));
    }

    /// <summary>
    /// The value list part: a blank cell (null, empty or whitespace) is the blank flag's, every other cell is
    /// looked up ordinal ignore-case. Hide passes what is not named; ShowOnly passes only what is named.
    /// </summary>
    public static bool MatchesValuePart(object? rawValue, ColumnFilterState filter)
    {
        if (!filter.HasValuePart)
            return true;

        var text = rawValue?.ToString();
        var named = string.IsNullOrWhiteSpace(text) ? filter.ValueBlank : filter.Values.Contains(text);
        return filter.ValueMode == ColumnValueMode.Hide ? !named : named;
    }

    private static bool MatchesText(object? rawValue, ColumnFilterState filter)
    {
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

        // Try numeric comparison first
        if (TryGetNumeric(rawValue, out var numericValue) && TryParseNumeric(filterValue, out var filterNumeric))
        {
            return comparison(numericValue.CompareTo(filterNumeric), 0);
        }

        // Fall back to string comparison
        var stringValue = rawValue.ToString() ?? string.Empty;
        return comparison(string.Compare(stringValue, filterValue, StringComparison.OrdinalIgnoreCase), 0);
    }

    private static bool CompareNumeric(object? rawValue, string filterValue, Func<decimal, decimal, bool> comparison)
    {
        if (rawValue == null)
            return false;

        // Compare both values as decimals
        if (TryGetNumeric(rawValue, out var numericValue) && TryParseNumeric(filterValue, out var filterNumeric))
        {
            return comparison(numericValue, filterNumeric);
        }

        // If we can't read both as numbers, the filter doesn't match
        return false;
    }

    /// <summary>
    /// The number a cell holds. A numeric cell is read as its value, with no round trip through text: a text rendering
    /// follows the thread culture, and a comma-decimal culture would turn 1.5 into "1,5" and then into 15. A cell of any
    /// other type is read as its text, the same way the typed value is.
    /// </summary>
    private static bool TryGetNumeric(object rawValue, out decimal result)
    {
        switch (rawValue)
        {
            case decimal d:
                result = d;
                return true;
            case double d:
                return TryFromDouble(d, out result);
            case float f:
                return TryFromDouble(f, out result);
            case byte or sbyte or short or ushort or int or uint or long or ulong:
                result = Convert.ToDecimal(rawValue, CultureInfo.InvariantCulture);
                return true;
            default:
                return TryParseNumeric(rawValue.ToString() ?? string.Empty, out result);
        }
    }

    private static bool TryFromDouble(double value, out decimal result)
    {
        if (double.IsFinite(value) && Math.Abs(value) < (double)decimal.MaxValue)
        {
            result = (decimal)value;
            return true;
        }

        result = 0;
        return false;
    }

    /// <summary>
    /// The number a typed value (or a text cell) means: read in the thread culture, with that culture's own group separator
    /// dropped, so a de-DE user's "1,5" is one and a half and "1.234,5" is 1234.5, and an en-US user's "1,234.5" is 1234.5.
    /// A value the culture cannot read is tried as invariant.
    /// </summary>
    private static bool TryParseNumeric(string value, out decimal result)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            result = 0;
            return false;
        }

        return TryParseNumeric(value, CultureInfo.CurrentCulture, out result)
            || TryParseNumeric(value, CultureInfo.InvariantCulture, out result);
    }

    private static bool TryParseNumeric(string value, CultureInfo culture, out decimal result)
    {
        var groupSeparator = culture.NumberFormat.NumberGroupSeparator;

        // Remove common formatting characters (the culture's group separator, percent signs, currency symbols)
        var cleanValue = value.Trim();
        if (groupSeparator.Length > 0)
            cleanValue = cleanValue.Replace(groupSeparator, "");
        cleanValue = cleanValue
            .Replace("%", "")
            .Replace("$", "")
            .Replace(" ", "");

        return decimal.TryParse(cleanValue, NumberStyles.Any, culture, out result);
    }
}
