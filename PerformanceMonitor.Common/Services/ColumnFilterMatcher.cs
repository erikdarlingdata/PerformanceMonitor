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
    /// The number a typed value, or a text cell, means. The grids format every number in en-US whatever the machine's culture,
    /// and a user types or pastes what they see, so the separators the text carries decide before the culture does. When both
    /// "," and "." appear, the one that appears last is the decimal separator and the other groups thousands: "1,234.5" and
    /// "1.234,5" are both 1234.5. When only one kind appears, exactly once, and not before a run of exactly three digits, it is
    /// the decimal separator in any culture: "1.5", "1,5", "0,25" and "12.3456". Otherwise (one kind repeated, or once before
    /// exactly three digits) the text is read in the thread culture with that culture's own group separator dropped, and as
    /// invariant when the culture cannot read it, so a de-DE text cell of "1.234" or "1.234.567" is its thousands and an en-US
    /// "1,234" is too. That last shape is the one the text cannot settle on its own. The percent sign, dollar sign and white
    /// space are dropped first.
    /// </summary>
    private static bool TryParseNumeric(string value, out decimal result)
    {
        var cleanValue = StripFormatting(value);
        if (cleanValue.Length == 0)
        {
            result = 0;
            return false;
        }

        if (SeparatorsDecide(cleanValue, out var normalized))
            return decimal.TryParse(normalized, NumberStyles.Any, CultureInfo.InvariantCulture, out result);

        return TryParseInCulture(cleanValue, CultureInfo.CurrentCulture, out result)
            || TryParseInCulture(cleanValue, CultureInfo.InvariantCulture, out result);
    }

    /// <summary>The text with the percent sign, the dollar sign and every white space character removed.</summary>
    private static string StripFormatting(string value) =>
        string.Concat(value.Where(c => c != '%' && c != '$' && !char.IsWhiteSpace(c)));

    /// <summary>
    /// Whether the separators in <paramref name="value"/> settle its reading on their own, and if so the text with the group
    /// separator removed and the decimal separator written as ".", ready for an invariant parse. False when the text has no
    /// separator, one kind repeated, or one kind once before exactly three digits: those are the culture's to read.
    /// </summary>
    private static bool SeparatorsDecide(string value, out string normalized)
    {
        var lastComma = value.LastIndexOf(',');
        var lastPoint = value.LastIndexOf('.');

        if (lastComma >= 0 && lastPoint >= 0)
        {
            var decimalSeparator = lastPoint > lastComma ? '.' : ',';
            var groupSeparator = decimalSeparator == '.' ? ',' : '.';
            normalized = value.Replace(groupSeparator.ToString(), "").Replace(decimalSeparator, '.');
            return true;
        }

        var last = Math.Max(lastComma, lastPoint);
        if (last >= 0 && value.IndexOf(value[last]) == last && !HasRunOfExactlyThreeDigits(value, last + 1))
        {
            normalized = value.Replace(value[last], '.');
            return true;
        }

        normalized = value;
        return false;
    }

    /// <summary>Whether <paramref name="value"/> at <paramref name="start"/> begins with a run of exactly three ASCII digits.</summary>
    private static bool HasRunOfExactlyThreeDigits(string value, int start)
    {
        var digits = 0;
        while (start + digits < value.Length && char.IsAsciiDigit(value[start + digits]))
            digits++;
        return digits == 3;
    }

    /// <summary>The text read in <paramref name="culture"/>, with that culture's own group separator dropped first.</summary>
    private static bool TryParseInCulture(string value, CultureInfo culture, out decimal result)
    {
        var groupSeparator = culture.NumberFormat.NumberGroupSeparator;
        var cleanValue = groupSeparator.Length > 0 ? value.Replace(groupSeparator, "") : value;
        return decimal.TryParse(cleanValue, NumberStyles.Any, culture, out result);
    }
}
