/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The operator half of the shared column filter (<see cref="ColumnFilterMatcher"/>) compares a numeric cell as the number it
/// holds, so under a comma-decimal culture 1.5 is one and a half and not fifteen. A typed value, or a text cell, is read by the
/// separators it carries rather than by the culture alone, because the grids format every number in en-US whatever the
/// machine's culture, and a user types or pastes what they see: when both "," and "." appear the last one is the decimal
/// separator; a lone separator that is not followed by exactly three digits is the decimal separator in any culture; a lone
/// separator before exactly three digits, or a repeated one, follows the user's culture. Lite and the Viewer both filter through
/// this code. Each test sets the thread's culture and restores it.
/// </summary>
public sealed class ColumnFilterNumericCultureTests
{
    private sealed class Row
    {
        public double AvgMs { get; set; }
        public decimal Amount { get; set; }
        public int Count { get; set; }
        public string? Text { get; set; }
    }

    private static bool Matches(string column, Row row, FilterOperator op, string typed) =>
        ColumnFilterMatcher.MatchesFilter(row, new ColumnFilterState { ColumnName = column, Operator = op, Value = typed });

    private static void Under(string culture, Action test)
    {
        var saved = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
            test();
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    [Fact]
    public void UnderACommaDecimalCulture_AFractionalCell_IsComparedAsItsValue() => Under("de-DE", () =>
    {
        Assert.False(Matches(nameof(Row.AvgMs), new Row { AvgMs = 1.5 }, FilterOperator.GreaterThan, "10"));
        Assert.True(Matches(nameof(Row.AvgMs), new Row { AvgMs = 15.5 }, FilterOperator.GreaterThan, "10"));
        Assert.True(Matches(nameof(Row.AvgMs), new Row { AvgMs = 1.5 }, FilterOperator.LessThan, "2"));
        Assert.False(Matches(nameof(Row.Amount), new Row { Amount = 1234.5m }, FilterOperator.GreaterThanOrEqual, "10000"));
    });

    [Fact]
    public void UnderACommaDecimalCulture_ATypedCommaDecimal_IsTheFraction() => Under("de-DE", () =>
    {
        Assert.True(Matches(nameof(Row.Count), new Row { Count = 2 }, FilterOperator.GreaterThan, "1,5"));
        Assert.False(Matches(nameof(Row.Count), new Row { Count = 1 }, FilterOperator.GreaterThan, "1,5"));
        Assert.True(Matches(nameof(Row.AvgMs), new Row { AvgMs = 1.5 }, FilterOperator.LessThanOrEqual, "1,5"));
        Assert.False(Matches(nameof(Row.AvgMs), new Row { AvgMs = 1.6 }, FilterOperator.LessThanOrEqual, "1,5"));
        /* the culture's own group separator is dropped, so a typed 1.234,5 is one thousand two hundred thirty-four and a half */
        Assert.True(Matches(nameof(Row.Amount), new Row { Amount = 1234.5m }, FilterOperator.LessThanOrEqual, "1.234,5"));
        Assert.False(Matches(nameof(Row.Amount), new Row { Amount = 1234.6m }, FilterOperator.LessThanOrEqual, "1.234,5"));
    });

    [Fact]
    public void UnderACommaDecimalCulture_EqualsOnAFractionalCell_IsItsValue() => Under("de-DE", () =>
    {
        /* Equals and NotEquals split the typed text on commas (a list of terms), so a whole number is what a user types here. */
        Assert.True(Matches(nameof(Row.AvgMs), new Row { AvgMs = 15 }, FilterOperator.Equals, "15"));
        Assert.False(Matches(nameof(Row.AvgMs), new Row { AvgMs = 1.5 }, FilterOperator.Equals, "15"));
        Assert.True(Matches(nameof(Row.AvgMs), new Row { AvgMs = 1.5 }, FilterOperator.NotEquals, "15"));
    });

    [Fact]
    public void UnderEnUs_AGroupedTypedValue_ReadsAsBefore() => Under("en-US", () =>
    {
        Assert.True(Matches(nameof(Row.Amount), new Row { Amount = 1234.5m }, FilterOperator.LessThanOrEqual, "1,234.5"));
        Assert.False(Matches(nameof(Row.Amount), new Row { Amount = 1234.6m }, FilterOperator.LessThanOrEqual, "1,234.5"));
        Assert.True(Matches(nameof(Row.AvgMs), new Row { AvgMs = 1.5 }, FilterOperator.GreaterThan, "1.25"));
        Assert.False(Matches(nameof(Row.AvgMs), new Row { AvgMs = 1.5 }, FilterOperator.GreaterThan, "10"));
    });

    /* The grid shows 1.5 as "1.5" on every machine, so a de-DE user who types or pastes it means one and a half, not fifteen. */
    [Fact]
    public void UnderACommaDecimalCulture_ATypedPointDecimal_IsTheFraction() => Under("de-DE", () =>
    {
        Assert.True(Matches(nameof(Row.Count), new Row { Count = 2 }, FilterOperator.GreaterThan, "1.5"));
        Assert.False(Matches(nameof(Row.Count), new Row { Count = 1 }, FilterOperator.GreaterThan, "1.5"));
        Assert.True(Matches(nameof(Row.AvgMs), new Row { AvgMs = 1.5 }, FilterOperator.LessThanOrEqual, "1.5"));
        Assert.False(Matches(nameof(Row.AvgMs), new Row { AvgMs = 1.6 }, FilterOperator.LessThanOrEqual, "1.5"));
        /* a point before four digits is a decimal point too: only exactly three digits make it a group separator */
        Assert.True(Matches(nameof(Row.AvgMs), new Row { AvgMs = 12.3 }, FilterOperator.LessThan, "12.3456"));
        Assert.False(Matches(nameof(Row.AvgMs), new Row { AvgMs = 12.4 }, FilterOperator.LessThan, "12.3456"));
    });

    /* With both separators present the last one is the decimal separator, so a pasted "1,234.5" is 1234.5 under de-DE too. */
    [Fact]
    public void UnderACommaDecimalCulture_ATypedPointDecimalWithCommaGroups_IsTheGroupedNumber() => Under("de-DE", () =>
    {
        Assert.True(Matches(nameof(Row.Amount), new Row { Amount = 1234.5m }, FilterOperator.LessThanOrEqual, "1,234.5"));
        Assert.False(Matches(nameof(Row.Amount), new Row { Amount = 1234.6m }, FilterOperator.LessThanOrEqual, "1,234.5"));
        Assert.True(Matches(nameof(Row.Amount), new Row { Amount = 1234.6m }, FilterOperator.GreaterThan, "1,234.5"));
    });

    /* A lone separator before exactly three digits is the one ambiguous shape, and it follows the user's culture: de-DE "1.234" is 1234. */
    [Fact]
    public void UnderACommaDecimalCulture_ASinglePointBeforeThreeDigits_IsTheCulturesGroupSeparator() => Under("de-DE", () =>
    {
        Assert.True(Matches(nameof(Row.Count), new Row { Count = 1234 }, FilterOperator.GreaterThanOrEqual, "1.234"));
        Assert.False(Matches(nameof(Row.Count), new Row { Count = 1233 }, FilterOperator.GreaterThanOrEqual, "1.234"));
        Assert.False(Matches(nameof(Row.Count), new Row { Count = 2 }, FilterOperator.GreaterThan, "1.234"));
    });

    /* A lone comma before one digit is a decimal comma even under en-US, where a comma otherwise groups thousands. */
    [Fact]
    public void UnderEnUs_ATypedCommaDecimal_IsTheFraction() => Under("en-US", () =>
    {
        Assert.True(Matches(nameof(Row.Count), new Row { Count = 2 }, FilterOperator.GreaterThan, "1,5"));
        Assert.False(Matches(nameof(Row.Count), new Row { Count = 1 }, FilterOperator.GreaterThan, "1,5"));
        Assert.True(Matches(nameof(Row.AvgMs), new Row { AvgMs = 1.5 }, FilterOperator.LessThanOrEqual, "1,5"));
        Assert.False(Matches(nameof(Row.AvgMs), new Row { AvgMs = 1.6 }, FilterOperator.LessThanOrEqual, "1,5"));
        /* the same lone comma before exactly three digits follows the culture: en-US "1,234" is 1234 */
        Assert.True(Matches(nameof(Row.Count), new Row { Count = 1234 }, FilterOperator.GreaterThanOrEqual, "1,234"));
        Assert.False(Matches(nameof(Row.Count), new Row { Count = 2 }, FilterOperator.GreaterThan, "1,234"));
    });

    /* A text cell is read by the same rule as the typed value. */
    [Fact]
    public void UnderACommaDecimalCulture_ATextCell_IsReadByTheSameRule() => Under("de-DE", () =>
    {
        Assert.True(Matches(nameof(Row.Text), new Row { Text = "1.5" }, FilterOperator.GreaterThan, "1"));
        Assert.False(Matches(nameof(Row.Text), new Row { Text = "1.5" }, FilterOperator.GreaterThan, "10"));
        Assert.True(Matches(nameof(Row.Text), new Row { Text = "1,5" }, FilterOperator.LessThan, "2"));
        Assert.True(Matches(nameof(Row.Text), new Row { Text = "1.234" }, FilterOperator.GreaterThanOrEqual, "1000"));
        Assert.True(Matches(nameof(Row.Text), new Row { Text = "1.234.567" }, FilterOperator.GreaterThan, "1000000"));
    });
}
