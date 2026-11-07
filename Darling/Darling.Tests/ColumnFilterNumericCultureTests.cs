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
/// holds and reads the typed value in the user's own culture, so under a comma-decimal culture 1.5 is one and a half and not
/// fifteen. Lite and the Viewer both filter through this code. Each test sets the thread's culture and restores it.
/// </summary>
public sealed class ColumnFilterNumericCultureTests
{
    private sealed class Row
    {
        public double AvgMs { get; set; }
        public decimal Amount { get; set; }
        public int Count { get; set; }
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
}
