/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8448: when a read takes whole hours and the typed length is not one, the hint names the rounded-up length
/// itself, so following it never narrows the range. "25.5h" is told to try 26h, not 1d. A whole number of days
/// still reads as days ("2d"), and the whole-days read keeps its own wording.
/// </summary>
public class RollingUnitHintLengthTests
{
    private static readonly Regex HintRx = new(@"try (?<hint>[^.]+)\.$", RegexOptions.CultureInvariant);

    [Theory]
    [InlineData(1.5, "2h")]
    [InlineData(25.5, "26h")]
    [InlineData(26.4, "27h")]
    [InlineData(47.5, "2d")]
    public void WholeHoursHint_NamesTheRoundedUpLength(double typedHours, string hint)
    {
        var refusal = RollingUnitRule.Refusal(TimeRangeSpec.Relative(TimeSpan.FromHours(typedHours)), RollingUnitRule.Hour);

        Assert.Equal("This read takes whole hours; try " + hint + ".", refusal);
    }

    [Theory]
    [InlineData(1.5)]
    [InlineData(25.5)]
    [InlineData(26.4)]
    [InlineData(47.5)]
    public void WholeHoursHint_TypedBack_IsAWholeHoursRangeNoShorterThanWhatWasTyped(double typedHours)
    {
        var typed = TimeSpan.FromHours(typedHours);
        var refusal = RollingUnitRule.Refusal(TimeRangeSpec.Relative(typed), RollingUnitRule.Hour);
        Assert.NotNull(refusal);

        var hint = HintRx.Match(refusal!).Groups["hint"].Value;
        var parsed = TimeRangeParser.Parse(hint, new DateTime(2026, 11, 9, 12, 0, 0, DateTimeKind.Utc), TimeZoneInfo.Utc);

        Assert.True(parsed.Range is not null, $"'{hint}' did not parse: {parsed.Error}");
        Assert.True(parsed.Range!.Span >= typed, $"'{hint}' is {parsed.Range.Span}, shorter than the typed {typed}.");
        Assert.Equal(0, parsed.Range.Span.Ticks % RollingUnitRule.Hour.Ticks);
    }

    [Fact]
    public void WholeDaysHint_NamesTheRoundedUpLengthInDays()
    {
        Assert.Equal("This read takes whole days; try 2d.", RollingUnitRule.Refusal(TimeRangeSpec.Relative(TimeSpan.FromHours(36)), RollingUnitRule.Day));
    }
}
