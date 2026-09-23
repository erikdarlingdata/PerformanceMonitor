/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3897: how the bucketed trend reads size their points, shared by both SKUs through
/// <see cref="TrendBuckets"/>. A day of <c>get_file_io_trend</c> was 12,500 points and 1.4 MB because a trend's
/// size was the window times the collection cadence; what replaces that is a width chosen to land near a point
/// budget, and a cap on the width a caller may ask for. These pin the arithmetic both halves stand on — the
/// automatic width always fits and is never wider than it must be, a refused width names one that fits, and the
/// point count is the one the payload will actually hold.
/// </summary>
public sealed class TrendBucketsTests
{
    /// <summary>The automatic width for the windows people ask about, at the MCP budget of 200 points: a day is
    /// 10-minute points, a week hourly, and six file I/O lines push a day to the hour.</summary>
    [Theory]
    [InlineData(1, 1, 1)]      // 61 points at one minute
    [InlineData(4, 1, 2)]      // 241 at one minute, 121 at two
    [InlineData(24, 1, 10)]    // 289 at five, 145 at ten
    [InlineData(72, 1, 30)]    // 289 at fifteen, 145 at thirty
    [InlineData(168, 1, 60)]   // 337 at thirty, 169 at sixty
    [InlineData(24, 6, 60)]    // six lines: 294 at thirty, 150 at sixty
    [InlineData(168, 6, 360)]  // six lines over a week: 258 at four hours, 174 at six
    public void TheAutomaticWidth_IsTheNarrowestLadderStepInsideTheBudget(int hoursBack, int series, int expected)
    {
        Assert.Null(TrendBuckets.Resolve(hoursBack, null, series, TrendBudget.Mcp(TrendBuckets.FileIoMaxPoints), out var width));
        Assert.Equal(expected, width);
    }

    /// <summary>
    /// The invariant under every window a read accepts (1-168 hours) and every line count file I/O can draw
    /// (1-6), at both budgets: the automatic width is a ladder step, it divides a day (so no bucket straddles
    /// UTC midnight), the answer fits the budget, and the next narrower step would not have.
    /// </summary>
    [Fact]
    public void EveryWindowAndLineCount_FitsTheBudget_AtTheNarrowestStepThatDoes()
    {
        var checkedCases = 0;
        foreach (var budget in new[] { TrendBudget.Mcp(TrendBuckets.DurationMaxPoints), TrendBudget.Chart })
        {
            for (var hoursBack = 1; hoursBack <= 168; hoursBack++)
            {
                for (var series = 1; series <= 6; series++)
                {
                    Assert.Null(TrendBuckets.Resolve(hoursBack, null, series, budget, out var width));

                    var step = Array.IndexOf(TrendBuckets.LadderMinutes, width);
                    Assert.True(step >= 0, $"{hoursBack}h x {series}: {width} is not a ladder width");
                    Assert.Equal(0, 1440 % width);
                    Assert.True(
                        TrendBuckets.PointsFor(hoursBack * 60, width, series) <= budget.AutoPoints,
                        $"{hoursBack}h x {series} at {width} min is over the {budget.AutoPoints}-point budget");
                    if (step > 0)
                    {
                        Assert.True(
                            TrendBuckets.PointsFor(hoursBack * 60, TrendBuckets.LadderMinutes[step - 1], series) > budget.AutoPoints,
                            $"{hoursBack}h x {series}: {TrendBuckets.LadderMinutes[step - 1]} min would have fit, so {width} is wider than it must be");
                    }

                    checkedCases++;
                }
            }
        }

        Assert.Equal(2 * 168 * 6, checkedCases);
    }

    /// <summary>
    /// The count includes the bucket the window's unaligned start cuts into, so the count that picks the width and
    /// the count that enforces the cap are the count the payload holds. An hour at ten minutes starting 10:05 touches
    /// 10:00, 10:10 … 11:00 — seven buckets — and so does one starting on the boundary, because the window's end is
    /// inclusive and its last collection opens a bucket of its own.
    /// </summary>
    [Theory]
    [InlineData(60, 10, 1, 7)]
    [InlineData(60, 1, 1, 61)]
    [InlineData(1440, 10, 1, 145)]
    [InlineData(1440, 60, 6, 150)]
    [InlineData(10080, 1440, 6, 48)]
    [InlineData(60, 7, 1, 10)]
    public void ThePointCount_IncludesTheEdgeBucket(int windowMinutes, int bucketMinutes, int series, int expected) =>
        Assert.Equal(expected, TrendBuckets.PointsFor(windowMinutes, bucketMinutes, series));

    /// <summary>A width outside one minute to one day is refused, naming the parameter — never clamped into range.</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    [InlineData(1441)]
    public void AWidthOutOfRange_IsRefused_NotClamped(int requested)
    {
        var refusal = TrendBuckets.Resolve(24, requested, 1, TrendBudget.Mcp(TrendBuckets.DurationMaxPoints), out _);

        Assert.True(McpHelpers.IsRefusalEnvelope(refusal));
        Assert.Equal("bucket_minutes", Parameter(refusal!));
        Assert.StartsWith($"Invalid bucket_minutes value '{requested}'.", McpHelpers.ErrorMessageOf(refusal!), StringComparison.Ordinal);

        /* The range half alone, which a read that learns its line count from the data asks first: the same bytes. */
        Assert.Equal(refusal, TrendBuckets.ValidateWidth(requested));
    }

    /// <summary>The range half passes a width it cannot judge against a cap — that waits for the line count.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData(1)]
    [InlineData(1440)]
    public void TheRangeHalf_PassesEveryWidthInsideADay(int? requested) =>
        Assert.Null(TrendBuckets.ValidateWidth(requested));

    /// <summary>
    /// A width that would put more points on the wire than the read's cap is refused, and the width the refusal
    /// names is exactly the narrowest that fits — so taking the advice is always served, and nothing narrower
    /// would have been. Swept over each read's cap, a spread of windows and widths, and the line counts file I/O
    /// can draw.
    /// </summary>
    [Fact]
    public void AWidthOverTheCap_IsRefused_NamingTheNarrowestWidthThatFits()
    {
        var refused = 0;
        var caps = new[]
        {
            TrendBuckets.FileIoMaxPoints, TrendBuckets.LockWaitMaxPoints, TrendBuckets.DurationMaxPoints,
            TrendBuckets.PgIoMaxPoints, TrendBuckets.PgDatabaseMaxPoints,
        };

        foreach (var cap in caps)
        {
            foreach (var hoursBack in new[] { 1, 4, 24, 72, 168 })
            {
                foreach (var series in new[] { 1, 6 })
                {
                    foreach (var requested in new[] { 1, 2, 5, 9, 30 })
                    {
                        var budget = TrendBudget.Mcp(cap);
                        var answer = TrendBuckets.Resolve(hoursBack, requested, series, budget, out var width);
                        var points = TrendBuckets.PointsFor(hoursBack * 60, requested, series);

                        if (points <= cap)
                        {
                            Assert.Null(answer);
                            Assert.Equal(requested, width);
                            continue;
                        }

                        refused++;
                        Assert.True(McpHelpers.IsRefusalEnvelope(answer));
                        Assert.Equal("bucket_minutes", Parameter(answer!));

                        var fits = TrendBuckets.NarrowestFitting(hoursBack * 60, series, cap, requested);
                        Assert.True(TrendBuckets.PointsFor(hoursBack * 60, fits, series) <= cap);
                        Assert.True(TrendBuckets.PointsFor(hoursBack * 60, fits - 1, series) > cap);
                        Assert.Contains($"Use bucket_minutes {fits} or wider", McpHelpers.ErrorMessageOf(answer!), StringComparison.Ordinal);
                        Assert.Null(TrendBuckets.Resolve(hoursBack, fits, series, budget, out _));
                    }
                }
            }
        }

        Assert.True(refused > 20, $"only {refused} cases were over a cap, so the sweep is not exercising the refusal");
    }

    /// <summary>The light single-line reads serve a whole day at one-minute points; the widest cannot.</summary>
    [Fact]
    public void TheCaps_ServeADayAtOneMinute_OnlyWhereThePointsAreSmall()
    {
        Assert.Null(TrendBuckets.Resolve(24, 1, 1, TrendBudget.Mcp(TrendBuckets.LockWaitMaxPoints), out _));
        Assert.Null(TrendBuckets.Resolve(24, 1, 1, TrendBudget.Mcp(TrendBuckets.DurationMaxPoints), out _));
        Assert.NotNull(TrendBuckets.Resolve(24, 1, 1, TrendBudget.Mcp(TrendBuckets.PgIoMaxPoints), out _));
        Assert.NotNull(TrendBuckets.Resolve(24, 1, 1, TrendBudget.Mcp(TrendBuckets.FileIoMaxPoints), out _));
        Assert.Equal(new TrendBudget(200, 650), TrendBudget.Mcp(TrendBuckets.PgIoMaxPoints));
        Assert.Equal(new TrendBudget(1500, 1500), TrendBudget.Chart);
    }

    /// <summary>The hourly rollup serves whole hours only: a finer or fractional-hour width is refused rather than
    /// answered at the hour, and an automatic width is raised to the hour rather than asking the rollup for points
    /// it does not hold.</summary>
    [Fact]
    public void TheHourlyTier_ServesWholeHours_AndRaisesTheAutomaticWidth()
    {
        Assert.Null(TrendBuckets.RequireWholeHours(null, 4));
        Assert.Null(TrendBuckets.RequireWholeHours(60, 4));
        Assert.Null(TrendBuckets.RequireWholeHours(120, 4));
        Assert.Null(TrendBuckets.RequireWholeHours(1440, 4));

        foreach (var requested in new[] { 1, 30, 90 })
        {
            var refusal = TrendBuckets.RequireWholeHours(requested, 4);
            Assert.True(McpHelpers.IsRefusalEnvelope(refusal));
            Assert.Equal("bucket_minutes", Parameter(refusal!));
            Assert.Contains("whole hours", McpHelpers.ErrorMessageOf(refusal!), StringComparison.Ordinal);
        }

        Assert.Equal(60, TrendBuckets.OnHourlyTier(null, 10));
        Assert.Equal(120, TrendBuckets.OnHourlyTier(null, 120));
        Assert.Equal(180, TrendBuckets.OnHourlyTier(180, 10));
    }

    /// <summary>The width as the payload's <c>bucket</c> word and the note's adjective. <c>1 hour</c> is the hourly
    /// tier's existing spelling, so an hourly answer reads as it always did.</summary>
    [Theory]
    [InlineData(1, "1 minute", "1-minute")]
    [InlineData(2, "2 minutes", "2-minute")]
    [InlineData(10, "10 minutes", "10-minute")]
    [InlineData(90, "90 minutes", "90-minute")]
    [InlineData(60, "1 hour", "1-hour")]
    [InlineData(360, "6 hours", "6-hour")]
    [InlineData(1440, "1 day", "1-day")]
    public void TheWidth_IsSpelledOneWay(int bucketMinutes, string word, string adjective)
    {
        Assert.Equal(word, TrendBuckets.Word(bucketMinutes));
        Assert.Equal(adjective, TrendBuckets.Adjective(bucketMinutes));
    }

    /// <summary>The note leads with what a point is and ends with how its width was chosen — the caller's, or the
    /// budget's with the way to change it.</summary>
    [Fact]
    public void TheAggregateNote_SaysWhatAPointIs_AndWhoChoseItsWidth()
    {
        var chosen = TrendBuckets.AggregateNote(10, requested: false, 200);
        Assert.StartsWith("Each point summarizes the collections in one 10-minute bucket", chosen, StringComparison.Ordinal);
        Assert.Contains("never averaged from per-collection values", chosen, StringComparison.Ordinal);
        Assert.Contains("near 200 points; pass bucket_minutes (1-1440)", chosen, StringComparison.Ordinal);

        var passed = TrendBuckets.AggregateNote(360, requested: true, 200);
        Assert.StartsWith("Each point summarizes the collections in one 6-hour bucket", passed, StringComparison.Ordinal);
        Assert.EndsWith("The width is the bucket_minutes you passed.", passed, StringComparison.Ordinal);
    }

    /// <summary>File I/O's lines: every series when folding would pool just one into "(other)" (a line of one series
    /// is that series under a worse name), else the top five plus the fold.</summary>
    [Theory]
    [InlineData(0, 0, 0)]
    [InlineData(1, 1, 1)]
    [InlineData(5, 5, 5)]
    [InlineData(6, 6, 6)]
    [InlineData(7, 5, 6)]
    [InlineData(40, 5, 6)]
    public void FileIoLines_FoldOnlyWhenTheFoldPoolsMoreThanOne(int active, int charted, int lines)
    {
        Assert.Equal(charted, TrendPayloads.ChartedFor(active));
        Assert.Equal(lines, TrendPayloads.LinesFor(active));
    }

    private static string? Parameter(string refusal) =>
        JsonDocument.Parse(refusal).RootElement.GetProperty("hints").GetProperty("parameter").GetString();
}
