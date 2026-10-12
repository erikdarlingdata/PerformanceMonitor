/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5703 (review bot): the Job History tab reads its Server and Category choices once per window and server, and again only after
/// <c>ChoicesLifetime</c>. The cache was keyed on the window's SPAN, which for a live window with a fixed start (Today, Week to Date,
/// Month to Date, Year to Date, "since X") grows on every refresh, so the key was new each time and the uncapped fleet-wide DISTINCT ran
/// on every auto-refresh. The key now holds what identifies the window (<see cref="JobHistoryChoicesKey"/>); these tests drive the viewer's
/// real window (<see cref="ViewerTimeRangeWindow"/>) through several refreshes against <see cref="JobHistoryChoicesFreshness"/>.
/// The Lite twin is Lite.Tests' JobHistoryChoicesCacheTests.
/// </summary>
[Trait("Reads", "Lite")]
public sealed class JobHistoryChoicesCacheTests
{
    private static readonly TimeSpan Lifetime = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan Refresh = TimeSpan.FromSeconds(30);
    private static readonly DateTime T0 = new(2026, 10, 12, 15, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The viewer's key for a load of <paramref name="spec"/> at <paramref name="nowUtc"/> (the tab's own computation).</summary>
    private static JobHistoryChoicesKey ViewerKey(TimeRangeSpec spec, DateTime nowUtc, TimeZoneInfo zone, int? serverId)
    {
        var (start, end, live) = ViewerTimeRangeWindow.Window(spec, nowUtc, zone);
        return JobHistoryChoicesKey.For(ViewerTimeRangeWindow.StartSlides(spec, nowUtc, zone), start, end, live, serverId);
    }

    /// <summary>How many of <paramref name="refreshes"/> loads, <see cref="Refresh"/> apart, start the choices reads.</summary>
    private static int ReadsAcross(TimeRangeSpec spec, TimeZoneInfo zone, int refreshes, DateTime from, JobHistoryChoicesFreshness? freshness = null, int? serverId = null)
    {
        freshness ??= new JobHistoryChoicesFreshness(Lifetime);
        var reads = 0;
        for (var i = 0; i < refreshes; i++)
        {
            var now = from + (Refresh * i);
            var key = ViewerKey(spec, now, zone, serverId);
            if (freshness.NeedsRead(key, now))
            {
                reads++;
                freshness.Record(key, now);
            }
        }

        return reads;
    }

    public static IEnumerable<object[]> LiveFixedStartWindows()
    {
        yield return new object[] { TimeRangeSpec.ForPeriod(CalendarPeriod.Today) };
        yield return new object[] { TimeRangeSpec.ForPeriod(CalendarPeriod.WeekToDate) };
        yield return new object[] { TimeRangeSpec.ForPeriod(CalendarPeriod.MonthToDate) };
        yield return new object[] { TimeRangeSpec.ForPeriod(CalendarPeriod.YearToDate) };
        yield return new object[] { TimeRangeSpec.SinceInstant(new DateTime(2026, 10, 10, 8, 0, 0, DateTimeKind.Unspecified)) };
    }

    [Theory]
    [MemberData(nameof(LiveFixedStartWindows))]
    public void ALiveWindowWithAFixedStart_ReadsTheChoicesOnceInsideTheLifetime(TimeRangeSpec spec)
    {
        /* 9 refreshes, 30 s apart: the last is 4 minutes after the first, inside the 5-minute lifetime. */
        Assert.Equal(1, ReadsAcross(spec, TimeZoneInfo.Utc, 9, T0));
    }

    [Theory]
    [MemberData(nameof(LiveFixedStartWindows))]
    public void ALiveWindowWithAFixedStart_ReadsTheChoicesAgainAfterTheLifetime(TimeRangeSpec spec)
    {
        var freshness = new JobHistoryChoicesFreshness(Lifetime);

        Assert.Equal(1, ReadsAcross(spec, TimeZoneInfo.Utc, 9, T0, freshness));
        /* The next refreshes run on, 30 s apart, from 4m30s: the 5-minute lifetime ends at 5m, so the 11th (5m) is not yet over it and the 12th (5m30s) is. */
        Assert.Equal(1, ReadsAcross(spec, TimeZoneInfo.Utc, 4, T0 + (Refresh * 9), freshness));
    }

    [Theory]
    [MemberData(nameof(LiveFixedStartWindows))]
    public void TheOldSpanKey_WasNewOnEveryRefresh_ForTheseWindows(TimeRangeSpec spec)
    {
        /* The shape this replaces: the key was the window's length. It grows with now for a live window with a fixed start. */
        TimeSpan SpanAt(DateTime now)
        {
            var (start, end, _) = ViewerTimeRangeWindow.Window(spec, now, TimeZoneInfo.Utc);
            return end - start;
        }

        Assert.NotEqual(SpanAt(T0), SpanAt(T0 + Refresh));
        Assert.Null(ViewerKey(spec, T0, TimeZoneInfo.Utc, null).Span);
        Assert.Equal(ViewerKey(spec, T0, TimeZoneInfo.Utc, null), ViewerKey(spec, T0 + Refresh, TimeZoneInfo.Utc, null));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(24)]
    [InlineData(168)]
    public void ARelativePreset_StillReadsOnceInsideTheLifetime_AndItsStartSlides(int hours)
    {
        var spec = TimeRangeSpec.Relative(TimeSpan.FromHours(hours));

        Assert.Equal(1, ReadsAcross(spec, TimeZoneInfo.Utc, 9, T0));
        Assert.True(ViewerTimeRangeWindow.StartSlides(spec, T0, TimeZoneInfo.Utc));
        Assert.Equal(TimeSpan.FromHours(hours), ViewerKey(spec, T0 + Refresh, TimeZoneInfo.Utc, null).Span);
    }

    [Fact]
    public void ACalendarPeriodThatHasEnded_IsKeyedByItsStartAndItsEnd_AndReadsOnce()
    {
        var spec = TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday);

        Assert.Equal(1, ReadsAcross(spec, TimeZoneInfo.Utc, 9, T0));
        var key = ViewerKey(spec, T0, TimeZoneInfo.Utc, null);
        Assert.NotNull(key.StartUtc);
        Assert.NotNull(key.FixedEndUtc);
        Assert.Null(key.Span);
    }

    [Fact]
    public void AChangedWindow_ReadsTheChoicesAgain_EvenInsideTheLifetime()
    {
        var freshness = new JobHistoryChoicesFreshness(Lifetime);

        Assert.Equal(1, ReadsAcross(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), TimeZoneInfo.Utc, 3, T0, freshness));
        Assert.Equal(1, ReadsAcross(TimeRangeSpec.ForPeriod(CalendarPeriod.MonthToDate), TimeZoneInfo.Utc, 3, T0 + (Refresh * 3), freshness));
        Assert.Equal(1, ReadsAcross(TimeRangeSpec.Relative(TimeSpan.FromHours(4)), TimeZoneInfo.Utc, 3, T0 + (Refresh * 6), freshness));
        Assert.Equal(1, ReadsAcross(TimeRangeSpec.Relative(TimeSpan.FromHours(24)), TimeZoneInfo.Utc, 3, T0 + (Refresh * 9), freshness));
    }

    [Fact]
    public void ALiveWindowThatCrossesMidnight_HasANewStart_AndReadsAgain()
    {
        var freshness = new JobHistoryChoicesFreshness(Lifetime);
        var spec = TimeRangeSpec.ForPeriod(CalendarPeriod.Today);
        var beforeMidnight = new DateTime(2026, 10, 12, 23, 59, 50, DateTimeKind.Unspecified);

        Assert.Equal(1, ReadsAcross(spec, TimeZoneInfo.Utc, 1, beforeMidnight, freshness));
        Assert.Equal(1, ReadsAcross(spec, TimeZoneInfo.Utc, 2, beforeMidnight + Refresh, freshness));
    }

    [Fact]
    public void ADifferentServer_IsADifferentKey_SoTheCategoryChoicesAreReadForIt()
    {
        var freshness = new JobHistoryChoicesFreshness(Lifetime);
        var spec = TimeRangeSpec.ForPeriod(CalendarPeriod.Today);

        Assert.Equal(1, ReadsAcross(spec, TimeZoneInfo.Utc, 3, T0, freshness, serverId: null));
        Assert.Equal(1, ReadsAcross(spec, TimeZoneInfo.Utc, 3, T0 + (Refresh * 3), freshness, serverId: 7));
        Assert.Equal(1, ReadsAcross(spec, TimeZoneInfo.Utc, 3, T0 + (Refresh * 6), freshness, serverId: 8));
        Assert.Equal(1, ReadsAcross(spec, TimeZoneInfo.Utc, 3, T0 + (Refresh * 9), freshness, serverId: null));
        Assert.NotEqual(ViewerKey(spec, T0, TimeZoneInfo.Utc, 7), ViewerKey(spec, T0, TimeZoneInfo.Utc, 8));
        Assert.NotEqual(ViewerKey(spec, T0, TimeZoneInfo.Utc, 7), ViewerKey(spec, T0, TimeZoneInfo.Utc, null));
    }

    [Fact]
    public void AReadThatNeverSucceeded_IsTriedAgainOnTheNextLoad()
    {
        /* The tab records the key only when both halves succeeded, so a failed read leaves the freshness untouched. */
        var freshness = new JobHistoryChoicesFreshness(Lifetime);
        var key = ViewerKey(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), T0, TimeZoneInfo.Utc, null);

        Assert.True(freshness.NeedsRead(key, T0));
        Assert.True(freshness.NeedsRead(key, T0 + Refresh));

        freshness.Record(key, T0 + Refresh);
        Assert.False(freshness.NeedsRead(key, T0 + (Refresh * 2)));
    }

    [Fact]
    public void ACalendarPeriod_AndASinceWindow_HaveAFixedStart_ARelativePresetAndAnUnresolvableSpecSlide()
    {
        Assert.False(ViewerTimeRangeWindow.StartSlides(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), T0, TimeZoneInfo.Utc));
        Assert.False(ViewerTimeRangeWindow.StartSlides(TimeRangeSpec.SinceInstant(T0.AddDays(-2)), T0, TimeZoneInfo.Utc));
        Assert.True(ViewerTimeRangeWindow.StartSlides(TimeRangeSpec.Relative(TimeSpan.FromHours(24)), T0, TimeZoneInfo.Utc));
        /* A "since" instant in the future does not resolve, and the window falls back to the sliding 24 hours. */
        var unresolvable = TimeRangeSpec.SinceInstant(T0.AddDays(2));
        if (!unresolvable.TryResolve(T0, TimeZoneInfo.Utc, out _, out _))
        {
            Assert.True(ViewerTimeRangeWindow.StartSlides(unresolvable, T0, TimeZoneInfo.Utc));
        }
    }

    // ── The category read is the picked server's (the bot's note) ───────────────────────────

    [Theory]
    [InlineData(false, false, "$1", "$2", null)]
    [InlineData(true, false, "$1", "$2", "$3")]
    [InlineData(false, true, "$1", "$2", "$3")]
    [InlineData(true, true, "$1", "$2", "$4")]
    public void TheCategoriesSql_NumbersItsParametersInTheOrderTheyAreBound(bool bounded, bool forServer, string start, string floor, string? last)
    {
        var sql = DarlingJobHistoryReader.BuildCategoriesSql(bounded, forServer);
        var bound = 2 + (bounded ? 1 : 0) + (forServer ? 1 : 0);

        /* The placeholders in the text are exactly $1..$N, N the number of parameters GetCategoriesAsync binds (start, floor, then the end
           when bounded, then the server when picked), and each sits on the column it is bound for. */
        var placeholders = new SortedSet<int>();
        foreach (Match m in Regex.Matches(sql, @"\$(\d+)"))
        {
            placeholders.Add(int.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture));
        }

        Assert.Equal(Enumerable.Range(1, bound), placeholders);
        Assert.Matches(@"jh\.collection_time >= " + Regex.Escape(floor), sql);
        Assert.Matches(@"jh\.run_datetime >= " + Regex.Escape(start) + @" \+", sql);

        if (bounded)
        {
            Assert.Matches(@"jh\.run_datetime <= \$3 \+", sql);
        }
        else
        {
            Assert.DoesNotMatch(@"jh\.run_datetime <=", sql);
        }

        if (forServer)
        {
            Assert.Matches(@"jh\.server_id = " + Regex.Escape(last!), sql);
        }
        else
        {
            Assert.DoesNotMatch(@"jh\.server_id = \$", sql);
        }
    }

    // ── The tabs hand the cache the real key and the picked server ──────────────────────────

    [Fact]
    public void TheViewerTab_KeysTheChoicesOnTheWindowAndServer_AndReadsTheCategoriesForThePickedServer()
    {
        var tab = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "JobHistoryTab.xaml.cs"));

        Assert.Contains("JobHistoryChoicesKey.For(", tab, StringComparison.Ordinal);
        Assert.Matches(@"windowStartUtc,\s*windowEndUtc,\s*windowIsLive,\s*serverId\)", tab);
        Assert.Contains("_choicesFreshness.NeedsRead(choicesKey, DateTime.UtcNow)", tab, StringComparison.Ordinal);
        Assert.Matches(@"GetJobHistoryCategoriesAsync\(windowStartUtc,\s*windowIsLive \? null : windowEndUtc,\s*serverId: serverId\)", tab);
        Assert.DoesNotContain("Span: windowEndUtc - windowStartUtc", tab, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLiteTab_KeysTheChoicesOnTheWindowAndServer_AndReadsTheCategoriesForThePickedServer()
    {
        var tab = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Lite", "Controls", "JobHistoryTab.xaml.cs"));

        Assert.Contains("LiteTimeRange.ChoicesKeyOf(RangePicker, startUtc, rangeEndUtc, nowUtc, serverId)", tab, StringComparison.Ordinal);
        Assert.Contains("_choicesFreshness.NeedsRead(choicesKey, DateTime.UtcNow)", tab, StringComparison.Ordinal);
        Assert.Contains("ReadKnownCategoriesAsync(startUtc, rangeEndUtc, serverId, choicesKey)", tab, StringComparison.Ordinal);
        Assert.Contains("service.GetJobHistoryCategoriesAsync(startUtc, endUtc, serverId)", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("(rangeEndUtc ?? nowUtc) - startUtc", tab, StringComparison.Ordinal);
    }
}
