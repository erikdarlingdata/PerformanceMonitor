/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5703 (review bot): Lite's Job History tab reads its Category choices once per window and server, and again only after
/// <c>ChoicesLifetime</c>, on a 30-second refresh timer. The cache was keyed on the window's SPAN, which for a live window with a fixed
/// start (Today, Week to Date, Month to Date, Year to Date, "since X") grows on every refresh, so the uncapped DISTINCT ran on every
/// tick. The key is now <see cref="LiteTimeRange.ChoicesKeyOf(ResolvedTimeRange?, DateTime, DateTime?, DateTime, int?)"/>; these tests drive
/// it through several refreshes against <see cref="JobHistoryChoicesFreshness"/>. The viewer's twin is Darling.Tests' JobHistoryChoicesCacheTests.
/// </summary>
public sealed class JobHistoryChoicesCacheTests
{
    private static readonly TimeSpan Lifetime = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan Refresh = TimeSpan.FromSeconds(30);
    private static readonly DateTime T0 = new(2026, 10, 12, 15, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The tab's key for a load of <paramref name="spec"/> at <paramref name="nowUtc"/>. The picker resolves the range at its
    /// own, slightly later, instant, as the real tab's does, so a span taken from the resolved instants would differ between loads.</summary>
    private static JobHistoryChoicesKey TabKey(TimeRangeSpec spec, DateTime nowUtc, int? serverId)
    {
        Assert.True(spec.TryResolve(nowUtc.AddMilliseconds(3), TimeZoneInfo.Utc, out var range, out _));
        var (startUtc, endUtc) = LiteTimeRange.BoundsOf(range!, nowUtc);
        return LiteTimeRange.ChoicesKeyOf(range, startUtc, endUtc, nowUtc, serverId);
    }

    private static int ReadsAcross(TimeRangeSpec spec, int refreshes, DateTime from, JobHistoryChoicesFreshness? freshness = null, int? serverId = null)
    {
        freshness ??= new JobHistoryChoicesFreshness(Lifetime);
        var reads = 0;
        for (var i = 0; i < refreshes; i++)
        {
            var now = from + (Refresh * i);
            var key = TabKey(spec, now, serverId);
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
    public void ALiveWindowWithAFixedStart_ReadsTheChoicesOnceInsideTheLifetime_AndAgainAfterIt(TimeRangeSpec spec)
    {
        var freshness = new JobHistoryChoicesFreshness(Lifetime);

        /* 9 timer ticks, 30 s apart: the last is 4 minutes after the first. */
        Assert.Equal(1, ReadsAcross(spec, 9, T0, freshness));
        /* Then 4:30, 5:00, 5:30, 6:00: the lifetime (strictly more than 5 minutes) is over at 5:30, so exactly one more read. */
        Assert.Equal(1, ReadsAcross(spec, 4, T0 + (Refresh * 9), freshness));
    }

    [Theory]
    [MemberData(nameof(LiveFixedStartWindows))]
    public void TheOldSpanKey_WasNewOnEveryTick_ForTheseWindows(TimeRangeSpec spec)
    {
        TimeSpan SpanAt(DateTime now)
        {
            Assert.True(spec.TryResolve(now, TimeZoneInfo.Utc, out var range, out _));
            var (start, end) = LiteTimeRange.BoundsOf(range!, now);
            return (end ?? now) - start;
        }

        Assert.NotEqual(SpanAt(T0), SpanAt(T0 + Refresh));
        Assert.Null(TabKey(spec, T0, null).Span);
        Assert.Equal(TabKey(spec, T0, null), TabKey(spec, T0 + Refresh, null));
    }

    [Theory]
    [InlineData(45)]
    [InlineData(60)]
    [InlineData(240)]
    [InlineData(1440)]
    public void ARelativePreset_StillReadsOnceInsideTheLifetime(int minutes)
    {
        var spec = TimeRangeSpec.Relative(TimeSpan.FromMinutes(minutes));

        Assert.Equal(1, ReadsAcross(spec, 9, T0));
        var key = TabKey(spec, T0 + Refresh, null);
        Assert.Null(key.StartUtc);
        Assert.Equal(TimeSpan.FromMinutes(minutes), key.Span);
    }

    [Fact]
    public void AChangedWindow_ReadsTheChoicesAgain_EvenInsideTheLifetime()
    {
        var freshness = new JobHistoryChoicesFreshness(Lifetime);

        Assert.Equal(1, ReadsAcross(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), 3, T0, freshness));
        Assert.Equal(1, ReadsAcross(TimeRangeSpec.ForPeriod(CalendarPeriod.MonthToDate), 3, T0 + (Refresh * 3), freshness));
        Assert.Equal(1, ReadsAcross(TimeRangeSpec.Relative(TimeSpan.FromHours(4)), 3, T0 + (Refresh * 6), freshness));
        Assert.Equal(1, ReadsAcross(TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday), 3, T0 + (Refresh * 9), freshness));
    }

    [Fact]
    public void ADifferentServer_IsADifferentKey_SoTheCategoryChoicesAreReadForIt()
    {
        var freshness = new JobHistoryChoicesFreshness(Lifetime);
        var spec = TimeRangeSpec.ForPeriod(CalendarPeriod.Today);

        Assert.Equal(1, ReadsAcross(spec, 3, T0, freshness, serverId: null));
        Assert.Equal(1, ReadsAcross(spec, 3, T0 + (Refresh * 3), freshness, serverId: 7));
        Assert.Equal(1, ReadsAcross(spec, 3, T0 + (Refresh * 6), freshness, serverId: 8));
        Assert.Equal(1, ReadsAcross(spec, 3, T0 + (Refresh * 9), freshness, serverId: null));
        Assert.NotEqual(TabKey(spec, T0, 7), TabKey(spec, T0, 8));
        Assert.NotEqual(TabKey(spec, T0, 7), TabKey(spec, T0, null));
    }

    [Fact]
    public void ANoRangePicker_IsTheSlidingTwentyFourHourFallback()
    {
        var key = LiteTimeRange.ChoicesKeyOf((ResolvedTimeRange?)null, T0.AddHours(-24), null, T0, serverId: null);

        Assert.Equal(new JobHistoryChoicesKey(null, TimeSpan.FromHours(24), null, null), key);
    }
}
