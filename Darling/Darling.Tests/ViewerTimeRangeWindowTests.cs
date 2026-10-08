/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5562: the Viewer's side of the shared time range picker. <see cref="ViewerTimeRangeWindow"/> is the pure core: the
/// window a held range reads, the whole hours the few integer readers take, the legacy preference index, what Apply to
/// All sends, and the cadence note. (A ViewerServerTab needs the app's resources, so the rules are pinned here.)
/// </summary>
public sealed class ViewerTimeRangeWindowTests
{
    private static readonly DateTime Now = new(2026, 10, 8, 15, 0, 0, DateTimeKind.Unspecified);
    private static readonly TimeZoneInfo Eastern = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");

    [Theory]
    [InlineData(0, "1h")]
    [InlineData(1, "4h")]
    [InlineData(2, "12h")]
    [InlineData(3, "1d")]
    [InlineData(4, "1w")]
    [InlineData(5, "1d")]
    [InlineData(-1, "1d")]
    public void LegacyIndex_MapsToTheRangeItAlwaysMeant(int index, string expectedId)
    {
        Assert.Equal(expectedId, ViewerTimeRangeWindow.FromLegacyIndex(index).Id);
    }

    [Fact]
    public void ACalendarPeriod_FlowsThroughTheWindow_InTheDisplayZone()
    {
        var (start, end, live) = ViewerTimeRangeWindow.Window(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), Now, Eastern);

        Assert.Equal(new DateTime(2026, 10, 8, 4, 0, 0), start);   // local midnight, EDT
        Assert.Equal(Now, end);
        Assert.True(live);
    }

    [Fact]
    public void AFinishedPeriod_KeepsItsInstants_AndIsNotLive()
    {
        var (start, end, live) = ViewerTimeRangeWindow.Window(TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday), Now, Eastern);

        Assert.Equal(new DateTime(2026, 10, 7, 4, 0, 0), start);
        Assert.Equal(new DateTime(2026, 10, 8, 4, 0, 0), end);
        Assert.False(live);
    }

    [Theory]
    [InlineData(5, 1)]
    [InlineData(45, 1)]
    [InlineData(60, 1)]
    [InlineData(61, 2)]
    [InlineData(24 * 60, 24)]
    public void ASubHourSpan_FlowsThroughTheWindow_AndRoundsUpToWholeHoursOnlyForTheIntegerReaders(int minutes, int expectedHours)
    {
        var (start, end, _) = ViewerTimeRangeWindow.Window(TimeRangeSpec.Relative(TimeSpan.FromMinutes(minutes)), Now, Eastern);

        Assert.Equal(TimeSpan.FromMinutes(minutes), end - start);
        Assert.Equal(expectedHours, ViewerTimeRangeWindow.HoursBack(start, end));
    }

    [Fact]
    public void ARangeThatCannotResolveYet_FallsBackToTwentyFourHours()
    {
        // Midnight plus one minute: Today is under the 5-minute floor.
        var justAfterMidnight = new DateTime(2026, 10, 8, 4, 1, 0, DateTimeKind.Unspecified);
        var (start, end, live) = ViewerTimeRangeWindow.Window(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), justAfterMidnight, Eastern);

        Assert.Equal(TimeSpan.FromHours(24), end - start);
        Assert.True(live);
    }

    [Fact]
    public void ApplyToAll_SendsACalendarPeriodAsTheInstantsItNames_AndLeavesARollingRangeAlone()
    {
        var rolling = TimeRangeSpec.Relative(TimeSpan.FromHours(4));
        Assert.Same(rolling, ViewerTimeRangeWindow.ForBroadcast(rolling, Now, Eastern));

        var today = ViewerTimeRangeWindow.ForBroadcast(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), Now, Eastern);
        Assert.Equal(TimeRangeKind.Since, today.Kind);
        Assert.Equal(new DateTime(2026, 10, 8, 4, 0, 0), today.StartUtc);

        var yesterday = ViewerTimeRangeWindow.ForBroadcast(TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday), Now, Eastern);
        Assert.Equal(TimeRangeKind.Fixed, yesterday.Kind);
        Assert.Equal(new DateTime(2026, 10, 7, 4, 0, 0), yesterday.StartUtc);
        Assert.Equal(new DateTime(2026, 10, 8, 4, 0, 0), yesterday.EndUtc);
    }

    [Fact]
    public void Preferences_LegacyIndexOnly_MapsToTheNewStringId()
    {
        var prefs = new ViewerPreferences { DefaultTimeRangeIndex = 4 }.Normalize();

        Assert.Equal("1w", prefs.DefaultTimeRange);
        Assert.Equal(4, prefs.DefaultTimeRangeIndex);
    }

    [Fact]
    public void Preferences_APeriodId_WinsOverTheLegacyIndex_AndAFixedRangeIsNeverKept()
    {
        var period = new ViewerPreferences { DefaultTimeRange = "previous-week", DefaultTimeRangeIndex = 0 }.Normalize();
        Assert.Equal("previous-week", period.DefaultTimeRange);
        Assert.Equal(3, period.DefaultTimeRangeIndex);   // the nearest the legacy combo could hold: its default

        var fixedRange = new ViewerPreferences
        {
            DefaultTimeRange = "fixed:2026-10-01T04:00:00Z/2026-10-02T04:00:00Z",
            DefaultTimeRangeIndex = 1,
        }.Normalize();
        Assert.Equal("4h", fixedRange.DefaultTimeRange);

        var unknown = new ViewerPreferences { DefaultTimeRange = "nonsense" }.Normalize();
        Assert.Equal("1d", unknown.DefaultTimeRange);
    }

    [Theory]
    [InlineData("Overview", "cpu_utilization")]
    [InlineData("Wait Stats", "wait_stats")]
    [InlineData("Queries", "query_stats")]
    [InlineData("Plan Viewer", null)]
    public void TheMainCollector_IsNamedByTheInnerTab(string header, string? expected)
    {
        Assert.Equal(expected, ViewerTimeRangeWindow.MainCollectorFor(header));
    }

    [Fact]
    public void TheSampleInterval_IsTheShippedCadence_WhenNothingOverridesIt()
    {
        Assert.Equal(TimeSpan.FromMinutes(1), ViewerTimeRangeWindow.SampleIntervalFor("wait_stats", 7, Array.Empty<CollectorScheduleRow>()));
        Assert.Null(ViewerTimeRangeWindow.SampleIntervalFor(null, 7, null));
        Assert.Null(ViewerTimeRangeWindow.SampleIntervalFor("not_a_collector", 7, null));
    }

    [Fact]
    public void ADataStart_ShowsTheNoteOnThePicker_OnlyWhenTheRangeStartsWellBeforeIt()
    {
        var range = TimeRangeSpec.Relative(TimeSpan.FromDays(2));
        Assert.True(range.TryResolve(Now, Eastern, out var resolved, out _));

        Assert.NotNull(TimeRangeNotes.DataStartNote(resolved!, Now.AddHours(-3)));
        Assert.Null(TimeRangeNotes.DataStartNote(resolved!, Now.AddDays(-3)));
    }
}
