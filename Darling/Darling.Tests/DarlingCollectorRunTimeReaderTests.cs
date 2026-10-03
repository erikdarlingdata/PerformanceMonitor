/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4938: the rules <c>get_collection_health</c> applies to show a collector's run time, with no store. The time and the
/// rows are passed in, so each rule is pinned at a fixed instant: the server row over the fleet row, -1 and NULL on the
/// server row, the whole-day rule, the server's clock (UTC while it is unknown), a disabled collector, and the note a
/// skipped day carries.
/// </summary>
public sealed class DarlingCollectorRunTimeReaderTests
{
    private const string Daily = "index_object_stats";
    private const string Minute = "wait_stats";
    private const string FiveMinutes = "running_jobs";
    private const int ServerId = 7;
    private const int TwoAm = 120;

    /* Noon UTC. A daily slot at 02:00 plus a spread under an hour has passed by ten hours, outside its grace. */
    private static readonly DateTime Now = new(2026, 6, 10, 12, 0, 0, DateTimeKind.Utc);

    private static ScheduleOverride Fleet(string collector, int? runAt, bool enabled = true, int? frequency = null) =>
        new(null, collector, frequency, null, enabled, RunAtMinute: runAt);

    private static ScheduleOverride Server(string collector, int? runAt, bool enabled = true, int? frequency = null) =>
        new(ServerId, collector, frequency, null, enabled, RunAtMinute: runAt);

    private static CollectorHealth Row(string collector, DateTime? lastRun) =>
        new() { CollectorName = collector, TotalRuns = 1, SuccessCount = 1, LastRunTime = lastRun, LastSuccessTime = lastRun };

    private static IReadOnlyDictionary<string, CollectorRunTimeReading> Compute(
        IReadOnlyList<ScheduleOverride> overrides, CollectorHealth row, Func<DateTime, DateTime>? localToUtc = null) =>
        DarlingCollectorRunTimeReader.Compute(ServerId, overrides, [row], localToUtc ?? CollectorRunTime.LocalIsUtc, Now);

    private static DateTime Ago(double hours) => DateTime.SpecifyKind(Now.AddHours(-hours), DateTimeKind.Unspecified);

    [Fact]
    public void TheServerRowWinsOverTheFleetRow()
    {
        var readings = Compute([Fleet(Daily, TwoAm), Server(Daily, 270)], Row(Daily, Ago(1)));

        Assert.Equal("04:30", readings[Daily].RunAt);
    }

    [Fact]
    public void MinusOneOnTheServerRowStopsTheFleetRunTime()
    {
        var readings = Compute([Fleet(Daily, TwoAm), Server(Daily, -1)], Row(Daily, Ago(1)));

        Assert.False(readings.ContainsKey(Daily));
    }

    [Fact]
    public void ANullRunTimeOnTheServerRowFallsThroughToTheFleetRow()
    {
        var readings = Compute([Fleet(Daily, TwoAm), Server(Daily, null, enabled: true)], Row(Daily, Ago(1)));

        Assert.Equal("02:00", readings[Daily].RunAt);
    }

    [Fact]
    public void ACollectorWithNoRunTimeHasNoReading()
    {
        Assert.Empty(Compute([], Row(Daily, Ago(1))));
        Assert.Empty(Compute([Fleet(Daily, -1)], Row(Daily, Ago(1))));
    }

    [Fact]
    public void ARunTimeOnACollectorThatIsNotOnWholeDaysIsIgnored()
    {
        /* wait_stats runs every minute: a run time there would only pick the minute past the hour. */
        Assert.Empty(Compute([Fleet(Minute, TwoAm)], Row(Minute, Ago(0.01))));

        /* A collector on a five-minute cadence takes a run time once its frequency moves to a whole day. */
        Assert.Empty(Compute([Fleet(FiveMinutes, TwoAm)], Row(FiveMinutes, Ago(0.1))));
        var readings = Compute([Fleet(FiveMinutes, TwoAm, frequency: 1440)], Row(FiveMinutes, Ago(1)));
        Assert.Equal("02:00", readings[FiveMinutes].RunAt);
    }

    [Fact]
    public void ACollectorTheCatalogDoesNotKnowHasNoReading()
    {
        Assert.Empty(Compute([Fleet("no_such_collector", TwoAm)], Row("no_such_collector", Ago(1))));
    }

    [Fact]
    public void WithNoClockTheRunTimeReadsAsUtc_AndAFixedOffsetMovesItByTheOffset()
    {
        var utc = Compute([Fleet(Daily, TwoAm)], Row(Daily, Ago(1)))[Daily].NextRunUtc!.Value;
        Assert.Equal(DateTimeKind.Utc, utc.Kind);
        Assert.Equal(TimeSpan.FromMinutes(TwoAm) + CollectorRunTime.Spread(ServerId), utc.TimeOfDay);
        Assert.True(utc > Now);

        /* Five hours behind UTC: 02:00 there is 07:00 UTC. */
        var west = Compute([Fleet(Daily, TwoAm)], Row(Daily, Ago(1)), ServerClock.Resolve(null, -300).ToUtc)[Daily].NextRunUtc!.Value;
        Assert.Equal(TimeSpan.FromHours(7) + CollectorRunTime.Spread(ServerId), west.TimeOfDay);
    }

    [Fact]
    public void ADisabledCollectorShowsItsRunTimeAndNoNextRun()
    {
        var reading = Compute([Fleet(Daily, TwoAm, enabled: false)], Row(Daily, Ago(40)))[Daily];

        Assert.Equal("02:00", reading.RunAt);
        Assert.Null(reading.NextRunUtc);
        Assert.Null(reading.NextRunUtcText);
        Assert.Null(reading.SkippedDayNote);
    }

    [Theory]
    [InlineData(1, false)]    // ran this morning: on time
    [InlineData(20, false)]   // ran yesterday: on time
    [InlineData(30, false)]   // a day late but still under the 36-hour stale line: not stale yet, so no note
    [InlineData(40, true)]    // a skipped day past the 36-hour stale line, with the next slot ahead
    [InlineData(60, true)]    // two days: the last run is also older than the worker's two-day floor, which makes no difference to the note
    public void ASkippedDayCarriesANoteOnlyOncePastTheStaleLineWithTheNextSlotAhead(double hoursSinceLastRun, bool expectNote)
    {
        var reading = Compute([Fleet(Daily, TwoAm)], Row(Daily, Ago(hoursSinceLastRun)))[Daily];

        Assert.True(reading.NextRunUtc > Now, "the next slot is ahead");
        if (!expectNote)
        {
            Assert.Null(reading.SkippedDayNote);
            return;
        }

        Assert.NotNull(reading.SkippedDayNote);
        Assert.Contains("Skipped day", reading.SkippedDayNote, StringComparison.Ordinal);
        Assert.Contains("36-hour stale line", reading.SkippedDayNote, StringComparison.Ordinal);
        Assert.Contains($"no run for {hoursSinceLastRun:0.#} hours", reading.SkippedDayNote, StringComparison.Ordinal);
        Assert.Contains(reading.NextRunUtc!.Value.ToString("u", System.Globalization.CultureInfo.InvariantCulture), reading.SkippedDayNote, StringComparison.Ordinal);
        Assert.Contains("not replayed", reading.SkippedDayNote, StringComparison.Ordinal);
    }

    [Fact]
    public void ACollectorDueNowHasNoNote_BecauseItsRunIsAMomentAway()
    {
        /* A run time whose slot for this server passed about ten minutes ago: noon is inside its 60-minute grace. */
        var spreadMinutes = (int)CollectorRunTime.Spread(ServerId).TotalMinutes;
        var runAt = 12 * 60 - spreadMinutes - 10;

        var reading = Compute([Fleet(Daily, runAt)], Row(Daily, Ago(40)))[Daily];

        Assert.Equal(Now, reading.NextRunUtc);
        Assert.Null(reading.SkippedDayNote);
    }

    [Fact]
    public void ADailyIntervalChosenOnAFasterShippedCadence_IsNotASkippedDayOnAnOrdinaryDay()
    {
        /* running_jobs ships on a five-minute cadence, so the health band calls it stale after four hours. Moved to a daily
           run time, ten hours since its last run is just a day's wait, not a skipped slot. */
        var ordinary = Compute([Fleet(FiveMinutes, TwoAm, frequency: 1440)], Row(FiveMinutes, Ago(10)))[FiveMinutes];
        Assert.Null(ordinary.SkippedDayNote);

        /* A day and a quarter is past one interval plus its grace, so a slot really was missed. */
        var skipped = Compute([Fleet(FiveMinutes, TwoAm, frequency: 1440)], Row(FiveMinutes, Ago(30)))[FiveMinutes];
        Assert.NotNull(skipped.SkippedDayNote);
    }

    [Fact]
    public void TheRunTimeIsNeverABandInput()
    {
        /* HealthStatus reads the real clock, so this row and the call use it too. */
        var realNow = DateTime.UtcNow;
        var row = Row(Daily, DateTime.SpecifyKind(realNow.AddHours(-40), DateTimeKind.Unspecified));
        var bandBefore = row.HealthStatus;

        _ = DarlingCollectorRunTimeReader.Compute(ServerId, [Fleet(Daily, TwoAm)], [row], CollectorRunTime.LocalIsUtc, realNow);

        Assert.Equal(bandBefore, row.HealthStatus);
        Assert.Equal(PerformanceMonitor.Common.CollectorHealthClassifier.Stale, bandBefore);
    }
}
