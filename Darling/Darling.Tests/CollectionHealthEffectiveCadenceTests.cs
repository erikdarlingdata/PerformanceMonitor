/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4999 (part of #4938): get_collection_health judges a collector against the interval it is SCHEDULED at on the
/// server, not the one it shipped with. A collector an operator moved to every 720 minutes was banded against its
/// shipped five, and a collector an override moved across the daily line stayed in (or out of) the sweep-pressure
/// roll-up by its shipped cadence while the worker's dispatch had already moved it.
///
/// <para>The resolution is the worker's own, <see cref="StoreConfigProvider.ResolveSchedule"/>: a per-server
/// override, else the fleet-wide one, else the shipped default. The tests apply a set of overrides to rows with
/// <see cref="DarlingDataReader.ApplyScheduledFrequencies"/>, the step the memoized health read runs on every row it
/// builds, so no store is needed.</para>
/// </summary>
public sealed class CollectionHealthEffectiveCadenceTests
{
    private const string FiveMinute = "memory_clerks";
    private const string Daily = "index_object_stats";
    private const int ServerId = 7;
    private const int OtherServerId = 8;

    private const string ReaderPath = "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingDataReader.cs";
    private const string WorkerPath = "Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs";

    private static CollectorHealth Row(string name, double avgMs = 0, double p95Ms = 0) =>
        new() { CollectorName = name, AvgDurationMs = avgMs, P95DurationMs = p95Ms };

    private static ScheduleOverride Override(int? serverId, string name, int? minutes) =>
        new(serverId, name, minutes, RetentionDays: null, Enabled: true);

    private static void Apply(CollectorHealth row, params ScheduleOverride[] overrides) =>
        DarlingDataReader.ApplyScheduledFrequencies(new[] { row }, ServerId, overrides);

    /// <summary>A row that bands HEALTHY on any cadence while its newest success is recent enough.</summary>
    private static CollectorHealth ProductiveRow(string name, double hoursSinceLastSuccess)
    {
        var last = DateTime.UtcNow.AddHours(-hoursSinceLastSuccess);
        return new CollectorHealth
        {
            CollectorName = name,
            TotalRuns = 10,
            SuccessCount = 10,
            RowsStored = 100,
            RunsWithRows = 10,
            LastSuccessTime = last,
            LastRunTime = last,
        };
    }

    [Fact]
    public void AnOverrideOf720OnAFiveMinuteCollector_IsJudgedAgainst720()
    {
        Assert.Equal(5, CollectorScheduleDefaults.All[FiveMinute].FrequencyMinutes);

        var row = Row(FiveMinute, avgMs: 30_000, p95Ms: 30_000);
        Assert.Equal(5, row.FrequencyMinutes);

        Apply(row, Override(ServerId, FiveMinute, 720));

        Assert.Equal(720, row.FrequencyMinutes);

        /* The roll-up amortises the run cost by the interval the row carries: 30,000 ms every 720 minutes. */
        var pressure = SweepPressureClassifier.Compute(DarlingMcpDataTools.SweepBodyCollectors(new[] { row }));
        Assert.Equal(30_000.0 / 720, pressure.BusyMsPerMinute, 3);
        Assert.Equal(720, pressure.PeakCollectorFrequencyMinutes);
    }

    [Fact]
    public void TheBandJudgesAgainstTheScheduledInterval_NotTheShippedOne()
    {
        /* Six hours since the newest success: past the four-hour floor a five-minute collector goes STALE at, well
           inside the 18 hours (one and a half intervals) a 720-minute collector gets. */
        var shipped = ProductiveRow(FiveMinute, hoursSinceLastSuccess: 6);
        Assert.Equal(CollectorHealthClassifier.Stale, shipped.HealthStatus);

        var scheduled = ProductiveRow(FiveMinute, hoursSinceLastSuccess: 6);
        Apply(scheduled, Override(ServerId, FiveMinute, 720));
        Assert.Equal(CollectorHealthClassifier.Healthy, scheduled.HealthStatus);
    }

    [Fact]
    public void APerServerOverrideBeatsTheFleetOne_AndAnotherServersIsIgnored()
    {
        var fleetOnly = Row(FiveMinute);
        Apply(fleetOnly, Override(null, FiveMinute, 720));
        Assert.Equal(720, fleetOnly.FrequencyMinutes);

        var both = Row(FiveMinute);
        Apply(both, Override(null, FiveMinute, 720), Override(ServerId, FiveMinute, 60));
        Assert.Equal(60, both.FrequencyMinutes);

        var elsewhere = Row(FiveMinute);
        Apply(elsewhere, Override(OtherServerId, FiveMinute, 720));
        Assert.Equal(5, elsewhere.FrequencyMinutes);
    }

    [Fact]
    public void AnOverrideThatMovesACollectorAcrossTheDailyLine_MovesItInTheRollUpToo()
    {
        const double LongP95Ms = 90_000;

        /* A five-minute collector scheduled daily runs beside the pass: its long run is not charged to the body. */
        var movedOut = Row(FiveMinute, avgMs: 40_000, p95Ms: LongP95Ms);
        Assert.Equal(
            SweepPressureClassifier.PeakCycleBodyOverrun,
            SweepPressureClassifier.Compute(DarlingMcpDataTools.SweepBodyCollectors(new[] { movedOut })).PeakCycleRisk);
        Apply(movedOut, Override(ServerId, FiveMinute, 1440));
        var outside = SweepPressureClassifier.Compute(DarlingMcpDataTools.SweepBodyCollectors(new[] { movedOut }));
        Assert.Equal(SweepPressureClassifier.PeakCycleFits, outside.PeakCycleRisk);
        Assert.Null(outside.PeakCollectorName);

        /* The reverse: a daily collector scheduled every five minutes runs in the pass, so its long run is. */
        var movedIn = Row(Daily, avgMs: 40_000, p95Ms: LongP95Ms);
        Assert.Equal(
            SweepPressureClassifier.PeakCycleFits,
            SweepPressureClassifier.Compute(DarlingMcpDataTools.SweepBodyCollectors(new[] { movedIn })).PeakCycleRisk);
        Apply(movedIn, Override(ServerId, Daily, 5));
        var inside = SweepPressureClassifier.Compute(DarlingMcpDataTools.SweepBodyCollectors(new[] { movedIn }));
        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, inside.PeakCycleRisk);
        Assert.Equal(Daily, inside.PeakCollectorName);
    }

    [Fact]
    public void WithNoOverrideARowKeepsItsShippedCadence_AndAnUnknownNameIsLeftAlone()
    {
        var known = Row(FiveMinute);
        Apply(known);
        Assert.Equal(5, known.FrequencyMinutes);

        /* An on-load collector's catalog 0 is its daily recapture, the substitution the dispatch makes. */
        var onLoad = Row("server_config");
        Apply(onLoad);
        Assert.Equal(CollectorScheduleDefaults.OnLoadRecaptureMinutes, onLoad.FrequencyMinutes);

        /* A name the catalog does not know would throw in ResolveSchedule; it stays unstamped at 0. */
        var unknown = Row("not_a_collector");
        Apply(unknown, Override(ServerId, "not_a_collector", 720));
        Assert.Null(unknown.EffectiveFrequencyMinutes);
        Assert.Equal(0, unknown.FrequencyMinutes);
    }

    /// <summary>
    /// The resolution is the worker's own call and not a copy of it, and it runs inside the memoized read: the
    /// memo hands one list to every caller of the minute, so a stamp applied after it returned would mutate rows
    /// other callers are enumerating.
    /// </summary>
    [Fact]
    public void TheReadResolvesThroughTheWorkersCall_InsideTheMemoizedRead()
    {
        var reader = ReadRepoFile(ReaderPath);
        var worker = ReadRepoFile(WorkerPath);
        var tools = ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs");

        Assert.Contains("StoreConfigProvider.ResolveSchedule(name, runtime.ServerId, _scheduleOverrides)", worker, StringComparison.Ordinal);
        Assert.Equal(1, Count(reader, "StoreConfigProvider.ResolveSchedule(row.CollectorName, serverId, overrides)"));

        /* One stamp, in the method that builds the rows, ahead of its return. */
        Assert.Equal(1, Count(reader, "ApplyScheduledFrequencies(rows, serverId, scheduleOverrides);"));
        Assert.True(
            reader.IndexOf("ApplyScheduledFrequencies(rows, serverId, scheduleOverrides);", StringComparison.Ordinal)
            < reader.IndexOf("internal const string ScheduleOverridesSql", StringComparison.Ordinal));

        /* The tool itself neither stamps nor resolves: it reads the row's interval. */
        Assert.DoesNotContain("ApplyScheduledFrequencies", tools, StringComparison.Ordinal);
        Assert.DoesNotContain("ResolveSchedule", tools, StringComparison.Ordinal);

        /* The overrides come from the sparse schedule table, schema-qualified, this server's rows and the fleet's. */
        Assert.Contains("FROM config.config_collector_schedules", DarlingDataReader.ScheduleOverridesSql, StringComparison.Ordinal);
        Assert.Contains("server_id IS NULL", DarlingDataReader.ScheduleOverridesSql, StringComparison.Ordinal);
    }

    private static int Count(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
