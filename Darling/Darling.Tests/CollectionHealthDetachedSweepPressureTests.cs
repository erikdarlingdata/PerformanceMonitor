/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4999 (part of #4938): get_collection_health's sweep-pressure roll-up compares the collectors' single-run
/// costs with the one 60-second body their server's sequential pass has to fit in, so it may count only the
/// collectors that run IN that pass. A collector that runs detached does not: it runs beside the pass, and a
/// long p95 on it said BODY_OVERRUN for a body that never waited on it (index_object_stats, once a day, naming
/// itself as the collector that owns the body).
///
/// <para>The decision is <see cref="DarlingWorker.RunsDetached"/>, which the worker's dispatch also asks, so the
/// two cannot disagree about what runs in the pass. The source pins below are what keep it that way: a second,
/// hand-copied version of the test in either file is a drift the behavioural cases cannot see.</para>
/// </summary>
public sealed class CollectionHealthDetachedSweepPressureTests
{
    private const string Daily = "index_object_stats";
    private const string OneMinute = "wait_stats";

    /// <summary>One run costs more than the whole 60,000 ms body, so a collector counted in the body reads BODY_OVERRUN.</summary>
    private const double LongP95Ms = 90_000;

    private const string WorkerPath = "Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs";
    private const string ToolsPath = "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs";

    private static CollectorHealth Row(string name, double avgMs, double p95Ms) =>
        new() { CollectorName = name, AvgDurationMs = avgMs, P95DurationMs = p95Ms };

    private static SweepPressure Rollup(params CollectorHealth[] rows) =>
        SweepPressureClassifier.Compute(DarlingMcpDataTools.SweepBodyCollectors(rows));

    [Fact]
    public void ADailyCollectorWithALongP95_IsNotChargedToTheSweepBody()
    {
        var rows = new[]
        {
            Row(Daily, avgMs: 40_000, p95Ms: LongP95Ms),
            Row(OneMinute, avgMs: 50, p95Ms: 100),
        };

        /* The fixture is a real overrun when the daily collector IS counted: this is what the tool said before. */
        var counted = SweepPressureClassifier.Compute(
            rows.Select(r => (r.CollectorName, r.AvgDurationMs, r.P95DurationMs, r.FrequencyMinutes)));
        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, counted.PeakCycleRisk);
        Assert.Equal(Daily, counted.PeakCollectorName);

        var pressure = Rollup(rows);
        Assert.Equal(SweepPressureClassifier.PeakCycleFits, pressure.PeakCycleRisk);
        Assert.Equal(OneMinute, pressure.PeakCollectorName);
    }

    [Fact]
    public void AOneMinuteCollectorWithTheSameP95_StillReadsBodyOverrun()
    {
        Assert.Equal(1, CollectorScheduleDefaults.All[OneMinute].FrequencyMinutes);

        var pressure = Rollup(
            Row(OneMinute, avgMs: 40_000, p95Ms: LongP95Ms),
            Row(Daily, avgMs: 50, p95Ms: 100));

        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, pressure.PeakCycleRisk);
        Assert.Equal(OneMinute, pressure.PeakCollectorName);
    }

    /// <summary>
    /// The three that were detached before the daily ones (by name, on a five-minute cadence) and one of each
    /// kind of daily collector: a catalog 1440-minute one, and an on-load one whose catalog 0 is its daily
    /// recapture. None of them runs in the pass, so none is charged to it.
    /// </summary>
    [Theory]
    [InlineData("query_store")]
    [InlineData("plan_correction")]
    [InlineData("pg_wait_sampling")]
    [InlineData("index_object_stats")]
    [InlineData("pg_index_bloat")]
    [InlineData("server_config")]
    public void ACollectorThatRunsDetached_IsLeftOutOfTheRollUp(string name)
    {
        Assert.True(CollectorScheduleDefaults.All.ContainsKey(name), $"'{name}' is not in the catalog");

        var pressure = Rollup(Row(name, avgMs: 40_000, p95Ms: LongP95Ms));

        Assert.Equal(SweepPressureClassifier.PeakCycleFits, pressure.PeakCycleRisk);
        Assert.Null(pressure.PeakCollectorName);
        Assert.Equal(0, pressure.BusyMsPerMinute);
    }

    /// <summary>
    /// Over the whole catalog, from the catalog's own cadences rather than from the predicate under test: a row
    /// is left out of the roll-up exactly when its collector is detached by name or runs at a daily cadence or
    /// slower, and kept otherwise.
    /// </summary>
    [Fact]
    public void TheRollUpKeepsExactlyTheCollectorsThatRunInThePass()
    {
        foreach (var (name, schedule) in CollectorScheduleDefaults.All)
        {
            var byName = name is "query_store" or "plan_correction" or "pg_wait_sampling";
            var effective = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(schedule.FrequencyMinutes);
            var expectedInPass = !byName && effective < 1440;

            var kept = DarlingMcpDataTools.SweepBodyRows(new[] { Row(name, 1, 1) }).Any();

            Assert.True(
                expectedInPass == kept,
                $"{name} (every {effective} min, detached by name: {byName}): in the pass = {expectedInPass}, kept in the roll-up = {kept}");
        }
    }

    /// <summary>
    /// The worker's dispatch asks <c>RunsDetached</c>, and this tool asks it too, with no second copy of the test
    /// on either side. A hand-copied version passes every behavioural case above until the day one copy is
    /// edited, which is the drift this pin exists to refuse.
    /// </summary>
    [Fact]
    public void TheWorkersDispatchAndTheHealthToolAskTheSameTest()
    {
        var worker = ReadRepoFile(WorkerPath);
        var tools = ReadRepoFile(ToolsPath);

        /* The test is defined once, from its two halves. */
        Assert.Contains(
            "IsDetachedByName(name) || IsDailyInterval(effectiveIntervalMinutes)", worker, StringComparison.Ordinal);
        Assert.Equal(1, Count(worker, "internal static bool RunsDetached("));

        /* The dispatch decides with it. */
        Assert.Equal(1, Count(worker, "if (!RunsDetached(name, interval))"));

        /* The three by-name predicates are chained in ONE place, IsDetachedByName, and nowhere else in the worker
           under the dispatch's variable name: a copy of the chain in the dispatch is the second copy. */
        Assert.Equal(1, Count(worker, "IsQueryStoreCollector(name) || IsPlanCorrectionCollector(name) || IsPgWaitSamplingCollector(name)"));

        /* The tool asks it through the worker, once, and does not name any piece of it. */
        Assert.Equal(1, Count(tools, "DarlingWorker.RunsDetached("));
        foreach (var piece in new[]
                 {
                     "IsQueryStoreCollector", "IsPlanCorrectionCollector", "IsPgWaitSamplingCollector", "IsDailyInterval",
                     "IsDetachedByName", "DailyCollectorIntervalMinutes",
                 })
        {
            Assert.DoesNotContain(piece, tools, StringComparison.Ordinal);
        }

        /* Both readings of the body's population, the roll-up and the heaviest list, come through the one filter. */
        Assert.Contains(
            "var pressure = SweepPressureClassifier.Compute(SweepBodyCollectors(rows));", tools, StringComparison.Ordinal);
        Assert.Contains("var heaviest = SweepBodyRows(rows)", tools, StringComparison.Ordinal);
    }

    /// <summary>
    /// The tool's own description says what the roll-up counts. It named "the collectors" without limit, which a
    /// reader takes literally: a daily collector's long run in a BODY_OVERRUN is the reading this change removes.
    /// </summary>
    [Fact]
    public void TheToolDescriptionSaysADetachedCollectorIsNotCountedInTheRollUp()
    {
        var method = typeof(DarlingMcpDataTools).GetMethod(nameof(DarlingMcpDataTools.GetCollectionHealth))!;
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.Contains("runs detached from the sweep body", description, StringComparison.Ordinal);
        Assert.Contains("leave it out", description, StringComparison.Ordinal);
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
