/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5824: the scheduled analysis pass's <c>collection_log</c> row, the pass as a cause in the skipped-slots row, and the
/// skipped-slots message when the per-collector counts tie.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class AnalysisPassRowTests
{
    private static readonly DateTime T0 = new(2026, 10, 14, 21, 30, 0, DateTimeKind.Utc);

    /// <summary>The row is never SUCCESS: CollectionCycleReaderCensusTests accepts a read that keeps only
    /// <c>status = 'SUCCESS'</c> (or PERMISSIONS / SESSION_MISSING) without the cycle-row predicate, so a pass status from
    /// that set would be counted as a collector run by those reads. Every other read goes through NotCycleRowSql, which
    /// names the pass.</summary>
    [Fact]
    public void ThePassNeverWritesAStatusTheCensusTreatsAsSafeUnfiltered()
    {
        var written = Enum.GetValues<DarlingWorker.AnalysisPassStatus>()
            .Select(s => DarlingWorker.DescribeAnalysisPass(s, 0, null, 1_000, null, insideFleetSlot: true).Status)
            .Distinct()
            .ToList();

        Assert.Equal(
            new[] { AnalysisPassRow.Abandoned, AnalysisPassRow.Analyzed, AnalysisPassRow.Error, AnalysisPassRow.Skipped },
            written.OrderBy(s => s, StringComparer.Ordinal).ToArray());
        foreach (var safeUnfiltered in new[] { "SUCCESS", "PERMISSIONS", "SESSION_MISSING" })
        {
            Assert.DoesNotContain(safeUnfiltered, written);
        }

        Assert.Contains("'" + AnalysisPassRow.CollectorName + "'", CollectionCycleRow.NotCycleRowSql, StringComparison.Ordinal);
        Assert.Contains("'" + CollectionCycleRow.CollectorName + "'", CollectionCycleRow.NotCycleRowSql, StringComparison.Ordinal);
    }

    /// <summary>The C# twin of NotCycleRowSql: get_collection_log's run_count is counted in C# over the page, which the census
    /// (it grades SQL text) cannot reach. A page of a cycle row, an analysis_pass row and two collector rows is two runs; the
    /// live count at both readers is pinned by <c>Group8_CollectionLog_ShowsTheCycleRow_ButCountsItApartFromTheRuns</c>. No
    /// file compares a row against the cycle row's name alone to decide what a run is.</summary>
    [Fact]
    public void ACountOfRunsInCSharp_LeavesOutTheCycleRowAndTheAnalysisPassRow()
    {
        var page = new[] { CollectionCycleRow.CollectorName, AnalysisPassRow.CollectorName, "wait_stats", "memory_stats" };
        Assert.Equal(2, page.Count(CollectionCycleRow.IsCollectorRun));
        Assert.Equal(["wait_stats", "memory_stats"], page.Where(CollectionCycleRow.IsCollectorRun).ToArray());

        var root = Path.Combine(CycleSkipFeedTests.RepoRoot(), "Darling");
        var offenders = new List<string>();
        foreach (var project in new[] { "PerformanceMonitor.Darling.Service", "PerformanceMonitor.Darling.Storage", "PerformanceMonitor.Darling.Viewer" })
        {
            foreach (var file in Directory.EnumerateFiles(Path.Combine(root, project), "*.cs", SearchOption.AllDirectories))
            {
                var rel = file.Replace('\\', '/');
                if (rel.Contains("/obj/", StringComparison.Ordinal) || rel.Contains("/bin/", StringComparison.Ordinal))
                {
                    continue;
                }

                if (File.ReadAllText(file).Contains("!= CollectionCycleRow.CollectorName", StringComparison.Ordinal))
                {
                    offenders.Add(Path.GetFileName(file));
                }
            }
        }

        Assert.True(offenders.Count == 0, "a run is decided by '!= CollectionCycleRow.CollectorName' in " + string.Join(", ", offenders)
            + "; use CollectionCycleRow.IsCollectorRun, which also leaves out the analysis_pass row");
    }

    /// <summary>The PLAN_REGRESSION read time is read off a pass only once its task has finished, whatever its status: a pass
    /// that ran past its budget, unwound and came back (TimedOut) keeps its real figure; a pass the sweep stopped waiting for
    /// may still be running, so it reads null and the figure is not read at all.</summary>
    [Fact]
    public void TheDetail_NamesTheFindings_ThePlanRegressionRead_AndTheSlot()
    {
        var (status, detail) = DarlingWorker.DescribeAnalysisPass(
            DarlingWorker.AnalysisPassStatus.Ran, 7, null, 84_000, 61_500, insideFleetSlot: true);
        Assert.Equal(AnalysisPassRow.Analyzed, status);
        Assert.Equal(
            "Scheduled analysis pass ran in 84s, 7 finding(s), PLAN_REGRESSION read 62s, inside this server's fleet collection slot.",
            detail);

        // Ran past its budget and finished: the task completed, so the figure the service stamped survives.
        var finishedRead = DarlingWorker.PlanRegressionReadOfFinishedPass(System.Threading.Tasks.Task.CompletedTask, () => 61_500);
        Assert.Equal(61_500, finishedRead);
        var (timedOut, timedOutDetail) = DarlingWorker.DescribeAnalysisPass(
            DarlingWorker.AnalysisPassStatus.TimedOut, 0, "analysis exceeded 120s", 125_000, finishedRead, insideFleetSlot: true);
        Assert.Equal(AnalysisPassRow.Abandoned, timedOut);
        Assert.Contains("timed out in 125s, 0 finding(s), PLAN_REGRESSION read 62s,", timedOutDetail, StringComparison.Ordinal);
        Assert.EndsWith(" analysis exceeded 120s", timedOutDetail, StringComparison.Ordinal);

        // Still running when the sweep gave up: null, and the service is not read.
        var reads = 0;
        var stillRunning = new System.Threading.Tasks.TaskCompletionSource().Task;
        var runningRead = DarlingWorker.PlanRegressionReadOfFinishedPass(stillRunning, () => { reads++; return 61_500; });
        Assert.Null(runningRead);
        Assert.Equal(0, reads);
        Assert.Null(DarlingWorker.PlanRegressionReadOfFinishedPass(null, () => 61_500));
        var (_, runningDetail) = DarlingWorker.DescribeAnalysisPass(
            DarlingWorker.AnalysisPassStatus.TimedOut, 0, "analysis exceeded 120s", 125_000, runningRead, insideFleetSlot: true);
        Assert.Contains("PLAN_REGRESSION read did not run or was not measured", runningDetail, StringComparison.Ordinal);
    }

    /// <summary>Darling's get_collection_log accepts ANALYZED; the shared vocabulary (Lite's) does not carry it.</summary>
    [Fact]
    public void DarlingsStatusVocabulary_AddsAnalyzed_AndTheSharedOneDoesNot()
    {
        Assert.Contains(AnalysisPassRow.Analyzed, DarlingMcpDataTools.CollectionLogStatuses);
        Assert.DoesNotContain(AnalysisPassRow.Analyzed, EnumeratedCollectorDriver.CollectionLogStatuses);
        Assert.Equal(EnumeratedCollectorDriver.CollectionLogStatuses.Count + 1, DarlingMcpDataTools.CollectionLogStatuses.Count);
        foreach (var word in new[] { AnalysisPassRow.Abandoned, AnalysisPassRow.Error, AnalysisPassRow.Skipped })
        {
            Assert.Contains(word, EnumeratedCollectorDriver.CollectionLogStatuses);
        }
    }

    /// <summary>The previous body's analysis pass is a cause: it ran after the collectors, in the same fleet slot, so the
    /// body length never held it and the row used to say "no single cause".</summary>
    [Fact]
    public void ALongAnalysisPass_IsTheCause_NotNoSingleCause()
    {
        var tally = new CycleSkipAccumulator();
        tally.AddGrid("wait_stats", 1, T0.AddMinutes(-2));
        var report = DarlingCycleSkip.Build(
            tally.TryTake(T0, DarlingCycleSkip.RowMinInterval)!, new CycleSkipCauses(4_000, "wait_stats", 3_000, 200, PrevAnalysisMs: 118_000), T0)!;

        Assert.Equal(DarlingCycleSkip.AnalysisPassName, report.SlowestItem);
        Assert.Equal(AnalysisPassRow.CollectorName, report.SlowestItem);
        Assert.Equal(118_000, report.SlowestItemMs);
        Assert.Equal(118_000, report.DurationMs);
        Assert.Contains("the previous pass's scheduled analysis held this server's fleet collection slot for 118s", report.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("no single cause", report.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AGateWait_SaysHowManySlotsAnalysisPassesHeld()
    {
        var tally = new CycleSkipAccumulator();
        tally.AddGrid("wait_stats", 1, T0.AddMinutes(-2));
        var report = DarlingCycleSkip.Build(
            tally.TryTake(T0, DarlingCycleSkip.RowMinInterval)!, new CycleSkipCauses(0, null, 0, 95_000, GateWaitAnalysisHolders: 3), T0)!;
        Assert.Contains("waited 95s for a fleet collection slot (3 slot(s) were held by scheduled analysis passes when it got one)", report.Message, StringComparison.Ordinal);

        var none = new CycleSkipAccumulator();
        none.AddGrid("wait_stats", 1, T0.AddMinutes(-2));
        var plain = DarlingCycleSkip.Build(none.TryTake(T0, DarlingCycleSkip.RowMinInterval)!, new CycleSkipCauses(0, null, 0, 95_000), T0)!;
        Assert.DoesNotContain("analysis", plain.Message, StringComparison.Ordinal);
    }

    /// <summary>A tie is not a signal: every collector at the same count says so, instead of three alphabetical names. The map
    /// holds the collectors that lost a slot, not every due one (SkipCreditFloor can leave a due collector out), so the wording
    /// claims only those.</summary>
    [Fact]
    public void ATieAcrossEveryCollector_SaysEveryCollectorThatLostASlot()
    {
        var tally = new CycleSkipAccumulator();
        foreach (var name in new[] { "attention", "ag_replica_states", "ag_database_replica_states", "query_stats", "wait_stats" })
        {
            tally.AddGrid(name, 1, T0.AddMinutes(-3));
        }

        var report = DarlingCycleSkip.Build(tally.TryTake(T0, DarlingCycleSkip.RowMinInterval)!, new CycleSkipCauses(0, null, 0, 0), T0)!;
        Assert.Contains("5 slot(s) skipped across 5 collector(s) (every collector that lost a slot, 1 slot(s) each)", report.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("every due collector", report.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("top:", report.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("ag_database_replica_states", report.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ATieAcrossTheCut_IsListedByCount_AndDistinctCountsKeepTheirNames()
    {
        Assert.Equal(
            "by count: 4 collectors at 2 slot(s) each, 16 collectors at 1 slot(s) each",
            DarlingCycleSkip.TopText(Counts(("a", 2, 4), ("b", 1, 16))));
        Assert.Equal(
            "by count: query_store_00 5, 3 collectors at 2 slot(s) each, 2 collectors at 1 slot(s) each",
            DarlingCycleSkip.TopText(Counts(("query_store", 5, 1), ("b", 2, 3), ("c", 1, 2))));
        Assert.Equal(
            "top: query_stats 3, wait_stats 2, attention 1",
            DarlingCycleSkip.TopText(new Dictionary<string, long> { ["attention"] = 1, ["wait_stats"] = 2, ["query_stats"] = 3 }));
        Assert.Equal("top: wait_stats 4", DarlingCycleSkip.TopText(new Dictionary<string, long> { ["wait_stats"] = 4 }));
    }

    private static Dictionary<string, long> Counts(params (string Prefix, long Slots, int Collectors)[] groups)
    {
        var map = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        foreach (var (prefix, slots, collectors) in groups)
        {
            for (var i = 0; i < collectors; i++)
            {
                map[prefix + "_" + i.ToString("00", System.Globalization.CultureInfo.InvariantCulture)] = slots;
            }
        }

        return map;
    }
}
