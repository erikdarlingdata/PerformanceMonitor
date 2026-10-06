/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5306: the Query Store history window's Total Executions counts each Query Store interval ONCE, at its latest
/// snapshot.
///
/// <para>The history grid lists every stored snapshot on purpose (#1841): query_store_stats rows are cumulative per
/// interval, and the collector re-fetches the open interval every cycle, so one interval collected N times is N rows
/// with a growing execution_count. Adding the column up over those rows counts the interval about N times. The
/// summary has to take the newest snapshot of each interval first, keyed on the same interval identity the aggregate
/// reads in LocalDataService.QueryStore.cs dedup on: plan_id, runtime_stats_interval_id, first_execution_time,
/// execution_type_desc and replica_role (database_name and query_id are constant in this one query-scoped read).</para>
///
/// <para>The expected numbers are written out against the true totals, so each one fails against the plain sum the
/// window used before.</para>
/// </summary>
public sealed class QueryStoreHistoryTotalExecutionsTests
{
    private static readonly DateTime T0 = new(2026, 3, 1, 10, 0, 0, DateTimeKind.Unspecified);

    /// <summary>One stored snapshot. An interval is named by its id; its first_execution_time follows from it.</summary>
    private static QueryStoreHistoryRow Snapshot(
        int collectedAtMinute,
        long executions,
        long? interval = 100,
        long plan = 11,
        string executionType = "Regular",
        string? replicaRole = null,
        bool legacyNoFirstExecution = false) =>
        new()
        {
            CollectionTime = T0.AddMinutes(collectedAtMinute),
            ExecutionCount = executions,
            PlanId = plan,
            RuntimeStatsIntervalId = interval,
            FirstExecutionTime = legacyNoFirstExecution ? null : T0.AddMinutes((interval ?? 0) / 100.0),
            ExecutionTypeDesc = executionType,
            ReplicaRole = replicaRole,
        };

    [Fact]
    public void OneIntervalCollectedThreeTimes_PlusAnotherCollectedOnce_TotalsTheLatestOfEach()
    {
        var rows = new List<QueryStoreHistoryRow>
        {
            Snapshot(5, 10, interval: 100),
            Snapshot(10, 25, interval: 100),
            Snapshot(15, 40, interval: 100),
            Snapshot(20, 5, interval: 200),
        };

        /* 40 (interval one's last snapshot) + 5 (interval two's only one). The plain sum of the column, which is
           what the window showed before, is 10 + 25 + 40 + 5 = 80. */
        Assert.Equal(80, rows.Sum(r => r.ExecutionCount));
        Assert.Equal(45, QueryStoreHistoryRow.TotalExecutions(rows));
    }

    [Fact]
    public void TwoPlansOfTheSameInterval_EachCountItsOwnLatestSnapshot()
    {
        /* Plan 11 and plan 22 ran in the SAME interval, so they share runtime_stats_interval_id and
           first_execution_time and differ only in plan_id. A key without plan_id would count one plan's work and
           drop the other's; a key with it counts each once. */
        var rows = new List<QueryStoreHistoryRow>
        {
            Snapshot(5, 10, interval: 100, plan: 11),
            Snapshot(5, 3, interval: 100, plan: 22),
            Snapshot(10, 25, interval: 100, plan: 11),
            Snapshot(10, 7, interval: 100, plan: 22),
            Snapshot(15, 40, interval: 100, plan: 11),
        };

        Assert.Equal(40 + 7, QueryStoreHistoryRow.TotalExecutions(rows));
    }

    [Fact]
    public void IntervalsSharingACollectionTime_BothCount()
    {
        /* An interval's closing fetch and the next interval's first fetch land in the same cycle, so two
           different intervals carry the same collection_time. Taking only the newest collection time, or keying
           on the collection time, would keep one of them and drop the other. */
        var rows = new List<QueryStoreHistoryRow>
        {
            Snapshot(5, 10, interval: 100),
            Snapshot(10, 30, interval: 100),
            Snapshot(10, 4, interval: 200),
            Snapshot(15, 12, interval: 200),
        };

        Assert.Equal(30 + 12, QueryStoreHistoryRow.TotalExecutions(rows));

        /* The same shape with the two intervals' ONLY snapshots on one collection time. */
        var sameInstantOnly = new List<QueryStoreHistoryRow>
        {
            Snapshot(10, 30, interval: 100),
            Snapshot(10, 12, interval: 200),
        };

        Assert.Equal(30 + 12, QueryStoreHistoryRow.TotalExecutions(sameInstantOnly));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SnapshotsOfOneIntervalTiedOnCollectionTime_TakeTheLargerCount_InEitherInputOrder(bool sliverFirst)
    {
        /* The #1907 residual, as the aggregate reads treat it: Query Store's flushed and in-memory slices of one
           interval were stored as two rows sharing the whole identity AND the collection time. The larger count
           is the flushed slice, and it wins whichever way the rows arrive. */
        var sliver = Snapshot(10, 25, interval: 100);
        var flushed = Snapshot(10, 100, interval: 100);
        var rows = sliverFirst ? new List<QueryStoreHistoryRow> { sliver, flushed } : new List<QueryStoreHistoryRow> { flushed, sliver };

        Assert.Equal(100, QueryStoreHistoryRow.TotalExecutions(rows));
    }

    [Fact]
    public void TheLatestSnapshotIsTheNewestByTime_NotTheLargestCount()
    {
        /* "Latest" is the newest collection_time, never the biggest execution_count: the later snapshot of this
           interval holds fewer executions than the earlier one. */
        var rows = new List<QueryStoreHistoryRow>
        {
            Snapshot(5, 90, interval: 100),
            Snapshot(10, 60, interval: 100),
        };

        Assert.Equal(60, QueryStoreHistoryRow.TotalExecutions(rows));
    }

    [Fact]
    public void ReplicaRoles_AreSeparateIntervals()
    {
        /* The primary and a readable secondary each report their own stats for the same interval. The dedup key
           carries replica_role, so neither replica's work is dropped. */
        var rows = new List<QueryStoreHistoryRow>
        {
            Snapshot(5, 10, interval: 100, replicaRole: "PRIMARY"),
            Snapshot(10, 40, interval: 100, replicaRole: "PRIMARY"),
            Snapshot(10, 9, interval: 100, replicaRole: "SECONDARY"),
        };

        Assert.Equal(40 + 9, QueryStoreHistoryRow.TotalExecutions(rows));
    }

    [Fact]
    public void ExecutionTypes_AreSeparateIntervals()
    {
        /* Query Store keeps a Regular, an Aborted and an Exception row per plan and interval. */
        var rows = new List<QueryStoreHistoryRow>
        {
            Snapshot(5, 10, interval: 100, executionType: "Regular"),
            Snapshot(10, 40, interval: 100, executionType: "Regular"),
            Snapshot(10, 2, interval: 100, executionType: "Aborted"),
        };

        Assert.Equal(40 + 2, QueryStoreHistoryRow.TotalExecutions(rows));
    }

    [Fact]
    public void RowsCollectedBeforeTheIntervalIdWasStored_FallBackOnFirstExecutionTime()
    {
        /* The id is NULL on every row collected before #1841 tier 2, and first_execution_time stays beside it as
           the proxy: two legacy intervals with different first executions are still two intervals. */
        var rows = new List<QueryStoreHistoryRow>
        {
            Snapshot(5, 4, interval: null),
            Snapshot(10, 8, interval: null),
            new()
            {
                CollectionTime = T0.AddMinutes(10),
                ExecutionCount = 3,
                PlanId = 11,
                RuntimeStatsIntervalId = null,
                FirstExecutionTime = T0.AddMinutes(30),
                ExecutionTypeDesc = "Regular",
            },
        };

        Assert.Equal(8 + 3, QueryStoreHistoryRow.TotalExecutions(rows));
    }

    [Fact]
    public void TwoIntervalsOfOnePlanWithNoFirstExecution_AreToldApartByTheirId()
    {
        /* A row whose first_execution_time is NULL has no identity under the proxy alone; the real id separates
           the two intervals, the case that made the aggregate reads put the id in their key. */
        var rows = new List<QueryStoreHistoryRow>
        {
            Snapshot(5, 6, interval: 100, legacyNoFirstExecution: true),
            Snapshot(10, 9, interval: 200, legacyNoFirstExecution: true),
        };

        Assert.Equal(6 + 9, QueryStoreHistoryRow.TotalExecutions(rows));
    }

    [Fact]
    public void RowsWithNeitherAnIntervalIdNorAFirstExecution_AreOneInterval_ThatCountsOnce()
    {
        /* The oldest legacy rows hold neither identity column: no #1841 tier 2 id, and no first_execution_time to
           stand in for it. The key's NULL parts match each other, as they do in the aggregate reads' PARTITION BY,
           so the helper reads these rows as ONE interval and counts its newest snapshot once: 8, not 4 + 8. If such
           rows were really two intervals the total would under-count them. The aggregate reads carry the same
           limit, because nothing left on the rows can tell them apart. A different plan is still its own interval. */
        var rows = new List<QueryStoreHistoryRow>
        {
            Snapshot(5, 4, interval: null, legacyNoFirstExecution: true),
            Snapshot(10, 8, interval: null, legacyNoFirstExecution: true),
        };

        Assert.Equal(8, QueryStoreHistoryRow.TotalExecutions(rows));
        Assert.Equal(T0.AddMinutes(10), Assert.Single(QueryStoreHistoryRow.LatestPerInterval(rows)).CollectionTime);

        rows.Add(Snapshot(10, 3, interval: null, plan: 22, legacyNoFirstExecution: true));

        Assert.Equal(8 + 3, QueryStoreHistoryRow.TotalExecutions(rows));
    }

    [Fact]
    public void LatestPerInterval_KeepsOneRowPerInterval_AndLeavesTheGridsListAlone()
    {
        /* The window binds this same list to its grid, which shows every snapshot by design. */
        var rows = new List<QueryStoreHistoryRow>
        {
            Snapshot(5, 10, interval: 100),
            Snapshot(10, 25, interval: 100),
            Snapshot(15, 40, interval: 100),
            Snapshot(20, 5, interval: 200),
        };
        var before = rows.ToList();

        var latest = QueryStoreHistoryRow.LatestPerInterval(rows);

        Assert.Equal(new long[] { 40, 5 }, latest.Select(r => r.ExecutionCount).OrderByDescending(c => c).ToArray());
        Assert.Equal(before, rows);
        Assert.Equal(4, rows.Count);
    }

    [Fact]
    public void NoRows_TotalZero()
    {
        Assert.Equal(0, QueryStoreHistoryRow.TotalExecutions(new List<QueryStoreHistoryRow>()));
    }

    /// <summary>
    /// The window is a WPF window this suite does not instantiate, so its wiring is a source pin: the summary totals
    /// through <c>QueryStoreHistoryRow.TotalExecutions</c> and no longer adds up the raw per-snapshot column.
    /// </summary>
    [Fact]
    public void TheWindowSummary_TotalsThroughTheHelper_NotAPlainSumOfTheSnapshots()
    {
        var source = CodeOnly(ReadLite("Windows", "QueryStoreHistoryWindow.xaml.cs"));

        Assert.Contains("QueryStoreHistoryRow.TotalExecutions(_historyData)", source, StringComparison.Ordinal);
        Assert.DoesNotContain(".Sum(r => r.ExecutionCount)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("Sum(r=>r.ExecutionCount)", source.Replace(" ", string.Empty), StringComparison.Ordinal);
    }

    /* Line and block comments removed, and line endings normalised, so a pin reads code only. */
    private static string CodeOnly(string source)
    {
        var lf = source.Replace("\r\n", "\n");
        lf = Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(lf, @"//[^\n]*", string.Empty);
    }

    private static string ReadLite(string folder, string file, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", folder, file)));
}
