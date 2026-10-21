/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6211: which plans of a database-hour get their text and plan XML fetched. Pure selection over runtime-statistics rows (no store, no SQL Server):
/// the live behaviour on a real Query Store is <see cref="QueryStoreTopNFetchLiveTests"/>.
/// </summary>
public sealed class QueryStoreFetchSelectionTests
{
    private static QueryStoreCollector.Row Row(
        long plan, long query = 0, long interval = 1, long executions = 1, long cpuUs = 0, long durationUs = 0, long physical = 0, long writes = 0,
        long memory = 0, long tempdb = 0, long logicalReads = 0, bool forced = false, string database = "db") => new()
    {
        DatabaseName = database,
        PlanId = plan,
        QueryId = query == 0 ? plan : query,
        RuntimeStatsIntervalId = interval,
        ExecutionTypeDesc = "Regular",
        ExecutionCount = executions,
        AvgCpuTimeUs = cpuUs,
        AvgDurationUs = durationUs,
        AvgPhysicalIoReads = physical,
        AvgLogicalIoWrites = writes,
        MaxQueryMaxUsedMemory = memory,
        MaxTempdbSpaceUsed = tempdb,
        AvgLogicalIoReads = logicalReads,
        IsForcedPlan = forced,
    };

    [Fact]
    public void EachMetricContributesItsOwnTopN_AndNothingElseIsSelected()
    {
        /* Plan 1 leads CPU, 2 duration, 3 physical reads, 4 writes, 5 memory, 6 tempdb, 7 executions; 8 to 12 lead nothing. N = 1. */
        var rows = new List<QueryStoreCollector.Row>
        {
            Row(1, cpuUs: 1000, executions: 1), Row(2, durationUs: 1000, executions: 1), Row(3, physical: 1000, executions: 1),
            Row(4, writes: 1000, executions: 1), Row(5, memory: 1000, executions: 1), Row(6, tempdb: 1000, executions: 1), Row(7, executions: 50),
            Row(8, cpuUs: 1, executions: 1), Row(9, durationUs: 1, executions: 1), Row(10, physical: 1, executions: 1), Row(11, writes: 1, executions: 1), Row(12, memory: 1, executions: 1),
        };

        var selection = QueryStoreFetchSelection.Select(rows, topN: 1);

        Assert.Equal(new long[] { 1, 2, 3, 4, 5, 6, 7 }, selection.PlanIds.OrderBy(p => p).ToArray());
    }

    [Fact]
    public void LogicalReads_AreNotAMetric()
    {
        /* Plan 1 has by far the most logical reads and loses every metric that counts: it is not selected at N = 1. */
        var rows = new List<QueryStoreCollector.Row>
        {
            Row(1, logicalReads: 9_000_000, executions: 1), Row(2, cpuUs: 5, durationUs: 5, executions: 9),
        };

        Assert.Equal(new long[] { 2 }, QueryStoreFetchSelection.Select(rows, topN: 1).PlanIds.ToArray());
    }

    [Fact]
    public void AZeroValue_IsNotRankedOnThatMetric_SoAQuietHourSelectsLittle()
    {
        /* Three plans, N = 100, none with memory, tempdb, writes or reads: each is still ranked (it ran), but only on the metrics where it is above zero. A plan that
           ran zero times and costs nothing is on no metric at all. */
        var rows = new List<QueryStoreCollector.Row>
        {
            Row(1, executions: 3), Row(2, executions: 0), Row(3, cpuUs: 0, executions: 0),
        };

        Assert.Equal(new long[] { 1 }, QueryStoreFetchSelection.Select(rows, topN: 100).PlanIds.ToArray());
    }

    [Fact]
    public void CpuAndDuration_RankOnTotals_NotOnTheAverage()
    {
        /* Plan 1: avg 100 us, 1000 executions = 100,000 us total. Plan 2: avg 5,000 us, 2 executions = 10,000 us total. Plan 1 leads CPU at N = 1. */
        var rows = new List<QueryStoreCollector.Row>
        {
            Row(1, cpuUs: 100, executions: 1000), Row(2, cpuUs: 5000, executions: 2),
        };

        var selection = QueryStoreFetchSelection.Select(rows, topN: 1);

        Assert.Contains(1L, selection.PlanIds);
        Assert.DoesNotContain(2L, selection.PlanIds);
    }

    [Fact]
    public void TheRankIsPerDatabaseAndPerInterval()
    {
        /* The same costly plan id in two intervals and a quiet plan in a third: each database-hour ranks on its own, so a plan that leads its own hour
           is selected even when another hour holds a bigger one. */
        var rows = new List<QueryStoreCollector.Row>
        {
            Row(1, interval: 10, cpuUs: 1_000_000, executions: 10),
            Row(2, interval: 11, cpuUs: 10, executions: 1),
            Row(3, interval: 10, database: "other", cpuUs: 5, executions: 1),
        };

        var selection = QueryStoreFetchSelection.Select(rows, topN: 1);

        Assert.Equal(new long[] { 1, 2, 3 }, selection.PlanIds.OrderBy(p => p).ToArray());
    }

    [Fact]
    public void ATieAtTheBoundary_BreaksOnTheLowerPlanId_SoTheSameRowsPickTheSamePlans()
    {
        var rows = new List<QueryStoreCollector.Row> { Row(9, executions: 5), Row(4, executions: 5), Row(6, executions: 5) };

        Assert.Equal(new long[] { 4 }, QueryStoreFetchSelection.Select(rows, topN: 1).PlanIds.ToArray());
    }

    [Fact]
    public void AForcedPlan_AndThePlansOfAForcedQuery_AndPlansPlanCorrectionNames_AreSelectedWhateverTheirRank()
    {
        var rows = new List<QueryStoreCollector.Row>
        {
            Row(1, executions: 1000, cpuUs: 1000),
            Row(2, forced: true, executions: 1),
            Row(3, query: 30, executions: 1),
            Row(4, query: 30, executions: 1),
            Row(5, executions: 1),
            Row(6, executions: 1),
        };

        var selection = QueryStoreFetchSelection.Select(
            rows, topN: 1, alwaysPlanIds: new HashSet<long> { 5 }, alwaysQueryIds: new HashSet<long> { 30 });

        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, selection.PlanIds.OrderBy(p => p).ToArray());
        Assert.DoesNotContain(6L, selection.PlanIds);
        /* The text goes with the plan: a selected plan's query is selected, an unselected plan's is not. */
        Assert.Contains(30L, selection.QueryIds);
        Assert.DoesNotContain(6L, selection.QueryIds);
    }

    [Fact]
    public void ZeroOrLess_SelectsEveryPlan_TheBehaviourBeforeTheSetting()
    {
        var rows = Enumerable.Range(1, 20).Select(i => Row(i, executions: 1)).ToList();

        Assert.Equal(20, QueryStoreFetchSelection.Select(rows, topN: 0).PlanIds.Count);
        Assert.Equal(20, QueryStoreFetchSelection.Select(rows, topN: -5).PlanIds.Count);
    }

    [Fact]
    public void TheHoursAlreadyStoredRows_RankWithTheBatch_ButAreNotFetchedAgain()
    {
        /* An earlier run stored plans 1 and 2 of this hour with big counts. This run reads only plan 3, which has a smaller count: on its own it would lead its
           hour, but against the stored rows it ranks third at N = 2, so it is not fetched. */
        var stored = new List<QueryStoreCollector.Row> { Row(1, executions: 500), Row(2, executions: 400) };
        var batch = new List<QueryStoreCollector.Row> { Row(3, executions: 300) };

        Assert.Empty(QueryStoreFetchSelection.Select(batch, topN: 2, alreadyStored: stored).PlanIds);
        Assert.Equal(new long[] { 3 }, QueryStoreFetchSelection.Select(batch, topN: 2).PlanIds.ToArray());

        /* The batch row replaces the stored row for the same plan and interval: plan 2 grew past plan 1, and is selected; plan 1, not read this run, is not. */
        var grown = new List<QueryStoreCollector.Row> { Row(2, executions: 900) };
        Assert.Equal(new long[] { 2 }, QueryStoreFetchSelection.Select(grown, topN: 1, alreadyStored: stored).PlanIds.ToArray());
    }
}
