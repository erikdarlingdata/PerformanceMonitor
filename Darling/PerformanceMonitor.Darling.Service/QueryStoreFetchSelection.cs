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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #6211: which of one database's Query Store plans get their XML and statement text fetched this run.
///
/// <para><b>Why.</b> Since 3.9.0 (#4875) every <c>query_store</c> run fetched the text and plan of every plan Darling had not stored yet.
/// On a churny database 63% of new plans ran once and 80-87% of the fetch cost went on plans nobody opens. The fetch now follows the
/// plans that cost something: in each database and each Query Store interval (hour), the top N plans on each of seven metrics.</para>
///
/// <para><b>The seven metrics</b> (the seat measured the coverage with exactly these rules, in the issue's last comment): CPU, duration,
/// physical reads and writes as TOTALS (the interval's average times its execution count, summed over the slices and execution types
/// of the plan), memory grant and tempdb as the MAX, and executions as the count. <b>Logical reads are not a metric</b> (Erik: not a
/// useful one). A plan with a zero value on a metric is not ranked on it, so a database-hour with few busy plans selects few plans.
/// Ties at the boundary break on the lower plan id, so the same rows always pick the same plans.</para>
///
/// <para><b>Where it runs: the service, over the rows the run already read.</b> The ranking needs the interval's per-plan totals, which
/// are the runtime-stats rows in the batch. Ranking them here adds no statement on the monitored server, and it cannot break the rule
/// that no statement joins two <c>sys.query_store_*</c> views (#2673, #2764, #6221): this class reads no Query Store view at all.</para>
///
/// <para><b>Always fetched, whatever the rank:</b> a forced plan (<c>is_forced_plan</c> on its row, or named by the forced-plans
/// collector), every other plan of a query that has a forced plan (#6221 judges a force by the stored XML of its variants), and the
/// plans the plan-correction collector names (the engine's own tuning recommendations: the regressed plan and the last good plan).
/// Darling collects no other "regressed" flag. The two store-side sets arrive as arguments; this class does not read the store.</para>
///
/// <para><b>Nothing to carry.</b> A plan that does not rank is still "not stored yet" to the next run, so a plan that ranks later
/// (its interval grew, or a later hour) is fetched then. Only selected plans enter the fetch's debt, so the candidate list is bounded by
/// 7 x N plans per database-hour plus the always-fetched sets, and an unranked plan never grows it.</para>
/// </summary>
internal static class QueryStoreFetchSelection
{
    /// <summary>The setting's default: top 100 per database-hour on each metric (the measured knee: 86% of new plans' CPU for about a third of the fetches).</summary>
    internal const int DefaultTopN = 100;

    /// <summary>The largest N the service honours; a larger value reads as this one. Anything at or below zero means "fetch every new plan".</summary>
    internal const int MaxTopN = 100_000;

    /// <summary>The seven metrics, in the order the reasons name them.</summary>
    internal static readonly IReadOnlyList<string> MetricNames = new[]
    {
        "CPU", "duration", "physical reads", "writes", "memory grant", "tempdb", "executions",
    };

    /// <summary>What a run selected: the plan ids to fetch the XML of, and the query ids to fetch the text of.</summary>
    internal sealed record Selection(HashSet<long> PlanIds, HashSet<long> QueryIds, int RankedPlans, int ConsideredPlans);

    /// <summary>
    /// Picks the plans of <paramref name="rows"/> (one database's batch) to fetch. <paramref name="topN"/> at or below zero selects every plan.
    /// <paramref name="alwaysQueryIds"/> are the queries with a forced plan (every plan of each is selected);
    /// <paramref name="alwaysPlanIds"/> are plan ids named by plan correction.
    /// </summary>
    internal static Selection Select(
        IReadOnlyList<QueryStoreCollector.Row> rows,
        int topN,
        IReadOnlySet<long>? alwaysPlanIds = null,
        IReadOnlySet<long>? alwaysQueryIds = null)
    {
        var plans = new HashSet<long>();
        var queries = new HashSet<long>();
        var considered = new HashSet<long>();

        /* One total per (database, interval, plan): the slices and execution types of one plan in one interval are rows of ONE plan's hour. */
        var perPlanHour = new Dictionary<(string Database, long Interval, long PlanId), Totals>();
        foreach (var row in rows)
        {
            if (row.PlanId <= 0)
            {
                continue;
            }

            considered.Add(row.PlanId);
            var key = (row.DatabaseName ?? "", IntervalOf(row), row.PlanId);
            perPlanHour.TryGetValue(key, out var totals);
            totals ??= new Totals(row.QueryId);
            totals.Add(row);
            perPlanHour[key] = totals;
        }

        var always = new HashSet<long>();
        foreach (var row in rows)
        {
            if (row.PlanId <= 0)
            {
                continue;
            }

            if (row.IsForcedPlan
                || (alwaysPlanIds is not null && alwaysPlanIds.Contains(row.PlanId))
                || (alwaysQueryIds is not null && alwaysQueryIds.Contains(row.QueryId)))
            {
                always.Add(row.PlanId);
            }
        }

        var ranked = new HashSet<long>();
        if (topN <= 0)
        {
            foreach (var plan in considered)
            {
                ranked.Add(plan);
            }
        }
        else
        {
            foreach (var hour in perPlanHour.GroupBy(kv => (kv.Key.Database, kv.Key.Interval)))
            {
                var members = hour.Select(kv => (PlanId: kv.Key.PlanId, Totals: kv.Value)).ToList();
                RankInto(ranked, members, topN, t => t.Cpu);
                RankInto(ranked, members, topN, t => t.Duration);
                RankInto(ranked, members, topN, t => t.PhysicalReads);
                RankInto(ranked, members, topN, t => t.Writes);
                RankInto(ranked, members, topN, t => t.MemoryGrant);
                RankInto(ranked, members, topN, t => t.Tempdb);
                RankInto(ranked, members, topN, t => t.Executions);
            }
        }

        foreach (var plan in ranked)
        {
            plans.Add(plan);
        }

        foreach (var plan in always)
        {
            plans.Add(plan);
        }

        foreach (var row in rows)
        {
            if (row.PlanId > 0 && row.QueryId > 0 && plans.Contains(row.PlanId))
            {
                queries.Add(row.QueryId);
            }
        }

        return new Selection(plans, queries, ranked.Count, considered.Count);
    }

    private static void RankInto(
        HashSet<long> into, List<(long PlanId, Totals Totals)> members, int topN, Func<Totals, double> metric)
    {
        foreach (var member in members
            .Select(m => (m.PlanId, Value: metric(m.Totals)))
            .Where(m => m.Value > 0)
            .OrderByDescending(m => m.Value)
            .ThenBy(m => m.PlanId)
            .Take(topN))
        {
            into.Add(member.PlanId);
        }
    }

    /// <summary>
    /// The Query Store interval a row belongs to: its runtime-stats interval id, else its interval start, else the hour of its last execution.
    /// </summary>
    private static long IntervalOf(QueryStoreCollector.Row row)
    {
        if (row.RuntimeStatsIntervalId is long id)
        {
            return id;
        }

        var instant = row.IntervalStartTimeUtc ?? row.LastExecutionTime ?? DateTime.MinValue;
        return -(instant.Ticks / TimeSpan.TicksPerHour) - 1;
    }

    private sealed class Totals
    {
        public Totals(long queryId) => QueryId = queryId;

        public long QueryId { get; }
        public double Cpu { get; private set; }
        public double Duration { get; private set; }
        public double PhysicalReads { get; private set; }
        public double Writes { get; private set; }
        public double MemoryGrant { get; private set; }
        public double Tempdb { get; private set; }
        public double Executions { get; private set; }

        public void Add(QueryStoreCollector.Row row)
        {
            double executions = Math.Max(0, row.ExecutionCount);
            Cpu += row.AvgCpuTimeUs * executions;
            Duration += row.AvgDurationUs * executions;
            PhysicalReads += row.AvgPhysicalIoReads * executions;
            Writes += row.AvgLogicalIoWrites * executions;
            MemoryGrant = Math.Max(MemoryGrant, row.MaxQueryMaxUsedMemory);
            Tempdb = Math.Max(Tempdb, row.MaxTempdbSpaceUsed);
            Executions += executions;
        }
    }
}
