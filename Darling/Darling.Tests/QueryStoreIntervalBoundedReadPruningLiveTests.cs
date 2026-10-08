/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// The bounded reads of the day-partitioned Query Store interval tables prune (#5571), on a seeded and promoted store.
/// A partitioned parent only helps a read that bounds <c>first_execution_time</c>; this runs <c>EXPLAIN</c> on the SQL
/// each shipped read sends and asserts the plan scans only the day partitions (and the legacy table) the bounds can
/// reach, fewer than the store has, with the DEFAULT partition allowed because it is empty. Each read runs twice: with a
/// custom plan (the parameters pruned at plan time) and with a generic plan (pruned at executor start, which EXPLAIN
/// reports as "Subplans Removed"), the two ways a prepared statement can run.
///
/// <para>Every statement is the product's own text, not a copy: the viewer grid and duration trend
/// (<see cref="ViewerDataService.QueryStoreTopTableSql"/>, <see cref="ViewerDataService.QueryStoreDurationTrendTableSql"/>), the
/// MCP top-queries, daily and history reads (<c>DarlingDataReader</c>, <c>DarlingMcpQueryStoreHistoryTools</c>), the two daily
/// builds (<see cref="PlanRegressionDaily.BuildDaySql"/>, <see cref="QueryStoreTopDaily.BuildDaySql"/>) and the collector reads
/// (<see cref="PgFactCollector.PlanRegressionTableSql"/>, <see cref="PgFactCollector.PlanRegressionDailySql"/>,
/// <see cref="PgDrillDownCollector.RegressedQueriesTableSql"/>). A control test strips a read's bound and proves the
/// assertion then fails.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreIntervalBoundedReadPruningLiveTests
{
    private static string Lit(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    /* What an EXPLAIN reported for one read: the leaves scanned and how many subplans the executor removed. */
    private sealed record Scan(IReadOnlyList<string> Relations, int SubplansRemoved);

    /* A leaf of the interval table: a day partition, the DEFAULT partition or the legacy table. The _coverage and _pending
       siblings share the table's name as a prefix and are not leaves. */
    private static bool IsLeaf(string relation, string table) =>
        System.Text.RegularExpressions.Regex.IsMatch(relation, $"^{table}_(p[0-9]{{8}}|default|legacy)$");

    private static void Walk(JsonElement node, string prefix, List<string> relations, ref int removed)
    {
        if (node.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in node.EnumerateObject())
            {
                if (property.Name == "Relation Name" && property.Value.GetString() is { } name && IsLeaf(name, prefix))
                {
                    relations.Add(name);
                }
                else if (property.Name == "Subplans Removed")
                {
                    removed += property.Value.GetInt32();
                }
                else
                {
                    Walk(property.Value, prefix, relations, ref removed);
                }
            }
        }
        else if (node.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in node.EnumerateArray())
            {
                Walk(item, prefix, relations, ref removed);
            }
        }
    }

    /// <summary>
    /// <c>PREPARE</c>s the statement with its parameter types and <c>EXPLAIN</c>s one <c>EXECUTE</c> in the given plan mode.
    /// Nothing runs: it is EXPLAIN without ANALYZE, which still does the executor's start-up pruning.
    /// </summary>
    private static async Task<Scan> ExplainAsync(
        NpgsqlConnection connection, string table, string parameterTypes, string sql, string arguments, string planCacheMode, CancellationToken ct)
    {
        await ExecAsync(connection, $"SET plan_cache_mode = {planCacheMode}", ct);
        await ExecAsync(connection, $"PREPARE pruning_probe({parameterTypes}) AS {sql}", ct);
        try
        {
            await using var command = new NpgsqlCommand($"EXPLAIN (FORMAT JSON) EXECUTE pruning_probe({arguments})", connection) { CommandTimeout = 120 };
            var json = (string)(await command.ExecuteScalarAsync(ct))!;
            using var document = JsonDocument.Parse(json);
            var relations = new List<string>();
            var removed = 0;
            Walk(document.RootElement, table, relations, ref removed);
            return new Scan(relations.Distinct().ToList(), removed);
        }
        finally
        {
            await ExecAsync(connection, "DEALLOCATE pruning_probe", ct);
        }
    }

    /* Seeds one row per day for the days around the promotion, promotes as of Now - 10 d (S = 9-30), and creates the days ahead. */
    private static async Task SeedAsync(NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, CancellationToken ct)
    {
        await PromoteAsync(connection, table, Now.AddDays(-10), NullLogger.Instance, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, table, Now, NullLogger.Instance, ct);
        var day = new DateTime(2026, 9, 28);
        for (var i = 0; i < 14; i++)
        {
            await InsertAsync(connection, table, day.AddDays(i).AddHours(10), 100 + i, ct);
        }

        await ExecAsync(connection, $"ANALYZE {table.Parent}", ct);
    }

    /* A leaf the bounds can reach: a day partition whose [D, D+1) meets [lo, hi], or the legacy table when lo is before S. */
    private static void AssertPruned(
        Scan scan, QueryStoreIntervalPartitions.IntervalTable table, IReadOnlyList<QueryStoreIntervalPartitions.PartitionInfo> leaves,
        Bounds[] ranges, string mode, string label)
    {
        var legacy = $"{table.Name}_legacy";
        var def = $"{table.Name}_default";
        var reachable = new HashSet<string>(StringComparer.Ordinal) { def };
        var s = leaves.Single(l => l.Name == table.Legacy).Upper!.Value;
        foreach (var (lo, hi) in ranges.Select(r => (r.Lo, r.Hi)))
        {
            if (lo < s)
            {
                reachable.Add(legacy);
            }

            foreach (var leaf in leaves.Where(l => !l.IsDefault && l.Name != table.Legacy))
            {
                if (leaf.Upper!.Value > lo && (hi is null || leaf.Lower!.Value <= hi.Value))
                {
                    reachable.Add(leaf.Name.Substring("collect.".Length));
                }
            }
        }

        var unreachable = scan.Relations.Where(r => !reachable.Contains(r)).ToList();
        Assert.True(unreachable.Count == 0, $"{label} ({mode}) scanned partitions the bounds cannot reach: {string.Join(", ", unreachable)}");

        var scannedData = scan.Relations.Count(r => r != def);
        var storedData = leaves.Count(l => !l.IsDefault);
        Assert.True(scannedData > 0, $"{label} ({mode}) scanned no partition: {string.Join(", ", scan.Relations)}");
        Assert.True(scannedData < storedData, $"{label} ({mode}) scanned {scannedData} of {storedData} partitions; nothing was pruned");
        if (mode == "force_generic_plan")
        {
            /* EXPLAIN of a generic plan reports pruning done at executor start as "Subplans Removed". The plan keeps every leaf
               and removes the unreachable ones at run time, so this is the one number that proves the generic plan prunes (a
               leaf-count comparison against the scanned relations always holds once the checks above have passed). */
            Assert.True(scan.SubplansRemoved > 0, $"{label} generic plan removed no subplans");
        }
    }

    private static readonly string[] Modes = { "force_custom_plan", "force_generic_plan" };

    /// <summary>One <c>first_execution_time</c> range a read's bounds reach: inclusive <c>Lo</c>, inclusive <c>Hi</c> (null = open above).</summary>
    private readonly record struct Bounds(DateTime Lo, DateTime? Hi);

    /// <summary>
    /// One shipped read: its SQL as the product sends it, the parameter types to <c>PREPARE</c> it with, one literal per
    /// parameter, and the ranges its bounds reach (the union, for a read with more than one arm). <c>GenericRanges</c>
    /// is the range the read reaches when the plan is generic, for the one read whose upper bound is wrapped in an
    /// <c>OR</c> over a parameter (the viewer grid's open window end), which only a plan-time constant can fold.
    /// </summary>
    private sealed record ReadCase(string Label, string Sql, string ParamTypes, string Args, Bounds[] Ranges, Bounds[]? GenericRanges = null);

    private static async Task AssertReadsPruneAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, IReadOnlyList<ReadCase> reads, CancellationToken ct)
    {
        /* The products send these unqualified and rely on the connection's search_path (collect, config, public). */
        await ExecAsync(connection, "SET search_path = collect, config, public", ct);
        var leaves = await QueryStoreIntervalPartitions.ReadPartitionsAsync(connection, table, ct);
        Assert.True(leaves.Count(l => !l.IsDefault) >= 10, "the seeded store has many partitions to prune");
        foreach (var read in reads)
        {
            foreach (var mode in Modes)
            {
                var scan = await ExplainAsync(connection, table.Name, read.ParamTypes, read.Sql, read.Args, mode, ct);
                var ranges = mode == "force_generic_plan" && read.GenericRanges is { } generic ? generic : read.Ranges;
                AssertPruned(scan, table, leaves, ranges, mode, read.Label);
            }
        }
    }

    /// <summary>The wide-table reads the viewer, the MCP tools and the trend reader send, over one window [from, to].</summary>
    private static ReadCase[] WideReads(DateTime from, DateTime to, DateTime day)
    {
        var purge = QueryStoreIntervalWide.PurgeEdgeMargin;
        var slack = QueryStoreIntervalWide.FirstExecUpperSlack;
        var f = $"'{Lit(from)}'::timestamp";
        var t = $"'{Lit(to)}'::timestamp";
        var edge = new DateTime(2026, 10, 5, 12, 0, 0);
        var spanStart = new DateTime(2026, 10, 6);
        var spanEnd = new DateTime(2026, 10, 7);
        var end = new DateTime(2026, 10, 7, 12, 0, 0);
        return new[]
        {
            /* The viewer grid: $3 may be NULL (an open window end), so its upper bound is $3 IS NULL OR ..., which only the
               custom plan (a constant $3) folds; the generic plan prunes below only. */
            new ReadCase(
                "viewer grid top read", ViewerDataService.QueryStoreTopTableSql, "integer, timestamp, timestamp, integer, text[], integer",
                $"1, {f}, {t}, 10, NULL::text[], 50",
                new[] { new Bounds(from - purge, to + slack) }, new[] { new Bounds(from - purge, null) }),
            new ReadCase(
                "MCP top-queries table read", DarlingDataReader.QueryStoreTopTableSql, "integer, timestamp, timestamp, integer, text[], text, text, integer",
                $"1, {f}, {t}, 10, NULL::text[], NULL::text, NULL::text, 50",
                new[] { new Bounds(from - purge, to + slack) }),
            /* The daily read: the wide table serves the two edges, [$2, $8) and [$9, $3], and the summary serves the days between. */
            new ReadCase(
                "MCP top-queries daily read", DarlingDataReader.QueryStoreTopDailyTableSql,
                "integer, timestamp, timestamp, integer, text[], text, text, date, date, integer",
                $"1, '{Lit(edge)}'::timestamp, '{Lit(end)}'::timestamp, 10, NULL::text[], NULL::text, NULL::text, '{Lit(spanStart)}'::date, '{Lit(spanEnd)}'::date, 50",
                new[] { new Bounds(edge - purge, spanStart + slack), new Bounds(spanEnd - purge, end + slack) }),
            /* The viewer's duration trend: arm 1 (interval_start_time_utc) and arm 2 (legacy rows, $4 is the window end). */
            new ReadCase(
                "viewer duration trend table read", ViewerDataService.QueryStoreDurationTrendTableSql, "integer, timestamp, timestamp, timestamp, text[]",
                $"1, {f}, {t}, {t}, NULL::text[]",
                new[]
                {
                    new Bounds(from - QueryStoreIntervalWide.IntervalStartSlack, to + QueryStoreIntervalWide.IntervalStartFirstExecMargin),
                    new Bounds(from - purge, to + slack),
                }),
            new ReadCase(
                "MCP query history table read", DarlingMcpQueryStoreHistoryTools.HistoryTableSql, "integer, text, bigint, timestamp, timestamp",
                $"1, 'db', 7, {f}, {t}",
                new[] { new Bounds(from - purge, to + slack) }),
            /* The top-daily build reads one collection day with the same floor and a one-hour skew slack above. */
            new ReadCase(
                "top-daily build", QueryStoreTopDaily.BuildDaySql, "integer, date", $"1, '{Lit(day)}'::date",
                new[] { new Bounds(day - purge, day.AddDays(1) + QueryStoreTopDaily.SkewSlack) }),
        };
    }

    /// <summary>The latest-table reads the collectors and the daily builder send.</summary>
    private static ReadCase[] LatestReads(DateTime day)
    {
        var floor = new DateTime(2026, 10, 7);
        var fl = $"'{Lit(floor)}'::timestamp";
        return new[]
        {
            /* The plan-regression daily build bounds first_execution_time on both ends: [D - 1 day, D + 1 day). */
            new ReadCase(
                "plan-regression daily build", PlanRegressionDaily.BuildDaySql, "integer, date, timestamp, timestamp",
                $"1, '{Lit(day)}'::date, '{Lit(day.AddDays(1))}'::timestamp, '{Lit(day.AddDays(-1))}'::timestamp",
                new[] { new Bounds(day.AddDays(-1), day.AddDays(1).AddTicks(-1)) }),
            /* The collector and drill-down reads bound it below only, so they also reach the days ahead and DEFAULT. */
            new ReadCase(
                "PLAN_REGRESSION fact, table read", PgFactCollector.PlanRegressionTableSql, "integer, timestamp, timestamp, timestamp",
                $"1, {fl}, {fl}, {fl}", new[] { new Bounds(floor, null) }),
            new ReadCase(
                "PLAN_REGRESSION fact, per-day read", PgFactCollector.PlanRegressionDailySql, "integer, timestamp, date[], timestamp, timestamp",
                $"1, {fl}, ARRAY[]::date[], {fl}, {fl}", new[] { new Bounds(floor, null) }),
            new ReadCase(
                "regressed-queries drill-down", PgDrillDownCollector.RegressedQueriesTableSql,
                "integer, timestamp, timestamp, timestamp, text[], bigint[], timestamp",
                $"1, {fl}, {fl}, {fl}, NULL::text[], NULL::bigint[], {fl}", new[] { new Bounds(floor, null) }),
        };
    }

    [Fact]
    public async Task EveryBoundedRead_OfTheWideTable_ScansOnlyTheDaysItsBoundsReach()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, Wide, ct);
        await AssertReadsPruneAsync(connection, Wide, WideReads(new DateTime(2026, 10, 7), new DateTime(2026, 10, 8), new DateTime(2026, 10, 6)), ct);
    }

    [Fact]
    public async Task EveryBoundedRead_OfTheLatestTable_ScansOnlyTheDaysItsBoundsReach()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, Latest, ct);
        await AssertReadsPruneAsync(connection, Latest, LatestReads(new DateTime(2026, 10, 6)), ct);
    }

    /// <summary>
    /// The assertion has teeth: the MCP history read with its two <c>first_execution_time</c> lines removed scans every
    /// partition, and <see cref="AssertPruned"/> refuses it. A read that loses its bound (or binds it through an
    /// expression that does not prune) stays green only if this control stops failing.
    /// </summary>
    [Fact]
    public async Task AReadThatLosesItsFirstExecutionTimeBound_FailsThePruningAssertion()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, Wide, ct);
        await ExecAsync(connection, "SET search_path = collect, config, public", ct);
        var leaves = await QueryStoreIntervalPartitions.ReadPartitionsAsync(connection, Wide, ct);

        var shipped = WideReads(new DateTime(2026, 10, 7), new DateTime(2026, 10, 8), new DateTime(2026, 10, 6)).Single(r => r.Label == "MCP query history table read");
        var lines = shipped.Sql.Split('\n');
        var stripped = string.Join("\n", lines.Where(l => !l.Contains("first_execution_time >= $4", StringComparison.Ordinal) && !l.Contains("first_execution_time <= $5", StringComparison.Ordinal)));
        Assert.NotEqual(shipped.Sql, stripped);

        foreach (var mode in Modes)
        {
            var intact = await ExplainAsync(connection, Wide.Name, shipped.ParamTypes, shipped.Sql, shipped.Args, mode, ct);
            AssertPruned(intact, Wide, leaves, shipped.Ranges, mode, shipped.Label);

            var scan = await ExplainAsync(connection, Wide.Name, shipped.ParamTypes, stripped, shipped.Args, mode, ct);
            Assert.ThrowsAny<Exception>(() => AssertPruned(scan, Wide, leaves, shipped.Ranges, mode, shipped.Label));
        }
    }
}
