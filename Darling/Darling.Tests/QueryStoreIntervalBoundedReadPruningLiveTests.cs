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
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// The bounded reads of the day-partitioned Query Store interval tables prune (#5571), on a seeded and promoted store.
/// A partitioned parent only helps a read that bounds <c>first_execution_time</c>; this runs <c>EXPLAIN</c> on each such
/// read's shape and asserts the plan scans only the day partitions (and the legacy table) the bounds can reach, fewer
/// than the store has, with the DEFAULT partition allowed because it is empty. Each shape runs twice: with a custom plan
/// (the parameters pruned at plan time) and with a generic plan (pruned at executor start, which EXPLAIN reports as
/// "Subplans Removed"), the two ways a prepared statement can run.
///
/// <para>The shapes come from the product where it exposes the SQL (<see cref="PlanRegressionDaily.BuildDaySql"/>,
/// <see cref="QueryStoreTopDaily.BuildDaySql"/>, <see cref="QueryStoreIntervalWide.PurgeEdgeMarginSql"/>,
/// <see cref="QueryStoreIntervalWide.FirstExecUpperSlackSql"/>). The windowed MCP, viewer and trend reads and the
/// lower-bound-only collector reads keep their SQL private inside larger statements, so they are covered by the predicate
/// shape they share with the product's own fragments.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreIntervalBoundedReadPruningLiveTests
{
    private static string Lit(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    /* What an EXPLAIN reported for one read: the leaves scanned and how many subplans the executor removed. */
    private sealed record Scan(IReadOnlyList<string> Relations, int SubplansRemoved);

    private static void Walk(JsonElement node, string prefix, List<string> relations, ref int removed)
    {
        if (node.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in node.EnumerateObject())
            {
                if (property.Name == "Relation Name" && property.Value.GetString() is { } name && name.StartsWith(prefix, StringComparison.Ordinal))
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
        DateTime lo, DateTime? hi, string mode, string label)
    {
        var legacy = $"{table.Name}_legacy";
        var def = $"{table.Name}_default";
        var reachable = new HashSet<string>(StringComparer.Ordinal) { def };
        var s = leaves.Single(l => l.Name == table.Legacy).Upper!.Value;
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

        var unreachable = scan.Relations.Where(r => !reachable.Contains(r)).ToList();
        Assert.True(unreachable.Count == 0, $"{label} ({mode}) scanned partitions the bounds cannot reach: {string.Join(", ", unreachable)}");

        var scannedData = scan.Relations.Count(r => r != def);
        var storedData = leaves.Count(l => !l.IsDefault);
        Assert.True(scannedData > 0, $"{label} ({mode}) scanned no partition: {string.Join(", ", scan.Relations)}");
        Assert.True(scannedData < storedData, $"{label} ({mode}) scanned {scannedData} of {storedData} partitions; nothing was pruned");
        if (mode == "force_generic_plan")
        {
            Assert.True(scan.SubplansRemoved > 0 || scan.Relations.Count < leaves.Count, $"{label} generic plan removed no subplans");
        }
    }

    private static readonly string[] Modes = { "force_custom_plan", "force_generic_plan" };

    [Fact]
    public async Task EveryBoundedRead_OfTheWideTable_ScansOnlyTheDaysItsBoundsReach()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var table = Wide;
        await SeedAsync(connection, table, ct);
        var leaves = await QueryStoreIntervalPartitions.ReadPartitionsAsync(connection, table, ct);
        Assert.True(leaves.Count(l => !l.IsDefault) >= 10, "the seeded store has many partitions to prune");

        var from = new DateTime(2026, 10, 7);
        var to = new DateTime(2026, 10, 8);
        var day = new DateTime(2026, 10, 6);

        foreach (var mode in Modes)
        {
            /* The windowed reads (MCP, viewer, trends, top-N): the collection window plus the first_execution_time floor and upper slack. */
            var window = $@"SELECT count(*) FROM collect.query_store_interval_wide
                WHERE server_id = $1
                AND   collection_time >= $2
                AND   collection_time <= $3
                AND   first_execution_time >= $2 - {QueryStoreIntervalWide.PurgeEdgeMarginSql}
                AND   first_execution_time <= $3 + {QueryStoreIntervalWide.FirstExecUpperSlackSql}";
            var scan = await ExplainAsync(connection, table.Name, "integer, timestamp, timestamp", window, $"1, '{Lit(from)}'::timestamp, '{Lit(to)}'::timestamp", mode, ct);
            AssertPruned(scan, table, leaves, from - QueryStoreIntervalWide.PurgeEdgeMargin, to + QueryStoreIntervalWide.FirstExecUpperSlack, mode, "windowed read");

            /* The top-daily build reads one collection day with the same floor and a one-hour skew slack above. */
            var topDaily = await ExplainAsync(connection, table.Name, "integer, date", QueryStoreTopDaily.BuildDaySql, $"1, '{Lit(day)}'::date", mode, ct);
            AssertPruned(topDaily, table, leaves, day - QueryStoreIntervalWide.PurgeEdgeMargin, day.AddDays(1) + QueryStoreTopDaily.SkewSlack, mode, "top-daily build");
        }
    }

    [Fact]
    public async Task EveryBoundedRead_OfTheLatestTable_ScansOnlyTheDaysItsBoundsReach()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var table = Latest;
        await SeedAsync(connection, table, ct);
        var leaves = await QueryStoreIntervalPartitions.ReadPartitionsAsync(connection, table, ct);
        Assert.True(leaves.Count(l => !l.IsDefault) >= 10, "the seeded store has many partitions to prune");

        var day = new DateTime(2026, 10, 6);

        foreach (var mode in Modes)
        {
            /* The plan-regression daily build bounds first_execution_time on both ends: [D - 1 day, D + 1 day). */
            var regression = await ExplainAsync(
                connection, table.Name, "integer, date, timestamp, timestamp", PlanRegressionDaily.BuildDaySql,
                $"1, '{Lit(day)}'::date, '{Lit(day.AddDays(1))}'::timestamp, '{Lit(day.AddDays(-1))}'::timestamp", mode, ct);
            AssertPruned(regression, table, leaves, day.AddDays(-1), day.AddDays(1).AddTicks(-1), mode, "plan-regression daily build");

            /* The collector and drill-down reads bound it below only, so they also reach the days ahead and DEFAULT. */
            var lowerOnly = @"SELECT count(*) FROM collect.query_store_interval_latest
                WHERE server_id = $1
                AND   last_execution_time >= $2
                AND   collection_time >= $3
                AND   first_execution_time >= $4";
            var floor = new DateTime(2026, 10, 7);
            var lower = await ExplainAsync(
                connection, table.Name, "integer, timestamp, timestamp, timestamp", lowerOnly,
                $"1, '{Lit(floor)}'::timestamp, '{Lit(floor)}'::timestamp, '{Lit(floor)}'::timestamp", mode, ct);
            AssertPruned(lower, table, leaves, floor, null, mode, "lower-bound-only read");
        }
    }
}
