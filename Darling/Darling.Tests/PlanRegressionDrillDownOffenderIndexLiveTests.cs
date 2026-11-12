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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// #5957: the PLAN_REGRESSION drill-down's table read (<see cref="PgDrillDownCollector.RegressedQueriesTableSql"/>) asks for
/// the fact's five offenders over the 14-day comparison window, but no index on <c>collect.query_store_interval_latest</c>
/// led with <c>(database_name, query_id)</c>, so it read every row the server had in the window and kept five queries'
/// rows (50,464 pages from disk per call on a 42-server store). This plants many queries in one database across the day
/// partitions of a promoted latest table, the offenders' cheap plans in complete days and their expensive plans through
/// the day in progress, builds the indexes a field store has, then
/// <see cref="QueryStoreBackgroundIndexes.LatestDatabaseQueryFirstExec"/> through the background path. The drill-down returns
/// the same rows and numbers before and after, those are the numbers the seed's regression works out to, a regressed query
/// that is not an offender never appears, and the product's statement, run under <c>EXPLAIN (ANALYZE, BUFFERS)</c> with the
/// custom plan the unprepared command gets, reads every seeded partition through the new index and touches a tenth of the
/// blocks or fewer.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class PlanRegressionDrillDownOffenderIndexLiveTests
{
    private const int ServerId = 1;
    private const string Db = "db";
    private const string NewIndexName = "ix_query_store_interval_latest_db_query_first_exec";

    /* The analysis window: the hour before the kit's fixed Now, so the comparison window is the 14 days before that. */
    private static readonly DateTime TimeRangeEnd = DateTime.SpecifyKind(Now, DateTimeKind.Unspecified);
    private static readonly DateTime TimeRangeStart = TimeRangeEnd.AddHours(-1);
    private static readonly DateTime WindowStart = PgFactCollector.PlanRegressionWindowFloor(TimeRangeStart.AddDays(-14));

    /* Five offenders and one query that regressed the same way but is not on the fact's list. */
    private static readonly long[] Offenders = { 9001, 9002, 9003, 9004, 9005 };
    private const long NotAnOffender = 9100;

    /* Promotes the latest table as of Now - 15 d and creates the days ahead, then plants one row per hourly interval from the
       window's start to the hour before Now: 800 ordinary queries in 'db' (one plan each), and six regressed queries whose
       cheap plan (5,000 us a call) ran the first seven days and whose expensive plan (50,000 us plus 1,000 per query, so
       no two factors tie) ran from then through today's open day. About 270,000 rows over fifteen day partitions. The regressed
       queries' rows are planted query by query, so the new index's read touches few heap pages; a collector interleaves them
       with the other queries of each collection, which costs the field read up to one heap page per offender row, still a
       small part of the server's day. */
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await PromoteAsync(connection, Latest, Now.AddDays(-15), NullLogger.Instance, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Latest, Now, NullLogger.Instance, ct);
        var first = WindowStart.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        var last = TimeRangeEnd.AddHours(-1).ToString("yyyy-MM-dd HH:00:00", CultureInfo.InvariantCulture);
        var switchAt = WindowStart.AddDays(7).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        await ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_latest
    (server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
     collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     last_execution_time, is_forced_plan, force_failure_count, query_text, avg_rowcount)
SELECT {ServerId}, '{Db}', q, q, NULL, extract(epoch FROM h)::bigint / 3600, h, h + interval '65 minutes', 'ph' || q, 'qh' || q,
       50, 2000 + q, 4000 + q, h + interval '59 minutes', false, 0, 'select ' || q, 10
FROM generate_series(timestamp '{first}', timestamp '{last}', interval '1 hour') AS h
CROSS JOIN generate_series(1, 800) AS q;

INSERT INTO collect.query_store_interval_latest
    (server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
     collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     last_execution_time, is_forced_plan, force_failure_count, query_text, avg_rowcount)
SELECT {ServerId}, '{Db}', q, q * 10 + CASE WHEN h < timestamp '{switchAt}' THEN 1 ELSE 2 END, NULL,
       extract(epoch FROM h)::bigint / 3600, h, h + interval '65 minutes',
       'off' || q || CASE WHEN h < timestamp '{switchAt}' THEN 'a' ELSE 'b' END, 'oqh' || q, 100,
       CASE WHEN h < timestamp '{switchAt}' THEN 5000 ELSE 50000 + (q - 9000) * 1000 END,
       CASE WHEN h < timestamp '{switchAt}' THEN 8000 ELSE 90000 END,
       h + interval '59 minutes', false, 0, 'offender ' || q, 10
FROM generate_series(timestamp '{first}', timestamp '{last}', interval '1 hour') AS h
CROSS JOIN (SELECT unnest(ARRAY[{string.Join(", ", Offenders)}, {NotAnOffender}]::bigint[]) AS q) AS o
ORDER BY q, h;", ct);
        await ExecAsync(connection, $"ANALYZE {Latest.Parent}", ct);
    }

    private static AnalysisContext NewContext() => new()
    {
        ServerId = ServerId,
        ServerName = "RegrOffenderSrv",
        TimeRangeStart = TimeRangeStart,
        TimeRangeEnd = TimeRangeEnd,
        ServerUtcOffset = TimeSpan.Zero,
        PlanRegressionReadsIntervalTable = true,
        PlanRegressionWindowStart = WindowStart,
        PlanRegressionOffenders = Offenders.Select(q => new PlanRegressionOffender(Db, q)).ToList(),
    };

    /* The drill-down's rows, as the product builds them, in the drill-down's own order. */
    private static async Task<List<JsonElement>> DrillDownRowsAsync(NpgsqlDataSource postgres)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_REGRESSION",
            StoryPath = "PLAN_REGRESSION",
            PathKeys = ["PLAN_REGRESSION"],
            /* Past the display gate: below it the expensive drill-downs are skipped wholesale. */
            Severity = 1.0,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], NewContext());

        if (finding.DrillDown is null || !finding.DrillDown.TryGetValue("regressed_queries", out var raw))
            return [];

        return [.. JsonSerializer.SerializeToElement(raw).EnumerateArray()];
    }

    /* One scan of a partition: the partition, the node type, every index the node and its children read, and the shared
       blocks (hit plus read) the node touched, its children included. */
    private sealed record LeafScan(string Leaf, string NodeType, IReadOnlyList<string> Indexes, long Blocks);

    private static void IndexesUnder(JsonElement node, List<string> indexes)
    {
        if (node.TryGetProperty("Index Name", out var name) && name.GetString() is { } index)
            indexes.Add(index);

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
                IndexesUnder(child, indexes);
        }
    }

    private static void Walk(JsonElement node, List<LeafScan> scans)
    {
        if (node.TryGetProperty("Relation Name", out var relation) && relation.GetString() is { } leaf)
        {
            var indexes = new List<string>();
            IndexesUnder(node, indexes);
            scans.Add(new LeafScan(leaf, node.GetProperty("Node Type").GetString()!, indexes,
                node.GetProperty("Shared Hit Blocks").GetInt64() + node.GetProperty("Shared Read Blocks").GetInt64()));
            return;
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
                Walk(child, scans);
        }
    }

    /* The product's statement with the drill-down's parameters, unprepared as the drill-down sends it, so the plan is the
       custom one: the scans of the latest table's partitions and the blocks (hit plus read) those scans touched. The rest of
       the statement (the plan-cache signature, the text join) reads other tables and is the same before and after. */
    private static async Task<(List<LeafScan> Scans, long Blocks, string Json)> ExplainAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + PgDrillDownCollector.RegressedQueriesTableSql, connection)
        {
            CommandTimeout = 120
        };
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(WindowStart);
        command.Parameters.AddWithValue(TimeRangeStart);
        command.Parameters.AddWithValue(TimeRangeEnd);
        command.Parameters.AddWithValue(Enumerable.Repeat(Db, Offenders.Length).ToArray());
        command.Parameters.AddWithValue(Offenders);
        command.Parameters.AddWithValue(WindowStart.AddDays(-1));
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        var plan = document.RootElement[0].GetProperty("Plan");
        var scans = new List<LeafScan>();
        Walk(plan, scans);
        var latest = scans.Where(s => s.Leaf.StartsWith("query_store_interval_latest", StringComparison.Ordinal)).ToList();
        return (latest, latest.Sum(s => s.Blocks), json);
    }

    private static string Describe(IEnumerable<LeafScan> scans) =>
        string.Join("; ", scans.Select(s => $"{s.Leaf} {s.NodeType} [{string.Join(", ", s.Indexes)}] {s.Blocks}"));

    /* The parent index or one of its leaves (the partitioned path names each leaf's index after the parent's). */
    private static bool IsNewIndex(string name) => name.StartsWith(NewIndexName, StringComparison.Ordinal);

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }

    [Fact]
    public async Task TheOffenderRead_SeeksTheNewIndexInEverySeededPartition_AndTheDrillDownRowsAndNumbersDoNotChange()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await SeedAsync(connection, ct);

        /* Before: the indexes a field store already has (the unique key, V153's, and the background server btree). */
        await QueryStoreBackgroundIndexes.EnsureAsync(connection, QueryStoreBackgroundIndexes.LatestServerFirstExec, NullLogger.Instance, ct);
        await ExecAsync(connection, $"ANALYZE {Latest.Parent}", ct);
        await ExecAsync(connection, "SET search_path = collect, config, public", ct);
        var before = await ExplainAsync(connection, ct);
        var rowsBefore = await DrillDownRowsAsync(postgres);

        /* The detector's view of the seed: each offender's latest plan is its expensive one and its best the cheap one, so
           the factor is the CPU ratio, worst first; the query that regressed the same way but is not an offender is absent. */
        Assert.Equal(Offenders.Reverse().ToArray(), rowsBefore.Select(r => r.GetProperty("query_id").GetInt64()).ToArray());
        Assert.DoesNotContain(rowsBefore, r => r.GetProperty("query_id").GetInt64() == NotAnOffender);
        foreach (var row in rowsBefore)
        {
            var q = row.GetProperty("query_id").GetInt64();
            var latestCpu = 50000.0 + (q - 9000) * 1000;
            Assert.Equal(Db, row.GetProperty("database").GetString());
            Assert.Equal($"off{q}b", row.GetProperty("latest_plan_hash").GetString());
            Assert.Equal($"off{q}a", row.GetProperty("best_plan_hash").GetString());
            Assert.Equal(q * 10 + 1, row.GetProperty("best_plan_id").GetInt64());
            Assert.Equal(latestCpu, row.GetProperty("latest_cpu_per_exec_us").GetDouble(), 6);
            Assert.Equal(5000.0, row.GetProperty("best_cpu_per_exec_us").GetDouble(), 6);
            Assert.Equal(latestCpu / 5000.0, row.GetProperty("regression_factor").GetDouble(), 6);
        }

        /* After: the new index, built the way the service builds it (the partitioned path, leaf by leaf). */
        await QueryStoreBackgroundIndexes.EnsureAsync(connection, QueryStoreBackgroundIndexes.LatestDatabaseQueryFirstExec, NullLogger.Instance, ct);
        Assert.True((bool)(await ScalarAsync(connection,
            $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{QueryStoreBackgroundIndexes.LatestDatabaseQueryFirstExecIndexName}'::regclass", ct))!);
        await ExecAsync(connection, $"ANALYZE {Latest.Parent}", ct);
        var after = await ExplainAsync(connection, ct);

        /* The partitions that hold rows; an empty one costs nothing however it is read. */
        var seeded = new List<string>();
        await using (var leaves = new NpgsqlCommand($"SELECT DISTINCT tableoid::regclass::text FROM {Latest.Parent}", connection))
        await using (var reader = await leaves.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
                seeded.Add(reader.GetString(0).Replace("collect.", "", StringComparison.Ordinal));
        }

        Assert.True(seeded.Count >= 14, $"{seeded.Count} seeded partitions");
        Assert.DoesNotContain(before.Scans, scan => scan.Indexes.Any(IsNewIndex));
        var busy = after.Scans.Where(scan => seeded.Contains(scan.Leaf)).ToList();
        Assert.Equal(seeded.OrderBy(l => l, StringComparer.Ordinal), busy.Select(s => s.Leaf).OrderBy(l => l, StringComparer.Ordinal));
        Assert.All(busy, scan =>
        {
            Assert.NotEqual("Seq Scan", scan.NodeType);
            Assert.Contains(scan.Indexes, IsNewIndex);
        });
        Assert.True(after.Blocks * 10 < before.Blocks, $"{after.Blocks} blocks after, {before.Blocks} before. Before: {Describe(before.Scans)}. After: {Describe(after.Scans)}");

        /* The same rows and the same numbers, serialized whole. */
        var rowsAfter = await DrillDownRowsAsync(postgres);
        Assert.Equal(rowsBefore.Select(r => r.GetRawText()), rowsAfter.Select(r => r.GetRawText()));
    }
}
