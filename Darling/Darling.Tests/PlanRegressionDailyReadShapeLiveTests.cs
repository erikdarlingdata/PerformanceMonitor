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
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5448's read shape, from the executed plan of <see cref="PgFactCollector.PlanRegressionDailySql"/> on a store with
/// three servers' worth of intervals, so a regression that reads the whole table (or reads the closed days' raw
/// intervals again) fails here and not on a field store.
///
/// <para>The closed days (<c>T-14</c> to <c>T-3</c>) are built through the real builder for every server, the read is
/// bound the way the collector binds it, and the plan is read with <c>EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)</c>.
/// Parallelism is off for the session so the per-node row counts are the whole counts and not per-worker averages.</para>
///
/// <para>Two shapes. On the heap (the interval table today) the live half reads through the one index it has,
/// <c>first_execution_time</c>, from the live floor up, so it touches the open days of every server and returns one
/// server's; the test asserts what holds with that index (it does not scan the whole table, it keeps rows read under a
/// bound set by the open days, and the day half reads <c>plan_regression_daily</c> through its unique index) and writes the
/// rows read, the rows returned and the blocks touched to <c>DARLING_5448_SHAPE_OUT</c> when that is set, which is where the
/// numbers in the PR come from. On a hypertable (the table is converted by hand: the product does not partition it yet,
/// though <c>QueryStoreIntervalLatest.ChunkFloorsSql</c> already has the arm for it) the live half scans only the chunks
/// at or above the live floor.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it. */
public sealed class PlanRegressionDailyReadShapeLiveTests
{
    private const int ServerA = -5448201;
    private static readonly int[] Servers = [ServerA, -5448202, -5448203];

    /// <summary>Queries per server per day; each has two plans, so the rows per server per day are twice this.</summary>
    private const int QueriesPerDay = 1500;

    private const int RowsPerServerDay = QueriesPerDay * 2;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task OnTheHeap_TheLiveHalfReadsOnlyFromTheLiveFloorUp_AndTheDayHalfUsesItsUniqueIndex()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 read-shape test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var (today, built) = await SeedAndBuildAsync(connection, ct);
        var floor = PgFactCollector.PlanRegressionWindowFloor(today.AddDays(-14));
        var builtDays = built.Select(DateOnly.FromDateTime).ToList();
        Assert.Equal(12, builtDays.Count);
        Assert.Equal(builtDays, await BuiltDaysAsync(connection, floor, ct));

        var totalRows = await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_interval_latest", ct);
        var openDays = Enumerable.Range(0, 3).Select(i => today.AddDays(-2 + i)).ToList();
        var expectedReturned = openDays.Count * RowsPerServerDay;
        Assert.Equal(Servers.Length * 17L * RowsPerServerDay, totalRows);

        var plan = await ExplainAsync(connection, today, floor, builtDays, ct);
        var live = ScansOf(plan, "query_store_interval_latest").ToList();
        var daily = ScansOf(plan, "plan_regression_daily").ToList();
        Assert.NotEmpty(live);
        Assert.NotEmpty(daily);

        var liveRead = live.Sum(s => s.RowsRead);
        var liveReturned = live.Sum(s => s.RowsReturned);
        var liveBlocks = live.Sum(s => s.Blocks);
        var dailyRead = daily.Sum(s => s.RowsRead);
        var dailyReturned = daily.Sum(s => s.RowsReturned);
        var dailyTotal = await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily", ct);

        Report("heap", totalRows, liveRead, liveReturned, liveBlocks, dailyTotal, dailyRead, dailyReturned, string.Join(",", live.Select(s => s.NodeType)));

        /* What the read must return from its live half: the open days of ONE server, nothing of the closed days. */
        Assert.Equal(expectedReturned, liveReturned);

        /* It never reads the closed days' raw rows: the first_execution_time index starts at the live floor, so what it
           reads is at most the floor day and the open days of every server (four of the seventeen seeded days). A sequential scan
           (or an index with no usable bound) reads every row of every server. */
        var readCeiling = Servers.Length * 5L * RowsPerServerDay;
        Assert.True(liveRead <= readCeiling,
            $"the live half read {liveRead} interval rows of {totalRows}; the floor day plus the open days of every server is {readCeiling}");
        Assert.DoesNotContain(live, s => s.NodeType == "Seq Scan");

        /* The day half. With three servers' totals the planner may well prefer a sequential scan of the small daily table
           (a third of it is this server's), which the report above records; what must hold is that the unique index
           serves the branch, so that on a store with many servers the read touches this server's built days only. With
           sequential scans off the plan has to use ux_plan_regression_daily and read no more than one server's share. */
        var indexed = ScansOf(await ExplainAsync(connection, today, floor, builtDays, ct, noSeqScan: true), "plan_regression_daily").ToList();
        Assert.NotEmpty(indexed);
        Assert.DoesNotContain(indexed, s => s.NodeType == "Seq Scan");
        Assert.All(indexed, s => Assert.Equal("ux_plan_regression_daily", s.IndexName));
        var indexedRead = indexed.Sum(s => s.RowsRead);
        Assert.True(indexedRead <= dailyTotal / Servers.Length,
            $"the day half read {indexedRead} of {dailyTotal} daily rows through the index, which is more than one server's share");
        Report("heap, seq scans off", totalRows, 0, 0, 0, dailyTotal, indexedRead, indexed.Sum(s => s.RowsReturned),
            string.Join(",", indexed.Select(s => s.NodeType + ":" + s.IndexName)));
    }

    [Fact]
    public async Task OnAHypertable_TheLiveHalfScansOnlyTheChunksAtOrAboveTheLiveFloor()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 read-shape test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.SkipWhen(!timescaleEnabled, "TimescaleDB is not available on this cluster.");
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

        /* A one-day chunk, so the 17 seeded days are 17 chunks and the floor's reach is countable. */
        await ExecAsync(connection,
            "SELECT create_hypertable('collect.query_store_interval_latest', by_range('first_execution_time', INTERVAL '1 day'), migrate_data => true)", ct);

        var (today, built) = await SeedAndBuildAsync(connection, ct);
        var floor = PgFactCollector.PlanRegressionWindowFloor(today.AddDays(-14));
        var builtDays = built.Select(DateOnly.FromDateTime).ToList();
        Assert.Equal(12, builtDays.Count);

        var chunks = await ScalarAsync(connection,
            "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_store_interval_latest'", ct);
        Assert.True(chunks >= 17, $"the seed made {chunks} chunk(s); the test needs the 17 days in separate chunks");

        var plan = await ExplainAsync(connection, today, floor, builtDays, ct);
        var live = ScansOf(plan, "_hyper_").ToList();
        Assert.NotEmpty(live);
        var livePlan = live.Select(s => s.Relation).Distinct().ToList();

        /* The live floor is the day before the first open day, T-3; chunks for T-3 through T are 4 (a skewed server's
           future rows could add more, none are seeded). Excluded chunks do not appear as scans at all. */
        var liveReturned = live.Sum(s => s.RowsReturned);
        Report("hypertable", await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_interval_latest", ct),
            live.Sum(s => s.RowsRead), liveReturned, live.Sum(s => s.Blocks), 0, 0, 0, $"{livePlan.Count} chunk(s) scanned of {chunks}");
        Assert.True(livePlan.Count <= 5, $"the live half scanned {livePlan.Count} of {chunks} chunks; the floor reaches back to T-3 only");
        Assert.Equal(3 * RowsPerServerDay, liveReturned);
    }

    /* ---------------------------------------------------------------------------------------------------------- */

    /// <summary>
    /// Seeds <see cref="Servers"/> with seventeen days of two-plan intervals ending today (T-16 through T), gives each a
    /// coverage claim below the window, and builds T-14 through T-3 for every server through the real builder. Returns
    /// today (naive UTC midnight) and the days built.
    /// </summary>
    private static async Task<(DateTime Today, List<DateTime> Built)> SeedAndBuildAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var today = now.Date;

        /* The late-row trigger fires per row for anything in its 16-day reach; the seed is not a late arrival, so it is
           off for the load and back on for the builds. */
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
        foreach (var server in Servers)
        {
            await using var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
SELECT
    @s, 'shapedb', q, q * 10 + p, NULL, (q % 22) + 1,
    d.d + (q % 22) * interval '1 hour',
    d.d + (q % 22) * interval '1 hour' + interval '35 minutes',
    'hash' || (q * 10 + p), 'qh' || q, 100, 1000 * p, 3000 * p,
    d.d + (q % 22) * interval '1 hour' + interval '30 minutes', false, 0
FROM generate_series((@t::date - 16)::timestamp, @t::timestamp, interval '1 day') AS d (d)
CROSS JOIN generate_series(1, @q) AS q
CROSS JOIN generate_series(1, 2) AS p", connection) { CommandTimeout = 120 };
            seed.Parameters.AddWithValue("s", server);
            seed.Parameters.AddWithValue("t", NpgsqlDbType.Timestamp, today);
            seed.Parameters.AddWithValue("q", QueriesPerDay);
            await seed.ExecuteNonQueryAsync(ct);

            await using var coverage = new NpgsqlCommand(
                "INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES (@s, @f, @f) ON CONFLICT (server_id) DO UPDATE SET filled_since = EXCLUDED.filled_since",
                connection);
            coverage.Parameters.AddWithValue("s", server);
            coverage.Parameters.AddWithValue("f", NpgsqlDbType.Timestamp, today.AddDays(-20));
            await coverage.ExecuteNonQueryAsync(ct);
        }

        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER trg_plan_regression_daily_late", ct);

        var built = new List<DateTime>();
        for (var d = -14; d <= -3; d++)
        {
            var day = today.AddDays(d);
            built.Add(day);
            foreach (var server in Servers)
            {
                var rows = await PlanRegressionDaily.BuildDayAsync(connection, server, DateOnly.FromDateTime(day), now, ct);
                Assert.Equal(RowsPerServerDay, rows);
            }
        }

        await ExecAsync(connection, "ANALYZE collect.query_store_interval_latest", ct);
        await ExecAsync(connection, "ANALYZE collect.plan_regression_daily", ct);
        await ExecAsync(connection, "ANALYZE collect.plan_regression_daily_built", ct);
        return (today, built);
    }

    private static async Task<List<DateOnly>> BuiltDaysAsync(NpgsqlConnection connection, DateTime floor, CancellationToken ct)
    {
        var days = new List<DateOnly>();
        await using var command = new NpgsqlCommand(PlanRegressionDaily.BuiltDaysSql, connection);
        command.Parameters.AddWithValue(ServerA);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) days.Add(reader.GetFieldValue<DateOnly>(0));
        return days;
    }

    /// <summary>The executed plan of the daily read, bound the way the collector binds it, parallelism off for the session.</summary>
    private static async Task<JsonElement> ExplainAsync(
        NpgsqlConnection connection, DateTime today, DateTime floor, List<DateOnly> builtDays, CancellationToken ct, bool noSeqScan = false)
    {
        await ExecAsync(connection, "SET max_parallel_workers_per_gather = 0", ct);
        await ExecAsync(connection, noSeqScan ? "SET enable_seqscan = off" : "RESET enable_seqscan", ct);
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + PgFactCollector.PlanRegressionDailySql, connection);
        command.Parameters.AddWithValue(ServerA);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = builtDays.ToArray() });
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, PgFactCollector.PlanRegressionLiveFloor(floor, builtDays));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor.AddDays(-1));
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        return JsonDocument.Parse(json).RootElement[0].GetProperty("Plan").Clone();
    }

    private readonly record struct Scan(string Relation, string NodeType, string? IndexName, double RowsRead, double RowsReturned, double Blocks);

    /// <summary>
    /// Every scan of a relation whose name contains <paramref name="relationPart"/>, from the executed plan: rows read are
    /// those returned plus those the node's filter and index recheck threw away, times the loops (a row is read once per
    /// loop); blocks are the node's shared hits and reads.
    /// </summary>
    private static IEnumerable<Scan> ScansOf(JsonElement node, string relationPart)
    {
        if (node.TryGetProperty("Relation Name", out var relation)
            && relation.GetString()!.Contains(relationPart, StringComparison.Ordinal)
            && node.GetProperty("Node Type").GetString()!.Contains("Scan", StringComparison.Ordinal)
            && !node.GetProperty("Node Type").GetString()!.StartsWith("Bitmap Index", StringComparison.Ordinal))
        {
            var loops = node.GetProperty("Actual Loops").GetDouble();
            var returned = node.GetProperty("Actual Rows").GetDouble() * loops;
            var removed = (Number(node, "Rows Removed by Filter") + Number(node, "Rows Removed by Index Recheck")) * loops;
            var blocks = Number(node, "Shared Hit Blocks") + Number(node, "Shared Read Blocks");
            var index = node.TryGetProperty("Index Name", out var i) ? i.GetString() : null;
            /* A bitmap heap scan names the index only on its child; a seq scan has none. */
            index ??= ChildIndexName(node);
            yield return new Scan(relation.GetString()!, node.GetProperty("Node Type").GetString()!, index, returned + removed, returned, blocks);
        }

        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray())
            {
                foreach (var scan in ScansOf(child, relationPart)) yield return scan;
            }
        }
    }

    private static string? ChildIndexName(JsonElement node)
    {
        if (!node.TryGetProperty("Plans", out var plans)) return null;
        foreach (var child in plans.EnumerateArray())
        {
            if (child.TryGetProperty("Index Name", out var i)) return i.GetString();
        }

        return null;
    }

    private static double Number(JsonElement node, string name) => node.TryGetProperty(name, out var v) ? v.GetDouble() : 0;

    private static void Report(string shape, long totalRows, double liveRead, double liveReturned, double liveBlocks,
        long dailyTotal, double dailyRead, double dailyReturned, string detail)
    {
        var line = string.Create(CultureInfo.InvariantCulture,
            $"{shape}: table_rows={totalRows} live_rows_read={liveRead} live_rows_returned={liveReturned} live_blocks={liveBlocks} daily_rows={dailyTotal} daily_rows_read={dailyRead} daily_rows_returned={dailyReturned} nodes={detail}");
        TestContext.Current.SendDiagnosticMessage(line);
        var path = Environment.GetEnvironmentVariable("DARLING_5448_SHAPE_OUT");
        if (!string.IsNullOrEmpty(path)) File.AppendAllText(path, line + Environment.NewLine);
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
