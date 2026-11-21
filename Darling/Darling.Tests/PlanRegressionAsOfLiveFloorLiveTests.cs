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
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9225: the PLAN_REGRESSION read of a pass anchored a few hours back reads the live half of the interval table from one
/// interval length below the first unbuilt day, where it read from a whole day below. The floor day's rows are all in the built
/// totals except the ones that cross midnight, so a whole-day floor read that day and threw nearly all of it away.
///
/// <para>The store has a server with sixty-minute intervals reported in the health captures, closed days built through T-3, and a
/// read anchored at 22:00 on T-1, so T-2 is the first unbuilt day. One query's costly plan first ran at 23:40 on T-3 and last
/// ran at 00:10 on T-2: it is in the live half and it sits between the new floor and midnight, so a floor that dropped it would lose the
/// only regression.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it. */
public sealed class PlanRegressionAsOfLiveFloorLiveTests
{
    private const int ServerA = -9225201;
    private static readonly int[] Servers = [ServerA, -9225202, -9225203];
    private const int QueriesPerDay = 1000;
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the #9225 live tests.";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task WithATrustedIntervalLength_TheFloorIsOneIntervalBelowTheFirstUnbuiltDay_ReadsFarFewerRows_AndReturnsTheSameRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await RigAsync(BaseConnectionString!, intervalMinutes: [60], oldestHealthDaysBack: 25, ct);

        var wholeDay = PgFactCollector.PlanRegressionLiveFloorWithWatermarks(rig.WindowFloor, rig.BuiltDays);
        Assert.Equal(rig.FirstUnbuilt.AddDays(-1), wholeDay);

        var resolved = await PgFactCollector.ResolvePlanRegressionLiveFloorAsync(
            rig.Connection, null, ServerA, rig.WindowFloor, rig.BuiltDays, rig.End, rig.Now, 60, ct);
        Assert.Equal(rig.FirstUnbuilt.AddHours(-1), resolved);

        var narrow = await ReadAsync(rig, resolved, ct);
        var wide = await ReadAsync(rig, wholeDay, ct);

        TestContext.Current.SendDiagnosticMessage(string.Create(CultureInfo.InvariantCulture,
            $"as_of live half: resolved floor reads {narrow.LiveRowsRead} rows in {narrow.LiveBlocks} blocks; whole-day floor reads {wide.LiveRowsRead} rows in {wide.LiveBlocks} blocks"));

        /* The floor day (T-3) holds a day of rows for every server and the new floor leaves only its last hour. */
        Assert.True(narrow.LiveRowsRead + QueriesPerDay <= wide.LiveRowsRead,
            $"the live half read {narrow.LiveRowsRead} interval rows from the resolved floor and {wide.LiveRowsRead} from a whole day below");
        Assert.NotEmpty(narrow.Rows);
        Assert.Equal(wide.Rows, narrow.Rows);
        Assert.Contains(narrow.Rows, r => r.StartsWith("900001|", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("no capture at all")]
    [InlineData("captures start too late")]
    [InlineData("a longer length was reported")]
    public async Task WithoutATrustedIntervalLength_TheFloorStaysAWholeDay_AndTheRowsAreTheSame(string situation)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        /* "Captures start too late": the oldest capture is inside the day before the first unbuilt day, so a setting from before it may have gone unseen. */
        var (lengths, daysBack) = situation switch
        {
            "no capture at all" => (Array.Empty<int>(), 0),
            "captures start too late" => (new[] { 60 }, 2),
            _ => (new[] { 60, 1440 }, 25),
        };
        await using var rig = await RigAsync(BaseConnectionString!, lengths, daysBack, ct);

        var wholeDay = PgFactCollector.PlanRegressionLiveFloorWithWatermarks(rig.WindowFloor, rig.BuiltDays);
        var resolved = await PgFactCollector.ResolvePlanRegressionLiveFloorAsync(
            rig.Connection, null, ServerA, rig.WindowFloor, rig.BuiltDays, rig.End, rig.Now, 60, ct);
        Assert.Equal(wholeDay, resolved);

        var floor = await ReadAsync(rig, resolved, ct);
        Assert.NotEmpty(floor.Rows);
        Assert.Contains(floor.Rows, r => r.StartsWith("900001|", StringComparison.Ordinal));
    }

    [Fact]
    public async Task APartialBuiltDaysWatermarkBelowTheNewFloor_StillSetsTheFloor()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await RigAsync(BaseConnectionString!, intervalMinutes: [60], oldestHealthDaysBack: 25, ct);

        /* T-3 built through 06:00: its watermark is below the resolved floor (23:00) and sets the floor, as the lowest partial watermark does without a resolved length. */
        var partial = rig.BuiltDays.Select(b => b.Day == DateOnly.FromDateTime(rig.FirstUnbuilt.AddDays(-1))
            ? new PlanRegressionDaily.BuiltDay(b.Day, rig.FirstUnbuilt.AddDays(-1).AddHours(6)) : b).ToList();
        var resolved = await PgFactCollector.ResolvePlanRegressionLiveFloorAsync(
            rig.Connection, null, ServerA, rig.WindowFloor, partial, rig.End, rig.Now, 60, ct);
        Assert.Equal(rig.FirstUnbuilt.AddDays(-1).AddHours(6), resolved);
    }

    [Fact]
    public async Task TheFact_OfAPassAnchoredBack_BindsTheResolvedFloor_AndReportsTheSameRegression()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await RigAsync(BaseConnectionString!, intervalMinutes: [60], oldestHealthDaysBack: 25, ct);
        await using var postgres = NpgsqlDataSource.Create(rig.Scratch.ConnectionString);

        /* The floor the collector binds, taken at the point it binds it (PgFactCollector.TestOnlyLiveFloorBound). */
        async Task<(string Fact, DateTime Floor)> RunAsync()
        {
            var bound = new List<DateTime>();
            PgFactCollector.TestOnlyLiveFloorBound.Value = bound.Add;
            try
            {
                var pass = new AnalysisContext
                {
                    ServerId = ServerA,
                    ServerName = "asof-floor",
                    TimeRangeStart = rig.End.AddHours(-4),
                    TimeRangeEnd = rig.End,
                    ServerUtcOffset = TimeSpan.Zero,
                    AsOfUtc = rig.End,
                };
                var fact = (await new PgFactCollector(postgres).CollectFactsAsync(pass)).SingleOrDefault(f => f.Key == "PLAN_REGRESSION");
                Assert.NotNull(fact);
                Assert.NotNull(pass.PlanRegressionWindowStart);
                Assert.Equal(1, fact!.Metadata["offender_count"]);
                return (JsonSerializer.Serialize(fact.Metadata.OrderBy(m => m.Key)), Assert.Single(bound));
            }
            finally
            {
                PgFactCollector.TestOnlyLiveFloorBound.Value = null;
            }
        }

        var trusted = await RunAsync();
        Assert.Equal(rig.FirstUnbuilt.AddHours(-1), trusted.Floor);

        /* The same store with no capture of the interval length: the whole-day floor. */
        await Exec(rig.Connection, "DELETE FROM collect.query_store_health", ct);
        var untrusted = await RunAsync();
        Assert.Equal(rig.FirstUnbuilt.AddDays(-1), untrusted.Floor);

        Assert.Equal(untrusted.Fact, trusted.Fact);
    }

    [Fact]
    public async Task AFirstUnbuiltDayPastTheWideTablesHorizon_KeepsTheWholeDayFloor_AndTheSameRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await RigAsync(BaseConnectionString!, intervalMinutes: [60], oldestHealthDaysBack: 25, ct);

        /* Days built only before T-11, so the first unbuilt day is older than the ten days the wide table keeps. The trust test lists its
           databases from the wide table and cannot see a database that only the 15-day latest table still holds. */
        var older = rig.BuiltDays.Where(b => b.Day < DateOnly.FromDateTime(rig.Now.Date.AddDays(-11))).ToList();
        Assert.NotEmpty(older);
        var firstUnbuilt = older.Max(b => b.Day).AddDays(1).ToDateTime(TimeOnly.MinValue);
        Assert.True(firstUnbuilt < rig.Now.AddDays(-QueryStoreIntervalPartitions.WideHorizonDays));

        var wholeDay = PgFactCollector.PlanRegressionLiveFloorWithWatermarks(rig.WindowFloor, older);
        Assert.Equal(firstUnbuilt.AddDays(-1), wholeDay);
        var resolved = await PgFactCollector.ResolvePlanRegressionLiveFloorAsync(
            rig.Connection, null, ServerA, rig.WindowFloor, older, rig.End, rig.Now, 60, ct);
        Assert.Equal(wholeDay, resolved);

        /* The same store and the same length with the first unbuilt day inside the horizon: one interval below it. */
        var inside = await PgFactCollector.ResolvePlanRegressionLiveFloorAsync(
            rig.Connection, null, ServerA, rig.WindowFloor, rig.BuiltDays, rig.End, rig.Now, 60, ct);
        Assert.Equal(rig.FirstUnbuilt.AddHours(-1), inside);

        var kept = await ReadAsync(rig, older, resolved, ct);
        var reference = await ReadAsync(rig, rig.BuiltDays, inside, ct);
        Assert.NotEmpty(kept.Rows);
        Assert.Equal(reference.Rows, kept.Rows);
    }

    /* ---------------------------------------------------------------------------------------------------------- */

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required DateTime End { get; init; }
        public required DateTime Now { get; init; }
        public required DateTime WindowFloor { get; init; }
        public required List<PlanRegressionDaily.BuiltDay> BuiltDays { get; init; }
        public DateTime FirstUnbuilt => End.Date.AddDays(-1);

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static async Task<Rig> RigAsync(string baseCs, int[] intervalMinutes, int oldestHealthDaysBack, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(baseCs, ct, emptyStore: true);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            /* One anchor owns both ends: the day after the pass's end day is "today", so no midnight between two clock reads can split them. */
            var end = LiveClock.AnchoredEndNotAfterNow();
            var today = end.Date.AddDays(1);
            var now = today;
            /* The pass's comparison window is 14 days back from its start, four hours before the end. */
            var windowFloor = PgFactCollector.PlanRegressionWindowFloor(end.AddHours(-4).AddDays(-PgFactCollector.PlanRegressionWindowDays));

            await Exec(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
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
        @s, 'asofdb', q, q * 10 + p, NULL, (q % 22) + 1,
        d.d + (q % 22) * interval '1 hour',
        d.d + (q % 22) * interval '1 hour' + interval '35 minutes',
        'hash' || (q * 10 + p), 'qh' || q, 100, 1000, 3000,
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

            /* One query on the first server: a cheap plan ten days back (a built day), and a costly one whose interval crosses T-2's
               midnight. Its first execution sits between the resolved floor (23:00) and midnight, and its last one is on T-2. */
            var firstUnbuilt = end.Date.AddDays(-1);
            await InsertAsync(connection, 900001, 9000011, "0xCHEAP", 10_000, end.Date.AddDays(-10).AddHours(10), ct);
            await InsertAsync(connection, 900001, 9000012, "0xCOSTLY", 500_000, firstUnbuilt.AddMinutes(-20), ct);
            await Exec(connection, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER trg_plan_regression_daily_late", ct);

            foreach (var length in intervalMinutes)
            {
                await using var health = new NpgsqlCommand(@"
    INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, interval_length_minutes)
    SELECT row_number() OVER (), c.c, @s, 'asof-floor', 'asofdb', @len
    FROM generate_series(@from::timestamp, @to::timestamp, interval '6 hours') AS c(c)", connection);
                health.Parameters.AddWithValue("s", ServerA);
                health.Parameters.AddWithValue("len", length);
                health.Parameters.AddWithValue("from", NpgsqlDbType.Timestamp, today.AddDays(-oldestHealthDaysBack));
                health.Parameters.AddWithValue("to", NpgsqlDbType.Timestamp, today.AddHours(1));
                await health.ExecuteNonQueryAsync(ct);
            }

            /* The window's first day through T-3 built whole for every server; the pass anchored at T-1 22:00 takes them all as whole by then. */
            for (var day = windowFloor; day <= today.AddDays(-3); day = day.AddDays(1))
            {
                foreach (var server in Servers)
                {
                    await PlanRegressionDaily.BuildDayAsync(connection, server, DateOnly.FromDateTime(day), now, ct);
                }
            }

            await Exec(connection, "ANALYZE collect.query_store_interval_latest", ct);
            await Exec(connection, "ANALYZE collect.plan_regression_daily", ct);
            await Exec(connection, "ANALYZE collect.plan_regression_daily_built", ct);

            var built = await QueryStoreIntervalLatest.ReadBuiltDaysAsync(connection, null, ServerA, windowFloor, 60, null, _ => false, ct);
            var wholeBy = end.AddDays(-PgFactCollector.PlanRegressionSkewMarginDays);
            built = built.Where(b => b.ThroughOrDayEnd <= wholeBy).ToList();
            Assert.Equal(13, built.Count);
            return new Rig { Scratch = scratch, Connection = connection, End = end, Now = now, WindowFloor = windowFloor, BuiltDays = built };
        }
        catch
        {
            /* The helper never returns a store it failed to finish, so the caller never owns one to drop. */
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
            throw;
        }
    }

    private static async Task InsertAsync(NpgsqlConnection connection, long queryId, long planId, string hash, long cpuUs, DateTime firstExec, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
VALUES (@s, 'asofdb', @q, @p, NULL, @p, @first, @col, @h, @qh, 100, @cpu, @dur, @last, false, 0)", connection);
        cmd.Parameters.AddWithValue("s", ServerA);
        cmd.Parameters.AddWithValue("q", queryId);
        cmd.Parameters.AddWithValue("p", planId);
        cmd.Parameters.AddWithValue("first", NpgsqlDbType.Timestamp, firstExec);
        cmd.Parameters.AddWithValue("col", NpgsqlDbType.Timestamp, firstExec.AddMinutes(55));
        cmd.Parameters.AddWithValue("h", hash);
        cmd.Parameters.AddWithValue("qh", "qh" + queryId);
        cmd.Parameters.AddWithValue("cpu", cpuUs);
        cmd.Parameters.AddWithValue("dur", cpuUs * 3);
        cmd.Parameters.AddWithValue("last", NpgsqlDbType.Timestamp, firstExec.AddMinutes(30));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private readonly record struct Read(List<string> Rows, double LiveRowsRead, double LiveBlocks);

    /// <summary>The anchored daily statement bound the way the collector binds it with <paramref name="liveFloor"/>: the rows it returns, and the interval rows its live half read (those returned plus those a filter threw away).</summary>
    private static Task<Read> ReadAsync(Rig rig, DateTime liveFloor, CancellationToken ct) =>
        ReadAsync(rig, rig.BuiltDays, liveFloor, ct);

    private static async Task<Read> ReadAsync(Rig rig, List<PlanRegressionDaily.BuiltDay> builtDays, DateTime liveFloor, CancellationToken ct)
    {
        await Exec(rig.Connection, "SET max_parallel_workers_per_gather = 0", ct);
        var rows = new List<string>();
        await using (var run = Bind(rig, PgFactCollector.PlanRegressionDailyAsOfSql, builtDays, liveFloor))
        await using (var reader = await run.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i =>
                    reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture))));
            }
        }

        await using var explain = Bind(rig, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + PgFactCollector.PlanRegressionDailyAsOfSql, builtDays, liveFloor);
        var plan = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(ct))!).RootElement[0].GetProperty("Plan");
        return new Read(rows, RowsRead(plan), Blocks(plan));
    }

    private static NpgsqlCommand Bind(Rig rig, string sql, List<PlanRegressionDaily.BuiltDay> builtDays, DateTime liveFloor)
    {
        var cmd = new NpgsqlCommand(sql, rig.Connection);
        cmd.Parameters.AddWithValue(ServerA);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, rig.WindowFloor);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = builtDays.Select(b => b.Day).ToArray() });
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, liveFloor);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, rig.WindowFloor.AddDays(-1));
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = builtDays.Select(b => b.ThroughOrDayEnd).ToArray() });
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, rig.End);
        return cmd;
    }

    /// <summary>Rows read by every scan of the interval table in the executed plan: returned plus removed by a filter or an index recheck, times the loops.</summary>
    private static double RowsRead(JsonElement node)
    {
        double total = 0;
        if (node.TryGetProperty("Relation Name", out var relation)
            && relation.GetString()!.Contains("query_store_interval_latest", StringComparison.Ordinal)
            && node.GetProperty("Node Type").GetString()!.Contains("Scan", StringComparison.Ordinal)
            && !node.GetProperty("Node Type").GetString()!.StartsWith("Bitmap Index", StringComparison.Ordinal))
        {
            var loops = node.GetProperty("Actual Loops").GetDouble();
            total += (node.GetProperty("Actual Rows").GetDouble() + Number(node, "Rows Removed by Filter") + Number(node, "Rows Removed by Index Recheck")) * loops;
        }

        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray()) total += RowsRead(child);
        }

        return total;
    }

    /// <summary>Shared blocks (hit and read) touched by every scan of the interval table in the executed plan.</summary>
    private static double Blocks(JsonElement node)
    {
        double total = 0;
        if (node.TryGetProperty("Relation Name", out var relation)
            && relation.GetString()!.Contains("query_store_interval_latest", StringComparison.Ordinal)
            && node.GetProperty("Node Type").GetString()!.Contains("Scan", StringComparison.Ordinal))
        {
            total += Number(node, "Shared Hit Blocks") + Number(node, "Shared Read Blocks");
        }

        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray()) total += Blocks(child);
        }

        return total;
    }

    private static double Number(JsonElement node, string name) => node.TryGetProperty(name, out var v) ? v.GetDouble() : 0;

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
