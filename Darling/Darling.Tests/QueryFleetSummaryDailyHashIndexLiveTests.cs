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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch databases through ScratchPostgres and never touches another test's rows. */

/// <summary>
/// #9223 (V292): the covering <c>(query_hash, bucket DESC)</c> index on the corrected daily Query Store rollup. A fresh store and an upgraded one
/// both end with it, a second convergence pass and a second run of the rung change nothing, the <c>mid</c> arm of the Query Store fleet statement
/// reads the daily rollup through it, and the fleet summary's rows are the same with and without it.
/// </summary>
public sealed class QueryFleetSummaryDailyHashIndexLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9223 daily hash index tests.";
    private const string Hash = "0xQS9223";
    private const string IndexName = TimescaleSupport.QueryStoreCorrectedDailyHashIndexName;
    private const int ServerCount = 12;
    private const int Groups = 300;

    private sealed record Store(ScratchPostgres Scratch, NpgsqlConnection Connection);

    /// <summary>The window ends at noon eight days back, so every whole day in it is older than the daily policy's four-day re-read span and the mid
    /// route may read it from the daily rollup (the shape #8570's tests use).</summary>
    private sealed record Timeline(DateTime End, int HoursBack)
    {
        public DateTime Start => End.AddHours(-HoursBack);
        public DateTime DStart => Start.Date.AddDays(1);
        public DateTime DEnd => End.Date;
    }

    private static Timeline NewTimeline()
    {
        var end = DateTime.SpecifyKind(LiveClock.Now().Date.AddDays(-8).AddHours(12), DateTimeKind.Unspecified);
        return new Timeline(end, 167);
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<Store> CreateStoreAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
            Assert.SkipWhen(!timescaleEnabled, "The live #9223 tests need TimescaleDB.");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
            await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            return new Store(scratch, connection);
        }
        catch
        {
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
            throw;
        }
    }

    private static async Task Refresh(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await refresh.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The corrected daily rollup's materialization hypertable, as <c>schema.name</c>.</summary>
    private static async Task<string> DailyMaterializationAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name)
              FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect' AND view_name = $1", c);
        command.Parameters.AddWithValue(TimescaleSupport.QueryStoreStatsCorrectedDailyView);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The state of the index on the daily materialization: null when there is none, otherwise its oid, its key and included column counts, and the
    /// leading key column. The oid changes when the index is rebuilt, so an unchanged oid across a pass means the pass issued no DDL against it.</summary>
    private sealed record IndexState(long Oid, int KeyColumns, int IncludedColumns, string LeadingColumn, int Copies);

    private static async Task<IndexState?> IndexAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"SELECT ic.oid::bigint, i.indnkeyatts::int, (i.indnatts - i.indnkeyatts)::int, a.attname,
                     (SELECT count(*)::int FROM pg_index i2 JOIN pg_class ic2 ON ic2.oid = i2.indexrelid JOIN pg_class t2 ON t2.oid = i2.indrelid
                      WHERE ic2.relname LIKE $1 AND t2.relname LIKE '_materialized_hypertable_%')
              FROM pg_index i
              JOIN pg_class ic ON ic.oid = i.indexrelid
              JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
              WHERE ic.relname = $2 AND i.indrelid = $3::regclass", c);
        command.Parameters.AddWithValue(IndexName + "%");
        command.Parameters.AddWithValue(IndexName);
        command.Parameters.AddWithValue(await DailyMaterializationAsync(c, ct));
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct)
            ? new IndexState(reader.GetInt64(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetString(3), reader.GetInt32(4))
            : null;
    }

    /// <summary>The kept-index step a fresh store (and every convergence pass) runs right after the daily view's CREATE.</summary>
    private static async Task RunConvergenceStepAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var tx = await c.BeginTransactionAsync(ct);
        await Exec(c, TimescaleSupport.QueryStoreRollupKeptIndexesSql[TimescaleSupport.QueryStoreStatsCorrectedDailyView], ct);
        await tx.CommitAsync(ct);
    }

    private static async Task RunV292Async(NpgsqlConnection c, CancellationToken ct)
    {
        var sql = PgMigrations.Scripts.Single(m => m.Version == 292).Sql;
        await using var tx = await c.BeginTransactionAsync(ct);
        await Exec(c, sql, ct);
        await tx.CommitAsync(ct);
    }

    private static async Task DropIndexAsync(NpgsqlConnection c, CancellationToken ct)
        => await Exec(c, $@"DO $$ DECLARE v regclass; BEGIN
            SELECT c.oid::regclass INTO v FROM pg_class c WHERE c.relkind = 'i' AND c.relname = '{IndexName}';
            IF v IS NOT NULL THEN EXECUTE format('DROP INDEX %s', v); END IF; END $$;", ct);

    private static void AssertIsTheCoveringIndex(IndexState? state, string when)
    {
        Assert.True(state is not null, "the daily rollup's materialization has no " + IndexName + " " + when);
        Assert.Equal("query_hash", state.LeadingColumn);
        Assert.Equal(2, state.KeyColumns);
        Assert.Equal(5, state.IncludedColumns);
        Assert.Equal(1, state.Copies);
    }

    /// <summary>A fresh store ends with the index, and a second convergence pass keeps the same index object and leaves no temporary one.
    /// Red on a store whose daily view the kept-index step never gave an index.</summary>
    [Fact]
    public async Task FreshStore_HasTheIndex_AndASecondConvergencePassChangesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var c = store.Connection;
        var bodySucceeded = false;
        try
        {
            var fresh = await IndexAsync(c, ct);
            AssertIsTheCoveringIndex(fresh, "on a fresh store");

            await RunConvergenceStepAsync(c, ct);
            var settled = await IndexAsync(c, ct);
            AssertIsTheCoveringIndex(settled, "after a second convergence pass");
            Assert.Equal(fresh!.Oid, settled!.Oid);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>An upgraded store (the index absent, as before V292) gets it from the rung, a second run keeps the same index object, and the convergence
    /// pass that follows does not rebuild it. Red when V292 does not build the index.</summary>
    [Fact]
    public async Task UpgradedStore_GetsTheIndexFromTheRung_AndASecondRunChangesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var c = store.Connection;
        var bodySucceeded = false;
        try
        {
            await DropIndexAsync(c, ct);
            Assert.Null(await IndexAsync(c, ct));

            await RunV292Async(c, ct);
            var built = await IndexAsync(c, ct);
            AssertIsTheCoveringIndex(built, "after the rung ran");

            await RunV292Async(c, ct);
            var again = await IndexAsync(c, ct);
            AssertIsTheCoveringIndex(again, "after the rung ran twice");
            Assert.Equal(built!.Oid, again!.Oid);

            await RunConvergenceStepAsync(c, ct);
            Assert.Equal(built.Oid, (await IndexAsync(c, ct))!.Oid);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static void AddParameters(NpgsqlCommand command, Timeline t, DateTime rawFrom, DarlingMcpQueryFleetSummaryTools.DailyMidPlan mid)
    {
        command.Parameters.AddWithValue(NpgsqlDbType.Text, Hash);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t.Start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t.End, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(rawFrom, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, 200);
        command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Integer, DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(mid.DStart, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(mid.DEnd, DateTimeKind.Unspecified));
    }

    private static async Task<(DarlingMcpQueryFleetSummaryTools.DailyMidPlan Plan, DateTime RawFrom)> PlanAsync(NpgsqlDataSource pg, Timeline t, CancellationToken ct)
    {
        var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
        var route = await QueryStoreTrendRouting.ResolveAsync(coverage, pg, ct);
        Assert.True(route.UseRollup);
        var plan = DarlingMcpQueryFleetSummaryTools.PlanQueryStoreDailyMid(coverage, t.Start, t.End, route.RawStartUtc, LiveClock.Now());
        Assert.NotNull(plan);
        Assert.True(await DarlingMcpQueryFleetSummaryTools.DailyHasNoHoleAsync(pg, plan, ct));
        return (plan, route.RawStartUtc);
    }

    /// <summary>The mid statement under EXPLAIN (ANALYZE, BUFFERS): the data pages it read, and the index each scan of a daily chunk used (null for a scan with none).</summary>
    private sealed record MidPlan(long Pages, List<string?> DailyChunkScans);

    private static async Task<MidPlan> ExplainMidAsync(NpgsqlDataSource pg, Timeline t, DateTime rawFrom, DarlingMcpQueryFleetSummaryTools.DailyMidPlan plan,
        HashSet<string> dailyChunks, CancellationToken ct)
    {
        await using var command = pg.CreateCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + DarlingMcpQueryFleetSummaryTools.BuildQueryStoreMidSql());
        AddParameters(command, t, rawFrom, plan);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement[0].GetProperty("Plan");
        var scans = new List<string?>();
        Walk(root, dailyChunks, scans);
        return new MidPlan(ExplainBuffers.DataPages(root.Clone()), scans);
    }

    private static void Walk(JsonElement node, HashSet<string> dailyChunks, List<string?> scans)
    {
        if (node.TryGetProperty("Relation Name", out var relation) && dailyChunks.Contains(relation.GetString()!))
        {
            scans.Add(node.TryGetProperty("Index Name", out var index) ? index.GetString() : null);
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                Walk(child, dailyChunks, scans);
            }
        }
    }

    private static async Task<HashSet<string>> DailyChunksAsync(NpgsqlConnection c, CancellationToken ct)
    {
        var chunks = new HashSet<string>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(
            @"SELECT ch.chunk_name FROM timescaledb_information.chunks ch
              JOIN timescaledb_information.continuous_aggregates ca ON ca.materialization_hypertable_name = ch.hypertable_name AND ca.materialization_hypertable_schema = ch.hypertable_schema
              WHERE ca.view_name = $1", c);
        command.Parameters.AddWithValue(TimescaleSupport.QueryStoreStatsCorrectedDailyView);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            chunks.Add(reader.GetString(0));
        }

        return chunks;
    }

    /// <summary>Per daily chunk: its rows, its heap size and the size of its piece of the new index, for the diagnostic line.</summary>
    private static async Task<string> ChunkSizesAsync(NpgsqlConnection c, CancellationToken ct)
    {
        var parts = new List<string>();
        await using var command = new NpgsqlCommand(
            @"SELECT t.relname, t.reltuples::bigint, pg_relation_size(t.oid), pg_relation_size(i.indexrelid)
              FROM pg_index i
              JOIN pg_class ic ON ic.oid = i.indexrelid
              JOIN pg_class t ON t.oid = i.indrelid
              WHERE ic.relname LIKE '%' || $1 || '%' AND t.relname LIKE '_hyper_%'
              ORDER BY t.relname", c);
        command.Parameters.AddWithValue(IndexName);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            parts.Add($"{reader.GetString(0)}: {reader.GetInt64(1)} rows, heap {reader.GetInt64(2) / 1024} kB, index {reader.GetInt64(3) / 1024} kB");
        }

        return string.Join("; ", parts);
    }

    private sealed record Shaped(string? Server, string? Database, long Executions, double CpuMs, double ElapsedMs, string? First, string? Last);

    private static async Task<Shaped[]> ReplyAsync(NpgsqlDataSource pg, Timeline t, CancellationToken ct)
    {
        var asOf = t.End.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture) + "Z";
        var json = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(pg, Hash, t.HoursBack, as_of: asOf, limit: 200, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        Assert.False(doc.RootElement.TryGetProperty("error", out _), json);
        return doc.RootElement.GetProperty("servers").EnumerateArray()
            .Where(r => r.GetProperty("source").GetString() == DarlingMcpQueryFleetSummaryTools.QueryStoreSource)
            .Select(r => new Shaped(r.GetProperty("server_name").GetString(), r.GetProperty("database_name").GetString(), r.GetProperty("execution_count").GetInt64(),
                r.GetProperty("cpu_ms").GetDouble(), r.GetProperty("elapsed_ms").GetDouble(), r.GetProperty("first_hour").GetString(), r.GetProperty("last_hour").GetString()))
            .ToArray();
    }

    /// <summary>A store at the shape the field read hurts on: several servers, the hash on one row per day per pair among many other hashes, the daily
    /// rollup's chunks uncompressed. With the index the mid arm reads every daily chunk through it (red on the code before V292, where the arm scans the
    /// chunk or walks the bucket index), and the fleet summary's rows are identical with the index dropped. The pages read before and after are reported.</summary>
    [Fact]
    public async Task MidArm_ReadsTheDailyRollupThroughTheIndex_AndTheReplyIsUnchanged()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var c = store.Connection;
        var bodySucceeded = false;
        try
        {
            for (var i = 1; i <= ServerCount; i++)
            {
                await DarlingMcpTestData.RegisterServerAsync(c, -922300 - i, "p9223q-" + i, ct);
            }

            /* No autovacuum or autoanalyze on the relations this test reads, so the page counts and the plan do not move with a background pass (#8725). */
            var tables = new List<string> { "collect.query_store_stats" };
            await using (var relations = new NpgsqlCommand(
                @"SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name) FROM timescaledb_information.continuous_aggregates", c))
            await using (var reader = await relations.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    tables.Add(reader.GetString(0));
                }
            }

            foreach (var table in tables)
            {
                await Exec(c, $"ALTER TABLE {table} SET (autovacuum_enabled = false)", ct);
            }

            var from = t.Start.Date;
            await Exec(c, $@"INSERT INTO collect.query_store_stats
                  (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash, query_id, plan_id, execution_type_desc, replica_role,
                   runtime_stats_interval_id, interval_start_time_utc, first_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, interval_end_time_utc)
                SELECT 900000000::bigint + row_number() OVER (), h.ts + interval '10 minutes', -922300 - s, 'p9223q-' || s,
                  CASE WHEN s = 1 AND g = 1 AND (extract(day FROM h.ts))::int % 2 = 0 THEN NULL ELSE 'db' || (g % 10) END, 'dbo.P',
                  CASE WHEN g = 1 THEN '{Hash}' ELSE '0x' || g END, g, g, 'Regular', 'PRIMARY', g, h.ts, h.ts, 1 + (extract(hour FROM h.ts))::int % 5, 2000 + g, 1000 + g, 900, 400, h.ts + interval '1 hour'
                FROM generate_series(TIMESTAMP '{from:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{t.End.AddHours(-1):yyyy-MM-dd HH:mm:ss}', interval '1 hour') AS h(ts)
                CROSS JOIN generate_series(1, {ServerCount}) s CROSS JOIN generate_series(1, {Groups}) g
                WHERE extract(hour FROM h.ts)::int % 6 = 0 OR g = 1", ct);
            await Refresh(c, TimescaleSupport.QueryStoreStatsIntervalHourlyView, t.Start.Date.AddDays(-1), t.End, ct);
            await Refresh(c, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, t.Start.Date.AddDays(-1), t.End, ct);
            await Refresh(c, TimescaleSupport.QueryStoreStatsCorrectedDailyView, t.Start.Date, t.DEnd, ct);
            await Exec(c, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            var daily = await DailyMaterializationAsync(c, ct);
            foreach (var table in new[] { "collect.query_store_stats", daily })
            {
                await Exec(c, $"VACUUM (ANALYZE) {table}", ct);
            }

            var dailyChunks = await DailyChunksAsync(c, ct);
            Assert.NotEmpty(dailyChunks);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);
            var (plan, rawFrom) = await PlanAsync(pg, t, ct);

            AssertIsTheCoveringIndex(await IndexAsync(c, ct), "on the seeded store");
            TestContext.Current.SendDiagnosticMessage("daily chunks with the index: " + await ChunkSizesAsync(c, ct));
            var withReply = await ReplyAsync(pg, t, ct);
            var with = await ExplainMidAsync(pg, t, rawFrom, plan, dailyChunks, ct);
            Assert.NotEmpty(withReply);
            Assert.Contains(withReply, r => string.IsNullOrEmpty(r.Database));
            Assert.NotEmpty(with.DailyChunkScans);
            Assert.All(with.DailyChunkScans, index => Assert.True(index is not null && index.Contains(IndexName, StringComparison.Ordinal),
                $"the mid arm read a daily chunk through '{index ?? "no index (a sequential scan)"}', not {IndexName}"));

            await DropIndexAsync(c, ct);
            await Exec(c, $"VACUUM (ANALYZE) {daily}", ct);
            var withoutReply = await ReplyAsync(pg, t, ct);
            var without = await ExplainMidAsync(pg, t, rawFrom, plan, dailyChunks, ct);

            TestContext.Current.SendDiagnosticMessage(
                $"mid statement pages {without.Pages} -> {with.Pages}; daily chunk scans without the index: {string.Join(", ", without.DailyChunkScans.Select(s => s ?? "seq scan"))}");
            Assert.Equal(withoutReply, withReply);
            Assert.DoesNotContain(without.DailyChunkScans, index => index is not null && index.Contains(IndexName, StringComparison.Ordinal));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
