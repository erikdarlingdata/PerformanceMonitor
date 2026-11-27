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
/// #9223: the raw tail of the Query Store fleet statement had no index with <c>query_hash</c> in it, so one hash over the newest raw chunk was a scan of
/// the whole chunk. The start-path tuning pass now builds <c>idx_query_store_stats_server_hash_time</c>, and the tail runs once per server through it.
/// A raw tier of many servers and many hashes, with the hash on a few rows of each server, must read the new index with the server and the hash both
/// in the index condition (the plan the one-filter shape cannot take on a PostgreSQL without a skip scan), and the tool's reply must be the same rows with
/// the index, without it, and from the one-filter statement.
/// </summary>
public sealed class QueryFleetSummaryQueryStoreTailIndexLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9223 raw tail index tests.";
    private const string Hash = "0xQS9223T";
    private const string IndexName = PgTableTuning.QueryStoreServerHashIndexName;
    private const int ServerCount = 8;
    private const int Groups = 400;
    private const int Hours = 5;

    private sealed record Store(ScratchPostgres Scratch, NpgsqlConnection Connection, DateTime Start, DateTime End);

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A store whose start-path tuning has run on an empty raw tier (so the index builds at once), then the raw rows: every server, every hash,
    /// two collections of each hourly interval (the newer one wins the dedupe), and the target hash on server two with a legacy row that has no interval start.</summary>
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
            for (var s = 1; s <= ServerCount; s++)
            {
                await DarlingMcpTestData.RegisterServerAsync(connection, -924000 - s, "p9223t-" + s, ct);
            }

            await TuningStartPasses.ConvergeAsync(connection, ct);

            /* One anchor for the fact: the tool is asked about this end as its as_of, and it refuses an as_of after the real clock, so a test clock
               pinned ahead of the real clock (the 23:30 and 00:30 passes) must not put the window past it. */
            var end = LiveClock.AnchoredEndNotAfterNow();
            var start = end.AddHours(-Hours);
            await Exec(connection, $@"INSERT INTO collect.query_store_stats
                  (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash, query_id, plan_id, execution_type_desc, replica_role,
                   runtime_stats_interval_id, interval_start_time_utc, first_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, interval_end_time_utc)
                SELECT 800000000::bigint + row_number() OVER (), h.ts + c.off, -924000 - s, 'p9223t-' || s,
                  CASE WHEN g = 1 THEN 'db' || (s % 2) ELSE 'db' || (g % 3) END, 'dbo.P',
                  CASE WHEN g = 1 THEN '{Hash}' ELSE '0xt' || g END, g, g, 'Regular', 'PRIMARY', g,
                  CASE WHEN s = 2 AND g = 1 AND c.n = 1 THEN NULL ELSE h.ts END,
                  h.ts, c.n * 5 + s + (extract(hour FROM h.ts))::int, 2000 + g, 1000 + g, 900, 400, h.ts + interval '1 hour'
                FROM generate_series(TIMESTAMP '{start:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{end.AddHours(-1):yyyy-MM-dd HH:mm:ss}', interval '1 hour') AS h(ts)
                CROSS JOIN (VALUES (1, interval '10 minutes'), (2, interval '40 minutes')) AS c(n, off)
                CROSS JOIN generate_series(1, {ServerCount}) s CROSS JOIN generate_series(1, {Groups}) g", ct);
            await Exec(connection, "ANALYZE collect.query_store_stats", ct);
            return new Store(scratch, connection, start, end);
        }
        catch
        {
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
            throw;
        }
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'collect' AND c.relname = $1)", c);
        command.Parameters.AddWithValue(IndexName);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static void AddParameters(NpgsqlCommand command, Store store)
    {
        command.Parameters.AddWithValue(NpgsqlDbType.Text, Hash);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(store.Start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(store.End, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(store.Start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, 200);
        command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Integer, DBNull.Value);
    }

    private sealed record Row(string Server, string Database, long Executions, long CpuUs, long ElapsedUs, DateTime First, DateTime Last);

    /// <summary>The statement's own rows, in the order it returns them.</summary>
    private static async Task<Row[]> RunAsync(NpgsqlDataSource pg, Store store, string sql, CancellationToken ct)
    {
        await using var command = pg.CreateCommand(sql);
        AddParameters(command, store);
        var rows = new List<Row>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new Row(reader.GetString(0), reader.GetString(2), reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5), reader.GetDateTime(6), reader.GetDateTime(7)));
        }

        return rows.ToArray();
    }

    private sealed record Scan(string Type, string Index, string IndexCond);

    private static async Task<List<Scan>> ExplainAsync(NpgsqlDataSource pg, Store store, string sql, CancellationToken ct)
    {
        await using var command = pg.CreateCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql);
        AddParameters(command, store);
        using var doc = JsonDocument.Parse((string)(await command.ExecuteScalarAsync(ct))!);
        var scans = new List<Scan>();
        Walk(doc.RootElement[0].GetProperty("Plan"), scans);
        return scans;
    }

    private static void Walk(JsonElement node, List<Scan> scans)
    {
        if (node.TryGetProperty("Index Name", out var index))
        {
            scans.Add(new Scan(node.GetProperty("Node Type").GetString()!, index.GetString()!,
                node.TryGetProperty("Index Cond", out var cond) ? cond.GetString()! : ""));
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                Walk(child, scans);
            }
        }
    }

    private static bool ReadsTheHashIndexPerServer(List<Scan> plan) =>
        plan.Any(s => s.Index.EndsWith(IndexName, StringComparison.Ordinal)
            && s.IndexCond.Contains("server_id", StringComparison.Ordinal) && s.IndexCond.Contains("query_hash", StringComparison.Ordinal));

    /// <summary>With the index built, the tail's plan takes it with the server and the hash both in the index condition, and the rows equal the one-filter
    /// statement's. Red when the tuning pass builds no such index, and red when the tail keeps its one-filter shape (a PostgreSQL 18 skip scan, where
    /// there is one, leaves the server out of the condition).</summary>
    [Fact]
    public async Task TheTail_ReadsTheNewIndexOncePerServer_AndReturnsTheSameRowsAsTheOneFilterShape()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var c = store.Connection;
        var bodySucceeded = false;
        try
        {
            Assert.True(await IndexExistsAsync(c, ct), "the start-path tuning pass built " + IndexName);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

            var perServerSql = DarlingMcpQueryFleetSummaryTools.BuildQueryStoreSql(useRollup: false, byServerIndex: true);
            var oneFilterSql = DarlingMcpQueryFleetSummaryTools.BuildQueryStoreSql(useRollup: false, byServerIndex: false);
            Assert.True(ReadsTheHashIndexPerServer(await ExplainAsync(pg, store, perServerSql, ct)), "the per-server tail reads " + IndexName + " with the server and the hash in the index condition");

            var oneFilter = await RunAsync(pg, store, oneFilterSql, ct);
            var perServer = await RunAsync(pg, store, perServerSql, ct);
            Assert.Equal(ServerCount, oneFilter.Select(r => r.Server).Distinct().Count());
            Assert.Equal(oneFilter, perServer);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private sealed record Shaped(string? Server, string? Database, long Executions, double CpuMs, double ElapsedMs, string? First, string? Last);

    private static async Task<Shaped[]> ReplyAsync(NpgsqlDataSource pg, Store store, CancellationToken ct)
    {
        var asOf = store.End.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture) + "Z";
        var json = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(pg, Hash, Hours, as_of: asOf, limit: 200, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        Assert.False(doc.RootElement.TryGetProperty("error", out _), json);
        Assert.True(doc.RootElement.TryGetProperty("servers", out _), json.Length > 300 ? json[..300] : json);
        return doc.RootElement.GetProperty("servers").EnumerateArray()
            .Where(r => r.GetProperty("source").GetString() == DarlingMcpQueryFleetSummaryTools.QueryStoreSource)
            .Select(r => new Shaped(r.GetProperty("server_name").GetString(), r.GetProperty("database_name").GetString(), r.GetProperty("execution_count").GetInt64(),
                r.GetProperty("cpu_ms").GetDouble(), r.GetProperty("elapsed_ms").GetDouble(), r.GetProperty("first_hour").GetString(), r.GetProperty("last_hour").GetString()))
            .ToArray();
    }

    /// <summary>The tool reads the catalog for the index: with it the statement it runs takes the per-server plan, and with it dropped (a store whose build has not
    /// run yet) the statement keeps the one-filter shape, reads no such index and answers the same rows. Red when the tool never looks, because the built-index
    /// statement is then the one-filter shape.</summary>
    [Fact]
    public async Task TheTool_FallsBackToTheOneFilterShapeWithoutTheIndex_AndAnswersTheSameRows()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var c = store.Connection;
        var bodySucceeded = false;
        try
        {
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);
            var built = await DarlingMcpQueryFleetSummaryTools.BuildQueryStoreStatementAsync(pg, useRollup: false, mid: false, ct);
            Assert.True(ReadsTheHashIndexPerServer(await ExplainAsync(pg, store, built, ct)), "with the index built the tool's statement reads it per server");
            var withIndex = await ReplyAsync(pg, store, ct);
            Assert.Equal(ServerCount, withIndex.Length);

            await Exec(c, "DROP INDEX collect." + IndexName, ct);
            var fallback = await DarlingMcpQueryFleetSummaryTools.BuildQueryStoreStatementAsync(pg, useRollup: false, mid: false, ct);
            Assert.DoesNotContain(await ExplainAsync(pg, store, fallback, ct), s => s.Index.EndsWith(IndexName, StringComparison.Ordinal));
            Assert.Equal(await RunAsync(pg, store, built, ct), await RunAsync(pg, store, fallback, ct));
            var without = await ReplyAsync(pg, store, ct);
            Assert.Equal(withIndex, without);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
