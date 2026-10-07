/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the retained sql_handle→module map (the join source for object_name on OLD query_stats CAGG windows): the
/// pure SQL-shape assertions, plus — gated on DARLING_TEST_PG — the RefreshAsync behaviour against a dev Postgres.
/// The two load-bearing clauses the map's correctness rests on are ACCUMULATE-never-forget (a handle keeps its
/// attribution after its procedure_stats source ages past the 2-day read window — the whole reason the table exists,
/// since an old-window CAGG panel must attribute a handle whose raw procedure_stats is long dropped) and ADVANCE-only
/// (a stale run never regresses a fresher attribution).
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so cross-test row churn cannot race
   another class's assertions. */
[Collection("live-postgres")]
public sealed class DarlingModuleMapTests
{
    /// <summary>Distinctive fake id/name — own-scoped cleanup never touches a real monitored server's rows.</summary>
    private const int TestServerId = -616162;
    private const string TestServerName = "modmap-e2e";

    /// <summary>Distinct per inserted row (procedure_stats keys on collection_id + object identity), based high to
    /// avoid colliding with a shared dev store's real collections.</summary>
    private static long s_collectionId = 5_000_000L;

    [Fact]
    public void CreateTable_HasCompositePkAndAttributionColumns()
    {
        var sql = DarlingModuleMap.CreateTableSql;

        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.module_map", sql, StringComparison.Ordinal);
        /* (server_name, sql_handle) identity — a handle reused across servers attributes per server. */
        Assert.Contains("PRIMARY KEY (server_name, sql_handle)", sql, StringComparison.Ordinal);
        foreach (var col in new[] { "server_name", "sql_handle", "database_name", "schema_name", "object_name", "last_seen" })
        {
            Assert.Contains(col, sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Refresh_UpsertsLatestPerHandle_FromRecentProcedureStats_Accumulating()
    {
        var sql = DarlingModuleMap.RefreshSql;

        Assert.Contains("INSERT INTO collect.module_map", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.procedure_stats", sql, StringComparison.Ordinal);
        /* the latest row per (server, handle) */
        Assert.Contains("DISTINCT ON (server_name, sql_handle)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY server_name, sql_handle, collection_time DESC", sql, StringComparison.Ordinal);
        /* bounded read, comfortably inside procedure_stats' 4-day raw retention */
        Assert.Contains("collection_time >= now() - interval '2 days'", sql, StringComparison.Ordinal);
        Assert.Contains("sql_handle IS NOT NULL", sql, StringComparison.Ordinal);
        /* accumulate: upsert, and only ever advance — a stale run can't regress a fresher attribution */
        Assert.Contains("ON CONFLICT (server_name, sql_handle) DO UPDATE SET", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE module_map.last_seen IS NULL OR EXCLUDED.last_seen >= module_map.last_seen", sql, StringComparison.Ordinal);
        /* the watermark moves in the same statement: forward only, and only when the read found a row */
        Assert.Contains("INSERT INTO collect.module_map_state", sql, StringComparison.Ordinal);
        Assert.Contains("refreshed_through = GREATEST(module_map_state.refreshed_through, EXCLUDED.refreshed_through)", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING max(collection_time) IS NOT NULL", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateStateTable_IsASingleRowRuntimeTableOfNaiveTimestamps()
    {
        var sql = DarlingModuleMap.CreateStateTableSql;

        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.module_map_state", sql, StringComparison.Ordinal);
        Assert.Contains("id integer PRIMARY KEY CHECK (id = 1)", sql, StringComparison.Ordinal);
        Assert.Contains("refreshed_through timestamp,", sql, StringComparison.Ordinal);
        Assert.Contains("refreshed_at timestamp", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("timestamptz", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RefreshSince_IsTheDailyStatementOverABoundSince_WithTheSameUpsertAndWatermark()
    {
        var sql = DarlingModuleMap.RefreshSinceSql;

        Assert.Contains("WHERE collection_time >= $1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now() -", sql, StringComparison.Ordinal);
        Assert.Contains("sql_handle IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (server_name, sql_handle)", sql, StringComparison.Ordinal);
        /* the #1801 lock-order ORDER BY survives */
        Assert.Contains("ORDER BY server_name, sql_handle, collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE module_map.last_seen IS NULL OR EXCLUDED.last_seen >= module_map.last_seen", sql, StringComparison.Ordinal);
        Assert.Contains("refreshed_through = GREATEST(module_map_state.refreshed_through, EXCLUDED.refreshed_through)", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING max(collection_time) IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("(now() AT TIME ZONE 'UTC')::timestamp", sql, StringComparison.Ordinal);
        /* the daily statement is the same tail over its own head: they share every clause after the source */
        var tail = sql[sql.IndexOf("up AS (", StringComparison.Ordinal)..];
        Assert.EndsWith(tail, DarlingModuleMap.RefreshSql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EndToEnd_Refresh_AccumulatesAndNeverRegresses_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live module_map test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* module_map is RUNTIME setup, not a migration — EnsureTableAsync creates it (and is itself under test). */
        Assert.True(await DarlingModuleMap.EnsureTableAsync(connection, null, ct));
        await DeleteTestRowsAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

            /* Round 1: two procs collected recently → both attributed. */
            await InsertProcAsync(connection, utcNow.AddHours(-2), "0xMODMAP_A", "neword", ct);
            await InsertProcAsync(connection, utcNow.AddHours(-2), "0xMODMAP_B", "payment", ct);
            await DarlingModuleMap.RefreshAsync(connection, null, ct);
            Assert.Equal("neword", await ObjectNameForAsync(connection, "0xMODMAP_A", ct));
            Assert.Equal("payment", await ObjectNameForAsync(connection, "0xMODMAP_B", ct));

            /* Their procedure_stats source rows age out (raw is dropped at 4 days) and a THIRD proc arrives.
               ACCUMULATE: A and B keep their attribution (the refresh never deletes), C is added — an old-window
               query_stats CAGG panel must still attribute a handle whose raw procedure_stats is long gone. */
            await DeleteProcRowsAsync(connection, ct);
            await InsertProcAsync(connection, utcNow.AddHours(-1), "0xMODMAP_C", "delivery", ct);
            await DarlingModuleMap.RefreshAsync(connection, null, ct);
            Assert.Equal("neword", await ObjectNameForAsync(connection, "0xMODMAP_A", ct));
            Assert.Equal("payment", await ObjectNameForAsync(connection, "0xMODMAP_B", ct));
            Assert.Equal("delivery", await ObjectNameForAsync(connection, "0xMODMAP_C", ct));

            /* Advance-only, part 1: a proc dropped + recreated re-maps its handle when a NEWER row arrives. */
            await DeleteProcRowsAsync(connection, ct);
            await InsertProcAsync(connection, utcNow, "0xMODMAP_A", "neword_v2", ct);
            await DarlingModuleMap.RefreshAsync(connection, null, ct);
            Assert.Equal("neword_v2", await ObjectNameForAsync(connection, "0xMODMAP_A", ct));

            /* Advance-only, part 2: a STALE run (an OLDER procedure_stats row, still inside the 2-day window) must
               NOT regress the fresher attribution — the ON CONFLICT WHERE guard. */
            await DeleteProcRowsAsync(connection, ct);
            await InsertProcAsync(connection, utcNow.AddDays(-1), "0xMODMAP_A", "neword_stale", ct);
            await DarlingModuleMap.RefreshAsync(connection, null, ct);
            Assert.Equal("neword_v2", await ObjectNameForAsync(connection, "0xMODMAP_A", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertProcAsync(NpgsqlConnection c, DateTime t, string sqlHandle, string objectName, CancellationToken ct) =>
        await Exec(c, @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", ct,
            Interlocked.Increment(ref s_collectionId), DateTime.SpecifyKind(t, DateTimeKind.Unspecified),
            TestServerId, TestServerName, "TestDb", "dbo", objectName, sqlHandle);

    private static async Task<string?> ObjectNameForAsync(NpgsqlConnection c, string sqlHandle, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand("SELECT object_name FROM collect.module_map WHERE server_name = $1 AND sql_handle = $2", c);
        cmd.Parameters.AddWithValue(TestServerName);
        cmd.Parameters.AddWithValue(sqlHandle);
        return await cmd.ExecuteScalarAsync(ct) as string;
    }

    private static async Task DeleteProcRowsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand($"DELETE FROM collect.procedure_stats WHERE server_id = {TestServerId}", c);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(
            $"DELETE FROM collect.procedure_stats WHERE server_id = {TestServerId}; " +
            $"DELETE FROM collect.module_map WHERE server_name = '{TestServerName}'; " +
            "DELETE FROM collect.module_map_state", c);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct, params object?[] values)
    {
        using var cmd = new NpgsqlCommand(sql, connection);
        foreach (var v in values)
        {
            cmd.Parameters.AddWithValue(v ?? (object)DBNull.Value);
        }

        await cmd.ExecuteNonQueryAsync(ct);
    }
}
