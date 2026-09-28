/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #4677 against a real PostgreSQL: the collector's statement carries <c>pg_stat_statements_info.dealloc</c> at
/// ordinal 30 (NULL, not an error, where the extension was never created), and <c>get_pg_top_queries</c> discloses
/// the eviction passes the store recorded. Needs a server with <c>pg_stat_statements</c> in
/// <c>shared_preload_libraries</c>.
/// </summary>
public sealed class PgStatementsDeallocLiveTests
{
    private const string ServerName = "pg-evictions-e2e";
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheCollectorRead_CarriesDeallocAtOrdinal30_OrNullWithoutTheExtension()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live #4677 collector read.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);

            /* (b) No extension in this database: the read must still run and ordinal 30 is NULL. The base view
               is absent too, so the gate is exercised alone, as the epoch's live proof does. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "DROP EXTENSION IF EXISTS pg_stat_statements");
            await using (var alone = new NpgsqlCommand("SELECT " + PgStatementStatsCollector.StatementsDeallocSql, connection))
            {
                Assert.True(await alone.ExecuteScalarAsync(ct) is null or DBNull);
            }

            /* (a) With it: the whole collector statement, both flavors' text, non-null at ordinal 30. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "CREATE EXTENSION pg_stat_statements");
            await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT 1");
            var major = connection.PostgreSqlVersion.Major;
            var direct = await ScalarAsync(connection, "SELECT dealloc FROM public.pg_stat_statements_info", ct);
            var read = await ReadOrdinal30Async(connection, Sql(major, aurora: false), ct);
            Assert.True(read.Count > 0);
            Assert.Equal(direct, read.Dealloc);

            /* The Aurora flavor reads aurora_stat_statements(), which stock PostgreSQL does not have, so it cannot
               run here; its text must carry the same twin as its last select item. */
            var aurora = Sql(major, aurora: true);
            Assert.Contains(PgStatementStatsCollector.StatementsDeallocSql + "                     AS stats_dealloc", aurora, StringComparison.Ordinal);
            Assert.Contains("aurora_stat_statements(false)", aurora, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task TopQueries_DisclosesTheEvictionPasses_OrUnknownWhenNothingRecordedThem()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live #4677 disclosure.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            var serverId = ServerIdHelper.GetDeterministicHashCode(ServerName);
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, ServerName, ct);
            var now = DateTime.UtcNow;
            await SeedStatsAsync(connection, ct, serverId, now.AddMinutes(-10));

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* No run recorded the counter: unknown, never zero. */
            var unknown = JsonDocument.Parse(await DarlingMcpPgStatementTools.GetPgTopQueries(dataSource, ServerName, 4)).RootElement.GetProperty("evictions");
            Assert.False(unknown.GetProperty("known").GetBoolean());
            Assert.Equal(JsonValueKind.Null, unknown.GetProperty("eviction_passes_in_window").ValueKind);
            Assert.Contains("unknown", unknown.GetProperty("note").GetString(), StringComparison.Ordinal);

            /* Two recorded runs (one beside a host note), one outside the window, plus the server-wide max. */
            await LogAsync(connection, ct, serverId, now.AddMinutes(-30), "statements_dealloc=2");
            await LogAsync(connection, ct, serverId, now.AddMinutes(-20), "abandoned by budget; statements_dealloc=3 statements_epoch_changes=1");
            await LogAsync(connection, ct, serverId, now.AddHours(-40), "statements_dealloc=100");
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.pg_server_config (collection_id, collection_time, server_id, server_name, name, setting) VALUES ($1, $2, $3, $4, 'pg_stat_statements.max', '5000')",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-5)), serverId, ServerName);

            var known = JsonDocument.Parse(await DarlingMcpPgStatementTools.GetPgTopQueries(dataSource, ServerName, 4)).RootElement.GetProperty("evictions");
            Assert.True(known.GetProperty("known").GetBoolean());
            Assert.Equal(5, known.GetProperty("eviction_passes_in_window").GetInt64());
            Assert.Equal(5000, known.GetProperty("max_entries").GetInt64());
            var note = known.GetProperty("note").GetString();
            Assert.Contains("evicted entries 5 time(s)", note, StringComparison.Ordinal);
            Assert.Contains("currently 5000", note, StringComparison.Ordinal);

            /* Known and zero: a count of 0, no note. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", serverId);
            await LogAsync(connection, ct, serverId, now.AddMinutes(-20), "statements_dealloc=0");
            var zero = JsonDocument.Parse(await DarlingMcpPgStatementTools.GetPgTopQueries(dataSource, ServerName, 4)).RootElement.GetProperty("evictions");
            Assert.True(zero.GetProperty("known").GetBoolean());
            Assert.Equal(0, zero.GetProperty("eviction_passes_in_window").GetInt64());
            Assert.Equal(JsonValueKind.Null, zero.GetProperty("note").ValueKind);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static async Task LogAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, DateTime at, string note) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, error_message) VALUES ($1, $2, $3, 'pg_statement_stats', $4, 'SUCCESS', $5)",
            CollectionIdGenerator.Next(), serverId, ServerName, DarlingMcpTestData.Naive(at), note);

    private static async Task SeedStatsAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, DateTime at) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned,
     shared_blks_hit, shared_blks_read, temp_blks_read, temp_blks_written, wal_bytes,
     delta_calls, delta_total_exec_time_ms, delta_rows)
VALUES ($1, $2, $3, $4, 42, 16384, 10, TRUE, 100, 5000, 91.5, 250, 10, 5, 0, 0, 0, 10, 500, 20)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, ServerName);

    private static string Sql(int major, bool aurora)
        => PgStatementStatsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 4677,
            ServerName = "live-target",
            CollectionTime = DateTime.UtcNow,
            Deltas = new NoOpDeltas(),
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                IsAurora = aurora,
                PostgresMajorVersion = major,
                PostgresVersionNum = major * 10000,
            },
        }).Text;

    private static async Task<(int Count, long? Dealloc)> ReadOrdinal30Async(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var count = 0;
        long? dealloc = null;
        while (await reader.ReadAsync(ct))
        {
            if (count == 0)
            {
                dealloc = reader.IsDBNull(30) ? null : reader.GetInt64(30);
            }

            count++;
        }

        return (count, dealloc);
    }

    private static async Task<long?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture);
    }

    private sealed class NoOpDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue, DateTime? collectionTime = null, int maxGapSeconds = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue, out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return 0;
        }
    }
}
