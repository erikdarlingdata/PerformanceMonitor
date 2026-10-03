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

        /* pg_stat_statements.max is a server setting, readable from any database once the library is preloaded. */
        var maxEntries = await ReadMaxEntriesAsync(baseConnectionString!, ct);
        Assert.SkipWhen(maxEntries > MaxEntriesTheEvictionCanFill,
            $"pg_stat_statements.max is {maxEntries}, more than the {MaxEntriesTheEvictionCanFill} this test fills to force an eviction pass.");

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

            /* #4981: the counter belongs to the SERVER, not to this test. pg_stat_statements_info.dealloc counts the
               eviction passes of every session on the cluster, and a full pg_stat_statements_reset() from another
               live class sets it back to zero and stamps a new stats_reset. Reading it directly once and asserting the
               collector's read equals it failed whenever another session moved it between the two reads. The
               collector's read must lie between a direct read taken just before it and one taken just after, which
               holds however much the other sessions add (the statements the test itself runs between the reads add
               to both ends alike). A reset between the two direct reads (a changed stats_reset) voids that attempt,
               and the read is tried again.

               The bracket only proves something once the counter is above zero: on a cluster that has never evicted
               it is [0, 0], and a collector that reported 0 for everything would pass. So every attempt first makes
               sure this cluster has evicted at least once (more distinct statements than pg_stat_statements.max),
               which also puts the stale-count case, a read below the direct one, out of the bracket. */
            (int Count, long? Dealloc) read = (0, null);
            (long Dealloc, string Epoch) before = (0, string.Empty);
            (long Dealloc, string Epoch) after = (0, string.Empty);
            for (var attempt = 0; attempt < 5; attempt++)
            {
                await EnsureEvictionsAsync(connection, maxEntries, ct);
                before = await DeallocAsync(connection, ct);
                read = await ReadOrdinal30Async(connection, Sql(major, aurora: false), ct);
                after = await DeallocAsync(connection, ct);
                if (before.Epoch == after.Epoch && before.Dealloc > 0)
                {
                    break;
                }
            }

            /* Five resets in a row is not noise. */
            Assert.Equal(before.Epoch, after.Epoch);
            Assert.True(before.Dealloc > 0, "the cluster has evicted nothing, so the bracket could not tell a collector that reports 0");
            Assert.True(read.Count > 0);
            Assert.NotNull(read.Dealloc);
            Assert.InRange(read.Dealloc!.Value, before.Dealloc, after.Dealloc);

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

    /// <summary>
    /// <c>collect.pg_server_config</c> holds per-database and per-role override rows beside the server-wide ones. Both
    /// reads of <c>pg_stat_statements.max</c> (the top-queries disclosure and the eviction finding) must answer with the
    /// server-wide 5000 even when a role override of 20000 is the NEWER row.
    /// </summary>
    [Fact]
    public async Task TheMaxEntriesReads_IgnoreARoleOverride_EvenWhenItIsNewer()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live #4677 override pin.");

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
            await LogAsync(connection, ct, serverId, now.AddMinutes(-20), "statements_dealloc=3");
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.pg_server_config (collection_id, collection_time, server_id, server_name, name, setting) VALUES ($1, $2, $3, $4, 'pg_stat_statements.max', '5000')",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-30)), serverId, ServerName);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.pg_server_config (collection_id, collection_time, server_id, server_name, name, setting, role_name) VALUES ($1, $2, $3, $4, 'pg_stat_statements.max', '20000', 'x')",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-5)), serverId, ServerName);

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var evictions = JsonDocument.Parse(await DarlingMcpPgStatementTools.GetPgTopQueries(dataSource, ServerName, 4)).RootElement.GetProperty("evictions");
            Assert.Equal(5000, evictions.GetProperty("max_entries").GetInt64());

            /* The finding's read. Its snapshot anchor is the newest snapshot at or before the window end, so the two
               rows sit in ONE snapshot here: the same instant, the override listed second. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE collect.pg_server_config SET collection_time = $1 WHERE server_id = $2", DarlingMcpTestData.Naive(now.AddMinutes(-5)), serverId);
            await using var cmd = new NpgsqlCommand(PerformanceMonitor.Darling.Analysis.PgTargetFactCollector.StatementsMaxEntriesSql, connection);
            cmd.Parameters.AddWithValue(serverId);
            cmd.Parameters.AddWithValue(DarlingMcpTestData.Naive(now));
            cmd.Parameters.AddWithValue(DarlingMcpTestData.Naive(now.AddDays(-1)));
            Assert.Equal("5000", await cmd.ExecuteScalarAsync(ct) as string);
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

    /// <summary>The most entries a cluster may hold for this test to fill it: each is a statement of its own to run.</summary>
    private const int MaxEntriesTheEvictionCanFill = 20000;

    private static async Task<int> ReadMaxEntriesAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand("SELECT current_setting('pg_stat_statements.max')::int", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Makes sure pg_stat_statements has evicted on this cluster, so <c>dealloc</c> is above zero: runs more distinct
    /// statements than the extension can hold, a batch at a time, until the counter moves. A statement's identity is
    /// the shape of its parse tree, and neither a constant nor an alias is part of it, so the statements differ in
    /// shape: how many select items, how long an addition each one carries, and which of WHERE, LIMIT and OFFSET
    /// follow. Does nothing when the counter is already above zero.
    /// </summary>
    private static async Task EnsureEvictionsAsync(NpgsqlConnection connection, int maxEntries, CancellationToken ct)
    {
        if ((await DeallocAsync(connection, ct)).Dealloc > 0)
        {
            return;
        }

        const int variants = 8;
        var side = (int)Math.Ceiling(Math.Sqrt((maxEntries + 1) / (double)variants)) + 1;
        var batch = new System.Text.StringBuilder();
        var inBatch = 0;

        for (var variant = 0; variant < variants; variant++)
        {
            var suffix = ((variant & 1) != 0 ? " WHERE true" : string.Empty)
                + ((variant & 2) != 0 ? " LIMIT 1" : string.Empty)
                + ((variant & 4) != 0 ? " OFFSET 0" : string.Empty);

            for (var items = 1; items <= side; items++)
            {
                for (var terms = 0; terms <= side; terms++)
                {
                    var item = "1" + string.Concat(System.Linq.Enumerable.Repeat("+1", terms));
                    batch.Append("SELECT ").Append(string.Join(",", System.Linq.Enumerable.Repeat(item, items))).Append(suffix).Append(';');

                    if (++inBatch < 400)
                    {
                        continue;
                    }

                    await DarlingMcpTestData.ExecAsync(connection, ct, batch.ToString());
                    batch.Clear();
                    inBatch = 0;
                    if ((await DeallocAsync(connection, ct)).Dealloc > 0)
                    {
                        return;
                    }
                }
            }
        }

        if (inBatch > 0)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, batch.ToString());
        }
    }

    /// <summary>
    /// The extension's own eviction counter and the stamp of the last reset, read together in one statement, so a caller
    /// can tell a reset (a new stamp) from an increase.
    /// </summary>
    private static async Task<(long Dealloc, string Epoch)> DeallocAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT dealloc, stats_reset::text FROM public.pg_stat_statements_info", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.IsDBNull(1) ? string.Empty : reader.GetString(1));
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
