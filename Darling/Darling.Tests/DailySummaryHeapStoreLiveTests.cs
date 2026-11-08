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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7147: the 31-day Daily Summary and calendar read on a store WITHOUT TimescaleDB, where there is no rollup and the
/// raw tier reads a month of <c>query_stats</c> and <c>wait_stats</c>. Two changes: the distinct-query count reads
/// <c>collect.query_stats</c> directly, because <c>v_query_stats</c> (the payload-resolving view) keeps the planner off
/// an index-only scan, and <see cref="PgTableTuning"/> builds a covering wait_stats index when wait_stats is a plain table so the
/// read has an index to scan (the query_stats side takes the composer index every store already has). These run the statement against a real plain store and prove the rows do not move.
/// </summary>
/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres. */
[Collection("live-postgres")]
public sealed class DailySummaryHeapStoreLiveTests
{
    private const int ServerId = 7147;
    private const int OtherServerId = 7148;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task RawRange_ReadsTheSameRowsDirectlyAsThroughTheView_AndPlansAnIndexOnlyScanOnlyDirectly()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the heap-store daily summary live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var end = LiveClock.Now().Date.AddDays(1);
        var start = end.AddDays(-6);
        await SeedAsync(connection, end, ct);

        /* The query_stats side takes the composer index idx_query_stats_server_hash_time, which the service's tuning pass
           builds on every store, so this store gets it the same way. */
        var current = DailySummarySql.RangeSql;
        Assert.Contains("FROM collect.query_stats", current, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM v_query_stats", current, StringComparison.Ordinal);
        var viaView = current.Replace("FROM collect.query_stats", "FROM v_query_stats", StringComparison.Ordinal);

        /* Before the covering indexes exist: both statements read the same rows. */
        var viewRows = await ReadAsync(connection, viaView, start, end, ct);
        var directRows = await ReadAsync(connection, current, start, end, ct);
        Assert.True(viewRows.Count >= 6, "the seed should give at least a row per seeded day");
        Assert.Equal(viewRows, directRows);

        await Exec(connection, "CREATE INDEX IF NOT EXISTS idx_query_stats_server_hash_time ON collect.query_stats (server_id, query_hash, collection_time DESC)", ct);

        /* A start builds at most one large index (the one-build budget), and the seeded tables are no longer empty, so the
           restart-row and legacy-row indexes take their starts first. Repeat the start pass until the cover exists. */
        for (var pass = 0; pass < 8 && !await IndexExistsAsync(connection, PgTableTuning.WaitStatsDailyCoverIndexName, ct); pass++)
        {
            await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);
        }

        Assert.True(await IndexExistsAsync(connection, PgTableTuning.WaitStatsDailyCoverIndexName, ct), "a plain store builds the wait_stats cover");

        Assert.Equal(viewRows, await ReadAsync(connection, viaView, start, end, ct));
        Assert.Equal(viewRows, await ReadAsync(connection, current, start, end, ct));

        /* The plan: with the heap paths off the table, the direct read scans the cover without touching the heap, and the
           view read cannot (its payload joins keep the digest columns in the scan's target list). #8204: the planner prices
           each index-only scan by the share of pages the visibility map marks, and one vacuum cannot mark them while an
           older snapshot is open in the database, so vacuum until every page is marked. */
        await LiveVisibilityMap.VacuumUntilAllVisibleAsync(connection, "collect.query_stats", ct);
        await LiveVisibilityMap.VacuumUntilAllVisibleAsync(connection, "collect.wait_stats", ct);
        await Exec(connection, "SET enable_seqscan = off; SET enable_bitmapscan = off", ct);
        var directPlan = await PlanAsync(connection, current, start, end, ct);
        var viewPlan = await PlanAsync(connection, viaView, start, end, ct);
        const string QueryStatsIndex = "Index Only Scan using idx_query_stats_server_hash_time";
        Assert.True(directPlan.Contains(QueryStatsIndex, StringComparison.Ordinal), directPlan);
        Assert.DoesNotContain(QueryStatsIndex, viewPlan, StringComparison.Ordinal);
        Assert.Contains("Index Only Scan using " + PgTableTuning.WaitStatsDailyCoverIndexName, directPlan, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheCovers_AreNotBuilt_OnAStoreThatHasTimescale()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the heap-store daily summary live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not available on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Migration alone leaves the tables plain even with the extension (#7152): the service's conversion step is what
           makes wait_stats a hypertable, so run it as the service does before the tuning pass. */
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);
        await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);

        Assert.True(await TableIsHypertableAsync(connection, "wait_stats", ct), "the scratch store converts wait_stats to a hypertable");
        Assert.False(await IndexExistsAsync(connection, PgTableTuning.WaitStatsDailyCoverIndexName, ct));
    }

    /* #7152: the extension is installed but the service ran in plain-PostgreSQL mode, so wait_stats is a plain heap. The cover
       is decided by the table, not by the extension, so it is built here. */
    [Fact]
    public async Task TheCovers_AreBuilt_OnAStoreWithTimescaleInstalledButAPlainWaitStatsTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the heap-store daily summary live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* The store was migrated without the extension (plain heaps); the extension arrives afterwards, as on the reporter's store. */
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB is not available on this cluster.");
        await using var installed = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')", connection);
        Assert.True((bool)(await installed.ExecuteScalarAsync(ct))!, "the extension is installed");
        Assert.False(await TableIsHypertableAsync(connection, "wait_stats", ct), "wait_stats is a plain table");

        await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);

        Assert.True(await IndexExistsAsync(connection, PgTableTuning.WaitStatsDailyCoverIndexName, ct));
    }

    /* #7152: the cover built while wait_stats was plain survives create_hypertable(migrate_data => true) onto the hypertable and
       its chunks. Once the table is a hypertable the pass drops it, including when a retry comes after a chunk is compressed. */
    [Fact]
    public async Task TheCover_IsDropped_WhenWaitStatsBecomesAHypertableAfterItWasBuilt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the heap-store daily summary live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB is not available on this cluster.");

        /* An old row and a recent one, so the conversion carries real rows into two chunks and the old chunk can be compressed. */
        await using (var seed = new NpgsqlCommand(
            "INSERT INTO collect.wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_wait_time_ms) "
            + "VALUES (1, now() AT TIME ZONE 'UTC' - interval '20 days', 7152, 'srv', 'W', 1), (2, now() AT TIME ZONE 'UTC', 7152, 'srv', 'W', 1)", connection))
        {
            await seed.ExecuteNonQueryAsync(ct);
        }

        await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);
        Assert.True(await CoverIndexCountAsync(connection, ct) > 0, "the cover is built while wait_stats is a plain table");

        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TableIsHypertableAsync(connection, "wait_stats", ct), "the conversion makes wait_stats a hypertable");
        Assert.True(await CoverIndexCountAsync(connection, ct) > 1, "the conversion carries the cover onto the hypertable and its chunks");

        /* The old chunk is compressed before the pass runs again, as on a retry some days later. */
        await using (var enable = new NpgsqlCommand(TimescaleSupport.EnableCompressionSql("collect.wait_stats"), connection))
        {
            await enable.ExecuteNonQueryAsync(ct);
        }

        await using (var chunks = new NpgsqlCommand(
            "SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'wait_stats' AND NOT is_compressed ORDER BY range_start LIMIT 1", connection))
        {
            var chunk = (string?)await chunks.ExecuteScalarAsync(ct);
            Assert.NotNull(chunk);
            await using var compress = new NpgsqlCommand($"SELECT compress_chunk('{chunk}'::regclass, if_not_compressed => true)", connection);
            await compress.ExecuteNonQueryAsync(ct);
        }

        await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);

        Assert.Equal(0, await CoverIndexCountAsync(connection, ct));
    }

    /* #7152: the hypertable check failing is "unknown", which neither builds nor drops. The check fails for real here: the pass
       runs as a role that cannot use the timescaledb_information schema, so the catalog read raises permission denied. The store
       is plain and already has the cover, which a drop on a guessed answer would remove. */
    [Fact]
    public async Task TheCover_IsLeftInPlace_WhenTheHypertableCheckFailsOnAPlainWaitStatsTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the heap-store daily summary live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB is not available on this cluster.");
        Assert.False(await TableIsHypertableAsync(connection, "wait_stats", ct), "wait_stats is a plain table");

        await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);
        Assert.Equal(1, await CoverIndexCountAsync(connection, ct));

        var role = "denied_7152_" + Guid.NewGuid().ToString("N")[..8];
        var bodySucceeded = false;
        try
        {
            /* The role owns wait_stats, so it is allowed to drop the cover, but it cannot read the Timescale catalog views. */
            foreach (var statement in new[]
            {
                $"CREATE ROLE {role}",
                $"GRANT USAGE ON SCHEMA collect TO {role}",
                $"ALTER TABLE collect.wait_stats OWNER TO {role}",
                "REVOKE USAGE ON SCHEMA timescaledb_information FROM PUBLIC",
                $"SET ROLE {role}",
            })
            {
                await using var setup = new NpgsqlCommand(statement, connection);
                await setup.ExecuteNonQueryAsync(ct);
            }

            var logger = new CapturingLogger();
            await PgTableTuning.ApplyAsync(connection, logger, ct);

            await using (var reset = new NpgsqlCommand("RESET ROLE", connection))
            {
                await reset.ExecuteNonQueryAsync(ct);
            }

            Assert.Contains(logger.Warnings, w => w.Contains("is a hypertable", StringComparison.Ordinal) && w.Contains("neither built nor dropped", StringComparison.Ordinal));
            Assert.Equal(1, await CoverIndexCountAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await using var cleanup = new NpgsqlCommand($"RESET ROLE; REASSIGN OWNED BY {role} TO CURRENT_USER; DROP OWNED BY {role}; DROP ROLE {role}", connection);
            try
            {
                await cleanup.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch (PostgresException) when (!bodySucceeded)
            {
                /* The body already failed; its error is the one to report. */
            }
        }
    }

    /* #7152: the cover is on the hypertable and a second session holds the table, so the drop times out on the lock. The line must
       say the index was not dropped (the cover is still there), not that it was not built. */
    [Fact]
    public async Task ABusyTable_ReportsTheCoverAsNotDropped_NotAsNotBuilt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the heap-store daily summary live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB is not available on this cluster.");

        await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TableIsHypertableAsync(connection, "wait_stats", ct), "the conversion makes wait_stats a hypertable");
        Assert.True(await CoverIndexCountAsync(connection, ct) > 0, "the cover is on the hypertable");

        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        await using var hold = await holder.BeginTransactionAsync(ct);
        try
        {
            await using (var lockTable = new NpgsqlCommand("LOCK TABLE collect.wait_stats IN ACCESS EXCLUSIVE MODE", holder, hold))
            {
                await lockTable.ExecuteNonQueryAsync(ct);
            }

            var logger = new CapturingLogger();
            await PgTableTuning.ApplyAsync(connection, logger, ct);

            Assert.Contains(logger.Infos, m => m.Contains(PgTableTuning.WaitStatsDailyCoverIndexName, StringComparison.Ordinal)
                && m.Contains("not dropped this pass", StringComparison.Ordinal));
            Assert.DoesNotContain(logger.Infos, m => m.Contains(PgTableTuning.WaitStatsDailyCoverIndexName, StringComparison.Ordinal)
                && m.Contains("not built", StringComparison.Ordinal));
        }
        finally
        {
            await hold.RollbackAsync(CancellationToken.None);
        }

        Assert.True(await CoverIndexCountAsync(connection, ct) > 0, "the cover is still on the hypertable after the busy pass");
    }

    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger
    {
        public List<string> Warnings { get; } = new();

        public List<string> Infos { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == Microsoft.Extensions.Logging.LogLevel.Warning)
            {
                Warnings.Add(formatter(state, exception));
            }
            else if (logLevel == Microsoft.Extensions.Logging.LogLevel.Information)
            {
                Infos.Add(formatter(state, exception));
            }
        }
    }

    /// <summary>The cover's indexes in <c>pg_index</c>: the one on <c>collect.wait_stats</c> and the one Timescale names after it on each chunk.</summary>
    private static async Task<long> CoverIndexCountAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT count(*) FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid WHERE ic.relname LIKE '%' || $1", connection);
        command.Parameters.AddWithValue(PgTableTuning.WaitStatsDailyCoverIndexName);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<bool> TableIsHypertableAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name = $1)", connection);
        command.Parameters.AddWithValue(table);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime end, CancellationToken ct)
    {
        /* Two servers (the filter has to select), six days at ten-minute cadence. Waits: twenty types, one stored with the
           trailing-space spelling and a share of zero deltas. Queries: fifty hashes a cycle out of two hundred, plus a
           NULL hash a day. The query rows carry digests with no dimension row, which the view's LEFT JOINs must tolerate. */
        var anchor = "'" + end.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "'";
        var sql = $"""
            INSERT INTO collect.wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_wait_time_ms)
            SELECT row_number() OVER (), g.ts, s.sid, 'srv' || s.sid,
                   CASE WHEN w.n = 1 AND g.ts < {anchor}::timestamp - interval '3 days' THEN 'WAIT_TYPE_1 ' ELSE 'WAIT_TYPE_' || w.n END,
                   CASE WHEN (w.n + extract(epoch FROM g.ts)::bigint / 600) % 4 = 0 THEN 0 ELSE ((w.n * 37 + extract(epoch FROM g.ts)::bigint / 600) % 900) + 1 END
            FROM generate_series({anchor}::timestamp - interval '6 days', {anchor}::timestamp - interval '10 minutes', interval '10 minutes') AS g(ts)
            CROSS JOIN (VALUES ({ServerId}), ({OtherServerId})) AS s(sid)
            CROSS JOIN generate_series(1, 20) AS w(n);

            INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest, query_plan_digest)
            SELECT row_number() OVER (), g.ts, s.sid, 'srv' || s.sid, 'db',
                   '0x' || md5(((q.n * 7 + extract(epoch FROM g.ts)::bigint / 600 * 13) % 200)::text),
                   decode(md5('t' || q.n), 'hex'), decode(md5('p' || q.n), 'hex')
            FROM generate_series({anchor}::timestamp - interval '6 days', {anchor}::timestamp - interval '10 minutes', interval '10 minutes') AS g(ts)
            CROSS JOIN (VALUES ({ServerId}), ({OtherServerId})) AS s(sid)
            CROSS JOIN generate_series(1, 50) AS q(n);

            INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash)
            SELECT 1000000 + d, {anchor}::timestamp - make_interval(days => d) + interval '1 hour', {ServerId}, 'srv{ServerId}', 'db', NULL
            FROM generate_series(1, 5) AS d;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static NpgsqlCommand Bind(NpgsqlConnection connection, string sql, DateTime start, DateTime end)
    {
        var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = end });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = EventWindowFloor.For(start) });
        return command;
    }

    private static async Task<List<string>> ReadAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = Bind(connection, sql, start, end);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? string.Empty;
            }

            rows.Add(string.Join("|", cells));
        }

        return rows;
    }

    private static async Task<string> PlanAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        var lines = new List<string>();
        await using var command = Bind(connection, "EXPLAIN " + sql, start, end);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            lines.Add(reader.GetString(0));
        }

        return string.Join("\n", lines);
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection connection, string name, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1)", connection);
        command.Parameters.AddWithValue(name);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
