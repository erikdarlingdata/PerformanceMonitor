/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so none is [Collection("live-postgres")]. */

/// <summary>
/// #8967: the background copy of the swapped-out <c>collect.attention_events_legacy</c> rows into <c>collect.attention_events</c>, on a real
/// store. A populated plain table is swapped by the rung, the copy runs to the end, and the new table holds the same rows.
/// </summary>
public sealed class AttentionLegacyCopyLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Version == 277);

    private const int RetentionDays = 30;

    /* About 20 rows a page: the statement text is 200 bytes. One row in ten is 40 days old, past the 30-day retention. */
    private static string SeedSql(int rows) =>
        "INSERT INTO collect.attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, statement_text) "
        + "SELECT g, CASE WHEN g % 10 = 0 THEN now() AT TIME ZONE 'UTC' - interval '40 days' ELSE now() AT TIME ZONE 'UTC' - make_interval(secs => g) END, "
        + "-8967, 'copy-test', CASE WHEN g % 3 = 0 THEN 'count' ELSE 'sample' END, g, 'db' || (g % 5), repeat('x', 200) "
        + $"FROM generate_series(1, {rows}) AS g";

    /* The row hash: the count, and one md5 over every row's text in key order. */
    private static string HashOf(string table, bool insideRetention) =>
        "SELECT count(*)::text || ':' || coalesce(md5(string_agg(md5(t::text), '' ORDER BY attention_event_id)), '') FROM " + table + " AS t "
        + (insideRetention ? $"WHERE collection_time >= now() AT TIME ZONE 'UTC' - interval '{RetentionDays} days'" : "");

    private sealed record Store(NpgsqlConnection Connection, NpgsqlDataSource DataSource, string ConnectionString);

    private static async Task RunAsync(bool timescale, Func<Store, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the attention legacy copy live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        if (timescale)
        {
            Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB is not available on this PostgreSQL.");
            await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await body(new Store(connection, dataSource, scratch.ConnectionString), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /* Seeds the plain table, swaps it with the rung, and makes the empty new table a hypertable on a TimescaleDB store. */
    private static async Task SwapAsync(Store store, int rows, CancellationToken ct)
    {
        await ExecAsync(store.Connection, SeedSql(rows), ct);
        await RunRungAsync(store.Connection, ct);
        await TimescaleSupport.ConvertToHypertablesAsync(store.Connection, null, ct);
    }

    private static Task<AttentionLegacyCopy.PassOutcome> PassAsync(
        Store store, int batchPages, IBatchWalPacer? pacer, CancellationToken ct, Func<int>? retention = null) =>
        AttentionLegacyCopy.RunPassAsync(
            store.DataSource, NullLogger.Instance, retention ?? (() => RetentionDays),
            pacer is null ? null : _ => Task.FromResult<IBatchWalPacer?>(pacer), batchPages, ct);

    [Fact]
    public async Task TheCopy_RunsToTheEnd_RowsMatchByCountAndHash_AndLegacyAndTheProgressRowAreGone()
    {
        await RunAsync(timescale: true, async (store, ct) =>
        {
            var c = store.Connection;
            await SwapAsync(store, 2000, ct);
            var expected = (string)(await ScalarAsync(c, HashOf("collect.attention_events_legacy", insideRetention: true), ct))!;
            Assert.StartsWith("1800:", expected, StringComparison.Ordinal);
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
            Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name = 'attention_events'", ct));

            var pacer = new CountingPacer();
            Assert.Equal(AttentionLegacyCopy.PassOutcome.Finished, await PassAsync(store, 7, pacer, ct));

            Assert.Equal(expected, await ScalarAsync(c, HashOf("collect.attention_events", insideRetention: false), ct));
            Assert.True(pacer.Batches > 3, "the copy ran in several batches");
            Assert.Equal(pacer.Batches, pacer.Charged);
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('collect.attention_events_legacy')", ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy_copy", ct));
            Assert.Equal(AttentionLegacyCopy.PassOutcome.NothingToDo, await PassAsync(store, 7, pacer, ct));
        });
    }

    [Fact]
    public async Task AnInterruptedCopy_ResumesAtItsLastBatch_WithNoDuplicates_AndTheSameCountAndHash()
    {
        await RunAsync(timescale: true, async (store, ct) =>
        {
            var c = store.Connection;
            await SwapAsync(store, 2000, ct);
            var expected = (string)(await ScalarAsync(c, HashOf("collect.attention_events_legacy", insideRetention: true), ct))!;

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var pacer = new CountingPacer { CancelAfterBatches = 3, Source = stop };
            Assert.Equal(AttentionLegacyCopy.PassOutcome.Cancelled, await PassAsync(store, 7, pacer, stop.Token));

            Assert.Equal(21L, await ScalarAsync(c, "SELECT next_page FROM collect.attention_events_legacy_copy", ct));
            var partial = (long)(await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct))!;
            Assert.InRange(partial, 1, 1799);
            Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('collect.attention_events_legacy')", ct));

            /* A second start of the service: another pass, and the progress row says where to begin. */
            Assert.Equal(AttentionLegacyCopy.PassOutcome.Finished, await PassAsync(store, 7, new CountingPacer(), ct));
            Assert.Equal(expected, await ScalarAsync(c, HashOf("collect.attention_events", insideRetention: false), ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) - count(DISTINCT attention_event_id) FROM collect.attention_events", ct));
        });
    }

    [Fact]
    public async Task RowsOlderThanTheCutoff_AreNotCopied_AndTheCutoffIsFixedWhenTheCopyStarts()
    {
        await RunAsync(timescale: true, async (store, ct) =>
        {
            var c = store.Connection;
            await SwapAsync(store, 2000, ct);
            Assert.Equal(200L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy WHERE collection_time < now() AT TIME ZONE 'UTC' - interval '30 days'", ct));

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Assert.Equal(AttentionLegacyCopy.PassOutcome.Cancelled, await PassAsync(store, 7, new CountingPacer { CancelAfterBatches = 1, Source = stop }, stop.Token));
            var cutoff = (DateTime)(await ScalarAsync(c, "SELECT cutoff FROM collect.attention_events_legacy_copy", ct))!;
            Assert.InRange(cutoff, DateTime.UtcNow.AddDays(-RetentionDays).AddMinutes(-5), DateTime.UtcNow.AddDays(-RetentionDays).AddMinutes(1));

            /* A later pass reads a different retention. The row written at the start is the one that counts. */
            Assert.Equal(AttentionLegacyCopy.PassOutcome.Finished, await PassAsync(store, 7, null, ct, retention: () => 1000));
            Assert.Equal(1800L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events WHERE collection_time < now() AT TIME ZONE 'UTC' - interval '30 days'", ct));
        });
    }

    [Fact]
    public async Task TheCopy_DoesNothing_WithNoLegacyTable_OnATimescaleStore()
    {
        await RunAsync(timescale: true, async (store, ct) =>
        {
            /* An empty table is not swapped, so there is no legacy table. */
            await RunRungAsync(store.Connection, ct);
            Assert.Equal(AttentionLegacyCopy.PassOutcome.NothingToDo, await PassAsync(store, 7, new CountingPacer(), ct));
            Assert.Equal(0L, await ScalarAsync(store.Connection, "SELECT count(*) FROM collect.attention_events_legacy_copy", ct));
        });
    }

    [Fact]
    public async Task TheCopy_DoesNothing_OnAPlainPostgresStoreWithRows()
    {
        await RunAsync(timescale: false, async (store, ct) =>
        {
            var c = store.Connection;
            await ExecAsync(c, "DROP EXTENSION IF EXISTS timescaledb CASCADE", ct);
            await ExecAsync(c, SeedSql(300), ct);
            await RunRungAsync(c, ct);
            Assert.Equal(AttentionLegacyCopy.PassOutcome.NothingToDo, await PassAsync(store, 7, new CountingPacer(), ct));
            Assert.Equal(300L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
        });
    }

    [Fact]
    public async Task OnAPlainTable_WithALegacyTableBesideIt_TheCopyStillRunsAndDropsIt()
    {
        await RunAsync(timescale: false, async (store, ct) =>
        {
            var c = store.Connection;
            await ExecAsync(c, SeedSql(600), ct);
            await ExecAsync(c, "ALTER TABLE collect.attention_events RENAME TO attention_events_legacy; ALTER INDEX collect.idx_attention_events_time RENAME TO idx_attention_events_time_legacy; "
                + "CREATE TABLE collect.attention_events (LIKE collect.attention_events_legacy INCLUDING DEFAULTS INCLUDING STORAGE); "
                + "CREATE INDEX idx_attention_events_time ON collect.attention_events(server_id, collection_time)", ct);
            var expected = (string)(await ScalarAsync(c, HashOf("collect.attention_events_legacy", insideRetention: true), ct))!;

            Assert.Equal(AttentionLegacyCopy.PassOutcome.Finished, await PassAsync(store, 5, null, ct));

            Assert.Equal(expected, await ScalarAsync(c, HashOf("collect.attention_events", insideRetention: false), ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('collect.attention_events_legacy')", ct));
        });
    }

    [Fact]
    public async Task TheCopy_RunsAsTheWritersLogin_WhichOwnsTheTables()
    {
        await RunAsync(timescale: true, async (store, ct) =>
        {
            var c = store.Connection;
            await SwapAsync(store, 1000, ct);
            var expected = (string)(await ScalarAsync(c, HashOf("collect.attention_events_legacy", insideRetention: true), ct))!;

            var owner = (string)(await ScalarAsync(c, "SELECT current_user", ct))!;
            var writer = "copy_writer_" + Guid.NewGuid().ToString("N")[..8];
            const string password = "CopyTestPw0123456789abcdef01";
            await ExecAsync(c, $"CREATE ROLE {writer} LOGIN NOSUPERUSER PASSWORD '{password}'; GRANT USAGE ON SCHEMA collect TO {writer}; "
                + $"ALTER TABLE collect.attention_events_legacy OWNER TO {writer}; ALTER TABLE collect.attention_events OWNER TO {writer}; "
                + $"ALTER TABLE collect.attention_events_legacy_copy OWNER TO {writer}", ct);
            try
            {
                var builder = new NpgsqlConnectionStringBuilder(store.ConnectionString) { Username = writer, Password = password };
                await using var writerSource = NpgsqlDataSource.Create(builder.ConnectionString);
                var outcome = await AttentionLegacyCopy.RunPassAsync(
                    writerSource, NullLogger.Instance, () => RetentionDays, null, 9, ct);
                Assert.Equal(AttentionLegacyCopy.PassOutcome.Finished, outcome);
                Assert.Equal(expected, await ScalarAsync(c, HashOf("collect.attention_events", insideRetention: false), ct));
            }
            finally
            {
                await ExecAsync(c, $"REASSIGN OWNED BY {writer} TO {owner}; DROP OWNED BY {writer}; DROP ROLE {writer}", CancellationToken.None);
            }
        });
    }

    [Fact]
    public async Task AttentionWindowSql_ReadsTheSameRows_BeforeTheSwap_AndAfterTheCopy()
    {
        await RunAsync(timescale: true, async (store, ct) =>
        {
            var c = store.Connection;
            await ExecAsync(c,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status) "
                + "SELECT g, -8967, 'copy-test', 'attention', now() AT TIME ZONE 'UTC' - make_interval(secs => 30 + 60 * g), 5, 'SUCCESS' FROM generate_series(0, 5) AS g", ct);
            await ExecAsync(c,
                "INSERT INTO collect.attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_fingerprint, statement_text) "
                + "SELECT g, now() AT TIME ZONE 'UTC' - make_interval(secs => 28 + 25 * (g % 20)), -8967, 'copy-test', CASE WHEN g % 4 = 0 THEN 'count' ELSE 'sample' END, 1 + g % 7, "
                + "'db' || (g % 3), 'app' || (g % 2), 'fp' || (g % 5), 'SELECT ' || (g % 5) FROM generate_series(1, 80) AS g", ct);

            var adapter = new DarlingAlertReadAdapter(store.DataSource);
            var before = JsonSerializer.Serialize(await adapter.GetAttentionWindowAsync("-8967", 10, ct));
            Assert.Contains("fp1", before, StringComparison.Ordinal);

            await RunRungAsync(c, ct);
            await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);
            var during = JsonSerializer.Serialize(await adapter.GetAttentionWindowAsync("-8967", 10, ct));
            /* #9024: the window reads the legacy rows while they wait there, so the swap changes nothing the read sees. */
            Assert.Equal(before, during);

            Assert.Equal(AttentionLegacyCopy.PassOutcome.Finished, await PassAsync(store, 3, null, ct));
            Assert.Equal(before, JsonSerializer.Serialize(await adapter.GetAttentionWindowAsync("-8967", 10, ct)));
        });
    }

    [Fact]
    public async Task ADropThatHitsTheLockTimeout_LeavesTheProgressRowAndLegacy_AndTheNextPassDropsThem()
    {
        await RunAsync(timescale: true, async (store, ct) =>
        {
            var c = store.Connection;
            await SwapAsync(store, 400, ct);
            var expected = (string)(await ScalarAsync(c, HashOf("collect.attention_events_legacy", insideRetention: true), ct))!;

            await using (var holder = new NpgsqlConnection(store.ConnectionString))
            {
                await holder.OpenAsync(ct);
                await using var transaction = await holder.BeginTransactionAsync(ct);
                await ExecAsync(holder, "LOCK TABLE collect.attention_events_legacy IN ACCESS SHARE MODE", ct, transaction);

                Assert.Equal(AttentionLegacyCopy.PassOutcome.RetryLater, await PassAsync(store, 5, null, ct));

                Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy_copy WHERE next_page >= last_page", ct));
                Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('collect.attention_events_legacy')", ct));
                Assert.Equal(expected, await ScalarAsync(c, HashOf("collect.attention_events", insideRetention: false), ct));
                await transaction.RollbackAsync(ct);
            }

            Assert.Equal(AttentionLegacyCopy.PassOutcome.Finished, await PassAsync(store, 5, null, ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('collect.attention_events_legacy')", ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy_copy", ct));
            Assert.Equal(expected, await ScalarAsync(c, HashOf("collect.attention_events", insideRetention: false), ct));
        });
    }

    /// <summary>Counts the batches it is told about, and cancels a token after a chosen number.</summary>
    private sealed class CountingPacer : IBatchWalPacer
    {
        public int Batches { get; private set; }

        public int Charged { get; private set; }

        public int CancelAfterBatches { get; init; }

        public CancellationTokenSource? Source { get; init; }

        public Task<long?> ReadWalPositionAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
        {
            Batches++;
            return Task.FromResult<long?>(0);
        }

        public Task AfterBatchAsync(NpgsqlConnection connection, long? before, CancellationToken cancellationToken)
        {
            Charged++;
            if (CancelAfterBatches > 0 && Charged >= CancelAfterBatches)
            {
                Source!.Cancel();
            }

            return Task.CompletedTask;
        }
    }

    /* The ladder runs a rung's text in a transaction; SET LOCAL needs one. */
    private static async Task RunRungAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var command = new NpgsqlCommand(Rung.Sql, connection, transaction);
        await command.ExecuteNonQueryAsync(ct);
        await transaction.CommitAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, NpgsqlTransaction? transaction = null)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
