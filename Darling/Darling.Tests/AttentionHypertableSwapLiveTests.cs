/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so none is [Collection("live-postgres")]. */

/// <summary>
/// #8967: <c>collect.attention_events</c> becomes a hypertable by swap. A populated plain table is renamed to <c>_legacy</c> beside an
/// empty one (V277, and the start-path step for a store that gains TimescaleDB later), the start-path conversion turns the empty table
/// into a hypertable, and every other store shape does nothing.
/// </summary>
public sealed class AttentionHypertableSwapLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Version == 277);

    private const string SeedSql =
        "INSERT INTO collect.attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count) "
        + "SELECT g, now() AT TIME ZONE 'UTC' - make_interval(mins => g), -8967, 'swap-test', 'count', 1 FROM generate_series(1, 5) AS g";

    private sealed record Store(NpgsqlConnection Connection, string ConnectionString, bool Timescale);

    private static async Task RunAsync(bool enableTimescale, Func<Store, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the attention swap live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescale = false;
        if (enableTimescale)
        {
            timescale = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
            Assert.SkipUnless(timescale, "TimescaleDB is not available on this PostgreSQL.");
            await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        }

        var bodySucceeded = false;
        try
        {
            await body(new Store(connection, scratch.ConnectionString, timescale), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task V277_OnAPopulatedPlainTable_RenamesItToLegacy_AndTheStartPathConvertsTheEmptyTable()
    {
        await RunAsync(enableTimescale: true, async (store, ct) =>
        {
            var c = store.Connection;
            await ExecAsync(c, SeedSql, ct);
            Assert.Equal(0L, await ScalarAsync(c, HypertableCountSql, ct));

            await RunRungAsync(c, ct);

            Assert.Equal(5L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy", ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
            Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'attention_events' AND indexname = 'idx_attention_events_time'", ct));
            Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'attention_events_legacy' AND indexname = 'idx_attention_events_time_legacy'", ct));
            Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('collect.attention_events_legacy_copy')", ct));

            await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);
            Assert.Equal(1L, await ScalarAsync(c, HypertableCountSql, ct));

            /* A row written after the swap lands in the hypertable, and the legacy table is untouched. */
            await ExecAsync(c, SeedSql.Replace("generate_series(1, 5)", "generate_series(101, 102)", StringComparison.Ordinal), ct);
            Assert.Equal(2L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
            Assert.Equal(5L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy", ct));
        });
    }

    [Fact]
    public async Task TheStartPathStep_SwapsAStoreThatGainsTimescaleLater_WithoutMigratingTheRows()
    {
        await RunAsync(enableTimescale: true, async (store, ct) =>
        {
            var c = store.Connection;
            await ExecAsync(c, SeedSql, ct);

            /* No rung re-run: this is the store that enabled TimescaleDB after V277 ran on an empty or plain table. */
            await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);

            Assert.Equal(5L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy", ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
            Assert.Equal(1L, await ScalarAsync(c, HypertableCountSql, ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'attention_events_legacy'", ct));
        });
    }

    [Fact]
    public async Task TheStartPathSwap_AfterV293_PutsTheCounterRowIndexOnTheNewTable_AndTheHypertableKeepsIt()
    {
        await RunAsync(enableTimescale: true, async (store, ct) =>
        {
            var c = store.Connection;
            var name = PgMigrations.AttentionCountIndexName;
            var onTable = "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND indexname = '" + name + "' AND tablename = ";

            /* The ladder has run (V293 built the index on the empty table) and the table then filled: the swap did not happen at V277, so the hourly
               pass does it. The swap puts a new table in place with the time index only, and the counter-row index stays on _legacy. */
            Assert.Equal(1L, await ScalarAsync(c, onTable + "'attention_events'", ct));
            await ExecAsync(c, SeedSql, ct);

            await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);

            Assert.Equal(5L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy", ct));
            Assert.Equal(1L, await ScalarAsync(c, HypertableCountSql, ct));
            Assert.Equal(1L, await ScalarAsync(c, onTable + "'attention_events'", ct));

            /* A row written to the hypertable after the swap lands in a chunk that has the index too. */
            await ExecAsync(c, SeedSql.Replace("generate_series(1, 5)", "generate_series(101, 102)", StringComparison.Ordinal), ct);
            var chunks = await ScalarAsync(c, "SELECT count(*) FROM show_chunks('collect.attention_events')", ct);
            Assert.True(chunks is long chunkCount && chunkCount >= 1L);
            Assert.Equal(chunks, await ScalarAsync(c, "SELECT count(*) FROM pg_index i JOIN pg_class t ON t.oid = i.indrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = '_timescaledb_internal' AND i.indrelid IN (SELECT c::oid FROM show_chunks('collect.attention_events') c) AND i.indpred IS NOT NULL AND i.indnkeyatts = 2 AND i.indnatts = 3", ct));

            /* A second pass swaps nothing and leaves the one index. */
            await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);
            Assert.Equal(1L, await ScalarAsync(c, onTable + "'attention_events'", ct));
        });
    }

    [Fact]
    public async Task TheSwap_DoesNothing_ForAnEmptyTable_AnAlreadyConvertedTable_AndARerun()
    {
        await RunAsync(enableTimescale: true, async (store, ct) =>
        {
            var c = store.Connection;

            /* Empty table: no rename, and the start path converts it in place. */
            await RunRungAsync(c, ct);
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('collect.attention_events_legacy')", ct));
            await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);
            Assert.Equal(1L, await ScalarAsync(c, HypertableCountSql, ct));

            /* Already a hypertable with rows: the rung finds nothing to rename. */
            await ExecAsync(c, SeedSql, ct);
            await RunRungAsync(c, ct);
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('collect.attention_events_legacy')", ct));
            Assert.Equal(5L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
        });
    }

    [Fact]
    public async Task TheSwap_RerunAfterTheSwap_LeavesBothTablesAsTheyWere()
    {
        await RunAsync(enableTimescale: true, async (store, ct) =>
        {
            var c = store.Connection;
            await ExecAsync(c, SeedSql, ct);
            await RunRungAsync(c, ct);
            await ExecAsync(c, SeedSql.Replace("generate_series(1, 5)", "generate_series(101, 103)", StringComparison.Ordinal), ct);

            await RunRungAsync(c, ct);
            await RunRungAsync(c, ct);

            Assert.Equal(5L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy", ct));
            Assert.Equal(3L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
        });
    }

    [Fact]
    public async Task OnAPlainPostgresStore_TheTableStaysPlainAndNothingIsRenamed()
    {
        await RunAsync(enableTimescale: false, async (store, ct) =>
        {
            var c = store.Connection;

            /* The scratch database is made with the extension installed; a plain-PostgreSQL store has none. */
            await ExecAsync(c, "DROP EXTENSION IF EXISTS timescaledb CASCADE", ct);
            await ExecAsync(c, SeedSql, ct);

            await RunRungAsync(c, ct);
            await TimescaleSupport.SwapAttentionEventsAsync(c, null, ct);

            Assert.Equal(5L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('collect.attention_events_legacy')", ct));
        });
    }

    [Fact]
    public async Task AfterTheSwap_TheWriterCanCopyIntoTheNewTable_AndTheReadRolesCanReadIt()
    {
        await RunAsync(enableTimescale: true, async (store, ct) =>
        {
            var c = store.Connection;
            var reader = "swap_reader_" + Guid.NewGuid().ToString("N")[..8];
            var owner = (string)(await ScalarAsync(c, "SELECT current_user", ct))!;
            await ExecAsync(c, $"CREATE ROLE {reader} LOGIN NOSUPERUSER PASSWORD 'SwapTestPw0123456789abcdef01'; GRANT USAGE ON SCHEMA collect TO {reader}; ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA collect GRANT SELECT ON TABLES TO {reader};", ct);
            var ok = false;
            try
            {
                await ExecAsync(c, SeedSql, ct);
                await RunRungAsync(c, ct);
                await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);

                await using (var importer = await c.BeginBinaryImportAsync(
                    "COPY collect.attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count) FROM STDIN (FORMAT BINARY)", ct))
                {
                    await importer.StartRowAsync(ct);
                    await importer.WriteAsync(9001L, NpgsqlTypes.NpgsqlDbType.Bigint, ct);
                    await importer.WriteAsync(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified), NpgsqlTypes.NpgsqlDbType.Timestamp, ct);
                    await importer.WriteAsync(-8967, NpgsqlTypes.NpgsqlDbType.Integer, ct);
                    await importer.WriteAsync("swap-test", NpgsqlTypes.NpgsqlDbType.Text, ct);
                    await importer.WriteAsync("count", NpgsqlTypes.NpgsqlDbType.Text, ct);
                    await importer.WriteAsync(1L, NpgsqlTypes.NpgsqlDbType.Bigint, ct);
                    await importer.CompleteAsync(ct);
                }

                var builder = new NpgsqlConnectionStringBuilder(store.ConnectionString) { Username = reader, Password = "SwapTestPw0123456789abcdef01" };
                await using var asReader = new NpgsqlConnection(builder.ConnectionString);
                await asReader.OpenAsync(ct);
                Assert.Equal(1L, await ScalarAsync(asReader, "SELECT count(*) FROM collect.attention_events", ct));
                ok = true;
            }
            finally
            {
                await using var cleanup = new NpgsqlConnection(store.ConnectionString);
                await cleanup.OpenAsync(CancellationToken.None);
                await ExecAsync(cleanup, $"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA collect REVOKE SELECT ON TABLES FROM {reader}", CancellationToken.None);
                if (ok)
                {
                    await ExecAsync(cleanup, $"REVOKE ALL ON ALL TABLES IN SCHEMA collect FROM {reader}; REVOKE USAGE ON SCHEMA collect FROM {reader}; DROP ROLE {reader}", CancellationToken.None);
                }
            }
        });
    }

    private const string HypertableCountSql =
        "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name = 'attention_events'";

    /* The ladder runs a rung's text in a transaction; SET LOCAL needs one. */
    private static async Task RunRungAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var command = new NpgsqlCommand(Rung.Sql, connection, transaction);
        await command.ExecuteNonQueryAsync(ct);
        await transaction.CommitAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
