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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so none is [Collection("live-postgres")]. */

/// <summary>
/// #8967 part 2: the attention watermark while the swapped-out table exists, and the start-path pass when the swap cannot run.
/// </summary>
public sealed class AttentionLegacyWatermarkLiveTests
{
    private const int ServerId = -8967;

    private const int OtherServerId = -8968;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string HypertableCountSql =
        "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name = 'attention_events'";

    private static readonly DateTime Base = DateTime.UtcNow.Date.AddDays(-2).AddHours(3);

    private sealed record Store(NpgsqlConnection Connection, string ConnectionString);

    private static async Task RunAsync(Func<Store, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the attention legacy watermark live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescale = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.SkipUnless(timescale, "TimescaleDB is not available on this PostgreSQL.");
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

        var bodySucceeded = false;
        try
        {
            await body(new Store(connection, scratch.ConnectionString), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>One row, written the way a sample row is: its event_time is the watermark column and collection_time the run's.</summary>
    private static async Task InsertAsync(
        NpgsqlConnection connection, string table, long id, int serverId, DateTime eventTime, CancellationToken ct, string? database = null)
    {
        await using var command = new NpgsqlCommand(
            $"INSERT INTO collect.{table} (attention_event_id, collection_time, server_id, server_name, event_kind, event_time, attention_count, database_name) "
            + "VALUES ($1, $2, $3, 'watermark-test', 'sample', $2, 0, $4)", connection);
        command.Parameters.AddWithValue(id);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database is null ? DBNull.Value : database);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Gives the store a populated plain attention table, then swaps it (the start-path step) so the legacy table holds the old rows.</summary>
    private static async Task SwapWithLegacyRowsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await InsertAsync(c, "attention_events", 1, ServerId, Base, ct);
        await InsertAsync(c, "attention_events", 2, ServerId, Base.AddHours(-5), ct);
        await InsertAsync(c, "attention_events", 3, OtherServerId, Base.AddHours(9), ct);
        await TimescaleSupport.SwapAttentionEventsAsync(c, null, ct);
        Assert.Equal(3L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy", ct));
        Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
    }

    [Fact]
    public async Task TheWatermark_ReadsTheGreaterOfTheNewAndTheLegacyTable_ForTheServer()
    {
        await RunAsync(async (store, ct) =>
        {
            var c = store.Connection;
            await SwapWithLegacyRowsAsync(c, ct);
            await using var dataSource = NpgsqlDataSource.Create(store.ConnectionString);
            var runner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator());

            /* New table empty: the legacy table's newest row for this server, not the other server's later one. */
            Assert.Equal(Base, await runner.GetLastCollectedTimeAsync(ServerId, "attention_events", "event_time", ct));

            /* The new table holds only older rows: the legacy table still wins. */
            await InsertAsync(c, "attention_events", 10, ServerId, Base.AddHours(-1), ct);
            Assert.Equal(Base, await runner.GetLastCollectedTimeAsync(ServerId, "attention_events", "event_time", ct));

            /* The new table holds a newer row: it wins. */
            await InsertAsync(c, "attention_events", 11, ServerId, Base.AddHours(2), ct);
            Assert.Equal(Base.AddHours(2), await runner.GetLastCollectedTimeAsync(ServerId, "attention_events", "event_time", ct));

            /* A server with rows in neither table reads as a first run. */
            Assert.Null(await runner.GetLastCollectedTimeAsync(ServerId - 100, "attention_events", "event_time", ct));
        });
    }

    [Fact]
    public async Task TheWatermark_AfterTheLegacyTableIsGone_ReadsTheNewTableAlone()
    {
        await RunAsync(async (store, ct) =>
        {
            var c = store.Connection;
            await SwapWithLegacyRowsAsync(c, ct);
            await using var dataSource = NpgsqlDataSource.Create(store.ConnectionString);
            var runner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator());

            await InsertAsync(c, "attention_events", 10, ServerId, Base.AddHours(-1), ct);
            await ExecAsync(c, "DROP TABLE collect.attention_events_legacy", ct);

            Assert.Equal(Base.AddHours(-1), await runner.GetLastCollectedTimeAsync(ServerId, "attention_events", "event_time", ct));

            await ExecAsync(c, "DELETE FROM collect.attention_events", ct);
            Assert.Null(await runner.GetLastCollectedTimeAsync(ServerId, "attention_events", "event_time", ct));
        });
    }

    [Fact]
    public async Task AFailedStartPathSwap_SkipsThePopulatedPlainTable_AndTheNextPassSwapsAndConvertsIt()
    {
        await RunAsync(async (store, ct) =>
        {
            var c = store.Connection;
            await InsertAsync(c, "attention_events", 1, ServerId, Base, ct);
            await InsertAsync(c, "attention_events", 2, ServerId, Base.AddHours(-5), ct);

            /* A reader holds the table when the service starts: its ACCESS SHARE lock makes the swap's rename time out. */
            var log = new CapturingTestLogger();
            await using (var reader = new NpgsqlConnection(store.ConnectionString))
            {
                await reader.OpenAsync(ct);
                await using var held = await reader.BeginTransactionAsync(ct);
                await ExecAsync(reader, "LOCK TABLE collect.attention_events IN ACCESS SHARE MODE", ct);

                await TimescaleSupport.ConvertToHypertablesAsync(c, log, 1, ct);

                /* The populated table was not converted (the conversion would have copied it under an exclusive lock), and it was not swapped. The two
                   warnings are the swap's and the conversion's skip. The limit is lowered to one byte so a two-row table counts as big. */
                Assert.Equal(0L, await ScalarAsync(c, HypertableCountSql, ct));
                Assert.Equal(2L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
                Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('collect.attention_events_legacy')", ct));
                Assert.Equal(2, log.CountAtLevel(LogLevel.Warning));
                await held.RollbackAsync(ct);
            }

            /* The lock is released: the next pass swaps the table and converts the empty one. */
            var next = new CapturingTestLogger();
            await TimescaleSupport.ConvertToHypertablesAsync(c, next, ct);

            Assert.Equal(1L, await ScalarAsync(c, HypertableCountSql, ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
            Assert.Equal(2L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy", ct));
            Assert.Equal(0, next.CountAtLevel(LogLevel.Warning));
        });
    }

    [Fact]
    public async Task ThePerDatabaseWatermark_ReadsTheGreaterOfTheNewAndTheLegacyTable_ForTheServerAndDatabase()
    {
        await RunAsync(async (store, ct) =>
        {
            var c = store.Connection;
            await InsertAsync(c, "attention_events", 1, ServerId, Base, ct, "db_a");
            await InsertAsync(c, "attention_events", 2, ServerId, Base.AddHours(-5), ct, "db_a");
            await InsertAsync(c, "attention_events", 3, ServerId, Base.AddHours(9), ct, "db_b");
            await InsertAsync(c, "attention_events", 4, OtherServerId, Base.AddHours(11), ct, "db_a");
            await TimescaleSupport.SwapAttentionEventsAsync(c, null, ct);
            Assert.Equal(4L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy", ct));
            Assert.Equal(0L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));

            await using var dataSource = NpgsqlDataSource.Create(store.ConnectionString);
            var runner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator());

            /* New table empty: legacy's newest row for this server and database, not another database's or another server's later row. */
            Assert.Equal(Base, await runner.GetLastCollectedTimeForDatabaseAsync(ServerId, "attention_events", "event_time", "database_name", "db_a", ct));
            Assert.Equal(Base.AddHours(9), await runner.GetLastCollectedTimeForDatabaseAsync(ServerId, "attention_events", "event_time", "database_name", "db_b", ct));

            /* A database with no row in either table reads as a first run. */
            Assert.Null(await runner.GetLastCollectedTimeForDatabaseAsync(ServerId, "attention_events", "event_time", "database_name", "db_c", ct));

            /* The new table holds only an older row for the database: legacy still wins. A newer row: the new table wins. */
            await InsertAsync(c, "attention_events", 10, ServerId, Base.AddHours(-1), ct, "db_a");
            Assert.Equal(Base, await runner.GetLastCollectedTimeForDatabaseAsync(ServerId, "attention_events", "event_time", "database_name", "db_a", ct));
            await InsertAsync(c, "attention_events", 11, ServerId, Base.AddHours(2), ct, "db_a");
            Assert.Equal(Base.AddHours(2), await runner.GetLastCollectedTimeForDatabaseAsync(ServerId, "attention_events", "event_time", "database_name", "db_a", ct));

            /* Once the legacy table is gone the new table is read alone. */
            await ExecAsync(c, "DROP TABLE collect.attention_events_legacy", ct);
            Assert.Equal(Base.AddHours(2), await runner.GetLastCollectedTimeForDatabaseAsync(ServerId, "attention_events", "event_time", "database_name", "db_a", ct));
            Assert.Null(await runner.GetLastCollectedTimeForDatabaseAsync(ServerId, "attention_events", "event_time", "database_name", "db_b", ct));
        });
    }

    [Fact]
    public async Task WithTheLegacyTablePresent_APlainNewTableOverTheLimitIsNotConverted_AndOneUnderItIs()
    {
        await RunAsync(async (store, ct) =>
        {
            var c = store.Connection;

            /* The swap ran and the create on the new table failed or never happened: the new table is plain, the legacy table is there, and the
               collector has been filling the new one. The hourly pass's swap returns early, and only the table's own size decides. */
            await InsertAsync(c, "attention_events", 1, ServerId, Base, ct);
            await TimescaleSupport.SwapAttentionEventsAsync(c, null, ct);
            await InsertAsync(c, "attention_events", 2, ServerId, Base.AddHours(1), ct);
            await InsertAsync(c, "attention_events", 3, ServerId, Base.AddHours(2), ct);

            var over = new CapturingTestLogger();
            await TimescaleSupport.ConvertToHypertablesAsync(c, over, 1, ct);

            Assert.Equal(0L, await ScalarAsync(c, HypertableCountSql, ct));
            Assert.Equal(2L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
            Assert.Equal(1, over.CountAtLevel(LogLevel.Warning));

            var under = new CapturingTestLogger();
            await TimescaleSupport.ConvertToHypertablesAsync(c, under, ct);

            Assert.Equal(1L, await ScalarAsync(c, HypertableCountSql, ct));
            Assert.Equal(2L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
            Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy", ct));
            Assert.Equal(0, under.CountAtLevel(LogLevel.Warning));

            /* Already a hypertable: a later pass converts it again without a warning, whatever the limit. */
            var again = new CapturingTestLogger();
            await TimescaleSupport.ConvertToHypertablesAsync(c, again, 1, ct);
            Assert.Equal(1L, await ScalarAsync(c, HypertableCountSql, ct));
            Assert.Equal(0, again.CountAtLevel(LogLevel.Warning));
        });
    }

    [Fact]
    public async Task OnTheSwappedHypertable_RetentionDropsAChunkAndDeletesNoRows_TheLegacyTableIsLeftAlone_AndTheTuningPassIsClean()
    {
        await RunAsync(async (store, ct) =>
        {
            var c = store.Connection;
            await InsertAsync(c, "attention_events", 1, ServerId, DateTime.UtcNow.AddDays(-45), ct);
            await InsertAsync(c, "attention_events", 2, ServerId, DateTime.UtcNow.AddMinutes(-5), ct);
            await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);
            Assert.Equal(1L, await ScalarAsync(c, HypertableCountSql, ct));
            Assert.Equal(2L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy", ct));

            await InsertAsync(c, "attention_events", 11, ServerId, DateTime.UtcNow.AddDays(-45), ct);
            await InsertAsync(c, "attention_events", 12, ServerId, DateTime.UtcNow.AddMinutes(-1), ct);

            var tuning = new CapturingTestLogger();
            await PgTableTuning.ApplyAsync(c, tuning, ct);
            Assert.Equal(0, tuning.CountAtLevel(LogLevel.Warning));

            await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
            var summary = await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: true, new CapturingTestLogger(), ct);

            Assert.Equal(1, summary.ChunksDropped);
            Assert.Equal(0, summary.RowsDeleted);
            Assert.Equal(1L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events", ct));
            Assert.Equal(2L, await ScalarAsync(c, "SELECT count(*) FROM collect.attention_events_legacy", ct));
        });
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
