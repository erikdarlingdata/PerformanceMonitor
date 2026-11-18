/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so none is [Collection("live-postgres")]. */

/// <summary>
/// #9024: the attention reads while the store 277 copy runs. The legacy table holds the old rows and the new table holds the rows written
/// since the swap plus the part of the old rows the copy has moved so far. Every read counts each row once, at any copy progress, and reads
/// the plain table when the legacy table is gone. The staging mirrors the swap's catalog steps on a plain table, so these facts need no
/// TimescaleDB.
/// </summary>
public sealed class AttentionLegacyReadLiveTests
{
    private const int ServerId = -9024;

    private const int NewServerId = -9025;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record Store(NpgsqlConnection Connection, NpgsqlDataSource DataSource);

    private static async Task RunAsync(Func<Store, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the attention legacy read live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var bodySucceeded = false;
        try
        {
            await body(new Store(connection, dataSource), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT count(*) FROM collect.{table}", connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>Count rows, one per minute: <paramref name="minutes"/> minutes from <paramref name="start"/>, each holding <paramref name="perMinute"/> attentions.</summary>
    private static async Task CountSeriesAsync(
        NpgsqlConnection connection, string table, long firstId, int serverId, DateTime start, int minutes, long perMinute, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"INSERT INTO collect.{table} (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count) "
            + "SELECT $1 + g, $2 + g * interval '1 minute', $3, 'legacy-read-test', 'count', $4 FROM generate_series(0, $5 - 1) AS g", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, firstId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, perMinute);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, minutes);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task SampleAsync(NpgsqlConnection connection, string table, long id, int serverId, DateTime time, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"INSERT INTO collect.{table} (attention_event_id, collection_time, server_id, server_name, event_kind, event_time, attention_count, database_name, client_app_name, statement_fingerprint, statement_text, interrupt_duration_us) "
            + "VALUES ($1, $2, $3, 'legacy-read-test', 'sample', $2, 0, 'dbX', 'appX', 'fp-x', 'SELECT 1', 30000000)", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, id);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The collector's successful run, one per minute.</summary>
    private static async Task RunsAsync(NpgsqlConnection connection, long firstId, int serverId, DateTime start, int minutes, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status) "
            + "SELECT $1 + g, $2, 'legacy-read-test', 'attention', $3 + g * interval '1 minute', 5, 'SUCCESS' FROM generate_series(0, $4 - 1) AS g", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, firstId);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, minutes);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The catalog steps of the store 277 swap on a plain table: the populated table becomes the legacy table, and an empty one takes its place.</summary>
    private static Task SwapAsync(NpgsqlConnection connection, CancellationToken ct) => ExecAsync(connection,
        "ALTER TABLE collect.attention_events RENAME TO attention_events_legacy;"
        + "ALTER INDEX collect.idx_attention_events_time RENAME TO idx_attention_events_time_legacy;"
        + "CREATE TABLE collect.attention_events (LIKE collect.attention_events_legacy INCLUDING DEFAULTS INCLUDING STORAGE);"
        + "CREATE INDEX idx_attention_events_time ON collect.attention_events(server_id, collection_time)", ct);

    /// <summary>The copy's effect so far: legacy rows of one server in a time range, moved into the new table with the same ids.</summary>
    private static async Task CopyAsync(NpgsqlConnection connection, int serverId, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.attention_events SELECT * FROM collect.attention_events_legacy WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static DateTime ThisMinute()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Unspecified);
    }

    [Fact]
    public async Task TheBaseline_SeesTheLegacyRows_AndCountsACopiedRowOnce_ThenReadsThePlainTableWhenTheLegacyTableIsGone()
    {
        await RunAsync(async (store, ct) =>
        {
            var c = store.Connection;
            var t0 = ThisMinute();
            var swapAt = t0.AddDays(-1);

            /* The week the old table held: 20 attentions a minute for two days, a ten minute burst of 300 a minute that only the legacy table holds,
               and an earlier burst of 200 a minute that the copy has already moved. A run every minute for three days. */
            await RunsAsync(c, 1, ServerId, t0.AddDays(-3), 3 * 1440 + 1, ct);
            await CountSeriesAsync(c, "attention_events", 1, ServerId, t0.AddDays(-3), 2 * 1440, 20, ct);
            await CountSeriesAsync(c, "attention_events", 100_000, ServerId, t0.AddHours(-60), 10, 280, ct);
            await CountSeriesAsync(c, "attention_events", 200_000, ServerId, t0.AddHours(-36), 10, 180, ct);
            await SwapAsync(c, ct);
            await CopyAsync(c, ServerId, t0.AddHours(-36), t0.AddHours(-36).AddMinutes(10), ct);

            /* Rows written since the swap. */
            await CountSeriesAsync(c, "attention_events", 1_000_000, ServerId, swapAt, 1441, 5, ct);

            var adapter = new DarlingAlertReadAdapter(store.DataSource);
            var key = ServerId.ToString(CultureInfo.InvariantCulture);

            var baseline = await adapter.GetAttentionBaselineAsync(key, t0.AddMinutes(1), ct);
            Assert.NotNull(baseline);
            /* The legacy-only burst sets the baseline. A read of the new table alone sees 200, and a read that counted the copied burst twice sees 400. */
            Assert.Equal(300d, baseline.P999ThreeMinuteAverage, 6);

            await ExecAsync(c, "DROP TABLE collect.attention_events_legacy", ct);
            var afterTheDrop = await adapter.GetAttentionBaselineAsync(key, t0.AddMinutes(1), ct);
            Assert.NotNull(afterTheDrop);
            Assert.Equal(200d, afterTheDrop.P999ThreeMinuteAverage, 6);
        });
    }

    [Fact]
    public async Task TheClientTimeoutsRead_CountsEachRowOnce_ForAServerWithLegacyRows_AndAServerWithNone()
    {
        await RunAsync(async (store, ct) =>
        {
            var c = store.Connection;
            var t0 = ThisMinute();
            var swapAt = t0.AddDays(-1);

            await RunsAsync(c, 1, ServerId, swapAt.AddHours(-1), 180, ct);
            await RunsAsync(c, 10_000, NewServerId, swapAt.AddHours(-1), 180, ct);
            await CountSeriesAsync(c, "attention_events", 1, ServerId, swapAt.AddMinutes(-60), 60, 20, ct);
            await SampleAsync(c, "attention_events", 5_001, ServerId, swapAt.AddMinutes(-20), ct);
            await SampleAsync(c, "attention_events", 5_002, ServerId, swapAt.AddMinutes(-10), ct);
            await SampleAsync(c, "attention_events", 5_003, ServerId, swapAt.AddMinutes(-5), ct);
            await SwapAsync(c, ct);

            /* The copy has moved the last fifteen minutes before the swap, so those rows are in BOTH tables. */
            await CopyAsync(c, ServerId, swapAt.AddMinutes(-15), swapAt, ct);
            Assert.Equal(15 + 2, await CountAsync(c, "attention_events", ct));

            await CountSeriesAsync(c, "attention_events", 1_000_000, ServerId, swapAt, 60, 5, ct);
            await SampleAsync(c, "attention_events", 5_004, ServerId, swapAt.AddMinutes(5), ct);
            await SampleAsync(c, "attention_events", 5_005, ServerId, swapAt.AddMinutes(10), ct);
            /* A server with no legacy row: every row it has is in the new table. */
            await CountSeriesAsync(c, "attention_events", 2_000_000, NewServerId, swapAt.AddMinutes(-5), 11, 7, ct);

            var from = swapAt.AddMinutes(-30);
            var to = swapAt.AddMinutes(30);

            /* 30 legacy minutes of 20 and 31 minutes of 5 since the swap. A new-table-only read gives 300 + 155, and a plain union gives 1055. */
            var (window, samples) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(store.DataSource, ServerId, from, to, 20, ct);
            Assert.Equal(30 * 20 + 31 * 5, window.Counts.Sum(r => r.Count));
            Assert.Equal(5, samples.Count);
            Assert.Equal(5, window.TopDatabases.Single(d => d.Name == "dbX").Samples);

            var (newWindow, _) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(store.DataSource, NewServerId, from, to, 20, ct);
            Assert.Equal(11 * 7, newWindow.Counts.Sum(r => r.Count));

            /* The legacy table is gone: the same read answers from the plain table. */
            await ExecAsync(c, "DROP TABLE collect.attention_events_legacy", ct);
            var (afterTheDrop, samplesAfterTheDrop) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(store.DataSource, ServerId, from, to, 20, ct);
            Assert.Equal(15 * 20 + 31 * 5, afterTheDrop.Counts.Sum(r => r.Count));
            Assert.Equal(4, samplesAfterTheDrop.Count);
        });
    }

    [Fact]
    public async Task TheAlertWindow_CountsEachRowOnce_WhenTheLookbackReachesTheLegacyRows()
    {
        await RunAsync(async (store, ct) =>
        {
            var c = store.Connection;
            var t0 = ThisMinute();
            var swapAt = t0.AddMinutes(-5);

            await RunsAsync(c, 1, ServerId, t0.AddMinutes(-12), 13, ct);
            await CountSeriesAsync(c, "attention_events", 1, ServerId, t0.AddMinutes(-8), 3, 10, ct);
            await SwapAsync(c, ct);
            await CopyAsync(c, ServerId, t0.AddMinutes(-7), swapAt, ct);
            await CountSeriesAsync(c, "attention_events", 1_000_000, ServerId, swapAt, 6, 1, ct);

            var adapter = new DarlingAlertReadAdapter(store.DataSource);
            var window = await adapter.GetAttentionWindowAsync(ServerId.ToString(CultureInfo.InvariantCulture), 10, ct);

            /* Three legacy minutes of 10 and six minutes of 1. Reading the new table alone gives 26, and a plain union gives 56. */
            Assert.Equal(3 * 10 + 6, window.Counts.Sum(r => r.Count));
        });
    }

    [Fact]
    public async Task ARead_WhoseLegacyTableIsDroppedAfterTheCheck_RunsOnceMoreOnThePlainTable()
    {
        await RunAsync(async (store, ct) =>
        {
            var c = store.Connection;
            var t0 = ThisMinute();
            await CountSeriesAsync(c, "attention_events", 1, ServerId, t0.AddMinutes(-30), 10, 3, ct);
            await SwapAsync(c, ct);
            await CountSeriesAsync(c, "attention_events", 1_000_000, ServerId, t0.AddMinutes(-10), 4, 3, ct);

            await using var reader = await store.DataSource.OpenConnectionAsync(ct);
            var calls = 0;
            var counted = await AttentionReadRelation.ReadAsync(reader, async events =>
            {
                /* The copy ends between the check and the read. */
                if (calls++ == 0)
                {
                    await ExecAsync(c, "DROP TABLE collect.attention_events_legacy", ct);
                }

                await using var command = new NpgsqlCommand($"SELECT COALESCE(SUM(attention_count), 0) FROM {events} WHERE server_id = $1", reader);
                command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
                return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
            }, ct);

            Assert.Equal(2, calls);
            Assert.Equal(4 * 3, counted);
        });
    }
}
