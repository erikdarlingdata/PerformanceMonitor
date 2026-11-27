/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9419: the store's own WAL figures. The difference between two stored rows, what counts as a reset, and the state of the pg_wal
/// figures, with no store.
/// </summary>
public sealed class StoreWalFiguresTests
{
    private static readonly DateTime T0 = new(2026, 11, 24, 6, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Reset0 = new(2026, 11, 1, 0, 0, 0, DateTimeKind.Unspecified);

    private static DarlingStoreWalReader.Sample Row(
        int hour, long bytes, long records = 0, long fpi = 0, long full = 0, long? write = 0, long? sync = 0,
        DateTime? statsReset = null, long? dirBytes = 1000, long? dirFiles = 3)
        => new(T0.AddHours(hour), records, fpi, bytes, full, write, sync, write, sync, statsReset ?? Reset0, dirBytes, dirFiles);

    [Fact]
    public void TwoRows_GiveTheDifference_AndTheMeasuredInterval()
    {
        var rows = new[]
        {
            Row(0, bytes: 1_000, records: 10, fpi: 2, full: 1, write: 5, sync: 3),
            Row(1, bytes: 5_000, records: 40, fpi: 7, full: 4, write: 9, sync: 4, dirBytes: 2000, dirFiles: 5),
        };

        var points = DarlingStoreWalReader.BuildPoints(rows, T0.AddHours(-1));

        Assert.Equal(2, points.Count);
        Assert.Equal(DarlingStoreWalReader.NoPrevious, points[0].Status);
        Assert.Null(points[0].WalBytes);
        var hour = points[1];
        Assert.Equal(DarlingStoreWalReader.Observed, hour.Status);
        Assert.Equal(3600.0, hour.IntervalSeconds);
        Assert.Equal(4_000, hour.WalBytes);
        Assert.Equal(30, hour.Records);
        Assert.Equal(5, hour.FullPageImages);
        Assert.Equal(3, hour.BuffersFull);
        Assert.Equal(4, hour.WriteCount);
        Assert.Equal(1, hour.SyncCount);
        Assert.Equal(2000, hour.DirBytes);
        Assert.Equal(5, hour.DirFiles);
    }

    [Fact]
    public void AMovedStatsReset_IsAReset_EvenWhenEveryCounterIsHigher()
    {
        var rows = new[]
        {
            Row(0, bytes: 1_000),
            Row(1, bytes: 9_000_000, records: 500, statsReset: Reset0.AddMinutes(30)),
        };

        var points = DarlingStoreWalReader.BuildPoints(rows, T0.AddMinutes(1));

        var point = Assert.Single(points);
        Assert.Equal(DarlingStoreWalReader.Reset, point.Status);
        Assert.Null(point.WalBytes);
        Assert.Null(point.IntervalSeconds);
    }

    [Fact]
    public void ACounterThatFell_IsAReset_NeverANegativeDelta()
    {
        var rows = new[] { Row(0, bytes: 9_000, records: 90), Row(1, bytes: 400, records: 5) };

        var point = Assert.Single(DarlingStoreWalReader.BuildPoints(rows, T0.AddMinutes(1)));

        Assert.Equal(DarlingStoreWalReader.Reset, point.Status);
        Assert.Null(point.Records);
    }

    [Fact]
    public void ARowFromBeforeTheWindow_IsOnlyTheBaseline()
    {
        var rows = new[] { Row(-5, bytes: 100), Row(1, bytes: 600), Row(2, bytes: 1_600) };

        var points = DarlingStoreWalReader.BuildPoints(rows, T0);

        Assert.Equal(2, points.Count);
        Assert.Equal(500, points[0].WalBytes);
        Assert.Equal(1_000, points[1].WalBytes);
        Assert.Equal(6 * 3600.0, points[0].IntervalSeconds);
    }

    [Fact]
    public void WriteAndSyncFiguresThatThisVersionLacks_StayNull_NotZero()
    {
        var rows = new[]
        {
            Row(0, bytes: 100, write: null, sync: null),
            Row(1, bytes: 300, write: null, sync: null),
        };

        var hour = DarlingStoreWalReader.BuildPoints(rows, T0)[1];

        Assert.Equal(200, hour.WalBytes);
        Assert.Null(hour.WriteCount);
        Assert.Null(hour.SyncCount);
        Assert.Null(hour.WriteMs);
        Assert.Null(hour.SyncMs);
    }

    [Fact]
    public void NoRows_AreEmpty()
    {
        var reading = DarlingStoreWalReader.From(Array.Empty<DarlingStoreWalReader.Sample>(), T0);

        Assert.Equal("empty", reading.Status);
        Assert.Empty(reading.Points);
    }

    [Fact]
    public void TheNewestRowWithoutPgWalFigures_IsAPrecondition_AndOneWithThemIsOk()
    {
        var without = DarlingStoreWalReader.From(new[] { Row(0, 100), Row(1, 200, dirBytes: null, dirFiles: null) }, T0);
        Assert.Equal("ok", without.Status);
        Assert.Equal("precondition", without.DirStatus);
        Assert.False(string.IsNullOrWhiteSpace(without.DirMessage));
        Assert.Null(without.Points[^1].DirBytes);

        var with = DarlingStoreWalReader.From(new[] { Row(0, 100, dirBytes: null, dirFiles: null), Row(1, 200) }, T0);
        Assert.Equal("ok", with.DirStatus);
        Assert.Null(with.DirMessage);
    }
}

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #9419 against a real store: two sweeps with WAL written between them give the hour's WAL bytes through <c>get_store_metrics</c>,
/// a statistics reset reads as a reset, a role that cannot read pg_wal gets the row and a precondition, and a store below the rung
/// answers unavailable.
/// </summary>
public sealed class StoreWalFiguresLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9419 tests (each mints its own scratch database).";

    private static async Task WriteWalAsync(NpgsqlConnection connection, int rows, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"CREATE TABLE IF NOT EXISTS public.wal_probe (id int, filler text); INSERT INTO public.wal_probe SELECT g, repeat('w', 200) FROM generate_series(1, {rows}) AS g", connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task SweepAtAsync(NpgsqlConnection connection, DateTime utcNow, CancellationToken ct)
    {
        var written = await StoreSelfMetrics.SweepAsync(connection, timescaleAvailable: false, utcNow, null, null, null, ct);
        Assert.True(written > 0, "the sweep wrote nothing");
    }

    private static async Task<List<(long Bytes, long Records, long? DirBytes)>> StoredAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var rows = new List<(long, long, long?)>();
        await using var command = new NpgsqlCommand(
            $"SELECT wal_bytes, wal_records, wal_dir_bytes FROM collect.store_metrics WHERE object_kind = '{StoreWalFigures.WalObjectKind}' ORDER BY metric_time", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetInt64(0), reader.GetInt64(1), reader.IsDBNull(2) ? null : reader.GetInt64(2)));
        }

        return rows;
    }

    [Fact]
    public async Task TwoSweeps_GiveTheHoursWalBytes_ThroughTheTool_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            var now = DateTime.UtcNow;
            await SweepAtAsync(connection, now.AddHours(-1), ct);
            await WriteWalAsync(connection, 20_000, ct);
            /* A backend adds its WAL counters to pg_stat_wal when it has been idle for about a second, so the sweep reads them after the pause. */
            await Task.Delay(TimeSpan.FromSeconds(2), ct);
            await SweepAtAsync(connection, now, ct);

            var stored = await StoredAsync(connection, ct);
            Assert.Equal(2, stored.Count);
            var storedDelta = stored[1].Bytes - stored[0].Bytes;
            Assert.True(storedDelta > 1_000_000, $"20,000 rows of 200 bytes should write well over 1 MB of WAL, the counters moved {storedDelta}");

            await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var reply = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(data, wal_hours_back: 24));
            var wal = reply.RootElement.GetProperty("wal");
            Assert.Equal("ok", wal.GetProperty("status").GetString());
            var points = wal.GetProperty("points").EnumerateArray().ToArray();
            Assert.Equal(2, points.Length);
            Assert.Equal("no_previous", points[0].GetProperty("status").GetString());
            Assert.Equal("observed", points[1].GetProperty("status").GetString());
            Assert.Equal(storedDelta, points[1].GetProperty("wal_bytes").GetInt64());
            Assert.Equal(stored[1].Records - stored[0].Records, points[1].GetProperty("wal_records").GetInt64());
            Assert.True(points[1].GetProperty("wal_records").GetInt64() > 20_000);
            Assert.Equal(3600.0, points[1].GetProperty("interval_seconds").GetDouble(), precision: 3);

            /* The pg_wal figures follow the test role's grant: a level when it can read pg_ls_waldir(), a precondition and nulls when it cannot. */
            await using var privilege = new NpgsqlCommand("SELECT has_function_privilege('pg_ls_waldir()', 'EXECUTE')", connection);
            var canReadDir = (bool)(await privilege.ExecuteScalarAsync(ct))!;
            if (canReadDir)
            {
                Assert.Equal("ok", wal.GetProperty("pg_wal_status").GetString());
                Assert.True(points[1].GetProperty("pg_wal_bytes").GetInt64() > 0);
                Assert.True(points[1].GetProperty("pg_wal_files").GetInt64() >= 1);
            }
            else
            {
                Assert.Equal("precondition", wal.GetProperty("pg_wal_status").GetString());
                Assert.Equal(JsonValueKind.Null, points[1].GetProperty("pg_wal_bytes").ValueKind);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStatisticsReset_BetweenTwoSweeps_ReadsAsAReset_NotANegativeHour_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            var now = DateTime.UtcNow;
            await WriteWalAsync(connection, 20_000, ct);
            await SweepAtAsync(connection, now.AddHours(-1), ct);
            await using (var reset = new NpgsqlCommand("SELECT pg_stat_reset_shared('wal')", connection))
            {
                await reset.ExecuteNonQueryAsync(ct);
            }

            await SweepAtAsync(connection, now, ct);

            await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var reply = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(data));
            var points = reply.RootElement.GetProperty("wal").GetProperty("points").EnumerateArray().ToArray();
            Assert.Equal(2, points.Length);
            Assert.Equal("reset", points[1].GetProperty("status").GetString());
            Assert.Equal(JsonValueKind.Null, points[1].GetProperty("wal_bytes").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ARoleThatCannotReadPgWal_StillGetsTheRow_AndTheToolAnswersPrecondition_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var role = "walnomon_" + Guid.NewGuid().ToString("N")[..8];
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await using (var setup = new NpgsqlCommand(
                $"CREATE ROLE {role} NOLOGIN; GRANT USAGE ON SCHEMA collect TO {role}; GRANT INSERT, SELECT ON collect.store_metrics TO {role}", connection))
            {
                await setup.ExecuteNonQueryAsync(ct);
            }

            var now = DateTime.UtcNow;
            await using (var become = new NpgsqlCommand($"SET ROLE {role}", connection))
            {
                await become.ExecuteNonQueryAsync(ct);
            }

            int written;
            try
            {
                written = await StoreWalFigures.CaptureAsync(connection, now, null, ct);
            }
            finally
            {
                await using var back = new NpgsqlCommand("RESET ROLE", connection);
                await back.ExecuteNonQueryAsync(CancellationToken.None);
            }

            Assert.Equal(1, written);
            var stored = await StoredAsync(connection, ct);
            var row = Assert.Single(stored);
            Assert.Null(row.DirBytes);

            await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var reply = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(data));
            var wal = reply.RootElement.GetProperty("wal");
            Assert.Equal("ok", wal.GetProperty("status").GetString());
            Assert.Equal("precondition", wal.GetProperty("pg_wal_status").GetString());
            var point = wal.GetProperty("points").EnumerateArray().Single();
            Assert.Equal(JsonValueKind.Null, point.GetProperty("pg_wal_bytes").ValueKind);
            Assert.Equal(JsonValueKind.Null, point.GetProperty("pg_wal_files").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role}", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    [Fact]
    public async Task AStoreBelowTheRung_AnswersUnavailable_AndTheSweepStillWrites_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, StoreWalFigures.Version - 1, ct);

            await SweepAtAsync(connection, DateTime.UtcNow, ct);

            await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var reply = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(data));
            var wal = reply.RootElement.GetProperty("wal");
            Assert.Equal("unavailable", wal.GetProperty("status").GetString());
            Assert.Empty(wal.GetProperty("points").EnumerateArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
