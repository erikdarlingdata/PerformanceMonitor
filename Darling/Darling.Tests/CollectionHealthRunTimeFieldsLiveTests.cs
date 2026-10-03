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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4938: <c>get_collection_health</c> shows a collector's run time. A collector with a run time carries
/// <c>run_at</c> (24-hour <c>HH:MM</c> on the monitored server's clock) and <c>next_run_utc</c> (the next time it is
/// due, UTC). A collector without one carries null for both. The health band is read from the shipped cadence and a
/// run time does not move it. A server with no clock yet reads the run time as UTC.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class CollectionHealthRunTimeFieldsLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the run-time health fields' live pins (each mints its own scratch database).";

    private const string DailyCollector = "index_object_stats";
    private const string MinuteCollector = "wait_stats";
    private const int TwoAm = 120;

    private static async Task<NpgsqlConnection> MigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, int serverId, string name, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
            VALUES ($1, $2, $2, TRUE, 15, $3, $3)
            """, connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task LogRunAsync(NpgsqlConnection connection, int serverId, string serverName, string collector, DateTime whenUtc, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected)
            VALUES ($1, $2, $3, $4, $5, 'SUCCESS', 120, 10)
            """, connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(whenUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(collector);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantOffsetAsync(NpgsqlConnection connection, int serverId, string serverName, int offsetMinutes, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes)
            VALUES ($1, $2, $3, $4, $5)
            """, connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(offsetMinutes);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task SetFleetRunTimeAsync(NpgsqlConnection connection, string collector, int minute, CancellationToken ct) =>
        ExecAsync(connection,
            $"INSERT INTO config.config_collector_schedules (server_id, collector_name, run_at_minute) VALUES (NULL, '{collector}', {minute})", ct);

    /// <summary>The full-detail rows of <c>get_collection_health</c> by collector. Full detail because a healthy
    /// collector otherwise compacts to seven fields.</summary>
    private static async Task<Dictionary<string, JsonElement>> ReadRowsAsync(NpgsqlDataSource postgres, string server, CancellationToken ct)
    {
        var json = await DarlingMcpDataTools.GetCollectionHealth(postgres, server, full_detail: true, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("collectors", out var collectors), json.Length > 400 ? json[..400] : json);
        return collectors.EnumerateArray().ToDictionary(r => r.GetProperty("collector").GetString()!, r => r.Clone());
    }

    private static DateTime ParseUtc(JsonElement value)
    {
        var parsed = DateTime.Parse(value.GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);
        Assert.Equal(DateTimeKind.Utc, parsed.Kind);
        return parsed;
    }

    [Fact]
    public async Task ACollectorWithARunTime_ShowsRunAtAndNextRunUtc_AndACollectorWithoutOneShowsNullForBoth()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const int serverId = 480_001;
            const string server = "runtime-health-a";
            await RegisterServerAsync(connection, serverId, server, ct);

            /* Both ran a minute ago, so the daily collector's next slot is ahead of now on any hour of the day. */
            var now = DateTime.UtcNow;
            await LogRunAsync(connection, serverId, server, DailyCollector, now.AddMinutes(-1), ct);
            await LogRunAsync(connection, serverId, server, MinuteCollector, now.AddMinutes(-1), ct);
            await SetFleetRunTimeAsync(connection, DailyCollector, TwoAm, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rows = await ReadRowsAsync(postgres, server, ct);

            var daily = rows[DailyCollector];
            Assert.Equal("02:00", daily.GetProperty("run_at").GetString());
            var next = ParseUtc(daily.GetProperty("next_run_utc"));
            Assert.True(next > now, $"the next run {next:o} must be ahead of {now:o}");
            Assert.True(next <= now.AddMinutes(CollectorRunTime.MaxStampAhead(1440).TotalMinutes), $"the next run {next:o} is more than a day and the spread away");

            /* No clock for this server, so the run time reads as UTC: 02:00 plus its fixed spread. */
            Assert.Equal(TimeSpan.FromMinutes(TwoAm) + CollectorRunTime.Spread(serverId), next.TimeOfDay);

            var minute = rows[MinuteCollector];
            Assert.Equal(JsonValueKind.Null, minute.GetProperty("run_at").ValueKind);
            Assert.Equal(JsonValueKind.Null, minute.GetProperty("next_run_utc").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ARunTimeIsOnTheServersClock_AndAServerWithNoClockReadsItAsUtc()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const int plainId = 480_011;
            const int offsetId = 480_012;
            const string plain = "runtime-health-utc";
            const string offset = "runtime-health-west";
            await RegisterServerAsync(connection, plainId, plain, ct);
            await RegisterServerAsync(connection, offsetId, offset, ct);

            var now = DateTime.UtcNow;
            await LogRunAsync(connection, plainId, plain, DailyCollector, now.AddMinutes(-1), ct);
            await LogRunAsync(connection, offsetId, offset, DailyCollector, now.AddMinutes(-1), ct);

            /* Five hours behind UTC, a fixed offset: its 02:00 is 07:00 UTC. */
            await PlantOffsetAsync(connection, offsetId, offset, -300, ct);
            await SetFleetRunTimeAsync(connection, DailyCollector, TwoAm, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var plainRow = (await ReadRowsAsync(postgres, plain, ct))[DailyCollector];
            var offsetRow = (await ReadRowsAsync(postgres, offset, ct))[DailyCollector];

            Assert.Equal("02:00", plainRow.GetProperty("run_at").GetString());
            Assert.Equal("02:00", offsetRow.GetProperty("run_at").GetString());
            Assert.Equal(TimeSpan.FromMinutes(TwoAm) + CollectorRunTime.Spread(plainId), ParseUtc(plainRow.GetProperty("next_run_utc")).TimeOfDay);
            Assert.Equal(TimeSpan.FromHours(7) + CollectorRunTime.Spread(offsetId), ParseUtc(offsetRow.GetProperty("next_run_utc")).TimeOfDay);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task TheHealthBand_IsTheSameWithAndWithoutARunTime_AndTheRunTimeIsNotHeldByTheHealthMemo()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const int serverId = 480_021;
            const string server = "runtime-health-band";
            await RegisterServerAsync(connection, serverId, server, ct);

            /* A daily collector that last ran 40 hours ago is past the 36-hour stale line. Skipping a day is a real
               miss and the row says so; a run time does not excuse it. */
            await LogRunAsync(connection, serverId, server, DailyCollector, DateTime.UtcNow.AddHours(-40), ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var before = (await ReadRowsAsync(postgres, server, ct))[DailyCollector];
            Assert.Equal(JsonValueKind.Null, before.GetProperty("run_at").ValueKind);
            var bandBefore = before.GetProperty("status").GetString();
            Assert.NotEqual(CollectorHealthClassifier.Healthy, bandBefore);

            /* The health rows are memoized for the call that follows. The run time is read fresh, so an edit shows. */
            await SetFleetRunTimeAsync(connection, DailyCollector, TwoAm, ct);
            var after = (await ReadRowsAsync(postgres, server, ct))[DailyCollector];

            Assert.Equal(bandBefore, after.GetProperty("status").GetString());
            Assert.Equal("02:00", after.GetProperty("run_at").GetString());
            Assert.Equal(JsonValueKind.String, after.GetProperty("next_run_utc").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
