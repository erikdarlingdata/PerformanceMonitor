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
/// due, UTC). A collector without one carries null for both on a full row, and nothing for either on a partial or a
/// compact row. The server's row wins over the fleet row, a run time of
/// -1 on the server's row stops the fleet's, and a collector whose interval is not a whole number of days has none.
/// The health band is read from the shipped cadence and a run time does not move it. A server with no clock yet reads
/// the run time as UTC. A day the collector skipped crosses the stale line before the next slot, and the row carries
/// a <c>run_time_note</c> that says so.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class CollectionHealthRunTimeFieldsLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the run-time health fields' live pins (each mints its own scratch database).";

    /* Two collectors on a daily interval (the second is an on-load collector the shipped schedule recaptures daily) and
       one on a one-minute interval. */
    private const string DailyCollector = "index_object_stats";
    private const string SecondDailyCollector = "server_config";
    private const string MinuteCollector = "wait_stats";

    /* A run time 12 hours from now, so today's slot and the one before it are both more than 11 hours from now whatever
       the server's spread is, and a test never lands inside the 60-minute window after a slot. A fixed 02:00 would sit in
       that window for two hours of every day. */
    private static int RunTimeHalfADayAway(DateTime nowUtc) => (nowUtc.Hour * 60 + nowUtc.Minute + 12 * 60) % 1440;

    /* The UTC time of day of a slot: the run time on a clock this many minutes behind UTC, plus the server's spread. */
    private static TimeSpan SlotTimeOfDayUtc(int runAtMinute, int serverId, int minutesBehindUtc = 0)
    {
        var slot = TimeSpan.FromMinutes(runAtMinute + minutesBehindUtc) + CollectorRunTime.Spread(serverId);
        return TimeSpan.FromTicks(slot.Ticks % TimeSpan.FromDays(1).Ticks);
    }

    private static async Task<NpgsqlConnection> MigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
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

    /// <summary>One failed run of a collector: an ERROR row in the window, which keeps the collector off the compact shape
    /// and gives it the partial one.</summary>
    private static async Task LogFailureAsync(NpgsqlConnection connection, int serverId, string serverName, string collector, DateTime whenUtc, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, error_message)
            VALUES ($1, $2, $3, $4, $5, 'ERROR', 120, 0, 'a failed run')
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

    /// <summary>One schedule row: a fleet row when <paramref name="serverId"/> is null, else that server's. The run
    /// time is minutes after midnight, or -1 for "no fixed time".</summary>
    private static async Task SetRunTimeAsync(NpgsqlConnection connection, int? serverId, string collector, int minute, CancellationToken ct, bool enabled = true)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO config.config_collector_schedules (server_id, collector_name, run_at_minute, enabled) VALUES ($1, $2, $3, $4)", connection);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)serverId ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        command.Parameters.AddWithValue(collector);
        command.Parameters.Add(new NpgsqlParameter { Value = (short)minute, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Smallint });
        command.Parameters.AddWithValue(enabled);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task SetFleetRunTimeAsync(NpgsqlConnection connection, string collector, int minute, CancellationToken ct) =>
        SetRunTimeAsync(connection, null, collector, minute, ct);

    /// <summary>The rows of <c>get_collection_health</c> by collector. Full detail by default, because a healthy
    /// collector otherwise compacts to seven fields.</summary>
    private static async Task<Dictionary<string, JsonElement>> ReadRowsAsync(NpgsqlDataSource postgres, string server, CancellationToken ct, bool fullDetail = true)
    {
        var json = await DarlingMcpDataTools.GetCollectionHealth(postgres, server, full_detail: fullDetail, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("collectors", out var collectors), json.Length > 400 ? json[..400] : json);
        return collectors.EnumerateArray().ToDictionary(r => r.GetProperty("collector").GetString()!, r => r.Clone());
    }

    private static DateTime ParseUtc(JsonElement value)
    {
        var parsed = DateTime.Parse(value.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
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

            /* Both ran a minute ago, so the daily collector's next slot is the one ahead of now. */
            var now = DateTime.UtcNow;
            var runAt = RunTimeHalfADayAway(now);
            await LogRunAsync(connection, serverId, server, DailyCollector, now.AddMinutes(-1), ct);
            await LogRunAsync(connection, serverId, server, MinuteCollector, now.AddMinutes(-1), ct);
            await SetFleetRunTimeAsync(connection, DailyCollector, runAt, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rows = await ReadRowsAsync(postgres, server, ct);

            var daily = rows[DailyCollector];
            Assert.Equal(CollectorRunTime.Format(runAt), daily.GetProperty("run_at").GetString());
            var next = ParseUtc(daily.GetProperty("next_run_utc"));
            Assert.True(next > now, $"the next run {next:o} must be ahead of {now:o}");
            Assert.True(next <= now + CollectorRunTime.MaxStampAhead(1440), $"the next run {next:o} is more than a day and the spread away");

            /* No clock for this server, so the run time reads as UTC: the run time plus its fixed spread. */
            Assert.Equal(SlotTimeOfDayUtc(runAt, serverId), next.TimeOfDay);

            /* Nothing was skipped: it ran a minute ago. */
            Assert.Equal(JsonValueKind.Null, daily.GetProperty("run_time_note").ValueKind);

            var minute = rows[MinuteCollector];
            Assert.Equal(JsonValueKind.Null, minute.GetProperty("run_at").ValueKind);
            Assert.Equal(JsonValueKind.Null, minute.GetProperty("next_run_utc").ValueKind);
            Assert.Equal(JsonValueKind.Null, minute.GetProperty("run_time_note").ValueKind);

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
            var runAt = RunTimeHalfADayAway(now);
            await LogRunAsync(connection, plainId, plain, DailyCollector, now.AddMinutes(-1), ct);
            await LogRunAsync(connection, offsetId, offset, DailyCollector, now.AddMinutes(-1), ct);

            /* Five hours behind UTC, a fixed offset: its run time is five hours later in UTC. */
            await PlantOffsetAsync(connection, offsetId, offset, -300, ct);
            await SetFleetRunTimeAsync(connection, DailyCollector, runAt, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var plainRow = (await ReadRowsAsync(postgres, plain, ct))[DailyCollector];
            var offsetRow = (await ReadRowsAsync(postgres, offset, ct))[DailyCollector];

            /* The run time is the same text on both: it is each server's own wall clock. */
            Assert.Equal(CollectorRunTime.Format(runAt), plainRow.GetProperty("run_at").GetString());
            Assert.Equal(CollectorRunTime.Format(runAt), offsetRow.GetProperty("run_at").GetString());
            Assert.Equal(SlotTimeOfDayUtc(runAt, plainId), ParseUtc(plainRow.GetProperty("next_run_utc")).TimeOfDay);
            Assert.Equal(SlotTimeOfDayUtc(runAt, offsetId, minutesBehindUtc: 300), ParseUtc(offsetRow.GetProperty("next_run_utc")).TimeOfDay);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task AServerRunTimeWinsOverTheFleetRunTime_MinusOneOnTheServerStopsIt_AndAnIntervalThatIsNotWholeDaysHasNone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const int ownId = 480_031;
            const int noneId = 480_032;
            const int fleetId = 480_033;
            const string own = "runtime-health-own";
            const string none = "runtime-health-none";
            const string fleetOnly = "runtime-health-fleet";
            var now = DateTime.UtcNow;
            foreach (var (id, name) in new[] { (ownId, own), (noneId, none), (fleetId, fleetOnly) })
            {
                await RegisterServerAsync(connection, id, name, ct);
                await LogRunAsync(connection, id, name, DailyCollector, now.AddMinutes(-1), ct);
                await LogRunAsync(connection, id, name, MinuteCollector, now.AddMinutes(-1), ct);
            }

            /* The fleet runs the daily collector at 02:00. One server has its own 04:30, and one has -1, which means no
               fixed time there and stops the fleet's 02:00 from applying. The fleet row also names the one-minute
               collector, which a run time cannot apply to. */
            await SetFleetRunTimeAsync(connection, DailyCollector, 120, ct);
            await SetFleetRunTimeAsync(connection, MinuteCollector, 120, ct);
            await SetRunTimeAsync(connection, ownId, DailyCollector, 270, ct);
            await SetRunTimeAsync(connection, noneId, DailyCollector, -1, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var ownRows = await ReadRowsAsync(postgres, own, ct);
            var noneRows = await ReadRowsAsync(postgres, none, ct);
            var fleetRows = await ReadRowsAsync(postgres, fleetOnly, ct);

            Assert.Equal("04:30", ownRows[DailyCollector].GetProperty("run_at").GetString());
            Assert.Equal(JsonValueKind.String, ownRows[DailyCollector].GetProperty("next_run_utc").ValueKind);

            Assert.Equal(JsonValueKind.Null, noneRows[DailyCollector].GetProperty("run_at").ValueKind);
            Assert.Equal(JsonValueKind.Null, noneRows[DailyCollector].GetProperty("next_run_utc").ValueKind);

            Assert.Equal("02:00", fleetRows[DailyCollector].GetProperty("run_at").GetString());

            foreach (var rows in new[] { ownRows, noneRows, fleetRows })
            {
                Assert.Equal(JsonValueKind.Null, rows[MinuteCollector].GetProperty("run_at").ValueKind);
                Assert.Equal(JsonValueKind.Null, rows[MinuteCollector].GetProperty("next_run_utc").ValueKind);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ADisabledCollectorWithARunTime_ShowsItsRunTimeAndNoNextRun()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const int serverId = 480_041;
            const string server = "runtime-health-off";
            await RegisterServerAsync(connection, serverId, server, ct);
            await LogRunAsync(connection, serverId, server, DailyCollector, DateTime.UtcNow.AddMinutes(-1), ct);
            await SetRunTimeAsync(connection, null, DailyCollector, 120, ct, enabled: false);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var row = (await ReadRowsAsync(postgres, server, ct))[DailyCollector];

            Assert.Equal("02:00", row.GetProperty("run_at").GetString());
            Assert.Equal(JsonValueKind.Null, row.GetProperty("next_run_utc").ValueKind);
            Assert.Equal(JsonValueKind.Null, row.GetProperty("run_time_note").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ACompactRowCarriesTheRunTimeOnlyWhenTheCollectorHasOne()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const int serverId = 480_051;
            const string server = "runtime-health-compact";
            await RegisterServerAsync(connection, serverId, server, ct);
            var now = DateTime.UtcNow;
            await LogRunAsync(connection, serverId, server, DailyCollector, now.AddMinutes(-1), ct);
            await LogRunAsync(connection, serverId, server, MinuteCollector, now.AddMinutes(-1), ct);
            var runAt = RunTimeHalfADayAway(now);
            await SetFleetRunTimeAsync(connection, DailyCollector, runAt, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rows = await ReadRowsAsync(postgres, server, ct, fullDetail: false);

            /* Both are healthy with nothing to report, so both compact. A compact row stays small: it carries the run
               time only for the collector that has one, and never the note, which a healthy row cannot need. */
            var daily = rows[DailyCollector];
            Assert.True(daily.GetProperty("compact").GetBoolean());
            Assert.Equal(CollectorRunTime.Format(runAt), daily.GetProperty("run_at").GetString());
            Assert.Equal(JsonValueKind.String, daily.GetProperty("next_run_utc").ValueKind);
            Assert.False(daily.TryGetProperty("run_time_note", out _));

            var minute = rows[MinuteCollector];
            Assert.True(minute.GetProperty("compact").GetBoolean());
            Assert.False(minute.TryGetProperty("run_at", out _));
            Assert.False(minute.TryGetProperty("next_run_utc", out _));
            Assert.False(minute.TryGetProperty("run_time_note", out _));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task APartialRowCarriesTheRunTimeOnlyWhenTheCollectorHasOne()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const int serverId = 480_061;
            const string server = "runtime-health-partial";
            await RegisterServerAsync(connection, serverId, server, ct);
            var now = DateTime.UtcNow;

            /* A success and then a failure for each collector: an error in the window fails the compact test, so both
               take the partial shape. */
            foreach (var collector in new[] { DailyCollector, MinuteCollector })
            {
                await LogRunAsync(connection, serverId, server, collector, now.AddMinutes(-2), ct);
                await LogFailureAsync(connection, serverId, server, collector, now.AddMinutes(-1), ct);
            }

            var runAt = RunTimeHalfADayAway(now);
            await SetFleetRunTimeAsync(connection, DailyCollector, runAt, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rows = await ReadRowsAsync(postgres, server, ct, fullDetail: false);

            /* Lite's partial row carries the run time only for a collector that has one, so a partial row stays as
               small as the rest of its keys and the two apps give a caller the same shape. The note keeps its rule:
               every partial row carries it, and it is null unless a day was skipped. */
            var daily = rows[DailyCollector];
            Assert.True(daily.GetProperty("partial_detail").GetBoolean());
            Assert.Equal(CollectorRunTime.Format(runAt), daily.GetProperty("run_at").GetString());
            Assert.Equal(JsonValueKind.String, daily.GetProperty("next_run_utc").ValueKind);
            Assert.Equal(JsonValueKind.Null, daily.GetProperty("run_time_note").ValueKind);

            var minute = rows[MinuteCollector];
            Assert.True(minute.GetProperty("partial_detail").GetBoolean());
            Assert.False(minute.TryGetProperty("run_at", out _));
            Assert.False(minute.TryGetProperty("next_run_utc", out _));
            Assert.Equal(JsonValueKind.Null, minute.GetProperty("run_time_note").ValueKind);

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
            Assert.Equal(CollectorHealthClassifier.Stale, before.GetProperty("status").GetString());

            /* The health rows are memoized for the call that follows. The run time is read fresh, so an edit shows. */
            var runAt = RunTimeHalfADayAway(DateTime.UtcNow);
            await SetFleetRunTimeAsync(connection, DailyCollector, runAt, ct);
            var after = (await ReadRowsAsync(postgres, server, ct))[DailyCollector];

            Assert.Equal(CollectorHealthClassifier.Stale, after.GetProperty("status").GetString());
            Assert.Equal(CollectorRunTime.Format(runAt), after.GetProperty("run_at").GetString());
            Assert.Equal(JsonValueKind.String, after.GetProperty("next_run_utc").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ASkippedDay_CrossesTheStaleLineBeforeTheNextSlot_AndTheRowSaysSo()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const int serverId = 480_061;
            const string server = "runtime-health-skipped";
            await RegisterServerAsync(connection, serverId, server, ct);

            /* The daily collector last ran 40 hours ago, so it missed a day and is past its 36-hour stale line, and its
               next slot is half a day away. The second daily collector ran a minute ago: nothing was skipped there. */
            var now = DateTime.UtcNow;
            var runAt = RunTimeHalfADayAway(now);
            await LogRunAsync(connection, serverId, server, DailyCollector, now.AddHours(-40), ct);
            await LogRunAsync(connection, serverId, server, SecondDailyCollector, now.AddMinutes(-1), ct);
            await SetFleetRunTimeAsync(connection, DailyCollector, runAt, ct);
            await SetFleetRunTimeAsync(connection, SecondDailyCollector, runAt, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rows = await ReadRowsAsync(postgres, server, ct);

            var skipped = rows[DailyCollector];
            Assert.Equal(CollectorHealthClassifier.Stale, skipped.GetProperty("status").GetString());
            var next = ParseUtc(skipped.GetProperty("next_run_utc"));
            Assert.True(next > now, "the next slot is still ahead of the stale reading");
            var note = skipped.GetProperty("run_time_note").GetString();
            Assert.NotNull(note);
            Assert.Contains("Skipped day", note, StringComparison.Ordinal);
            Assert.Contains("36-hour stale line", note, StringComparison.Ordinal);
            Assert.Contains(next.ToString("u", CultureInfo.InvariantCulture), note, StringComparison.Ordinal);
            Assert.Contains("not replayed", note, StringComparison.Ordinal);

            var onTime = rows[SecondDailyCollector];
            Assert.Equal(CollectorHealthClassifier.Healthy, onTime.GetProperty("status").GetString());
            Assert.Equal(JsonValueKind.Null, onTime.GetProperty("run_time_note").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ACollectorDueNowInsideItsGrace_HasNoSkippedDayNote_EvenPastTheStaleLine()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const int serverId = 480_071;
            const string server = "runtime-health-due";
            await RegisterServerAsync(connection, serverId, server, ct);

            /* A run time whose slot for this server passed about ten minutes ago: now is inside the 60-minute grace, so
               the collector is due now. It last ran 40 hours ago, but a run is a moment away, so there is no note. */
            var now = DateTime.UtcNow;
            var spreadMinutes = (int)CollectorRunTime.Spread(serverId).TotalMinutes;
            var runAt = ((now.Hour * 60 + now.Minute - spreadMinutes - 10) % 1440 + 1440) % 1440;
            await LogRunAsync(connection, serverId, server, DailyCollector, now.AddHours(-40), ct);
            await SetFleetRunTimeAsync(connection, DailyCollector, runAt, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var row = (await ReadRowsAsync(postgres, server, ct))[DailyCollector];

            Assert.Equal(CollectorHealthClassifier.Stale, row.GetProperty("status").GetString());
            var next = ParseUtc(row.GetProperty("next_run_utc"));
            Assert.True(Math.Abs((next - DateTime.UtcNow).TotalMinutes) < 2, $"due now: the next run {next:o} is the time of the read");
            Assert.Equal(JsonValueKind.Null, row.GetProperty("run_time_note").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
