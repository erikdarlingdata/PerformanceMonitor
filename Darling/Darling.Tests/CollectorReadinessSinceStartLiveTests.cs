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

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #9417: <c>get_collector_readiness</c> against a live store. Before a collector's first good cycle since the service start it is on the
/// waiting list with its errors, a collector that ran just before the start and is not due again yet is not waiting, and after the first good
/// cycle it moves to done. When every due collector has one, the verdict says so and the first full cycle is the newest of those first good
/// cycles.
/// </summary>
public sealed class CollectorReadinessSinceStartLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task DueCollectorWaitsUntilItsFirstGoodCycleSinceTheStart_ThenMovesToDone()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live get_collector_readiness test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var now = DateTime.UtcNow;
            var start = now.AddMinutes(-10);
            await RegisterServerAsync(connection, 9417001, "readiness-a", ct);
            await RegisterServerAsync(connection, 9417002, "readiness-b", ct);

            /* A daily collector that ran five minutes before the restart waits out its interval: not waiting, not done. */
            var daily = CollectorCatalog.All
                .Select(d => d.Name)
                .First(n => CollectorScheduleDefaults.All.TryGetValue(n, out var e) && e.DefaultEnabled && e.FrequencyMinutes >= 1440);
            await InsertAsync(connection, 9417001, "readiness-a", daily, start.AddMinutes(-5), "SUCCESS", null, ct);

            /* Server A: wait_stats is good since the start, memory_stats only errored. */
            await InsertAsync(connection, 9417001, "readiness-a", "wait_stats", start.AddMinutes(1), "SUCCESS", null, ct);
            await InsertAsync(connection, 9417001, "readiness-a", "memory_stats", start.AddMinutes(2), "ERROR", "simulated failure", ct);
            /* A good cycle from BEFORE the start does not count as a good cycle since it. */
            await InsertAsync(connection, 9417001, "readiness-a", "file_io_stats", start.AddMinutes(-30), "SUCCESS", null, ct);

            using (var doc = JsonDocument.Parse(await DarlingMcpCollectorReadinessTools.GetCollectorReadiness(postgres, null, start, now, ct)))
            {
                var root = doc.RootElement;
                Assert.Equal("ok", root.GetProperty("status").GetString());
                Assert.False(root.GetProperty("verdict").GetString()!.StartsWith("Every due collector", StringComparison.Ordinal));
                Assert.Equal("not yet", root.GetProperty("first_full_cycle").GetProperty("status").GetString());
                Assert.Equal(JsonValueKind.Null, root.GetProperty("first_full_cycle").GetProperty("at").ValueKind);
                var waiting = root.GetProperty("waiting").EnumerateArray().ToList();
                bool Waits(string server, string collector) =>
                    waiting.Any(w => w.GetProperty("server").GetString() == server && w.GetProperty("collector").GetString() == collector);
                Assert.True(Waits("readiness-a", "memory_stats"));
                Assert.Equal(1, waiting.Single(w => w.GetProperty("collector").GetString() == "memory_stats" && w.GetProperty("server").GetString() == "readiness-a")
                    .GetProperty("errors_since_start").GetInt64());
                Assert.True(Waits("readiness-a", "file_io_stats"));
                Assert.True(Waits("readiness-b", "wait_stats"));
                Assert.False(Waits("readiness-a", "wait_stats"));
                Assert.False(Waits("readiness-a", daily));
                Assert.True(root.GetProperty("not_yet_due_count").GetInt32() >= 1);
            }

            /* The first good cycle of memory_stats since the start moves it to done on a read of that one server. */
            await InsertAsync(connection, 9417001, "readiness-a", "memory_stats", start.AddMinutes(4), "SUCCESS", null, ct);
            using (var doc = JsonDocument.Parse(await DarlingMcpCollectorReadinessTools.GetCollectorReadiness(postgres, "readiness-a", start, now, ct)))
            {
                var root = doc.RootElement;
                Assert.DoesNotContain(root.GetProperty("waiting").EnumerateArray(), w => w.GetProperty("collector").GetString() == "memory_stats");
                var done = root.GetProperty("done").EnumerateArray().Single(d => d.GetProperty("collector").GetString() == "memory_stats");
                Assert.Equal(1, done.GetProperty("errors_since_start").GetInt64());
            }

            /* Every collector the catalog runs gets a good cycle on both servers: the verdict flips and the first full cycle is the newest first good cycle. */
            /* PostgreSQL keeps microseconds, so the expected instant is cut to them. */
            var newest = start.AddMinutes(6);
            newest = new DateTime(newest.Ticks - (newest.Ticks % 10), DateTimeKind.Utc);
            foreach (var name in CollectorCatalog.All.Select(d => d.Name))
            {
                await InsertAsync(connection, 9417001, "readiness-a", name, start.AddMinutes(3), "SUCCESS", null, ct);
                await InsertAsync(connection, 9417002, "readiness-b", name, name == "wait_stats" ? newest : start.AddMinutes(3), "SUCCESS", null, ct);
            }

            using (var doc = JsonDocument.Parse(await DarlingMcpCollectorReadinessTools.GetCollectorReadiness(postgres, null, start, now, ct)))
            {
                var root = doc.RootElement;
                Assert.Equal(0, root.GetProperty("waiting_count").GetInt32());
                Assert.StartsWith("Every due collector has a good cycle", root.GetProperty("verdict").GetString(), StringComparison.Ordinal);
                var cycle = root.GetProperty("first_full_cycle");
                Assert.Equal("reached", cycle.GetProperty("status").GetString());
                Assert.Equal(DateTime.SpecifyKind(newest, DateTimeKind.Utc).ToString("o"), cycle.GetProperty("at").GetString());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task LastRunOlderThanTheWorkersTwoDayFloor_ReadsAsNeverRun_ForAnIntervalOverTwoDays()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live get_collector_readiness test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var now = DateTime.UtcNow;
            var start = now.AddMinutes(-10);
            await RegisterServerAsync(connection, 9417011, "readiness-floor", ct);

            /* Both collectors run every five days. One last ran three days before the start, past the worker's two-day floor, so the worker
               seeds it as never run and it is due from the start. The other ran one day before, inside the floor, so it waits out its interval. */
            await SetIntervalAsync(connection, 9417011, "wait_stats", 7200, ct);
            await SetIntervalAsync(connection, 9417011, "memory_stats", 7200, ct);
            await InsertAsync(connection, 9417011, "readiness-floor", "wait_stats", start.AddDays(-3), "SUCCESS", null, ct);
            await InsertAsync(connection, 9417011, "readiness-floor", "memory_stats", start.AddDays(-1), "SUCCESS", null, ct);

            using var doc = JsonDocument.Parse(await DarlingMcpCollectorReadinessTools.GetCollectorReadiness(postgres, "readiness-floor", start, now, ct));
            var waiting = doc.RootElement.GetProperty("waiting").EnumerateArray().ToList();
            var old = waiting.Single(w => w.GetProperty("collector").GetString() == "wait_stats");
            Assert.Equal(DateTime.SpecifyKind(start, DateTimeKind.Utc).ToString("o"), old.GetProperty("waiting_since").GetString());
            Assert.DoesNotContain(waiting, w => w.GetProperty("collector").GetString() == "memory_stats");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task FirstFullCycleCountsOnlyPairsDueAtTheStart_AndStaysReachedWhileALateDuePairKeepsFailing()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live get_collector_readiness test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var now = DateTime.UtcNow;
            var start = now.AddMinutes(-10);
            await RegisterServerAsync(connection, 9417021, "readiness-late", ct);

            /* memory_stats runs every 12 minutes and last ran five minutes before the start, so it came due seven minutes after the start. */
            await SetIntervalAsync(connection, 9417021, "memory_stats", 12, ct);
            await InsertAsync(connection, 9417021, "readiness-late", "memory_stats", start.AddMinutes(-5), "SUCCESS", null, ct);
            await InsertAsync(connection, 9417021, "readiness-late", "memory_stats", start.AddMinutes(8), "ERROR", "still failing", ct);

            /* Every other collector the catalog runs is due at the start and has a good cycle. */
            var good = start.AddMinutes(3);
            good = new DateTime(good.Ticks - (good.Ticks % 10), DateTimeKind.Utc);
            foreach (var name in CollectorCatalog.All.Select(d => d.Name).Where(n => n != "memory_stats"))
            {
                await InsertAsync(connection, 9417021, "readiness-late", name, good, "SUCCESS", null, ct);
            }

            foreach (var asOf in new[] { now, now.AddMinutes(1) })
            {
                using var doc = JsonDocument.Parse(await DarlingMcpCollectorReadinessTools.GetCollectorReadiness(postgres, null, start, asOf, ct));
                var root = doc.RootElement;
                Assert.Contains(root.GetProperty("waiting").EnumerateArray(), w => w.GetProperty("collector").GetString() == "memory_stats");
                var cycle = root.GetProperty("first_full_cycle");
                Assert.Equal("reached", cycle.GetProperty("status").GetString());
                Assert.Equal(good.ToString("o"), cycle.GetProperty("at").GetString());
                var server = root.GetProperty("done").EnumerateArray().Single();
                Assert.Equal(good.ToString("o"), server.GetProperty("first_full_cycle_at").GetString());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// #9585: with no collector waiting, three collectors that run at a set time and have no good cycle since the start (one logged an ERROR, two have not run) are named as such in the
    /// verdict, not counted as "not due yet". The reply's own fields say the same: not_yet_due_count 0 and fixed_time_count 3.
    /// </summary>
    [Fact]
    public async Task AllDoneVerdict_NamesSetTimeCollectorsApart_WhenNoneIsNotYetDue()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live get_collector_readiness test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var now = DateTime.UtcNow;
            var start = now.AddMinutes(-10);
            await RegisterServerAsync(connection, 9585001, "readiness-settime", ct);

            /* Three daily collectors run at a fixed time of day and have no good cycle: the first has an ERROR run since the start, the other two have no run at all. Every other collector the catalog runs has a good cycle since the start. */
            var setTime = DailyCollectors(3);
            Assert.Equal(3, setTime.Count);
            foreach (var name in setTime)
            {
                await SetRunTimeAsync(connection, 9585001, name, 120, ct);
            }

            await InsertAsync(connection, 9585001, "readiness-settime", setTime[0], start.AddMinutes(2), "ERROR", "simulated failure", ct);

            foreach (var name in CollectorCatalog.All.Select(d => d.Name).Except(setTime))
            {
                await InsertAsync(connection, 9585001, "readiness-settime", name, start.AddMinutes(3), "SUCCESS", null, ct);
            }

            using var doc = JsonDocument.Parse(await DarlingMcpCollectorReadinessTools.GetCollectorReadiness(postgres, null, start, now, ct));
            var root = doc.RootElement;
            var verdict = root.GetProperty("verdict").GetString()!;
            Assert.Equal(0, root.GetProperty("waiting_count").GetInt32());
            Assert.Equal(0, root.GetProperty("not_yet_due_count").GetInt32());
            Assert.Equal(3, root.GetProperty("fixed_time_count").GetInt32());
            Assert.StartsWith("Every due collector has a good cycle", verdict, StringComparison.Ordinal);
            Assert.Contains(", 3 run at a set time and have no good cycle since the start", verdict, StringComparison.Ordinal);
            Assert.DoesNotContain("not due yet", verdict, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// #9585: with both kinds present, the verdict carries each phrase with its own count, and each count is the reply's field of the same
    /// meaning. The counts differ (two and three) so a sum or a swap cannot read as right.
    /// </summary>
    [Fact]
    public async Task AllDoneVerdict_NamesBothKinds_EachWithItsOwnCount()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live get_collector_readiness test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var now = DateTime.UtcNow;
            var start = now.AddMinutes(-10);
            await RegisterServerAsync(connection, 9585011, "readiness-both", ct);

            /* Three daily collectors run at a fixed time of day and have no run. Two collectors given an hourly interval last ran five minutes before the start, so they wait out the rest of the hour. */
            var setTime = DailyCollectors(3);
            Assert.Equal(3, setTime.Count);
            foreach (var name in setTime)
            {
                await SetRunTimeAsync(connection, 9585011, name, 120, ct);
            }

            var notYetDue = IntervalCollectors(2);
            Assert.Equal(2, notYetDue.Count);
            foreach (var name in notYetDue)
            {
                await SetIntervalAsync(connection, 9585011, name, 60, ct);
                await InsertAsync(connection, 9585011, "readiness-both", name, start.AddMinutes(-5), "SUCCESS", null, ct);
            }

            foreach (var name in CollectorCatalog.All.Select(d => d.Name).Except(setTime).Except(notYetDue))
            {
                await InsertAsync(connection, 9585011, "readiness-both", name, start.AddMinutes(3), "SUCCESS", null, ct);
            }

            using var doc = JsonDocument.Parse(await DarlingMcpCollectorReadinessTools.GetCollectorReadiness(postgres, null, start, now, ct));
            var root = doc.RootElement;
            var verdict = root.GetProperty("verdict").GetString()!;
            var notYetDueCount = root.GetProperty("not_yet_due_count").GetInt32();
            var fixedTimeCount = root.GetProperty("fixed_time_count").GetInt32();
            Assert.Equal(0, root.GetProperty("waiting_count").GetInt32());
            Assert.Equal(2, notYetDueCount);
            Assert.Equal(3, fixedTimeCount);
            Assert.Contains($", {notYetDueCount} not due yet", verdict, StringComparison.Ordinal);
            Assert.Contains($", {fixedTimeCount} run at a set time and have no good cycle since the start", verdict, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// #9585: in the fleet answer (no server_name) the per-server row counts set-time collectors apart from not-yet-due ones, as the top-level
    /// fields do: collectors_not_yet_due is 2 and collectors_fixed_time is 3, so a sum or a swap cannot pass.
    /// </summary>
    [Fact]
    public async Task FleetPerServerRow_CountsSetTimeCollectorsApartFromNotYetDueOnes()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live get_collector_readiness test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var now = DateTime.UtcNow;
            var start = now.AddMinutes(-10);
            await RegisterServerAsync(connection, 9585021, "readiness-fleetrow", ct);

            /* Three daily collectors run at a fixed time of day and have no run. Two collectors given an hourly interval last ran five minutes before the start, so they wait out the rest of the hour. */
            var setTime = DailyCollectors(3);
            Assert.Equal(3, setTime.Count);
            foreach (var name in setTime)
            {
                await SetRunTimeAsync(connection, 9585021, name, 120, ct);
            }

            var notYetDue = IntervalCollectors(2);
            Assert.Equal(2, notYetDue.Count);
            foreach (var name in notYetDue)
            {
                await SetIntervalAsync(connection, 9585021, name, 60, ct);
                await InsertAsync(connection, 9585021, "readiness-fleetrow", name, start.AddMinutes(-5), "SUCCESS", null, ct);
            }

            foreach (var name in CollectorCatalog.All.Select(d => d.Name).Except(setTime).Except(notYetDue))
            {
                await InsertAsync(connection, 9585021, "readiness-fleetrow", name, start.AddMinutes(3), "SUCCESS", null, ct);
            }

            using var doc = JsonDocument.Parse(await DarlingMcpCollectorReadinessTools.GetCollectorReadiness(postgres, null, start, now, ct));
            var row = doc.RootElement.GetProperty("done").EnumerateArray().Single(d => d.GetProperty("server").GetString() == "readiness-fleetrow");
            Assert.Equal(2, row.GetProperty("collectors_not_yet_due").GetInt32());
            Assert.Equal(3, row.GetProperty("collectors_fixed_time").GetInt32());
            Assert.Equal(0, row.GetProperty("collectors_waiting").GetInt32());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>The first <paramref name="count"/> catalog collectors on this engine that run once a day by default: a fixed run time applies only to a whole number of days.</summary>
    private static List<string> DailyCollectors(int count) =>
        CollectorCatalog.All
            .Select(d => d.Name)
            .Where(n => !n.StartsWith("pg_", StringComparison.Ordinal)
                && CollectorScheduleDefaults.All.TryGetValue(n, out var e) && e.DefaultEnabled && e.FrequencyMinutes >= 1440 && e.FrequencyMinutes % 1440 == 0)
            .Take(count)
            .ToList();

    /// <summary>The first <paramref name="count"/> catalog collectors on this engine that run every few minutes by default and keep a stretched interval: a delta-family collector refuses an interval past its gap cap.</summary>
    private static List<string> IntervalCollectors(int count) =>
        CollectorCatalog.All
            .Select(d => d.Name)
            .Where(n => !n.StartsWith("pg_", StringComparison.Ordinal) && !CollectorDeltaCalculator.IsDeltaFamily(n)
                && CollectorScheduleDefaults.All.TryGetValue(n, out var e) && e.DefaultEnabled && e.FrequencyMinutes is > 0 and < 1440)
            .Take(count)
            .ToList();

    /// <summary>One per-server run time (<c>config.config_collector_run_times</c>), in minutes after midnight.</summary>
    private static async Task SetRunTimeAsync(NpgsqlConnection connection, int serverId, string collector, int minute, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO config.config_collector_run_times (server_id, collector_name, run_at_minute) VALUES ($1, $2, $3)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(collector);
        command.Parameters.Add(new NpgsqlParameter { Value = (short)minute, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Smallint });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task SetIntervalAsync(NpgsqlConnection connection, int serverId, string collector, int minutes, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) VALUES ($1,$2,$3,NULL,TRUE)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(collector);
        command.Parameters.AddWithValue(minutes);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, int serverId, string name, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 15, $3, $3)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, int serverId, string serverName, string collector, DateTime at, string status, string? error, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, error_message)
VALUES ($1,$2,$3,$4,$5,$6,10,5,$7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(collector);
        command.Parameters.AddWithValue(status);
        command.Parameters.AddWithValue((object?)error ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }
}
