using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4938, through the scheduler's two real entry points: the scheduled sweep
/// (<see cref="RemoteCollectorService.RunDueCollectorsAsync(DateTime, CancellationToken)"/>) and the run a newly opened
/// server tab starts (<see cref="RemoteCollectorService.RunAllCollectorsForServerAsync"/>). A collector name the
/// dispatch does not know fails before any SQL Server is reached and still writes its collection_log row, so a row
/// per name says whether the run happened. Everything else is the file and the local database, the two things a
/// restart keeps: a daily collector that ran today is not run again, a collector with a run time waits for its time,
/// and opening a tab does not run either.
/// </summary>
public sealed class CollectorRunTimeSweepTests : IDisposable
{
    private readonly List<DuckDbInitializer> _initializers = [];

    private static long s_nextLogId = -1_000_000;

    private readonly string _dir = Directory.CreateTempSubdirectory("pm-lite-run-time-sweep-tests-").FullName;

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private async Task<(DuckDbInitializer DuckDb, ServerManager Servers, ServerConnection Server)> OpenAsync()
    {
        var duckDb = new DuckDbInitializer(Path.Combine(_dir, "pm.duckdb"));
        _initializers.Add(duckDb);
        await duckDb.InitializeAsync();
        var servers = new ServerManager(_dir);
        var server = new ServerConnection { ServerName = "sweep-test", DisplayName = "sweep-test" };
        servers.AddServer(server);
        return (duckDb, servers, server);
    }

    /* The server's own schedule, set on a new scheduler (a restart: nothing about runs is in memory). The scheduler is
       built from the folder first: a load drops collectors the dispatch does not know, and these names are made up. */
    private ScheduleManager NewScheduler(ServerConnection server, params (string Name, int Frequency, string? RunAt)[] collectors)
    {
        var manager = new ScheduleManager(_dir);
        manager.SetScheduleForServer(server.Id, collectors.Select(c => new CollectorSchedule
        {
            Name = c.Name,
            Enabled = true,
            FrequencyMinutes = c.Frequency,
            RetentionDays = 30,
            RunAt = c.RunAt,
        }).ToList());
        return manager;
    }

    private static async Task LogAsync(DuckDbInitializer duckDb, ServerConnection server, string collector, DateTime utc)
    {
        using var readLock = duckDb.AcquireReadLock();
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
            VALUES ($1, $2, 'sweep-test', $3, $4, 10, 'SUCCESS')";
        command.Parameters.Add(new DuckDBParameter { Value = Interlocked.Decrement(ref s_nextLogId) });
        command.Parameters.Add(new DuckDBParameter { Value = RemoteCollectorService.GetServerId(server) });
        command.Parameters.Add(new DuckDBParameter { Value = collector });
        command.Parameters.Add(new DuckDBParameter { Value = utc });
        await command.ExecuteNonQueryAsync();
    }

    /* How many times the collector ran or was logged for the server. */
    private static async Task<long> RowsAsync(DuckDbInitializer duckDb, ServerConnection server, string collector)
    {
        using var readLock = duckDb.AcquireReadLock();
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT count(*) FROM collection_log WHERE server_id = $1 AND collector_name = $2";
        command.Parameters.Add(new DuckDBParameter { Value = RemoteCollectorService.GetServerId(server) });
        command.Parameters.Add(new DuckDBParameter { Value = collector });
        return Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    [Fact]
    public void AFileWithRunAt_LoadsInTheSameWayWhereTheTypeHasNoRunAtField_SoAnOlderLiteStillStartsAndReadsIt()
    {
        /* This test uses nothing that is new in #4938, so it passes on a build that has no run_at field: that build
           reads the file with System.Text.Json defaults, which skip a field the type does not have. What it then does
           with the file: it keeps every schedule and ignores run_at, and the next time it saves the schedule it writes
           the file without run_at (the run time is lost on a downgrade). A load that threw would reset the schedule. */
        File.WriteAllText(Path.Combine(_dir, "collection_schedule.json"), """
            {
              "version": 2,
              "default_schedule": [
                { "name": "wait_stats", "enabled": true, "frequency_minutes": 7, "retention_days": 30 },
                { "name": "index_object_stats", "enabled": true, "frequency_minutes": 1440, "retention_days": 30, "run_at": "02:00" }
              ],
              "server_overrides": {
                "srv": { "collectors": [ { "name": "wait_stats", "enabled": true, "frequency_minutes": 9, "retention_days": 30, "run_at": "03:00" } ] }
              }
            }
            """);

        var manager = new ScheduleManager(_dir);

        Assert.Equal(7, manager.GetDefaultSchedule().First(s => s.Name == "wait_stats").FrequencyMinutes);
        Assert.Equal(1440, manager.GetDefaultSchedule().First(s => s.Name == "index_object_stats").FrequencyMinutes);
        Assert.Equal(9, manager.GetScheduleForServer("srv", "wait_stats")!.FrequencyMinutes);
    }

    [Fact]
    public async Task ARestart_DoesNotRunADailyCollectorAgain_ThatRanEarlierToday_InTheScheduledSweep()
    {
        var (duckDb, servers, server) = await OpenAsync();
        try
        {
            var now = DateTime.UtcNow;
            var scheduler = NewScheduler(server,
                ("no_such_ran_today", 1440, null),
                ("no_such_ran_30h_ago", 1440, null),
                ("no_such_never_ran", 1440, null));
            await LogAsync(duckDb, server, "no_such_ran_today", now.AddHours(-3));
            await LogAsync(duckDb, server, "no_such_ran_30h_ago", now.AddHours(-30));

            /* The restart: a new scheduler and a new service over the same local database, with nothing in memory. */
            var service = new RemoteCollectorService(duckDb, servers, scheduler);
            await service.RunDueCollectorsAsync(now, CancellationToken.None);

            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_ran_today"));
            Assert.Equal(2, await RowsAsync(duckDb, server, "no_such_ran_30h_ago"));
            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_never_ran"));
        }
        finally
        {
            duckDb.Dispose();
        }
    }

    [Fact]
    public async Task OpeningATab_DoesNotRunADailyCollectorThatRanToday_OrOneWithARunTime_ButRunsTheOnLoadAndTheHourlyOnes()
    {
        var (duckDb, servers, server) = await OpenAsync();
        try
        {
            var now = DateTime.UtcNow;
            var scheduler = NewScheduler(server,
                ("no_such_on_load", 0, null),
                ("no_such_on_load_run_time", 0, "03:00"),
                ("no_such_daily_ran_today", 1440, null),
                ("no_such_daily_ran_30h_ago", 1440, null),
                ("no_such_daily_run_time", 1440, "03:00"),
                ("no_such_hourly", 60, null));
            await LogAsync(duckDb, server, "no_such_daily_ran_today", now.AddHours(-3));
            await LogAsync(duckDb, server, "no_such_daily_ran_30h_ago", now.AddHours(-30));
            await LogAsync(duckDb, server, "no_such_daily_run_time", now.AddHours(-30));

            var service = new RemoteCollectorService(duckDb, servers, scheduler);
            await service.RunAllCollectorsForServerAsync(server);

            /* The connect capture of an on-load collector still runs, and so does an hourly collector. */
            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_on_load"));
            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_on_load_run_time"));
            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_hourly"));

            /* A daily collector that ran today is left alone; one that is due runs; one with a run time waits for it. */
            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_daily_ran_today"));
            Assert.Equal(2, await RowsAsync(duckDb, server, "no_such_daily_ran_30h_ago"));
            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_daily_run_time"));
        }
        finally
        {
            duckDb.Dispose();
        }
    }

    [Fact]
    public async Task ACollectorWithARunTime_RunsInTheScheduledSweepOnlyInsideItsHour_AndAMissedDayIsNotReplayed()
    {
        var (duckDb, servers, server) = await OpenAsync();
        try
        {
            /* No server_properties row: the clock is unknown, so the run time reads as UTC. The slot is today's date at
               the run time plus the server's fixed spread; the sweeps below are timed from it. */
            var today = DateOnly.FromDateTime(DateTime.UtcNow);
            var slot = CollectorRunTime.SlotUtc(today, 120, RemoteCollectorService.GetServerId(server), CollectorRunTime.LocalIsUtc);
            var scheduler = NewScheduler(server, ("no_such_run_time", 1440, "02:00"));
            var service = new RemoteCollectorService(duckDb, servers, scheduler);

            /* Before the hour, and after it with no run on record (Lite was closed through the whole hour): no run. */
            await service.RunDueCollectorsAsync(slot - TimeSpan.FromMinutes(1), CancellationToken.None);
            Assert.Equal(0, await RowsAsync(duckDb, server, "no_such_run_time"));
            await service.RunDueCollectorsAsync(slot + CollectorRunTime.Grace + TimeSpan.FromMinutes(1), CancellationToken.None);
            Assert.Equal(0, await RowsAsync(duckDb, server, "no_such_run_time"));

            /* Inside the hour: one run. A made-up collector fails, and a failed attempt still ends that day's run (#4938),
               so the same hour does not run it twice. */
            await service.RunDueCollectorsAsync(slot + TimeSpan.FromMinutes(10), CancellationToken.None);
            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_run_time"));
            await service.RunDueCollectorsAsync(slot + TimeSpan.FromMinutes(20), CancellationToken.None);
            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_run_time"));
        }
        finally
        {
            duckDb.Dispose();
        }
    }

    [Fact]
    public async Task ACollectorWithARunTime_ThatKeepsFailing_RunsOnceInsideItsHour_NotOnEverySweep_AndTheNextDaysSlotRunsItAgain()
    {
        var (duckDb, servers, server) = await OpenAsync();
        try
        {
            var today = DateOnly.FromDateTime(DateTime.UtcNow);
            var id = RemoteCollectorService.GetServerId(server);
            var slot = CollectorRunTime.SlotUtc(today, 120, id, CollectorRunTime.LocalIsUtc);
            var nextSlot = CollectorRunTime.SlotUtc(today.AddDays(1), 120, id, CollectorRunTime.LocalIsUtc);

            /* A made-up collector fails on every attempt and writes one ERROR row per attempt. The scheduler used to
               treat the failure as "not run", so every sweep of the hour ran the collector again, while the startup
               read of the log had always counted that row as the run. */
            var scheduler = NewScheduler(server, ("no_such_failing_run_time", 1440, "02:00"));
            var service = new RemoteCollectorService(duckDb, servers, scheduler);

            foreach (var minutes in new[] { 1, 2, 3, 10, 30, 59 })
            {
                await service.RunDueCollectorsAsync(slot + TimeSpan.FromMinutes(minutes), CancellationToken.None);
            }

            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_failing_run_time"));

            /* The rest of the day: no run. The next due time is the next day's slot, not another try on a later sweep. */
            await service.RunDueCollectorsAsync(slot + TimeSpan.FromHours(5), CancellationToken.None);
            await service.RunDueCollectorsAsync(nextSlot - TimeSpan.FromMinutes(1), CancellationToken.None);
            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_failing_run_time"));

            /* The next day's slot runs it once more, and fails once more, and ends that day too. */
            foreach (var minutes in new[] { 5, 15, 45 })
            {
                await service.RunDueCollectorsAsync(nextSlot + TimeSpan.FromMinutes(minutes), CancellationToken.None);
            }

            Assert.Equal(2, await RowsAsync(duckDb, server, "no_such_failing_run_time"));
        }
        finally
        {
            duckDb.Dispose();
        }
    }

    [Fact]
    public async Task ADailyCollectorWithNoRunTime_ThatKeepsFailing_RunsOncePerDay_NotOnEverySweep()
    {
        var (duckDb, servers, server) = await OpenAsync();
        try
        {
            var t0 = new DateTime(2026, 7, 15, 9, 0, 0, DateTimeKind.Utc);

            /* A made-up collector fails on every attempt and writes one ERROR row per attempt. The scheduler counted only a
               success for a daily collector with no run time, so a failing one (a missing permission, say) ran on every
               sweep of the day, while the startup read of the log had always counted that row as the run. */
            var scheduler = NewScheduler(server, ("no_such_failing_daily", 1440, null));
            var service = new RemoteCollectorService(duckDb, servers, scheduler);

            foreach (var minutes in new[] { 0, 1, 2, 3, 10, 30, 59, 120, 600 })
            {
                await service.RunDueCollectorsAsync(t0 + TimeSpan.FromMinutes(minutes), CancellationToken.None);
            }

            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_failing_daily"));

            /* A day after the attempt it is due again, runs once more, fails once more, and ends that day too. */
            await service.RunDueCollectorsAsync(t0 + TimeSpan.FromDays(1) - TimeSpan.FromMinutes(1), CancellationToken.None);
            Assert.Equal(1, await RowsAsync(duckDb, server, "no_such_failing_daily"));

            foreach (var minutes in new[] { 0, 5, 45 })
            {
                await service.RunDueCollectorsAsync(t0 + TimeSpan.FromDays(1) + TimeSpan.FromMinutes(minutes), CancellationToken.None);
            }

            Assert.Equal(2, await RowsAsync(duckDb, server, "no_such_failing_daily"));
        }
        finally
        {
            duckDb.Dispose();
        }
    }

    [Fact]
    public async Task AFailingCollectorThatRunsEveryFifteenMinutes_StillTriesAgainOnTheNextSweep()
    {
        var (duckDb, servers, server) = await OpenAsync();
        try
        {
            var t0 = new DateTime(2026, 7, 15, 9, 0, 0, DateTimeKind.Utc);

            /* A shorter interval keeps the old rule: only a success counts, so a failing collector runs on every sweep. */
            var scheduler = NewScheduler(server, ("no_such_failing_frequent", 15, null));
            var service = new RemoteCollectorService(duckDb, servers, scheduler);

            foreach (var minutes in new[] { 0, 1, 2, 3 })
            {
                await service.RunDueCollectorsAsync(t0 + TimeSpan.FromMinutes(minutes), CancellationToken.None);
            }

            Assert.Equal(4, await RowsAsync(duckDb, server, "no_such_failing_frequent"));
        }
        finally
        {
            duckDb.Dispose();
        }
    }
}
