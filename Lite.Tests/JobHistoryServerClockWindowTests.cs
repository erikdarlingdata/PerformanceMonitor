/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4966: <see cref="LocalDataService.GetJobHistoryAsync"/> windows on <c>run_datetime</c>, the monitored server's LOCAL
/// wall clock, so the window has to be worked out on THAT server's clock. It used to compare the stored wall clock with
/// the host's (<c>DateTime.Now.AddHours(-hoursBack)</c>), which is right only when the server lives in the host's zone:
/// a server 5 hours ahead kept a run from hours before the window, and a server 5 hours behind dropped a run from
/// inside it. Each test names its server's offset RELATIVE TO THE HOST'S, so the same shift happens on a UTC CI
/// runner and on a developer machine in any zone. The grid still shows the stored wall clock (the time SSMS shows), so
/// every test also checks <see cref="JobHistoryRow.RunDateTime"/> comes back as stored.
/// </summary>
public sealed class JobHistoryServerClockWindowTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerA = 601;
    private const int ServerB = 602;
    private const int ServerNoClock = 603;
    private const int WindowHours = 2;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public JobHistoryServerClockWindowTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>This machine's UTC offset now, in minutes: what a server "on the host's own zone" reports.</summary>
    private static int HostOffsetMinutes() =>
        (int)TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).TotalMinutes;

    /// <summary>The server's wall clock at <paramref name="utc"/> for a fixed offset, at whole seconds, the way
    /// <c>sysjobhistory</c> stores it (run_date / run_time carry no sub-second part).</summary>
    private static DateTime Wall(DateTime utc, int offsetMinutes)
    {
        var t = utc.AddMinutes(offsetMinutes);
        return new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    /// <summary>One collected clock for the server: a fixed offset, no zone id (a SQL Server before 2022).</summary>
    private async Task SeedClockAsync(int serverId, int offsetMinutes, string? timeZoneId = null)
    {
        var connection = await SeedConnectionAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name,
             edition, product_version, product_level, engine_edition,
             cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id)
            VALUES ($1, $2, $3, 'TestSrv', 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $4, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = -_nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = offsetMinutes });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)timeZoneId ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One successful step-0 outcome whose <c>run_datetime</c> is <paramref name="wall"/>, the server's own clock.</summary>
    private async Task InsertRunAsync(int serverId, string jobId, DateTime wall)
    {
        var connection = await SeedConnectionAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO job_history
    (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name,
     job_enabled, category_name, step_id, step_name, run_status, run_status_desc, run_datetime,
     run_duration_seconds, retries_attempted, message)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, TRUE, 'Uncategorized (Local)', 0, '(Job outcome)', 1, 'The job succeeded.', $8, 30, 0, NULL)";
        var id = _nextId++;
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"S{serverId}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = jobId });
        cmd.Parameters.Add(new DuckDBParameter { Value = jobId });
        cmd.Parameters.Add(new DuckDBParameter { Value = wall });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// A server 5 hours off the host, either way. The run 30 minutes ago is inside a 2-hour window and the run 3 hours ago is
    /// outside it, on the server's own clock. Against the host's clock the server 5 hours ahead kept BOTH (its 3-hours-ago run
    /// reads as 2 hours in the host's future) and the server 5 hours behind dropped BOTH (its 30-minutes-ago run reads as
    /// 5.5 hours old). The stored wall clock comes back as stored.
    /// </summary>
    [Theory]
    [InlineData(300)]
    [InlineData(-300)]
    public async Task ServerFiveHoursOffTheHost_WindowsOnItsOwnClock(int hoursOffTheHostMinutes)
    {
        var offset = HostOffsetMinutes() + hoursOffTheHostMinutes;
        await SeedClockAsync(ServerA, offset);
        var now = DateTime.UtcNow;
        var inside = Wall(now.AddMinutes(-30), offset);
        await InsertRunAsync(ServerA, "inside_job", inside);
        await InsertRunAsync(ServerA, "outside_job", Wall(now.AddHours(-3), offset));

        var rows = await new LocalDataService(_duckDb).GetJobHistoryAsync(hoursBack: WindowHours, limit: 100, serverId: ServerA);

        var row = Assert.Single(rows);
        Assert.Equal("inside_job", row.JobId);
        Assert.Equal(inside, row.RunDateTime);
    }

    /// <summary>The control: a server on the host's own zone windows exactly as the read always did, and keeps its newest-first order.</summary>
    [Fact]
    public async Task ServerOnTheHostsOwnZone_ReturnsTheSameRunsAsBefore()
    {
        var offset = HostOffsetMinutes();
        await SeedClockAsync(ServerA, offset);
        var now = DateTime.UtcNow;
        await InsertRunAsync(ServerA, "older_inside", Wall(now.AddMinutes(-90), offset));
        await InsertRunAsync(ServerA, "newer_inside", Wall(now.AddMinutes(-30), offset));
        await InsertRunAsync(ServerA, "outside_job", Wall(now.AddHours(-3), offset));

        var scoped = await new LocalDataService(_duckDb).GetJobHistoryAsync(hoursBack: WindowHours, limit: 100, serverId: ServerA);
        var fleet = await new LocalDataService(_duckDb).GetJobHistoryAsync(hoursBack: WindowHours, limit: 100, serverId: null);

        Assert.Equal(["newer_inside", "older_inside"], scoped.Select(r => r.JobId).ToArray());
        Assert.Equal(["newer_inside", "older_inside"], fleet.Select(r => r.JobId).ToArray());
        Assert.Equal(Wall(now.AddMinutes(-30), offset), scoped[0].LastSuccessfulRun);
    }

    /// <summary>
    /// The all-servers view, two servers in different zones: each is windowed on its own clock, and the rows come back newest
    /// first by the real instant of each run, not by the wall clock its server stored. Server A is 5 hours ahead of the host and
    /// server B 5 hours behind; A's last run was 40 minutes ago and B's 20 minutes ago, so B's is the newest although A's
    /// stored wall clock is 10 hours later than B's. Each server's run from 3 hours ago is outside the window.
    /// </summary>
    [Fact]
    public async Task AllServers_WindowEachServerOnItsOwnClock_AndOrderByTheRealInstant()
    {
        var offsetA = HostOffsetMinutes() + 300;
        var offsetB = HostOffsetMinutes() - 300;
        await SeedClockAsync(ServerA, offsetA);
        await SeedClockAsync(ServerB, offsetB);
        var now = DateTime.UtcNow;
        await InsertRunAsync(ServerA, "a_inside", Wall(now.AddMinutes(-40), offsetA));
        await InsertRunAsync(ServerA, "a_outside", Wall(now.AddHours(-3), offsetA));
        await InsertRunAsync(ServerB, "b_inside", Wall(now.AddMinutes(-20), offsetB));
        await InsertRunAsync(ServerB, "b_outside", Wall(now.AddHours(-3), offsetB));

        var rows = await new LocalDataService(_duckDb).GetJobHistoryAsync(hoursBack: WindowHours, limit: 100, serverId: null);

        Assert.Equal(["b_inside", "a_inside"], rows.Select(r => r.JobId).ToArray());
        Assert.Equal(Wall(now.AddMinutes(-20), offsetB), rows[0].RunDateTime);
        Assert.Equal(Wall(now.AddMinutes(-40), offsetA), rows[1].RunDateTime);
    }

    /// <summary>The cap takes the newest runs by real instant: with room for one, B's run from 20 minutes ago beats A's from 40
    /// even though A's stored wall clock is the later one.</summary>
    [Fact]
    public async Task AllServers_TheCapKeepsTheNewestRunByRealInstant()
    {
        var offsetA = HostOffsetMinutes() + 300;
        var offsetB = HostOffsetMinutes() - 300;
        await SeedClockAsync(ServerA, offsetA);
        await SeedClockAsync(ServerB, offsetB);
        var now = DateTime.UtcNow;
        await InsertRunAsync(ServerA, "a_inside", Wall(now.AddMinutes(-40), offsetA));
        await InsertRunAsync(ServerB, "b_inside", Wall(now.AddMinutes(-20), offsetB));

        var rows = await new LocalDataService(_duckDb).GetJobHistoryAsync(hoursBack: WindowHours, limit: 1, serverId: null);

        Assert.Equal("b_inside", Assert.Single(rows).JobId);
    }

    /// <summary>
    /// A server with no collected clock yet (its server_properties row has not landed) is windowed on the machine's clock, which
    /// is what the read did for every server before the window followed the server, and what the Alert History tab ends on when
    /// a server has neither a collected clock nor an open tab. The viewer reads such a server's times as UTC instead, because its
    /// store reads an uncollected offset as 0. Another server's clock is not borrowed.
    /// </summary>
    [Fact]
    public async Task ServerWithNoCollectedClock_IsWindowedOnTheMachinesClock()
    {
        /* A fixed machine zone that is not UTC (+05:30), named through the service's seam: a fallback to UTC fails this on any host. */
        var machine = TimeZoneInfo.CreateCustomTimeZone("test+0530", TimeSpan.FromMinutes(330), "test+0530", "test+0530");
        var host = 330;
        var offsetB = host - 300;
        await SeedClockAsync(ServerB, offsetB);
        var now = DateTime.UtcNow;
        await InsertRunAsync(ServerNoClock, "no_clock_inside", Wall(now.AddMinutes(-30), host));
        await InsertRunAsync(ServerNoClock, "no_clock_outside", Wall(now.AddHours(-3), host));
        await InsertRunAsync(ServerB, "b_inside", Wall(now.AddMinutes(-20), offsetB));

        var service = new LocalDataService(_duckDb) { MachineZone = machine };
        var scoped = await service.GetJobHistoryAsync(hoursBack: WindowHours, limit: 100, serverId: ServerNoClock);
        var fleet = await service.GetJobHistoryAsync(hoursBack: WindowHours, limit: 100, serverId: null);

        Assert.Equal("no_clock_inside", Assert.Single(scoped).JobId);
        Assert.Equal(["b_inside", "no_clock_inside"], fleet.Select(r => r.JobId).ToArray());
    }

    /// <summary>
    /// #4966: the link between a server's collected clock and the machine's. A server with no collected clock yet, whose open tab keeps
    /// the fixed offset its connect probe read (here 5 hours off the machine), is windowed on that tab's clock, the chain the Alert
    /// History tab uses. The tab layer hands the open tabs' clocks in as a plain dictionary (the open tabs are UI objects). Without the
    /// link the server fell straight to the machine's clock and kept both runs.
    /// </summary>
    [Fact]
    public async Task ServerWithNoCollectedClock_AndAnOpenTabFiveHoursOffTheMachine_IsWindowedOnTheTabsClock()
    {
        var tabOffset = HostOffsetMinutes() + 300;
        var now = DateTime.UtcNow;
        await InsertRunAsync(ServerNoClock, "inside_job", Wall(now.AddMinutes(-30), tabOffset));
        await InsertRunAsync(ServerNoClock, "outside_job", Wall(now.AddHours(-3), tabOffset));
        var tabs = new Dictionary<int, ServerClock> { [ServerNoClock] = ServerClock.FixedOffset(tabOffset) };

        var rows = await new LocalDataService(_duckDb).GetJobHistoryAsync(now.AddHours(-WindowHours), 100, ServerNoClock, tabs);

        Assert.Equal("inside_job", Assert.Single(rows).JobId);
    }

    /// <summary>The chain's order: a collected clock beats the open tab's, and another server's tab clock is never borrowed.</summary>
    [Fact]
    public async Task ACollectedClock_BeatsTheOpenTabsClock_AndAnotherServersTabIsNotBorrowed()
    {
        var host = HostOffsetMinutes();
        await SeedClockAsync(ServerA, host);
        var now = DateTime.UtcNow;
        await InsertRunAsync(ServerA, "a_inside", Wall(now.AddMinutes(-30), host));
        await InsertRunAsync(ServerA, "a_outside", Wall(now.AddHours(-3), host));
        await InsertRunAsync(ServerNoClock, "no_clock_inside", Wall(now.AddMinutes(-30), host));
        await InsertRunAsync(ServerNoClock, "no_clock_outside", Wall(now.AddHours(-3), host));
        var tabs = new Dictionary<int, ServerClock>
        {
            [ServerA] = ServerClock.FixedOffset(host + 300),
            [ServerB] = ServerClock.FixedOffset(host - 300)
        };

        var rows = await new LocalDataService(_duckDb).GetJobHistoryAsync(now.AddHours(-WindowHours), 100, null, tabs);

        Assert.Equal(["a_inside", "no_clock_inside"], rows.Select(r => r.JobId).OrderBy(j => j, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// Daylight saving, pinned (#4966): a server whose collected clock is a zone with daylight saving. One window starts in January
    /// (12:00 Eastern Standard) and one in July (12:00 Eastern Daylight); each has a run 15 minutes after its start (kept) and one 15
    /// minutes before it (dropped). The read converts each run with the clock at the run's own date, so both seasons come out right;
    /// any single offset (the one in force today included) gets one of them wrong.
    /// </summary>
    [Theory]
    [InlineData(2026, 1, 15, 17)]
    [InlineData(2026, 7, 15, 16)]
    public async Task AWindowStartingInWinterOrSummer_ConvertsEachRunWithTheClockAtItsOwnDate(int year, int month, int day, int startUtcHour)
    {
        await SeedClockAsync(ServerA, -300, "Eastern Standard Time");
        var startUtc = new DateTime(year, month, day, startUtcHour, 0, 0, DateTimeKind.Utc);
        var wallStart = new DateTime(year, month, day, 12, 0, 0, DateTimeKind.Unspecified);
        await InsertRunAsync(ServerA, "kept", wallStart.AddMinutes(15));
        await InsertRunAsync(ServerA, "dropped", wallStart.AddMinutes(-15));

        var rows = await new LocalDataService(_duckDb).GetJobHistoryAsync(startUtc, 100, ServerA);

        Assert.Equal("kept", Assert.Single(rows).JobId);
    }
}
