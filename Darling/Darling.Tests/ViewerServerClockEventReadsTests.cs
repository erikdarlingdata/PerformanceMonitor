/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: System Events (Default Trace) no longer subtracts the latest offset in SQL. The stored server-local
/// event time is converted in C# with the server's <see cref="ServerClock"/>, so an event on either side of a
/// daylight-saving change lands at its real UTC time; the SQL pre-filters an hour wide and the window is applied
/// exactly after the conversion. US Eastern springs forward on 2026-03-08 (07:00Z) and falls back on
/// 2026-11-01 (06:00Z). The reader is fed a DataTable in the SELECT's column order, as
/// <see cref="ViewerJobHistoryServerClockTests"/> does.
/// </summary>
/* Serialized with the classes that flip the process-wide ViewerTimeHelper statics: a row's constructor formats
   its display time through them. */
[Collection("viewer-time-statics")]
public sealed class ViewerDefaultTraceServerClockTests
{
    private const string EasternWindowsId = "Eastern Standard Time";

    private static DateTime Naive(int year, int month, int day, int hour, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    /* The clock the viewer builds for an Eastern server whose newest snapshot said -300 (or -240 in summer). */
    private static ServerClock Eastern(int snapshotOffsetMinutes = -300) => ServerClock.Resolve(EasternWindowsId, snapshotOffsetMinutes);

    private static DataTable NewTable()
    {
        var table = new DataTable();
        table.Columns.Add("event_time_local", typeof(DateTime));
        table.Columns.Add("event_name", typeof(string));
        table.Columns.Add("database_name", typeof(string));
        table.Columns.Add("object_name", typeof(string));
        table.Columns.Add("login_name", typeof(string));
        table.Columns.Add("host_name", typeof(string));
        table.Columns.Add("application_name", typeof(string));
        table.Columns.Add("spid", typeof(int));
        table.Columns.Add("duration_us", typeof(long));
        table.Columns.Add("integer_data", typeof(long));
        table.Columns.Add("severity", typeof(int));
        table.Columns.Add("error_number", typeof(int));
        table.Columns.Add("text_data", typeof(string));
        return table;
    }

    /// <summary>One stored event. <paramref name="label"/> rides in database_name so a test can say which
    /// event came back; the default name is a Default Trace category that is significant as collected.</summary>
    private static void AddEvent(DataTable table, DateTime? local, string label,
        string eventName = "Data File Auto Grow", int? severity = null)
    {
        table.Rows.Add(
            local.HasValue ? (object)local.Value : DBNull.Value,
            eventName, label, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
            55, 1_500_000L, 256L,
            severity.HasValue ? (object)severity.Value : DBNull.Value,
            DBNull.Value, DBNull.Value);
    }

    private static async Task<List<DefaultTraceEventRow>> ReadAsync(DataTable table, ServerClock clock, DateTime startUtc, DateTime endUtc)
    {
        using var reader = table.CreateDataReader();
        return await ViewerDataService.ReadDefaultTraceEventsAsync(reader, clock, startUtc, endUtc, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ADefaultTraceEventStoredAtServerLocalTime_OnEachSideOfAChange_LandsAtTheRightUtcTime()
    {
        var table = NewTable();
        AddEvent(table, Naive(2026, 3, 7, 8), "before-spring");
        AddEvent(table, Naive(2026, 3, 9, 8), "after-spring");
        AddEvent(table, Naive(2026, 10, 31, 8), "before-fall");
        AddEvent(table, Naive(2026, 11, 2, 8), "after-fall");

        var rows = await ReadAsync(table, Eastern(), Naive(2026, 1, 1, 0), Naive(2026, 12, 31, 0));

        /* Standard time (-5 h), daylight time (-4 h), daylight time, standard time. One subtracted offset puts
           two of these four an hour off, whichever offset the latest snapshot happened to carry. */
        var byLabel = rows.ToDictionary(r => r.DatabaseName!, r => r.EventTimeUtc);
        Assert.Equal(Naive(2026, 3, 7, 13), byLabel["before-spring"]);
        Assert.Equal(Naive(2026, 3, 9, 12), byLabel["after-spring"]);
        Assert.Equal(Naive(2026, 10, 31, 12), byLabel["before-fall"]);
        Assert.Equal(Naive(2026, 11, 2, 13), byLabel["after-fall"]);
    }

    [Fact]
    public async Task ADefaultTraceEventInASkippedOrRepeatedLocalHour_DoesNotThrow()
    {
        var table = NewTable();
        AddEvent(table, Naive(2026, 3, 8, 2, 30), "skipped");
        AddEvent(table, Naive(2026, 11, 1, 1, 30), "repeated");

        var rows = await ReadAsync(table, Eastern(), Naive(2026, 1, 1, 0), Naive(2026, 12, 31, 0));

        var byLabel = rows.ToDictionary(r => r.DatabaseName!, r => r.EventTimeUtc);
        Assert.Equal(Naive(2026, 3, 8, 7, 30), byLabel["skipped"]);
        Assert.Equal(Naive(2026, 11, 1, 5, 30), byLabel["repeated"]);
    }

    [Fact]
    public async Task AWindowEdgeWithinAnHourOfTheFallBack_KeepsExactlyTheEventsInsideIt()
    {
        /* The window is 01:30 EDT to 02:30 EST: 05:30Z to 07:30Z, across the 06:00Z change. */
        var start = Naive(2026, 11, 1, 5, 30);
        var end = Naive(2026, 11, 1, 7, 30);

        var table = NewTable();
        AddEvent(table, Naive(2026, 11, 1, 0, 59), "00:59");   /* 04:59Z: before the window */
        AddEvent(table, Naive(2026, 11, 1, 1, 15), "01:15");   /* repeated hour, first occurrence: 05:15Z, before */
        AddEvent(table, Naive(2026, 11, 1, 1, 45), "01:45");   /* repeated hour, first occurrence: 05:45Z, inside */
        AddEvent(table, Naive(2026, 11, 1, 2, 10), "02:10");   /* standard time only: 07:10Z, inside */
        AddEvent(table, Naive(2026, 11, 1, 2, 40), "02:40");   /* 07:40Z: after the window */

        var rows = await ReadAsync(table, Eastern(), start, end);

        /* One subtracted -300 would also keep 00:59 (05:59Z) and 01:15 (06:15Z). */
        Assert.Equal(new[] { "02:10", "01:45" }, rows.Select(r => r.DatabaseName).ToArray());
        Assert.Equal(new DateTime?[] { Naive(2026, 11, 1, 7, 10), Naive(2026, 11, 1, 5, 45) }, rows.Select(r => r.EventTimeUtc).ToArray());
    }

    [Fact]
    public async Task AWindowEdgeWithinAnHourOfTheSpringForward_KeepsExactlyTheEventsInsideIt()
    {
        /* The window is 01:30 EST to 04:30 EDT: 06:30Z to 08:30Z, across the 07:00Z change. */
        var start = Naive(2026, 3, 8, 6, 30);
        var end = Naive(2026, 3, 8, 8, 30);

        var table = NewTable();
        AddEvent(table, Naive(2026, 3, 8, 1, 10), "01:10");    /* 06:10Z: before the window */
        AddEvent(table, Naive(2026, 3, 8, 1, 59), "01:59");    /* 06:59Z: inside */
        AddEvent(table, Naive(2026, 3, 8, 3, 5), "03:05");     /* daylight time: 07:05Z, inside */
        AddEvent(table, Naive(2026, 3, 8, 4, 29), "04:29");    /* 08:29Z: inside, a minute from the edge */
        AddEvent(table, Naive(2026, 3, 8, 4, 45), "04:45");    /* 08:45Z: after the window */

        /* The newest snapshot may carry either side's offset; the zone decides, so both give the same rows. */
        foreach (var snapshotOffset in new[] { -300, -240 })
        {
            var rows = await ReadAsync(table, Eastern(snapshotOffset), start, end);

            Assert.Equal(new[] { "04:29", "03:05", "01:59" }, rows.Select(r => r.DatabaseName).ToArray());
            Assert.Equal(
                new DateTime?[] { Naive(2026, 3, 8, 8, 29), Naive(2026, 3, 8, 7, 5), Naive(2026, 3, 8, 6, 59) },
                rows.Select(r => r.EventTimeUtc).ToArray());
        }
    }

    [Fact]
    public async Task TheWindowEdges_AreInclusive_AndTheSqlPreFilterMarginIsTrimmed()
    {
        var start = Naive(2026, 11, 2, 13);
        var end = Naive(2026, 11, 2, 15);

        var table = NewTable();
        AddEvent(table, Naive(2026, 11, 2, 7, 59), "just-before");  /* 12:59Z: in the SQL's extra hour, out here */
        AddEvent(table, Naive(2026, 11, 2, 8), "at-start");         /* 13:00Z */
        AddEvent(table, Naive(2026, 11, 2, 10), "at-end");          /* 15:00Z */
        AddEvent(table, Naive(2026, 11, 2, 10, 1), "just-after");   /* 15:01Z: in the SQL's extra hour, out here */

        var rows = await ReadAsync(table, Eastern(), start, end);

        Assert.Equal(new[] { "at-end", "at-start" }, rows.Select(r => r.DatabaseName).ToArray());
    }

    [Fact]
    public async Task TheRows_ComeBackNewestFirst_ByRealUtcTime_AndTiesKeepTheReadersOrder()
    {
        var table = NewTable();
        /* Handed over oldest first, with two events at the same instant between them. */
        AddEvent(table, Naive(2026, 11, 2, 8), "oldest");
        AddEvent(table, Naive(2026, 11, 2, 9), "tie-first");
        AddEvent(table, Naive(2026, 11, 2, 9), "tie-second");
        AddEvent(table, Naive(2026, 11, 2, 9), "tie-third");
        AddEvent(table, Naive(2026, 11, 2, 10), "newest");

        var rows = await ReadAsync(table, Eastern(), Naive(2026, 11, 2, 0), Naive(2026, 11, 3, 0));

        Assert.Equal(
            new[] { "newest", "tie-first", "tie-second", "tie-third", "oldest" },
            rows.Select(r => r.DatabaseName).ToArray());
    }

    [Fact]
    public async Task AnEventWithNoTime_OrBelowTheSignificanceGate_IsDropped()
    {
        var table = NewTable();
        AddEvent(table, Naive(2026, 11, 2, 8), "significant");
        AddEvent(table, null, "no-time");
        AddEvent(table, Naive(2026, 11, 2, 8), "low-severity-errorlog", eventName: "ErrorLog", severity: 10);
        AddEvent(table, Naive(2026, 11, 2, 8), "severe-errorlog", eventName: "ErrorLog", severity: 20);

        var rows = await ReadAsync(table, Eastern(), Naive(2026, 11, 2, 0), Naive(2026, 11, 3, 0));

        Assert.Equal(new[] { "significant", "severe-errorlog" }, rows.Select(r => r.DatabaseName).ToArray());
    }

    [Fact]
    public async Task AServerWithAFixedOffsetOrNoClock_ConvertsAsBefore()
    {
        var table = NewTable();
        AddEvent(table, Naive(2026, 10, 31, 8), "event");
        var start = Naive(2026, 1, 1, 0);
        var end = Naive(2026, 12, 31, 0);

        /* No time zone id (a SQL Server before 2022): the stored offset is applied as a fixed shift. */
        var fixedRows = await ReadAsync(table, ServerClock.Resolve(null, -300), start, end);
        Assert.Equal(Naive(2026, 10, 31, 13), Assert.Single(fixedRows).EventTimeUtc);

        /* No server_properties row: the stored time is read as UTC (the old COALESCE(..., 0)). */
        var utcRows = await ReadAsync(table, ServerClock.Utc, start, end);
        Assert.Equal(Naive(2026, 10, 31, 8), Assert.Single(utcRows).EventTimeUtc);
    }

    [Fact]
    public async Task TheRowKeepsItsOtherColumns()
    {
        var table = NewTable();
        table.Rows.Add(Naive(2026, 11, 2, 8), "Data File Auto Grow", "SalesDB", "obj", "sa", "HOST", "app",
            55, 1_500_000L, 256L, DBNull.Value, 5, "text");

        var row = Assert.Single(await ReadAsync(table, Eastern(), Naive(2026, 11, 2, 0), Naive(2026, 11, 3, 0)));

        Assert.Equal("SalesDB", row.DatabaseName);
        Assert.Equal("obj", row.ObjectName);
        Assert.Equal("sa", row.LoginName);
        Assert.Equal("HOST", row.HostName);
        Assert.Equal("app", row.ApplicationName);
        Assert.Equal(55, row.Spid);
        Assert.Equal(1500m, row.DurationMs);
        Assert.Equal(2m, row.GrowthMb);
        Assert.Equal(5, row.ErrorNumber);
        Assert.Equal("text", row.TextData);
    }
}

/// <summary>
/// #4766: the SQL Agent status header no longer subtracts the latest offset in SQL. The stored server-local next
/// scheduled run is converted in C# with that row's server clock, so a next run on the far side of a
/// daylight-saving change lands at its real UTC time. The SQL text is pinned here too, because the conversion
/// only holds while the SELECT returns the run raw.
/// </summary>
public sealed class ViewerAgentStatusServerClockTests
{
    private const string EasternWindowsId = "Eastern Standard Time";

    private static DateTime Naive(int year, int month, int day, int hour, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    private static DataTable NewTable()
    {
        var table = new DataTable();
        table.Columns.Add("server_id", typeof(int));
        table.Columns.Add("server_name", typeof(string));
        table.Columns.Add("agent_running", typeof(bool));
        table.Columns.Add("agent_status_desc", typeof(string));
        table.Columns.Add("agent_startup_desc", typeof(string));
        table.Columns.Add("next_scheduled_run_local", typeof(DateTime));
        return table;
    }

    private static void AddRow(DataTable table, int serverId, DateTime? nextRunLocal) =>
        table.Rows.Add(serverId, "srv" + serverId, true, "Running", "Auto",
            nextRunLocal.HasValue ? (object)nextRunLocal.Value : DBNull.Value);

    private static async Task<List<ViewerAgentStatusRow>> ReadAsync(DataTable table, IReadOnlyDictionary<int, ServerClock> clocks)
    {
        using var reader = table.CreateDataReader();
        return await ViewerDataService.ReadAgentStatusRowsAsync(reader, clocks, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ANextRunOnTheFarSideOfAChange_ConvertsToItsRealUtcTime()
    {
        /* The newest snapshot of each server was taken on the other side of the change from its next run. */
        var clocks = new Dictionary<int, ServerClock>
        {
            [1] = ServerClock.Resolve(EasternWindowsId, -240),   /* snapshot in summer, next run in winter */
            [2] = ServerClock.Resolve(EasternWindowsId, -300),   /* snapshot in winter, next run in summer */
        };

        var table = NewTable();
        AddRow(table, 1, Naive(2026, 11, 2, 8));
        AddRow(table, 2, Naive(2026, 3, 9, 8));

        var rows = await ReadAsync(table, clocks);

        /* Standard time (-5 h) then daylight time (-4 h). One subtracted snapshot offset is an hour off on both. */
        Assert.Equal(Naive(2026, 11, 2, 13), rows.Single(r => r.ServerId == 1).NextScheduledRunUtc);
        Assert.Equal(Naive(2026, 3, 9, 12), rows.Single(r => r.ServerId == 2).NextScheduledRunUtc);
    }

    [Fact]
    public async Task EachRow_UsesItsOwnServersClock_AndAServerWithNoClockIsReadAsUtc()
    {
        var clocks = new Dictionary<int, ServerClock>
        {
            [1] = ServerClock.Resolve(EasternWindowsId, -300),
            [3] = ServerClock.FixedOffset(120),
        };

        var table = NewTable();
        AddRow(table, 1, Naive(2026, 11, 2, 8));
        AddRow(table, 2, Naive(2026, 11, 2, 8));   /* no server_properties row */
        AddRow(table, 3, Naive(2026, 11, 2, 8));   /* fixed offset: two hours ahead */
        AddRow(table, 4, null);                    /* nothing scheduled */

        var rows = await ReadAsync(table, clocks);

        Assert.Equal(Naive(2026, 11, 2, 13), rows.Single(r => r.ServerId == 1).NextScheduledRunUtc);
        Assert.Equal(Naive(2026, 11, 2, 8), rows.Single(r => r.ServerId == 2).NextScheduledRunUtc);
        Assert.Equal(Naive(2026, 11, 2, 6), rows.Single(r => r.ServerId == 3).NextScheduledRunUtc);
        Assert.Null(rows.Single(r => r.ServerId == 4).NextScheduledRunUtc);
        Assert.Equal("None scheduled", rows.Single(r => r.ServerId == 4).NextScheduledRunLocal);
    }

    [Fact]
    public async Task ANextRunInASkippedOrRepeatedLocalHour_DoesNotThrow()
    {
        var clocks = new Dictionary<int, ServerClock> { [1] = ServerClock.Resolve(EasternWindowsId, -300) };

        var table = NewTable();
        AddRow(table, 1, Naive(2026, 3, 8, 2, 30));

        Assert.Equal(Naive(2026, 3, 8, 7, 30), Assert.Single(await ReadAsync(table, clocks)).NextScheduledRunUtc);

        var repeated = NewTable();
        AddRow(repeated, 1, Naive(2026, 11, 1, 1, 30));

        Assert.Equal(Naive(2026, 11, 1, 5, 30), Assert.Single(await ReadAsync(repeated, clocks)).NextScheduledRunUtc);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TheSql_ReturnsTheNextRunRaw_AndNoLongerSubtractsTheOffset(bool scoped)
    {
        var sql = ViewerDataService.BuildAgentStatusSql(scoped);

        Assert.Contains("a.next_scheduled_run AS next_scheduled_run_local", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("make_interval", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("utc_offset_minutes", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("next_scheduled_run_utc", sql, StringComparison.Ordinal);
        Assert.Contains("ROW_NUMBER() OVER (PARTITION BY a.server_id ORDER BY a.collection_time DESC) AS rn", sql, StringComparison.Ordinal);
        Assert.Equal(scoped, sql.Contains("WHERE a.server_id = $1", StringComparison.Ordinal));
    }
}
