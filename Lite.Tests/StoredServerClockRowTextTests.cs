/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: a time read from a STORED server wall-clock column prints as the plain wall time in the repeated autumn
/// hour, never with a UTC offset.
///
/// <para>Text that shows one instant appends the instant's offset there (<c>ServerTimeHelper.FormatServerTime</c>), and
/// that is only true for a real UTC instant. A Default Trace event comes in as the server's own wall clock and goes to
/// UTC through <see cref="ServerClock.ToUtc"/>, which maps BOTH passes of a repeated local time to the first ("A stored
/// server-local time cannot say which one it was"). Formatted with the offset, the second pass would print the first
/// pass's: an event at 01:30 the second time (06:30Z, -05:00) on a US Eastern server printed "-04:00". US Eastern falls
/// back on 2026-11-01 at 06:00Z. The Darling viewer's twin is <c>StoredServerClockRowTextTests</c>.</para>
/// </summary>
/* Installs ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics; joins the
   collection every other class that writes them uses. */
[Collection("server-time-helper")]
public sealed class StoredServerClockRowTextTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 8812;
    private const string EasternZone = "Eastern Standard Time";

    private readonly DuckDbInitializer _duckDb;
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public StoredServerClockRowTextTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        ServerTimeHelper.ActiveServerClock = Eastern();
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
    }

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
        _seedConn?.Dispose();
    }

    private static DateTime At(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    private static ServerClock Eastern() => ServerClock.Resolve(EasternZone, -300);

    /// <summary>A system_health row: its event time is the XE <c>@timestamp</c>, a real UTC instant.</summary>
    private static string TrueUtcRowText(DateTime naiveUtc) =>
        new SchedulerIssueRow(new SchedulerIssueRecord { EventTime = naiveUtc }).EventTimeLocal;

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task SeedDefaultTraceAsync(DateTime eventTimeServerLocal, string text)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name,
     database_name, duration_us, integer_data, severity, error_number, text_data)
VALUES ($1, $2, $3, 'TestSrv', $4, 'Server Memory Change', NULL, NULL, NULL, NULL, NULL, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTimeServerLocal });
        cmd.Parameters.Add(new DuckDBParameter { Value = text });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task ADefaultTraceEventReadFromTheStoredWallClock_PrintsNoOffset_InBothPassesOfTheRepeatedHour_WhileARealInstantStillDoes()
    {
        var service = new LocalDataService(_duckDb);

        /* Two events, one in each pass of the hour: the trace file stores 01:30 for both. */
        await SeedDefaultTraceAsync(At(2026, 11, 1, 1, 30), "first-pass");
        await SeedDefaultTraceAsync(At(2026, 11, 1, 1, 30), "second-pass");
        await SeedDefaultTraceAsync(At(2026, 11, 1, 2, 30), "after");

        var rows = await service.GetDefaultTraceEventsAsync(
            ServerId, fromDate: At(2026, 11, 1, 0, 0), toDate: At(2026, 11, 1, 12, 0), serverClock: Eastern());

        Assert.Equal(3, rows.Count);
        Assert.Equal(At(2026, 11, 1, 5, 30), rows.Single(r => r.TextData == "first-pass").EventTimeUtc);
        Assert.Equal(At(2026, 11, 1, 5, 30), rows.Single(r => r.TextData == "second-pass").EventTimeUtc);

        Assert.Equal("2026-11-01 01:30:00", rows.Single(r => r.TextData == "first-pass").EventTimeLocal);
        Assert.Equal("2026-11-01 01:30:00", rows.Single(r => r.TextData == "second-pass").EventTimeLocal);
        Assert.Equal("2026-11-01 02:30:00", rows.Single(r => r.TextData == "after").EventTimeLocal);

        /* A real instant in the same test keeps the offset: the two passes of 01:30 are 05:30Z and 06:30Z. */
        Assert.Equal("2026-11-01 01:30:00 -04:00", TrueUtcRowText(At(2026, 11, 1, 5, 30)));
        Assert.Equal("2026-11-01 01:30:00 -05:00", TrueUtcRowText(At(2026, 11, 1, 6, 30)));
        Assert.Equal("", new DefaultTraceEventRow(
            null, DefaultTraceEventCategory.ErrorLog, "ErrorLog", null, null, null, null, null, null, null, null, 20, 823, "x").EventTimeLocal);
    }

    [Fact]
    public void TheStoredWallClockRenderer_IsTheFormatServerTimeText_AndTheRealInstantRendererDiffersFromItOnlyInTheRepeatedHour()
    {
        foreach (var instant in new[] { At(2026, 11, 1, 4, 59), At(2026, 11, 1, 7, 0), At(2026, 7, 6, 16, 0) })
        {
            Assert.Equal(SystemEventRowFormat.Local(instant), SystemEventRowFormat.StoredWallClock(instant));
        }

        var secondPass = At(2026, 11, 1, 6, 30);
        Assert.Equal("2026-11-01 01:30:00", SystemEventRowFormat.StoredWallClock(secondPass));
        Assert.Equal("2026-11-01 01:30:00 -05:00", SystemEventRowFormat.Local(secondPass));
        Assert.Equal("", SystemEventRowFormat.StoredWallClock(null));
    }

    [Theory]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    public void TheStoredWallClockRenderer_FollowsTheDisplayMode_LikeTheRealInstantRenderer(TimeDisplayMode mode)
    {
        ServerTimeHelper.CurrentDisplayMode = mode;

        /* Outside the repeated hour on any machine zone the two renderers word the instant the same way; UTC mode
           has no repeated hour at all, so it never differs. */
        Assert.Equal(SystemEventRowFormat.Local(At(2026, 7, 6, 16, 0)), SystemEventRowFormat.StoredWallClock(At(2026, 7, 6, 16, 0)));

        if (mode == TimeDisplayMode.UTC)
        {
            Assert.Equal("2026-11-01 05:30:00", SystemEventRowFormat.StoredWallClock(At(2026, 11, 1, 5, 30)));
            Assert.Equal("2026-11-01 06:30:00", SystemEventRowFormat.StoredWallClock(At(2026, 11, 1, 6, 30)));
        }
    }

    /* The Job History and Agent status grids were never converted: Lite shows the stored wall clock as it is. The
       pins keep them bare if a later change routes them through the instant renderer. */
    [Fact]
    public void AJobStepAndANextRun_StoredAsTheWallClock_AreShownAsItIs_InTheRepeatedHour()
    {
        var step = new JobHistoryRow { RunDateTime = At(2026, 11, 1, 1, 30), LastSuccessfulRun = At(2026, 11, 1, 1, 45) };
        var agent = new AgentStatusRow { NextScheduledRun = At(2026, 11, 1, 1, 30) };

        Assert.Equal("2026-11-01 01:30:00", step.RunTimeLocal);
        Assert.Equal("2026-11-01 01:45:00", step.LastSuccessfulRunLocal);
        Assert.Equal("2026-11-01 01:30:00", agent.NextScheduledRunLocal);

        Assert.Equal("2026-11-01 01:30:00 -05:00", TrueUtcRowText(At(2026, 11, 1, 6, 30)));
    }
}
