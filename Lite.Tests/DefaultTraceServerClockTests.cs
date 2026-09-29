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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: the System Events (Default Trace) read converts each server-local <c>event_time</c> to UTC through the
/// server's clock instead of subtracting one offset from every row, and applies the exact UTC window to the
/// converted rows (the SQL window is only a pre-filter, one hour wider on each side).
///
/// <para>The Darling viewer's twin is <c>ViewerDefaultTraceServerClockTests</c>. Same US Eastern dates: the 2026
/// spring-forward is 8 March (02:00 EST to 03:00 EDT, 07:00 UTC) and the fall-back is 1 November (02:00 EDT to
/// 01:00 EST, 06:00 UTC). Lite runs on DuckDB, so every one of these runs here.</para>
/// </summary>
/* The un-clocked path of the read falls back to ServerTimeHelper.ActiveServerClock, a process-wide static. */
[Collection("server-time-helper")]
public sealed class DefaultTraceServerClockTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 8811;
    private const string EasternZone = "Eastern Standard Time";

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public DefaultTraceServerClockTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static DateTime At(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    private static ServerClock Eastern() => ServerClock.Resolve(EasternZone, -300);

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

    // ── System Events (Default Trace): server-local event_time converted per row ──

    [Fact]
    public async Task DefaultTrace_RowsOnBothSidesOfSpringForward_ComeBackWithTheRightUtcTimes()
    {
        var service = new LocalDataService(_duckDb);
        await SeedDefaultTraceAsync(At(2026, 3, 8, 1, 30), "before-change");   /* 01:30 EST = 06:30 UTC */
        await SeedDefaultTraceAsync(At(2026, 3, 8, 3, 30), "after-change");    /* 03:30 EDT = 07:30 UTC */

        /* A custom range in the server's wall clock, straddling the change. */
        var rows = await service.GetDefaultTraceEventsAsync(
            ServerId, fromDate: At(2026, 3, 7, 12, 0), toDate: At(2026, 3, 9, 12, 0), serverClock: Eastern());

        Assert.Equal(2, rows.Count);
        Assert.Equal(At(2026, 3, 8, 7, 30), rows.Single(r => r.TextData == "after-change").EventTimeUtc);
        Assert.Equal(At(2026, 3, 8, 6, 30), rows.Single(r => r.TextData == "before-change").EventTimeUtc);
    }

    [Fact]
    public async Task DefaultTrace_RowsAcrossFallBack_ComeBackWithTheRightUtcTimes()
    {
        var service = new LocalDataService(_duckDb);
        await SeedDefaultTraceAsync(At(2026, 11, 1, 0, 30), "before-change");   /* 00:30 EDT = 04:30 UTC */
        await SeedDefaultTraceAsync(At(2026, 11, 1, 2, 30), "after-change");    /* 02:30 EST = 07:30 UTC */

        var rows = await service.GetDefaultTraceEventsAsync(
            ServerId, fromDate: At(2026, 10, 31, 12, 0), toDate: At(2026, 11, 2, 12, 0), serverClock: Eastern());

        Assert.Equal(2, rows.Count);
        Assert.Equal(At(2026, 11, 1, 4, 30), rows.Single(r => r.TextData == "before-change").EventTimeUtc);
        Assert.Equal(At(2026, 11, 1, 7, 30), rows.Single(r => r.TextData == "after-change").EventTimeUtc);
    }

    [Fact]
    public async Task DefaultTrace_TheExactUtcWindowHolds_InsideThePreFilterMargin()
    {
        var service = new LocalDataService(_duckDb);

        /* Window: 06:00 to 07:00 UTC on 8 March, i.e. 01:00 EST to 03:00 EDT. The SQL pre-filter is one hour wider
           on each side (00:00 to 04:00 server-local); the exact UTC window is applied to the converted rows. */
        await SeedDefaultTraceAsync(At(2026, 3, 8, 0, 30), "outside-early");   /* 05:30 UTC: in the margin, out of the window */
        await SeedDefaultTraceAsync(At(2026, 3, 8, 1, 30), "inside");          /* 06:30 UTC */
        await SeedDefaultTraceAsync(At(2026, 3, 8, 3, 0), "at-the-end");       /* 07:00 UTC: the end is inclusive */
        await SeedDefaultTraceAsync(At(2026, 3, 8, 3, 30), "outside-late");    /* 07:30 UTC: in the margin, out of the window */

        var rows = await service.GetDefaultTraceEventsAsync(
            ServerId, hoursBack: 1, asOfUtc: At(2026, 3, 8, 7, 0), serverClock: Eastern());

        Assert.Equal(new[] { "at-the-end", "inside" }, rows.Select(r => r.TextData).OrderBy(t => t, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task DefaultTrace_StoredSkippedAndRepeatedLocalTimes_DoNotThrow()
    {
        var service = new LocalDataService(_duckDb);
        await SeedDefaultTraceAsync(At(2026, 3, 8, 2, 30), "skipped");     /* never happened: reads as 03:30 EDT = 07:30 UTC */

        var spring = await service.GetDefaultTraceEventsAsync(
            ServerId, fromDate: At(2026, 3, 8, 0, 0), toDate: At(2026, 3, 8, 12, 0), serverClock: Eastern());
        Assert.Equal(At(2026, 3, 8, 7, 30), Assert.Single(spring).EventTimeUtc);

        await SeedDefaultTraceAsync(At(2026, 11, 1, 1, 30), "repeated");   /* twice: the first occurrence, 05:30 UTC */

        var fall = await service.GetDefaultTraceEventsAsync(
            ServerId, fromDate: At(2026, 11, 1, 0, 0), toDate: At(2026, 11, 1, 12, 0), serverClock: Eastern());
        Assert.Equal(At(2026, 11, 1, 5, 30), Assert.Single(fall).EventTimeUtc);
    }

    [Fact]
    public async Task DefaultTrace_FixedOffsetServer_ReadsTheSameRowsAsBefore()
    {
        var service = new LocalDataService(_duckDb);
        await SeedDefaultTraceAsync(At(2026, 3, 8, 3, 30), "row");

        /* The int offset is the fixed-offset clock: 03:30 at UTC-5 is 08:30 UTC on both sides of the change. */
        var rows = await service.GetDefaultTraceEventsAsync(
            ServerId, fromDate: At(2026, 3, 8, 0, 0), toDate: At(2026, 3, 8, 12, 0), serverClock: ServerClock.FixedOffset(-300));

        Assert.Equal(At(2026, 3, 8, 8, 30), Assert.Single(rows).EventTimeUtc);
    }
}
