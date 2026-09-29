/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: the windowed reads turn a picker range's server-local bounds into UTC through the server's clock, so each
/// bound uses the offset in force at that bound, and an hours-back window in server-local time is the server-local
/// rendering of its two UTC ends. One offset for both bounds was an hour off at the far bound of any range that
/// crossed a daylight saving change.
///
/// <para>US Eastern, 2026: the spring-forward is 8 March (02:00 EST to 03:00 EDT, 07:00 UTC) and the fall-back is
/// 1 November (02:00 EDT to 01:00 EST, 06:00 UTC). The clock is passed in, so none of this touches the
/// process-wide time settings.</para>
/// </summary>
public sealed class WindowedReadServerClockTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4766;
    private const string EasternZone = "Eastern Standard Time";

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public WindowedReadServerClockTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static DateTime At(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    private static ServerClock Eastern() => ServerClock.Resolve(EasternZone, -300);

    // ── GetTimeRange: a custom range's server-local bounds to UTC ──

    [Fact]
    public void GetTimeRange_CustomRangeFromWinterToSummer_ConvertsEachBoundWithTheOffsetInForceThere()
    {
        var (start, end) = LocalDataService.GetTimeRange(
            24, At(2026, 3, 1, 12, 0), At(2026, 3, 15, 12, 0), asOfUtc: null, Eastern());

        Assert.Equal(At(2026, 3, 1, 17, 0), start);    /* 12:00 EST, UTC-5 */
        Assert.Equal(At(2026, 3, 15, 16, 0), end);     /* 12:00 EDT, UTC-4 */
    }

    [Fact]
    public void GetTimeRange_CustomRangeFromSummerToWinter_ConvertsEachBoundWithTheOffsetInForceThere()
    {
        var (start, end) = LocalDataService.GetTimeRange(
            24, At(2026, 10, 25, 12, 0), At(2026, 11, 8, 12, 0), asOfUtc: null, Eastern());

        Assert.Equal(At(2026, 10, 25, 16, 0), start);  /* 12:00 EDT, UTC-4 */
        Assert.Equal(At(2026, 11, 8, 17, 0), end);     /* 12:00 EST, UTC-5 */
    }

    [Fact]
    public void GetTimeRange_CustomRangeInsideWinter_UsesTheWinterOffsetAtBothBounds()
    {
        var (start, end) = LocalDataService.GetTimeRange(
            24, At(2026, 1, 10, 8, 0), At(2026, 1, 11, 8, 0), asOfUtc: null, Eastern());

        Assert.Equal(At(2026, 1, 10, 13, 0), start);
        Assert.Equal(At(2026, 1, 11, 13, 0), end);
    }

    [Fact]
    public void GetTimeRange_CustomRangeInsideSummer_UsesTheSummerOffsetAtBothBounds()
    {
        var (start, end) = LocalDataService.GetTimeRange(
            24, At(2026, 7, 10, 8, 0), At(2026, 7, 11, 8, 0), asOfUtc: null, Eastern());

        Assert.Equal(At(2026, 7, 10, 12, 0), start);
        Assert.Equal(At(2026, 7, 11, 12, 0), end);
    }

    [Fact]
    public void GetTimeRange_UtcClockAndFixedOffsetClock_ShiftBothBoundsByTheirOneOffset()
    {
        var from = At(2026, 3, 1, 12, 0);
        var to = At(2026, 3, 15, 12, 0);

        var (utcStart, utcEnd) = LocalDataService.GetTimeRange(24, from, to, asOfUtc: null, ServerClock.Utc);
        Assert.Equal(from, utcStart);
        Assert.Equal(to, utcEnd);

        /* A fixed offset has no daylight saving: 12:00 at UTC-4 is 16:00 UTC on both sides of the change. */
        var (fixedStart, fixedEnd) = LocalDataService.GetTimeRange(24, from, to, asOfUtc: null, ServerClock.FixedOffset(-240));
        Assert.Equal(At(2026, 3, 1, 16, 0), fixedStart);
        Assert.Equal(At(2026, 3, 15, 16, 0), fixedEnd);
    }

    [Fact]
    public void GetTimeRange_HoursBack_IsTheUtcWindowOnTheAnchor_WhateverTheClock()
    {
        var anchor = At(2026, 3, 8, 8, 0);

        var (start, end) = LocalDataService.GetTimeRange(6, fromDate: null, toDate: null, anchor, Eastern());

        Assert.Equal(At(2026, 3, 8, 2, 0), start);
        Assert.Equal(anchor, end);
    }

    // ── GetTimeRangeServerLocal: the server-local rendering of a UTC window ──

    [Fact]
    public void GetTimeRangeServerLocal_HoursBackJustAfterSpringForward_StartsAtTheServerLocalTimeOfAnchorMinusHoursBack()
    {
        var anchor = At(2026, 3, 8, 8, 0);   /* 04:00 EDT, an hour after the change */

        var (start, end) = LocalDataService.GetTimeRangeServerLocal(6, fromDate: null, toDate: null, anchor, Eastern());

        Assert.Equal(At(2026, 3, 8, 4, 0), end);
        Assert.Equal(At(2026, 3, 7, 21, 0), start);    /* 02:00 UTC is 21:00 EST: seven wall-clock hours for six real ones */
    }

    [Fact]
    public void GetTimeRangeServerLocal_HoursBackJustAfterFallBack_EndsAtTheServerLocalTimeOfTheAnchor()
    {
        var anchor = At(2026, 11, 1, 8, 0);  /* 03:00 EST, two hours after the change */

        var (start, end) = LocalDataService.GetTimeRangeServerLocal(6, fromDate: null, toDate: null, anchor, Eastern());

        Assert.Equal(At(2026, 11, 1, 3, 0), end);
        Assert.Equal(At(2026, 10, 31, 22, 0), start);  /* 02:00 UTC is 22:00 EDT: five wall-clock hours for six real ones */
    }

    [Fact]
    public void GetTimeRangeServerLocal_UtcClockAndFixedOffsetClock_ShiftBothEndsByTheirOneOffset()
    {
        var anchor = At(2026, 3, 8, 8, 0);

        var (utcStart, utcEnd) = LocalDataService.GetTimeRangeServerLocal(6, null, null, anchor, ServerClock.Utc);
        Assert.Equal(At(2026, 3, 8, 2, 0), utcStart);
        Assert.Equal(anchor, utcEnd);

        var (fixedStart, fixedEnd) = LocalDataService.GetTimeRangeServerLocal(6, null, null, anchor, ServerClock.FixedOffset(-240));
        Assert.Equal(At(2026, 3, 7, 22, 0), fixedStart);
        Assert.Equal(At(2026, 3, 8, 4, 0), fixedEnd);
    }

    [Fact]
    public void GetTimeRangeServerLocal_CustomRange_StaysAsGiven()
    {
        var from = At(2026, 3, 1, 12, 0);
        var to = At(2026, 3, 15, 12, 0);

        var (start, end) = LocalDataService.GetTimeRangeServerLocal(24, from, to, At(2026, 3, 16, 0, 0), Eastern());

        Assert.Equal(from, start);
        Assert.Equal(to, end);
    }

    // ── One read end to end: a custom range across the spring-forward ──

    [Fact]
    public async Task GetAlertCounts_CustomRangeAcrossSpringForward_KeepsRowsJustInsideBothBounds_AndDropsRowsJustOutside()
    {
        var service = new LocalDataService(_duckDb);

        /* The range 20:00 EST on 7 March to 04:00 EDT on 8 March is 01:00 UTC to 08:00 UTC: seven real hours,
           eight on the wall clock. */
        await SeedDeadlockAsync(At(2026, 3, 8, 0, 59));   /* a minute before the start: dropped */
        await SeedDeadlockAsync(At(2026, 3, 8, 1, 1));    /* a minute after the start: kept */
        await SeedDeadlockAsync(At(2026, 3, 8, 7, 59));   /* a minute before the end: kept */
        await SeedDeadlockAsync(At(2026, 3, 8, 8, 1));    /* a minute after the end: dropped */

        var (blocking, deadlocks, latest) = await service.GetAlertCountsAsync(
            ServerId, hoursBack: 24, fromDate: At(2026, 3, 7, 20, 0), toDate: At(2026, 3, 8, 4, 0), Eastern());

        Assert.Equal(0, blocking);
        Assert.Equal(2, deadlocks);
        Assert.Equal(At(2026, 3, 8, 7, 59), latest);
    }

    private async Task SeedDeadlockAsync(DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml)
VALUES ($1, $2, $3, 'TestSrv', $4, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = "<deadlock><victim-list/><process-list/></deadlock>" });
        await cmd.ExecuteNonQueryAsync();
    }
}
