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
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: the Alerts History grid's Time column shows an alert in the selected display mode, on the clock of the
/// server the row belongs to. It used to be <c>AlertTime.ToLocalTime()</c> in every mode, so a grid in UTC or Server
/// mode showed this machine's time, and two servers in one list could not each have their own clock.
///
/// <para>US Eastern is the test zone: the autumn change is 1 November 2026 at 06:00 UTC, so 05:30Z is the first 01:30
/// on an Eastern wall clock and 06:30Z is the second.</para>
/// </summary>
/* Sets ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics, so it joins the
   collection every other class that writes them uses; both are restored in Dispose. */
[Collection("server-time-helper")]
public sealed class AlertHistoryRowClockTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string EasternZone = "Eastern Standard Time";

    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;
    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public AlertHistoryRowClockTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
        _seedConn?.Dispose();
    }

    private static ServerClock Eastern() => ServerClock.Resolve(EasternZone, -300);

    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    private static AlertHistoryRow Row(int serverId, DateTime alertTimeUtc, ServerClock? clock = null) => new()
    {
        ServerId = serverId,
        ServerName = $"Server{serverId}",
        AlertTime = alertTimeUtc,
        Clock = clock,
    };

    // ── the row's own conversion ─────────────────────────────────────────────────

    [Fact]
    public void TimeLocal_AnEasternRowInTheRepeatedHour_Shows0130InServerMode_And0630InUtcMode()
    {
        var row = Row(1, Utc(2026, 11, 1, 6, 30), Eastern());

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-11-01 01:30:00", row.TimeLocal);

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("2026-11-01 06:30:00", row.TimeLocal);
    }

    /// <summary>Local mode is this machine's zone, whatever clock the row carries; it is what the column always showed.</summary>
    [Fact]
    public void TimeLocal_InLocalMode_IsThisMachinesZone_OnAnyClock()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;

        foreach (var clock in new[] { Eastern(), ServerClock.FixedOffset(330) })
        {
            foreach (var utc in new[] { Utc(2026, 11, 1, 5, 30), Utc(2026, 11, 1, 6, 30), Utc(2026, 7, 1, 12, 0) })
            {
                var expected = TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(utc, DateTimeKind.Utc), TimeZoneInfo.Local);
                Assert.Equal(expected.ToString("yyyy-MM-dd HH:mm:ss"), Row(1, utc, clock).TimeLocal);
            }
        }
    }

    [Fact]
    public void TimeLocal_WithNoClockStamped_TakesTheActiveServersClock()
    {
        ServerTimeHelper.ActiveServerClock = Eastern();
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        Assert.Equal("2026-11-01 01:30:00", Row(1, Utc(2026, 11, 1, 6, 30)).TimeLocal);
        Assert.Equal("2026-07-01 08:00:00", Row(1, Utc(2026, 7, 1, 12, 0)).TimeLocal);
    }

    // ── the list's clock chain, one per server ───────────────────────────────────

    /// <summary>
    /// Two servers in one list: each row converts on its own server's clock, not the active tab's (a third server's,
    /// at +10:00 here), and the Eastern server's row from before the change is on daylight time.
    /// </summary>
    [Fact]
    public void StampClocks_TwoServersWithDifferentClocks_EachConvertOnTheirOwn()
    {
        ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(600);
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var instant = Utc(2026, 11, 1, 6, 30);
        var eastern = Row(1, instant);
        var india = Row(2, instant);
        var easternSummer = Row(1, Utc(2026, 7, 1, 12, 0));
        var collected = new Dictionary<int, ServerClock?> { [1] = Eastern(), [2] = ServerClock.FixedOffset(330) };

        AlertsHistoryTab.StampClocks(new[] { eastern, india, easternSummer }, collected, openTabClock: null);

        Assert.Equal("2026-11-01 01:30:00", eastern.TimeLocal);
        Assert.Equal("2026-11-01 12:00:00", india.TimeLocal);
        Assert.Equal("2026-07-01 08:00:00", easternSummer.TimeLocal);
    }

    /// <summary>
    /// A server with no collected clock takes its OWN open tab's clock (asked once per server, however many rows it has),
    /// and a server with neither takes this machine's offset, never UTC and never the active tab's.
    /// </summary>
    [Fact]
    public void StampClocks_WithNoCollectedClock_TakesTheServersOwnOpenTab_ThenTheMachine()
    {
        ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(600);
        var asked = new List<int>();
        Func<int, ServerClock?> openTabs = id =>
        {
            asked.Add(id);
            return id == 3 ? ServerClock.FixedOffset(60) : null;
        };
        var instant = Utc(2026, 6, 1, 12, 0);
        var withTab = Row(3, instant);
        var withTabAgain = Row(3, instant.AddMinutes(5));
        var withNothing = Row(4, instant);

        AlertsHistoryTab.StampClocks(
            new[] { withTab, withTabAgain, withNothing }, new Dictionary<int, ServerClock?>(), openTabs);

        Assert.Equal(new[] { 3, 4 }, asked);
        Assert.Same(withTab.Clock, withTabAgain.Clock);
        Assert.Equal(60, withTab.Clock!.OffsetMinutesAt(instant));
        Assert.Equal(
            (int)TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).TotalMinutes,
            withNothing.Clock!.OffsetMinutesAt(instant));
    }

    [Fact]
    public void StampClocks_ACollectedClockBeatsTheOpenTabs_AndTheLookupIsOptional()
    {
        var collected = new Dictionary<int, ServerClock?> { [1] = Eastern() };
        var rowWithBoth = Row(1, Utc(2026, 7, 1, 12, 0));
        var rowWithNoLookup = Row(1, Utc(2026, 7, 1, 12, 0));

        AlertsHistoryTab.StampClocks(new[] { rowWithBoth }, collected, id => ServerClock.FixedOffset(330));
        AlertsHistoryTab.StampClocks(new[] { rowWithNoLookup }, collected, openTabClock: null);

        Assert.Equal(-240, rowWithBoth.Clock!.OffsetMinutesAt(Utc(2026, 7, 1, 12, 0)));
        Assert.Equal(-240, rowWithNoLookup.Clock!.OffsetMinutesAt(Utc(2026, 7, 1, 12, 0)));
    }

    // ── the collected clocks of one load ─────────────────────────────────────────

    private async Task SeedServerPropertiesAsync(int serverId, DateTime collectionTime, int offset, string? zoneId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name,
             edition, product_version, product_level, engine_edition,
             cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id)
            VALUES ($1, $2, $3, 'TestSrv', 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $4, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = -_nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = offset });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)zoneId ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// The load reads one clock per distinct server in the list, however many rows a server has, and a server with
    /// nothing collected comes back null so the chain moves on to its open tab. Read from the store the way the tab
    /// reads it, then stamped and rendered.
    /// </summary>
    [Fact]
    public async Task ReadCollectedClocksAsync_ReadsEachDistinctServersOwnClock_AndNullWhereNothingIsCollected()
    {
        const int easternServer = 8811;
        const int indiaServer = 8812;
        const int newServer = 8813;
        await SeedServerPropertiesAsync(easternServer, Utc(2026, 2, 10, 0, 0), -300, EasternZone);
        await SeedServerPropertiesAsync(indiaServer, Utc(2026, 2, 10, 0, 0), 330, null);
        var service = new LocalDataService(_duckDb);
        var instant = Utc(2026, 11, 1, 6, 30);
        var rows = new[] { Row(easternServer, instant), Row(indiaServer, instant), Row(easternServer, instant.AddMinutes(1)), Row(newServer, instant) };

        var clocks = await AlertsHistoryTab.ReadCollectedClocksAsync(service, rows);

        Assert.Equal(new[] { easternServer, indiaServer, newServer }.OrderBy(i => i), clocks.Keys.OrderBy(i => i));
        Assert.Null(clocks[newServer]);

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        AlertsHistoryTab.StampClocks(rows, clocks, openTabClock: null);
        Assert.Equal("2026-11-01 01:30:00", rows[0].TimeLocal);
        Assert.Equal("2026-11-01 12:00:00", rows[1].TimeLocal);
        Assert.Equal("2026-11-01 01:31:00", rows[2].TimeLocal);
    }
}
