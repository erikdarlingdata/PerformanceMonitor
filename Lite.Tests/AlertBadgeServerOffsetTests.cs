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
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The alert-badge read windows in the offset of the server it NAMES, not in the offset of whichever
/// server tab the desktop happens to have selected.
///
/// <para><b>Why this read and not the others.</b> Every other windowed read in <c>ServerTab</c> sits
/// behind the <c>IsVisible</c> gate, where the tab doing the reading IS the selected tab and the two
/// offsets are the same number. The badge is the exception: it runs on every tab's own 60-second timer
/// whether or not that tab is on screen, so the server it counts for and the server the desktop is
/// showing are routinely different ones. A count windowed against the wrong clock is silent — it
/// under- or over-reports and nothing errors — and the badge exists precisely to be trusted without
/// opening the tab.</para>
///
/// <para><b>No offset is applied to the window at all.</b> The tab holds a custom range as UTC instants
/// (<c>ServerTab.GetCurrentWindowUtc</c>) and hands both to <see cref="LocalDataService.GetAlertCountsAsync"/>
/// as they are; the display mode and either server's offset decide only what the pickers show. So the read has
/// to return each server its own counts over the same two instants in every mode, and a change that puts an
/// offset back into the window (the desktop's process-wide clock, or a server's own) moves the counts in the
/// modes it touches. Every mode is therefore exercised here, and each runs the REAL read.</para>
///
/// <para><b>Why the assertion is an invariance, not a pinned window.</b> A test that pins one expected
/// timestamp passes against the unfixed code whenever the static happens to hold the right server's
/// offset — which is how this defect stayed hidden. The contract here is independence: the same reads run
/// under four values of the process-wide static, including one belonging to neither server, and every run
/// must return each server its own counts. Nothing here seeds against a value it read out of the static;
/// the offsets are the servers' own, supplied the way production supplies them.</para>
/// </summary>
/* Writes ServerTimeHelper.UtcOffsetMinutes and CurrentDisplayMode, both process-wide mutable statics.
   xUnit runs test classes in parallel, so this joins the collection the other offset-touching classes
   use rather than racing them. */
[Collection("server-time-helper")]
public sealed class AlertBadgeServerOffsetTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    /* UTC-8 and UTC+5:30 — 13.5 hours apart, so a 30-minute window built with the wrong server's offset
       cannot accidentally still contain the right server's rows. One offset is a HALF hour, so an
       implementation that rounds to whole hours is caught too. */
    private const int WestOffset = -480;
    private const int EastOffset = 330;

    /* Synthetic ids and names. Told apart by COUNT SHAPE, not by row totals: a read that returns the
       other server's rows cannot look right, and neither can one that returns both servers'. */
    private const int WestServerId = 60801;
    private const int EastServerId = 60802;
    private const string WestServerName = "WestZoneSrv";
    private const string EastServerName = "EastZoneSrv";

    private const int WestBlocking = 2;
    private const int WestDeadlocks = 1;
    private const int EastBlocking = 1;
    private const int EastDeadlocks = 3;

    /* The four values the desktop static is run under: each server's own, a UTC host with no tab ever
       opened, and an offset belonging to neither server. */
    private static readonly int[] DesktopStatics = [WestOffset, EastOffset, 0, 720];

    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;
    private DuckDBConnection? _seedConn;

    public AlertBadgeServerOffsetTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>
    /// The window the operator means, in UTC. Everything seeded is placed relative to this, and every
    /// display mode's picker values are derived so that the production pair lands back on exactly it.
    /// </summary>
    private static readonly DateTime WindowStartUtc = ChooseBase();
    private static readonly DateTime WindowEndUtc = WindowStartUtc.AddMinutes(30);

    /// <summary>
    /// A base instant ~2h old whose LOCAL rendering is neither ambiguous nor invalid, so the
    /// <c>LocalTime</c> case's picker value converts back to the instant it came from. A DST boundary
    /// would otherwise make that one mode flaky for the hour a year it lands in.
    /// </summary>
    private static DateTime ChooseBase()
    {
        var candidate = Truncate(DateTime.UtcNow.AddHours(-2));
        for (var i = 0; i < 8; i++)
        {
            var startLocal = ToLocalPicker(candidate);
            var endLocal = ToLocalPicker(candidate.AddMinutes(30));
            if (!TimeZoneInfo.Local.IsAmbiguousTime(startLocal) && !TimeZoneInfo.Local.IsInvalidTime(startLocal)
                && !TimeZoneInfo.Local.IsAmbiguousTime(endLocal) && !TimeZoneInfo.Local.IsInvalidTime(endLocal))
                return candidate;
            candidate = candidate.AddHours(-6);
        }
        return candidate;
    }

    private static DateTime ToLocalPicker(DateTime utc) =>
        DateTime.SpecifyKind(
            TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(utc, DateTimeKind.Utc), TimeZoneInfo.Local),
            DateTimeKind.Unspecified);

    /// <summary>
    /// The contract. In every display mode, and whatever the desktop static holds, each server's badge
    /// counts its own rows over the window the operator picked.
    /// </summary>
    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]  // the default
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    public async Task TheBadgeCountsInItsOwnServersOffset_InEveryDisplayMode_WhateverTheDesktopStaticHolds(TimeDisplayMode mode)
    {
        await SeedBothServersAsync();

        var savedOffset = ServerTimeHelper.UtcOffsetMinutes;
        var savedMode = ServerTimeHelper.CurrentDisplayMode;
        try
        {
            ServerTimeHelper.CurrentDisplayMode = mode;

            foreach (var desktopStatic in DesktopStatics)
            {
                ServerTimeHelper.UtcOffsetMinutes = desktopStatic;

                var west = await ReadBadgeAsync(WestServerId, WestOffset, mode);
                var east = await ReadBadgeAsync(EastServerId, EastOffset, mode);

                Assert.True(
                    west == (WestBlocking, WestDeadlocks),
                    $"{mode} / static {desktopStatic}: west badge was {west}, expected ({WestBlocking}, {WestDeadlocks})");
                Assert.True(
                    east == (EastBlocking, EastDeadlocks),
                    $"{mode} / static {desktopStatic}: east badge was {east}, expected ({EastBlocking}, {EastDeadlocks})");
            }
        }
        finally
        {
            ServerTimeHelper.UtcOffsetMinutes = savedOffset;
            ServerTimeHelper.CurrentDisplayMode = savedMode;
        }
    }

    /// <summary>
    /// The same reads over a preset window (no custom range) must also be independent of the static — a preset
    /// has no bounds to hand over, so a server 13.5 hours away still gets its own rows. Guards against a
    /// "fix" that read the ambient clock into the hoursBack branch.
    /// </summary>
    [Fact]
    public async Task APresetWindowIsAlsoIndependentOfTheDesktopStatic()
    {
        await SeedBothServersAsync();

        var savedOffset = ServerTimeHelper.UtcOffsetMinutes;
        try
        {
            /* The seeded rows are ~2h old, so a 24h preset contains them and the out-of-window decoys
               (3h before the window) as well — the assertion is that the answer does not MOVE. */
            (int, int)? firstWest = null;
            (int, int)? firstEast = null;

            foreach (var desktopStatic in DesktopStatics)
            {
                ServerTimeHelper.UtcOffsetMinutes = desktopStatic;

                var west = await ReadPresetBadgeAsync(WestServerId, WestOffset);
                var east = await ReadPresetBadgeAsync(EastServerId, EastOffset);

                firstWest ??= west;
                firstEast ??= east;

                Assert.True(west == firstWest, $"static {desktopStatic}: west preset badge moved to {west} from {firstWest}");
                Assert.True(east == firstEast, $"static {desktopStatic}: east preset badge moved to {east} from {firstEast}");
            }

            /* And the two servers are still told apart, so the invariance above is not vacuous. */
            Assert.NotEqual(firstWest, firstEast);
        }
        finally
        {
            ServerTimeHelper.UtcOffsetMinutes = savedOffset;
        }
    }

    /// <summary>
    /// The badge read as the tab drives it (#4766): the tab holds the range as UTC instants, so the display mode and the
    /// server's offset decide only what the pickers show, and the read is handed the same two instants whatever they are.
    /// </summary>
    private async Task<(int Blocking, int Deadlocks)> ReadBadgeAsync(int serverId, int utcOffsetMinutes, TimeDisplayMode mode)
    {
        _ = (utcOffsetMinutes, mode);
        var (blocking, deadlocks, _) = await new LocalDataService(_duckDb)
            .GetAlertCountsAsync(serverId, hoursBack: 24, fromDate: WindowStartUtc, toDate: WindowEndUtc);
        return (blocking, deadlocks);
    }

    private async Task<(int Blocking, int Deadlocks)> ReadPresetBadgeAsync(int serverId, int utcOffsetMinutes)
    {
        var (blocking, deadlocks, _) = await new LocalDataService(_duckDb)
            .GetAlertCountsAsync(serverId, hoursBack: 24, fromDate: null, toDate: null);
        return (blocking, deadlocks);
    }

    private async Task SeedBothServersAsync()
    {
        if (_nextId > 1) return;

        /* In-window rows, spread so no two share a timestamp. Out-of-window decoys sit 3h before the
           window start — inside a 24h preset, outside the custom range — so a window shifted by either
           server's offset lands on nothing rather than on a plausible-looking count. */
        var inWindow = WindowStartUtc.AddMinutes(5);
        var decoy = WindowStartUtc.AddHours(-3);

        for (var i = 0; i < WestBlocking; i++)
            await SeedBlockedProcessReportAsync(WestServerId, WestServerName, inWindow.AddMinutes(i));
        for (var i = 0; i < WestDeadlocks; i++)
            await SeedDeadlockAsync(WestServerId, WestServerName, inWindow.AddMinutes(i));
        await SeedBlockedProcessReportAsync(WestServerId, WestServerName, decoy);
        await SeedDeadlockAsync(WestServerId, WestServerName, decoy);

        for (var i = 0; i < EastBlocking; i++)
            await SeedBlockedProcessReportAsync(EastServerId, EastServerName, inWindow.AddMinutes(i));
        for (var i = 0; i < EastDeadlocks; i++)
            await SeedDeadlockAsync(EastServerId, EastServerName, inWindow.AddMinutes(i));
        await SeedBlockedProcessReportAsync(EastServerId, EastServerName, decoy);
        await SeedDeadlockAsync(EastServerId, EastServerName, decoy);
    }

    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedBlockedProcessReportAsync(int serverId, string serverName, DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocked_last_tran_started, blocking_spid, blocking_last_tran_started,
     wait_time_ms, lock_mode, blocked_status, blocking_status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SynthDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = 55 });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = 66 });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "X" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "suspended" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "running" });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedDeadlockAsync(int serverId, string serverName, DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $5, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = "<deadlock><victim-list/><process-list/></deadlock>" });
        await cmd.ExecuteNonQueryAsync();
    }
}
