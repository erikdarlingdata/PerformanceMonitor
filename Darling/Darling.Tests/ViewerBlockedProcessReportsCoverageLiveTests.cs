/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Blocked Process Reports grid's "Showing since" notice against a real store (#4966), for the three cases its two
/// collectors and its event times raise. The grid merges the XE blocked_process_reports rows with the always-on DMV
/// snapshots that stand in for them when XE collection is off, and each read keeps its newest 200 rows. Every test reads
/// the grid through the same calls the server tab makes (the merged read, the coverage probe) and raises the banner
/// through the same shared function (<c>ShowEventDataStartAsync</c>), so a change to the rule fails here.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. The banner's time text reads the
   process-wide display mode, which the culture pin sets (restored in Dispose); the shared collection serializes it with
   every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerBlockedProcessReportsCoverageLiveTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private const int FirstCollectionServerId = -499801;
    private const int DmvOnlyServerId = -499802;
    private const int DmvOnlyQuietServerId = -499803;
    private const int DmvCappedServerId = -499804;
    private const int XeMidwayServerId = -499805;
    private const int ChartsDmvOnlyServerId = -499806;
    private const int XeWholeWindowServerId = -499807;
    private const string XeCollector = "blocked_process_report";
    private const string DmvCollector = "dmv_blocking_snapshot";

    /* The range ends before the server's first collection, which stored the events of the days before it: the probe finds no
       coverage in the range, yet the grid lists events from a day into it. The notice names the first of them. */
    [Fact]
    public async Task ARangeThatEndsBeforeTheFirstCollection_GivesANotice_AtTheEarliestReport_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(rangeEndsDaysAgo: 3, ct);

        /* Added a day after the range ended; its first collection stored the reports of the 6 days before the range's end. */
        await store.AddServerAsync(FirstCollectionServerId, store.End.AddDays(1), ct);
        await store.InsertXeReportsAsync(FirstCollectionServerId, store.Start.AddDays(1), store.End, collectedAtUtc: store.End.AddDays(1), ct);

        var coverage = await store.Viewer.GetBlockedProcessReportsDataStartAsync(FirstCollectionServerId, store.Start, store.End, ct);
        var banner = await store.BannerAsync(FirstCollectionServerId, ct);

        Assert.Null(coverage);
        Assert.Equal(Since(store.Start.AddDays(1)), banner);
    }

    /* XE collection is off (no report, no run of that collector) and the DMV collector began 2 days before the range's end;
       its first snapshot of blocking came 3 hours later. The DMV collector covers the grid from the day it was added, so the
       notice names that day, not the later first row and not nothing. */
    [Fact]
    public async Task WithXeCollectionOff_TheNotice_NamesTheDmvCoverageStart_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(rangeEndsDaysAgo: 0, ct);

        await store.AddServerAsync(DmvOnlyServerId, store.End.AddDays(-2), ct);
        await store.LogRunsAsync(DmvOnlyServerId, DmvCollector, store.End.AddDays(-2), store.End, ct);
        await store.InsertDmvSnapshotsAsync(DmvOnlyServerId, store.End.AddDays(-2).AddHours(3), store.End, TimeSpan.FromHours(6), ct);

        var coverage = await store.Viewer.GetBlockedProcessReportsDataStartAsync(DmvOnlyServerId, store.Start, store.End, ct);
        var banner = await store.BannerAsync(DmvOnlyServerId, ct);

        Assert.Equal(store.End.AddDays(-2), coverage);
        Assert.Equal(Since(store.End.AddDays(-2)), banner);
    }

    /* XE collection is off and the DMV collector has run for 40 days, the whole range: the first snapshot of blocking came 5
       hours into the range, which is a quiet start, not a cut. The grid's own first row is no reason for a notice. */
    [Fact]
    public async Task WithXeCollectionOff_AQuietDmvStart_GivesNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(rangeEndsDaysAgo: 0, ct);

        await store.AddServerAsync(DmvOnlyQuietServerId, store.End.AddDays(-40), ct);
        await store.LogRunsAsync(DmvOnlyQuietServerId, DmvCollector, store.End.AddDays(-40), store.End, ct);
        await store.InsertDmvSnapshotsAsync(DmvOnlyQuietServerId, store.Start.AddHours(5), store.End, TimeSpan.FromHours(6), ct);

        var coverage = await store.Viewer.GetBlockedProcessReportsDataStartAsync(DmvOnlyQuietServerId, store.Start, store.End, ct);
        var banner = await store.BannerAsync(DmvOnlyQuietServerId, ct);

        Assert.NotNull(coverage);
        Assert.False(RawWindowFloor.IsTruncated(coverage, store.Start));
        Assert.Null(banner);
    }

    /* A parallel blocked query shows as several snapshot rows for one pair a minute (one per thread), so the DMV read can
       fill its 200-row LIMIT while the merge, which keeps one row per pair per minute, leaves far fewer. The older reports of
       that read are not in the grid, whatever the count of the merged list says: the notice names the oldest row the DMV read
       returned, though the store covers the whole range. */
    [Fact]
    public async Task ADmvReadThatFillsItsCap_GivesANotice_AtItsOldestRow_WhereTheMergedListStaysUnderTheCap_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(rangeEndsDaysAgo: 0, ct);

        await store.AddServerAsync(DmvCappedServerId, store.End.AddDays(-40), ct);
        await store.LogRunsAsync(DmvCappedServerId, DmvCollector, store.End.AddDays(-40), store.End, ct);
        /* 3 rows a minute for the last 100 minutes: 300 rows, one pair. */
        await store.InsertDmvSnapshotsAsync(DmvCappedServerId, store.End.AddMinutes(-100), store.End.AddSeconds(-20), TimeSpan.FromSeconds(20), ct);

        var read = await store.Viewer.ReadRecentBlockedProcessReportsAsync(DmvCappedServerId, store.Start, store.End, cancellationToken: ct);
        var oldestReturned = await store.OldestOfNewestDmvRowsAsync(DmvCappedServerId, ViewerDataService.BlockedProcessReportsRowCap, ct);
        var banner = await store.BannerAsync(DmvCappedServerId, ct);

        Assert.InRange(read.Rows.Count, 1, ViewerDataService.BlockedProcessReportsRowCap - 1);
        Assert.False(ViewerEventDataStart.ReadHitCap(read.Rows.Count, ViewerDataService.BlockedProcessReportsRowCap));
        Assert.Equal(oldestReturned, read.CappedSourceStartUtc);
        Assert.True(read.Rows.Min(r => r.EventTime) > oldestReturned.AddMinutes(-1));
        Assert.Equal(Since(oldestReturned), banner);
    }

    /* The Blocking charts read the XE table when it holds a report in the window. The DMV collector has run for 40 days and the
       XE collector began 3 days before the range's end: the grid's probe takes the earlier of the two and reads covered, while
       the charts' probe names the XE start, the first day their data can exist. */
    [Fact]
    public async Task TheChartsProbe_NamesTheXeStart_WhereTheGridProbeReadsCoveredByTheDmv_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(rangeEndsDaysAgo: 0, ct);

        await store.AddServerAsync(XeMidwayServerId, store.End.AddDays(-40), ct);
        await store.LogRunsAsync(XeMidwayServerId, DmvCollector, store.End.AddDays(-40), store.End, ct);
        await store.LogRunsAsync(XeMidwayServerId, XeCollector, store.End.AddDays(-3), store.End, ct);
        await store.InsertXeReportsAsync(XeMidwayServerId, store.End.AddDays(-3).AddHours(6), store.End, store.End.AddDays(-3), ct);

        var gridProbe = await store.Viewer.GetBlockedProcessReportsDataStartAsync(XeMidwayServerId, store.Start, store.End, ct);
        var chartsProbe = await store.Viewer.GetBlockingChartDataStartAsync(XeMidwayServerId, store.Start, store.End, cancellationToken: ct);

        Assert.False(RawWindowFloor.IsTruncated(gridProbe, store.Start));
        Assert.Equal(store.End.AddDays(-3), chartsProbe);
    }

    /* No XE report in the window: the charts' probe is the two-source probe, as before. */
    [Fact]
    public async Task TheChartsProbe_WithNoXeReportInTheWindow_IsTheGridProbe_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(rangeEndsDaysAgo: 0, ct);

        await store.AddServerAsync(ChartsDmvOnlyServerId, store.End.AddDays(-2), ct);
        await store.LogRunsAsync(ChartsDmvOnlyServerId, DmvCollector, store.End.AddDays(-2), store.End, ct);
        await store.InsertDmvSnapshotsAsync(ChartsDmvOnlyServerId, store.End.AddDays(-2).AddHours(3), store.End, TimeSpan.FromHours(6), ct);

        var chartsProbe = await store.Viewer.GetBlockingChartDataStartAsync(ChartsDmvOnlyServerId, store.Start, store.End, cancellationToken: ct);

        Assert.Equal(store.End.AddDays(-2), chartsProbe);
        Assert.Equal(await store.Viewer.GetBlockedProcessReportsDataStartAsync(ChartsDmvOnlyServerId, store.Start, store.End, ct), chartsProbe);
    }

    /* The XE collector covers the whole range: the charts' probe reads covered. */
    [Fact]
    public async Task TheChartsProbe_WithXeCoveringTheWholeWindow_ReadsCovered_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(rangeEndsDaysAgo: 0, ct);

        await store.AddServerAsync(XeWholeWindowServerId, store.End.AddDays(-40), ct);
        await store.LogRunsAsync(XeWholeWindowServerId, XeCollector, store.End.AddDays(-40), store.End, ct);
        await store.InsertXeReportsAsync(XeWholeWindowServerId, store.Start.AddHours(5), store.End, store.End.AddDays(-40), ct);

        var chartsProbe = await store.Viewer.GetBlockingChartDataStartAsync(XeWholeWindowServerId, store.Start, store.End, cancellationToken: ct);

        Assert.NotNull(chartsProbe);
        Assert.False(RawWindowFloor.IsTruncated(chartsProbe, store.Start));
    }

    private static string Since(DateTime utc) =>
        "Showing since " + utc.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private Store(ScratchPostgres scratch, ViewerDataService viewer, DateTime end)
        {
            _scratch = scratch;
            Viewer = viewer;
            End = end;
        }

        public ViewerDataService Viewer { get; }

        /// <summary>The end of the 7-day range every test reads, to the minute.</summary>
        public DateTime End { get; }

        public DateTime Start => End.AddDays(-7);

        public static async Task<Store> CreateAsync(int rangeEndsDaysAgo, CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live Blocked Process Reports coverage tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await connection.OpenAsync(ct);
                    await PgMigrations.MigrateAsync(connection, ct);
                }

                var now = DateTime.UtcNow.AddDays(-rangeEndsDaysAgo);
                var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
                return new Store(scratch, new ViewerDataService(scratch.ConnectionString), end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /// <summary>
        /// The banner the server tab raises for the grid over the range: the coverage probe and the merged read it starts
        /// beside, handed to the shared function the tab hands them to, with the cap arguments the tab passes. Null when hidden.
        /// </summary>
        public async Task<string?> BannerAsync(int serverId, CancellationToken ct)
        {
            var probe = Viewer.GetBlockedProcessReportsDataStartAsync(serverId, Start, End, ct);
            var read = await Viewer.ReadRecentBlockedProcessReportsAsync(serverId, Start, End, cancellationToken: ct);
            await probe.WaitAsync(ct);

            string? text = null;
            OnStaThread(() =>
            {
                ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
                /* Seeded visible, so a no-op cannot pass as a hidden banner. */
                var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };

                ViewerServerTab.ShowEventDataStartAsync(
                    banner, probe, "Blocked Process Reports", Start, read.Rows.Select(r => r.EventTime),
                    ViewerDataService.BlockedProcessReportsRowCap, read.CappedSourceStartUtc).GetAwaiter().GetResult();

                text = banner.Visibility == Visibility.Visible ? banner.Text : null;
            });
            return text;
        }

        /* The registry row (created_date: the server's first successful connect). */
        public async Task AddServerAsync(int serverId, DateTime addedUtc, CancellationToken ct)
        {
            await using var connection = await OpenAsync(ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, ServerName(serverId), ct);

            await using var update = new NpgsqlCommand("UPDATE collect.servers SET created_date = $2 WHERE server_id = $1", connection);
            update.Parameters.AddWithValue(serverId);
            update.Parameters.AddWithValue(Naive(addedUtc));
            await update.ExecuteNonQueryAsync(ct);
        }

        /* The collector's runs from fromUtc to toUtc, every 30 minutes, whether or not anything happened. */
        public async Task LogRunsAsync(int serverId, string collector, DateTime fromUtc, DateTime toUtc, CancellationToken ct)
        {
            await using var connection = await OpenAsync(ct);
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT
    row_number() OVER () + $6,
    $1,
    $2,
    $5,
    t,
    12,
    'SUCCESS',
    0
FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t", connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(ServerName(serverId));
            insert.Parameters.AddWithValue(Naive(fromUtc));
            insert.Parameters.AddWithValue(Naive(toUtc));
            insert.Parameters.AddWithValue(collector);
            insert.Parameters.AddWithValue(-(long)serverId * 1000L);
            await insert.ExecuteNonQueryAsync(ct);
        }

        /* One XE report every 6 hours from firstUtc to lastUtc, all collected at collectedAtUtc (a first collection storing history). */
        public async Task InsertXeReportsAsync(int serverId, DateTime firstUtc, DateTime lastUtc, DateTime collectedAtUtc, CancellationToken ct)
        {
            await using var connection = await OpenAsync(ct);
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms)
SELECT
    row_number() OVER () + $6,
    $5::timestamp,
    $1,
    $2,
    t,
    'EventDb',
    55,
    56,
    1000
FROM generate_series($3::timestamp, $4::timestamp, interval '6 hours') AS t", connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(ServerName(serverId));
            insert.Parameters.AddWithValue(Naive(firstUtc));
            insert.Parameters.AddWithValue(Naive(lastUtc));
            insert.Parameters.AddWithValue(Naive(collectedAtUtc));
            insert.Parameters.AddWithValue(-(long)serverId * 1000L);
            await insert.ExecuteNonQueryAsync(ct);
        }

        /* One DMV blocking snapshot row of the same SPID pair at each step from firstUtc to lastUtc, its event time the collection time (the snapshot collector stamps the two alike). */
        public async Task InsertDmvSnapshotsAsync(int serverId, DateTime firstUtc, DateTime lastUtc, TimeSpan step, CancellationToken ct)
        {
            await using var connection = await OpenAsync(ct);
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.dmv_blocking_snapshots
    (collection_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocking_status,
     blocked_sql_text, blocking_sql_text, contentious_object)
SELECT
    row_number() OVER (ORDER BY t) + $6,
    t,
    $1,
    $2,
    t,
    'AppDb',
    55,
    56,
    900,
    'S',
    'running',
    'SELECT 2',
    'UPDATE t SET c = 2',
    'dbo.t'
FROM generate_series($3::timestamp, $4::timestamp, $5::interval) AS t", connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(ServerName(serverId));
            insert.Parameters.AddWithValue(Naive(firstUtc));
            insert.Parameters.AddWithValue(Naive(lastUtc));
            insert.Parameters.AddWithValue(step);
            insert.Parameters.AddWithValue(-(long)serverId * 1000L);
            await insert.ExecuteNonQueryAsync(ct);
        }

        /* The oldest event time among the newest rows the DMV read keeps, worked out here from the table, not from the read. */
        public async Task<DateTime> OldestOfNewestDmvRowsAsync(int serverId, int rowCap, CancellationToken ct)
        {
            await using var connection = await OpenAsync(ct);
            await using var select = new NpgsqlCommand(
                $"SELECT MIN(event_time) FROM (SELECT event_time FROM collect.dmv_blocking_snapshots WHERE server_id = $1 ORDER BY event_time DESC LIMIT {rowCap}) AS newest",
                connection);
            select.Parameters.AddWithValue(serverId);
            return (DateTime)(await select.ExecuteScalarAsync(ct))!;
        }

        private async Task<NpgsqlConnection> OpenAsync(CancellationToken ct)
        {
            var connection = new NpgsqlConnection(_scratch.ConnectionString);
            await connection.OpenAsync(ct);
            return connection;
        }

        private static string ServerName(int serverId) => $"bpr-coverage-{-serverId}";

        /* SpecifyKind(Unspecified), not the bare value: Npgsql infers timestamptz from Kind=Utc and PostgreSQL then zone-shifts the bounds against the store's NAIVE timestamp columns. */
        private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

        public async ValueTask DisposeAsync()
        {
            await Viewer.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }

    /* WPF objects require STA; same shape as ViewerEventDataStartTests. */
    private static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }
    }
}
