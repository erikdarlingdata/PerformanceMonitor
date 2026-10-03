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
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fixture reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres, seeds it
   once, and every fact then reads inside it and never writes. */
/// <summary>
/// The store behind <see cref="ViewerCollectionLogDataStartLiveTests"/>: one server per scenario, each holding the collection log
/// runs its Collection Log grid or drill shows. Every time below is measured back from <see cref="End"/>, the minute the store was
/// seeded at, in naive UTC.
/// </summary>
public sealed class CollectionLogDataStartStore : IAsyncLifetime
{
    public const string WaitStats = "wait_stats";
    public const string CpuUtilization = "cpu_utilization";

    /// <summary>Added two days before <see cref="End"/>: its first collection falls inside a week's range.</summary>
    public const int NewServerId = -496801;

    /// <summary>Monitored for months, so the log covers a week's range; its collector ran for the last three days only.</summary>
    public const int QuietServerId = -496802;

    /// <summary>Monitored for months; a run every 5 minutes for the last 20 minutes.</summary>
    public const int ShortServerId = -496803;

    /// <summary>Monitored for months; exactly <see cref="ViewerDataService.CollectionLogRowCap"/> runs, 10 minutes apart.</summary>
    public const int FullPageServerId = -496804;

    /// <summary>Monitored for months; one run fewer than the cap.</summary>
    public const int UnderCapServerId = -496805;

    /// <summary>Monitored for months; 100 runs more than the cap.</summary>
    public const int OverCapServerId = -496806;

    /// <summary>Monitored for months; one collector ran for 30 days, another (the one drilled) for the last three.</summary>
    public const int DrillQuietServerId = -496807;

    /// <summary>Added two days before <see cref="End"/>; one collector from the first collection, the one drilled from a day later.</summary>
    public const int DrillNewServerId = -496808;

    /// <summary>Monitored for months; the drilled collector ran 9 days, 8 days and 1 day before <see cref="End"/>.</summary>
    public const int PinnedDrillServerId = -496809;

    private ScratchPostgres? _scratch;
    private long _nextLogId = 1;

    public ViewerDataService? Viewer { get; private set; }

    /// <summary>The minute the history ends at, naive UTC.</summary>
    public DateTime End { get; private set; } = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

    public static string ServerName(int serverId) => $"clds-{-serverId}";

    public async ValueTask InitializeAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(baseConnectionString))
        {
            return;
        }

        var now = DateTime.UtcNow;
        End = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Unspecified);
        _scratch = await ScratchPostgres.CreateAsync(baseConnectionString, CancellationToken.None);
        await using (var connection = new NpgsqlConnection(_scratch.ConnectionString))
        {
            await connection.OpenAsync();
            await PgMigrations.MigrateAsync(connection, CancellationToken.None);
            await SeedAsync(connection);
        }

        Viewer = new ViewerDataService(_scratch.ConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        if (Viewer is not null)
        {
            await Viewer.DisposeAsync();
        }

        if (_scratch is not null)
        {
            await _scratch.DisposeAsync();
        }
    }

    private async Task SeedAsync(NpgsqlConnection connection)
    {
        var cap = ViewerDataService.CollectionLogRowCap;

        /* New: added two days ago, a run an hour since. */
        await AddServerAsync(connection, NewServerId, End.AddDays(-2));
        await RunsAsync(connection, NewServerId, WaitStats, End.AddDays(-2), End, 60);

        /* Quiet: monitored for months, its collector's first run comes three days back (its log covers a week's range all the same). */
        await AddServerAsync(connection, QuietServerId, End.AddDays(-120));
        await RunsAsync(connection, QuietServerId, WaitStats, End.AddDays(-3), End, 60);

        /* Short: monitored for months, and five runs in the last 20 minutes. */
        await AddServerAsync(connection, ShortServerId, End.AddDays(-120));
        await RunsAsync(connection, ShortServerId, WaitStats, End.AddMinutes(-20), End, 5);

        /* The page: a run every 10 minutes ending at End. Exactly the cap, one under it, and 100 over it. */
        await AddServerAsync(connection, FullPageServerId, End.AddDays(-120));
        await RunsAsync(connection, FullPageServerId, WaitStats, End.AddMinutes(-10 * (cap - 1)), End, 10);

        await AddServerAsync(connection, UnderCapServerId, End.AddDays(-120));
        await RunsAsync(connection, UnderCapServerId, WaitStats, End.AddMinutes(-10 * (cap - 2)), End, 10);

        await AddServerAsync(connection, OverCapServerId, End.AddDays(-120));
        await RunsAsync(connection, OverCapServerId, WaitStats, End.AddMinutes(-10 * (cap + 99)), End, 10);

        /* The drill. Quiet: another collector ran 30 days, the drilled one began three days back. New: the other collector's first
           run is the server's first collection (two days back), the drilled one began a day after it. */
        await AddServerAsync(connection, DrillQuietServerId, End.AddDays(-120));
        await RunsAsync(connection, DrillQuietServerId, CpuUtilization, End.AddDays(-30), End, 60);
        await RunsAsync(connection, DrillQuietServerId, WaitStats, End.AddDays(-3), End, 60);

        await AddServerAsync(connection, DrillNewServerId, End.AddDays(-2));
        await RunsAsync(connection, DrillNewServerId, CpuUtilization, End.AddDays(-2), End, 60);
        await RunsAsync(connection, DrillNewServerId, WaitStats, End.AddDays(-1), End, 60);

        /* Pinned: a run 9 days back and one 8 days back, inside the week that ended 3 days ago and outside the week that ends now. */
        await AddServerAsync(connection, PinnedDrillServerId, End.AddDays(-120));
        await RunsAsync(connection, PinnedDrillServerId, WaitStats, End.AddDays(-9), End.AddDays(-9), 60);
        await RunsAsync(connection, PinnedDrillServerId, WaitStats, End.AddDays(-8), End.AddDays(-8), 60);
        await RunsAsync(connection, PinnedDrillServerId, WaitStats, End.AddDays(-1), End.AddDays(-1), 60);
    }

    private static async Task AddServerAsync(NpgsqlConnection connection, int serverId, DateTime createdUtc)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, ServerName(serverId), CancellationToken.None);
        await DarlingMcpTestData.ExecAsync(
            connection, CancellationToken.None, "UPDATE collect.servers SET created_date = $2 WHERE server_id = $1",
            serverId, DarlingMcpTestData.Naive(createdUtc));
    }

    /* The collector's runs from first to last, every stepMinutes, whether or not anything happened. */
    private async Task RunsAsync(NpgsqlConnection connection, int serverId, string collector, DateTime first, DateTime last, int stepMinutes)
    {
        await DarlingMcpTestData.ExecAsync(
            connection, CancellationToken.None,
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
            + "SELECT $5 + row_number() OVER (ORDER BY t), $1, $2, $6, t, 10 + (row_number() OVER (ORDER BY t) % 50), 'SUCCESS', 0 "
            + $"FROM generate_series($3::timestamp, $4::timestamp, interval '{stepMinutes} minutes') AS t",
            serverId, ServerName(serverId), DarlingMcpTestData.Naive(first), DarlingMcpTestData.Naive(last), _nextLogId, collector);
        _nextLogId += 100_000;
    }
}

/// <summary>
/// The Collection Log surfaces say where their data starts (#4966), over a store: the Collection Health tab's grid and the
/// per-collector drill. Each fact runs the surface's own steps against the seeded store: the coverage probe and the read it starts
/// beside (the tab's banner step, <c>ViewerServerTab.ShowCollectionLogDataStartAsync</c>, or the drill's one call,
/// <c>CollectionLogWindow.ReadDrillAsync</c>), and reads the note off a real banner control. The coverage rule: the later of the
/// server's first collection and the log's retention edge, never a collector's first run. A full page of the cap names its oldest
/// run whatever the store covers. A range of 90 minutes or less makes no probe call.
/// </summary>
/* The banner's time text reads the process-wide display mode, which the culture pin sets (restored in Dispose); the shared
   collection serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerCollectionLogDataStartLiveTests : IClassFixture<CollectionLogDataStartStore>, IDisposable
{
    private readonly CollectionLogDataStartStore _store;
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public ViewerCollectionLogDataStartLiveTests(CollectionLogDataStartStore store) => _store = store;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private DateTime End => _store.End;

    /* The tab's default range: a week ending at the store's last minute. */
    private DateTime Start => End.AddDays(-7);

    private static string Since(DateTime utc) => DataStartBannerReadout.Since(utc);

    private void RequireStore() =>
        Assert.SkipWhen(_store.Viewer is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live Collection Log data-start tests.");

    /// <summary>The probe's answer, the runs the grid shows and the banner text (null when hidden) for the tab's range.</summary>
    private sealed record TabAnswer(DateTime? Coverage, int Rows, string? Banner);

    /// <summary>The drill's runs and its banner text (null when hidden).</summary>
    private sealed record DrillAnswer(List<CollectionLogRow> Rows, string? Banner);

    /// <summary>
    /// What the tab does for the range: the coverage probe starts beside the capped read, and the banner step takes both. The probe
    /// completes before the step runs, so the banner (a WPF control, owned by one thread) is touched on one thread only.
    /// </summary>
    private async Task<TabAnswer> TabAsync(int serverId, DateTime startUtc, DateTime endUtc)
    {
        RequireStore();

        var ct = TestContext.Current.CancellationToken;
        var viewer = _store.Viewer!;
        var probe = viewer.GetCollectionLogDataStartAsync(serverId, startUtc, endUtc, ct);
        var rows = await viewer.GetRecentCollectionLogAsync(serverId, startUtc, endUtc, cancellationToken: ct);
        await probe.WaitAsync(ct);

        string? text = null;
        DataStartBannerReadout.OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            /* Seeded visible, so a no-op cannot pass as a hidden banner. */
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };

            ViewerServerTab.ShowCollectionLogDataStartAsync(banner, probe, startUtc, rows).GetAwaiter().GetResult();

            text = banner.Visibility == Visibility.Visible ? banner.Text : null;
        });
        return new TabAnswer(probe.Result, rows.Count, text);
    }

    /// <summary>
    /// The drill's one call (<c>CollectionLogWindow.ReadDrillAsync</c>) on a thread with a dispatcher, as the window calls it from its
    /// Loaded handler: its awaits resume on the thread that owns the banner.
    /// </summary>
    private DrillAnswer Drill(int serverId, string collector, DateTime? asOfUtc)
    {
        RequireStore();

        var viewer = _store.Viewer!;
        DrillAnswer? answer = null;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            var dispatcher = Dispatcher.CurrentDispatcher;
            dispatcher.BeginInvoke(new Action(async () =>
            {
                try
                {
                    ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
                    var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };
                    var rows = await CollectionLogWindow.ReadDrillAsync(viewer, serverId, collector, banner, asOfUtc);
                    answer = new DrillAnswer(rows, banner.Visibility == Visibility.Visible ? banner.Text : null);
                }
                catch (Exception ex)
                {
                    error = ex;
                }
                finally
                {
                    dispatcher.BeginInvokeShutdown(DispatcherPriority.Background);
                }
            }));
            Dispatcher.Run();
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }

        return answer!;
    }

    // ── The tab's grid ──

    /* The server was added two days into the range, so its log starts there: the note names that first collection. */
    [Fact]
    public async Task ARangeThatStartsBeforeTheFirstCollection_NamesTheFirstCollection()
    {
        var tab = await TabAsync(CollectionLogDataStartStore.NewServerId, Start, End);

        Assert.Equal(End.AddDays(-2), tab.Coverage);
        Assert.Equal(49, tab.Rows);
        Assert.Equal(Since(End.AddDays(-2)), tab.Banner);
    }

    /* The server has been monitored for months and its collector began three days back: the log covers the whole range, and a run
       that comes late in it is a quiet start, not a cut. No note. */
    [Fact]
    public async Task AQuietStart_GivesNoNote()
    {
        var tab = await TabAsync(CollectionLogDataStartStore.QuietServerId, Start, End);

        Assert.NotNull(tab.Coverage);
        Assert.True(tab.Coverage < Start, "the log's coverage starts before the range");
        Assert.Equal(73, tab.Rows);
        Assert.Null(tab.Banner);
    }

    /* A one-hour range asks the probe nothing (90 minutes or less: it answers null without a query), and five runs are a page
       under the cap: no note. The same server over six hours does get an answer from the probe, so the null above is the rule and
       not an empty store. */
    [Fact]
    public async Task AOneHourRange_MakesNoProbeCall_AndAPageUnderTheCapShowsNoNote()
    {
        var hour = await TabAsync(CollectionLogDataStartStore.ShortServerId, End.AddHours(-1), End);

        Assert.Null(hour.Coverage);
        Assert.Equal(5, hour.Rows);
        Assert.Null(hour.Banner);

        var sixHours = await TabAsync(CollectionLogDataStartStore.ShortServerId, End.AddHours(-6), End);

        Assert.NotNull(sixHours.Coverage);
        Assert.Equal(5, sixHours.Rows);
        Assert.Null(sixHours.Banner);
    }

    /* Exactly the cap's worth of runs, 10 minutes apart, ending at End: the read returns a full page, which may have left older runs
       out, so the note names its oldest run although the store covers the range (monitored for months). */
    [Fact]
    public async Task AFullPageOfTheCap_NamesItsOldestRun_EvenWhereTheStoreCoversTheRange()
    {
        var cap = ViewerDataService.CollectionLogRowCap;
        var tab = await TabAsync(CollectionLogDataStartStore.FullPageServerId, Start, End);

        Assert.True(tab.Coverage < Start, "the log's coverage starts before the range");
        Assert.Equal(cap, tab.Rows);
        Assert.Equal(Since(End.AddMinutes(-10 * (cap - 1))), tab.Banner);
    }

    /* One run fewer than the cap is not a full page: the coverage rule applies, the store covers the range, and there is no note. */
    [Fact]
    public async Task APageOneRunUnderTheCap_KeepsTheCoverageRule()
    {
        var tab = await TabAsync(CollectionLogDataStartStore.UnderCapServerId, Start, End);

        Assert.True(tab.Coverage < Start, "the log's coverage starts before the range");
        Assert.Equal(ViewerDataService.CollectionLogRowCap - 1, tab.Rows);
        Assert.Null(tab.Banner);
    }

    /* 100 runs more than the cap are stored; the read keeps the newest page, and the note names the oldest run it RETURNED, not the
       oldest the store holds. */
    [Fact]
    public async Task AFullPageWithOlderRunsBehindIt_NamesTheOldestReturnedRun()
    {
        var cap = ViewerDataService.CollectionLogRowCap;
        var tab = await TabAsync(CollectionLogDataStartStore.OverCapServerId, Start, End);

        Assert.Equal(cap, tab.Rows);
        Assert.Equal(Since(End.AddMinutes(-10 * (cap - 1))), tab.Banner);
    }

    // ── The drill ──

    /* The drilled collector began three days back on a server monitored for months: the week is covered, and a collector that
       ran late in it says nothing. */
    [Fact]
    public void ACollectorThatStartedLateInACoveredWeek_SaysNothing()
    {
        var drill = Drill(CollectionLogDataStartStore.DrillQuietServerId, CollectionLogDataStartStore.WaitStats, asOfUtc: null);

        Assert.Equal(73, drill.Rows.Count);
        Assert.Null(drill.Banner);
    }

    /* The server was added two days back, its first collection (another collector's first run) with it; the drilled collector began
       a day after. The note names the server's first collection, never the drilled collector's first run, for either collector. */
    [Fact]
    public void AServerWhoseFirstCollectionFallsInsideTheWeek_NamesIt_NotTheDrilledCollectorsFirstRun()
    {
        var late = Drill(CollectionLogDataStartStore.DrillNewServerId, CollectionLogDataStartStore.WaitStats, asOfUtc: null);
        var first = Drill(CollectionLogDataStartStore.DrillNewServerId, CollectionLogDataStartStore.CpuUtilization, asOfUtc: null);

        Assert.Equal(25, late.Rows.Count);
        Assert.Equal(End.AddDays(-1), late.Rows.Min(r => r.CollectionTime));
        Assert.Equal(Since(End.AddDays(-2)), late.Banner);

        Assert.Equal(49, first.Rows.Count);
        Assert.Equal(Since(End.AddDays(-2)), first.Banner);
    }

    /* The drill's end is pinned three days back, so its week runs from ten days back to three. A run 9 days back and one 8 days back
       are inside that week and outside the week that ends now: the read takes the probe's window, so both are in the rows. The run a
       day back is after the week's end, so the read leaves it out (#4966: a read with no end listed it). */
    [Fact]
    public void APinnedDrillWindow_ReadsTheProbesWindow_FromItsStartToItsEnd()
    {
        var drill = Drill(CollectionLogDataStartStore.PinnedDrillServerId, CollectionLogDataStartStore.WaitStats, asOfUtc: End.AddDays(-3));

        Assert.Equal(new[] { End.AddDays(-8), End.AddDays(-9) }, drill.Rows.Select(r => r.CollectionTime).ToArray());
        Assert.Null(drill.Banner);
    }
}
