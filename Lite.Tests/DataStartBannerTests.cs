/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Lite twin of #4953's "where the data starts" notice: Active Queries (<c>query_snapshots</c>) and Current
/// Waits (<c>waiting_tasks</c>) say where their stored rows start when a window or custom range reaches further
/// back than the store holds (retention, the 3-month archive's first month, or a server added recently), through
/// the ONE shared probe (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>) and the ONE banner step
/// (<see cref="ServerTab.ApplyWindowFloorToBanner"/>) the Queries grids already use. A server that holds a row
/// before the window has had the window served whole (the probe answers the requested start), so a quiet start
/// inside the window, with older rows in the store, raises no banner.
/// </summary>
[Collection("server-time-helper")]
public sealed class DataStartBannerTests : IDisposable
{
    private const int ServerId = 4242;
    private const string ServerName = "DataStartServer";
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public DataStartBannerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "DataStartBanner_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    private async Task SeedSnapshotAsync(DateTime at)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, cpu_time_ms, total_elapsed_time_ms)
VALUES ($1, $2, $3, $4, 55, 'Db', 'SELECT 1', 'running', 10, 20)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedWaitingTaskAsync(DateTime at)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, database_name)
VALUES ($1, $2, $3, $4, 55, 'LCK_M_X', 3000, 60, 'Db')";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// The collector's runs in collection_log, every <paramref name="everyMinutes"/> minutes from
    /// <paramref name="firstUtc"/> to <paramref name="lastUtc"/>, whether or not anything waited or ran.
    /// </summary>
    private async Task SeedLogRunsAsync(string collector, DateTime firstUtc, DateTime lastUtc, int everyMinutes)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + row_number() OVER (), $2, $3, $4, g.t, 12, 'SUCCESS', 0
FROM generate_series($5::TIMESTAMP, $6::TIMESTAMP, INTERVAL {everyMinutes} MINUTE) AS g(t)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(firstUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(lastUtc) });
        _nextId += await cmd.ExecuteNonQueryAsync() + 1;
    }

    /// <summary>The probe, then the banner step the surface runs on the result: (visible, text).</summary>
    private async Task<(bool Visible, string Text)> BannerForAsync(QueryWindowRelation relation, DateTime startUtc, DateTime endUtc)
    {
        var floor = await new LocalDataService(_duckDb).GetQueryWindowFloorAsync(relation, ServerId, startUtc, endUtc);
        return OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            ServerTab.ApplyWindowFloorToBanner(banner, floor, startUtc, TimeZoneInfo.Utc);
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text);
        });
    }

    /// <summary>
    /// The relation list is a closed enum, so no view name ever comes from the caller. Each member must name a real
    /// archive view (<c>v_</c> plus a table in the archivable set) whose time column is the one
    /// <see cref="LocalDataService.QueryWindowRelationTimeColumn"/> names for it (<c>collection_time</c>, or
    /// <c>capture_time</c> on the three config snapshots, #4966), with a <c>server_id</c> column, which is what the
    /// probe's SQL assumes. The archive purges each table by that same column, so the probe's window and the
    /// retention edge are measured on one clock.
    /// </summary>
    [Fact]
    public async Task EveryRelation_NamesARealArchiveView_WithServerIdAndItsTimeColumn()
    {
        await _duckDb.InitializeAsync();
        foreach (var relation in Enum.GetValues<QueryWindowRelation>())
        {
            var view = LocalDataService.QueryWindowRelationView(relation);
            Assert.StartsWith("v_", view, StringComparison.Ordinal);
            var table = view[2..];
            var timeColumn = LocalDataService.QueryWindowRelationTimeColumn(relation);
            Assert.Contains(table, DuckDbInitializer.ArchivableTables);
            Assert.Contains(ArchiveService.ArchivableTables, t => t.Table == table && t.TimeColumn == timeColumn);

            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            using var readLock = _duckDb.AcquireReadLock();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{view}' AND column_name IN ('server_id', '{timeColumn}')";
            Assert.Equal(2L, Convert.ToInt64(await cmd.ExecuteScalarAsync()));
        }
    }

    /// <summary>A custom range that starts before the oldest stored snapshot gets the banner, worded at that oldest snapshot.</summary>
    [Fact]
    public async Task ActiveQueries_RangeStartsBeforeTheOldestStoredSnapshot_ShowsTheBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var oldest = end.AddDays(-2);
        await SeedSnapshotAsync(oldest);
        await SeedSnapshotAsync(end.AddHours(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.QuerySnapshots, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.StartsWith("Showing since ", text, StringComparison.Ordinal);
        Assert.Contains(Naive(oldest).ToString("yyyy-MM-dd HH:"), text, StringComparison.Ordinal);
    }

    /// <summary>Same for Current Waits (<c>waiting_tasks</c>), which holds a row only while something waits.</summary>
    [Fact]
    public async Task CurrentWaits_RangeStartsBeforeTheOldestStoredRow_ShowsTheBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedWaitingTaskAsync(end.AddDays(-2));
        await SeedWaitingTaskAsync(end.AddHours(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.WaitingTasks, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.StartsWith("Showing since ", text, StringComparison.Ordinal);
    }

    /// <summary>A window the stored snapshots fully cover (rows reach back past its start) shows no banner.</summary>
    [Fact]
    public async Task ActiveQueries_WindowInsideTheStoredRows_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        for (var day = 10; day >= 0; day--)
        {
            await SeedSnapshotAsync(end.AddDays(-day).AddMinutes(-5));
        }

        var (visible, text) = await BannerForAsync(QueryWindowRelation.QuerySnapshots, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// The quiet-start guard on the sparse table: one old wait row, then nothing until two hours ago. The window's
    /// own oldest row sits far past its start while the store holds older rows, so nothing is missing and there is no banner.
    /// </summary>
    [Fact]
    public async Task CurrentWaits_QuietStartInsideTheWindow_WithOlderRowsBeforeIt_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedWaitingTaskAsync(end.AddDays(-27));
        await SeedWaitingTaskAsync(end.AddHours(-2));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.WaitingTasks, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// A server monitored for months and idle overnight, whose older rows are gone: the 7-day window's first waiting
    /// task comes 5 hours after its start and no older row exists. Its collector ran all along, so the window is
    /// covered: no banner.
    /// </summary>
    [Fact]
    public async Task CurrentWaits_IdleStart_WithNoOlderRow_OnAServerMonitoredForMonths_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync("waiting_tasks", end.AddDays(-9), end, 30);
        await SeedWaitingTaskAsync(end.AddDays(-7).AddHours(5));
        await SeedWaitingTaskAsync(end.AddHours(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.WaitingTasks, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// A server collected for 30 days whose first waiting task ever came 3 days ago: a 7-day window is covered, so
    /// there is no banner. Active Queries reads the same way through its own collector's runs.
    /// </summary>
    [Fact]
    public async Task FirstRowThreeDaysAgo_OnAServerCollectedForThirtyDays_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync("waiting_tasks", end.AddDays(-30), end, 360);
        await SeedLogRunsAsync("query_snapshots", end.AddDays(-30), end, 360);
        await SeedWaitingTaskAsync(end.AddDays(-3));
        await SeedSnapshotAsync(end.AddDays(-3));

        Assert.False((await BannerForAsync(QueryWindowRelation.WaitingTasks, end.AddDays(-7), end)).Visible);
        Assert.False((await BannerForAsync(QueryWindowRelation.QuerySnapshots, end.AddDays(-7), end)).Visible);
    }

    /// <summary>
    /// A server added 2 days ago, whose first waiting task came a day later: a 7-day window starts before its
    /// coverage, and the banner names where coverage starts (its first collection), not its first row.
    /// </summary>
    [Fact]
    public async Task CurrentWaits_ServerAddedTwoDaysAgo_SaysSinceItsFirstCollection_NotItsFirstRow()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var added = end.AddDays(-2);
        await SeedLogRunsAsync("waiting_tasks", added, end, 30);
        await SeedWaitingTaskAsync(end.AddDays(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.WaitingTasks, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Equal("Showing since " + PerformanceMonitor.Ui.DisplayZone.Format(Naive(added), TimeZoneInfo.Utc, "yyyy-MM-dd HH:mm:ss"), text);
    }

    /// <summary>A server with no row in the window at all has nothing to say about where its data starts: no banner.</summary>
    [Fact]
    public async Task ActiveQueries_NoSnapshotInTheWindow_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedSnapshotAsync(end.AddDays(-20));

        var (visible, _) = await BannerForAsync(QueryWindowRelation.QuerySnapshots, end.AddDays(-7), end);

        Assert.False(visible);
    }

    /// <summary>
    /// Source pins for the wiring: each surface has its banner TextBlock, and each read path of the surface (the
    /// sub-tab switch and the full refresh, and for Active Queries the slicer handler) refreshes it through the
    /// shared helper over the UTC window the grid read. Text-scans SOURCE, like QueryWindowTruncationTests.
    /// </summary>
    [Fact]
    public void ActiveQueriesAndCurrentWaits_AreWiredThroughTheSharedBannerHelper()
    {
        var xaml = File.ReadAllText(ControlsFile("ServerTab.xaml"));
        Assert.Contains("x:Name=\"ActiveQueriesWindowTruncatedBanner\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"CurrentWaitsWindowTruncatedBanner\"", xaml, StringComparison.Ordinal);

        var refreshSource = File.ReadAllText(ControlsFile("ServerTab.Refresh.cs"));
        var slicersSource = File.ReadAllText(ControlsFile("ServerTab.Slicers.cs"));

        Assert.Equal(2, Regex.Matches(refreshSource,
            @"RefreshWindowTruncatedBannerAsync\(QueryWindowRelation\.QuerySnapshots,\s*ActiveQueriesWindowTruncatedBanner,\s*windowStart\d?,\s*windowEnd\d?\)").Count);
        Assert.Equal(2, Regex.Matches(refreshSource,
            @"RefreshWindowTruncatedBannerAsync\(QueryWindowRelation\.WaitingTasks,\s*CurrentWaitsWindowTruncatedBanner,\s*windowStart\d?,\s*windowEnd\d?\)").Count);
        Assert.Single(Regex.Matches(slicersSource,
            @"RefreshWindowTruncatedBannerAsync\(QueryWindowRelation\.QuerySnapshots,\s*ActiveQueriesWindowTruncatedBanner,\s*e\.StartUtc,\s*e\.EndUtc\)"));
    }

    /// <summary>
    /// The two drill-downs that open Active Queries on a narrow window (the chart and Overview "Show Active Queries at
    /// This Time" drill, and the heatmap drill) load the grid for that window, so they refresh the "Showing since" banner for
    /// it too: left alone it keeps describing the last range read, and can claim a cut the drill window does not have or
    /// miss one it does. The banner takes the SAME pair the grid read takes. That pair is naive UTC end to end:
    /// <c>GetDrillWindow</c> builds it from the clicked UTC instant (#4766), and <c>GetLatestQuerySnapshotsAsync</c> hands a
    /// custom range straight through <c>GetTimeRange</c> to UTC <c>collection_time</c>, as the slicer handler does with
    /// <c>e.StartUtc</c>/<c>e.EndUtc</c>, so nothing between them converts through the server's clock. The refresh follows
    /// the bind, as the range read's does. Comments are stripped first, so a sentence that names the call cannot satisfy
    /// the pin.
    /// </summary>
    [Theory]
    [InlineData("private async void OnActiveQueriesDrillDown(")]
    [InlineData("private async void OnHeatmapDrillDown(")]
    public void ActiveQueriesDrillDowns_RefreshTheBannerForTheDrillWindow_AfterTheRowsAreBound(string signature)
    {
        var lf = File.ReadAllText(ControlsFile("ServerTab.DrillDown.cs")).Replace("\r\n", "\n");
        var code = Regex.Replace(Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"//[^\n]*", string.Empty);
        var start = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{signature}' is no longer in ServerTab.DrillDown.cs; update this pin.");
        var end = code.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the end of '{signature}' was not found.");
        var body = code[start..end];

        var read = Regex.Match(body, @"GetLatestQuerySnapshotsAsync\(_serverId,\s*0,\s*(?<from>\w+),\s*(?<to>\w+)\)");
        Assert.True(read.Success, $"'{signature}' no longer reads the grid through GetLatestQuerySnapshotsAsync(_serverId, 0, from, to); update this pin.");
        var bound = body.IndexOf("_querySnapshotsFilterMgr!.UpdateData(snapshots);", StringComparison.Ordinal);
        Assert.True(bound > read.Index, $"'{signature}' no longer binds the rows it read; update this pin.");

        var banner = Regex.Matches(body,
            @"await RefreshWindowTruncatedBannerAsync\(QueryWindowRelation\.QuerySnapshots,\s*ActiveQueriesWindowTruncatedBanner,\s*"
            + Regex.Escape(read.Groups["from"].Value) + @",\s*" + Regex.Escape(read.Groups["to"].Value) + @"\);");
        Assert.True(banner.Count == 1,
            $"'{signature}' must refresh the Active Queries banner exactly once over the pair its grid read takes ("
            + read.Groups["from"].Value + ", " + read.Groups["to"].Value + "); found " + banner.Count);
        Assert.True(banner[0].Index > bound, $"'{signature}' must refresh the banner after the rows are bound, as the range read does");
    }

    /// <summary>
    /// The Live Snapshot button swaps the Active Queries grid for the rows the server returns now, so the "Showing since"
    /// banner the last range read raised no longer describes the grid. The handler collapses it and clears its text through
    /// the shared step (<see cref="ServerTab.SetWindowTruncatedBanner"/>, whose not-truncated state
    /// <c>QueryWindowTruncationTests</c> drives) as the live rows are bound, and not before, so a live read that fails,
    /// which leaves the range rows in the grid, keeps the banner that still describes them. The next range read raises
    /// the banner again through the shared helper (the pin above).
    /// </summary>
    [Fact]
    public void LiveSnapshot_HidesTheActiveQueriesBanner_AsTheLiveRowsAreBound_AndNotWhenTheReadFails()
    {
        var source = File.ReadAllText(ControlsFile("ServerTab.xaml.cs"));
        var handler = Regex.Match(source, @"private async void LiveSnapshot_Click\(.*?\r?\n    \}\r?\n", RegexOptions.Singleline);
        Assert.True(handler.Success, "LiveSnapshot_Click was not found in ServerTab.xaml.cs");
        var body = handler.Value;

        var bound = body.IndexOf("_querySnapshotsFilterMgr!.UpdateData(results);", StringComparison.Ordinal);
        var failed = body.IndexOf("catch (Exception ex)", StringComparison.Ordinal);
        Assert.True(bound >= 0, "LiveSnapshot_Click no longer binds the live rows through _querySnapshotsFilterMgr.UpdateData(results)");
        Assert.True(failed > bound, "LiveSnapshot_Click no longer has its failure arm after the rows are bound");

        var hides = Regex.Matches(body, @"SetWindowTruncatedBanner\(ActiveQueriesWindowTruncatedBanner,\s*truncated:\s*false,");
        Assert.True(hides.Count == 1,
            "LiveSnapshot_Click must hide the Active Queries banner exactly once (found " + hides.Count + "): the live rows replace the range rows the banner described");
        Assert.True(hides[0].Index > bound && hides[0].Index < failed,
            "the banner must come down after the live rows are bound and before the failure arm, so a failed live read keeps it");
    }

    /// <summary>WPF objects require STA; same shape as QueryWindowTruncationTests.</summary>
    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }

        return result;
    }

    private static string ControlsFile(string name) =>
        Path.GetFullPath(Path.Combine(ControlsDir(), name));

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));
}
