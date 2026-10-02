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
    /// archive view (<c>v_</c> plus a table in the archivable set) whose time column is <c>collection_time</c>, with
    /// a <c>server_id</c> column, which is what the probe's SQL assumes.
    /// </summary>
    [Fact]
    public async Task EveryRelation_NamesARealArchiveView_WithServerIdAndCollectionTime()
    {
        await _duckDb.InitializeAsync();
        foreach (var relation in Enum.GetValues<QueryWindowRelation>())
        {
            var view = LocalDataService.QueryWindowRelationView(relation);
            Assert.StartsWith("v_", view, StringComparison.Ordinal);
            var table = view[2..];
            Assert.Contains(table, DuckDbInitializer.ArchivableTables);
            Assert.Contains(ArchiveService.ArchivableTables, t => t.Table == table && t.TimeColumn == "collection_time");

            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            using var readLock = _duckDb.AcquireReadLock();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{view}' AND column_name IN ('server_id', 'collection_time')";
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
