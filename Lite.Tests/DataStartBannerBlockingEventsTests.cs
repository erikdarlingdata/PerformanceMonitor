/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The "where the data starts" notice (#4966) on the Blocking tab's two list surfaces: the Blocked Process Reports
/// grid (<c>blocked_process_reports</c>, collector <c>blocked_process_report</c>) and the Deadlocks grid
/// (<c>deadlocks</c>, collector <c>deadlocks</c>). Both go through the ONE shared probe
/// (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>) and the ONE banner step
/// (<see cref="ServerTab.ApplyWindowFloorToBanner"/>) the Queries grids, Active Queries and Current Waits use. Both
/// tables hold a row only when something happens (a block past the threshold, a deadlock), so coverage comes from
/// the collector's runs in <c>collection_log</c>, not from the oldest row: a server that was collected over the whole
/// window and had no event near its start has no gap to disclose.
/// </summary>
[Collection("server-time-helper")]
public sealed class DataStartBannerBlockingEventsTests : IDisposable
{
    private const int ServerId = 4343;
    private const string ServerName = "BlockingEventsServer";
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public DataStartBannerBlockingEventsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "DataStartBannerBlocking_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    /// <summary>The collector whose runs the relation's coverage is read from.</summary>
    private static string CollectorOf(QueryWindowRelation relation) => relation switch
    {
        QueryWindowRelation.BlockedProcessReports => "blocked_process_report",
        QueryWindowRelation.Deadlocks => "deadlocks",
        _ => throw new ArgumentOutOfRangeException(nameof(relation))
    };

    /// <summary>One stored event row (a blocked process report, or a deadlock) collected at <paramref name="at"/>.</summary>
    private async Task SeedEventAsync(QueryWindowRelation relation, DateTime at)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = relation == QueryWindowRelation.BlockedProcessReports
            ? @"
INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time)
VALUES ($1, $2, $3, $4, $2)"
            : @"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time)
VALUES ($1, $2, $3, $4, $2)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// The collector's runs in collection_log, every <paramref name="everyMinutes"/> minutes from
    /// <paramref name="firstUtc"/> to <paramref name="lastUtc"/>, whether or not anything was captured.
    /// </summary>
    private async Task SeedLogRunsAsync(QueryWindowRelation relation, DateTime firstUtc, DateTime lastUtc, int everyMinutes)
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
        cmd.Parameters.Add(new DuckDBParameter { Value = CollectorOf(relation) });
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

    private static string SinceText(DateTime instant) =>
        "Showing since " + PerformanceMonitor.Ui.DisplayZone.Format(Naive(instant), TimeZoneInfo.Utc, "yyyy-MM-dd HH:mm:ss");

    /// <summary>The two relations are in the probe's closed maps, with the collector the grid's table is filled by.</summary>
    [Fact]
    public void BlockingEventRelations_NameTheirArchiveViewAndCollector()
    {
        Assert.Equal("v_blocked_process_reports", LocalDataService.QueryWindowRelationView(QueryWindowRelation.BlockedProcessReports));
        Assert.Equal("v_deadlocks", LocalDataService.QueryWindowRelationView(QueryWindowRelation.Deadlocks));
        Assert.Equal("blocked_process_report", LocalDataService.QueryWindowRelationCollector(QueryWindowRelation.BlockedProcessReports));
        Assert.Equal("deadlocks", LocalDataService.QueryWindowRelationCollector(QueryWindowRelation.Deadlocks));
    }

    /// <summary>Rows that start inside the range: a 7-day range whose oldest stored event is 2 days old shows the banner, worded at that event.</summary>
    [Theory]
    [InlineData(QueryWindowRelation.BlockedProcessReports)]
    [InlineData(QueryWindowRelation.Deadlocks)]
    public async Task RangeStartsBeforeTheOldestStoredRow_ShowsTheBanner(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var oldest = end.AddDays(-2);
        await SeedEventAsync(relation, oldest);
        await SeedEventAsync(relation, end.AddHours(-1));

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.StartsWith("Showing since ", text, StringComparison.Ordinal);
        Assert.Contains(Naive(oldest).ToString("yyyy-MM-dd HH:"), text, StringComparison.Ordinal);
    }

    /// <summary>
    /// A server added 2 days ago whose first event came a day later: the 7-day range starts before its coverage, and
    /// the banner names where coverage starts (its first collection), not its first event.
    /// </summary>
    [Theory]
    [InlineData(QueryWindowRelation.BlockedProcessReports)]
    [InlineData(QueryWindowRelation.Deadlocks)]
    public async Task ServerAddedTwoDaysAgo_SaysSinceItsFirstCollection_NotItsFirstRow(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var added = end.AddDays(-2);
        await SeedLogRunsAsync(relation, added, end, 30);
        await SeedEventAsync(relation, end.AddDays(-1));

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Equal(SinceText(added), text);
    }

    /// <summary>
    /// The quiet-start guard: the collector ran across the whole range (and for two days before it), and the first
    /// event inside the range came 5 hours after its start. The store covered the range, so there is no banner.
    /// </summary>
    [Theory]
    [InlineData(QueryWindowRelation.BlockedProcessReports)]
    [InlineData(QueryWindowRelation.Deadlocks)]
    public async Task QuietStart_StoreCoveredTheRange_FirstRowComesLate_ShowsNoBanner(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync(relation, end.AddDays(-9), end, 30);
        await SeedEventAsync(relation, end.AddDays(-7).AddHours(5));
        await SeedEventAsync(relation, end.AddHours(-1));

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// The same guard through a row alone: one old event, then nothing until two hours ago. The range's own first
    /// event sits far past its start while the store holds an older one, so nothing is missing.
    /// </summary>
    [Theory]
    [InlineData(QueryWindowRelation.BlockedProcessReports)]
    [InlineData(QueryWindowRelation.Deadlocks)]
    public async Task QuietStart_WithAnOlderRowBeforeTheRange_ShowsNoBanner(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedEventAsync(relation, end.AddDays(-27));
        await SeedEventAsync(relation, end.AddHours(-2));

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>A server with no event and no run of the collector in the range has nothing to say about where its data starts.</summary>
    [Theory]
    [InlineData(QueryWindowRelation.BlockedProcessReports)]
    [InlineData(QueryWindowRelation.Deadlocks)]
    public async Task NothingInTheRange_ShowsNoBanner(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedEventAsync(relation, end.AddDays(-20));

        var (visible, _) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.False(visible);
    }

    /// <summary>The two XAML banners exist, one per grid, next to the grid they describe.</summary>
    [Fact]
    public void BothBanners_AreDeclaredInTheBlockingSubTabs()
    {
        var xaml = File.ReadAllText(ControlsFile("ServerTab.xaml"));
        Assert.Contains("x:Name=\"BlockedProcessReportsWindowTruncatedBanner\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"DeadlocksWindowTruncatedBanner\"", xaml, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every read path of the two grids refreshes the banner through the shared helper, over the UTC window that
    /// path's grid read takes, AFTER the rows are bound: the sub-tab switch and the full refresh
    /// (<c>GetQueriesTabWindowUtc</c>, the same <c>GetTimeRange</c> pair the grid reads resolve), the slicer handler
    /// (<c>e.StartUtc</c>/<c>e.EndUtc</c>) and the chart drill-down (its own naive-UTC pair). Text-scans SOURCE with
    /// comments stripped, so a sentence that names the call cannot satisfy the pin.
    /// </summary>
    [Theory]
    [InlineData("BlockedProcessReports", "BlockedProcessReportsWindowTruncatedBanner", "_blockedProcessFilterMgr!.UpdateData(", "OnBlockingSlicerChanged", "OnBlockingDrillDown",
        @"LocalDataService\.BlockedProcessReportGridCap,\s*BlockedProcessRowTimeUtc")]
    [InlineData("Deadlocks", "DeadlocksWindowTruncatedBanner", "_deadlockFilterMgr!.UpdateData(", "OnDeadlockSlicerChanged", "OnDeadlockDrillDown",
        @"LocalDataService\.DeadlockGridCap,\s*DeadlockRowTimeUtc")]
    public void EveryReadPath_RefreshesTheBanner_OverTheWindowItRead_AfterTheRowsAreBound(
        string relation, string banner, string bindCall, string slicerHandler, string drillHandler, string capAndRowTime)
    {
        /* #4966: both grids read a capped page, so every path goes through the cap-aware step with the rows the grid shows, the
           ONE constant that is the read's cap, and the time each row is capped on. */
        var call = @"await RefreshCappedGridBannerAsync\(QueryWindowRelation\." + relation + @",\s*" + banner + @",\s*";
        var rowsCapAndTime = @",\s*[\w.]+,\s*" + capAndRowTime + @"[^;]*\);";

        /* The slicer handler: its own e.StartUtc / e.EndUtc, after the bind. */
        var slicer = MethodBody(Code("ServerTab.Slicers.cs"), "private async void " + slicerHandler + "(");
        var slicerCall = Regex.Matches(slicer, call + @"e\.StartUtc,\s*e\.EndUtc" + rowsCapAndTime);
        Assert.True(slicerCall.Count == 1, $"{slicerHandler} must refresh the {relation} banner exactly once over e.StartUtc, e.EndUtc; found {slicerCall.Count}");
        Assert.True(slicerCall[0].Index > slicer.IndexOf(bindCall, StringComparison.Ordinal) && slicer.Contains(bindCall, StringComparison.Ordinal),
            $"{slicerHandler} must refresh the banner after it binds the rows");

        /* The drill-down: the pair its grid read takes, after the bind. */
        var drill = MethodBody(Code("ServerTab.DrillDown.cs"), "private async void " + drillHandler + "(");
        var drillCall = Regex.Matches(drill, call + @"fromDate,\s*toDate" + rowsCapAndTime);
        Assert.True(drillCall.Count == 1, $"{drillHandler} must refresh the {relation} banner exactly once over fromDate, toDate; found {drillCall.Count}");
        Assert.True(drillCall[0].Index > drill.IndexOf(bindCall, StringComparison.Ordinal) && drill.Contains(bindCall, StringComparison.Ordinal),
            $"{drillHandler} must refresh the banner after it binds the rows");

        /* The sub-tab switch and the full refresh: each over GetQueriesTabWindowUtc's pair, after its bind. */
        var refresh = MethodBody(Code("ServerTab.Refresh.cs"), "private async System.Threading.Tasks.Task RefreshBlockingAsync(");
        var refreshCalls = Regex.Matches(refresh, call + @"(?<s>windowStart\w*),\s*(?<e>windowEnd\w*)" + rowsCapAndTime);
        Assert.True(refreshCalls.Count == 2, $"RefreshBlockingAsync must refresh the {relation} banner twice (sub-tab switch, full refresh); found {refreshCalls.Count}");
        foreach (Match refreshCall in refreshCalls)
        {
            /* The pair is the one the grid read resolves: GetQueriesTabWindowUtc is GetTimeRange(hoursBack, fromDate, toDate, null). */
            var declared = $"var ({refreshCall.Groups["s"].Value}, {refreshCall.Groups["e"].Value}) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);";
            var at = refresh.IndexOf(declared, StringComparison.Ordinal);
            Assert.True(at >= 0 && at < refreshCall.Index, $"RefreshBlockingAsync must declare '{declared}' before the {relation} banner call that uses it");
        }

        var firstBind = refresh.IndexOf(bindCall, StringComparison.Ordinal);
        var secondBind = refresh.IndexOf(bindCall, firstBind + 1, StringComparison.Ordinal);
        Assert.True(firstBind >= 0 && secondBind > firstBind, $"RefreshBlockingAsync no longer binds the {relation} grid twice; update this pin.");
        Assert.True(refreshCalls[0].Index > firstBind && refreshCalls[0].Index < secondBind, "the sub-tab switch must refresh the banner after its own bind");
        Assert.True(refreshCalls[1].Index > secondBind, "the full refresh must refresh the banner after its own bind");
    }

    /// <summary>The source with block and line comments removed, LF line ends.</summary>
    private static string Code(string name)
    {
        var lf = File.ReadAllText(ControlsFile(name)).Replace("\r\n", "\n");
        return Regex.Replace(Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"//[^\n]*", string.Empty);
    }

    /// <summary>From <paramref name="signature"/> to the method's closing brace (a line holding only four spaces and a brace).</summary>
    private static string MethodBody(string code, string signature)
    {
        var start = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{signature}' is no longer in the source; update this pin.");
        var end = code.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the end of '{signature}' was not found.");
        return code[start..end];
    }

    /// <summary>WPF objects require STA; same shape as DataStartBannerTests.</summary>
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
