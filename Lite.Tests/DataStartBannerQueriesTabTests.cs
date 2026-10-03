/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
/// #4966: "where the data starts" on the Queries tab's Plan Corrections grid and Query Heatmap, and the result for
/// every other surface in the same group that carries no notice. Plan Corrections reads <c>v_plan_correction</c>,
/// which holds a row only while the engine has a recommendation, so its coverage comes from the
/// <c>plan_correction</c> collector's runs, like Active Queries and Current Waits. The heatmap reads
/// <c>v_query_stats</c>, the table behind the Top Queries grid, so it asks the same
/// <see cref="QueryWindowRelation.QueryStats"/> question. Both go through the ONE probe
/// (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>) and the ONE banner step
/// (<see cref="ServerTab.ApplyWindowFloorToBanner"/>). A quiet start (the store covered the range, its first row comes
/// late) shows no banner.
/// </summary>
[Collection("server-time-helper")]
public sealed class DataStartBannerQueriesTabTests : IDisposable
{
    private const int ServerId = 4343;
    private const string ServerName = "QueriesTabServer";
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public DataStartBannerQueriesTabTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "DataStartQueriesTab_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    private async Task SeedPlanCorrectionAsync(DateTime at)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state, score)
VALUES ($1, $2, $3, $4, 'Db', 'PR_1', 'Active', 50)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedQueryStatsAsync(DateTime at)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, 'Db', '0xAB', '0xH0xAB', $2, 10, 5000, 5000, 'SELECT 1')";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>The collector's runs in collection_log, every <paramref name="everyMinutes"/> minutes, whether or not anything was stored.</summary>
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

    /// <summary>A range that starts before the oldest stored plan correction, with no run logged before it, gets the banner worded at that row.</summary>
    [Fact]
    public async Task PlanCorrections_RangeStartsBeforeTheOldestStoredRow_ShowsTheBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var oldest = end.AddDays(-2);
        await SeedPlanCorrectionAsync(oldest);
        await SeedPlanCorrectionAsync(end.AddHours(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.PlanCorrection, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.StartsWith("Showing since ", text, StringComparison.Ordinal);
        Assert.Contains(Naive(oldest).ToString("yyyy-MM-dd HH:"), text, StringComparison.Ordinal);
    }

    /// <summary>
    /// A server added 2 days ago whose first recommendation came a day later: a 7-day range starts before its
    /// coverage, and the banner names where coverage starts (the collector's first run), not the first row.
    /// </summary>
    [Fact]
    public async Task PlanCorrections_ServerAddedTwoDaysAgo_SaysSinceItsFirstCollection_NotItsFirstRow()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var added = end.AddDays(-2);
        await SeedLogRunsAsync("plan_correction", added, end, 30);
        await SeedPlanCorrectionAsync(end.AddDays(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.PlanCorrection, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Equal("Showing since " + PerformanceMonitor.Ui.DisplayZone.Format(Naive(added), TimeZoneInfo.Utc, "yyyy-MM-dd HH:mm:ss"), text);
    }

    /// <summary>
    /// The quiet-start guard: the collector ran for 30 days and the engine's first recommendation in all that time
    /// came 3 days ago, so a 7-day range is covered whole and nothing is missing. No banner.
    /// </summary>
    [Fact]
    public async Task PlanCorrections_QuietStart_OnAServerCollectedForThirtyDays_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync("plan_correction", end.AddDays(-30), end, 360);
        await SeedPlanCorrectionAsync(end.AddDays(-3));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.PlanCorrection, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>The same guard with only stored rows to go on: an older row before the range proves it was served whole.</summary>
    [Fact]
    public async Task PlanCorrections_QuietStartInsideTheRange_WithAnOlderRowBeforeIt_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedPlanCorrectionAsync(end.AddDays(-27));
        await SeedPlanCorrectionAsync(end.AddHours(-2));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.PlanCorrection, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>A server with no plan correction and no logged run in the range has nothing to say about where its data starts.</summary>
    [Fact]
    public async Task PlanCorrections_NoRowAndNoRunInTheRange_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedPlanCorrectionAsync(end.AddDays(-20));

        var (visible, _) = await BannerForAsync(QueryWindowRelation.PlanCorrection, end.AddDays(-7), end);

        Assert.False(visible);
    }

    /// <summary>A range that starts before the oldest stored query_stats row gets the banner the heatmap shows.</summary>
    [Fact]
    public async Task QueryHeatmap_RangeStartsBeforeTheOldestStoredRow_ShowsTheBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var oldest = end.AddDays(-2);
        await SeedQueryStatsAsync(oldest);
        await SeedQueryStatsAsync(end.AddHours(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.QueryStats, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.StartsWith("Showing since ", text, StringComparison.Ordinal);
        Assert.Contains(Naive(oldest).ToString("yyyy-MM-dd HH:"), text, StringComparison.Ordinal);
    }

    /// <summary>The quiet start: a row before the range proves it was served whole, however late the first row inside it comes.</summary>
    [Fact]
    public async Task QueryHeatmap_QuietStartInsideTheRange_WithAnOlderRowBeforeIt_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedQueryStatsAsync(end.AddDays(-27));
        await SeedQueryStatsAsync(end.AddHours(-2));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.QueryStats, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// Why the heatmap carries a notice and a time-axis chart does not: its columns are the 5-minute buckets that
    /// hold a row, so five empty days at the start of a 7-day range produce no column at all.
    /// </summary>
    [Fact]
    public async Task QueryHeatmap_DrawsNoColumnForAnEmptyStretchBeforeTheFirstStoredRow()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedQueryStatsAsync(end.AddDays(-2));
        await SeedQueryStatsAsync(end.AddHours(-1));

        var result = await new LocalDataService(_duckDb).GetQueryHeatmapAsync(ServerId, HeatmapMetric.Duration, hoursBack: 7 * 24);

        Assert.Equal(2, result.TimeBuckets.Length);
        Assert.True(result.TimeBuckets[0] > end.AddDays(-3), "the first column is the first stored bucket, not the start of the range");
    }

    /// <summary>
    /// The two surfaces have their banner TextBlocks, collapsed until a read raises them, and the two helpers hand the
    /// shared step the SAME UTC pair the surface's read takes (<c>GetQueriesTabWindowUtc</c>, #4279) with the right
    /// relation: Plan Corrections its own, the heatmap the Top Queries relation, because both read query_stats.
    /// </summary>
    [Fact]
    public void BothSurfaces_HaveTheirBanner_AndTheirHelpersTakeTheGridsUtcWindow()
    {
        var xaml = File.ReadAllText(ControlsFile("ServerTab.xaml"));
        Assert.Matches(@"<TextBlock[^>]*x:Name=""PlanCorrectionsWindowTruncatedBanner""[^>]*Visibility=""Collapsed""", xaml);
        Assert.Matches(@"<TextBlock[^>]*x:Name=""QueryHeatmapWindowTruncatedBanner""[^>]*Visibility=""Collapsed""", xaml);

        var helpers = CodeOf("ServerTab.QueriesDataStart.cs");
        Assert.Matches(
            @"RefreshPlanCorrectionsBannerAsync\(int hoursBack, DateTime\? fromDate, DateTime\? toDate\)\s*\{\s*var \(windowStart, windowEnd\) = LocalDataService\.GetQueriesTabWindowUtc\(hoursBack, fromDate, toDate\);\s*return RefreshWindowTruncatedBannerAsync\(QueryWindowRelation\.PlanCorrection, PlanCorrectionsWindowTruncatedBanner, windowStart, windowEnd\);",
            helpers);
        Assert.Matches(
            @"RefreshQueryHeatmapBannerAsync\(int hoursBack, DateTime\? fromDate, DateTime\? toDate\)\s*\{\s*var \(windowStart, windowEnd\) = LocalDataService\.GetQueriesTabWindowUtc\(hoursBack, fromDate, toDate\);\s*return RefreshWindowTruncatedBannerAsync\(QueryWindowRelation\.QueryStats, QueryHeatmapWindowTruncatedBanner, windowStart, windowEnd\);",
            helpers);
    }

    /// <summary>
    /// Every read path of Plan Corrections (the sub-tab switch and the full refresh) refreshes its banner after the
    /// grid is bound. A census over every ServerTab file, so a read path added later without the call fails here.
    /// </summary>
    [Fact]
    public void PlanCorrections_EveryReadPathRefreshesTheBanner_AfterTheGridIsBound()
    {
        var refresh = Body(CodeOf("ServerTab.Refresh.cs"), "private async System.Threading.Tasks.Task RefreshQueriesAsync(");
        var bound = Positions(refresh, "_planCorrectionFilterMgr!.UpdateData(");
        var banner = Positions(refresh, "await RefreshPlanCorrectionsBannerAsync(hoursBack, fromDate, toDate);");

        Assert.Equal(2, bound.Count);
        AssertEachFollowsItsBind(bound, banner, "Plan Corrections");

        Assert.Equal(
            AllServerTabCode().Sum(code => Positions(code, "_dataService.GetPlanCorrectionsAsync(").Count),
            AllServerTabCode().Sum(code => Positions(code, "await RefreshPlanCorrectionsBannerAsync(").Count));
    }

    /// <summary>
    /// Every read path of the Query Heatmap (the sub-tab switch, the full refresh and the metric picker) refreshes its
    /// banner after the chart is drawn. The same census.
    /// </summary>
    [Fact]
    public void QueryHeatmap_EveryReadPathRefreshesTheBanner_AfterTheChartIsDrawn()
    {
        var refresh = Body(CodeOf("ServerTab.Refresh.cs"), "private async System.Threading.Tasks.Task RefreshQueriesAsync(");
        var drawn = Positions(refresh, "UpdateQueryHeatmapChart(");
        var banner = Positions(refresh, "await RefreshQueryHeatmapBannerAsync(hoursBack, fromDate, toDate);");
        Assert.Equal(2, drawn.Count);
        AssertEachFollowsItsBind(drawn, banner, "Query Heatmap (sub-tab switch and full refresh)");

        var picker = Body(CodeOf("ServerTab.Charts.cs"), "private async void HeatmapMetric_SelectionChanged(");
        var pickerDrawn = Positions(picker, "UpdateQueryHeatmapChart(result);");
        var pickerBanner = Positions(picker, "await RefreshQueryHeatmapBannerAsync(hoursBack, fromDate, toDate);");
        Assert.Single(pickerDrawn);
        AssertEachFollowsItsBind(pickerDrawn, pickerBanner, "Query Heatmap (metric picker)");

        Assert.Equal(
            AllServerTabCode().Sum(code => Positions(code, "_dataService.GetQueryHeatmapAsync(").Count),
            AllServerTabCode().Sum(code => Positions(code, "await RefreshQueryHeatmapBannerAsync(").Count));
    }

    /// <summary>
    /// The surfaces of the same group that carry NO notice, and why. A chart whose time axis is pinned to the asked
    /// range already draws the empty span before the first stored row, so a reader cannot take it for a quiet period;
    /// this holds each of them to that: every one of these methods pins the X axis to the window the read took
    /// (<c>GetChartWindow</c>, or <c>GetXAxisWindow</c> for the Overview's five timeline charts), so one that moves to an axis the data
    /// decides fails here and needs a notice instead. The Memory Overview summary reads only the newest snapshot, which
    /// is not a window at all.
    /// </summary>
    [Theory]
    [InlineData("ServerTab.Charts.cs", "private void UpdateCpuChart(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateMemoryChart(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateMemoryGrantCharts(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateMemoryPressureEventsChart(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateTempDbChart(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateTempDbSizeChart(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateTempDbFileIoChart(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateFileIoCharts(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateFileIoThroughputCharts(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateQueryDurationTrendChart(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateProcDurationTrendChart(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateQueryStoreDurationTrendChart(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateExecutionCountTrendChart(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Pickers.cs", "private async System.Threading.Tasks.Task UpdateMemoryClerksChartFromPickerAsync(", "GetChartWindow(hoursBack, fromDate, toDate)")]
    [InlineData("ServerTab.Pickers.cs", "private async System.Threading.Tasks.Task UpdateWaitStatsChartFromPickerAsync(", "GetChartWindow(hoursBack, null, null)")]
    [InlineData("ServerTab.Pickers.cs", "private async System.Threading.Tasks.Task UpdatePerfmonChartFromPickerAsync(", "GetChartWindow(hoursBack, null, null)")]
    [InlineData("CorrelatedTimelineLanesControl.xaml.cs", "private void SyncXAxes(", "GetXAxisWindow(hoursBack, fromDate, toDate, DateTime.UtcNow)")]
    public void ChartSurfacesWithoutANotice_PinTheirXAxisToTheRequestedRange(string file, string signature, string window)
    {
        var body = Body(CodeOf(file), signature);
        Assert.Contains(window, body, StringComparison.Ordinal);
        Assert.Contains(".SetLimitsX(", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The two surfaces that DO carry a notice are the ones whose axis the data decides. The heatmap's X axis counts
    /// its drawn columns, which is why it needs the banner (see the column test above). Plan Corrections is a grid.
    /// </summary>
    [Fact]
    public void QueryHeatmap_XAxisCountsDrawnColumns_SoItCarriesTheNotice()
    {
        var body = Body(CodeOf("ServerTab.Charts.cs"), "private void UpdateQueryHeatmapChart(");
        Assert.Contains("SetLimitsX(-0.5, numCols - 0.5)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("GetChartWindow(", body, StringComparison.Ordinal);
    }

    /// <summary>The Memory Overview summary reads the newest snapshot only: no window, so nothing to say about where one starts.</summary>
    [Fact]
    public void MemoryOverviewSummary_ReadsTheNewestSnapshotOnly()
    {
        var refresh = CodeOf("ServerTab.Refresh.cs");
        Assert.Equal(2, Positions(refresh, "_dataService.GetLatestMemoryStatsAsync(_serverId)").Count);
        Assert.DoesNotContain("GetLatestMemoryStatsAsync(_serverId, hoursBack", refresh, StringComparison.Ordinal);
    }

    private static void AssertEachFollowsItsBind(List<int> binds, List<int> banners, string what)
    {
        Assert.True(banners.Count == binds.Count,
            $"{what}: expected one banner refresh per read path ({binds.Count}), found {banners.Count}");
        for (var i = 0; i < binds.Count; i++)
        {
            Assert.True(banners[i] > binds[i], $"{what}: the banner refresh must follow the read it describes");
            if (i + 1 < binds.Count)
            {
                Assert.True(banners[i] < binds[i + 1], $"{what}: the banner refresh must sit with the read it describes, not the next one");
            }
        }
    }

    private static List<int> Positions(string text, string token)
    {
        var found = new List<int>();
        for (var at = text.IndexOf(token, StringComparison.Ordinal); at >= 0; at = text.IndexOf(token, at + token.Length, StringComparison.Ordinal))
        {
            found.Add(at);
        }

        return found;
    }

    /// <summary>The method that starts at <paramref name="signature"/>, up to the first closing brace at member indentation.</summary>
    private static string Body(string code, string signature)
    {
        var start = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{signature}' was not found; update this pin.");
        var end = code.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the end of '{signature}' was not found.");
        return code[start..end];
    }

    /// <summary>A Controls source file with line endings folded to LF and comments removed, so a sentence naming a call cannot satisfy a pin.</summary>
    private static string CodeOf(string name)
    {
        var lf = File.ReadAllText(ControlsFile(name)).Replace("\r\n", "\n");
        return Regex.Replace(Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"(?<!:)//[^\n]*", string.Empty);
    }

    private static IEnumerable<string> AllServerTabCode() =>
        Directory.EnumerateFiles(ControlsDir(), "ServerTab*.cs").Select(path => CodeOf(Path.GetFileName(path)));

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
