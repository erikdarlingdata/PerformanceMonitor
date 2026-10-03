/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4966: the "Showing since" notice on the Collection Log, the nine System Events grids that read stored events
/// (Scheduler Issues, Severe Errors, Memory Conditions, Memory Broker, Memory Node OOM, Significant Waits, CPU Tasks,
/// I/O Issues and Default Trace), the three Config Changes grids and Long Queries. Each one reads its rows over the
/// toolbar's window, so a window that starts before the store's coverage drew a shorter range with no notice. The
/// notice comes from the ONE shared probe (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>) and the ONE banner
/// step (<see cref="ServerTab.ApplyWindowFloorToBanner"/>) the Queries tab, Active Queries and Current Waits use, and
/// each surface has the two tests the issue asks for: rows that start inside the range read the notice back, and a
/// quiet start (the collector covered the whole range, the first row comes late) shows none.
///
/// <para>The surfaces with no notice are pinned too (<see cref="SurfacesWithoutANotice_KeepTheShapeThatNeedsNone"/>):
/// the Health Summary is a fixed seven-day aggregate that does not read the toolbar's range, and the Duration Trends,
/// Corruption Events and Contention Events charts pin their time axis to the asked range, so an empty span is already
/// drawn as one.</para>
/// </summary>
[Collection("server-time-helper")]
public sealed class DataStartBannerSurfaceTests : IDisposable
{
    private const int ServerId = 4343;
    private const string ServerName = "SurfaceBannerServer";
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public DataStartBannerSurfaceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "SurfaceBanner_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>One relation per surface that gets a notice. The eight system_health grids share a relation, as they
    /// share a collector and a table; the wiring test below pins each grid's own banner and call.</summary>
    public static TheoryData<QueryWindowRelation> NoticeRelations => new()
    {
        QueryWindowRelation.CollectionLog,
        QueryWindowRelation.SystemHealthEvents,
        QueryWindowRelation.DefaultTraceEvents,
        QueryWindowRelation.ServerConfig,
        QueryWindowRelation.DatabaseConfig,
        QueryWindowRelation.TraceFlags,
        QueryWindowRelation.LongQueryCompletions
    };

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    private static string Literal(DateTime instant) =>
        "TIMESTAMP '" + Naive(instant).ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'";

    /// <summary>
    /// One row in the relation's table with its time column at <paramref name="at"/>. Every other column the table
    /// requires (NOT NULL without a default) gets a placeholder of its type, found from the table itself, so the test
    /// does not repeat any collector's column list.
    /// </summary>
    private async Task SeedRowAsync(QueryWindowRelation relation, DateTime at)
    {
        var table = LocalDataService.QueryWindowRelationView(relation)[2..];
        var timeColumn = LocalDataService.QueryWindowRelationTimeColumn(relation);

        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();

        var names = new List<string>();
        var values = new List<string>();
        using (var info = connection.CreateCommand())
        {
            info.CommandText = $"SELECT name, type, \"notnull\", pk, dflt_value IS NOT NULL FROM pragma_table_info('{table}') ORDER BY cid";
            using var reader = await info.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var name = reader.GetString(0);
                var type = reader.GetString(1);
                var pk = reader.GetBoolean(3);
                if (!(reader.GetBoolean(2) || pk) || reader.GetBoolean(4))
                {
                    continue;
                }

                names.Add(name);
                values.Add(PlaceholderFor(name, type, pk, timeColumn, at));
            }
        }

        using var insert = connection.CreateCommand();
        insert.CommandText = $"INSERT INTO {table} ({string.Join(", ", names)}) VALUES ({string.Join(", ", values)})";
        await insert.ExecuteNonQueryAsync();
    }

    private string PlaceholderFor(string name, string type, bool pk, string timeColumn, DateTime at)
    {
        if (name == timeColumn) return Literal(at);
        if (name == "server_id") return ServerId.ToString();
        if (name == "server_name") return $"'{ServerName}'";
        if (pk) return (_nextId++).ToString();

        var upper = type.ToUpperInvariant();
        if (upper.StartsWith("VARCHAR", StringComparison.Ordinal)) return "'x'";
        if (upper.StartsWith("TIMESTAMP", StringComparison.Ordinal)) return Literal(at);
        if (upper == "BOOLEAN") return "false";
        if (upper.Contains("INT", StringComparison.Ordinal) || upper.StartsWith("DECIMAL", StringComparison.Ordinal)
            || upper is "DOUBLE" or "FLOAT" or "REAL")
        {
            return $"CAST(1 AS {type})";
        }

        return $"CAST('x' AS {type})";
    }

    /// <summary>
    /// The collector's runs in collection_log, every <paramref name="everyMinutes"/> minutes from
    /// <paramref name="firstUtc"/> to <paramref name="lastUtc"/>, whether or not they stored a row.
    /// </summary>
    private async Task SeedLogRunsAsync(string collector, DateTime firstUtc, DateTime lastUtc, int everyMinutes)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT {_nextId} + row_number() OVER (), {ServerId}, '{ServerName}', '{collector}', g.t, 12, 'SUCCESS', 0
FROM generate_series({Literal(firstUtc)}, {Literal(lastUtc)}, INTERVAL {everyMinutes} MINUTE) AS g(t)";
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
    /// A range that starts before the first stored row, and before the collector's first run, gets the notice, worded
    /// at that first row or run. Covers the config snapshots too, whose time column is <c>capture_time</c>.
    /// </summary>
    [Theory]
    [MemberData(nameof(NoticeRelations))]
    public async Task RowsStartInsideTheRange_ShowTheNotice(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var first = end.AddDays(-2);
        await SeedRowAsync(relation, first);
        await SeedRowAsync(relation, end.AddHours(-1));
        if (LocalDataService.QueryWindowRelationCollector(relation) is { } collector)
        {
            await SeedLogRunsAsync(collector, first, end, 60);
        }

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.StartsWith("Showing since ", text, StringComparison.Ordinal);
        Assert.Contains(Naive(first).ToString("yyyy-MM-dd HH:"), text, StringComparison.Ordinal);
    }

    /// <summary>
    /// A quiet start is no reason for a notice. The store covered the whole range, but the first row inside it comes
    /// three days in: for an event or snapshot table the collector ran for a month and stored nothing near the start,
    /// and for the run log itself the oldest row is a month old.
    /// </summary>
    [Theory]
    [MemberData(nameof(NoticeRelations))]
    public async Task QuietStart_TheStoreCoveredTheRange_ShowsNoNotice(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        if (LocalDataService.QueryWindowRelationCollector(relation) is { } collector)
        {
            await SeedLogRunsAsync(collector, end.AddDays(-30), end, 180);
        }
        else
        {
            await SeedRowAsync(relation, end.AddDays(-30));
        }

        await SeedRowAsync(relation, end.AddDays(-3));
        await SeedRowAsync(relation, end.AddHours(-1));

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// Each relation reads the view its grid reads, by the column that table is purged on, and takes its coverage from
    /// the collector whose runs the store logs under that name. The Collection Log is the run log, so it keeps the
    /// row-only probe.
    /// </summary>
    [Theory]
    [InlineData(QueryWindowRelation.CollectionLog, "v_collection_log", null, "collection_time")]
    [InlineData(QueryWindowRelation.SystemHealthEvents, "v_system_health_events", "system_health_events", "collection_time")]
    [InlineData(QueryWindowRelation.DefaultTraceEvents, "v_default_trace_events", "default_trace_events", "collection_time")]
    [InlineData(QueryWindowRelation.ServerConfig, "v_server_config", "server_config", "capture_time")]
    [InlineData(QueryWindowRelation.DatabaseConfig, "v_database_config", "database_config", "capture_time")]
    [InlineData(QueryWindowRelation.TraceFlags, "v_trace_flags", "trace_flags", "capture_time")]
    [InlineData(QueryWindowRelation.LongQueryCompletions, "v_long_query_completions", "long_query_completions", "collection_time")]
    public void Relations_NameTheViewCollectorAndTimeColumnTheirGridReads(
        QueryWindowRelation relation, string view, string? collector, string timeColumn)
    {
        Assert.Equal(view, LocalDataService.QueryWindowRelationView(relation));
        Assert.Equal(collector, LocalDataService.QueryWindowRelationCollector(relation));
        Assert.Equal(timeColumn, LocalDataService.QueryWindowRelationTimeColumn(relation));
    }

    /// <summary>
    /// The wiring: each surface has its banner TextBlock, and the one method that reads it refreshes the banner after
    /// the rows are bound, through the shared helper and over the UTC window the read took. Every read path of these
    /// surfaces goes through that method: the sub-tab switch and the toolbar's range change reach the System Events,
    /// Config Changes and Long Queries loaders through <c>RefreshVisibleTabAsync</c>, and a loader's own Refresh button
    /// calls the same refresh. Comments are stripped first, so a sentence that names the call cannot satisfy the pin.
    /// </summary>
    [Theory]
    [InlineData("private async System.Threading.Tasks.Task RefreshCollectionHealthAsync(", "ServerTab.Refresh.cs", "CollectionLog", "CollectionLogWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadSchedulerIssuesAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "SchedulerIssuesWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadSevereErrorsAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "SevereErrorsWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadMemoryConditionsAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "MemoryConditionsWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadMemoryBrokerAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "MemoryBrokerWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadMemoryNodeOomAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "MemoryNodeOomWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadSignificantWaitsAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "SignificantWaitsWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadCpuTasksAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "CpuTasksWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadIoIssuesAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "IoIssuesWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadDefaultTraceEventsAsync(", "ServerTab.SystemEvents.cs", "DefaultTraceEvents", "DefaultTraceWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadServerConfigChangesAsync(", "ServerTab.ConfigChanges.cs", "ServerConfig", "ServerConfigChangesWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadDatabaseConfigChangesAsync(", "ServerTab.ConfigChanges.cs", "DatabaseConfig", "DatabaseConfigChangesWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadTraceFlagChangesAsync(", "ServerTab.ConfigChanges.cs", "TraceFlags", "TraceFlagChangesWindowTruncatedBanner")]
    [InlineData("private async Task RefreshLongQueriesAsync(", "ServerTab.LongQueries.cs", "LongQueryCompletions", "LongQueriesWindowTruncatedBanner")]
    public void EverySurface_HasABanner_RefreshedAfterTheRowsAreBound(string signature, string file, string relation, string banner)
    {
        var xaml = File.ReadAllText(ControlsFile("ServerTab.xaml"));
        Assert.Contains($"x:Name=\"{banner}\"", xaml, StringComparison.Ordinal);

        var code = StripComments(File.ReadAllText(ControlsFile(file)).Replace("\r\n", "\n"));
        var start = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{signature} is not in {file}");
        var next = Regex.Match(code[(start + signature.Length)..], @"\n    (private|internal|public) ");
        var body = next.Success ? code.Substring(start, signature.Length + next.Index) : code[start..];

        var call = $"RefreshStoredWindowBannerAsync(QueryWindowRelation.{relation}, {banner}, hoursBack, fromDate, toDate)";
        Assert.Contains(call, body, StringComparison.Ordinal);
        var bind = body.IndexOf("UpdateData(", StringComparison.Ordinal);
        Assert.True(bind >= 0 && bind < body.IndexOf(call, StringComparison.Ordinal),
            $"{banner} must be refreshed after the grid's rows are bound");
    }

    /// <summary>
    /// The one helper every group surface calls: it takes the window the grids' reads take
    /// (<see cref="LocalDataService.GetQueriesTabWindowUtc"/>, the same <c>GetTimeRange</c> call) and hands it to the
    /// shared banner step, which hides the banner when the probe throws instead of unwinding the refresh.
    /// </summary>
    [Fact]
    public void TheBannerHelper_ProbesTheWindowTheGridsReadThroughTheSharedStep()
    {
        var code = StripComments(File.ReadAllText(ControlsFile("ServerTab.SystemEvents.cs")).Replace("\r\n", "\n"));
        var start = code.IndexOf("RefreshStoredWindowBannerAsync(QueryWindowRelation relation,", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var body = code[start..Math.Min(code.Length, start + 600)];
        Assert.Contains("LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate)", body, StringComparison.Ordinal);
        Assert.Contains("RefreshWindowTruncatedBannerAsync(relation, banner, startUtc, endUtc)", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The surfaces that need no notice, and the shape that makes it true. The Health Summary reads a fixed seven
    /// days per collector, with no range parameter, so a picked range never starts before its data. The Duration
    /// Trends, Corruption Events and Contention Events charts set their X axis to the asked range, so a span with no
    /// data is drawn as the empty span it is. If one of them starts reading the toolbar's range as a grid, or stops
    /// pinning its axis, it needs a notice and this test says so.
    /// </summary>
    [Fact]
    public void SurfacesWithoutANotice_KeepTheShapeThatNeedsNone()
    {
        var health = File.ReadAllText(RepoFile("Lite", "Services", "LocalDataService.CollectionHealth.cs"));
        Assert.Contains("public async Task<List<CollectorHealthRow>> GetCollectionHealthAsync(int serverId)", health, StringComparison.Ordinal);
        Assert.Contains("GetCollectionHealthAsync(_serverId)", File.ReadAllText(ControlsFile("ServerTab.Refresh.cs")), StringComparison.Ordinal);

        var durationChart = Regex.Match(File.ReadAllText(ControlsFile("ServerTab.Charts.cs")).Replace("\r\n", "\n"),
            @"private void UpdateCollectorDurationChart\(.*?\n    \}\n", RegexOptions.Singleline);
        Assert.True(durationChart.Success);
        Assert.Contains("SetLimitsX(xMin, xMax)", durationChart.Value, StringComparison.Ordinal);

        var systemCharts = File.ReadAllText(ControlsFile("ServerTab.SystemHealthCharts.cs"));
        Assert.Contains("ChartPalette.CyclingColor(3), xMin, xMax)", systemCharts, StringComparison.Ordinal);
        Assert.Contains("RenderSickSpinlocksChart(SickSpinlocksChart, _sickSpinlocksHover, data, xMin, xMax)", systemCharts, StringComparison.Ordinal);
        Assert.Contains("RenderCpuComparisonChart(CpuComparisonChart, _cpuComparisonHover, data, xMin, xMax)", systemCharts, StringComparison.Ordinal);
        Assert.Equal(3, Regex.Matches(File.ReadAllText(RepoFile("PerformanceMonitor.Ui", "SystemHealthChartRenderer.cs")),
            Regex.Escape("chart.Plot.Axes.SetLimitsX(xMin, xMax)")).Count);
    }

    private static string StripComments(string lfSource) =>
        Regex.Replace(Regex.Replace(lfSource, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"//[^\n]*", string.Empty);

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

    private static string ControlsFile(string name) => RepoFile("Lite", "Controls", name);

    private static string RepoFile(string folder, string subFolderOrFile, string? file = null, [CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", folder, subFolderOrFile, file ?? string.Empty));
}
