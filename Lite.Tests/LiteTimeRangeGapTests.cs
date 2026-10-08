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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5562 gaps the first Lite pass left: FinOps pickers are rolling only (R5), Job History and Alert History read a real END
/// bound ahead of their row caps (R6), Alert History's longest choice is the alert log's retention (R8), the sample note
/// names the main collector of the sub-tab on screen (R3), and every banner site feeds the picker's data start (R7).
/// </summary>
public sealed class LiteTimeRangeGapTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 9000;

    public LiteTimeRangeGapTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static string LiteFile(string relative, [CallerFilePath] string thisFile = "")
        => File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", relative))).ReplaceLineEndings("\n");

    private async Task<DuckDBConnection> SeedAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    // ---- R5: FinOps pickers are rolling only ----

    [Fact]
    public void RollingOnlyLimits_RefuseCalendarFixedAndSinceAndTooShort()
    {
        var now = new DateTime(2026, 10, 8, 12, 0, 0, DateTimeKind.Unspecified);
        var oneHour = TimeSpan.FromHours(1);

        Assert.Null(TimeRangeLimits.Refusal(TimeRangeSpec.Relative(TimeSpan.FromHours(36)), rollingOnly: true, oneHour));
        Assert.NotNull(TimeRangeLimits.Refusal(TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousWeek), rollingOnly: true, oneHour));
        Assert.NotNull(TimeRangeLimits.Refusal(TimeRangeSpec.FixedRange(now.AddDays(-3), now.AddDays(-2)), rollingOnly: true, oneHour));
        Assert.NotNull(TimeRangeLimits.Refusal(TimeRangeSpec.SinceInstant(now.AddDays(-3)), rollingOnly: true, oneHour));
        Assert.NotNull(TimeRangeLimits.Refusal(TimeRangeSpec.Relative(TimeSpan.FromMinutes(30)), rollingOnly: true, oneHour));
        Assert.Null(TimeRangeLimits.Refusal(TimeRangeSpec.Relative(TimeSpan.FromHours(1)), rollingOnly: true, oneHour));

        /* Not rolling only: a calendar period is fine, but the minimum span still refuses a short length. */
        Assert.Null(TimeRangeLimits.Refusal(TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousWeek), rollingOnly: false, oneHour));
        Assert.NotNull(TimeRangeLimits.Refusal(TimeRangeSpec.Relative(TimeSpan.FromMinutes(30)), rollingOnly: false, oneHour));
    }

    [Fact]
    public void RollingOnlyCoerce_ReadsAFinishedPeriodAsTheLengthFromItsStartToNow()
    {
        var now = new DateTime(2026, 10, 8, 12, 0, 0, DateTimeKind.Unspecified);
        var spec = TimeRangeLimits.Coerce(TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday), rollingOnly: true, TimeSpan.FromHours(1), now, TimeZoneInfo.Utc);

        Assert.Equal(TimeRangeKind.Relative, spec.Kind);
        Assert.Equal(TimeSpan.FromHours(36), spec.Span); /* yesterday starts Oct 7 00:00, 36 hours before Oct 8 12:00 */
        Assert.Equal(TimeSpan.FromHours(1), TimeRangeLimits.Coerce(TimeRangeSpec.Relative(TimeSpan.FromMinutes(15)), true, TimeSpan.FromHours(1), now, TimeZoneInfo.Utc).Span);
        var held = TimeRangeSpec.Relative(TimeSpan.FromHours(4));
        Assert.Same(held, TimeRangeLimits.Coerce(held, true, TimeSpan.FromHours(1), now, TimeZoneInfo.Utc));
    }

    [Fact]
    public void FinOps_EveryPickerIsConfiguredRollingOnly_HeatmapInWholeDays()
    {
        var code = LiteFile(Path.Combine("Controls", "FinOpsTab.xaml.cs"));
        var xaml = LiteFile(Path.Combine("Controls", "FinOpsTab.xaml"));
        var helper = LiteFile(Path.Combine("Helpers", "LiteTimeRange.cs"));

        Assert.Equal(5, Regex.Matches(xaml, @"<ui:TimeRangePicker [^>]*Compact=""True""").Count);
        Assert.Equal(4, Regex.Matches(code, @"LiteTimeRange\.ConfigureFinOpsPicker\(\w+TimeRangePicker, LiteTimeRange\.FinOpsMinSpan\)").Count);
        Assert.Contains("LiteTimeRange.ConfigureFinOpsPicker(ObjectHeatmapWindowPicker, LiteTimeRange.FinOpsHeatmapMinSpan)", code, StringComparison.Ordinal);
        Assert.Contains("picker.RollingOnly = true;", helper, StringComparison.Ordinal);
        Assert.Contains("picker.Compact = true;", helper, StringComparison.Ordinal);
        Assert.Equal(TimeSpan.FromHours(1), LiteTimeRange.FinOpsMinSpan);
        Assert.Equal(TimeSpan.FromDays(1), LiteTimeRange.FinOpsHeatmapMinSpan);
    }

    // ---- R6: Job History end bound ahead of the cap ----

    private async Task InsertRunAsync(int serverId, string jobId, DateTime wall)
    {
        var connection = await SeedAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO job_history
    (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name,
     job_enabled, category_name, step_id, step_name, run_status, run_status_desc, run_datetime,
     run_duration_seconds, retries_attempted, message)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, TRUE, 'Uncategorized (Local)', 0, '(Job outcome)', 1, 'The job succeeded.', $8, 30, 0, NULL)";
        var id = _nextId++;
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"S{serverId}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = jobId });
        cmd.Parameters.Add(new DuckDBParameter { Value = jobId });
        cmd.Parameters.Add(new DuckDBParameter { Value = wall });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// A window that ENDED two hours ago, with more runs after its end than the cap: the end is in the SQL ahead of the
    /// LIMIT, so the cap is spent on the runs inside the window (the newest of them), not on the newer runs after it.
    /// </summary>
    [Fact]
    public async Task JobHistory_EndBoundIsAppliedBeforeTheRowCap()
    {
        const int server = 701;
        var host = DateTime.Now - DateTime.UtcNow; /* no collected clock: the machine's, so wall = utc + host offset */
        DateTime Wall(DateTime utc)
        {
            var t = utc + host;
            return new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);
        }

        var now = DateTime.UtcNow;
        var start = now.AddHours(-6);
        var end = now.AddHours(-2);
        for (var i = 0; i < 5; i++)
        {
            await InsertRunAsync(server, $"inside_{i}", Wall(end.AddMinutes(-10 * (i + 1))));
        }

        for (var i = 0; i < 6; i++)
        {
            await InsertRunAsync(server, $"after_{i}", Wall(end.AddMinutes(10 * (i + 1))));
        }

        var service = new LocalDataService(_duckDb);
        var cap = 3;
        var bounded = await service.GetJobHistoryAsync(start, cap, server, openTabClocks: null, windowEndUtc: end);
        var unbounded = await service.GetJobHistoryAsync(start, cap, server);

        Assert.Equal(cap, bounded.Count);
        Assert.All(bounded, r => Assert.StartsWith("inside_", r.JobId));
        Assert.Equal(new[] { "inside_0", "inside_1", "inside_2" }, bounded.Select(r => r.JobId).ToArray()); /* newest inside the window first */
        Assert.All(unbounded, r => Assert.StartsWith("after_", r.JobId)); /* the old shape spent the whole cap after the end */
    }

    [Fact]
    public void JobHistoryTab_PassesTheRangeEndToTheRead_AndTheEndIsExclusive()
    {
        var tab = LiteFile(Path.Combine("Controls", "JobHistoryTab.xaml.cs"));
        var sql = LiteFile(Path.Combine("Services", "LocalDataService.JobHistory.cs"));

        Assert.Contains("GetJobHistoryWithClocksAsync(startUtc, RowCap, serverId, openTabClocks, rangeEndUtc)", tab, StringComparison.Ordinal);
        Assert.Contains("LiteTimeRange.BoundsOf(RangePicker, 24, nowUtc)", tab, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(sql, @"\{endClause\}").Count); /* job_stats and base, both ahead of the LIMIT */
        Assert.Contains("run_datetime < $5", sql, StringComparison.Ordinal);
    }

    // ---- Alert History: start and exclusive end, Dismiss All scope ----

    private async Task InsertAlertAsync(DateTime alertTimeUtc, string metric)
    {
        var connection = await SeedAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type)
VALUES ($1, 702, 'S702', $2, 1, 1, TRUE, 'tray')";
        cmd.Parameters.Add(new DuckDBParameter { Value = alertTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = metric });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task AlertHistory_FinishedRangeShowsNoAlertAfterItsEnd_AndDismissAllTouchesOnlyThoseRows()
    {
        var now = DateTime.UtcNow;
        var start = now.AddHours(-10);
        var end = now.AddHours(-4);
        await InsertAlertAsync(end.AddHours(-2), "inside_a");
        await InsertAlertAsync(end.AddHours(-1), "inside_b");
        await InsertAlertAsync(end, "at_end"); /* the end is exclusive */
        await InsertAlertAsync(end.AddHours(1), "after_a");
        await InsertAlertAsync(end.AddHours(2), "after_b");

        var service = new LocalDataService(_duckDb);
        var finished = await service.GetAlertHistoryWindowAsync(start, end, serverId: 702, limit: 100);
        Assert.Equal(new[] { "inside_b", "inside_a" }, finished.Select(a => a.MetricName).ToArray());

        var live = await service.GetAlertHistoryWindowAsync(start, null, serverId: 702, limit: 100);
        Assert.Equal(5, live.Count);

        /* The cap counts the rows inside the window, not the newer ones after its end. */
        var capped = await service.GetAlertHistoryWindowAsync(start, end, serverId: 702, limit: 1);
        Assert.Equal("inside_b", Assert.Single(capped).MetricName);

        var dismissed = await service.DismissAllVisibleAlertsWindowAsync(start, end, serverId: 702);
        Assert.Equal(2, dismissed);
        var remaining = await service.GetAlertHistoryWindowAsync(start, null, serverId: 702, limit: 100);
        Assert.Equal(new[] { "after_b", "after_a", "at_end" }, remaining.Select(a => a.MetricName).ToArray());
    }

    // ---- R8: Alert History's longest choice ----

    [Theory]
    [InlineData(2026, 10, 8, 92)] /* Jul 8 to Oct 8 */
    [InlineData(2026, 3, 1, 90)]  /* Dec 1 to Mar 1 */
    [InlineData(2026, 6, 1, 92)]  /* Mar 1 to Jun 1 */
    public void AlertHistoryLongest_IsTheAlertLogsRetentionInDays(int year, int month, int day, int days)
    {
        var now = new DateTime(year, month, day, 0, 0, 0, DateTimeKind.Utc);
        Assert.Equal(TimeSpan.FromDays(days), LiteTimeRange.AlertHistoryLongest(now));
        Assert.Equal(days + "d", LiteTimeRange.AlertHistoryLongestText(now));
        Assert.Equal(3, RetentionService.ArchiveRetentionMonths);
        Assert.True(LiteTimeRange.AlertHistoryLongest(now) <= TimeSpan.FromDays(365));

        /* It types into the picker's grammar as a rolling length. */
        var parsed = TimeRangeParser.Parse(LiteTimeRange.AlertHistoryLongestText(now), now, TimeZoneInfo.Utc);
        Assert.True(parsed.Ok);
        Assert.Equal(days * 24, parsed.Spec!.WholeHours);
    }

    // ---- R3: the sample note names the sub-tab's main collector ----

    [Theory]
    [InlineData("Wait Stats", null, "wait_stats")]
    [InlineData("Queries", "Active Queries", "query_snapshots")]
    [InlineData("Queries", "Top Procedures by Duration", "procedure_stats")]
    [InlineData("Queries", "Query Store by Duration", "query_store")]
    [InlineData("Memory", "Memory Clerks", "memory_clerks")]
    [InlineData("Memory", "Memory Grants", "memory_grant_stats")]
    [InlineData("Blocking", "Current Waits", "waiting_tasks")]
    [InlineData("Blocking", "Deadlocks", "deadlocks")]
    [InlineData("System Events", "Default Trace", "default_trace_events")]
    [InlineData("System Events", "Severe Errors", "system_health_events")]
    [InlineData("Latches & Spinlocks", null, "latch_stats")]
    [InlineData("Running Jobs", null, "running_jobs")]
    [InlineData("Overview", null, null)]
    [InlineData("Configuration", "Trace Flags", null)]
    [InlineData("Daily Summary", null, null)]
    public void MainCollectorFor_NamesTheSubTabsOwnCollector(string tab, string? sub, string? expected)
        => Assert.Equal(expected, LiteTimeRange.MainCollectorFor(tab, sub));

    [Fact]
    public void MainCollectors_AreRealCollectorsWithAShippedCadence()
    {
        var names = new[]
        {
            "wait_stats", "query_stats", "query_snapshots", "procedure_stats", "query_store", "plan_correction", "cpu_utilization",
            "memory_stats", "memory_clerks", "memory_grant_stats", "memory_pressure_events", "file_io_stats", "tempdb_stats",
            "blocked_process_report", "waiting_tasks", "deadlocks", "perfmon_stats", "running_jobs", "latch_stats",
            "cpu_scheduler_stats", "plan_cache_stats", "session_stats", "default_trace_events", "system_health_events"
        };
        foreach (var name in names)
        {
            Assert.True(CollectorScheduleDefaults.All.ContainsKey(name), name);
        }
    }

    [Fact]
    public void SampleIntervalForCollector_UsesTheSchedule_ThenTheShippedDefault()
    {
        Assert.Null(LiteTimeRange.SampleIntervalForCollector(null, null));
        Assert.Equal(TimeSpan.FromMinutes(5), LiteTimeRange.SampleIntervalForCollector("query_store", null)); /* shipped default */
        Assert.Equal(TimeSpan.FromMinutes(15), LiteTimeRange.SampleIntervalForCollector("query_store", new CollectorSchedule { Name = "query_store", Enabled = true, FrequencyMinutes = 15 }));
        Assert.Null(LiteTimeRange.SampleIntervalForCollector("query_store", new CollectorSchedule { Name = "query_store", Enabled = false, FrequencyMinutes = 5 }));
        Assert.Null(LiteTimeRange.SampleIntervalForCollector("server_config", null)); /* on load: no cadence */
    }

    [Fact]
    public void ServerTab_SampleNoteFollowsTheSubTabOnScreen_NotWaitStatsForEverything()
    {
        var tab = LiteFile(Path.Combine("Controls", "ServerTab.TimeRange.cs"));
        var main = LiteFile("MainWindow.xaml.cs");

        Assert.DoesNotContain("MainCollectorName", tab, StringComparison.Ordinal);
        Assert.Contains("CurrentMainCollector() is { } collector ? _sampleIntervalProvider?.Invoke(collector)", tab, StringComparison.Ordinal);
        Assert.Contains("Selector.SelectionChangedEvent", tab, StringComparison.Ordinal);
        Assert.Contains("SampleIntervalForCollector(", main, StringComparison.Ordinal);
        Assert.DoesNotContain("\"wait_stats\"", main.Substring(main.IndexOf("SetSampleIntervalSource", StringComparison.Ordinal), 600), StringComparison.Ordinal);
    }

    // ---- R7: every banner site feeds the picker's data start ----

    [Fact]
    public void EveryBannerSite_FeedsThePickersDataStart()
    {
        /* The call sites of ApplyWindowFloorToBanner outside its definition, by file. A new site must either feed the picker
           (ServerTab.FeedDataStart / the Job History tab's onFloor) or be added here with the reason it cannot. */
        var expected = new[]
        {
            ("Controls/JobHistoryTab.xaml.cs", 1, "onFloor?.Invoke(floor)"),                       /* the tab's own picker */
            ("Controls/LiteBlockingLaneDataStart.cs", 1, "onStartChosen?.Invoke(start)"),          /* Overview lanes, fed by the tab */
            ("Controls/ServerTab.BlockingChartsDataStart.cs", 1, "FeedDataStart("),
            ("Controls/ServerTab.Refresh.cs", 1, "FeedDataStart(LiteTimeRange.CollectorOfRelation(relation), floor)"),
            ("Windows/CollectionLogWindow.xaml.cs", 1, null),                                      /* its own window: no picker */
        };

        var root = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(ThisFile())!, "..", "Lite"));
        var sites = Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar) && !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar))
            .Select(f => (File: f, Calls: Regex.Matches(File.ReadAllText(f), @"(?<![/<=.\w])(ServerTab\.)?ApplyWindowFloorToBanner\(", RegexOptions.None)
                .Count(m => !IsDocOrDefinition(File.ReadAllText(f), m.Index))))
            .Where(x => x.Calls > 0)
            .ToList();

        Assert.Equal(expected.Length, sites.Count);
        foreach (var (relative, calls, feed) in expected)
        {
            var full = Path.Combine(root, relative.Replace('/', Path.DirectorySeparatorChar));
            var site = Assert.Single(sites, s => s.File == full);
            Assert.Equal(calls, site.Calls);
            if (feed is not null)
            {
                Assert.Contains(feed, File.ReadAllText(full), StringComparison.Ordinal);
            }
        }

        /* The central funnel feeds for every relation, and the tab points the picker at the page on screen. */
        var refresh = LiteFile(Path.Combine("Controls", "ServerTab.Refresh.cs"));
        Assert.DoesNotContain("if (relation == QueryWindowRelation.QueryStats)\n        {\n            RangePicker.DataStartUtc", refresh, StringComparison.Ordinal);
        Assert.Contains("CorrelatedLanes.DataStartFound +=", LiteFile(Path.Combine("Controls", "ServerTab.xaml.cs")), StringComparison.Ordinal);
    }

    private static string ThisFile([CallerFilePath] string path = "") => path;

    private static bool IsDocOrDefinition(string text, int index)
    {
        var lineStart = text.LastIndexOf('\n', index) + 1;
        var line = text.Substring(lineStart, text.IndexOf('\n', index) is var e and >= 0 ? e - lineStart : text.Length - lineStart).TrimStart();
        return line.StartsWith("//", StringComparison.Ordinal) || line.StartsWith("///", StringComparison.Ordinal)
            || line.StartsWith("*", StringComparison.Ordinal) || line.StartsWith("/*", StringComparison.Ordinal)
            || line.Contains("internal static bool ApplyWindowFloorToBanner(", StringComparison.Ordinal);
    }

    [Fact]
    public void CollectorOfRelation_NamesARealCollectorForEveryScheduledRelation()
    {
        foreach (var relation in Enum.GetValues<QueryWindowRelation>())
        {
            var name = LiteTimeRange.CollectorOfRelation(relation);
            if (name is not null)
            {
                Assert.True(CollectorScheduleDefaults.All.ContainsKey(name), relation + " -> " + name);
            }
        }
    }
}
