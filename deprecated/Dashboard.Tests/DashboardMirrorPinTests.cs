/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Analysis;
using PerformanceMonitorDashboard.Mcp;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// #3653: the frozen Dashboard's mirrors of five of the 2026-09 brains-review honesty fixes, pinned in the
/// only idiom the Dashboard's test project has for its SQL-Server-side readers — the source text, because
/// there is no test database — plus the pure-logic pieces called directly. Each section names the merged
/// PR whose class it mirrors; the Dashboard says the same thing in its own voice, and these pins hold the
/// voice to the meaning: a page count is not a total, one slow wait does not page, a mute that matched
/// nothing says so, an unbanded collector dot is Unknown (its own file: ServerHealthStatusCollectorVerdictTests),
/// and the per-second counter does not truncate.
/// </summary>
public class DashboardMirrorPinTests
{
    /* ---------------- #3594's class: page counts stop posing as totals ---------------- */

    [Theory]
    [InlineData(0, 30, false)]      // reader saw nothing: nothing beyond the page
    [InlineData(5, 30, false)]      // reader under its own cap, all rows fit: the whole window is here
    [InlineData(30, 30, false)]     // exactly the page, reader under its cap: still the whole window
    [InlineData(31, 30, true)]      // a row in hand beyond the page was dropped here
    [InlineData(100, 30, true)]     // reader at its cap AND beyond the page: dropped here regardless
    [InlineData(100, 100, null)]    // reader filled to its cap, page took it all: rows beyond are unknowable
    [InlineData(100, 500, null)]    // a limit above the reader's cap cannot widen the page; same unknown
    [InlineData(99, 100, false)]    // one under the cap: the reader saw the whole window
    public void PageTruncated_ReportsWhatIsKnown_AndNullWhereTheReaderCapHidesIt(int readerRows, int limit, bool? expected)
    {
        Assert.Equal(100, DatabaseService.EventGridCap);
        Assert.Equal(expected, McpBlockingTools.PageTruncated(readerRows, limit));
    }

    [Fact]
    public void BlockingReaders_EveryRowCapLiteral_IsTheNamedGridCap()
    {
        /* The readers keep their fixed TOP (the grids' cap) rather than a parameterised one; the const the MCP
           tools publish as limit_applied_by_reader must BE that literal. Every numeric TOP in the file is either
           a single-row fetch (the on-demand XML reads) or the cap. */
        var src = Read("deprecated", "Dashboard", "Services", "DatabaseService.QueryPerformance.Blocking.cs");
        var tops = Regex.Matches(src, @"SELECT TOP \((\d+)\)").Select(m => int.Parse(m.Groups[1].Value)).ToList();
        Assert.NotEmpty(tops);
        Assert.All(tops, t => Assert.True(t == 1 || t == DatabaseService.EventGridCap, $"TOP ({t}) is neither a single-row fetch nor EventGridCap"));
        Assert.Equal(4, tops.Count(t => t == DatabaseService.EventGridCap));   // BPR + deadlock readers, both window arms
        Assert.Contains("const int gridCap = EventGridCap;", src);           // the DMV merge's re-cap
    }

    [Fact]
    public void PagedTools_PublishReturnedCounts_NotTotals()
    {
        var blocking = Read("deprecated", "Dashboard", "Mcp", "McpBlockingTools.cs");
        Assert.DoesNotContain("total_events", blocking);
        Assert.DoesNotContain("total_deadlocks", blocking);
        Assert.Contains("events_returned = result.Count", blocking);
        Assert.Contains("deadlocks_returned = result.Count", blocking);
        Assert.Equal(2, Regex.Matches(blocking, @"truncated = PageTruncated\(rows\.Count, limit\)").Count);
        Assert.Equal(2, Regex.Matches(blocking, @"limit_applied_by_reader = DatabaseService\.EventGridCap").Count);

        /* The alert-history store takes a limit, so that tool uses the shared dialect: fetch one past the limit
           and OBSERVE the extra row, never infer from count >= limit. */
        var alerts = Read("deprecated", "Dashboard", "Mcp", "McpAlertTools.cs");
        Assert.DoesNotContain("total_alerts", alerts);
        Assert.Contains("GetAlertHistory(hours_back, limit + 1)", alerts);
        Assert.Contains("var truncated = fetched.Count > limit;", alerts);
        Assert.Contains("alerts_returned = alerts.Count", alerts);
        Assert.DoesNotContain(">= limit", alerts);
    }

    /* ---------------- #3593's class: one slow wait stops paging ---------------- */

    [Fact]
    public void PoisonWaitRead_IsAWindowAccumulation_NotTheNewestDeltas()
    {
        var src = Read("deprecated", "Dashboard", "Services", "DatabaseService.NocHealth.cs");
        var sql = Between(src, "private async Task<List<PoisonWaitDelta>> GetPoisonWaitDeltasAsync", "var results = new List<PoisonWaitDelta>();");

        /* Presences: the sums, the grouping, the window bound from the evaluator, worst-first. */
        Assert.Contains("wait_time_ms_delta = SUM(wait_time_ms_delta)", sql);
        Assert.Contains("waiting_tasks_count_delta = SUM(waiting_tasks_count_delta)", sql);
        Assert.Contains("GROUP BY wait_type", sql);
        Assert.Contains("ORDER BY SUM(wait_time_ms_delta) DESC", sql);
        Assert.Contains("DATEADD(MINUTE, -@window_minutes, SYSDATETIME())", sql);
        Assert.Contains("Value = PoisonWaitEvaluator.WindowMinutes", Between(src, "GetPoisonWaitDeltasAsync", "Failed to get poison wait deltas"));

        /* Absences: the retired shape's three tells. A TOP on a sum is an undercount; the task filter dropped a
           task still waiting across the interval boundary; a literal window can drift from the evaluator's. */
        Assert.DoesNotContain("TOP (3)", sql);
        Assert.DoesNotContain("AND waiting_tasks_count_delta > 0", sql);
        Assert.DoesNotContain("DATEADD(MINUTE, -10", sql);

        /* The wait-type list is the evaluator's, in the store's spelling. */
        foreach (var waitType in PoisonWaitEvaluator.SqlServerWaitTypes)
        {
            Assert.Contains($"N'{waitType}'", sql);
        }
    }

    [Fact]
    public void PoisonWaitLoop_GradesOnTheSharedBars_HoldsOnAnEmptyRead_AndNoLongerReadsTheKnob()
    {
        var src = Read("deprecated", "Dashboard", "MainWindow.AlertEngine.cs");
        var block = Between(src, "/* Poison wait alerts.", "/* Long-running query alerts */");

        Assert.Contains("PoisonWaitEvaluator.Grade(w.DeltaMs) is not null", block);
        Assert.DoesNotContain("AvgMsPerWait >= prefs.PoisonWaitThresholdMs", block);
        Assert.DoesNotContain("prefs.PoisonWaitThresholdMs", block);

        /* The clear arm requires an observation: an empty read is collector silence, not a quiet server. */
        Assert.Contains("else if (health.PoisonWaits.Count > 0 && _activePoisonWaitAlert.TryRemove(serverId, out var wasPoisonWait) && wasPoisonWait)", block);

        /* The threshold text every record/email carries is the shared bar, derived not typed. */
        Assert.Contains("PoisonWaitEvaluator.WarningAvgWaiters * poisonWindowSeconds:N0}s accumulated over {PoisonWaitEvaluator.WindowMinutes}m", block);
        Assert.Equal(3, Regex.Matches(block, @"\bpoisonThresholdText\b(?!\s*=)").Count);   // fire record, email, clear record
    }

    [Fact]
    public void PoisonWaitBars_AreTheSharedEvaluators_AndTheDashboardGradesLikeTheEngine()
    {
        /* The Dashboard references PoisonWaitEvaluator directly (its csproj references PerformanceMonitor.Alerting),
           so there is no copied constant to pin equal — these pin the figures the loop's comments and the Settings
           tooltip state, so a shared-side move shows up here as a falsified label rather than a silent drift. */
        Assert.Equal(10, PoisonWaitEvaluator.WindowMinutes);
        Assert.Equal(1.0, PoisonWaitEvaluator.WarningAvgWaiters);
        Assert.Equal(10.0, PoisonWaitEvaluator.CriticalAvgWaiters);

        /* The exact shapes the brief names: one 600 ms wait is silent; 600 s accumulated fires; one ms under does not. */
        Assert.Null(PoisonWaitEvaluator.Grade(600));
        Assert.Null(PoisonWaitEvaluator.Grade(599_999));
        Assert.Equal(AlertSeverityLevel.Warning, PoisonWaitEvaluator.Grade(600_000));
        Assert.Equal(AlertSeverityLevel.Critical, PoisonWaitEvaluator.Grade(6_000_000));

        /* The storm the average could not see: 10,000 waits of 50 ms a minute, sustained across the window —
           5,000 s of starvation on a 50 ms average, which the retired 500 ms-avg bar never noticed. */
        Assert.Equal(AlertSeverityLevel.Warning, PoisonWaitEvaluator.Grade(10 * 10_000 * 50));

        /* The Settings preview and tooltip speak in these figures. */
        var xaml = Read("deprecated", "Dashboard", "SettingsWindow.xaml");
        Assert.Contains("IsEnabled=\"False\"", Between(xaml, "x:Name=\"PoisonWaitThresholdTextBox\"", "/>"));
        Assert.Contains("600 s of wait inside 10 minutes (WARNING) or 6,000 s (CRITICAL)", xaml);
        var settings = Read("deprecated", "Dashboard", "SettingsWindow.xaml.cs");
        Assert.DoesNotContain("ms avg\"", settings);
        Assert.Contains("PoisonWaitEvaluator.WarningAvgWaiters", settings);
    }

    /* ---------------- #3615's class: a mute that matched nothing says so ---------------- */

    [Fact]
    public void MuteAnalysisFinding_ReportsWhatTheWriteDid()
    {
        var src = Read("deprecated", "Dashboard", "Mcp", "McpAnalysisTools.cs");
        var body = Between(src, "[McpServerTool(Name = \"mute_analysis_finding\")", "McpHelpers.FormatError(\"mute_analysis_finding\"");

        Assert.Contains("var registered = await analysisService.MuteFindingAsync(finding, reason);", body);
        Assert.Contains("var matchedNow = await analysisService.CountStoredFindingsAsync(serverId, story_path_hash);", body);
        Assert.Contains("status = matchedNow > 0 ? \"muted\" : \"muted_unmatched\"", body);
        Assert.Contains("status = \"error\"", body);
        Assert.Contains("status = \"invalid\"", body);
        Assert.Contains("matched_now = matchedNow", body);
        Assert.DoesNotContain("new { status = \"muted\", story_path_hash, reason }", body);

        /* The description says what the payload says. */
        var description = Between(body, "Description(\"", "\")]");
        Assert.Contains("registered", description);
        Assert.Contains("matched_now", description);
        Assert.Contains("muted_unmatched", description);

        /* The store says whether the row landed, and the count is a real read of the findings table. */
        Assert.Equal(typeof(Task<bool>), typeof(SqlServerFindingStore).GetMethod(nameof(SqlServerFindingStore.MuteStoryAsync))!.ReturnType);
        Assert.Equal(typeof(Task<bool>), typeof(AnalysisService).GetMethod(nameof(AnalysisService.MuteFindingAsync))!.ReturnType);
        Assert.Equal(typeof(Task<long>), typeof(SqlServerFindingStore).GetMethod(nameof(SqlServerFindingStore.CountStoredFindingsAsync))!.ReturnType);
        var storeSrc = Read("deprecated", "Dashboard", "Analysis", "SqlServerFindingStore.cs");
        var countSql = Between(storeSrc, "public async Task<long> CountStoredFindingsAsync", "var result = await cmd.ExecuteScalarAsync();");
        Assert.Contains("COUNT_BIG(*)", countSql);
        Assert.Contains("FROM config.analysis_findings", countSql);
        Assert.Contains("WHERE server_id = @serverId", countSql);
        Assert.Contains("AND   story_path_hash = @storyPathHash", countSql);
    }

    /* ---------------- #3540 A11: the per-second counter stops truncating ---------------- */

    [Fact]
    public void PerfmonReads_RecomputeThePerSecondRate_AsAFraction()
    {
        /* The table's computed column is bigint / integer — T-SQL integer division — so 59 events over a 60-second
           interval read as 0/sec. Both perfmon readers now compute the rate on the read as a fraction; the raw
           computed column is no longer selected by either. */
        var src = Read("deprecated", "Dashboard", "Services", "DatabaseService.ResourceMetrics.Perfmon.cs");
        var lf = src.Replace("\r\n", "\n");
        Assert.Equal(2, Regex.Matches(lf, @"cntr_value_per_second =\n\s+ps\.cntr_value_delta \* 1\.0 /\n\s+NULLIF\(ps\.sample_interval_seconds, 0\)").Count);
        Assert.DoesNotContain("ps.cntr_value_per_second", src);

        /* The model carries the fraction; the reader materialises it as a double (the numeric the * 1.0 produces). */
        Assert.Equal(typeof(double?), typeof(PerfmonStatsItem).GetProperty(nameof(PerfmonStatsItem.CntrValuePerSecond))!.PropertyType);
        Assert.Equal(2, Regex.Matches(src, @"CntrValuePerSecond = reader\.IsDBNull\(10\) \? null : Convert\.ToDouble\(reader\.GetValue\(10\), CultureInfo\.InvariantCulture\)").Count);
        Assert.DoesNotContain("reader.GetInt64(10)", src);
    }

    /* ---------------- #3616's class: the lab-box markers ---------------- */

    [Fact]
    public void AnomalyDetector_NoLongerNamesTheLabBox_AndSaysWhatWasMeasured()
    {
        var src = Read("deprecated", "Dashboard", "Analysis", "SqlServerAnomalyDetector.cs");
        Assert.DoesNotContain("HAMMERDB", src, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("SQL2025", src);
        Assert.Contains("unmeasured\n    /// on the Dashboard tier", src.Replace("\r\n", "\n"));
        /* Comment-only: the numbers did not move. */
        Assert.Contains("private const double DefaultRatioThreshold = 4.0;", src);
        Assert.Contains("private const double WaitProfileFallbackMsPerSec = 250.0;", src);
    }

    /* ---------------- helpers ---------------- */

    private static string Read(params string[] pathParts)
    {
        var root = FindRepoRoot();
        Assert.True(root is not null, $"Could not locate the repo root (a directory containing deprecated/Dashboard/Dashboard.csproj) by walking up from {AppContext.BaseDirectory}.");
        var path = Path.Combine(new[] { root! }.Concat(pathParts).ToArray());
        Assert.True(File.Exists(path), $"Source file not found: {path}");
        return File.ReadAllText(path);
    }

    /// <summary>The slice of <paramref name="src"/> from the first <paramref name="start"/> to the next <paramref name="end"/>, both asserted present.</summary>
    private static string Between(string src, string start, string end)
    {
        var s = src.IndexOf(start, StringComparison.Ordinal);
        Assert.True(s >= 0, $"anchor not found: {start}");
        var e = src.IndexOf(end, s + start.Length, StringComparison.Ordinal);
        Assert.True(e >= 0, $"anchor not found after start: {end}");
        return src.Substring(s, e - s);
    }

    /// <summary>Walks up from the test output directory to the repo root — the directory holding the Dashboard project. Mirrors ThemeParityTests.</summary>
    private static string? FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        for (int i = 0; i < 10 && dir is not null; i++)
        {
            if (File.Exists(Path.Combine(dir.FullName, "deprecated", "Dashboard", "Dashboard.csproj")))
            {
                return dir.FullName;
            }
            dir = dir.Parent;
        }
        return null;
    }
}
