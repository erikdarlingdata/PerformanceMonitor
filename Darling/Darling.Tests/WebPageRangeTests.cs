/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The other nine web pages on the shared time range picker (#5562, lane L2b): the Alert History, Fleet Sweeps, Job History,
/// FinOps (Database Resources, High Impact, Optimization, Storage Growth), Custom Views and view-editor pages, and the server
/// page's "collected every N minutes" note. Source pins plus a Node run of the pure parts (skipped without Node).
/// </summary>
public sealed class WebPageRangeTests
{
    private static string Js(params string[] parts) =>
        ReadRepoFile(new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js" }.Concat(parts).ToArray()).ReplaceLineEndings("\n");

    private static bool TryRunNode(out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "page-range-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the page-range harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the page-range harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    [Fact]
    public void EveryFinOpsTabPicksItsWindowOnTheRollingOnlyPicker_NotASelect()
    {
        // R5: Compact, rolling-only, no calendar period, no custom end, an hour at least.
        var window = Js("pages", "finops", "window.js");
        Assert.Contains("compact: true,", window);
        Assert.Contains("rollingOnly: true,", window);
        Assert.Contains("minSpanMs: opts.minSpanMs || ROLLING_ONLY_MIN_SPAN_MS", window);
        Assert.Contains("read: \"get_finops\"", window);
        foreach (var tab in new[] { "database-resources", "high-impact", "optimization", "storage-growth" })
        {
            var src = Js("pages", "finops", tab + ".js");
            Assert.Contains("finopsWindowControl(", src);
            Assert.DoesNotContain("WINDOWS", src);
            Assert.DoesNotContain("range-select-inline", src);
        }
    }

    [Fact]
    public void TheStorageGrowthPickerTakesTheViewsOwnReachAndWholeDays()
    {
        var src = Js("pages", "finops", "storage-growth.js");
        Assert.Contains("const STORAGE_GROWTH_REACH_HOURS = 2160;", src);
        Assert.Contains("reachHours: STORAGE_GROWTH_REACH_HOURS,", src);
        Assert.Contains("minSpanMs: DAY_MS,", src);
        Assert.Contains("stepMs: DAY_MS,", src);

        // The reach is the view's, as the tool's own description says; the get_finops catalog entry says 168 for its other views.
        var tool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.cs");
        Assert.Contains("storage_growth: max 2160", tool);
    }

    [Fact]
    public void ViewsAndTheEditorTakeTheirRangeFromTheRollingPicker_AndNothingIsClamped()
    {
        var views = Js("pages", "views.js");
        Assert.Contains("rollingHoursPicker({", views);
        Assert.Contains("reachHours: MAX_RANGE_HOURS,", views);
        Assert.Contains("const MAX_RANGE_HOURS = 24 * 90;", views);
        Assert.DoesNotContain("Math.min(n * parseInt(unit.value, 10), MAX_RANGE_HOURS)", views);
        Assert.DoesNotContain("VIEW_RANGE_OPTIONS", views);

        // The view's default range is the editor's "view default": the compose runner's 90 days, refused beyond.
        var editor = Js("editor.js");
        Assert.Contains("rollingHoursPicker({", editor);
        Assert.Contains("reachHours: MAX_VIEW_RANGE_HOURS,", editor);
        Assert.Contains("const MAX_VIEW_RANGE_HOURS = 24 * 90;", editor);
        Assert.DoesNotContain("aria-label\": \"Default time range\" });\n  rangeSel.appendChild", editor);
    }

    [Fact]
    public void SweepsAndJobHistorySendTheEndOfAFinishedRange()
    {
        var sweeps = Js("pages", "sweeps.js");
        Assert.Contains("\"&as_of=\" + encodeURIComponent(w.asOf)", sweeps);
        Assert.Contains("read: \"get_sweep_reports\"", sweeps);
        Assert.DoesNotContain("SPANS", sweeps);

        var jobs = Js("pages", "job-history.js");
        Assert.Contains("read: \"get_job_history\"", jobs);
        Assert.Contains("if (w.asOf) p.as_of = w.asOf;", jobs);
        Assert.DoesNotContain("WINDOWS", jobs);
    }

    [Fact]
    public void EveryServerTabThatDeclaresACollectorNamesARealOne_AndTheViewerAgreesOnTheSharedTabs()
    {
        var tabs = Js("pages", "server-tabs.js");
        var declared = Regex.Matches(tabs, "(?m)^    id: \"([a-z-]+)\",\n    label: \"([^\"]+)\",\n    collector: \"([a-z_]+)\",").Select(m => (m.Groups[2].Value, m.Groups[3].Value)).ToList();
        Assert.Equal(9, declared.Count);

        foreach (var (_, collector) in declared)
            Assert.True(CollectorScheduleDefaults.All.ContainsKey(collector), collector + " is not a collector the schedule knows");

        // The SQL Server tabs the Viewer also has name the same main collector (the Viewer's table: ViewerTimeRangeWindow.MainCollectorFor).
        foreach (var (label, collector) in declared.Take(7))
        {
            var viewer = PerformanceMonitor.Darling.Viewer.ViewerTimeRangeWindow.MainCollectorFor(label);
            Assert.Equal(viewer, collector);
        }
    }

    [Fact]
    public void TheServerPageGivesThePickerTheTabsActualCollectorInterval_AndNeverWidensTheRange()
    {
        var server = Js("pages", "server.js");
        Assert.Contains("serverCatalog(server).then((catalog) => {", server);
        Assert.Contains("picker.setSampleInterval(collectorIntervalFromCatalog(catalog, tab.collector));", server);
        // The note is set before the keep-the-panels return, so a poll that keeps the grid still gets it.
        Assert.True(server.IndexOf("  applySampleNote();", StringComparison.Ordinal) < server.IndexOf("if (keepGrid) {", StringComparison.Ordinal));
        // The server's own schedule: the catalog is asked for this server.
        Assert.Contains("\"/api/catalog?server=\" + encodeURIComponent(server)", Js("page-range.js"));
    }

    [Fact]
    public void ThePureParts_RunUnderNode()
    {
        if (!TryRunNode(out var r)) return;

        // The interval is the catalog's own (a per-server schedule row over the default), and a collector it does not know gives none.
        var interval = r.GetProperty("interval");
        Assert.Equal(1, interval.GetProperty("waitStatsDefault").GetInt32());
        Assert.Equal(5, interval.GetProperty("waitStatsServerOverride").GetInt32());
        Assert.Equal(JsonValueKind.Null, interval.GetProperty("unknownCollector").ValueKind);
        Assert.Equal(JsonValueKind.Null, interval.GetProperty("noCollector").ValueKind);
        Assert.Equal(JsonValueKind.Null, interval.GetProperty("noCatalog").ValueKind);

        // Fewer than 3 samples at the interval: the one-line note. 3 or more: none.
        var note = r.GetProperty("note");
        Assert.Equal("Data here is collected every 5 minutes.", note.GetProperty("fiveMinutesAtFive").GetString());
        Assert.Equal("Data here is collected every 5 minutes.", note.GetProperty("fourteenMinutesAtFive").GetString());
        Assert.Equal(JsonValueKind.Null, note.GetProperty("fifteenMinutesAtFive").ValueKind);
        Assert.Equal(JsonValueKind.Null, note.GetProperty("fiveMinutesAtOne").ValueKind);
        Assert.Equal("Data here is collected every hour.", note.GetProperty("thirtyMinutesAtHour").GetString());
        Assert.Equal(JsonValueKind.Null, note.GetProperty("threeHoursAtHour").ValueKind);
        Assert.Equal(JsonValueKind.Null, note.GetProperty("noInterval").ValueKind);

        // R5: only a rolling, whole-hour length of an hour or more; a daily read wants whole days of a day or more.
        var rolling = r.GetProperty("rolling");
        Assert.Equal(JsonValueKind.Null, rolling.GetProperty("ok").ValueKind);
        Assert.Equal("This page reads at least 1h back from now.", rolling.GetProperty("underAnHour").GetString());
        Assert.Equal("This page reads whole hours back from now, such as 6h or 3d.", rolling.GetProperty("fractionalHours").GetString());
        foreach (var name in new[] { "calendar", "finished", "since" })
            Assert.Equal("This page reads a length back from now, so pick a rolling length such as 24h or 7d.", rolling.GetProperty(name).GetString());
        Assert.Equal(JsonValueKind.Null, rolling.GetProperty("daysOk").ValueKind);
        Assert.Equal("This page reads whole days back from now, such as 7d or 30d.", rolling.GetProperty("daysHours").GetString());
        Assert.Equal("This page reads at least 1d back from now.", rolling.GetProperty("daysBelowFloor").GetString());

        // A held range gives the read whole hours (rounded up) and, when finished, its end.
        var window = r.GetProperty("window");
        Assert.Equal(1, window.GetProperty("thirtyMinutes").GetProperty("hours").GetInt32());
        Assert.Equal(JsonValueKind.Null, window.GetProperty("rolling").GetProperty("asOf").ValueKind);
        Assert.Equal(24, window.GetProperty("finished").GetProperty("hours").GetInt32());
        Assert.Equal("2026-10-08T10:00:00.000Z", window.GetProperty("finished").GetProperty("asOf").GetString());

        // The sweep timeline and the job runs are cut at the range's start (newest first, so a row cap cannot hide a run in range).
        Assert.Equal(3, r.GetProperty("sweeps").GetProperty("kept").GetInt32());
        Assert.Equal(2, r.GetProperty("sweeps").GetProperty("all").GetInt32());
        Assert.Equal(2, r.GetProperty("jobs").GetProperty("kept").GetInt32());
    }
}
