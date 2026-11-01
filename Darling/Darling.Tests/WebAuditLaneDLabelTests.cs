/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Source pins for the #5644 web UI audit labels W9, W14, W16 and W19: each label says what its value is. No JS runner
/// is available to the C# tests, so these read the page sources, the way <c>MuteRulesPageTests</c> does.
/// </summary>
public sealed class WebAuditLaneDLabelTests
{
    private static string Js(params string[] parts) =>
        ReadRepoFile([.. new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js" }, .. parts]).ReplaceLineEndings("\n");

    [Fact]
    public void W9_TheMemoryGrantWindowShowsTheTimeOfThePeakGrantBesideThePeakGrant()
    {
        var tabs = Js("pages", "server-tabs.js");
        Assert.Contains("{ key: \"peak_waiters_at\", label: \"Peak waiters at\", format: \"time\" },", tabs, StringComparison.Ordinal);
        Assert.Contains(
            "{ key: \"peak_granted_memory_mb\", label: \"Peak granted\", format: \"mb\" },\n  { key: \"peak_granted_at\", label: \"Peak granted at\", format: \"time\" },",
            tabs, StringComparison.Ordinal);
        Assert.DoesNotContain("label: \"Peak at\"", tabs, StringComparison.Ordinal);
    }

    [Fact]
    public void W14_TriageAndMuteAreTheFirstAlertHistoryColumns()
    {
        var alerts = Js("pages", "alerts.js");
        Assert.Contains("[SELECT_COLUMN, TRIAGE_COLUMN, MUTE_COLUMN].concat(base)", alerts, StringComparison.Ordinal);
        Assert.Contains(": [TRIAGE_COLUMN].concat(base)", alerts, StringComparison.Ordinal);
        Assert.DoesNotContain("{ key: \"triage\", label: \"Triage\", csv: false, render: (a) => triageCell(a) },\n];", alerts, StringComparison.Ordinal);
    }

    [Fact]
    public void W16_TheDeadlockTrendChartAndTheDeadlocksTableAreNamedApartAndEmptyTheSameWay()
    {
        var tabs = Js("pages", "server-tabs.js");
        Assert.Contains("linePanel(\"Deadlock trend\", \"get_deadlock_trend\", { server, hours: ctx.hours }, \"trend\", \"time\", COUNT_SERIES, {\n        subtitle: ctx.label,\n        format: \"int\",\n        atTimeItem: \"deadlocks\",\n        emptyText: DEADLOCKS_EMPTY_TEXT,", tabs, StringComparison.Ordinal);
        Assert.Contains("const DEADLOCKS_EMPTY_TEXT = \"No deadlocks in this window.\";", tabs, StringComparison.Ordinal);
        Assert.DoesNotContain("an empty trend here means none happened", tabs, StringComparison.Ordinal);
    }

    [Fact]
    public void W19_EachMisleadingLabelNamesItsValue()
    {
        var tabs = Js("pages", "server-tabs.js");
        Assert.Contains("{ key: \"hyperthread_ratio\", label: \"Logical CPUs per socket\"", tabs, StringComparison.Ordinal);
        Assert.DoesNotContain("label: \"HT ratio\"", tabs, StringComparison.Ordinal);
        Assert.Contains("{ key: \"plan_cache_mb\", label: \"Plan cache (all cache stores)\", format: \"mb\" },", tabs, StringComparison.Ordinal);
        Assert.Contains("{ key: \"summary.total_size_mb\", label: \"Cached plans size\", format: \"mb\" },", tabs, StringComparison.Ordinal);
        Assert.Contains("{ key: \"pct_of_sweep_budget_per_run\", label: \"% of run budget\"", tabs, StringComparison.Ordinal);
        /* fmtMs prints seconds above a second ("32.9 s"), so the header carries no unit. */
        Assert.Contains("{ key: \"duration_ms\", label: \"Duration\", format: \"ms\" },\n  { key: \"signal_duration_ms\", label: \"Signal duration\", format: \"ms\" },", tabs, StringComparison.Ordinal);

        var utilization = Js("pages", "finops", "utilization.js");
        foreach (var label in new[] { "Avg SQL Server CPU %", "P95 SQL Server CPU %", "Max SQL Server CPU %" })
            Assert.Contains("label: \"" + label + "\"", utilization, StringComparison.Ordinal);
        Assert.Contains("label: \"Avg SQL Server CPU %\"", Js("pages", "finops", "server-inventory.js"), StringComparison.Ordinal);

        var fleet = Js("pages", "fleet.js");
        Assert.Contains("chip(c.total_cpu_percent != null ? \"Total CPU\" : \"CPU\", cpuValue, c.cpu_severity, cpuDetail)", fleet, StringComparison.Ordinal);
        Assert.Contains("fmtInt(threadsInUse) + \" in use / \" + fmtInt(c.total_threads) + \" threads\"", fleet, StringComparison.Ordinal);
    }
}
