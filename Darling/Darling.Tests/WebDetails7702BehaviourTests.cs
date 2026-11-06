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
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The small web details of #7702, run from the shipped page scripts under Node (<c>web-details-7702-harness.mjs</c>): the FinOps
/// Wait Stats Summary prints its wait figures as durations, the version store chart's ticks carry only the decimals the step needs, the
/// Database Scoped Configuration subtitle says when the snapshot was taken in words, the Configuration Audit groups a whole-number figure,
/// Buffer Pool Residency prints the relation kind in words, Admin prints the engine in words, an Index Bloat row with no estimate has no
/// basis, the Alert Settings headings share one style and keep sp_server_diagnostics as a name, the notify severity carries its word, an
/// event-count trend draws the buckets the read left out at 0, through the chart the panel draws with, and a DMV snapshot trend is drawn
/// as it came. Node is skipped when it is not installed.
/// </summary>
public sealed class WebDetails7702BehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-details-7702-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the web details 7702 harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the web details 7702 harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.ValueKind == JsonValueKind.Null ? null! : e.GetString()!).ToArray();

    [Fact]
    public void TheFinOpsWaitStatsSummary_PrintsItsWaitFiguresAsDurations_AndItsLabelsCarryNoUnit()
    {
        var run = Run();
        var cells = run.GetProperty("waitCells");
        Assert.Equal("6.7 min", cells.GetProperty("Total wait").GetString());
        Assert.Equal("2.2 min", cells.GetProperty("Top wait").GetString());
        Assert.DoesNotContain(Strings(run.GetProperty("waitLabels")), l => l.Contains("(ms)", StringComparison.Ordinal));
    }

    [Fact]
    public void TheVersionStoreChart_PrintsAWholeNumberTickWithoutDecimals_AndATooltipWithTwo()
    {
        Assert.Equal(new[] { "20", "0", "0.5", "12.35" }, Strings(Run().GetProperty("pvsTicks")));
    }

    [Fact]
    public void TheConfigurationAudit_GroupsAWholeNumberFigure_AndLeavesAnyOtherValueAlone()
    {
        var run = Run();
        Assert.Equal(new[] { "92,160", "2,147,483,647", "ON", "1.5", "—", "57,344" }, Strings(run.GetProperty("auditFigures")));
        var cells = run.GetProperty("auditCells");
        Assert.Equal("92,160", cells.GetProperty("Current").GetString());
        Assert.Equal("57,344", cells.GetProperty("Suggested").GetString());
    }

    [Fact]
    public void TheDatabaseScopedConfigurationSubtitle_SaysWhenTheSnapshotWasTaken_NotTheSystemView()
    {
        var heading = Run().GetProperty("scopedHeading").GetString()!;
        Assert.Contains("Newest snapshot, taken when the service connected", heading, StringComparison.Ordinal);
        Assert.DoesNotContain("sys.", heading, StringComparison.Ordinal);
    }

    [Fact]
    public void BufferPoolResidency_PrintsTheRelationKindInWords()
    {
        var run = Run();
        Assert.Equal(new[] { "Table", "Index", "TOAST", "Materialized view", "x", "—" }, Strings(run.GetProperty("kindWords")));
        Assert.Equal("Table", run.GetProperty("kindCell").GetString());
    }

    [Fact]
    public void Admin_PrintsTheEngineInWords_OnTheServersAndServerActionsTabs()
    {
        var run = Run();
        Assert.Equal(new[] { "SQL Server", "PostgreSQL", "PostgreSQL", "other", "—" }, Strings(run.GetProperty("engineWords")));
        Assert.Equal(new[] { "SQL Server", "PostgreSQL" }, Strings(run.GetProperty("serverEngineCells")));
        Assert.Equal(new[] { "SQL Server", "PostgreSQL" }, Strings(run.GetProperty("actionEngineCells")));
    }

    [Fact]
    public void AnIndexBloatRowTheReadSkipped_HasNoBasis()
    {
        Assert.Equal(new[] { "—", "Estimated", "Measured" }, Strings(Run().GetProperty("basisCells")));
    }

    [Fact]
    public void TheAlertSettingsHeadings_CapitalizeEachWord()
    {
        Assert.Equal(
            new[] { "Alert Thresholds", "Long Running Query Filters", "Health Bands", "Global Alert Filters", "Automated Analysis", "Fleet Sweep Reports", "Daily Health Report" },
            Strings(Run().GetProperty("sectionTitles")));
    }

    [Fact]
    public void TheLongRunningQueryFilter_KeepsTheProcedureName()
    {
        Assert.Equal("Long-running query: exclude sp_server_diagnostics", Run().GetProperty("spServerDiagnosticsLabel").GetString());
    }

    [Fact]
    public void TheNotifySeverity_CarriesTheWordTheFindingsPageGivesTheScore()
    {
        var run = Run();
        Assert.Equal(new[] { "Critical (1.5)", "Warning (0.75)", "Info (0.2)", "Critical (1.5)", "—" }, Strings(run.GetProperty("notifySeverity")));
        Assert.Equal(new[] { "Automated Analysis: notify severity = Critical (1.5)" }, Strings(run.GetProperty("notifySeverityRow")));
    }

    [Fact]
    public void AnEventCountTrend_DrawsTheBucketsTheReadLeftOutAtZero_AcrossTheWindow()
    {
        var run = Run();
        var fill = run.GetProperty("zeroFill");
        Assert.Equal(16, fill.GetProperty("count").GetInt32());
        Assert.Equal("2026-11-05T09:50:00.000Z", fill.GetProperty("first").GetString());
        Assert.Equal("2026-11-05T10:05:00.000Z", fill.GetProperty("last").GetString());
        Assert.Equal(14, fill.GetProperty("zeros").GetInt32());
        Assert.Equal(2, fill.GetProperty("kept").GetInt32());
        Assert.Equal(5, fill.GetProperty("sum").GetInt32());
        Assert.Equal(new[] { 4, 1 }, run.GetProperty("zeroFillNoWindow").EnumerateArray().Select(e => e.GetInt32()).ToArray());
        Assert.Equal(0, run.GetProperty("zeroFillEmpty").GetInt32());
    }

    /// <summary>
    /// Through the line viz, as the Blocking tab draws it: the chart is handed every 5 minute bucket of the one hour window (the reply's own
    /// bucket, not the points' step), aligned on the first point, with the two reported buckets kept and the other eleven at 0.
    /// </summary>
    [Fact]
    public void AnEventCountPanel_HandsTheChartTheFilledBuckets_OfTheReplysBucketAcrossThePanelsWindow()
    {
        var run = Run();
        var fill = run.GetProperty("zeroFillPanel");
        Assert.Equal(13, fill.GetProperty("count").GetInt32());
        Assert.Equal("2026-11-05T09:05:00.000Z", fill.GetProperty("first").GetString());
        Assert.Equal("2026-11-05T10:05:00.000Z", fill.GetProperty("last").GetString());
        Assert.Equal(11, fill.GetProperty("zeros").GetInt32());
        Assert.Equal(2, fill.GetProperty("kept").GetInt32());
        Assert.Equal(5, fill.GetProperty("sum").GetInt32());
        Assert.Equal(new[] { "2026-11-05T09:05:00.000Z", "2026-11-05T10:05:00.000Z" }, Strings(fill.GetProperty("window")));
        var noSource = run.GetProperty("zeroFillPanelNoSource");
        Assert.Equal(13, noSource.GetProperty("count").GetInt32());
        Assert.Equal(11, noSource.GetProperty("zeros").GetInt32());
    }

    /// <summary>
    /// The DMV snapshot samples blocking on a schedule, so a bucket it has no row for is time nobody observed: its trend reaches the chart
    /// as the read returned it, two points and no 0.
    /// </summary>
    [Fact]
    public void ADmvSnapshotTrend_IsHandedToTheChartAsItCame_WithNoFill()
    {
        var snapshot = Run().GetProperty("zeroFillPanelSnapshot");
        Assert.Equal(2, snapshot.GetProperty("count").GetInt32());
        Assert.Equal(0, snapshot.GetProperty("zeros").GetInt32());
        Assert.Equal(2, snapshot.GetProperty("kept").GetInt32());
        Assert.Equal("2026-11-05T10:00:00.000Z", snapshot.GetProperty("first").GetString());
        Assert.Equal("2026-11-05T10:05:00.000Z", snapshot.GetProperty("last").GetString());
    }
}
