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
/// What the web PostgreSQL tabs draw for the Database Trend, Query Duration Trend, Wait Trend and I/O Trend sections (#7394), from the shipped
/// <c>server-tabs.js</c>, <c>panels.js</c> and <c>charts.js</c> run under Node against a scripted <c>/api/read</c>: the chart per series group the
/// desktop viewer draws, blank values left blank, a statistics or profile reset marked on the chart, a latency chart with no measured value left
/// undrawn with its reason, and an empty answer said once. Node is skipped when it is not installed.
/// </summary>
public sealed class WebPgTrendChartsBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-pg-trend-charts-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the PostgreSQL trend charts harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the PostgreSQL trend charts harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static string[] Labels(JsonElement chart) => Strings(chart.GetProperty("labels"));

    /// <summary>#7549: a window with no temp bytes, no temp files and no deadlock draws one line each in place of two flat charts.</summary>
    [Fact]
    public void AQuietDatabase_SaysSoInOneLine_InPlaceOfFlatTempAndDeadlockCharts()
    {
        var r = Run("databaseQuiet");
        Assert.Equal(new[] { "Database Trend", "Database Transactions per Second" }, Strings(r.GetProperty("titles")));
        var empties = Strings(r.GetProperty("empties"));
        Assert.Contains("No temp bytes were written in this window.", empties);
        Assert.Contains("No temp files were created and no deadlocks were recorded in this window.", empties);
    }

    /// <summary>#7549: a quiet window that crossed a statistics reset says so in the one line, and one that did not leaves the line as it was.</summary>
    [Fact]
    public void AQuietWindowThatCrossedAStatsReset_SaysSoInTheOneLine()
    {
        var empties = Strings(Run("databaseQuietReset").GetProperty("empties"));
        Assert.Contains("No temp bytes were written in this window. (statistics were reset in this window)", empties);
        Assert.Contains("No temp files were created and no deadlocks were recorded in this window. (statistics were reset in this window)", empties);

        Assert.DoesNotContain(Strings(Run("databaseQuiet").GetProperty("empties")), e => e.Contains("reset", StringComparison.Ordinal));
    }

    /// <summary>#7549: one point above 0 in any series keeps the chart.</summary>
    [Fact]
    public void OnePointAboveZero_KeepsTheTempBytesChart_AndADeadlockKeepsTheCountsChart()
    {
        var r = Run("databaseOneDeadlock");
        Assert.Equal(
            new[] { "Database Trend", "Database Transactions per Second", "Database Temp Bytes", "Database Temp Files and Deadlocks" },
            Strings(r.GetProperty("titles")));
        Assert.DoesNotContain(Strings(r.GetProperty("empties")), e => e.StartsWith("No temp ", StringComparison.Ordinal));
    }

    [Fact]
    public void TheDatabaseTrend_DrawsTheDesktopsFourSeriesGroups_AsCharts_NotATable()
    {
        var r = Run("database");

        Assert.Equal(
            new[] { "Database Trend", "Database Transactions per Second", "Database Temp Bytes", "Database Temp Files and Deadlocks" },
            Strings(r.GetProperty("titles")));
        Assert.Equal(new[] { "Cache Hit %", "Worst Hit %", "Rollback %" }, Labels(r.GetProperty("percent")));
        Assert.Equal(new[] { "Xact/sec" }, Labels(r.GetProperty("xact")));
        Assert.Equal(new[] { "Temp Bytes" }, Labels(r.GetProperty("bytes")));
        Assert.Equal(new[] { "Temp Files", "Deadlocks" }, Labels(r.GetProperty("counts")));
        Assert.Equal(0, r.GetProperty("tables").GetInt32());
    }

    [Fact]
    public void AStatsReset_IsMarkedOnEveryDatabaseChart_AndNothingIsMarkedWithoutOne()
    {
        var reset = Run("database");
        foreach (var chart in new[] { "percent", "xact", "bytes", "counts" })
        {
            Assert.Equal(1, reset.GetProperty(chart).GetProperty("resets").GetInt32());
            Assert.Equal("Stats reset", reset.GetProperty(chart).GetProperty("resetName").GetString());
        }

        Assert.All(Strings(reset.GetProperty("keys")), k => Assert.Contains("Stats reset", k));
        Assert.Equal(4, reset.GetProperty("keys").GetArrayLength());

        var none = Run("databaseNoReset");
        Assert.Equal(0, none.GetProperty("percent").GetProperty("resets").GetInt32());
        Assert.Empty(none.GetProperty("keys").EnumerateArray());
    }

    [Fact]
    public void TheQueryDurationTrend_DrawsDurationAndCallRateAsTwoCharts_AndAnIntervalWithNoCallsStaysBlank()
    {
        var r = Run("query");

        Assert.Equal(new[] { "Mean ms/exec", "Peak ms/exec" }, Labels(r.GetProperty("duration")));
        Assert.Equal(new[] { "Calls/sec" }, Labels(r.GetProperty("calls")));
        var mean = r.GetProperty("mean").EnumerateArray().ToArray();
        Assert.Equal(JsonValueKind.Null, mean[1].ValueKind);
    }

    [Fact]
    public void TheWaitTrend_DrawsOneSeries_AndMarksAProfileReset()
    {
        var r = Run("wait");

        Assert.Equal(new[] { "Est. wait ms/sec" }, Labels(r.GetProperty("wait")));
        Assert.Equal(1, r.GetProperty("wait").GetProperty("resets").GetInt32());
        Assert.Equal("Profile reset", r.GetProperty("wait").GetProperty("resetName").GetString());
        Assert.Contains("Profile reset", Assert.Single(Strings(r.GetProperty("keys"))));
    }

    [Fact]
    public void TheIoTrend_DropsASeriesWithNoValue_KeepsABlankIntervalBlank_AndMarksTheReset()
    {
        var r = Run("io");

        /* Writes/sec is null on every point (no write counters), so it is not drawn; it must not become a zero line. */
        Assert.Equal(new[] { "Reads/sec", "Peak Reads/sec", "Evictions/sec" }, Labels(r.GetProperty("ops")));
        Assert.Equal(new[] { "Hit %" }, Labels(r.GetProperty("hit")));
        Assert.Equal(new[] { "Avg Read (ms)" }, Labels(r.GetProperty("read")));
        var latency = r.GetProperty("readValues").EnumerateArray().ToArray();
        Assert.Equal(JsonValueKind.Null, latency[1].ValueKind);
        Assert.Equal(0.4, latency[0].GetDouble());

        /* No byte counters: no chart, and the panel says why. */
        Assert.Equal(JsonValueKind.Null, r.GetProperty("bytes").ValueKind);
        Assert.Contains(Strings(r.GetProperty("empties")), e => e.StartsWith("No read bytes in this window", StringComparison.Ordinal));
        Assert.Equal(1, r.GetProperty("ops").GetProperty("resets").GetInt32());
        Assert.Equal(3, r.GetProperty("keys").GetArrayLength());
    }

    [Fact]
    public void WithTrackIoTimingOff_TheLatencyChartIsNotDrawn_AndSaysWhyInsteadOfPlottingZero()
    {
        var r = Run("ioTimingOff");

        var titles = Strings(r.GetProperty("titles"));
        Assert.DoesNotContain("I/O Avg Read (ms)", titles);
        Assert.Contains("I/O Trend", titles);
        Assert.Contains(Strings(r.GetProperty("empties")), e => e.StartsWith("No read latency", StringComparison.Ordinal) && e.Contains("has track_io_timing off", StringComparison.Ordinal));
        Assert.Contains(Strings(r.GetProperty("empties")), e => e.StartsWith("No read bytes", StringComparison.Ordinal) && e.Contains("no byte counters", StringComparison.Ordinal));
    }

    /* #7394: avg_read_ms is also null when the followed pair did no reads. A server with track_io_timing ON must not be told it is off. */
    [Fact]
    public void WithTrackIoTimingOn_AndAPairThatDidNoReads_TheStripsDoNotBlameTheSetting()
    {
        var r = Run("ioNoReads");

        var empties = Strings(r.GetProperty("empties"));
        Assert.DoesNotContain("I/O Avg Read (ms)", Strings(r.GetProperty("titles")));
        Assert.Contains(empties, e => e.StartsWith("No read latency", StringComparison.Ordinal) && e.Contains("did no reads", StringComparison.Ordinal));
        Assert.DoesNotContain(empties, e => e.Contains("track_io_timing", StringComparison.Ordinal));
        Assert.Contains(empties, e => e.StartsWith("No read bytes", StringComparison.Ordinal) && e.Contains("did no reads", StringComparison.Ordinal));
        Assert.DoesNotContain(empties, e => e.Contains("no byte counters", StringComparison.Ordinal));
    }

    /* The setting was never collected: io_timing_tracked false is only what the data showed, so the page names both causes and asserts neither. */
    [Fact]
    public void WhenTheTimingSettingWasNeverCollected_TheLatencyStripNamesBothCauses_AndDoesNotStateTheSetting()
    {
        var r = Run("ioTimingInferred");

        var latency = Strings(r.GetProperty("empties")).Single(e => e.StartsWith("No read latency", StringComparison.Ordinal));
        Assert.Contains("did no reads", latency, StringComparison.Ordinal);
        Assert.Contains("cannot tell which", latency, StringComparison.Ordinal);
        Assert.DoesNotContain("has track_io_timing off", latency, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEmptyAnswer_DrawsNoChart_AndSaysItOnceForEachTrend()
    {
        var r = Run("empty");

        Assert.Equal(0, r.GetProperty("charts").GetInt32());
        /* The Query Duration and Wait trends lead with their own plain sentence and keep the tool's text behind "More detail" (#7410). */
        Assert.Collection(
            Strings(r.GetProperty("activity")),
            e => Assert.Equal("No database trend.", e),
            e => Assert.Contains("No query trend.", e, StringComparison.Ordinal));
        Assert.Contains("No wait trend.", Assert.Single(Strings(r.GetProperty("waits"))), StringComparison.Ordinal);
        Assert.Equal(new[] { "No io trend." }, Strings(r.GetProperty("io")));
    }
}
