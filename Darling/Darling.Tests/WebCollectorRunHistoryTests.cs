/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5227: on the web SQL Server Collection Health tab a click on a Collectors row re-requests the Collection Log for that
/// collector over 7 days, a chip clears the pick, and the pick survives the 60 s rebuild of the tab.
/// </summary>
public sealed class WebCollectorRunHistoryTests
{
    private static JsonElement RunHarness()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-collector-run-history-harness.mjs"));
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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the collector run-history harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the collector run-history harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void AnUnpickedTab_ReadsTheLogForTheWindow_WithNoCollectorFilter()
    {
        var r = RunHarness();
        var before = r.GetProperty("before");

        Assert.Equal(1, before.GetArrayLength());
        Assert.Equal("24", before[0].GetProperty("hours").GetString());
        Assert.False(before[0].TryGetProperty("collector_name", out _));
        Assert.True(r.GetProperty("clickable").GetBoolean(), "a Collectors row is clickable (pointer cursor and a title)");
    }

    [Fact]
    public void ClickingACollectorRow_ReadsThatCollectorsLog_OverSevenDays_AndShowsTheChip()
    {
        var r = RunHarness();
        var click = r.GetProperty("afterClick");

        Assert.Equal(1, click.GetArrayLength());
        Assert.Equal("query_store", click[0].GetProperty("collector_name").GetString());
        Assert.Equal("168", click[0].GetProperty("hours").GetString());
        Assert.Equal("collector query_store \u00d7", r.GetProperty("chipAfterClick")[0].GetString());
    }

    [Fact]
    public void ThePick_SurvivesARebuildOfTheTab()
    {
        var r = RunHarness();
        var rebuilt = r.GetProperty("afterRebuild");

        Assert.Equal(1, rebuilt.GetArrayLength());
        Assert.Equal("query_store", rebuilt[0].GetProperty("collector_name").GetString());
        Assert.Equal("168", rebuilt[0].GetProperty("hours").GetString());
        Assert.Equal(1, r.GetProperty("rebuildChips").GetInt32());
    }

    [Fact]
    public void TheChip_ClearsThePick_AndTheLogReturnsToTheWindow()
    {
        var cleared = RunHarness().GetProperty("afterClear");

        Assert.Equal(1, cleared.GetArrayLength());
        Assert.False(cleared[0].TryGetProperty("collector_name", out _));
        Assert.Equal("24", cleared[0].GetProperty("hours").GetString());
    }
}
