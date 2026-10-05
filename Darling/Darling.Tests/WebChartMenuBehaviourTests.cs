/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Every web line chart carries a small menu (a visible button, or right-click) with Copy Image, Save Image As,
/// Reset zoom (only while zoomed), Export Data to CSV and Show Data Source (only when the chart knows its read). These run
/// the shipped <c>charts.js</c> under Node (<c>web-chart-menu-harness.mjs</c>). Skipped when Node is not installed.
/// </summary>
public sealed class WebChartMenuBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-chart-menu-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped chart script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the chart menu harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the chart menu harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void TheMenuOffersTheDesktopItemsInOrder_AndOpensFromTheButtonAndFromRightClick()
    {
        var r = Run();
        Assert.True(r.GetProperty("hasButton").GetBoolean());
        Assert.Equal(
            new[] { "Copy Image", "Save Image As...", "Export Data to CSV...", "Show Data Source" },
            Strings(r.GetProperty("itemsUnzoomed")));
        Assert.Equal("true", r.GetProperty("expanded").GetString());
        Assert.Equal(4, r.GetProperty("contextMenuItems").GetInt32());
        Assert.True(r.GetProperty("contextMenuPrevented").GetBoolean());
    }

    [Fact]
    public void ExportDataToCsv_WritesOneRowPerPointAndSeries()
    {
        var csv = Strings(Run().GetProperty("csv"));
        Assert.Equal("DateTime (UTC),Series,Value", csv[0]);
        Assert.Equal(1 + 11 * 2, csv.Length);
        Assert.Equal("2026-01-01 00:03:00,Alpha,3", csv[7]);
        Assert.Equal("2026-01-01 00:03:00,\"Beta, two\",6", csv[8]);
    }

    [Fact]
    public void ShowDataSource_NamesTheReadAndItsParameters_AndIsHiddenWithoutOne()
    {
        var r = Run();
        var text = r.GetProperty("sourceText").GetString()!;
        Assert.Contains("Read: get_wait_stats", text);
        Assert.Contains("hours = 4", text);
        Assert.Contains("server = A", text);
        Assert.DoesNotContain("Show Data Source", Strings(r.GetProperty("noSourceItems")));
    }

    [Fact]
    public void ResetZoom_IsOfferedOnlyWhileZoomed_AndClearsTheZoom()
    {
        var r = Run();
        Assert.DoesNotContain("Reset zoom", Strings(r.GetProperty("itemsUnzoomed")));
        Assert.Contains("Reset zoom", Strings(r.GetProperty("zoomedItems")));
        Assert.False(r.GetProperty("zoomAfterReset").GetBoolean());
        Assert.DoesNotContain("Reset zoom", Strings(r.GetProperty("menuAfterReset")));
    }

    [Fact]
    public void ARightClickMenu_IsPlacedByLeftAndTopOnly_AndStaysInsideTheChart()
    {
        var style = Run().GetProperty("rightClickStyle");
        Assert.Equal("auto", style.GetProperty("right").GetString());
        Assert.Equal("824px", style.GetProperty("left").GetString());
        Assert.Equal("220px", style.GetProperty("top").GetString());
    }

    [Fact]
    public void ExportDataToCsv_OnAZoomedChart_WritesEveryLoadedPoint()
    {
        Assert.Equal(1 + 11 * 2, Run().GetProperty("zoomedCsvRows").GetInt32());
    }

    [Fact]
    public void TabbingOutOfTheMenu_ClosesIt()
    {
        Assert.Equal(0, Run().GetProperty("menuAfterTab").GetInt32());
    }

    [Fact]
    public void TheOpenMenuAndSourcePanel_SurviveARebuildOfTheSameChart_ButNotOnAnotherChart()
    {
        var r = Run();
        Assert.Equal(1, r.GetProperty("rebuiltMenuOpen").GetInt32());
        Assert.Contains("Read: get_wait_stats", r.GetProperty("rebuiltSource").GetString());
        Assert.Equal(0, r.GetProperty("otherChartMenuOpen").GetInt32());
    }
}
