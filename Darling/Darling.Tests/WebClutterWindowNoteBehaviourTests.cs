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
/// #8929: the strip on the two Query Store clutter panels at a window that reaches past the raw tier. The service writes the sentence for
/// an MCP caller ("covers effective_hours_back, not hours_back", naming <c>query_store_stats</c>). The shipped <c>pages/server-tabs.js</c>,
/// <c>util.js</c> and <c>plain-text.js</c> run under Node (<c>web-clutter-window-note-harness.mjs</c>) with a stub fetch that answers the
/// read with that sentence and the scenario's <c>effective_hours_back</c>. The page says the span the reply states, in hours or days, and
/// names no field and no store table. Node is skipped when it is not installed.
/// </summary>
public sealed class WebClutterWindowNoteBehaviourTests
{
    private static string[] NoticesOf(string effective, string title)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-clutter-window-note-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(effective);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return [];
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the clutter window note harness did not finish in 60 s for " + effective);
            }

            Assert.True(proc.ExitCode == 0, "the clutter window note harness failed for " + effective + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            Assert.Empty(doc.RootElement.GetProperty("rejections").EnumerateArray());
            var panels = doc.RootElement.GetProperty("panels").EnumerateArray()
                .Where(p => p.GetProperty("title").GetString() == title).ToArray();
            Assert.Single(panels);
            return panels[0].GetProperty("notices").EnumerateArray().Select(n => n.GetString()!).ToArray();
        }
    }

    [Theory]
    [InlineData("Query Store Clutter")]
    [InlineData("Query Store Overhead (per server)")]
    public void ThePanelsStrip_NamesTheSpanTheReplyStates_NotTheFieldName(string title)
    {
        var notice = Assert.Single(NoticesOf("72", title));

        Assert.Contains("covers the last 3 days, not the whole time range", notice);
        Assert.DoesNotContain("_hours_back", notice);
        Assert.DoesNotContain("hours_back", notice);
        Assert.DoesNotContain("query_store_stats", notice);
        Assert.Contains("raw query statistics", notice);
    }

    [Fact]
    public void ASpanUnderTwoDays_IsSaidInHours()
    {
        var notice = Assert.Single(NoticesOf("30", "Query Store Clutter"));

        Assert.Contains("covers the last 30 hours, not the whole time range", notice);
    }

    [Theory]
    [InlineData("47", "the last 47 hours")]
    [InlineData("48", "the last 2 days")]
    [InlineData("1", "the last 1 hour")]
    [InlineData("1.5", "the last 1.5 hours")]
    [InlineData("24", "the last 24 hours")]
    public void TheSpanText_SwitchesToDaysAtTwoDays_AndSaysOneInTheSingular(string effective, string span)
    {
        var notice = Assert.Single(NoticesOf(effective, "Query Store Clutter"));

        Assert.Contains("covers " + span + ", not the whole time range", notice);
        Assert.DoesNotContain("1 hours", notice);
        Assert.DoesNotContain("1 days", notice);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-5")]
    [InlineData("null")]
    [InlineData("true")]
    [InlineData("\"72\"")]
    public void AValueThatIsNoPositiveNumber_SaysNoSpan_AndStillNamesNoField(string effective)
    {
        var notice = Assert.Single(NoticesOf(effective, "Query Store Clutter"));

        if (effective == "\"72\"")
            Assert.Contains("covers the last 3 days", notice);
        else
            Assert.DoesNotContain("the last ", notice);
        Assert.DoesNotContain("hours_back", notice);
        Assert.DoesNotContain("query_store_stats", notice);
    }

    [Fact]
    public void AReplyWithNoSpan_StillNamesNoFieldOrTable()
    {
        var notice = Assert.Single(NoticesOf("none", "Query Store Clutter"));

        Assert.DoesNotContain("hours_back", notice);
        Assert.DoesNotContain("query_store_stats", notice);
    }
}
