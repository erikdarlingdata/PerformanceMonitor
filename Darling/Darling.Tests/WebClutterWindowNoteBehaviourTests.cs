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
/// #8929, #9382: the strip on the two Query Store clutter panels at a window that reaches past what the arms hold. The window block carries a span per
/// arm (<c>plan_churn_effective_hours_back</c> and <c>wait_effective_hours_back</c>, null when that arm reaches the window start), and the shipped
/// <c>pages/server-tabs.js</c>, <c>util.js</c> and <c>plain-text.js</c> run under Node (<c>web-clutter-window-note-harness.mjs</c>) with a stub
/// fetch that answers the read with such a block. The Clutter panel (plan churn) says plan churn's span, the Overhead panel (QDS waits) says the wait
/// arm's, a panel whose arm is not cut draws no strip, and neither names a field or a store table. Node is skipped when it is not installed.
/// </summary>
public sealed class WebClutterWindowNoteBehaviourTests
{
    private const string Clutter = "Query Store Clutter";
    private const string Overhead = "Query Store Overhead (per server)";

    private static string[] NoticesOf(string planChurn, string wait, string title)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-clutter-window-note-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(planChurn);
        psi.ArgumentList.Add(wait);

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
                Assert.Fail("the clutter window note harness did not finish in 60 s for " + planChurn + " / " + wait);
            }

            Assert.True(proc.ExitCode == 0, "the clutter window note harness failed for " + planChurn + " / " + wait + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            Assert.Empty(doc.RootElement.GetProperty("rejections").EnumerateArray());
            var panels = doc.RootElement.GetProperty("panels").EnumerateArray()
                .Where(p => p.GetProperty("title").GetString() == title).ToArray();
            Assert.Single(panels);
            return panels[0].GetProperty("notices").EnumerateArray().Select(n => n.GetString()!).ToArray();
        }
    }

    private static void AssertWholeSentenceWithNoFieldOrTable(string notice)
    {
        Assert.DoesNotContain("hours_back", notice);
        Assert.DoesNotContain("wait_stats", notice);
        Assert.DoesNotContain("query_store_stats", notice);
        Assert.DoesNotContain("effective", notice);
        Assert.DoesNotContain("(", notice);
        Assert.EndsWith(".", notice);
    }

    [Fact]
    public void PlanChurnCutOnly_TheClutterPanelSaysItsSpan_AndTheOverheadPanelSaysNothing()
    {
        var notice = Assert.Single(NoticesOf("72", "null", Clutter));

        Assert.Equal("Plan churn covers the last 3 days, not the whole time range.", notice);
        AssertWholeSentenceWithNoFieldOrTable(notice);
        Assert.Empty(NoticesOf("72", "null", Overhead));
    }

    [Fact]
    public void WaitArmCutOnly_TheOverheadPanelSaysItsSpan_AndTheClutterPanelSaysNothing()
    {
        var notice = Assert.Single(NoticesOf("null", "2", Overhead));

        Assert.Equal("The QDS wait table covers the last 2 hours, not the whole time range.", notice);
        AssertWholeSentenceWithNoFieldOrTable(notice);
        Assert.Empty(NoticesOf("null", "2", Clutter));
    }

    [Fact]
    public void BothArmsCut_EachPanelSaysItsOwnArmsSpan()
    {
        var clutter = Assert.Single(NoticesOf("72", "2", Clutter));
        var overhead = Assert.Single(NoticesOf("72", "2", Overhead));

        Assert.Equal("Plan churn covers the last 3 days, not the whole time range.", clutter);
        Assert.Equal("The QDS wait table covers the last 2 hours, not the whole time range.", overhead);
        AssertWholeSentenceWithNoFieldOrTable(clutter);
        AssertWholeSentenceWithNoFieldOrTable(overhead);
    }

    [Fact]
    public void ASpanUnderTwoDays_IsSaidInHours()
    {
        var notice = Assert.Single(NoticesOf("30", "null", Clutter));

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
        var notice = Assert.Single(NoticesOf(effective, "null", Clutter));

        Assert.Contains("covers " + span + ", not the whole time range", notice);
        Assert.DoesNotContain("1 hours", notice);
        Assert.DoesNotContain("1 days", notice);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-5")]
    [InlineData("null")]
    [InlineData("true")]
    [InlineData("none")]
    public void AnArmValueThatIsNoPositiveNumber_DrawsNoStrip(string value)
    {
        Assert.Empty(NoticesOf(value, "null", Clutter));
        Assert.Empty(NoticesOf("null", value, Overhead));
    }

    [Fact]
    public void ANumberSentAsText_IsStillRead()
    {
        var notice = Assert.Single(NoticesOf("\"72\"", "null", Clutter));

        Assert.Contains("covers the last 3 days", notice);
    }
}
