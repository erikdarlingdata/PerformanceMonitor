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
/// Where an open plan or history panel sits and what it watches (click-through 1), from the shipped <c>plan-row.js</c> run
/// under Node (<c>web-plan-row-harness.mjs</c>). Node is skipped when it is not installed. The panel is pinned to the grid's
/// visible area, so a grid scrolled right to reach the Plan column does not leave the panel's text off-screen; and one
/// observer per open panel is let go when the panel closes, redraws or leaves the page.
/// </summary>
public sealed class PlanRowBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-plan-row-harness.mjs"));
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
                Assert.Fail("the plan row harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the plan row harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static void AssertPin(JsonElement r, string name, int left, int width)
    {
        Assert.Equal(left, r.GetProperty(name).GetProperty("left").GetInt32());
        Assert.Equal(width, r.GetProperty(name).GetProperty("width").GetInt32());
    }

    [Fact]
    public void ThePanel_CoversTheGridsVisibleArea_WhateverTheScroll()
    {
        var r = Run("geometry");
        // Scrolled 2487 px right in a 600 px wrap: the panel starts at the visible left edge, not at the row's own left edge.
        AssertPin(r, "scrolledRight", 2487, 600);
        AssertPin(r, "notScrolled", 0, 600);
        // A table narrower than the wrap: the panel is the row's width, never wider.
        AssertPin(r, "narrowTable", 0, 400);
        // The wrap's own offset and border count.
        AssertPin(r, "offsetWrap", 1251, 600);
        // Never pushed past either end of the row.
        AssertPin(r, "pastTheEnd", 2487, 600);
        AssertPin(r, "beforeTheStart", 0, 600);
    }

    [Fact]
    public void ThePanel_FollowsTheWrapsScroll_AndItsResize()
    {
        var r = Run("pinsToVisibleArea");
        Assert.Equal("2487px", r.GetProperty("afterOpen").GetProperty("left").GetString());
        Assert.Equal("600px", r.GetProperty("afterOpen").GetProperty("width").GetString());
        Assert.Equal("auto", r.GetProperty("afterOpen").GetProperty("right").GetString());
        AssertPinText(r, "afterScroll", "1000px", "600px");
        AssertPinText(r, "afterResize", "1000px", "450px");
        // The panel (it loads and grows), the row (a re-wrap changes its height) and the wrap (the visible width).
        Assert.Equal(new[] { "div:plan-panel", "tr:", "div:table-wrap" }, r.GetProperty("watching").EnumerateArray().Select(e => e.GetString()).ToArray());
    }

    private static void AssertPinText(JsonElement r, string name, string left, string width)
    {
        Assert.Equal(left, r.GetProperty(name).GetProperty("left").GetString());
        Assert.Equal(width, r.GetProperty(name).GetProperty("width").GetString());
    }

    [Fact]
    public void ClosingAPanel_LeavesNoObserver_AndRedrawingNeverStacksThem()
    {
        var r = Run("observerLifecycle");
        Assert.Equal(0, r.GetProperty("start").GetInt32());
        Assert.Equal(1, r.GetProperty("afterOpen").GetInt32());
        Assert.Equal(1, r.GetProperty("scrollListenersOpen").GetInt32());
        // Closing disconnects the observer and removes the wrap's scroll listener.
        Assert.Equal(0, r.GetProperty("afterClose").GetInt32());
        Assert.Equal(0, r.GetProperty("scrollListenersClosed").GetInt32());
        Assert.Equal("plan-cell", r.GetProperty("closedClass").GetString());
        // Four redraws of one open panel still leave exactly one observer and one scroll listener.
        Assert.Equal(1, r.GetProperty("afterRedraws").GetInt32());
        Assert.Equal(1, r.GetProperty("scrollListenersRedrawn").GetInt32());
    }

    [Fact]
    public void AGridRerender_LetsGoOfThePanelsOfTheRowsItDiscarded()
    {
        var r = Run("observerLifecycle");
        Assert.Equal(2, r.GetProperty("beforeRerender").GetInt32());
        // The discarded row's observer is disconnected; the panel still on the page keeps its own.
        Assert.Equal(1, r.GetProperty("afterRerender").GetInt32());
        Assert.Equal(1, r.GetProperty("scrollListenersAfterRerender").GetInt32());
    }
}
