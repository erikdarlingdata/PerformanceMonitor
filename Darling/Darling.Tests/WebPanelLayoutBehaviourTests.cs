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
/// A half-width web panel with no partner on its row takes the whole row (#7534). <c>panel-layout.js</c> marks the last panel of an odd
/// run of span-1 panels <c>span-solo</c>, run here under Node (<c>web-panel-layout-harness.mjs</c>, skipped when Node is not installed).
/// </summary>
public sealed class WebPanelLayoutBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-panel-layout-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the panel layout harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the panel layout harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Names(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void ALoneHalfWidthPanel_TakesTheWholeRow()
    {
        var r = Run();
        Assert.Equal(new[] { "a" }, Names(r.GetProperty("loneBeforeWide")));
        Assert.Equal(new[] { "c", "d" }, Names(r.GetProperty("three")));
    }

    [Fact]
    public void TwoHalfWidthPanels_ShareARow()
    {
        Assert.Empty(Names(Run().GetProperty("pair")));
    }

    [Fact]
    public void APartnerThatHides_LeavesTheOtherFullWidth_AndTheMarkLiftsWhenItReturns()
    {
        var r = Run();
        Assert.Empty(Names(r.GetProperty("hiddenPartner").GetProperty("before")));
        Assert.Equal(new[] { "a" }, Names(r.GetProperty("hiddenPartner").GetProperty("after")));
        Assert.Empty(Names(r.GetProperty("shownAgain")));
    }

    [Fact]
    public void PanelsInADisplayContentsSlot_AreCountedInTheGrid()
    {
        var r = Run().GetProperty("slot");
        Assert.Equal(new[] { "inner" }, Names(r.GetProperty("alone")));
        Assert.Empty(Names(r.GetProperty("paired")));
    }

    [Fact]
    public void AnEmptyIntroLine_DoesNotSplitAPair_AndAFilledOneDoes()
    {
        var r = Run().GetProperty("intro");
        Assert.Empty(Names(r.GetProperty("empty")));
        Assert.Equal(new[] { "a", "b" }, Names(r.GetProperty("filled")));
    }

    [Fact]
    public void AddingAPanel_RunsTheSameWalkAgain()
    {
        var r = Run().GetProperty("observed");
        Assert.Equal(new[] { "a" }, Names(r.GetProperty("first")));
        Assert.Empty(Names(r.GetProperty("after")));
    }
}
