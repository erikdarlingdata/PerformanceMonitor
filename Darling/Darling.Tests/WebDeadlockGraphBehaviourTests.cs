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
/// The web deadlock graph (#5246), from the shipped <c>deadlock-graph.js</c> run under Node
/// (<c>web-deadlock-graph-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class WebDeadlockGraphBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-deadlock-graph-harness.mjs"));
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
                Assert.Fail("the deadlock graph harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the deadlock graph harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string Str(JsonElement r, string name) => r.GetProperty(name).GetString()!;
    private static int Int(JsonElement r, string name) => r.GetProperty(name).GetInt32();

    [Fact]
    public void TheGraph_IsNotDrawnUntilTheRowIsOpened_AndIsDrawnOnce()
    {
        var r = Run("lazy");
        Assert.Equal("details", Str(r, "tag"));
        Assert.Equal("Graph (2 processes)", Str(r, "summary"));
        Assert.Equal(0, Int(r, "svgBefore"));
        Assert.Equal(1, Int(r, "svgAfter"));
        Assert.Equal(1, Int(r, "svgAfterReopen"));
    }

    [Fact]
    public void TheGraph_DrawsACardPerProcess_AnArrowPerEdge_AndMarksTheVictim()
    {
        var r = Run("draws");
        Assert.Equal(2, Int(r, "nodes"));
        Assert.Equal(1, Int(r, "victims"));
        Assert.Equal(2, Int(r, "paths"));
        Assert.Equal(2, Int(r, "markerEnds"));
        Assert.Equal(0, Int(r, "cycleFrames"));
        Assert.Equal("0 0 700 300", Str(r, "viewBox"));
        Assert.All(r.GetProperty("labels").EnumerateArray(), l => Assert.Equal("KEY AppDb.dbo.t · X", l.GetString()));
        Assert.Contains("Waiter requests X, owner holds U (keylock)", r.GetProperty("titles").EnumerateArray().Select(t => t.GetString()));
        Assert.Equal("SPID 51", r.GetProperty("selectedAtOpen")[0].GetString());
        Assert.Equal("SPID 51 (victim)", Str(r, "head"));
    }

    [Fact]
    public void ClickingOrPressingEnterOnACard_ShowsItsStatementAsText()
    {
        var r = Run("click");
        Assert.Equal("SPID 52", Str(r, "head"));
        Assert.Equal("update dbo.t set a = 1", Str(r, "pre"));
        Assert.Equal(1, Int(r, "cutNote"));
        Assert.Equal(1, Int(r, "selected"));
        Assert.Equal("SPID 51 (victim)", Str(r, "headAfterEnter"));
    }

    [Fact]
    public void HostileStatementText_IsShownAsText_AndCreatesNoElement()
    {
        var r = Run("hostile");
        Assert.Equal(0, Int(r, "scripts"));
        Assert.Contains("<script>alert(1)</script>", Str(r, "pre"));
    }

    [Fact]
    public void SeveralCycles_GetADashedFrameEachAndAreCountedInTheSummary()
    {
        var r = Run("twoCycles");
        Assert.Equal(2, Int(r, "cycleFrames"));
        Assert.Equal(new[] { "Cycle 1 (2)", "Cycle 2 (2)" }, r.GetProperty("cycleLabels").EnumerateArray().Select(l => l.GetString()).ToArray());
        Assert.Equal("Graph (2 processes, 2 cycles)", Str(r, "summary"));
    }

    [Fact]
    public void AParallelSelfEdge_IsALoopOverTheCard()
    {
        var r = Run("selfLoop");
        Assert.Equal(1, Int(r, "paths"));
        Assert.True(r.GetProperty("curve").GetBoolean());
        Assert.Equal(0, Int(r, "cycleFrames"));
        Assert.Equal("Graph (1 process)", Str(r, "summary"));
    }

    [Fact]
    public void ATooLargeDeadlock_SaysSoInsteadOfDrawing()
    {
        var r = Run("tooLarge");
        Assert.Equal("#text", Str(r, "tag"));
        Assert.Contains("75 processes", Str(r, "text"));
        Assert.Contains("Save XML", Str(r, "text"));
    }

    [Fact]
    public void APreviewXml_SaysTheGraphNeedsTheFullXml()
    {
        var r = Run("truncatedXml");
        Assert.Contains("needs the full XML", Str(r, "preview"));
        Assert.Equal("—", Str(r, "none"));
    }

    [Fact]
    public void AnOpenGraph_StaysOpenAcrossARebuild_ForThatServerOnly()
    {
        var r = Run("openSurvivesRebuild");
        Assert.True(r.GetProperty("reopened").GetBoolean());
        Assert.Equal(1, Int(r, "svg"));
        Assert.False(r.GetProperty("otherServerOpen").GetBoolean());
        Assert.Equal(0, Int(r, "otherServerSvg"));
    }
}
