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
/// #7924: a saved notebook with its auto-refresh on keeps its document on screen across a tick. Each cell is refreshed in place
/// once its own new read returns, the reader's scroll position stays, and a zoomed panel cell stays zoomed. Run as the shipped
/// page scripts under Node against a scripted <c>/api/compose/run</c> (<c>web-notebook-refresh-harness.mjs</c>). Node is skipped
/// when it is not installed.
/// </summary>
public sealed class WebNotebookRefreshBehaviourTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-notebook-refresh-harness.mjs"));
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
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the notebook refresh harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the notebook refresh harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();
    private static string? Str(JsonElement e, string name) => e.GetProperty(name).ValueKind == JsonValueKind.Null ? null : e.GetProperty(name).GetString();

    [Fact]
    public void TheTick_KeepsTheDocumentOnScreen_AndRefreshesEachCellInPlace()
    {
        var run = Result.Value;
        Assert.Equal(3, run.GetProperty("cellCount").GetInt32());
        Assert.Equal(0, run.GetProperty("firstLoading").GetInt32());
        // The tick reads every panel cell again, as a dashboard's poll does, with the document still on screen.
        Assert.Equal(Strings(run.GetProperty("firstCalls")), Strings(run.GetProperty("tickCalls")));
        Assert.True(run.GetProperty("docKeptWhileReading").GetBoolean(), "the document left the page while the tick's reads were outstanding");
        Assert.Equal(0, run.GetProperty("loadingOnPageWhileReading").GetInt32());
        Assert.True(run.GetProperty("docKeptAfterTick").GetBoolean(), "the document was replaced when the tick's reads returned");
        Assert.True(run.GetProperty("cellsKeptAfterTick").GetBoolean(), "the cells were replaced instead of refreshed in place");
        Assert.Equal(0, run.GetProperty("loadingAfterTick").GetInt32());
        Assert.Equal("Night notes", run.GetProperty("headingAfterTick").GetString());
        Assert.Equal("Notes", run.GetProperty("markdownAfterTick").GetString());
    }

    [Fact]
    public void TheTick_KeepsTheReadersScrollPosition()
    {
        var run = Result.Value;
        var before = run.GetProperty("scrollBefore").GetInt32();
        Assert.True(before > 0, "the harness did not scroll the page");
        Assert.Equal(before, run.GetProperty("scrollWhileReading").GetInt32());
        Assert.Equal(before, run.GetProperty("scrollAfterTick").GetInt32());
    }

    [Fact]
    public void AZoomedPanelCell_StaysZoomedAcrossTheTick()
    {
        var run = Result.Value;
        var zoomed = run.GetProperty("zoomed");
        Assert.NotNull(Str(zoomed, "windowStart"));
        Assert.Equal(1, run.GetProperty("zoomChipBefore").GetInt32());
        var tick = run.GetProperty("tickWindowZoomed");
        Assert.Equal(Str(zoomed, "windowStart"), Str(tick, "windowStart"));
        Assert.Equal(Str(zoomed, "windowEnd"), Str(tick, "windowEnd"));
        Assert.Equal(1, run.GetProperty("zoomChipAfterTick").GetInt32());
        // The other panel cell was not zoomed, and the tick does not zoom it.
        var other = run.GetProperty("tickWindowOther");
        Assert.Null(Str(other, "windowStart"));
        Assert.Null(Str(other, "windowEnd"));
    }

    [Fact]
    public void ASecondTick_LandsTheSameWay()
    {
        var run = Result.Value;
        Assert.Equal(0, run.GetProperty("secondTickLoadingOnPage").GetInt32());
        Assert.True(run.GetProperty("secondTickDocKept").GetBoolean(), "the second tick replaced the document");
        Assert.Equal(Str(run.GetProperty("zoomed"), "windowStart"), Str(run.GetProperty("secondTickWindowZoomed"), "windowStart"));
    }
}
