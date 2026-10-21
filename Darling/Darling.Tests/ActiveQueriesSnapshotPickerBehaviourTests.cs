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
/// #6302: the Active Queries tab opens on the latest stored snapshot with a picker over the window's snapshots, as the desktop viewer
/// does, and "All snapshots in window" keeps the table as it was. The shipped page runs under Node
/// (<c>active-snapshots-harness.mjs</c>) against a scripted <c>/api/read</c>; Node is skipped when it is not installed.
/// </summary>
public sealed class ActiveQueriesSnapshotPickerBehaviourTests
{
    private const string Newest = "2026-10-21T12:03:00.0000000";
    private const string Middle = "2026-10-21T12:02:00.0000000";

    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "active-snapshots-harness.mjs"));
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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the active snapshots harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the active snapshots harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string?[] Asks(JsonElement step, string key) =>
        step.GetProperty("asks").EnumerateArray().Select(a => a.GetProperty(key).GetString()).ToArray();

    [Fact]
    public void TheTabOpensOnTheLatestSnapshot_NotTheWholeWindow()
    {
        var first = Run("default").GetProperty("first");

        // One read for the snapshot list, one for the newest snapshot's rows; no flat read over the window.
        Assert.Equal(new string?[] { "true", null }, Asks(first, "snapshots_only"));
        Assert.Equal(new string?[] { null, Newest }, Asks(first, "collection_time"));
        Assert.Equal("latest", first.GetProperty("selected").GetString());
        Assert.Contains("snapshot at", first.GetProperty("subtitle").GetString());
    }

    [Fact]
    public void ThePickerListsEverySnapshotWithItsRowCount_BetweenTheLatestAndAllChoices()
    {
        var options = Run("default").GetProperty("first").GetProperty("options").EnumerateArray().ToArray();

        Assert.Equal(new[] { "latest", "all", Newest, Middle, "2026-10-21T12:01:00.0000000" },
            options.Select(o => o.GetProperty("value").GetString()).ToArray());
        Assert.Equal("All snapshots in window", options[1].GetProperty("label").GetString());
        Assert.EndsWith("(3 rows)", options[2].GetProperty("label").GetString());
        Assert.EndsWith("(1 row)", options[4].GetProperty("label").GetString());
    }

    [Fact]
    public void PickingATimeReadsThatSnapshot_AllReadsTheWindowAsBefore_LatestComesBack()
    {
        var run = Run("pick");

        Assert.Equal(new string?[] { Middle }, Asks(run.GetProperty("picked"), "collection_time"));
        Assert.Equal(Middle, run.GetProperty("picked").GetProperty("selected").GetString());

        // "All snapshots in window" is the request the page made before the picker: the 50-row page, no snapshot named.
        var all = run.GetProperty("all");
        Assert.Equal(new string?[] { null }, Asks(all, "collection_time"));
        Assert.Equal(new string?[] { "50" }, Asks(all, "limit"));
        Assert.Equal("all", all.GetProperty("selected").GetString());

        Assert.Equal(new string?[] { Newest }, Asks(run.GetProperty("latest"), "collection_time"));
        Assert.Equal("latest", run.GetProperty("latest").GetProperty("selected").GetString());
    }

    [Fact]
    public void AJumpToAMoment_SelectsTheNearestSnapshot_AndKeepsItAcrossTheRebuild()
    {
        var run = Run("jump");

        // 12:02:20 is nearer 12:02 than 12:03.
        var first = run.GetProperty("first");
        Assert.Equal(new string?[] { null, Middle }, Asks(first, "collection_time"));
        Assert.Equal(Middle, first.GetProperty("selected").GetString());

        // The rebuild reads the list again and draws the same snapshot, not whichever is newest.
        Assert.Equal(new string?[] { null, Middle }, Asks(run.GetProperty("again"), "collection_time"));
    }

    [Fact]
    public void AListHoldingNoSnapshots_DrawsTheWholeWindowTableWithNoPicker()
    {
        var first = Run("listEmpty").GetProperty("first");

        Assert.False(first.GetProperty("hasSelect").GetBoolean());
        Assert.True(first.GetProperty("hasCard").GetBoolean());
        Assert.Equal(new string?[] { "true", null }, Asks(first, "snapshots_only"));
        Assert.Equal(new string?[] { null, null }, Asks(first, "collection_time"));
    }

    [Fact]
    public void AWindowLongerThanTheHistoryTheServerKeeps_AsksTheSnapshotListAgainAtTheKeptHours()
    {
        var first = Run("kept").GetProperty("first");
        string?[] HoursOf(Func<JsonElement, bool> pick) =>
            first.GetProperty("asks").EnumerateArray().Where(pick).Select(a => a.GetProperty("hours").GetString()).ToArray();

        // The list and the newest snapshot's rows are each refused at 720 hours and asked again at 168, as every ranged read is.
        Assert.Equal(new string?[] { "720", "168" }, HoursOf(a => a.GetProperty("snapshots_only").GetString() == "true"));
        Assert.Equal(new string?[] { "720", "168" }, HoursOf(a => a.GetProperty("collection_time").GetString() == Newest));
        Assert.Equal("latest", first.GetProperty("selected").GetString());
    }

    [Fact]
    public void AWaitFilter_NarrowsTheSnapshotListToo()
    {
        var first = Run("wait").GetProperty("first");

        /* The time slicer's strip (#6301) charts the whole window's snapshots, so it asks for the list with no wait: it is the one ask without
           a wait_type, and the list and the grid are the two that carry it. */
        Assert.Equal(new string?[] { "PAGEIOLATCH_SH", "PAGEIOLATCH_SH" }, Asks(first, "wait_type").Where(w => w is not null).ToArray());
    }
}
