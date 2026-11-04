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
/// A long web grid scrolls inside its own box (#7393), and the auto-refresh swaps in a new box that starts at the top. The shipped
/// <c>keepScroll</c> (<c>refresh-in-place.js</c>) puts a grid back where the reader had scrolled it, the way it does the page (#6933).
/// Run under Node from <c>web-grid-scroll-harness.mjs</c> over grids the real <c>panels.js</c> draws. Skipped when Node is not installed.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class GridScrollKeepTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-grid-scroll-harness.mjs"));
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
                Assert.Fail("the grid scroll harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the grid scroll harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static int Int(JsonElement r, string name) => r.GetProperty(name).GetInt32();

    [Fact]
    public void A_refresh_puts_a_scrolled_grid_back_where_the_reader_had_it()
    {
        var r = Run("swapKeepsPosition");
        Assert.Equal(700, Int(r, "top"));
        Assert.Equal(40, Int(r, "left"));
    }

    [Fact]
    public void A_grid_that_came_back_shorter_stops_at_its_new_end()
    {
        var r = Run("shorterGridClamps");
        Assert.Equal(Int(r, "max"), Int(r, "top"));
        Assert.True(Int(r, "top") > 0);
    }

    [Fact]
    public void Two_grids_with_one_name_each_get_their_own_position_back()
    {
        var r = Run("twoGridsOneName");
        Assert.Equal(100, Int(r, "first"));
        Assert.Equal(900, Int(r, "second"));
    }

    [Fact]
    public void A_grid_the_refresh_did_not_replace_is_left_alone_and_the_replaced_one_is_restored()
    {
        var r = Run("otherGridUntouched");
        Assert.Equal(500, Int(r, "swapped"));
        Assert.Equal(0, Int(r, "other"));
        Assert.True(r.GetProperty("sameBox").GetBoolean());
    }

    [Fact]
    public void A_grid_is_not_given_the_position_of_a_same_name_grid_that_lost_its_box()
    {
        var r = Run("wrapRemovedAheadOfSameName");
        Assert.Equal(1, Int(r, "wraps"));
        Assert.True(r.GetProperty("sameBox").GetBoolean());
        Assert.Equal(200, Int(r, "b"));
    }

    [Fact]
    public void A_grid_at_the_top_stays_at_the_top()
    {
        Assert.Equal(0, Int(Run("topGridStaysTop"), "top"));
    }

    [Fact]
    public void A_different_grid_in_the_same_place_does_not_inherit_the_position()
    {
        Assert.Equal(0, Int(Run("differentGridNotRestored"), "top"));
    }
}
