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
/// #7655: three display details on the web charts, tiles and grids. The Blocking tab's Waiting Tasks and Lock Waits axes name their unit and
/// group their thousands as the other axes do, and the PostgreSQL Severity tiles and the Severity column of the grids under them print a
/// plain word rather than the read's token. The shipped page runs under Node (<c>web-details-7655-harness.mjs</c>) against a scripted
/// <c>/api/read</c>; Node is skipped when it is not installed.
/// </summary>
public sealed class WebDetails7655BehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-details-7655-harness.mjs"));
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
                Assert.Fail("the display details harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the display details harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e, string name) => e.GetProperty(name).EnumerateArray().Select(x => x.GetString()!).ToArray();

    /// <summary>
    /// Waiting Tasks charts total wait milliseconds and its axis read "0, 10000 ... 50000" with no unit anywhere on the chart (the legend
    /// lists wait types). The axis now prints the wait in the unit of its largest tick, as the other duration axes do, so 46,800 ms of
    /// waiting reads in seconds and the top tick is "50 s".
    /// </summary>
    [Fact]
    public void WaitingTasks_PrintsItsAxisInTheUnitOfItsLargestTick()
    {
        var axis = Run("blocking").GetProperty("waitingTasks");

        Assert.True(axis.GetProperty("present").GetBoolean());
        var labels = Strings(axis, "labels");
        Assert.NotEmpty(labels);
        Assert.All(labels, l => Assert.Matches(@"^\d[\d,.]* (ms|s|min|h)$", l));
        Assert.Equal("50 s", labels.Max(l => (int.Parse(l.Split(' ')[0]), l)).l);
        Assert.Empty(Strings(axis, "caption"));
    }

    /// <summary>
    /// Lock Waits charts milliseconds of lock wait per second and its axis ran up to "3000" with no unit and no thousands separator. The
    /// caption names the unit and the ticks are grouped, as the Memory Clerks axis is ("MB" over "50,000").
    /// </summary>
    [Fact]
    public void LockWaits_NamesMillisecondsPerSecond_AndGroupsItsThousands()
    {
        var axis = Run("blocking").GetProperty("lockWaits");

        Assert.True(axis.GetProperty("present").GetBoolean());
        var labels = Strings(axis, "labels");
        Assert.Contains("3,000", labels);
        Assert.DoesNotContain(labels, l => l.Length > 3 && !l.Contains(','));
        Assert.Equal(new[] { "ms/s" }, Strings(axis, "caption"));
    }

    /// <summary>
    /// The Lock Waits axis over figures between 1.000 and 1.008 ms/s steps by a fraction. Its tick labels take their decimals from the
    /// step, as the other whole-number axes do (#6158): every tick prints the same decimals (0.0, 0.5, 1.0, 1.5), no two read alike and
    /// none is left blank. A formatter that ignores the step prints "0, 0.5, 1, 1.5", and on a finer step one label twice.
    /// </summary>
    [Fact]
    public void LockWaits_KeepsEveryTickDistinct_OnANarrowRange()
    {
        var axis = Run("blockingNarrow").GetProperty("lockWaits");

        Assert.True(axis.GetProperty("present").GetBoolean());
        var labels = Strings(axis, "labels");
        Assert.True(labels.Length >= 3, "the axis drew " + labels.Length + " tick labels");
        Assert.All(labels, l => Assert.Matches(@"^\d[\d,]*(\.\d+)?$", l));
        Assert.Equal(labels.Length, labels.Distinct().Count());
        var decimals = labels.Select(l => l.Contains('.') ? l.Length - l.IndexOf('.') - 1 : 0).Distinct().ToArray();
        Assert.True(decimals.Length == 1 && decimals[0] >= 1, "the ticks read " + string.Join(", ", labels) + ": one axis, one precision, taken from its step");
        Assert.Equal(new[] { "ms/s" }, Strings(axis, "caption"));
    }

    /// <summary>
    /// The Severity tile of Freeze Headroom, Autovacuum Backlog and Replication Slots printed the read's token as it arrived ("ok",
    /// "warning_inactive_and_growing"). The token stays in the tool reply; the tile prints a plain word from one shared map, starting with a
    /// capital as the severity words elsewhere on these pages do, with the token's reason after it in words.
    /// </summary>
    [Theory]
    [InlineData("wraparoundOk", "Healthy")]
    [InlineData("wraparoundCritical", "Critical: failsafe range")]
    [InlineData("autovacuumWarning", "Warning: past threshold and growing")]
    [InlineData("autovacuumCritical", "Critical: far past threshold")]
    [InlineData("slotsWarning", "Warning: inactive and growing")]
    [InlineData("slotsPlainWarning", "Warning")]
    public void TheSeverityTile_PrintsAPlainWord_NotTheReadsToken(string tile, string expected)
    {
        var r = Run("tiles");

        Assert.Equal(expected, r.GetProperty(tile).GetString());
    }

    /// <summary>A reply with no severity keeps the tile's dash: the word map never invents a word for a missing value.</summary>
    [Fact]
    public void TheSeverityTile_KeepsItsDash_WhenTheReadHasNoSeverity()
    {
        Assert.Equal("—", Run("tiles").GetProperty("slotsMissing").GetString());
    }

    /// <summary>
    /// The Severity column of Per-Database Headroom, Tables Behind and Slots carries the same tokens as the tile over it, and prints the
    /// same word for the same token: a tile that reads "Critical: failsafe range" over a grid row that reads "critical_failsafe_range"
    /// names one severity two ways. The grid's words are pinned, and each is compared with the tile drawn for that token, so the two
    /// cannot drift apart. A row with no severity keeps the grid's dash.
    /// </summary>
    [Theory]
    [InlineData("wraparoundGrid", new[] { "Healthy", "Critical: failsafe range" }, new[] { "wraparoundOk", "wraparoundCritical" })]
    [InlineData("autovacuumGrid", new[] { "Warning: past threshold and growing", "Critical: far past threshold" }, new[] { "autovacuumWarning", "autovacuumCritical" })]
    [InlineData("slotsGrid", new[] { "Warning: inactive and growing", "Warning", "—" }, new[] { "slotsWarning", "slotsPlainWarning", "slotsMissing" })]
    public void TheSeverityColumn_PrintsTheTilesWord_ForTheSameToken(string grid, string[] expected, string[] tiles)
    {
        var r = Run("tiles");

        var cells = Strings(r, grid);
        Assert.Equal(expected, cells);
        Assert.Equal(tiles.Select(t => r.GetProperty(t).GetString()).ToArray(), cells);
    }
}
