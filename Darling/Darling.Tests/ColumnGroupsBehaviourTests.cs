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
/// Column groups on the web dashboard's shared table renderer (#4843), from the shipped <c>panels.js</c> run under Node (<c>web-column-groups-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class ColumnGroupsBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-column-groups-harness.mjs"));
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
                Assert.Fail("the column groups harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the column groups harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string Str(JsonElement r, string name) => r.GetProperty(name).GetString()!;

    private static string[] Arr(JsonElement r, string name) => r.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void ATableWithoutGroups_HasNoPicker_AndShowsEveryColumn()
    {
        var r = Run("plain");
        Assert.False(r.GetProperty("hasPicker").GetBoolean());
        Assert.Equal(new[] { "Name", "A", "B", "C" }, Arr(r, "heads"));
        Assert.Equal("grid-box", Str(r, "top"));
    }

    [Fact]
    public void Groups_DrawAColumnsStrip_AndShowOnlyTheDefaultsAndUngroupedColumns()
    {
        var r = Run("defaults");
        Assert.Equal(new[] { "Columns:", "G1", "G2" }, Arr(r, "labels"));
        Assert.Equal(new[] { "Name", "A" }, Arr(r, "heads"));
        Assert.Equal(new[] { "x", "3" }, Arr(r, "cells"));
        Assert.Equal(new[] { "true", "false" }, Arr(r, "pressed"));
    }

    [Fact]
    public void ATogglePressed_RevealsAndHidesItsColumns_ThroughBothHeaderAndBody()
    {
        var r = Run("toggling");
        Assert.Equal(new[] { "Name", "A", "B", "C" }, Arr(r, "on"));
        Assert.Equal(new[] { "x", "3", "30", "1" }, Arr(r, "onCells"));
        Assert.Equal(new[] { "Name", "B", "C" }, Arr(r, "g1Off"));
        Assert.Equal(new[] { "Name" }, Arr(r, "allOff"));
    }

    [Fact]
    public void TheChoice_SurvivesARebuild_AndStartsFromTheDefaultsForAnotherServer()
    {
        var r = Run("repaint");
        Assert.Equal(new[] { "Name", "A", "B", "C" }, Arr(r, "rebuilt"));
        Assert.Equal(new[] { "Name", "A" }, Arr(r, "otherServer"));
        Assert.Equal(new[] { "Name", "A", "B", "C" }, Arr(r, "backToA"));
    }

    [Fact]
    public void SortingAColumn_ThenHidingIt_KeepsTheOrder_AndOtherColumnsStillSort()
    {
        var r = Run("sortHidden");
        var byB = new[] { "y", "z", "x" };
        Assert.Equal(byB, Arr(r, "asc"));
        Assert.Equal(byB, Arr(r, "hiddenAsc"));
        Assert.Equal(byB, Arr(r, "repaintAsc"));
        Assert.Equal(new[] { "Name", "A" }, Arr(r, "heads"));
        Assert.Equal(byB, Arr(r, "byA"));
    }

    [Fact]
    public void CopyAll_CarriesEveryColumn_HiddenOnesIncluded_AndThePickerSitsAboveTheTools()
    {
        var r = Run("exportAll");
        Assert.Equal("Name\tA\tB\tC\nx\t3\t30\t1\ny\t1\t10\t3\nz\t2\t20\t2", Str(r, "copy"));
        Assert.Equal(new[] { "col-picker", "grid-tools", "table-wrap" }, Arr(r, "stripOrder"));
    }
}
