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
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5311: the Locking &amp; Contention tab shades its four wait cells (row lock, page lock, page latch, page I/O latch) with the
/// band the service sent in each row's <c>heat</c> list, and computes nothing. Run from the shipped page script under Node
/// (<c>finops-locking-heat-harness.mjs</c>), skipped when Node is not installed. The scripted rows carry bands that do NOT follow
/// their wait values (the smallest waits carry the top bands), so a class that matches the response proves the browser did not
/// compute its own. A null band, or a row with no <c>heat</c>, shows no band class.
/// </summary>
public sealed class FinOpsLockingHeatBehaviourTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "finops-locking-heat-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the locking heat harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the locking heat harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Classes(string row) =>
        Result.Value.GetProperty("rows").GetProperty(row).EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void TheFourWaitCells_TakeTheBandClassTheResponseCarried_NotOneComputedFromTheirValues()
    {
        Assert.Equal(["num heat-band-7", "num heat-band-6", "num heat-band-5", "num heat-band-4"], Classes("small"));
        Assert.Equal(["num heat-band-0", "num heat-band-1", "num heat-band-2", "num heat-band-3"], Classes("large"));
    }

    [Fact]
    public void ANullBand_AndARowWithNoHeat_ShowNoBandClass_AndTheTextStaysText()
    {
        Assert.Equal(["num", "num", "num heat-band-3", "num"], Classes("nulls"));
        Assert.Equal(["num", "num", "num", "num"], Classes("noheat"));
    }

    [Fact]
    public void OnlyTheFourWaitCellsAreShaded()
    {
        foreach (var row in new[] { "small", "large", "nulls", "noheat" })
            Assert.Empty(Result.Value.GetProperty("rows").GetProperty(row + "#others").EnumerateArray());
    }

    [Fact]
    public void TheTabSource_ComputesNoBand_ItOnlyNamesTheClassFromTheRow()
    {
        var js = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "locking.js");
        Assert.DoesNotMatch(new Regex(@"Math\.(log|floor|max|min|round|ceil)", RegexOptions.None, TimeSpan.FromSeconds(5)), js);
        Assert.Equal(4, Regex.Matches(js, @"cellClass: heatClass\([0-3]\)", RegexOptions.None, TimeSpan.FromSeconds(5)).Count);
    }
}
