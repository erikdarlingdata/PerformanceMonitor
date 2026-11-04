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
/// #7582: the Overview's Analysis Findings grid prints one row per sentence. Chains that start at the same fact print the same
/// headline, so the read's one-entry-per-chain rows used to show one sentence on two or three rows. These run the shipped grid under
/// Node (<c>web-finding-merge-harness.mjs</c>, skipped when Node is not installed).
/// </summary>
public sealed class WebFindingMergeTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-finding-merge-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        Process proc;
        try { proc = Process.Start(psi)!; }
        catch (Win32Exception) { Assert.Skip("Node is not installed, so the shipped page script cannot be run."); return default; }
        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the finding merge harness did not finish in 20 s for scenario " + scenario);
            }
            Assert.True(proc.ExitCode == 0, "the finding merge harness failed for " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string S(JsonElement e, string name) => e.GetProperty(name).GetString()!;

    [Fact]
    public void OneSentenceOnThreeRows_IsOneWarningRow_WithTheOccurrencesAddedUp_AndTheEarliestAndLatestTimes()
    {
        var r = Run("sameSentence");
        var row = r.GetProperty("rows").EnumerateArray().Single();
        var expected = r.GetProperty("expected");
        Assert.Equal("Exclusive latch contention on a non-buffer resource", S(row, "finding"));
        Assert.Equal("Warning", S(row, "severity"));
        Assert.Equal("33", S(row, "occurrences"));
        Assert.Equal(S(expected, "first"), S(row, "firstSeen"));
        Assert.Equal(S(expected, "last"), S(row, "lastSeen"));
    }

    [Fact]
    public void AMergedRow_ListsItsChainsInWords_EachWithItsOwnSeverityAndOccurrences_AndNoArrowsOrHashes()
    {
        var chains = Run("sameSentence").GetProperty("rows").EnumerateArray().Single().GetProperty("chains").EnumerateArray().Select(c => c.GetString()!).ToArray();
        Assert.Equal(new[]
        {
            "Seen as 3 chains:",
            "Latch ex, then Cxpacket · Warning · 19 occurrences",
            "Latch ex, then Tempdb usage · Info · 8 occurrences",
            "Latch ex · Info · 6 occurrences",
        }, chains);
        Assert.DoesNotContain(chains, c => c.Contains('→', StringComparison.Ordinal) || c.Contains("_", StringComparison.Ordinal));
    }

    [Fact]
    public void ARowWithNewestScore062_AndPeak13_ReadsWarning()
    {
        var row = Run("peak").GetProperty("rows").EnumerateArray().Single();
        Assert.Equal("Warning", S(row, "severity"));
    }

    [Fact]
    public void TwoDifferentSentences_StayTwoRows()
    {
        var rows = Run("twoSentences").GetProperty("rows").EnumerateArray().ToArray();
        Assert.Equal(new[] { "Sentence one", "Sentence two" }, rows.Select(x => S(x, "finding")).ToArray());
        Assert.Equal(new[] { "Info", "Critical" }, rows.Select(x => S(x, "severity")).ToArray());
    }

    [Fact]
    public void TheSameChainOnTwoIncidents_IsOneCriticalRow_WithNothingToOpen()
    {
        var row = Run("sameChain").GetProperty("rows").EnumerateArray().Single();
        Assert.Equal("Critical", S(row, "severity"));
        Assert.Equal("21", S(row, "occurrences"));
        Assert.Empty(row.GetProperty("chains").EnumerateArray());
    }

    [Fact]
    public void TwoPileupsOnDifferentStatements_WithOneHeadline_AreTwoRows_AndNoCellPrintsAHash()
    {
        var rows = Run("pileupTwoStatements").GetProperty("rows").EnumerateArray().ToArray();
        Assert.Equal(new[] { "3", "5" }, rows.Select(x => S(x, "occurrences")).OrderBy(x => x, StringComparer.Ordinal).ToArray());
        Assert.Equal(2, rows.Length);
        foreach (var row in rows)
        {
            var text = S(row, "text");
            Assert.DoesNotContain("0x", text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("→", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ThePileupOfOneStatement_OnTwoIncidents_IsOneRow_WithTheOccurrencesAddedUp()
    {
        var row = Run("pileupSameStatement").GetProperty("rows").EnumerateArray().Single();
        Assert.Equal("8", S(row, "occurrences"));
        Assert.DoesNotContain("0x", S(row, "text"), StringComparison.OrdinalIgnoreCase);
    }
}
