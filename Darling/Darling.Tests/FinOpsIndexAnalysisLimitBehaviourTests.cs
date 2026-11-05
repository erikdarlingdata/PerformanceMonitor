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
/// How many recommendations the FinOps Index Analysis tab asks for and what it says when the read cut the list (#5238), run from
/// the shipped page script under Node (<c>finops-index-analysis-limit-harness.mjs</c>): the first read asks for 500, a cut list
/// says it stops at the limit and how many are not shown (one left over reads "is", not "are"), a cut list of one database does not send the reader back to the
/// Database box, and a list that fits, or has exactly as many as the limit lists, says nothing about a cut. Node is skipped when
/// it is not installed.
/// </summary>
public sealed class FinOpsIndexAnalysisLimitBehaviourTests
{
    private const string Tail = " Analyzed from each database's newest collected snapshot.";

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "finops-index-analysis-limit-harness.mjs"));
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
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the index analysis limit harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the index analysis limit harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Notices(JsonElement step) => step.GetProperty("notices").EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static string[] Limits(JsonElement step) => step.GetProperty("reads").EnumerateArray().Select(e => e.GetProperty("limit").GetString()!).ToArray();

    [Fact]
    public void TheFirstReadAsksForFiveHundredRecommendations_AndSoDoesAFilteredOne()
    {
        var run = Run();
        Assert.Equal(new[] { "500" }, Limits(run.GetProperty("cut")));
        Assert.Equal(new[] { "500" }, Limits(run.GetProperty("filtered")));
        Assert.Equal("Alpha", run.GetProperty("filtered").GetProperty("reads")[0].GetProperty("database_name").GetString());
    }

    [Fact]
    public void ACutListSaysItStopsAtTheLimit_HowManyAreNotShown_AndWhereToNarrowIt()
    {
        var notices = Notices(Run().GetProperty("cut"));
        Assert.Equal(
            new[] { "Showing the largest 500 of 612 recommendations. The list stops at 500, so 112 more are not shown. Choose a database above to see its own list." + Tail },
            notices);
    }

    [Fact]
    public void ACutListOfOneDatabaseDoesNotSendTheReaderBackToTheDatabaseBox()
    {
        var notices = Notices(Run().GetProperty("filtered"));
        Assert.Equal(
            new[] { "Showing the largest 500 of 503 recommendations. The list stops at 500, so 3 more are not shown. Database Alpha." + Tail },
            notices);
    }

    /* The count of what is left reads "1 more is not shown" for one and "N more are not shown" for any other number. */
    [Fact]
    public void ACutListLeavingExactlyOneSaysOneMoreIsNotShown_NotOneMoreAreNotShown()
    {
        var notices = Notices(Run().GetProperty("oneOver"));
        Assert.Equal(
            new[] { "Showing the largest 500 of 501 recommendations. The list stops at 500, so 1 more is not shown. Choose a database above to see its own list." + Tail },
            notices);
    }

    [Fact]
    public void AListThatFits_OrHasExactlyAsManyAsTheLimit_SaysNothingAboutACut()
    {
        var run = Run();
        Assert.Equal(new[] { "3 recommendations." + Tail }, Notices(run.GetProperty("fits")));
        Assert.Equal(new[] { "500 recommendations." + Tail }, Notices(run.GetProperty("exact")));
    }

    /* The Database box is shared with the Locking tab (#5244 L8c): a typed "alpha" is sent as the listed "Alpha" (PostgreSQL = is
       case-sensitive); a name no list holds is sent as typed. RED before the lift: the raw draft was sent. */
    [Fact]
    public void ATypedNameThatDiffersOnlyByCase_IsSentInItsStoredSpelling()
    {
        var step = Run().GetProperty("storedSpelling");
        Assert.Equal(new[] { "Alpha" }, step.GetProperty("typedLower").EnumerateArray().Select(e => e.GetString()!).ToArray());
        Assert.Equal(new[] { "Gamma" }, step.GetProperty("typedUnknown").EnumerateArray().Select(e => e.GetString()!).ToArray());
        Assert.Equal("Gamma", step.GetProperty("shown").GetString());
    }

    /* #7702 item 1: a 0.002 GB figure printed 0.00. It now prints in the unit that fits (2.0 MB), and an index of 0.4 MB, which the read
       sends as 0.000391 GB, prints 0.4 MB, not 0.400384 MB. #7701: a left-over database says since when, Collected sits right after
       Database in the roll-up, and a live database that shares its name with a left-over one keeps its Collected time on its
       recommendations (the left-over twin does not make the name ambiguous). */
    [Fact]
    public void ASmallSizePrintsNonZero_ALeftOverDatabaseCarriesItsMark_AndCollectedFollowsDatabase()
    {
        var step = Run().GetProperty("sizes");
        var text = step.GetProperty("text").GetString()!;
        Assert.Contains("2.0 MB", text, StringComparison.Ordinal);
        Assert.Contains("0.41 MB", text, StringComparison.Ordinal);
        Assert.Contains("0.4 MB", text, StringComparison.Ordinal);
        Assert.DoesNotContain("0.400384", text, StringComparison.Ordinal);
        Assert.DoesNotContain("0.4096", text, StringComparison.Ordinal);
        Assert.DoesNotContain("0.00", text, StringComparison.Ordinal);
        Assert.Contains("Not collected since", text, StringComparison.Ordinal);
        var header = step.GetProperty("header").EnumerateArray().Select(e => e.GetString()!).ToArray();
        Assert.Equal("Collected", header[header.ToList().IndexOf("Database") + 1]);
    }

    [Fact]
    public void ALiveDatabaseSharingANameWithALeftOverOne_KeepsItsCollectedTimeOnItsRecommendations()
    {
        var tables = Run().GetProperty("sizes").GetProperty("tables").EnumerateArray().ToList();
        var grid = tables.Single(t => t.GetProperty("rows").EnumerateArray().Any(r => r.EnumerateArray().Any(c => c.GetString() == "IX1")));
        var header = grid.GetProperty("header").EnumerateArray().Select(e => e.GetString()!).ToList();
        var row = grid.GetProperty("rows").EnumerateArray().Single(r => r.EnumerateArray().Any(c => c.GetString() == "IX1"))
            .EnumerateArray().Select(c => c.GetString()!).ToList();
        var collected = row[header.IndexOf("Collected")];
        Assert.NotEqual("—", collected);
        Assert.NotEmpty(collected);
    }
}
