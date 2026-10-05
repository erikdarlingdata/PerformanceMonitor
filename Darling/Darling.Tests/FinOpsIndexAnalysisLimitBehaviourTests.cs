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
/// says it stops at the limit and how many are not shown, a cut list of one database does not send the reader back to the
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

    [Fact]
    public void AListThatFits_OrHasExactlyAsManyAsTheLimit_SaysNothingAboutACut()
    {
        var run = Run();
        Assert.Equal(new[] { "3 recommendations." + Tail }, Notices(run.GetProperty("fits")));
        Assert.Equal(new[] { "500 recommendations." + Tail }, Notices(run.GetProperty("exact")));
    }
}
