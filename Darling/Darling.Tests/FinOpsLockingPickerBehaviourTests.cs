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
/// The FinOps Database box (<c>js/pages/finops/database-box.js</c>) that the Locking &amp; Contention tab gained and Index Analysis
/// now shares (#5231, #5244 L6 and L8c), run from the shipped page scripts under Node (<c>finops-locking-picker-harness.mjs</c>):
/// the Locking read sends <c>database_name</c> only for a typed name, a typed "salesdb" is sent as the stored "SalesDb" (also on
/// Index Analysis), a name that matches no suggestion goes as typed, each of the six awkward names is ONE name through the draft and
/// the send, an uncut list reads nothing while typing, and a list the route cut asks again once with the newest text. Node is
/// skipped when it is not installed.
/// </summary>
public sealed class FinOpsLockingPickerBehaviourTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static readonly string[] Awkward = { "A,B", "x]", " SalesDb", "O'Brien", "50%+off", "<img src=x onerror=alert(1)>" };

    public static TheoryData<string> AwkwardNames()
    {
        var data = new TheoryData<string>();
        foreach (var name in Awkward) data.Add(name);
        return data;
    }

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "finops-locking-picker-harness.mjs"));
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
                Assert.Fail("the locking picker harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the locking picker harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    /// <summary>The database_name values of each read the step made: one array per read, empty when the read named no database.</summary>
    private static string[][] Reads(JsonElement reads) =>
        reads.EnumerateArray().Select(r => r.EnumerateArray().Select(e => e.GetString()!).ToArray()).ToArray();

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void TheFirstReadsAskTheRouteForTheServersNames_AndTheTableNamesNoDatabase()
    {
        var first = Result.Value.GetProperty("first");
        var route = first.GetProperty("route").EnumerateArray().ToArray();
        Assert.Single(route);
        Assert.Equal("srv-a", route[0].GetProperty("server").GetString());
        Assert.Equal(JsonValueKind.Null, route[0].GetProperty("search").ValueKind);
        Assert.Equal(new[] { Array.Empty<string>() }, Reads(first.GetProperty("reads")));
        Assert.Equal(new[] { "SalesDb" }.Concat(Awkward), Strings(first.GetProperty("options")));
        Assert.Equal(0, first.GetProperty("images").GetInt32());
    }

    /* RED on the base: there was no box, so nothing was typed and nothing was sent. */
    [Fact]
    public void ATypedSalesdbIsSentAsTheStoredSalesDb_AndTheBoxShowsIt()
    {
        var step = Result.Value.GetProperty("lowercase");
        Assert.Equal(new[] { new[] { "SalesDb" } }, Reads(step.GetProperty("reads")));
        Assert.Equal("SalesDb", step.GetProperty("shown").GetString());
    }

    [Fact]
    public void ABlankBoxIsAllDatabases_AndANameThatMatchesNoSuggestionGoesAsTyped()
    {
        Assert.Equal(new[] { Array.Empty<string>() }, Reads(Result.Value.GetProperty("blank").GetProperty("reads")));
        Assert.Equal(new[] { new[] { "Nope" } }, Reads(Result.Value.GetProperty("unknown").GetProperty("reads")));
    }

    [Theory]
    [MemberData(nameof(AwkwardNames))]
    public void EachAwkwardName_ListedOrNot_GoesAsOneNameExactlyAsTyped(string name)
    {
        var run = Result.Value;
        var listed = run.GetProperty("awkward").GetProperty(name);
        Assert.Equal(new[] { new[] { name } }, Reads(listed.GetProperty("reads")));
        Assert.Equal(0, listed.GetProperty("images").GetInt32());
        Assert.Equal(new[] { new[] { name } }, Reads(run.GetProperty("awkwardNotListed").GetProperty(name)));
    }

    [Fact]
    public void TwoSuggestionsThatDifferOnlyByCase_AreNotGuessedBetween()
    {
        var ambiguous = Result.Value.GetProperty("ambiguous");
        Assert.Equal(new[] { new[] { "FOO" } }, Reads(ambiguous.GetProperty("FOO")));
        Assert.Equal(new[] { new[] { "foo" } }, Reads(ambiguous.GetProperty("foo")));
        Assert.Equal(new[] { new[] { "Foo" } }, Reads(ambiguous.GetProperty("Foo")));
    }

    [Fact]
    public void AListTheRouteDidNotCut_ReadsNothingWhileTheReaderTypes()
    {
        Assert.Equal(0, Result.Value.GetProperty("uncutTyping").GetProperty("routeReads").GetInt32());
    }

    [Fact]
    public void ACutList_SaysSo_AndAsksOnceWithTheNewestText_AfterThePause()
    {
        var run = Result.Value;
        var cut = run.GetProperty("cut");
        Assert.Equal(new[] { "Alpha", "Beta" }, Strings(cut.GetProperty("options")));
        Assert.Equal(new[] { "The list stops at 2 databases, so more are not shown. Type part of a name to find it." }, Strings(cut.GetProperty("hint")));
        Assert.Equal(1, cut.GetProperty("routeReads").GetInt32());

        var search = run.GetProperty("cutSearch");
        Assert.Equal(0, search.GetProperty("early").GetInt32());
        Assert.Equal(new[] { "sale" }, Strings(search.GetProperty("searches")));
        Assert.Equal(new[] { "SalesDb", "SalesEU" }, Strings(search.GetProperty("options")));
        Assert.Equal(new[] { "" }, Strings(search.GetProperty("hint")));
    }

    [Fact]
    public void ANamePastTheCut_IsMatchedFromTheSearchAnswer_AndAnEmptyBoxListsTheFirstPageAgain()
    {
        var run = Result.Value;
        Assert.Equal(new[] { new[] { "SalesDb" } }, Reads(run.GetProperty("cutMatch").GetProperty("reads")));
        Assert.Equal(new[] { "Alpha", "Beta" }, Strings(run.GetProperty("cutCleared").GetProperty("options")));
    }

    [Fact]
    public void WhenTwoSearchesAreInFlight_TheNewestAnswerWins_EvenWhenTheOlderOneLandsLast()
    {
        var step = Result.Value.GetProperty("newestWins");
        Assert.Equal(new[] { "a", "ab" }, Strings(step.GetProperty("searches")));
        Assert.Equal(new[] { "Abacus" }, Strings(step.GetProperty("afterNewest")));
        Assert.Equal(new[] { "Abacus" }, Strings(step.GetProperty("afterStale")));
    }

    /* Index Analysis shares the box, so it gets the stored spelling too (#5244 L8c). Its list still comes from its own read. */
    [Fact]
    public void IndexAnalysisSendsTheStoredSpelling_Too()
    {
        var step = Result.Value.GetProperty("indexAnalysis");
        Assert.Equal(new[] { "SalesDb" }, Strings(step.GetProperty("database_name")));
        Assert.Equal("SalesDb", step.GetProperty("shown").GetString());
        Assert.Equal(new[] { "SalesDb", "Other" }, Strings(step.GetProperty("options")));
    }
}
