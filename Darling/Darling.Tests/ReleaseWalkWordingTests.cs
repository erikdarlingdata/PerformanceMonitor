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
using PerformanceMonitor.PlanAnalysis;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The release walk's web wording and count fixes, run against the shipped page modules under Node
/// (<c>web-release-walk-harness.mjs</c>; skipped when Node is not installed): W1 the offline fleet card's age, W5 the
/// Availability Group counts, W7 and W11 the words for a precondition and an answered-only answer, and W8 the Repro
/// script's time stamp.
/// </summary>
public sealed class ReleaseWalkWordingTests
{
    private static string Scenario(string scenario, string[]? inputs = null)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-release-walk-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        if (inputs is not null)
        {
            psi.Environment["HARNESS_INPUT"] = JsonSerializer.Serialize(inputs);
        }

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page code cannot be run.");
            return "";
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the release-walk harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the release-walk harness failed: " + error.Result);
            return output;
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void ADarkPastTheFleetWindowCard_SaysItLastCollectedMoreThanTwoDaysAgo()
    {
        using var doc = JsonDocument.Parse(Scenario("fleetDark"));
        var texts = Strings(doc.RootElement.GetProperty("dark"));
        Assert.Contains("Offline · last collected more than 2 days ago", texts);
        Assert.Contains("no recent collection · last collected more than 2 days ago", texts);
        Assert.Contains(texts, t => t == "last collected more than 2 days ago");
        Assert.DoesNotContain(texts, t => t == "no recent collection" || t.EndsWith("· no recent collection", StringComparison.Ordinal));
    }

    [Fact]
    public void ADarkCardThePageGetsALastCollectionFor_ShowsTheRealAge_NotTheTwoDayBound()
    {
        using var doc = JsonDocument.Parse(Scenario("fleetDarkRealAge"));
        var texts = Strings(doc.RootElement.GetProperty("dark"));
        Assert.Contains("Offline · last collect 12d ago", texts);
        Assert.Contains("last collected 12d ago", texts);
        Assert.DoesNotContain(texts, t => t.Contains("more than 2 days", StringComparison.Ordinal));
    }

    [Fact]
    public void AProblemListThatCouldNotBeRead_StaysOnThePageAndShowsTheError_NotHiddenAsIfThereWereNoProblems()
    {
        using var doc = JsonDocument.Parse(Scenario("serverTabReadFails"));
        Assert.True(doc.RootElement.GetProperty("found").GetBoolean(), "the Analysis could not read these data families panel was not found");
        Assert.NotEqual("none", doc.RootElement.GetProperty("display").GetString());
        Assert.Contains("The store took too long to answer this read", doc.RootElement.GetProperty("text").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void AnOfflineCardWithALastCollection_KeepsItsExactAge()
    {
        using var doc = JsonDocument.Parse(Scenario("fleetInWindow"));
        var texts = Strings(doc.RootElement.GetProperty("dark"));
        Assert.Contains("Offline · last collect 5h ago", texts);
        Assert.Contains("last collected 5h ago", texts);
        Assert.DoesNotContain(texts, t => t.Contains("more than 2 days", StringComparison.Ordinal));
    }

    [Fact]
    public void OneGroupSeenByTwoServers_ReadsOneGroup_AndSaysWhatTheCardsAre()
    {
        using var doc = JsonDocument.Parse(Scenario("ag"));
        Assert.Equal(new[] { "Group", "Reporting servers", "Views" }, Strings(doc.RootElement.GetProperty("labels")));
        Assert.Contains(Strings(doc.RootElement.GetProperty("notes")), n => n.StartsWith("Each card below is one reporting server's view of a group", StringComparison.Ordinal));
    }

    [Fact]
    public void OneViewPerGroup_KeepsThePluralAndNeedsNoCardNote()
    {
        using var doc = JsonDocument.Parse(Scenario("agOnePerGroup"));
        Assert.Equal(new[] { "Groups", "Reporting servers", "Views" }, Strings(doc.RootElement.GetProperty("labels")));
        Assert.DoesNotContain(Strings(doc.RootElement.GetProperty("notes")), n => n.StartsWith("Each card below", StringComparison.Ordinal));
    }

    private const string KcachePrecondition =
        "The pg_kernel_stats collector is running against example-pg-01 but the PostgreSQL extension it reads is not installed there: " +
        "its last run was recorded as a named non-fatal skip, so the per-query OS CPU is not being stored. This is a runtime PRECONDITION, " +
        "not a permanent engine capability gap and not a collection outage: it can be satisfied on the monitored server.";

    private const string EngineGate =
        "example-pg-01 runs PostgreSQL, so the sql_server_only collector does not run on it. This is a permanent engine capability gap, " +
        "not a collection outage: checking collection health, enabling a collector or starting a capture cannot change it.";

    [Fact]
    public void APreconditionSentence_IsNotTurnedIntoTheDoesNotApplyLine()
    {
        using var doc = JsonDocument.Parse(Scenario("plain", new[] { KcachePrecondition, EngineGate }));
        var output = Strings(doc.RootElement.GetProperty("out"));
        var line = doc.RootElement.GetProperty("line").GetString()!;
        Assert.NotEqual(line, output[0]);
        Assert.Contains("extension it reads is not installed there", output[0], StringComparison.Ordinal);
        Assert.Equal(line, output[1]);
    }

    [Fact]
    public void TheAnsweredRankingsEmptyAnswer_DropsTheParameterName()
    {
        var message =
            "answered_only excluded every row: example-pg-01 has 3 candidate btree index(es) in this window and NOT ONE of them has a trusted bloat answer. " +
            "This is not a clean bill of health and it is not an empty server - the suppression breakdown below says why each index has no answer and which reasons are remediable. " +
            "Re-run without answered_only to see the rows and their reasons. Coverage sentence.";
        using var doc = JsonDocument.Parse(Scenario("plain", new[] { message }));
        var o = Strings(doc.RootElement.GetProperty("out"))[0];
        Assert.DoesNotContain("answered_only", o, StringComparison.Ordinal);
        Assert.StartsWith("Nothing to rank: example-pg-01 has 3 candidate btree index(es)", o, StringComparison.Ordinal);
        Assert.Contains("the Index Bloat table above says why each index has no answer.", o, StringComparison.Ordinal);
        Assert.DoesNotContain("breakdown below", o, StringComparison.Ordinal);
        Assert.EndsWith("Coverage sentence.", o, StringComparison.Ordinal);
    }

    [Fact]
    public void TheKcachePanel_NamesTheMissingExtensionInPlainWords()
    {
        var tabs = System.IO.File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));
        var panels = System.IO.File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js"));
        Assert.Contains("pg_stat_kcache is not installed on this server", tabs, StringComparison.Ordinal);
        Assert.Contains("res.status === \"precondition\" && desc.extensionMissingLine", panels, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePostgreSqlEmptyAnswers_NoLongerSayTheStoreCannotClassifyTheServer()
    {
        foreach (var file in new[] { "DarlingMcpPgWraparoundTools.cs", "DarlingMcpPgStatementTools.cs", "DarlingMcpPgWaitTools.cs" })
        {
            var text = System.IO.File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "Mcp", file));
            Assert.DoesNotContain("cannot classify", text, StringComparison.Ordinal);
            // the sentence is spread over concatenated literals; join them before looking for it
            var joined = System.Text.RegularExpressions.Regex.Replace(text, "\"\\s*\\+\\s*\"", "");
            Assert.Contains("offline, nothing is being collected", joined, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheReproScriptsGeneratedLine_IsUtcAndSaysSo()
    {
        var before = DateTime.UtcNow.AddSeconds(-2);
        var script = ReproScriptBuilder.BuildReproScript("SELECT 1", "master", planXml: null, isolationLevel: null);
        var line = script.Split('\n').Select(l => l.TrimEnd('\r')).First(l => l.StartsWith("Generated: ", StringComparison.Ordinal));
        Assert.EndsWith(" UTC", line, StringComparison.Ordinal);
        var stamp = DateTime.ParseExact(line["Generated: ".Length..^" UTC".Length], "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        Assert.InRange(stamp, before, DateTime.UtcNow.AddSeconds(2));
    }
}
