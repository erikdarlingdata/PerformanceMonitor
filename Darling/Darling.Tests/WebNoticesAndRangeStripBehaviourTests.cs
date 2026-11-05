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
/// #7416 items 7, 8, 12, 13, 17, 23 and 24 on the web pages: the partial window notice, the range strip's single sample and empty window,
/// the restart notice, a tab note, the Query Store reasons and the Run live now line. These run the shipped page scripts under Node
/// (<c>web-notices-truths-harness.mjs</c>). Skipped when Node is not installed.
/// </summary>
public sealed class WebNoticesAndRangeStripBehaviourTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-notices-truths-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

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
                Assert.Fail("the notices harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the notices harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void ThePartialWindowNotice_StartsACapitalSentence_AndSaysEachDateOnce()
    {
        var capped = Result.Value.GetProperty("capped");
        Assert.StartsWith("Partial window: this grid shows only the newest rows", capped.GetProperty("text").GetString());
        Assert.Equal(1, capped.GetProperty("oldestCount").GetInt32());
        Assert.Equal(1, capped.GetProperty("startCount").GetInt32());

        var dataStart = Result.Value.GetProperty("dataStart");
        Assert.StartsWith("Partial window: this panel's data starts at", dataStart.GetProperty("text").GetString());
        Assert.Equal(1, dataStart.GetProperty("firstCount").GetInt32());

        // A sentence the service wrote for a tool, drawn as it was sent, still opens with a capital.
        Assert.StartsWith("Partial window: this panel read", Result.Value.GetProperty("sentOnly").GetString());
    }

    [Fact]
    public void TheRestartNotice_SaysWhatHappenedInWords_KeepsTheToolTextOneClickAway_AndSkipsAChartOfLevels()
    {
        var restart = Result.Value.GetProperty("restart");
        Assert.EndsWith("Rates start over there.", restart.GetProperty("text").GetString());
        Assert.StartsWith("SQL Server restarted at ", restart.GetProperty("text").GetString());
        Assert.DoesNotContain("baseline", restart.GetProperty("text").GetString());
        Assert.Equal(1, restart.GetProperty("details").GetInt32());
        Assert.Contains("baseline discontinuity at", restart.GetProperty("full").GetString());
        Assert.Contains("wait_stats", restart.GetProperty("full").GetString());
        Assert.Equal(JsonValueKind.Null, restart.GetProperty("levels").ValueKind);

        // The Memory tab's clerks, plan cache and tempdb size are levels: the page read carries the restart entry and none is drawn.
        var memory = Result.Value.GetProperty("memoryTab");
        Assert.True(memory.GetProperty("drewClerks").GetBoolean(), "the Memory tab drew no clerk chart, so the check below proves nothing");
        Assert.False(memory.GetProperty("restartText").GetBoolean());
    }

    [Fact]
    public void ATabNote_IsQuietText_NotTheYellowWarning()
    {
        var note = Result.Value.GetProperty("tabNote");
        Assert.Contains("source", note.GetProperty("cls").GetString());
        Assert.DoesNotContain("notice", note.GetProperty("cls").GetString());
    }

    [Fact]
    public void ARangeStrip_DrawsAMarkForOneSample_AndNothingForAWindowWithNoRows()
    {
        Assert.Equal(1, Result.Value.GetProperty("singleSample").GetProperty("dots").GetInt32());
        Assert.Equal(2, Result.Value.GetProperty("singleSample").GetProperty("stem").GetInt32());
        Assert.Equal(0, Result.Value.GetProperty("multiSample").GetProperty("dots").GetInt32());
        // stripOf gives the blocking strip a one minute step: reports five minutes apart are four dots, not the line their steady cadence would draw.
        Assert.Equal(4, Result.Value.GetProperty("sparseSample").GetProperty("dots").GetInt32());

        // A read that answered "empty" draws no strip, and nor does a window where every read is empty.
        Assert.True(Result.Value.GetProperty("emptyStrip").GetProperty("hidden").GetBoolean());
        var none = Result.Value.GetProperty("noRows");
        Assert.Equal(2, none.GetProperty("strips").GetInt32());
        Assert.Equal(0, none.GetProperty("shown").GetInt32());
    }

    [Fact]
    public void TheQueryStoreReasons_ReadInWords_AndUnknownSaysItWasNotGraded()
    {
        var qs = Result.Value.GetProperty("qs");
        Assert.Equal("Query Store off (not graded)", qs.GetProperty("off").GetString());
        Assert.Equal("Not graded", qs.GetProperty("none").GetString());
        Assert.Equal("High plan churn, Query Store read-only", qs.GetProperty("graded").GetString());
    }

    /// <summary>#7586: a read-only database showed <c>qs_read_only_state</c> raw in the Reasons column. Every reason token the server can emit
    /// reads in the words <c>QueryStoreClutter.ReasonText</c> gives it, and none prints as the token.</summary>
    [Fact]
    public void EveryQueryStoreReasonToken_ReadsInWords_NotAsTheToken()
    {
        var all = Result.Value.GetProperty("qs").GetProperty("all");
        var tokens = typeof(PerformanceMonitor.Darling.Storage.QueryStoreClutter)
            .GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Where(f => f.IsLiteral && f.Name.StartsWith("Reason", StringComparison.Ordinal))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToArray();
        Assert.Equal(12, tokens.Length);
        foreach (var token in tokens)
        {
            Assert.True(all.TryGetProperty(token, out var words), "the page has no words for " + token);
            Assert.Equal(PerformanceMonitor.Darling.Storage.QueryStoreClutter.ReasonText(token), words.GetString());
            Assert.NotEqual(token, words.GetString());
        }

        Assert.Equal("Query Store read-only (database state)", all.GetProperty("qs_read_only_state").GetString());
    }

    [Fact]
    public void RunLiveNow_PutsItsSentenceBesideTheButton_InTheSharedRunRow()
    {
        var live = Result.Value.GetProperty("live");
        Assert.Contains("run-row", live.GetProperty("rowClass").GetString());
        Assert.Equal(new[] { "button", "p" }, live.GetProperty("kids").EnumerateArray().Select(k => k.GetString()).ToArray());
        Assert.Contains("muted", live.GetProperty("noteClass").GetString());
        var text = live.GetProperty("noteText").GetString();
        Assert.StartsWith("Reads the running requests from the monitored server once, now.", text);
        Assert.DoesNotContain("a newer request more than an hour later removes it", text);
    }

    [Fact]
    public void ACaption_AndATabNote_AreOneQuietStrip_AndACaveatIsTheYellowOne()
    {
        var caption = Result.Value.GetProperty("caption");
        Assert.Contains("source", caption.GetProperty("plain").GetString());
        Assert.DoesNotContain("notice", caption.GetProperty("plain").GetString());
        Assert.Contains("notice", caption.GetProperty("caveat").GetString());
    }

    [Fact]
    public void TheMemoryClerksTable_UsesTheFullWidth_LikeEveryOtherPanelOnTheMemoryTab()
    {
        Assert.Contains("span-2", Result.Value.GetProperty("clerksPanel").GetString());
        var halves = Result.Value.GetProperty("memoryPanels").EnumerateArray().Select(c => c.GetString()!).Where(c => !c.Contains("span-2")).ToArray();
        Assert.Empty(halves);
    }

    /// <summary>
    /// #7416 item 21: the Top Queries per-query trend says the earlier part "aged out" only when the read says the requested start is
    /// older than the tier keeps, and then names how long the tier keeps data. A first point that is merely later than the start
    /// (a query first run five hours ago) names no cause.
    /// </summary>
    [Fact]
    public void TheQueryTrendNote_SaysAgedOutOnlyWhenTheStartIsOlderThanTheTierKeeps()
    {
        var notes = Result.Value.GetProperty("queryTrendNotes");
        var late = Assert.Single(notes.GetProperty("late").EnumerateArray()).GetString()!;
        Assert.Contains("History for this query starts 4.9 hours back, not the 24 the window asked for.", late);
        Assert.Contains("The store holds nothing earlier in this window at this tier.", late);
        Assert.DoesNotContain("aged out", late);

        var old = Assert.Single(notes.GetProperty("old").EnumerateArray()).GetString()!;
        Assert.Contains("History for this query starts 90 hours back, not the 168 the window asked for.", old);
        Assert.Contains("The earlier part has aged out of this tier, which keeps 4 days of data.", old);
        Assert.DoesNotContain("holds nothing earlier", old);

        Assert.DoesNotContain("starts", Assert.Single(notes.GetProperty("whole").EnumerateArray()).GetString()!);
    }
}
