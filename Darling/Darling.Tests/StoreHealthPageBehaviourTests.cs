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
/// #8787: the Store Health page on the Darling web. These run the shipped <c>store-health.js</c> with <c>panels.js</c> and <c>util.js</c> under Node
/// (<c>store-health-harness.mjs</c>) against a scripted read; skipped when Node is not installed.
/// </summary>
public sealed class StoreHealthPageBehaviourTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "store-health-harness.mjs"));
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
                Assert.Fail("the store health harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the store health harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string[][] Rows(JsonElement rows) =>
        rows.EnumerateArray().Select(r => r.EnumerateArray().Select(c => c.GetString()!).ToArray()).ToArray();

    private static string[] Names(JsonElement asks, string noSection) =>
        asks.EnumerateArray().Select(a => a.GetProperty("section").ValueKind == JsonValueKind.Null ? noSection : a.GetProperty("section").GetString()!).ToArray();

    [Fact]
    public void WhenTheStatisticsWereNeverReset_ThePageSaysWhyThePerDayColumnsAreEmpty_AndCountedSinceReadsBefore()
    {
        var never = Result.Value.GetProperty("neverReset");
        var notes = never.GetProperty("note").EnumerateArray().Select(n => n.GetString()!).ToArray();
        Assert.Contains(notes, n => n.Contains("per-day columns are empty", StringComparison.Ordinal) && n.Contains("never reset", StringComparison.Ordinal));
        Assert.DoesNotContain(never.GetProperty("noteAfterReset").EnumerateArray().Select(n => n.GetString()!), n => n.Contains("per-day columns", StringComparison.Ordinal));
        Assert.StartsWith("before ", never.GetProperty("postmasterCell").GetString()!, StringComparison.Ordinal);
        Assert.DoesNotContain("before", never.GetProperty("resetCell").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePage_ReadsTablesAndIndexesAsTwoReads_AndNeverAsksForAll()
    {
        var r = Result.Value;
        Assert.Equal(new[] { "tables", "indexes", "readiness" }, Names(r.GetProperty("first").GetProperty("asks"), "readiness"));
        Assert.Equal(0, r.GetProperty("neverAll").GetInt32());
        Assert.Equal(new[] { "tables", "indexes", "readiness" }, Names(r.GetProperty("refresh").GetProperty("asks"), "readiness"));
    }

    [Fact]
    public void TheTablesGrid_DrawsBeforeTheIndexesReadReturns_AndTheIndexesSectionSaysLoading()
    {
        var first = Result.Value.GetProperty("first");
        Assert.Equal(6, Rows(first.GetProperty("tableRows")).Length);
        Assert.Contains("Loading indexes", first.GetProperty("indexesWhileHeld").GetString()!, StringComparison.Ordinal);
        Assert.Contains("Loading partition index coverage", first.GetProperty("coverageWhileHeld").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void ATableWithNoEstimate_ShowsTheReasonPhrase_AndNeverZero()
    {
        var rows = Rows(Result.Value.GetProperty("first").GetProperty("tableRows"));
        var fresh = rows.Single(r => r[0] == "collect.fresh_table");
        Assert.Equal("Never analyzed", fresh[8]);
        Assert.Equal("—", fresh[9]);
        var candidate = rows.Single(r => r[0] == "collect.store_metrics");
        Assert.Equal("Rewrite candidate", candidate[3]);
        Assert.Equal("75%", candidate[8]);
    }

    /// <summary>#8924: the service holds each part of the read for a few minutes, so the page stamps an answer with the time the store
    /// gave it (tables_as_of, indexes_as_of) and its age, never with the time the answer arrived. The scripted tables answer is 240 seconds
    /// old and the indexes answer new.</summary>
    [Fact]
    public void ThePage_SaysWhenTheStoreAnsweredEachPart_AndHowOldAHeldAnswerIs()
    {
        var first = Result.Value.GetProperty("first");
        var line = Assert.Single(first.GetProperty("readLine").EnumerateArray().Select(t => t.GetString()!));
        Assert.StartsWith("Tables read ", line, StringComparison.Ordinal);
        Assert.EndsWith(" (4 min ago)", line, StringComparison.Ordinal);
        Assert.Contains("2026", line, StringComparison.Ordinal);
        Assert.Equal(
            "The store's tables and indexes are read at most once every 5 min. A refresh inside that time shows the same read; after it, a refresh shows the last read at once and a new read runs behind it.",
            first.GetProperty("readLineTitle")[0].GetString());

        var indexLines = Result.Value.GetProperty("afterIndexes").GetProperty("indexSummary").EnumerateArray().Select(t => t.GetString()!).ToArray();
        var indexLine = Assert.Single(indexLines, t => t.StartsWith("Indexes read ", StringComparison.Ordinal));
        Assert.DoesNotContain("ago", indexLine, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHeader_ShowsTheCountsTheReasonsAndTheReadinessLine_AndTheNoteStatesItIsAnEstimate()
    {
        var first = Result.Value.GetProperty("first");
        var tiles = first.GetProperty("tiles").EnumerateArray().Select(t => t.GetString()!).ToArray();
        Assert.Contains(tiles, t => t.Contains("Rewrite candidate", StringComparison.Ordinal) && !t.Contains("Rewrite candidates", StringComparison.Ordinal) && t.StartsWith("1", StringComparison.Ordinal));
        Assert.Contains("Never analyzed: 1", first.GetProperty("headerText").GetString()!, StringComparison.Ordinal);
        Assert.Contains("Not safe for maintenance now.", first.GetProperty("readiness").GetString()!, StringComparison.Ordinal);
        Assert.Contains("estimate", first.GetProperty("note")[0].GetString()!, StringComparison.Ordinal);
    }

    /// <summary>#8950: a table behind on autovacuum is flagged in its own column with how long it has gone without a vacuum, and comes
    /// first in the read's order. A table cancelled repeatedly shows its count, flagged, and the time of the last cancel.</summary>
    [Fact]
    public void ATableBehindOnAutovacuum_IsFlaggedFirst_AndATableCancelledRepeatedly_ShowsItsCancels()
    {
        var av = Result.Value.GetProperty("autovacuum");
        var headers = av.GetProperty("headers").EnumerateArray().Select(h => h.GetString()!).ToList();
        var behindAt = headers.IndexOf("Autovacuum behind");
        var cancelsAt = headers.IndexOf("Autovacuum cancels (24 h)");
        var lastAt = headers.IndexOf("Last cancel");
        var pastAt = headers.IndexOf("Parts past the start point");
        Assert.True(behindAt > 0 && cancelsAt > 0 && lastAt > 0 && pastAt > 0, string.Join(", ", headers));
        Assert.Contains("7-day growth", headers);

        var rows = Rows(av.GetProperty("rows"));
        Assert.Equal("collect.query_snapshots", rows[0][0]);
        Assert.Equal("Behind, no vacuum in 3 days", rows[0][behindAt]);
        Assert.Equal("3", rows[0][pastAt]);
        var cancelled = rows.Single(r => r[0] == "collect.wait_stats");
        Assert.Equal("No", cancelled[behindAt]);
        Assert.Equal("5", cancelled[cancelsAt]);
        Assert.Contains("2026", cancelled[lastAt], StringComparison.Ordinal);
        var quiet = rows.Single(r => r[0] == "collect.store_metrics");
        Assert.Equal("No", quiet[behindAt]);
        Assert.Equal("0", quiet[cancelsAt]);
        Assert.Equal("—", quiet[lastAt]);
        Assert.Equal(new[] { "30 h", "3 days" }, av.GetProperty("hours").EnumerateArray().Select(h => h.GetString()!).ToArray());
    }

    /// <summary>#8950: beside the store size the header shows the write budget the store's autovacuum settings allow, the tables behind and
    /// cancelled repeatedly, and one line of the settings as they apply, naming a setting that falls back to another. A cost delay of 0 reads
    /// as unthrottled. When the settings read ran past its limit the header says they were left out.</summary>
    [Fact]
    public void TheHeader_ShowsTheAutovacuumSettingsAndWriteBudget_BesideTheStoreSize()
    {
        var av = Result.Value.GetProperty("autovacuum");
        var tiles = av.GetProperty("tiles").EnumerateArray().Select(t => t.GetString()!).ToArray();
        Assert.StartsWith("1,000.0 MBStore tables and indexes", tiles[0], StringComparison.Ordinal);
        Assert.Equal("39.1 MB/sAutovacuum write budgetthe most autovacuum may write per second", tiles[1]);
        Assert.Contains(tiles, t => t.StartsWith("1Table behind on autovacuum", StringComparison.Ordinal));
        Assert.Contains(tiles, t => t.StartsWith("1Table with autovacuum cancelled repeatedly", StringComparison.Ordinal));

        var line = Assert.Single(av.GetProperty("settingsLine").EnumerateArray().Select(t => t.GetString()!));
        Assert.Equal(
            "Autovacuum is on: 1 of 3 workers running, it looks for work every 1 min, cost limit 200 (from vacuum_cost_limit), cost delay 2 ms, "
            + "64.0 MB of memory per worker (from maintenance_work_mem). The store log has 8 cancelled autovacuums in the last 24 hours, "
            + "2 for a relation the store no longer has or one in another database.",
            line);

        Assert.StartsWith("UnthrottledAutovacuum write budget", av.GetProperty("unthrottledTiles")[1].GetString()!, StringComparison.Ordinal);
        var unthrottled = Assert.Single(av.GetProperty("unthrottledLine").EnumerateArray().Select(t => t.GetString()!));
        Assert.Contains("cost limit 200, cost delay 0 ms, 64.0 MB of memory per worker.", unthrottled, StringComparison.Ordinal);
        Assert.Contains("no cancelled autovacuum in the last 24 hours", unthrottled, StringComparison.Ordinal);

        Assert.Equal(0, av.GetProperty("leftOutLines").GetInt32());
        var leftOut = av.GetProperty("leftOutHeader").GetString()!;
        Assert.Contains("The autovacuum settings were left out.", leftOut, StringComparison.Ordinal);
        Assert.DoesNotContain("write budget", leftOut, StringComparison.Ordinal);
    }

    [Fact]
    public void TheIndexesGrid_ListsFlaggedIndexesFirst_AndCoverageShowsOnlyIncompleteParents()
    {
        var after = Result.Value.GetProperty("afterIndexes");
        var rows = Rows(after.GetProperty("indexRows"));
        Assert.Equal("Invalid", rows[0][0]);
        Assert.Equal("Larger than its table", rows[1][0]);
        Assert.Contains(rows, r => r.Any(c => c.Contains("Table not analyzed", StringComparison.Ordinal)));
        /* The Not Estimated cell shows the short word first and carries the full sentence in its disclosure. */
        Assert.Contains(rows, r => r.Any(c => c.StartsWith("Not a btree index", StringComparison.Ordinal)));
        Assert.DoesNotContain(rows, r => r.Any(c => c.Contains("reltuples", StringComparison.Ordinal)));
        var coverage = Rows(after.GetProperty("coverageRows"));
        Assert.Single(coverage);
        Assert.Equal("collect.parts_2", coverage[0][6]);
    }

    /// <summary>#8897: a rolled-up row's flag says how many of its copies are larger than their table, even one, and its empty-space percent
    /// names the size it is a share of when only some copies were estimated. A single copy and a fully estimated row keep the plain text.</summary>
    [Fact]
    public void ARolledUpIndexRow_SaysHowManyCopiesAreLarger_AndNamesTheBasisOfAPartialEstimate()
    {
        var rows = Rows(Result.Value.GetProperty("afterIndexes").GetProperty("indexRows"));
        var one = rows.Single(r => r.Contains("ix_rolled_one"));
        Assert.Equal("1 of 164 copies larger than its table", one[0]);
        Assert.Contains(one, c => c.Contains("100% of the 6.0 MB estimated", StringComparison.Ordinal));
        var several = rows.Single(r => r.Contains("ix_rolled_several"));
        Assert.Equal("7 of 164 copies larger than its table", several[0]);
        Assert.Contains("25%", several);
        Assert.DoesNotContain(several, c => c.Contains("estimated", StringComparison.Ordinal) && !c.Contains("Not Estimated", StringComparison.Ordinal));
        Assert.Equal("Larger than its table", rows.Single(r => r.Contains("ix_big"))[0]);
    }

    [Fact]
    public void WhenEveryPartitionHasEveryIndex_TheCoverageSectionIsOneLine()
    {
        var clean = Result.Value.GetProperty("cleanCoverage");
        Assert.Equal(0, clean.GetProperty("headings").GetInt32());
        Assert.Contains("Every partition", clean.GetProperty("text").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void WhenOnlyTheCoverageReadRanPastItsLimit_TheIndexRowsStay_AndTheCoverageSectionSaysItWasLeftOut()
    {
        var left = Result.Value.GetProperty("coverageLeftOut");
        Assert.True(left.GetProperty("indexGridRows").GetInt32() > 0, "the index rows are still drawn");
        var text = left.GetProperty("text").GetString()!;
        Assert.Contains("coverage was left out", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Every partition", text, StringComparison.Ordinal);
    }

    [Fact]
    public void Measure_AsksForTheRowsTable_LocksEveryButtonWhileItRuns_AndShowsTheResult()
    {
        var m = Result.Value.GetProperty("measure");
        var asks = m.GetProperty("asks").EnumerateArray().ToArray();
        Assert.Single(asks);
        Assert.Equal("collect.store_metrics", asks[0].GetProperty("table").GetString());
        Assert.Contains("has run for", m.GetProperty("panelWhileRunning").GetString()!, StringComparison.Ordinal);
        Assert.All(m.GetProperty("disabledWhileRunning").EnumerateArray(), b => Assert.True(b.GetBoolean()));
        Assert.All(m.GetProperty("disabledAfter").EnumerateArray(), b => Assert.False(b.GetBoolean()));
        var panel = m.GetProperty("panelText").GetString()!;
        Assert.Contains("700 of 1,000", panel, StringComparison.Ordinal);
        Assert.Contains("Can VACUUM shrink the file?", panel, StringComparison.Ordinal);
        Assert.Contains("used by live rows", panel, StringComparison.Ordinal);
        Assert.Equal(4, m.GetProperty("histogramRows").GetArrayLength());
    }
}
