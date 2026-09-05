/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3017 item 3: <c>get_collection_health</c> reported what a collector SPENT and never what it BOUGHT.
///
/// <para><b>Every statistic on a collector row described cost</b> — <c>total_runs</c>,
/// <c>avg_duration_ms</c>, <c>p95_duration_ms</c>, <c>max_duration_ms</c>, and the sweep-pressure roll-up
/// built from them. The rows figure lived on <c>get_collector_cost</c> instead, a different tool over a
/// different series, so correlating spend against output was a join a reader had to know to make.</para>
///
/// <para><b>Measured.</b> <c>pg_deadlocks</c> was the single most expensive collector on a managed store —
/// 49,258,335 ms over 79,333 runs in seven days, about 13.7 h/week — and stored zero rows.</para>
///
/// <para><b>And that zero was CORRECT, which is the whole difficulty.</b> The reader was working on all 50
/// targets and there were no deadlocks to find. Zero rows is the correct resting state for a collector that
/// stores a row only when an event occurs, so a verdict keyed on cost-plus-zero-rows fires on the healthy
/// quiet install rather than the blind one — the cry-wolf failure <c>target_has_user_databases</c> (#1852)
/// exists to prevent. Telling the two apart needs a third term, and #3010's
/// <see cref="CollectorHealth.DeniedSinceLastSuccess"/> is it: zero output WITH a current denial is a
/// collector that could not read, where zero output alone is one that read and found nothing.</para>
///
/// <para>So what shipped is a fact beside the cost and not a band, on the shape #3027's
/// <c>deadlock_coverage</c> established one level up: numbers, a denominator, a named cause, and a sentence
/// that names both windows and disclaims the one it did not measure.</para>
/// </summary>
public sealed class CollectionOutputBesideCostTests
{
    /* The measured pg_deadlocks counts, so every row below is the shape this issue was filed on rather
       than a convenient invention. Runs and cost from #3017; the success/denial split from #3010's own
       fixture, which is the same collector over the same window. */
    private const long MeasuredRuns = 79_333;
    private const long MeasuredSuccesses = 63_448;
    private const long MeasuredDenials = 15_885;

    private static CollectorHealth Row(long rowsStored, long runsWithRows, bool denialIsNewest) => new()
    {
        CollectorName = "pg_deadlocks",
        TotalRuns = MeasuredRuns,
        SuccessCount = MeasuredSuccesses,
        ErrorCount = 0,
        PermissionDeniedCount = MeasuredDenials,
        RowsStored = rowsStored,
        RunsWithRows = runsWithRows,
        /* The ONLY thing that differs between the "could not read" and "read and found nothing" rows: which
           side of the last success the newest denial falls on. Same counts, same cost, same band. */
        LastDeniedTime = denialIsNewest ? DateTime.UtcNow.AddMinutes(-1) : DateTime.UtcNow.AddDays(-6),
        LastSuccessTime = denialIsNewest ? DateTime.UtcNow.AddMinutes(-30) : DateTime.UtcNow.AddMinutes(-1),
        LastRunTime = DateTime.UtcNow.AddMinutes(-1),
        AvgDurationMs = 620.9,
        P95DurationMs = 1_400,
        MaxDurationMs = 9_100,
    };

    /// <summary>
    /// THE REQUIREMENT: all three readings must be separable from ONE row, without a second tool call.
    /// Expensive and productive, expensive and correctly empty, expensive and blind.
    /// </summary>
    [Fact]
    public void TheThreeReadings_AreSeparableFromOneRow()
    {
        var productive = Row(rowsStored: 41_622, runsWithRows: 19_004, denialIsNewest: false);
        var correctlyEmpty = Row(rowsStored: 0, runsWithRows: 0, denialIsNewest: false);
        var blind = Row(rowsStored: 0, runsWithRows: 0, denialIsNewest: true);

        /* Expensive and PRODUCTIVE: the numbers say it and no sentence fires. A note on the healthy case is
           how a signal teaches people to ignore it. */
        Assert.Null(productive.OutputFinding);
        Assert.True(productive.ProductiveRunPercent > 0);

        /* Expensive and CORRECTLY EMPTY: read, found nothing, needs no action - and says so, rather than
           implying a fault. It must NOT send anyone after a grant. */
        Assert.NotNull(correctlyEmpty.OutputFinding);
        Assert.Contains("read and found nothing", correctlyEmpty.OutputFinding, StringComparison.Ordinal);
        Assert.Contains("correct resting state", correctlyEmpty.OutputFinding, StringComparison.Ordinal);
        Assert.DoesNotContain("grant", correctlyEmpty.OutputFinding, StringComparison.Ordinal);

        /* Expensive and BLIND: the spend bought nothing because nothing could be read. This one IS a grant,
           and the assertion above is its positive control - the same DoesNotContain form over the same
           token on the row that demonstrably carries it, so a check passing by matching nothing cannot
           hide in the pair. */
        Assert.NotNull(blind.OutputFinding);
        Assert.Contains("grant", blind.OutputFinding, StringComparison.Ordinal);
        Assert.Contains("refused NOW", blind.OutputFinding, StringComparison.Ordinal);
        Assert.DoesNotContain("read and found nothing", blind.OutputFinding, StringComparison.Ordinal);

        /* And the two zero rows really are the SAME spend with the SAME output. Only the third term differs,
           which is the entire point of having waited for #3010. */
        Assert.Equal(correctlyEmpty.RowsStored, blind.RowsStored);
        Assert.Equal(correctlyEmpty.TotalRuns, blind.TotalRuns);
        Assert.NotEqual(correctlyEmpty.DeniedSinceLastSuccess, blind.DeniedSinceLastSuccess);
    }

    /// <summary>
    /// NOT A BAND, asserted in both directions the issue was explicit about.
    ///
    /// <para>Output cannot move the band: the productive row, the correctly-empty row and the blind row all
    /// carry the same verdict. That is the #1852 discipline — a legitimately empty collector stays HEALTHY —
    /// and it is what stops this instrument firing on the quiet install.</para>
    /// </summary>
    [Fact]
    public void TheBandIsIdentical_AcrossAllThreeReadings()
    {
        var productive = Row(rowsStored: 41_622, runsWithRows: 19_004, denialIsNewest: false);
        var correctlyEmpty = Row(rowsStored: 0, runsWithRows: 0, denialIsNewest: false);
        var blind = Row(rowsStored: 0, runsWithRows: 0, denialIsNewest: true);

        Assert.Equal(CollectorHealthClassifier.Healthy, productive.HealthStatus);
        Assert.Equal(CollectorHealthClassifier.Healthy, correctlyEmpty.HealthStatus);
        Assert.Equal(CollectorHealthClassifier.Healthy, blind.HealthStatus);

        /* The counterfactual, so the three above are not merely three copies of a row that can only ever
           band HEALTHY: the SAME output figures with a stale success clock still band on the clock. */
        var stale = Row(rowsStored: 0, runsWithRows: 0, denialIsNewest: false);
        stale.LastSuccessTime = DateTime.UtcNow.AddDays(-5);
        stale.LastRunTime = DateTime.UtcNow.AddDays(-5);
        Assert.NotEqual(CollectorHealthClassifier.Healthy, stale.HealthStatus);
    }

    /// <summary>
    /// And the direction the equality above cannot reach on its own: <c>denied_since_last_success</c> must
    /// not have become a BAND input on the way to being read here.
    ///
    /// <para><see cref="CollectorHealthClassifier.DeniedSinceLastSuccess"/>'s own doc comment is explicit
    /// that it reports and does not band, and <c>LastErrorCurrencyTests</c> pins that the band does not read
    /// the predicate. #3017 consumes the predicate for the first time, so the risk it introduces is exactly
    /// that consumption leaking into the banding chain. This reads
    /// <see cref="CollectorHealthClassifier.Classify"/>'s parameter list off the TYPE rather than asserting
    /// over a hand-written list of what it takes today, so a TENTH parameter — an output count, a denial
    /// flag, anything — fails here instead of passing unnoticed.</para>
    /// </summary>
    [Fact]
    public void TheBandingSignature_TakesNoOutputAndNoDenialCurrency()
    {
        var parameters = typeof(CollectorHealthClassifier)
            .GetMethod(nameof(CollectorHealthClassifier.Classify), BindingFlags.Public | BindingFlags.Static)!
            .GetParameters()
            .Select(p => p.Name!)
            .ToArray();

        /* The precondition, named so a signature change reports itself instead of turning the assertions
           below into a vacuous pass over a list that no longer means what this test thinks. */
        Assert.Equal(9, parameters.Length);

        Assert.Equal(
            new[]
            {
                "totalRuns", "successCount", "errorCount", "permissionDeniedCount", "abandonedCount",
                "hoursSinceLastSuccess", "hoursSinceLastRun", "frequencyMinutes", "isOnLoad",
            },
            parameters);

        /* And the same statement by shape rather than by the exact list, so a RENAME that smuggles one in
           still fails: nothing the band takes may be about rows, output, or denial currency. */
        Assert.DoesNotContain(parameters, p => p.Contains("rows", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(parameters, p => p.Contains("output", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(parameters, p => p.Contains("denied", StringComparison.OrdinalIgnoreCase)
                                            && p.Contains("since", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// The note has to name BOTH windows and disclaim the one it did not read. This is the half #3027 called
    /// the harder one, and the half two earlier attempts at the analogous work got wrong: a sentence claiming
    /// both figures were "read in this window" is the same defect one level up.
    /// </summary>
    [Fact]
    public void TheWindowNote_NamesBothWindows_AndDisclaimsTheOneItDidNotMeasure()
    {
        var note = CollectorHealthClassifier.OutputWindowNote;

        /* The window this DID measure, said as the same window the cost figures cover - one aggregate over
           one window, so cost and output on a row cannot describe different runs. */
        Assert.Contains("SAME fixed trailing seven days", note, StringComparison.Ordinal);
        Assert.Contains("total_runs", note, StringComparison.Ordinal);

        /* The window it did NOT measure, named as a tool and disclaimed outright rather than left for a
           reader to assume the two reconcile. */
        Assert.Contains("get_collector_cost", note, StringComparison.Ordinal);
        Assert.Contains("hourly", note, StringComparison.Ordinal);
        Assert.Contains("days_back", note, StringComparison.Ordinal);
        Assert.Contains("make no claim", note, StringComparison.Ordinal);

        /* And the third thing it is not: rows STORED is not what the monitored engine counted. #3030
           measured a route storing zero rows ever across 85,549 SUCCESS runs while the engines counted six
           new deadlocks, so this instrument reports "expensive, zero rows, not denied" for a collector that
           is silently capturing nothing - the reading that should prompt investigation. Saying what is not
           measured is what stops the figure being read as a yield measurement. */
        Assert.Contains("STORED", note, StringComparison.Ordinal);
        Assert.Contains("engine counted", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// The read has to project both columns, or <see cref="CollectorHealth.RowsStored"/> defaults to 0 and
    /// the whole instrument silently becomes "every collector stored nothing" — alarming, wrong, compiling.
    /// </summary>
    [Fact]
    public void TheHealthRead_ProjectsBothOutputColumns_OverItsOwnWindow()
    {
        var sql = DarlingDataReader.CollectionHealthSql;

        Assert.Contains("AS rows_stored", sql, StringComparison.Ordinal);
        Assert.Contains("AS runs_with_rows", sql, StringComparison.Ordinal);

        /* COALESCE, so a zero is unambiguous at the STORE. Without it a collector whose every rows_collected
           is NULL returns NULL, which a reader has to guess between "not measured" and "stored nothing" -
           and the whole point of the column is that the second becomes a fact rather than an absence. */
        Assert.Contains("COALESCE(SUM(rows_collected), 0) AS rows_stored", sql, StringComparison.Ordinal);

        /* The denominator's partner, off the same rows_collected > 0 test get_pg_blocking already reports
           captures_with_blocking from. */
        Assert.Contains(
            "SUM(CASE WHEN rows_collected > 0 THEN 1 ELSE 0 END) AS runs_with_rows",
            sql,
            StringComparison.Ordinal);

        /* Both come from THIS query's window, not a second parameter: the read is bounded by $2 alone, so
           cost and output on a row cannot describe different runs. The sibling assertion is the positive
           control - the same Contains form over the bound this read demonstrably carries. */
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$3", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE CROSS-SKU PIN, and the one this repo's history says is most needed: both MCP surfaces read this
    /// result set POSITIONALLY, so a column appended on one side alone silently re-maps every column after
    /// it in the other.
    ///
    /// <para>#3006 had to land its three fields on BOTH SKUs with a suite per side for exactly this reason.
    /// So rather than asserting that Lite happens to contain two alias strings, this derives the whole
    /// ordinal map from each reader's source and compares the MAPS. It sees the set grow in both directions:
    /// a column added to one side only makes the maps unequal, and a column added to both trips the count
    /// precondition until a decision reaches this test.</para>
    /// </summary>
    [Fact]
    public void BothSkusReaders_MapTheSameOrdinals_ToTheSameFields()
    {
        var darling = OrdinalMap(
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs"),
            "public static async Task<List<CollectorHealth>> GetCollectionHealthAsync");
        var lite = OrdinalMap(
            Path.Combine("Lite", "Services", "LocalDataService.CollectionHealth.cs"),
            "public async Task<List<CollectorHealthRow>> GetCollectionHealthAsync");

        /* The precondition. 24 columns since #3017 - 16 at #2460, plus #2472's four fan-out columns,
           #2804's abandoned_count, #3010's last_denied_time and this change's two. A parse that stopped
           finding them would otherwise turn the comparison below into two empty maps agreeing. */
        Assert.Equal(24, darling.Count);
        Assert.Equal(24, lite.Count);

        /* No gaps and no duplicates: ordinals 0..23 exactly once each. A duplicate would let two fields
           read one column while a third read nothing, which compiles and is silently wrong. */
        Assert.Equal(Enumerable.Range(0, 24), darling.Values.OrderBy(o => o));
        Assert.Equal(Enumerable.Range(0, 24), lite.Values.OrderBy(o => o));

        /* And the same field at the same ordinal on both sides. */
        Assert.Equal(
            darling.OrderBy(kv => kv.Value).Select(kv => $"{kv.Value}={kv.Key}").ToArray(),
            lite.OrderBy(kv => kv.Value).Select(kv => $"{kv.Value}={kv.Key}").ToArray());

        /* APPENDED, never inserted - the two new columns take the two highest ordinals, so nothing before
           them moved. This is the assertion a bare map-equality cannot make: two sides that BOTH inserted
           mid-list would agree with each other and disagree with every already-stamped read. */
        Assert.Equal(22, darling["RowsStored"]);
        Assert.Equal(23, darling["RunsWithRows"]);
        Assert.Equal(21, darling["LastDeniedTime"]);
    }

    /// <summary>
    /// The WIRING pin, because no behavioural test reaches it: the fields are emitted by an anonymous object
    /// inside an MCP tool method that needs a live store to invoke. #3010's own wiring pin exists because a
    /// mutation removing <c>last_error_at</c> from that row left the entire suite green.
    /// </summary>
    [Theory]
    [InlineData("rows_stored = r.RowsStored,")]
    [InlineData("runs_with_rows = r.RunsWithRows,")]
    [InlineData("productive_run_pct = Math.Round(r.ProductiveRunPercent, 1),")]
    public void BothSkusTools_PutTheOutputFigures_BesideTheCost(string wiring)
    {
        foreach (var relative in ToolSources)
        {
            var source = ReadRepoFile(relative);

            /* The cost figures this has to sit BESIDE, asserted first: the same field emitted somewhere else
               in the file would satisfy a bare Contains. These anchors are also the POSITIVE CONTROL for the
               span assertion below, so a check that passed by matching nothing at all cannot hide here. */
            Assert.Contains("total_runs = r.TotalRuns,", source, StringComparison.Ordinal);
            Assert.Contains("max_duration_ms = Math.Round(r.MaxDurationMs, 0),", source, StringComparison.Ordinal);
            Assert.Contains(wiring, source, StringComparison.Ordinal);

            var start = source.IndexOf("total_runs = r.TotalRuns,", StringComparison.Ordinal);
            var end = source.IndexOf("last_success = r.LastSuccessTime", StringComparison.Ordinal);
            Assert.True(end > start, $"{relative}: the get_collection_health row's field order moved - this pin needs re-anchoring");
            Assert.Contains(wiring, source[start..end], StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The finding rides directly beneath the term that gives it its meaning, and the window note rides once
    /// on the response rather than ~41 times inside it.
    /// </summary>
    [Fact]
    public void BothSkusTools_PutTheFindingUnderItsThirdTerm_AndTheWindowNoteOnTheResponse()
    {
        foreach (var relative in ToolSources)
        {
            var source = ReadRepoFile(relative);

            Assert.Contains("output_finding = r.OutputFinding,", source, StringComparison.Ordinal);
            Assert.Contains(
                "output_note = CollectorHealthClassifier.OutputWindowNote,",
                source,
                StringComparison.Ordinal);

            /* Between the predicate that separates the two zero readings and the #1837 note block, so a
               reader meets the finding while the term it depends on is still on screen. */
            var denied = source.IndexOf("denied_since_last_success = r.DeniedSinceLastSuccess,", StringComparison.Ordinal);
            var finding = source.IndexOf("output_finding = r.OutputFinding,", StringComparison.Ordinal);
            var note = source.IndexOf("last_note = r.LastNote,", StringComparison.Ordinal);

            Assert.True(denied > 0 && note > denied, $"{relative}: the row's field order moved - this pin needs re-anchoring");
            Assert.True(finding > denied, $"{relative}: the finding is emitted before the term that gives it meaning");
            Assert.True(finding < note, $"{relative}: the finding drifted out of the block it qualifies");

            /* The note is on the RESPONSE, not the row: it is identical for every collector, and ~41 copies
               of a paragraph is a payload cost with no reader benefit. Bounded against the end of the row
               projection rather than merely against a field inside it - the sweep-pressure computation is
               the first statement after `rows.Select(...)` closes, so a note before it would be a note on
               every row. */
            var rowProjectionEnds = source.IndexOf("var pressure = SweepPressureClassifier.Compute(", StringComparison.Ordinal);
            var windowNote = source.IndexOf("output_note = CollectorHealthClassifier.OutputWindowNote,", StringComparison.Ordinal);
            Assert.True(rowProjectionEnds > note, $"{relative}: the row projection no longer ends where this pin expects");
            Assert.True(windowNote > rowProjectionEnds, $"{relative}: the window note moved inside the per-collector row");
        }
    }

    /// <summary>
    /// The FOURTH surface. Darling's Collection Health exists as the WPF Viewer grid, the MCP tool, and the
    /// web dashboard's table, which renders whatever <c>COLLECTOR_COLUMNS</c> lists out of that same tool's
    /// payload — a field added to the tool but not to that list is silently dropped, leaving the browser as
    /// the one surface still hiding it. #1837's own suite pins that relationship; this is the same debt.
    /// </summary>
    [Fact]
    public void TheWebTable_RendersWhatTheSpendBought()
    {
        var source = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));

        /* The DEFINITION, not an earlier `columns: COLLECTOR_COLUMNS` use site. */
        var start = source.IndexOf("const COLLECTOR_COLUMNS", StringComparison.Ordinal);
        Assert.True(start >= 0, "server-tabs.js must still define COLLECTOR_COLUMNS");
        var columns = source[start..];
        columns = columns[..columns.IndexOf("];", StringComparison.Ordinal)];

        /* A column the list demonstrably carries, as the positive control for the three below: this slice
           really is the array, not an empty string that every Contains would pass against. */
        Assert.Contains("\"total_runs\"", columns, StringComparison.Ordinal);

        Assert.Contains("\"rows_stored\"", columns, StringComparison.Ordinal);
        Assert.Contains("\"runs_with_rows\"", columns, StringComparison.Ordinal);
        Assert.Contains("\"output_finding\"", columns, StringComparison.Ordinal);
    }

    /// <summary>
    /// And the tool DESCRIPTION has to teach the three readings, because two numbers do not help a caller
    /// who does not know that a zero can be correct — and who, told nothing, either ignores every zero or
    /// investigates every one. The sentence is part of the fix, not commentary on it.
    /// </summary>
    [Fact]
    public void BothSkusToolDescriptions_TeachThatACorrectZeroExists()
    {
        foreach (var relative in ToolSources)
        {
            var source = ReadRepoFile(relative);

            Assert.Contains("what it BOUGHT", source, StringComparison.Ordinal);
            Assert.Contains("that zero was CORRECT", source, StringComparison.Ordinal);
            Assert.Contains("deliberately NOT a band", source, StringComparison.Ordinal);
            Assert.Contains("needs a grant", source, StringComparison.Ordinal);
        }
    }

    private static readonly string[] ToolSources =
    [
        Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"),
        Path.Combine("Lite", "Mcp", "McpHealthTools.cs"),
    ];

    /// <summary>
    /// Every <c>Field = reader.Something(N)</c> assignment inside one reader method, as field -> ordinal.
    /// Derived rather than listed, so the map grows with the read instead of describing the read as it was
    /// the day this test was written.
    /// </summary>
    private static Dictionary<string, int> OrdinalMap(string relativePath, string methodSignature)
    {
        var source = ReadRepoFile(relativePath);
        var start = source.IndexOf(methodSignature, StringComparison.Ordinal);
        Assert.True(start > 0, $"{relativePath}: {methodSignature} not found - this pin needs re-anchoring");

        /* Bounded at the method's own `return`, so a later method in the same file cannot contribute
           ordinals to this map. */
        var end = source.IndexOf("return ", start, StringComparison.Ordinal);
        Assert.True(end > start, $"{relativePath}: {methodSignature} has no return - this pin needs re-anchoring");

        var map = new Dictionary<string, int>(StringComparer.Ordinal);

        /* Non-greedy up to the FIRST reader call on the line: TargetHasUserDatabases reads the same ordinal
           twice (IsDBNull then GetValue), and a greedy match would take the second one. */
        foreach (var match in Regex.Matches(source[start..end], @"^\s*(\w+) = [^\r\n]*?reader\.\w+\((\d+)\)", RegexOptions.Multiline).Cast<Match>())
        {
            map[match.Groups[1].Value] = int.Parse(match.Groups[2].Value, System.Globalization.CultureInfo.InvariantCulture);
        }

        return map;
    }

    private static string ReadRepoFile(string relative) =>
        File.ReadAllText(Path.Combine(RepoRoot(), relative));

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
