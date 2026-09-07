/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's half of #3017 item 3. The defect was measured on a Darling fleet, but the SHAPE is Lite's too:
/// the same <c>get_collection_health</c> tool name, the same per-collector row whose every statistic
/// described what the collector SPENT, and the same rows figure absent from it.
///
/// <para><b>Parity matters here for a specific reason rather than as a habit.</b> Both SKUs' health reads
/// are consumed POSITIONALLY, and #3017 appends two columns to that result set — so a column landing on one
/// side alone does not merely omit a field, it re-maps every column after it on the side that was not
/// edited. #3006 had to land its three fields on both SKUs with a suite per side for exactly this reason,
/// and this repo's recurring failure is the parity change that arrives on one surface.</para>
///
/// <para>The cross-SKU ordinal comparison itself lives in <c>Darling.Tests</c>, which is the side that can
/// reference one SQL const directly and read the other's source: <c>Lite.Tests</c> has no reference to any
/// Darling project. What lives here is Lite's own behaviour and Lite's own wiring.</para>
/// </summary>
public sealed class CollectionOutputBesideCostTests
{
    /* #3010's own Lite fixture counts, so this row is the same shape its sibling suite already pins. */
    private static CollectorHealthRow Row(
        long rowsStored,
        long runsWithRows,
        bool denialIsNewest,
        long noteCount = 0) => new()
    {
        CollectorName = "deadlocks",
        TotalRuns = 79_333,
        SuccessCount = 63_448,
        ErrorCount = 0,
        PermissionDeniedCount = 15_885,
        RowsStored = rowsStored,
        RunsWithRows = runsWithRows,
        /* The ONLY difference between "could not read" and "read and found nothing": which side of the last
           success the newest denial falls on. Same counts, same cost, same band. */
        LastDeniedTime = denialIsNewest ? DateTime.UtcNow.AddMinutes(-1) : DateTime.UtcNow.AddDays(-6),
        LastSuccessTime = denialIsNewest ? DateTime.UtcNow.AddMinutes(-30) : DateTime.UtcNow.AddMinutes(-1),
        LastRunTime = DateTime.UtcNow.AddMinutes(-1),
        AvgDurationMs = 620.9,
        P95DurationMs = 1_400,
        MaxDurationMs = 9_100,
        /* note_count is COUNT(error_message) over SUCCESS runs and last_note is the newest of
           them, ordered notes-first, so a positive count always has a note to point at. Kept
           consistent here rather than set independently, because a row with a count and no note
           is a state the read cannot produce. */
        NoteCount = noteCount,
        LastNote = noteCount > 0 ? "enumeration yielded 0 items - nothing to collect this cycle" : null,
    };

    /// <summary>
    /// The three readings, separable from ONE row: expensive and productive, expensive and correctly empty,
    /// expensive and blind. Zero rows is the correct resting state for <c>deadlocks</c> on a well-behaved
    /// SQL Server, which is why the empty row must read as needing no action.
    /// </summary>
    [Fact]
    public void TheThreeReadings_AreSeparableFromOneRow()
    {
        var productive = Row(rowsStored: 41_622, runsWithRows: 19_004, denialIsNewest: false);
        var correctlyEmpty = Row(rowsStored: 0, runsWithRows: 0, denialIsNewest: false);
        var blind = Row(rowsStored: 0, runsWithRows: 0, denialIsNewest: true);

        Assert.Null(productive.OutputFinding);
        Assert.True(productive.ProductiveRunPercent > 0);

        Assert.NotNull(correctlyEmpty.OutputFinding);
        Assert.Contains("read and found nothing", correctlyEmpty.OutputFinding, StringComparison.Ordinal);
        Assert.Contains("correct resting state", correctlyEmpty.OutputFinding, StringComparison.Ordinal);
        /* Must NOT send anyone after a grant. The Contains below is this negative's positive control: the
           same token, the same comparison, on the row that demonstrably carries it. */
        Assert.DoesNotContain("grant", correctlyEmpty.OutputFinding, StringComparison.Ordinal);

        Assert.NotNull(blind.OutputFinding);
        Assert.Contains("grant", blind.OutputFinding, StringComparison.Ordinal);
        Assert.Contains("refused NOW", blind.OutputFinding, StringComparison.Ordinal);

        /* Same spend, same output, opposite readings - the third term is the entire difference. */
        Assert.Equal(correctlyEmpty.RowsStored, blind.RowsStored);
        Assert.NotEqual(correctlyEmpty.DeniedSinceLastSuccess, blind.DeniedSinceLastSuccess);
    }

    /// <summary>
    /// A DELIBERATE zero and a BROKEN zero must be distinguishable, and the fourth term is what does it
    /// (#3160). Both rows here are the same collector with the same spend and the same zero output; the ONLY
    /// input that differs is how many runs recorded a note about what they found.
    ///
    /// <para><b>Same collector on both rows on purpose.</b> The mechanism is keyed on the row's counts and
    /// never on the collector's NAME. A name list is what #2511 exists to refuse, because it goes stale in
    /// the direction that makes it pass: the next periodic collector to break gets the event-collector
    /// sentence until somebody remembers to add it. Driving both readings out of one name is the assertion
    /// that no name list is consulted.</para>
    ///
    /// <para>The measured subject was <c>query_store</c>, which is not an event collector and stored zero
    /// rows on 11,728 consecutive runs on a read-replica fleet, every one carrying an empty-enumeration
    /// note. It got the event-collector sentence anyway — the right conclusion from a rationale that does
    /// not hold, and the same sentence a <c>query_store</c> that had genuinely stopped would have got.</para>
    /// </summary>
    [Fact]
    public void TheNotedZero_DefersToTheNote_AndTheUnnotedZeroKeepsTheCategoryReading()
    {
        var noted = Row(rowsStored: 0, runsWithRows: 0, denialIsNewest: false, noteCount: 79_333);
        var unnoted = Row(rowsStored: 0, runsWithRows: 0, denialIsNewest: false, noteCount: 0);

        Assert.NotNull(noted.OutputFinding);
        Assert.NotNull(unnoted.OutputFinding);

        /* The NOTED row must not assert the category. Both halves are named, because "needs no action" is
           the clause that reads as reassurance over a collector that has actually stopped. */
        Assert.DoesNotContain("correct resting state", noted.OutputFinding, StringComparison.Ordinal);
        Assert.DoesNotContain("needs no action", noted.OutputFinding, StringComparison.Ordinal);

        /* It defers instead: how many runs said something, and where to read what they said. */
        Assert.Contains("last_note", noted.OutputFinding, StringComparison.Ordinal);
        Assert.Contains("note_count", noted.OutputFinding, StringComparison.Ordinal);

        /* The UNNOTED row is the positive control for the two DoesNotContain assertions above - the same
           tokens over a row that demonstrably carries them, so a check passing by matching nothing cannot
           hide in the pair. */
        Assert.Contains("correct resting state", unnoted.OutputFinding, StringComparison.Ordinal);
        Assert.Contains("needs no action", unnoted.OutputFinding, StringComparison.Ordinal);

        /* And that reading now states the precondition it rests on instead of asserting a category. */
        Assert.Contains("No run recorded a note", unnoted.OutputFinding, StringComparison.Ordinal);

        /* Neither is the denial reading, and both still report the spend and the zero. */
        foreach (var finding in new[] { noted.OutputFinding, unnoted.OutputFinding })
        {
            Assert.DoesNotContain("grant", finding, StringComparison.Ordinal);
            Assert.Contains("Stored 0 rows", finding, StringComparison.Ordinal);
        }

        /* Same spend, same output, one differing term - the shape this suite already uses for the third. */
        Assert.Equal(noted.RowsStored, unnoted.RowsStored);
        Assert.Equal(noted.TotalRuns, unnoted.TotalRuns);
        Assert.NotEqual(noted.NoteCount, unnoted.NoteCount);
    }

    /// <summary>
    /// The finding's signature read off the TYPE rather than asserted over a hand-written list, so a FIFTH
    /// parameter reports itself here instead of passing unnoticed — the discipline
    /// <see cref="TheBandingSignature_TakesNoOutputAndNoDenialCurrency"/> applies to the band, one method
    /// over.
    ///
    /// <para><b>The load-bearing half is that none of it is a string.</b> The note's prose has exactly one
    /// home, <see cref="CollectorHealthClassifier.FormatCollectionNote"/>. A finding that took the note TEXT
    /// would be a second copy of a sentence whose whole value is being accurate about one server's answer,
    /// and the copy that drifts is never the one being read. Taking the COUNT and pointing at the field is
    /// what keeps that single copy, so the absence of a string parameter is the property, not an
    /// accident.</para>
    /// </summary>
    [Fact]
    public void TheFindingSignature_TakesTheNoteCount_AndNoNoteText()
    {
        var parameters = typeof(CollectorHealthClassifier)
            .GetMethod(
                nameof(CollectorHealthClassifier.FormatOutputFinding),
                BindingFlags.Public | BindingFlags.Static)!
            .GetParameters();

        /* The precondition, named so a signature change reports itself rather than turning the assertions
           below into a vacuous pass over a list that no longer means what this test thinks. */
        Assert.Equal(4, parameters.Length);

        Assert.Equal(
            new[] { "rowsStored", "totalRuns", "deniedSinceLastSuccess", "noteCount" },
            parameters.Select(p => p.Name!).ToArray());

        Assert.DoesNotContain(parameters, p => p.ParameterType == typeof(string));
    }

    /// <summary>
    /// NOT A BAND. All three readings carry one verdict, and the staleness counterfactual proves that is a
    /// statement about the output figures rather than a row which could only ever band HEALTHY.
    /// </summary>
    [Fact]
    public void TheBandIsIdentical_AcrossAllThreeReadings()
    {
        Assert.Equal(CollectorHealthClassifier.Healthy, Row(41_622, 19_004, false).HealthStatus);
        Assert.Equal(CollectorHealthClassifier.Healthy, Row(0, 0, false).HealthStatus);
        Assert.Equal(CollectorHealthClassifier.Healthy, Row(0, 0, true).HealthStatus);

        var stale = Row(0, 0, false);
        stale.LastSuccessTime = DateTime.UtcNow.AddDays(-5);
        stale.LastRunTime = DateTime.UtcNow.AddDays(-5);
        Assert.NotEqual(CollectorHealthClassifier.Healthy, stale.HealthStatus);
    }

    /// <summary>
    /// And the direction that equality cannot reach: <c>denied_since_last_success</c> must not have become a
    /// band input on the way to being consumed by the finding. Read off the TYPE with a count precondition,
    /// so a tenth parameter — an output count, a denial flag, anything — fails here rather than passing
    /// unnoticed while <see cref="CollectorHealthClassifier.DeniedSinceLastSuccess"/>'s doc comment still
    /// claims it never bands.
    /// </summary>
    [Fact]
    public void TheBandingSignature_TakesNoOutputAndNoDenialCurrency()
    {
        var parameters = typeof(CollectorHealthClassifier)
            .GetMethod(nameof(CollectorHealthClassifier.Classify), BindingFlags.Public | BindingFlags.Static)!
            .GetParameters()
            .Select(p => p.Name!)
            .ToArray();

        Assert.Equal(9, parameters.Length);
        Assert.DoesNotContain(parameters, p => p.Contains("rows", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(parameters, p => p.Contains("output", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(parameters, p => p.Contains("denied", StringComparison.OrdinalIgnoreCase)
                                            && p.Contains("since", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// The DuckDB read has to project both columns, or <see cref="CollectorHealthRow.RowsStored"/> defaults
    /// to 0 and the instrument silently reports that every collector stored nothing.
    /// </summary>
    [Fact]
    public void TheCollectionHealthRead_ProjectsBothOutputColumns_OverItsOwnWindow()
    {
        var sql = LocalDataService.CollectionHealthSql;

        /* Byte-identical to Darling's, which is the standing contract for this read: both MCP surfaces
           consume the result set positionally and these two aliases are appended at the same ordinals. */
        Assert.Contains("COALESCE(SUM(rows_collected), 0) AS rows_stored", sql, StringComparison.Ordinal);
        Assert.Contains(
            "SUM(CASE WHEN rows_collected > 0 THEN 1 ELSE 0 END) AS runs_with_rows",
            sql,
            StringComparison.Ordinal);

        /* Both from THIS query's window - one bound, so cost and output cannot describe different runs. The
           Contains is the positive control for the negative beside it. */
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$3", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The WIRING pin, because no behavioural test reaches it: the fields are emitted by an anonymous object
    /// inside an MCP tool method. A mutation removing one left the whole suite green on the Darling side
    /// during #3010, so both SKUs pin their own tool by source.
    /// </summary>
    [Theory]
    [InlineData("rows_stored = r.RowsStored,")]
    [InlineData("runs_with_rows = r.RunsWithRows,")]
    [InlineData("productive_run_pct = Math.Round(r.ProductiveRunPercent, 1),")]
    [InlineData("output_finding = r.OutputFinding,")]
    [InlineData("output_note = CollectorHealthClassifier.OutputWindowNote,")]
    public void TheHealthTool_ReportsWhatTheSpendBought(string wiring)
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Mcp", "McpHealthTools.cs"));

        /* The cost anchors this has to sit beside, asserted first - a field emitted by another tool in the
           same file would satisfy a bare Contains. They are also the POSITIVE CONTROL for the span
           assertions, so a check that passed by matching nothing cannot hide here. */
        Assert.Contains("total_runs = r.TotalRuns,", source, StringComparison.Ordinal);
        Assert.Contains("max_duration_ms = Math.Round(r.MaxDurationMs, 0),", source, StringComparison.Ordinal);
        Assert.Contains(wiring, source, StringComparison.Ordinal);
    }

    /// <summary>
    /// And where they sit, which is the half of "put the fact beside the cost" that a Contains cannot state:
    /// the three numbers inside the cost block, the finding directly under the term that gives it meaning,
    /// and the window note ONCE on the response rather than on all ~41 rows.
    /// </summary>
    [Fact]
    public void TheOutputFigures_SitBesideTheCost_AndTheNoteSitsOnTheResponse()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Mcp", "McpHealthTools.cs"));

        var costStart = source.IndexOf("total_runs = r.TotalRuns,", StringComparison.Ordinal);
        var costEnd = source.IndexOf("last_success = r.LastSuccessTime", StringComparison.Ordinal);
        var denied = source.IndexOf("denied_since_last_success = r.DeniedSinceLastSuccess,", StringComparison.Ordinal);
        var finding = source.IndexOf("output_finding = r.OutputFinding,", StringComparison.Ordinal);
        var note = source.IndexOf("last_note = r.LastNote,", StringComparison.Ordinal);
        var rowProjectionEnds = source.IndexOf("var pressure = SweepPressureClassifier.Compute(", StringComparison.Ordinal);
        var windowNote = source.IndexOf("output_note = CollectorHealthClassifier.OutputWindowNote,", StringComparison.Ordinal);

        Assert.True(costEnd > costStart, "the get_collection_health row's field order moved - this pin needs re-anchoring");
        Assert.Contains("rows_stored = r.RowsStored,", source[costStart..costEnd], StringComparison.Ordinal);
        Assert.Contains("runs_with_rows = r.RunsWithRows,", source[costStart..costEnd], StringComparison.Ordinal);

        Assert.True(finding > denied, "the finding is emitted before the term that gives it meaning");
        Assert.True(finding < note, "the finding drifted out of the block it qualifies");

        Assert.True(rowProjectionEnds > note, "the row projection no longer ends where this pin expects");
        Assert.True(windowNote > rowProjectionEnds, "the window note moved inside the per-collector row");
    }

    /// <summary>
    /// The tool DESCRIPTION teaches the three readings on both SKUs, and the two descriptions are byte-
    /// identical by standing convention — a caller who learns on one SKU must not be taught something else
    /// on the other.
    /// </summary>
    [Fact]
    public void BothSkusToolDescriptions_TeachThatACorrectZeroExists()
    {
        foreach (var relative in new[]
        {
            Path.Combine("Lite", "Mcp", "McpHealthTools.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"),
        })
        {
            var source = File.ReadAllText(Path.Combine(RepoRoot(), relative));

            Assert.Contains("what it BOUGHT", source, StringComparison.Ordinal);
            Assert.Contains("that zero was CORRECT", source, StringComparison.Ordinal);
            Assert.Contains("deliberately NOT a band", source, StringComparison.Ordinal);
            Assert.Contains("needs a grant", source, StringComparison.Ordinal);
        }
    }

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
