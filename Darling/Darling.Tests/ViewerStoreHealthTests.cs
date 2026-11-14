/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static PerformanceMonitor.Darling.Storage.DarlingStoreTableHealthReader;
using static PerformanceMonitor.Darling.Storage.DarlingStoreTableMeasureReader;

namespace Darling.Tests;

/* #1776 own-store: the live test mints its own scratch database through ScratchPostgres, which drops it; nothing is planted in a shared store. */

/// <summary>
/// #8787: the Viewer's Store Health tab. The rows it shows come from the Storage readers through <see cref="StoreHealthRows"/>: a missing
/// estimate shows the reason phrase and never 0, a rewrite candidate is flagged, Measure targets only a plain table and sends the
/// selected table's name to the reader, and the index rows list the flagged ones first. The live test reads a real store through the
/// Viewer's own data service and measures a table picked from the rows.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerStoreHealthTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static StoreTableHealthRow TableRow(
        string name = "store_things", string kind = KindTable, long totalBytes = 2_000_000_000, long? emptyBytes = 1_500_000_000,
        double? emptyShare = 0.8, string? reason = null, bool candidate = false, bool pastFloors = false, bool? tailOccupied = null) =>
        new(
            "collect", name, kind, totalBytes, totalBytes - 100, 60, 40, 1234, 0, 0, emptyBytes, emptyShare, 1, emptyBytes is null ? 0 : 1, reason,
            500, 100, null, null, null, null, false, null, candidate, 10, 5, null, 50.0, 1000.0, 0, null, 0, 0, 100, 10, 0, 0, null, 0, 0, 0,
            null, StatsSourceReset, 24, pastFloors, 0, 0, 0, tailOccupied);

    private static StoreIndexHealthRow IndexRow(string index, long bytes, bool valid = true, bool larger = false, bool rarely = false) =>
        new("collect", "store_things", KindTable, index, false, 1, 0, bytes, 1000, 0, 0.5, false, false, valid, larger, larger ? 1 : 0, rarely,
            null, StatsSourceReset, null, null, 0, "below_size_floor");

    [Fact]
    public void ATableWithNoEstimate_ShowsTheReasonPhrase_NeverZero()
    {
        foreach (var reason in new[] { ReasonNeverAnalyzed, ReasonStaleStatistics, ReasonBelowSizeFloor, ReasonNoChunks })
        {
            var row = StoreHealthRows.Table(TableRow(emptyBytes: null, emptyShare: null, reason: reason));

            Assert.Equal(DescribeReason(reason), row.EmptySpace);
            Assert.DoesNotContain("0 B", row.EmptySpace);
            Assert.Null(row.EmptySpaceSort);
        }

        var estimated = StoreHealthRows.Table(TableRow());
        Assert.Contains("(80.0%)", estimated.EmptySpace.Replace(',', '.'));
        Assert.Equal(0.8, estimated.EmptySpaceSort);
    }

    [Fact]
    public void ARewriteCandidate_IsFlagged_AndATableWhoseEndWasNotChecked_SaysSo()
    {
        var candidate = StoreHealthRows.Table(TableRow(candidate: true, pastFloors: true, tailOccupied: true));
        Assert.True(candidate.IsFlagged);
        Assert.Equal("Rewrite candidate", candidate.Flag);
        Assert.StartsWith("No", candidate.VacuumCanReturn);

        var unread = StoreHealthRows.Table(TableRow(pastFloors: true, tailOccupied: null));
        Assert.False(unread.IsFlagged);
        Assert.Contains("not checked", unread.Flag);
        Assert.Equal("Not checked", unread.VacuumCanReturn);

        var truncatable = StoreHealthRows.Table(TableRow(pastFloors: true, tailOccupied: false));
        Assert.Equal("", truncatable.Flag);
        Assert.Equal("Yes", truncatable.VacuumCanReturn);
    }

    [Fact]
    public void Measure_TargetsOnlyAPlainTable_AndSendsTheSelectedTablesName()
    {
        Assert.Equal("collect.store_things", StoreHealthRows.Table(TableRow()).MeasureTarget);
        Assert.Null(StoreHealthRows.Table(TableRow(kind: KindHypertable)).MeasureTarget);
        Assert.Null(StoreHealthRows.Table(TableRow(kind: KindPartitioned)).MeasureTarget);
        Assert.Null(StoreHealthRows.Table(TableRow(kind: KindContinuousAggregate)).MeasureTarget);
    }

    [Fact]
    public void TheHeader_CountsTablesCandidatesAndMissingEstimates_WithTheReasonPhrases()
    {
        var rows = new List<StoreTableHealthRow>
        {
            TableRow("a", candidate: true, pastFloors: true, tailOccupied: true),
            TableRow("b", emptyBytes: null, emptyShare: null, reason: ReasonNeverAnalyzed),
            TableRow("c", emptyBytes: null, emptyShare: null, reason: ReasonNeverAnalyzed),
        };

        var header = StoreHealthRows.HeaderText(rows);

        Assert.Contains("3 store tables", header);
        Assert.Contains("1 rewrite candidates", header);
        Assert.Contains("2 tables without a usable empty space estimate", header);
        Assert.Contains("2 " + DescribeReason(ReasonNeverAnalyzed), header);
    }

    [Fact]
    public void TheIndexRows_ListFlaggedIndexesFirst_ThenBySize()
    {
        var rows = StoreHealthRows.Indexes(new[]
        {
            IndexRow("ix_big_fine", 9_000_000),
            IndexRow("ix_small_rare", 100, rarely: true),
            IndexRow("ix_mid_invalid", 5_000, valid: false),
            IndexRow("ix_small_fine", 200),
        });

        Assert.Equal(new[] { "ix_mid_invalid", "ix_small_rare", "ix_big_fine", "ix_small_fine" }, rows.Select(r => r.Index));
        Assert.Equal("Invalid", rows[0].Flags);
        Assert.True(rows[1].IsFlagged);
        Assert.False(rows[2].IsFlagged);
    }

    [Fact]
    public void ACoverageRowWithAMissingLeaf_IsFlaggedAndNamesIt()
    {
        var complete = new StoreIndexCoverageRow("collect", "t", "ix_ok", true, 4, 4, 0, Array.Empty<string>(), 0, Array.Empty<string>());
        var missing = new StoreIndexCoverageRow("collect", "t", "ix_gap", true, 4, 3, 1, new[] { "collect.t_p4" }, 0, Array.Empty<string>());

        var rows = StoreHealthRows.CoverageRows(new[] { complete, missing });

        Assert.Equal("ix_gap", rows[0].Index);
        Assert.True(rows[0].IsFlagged);
        Assert.Contains("collect.t_p4", rows[0].Detail);
        Assert.False(rows[1].IsFlagged);
    }

    [Fact]
    public void TheMeasureText_ShowsTheVerdictAndTheElapsedTime_AndARefusalKeepsItsSentence()
    {
        var measure = new StoreTableMeasure("collect.store_things", 1000, 100, 900, 0.9, 5000, 50.0, 99, 999, 900, 0, 0, 0, 100, false,
            VerdictVacuumCanTruncate, "VACUUM can give the space back.", 1400);
        var measured = StoreHealthRows.MeasureText("collect.store_things",
            new StoreTableMeasureResult(StoreTableMeasureOutcome.Measured, null, measure), TimeSpan.FromSeconds(1.4));
        Assert.Contains("VACUUM can give the space back.", measured);
        Assert.Contains("5,000 rows in 1,000 pages", measured);
        Assert.Contains("900 empty pages sit at the end", measured);
        Assert.Contains("1.4 s", measured.Replace(',', '.'));

        var refused = StoreHealthRows.MeasureText("collect.store_things",
            new StoreTableMeasureResult(StoreTableMeasureOutcome.Refused, "That table is not one Measure can read.", null), TimeSpan.FromMilliseconds(40));
        Assert.Contains("That table is not one Measure can read.", refused);
    }

    [Fact]
    public async Task TheViewersStoreHealthReads_FeedTheTabsRows_AndMeasureReadsTheSelectedTable_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Viewer Store Health test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, null, ct);
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        /* The two halves are separate reads, and the tables read alone returns the grid's rows. */
        var tables = await viewer.GetStoreTableHealthAsync(ct);
        var rows = StoreHealthRows.Tables(tables);
        Assert.NotEmpty(rows);
        Assert.Equal(tables.Count, rows.Count);

        /* A table nobody has analyzed has no usable estimate: its cell holds the reason phrase, not a zero. */
        foreach (var (source, shown) in tables.Zip(rows))
        {
            if (source.EstimatedEmptyBytes is null)
            {
                Assert.Equal(DescribeReason(source.EstimateUnavailableReason ?? "no estimate"), shown.EmptySpace);
            }
        }

        var indexes = await viewer.GetStoreIndexHealthAsync(ct);
        Assert.NotEmpty(StoreHealthRows.Indexes(indexes.Indexes));
        StoreHealthRows.CoverageRows(indexes.Coverage);

        var readiness = await viewer.GetStoreMaintenanceReadinessAsync(ct);
        Assert.False(string.IsNullOrWhiteSpace(StoreHealthRows.ReadinessText(readiness)));

        /* Measure sends the selected plain table's name and the reader measures that table. */
        var selected = rows.First(r => r.MeasureTarget is not null);
        var result = await viewer.MeasureStoreTableAsync(selected.MeasureTarget!, ct);
        Assert.Equal(StoreTableMeasureOutcome.Measured, result.Outcome);
        Assert.Contains(selected.MeasureTarget!.Split('.')[1], result.Measure!.QualifiedName);
        Assert.Contains(selected.MeasureTarget, StoreHealthRows.MeasureText(selected.MeasureTarget, result, TimeSpan.FromSeconds(1)));
    }
}
