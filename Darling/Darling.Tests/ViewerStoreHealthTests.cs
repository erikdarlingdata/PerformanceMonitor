/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
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
        Assert.Contains("1 rewrite candidate.", header);
        Assert.Contains("2 tables without a usable empty space estimate", header);
        Assert.Contains("2 " + DescribeReason(ReasonNeverAnalyzed), header);
    }

    [Fact]
    public void TheCountSentences_SayOneInTheSingular()
    {
        var one = new List<StoreTableHealthRow>
        {
            TableRow("a", candidate: true, pastFloors: true, tailOccupied: true),
        };
        var oneWithoutEstimate = new List<StoreTableHealthRow>
        {
            TableRow("b", emptyBytes: null, emptyShare: null, reason: ReasonNeverAnalyzed),
        };

        var header = StoreHealthRows.HeaderText(one);
        Assert.Contains("1 store table,", header);
        Assert.Contains("1 rewrite candidate.", header);
        Assert.Contains("0 tables without a usable empty space estimate", header);
        Assert.Contains("1 table without a usable empty space estimate", StoreHealthRows.HeaderText(oneWithoutEstimate));

        Assert.StartsWith("1 store index.", StoreHealthRows.IndexHeaderText(new[] { IndexRow("ix_only", 100) }));
        Assert.StartsWith("2 store indexes.", StoreHealthRows.IndexHeaderText(new[] { IndexRow("ix_one", 100), IndexRow("ix_two", 200) }));

        var measure = new StoreTableMeasure("collect.store_things", 1, 1, 0, 0.0, 1, 1.0, 0, 0, 0, 1, 0, 0, 0, 100, 8000, 0.9, 100, false,
            VerdictDense, "Dense.", 5);
        var text = StoreHealthRows.MeasureText("collect.store_things",
            new StoreTableMeasureResult(StoreTableMeasureOutcome.Measured, null, measure), TimeSpan.FromSeconds(1));
        Assert.Contains("1 row in 1 page.", text);
        var trailing = measure with { TotalPages = 3, TrailingEmptyPages = 1 };
        Assert.Contains("1 empty page sits at the end", StoreHealthRows.MeasureText("collect.store_things",
            new StoreTableMeasureResult(StoreTableMeasureOutcome.Measured, null, trailing), TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void ATableBehindOrCancelledRepeatedly_IsFlagged_WithEachFlagThatApplies()
    {
        var all = StoreHealthRows.Table(TableRow(candidate: true, pastFloors: true, tailOccupied: true)
            with { Behind = true, BehindHours = 30.4, AutovacuumCancels = 3, RelationsPastTrigger = 2 });
        Assert.Equal("Behind on autovacuum, Autovacuum cancelled repeatedly, Rewrite candidate", all.Flag);
        Assert.True(all.IsFlagged);
        Assert.True(all.IsBehind);
        Assert.True(all.IsCancelledRepeatedly);
        Assert.Equal("3", all.AutovacuumCancels);
        Assert.Equal(3, all.AutovacuumCancelsSort);
        Assert.Equal("2", all.PastTrigger);
        Assert.Equal(2, all.PastTriggerSort);

        var behindUnchecked = StoreHealthRows.Table(TableRow(pastFloors: true, tailOccupied: null) with { Behind = true, BehindHours = 25 });
        Assert.Equal("Behind on autovacuum, Mostly empty, end not checked", behindUnchecked.Flag);
        Assert.True(behindUnchecked.IsFlagged);

        var cancelled = StoreHealthRows.Table(TableRow() with { AutovacuumCancels = RepeatedCancelMinimum });
        Assert.Equal("Autovacuum cancelled repeatedly", cancelled.Flag);
        Assert.True(cancelled.IsFlagged);
        Assert.False(cancelled.IsBehind);

        var once = StoreHealthRows.Table(TableRow() with { AutovacuumCancels = 1, LastAutovacuumCancel = new DateTime(2026, 11, 15, 14, 0, 0, DateTimeKind.Utc) });
        Assert.Equal("", once.Flag);
        Assert.False(once.IsFlagged);
        Assert.False(once.IsCancelledRepeatedly);
        Assert.Equal("1", once.AutovacuumCancels);
        Assert.Equal(new DateTime(2026, 11, 15, 14, 0, 0, DateTimeKind.Utc), once.LastCancelUtc);
        Assert.NotEqual("", once.LastCancel);
        Assert.Equal("", StoreHealthRows.Table(TableRow()).LastCancel);
    }

    [Fact]
    public void TheBehindCell_SaysHowLongInHoursThenDays_AndSortsTheLongestFirst()
    {
        var notBehind = StoreHealthRows.Table(TableRow() with { RelationsPastTrigger = 1, BehindHours = 5 });
        Assert.Equal("No", notBehind.Behind);
        Assert.Equal(0, notBehind.BehindSort);

        var hours = StoreHealthRows.Table(TableRow() with { Behind = true, BehindHours = 30.4 });
        Assert.Equal("Behind, no vacuum in 30 h", hours.Behind);
        Assert.Equal(31.4, hours.BehindSort, 6);

        var days = StoreHealthRows.Table(TableRow() with { Behind = true, BehindHours = 100 });
        Assert.Equal("Behind, no vacuum in 4 days", days.Behind);
        Assert.True(days.BehindSort > hours.BehindSort);

        Assert.Equal("Behind", StoreHealthRows.BehindText(TableRow() with { Behind = true }));
        Assert.Equal("0 h", StoreHealthRows.HoursText(0.5));
        Assert.Equal("71 h", StoreHealthRows.HoursText(71.9));
        Assert.Equal("3 days", StoreHealthRows.HoursText(72));
    }

    [Fact]
    public void TheHeader_CountsTablesBehindAndTablesCancelledRepeatedly()
    {
        var rows = new List<StoreTableHealthRow>
        {
            TableRow("a") with { Behind = true, BehindHours = 30 },
            TableRow("b") with { Behind = true, BehindHours = 40, AutovacuumCancels = 2 },
            TableRow("c") with { AutovacuumCancels = 1 },
        };

        Assert.Contains(
            "3 store tables, " + PgDisplay.Bytes(6_000_000_000) + " in total. "
            + "2 tables behind on autovacuum (past the start point, no vacuum in 24 hours). "
            + "1 table with autovacuum cancelled 2 or more times in 24 hours. "
            + "0 rewrite candidates. ",
            StoreHealthRows.HeaderText(rows));
        Assert.Contains(
            "0 tables behind on autovacuum (past the start point, no vacuum in 24 hours). "
            + "0 tables with autovacuum cancelled 2 or more times in 24 hours. ",
            StoreHealthRows.HeaderText([TableRow("c") with { AutovacuumCancels = 1 }]));
    }

    [Fact]
    public void TheAutovacuumSettingsLine_GivesTheBudgetTheSettingsInForceAndTheCancels()
    {
        static string N1(double value) => value.ToString("N1", CultureInfo.CurrentCulture);

        /* PostgreSQL's defaults: three settings fall back to their plain twins, which the line names. */
        Assert.Equal(
            $"Autovacuum write budget {N1(39.0625)} MB/s: the most autovacuum may write per second. "
            + "Autovacuum is on: 1 of 3 workers running, it looks for work every 1 min, cost limit 200 (from vacuum_cost_limit), "
            + $"cost delay 2 ms, {N1(64)} MB of memory per worker (from maintenance_work_mem). "
            + "The store log has 4 cancelled autovacuums in the last 24 hours, 1 for a relation the store no longer has or one in another database.",
            StoreHealthRows.AutovacuumSettingsText(Settings(true, 3, 1, 60, -1, 200, 2, 0, -1, 65536, 20, 8192, 4, 1)));

        /* Its own values, no cost delay, and one cancel the store has a table for. */
        Assert.Equal(
            "Autovacuum write budget: unthrottled, the cost delay is 0, so vacuum does not sleep. "
            + "Autovacuum is off: 0 of 6 workers running, it looks for work every 2 h, cost limit 500, "
            + $"cost delay 0 ms, {N1(1)} GB of memory per worker. "
            + "The store log has 1 cancelled autovacuum in the last 24 hours.",
            StoreHealthRows.AutovacuumSettingsText(Settings(false, 6, 0, 7200, 500, 200, 0, 20, 1_048_576, 65536, 20, 8192, 1)));

        Assert.EndsWith(
            "it looks for work every 15 s, cost limit 200 (from vacuum_cost_limit), cost delay 20 ms (from vacuum_cost_delay), "
            + $"{N1(64)} MB of memory per worker (from maintenance_work_mem). The store log has no cancelled autovacuum in the last 24 hours.",
            StoreHealthRows.AutovacuumSettingsText(Settings(true, 3, 0, 15, -1, 200, -1, 20, -1, 65536, 20, 8192, 0)));
    }

    [Fact]
    public void AnIndexWithNoEstimate_ShowsAPlainSentence_NotCatalogWords()
    {
        const string collectorText = "the parent table has never been analyzed (reltuples = -1), so there is no row count to model from. "
            + "An ANALYZE of the parent makes this index estimable.";
        Assert.Equal("The table has never been analyzed, so no estimate is made.", PlainEstimateReason(collectorText));
        Assert.Equal(InvalidIndexReason, PlainEstimateReason(InvalidIndexReason));
        Assert.Equal("Some new reason this build does not know.", PlainEstimateReason("Some new reason this build does not know."));
        Assert.Null(PlainEstimateReason(null));
        foreach (var stored in new[]
        {
            "partial index: the model scales the parent reltuples, but the index holds only the rows matching its predicate.",
            "a key column is name-typed, whose pg_stats width is the padded 64 bytes.",
            "column widths are not visible for every key: pg_stats filters on has_column_privilege.",
            "the index occupies no pages yet, so there is nothing to be bloated.",
            "the model predicts more pages than the index occupies, which is negative bloat.",
        })
        {
            var plain = PlainEstimateReason(stored)!;
            Assert.DoesNotContain("reltuples", plain, StringComparison.Ordinal);
            Assert.DoesNotContain("pg_stats", plain, StringComparison.Ordinal);
            Assert.DoesNotContain("-typed", plain, StringComparison.Ordinal);
            Assert.NotEqual(stored, plain);
        }
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

    /// <summary>A rolled-up index row over 164 copies, with the larger-than-table count, the estimate coverage and the sizes set per case.</summary>
    private static StoreIndexHealthRow RolledUpRow(int copies, int larger, long bytes, long? reclaim, double? share, long? basis, int usable) =>
        IndexRow("ix_rolled", bytes, larger: larger > 0) with
        {
            IndexCount = copies,
            LargerThanHeapCount = larger,
            EstimatedReclaimableBytes = reclaim,
            EstimatedBloatShare = share,
            EstimatedBasisBytes = basis,
            EstimateIndexesUsable = usable,
            EstimateSkippedReason = reclaim is null ? "below_size_floor" : null,
        };

    [Fact]
    public void ARolledUpRow_SaysHowManyOfItsCopiesAreLargerThanTheirTable_EvenWhenItIsOne()
    {
        const long mb = 1024 * 1024;
        Assert.Equal("1 of 164 copies larger than its table", StoreHealthRows.Index(RolledUpRow(164, 1, 18 * mb, null, null, null, 0)).Flags);
        Assert.Equal("7 of 164 copies larger than its table", StoreHealthRows.Index(RolledUpRow(164, 7, 18 * mb, null, null, null, 0)).Flags);
        Assert.Equal("164 of 164 copies larger than its table", StoreHealthRows.Index(RolledUpRow(164, 164, 18 * mb, null, null, null, 0)).Flags);
        Assert.Equal("Larger than its table", StoreHealthRows.Index(RolledUpRow(1, 1, 18 * mb, null, null, null, 0)).Flags);
        Assert.Equal(string.Empty, StoreHealthRows.Index(RolledUpRow(164, 0, 18 * mb, null, null, null, 0)).Flags);
    }

    [Fact]
    public void TheEmptySpaceCell_NamesItsBasis_WhenTheEstimateCoveredOnlySomeOfTheRowsSize()
    {
        const long mb = 1024 * 1024;
        var partial = StoreHealthRows.Index(RolledUpRow(164, 1, 18 * mb, 6 * mb, 1.0, 6 * mb, 40)).Reclaimable;
        Assert.Equal("6.0 MB of the 6.0 MB estimated (100.0%)", partial.Replace(',', '.'));

        // A partial estimate that found nothing still names what it covered.
        var none = StoreHealthRows.Index(RolledUpRow(164, 0, 18 * mb, 0, 0.0, 6 * mb, 40)).Reclaimable;
        Assert.Equal("0 B of the 6.0 MB estimated (0.0%)", none.Replace(',', '.'));
    }

    [Fact]
    public void TheEmptySpaceCell_KeepsItsPlainText_WhenEveryCopyWasEstimated_OrThereIsOneCopy()
    {
        const long mb = 1024 * 1024;
        // Every copy has an estimate. A live read sizes the estimate and the row in one statement, so the basis equals the row's size and is not named.
        var all = StoreHealthRows.Index(RolledUpRow(164, 0, 18 * mb, 4 * mb, 0.25, 18 * mb, 164)).Reclaimable;
        Assert.Equal("4.0 MB (25.0%)", all.Replace(',', '.'));

        var single = StoreHealthRows.Index(RolledUpRow(1, 0, 18 * mb, 4 * mb, 0.25, 18 * mb, 1)).Reclaimable;
        Assert.Equal("4.0 MB (25.0%)", single.Replace(',', '.'));

        // An estimate from a read that predates the basis column keeps the plain text.
        var unknown = StoreHealthRows.Index(RolledUpRow(164, 0, 18 * mb, 4 * mb, 0.25, null, 40)).Reclaimable;
        Assert.Equal("4.0 MB (25.0%)", unknown.Replace(',', '.'));
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
        var measure = new StoreTableMeasure("collect.store_things", 1000, 100, 900, 0.9, 5000, 50.0, 99, 999, 900, 0, 0, 0, 100, 1_000_000, 7_000_000, 0.9, 100, false,
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
        var tablesRead = await viewer.GetStoreTableHealthAsync(cancellationToken: ct);
        var tables = tablesRead.Rows;
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

        var indexes = await viewer.GetStoreIndexHealthAsync(cancellationToken: ct);
        Assert.NotEmpty(StoreHealthRows.Indexes(indexes.Indexes));
        StoreHealthRows.CoverageRows(indexes.Coverage);

        /* #8924: a repeat is the held read itself (the same list, the same read time), so the tab's refresh timer sends the store
           neither statement. The Refresh button's read is a new one. */
        var tablesAgain = await viewer.GetStoreTableHealthAsync(cancellationToken: ct);
        Assert.Same(tablesRead.Rows, tablesAgain.Rows);
        Assert.Equal(tablesRead.ReadAtUtc, tablesAgain.ReadAtUtc);
        var indexesAgain = await viewer.GetStoreIndexHealthAsync(cancellationToken: ct);
        Assert.Same(indexes.Indexes, indexesAgain.Indexes);
        Assert.Same(indexes.Coverage, indexesAgain.Coverage);
        Assert.NotNull(indexes.ReadAtUtc);
        Assert.Equal(indexes.ReadAtUtc, indexesAgain.ReadAtUtc);

        var tablesFresh = await viewer.GetStoreTableHealthAsync(fresh: true, ct);
        Assert.NotSame(tablesRead.Rows, tablesFresh.Rows);
        Assert.Equal(tablesRead.Rows.Count, tablesFresh.Rows.Count);
        var indexesFresh = await viewer.GetStoreIndexHealthAsync(fresh: true, ct);
        Assert.NotSame(indexes.Indexes, indexesFresh.Indexes);
        Assert.NotSame(indexes.Coverage, indexesFresh.Coverage);

        var readiness = await viewer.GetStoreMaintenanceReadinessAsync(ct);
        Assert.False(string.IsNullOrWhiteSpace(StoreHealthRows.ReadinessText(readiness)));

        /* Measure sends the selected plain table's name and the reader measures that table. */
        var selected = rows.First(r => r.MeasureTarget is not null);
        var result = await viewer.MeasureStoreTableAsync(selected.MeasureTarget!, ct);
        Assert.Equal(StoreTableMeasureOutcome.Measured, result.Outcome);
        Assert.Contains(selected.MeasureTarget!.Split('.')[1], result.Measure!.QualifiedName);
        Assert.Contains(selected.MeasureTarget, StoreHealthRows.MeasureText(selected.MeasureTarget, result, TimeSpan.FromSeconds(1)));
    }

    /* #9364: the slow-load line's note on a tables or indexes read. The call began 5 s before "now", so a read stamped within the last 5 s was
       taken for it, and an older stamp is a held measurement whose age is counted to "now". */
    [Theory]
    [InlineData(4812, 0, "sized 4,812 relations, measured live")]
    [InlineData(1, 5, "sized 1 relation, measured live")]
    [InlineData(0, 3, "sized 0 relations, measured live")]
    [InlineData(4812, 42, "sized 4,812 relations, from the held measurement taken 42 s ago")]
    [InlineData(4812, 60, "sized 4,812 relations, from the held measurement taken 1 min 0 s ago")]
    [InlineData(2, 251, "sized 2 relations, from the held measurement taken 4 min 11 s ago")]
    public void TheReadNote_SaysHowManyRelationsWereSized_AndWhetherTheyWereMeasuredLiveOrHeld(int relations, int readSecondsAgo, string expected)
    {
        var now = new DateTime(2026, 11, 23, 10, 0, 0, DateTimeKind.Utc);

        Assert.Equal(expected, StoreHealthRows.ReadNote(relations, now.AddSeconds(-readSecondsAgo), now.AddSeconds(-5), now));
    }

    [Fact]
    public void ASlowLoadLine_ShowsTheReadNoteAfterTheReadTime_AndLeavesAReadWithNoNoteAsItWas()
    {
        var timer = new ViewerLoadTimer(0);
        timer.Track("store tables read", Task.FromResult(1));
        timer.Track("store maintenance check", Task.FromResult(2));
        var now = new DateTime(2026, 11, 23, 10, 0, 0, DateTimeKind.Utc);
        timer.NotePhase("store tables read", StoreHealthRows.ReadNote(4812, now, now.AddSeconds(-5), now));

        var line = timer.Finish("Store Health");

        Assert.NotNull(line);
        Assert.Matches(@"store tables read \d+ ms \(sized 4,812 relations, measured live\)", line);
        Assert.Matches(@"store maintenance check \d+ ms(?! \()", line);
    }

    /// <summary>#9374: a read of the store's last measured sizes that fails for any reason but the caller's cancel answers null, so the tab reads
    /// live as it did before. A store with no last sizes answers null too, and a cancel still cancels.</summary>
    [Fact]
    public async Task AFailedLastSizesRead_AnswersNull_SoTheTabReadsLive_AndACancelStillCancels()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=never_opened;Timeout=2");
        var now = DateTime.UtcNow;

        Assert.Null(await ViewerDataService.ReadLastSizesHealthAsync(
            store, (_, _) => throw new InvalidOperationException("the sizes table is unreadable"), now, ct));
        Assert.Null(await ViewerDataService.ReadLastSizesHealthAsync(
            store, (_, _) => Task.FromResult<MeasuredSizePass?>(null), now, ct));

        // The sizes read worked but the reads from it failed (no store behind the data source): still null.
        var pass = new MeasuredSizePass(new RelationSizePass([1], [1], [8192], [0], [0]), now);
        Assert.Null(await ViewerDataService.ReadLastSizesHealthAsync(store, (_, _) => Task.FromResult<MeasuredSizePass?>(pass), now, ct));

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ViewerDataService.ReadLastSizesHealthAsync(
            store, (_, token) => Task.FromCanceled<MeasuredSizePass?>(token), now, cancelled.Token));
    }
}
