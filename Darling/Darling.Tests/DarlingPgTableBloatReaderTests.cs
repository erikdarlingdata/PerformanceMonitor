/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the per-table bloat read (#2542). One rule governs the whole surface: never present an estimate as
/// a measurement. Every test here is that rule in a different place.
/// </summary>
public class DarlingPgTableBloatReaderTests
{
    private static string Sql => DarlingPgTableBloatReader.PgTableBloatSql;

    /// <summary>
    /// The shipped SQL aligns its join columns, so a fragment typed with single spaces will not be found in
    /// it. Assertions about STRUCTURE run against this; assertions about exact rendering keep using
    /// <see cref="Sql"/>.
    /// </summary>
    private static string Squeezed =>
        System.Text.RegularExpressions.Regex.Replace(DarlingPgTableBloatReader.PgTableBloatSql, @"\s+", " ");

    private static string ProbeSql => DarlingPgTableBloatReader.PgTableBloatProbeSql;

    // ── Scoping ──────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void ScopesToOneServerAndOneWindow()
    {
        Assert.Contains("server_id = $1", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", Sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT 50", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The series identity is (database, schema, table). <c>IS NOT DISTINCT FROM</c> on every join column,
    /// because <c>database_name</c> is nullable in the store and an ordinary <c>=</c> would drop those rows
    /// rather than matching them.
    /// </summary>
    [Fact]
    public void KeysTheSeriesOnDatabaseSchemaAndTable()
    {
        Assert.Contains("DISTINCT ON (database_name, schema_name, table_name)", Squeezed, StringComparison.Ordinal);

        foreach (var column in new[] { "database_name", "schema_name", "table_name" })
        {
            Assert.Contains($"e.{column} IS NOT DISTINCT FROM l.{column}", Squeezed, StringComparison.Ordinal);
        }
    }

    // ── The estimate is never filtered away, and never ranked ahead of what is known ─────────────

    /// <summary>
    /// A row whose estimate cannot be trusted is SORTED LAST, not filtered out. Filtering would silently
    /// drop the rows a reader most needs to see — a table whose estimate is unusable is still a table
    /// somebody asked about — while letting an unusable estimate RANK would put the least-known tables at
    /// the top of a list that reads as a work queue.
    ///
    /// <para>The assertion is on the compound sort EXPRESSION rather than on a single column, and that
    /// shape is the fix for a review finding: the first draft ordered on <c>estimate_unavailable</c>
    /// alone, which left the other two suppression reasons free to rank. <see cref="TheSortKeyCoversEverySuppressionReason"/>
    /// pins the conditions themselves; this one pins that the tier exists, sorts ascending, and is not a
    /// filter.</para>
    /// </summary>
    [Fact]
    public void SortsUntrustworthyEstimatesLastRatherThanRemovingThem()
    {
        var order = Squeezed[Squeezed.LastIndexOf("ORDER BY", StringComparison.Ordinal)..];

        Assert.Contains("l.estimate_unavailable", order, StringComparison.Ordinal);
        /* The whole compound expression is the first sort key and it sorts ASCENDING, so false (publishable)
           comes first and every suppressed row lands below every trustworthy one. */
        Assert.Contains(") ASC", order, StringComparison.Ordinal);
        Assert.True(
            order.IndexOf(") ASC", StringComparison.Ordinal)
            < order.IndexOf("l.bloat_bytes_estimate DESC", StringComparison.Ordinal),
            "the trustworthiness tier must outrank the size ordering, or a suppressed row with a large "
            + "estimate still reaches the top of the LIMIT");

        /* No WHERE on the flag anywhere: the decision belongs to the tool, which suppresses the NUMBER
           rather than the ROW. */
        Assert.DoesNotContain("WHERE l.estimate_unavailable", Squeezed, StringComparison.Ordinal);
        Assert.DoesNotContain("AND NOT estimate_unavailable", Squeezed, StringComparison.Ordinal);
    }

    /// <summary>
    /// The trend is carried on the MEASURED size as well as on the estimate, so it survives a suppressed
    /// estimate. A spot bloat percentage is what gets someone to run VACUUM FULL on a Tuesday; whether the
    /// waste is accumulating, holding, or already being reclaimed is the part that decides anything.
    /// </summary>
    [Fact]
    public void CarriesTheTrendOnTheMeasuredSizeToo()
    {
        Assert.Contains("heap_bytes AS first_heap_bytes", Squeezed, StringComparison.Ordinal);
        Assert.Contains("bloat_bytes_estimate AS first_bloat_bytes_estimate", Squeezed, StringComparison.Ordinal);
        Assert.Contains("e.first_seen_at", Sql, StringComparison.Ordinal);
        Assert.Contains("s.sample_count", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The trust signals reach the projection whether or not they fire, so a reader can see WHY an estimate
    /// was or was not published rather than taking it on faith.
    /// </summary>
    [Theory]
    [InlineData("l.estimate_unavailable")]
    [InlineData("l.mods_since_analyze")]
    [InlineData("l.last_analyzed")]
    [InlineData("l.live_tuples")]
    [InlineData("l.dead_tuples")]
    [InlineData("l.fillfactor")]
    [InlineData("l.alignment_bytes")]
    [InlineData("l.pgstattuple_available")]
    [InlineData("l.estimated_tuple_bytes")]
    [InlineData("l.estimated_heap_pages")]
    public void CarriesTheEstimatorsOwnInputs(string column)
    {
        Assert.Contains(column, Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The measured sizes travel too, and they are the columns a reader can trust unconditionally: they are
    /// filesystem readings rather than arithmetic.
    /// </summary>
    [Theory]
    [InlineData("l.heap_bytes")]
    [InlineData("l.heap_pages")]
    [InlineData("l.toast_bytes")]
    [InlineData("l.index_bytes")]
    public void CarriesTheMeasuredSizes(string column)
    {
        Assert.Contains(column, Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeCountsSnapshotsAsWellAsRows()
    {
        Assert.Contains("FROM pg_table_bloat_stats", ProbeSql, StringComparison.Ordinal);
        Assert.Contains("count(DISTINCT collection_time)", ProbeSql, StringComparison.Ordinal);
        Assert.Contains("AS rows_ever", ProbeSql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT", ProbeSql, StringComparison.Ordinal);
    }

    // ── Suppression: the whole point ─────────────────────────────────────────────────────────────

    private static DarlingPgTableBloatReader.PgTableBloatRow Row(
        long liveTuples = 150_000,
        long deadTuples = 500,
        long modsSinceAnalyze = 0,
        bool estimateUnavailable = false,
        bool pgstattupleAvailable = true,
        decimal bloatPct = 75.14m) =>
        new(
            DatabaseName: "appdb",
            SchemaName: "public",
            TableName: "widget",
            MeasuredAt: DateTime.UtcNow,
            HeapBytes: 56_516_608,
            HeapPages: 6_899,
            ToastBytes: 0,
            IndexBytes: 1_000,
            LiveTuples: liveTuples,
            DeadTuples: deadTuples,
            ModsSinceAnalyze: modsSinceAnalyze,
            LastAnalyzed: DateTime.UtcNow,
            EstimatedTupleBytes: 436,
            EstimatedHeapPages: 1_715,
            FillFactor: 100,
            BloatBytesEstimate: 42_467_328,
            BloatPctEstimate: bloatPct,
            EstimateUnavailable: estimateUnavailable,
            AlignmentBytes: 8,
            PgstattupleAvailable: pgstattupleAvailable,
            FirstHeapBytes: 50_000_000,
            FirstBloatBytesEstimate: 30_000_000,
            FirstSeenAt: DateTime.UtcNow.AddDays(-7),
            SampleCount: 2);

    /// <summary>Current statistics and a real bloat figure: the estimate is published.</summary>
    [Fact]
    public void PublishesTheEstimateWhenTheStatisticsAreCurrent()
    {
        Assert.Null(DarlingMcpPgTableBloatTools.EstimateSuppressionReason(Row()));
    }

    /// <summary>
    /// STALE column-width statistics suppress the estimate. This is the measured failure the whole guard
    /// exists for: two byte-identical 8,998-page tables with a true bloat of 10.93% estimated 92.64% and
    /// 11.01%, differing only in whether ANALYZE had run after a widening UPDATE — 81 percentage points,
    /// with nothing in the arithmetic to show it. The stale table had modified 200% of its rows since its
    /// last analyze and the fresh one 0%, which is why that ratio is the signal.
    /// </summary>
    [Fact]
    public void SuppressesTheEstimateWhenTheStatisticsAreStale()
    {
        var reason = DarlingMcpPgTableBloatTools.EstimateSuppressionReason(
            Row(liveTuples: 150_000, modsSinceAnalyze: 300_000));

        Assert.NotNull(reason);
        Assert.Contains("STALE", reason, StringComparison.Ordinal);
        Assert.Contains("ANALYZE", reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// The threshold is a boundary rather than a mood: just under it publishes, just over it suppresses.
    /// A fifth of the table, chosen low because the failure is asymmetric — a needlessly cautious estimate
    /// costs a second look, an over-trusted one costs a table rewrite.
    /// </summary>
    [Fact]
    public void TheStalenessThresholdIsABoundary()
    {
        Assert.Equal(0.2, DarlingMcpPgTableBloatTools.StaleStatisticsChurnRatio);

        Assert.Null(DarlingMcpPgTableBloatTools.EstimateSuppressionReason(
            Row(liveTuples: 100_000, modsSinceAnalyze: 19_999)));

        Assert.NotNull(DarlingMcpPgTableBloatTools.EstimateSuppressionReason(
            Row(liveTuples: 100_000, modsSinceAnalyze: 20_001)));
    }

    /// <summary>
    /// No column statistics at all suppresses the estimate, and the reason names the GRANT — because the
    /// usual cause is not a missing ANALYZE but permissions. <c>pg_stats</c> is filtered by SELECT privilege
    /// and <c>pg_monitor</c> does not confer it on user tables.
    ///
    /// <para>Measured against a <c>pg_monitor</c>-only role on a live target: zero <c>pg_stats</c> rows
    /// visible, and the estimator did not fail — it returned 88.59% for a table whose true bloat is 0.50%,
    /// 95.03% for one that is really 74.82%, and 22.57% for one that is really 0.46%. Every row an argument
    /// for rewriting a production table. Adding <c>pg_read_all_data</c> restored byte-identical agreement
    /// with the superuser's numbers.</para>
    /// </summary>
    [Fact]
    public void SuppressesTheEstimateWhenThereAreNoColumnStatistics_AndNamesTheGrant()
    {
        var reason = DarlingMcpPgTableBloatTools.EstimateSuppressionReason(Row(estimateUnavailable: true));

        Assert.NotNull(reason);
        Assert.Contains("SUPPRESSED", reason, StringComparison.Ordinal);
        Assert.Contains("pg_monitor", reason, StringComparison.Ordinal);
        Assert.Contains("pg_read_all_data", reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// A never-analyzed table suppresses the estimate. PostgreSQL 14+ reports <c>reltuples = -1</c> as an
    /// explicit "unknown" distinct from "empty", and the reader preserves the -1 rather than flooring it to
    /// zero precisely so this check has something to key on.
    /// </summary>
    [Fact]
    public void SuppressesTheEstimateForANeverAnalyzedTable()
    {
        var reason = DarlingMcpPgTableBloatTools.EstimateSuppressionReason(Row(liveTuples: -1));

        Assert.NotNull(reason);
        Assert.Contains("never been analyzed", reason, StringComparison.Ordinal);
    }

    // ── The always-available fallback ────────────────────────────────────────────────────────────

    /// <summary>
    /// The dead-tuple fraction needs no width model and no SELECT on the table, so it survives every
    /// suppression above — a permissions gap degrades the answer instead of removing it. It says it is
    /// MEASURED, and it says what it does NOT cover.
    /// </summary>
    [Fact]
    public void TheDeadTupleFallbackIsMeasuredAndSaysWhatItMisses()
    {
        var finding = DarlingMcpPgTableBloatTools.DeadTupleFinding(150_000, 50_000);

        Assert.Contains("25%", finding, StringComparison.Ordinal);
        Assert.Contains("MEASURED", finding, StringComparison.Ordinal);
        Assert.Contains("NARROWER", finding, StringComparison.Ordinal);
    }

    /// <summary>A never-analyzed table has no row count to compare dead tuples against, and the fallback
    /// says so rather than dividing by a negative.</summary>
    [Fact]
    public void TheDeadTupleFallbackRefusesANeverAnalyzedTable()
    {
        Assert.Contains(
            "never been analyzed",
            DarlingMcpPgTableBloatTools.DeadTupleFinding(-1, 0),
            StringComparison.Ordinal);
    }

    // ── Remedies ─────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A high estimate leads with CONFIRM, not with a remedy, and names <c>pgstattuple</c> before it names
    /// <c>VACUUM FULL</c>. VACUUM FULL holds an ACCESS EXCLUSIVE lock for the whole rewrite and needs free
    /// disk equal to the finished table, so it is the last option rather than the first — and it must never
    /// be reached for on the strength of a number that has not been checked.
    /// </summary>
    [Fact]
    public void AHighEstimateLeadsWithConfirmationRatherThanARemedy()
    {
        var finding = DarlingMcpPgTableBloatTools.BloatFinding(85m, 60_000_000, true, "public.widget");

        Assert.Contains("CONFIRM IT BEFORE", finding, StringComparison.Ordinal);
        Assert.Contains("ACCESS EXCLUSIVE", finding, StringComparison.Ordinal);
        Assert.True(
            finding.IndexOf("pgstattuple", StringComparison.Ordinal)
            < finding.IndexOf("VACUUM FULL", StringComparison.Ordinal),
            "the confirmation step must be named before the destructive remedy");
    }

    /// <summary>
    /// The escalation command is written out, and it is the RIGHT command for this database: suggesting
    /// pgstattuple where the extension is not installed produces "function does not exist", which reads as
    /// a broken tool rather than as a missing extension.
    /// </summary>
    [Fact]
    public void NamesTheExactEscalationCommandForThisDatabase()
    {
        Assert.Contains(
            "SELECT * FROM pgstattuple('public.widget')",
            DarlingMcpPgTableBloatTools.BloatFinding(85m, 60_000_000, true, "public.widget"),
            StringComparison.Ordinal);

        Assert.Contains(
            "CREATE EXTENSION pgstattuple",
            DarlingMcpPgTableBloatTools.BloatFinding(85m, 60_000_000, false, "public.widget"),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// A moderate estimate sends the reader at the CAUSE first. Reclaiming space while autovacuum is being
    /// blocked buys time until the same thing happens again, so the finding names the two reads that say
    /// which blocker it is.
    /// </summary>
    [Fact]
    public void AModerateEstimateSendsTheReaderAtTheCauseFirst()
    {
        var finding = DarlingMcpPgTableBloatTools.BloatFinding(35m, 10_000_000, true, "public.widget");

        Assert.Contains("get_pg_autovacuum_health", finding, StringComparison.Ordinal);
        Assert.Contains("get_pg_xmin_horizon", finding, StringComparison.Ordinal);
    }

    /// <summary>
    /// A low estimate says the error bar is a meaningful share of the number rather than reporting a
    /// precise-looking small figure. Measured: the estimator's error was under about 2 percentage points
    /// with current statistics, which at a true 0.50% is a large RELATIVE error.
    /// </summary>
    [Fact]
    public void ALowEstimateAdmitsItsOwnErrorBar()
    {
        Assert.Contains(
            "error bar",
            DarlingMcpPgTableBloatTools.BloatFinding(5m, 100_000, true, "public.widget"),
            StringComparison.Ordinal);
    }

    // ── One rule, two surfaces ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// The MCP read and the WPF grid reach the SAME decision on every row, because they make the same
    /// call rather than each carrying a copy of the rule.
    ///
    /// <para>This is the invariant, not the instance. The first draft had the threshold written as a
    /// literal in both projects: matching, correct, and free to drift apart at the next edit — silently,
    /// and in the direction where one surface publishes a figure the other had already withheld. Asserting
    /// the two agree on a handful of examples would not have caught that either, since both copies were
    /// right on the day. What makes this hold is that the rule moved to
    /// <see cref="DarlingPgTableBloatReader.EstimateIsUnpublishable"/>, which both now call.</para>
    ///
    /// <para>The cases sweep BOTH sides of the churn threshold and every flag combination, so a change to
    /// the rule that moved only one consumer would be caught wherever it moved it.</para>
    /// </summary>
    [Fact]
    public void TheMcpReadAndTheViewerGridSuppressExactlyTheSameRows()
    {
        var cases = new[]
        {
            Row(),
            Row(liveTuples: 150_000, modsSinceAnalyze: 300_000),
            Row(liveTuples: 100_000, modsSinceAnalyze: 19_999),
            Row(liveTuples: 100_000, modsSinceAnalyze: 20_001),
            Row(estimateUnavailable: true),
            Row(liveTuples: -1),
            Row(liveTuples: 0, modsSinceAnalyze: 5_000),
            Row(liveTuples: -1, modsSinceAnalyze: 999_999, estimateUnavailable: true),
        };

        foreach (var row in cases)
        {
            var mcpSuppressed = DarlingMcpPgTableBloatTools.EstimateSuppressionReason(row) != null;
            var shared = DarlingPgTableBloatReader.EstimateIsUnpublishable(row);

            Assert.Equal(shared, mcpSuppressed);

            /* And the grid renders the dash exactly when the rule says to. PgDisplay lives in the viewer
               project, which Darling.Tests also references, so the real projection is what is checked here
               rather than a re-implementation of it. */
            var projected = PerformanceMonitor.Darling.Viewer.PgDisplay.TableBloat(row);
            Assert.Equal(shared, projected.EstimateSuppressed);

            if (shared)
            {
                Assert.Equal(PerformanceMonitor.Darling.Viewer.PgDisplay.NotApplicableText, projected.BloatPctEstimate);
                Assert.Equal(PerformanceMonitor.Darling.Viewer.PgDisplay.NotApplicableText, projected.BloatEstimate);
                /* A suppressed row can never be painted as a high-bloat one: that colour asserts exactly
                   what the suppression denies. */
                Assert.False(projected.IsHighBloat);
            }

            /* The measured fallback survives suppression in every case — that is what makes a permissions
               gap degrade the answer instead of removing it. */
            Assert.NotEmpty(DarlingMcpPgTableBloatTools.DeadTupleFinding(row.LiveTuples, row.DeadTuples));
        }
    }

    /// <summary>
    /// The threshold the tool re-exports is the one the shared rule uses. A second literal here would be
    /// the drift this whole arrangement exists to prevent.
    /// </summary>
    [Fact]
    public void TheToolsThresholdIsTheSharedOne()
    {
        Assert.Equal(
            DarlingPgTableBloatReader.StaleStatisticsChurnRatio,
            DarlingMcpPgTableBloatTools.StaleStatisticsChurnRatio);
    }

    // ── The zero-live-tuples gap (review finding) ────────────────────────────────────────────────

    /// <summary>
    /// A table analyzed while EMPTY and then written to must not publish an estimate. <c>live_tuples = 0</c>
    /// is a real state distinct from the <c>-1</c> never-analyzed sentinel — it is what a table analyzed
    /// just after a TRUNCATE or bulk delete records — and the first draft's <c>&gt; 0</c> guard
    /// short-circuited the whole staleness clause there, so a row anchored to a stale zero row count
    /// published with no suppression signal at all.
    ///
    /// <para>That is the false-confidence failure the function exists to prevent, reached through the one
    /// input value the guard did not cover, and it lands in exactly the "autovacuum has fallen behind"
    /// shape this feature is built for. Proven red by construction: with <c>&gt; 0</c> this case returns
    /// null.</para>
    /// </summary>
    [Fact]
    public void SuppressesATableAnalyzedWhileEmptyAndThenWrittenTo()
    {
        var reason = DarlingMcpPgTableBloatTools.EstimateSuppressionReason(
            Row(liveTuples: 0, deadTuples: 0, modsSinceAnalyze: 400_000));

        Assert.NotNull(reason);
        Assert.Contains("STALE", reason, StringComparison.Ordinal);

        Assert.True(DarlingPgTableBloatReader.EstimateIsUnpublishable(
            Row(liveTuples: 0, deadTuples: 0, modsSinceAnalyze: 400_000)));
    }

    /// <summary>
    /// The control, so the widened guard does not simply suppress everything at zero: a genuinely empty
    /// table with NO modifications since its analyze still publishes. Without this the fix above could be
    /// satisfied by suppressing every zero-row table, which would be a different wrong answer.
    /// </summary>
    [Fact]
    public void AnEmptyTableWithNoModificationsStillPublishes()
    {
        Assert.Null(DarlingMcpPgTableBloatTools.EstimateSuppressionReason(
            Row(liveTuples: 0, deadTuples: 0, modsSinceAnalyze: 0)));
    }

    // ── The sort key (review finding) ────────────────────────────────────────────────────────────

    /// <summary>
    /// The ORDER BY accounts for ALL THREE suppression reasons, not just <c>estimate_unavailable</c>.
    ///
    /// <para>A row suppressed for stale statistics or for never having been analyzed still carries an
    /// arbitrarily large and untrustworthy <c>bloat_bytes_estimate</c> — the measured stale case was
    /// 94.28% — so ordering on the flag alone let precisely those rows take the top of a LIMIT that reads
    /// as a work queue, which is the outcome the ordering exists to prevent.</para>
    ///
    /// <para>This is a text pin because an ORDER BY cannot call into C#, so the SQL necessarily carries a
    /// twin of <see cref="DarlingPgTableBloatReader.EstimateIsUnpublishable"/>. Dropping any one condition
    /// turns this red rather than quietly re-ranking.</para>
    /// </summary>
    [Fact]
    public void TheSortKeyCoversEverySuppressionReason()
    {
        var order = Squeezed[Squeezed.LastIndexOf("ORDER BY", StringComparison.Ordinal)..];

        Assert.Contains("l.estimate_unavailable", order, StringComparison.Ordinal);
        Assert.Contains("l.live_tuples < 0", order, StringComparison.Ordinal);
        Assert.Contains("l.mods_since_analyze > l.live_tuples", order, StringComparison.Ordinal);

        /* And the zero case, which is the whole point of the sibling fix above: >= rather than >. */
        Assert.Contains("l.live_tuples >= 0", order, StringComparison.Ordinal);
    }

    /// <summary>
    /// The ratio written into the ORDER BY is the shared one. A raw string literal cannot be spliced, so
    /// the number is inlined in the SQL; this is what stops it drifting after somebody tunes the constant
    /// and misses the query.
    /// </summary>
    [Fact]
    public void TheSortKeyUsesTheSharedChurnRatio()
    {
        Assert.Equal(
            DarlingPgTableBloatReader.StaleStatisticsChurnRatio,
            double.Parse(DarlingPgTableBloatReader.StaleStatisticsChurnRatioSql, CultureInfo.InvariantCulture));

        Assert.Contains(
            "l.live_tuples * " + DarlingPgTableBloatReader.StaleStatisticsChurnRatioSql,
            Squeezed,
            StringComparison.Ordinal);
    }

    // ── Severity, and web/desktop parity (review finding) ────────────────────────────────────────

    /// <summary>
    /// The read hands the browser a SERVER-computed band, because the web renderer never re-derives one —
    /// so without this the web grid could not colour a row at all and the two front ends disagreed about
    /// which rows look urgent.
    ///
    /// <para><c>Unknown</c> for a suppressed estimate is the load-bearing case rather than a fallback: the
    /// row has no trustworthy number, so giving it a severity would assert exactly what the suppression
    /// denies. It matches the neutral grey the WPF grid paints for the same state.</para>
    /// </summary>
    [Fact]
    public void TheSeverityBandNeverColoursASuppressedRow()
    {
        Assert.Equal("Unknown", DarlingMcpPgTableBloatTools.BloatSeverity(Row(estimateUnavailable: true)));
        Assert.Equal("Unknown", DarlingMcpPgTableBloatTools.BloatSeverity(
            Row(liveTuples: 150_000, modsSinceAnalyze: 300_000)));
        Assert.Equal("Unknown", DarlingMcpPgTableBloatTools.BloatSeverity(Row(liveTuples: -1)));
    }

    /// <summary>A published estimate bands by its percentage, on the same 50/20 boundaries the WPF grid
    /// paints red at.</summary>
    [Fact]
    public void TheSeverityBandTracksAPublishedEstimate()
    {
        Assert.Equal("Critical", DarlingMcpPgTableBloatTools.BloatSeverity(Row()));
        Assert.Equal("Healthy", DarlingMcpPgTableBloatTools.BloatSeverity(Row(bloatPct: 5m)));
        Assert.Equal("Warning", DarlingMcpPgTableBloatTools.BloatSeverity(Row(bloatPct: 35m)));
        Assert.Equal("Critical", DarlingMcpPgTableBloatTools.BloatSeverity(Row(bloatPct: 50m)));
    }
}
