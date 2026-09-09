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
using System.Text.RegularExpressions;
using System.Threading;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The statistics-based btree bloat estimate, pinned at the properties that were measured wrong before
/// they were measured right (#3234). This collector replaced a <c>pgstatindex</c> census; the exact
/// function is now an on-request escalation and the rotation cursor, per-index ceiling and cycle byte
/// budget that census needed are all gone.
/// </summary>
public class PgIndexBloatCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext Context()
        => new()
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
            },
            ExcludedDatabases = Array.Empty<string>(),
            State = CollectorContext.NoState,
            CurrentDatabaseName = "app",
        };

    private static CollectorQuery Plan() => PgIndexBloatCollector.Instance.BuildQuery(Context());

    private static string Sql() => Plan().Text;

    /// <summary>
    /// Only b-trees are candidates. Verified against a live server for the exact path: GIN, BRIN and hash
    /// each raise <c>relation "x" is not a btree index</c>. The estimate would not RAISE on them, but it
    /// would happily model a page layout none of them use, which is worse — a wrong number instead of an
    /// error.
    /// </summary>
    [Fact]
    public void OnlyBtreeIndexes_AreCandidates()
        => Assert.Matches(new Regex(@"am\.amname\s*=\s*'btree'"), Sql());

    [Fact]
    public void InvalidIndexes_AreExcluded()
    {
        var sql = Sql();

        Assert.Contains("x.indisvalid", sql, StringComparison.Ordinal);
        Assert.Contains("x.indisready", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The candidate set stays fenced with <c>OFFSET 0</c>. It no longer guards a function that raises,
    /// but it still decides that the per-attribute <c>pg_stats</c> joins are applied to b-trees only
    /// rather than to every index on the server.
    /// </summary>
    [Fact]
    public void TheCandidateSet_IsFenced()
        => Assert.Contains("OFFSET 0", Sql(), StringComparison.Ordinal);

    /// <summary>Writer-only: <c>reltuples</c> and <c>relpages</c> are maintained by VACUUM and ANALYZE.</summary>
    [Fact]
    public void AppliesTo_PrimariesOnly()
    {
        Assert.True(PgIndexBloatCollector.Instance.AppliesTo(
            new CollectorTargetInfo { IsInRecovery = false }));
        Assert.False(PgIndexBloatCollector.Instance.AppliesTo(
            new CollectorTargetInfo { IsInRecovery = true }));
    }

    /// <summary>
    /// <b>No extension is required, and that is a coverage claim rather than housekeeping.</b> The exact
    /// census depended on pgstattuple, so a database without it failed collection for that database
    /// outright — observed on the <c>postgres</c> maintenance database of the first production target. If
    /// a dependency reappears here, every such database goes dark again.
    /// </summary>
    [Fact]
    public void NoExtensionIsRequired()
    {
        Assert.Empty(PgIndexBloatCollector.Instance.RequiredPgExtensions);
        Assert.DoesNotContain("pgstatindex", Sql(), StringComparison.Ordinal);
    }

    /// <summary>
    /// Stateless. The estimate covers every index in one statement, so there is no position to resume
    /// from. Re-introducing a cursor re-opens the prune question
    /// <c>CollectorStateContractTests.EveryDeclaredStateKeyPrefixHasAPruneVerdict</c> exists to force.
    /// </summary>
    [Fact]
    public void StateKeys_IsEmpty()
        => Assert.Empty(PgIndexBloatCollector.Instance.StateKeys);

    /// <summary>
    /// The page arithmetic in the SQL is the arithmetic the C# constants describe. Two copies of one fact,
    /// so this is the pin that stops them drifting: 8,192 minus a 24-byte page header minus the 16-byte
    /// btree special area is what makes a leaf page hold 8,152 usable bytes before fillfactor.
    /// </summary>
    [Fact]
    public void ThePageArithmetic_MatchesPostgresPageLayout()
    {
        var sql = Sql();

        Assert.Equal(24, PgIndexBloatCollector.PageHeaderBytes);
        Assert.Equal(16, PgIndexBloatCollector.BtreeSpecialAreaBytes);
        Assert.Equal(4, PgIndexBloatCollector.LinePointerBytes);
        Assert.Equal(8, PgIndexBloatCollector.IndexTupleHeaderBytes);
        Assert.Equal(90, PgIndexBloatCollector.DefaultBtreeFillfactor);

        /* usable = block_size - page header - btree special area */
        Assert.Matches(
            new Regex(@"a\.bs\s*-\s*24\s*-\s*16"),
            sql);

        /* the line pointer is charged once per tuple, alongside the modelled tuple */
        Assert.Matches(new Regex(@"est_tuple_bytes\s*\+\s*4"), sql);

        /* fillfactor defaults to 90 when the index declares none */
        Assert.Matches(new Regex(@"fillfactor=\(\[0-9\]\+\)'\)::int,\s*90"), sql);
    }

    /// <summary>
    /// <b>The single most load-bearing property in this collector, and the one two earlier formulations
    /// got wrong in opposite directions.</b>
    ///
    /// <para><c>pg_stats.avg_width</c> describes the entries that EXIST, so an attribute present in a
    /// fraction of tuples contributes its ALIGNED width in that fraction of them. The null factor must
    /// therefore multiply the aligned per-attribute width.</para>
    ///
    /// <para>Measured on a live 2,500-index target against <c>pgstatindex</c> ground truth: discounting
    /// BEFORE aligning a summed width — which is what the ioguix query does — collapses near-fully-null
    /// leading columns to nothing and gives a p90 error of <b>40.57pp</b>. Not discounting at all
    /// over-counts them and suppresses 901 indexes as negative bloat. This form gives median absolute
    /// <b>2.79pp</b> and p90 <b>6.63pp</b>.</para>
    ///
    /// <para>Both wrong forms are asserted ABSENT rather than only the right one asserted present,
    /// because the failure is a rearrangement of the same tokens and a presence-only pin passes on either
    /// of them.</para>
    /// </summary>
    [Fact]
    public void TheNullDiscount_MultipliesTheAlignedWidth_NotTheRawWidth()
    {
        var sql = Sql();

        /* Right: (1 - null_frac) * CASE ... maxalign * CEIL(avg_width / maxalign) ... */
        Assert.Matches(
            new Regex(@"\(1\s*-\s*COALESCE\(k\.null_frac,\s*0\)\)\s*\r?\n?\s*\*\s*CASE\s+WHEN\s+COALESCE\(k\.avg_width,\s*0\)\s*=\s*0"),
            sql);
        Assert.Matches(
            new Regex(@"p\.maxalign\s*\r?\n?\s*\*\s*CEIL\(COALESCE\(k\.avg_width,\s*0\)::numeric\s*/\s*p\.maxalign\)"),
            sql);

        /* Wrong form 1 (ioguix, and this collector's first version): the discount lands on the RAW width. */
        Assert.DoesNotMatch(
            new Regex(@"\(1\s*-\s*COALESCE\(k\.null_frac,\s*0\)\)\s*\*\s*COALESCE\(k\.avg_width"),
            sql);

        /* Wrong form 2: no discount at all, the aligned width summed bare. */
        Assert.DoesNotMatch(
            new Regex(@"SUM\(\s*CASE\s+WHEN\s+COALESCE\(k\.avg_width,\s*0\)\s*=\s*0"),
            sql);

        /* And the aligned figure is what actually feeds the tuple size, rather than being computed and
           then ignored in favour of an unaligned sum. */
        Assert.Contains("+ a.aligned_data_bytes)::numeric", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Non-key INCLUDE columns are stored in the leaf tuple, so the width scan runs to
    /// <c>indnatts</c> and not <c>indnkeyatts</c>. Omitting them under-counts the tuple, which
    /// over-reports bloat.
    /// </summary>
    [Fact]
    public void TheWidthScan_IncludesNonKeyColumns()
    {
        var sql = Sql();

        Assert.Matches(new Regex(@"ia\.attnum\s+BETWEEN\s+1\s+AND\s+i\.indnatts"), sql);
        Assert.DoesNotContain("indnkeyatts", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// An expression key reads its width under the INDEX's name in <c>pg_stats</c> rather than the
    /// parent table's, which #2561 measured after first concluding expression indexes were unmodellable
    /// (they are not; looking under the table's column names is what produced -295.84%). Zero expression
    /// indexes existed on the validation target, so this arm is implemented and unexercised — which is
    /// exactly why it needs a pin rather than a measurement.
    /// </summary>
    [Fact]
    public void ExpressionKeys_ReadTheirWidthUnderTheIndexName()
    {
        var sql = Sql();

        Assert.Matches(new Regex(@"se\.tablename\s*=\s*i\.index_name"), sql);
        Assert.Matches(new Regex(@"i\.indkey\[ia\.attnum\s*-\s*1\]\s*=\s*0"), sql);
    }

    /// <summary>
    /// Every conjunct of the estimate gate has its own reason arm, so no index can be suppressed without
    /// the row saying which condition suppressed it. The correspondence is the property: a gate that grew
    /// a condition without a matching arm returns a NULL estimate and a NULL reason, which reads as
    /// healthy.
    /// </summary>
    [Fact]
    public void EveryGateConjunct_HasItsOwnReasonArm()
    {
        var sql = Sql();

        foreach (var arm in new[]
                 {
                     "f.is_partial",
                     "f.table_rows < 0",
                     "f.has_name_typed",
                     "f.widths_known <> f.key_count",
                     "f.index_pages = 0",
                     "f.est_leaf_pages > f.index_pages",
                 })
        {
            Assert.Contains(arm, sql, StringComparison.Ordinal);
        }

        /* The gate and the reason list are the same conditions, so the gate's own conjuncts appear too. */
        foreach (var conjunct in new[]
                 {
                     "NOT f.is_partial",
                     "f.table_rows >= 0",
                     "NOT f.has_name_typed",
                     "f.widths_known = f.key_count",
                     "f.est_leaf_pages <= f.index_pages",
                 })
        {
            Assert.Contains(conjunct, sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The estimate and the reason are complements: both the percentage and the reclaimable byte figure
    /// are NULL under exactly the gate that populates a reason. A suppressed estimate must never be
    /// readable as zero bloat — the same rule <c>estimate_unavailable</c> carries on the table side, where
    /// a footnote under a large red percentage was judged not to be a safeguard (#2542).
    /// </summary>
    [Fact]
    public void TheEstimateIsNull_ExactlyWhenAReasonIsPopulated()
    {
        var sql = Sql();
        var gate = new Regex(@"CASE WHEN f\.index_pages > 0[\s\S]*?END::(double precision|bigint)");
        var gated = gate.Matches(sql);

        /* est_bloat_pct and est_reclaimable_bytes, both gated, and nothing else. */
        Assert.Equal(2, gated.Count);

        foreach (Match match in gated)
        {
            Assert.Contains("NOT f.is_partial", match.Value, StringComparison.Ordinal);
            Assert.Contains("f.est_leaf_pages <= f.index_pages", match.Value, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// No bloat percentage is derived from a density here, because this collector no longer reads one — and
    /// the rule that made that important survives the change. A zero-bloat index reads 89.98 to 91.48, so
    /// <c>100 - avg_leaf_density</c> invents about ten points on a perfect index and there is no constant
    /// to subtract (#2561).
    /// </summary>
    [Fact]
    public void NoBloatPercentIsDerivedFromADensity()
    {
        var sql = Sql();

        Assert.DoesNotContain("avg_leaf_density", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("100 -", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ranked by reclaimable BYTES and never by percentage. A 64 kB index at 20 percent tops a
    /// percentage-ranked list and is worth 50 kB, next to a 10 GB index at 45 percent worth 5.37 GB
    /// (#2561). This also disposes of the small-index problem for free: 8 of the 10 worst validation
    /// outliers had between 2,000 and 8,000 rows, where page-count rounding dominates the percentage, and
    /// every one of them sorts to the bottom on bytes.
    /// </summary>
    [Fact]
    public void TheStatementRanksByReclaimableBytes()
    {
        var sql = Sql();

        Assert.Matches(
            new Regex(@"ORDER BY\s+est_reclaimable_bytes\s+DESC\s+NULLS\s+LAST"),
            sql);
        Assert.DoesNotMatch(new Regex(@"ORDER BY\s+est_bloat_pct"), sql);
    }

    /// <summary>
    /// pgstattuple availability is REPORTED but not required, because it decides whether the per-row
    /// escalation command needs a <c>CREATE EXTENSION</c> in front of it. Read per database, since
    /// <c>pg_extension</c> is per-database and a cluster-wide answer from one connection is one
    /// database's (#2599).
    /// </summary>
    [Fact]
    public void PgstattupleAvailability_IsReportedNotRequired()
    {
        var sql = Sql();

        Assert.Matches(new Regex(@"e\.extname\s*=\s*'pgstattuple'"), sql);
        Assert.Contains("pgstattuple_available", sql, StringComparison.Ordinal);
        Assert.Empty(PgIndexBloatCollector.Instance.RequiredPgExtensions);
    }

    /// <summary>
    /// The deduplication reason carries the measurement that justifies calling it structural rather than
    /// a bug, because without it the arm reads as an unexplained shrug on roughly half the fleet.
    /// </summary>
    [Fact]
    public void TheDeduplicationReason_CarriesItsMeasurement()
    {
        var sql = Sql();

        Assert.Contains("deduplication", sql, StringComparison.Ordinal);
        Assert.Contains("0.0018", sql, StringComparison.Ordinal);
        Assert.Contains("posting list", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every suppression reason that CAN be remedied says how, and the two that cannot say so plainly.
    /// A reason without a next step is a dead end the operator has to research.
    /// </summary>
    [Fact]
    public void RemediableReasons_NameTheirRemedy()
    {
        var sql = Sql();

        Assert.Contains("An ANALYZE of the parent makes this index estimable", sql, StringComparison.Ordinal);
        Assert.Contains("Grant pg_read_all_data", sql, StringComparison.Ordinal);
        Assert.Contains("Run pgstatindex", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Sixty seconds, against a statement measured at 1.7 to 2.1 seconds on a 2,500-index target
    /// including connect and result transfer. The exact census needed 300 and spent 84 percent of it; this
    /// one reads no index pages at all.
    /// </summary>
    [Fact]
    public void ItOverridesTheCommandTimeout()
        => Assert.Equal(60, PgIndexBloatCollector.Instance.CommandTimeoutSecondsOverride);

    /// <summary>
    /// The payload contract: one column per value written, in the same order. The exact-measurement
    /// columns are retained and written NULL so the store's 90 days of pgstatindex history stays readable
    /// and the migration for this change only ADDS columns.
    /// </summary>
    [Fact]
    public void PayloadColumns_MatchTheValuesWritten()
    {
        var columns = PgIndexBloatCollector.Instance.PayloadColumns;

        Assert.Equal(21, columns.Count);

        Assert.Equal(
            new[]
            {
                "database_name", "schema_name", "table_name", "index_name", "index_bytes",
                "tree_level", "internal_pages", "leaf_pages", "empty_pages", "deleted_pages",
                "avg_leaf_density", "leaf_fragmentation",
                "skipped_reason",
                "index_pages", "table_rows", "fillfactor", "est_tuple_bytes", "est_leaf_pages",
                "est_bloat_pct", "est_reclaimable_bytes", "pgstattuple_available",
            },
            columns.Select(c => c.Name).ToArray());

        var writer = new RecordingCollectorRowWriter();

        PgIndexBloatCollector.Instance.WritePayload(
            new PgIndexBloatCollector.Row(
                DatabaseName: "app",
                SchemaName: "public",
                TableName: "t",
                IndexName: "ix",
                IndexBytes: 8192,
                IndexPages: 1,
                TableRows: 10,
                Fillfactor: 90,
                EstTupleBytes: 16,
                EstLeafPages: 1,
                EstBloatPct: 0,
                EstReclaimableBytes: 0,
                SkippedReason: null,
                PgstattupleAvailable: true,
                IndexOid: 42),
            writer,
            Context());

        Assert.Equal(columns.Count, writer.Values.Count);
    }

    /// <summary>
    /// The index oid is projected — the escalation command is built from schema and index name, and the
    /// oid is the identity a reader joins on — but it is NOT a payload column, because a stored oid is
    /// not stable across a dump and restore and would read as an identity that is.
    /// </summary>
    [Fact]
    public void TheIndexOidIsProjected_ButNotAPayloadColumn()
    {
        Assert.Contains("AS index_oid", Sql(), StringComparison.Ordinal);
        Assert.DoesNotContain(
            "index_oid",
            string.Join(",", PgIndexBloatCollector.Instance.PayloadColumns.Select(c => c.Name)),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// None of the removed census machinery came back. Each of these was a real member with its own pins
    /// before #3234, and re-adding one without re-adding its guard is the way the ceiling, the budget and
    /// the cursor would return unmeasured.
    /// </summary>
    [Fact]
    public void TheCensusMachinery_IsGone()
    {
        var members = typeof(PgIndexBloatCollector)
            .GetMembers()
            .Select(m => m.Name)
            .ToArray();

        foreach (var gone in new[]
                 {
                     "MeasureCeilingBytes", "CycleMeasureBudgetBytes", "MeasuredBlocksPerSecond",
                     "RotationCursorKeyPrefix", "RotationPassCompleteMarker", "MaxSplicedCursors",
                     "FormatCursor", "TryParseCursor", "ReadCursors",
                 })
        {
            Assert.DoesNotContain(gone, members);
        }

        /* And the statement takes no parameters, because the only ones it ever had were spliced cursors. */
        Assert.Empty(Plan().Parameters);
    }
}
