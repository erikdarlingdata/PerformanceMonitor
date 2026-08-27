/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the per-index usage read (#2541). The arithmetic half is small; the half that matters is that no
/// advice this read emits can send somebody to DROP an index the data cannot support dropping.
/// </summary>
public class DarlingPgIndexUsageReaderTests
{
    private static string Sql => DarlingPgIndexUsageReader.PgIndexUsageSql;

    /// <summary>
    /// The shipped SQL aligns its select list and its join columns into columns, so a fragment typed with
    /// single spaces will not be found in it. Assertions about STRUCTURE run against this; assertions about
    /// exact rendering keep using <see cref="Sql"/>.
    /// </summary>
    private static string Squeezed =>
        Regex.Replace(DarlingPgIndexUsageReader.PgIndexUsageSql, @"\s+", " ");

    private static string ProbeSql => DarlingPgIndexUsageReader.PgIndexUsageProbeSql;

    // ── Scoping ──────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Scoped to one server and one window, by positional parameter rather than by anything spliced. Four
    /// parameters, and the row limit is one of them: a hardcoded LIMIT shipped once on a sibling read and
    /// had to be corrected.
    /// </summary>
    [Fact]
    public void ScopesToOneServerAndOneWindow()
    {
        Assert.Contains("server_id = $1", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", Sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", Sql, StringComparison.Ordinal);

        /* No literal limit anywhere: the caller's limit has to be the only one. */
        Assert.DoesNotContain("LIMIT 50", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT 25", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The series identity is (database, schema, table, index) — all four. Dropping the database from the
    /// key would collapse two same-named indexes in different databases of one cluster into one series and
    /// difference each against the other, which is exactly the shape a per-database fan-out produces.
    /// </summary>
    [Fact]
    public void KeysTheSeriesOnAllFourNameParts()
    {
        Assert.Contains(
            "DISTINCT ON (database_name, schema_name, table_name, index_name)",
            Squeezed,
            StringComparison.Ordinal);

        /* IS NOT DISTINCT FROM on every join column, because database_name is nullable in the store and
           an ordinary = would silently drop those rows rather than matching them. */
        foreach (var column in new[] { "database_name", "schema_name", "table_name", "index_name" })
        {
            Assert.Contains($"e.{column} IS NOT DISTINCT FROM l.{column}", Squeezed, StringComparison.Ordinal);
            Assert.Contains($"s.{column} IS NOT DISTINCT FROM l.{column}", Squeezed, StringComparison.Ordinal);
        }
    }

    // ── The clamp, and why it is not decoration ──────────────────────────────────────────────────

    /// <summary>
    /// <c>scans_in_window</c> is clamped at zero. Proven on a live store rather than argued: a fixture whose
    /// counter runs 9000 → 50 → 100 across a statistics reset returns 0 with the clamp and <b>-8900</b>
    /// without it, and a negative scan count in a list ranked least-used-first is the worst possible value
    /// for it to take.
    /// </summary>
    [Fact]
    public void ClampsTheWindowedScanCountAtZero()
    {
        Assert.Contains("GREATEST(l.index_scans - e.first_scans, 0)", Squeezed, StringComparison.Ordinal);

        /* The bare subtraction must not appear anywhere: it is the reverted form, and it is what produced
           the -8900. */
        Assert.DoesNotContain("(l.index_scans - e.first_scans)  ", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The reset detector uses <c>IS DISTINCT FROM</c>, and the difference from <c>&lt;&gt;</c> is a real
    /// defect rather than a style preference. <c>stats_reset</c> is NULL until a database's statistics are
    /// reset for the FIRST time, so a first-ever reset moves the column NULL → timestamp; <c>&lt;&gt;</c>
    /// evaluates to NULL there and reports no reset at all.
    ///
    /// <para>Measured on a live store with a fixture that does exactly that: the shipped form reports the
    /// reset, the <c>&lt;&gt;</c> form returns NULL and the reset is completely invisible — and it is
    /// invisible in the one case the counter arithmetic cannot see either.</para>
    /// </summary>
    [Fact]
    public void DetectsAResetFromTheTimestamp_IncludingTheFirstEverReset()
    {
        Assert.Contains(
            "l.stats_reset IS DISTINCT FROM e.first_stats_reset",
            Squeezed,
            StringComparison.Ordinal);

        /* Pinned OUT by name. This is the form that misses a first-ever reset. */
        Assert.DoesNotContain("l.stats_reset <> e.first_stats_reset", Sql, StringComparison.Ordinal);

        /* And the timestamp has to be carried from the EARLIEST sample for the comparison to mean
           anything. */
        Assert.Contains("stats_reset AS first_stats_reset", Squeezed, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both scan figures reach the projection. The lifetime counter is what anyone querying the server
    /// directly sees, so a read that reported only the windowed figure would look like it disagreed with
    /// psql; the windowed one is the answer to the question actually being asked and exists only because
    /// the store keeps history the server does not.
    /// </summary>
    [Fact]
    public void ReportsBothTheLifetimeAndTheWindowedScanCount()
    {
        Assert.Contains("l.index_scans AS total_scans", Squeezed, StringComparison.Ordinal);
        Assert.Contains("AS scans_in_window", Squeezed, StringComparison.Ordinal);
    }

    // ── Ordering ─────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Invalid indexes sort above everything. An invalid index is the one case where "unused" and "safe to
    /// remove" genuinely coincide — the planner will not use it, so it reports zero scans forever, while
    /// writes still maintain it — so it must never be pushed under the row limit by a larger index that is
    /// merely idle. Verified live: a 16 KB invalid index sorted above an 8 MB one.
    /// </summary>
    [Fact]
    public void SortsInvalidIndexesFirst_ThenTheBiggestUnscanned()
    {
        var order = Sql[Sql.LastIndexOf("ORDER BY", StringComparison.Ordinal)..];
        Assert.Contains("l.is_valid ASC", order, StringComparison.Ordinal);
        Assert.Contains("l.index_bytes ELSE -1 END DESC", order, StringComparison.Ordinal);

        Assert.True(
            order.IndexOf("l.is_valid ASC", StringComparison.Ordinal)
            < order.IndexOf("l.index_bytes ELSE -1 END DESC", StringComparison.Ordinal),
            "the invalid-index tier must be the FIRST sort key, or a large idle index can bury a broken one");
    }

    // ── Evidence the tool needs to refuse bad advice ─────────────────────────────────────────────

    /// <summary>
    /// Every droppability fact reaches the projection. This is the substance: <c>idx_scan = 0</c> is one
    /// fact, and on its own it produces advice that breaks schemas — the single most common zero-scan index
    /// on any schema is a unique index backing a constraint, because enforcing uniqueness is not a scan of
    /// the kind the counter increments.
    /// </summary>
    [Theory]
    [InlineData("l.is_unique")]
    [InlineData("l.is_primary_key")]
    [InlineData("l.is_valid")]
    [InlineData("l.is_ready")]
    [InlineData("l.is_replica_identity")]
    [InlineData("l.is_partial")]
    [InlineData("l.is_expression")]
    [InlineData("l.supports_constraint")]
    [InlineData("l.index_definition")]
    public void CarriesEveryFactThatDecidesDroppability(string column)
    {
        Assert.Contains(column, Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>first_seen_at</c> and <c>sample_count</c> both travel, because PostgreSQL records no index
    /// creation time anywhere: how long WE have been watching is the only available evidence that an index
    /// is old enough to judge, and an index created twenty minutes ago has zero scans for the same reason a
    /// dead one does.
    /// </summary>
    [Fact]
    public void CarriesHowLongItHasBeenWatched()
    {
        Assert.Contains("e.first_seen_at", Sql, StringComparison.Ordinal);
        Assert.Contains("s.sample_count", Sql, StringComparison.Ordinal);
        Assert.Contains("count(*) AS sample_count", Squeezed, StringComparison.Ordinal);
    }

    // ── The honest-empty denominator ─────────────────────────────────────────────────────────────

    /// <summary>
    /// The probe reads the SAME relation the read walks, and deliberately without the read's ordering or
    /// limit, so it can neither report "collected" for rows the read cannot see nor "uncollected" for a
    /// server whose indexes are simply all in use. It counts DISTINCT collection times as well as rows,
    /// because a windowed scan count is a difference and needs two snapshots before it produces anything.
    /// </summary>
    [Fact]
    public void TheProbeCountsSnapshotsAsWellAsRows()
    {
        Assert.Contains("FROM pg_index_usage_stats", ProbeSql, StringComparison.Ordinal);
        Assert.Contains("count(DISTINCT collection_time)", ProbeSql, StringComparison.Ordinal);
        Assert.Contains("AS rows_ever", ProbeSql, StringComparison.Ordinal);

        Assert.DoesNotContain("LIMIT", ProbeSql, StringComparison.Ordinal);
        Assert.DoesNotContain("ORDER BY", ProbeSql, StringComparison.Ordinal);
    }

    // ── The droppability classifier ──────────────────────────────────────────────────────────────

    private static DarlingPgIndexUsageReader.PgIndexUsageRow Row(
        bool unique = false,
        bool primaryKey = false,
        bool valid = true,
        bool replicaIdentity = false,
        bool partial = false,
        bool expression = false,
        bool supportsConstraint = false,
        int sampleCount = 5) =>
        new(
            DatabaseName: "appdb",
            SchemaName: "public",
            TableName: "widget",
            IndexName: "widget_idx",
            MeasuredAt: DateTime.UtcNow,
            TotalScans: 0,
            ScansInWindow: 0,
            TuplesRead: 0,
            TuplesFetched: 0,
            BlocksRead: 0,
            BlocksHit: 0,
            IndexBytes: 1_000_000,
            TableBytes: 9_000_000,
            IsUnique: unique,
            IsPrimaryKey: primaryKey,
            IsValid: valid,
            IsReady: true,
            IsReplicaIdentity: replicaIdentity,
            IsPartial: partial,
            IsExpression: expression,
            SupportsConstraint: supportsConstraint,
            IndexMethod: "btree",
            ColumnCount: 1,
            IndexDefinition: "CREATE INDEX widget_idx ON public.widget (a)",
            LastScan: null,
            StatsReset: null,
            FirstSeenAt: DateTime.UtcNow.AddDays(-30),
            SampleCount: sampleCount,
            StatsWereResetInWindow: false);

    /// <summary>
    /// A zero-scan PRIMARY KEY is never called droppable. Enforcing the key on every INSERT is not a scan
    /// and is not counted, so the counter alone would tell somebody to drop their primary key.
    /// </summary>
    [Fact]
    public void NeverRecommendsDroppingAPrimaryKey()
    {
        var finding = DarlingMcpPgIndexUsageTools.DroppabilityFinding(
            Row(unique: true, primaryKey: true, supportsConstraint: true), 0, 5);

        Assert.Contains("PRIMARY KEY", finding, StringComparison.Ordinal);
        Assert.Contains("cannot be dropped", finding, StringComparison.Ordinal);
    }

    /// <summary>A zero-scan constraint index is never called droppable: dropping the index drops the
    /// constraint, which changes what data the table will accept.</summary>
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void NeverRecommendsDroppingAConstraintIndex(bool unique, bool supportsConstraint)
    {
        var finding = DarlingMcpPgIndexUsageTools.DroppabilityFinding(
            Row(unique: unique, supportsConstraint: supportsConstraint), 0, 5);

        Assert.Contains("constraint", finding, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("no constraint, key or replica-identity", finding, StringComparison.Ordinal);
    }

    /// <summary>A replica-identity index is never called droppable: dropping it breaks logical replication
    /// of UPDATE and DELETE for its table.</summary>
    [Fact]
    public void NeverRecommendsDroppingAReplicaIdentityIndex()
    {
        Assert.Contains(
            "REPLICA IDENTITY",
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Row(replicaIdentity: true), 0, 5),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// One sample is not enough to claim disuse, and the refusal says why. A windowed scan count is a
    /// DIFFERENCE and needs at least two samples before it is a measurement rather than a reading.
    /// </summary>
    [Fact]
    public void RefusesADisuseClaimOnOneSample()
    {
        Assert.Contains(
            "Not enough history",
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Row(sampleCount: 1), 0, 1),
            StringComparison.Ordinal);

        Assert.Equal(2, DarlingMcpPgIndexUsageTools.MinimumSamplesForADisuseClaim);
    }

    /// <summary>
    /// An INVALID index is a finding even on one sample, because it is a statement about the catalog rather
    /// than about the counters — the planner will never use it whatever the scan history says.
    /// </summary>
    [Fact]
    public void ReportsAnInvalidIndexEvenWithoutEnoughHistory()
    {
        Assert.Contains(
            "INVALID index",
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Row(valid: false, sampleCount: 1), 0, 1),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// Even the clear case is qualified. Two things the data structurally cannot see survive every check
    /// above: a query that runs less often than the window is long, and a rare query whose plan is only
    /// acceptable because this index exists.
    /// </summary>
    [Fact]
    public void EvenTheClearCaseNamesWhatTheDataCannotSee()
    {
        var finding = DarlingMcpPgIndexUsageTools.DroppabilityFinding(Row(), 0, 5);

        Assert.Contains("less often than the window", finding, StringComparison.Ordinal);
        Assert.Contains("rare query", finding, StringComparison.Ordinal);

        /* And it is never phrased as an instruction. */
        Assert.DoesNotContain("Drop it", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("drop this index", finding, StringComparison.Ordinal);
    }

    /// <summary>A partial or expression index reads as unused when the planner cannot MATCH it, which is a
    /// different condition with a different fix, so the finding names it.</summary>
    [Fact]
    public void SeparatesUnusedFromUnusable()
    {
        Assert.Contains(
            "PARTIAL index",
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Row(partial: true), 0, 5),
            StringComparison.Ordinal);

        Assert.Contains(
            "EXPRESSION index",
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Row(expression: true), 0, 5),
            StringComparison.Ordinal);
    }

    /// <summary>A scanned index reports in-use and nothing else. The classifier must not manufacture a
    /// finding for a healthy index.</summary>
    [Fact]
    public void ReportsAScannedIndexAsInUse()
    {
        Assert.Contains(
            "In use",
            DarlingMcpPgIndexUsageTools.DroppabilityFinding(Row(), 42, 5),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The cost sentence states the VACUUM half, which is the reason an unused index costs more here than
    /// on SQL Server: index cleanup is a large share of vacuum work, so a dead index on a hot table slows
    /// the maintenance that keeps the table healthy.
    /// </summary>
    [Fact]
    public void TheCostSentenceNamesTheVacuumCost()
    {
        var cost = DarlingMcpPgIndexUsageTools.CostFinding(1_000_000, 9_000_000, 500, 20);

        Assert.Contains("VACUUM", cost, StringComparison.Ordinal);
        Assert.Contains("write", cost, StringComparison.OrdinalIgnoreCase);
    }

    // ── Severity, and web/desktop parity (review finding) ────────────────────────────────────────

    /// <summary>
    /// The read hands the browser a SERVER-computed band, because the web renderer never re-derives one —
    /// without it the web grid could not colour a row and the two front ends disagreed about which rows
    /// look urgent.
    ///
    /// <para>The ranking mirrors the WPF row-style triggers exactly, including the case that is easiest to
    /// get wrong: an index we have not watched for two samples is <b>Healthy</b>, not Warning.
    /// Too-early-to-say must not look like a finding, and it is the same reason the droppability sentence
    /// refuses a disuse claim there.</para>
    /// </summary>
    [Fact]
    public void TheSeverityBandMirrorsTheDesktopHighlights()
    {
        Assert.Equal("Critical", DarlingMcpPgIndexUsageTools.IndexSeverity(Row(valid: false)));
        Assert.Equal("Warning", DarlingMcpPgIndexUsageTools.IndexSeverity(Row()));
        Assert.Equal("Healthy", DarlingMcpPgIndexUsageTools.IndexSeverity(Row(sampleCount: 1)));

        /* Every structural blocker keeps the row Healthy: a constraint index reporting zero scans is the
           normal state, not a finding, and colouring it amber is how somebody ends up dropping it. */
        Assert.Equal("Healthy", DarlingMcpPgIndexUsageTools.IndexSeverity(Row(primaryKey: true, unique: true, supportsConstraint: true)));
        Assert.Equal("Healthy", DarlingMcpPgIndexUsageTools.IndexSeverity(Row(supportsConstraint: true)));
        Assert.Equal("Healthy", DarlingMcpPgIndexUsageTools.IndexSeverity(Row(unique: true)));
        Assert.Equal("Healthy", DarlingMcpPgIndexUsageTools.IndexSeverity(Row(replicaIdentity: true)));

        /* INVALID outranks every blocker, matching the XAML's severity ordering. */
        Assert.Equal("Critical", DarlingMcpPgIndexUsageTools.IndexSeverity(Row(valid: false, primaryKey: true, supportsConstraint: true)));
    }
}
