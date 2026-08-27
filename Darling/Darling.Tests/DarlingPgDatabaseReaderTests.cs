/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the pg_stat_database read (#2539): windowed differences clamped per interval, a statistics reset
/// reported as a reset rather than surfacing as a negative rate or a spike, and prose that says what each
/// number means and what it cannot.
/// </summary>
public class DarlingPgDatabaseReaderTests
{
    private static string Sql => DarlingPgDatabaseReader.PgDatabaseSql;

    /// <summary>
    /// The database is the series identity — including PostgreSQL's own NULL-named shared-relations row,
    /// which PARTITION BY and GROUP BY both treat as one group. Partitioning on anything coarser would
    /// difference one database's counters against another's.
    /// </summary>
    [Fact]
    public void DifferencesWithinOneDatabase()
    {
        Assert.Contains("PARTITION BY database_name", Sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time", Sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY database_name", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every counter is differenced against its own predecessor and clamped at zero PER INTERVAL, so a
    /// reset drops one interval instead of producing a large negative figure — and, because the clamp is
    /// per interval rather than on the window total, it cannot produce a spike either.
    /// </summary>
    [Theory]
    [InlineData("xact_commit")]
    [InlineData("xact_rollback")]
    [InlineData("blks_read")]
    [InlineData("blks_hit")]
    [InlineData("temp_files")]
    [InlineData("temp_bytes")]
    [InlineData("deadlocks")]
    public void ClampsEveryDifferencedCounterAtZero(string column)
    {
        Assert.Contains($"LAG({column})", Sql, StringComparison.Ordinal);
        Assert.Contains($"GREATEST(raw_{column}, 0)", Sql, StringComparison.Ordinal);
        Assert.Contains($"SUM(d_{column})", Sql, StringComparison.Ordinal);

        /* No lifetime cumulative reading may reach the projection: MAX() of a since-reset counter is the
           counter itself, which is the exact number this read exists NOT to report. */
        Assert.DoesNotContain($"MAX({column})", Sql, StringComparison.Ordinal);
    }

    /// <summary>All seven clamps present and none left bare.</summary>
    [Fact]
    public void ClampCountMatchesTheDifferencedColumnCount()
    {
        Assert.Equal(7, Sql.Split("GREATEST(raw_").Length - 1);
        Assert.Equal(7, Sql.Split("OVER series AS raw_").Length - 1);
    }

    /// <summary>
    /// THE assertion of this file, and the reason stats_reset is collected at all.
    ///
    /// <para>Two independent reset signals, because neither one sees every reset. The EXPLICIT one — the
    /// server's own stats_reset moving — is the only thing that can see a reset followed by enough activity
    /// to climb back past the old value inside one collection interval, where the difference is positive and
    /// the arithmetic sees an ordinary busy minute. The IMPLICIT one — a counter below its predecessor —
    /// catches a crash restart, and a target that has never been reset at all, where stats_reset is NULL and
    /// cannot move.</para>
    /// </summary>
    [Fact]
    public void ReportsAResetFromBothTheTimestampAndTheCounters()
    {
        /* IS DISTINCT FROM, not <>, so a NULL on either side is a real comparison rather than NULL. */
        Assert.Contains("stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series", Sql, StringComparison.Ordinal);

        /* The first sample of a series is excluded by its POSITION, not by its LAG being NULL. Pinned as an
           explicit DoesNotContain because the LAG form reads correctly and is wrong: LAG(stats_reset) is
           NULL both when there is no previous row AND when the previous row's stats_reset was itself NULL —
           the ordinary state of a database nobody has reset — so a database's FIRST reset moves stats_reset
           NULL -> timestamp and never fires. */
        Assert.Contains("ROW_NUMBER() OVER series > 1", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(stats_reset) OVER series IS NOT NULL", Sql, StringComparison.Ordinal);

        Assert.Contains("count(*) FILTER (WHERE reset_here)", Sql, StringComparison.Ordinal);
        Assert.Contains("AS stats_reset_count", Sql, StringComparison.Ordinal);

        Assert.Contains("count(*) FILTER (WHERE rewound_here)", Sql, StringComparison.Ordinal);
        Assert.Contains("AS counter_rewind_count", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The rewind check reads the RAW differences, not the clamped ones. Against the clamped values it
    /// could never fire — GREATEST has already turned every negative into zero — which is exactly the shape
    /// of a guard that stops guarding while still looking present.
    /// </summary>
    [Fact]
    public void TheRewindCheckReadsTheUnclampedDifferences()
    {
        var least = Sql[Sql.IndexOf("LEAST(", StringComparison.Ordinal)..];
        var rewound = least[..least.IndexOf("AS rewound_here", StringComparison.Ordinal)];

        foreach (var column in new[]
                 {
                     "raw_xact_commit", "raw_xact_rollback", "raw_blks_read", "raw_blks_hit",
                     "raw_temp_files", "raw_temp_bytes", "raw_deadlocks",
                 })
        {
            Assert.Contains(column, rewound, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("d_xact_commit", rewound, StringComparison.Ordinal);
        Assert.Contains("< 0)", rewound, StringComparison.Ordinal);
    }

    /// <summary>
    /// A database that was reset and then sat idle has all-zero differences. Filtering it out on activity
    /// alone would hide the one fact that explains why its numbers look the way they do, so the HAVING
    /// carries both reset counts as alternatives.
    /// </summary>
    [Fact]
    public void KeepsARowWhoseOnlyEventWasAReset()
    {
        var having = Sql[Sql.IndexOf("HAVING", StringComparison.Ordinal)..];

        Assert.Contains("OR count(*) FILTER (WHERE reset_here) > 0", having, StringComparison.Ordinal);
        Assert.Contains("OR count(*) FILTER (WHERE rewound_here) > 0", having, StringComparison.Ordinal);

        /* And the activity clause still considers every counter, or a database whose only activity was a
           deadlock or a spill would vanish. */
        foreach (var counter in new[] { "d_xact_commit", "d_blks_read", "d_temp_files", "d_deadlocks" })
        {
            Assert.Contains(counter, having, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Ordered by spilled BYTES first. That is the question this read exists to answer and the one no other
    /// read here can; blocks read only breaks the tie, so a busy database still outranks an idle one when
    /// nothing spilled at all.
    /// </summary>
    [Fact]
    public void OrdersBySpilledBytesBeforeBlocksRead()
    {
        var order = Sql[Sql.IndexOf("ORDER BY coalesce", StringComparison.Ordinal)..];
        var tempAt = order.IndexOf("d_temp_bytes", StringComparison.Ordinal);
        var readsAt = order.IndexOf("d_blks_read", StringComparison.Ordinal);

        Assert.True(tempAt >= 0 && readsAt >= 0 && tempAt < readsAt);
    }

    [Fact]
    public void ReadsTheDatabaseStatsTableAndBoundsTheRowCount()
    {
        Assert.Contains("FROM pg_database_stats", Sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", Sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The empty-answer denominator is the DATA, on the same relation the read walks — pg_stat_database is a
    /// PERIODIC surface, so any stored sample proves somebody looked. It must NOT carry the read's own
    /// HAVING filter, or a genuinely quiet server would be reported as never collected: the #2508 false
    /// alarm, one engine over.
    /// </summary>
    [Fact]
    public void TheCoverageProbeIsUnfilteredAndCappedAtTwoSamples()
    {
        var probe = DarlingPgDatabaseReader.DatabaseStatsCoverageSql;

        Assert.Contains("FROM pg_database_stats", probe, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", probe, StringComparison.Ordinal);

        /* Two, not one: these are cumulative counters, so a single sample produces no difference at all and
           an empty result there means "cannot difference yet", not "quiet". */
        Assert.Contains("LIMIT 2", probe, StringComparison.Ordinal);
        Assert.Contains("samples_in_window", probe, StringComparison.Ordinal);
        Assert.Contains("ever_collected", probe, StringComparison.Ordinal);

        Assert.DoesNotContain("HAVING", probe, StringComparison.Ordinal);
        Assert.DoesNotContain("temp_files", probe, StringComparison.Ordinal);
    }

    /* ───────── the prose, which is where the remedy lives ───────── */

    /// <summary>
    /// Zero temp files is a real all-clear and must read as one — this is the branch that says "work_mem
    /// held everything", not a hedge.
    /// </summary>
    [Fact]
    public void NoSpillReadsAsAnAllClear()
    {
        var finding = DarlingMcpPgDatabaseTools.SpillFinding(0, 0);

        Assert.Contains("No temp files", finding, StringComparison.Ordinal);
        Assert.Contains("work_mem", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("get_pg_top_queries", finding, StringComparison.Ordinal);
    }

    /// <summary>
    /// The split that decides the remedy: many small files is a work_mem shortfall, a few enormous ones is
    /// usually a plan or index problem that work_mem will not fix. A single generic "it spilled" would leave
    /// a reader choosing between those at random.
    /// </summary>
    [Fact]
    public void TheSpillFindingSeparatesManySmallFilesFromAFewHugeOnes()
    {
        var small = DarlingMcpPgDatabaseTools.SpillFinding(4_000, 4_000L * 64 * 1024);
        var huge = DarlingMcpPgDatabaseTools.SpillFinding(2, 2L * 4 * 1024 * 1024 * 1024);

        Assert.NotEqual(small, huge);

        Assert.Contains("Many small spill files", small, StringComparison.Ordinal);
        Assert.Contains("very large spill files", huge, StringComparison.Ordinal);
        Assert.Contains("missing index", huge, StringComparison.Ordinal);
    }

    /// <summary>
    /// The honest limit, stated on the finding itself: pg_stat_database is database-scoped and cannot name
    /// the query. A spill finding that did not say so would read as an attribution it is not.
    /// </summary>
    [Fact]
    public void TheSpillFindingSaysItCannotNameTheQuery_AndPointsAtWhatCan()
    {
        var finding = DarlingMcpPgDatabaseTools.SpillFinding(10, 10L * 1024 * 1024);

        Assert.Contains("cannot name the QUERY", finding, StringComparison.Ordinal);
        Assert.Contains("get_pg_top_queries", finding, StringComparison.Ordinal);

        /* And the Aurora caveat, because that read is Aurora-only — on stock PostgreSQL this counter is the
           only temp-file evidence available anywhere, and pointing at a tool that cannot answer there would
           be worse than pointing at nothing. */
        Assert.Contains("Aurora", finding, StringComparison.Ordinal);
        Assert.Contains("stock PostgreSQL", finding, StringComparison.Ordinal);
    }

    /// <summary>
    /// The caveat that keeps the hit ratio honest. blks_read means "not in shared_buffers", not "read from
    /// disk": the OS page cache and, on Aurora, the storage layer sit underneath. Without this a low ratio
    /// sends someone buying memory for latency that may not exist.
    /// </summary>
    [Theory]
    [InlineData(99.8)]
    [InlineData(97.0)]
    [InlineData(80.0)]
    public void TheCacheFindingAlwaysCarriesTheNotADiskMeasurementCaveat(double pct)
    {
        var finding = DarlingMcpPgDatabaseTools.CacheHitFinding(pct);

        Assert.Contains("shared_buffers", finding, StringComparison.Ordinal);
        Assert.Contains("OS page cache", finding, StringComparison.Ordinal);
    }

    /// <summary>
    /// Three distinct verdicts around the conventional 99% target, and no accesses at all is not a ratio of
    /// zero — it is the absence of a ratio.
    /// </summary>
    [Fact]
    public void TheCacheFindingBandsAreDistinct_AndNoAccessesIsNotZeroPercent()
    {
        var findings = new[] { 99.9, 97.0, 80.0 }
            .Select(p => DarlingMcpPgDatabaseTools.CacheHitFinding(p))
            .ToArray();

        Assert.Equal(findings.Length, findings.Distinct(StringComparer.Ordinal).Count());

        var none = DarlingMcpPgDatabaseTools.CacheHitFinding(null);
        Assert.Contains("not a ratio of", none, StringComparison.Ordinal);
        Assert.DoesNotContain("shared_buffers is absorbing", none, StringComparison.Ordinal);
    }

    /// <summary>
    /// The rollback finding is about the RATIO, not the count: a busy database legitimately rolls back more
    /// transactions than a quiet one, so the same 500 rollbacks must read differently against 500 commits
    /// and against 5,000,000.
    /// </summary>
    [Fact]
    public void TheRollbackFindingIsAboutTheRatioNotTheCount()
    {
        var storm = DarlingMcpPgDatabaseTools.RollbackFinding(500, 500);
        var normal = DarlingMcpPgDatabaseTools.RollbackFinding(5_000_000, 500);

        Assert.Contains("rollback storm", storm, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("rollback storm", normal, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("normal share", normal, StringComparison.Ordinal);

        /* PostgreSQL counts an ERROR-aborted transaction as a rollback, so this is not a count of explicit
           ROLLBACK statements and must not be read as one. */
        Assert.Contains("ERROR-aborted", storm, StringComparison.Ordinal);

        Assert.Contains("No transactions", DarlingMcpPgDatabaseTools.RollbackFinding(0, 0), StringComparison.Ordinal);
    }
}
