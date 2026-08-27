/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the read side of pg_statement_stats: every counter covers the WINDOW — the rate columns from
/// stored deltas, the block/WAL columns differenced here — the difference is reset-safe, and the
/// grouping matches the collector's identity.
/// </summary>
public class DarlingPgStatementReaderTests
{
    private static string Sql => DarlingPgStatementReader.PgTopQueriesSql;

    /// <summary>
    /// calls, total time and rows have per-interval deltas in the store, so they are SUMmed. Summing
    /// their cumulative counterparts instead would multiply each query's whole lifetime by the number
    /// of snapshots in the window.
    /// </summary>
    [Fact]
    public void SumsTheDeltaColumnsForRateMetrics()
    {
        Assert.Contains("SUM(delta_calls)", Sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_total_exec_time_ms)", Sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_rows)", Sql, StringComparison.Ordinal);

        Assert.DoesNotContain("SUM(calls)", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SUM(rows_returned)", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SUM(total_exec_time_ms)", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The block and WAL columns keep no stored deltas, so they are differenced HERE rather than read as
    /// the window's MAX. Reading them as MAX put a lifetime cumulative figure in the same row as a
    /// windowed one, which a consumer cannot see and cannot correct for: it reads total_exec_time_ms for
    /// the last hour beside shared_blks_read since the last counter reset, and any per-call ratio it
    /// derives from the pair is nonsense.
    /// </summary>
    [Theory]
    [InlineData("shared_blks_hit")]
    [InlineData("shared_blks_read")]
    [InlineData("storage_blks_read")]
    [InlineData("orcache_blks_hit")]
    [InlineData("temp_blks_read")]
    [InlineData("temp_blks_written")]
    [InlineData("wal_bytes")]
    public void DifferencesTheCumulativeColumnsAcrossTheWindow(string column)
    {
        Assert.Contains($"LAG({column})", Sql, StringComparison.Ordinal);
        Assert.Contains($"SUM(d_{column})", Sql, StringComparison.Ordinal);

        /* The lifetime reading must not survive anywhere in the projection. */
        Assert.DoesNotContain($"MAX({column})", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain($"SUM({column})", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// GREATEST(..., 0) is what makes the differencing safe. A counter reset — an explicit
    /// pg_stat_statements_reset(), an eviction and re-entry, or a major-version upgrade (queryid is not
    /// stable across majors) — makes one interval negative, and an unclamped difference would report
    /// that as a large negative figure. Clamping drops the reset interval and keeps the rest, the same
    /// rule the stored delta machinery applies.
    /// </summary>
    [Fact]
    public void ClampsEachIntervalAtZeroSoACounterResetCannotGoNegative()
    {
        var clamped = Sql.Split("GREATEST(").Length - 1;

        /* One per differenced column — seven of them, all clamped, none left bare. */
        Assert.Equal(7, clamped);
        Assert.Equal(7, Sql.Split("OVER series, 0)").Length - 1);
    }

    /// <summary>
    /// The LAG partition must be the FULL series identity, matching how the stored deltas are keyed. The
    /// same normalized statement run by another user or against another database is a separate
    /// pg_stat_statements entry with its own counters, so differencing across those would interleave
    /// unrelated series and produce garbage intervals.
    /// </summary>
    [Fact]
    public void DifferencesWithinTheFullSeriesIdentityNotJustTheQueryId()
    {
        Assert.Contains("PARTITION BY queryid, database_id, user_id, toplevel", Sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time", Sql, StringComparison.Ordinal);

        /* Rolled up to the coarser grain a "top queries" answer wants, AFTER the differencing. */
        var windowAt = Sql.IndexOf("WINDOW series AS", StringComparison.Ordinal);
        var groupAt = Sql.IndexOf("GROUP BY differenced.queryid, database_id", StringComparison.Ordinal);
        Assert.True(windowAt < groupAt);
    }

    /// <summary>
    /// A series with one sample in the window has no measurable interval — its increment happened before
    /// the window began — so it must report 0, not its lifetime total. coalesce on the SUM, never on the
    /// cumulative column.
    /// </summary>
    [Fact]
    public void ASingleSampleSeriesReportsZeroRatherThanItsLifetimeTotal()
    {
        Assert.Contains("coalesce(SUM(d_shared_blks_read), 0)", Sql, StringComparison.Ordinal);
        Assert.Contains("coalesce(SUM(d_wal_bytes), 0)", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The two high-water marks are NOT counters and must stay MAX — differencing a high-water mark is
    /// meaningless, and summing one would invent a total that never occurred.
    /// </summary>
    [Fact]
    public void HighWaterMarksStayMax()
    {
        Assert.Contains("MAX(max_exec_time_ms)", Sql, StringComparison.Ordinal);
        Assert.Contains("MAX(max_exec_peakmem_bytes)", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(max_exec_peakmem_bytes)", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(max_exec_time_ms)", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Grouped by the collector's identity, not by queryid alone: the same normalized statement against
    /// a different database is a separate pg_stat_statements entry with its own counters.
    /// </summary>
    [Fact]
    public void GroupsByQueryIdentityNotQueryIdAlone()
    {
        /* #2554: QUALIFIED. The LEFT JOIN put t.queryid in scope, so a bare `queryid` here was ambiguous
           (42702) and the read threw on every call, on every engine. Pinned in the qualified form
           deliberately — reverting to the bare one restores a query that cannot parse, so this assertion
           says WHICH form is correct rather than merely that a GROUP BY exists. */
        Assert.Contains("GROUP BY differenced.queryid, database_id", Sql, StringComparison.Ordinal);
    }

    /// <summary>Total time is the currency: heaviest first, and bounded.</summary>
    [Fact]
    public void OrdersByTotalTimeAndBoundsTheResult()
    {
        Assert.Contains("ORDER BY SUM(delta_total_exec_time_ms) DESC", Sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Shapes that did not execute in the window are excluded. pg_stat_statements retains an entry long
    /// after its last execution, so without this the result is padded with idle shapes showing zero.
    /// </summary>
    [Fact]
    public void ExcludesShapesThatDidNotRunInTheWindow()
    {
        Assert.Contains("HAVING SUM(delta_total_exec_time_ms) > 0", Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void UsesTheStandardServerAndWindowParameters()
    {
        Assert.Contains("server_id = $1", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadsThePostgresStatementTable()
    {
        Assert.Contains("FROM pg_statement_stats", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_query_stats", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The Aurora I/O split columns are selected — they are the reason this reads
    /// aurora_stat_statements rather than the vanilla view, and dropping them would silently reduce
    /// this to a worse version of the community query.
    /// </summary>
    [Fact]
    public void SelectsTheAuroraIoSourceSplit()
    {
        Assert.Contains("storage_blks_read", Sql, StringComparison.Ordinal);
        Assert.Contains("orcache_blks_hit", Sql, StringComparison.Ordinal);
    }
}
