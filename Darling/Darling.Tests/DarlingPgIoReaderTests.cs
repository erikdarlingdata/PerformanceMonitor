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
/// Pins the pg_stat_io read: windowed differences clamped per interval, NULL surviving the arithmetic so
/// "not measured" stays distinguishable from "measured zero", and a distinct explanation per context.
/// </summary>
public class DarlingPgIoReaderTests
{
    private static string Sql => DarlingPgIoReader.PgIoSql;

    /// <summary>
    /// The triple is the series identity, so the LAG must partition on all three. Partitioning on fewer
    /// would difference unrelated series against each other — a client backend's reads against the
    /// checkpointer's — and produce garbage intervals.
    /// </summary>
    [Fact]
    public void DifferencesWithinTheFullBackendObjectContextTriple()
    {
        Assert.Contains("PARTITION BY backend_type, object_type, context", Sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time", Sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY backend_type, object_type, context", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every differenced counter is clamped at zero per interval, so pg_stat_reset_shared('io') or a
    /// restart drops one interval instead of producing a large negative figure.
    /// </summary>
    [Theory]
    [InlineData("reads")]
    [InlineData("read_time_ms")]
    [InlineData("hits")]
    [InlineData("extends")]
    [InlineData("extend_time_ms")]
    [InlineData("evictions")]
    [InlineData("reuses")]
    [InlineData("writes")]
    [InlineData("write_time_ms")]
    public void ClampsEveryDifferencedCounterAtZero(string column)
    {
        Assert.Contains($"GREATEST({column}", Sql, StringComparison.Ordinal);
        Assert.Contains($"LAG({column})", Sql, StringComparison.Ordinal);
        Assert.Contains($"SUM(d_{column})", Sql, StringComparison.Ordinal);

        /* No lifetime cumulative reading may reach the projection. */
        Assert.DoesNotContain($"MAX({column})", Sql, StringComparison.Ordinal);
    }

    /// <summary>All nine clamps present and none left bare.</summary>
    [Fact]
    public void ClampCountMatchesTheDifferencedColumnCount()
    {
        /* 9 counters before V101 (#2655) plus the three byte totals PostgreSQL 18 measures. Every
           differenced column needs its own clamp: these are cumulative, so an unclamped one goes negative
           on a stats reset or a restart and drags the window's sum below what actually happened. */
        Assert.Equal(12, Sql.Split("GREATEST(").Length - 1);
        Assert.Equal(12, Sql.Split("OVER series, 0)").Length - 1);
    }

    /// <summary>
    /// The load-bearing distinction. On Aurora every write counter is NULL because backends there do not
    /// write data files, so the read has to report whether writes are TRACKED separately from their value.
    /// Without it a caller cannot tell "no writes happened" from "writes are not measured here", and would
    /// read the second as the first.
    /// </summary>
    [Fact]
    public void ReportsWhetherWriteCountersAreTrackedAtAll()
    {
        Assert.Contains("(writes IS NOT NULL) AS writes_tracked", Sql, StringComparison.Ordinal);
        Assert.Contains("bool_or(writes_tracked)", Sql, StringComparison.Ordinal);
        Assert.Contains("write_counters_tracked", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Only combinations that actually moved. An idle triple is not a finding and would crowd out the ones
    /// that are — and the HAVING has to consider all four activity counters, not just reads, or a
    /// write-only or extend-only combination would vanish.
    /// </summary>
    [Fact]
    public void FiltersToCombinationsThatMovedUsingEveryActivityCounter()
    {
        var having = Sql[Sql.IndexOf("HAVING", StringComparison.Ordinal)..];

        foreach (var counter in new[] { "d_reads", "d_writes", "d_extends", "d_hits" })
        {
            Assert.Contains(counter, having, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Ordered by read TIME first, then read count. Time is what a user feels; a combination doing many
    /// cheap reads matters less than one doing fewer expensive ones, and ordering by count would invert
    /// exactly that.
    /// </summary>
    [Fact]
    public void OrdersByReadTimeBeforeReadCount()
    {
        var order = Sql[Sql.IndexOf("ORDER BY coalesce", StringComparison.Ordinal)..];
        var timeAt = order.IndexOf("d_read_time_ms", StringComparison.Ordinal);
        var countAt = order.IndexOf("d_reads", StringComparison.Ordinal);

        Assert.True(timeAt >= 0 && countAt >= 0 && timeAt < countAt);
    }

    [Fact]
    public void ReadsTheIoTableAndBoundsTheRowCount()
    {
        Assert.Contains("FROM pg_io_stats", Sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", Sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every context PostgreSQL and Aurora emit gets its own explanation, and the explanations are
    /// genuinely different — the context is the dimension that changes the REMEDY, so a generic message
    /// would defeat the purpose of surfacing it.
    /// </summary>
    [Theory]
    [InlineData("normal", "shared_buffers")]
    [InlineData("bulkread", "bypass")]
    [InlineData("bulkwrite", "ring buffer")]
    [InlineData("vacuum", "autovacuum")]
    [InlineData("index", "Index")]
    [InlineData("walreplay", "standby")]
    public void EveryContextHasItsOwnMeaning(string context, string expectedFragment)
    {
        var meaning = DarlingMcpPgIoTools.ContextMeaning(context);

        Assert.Contains(expectedFragment, meaning, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Unrecognized", meaning, StringComparison.Ordinal);
    }

    /// <summary>
    /// The bulkread explanation must say adding memory will NOT help. That is the single most common
    /// misreading of this view: high bulkread volume looks like memory pressure and is not, because those
    /// reads use a small ring buffer by design.
    /// </summary>
    [Fact]
    public void BulkreadExplanationWarnsThatMoreMemoryWillNotHelp()
    {
        var meaning = DarlingMcpPgIoTools.ContextMeaning("bulkread");

        Assert.Contains("will not", meaning, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("shared_buffers", meaning, StringComparison.Ordinal);
    }

    [Fact]
    public void ContextMeaningsAreDistinct()
    {
        var meanings = new[] { "normal", "bulkread", "bulkwrite", "vacuum", "index", "walreplay" }
            .Select(DarlingMcpPgIoTools.ContextMeaning)
            .ToArray();

        Assert.Equal(meanings.Length, meanings.Distinct().Count());
    }

    [Fact]
    public void UnknownContextIsReportedRatherThanGuessedAt()
    {
        Assert.Contains("Unrecognized", DarlingMcpPgIoTools.ContextMeaning("something_new"), StringComparison.Ordinal);
    }
}
