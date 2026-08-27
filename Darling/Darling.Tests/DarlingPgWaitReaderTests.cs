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
/// Pins the read side of pg_wait_stats: the aggregation reads deltas rather than raw counters, keeps
/// unnamed events visible, and converts the stored microseconds exactly once.
/// </summary>
public class DarlingPgWaitReaderTests
{
    private static string Sql => DarlingPgWaitReader.PgWaitStatsSql;

    /// <summary>
    /// The single most important property of this query. The store holds CUMULATIVE counters plus
    /// per-interval deltas; summing the cumulative columns across snapshots would multiply the entire
    /// history by the number of snapshots in the window — a number that looks plausible and is wrong
    /// by orders of magnitude.
    /// </summary>
    [Fact]
    public void AggregatesDeltasNeverRawCumulativeCounters()
    {
        Assert.Contains("SUM(delta_waits)", Sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_wait_time_us)", Sql, StringComparison.Ordinal);

        Assert.DoesNotContain("SUM(waits)", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SUM(wait_time_us)", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// wait_type and wait_event are nullable because their lookups are LEFT JOINed in the collector. An
    /// event Aurora reports but does not name is the new-wait-type case an operator most wants to see,
    /// so it gets a synthetic label from the numeric ids instead of being dropped by the GROUP BY.
    /// </summary>
    [Fact]
    public void SurfacesUnnamedEventsInsteadOfDroppingThem()
    {
        Assert.Contains("COALESCE(wait_type", Sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(wait_event", Sql, StringComparison.Ordinal);
        Assert.Contains("unknown_type_", Sql, StringComparison.Ordinal);
        Assert.Contains("unknown_event_", Sql, StringComparison.Ordinal);

        /* No WHERE clause filtering the nulls away, which would defeat the COALESCE. */
        Assert.DoesNotContain("wait_event IS NOT NULL", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("wait_type IS NOT NULL", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The store keeps microseconds because that is what Aurora reports. Conversion happens once, in
    /// the read layer, so no consumer has to remember the unit — and the division is float, not
    /// integer, so sub-millisecond events do not collapse to zero.
    /// </summary>
    [Fact]
    public void ConvertsMicrosecondsToMillisecondsWithoutIntegerTruncation()
    {
        Assert.Contains("/ 1000.0", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("/ 1000 ", Sql, StringComparison.Ordinal);
    }

    /// <summary>Guarded average: a window where an event accrued time but zero waits must not divide by zero.</summary>
    [Fact]
    public void GuardsTheAverageAgainstZeroWaits()
    {
        Assert.Contains("WHEN SUM(delta_waits) > 0", Sql, StringComparison.Ordinal);
        Assert.Contains("ELSE 0", Sql, StringComparison.Ordinal);
    }

    /// <summary>Same windowing contract as every other read in this store: $1 server, $2/$3 bounds.</summary>
    [Fact]
    public void UsesTheStandardServerAndWindowParameters()
    {
        Assert.Contains("server_id = $1", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", Sql, StringComparison.Ordinal);
    }

    /// <summary>Heaviest first, and bounded — an operator reads the top of this list, not all of it.</summary>
    [Fact]
    public void OrdersByWeightAndBoundsTheResult()
    {
        Assert.Contains("ORDER BY SUM(delta_wait_time_us) DESC", Sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Rows that accrued no time in the window are excluded. Without this the result is padded with
    /// every event the instance has ever seen, each showing zero, burying the handful that moved.
    /// </summary>
    [Fact]
    public void ExcludesEventsThatDidNotMoveInTheWindow()
    {
        Assert.Contains("HAVING SUM(delta_wait_time_us) > 0", Sql, StringComparison.Ordinal);
    }

    /// <summary>Reads the collector's table, not the SQL Server one.</summary>
    [Fact]
    public void ReadsThePostgresWaitTable()
    {
        Assert.Contains("FROM pg_wait_stats", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_wait_stats", Sql, StringComparison.Ordinal);
    }
}
