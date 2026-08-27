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
/// Pins the freeze-headroom read: a level is read as the latest value plus the window peak, never
/// aggregated, and the severity ladder maps to real PostgreSQL behaviour changes.
/// </summary>
public class DarlingPgWraparoundReaderTests
{
    private static string Sql => DarlingPgWraparoundReader.PgWraparoundSql;

    /// <summary>
    /// Freeze age is a level, not accumulated work. Averaging it would blur the only number that
    /// matters and summing it would be meaningless — the opposite discipline from the rate collectors,
    /// where summing deltas is mandatory.
    /// </summary>
    [Fact]
    public void ReadsTheLatestValuePerDatabaseRatherThanAggregating()
    {
        Assert.Contains("DISTINCT ON (database_name)", Sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY database_name, collection_time DESC", Sql, StringComparison.Ordinal);

        Assert.DoesNotContain("SUM(frozen_xid_age)", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("AVG(frozen_xid_age)", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The window peak is what distinguishes a healthy freezing sawtooth from a monotonic climb, so both
    /// counters carry one. Computed as window functions so they see every row in the window, not only
    /// the row DISTINCT ON keeps.
    /// </summary>
    [Fact]
    public void CarriesAWindowPeakForBothCounters()
    {
        Assert.Contains("MAX(frozen_xid_age) OVER (PARTITION BY database_name)", Sql, StringComparison.Ordinal);
        Assert.Contains("MAX(min_multixid_age) OVER (PARTITION BY database_name)", Sql, StringComparison.Ordinal);
    }

    /// <summary>Both independent counters are read; reporting one would give false comfort.</summary>
    [Fact]
    public void ReadsBothTransactionIdAndMultiXactColumns()
    {
        foreach (var column in new[]
                 {
                     "frozen_xid_age", "min_multixid_age",
                     "pct_toward_wraparound", "pct_toward_multixact_wraparound",
                     "pct_toward_emergency_vacuum", "pct_toward_multixact_emergency",
                     "xids_remaining", "multixids_remaining",
                 })
        {
            Assert.Contains(column, Sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void UsesTheStandardServerAndWindowParameters()
    {
        Assert.Contains("server_id = $1", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", Sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadsTheWraparoundTable()
    {
        Assert.Contains("FROM pg_wraparound_stats", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Each severity boundary is a documented PostgreSQL behaviour change, not a round number: failsafe
    /// mode around 1.6B ids, server warnings near 40M remaining. An "ok" that quietly spans the failsafe
    /// range would be worse than no label at all.
    /// </summary>
    [Theory]
    [InlineData(10.0, 20.0, "ok")]
    [InlineData(10.0, 100.0, "info_anti_wraparound_vacuum_expected")]
    [InlineData(60.0, 100.0, "warning")]
    [InlineData(80.0, 100.0, "critical_failsafe_range")]
    [InlineData(99.0, 100.0, "critical_wraparound_imminent")]
    public void ClassifiesSeverityFromTheDocumentedLadder(double pctWrap, double pctEmergency, string expected)
    {
        Assert.Equal(expected, DarlingMcpPgWraparoundTools.Classify(pctWrap, pctEmergency));
    }

    /// <summary>
    /// A database past its emergency-vacuum threshold but nowhere near the ceiling is INFO, not a
    /// warning: a forced anti-wraparound vacuum is normal operation, and treating it as an alert is how
    /// wraparound monitoring earns a reputation for crying wolf.
    /// </summary>
    [Fact]
    public void AntiWraparoundVacuumAloneIsInformationalNotAWarning()
    {
        Assert.Equal("info_anti_wraparound_vacuum_expected", DarlingMcpPgWraparoundTools.Classify(9.3, 100.0));
    }
}
