/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the High CPU alert's read (#2719). Structural SQL-text assertions rather than a live query, matching
/// <see cref="DarlingPgSessionStatesReaderTests"/>'s own pattern for this class of read — the shape was
/// verified against a real local PostgreSQL 17 (freshest non-null reading wins, a stale row is excluded, a
/// null-cpu_percent row is excluded even when it is the freshest sample, a different server's row never
/// leaks in) before this test was written; this pins the SQL that made that hold.
/// </summary>
public class DarlingPgCpuUtilizationReaderTests
{
    private static string Sql => DarlingPgCpuUtilizationReader.LatestCpuSql;

    [Fact]
    public void LatestCpuSql_ScopesToOneServerAndAFreshnessFloor()
    {
        Assert.Contains("server_id = $1", Sql, StringComparison.Ordinal);
        Assert.Contains("sample_time >= $2", Sql, StringComparison.Ordinal);
    }

    /// <summary>A row with no cpu_percent (a PI data point for a period with no sample) must never win over
    /// an older row that actually has a value — the alert has nothing to compare against a null.</summary>
    [Fact]
    public void LatestCpuSql_ExcludesNullReadings()
    {
        Assert.Contains("cpu_percent IS NOT NULL", Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void LatestCpuSql_OrdersByNewestSampleAndTakesOne()
    {
        Assert.Contains("ORDER BY sample_time DESC", Sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void LatestCpuSql_ReadsOnlyItsOwnTable()
    {
        Assert.Contains("FROM pg_cpu_utilization", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Freshness is wider than the collector's 5-minute cadence but narrower than the Tier 0 predictors' 2
    /// hours — see the field's own doc comment for why CPU needs a tighter bound than a wraparound age does.
    /// </summary>
    [Fact]
    public void Freshness_IsFifteenMinutes()
    {
        Assert.Equal(TimeSpan.FromMinutes(15), DarlingPgCpuUtilizationReader.Freshness);
    }
}
