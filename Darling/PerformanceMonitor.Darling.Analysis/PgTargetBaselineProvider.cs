/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Baselines for a PostgreSQL-target pass (#3542): <see cref="PgBaselineProvider"/> with ONE thing swapped —
/// which SQL computes a metric's hour×day-of-week buckets. Everything else is inherited by construction
/// rather than copied: the bucket cache, the naive-UTC parameter binding, the eight-column robust reader,
/// the timeout classification (<see cref="PgBaselineProvider.IsCommandTimeout"/> — one definition,
/// <c>BaselineTimeoutIsNamedTests</c>) and the degrade-to-<c>BaselineBucket.Empty</c> posture. The base
/// class's <see cref="PgBaselineProvider.ResolveBaselineQuery"/> is the seam.
///
/// <para>Each metric's SQL is one <c>WITH clean AS (SELECT collection_time, &lt;value&gt; AS v FROM &lt;raw
/// table&gt; WHERE server_id = $1 AND collection_time &gt;= $2 AND collection_time &lt; $3 …)</c> CTE followed
/// by <see cref="PgBaselineProvider.RobustTierScaffold"/> — the cheapest possible port shape, and the same
/// raw-hypertable precedent the SQL Server CPU and I/O metrics already argue for in-code. The PostgreSQL raw
/// tables carry 30-day retention, which exactly covers the 30-day baseline window (D10): if those tables are
/// ever enrolled in the 4-day-raw CAGG tiering, PostgreSQL baseline CAGGs become a prerequisite and this
/// comment is where that dependency is written down.</para>
///
/// <para>Skeleton in the plumbing lane: every metric resolves to null ("no baseline"), so the filled
/// detectors' <c>AnomalyGate</c> calls take the never-blind absolute-fallback path until lane 9 lands the five
/// <c>clean</c> CTEs (TPS, session count, deadlock rate, wait ms/sec, CPU) under metric names added to the
/// shared <c>MetricNames</c>.</para>
/// </summary>
public sealed class PgTargetBaselineProvider : PgBaselineProvider
{
    public PgTargetBaselineProvider(NpgsqlDataSource postgres, ILogger? logger = null)
        : base(postgres, logger)
    {
    }

    /// <summary>
    /// The PostgreSQL-target metric → SQL map. Internal so Darling.Tests can pin every query's table and shape
    /// ungated, as <c>PgBaselineProvider.GetBaselineQuery</c> is pinned.
    /// <para>/* filled by lane 9 — one arm per metric name, each `WITH clean AS (…)," + RobustTierScaffold`. */</para>
    /// </summary>
    internal static string? GetPgTargetBaselineQuery(string metricName) => metricName switch
    {
        _ => null,
    };

    protected override string? ResolveBaselineQuery(string metricName) => GetPgTargetBaselineQuery(metricName);
}
