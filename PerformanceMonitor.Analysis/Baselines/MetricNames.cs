/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis.Baselines;

/// <summary>Metric name constants used as baseline cache keys. Shared by Lite + Darling so the two
/// active apps key their baselines identically (the deprecated Dashboard keeps its own
/// <c>SqlServerMetricNames</c>).</summary>
public static class MetricNames
{
    public const string Cpu = "cpu";
    public const string BatchRequests = "batch_requests";
    public const string WaitStats = "wait_stats";
    public const string SessionCount = "session_count";
    public const string QueryDuration = "query_duration";
    public const string IoLatency = "io_latency";
    public const string Blocking = "blocking";
    public const string Deadlock = "deadlock";
    public const string Memory = "memory";

    // Chart-unit metrics (for UI bands — units match what the chart displays)
    public const string WaitMsPerSec = "wait_ms_per_sec";
    public const string BlockingPerMinute = "blocking_per_minute";

    /* #3542 lane 9: the PostgreSQL-TARGET baselines, computed by PgTargetBaselineProvider over the pg_* raw
       hypertables. Prefixed so a PostgreSQL bucket can never be served from — or cached under — a SQL Server
       metric's key on the same store (the provider caches by "{serverId}:{metricName}", and a server has one
       engine, but the names are the only thing that makes the two families disjoint by construction). Declared
       HERE and not in a Darling-local twin: SharedBaselineModelPinTests pins that Darling holds no private copy
       of the baseline model, and a metric-name registry split by assembly would be the first such copy. */
    /// <summary>Transactions per second — <c>pg_database_stats</c> <c>xact_commit + xact_rollback</c> deltas over the interval.</summary>
    public const string PgTps = "pg_tps";
    /// <summary>Instance-wide session count — <c>pg_session_states</c>' denormalised <c>total_sessions</c> per capture.</summary>
    public const string PgSessionCount = "pg_session_count";
    /// <summary>Deadlocks per hour — the reset-aware <c>pg_database_stats.deadlocks</c> delta, rated over the interval.</summary>
    public const string PgDeadlockRate = "pg_deadlock_rate";
    /// <summary>All-types wait milliseconds per second — Aurora <c>pg_wait_stats</c> deltas; stock sampling has no arm in v1.</summary>
    public const string PgWaitMsPerSec = "pg_wait_ms_per_sec";
    /// <summary>Instance CPU as percent of the configured capacity ceiling — <c>pg_cpu_utilization.acu_utilization_percent</c> (Aurora only).</summary>
    public const string PgCpu = "pg_cpu";
}
