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
    /// <summary>All-types wait milliseconds per second — Aurora <c>pg_wait_stats</c> deltas (the engine's measured
    /// wait time). Stock sampling is NOT this metric: it has its own name below since lane 24.</summary>
    public const string PgWaitMsPerSec = "pg_wait_ms_per_sec";
    /// <summary>All-types SAMPLED wait milliseconds per second the sampler was watching — stock <c>pg_wait_sampling</c>
    /// <c>Δsample_count × profile_period_ms</c> over each collection's <c>sampled_ms</c> (V133; NULL = the whole
    /// interval), CPU/Running excluded. The same UNIT as <see cref="PgWaitMsPerSec"/> and a different INSTRUMENT
    /// (a per-backend-sample count quantised at the period), so it is its own name and its buckets are never
    /// pooled with the measured series (#3689 §5; #3691 lane 24).</summary>
    public const string PgSampledWaitMsPerSec = "pg_sampled_wait_ms_per_sec";
    /// <summary>Instance CPU as percent of the configured capacity ceiling — <c>pg_cpu_utilization.acu_utilization_percent</c> (Aurora only).</summary>
    public const string PgCpu = "pg_cpu";

    /* #3691 v2 plumbing: the four v2 PostgreSQL-target baselines. Names only — the CTE behind each is the
       content lane's (PgTargetBaselineProvider.{Io,Replication,Wal}.cs), and the provider's arm for a name
       whose lane has not landed answers null, which the shared reader treats as "no baseline for this metric". */
    /// <summary>Mean data-file read latency (ms per read) — the reset-aware <c>pg_io_stats</c> difference per collection. Lane 11.</summary>
    public const string PgIoReadLatency = "pg_io_read_latency";
    /// <summary>Replay lag in bytes on the worst standby — <c>pg_replication_stats</c> per collection. Lane 12.</summary>
    public const string PgReplayLagBytes = "pg_replay_lag_bytes";
    /// <summary>WAL bytes per second — the reset-aware <c>pg_write_stats</c> WAL counter difference over the interval. Lane 15.</summary>
    public const string PgWalBytesPerSec = "pg_wal_bytes_per_sec";
    /// <summary>Autovacuum workers busy per capture — declared for wave 2; no provider arm yet.</summary>
    public const string PgAutovacuumWorkers = "pg_autovacuum_workers";

    /* #3691 wave-3 plumbing (the between-waves batch): the blocking family's baseline. Name only, the v2 shape — the
       CTE is lane 17's (PgTargetBaselineProvider.Blocking.cs) and the arm answers null until it lands. */
    /// <summary>Sessions blocked per capture — the count of <c>pg_blocking_edges</c> waiters in each collection, a
    /// point series (no differencing). Lane 17.</summary>
    public const string PgBlockedSessions = "pg_blocked_sessions";

    /* #3691 v3 plumbing: the plan and kernel families' baselines. Names only, the v2 shape — the CTE behind each is
       its content lane's (PgTargetBaselineProvider.Plans.cs / .Kernel.cs) and the provider's arm answers null until
       it lands, which the shared reader treats as "no baseline for this metric". */
    /// <summary>A statement's mean execution ms per collection — the reset-aware <c>pg_statement_stats</c>
    /// <c>total_exec_time / calls</c> difference. Two arms answer to this one name since lane 33: the UNKEYED arm (lane
    /// 27's, the SERVER-WIDE per-call mean — the reader keyed a series on (server, metric) alone when it was written, so
    /// WHICH statement it names was lane 27's decision, and it chose the server) and the KEYED arm (lane 33's, one
    /// statement's per-call mean, reached through the five-argument overload with the <c>queryid</c> as the key) —
    /// so lane 27's follow-up can switch its detector to the statement's own series without a new name. Lane 27 / 33.</summary>
    public const string PgStatementMeanMs = "pg_statement_mean_ms";

    /* #3691 lane 33: the first KEYED metric. Since lane 33 the reader has a key dimension — the five-argument
       PgBaselineProvider.GetBaselineAsync overload keys a series on (server, metric, key), and a provider declares which
       metrics are keyed through ResolveKeyedBaselineQuery — so this name has NO unkeyed arm at all (a statement's share
       of the server's total is 1.0 by construction when there is no statement) and answers only when a queryid is
       passed as the key. Lane 34 is the consumer (the bad-actor share re-graded as deviation from the statement's OWN
       hour-of-week share, Erik's 2026-09-20 ruling). */
    /// <summary>ONE statement's share of the collection's total execution time — Σ <c>delta_total_exec_time_ms</c> for
    /// the keyed <c>queryid</c> over Σ for every statement row of the same <c>collection_time</c>, from
    /// <c>pg_statement_stats</c>' stored deltas, a fraction in [0, 1] per collection. Keyed by <c>queryid</c> as text;
    /// no server-wide arm exists. Lane 33 (the seam), lane 34 (the consumer).</summary>
    public const string PgStatementShare = "pg_statement_share";
    /// <summary>Cores busy — user-plus-system CPU seconds per wall second from the reset-aware <c>pg_kernel_stats</c>
    /// differences summed across statements per collection. Lane 28.</summary>
    public const string PgCpuBurnCores = "pg_cpu_burn_cores";
}
