/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Baselines for a PostgreSQL-target pass (#3542): <see cref="PgBaselineProvider"/> with TWO things swapped —
/// which SQL computes a metric's hour×day-of-week buckets, and where the target's clock is read from (#3691,
/// <c>PgTargetBaselineProvider.Clock.cs</c>). Everything else is inherited by construction rather than copied:
/// the bucket cache, the naive-UTC parameter binding, the eight-column robust reader, the timeout
/// classification (<see cref="PgBaselineProvider.IsCommandTimeout"/> — one definition,
/// <c>BaselineTimeoutIsNamedTests</c>) and the degrade-to-<c>BaselineBucket.Empty</c> posture. The base
/// class's <see cref="PgBaselineProvider.ResolveBaselineQuery"/> and <see cref="PgBaselineProvider.ReadServerClockAsync"/>
/// are the seams.
///
/// <para><b>The clock the buckets key on (#3749 Q6, then #3691).</b> Since #3749 the base binds six parameters, not
/// three: <c>$1</c> server_id, <c>$2</c> window start and <c>$3</c> analysis time as before, then <c>$4..$6</c> —
/// the one offset transition inside the window and the offset minutes before/after it — and the shared
/// <see cref="PgBaselineProvider.RobustTierScaffold"/> keys hour, day-of-week and the distinct-day date on
/// <c>BaselineLocalClock.LocalCollectionTimeSql</c> rather than bare <c>collection_time</c>, so every arm below
/// inherits the target-local key without naming it (the census in <c>LocalClockBucketKeyTests</c> is what forbids
/// an arm from keying on anything else). PostgreSQL targets resolve their clock from the <c>TimeZone</c> setting in
/// the latest <c>pg_server_config</c> snapshot at or before the window end; UTC only when no snapshot carries a
/// server-scoped one. Before #3691 UTC was the RULE for this engine — the base read the clock from
/// <c>server_properties</c>, a table no PostgreSQL collector writes, so every PostgreSQL bucket was UTC hour-of-week
/// while the SQL Server side was local; it is now the FALLBACK, and a target that keeps <c>UTC</c> keys exactly as it
/// did.</para>
///
/// <para>Each metric's SQL is one <c>WITH clean AS (SELECT collection_time, &lt;value&gt; AS v FROM &lt;raw
/// table&gt; WHERE server_id = $1 AND collection_time &gt;= $2 AND collection_time &lt; $3 …)</c> CTE followed
/// by <see cref="PgBaselineProvider.RobustTierScaffold"/> — the cheapest possible port shape, and the same
/// raw-hypertable precedent the SQL Server CPU and I/O metrics already argue for in-code. The PostgreSQL raw
/// tables carry 30-day retention, which exactly covers the 30-day baseline window (D10): if those tables are
/// ever enrolled in the 4-day-raw CAGG tiering, PostgreSQL baseline CAGGs become a prerequisite and this
/// comment is where that dependency is written down.</para>
///
/// <para><b>The retention dependency, stated (D10).</b> Every table the arms read is purged service-side at
/// <c>CollectorScheduleDefaults.All[table].RetentionDays</c> — the v1 four (<c>pg_database_stats</c>,
/// <c>pg_session_states</c>, <c>pg_wait_stats</c>, <c>pg_cpu_utilization</c>), the v2 three (<c>pg_io_stats</c>,
/// lane 11; <c>pg_replication_stats</c>, lane 12; <c>pg_write_stats</c>, lane 15) and <c>pg_wait_sampling</c>
/// (lane 24) — and NONE is in
/// <c>TimescaleSupport.RawTierCoverage</c>, the 4-day raw tier that #1757 found under the SQL Server baselines —
/// <c>PgTargetAnomalyTests</c> pins both halves. So the supply is exactly the window: a 30-day question over 30
/// days of rows, with the purge grain eating the oldest sliver. Retention is user-editable per collector, and
/// <c>DarlingRetentionHorizons.BaselineServingRawCollectors</c> floors the purge horizon at the baseline window
/// for every one of these exactly as it does for the SQL Server raw-reading arms (<c>cpu_utilization</c>,
/// <c>file_io_stats</c>) — the v1 four since #3711, lane 12's table with its lane, lanes 11 and 15's with the
/// #3691 between-waves batch; <c>BaselineSupplyTests</c> derives the floored set from the arms' own query text,
/// so an arm added here without a floor fails there. If these tables are ever tiered to 4-day raw, PostgreSQL
/// baseline CAGGs become a prerequisite and this comment is where that is written down.</para>
///
/// <para><b>What each metric is, in one line</b> (the arms below carry the rest): <c>pg_tps</c> — transactions
/// per second per collection, the reset-aware per-database difference the <c>PG_TPS</c> fact takes, summed and
/// rated over the collection's own gap; <c>pg_session_count</c> — the denormalised <c>total_sessions</c> of
/// each capture; <c>pg_deadlock_rate</c> — deadlocks per HOUR per collection off the same difference;
/// <c>pg_wait_ms_per_sec</c> — the Aurora all-types wait rate under the three-state interval, CPU excluded;
/// <c>pg_cpu</c> — percent of the configured capacity ceiling (Aurora only); <c>pg_sampled_wait_ms_per_sec</c>
/// (lane 24, #3691) — stock PostgreSQL's SAMPLED wait rate over the time the sampler was watching
/// (<c>sampled_ms</c>, V133). v1 gave the sampled estimate NO baseline because a per-backend-sample count
/// quantised at <c>profile_period</c> has a different noise distribution from a measured microsecond sum, and a
/// bucket that mixed the two (or a threshold tuned on one applied to the other) would be the unit error
/// adversarial item A names; the arm that arrived is its own metric name, never <c>pg_wait_ms_per_sec</c>, and
/// <c>pg_wait_sampling</c> joined the floored set with it.</para>
/// </summary>
public sealed partial class PgTargetBaselineProvider : PgBaselineProvider
{
    public PgTargetBaselineProvider(NpgsqlDataSource postgres, ILogger? logger = null)
        : base(postgres, logger)
    {
    }

    /* The per-database difference, verbatim from DarlingPgDatabaseReader.PgDatabaseSql through the PG_TPS fact's
       read (PgTargetFactCollector.Database.cs) — the sixth site of the shape, and it must agree with the other
       five or the baseline a rate is judged against is not the rate the fact states. Per-series LAG under
       PARTITION BY database_name; GREATEST(raw, 0) so a rewound counter adds nothing; the explicit reset recorded
       as ROW_NUMBER() > 1 AND stats_reset IS DISTINCT FROM its LAG (the NULL → timestamp first-reset trap that
       read documents, not re-derived here). The interval is the series' own gap — LAG(collection_time) — taken
       as MAX per collection so every database's row in one collection shares one denominator; a collection
       whose predecessor is missing (the window's first, or a database's first sighting) has a NULL gap and
       drops out of clean, so the first row after a collector outage is rated over the whole outage (a correct
       per-second rate) and the first row of a series is never rated at all. */
    private const string DatabaseCounterDeltasCte = @"
WITH sampled AS (
    SELECT database_name,
           collection_time,
           (xact_commit + xact_rollback) - LAG(xact_commit + xact_rollback) OVER series AS raw_xacts,
           deadlocks - LAG(deadlocks) OVER series AS raw_deadlocks,
           extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER series))) AS interval_sec,
           (ROW_NUMBER() OVER series > 1
            AND stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series) AS reset_here
    FROM pg_database_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    WINDOW series AS (
        PARTITION BY database_name
        ORDER BY collection_time
    )
),
per_collection AS (
    SELECT collection_time,
           SUM(GREATEST(raw_xacts, 0))::DOUBLE PRECISION AS xacts,
           SUM(GREATEST(raw_deadlocks, 0))::DOUBLE PRECISION AS deadlocks,
           MAX(interval_sec) AS interval_sec
    FROM sampled
    WHERE raw_xacts IS NOT NULL
    GROUP BY collection_time
),";

    /// <summary>
    /// The PostgreSQL-target metric → SQL map. Internal so Darling.Tests can pin every query's table and shape
    /// ungated, as <c>PgBaselineProvider.GetBaselineQuery</c> is pinned. One arm per metric name, each a CTE chain
    /// ending in <c>clean(collection_time, v)</c> followed by the ONE <see cref="PgBaselineProvider.RobustTierScaffold"/>,
    /// so every PostgreSQL bucket carries the eight robust columns and the sentinel tiers. Window bounds are
    /// <c>&gt;= $2 AND &lt; $3</c>, the SQL Server arms' half-open shape, and every bound is a parameter — the base
    /// class binds <c>analysisTime.AddDays(-BaselineWindowDays)</c> and <c>analysisTime</c> naive-UTC, never a bare
    /// <c>now()</c>, so an anchored pass (#2506) baselines the 30 days before ITS window — and, since #3749, the
    /// three clock parameters <c>$4..$6</c> the scaffold's key consumes (class summary), which an arm never names.
    /// An arm's own text references only <c>$1..$3</c>; neither PostgreSQL nor Npgsql objects to the three it does
    /// not use (measured in #3749), and the scaffold appended to it is where they are read.
    /// </summary>
    internal static string? GetPgTargetBaselineQuery(string metricName) => metricName switch
    {
        /* Transactions per second: the summed per-database difference over the collection's own gap. The
           ::DOUBLE PRECISION cast is the io-arm rule — STDDEV_SAMP over numeric can overflow System.Decimal at
           materialization. No restart-signature exclusion (the SQL Server batch-request arm drops the first 0
           after a >1000 sample): the PostgreSQL counters are differenced with the reset recorded explicitly and
           a rewind clamped to 0, so a restart reads as ONE zero-or-small interval, which a median/MAD frame
           absorbs — the classical arm needed the exclusion because a restart poisoned a mean. */
        MetricNames.PgTps => DatabaseCounterDeltasCte + @"
clean AS (
    SELECT collection_time, xacts / interval_sec AS v
    FROM per_collection
    WHERE interval_sec > 0
)," + RobustTierScaffold,

        /* Deadlocks per HOUR per collection, so the bucket mean is directly comparable to the window's
           deadlocks / observed hours (the PG_DEADLOCK_RATE fact's unit) and the ratio is unit-free. Mostly zeros
           on a healthy server, which is the point: the median and MAD sit at 0, EffectiveRobustSigma reads 0,
           and the detector judges the RATIO against the mean — the event-family shape — rather than a modified
           z against a collapsed frame. */
        MetricNames.PgDeadlockRate => DatabaseCounterDeltasCte + @"
clean AS (
    SELECT collection_time, deadlocks * 3600.0 / interval_sec AS v
    FROM per_collection
    WHERE interval_sec > 0
)," + RobustTierScaffold,

        /* The instance-wide session count of each capture: MAX(total_sessions) per collection_time is a PICK,
           not an aggregate — pg_session_states repeats the instance totals on every stored row so any one row
           answers "out of how many" (PgTargetFactCollector.Sessions.cs states the V86 design). The same read
           the PG_CONNECTION_SATURATION fact takes its peak from, so the anomaly and the fact it folds into
           count the same thing. Two properties of the source ride into the baseline and the detector's doc
           says them: it is an EXCEPTION table, storing a capture only when some session was over the
           collector's floors, so quiet minutes are ABSENT and the buckets describe the captures that had
           something to report (the median sits high of the true median; the detector's peak-vs-bucket
           comparison is like-for-like because the window read has the same gap); and total_sessions counts
           PostgreSQL's own background processes (backend_type redacts), a few points high on every row alike. */
        MetricNames.PgSessionCount => @"
WITH clean AS (
    SELECT collection_time, MAX(total_sessions)::DOUBLE PRECISION AS v
    FROM pg_session_states
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   total_sessions IS NOT NULL
    GROUP BY collection_time
)," + RobustTierScaffold,

        /* The Aurora all-types wait rate, ms per second, CPU excluded — PgAnomalyDetector.WaitRateWindowSql's
           per_collection over pg_wait_stats with the wait partial's three-state interval
           (PgTargetFactCollector.Waits.cs): the STORED sample_interval_seconds (MAX over the collection's rows;
           0 only when every row was unknowable — a restart) through NULLIF so a restart collection is NOT a
           sample, a pre-V128 NULL falling back to LAG(collection_time). Never ELSE 0. `CPU` is a wait TYPE on
           Aurora (on-CPU time under the same function) and is excluded here exactly as the wait facts' share
           denominator excludes it, so the window's rate and its baseline agree on what "waiting" is. Only
           pg_wait_stats: stock's pg_wait_sampling estimate has no arm in v1 (class summary). */
        MetricNames.PgWaitMsPerSec => @"
WITH per_collection AS (
    SELECT collection_time,
           CAST(SUM(GREATEST(delta_wait_time_us, 0)) FILTER (WHERE lower(wait_type) IS DISTINCT FROM 'cpu') AS DOUBLE PRECISION) / 1000.0 AS total_wait_ms,
           CASE WHEN MAX(sample_interval_seconds) IS NULL
                THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
                ELSE NULLIF(MAX(sample_interval_seconds), 0)
           END AS interval_sec
    FROM pg_wait_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY collection_time
),
clean AS (
    SELECT collection_time, coalesce(total_wait_ms, 0) / interval_sec AS v
    FROM per_collection
    WHERE interval_sec > 0
)," + RobustTierScaffold,

        /* Percent of the CONFIGURED capacity ceiling — acu_utilization_percent, never cpu_percent (#3281: on the
           db.serverless class the whole measured fleet runs, cpu_percent is percent of the capacity CURRENTLY
           ALLOCATED, which is re-sized continuously, so a one-vCPU minute reads 100 while the instance sits at a
           third of its ceiling; the fleet card bands on the ACU reading for the same reason). Rows with no
           capacity sample contribute nothing rather than a percentage of an unknown denominator; a provisioned
           instance (ACU always NULL) therefore has no CPU baseline and its PG_CPU_PERCENT fact says the reading
           is not graded — "Unknown, never Healthy", the card's own rule (#3271). Raw hypertable at the
           collector's five-minute grain, the same precedent the SQL Server CPU arm argues. */
        MetricNames.PgCpu => @"
WITH clean AS (
    SELECT collection_time, acu_utilization_percent::DOUBLE PRECISION AS v
    FROM pg_cpu_utilization
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   acu_utilization_percent IS NOT NULL
)," + RobustTierScaffold,

        /* v2 (#3691): one arm per new metric, each to a partial in its own file (PgTargetBaselineProvider.{Io,
           Replication,Wal}.cs) so lanes 11 / 12 / 15 never edit this switch. A stub answers null — the same
           "no arm for this metric" the default below gives — so the shared reader reports no baseline for the
           metric rather than a bucket built from nothing. pg_autovacuum_workers is wave 2: no arm, no stub. */
        MetricNames.PgIoReadLatency => IoReadLatencyBaselineQuery(),
        MetricNames.PgReplayLagBytes => ReplayLagBaselineQuery(),
        MetricNames.PgWalBytesPerSec => WalBytesPerSecBaselineQuery(),
        /* wave 3 (#3691, between waves): the blocking family's arm, null until lane 17 fills its partial. */
        MetricNames.PgBlockedSessions => BlockedSessionsBaselineQuery(),
        /* lane 24 (#3691): stock's SAMPLED wait rate over sampled_ms (V133) — its own metric, never pooled with the
           Aurora pg_wait_ms_per_sec arm above (PgTargetBaselineProvider.WaitsSampled.cs). */
        MetricNames.PgSampledWaitMsPerSec => SampledWaitBaselineQuery(),
        /* v3 (#3691 plumbing): the plan and kernel families' arms, null until lanes 27 / 28 fill their partials
           (PgTargetBaselineProvider.Plans.cs / .Kernel.cs). */
        MetricNames.PgStatementMeanMs => StatementMeanMsBaselineQuery(),
        MetricNames.PgCpuBurnCores => CpuBurnCoresBaselineQuery(),

        _ => null,
    };

    private static partial string? IoReadLatencyBaselineQuery();
    private static partial string? ReplayLagBaselineQuery();
    private static partial string? WalBytesPerSecBaselineQuery();
    private static partial string? BlockedSessionsBaselineQuery();
    private static partial string? SampledWaitBaselineQuery();
    private static partial string? StatementMeanMsBaselineQuery();
    private static partial string? CpuBurnCoresBaselineQuery();

    protected override string? ResolveBaselineQuery(string metricName) => GetPgTargetBaselineQuery(metricName);
}
