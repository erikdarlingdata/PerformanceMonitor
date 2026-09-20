/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The baselines-and-anomalies family of the PostgreSQL-target analysis pass (#3542 lane 9, design §2b): five
/// <c>clean</c> CTEs over the PostgreSQL raw hypertables under the ONE <c>RobustTierScaffold</c>, five detectors
/// through the shared <c>AnomalyGate</c>, and the scorer / advice arms that let a PostgreSQL anomaly grade, escape
/// the tuning-class cap, fold into its regular parent and speak in PostgreSQL nouns.
///
/// <para><b>What is pinned.</b> Each metric's table and shape (the reset-aware difference for the two counter
/// families, the three-state interval and the CPU exclusion for the wait rate, <c>acu_utilization_percent</c> and
/// never <c>cpu_percent</c> for CPU, the denormalised pick for sessions); the supply invariant (D10 — every source
/// table's default retention covers the 30-day window and none is in the 4-day raw tier); the detector SQL's
/// dialect, tables and observed-time divisor; the deviation / ratio membership with the wait profile registered as
/// a ratio; the ratio ramp's values; <b>the extremity escape reachable for <c>ANOMALY_PG_SESSION_SPIKE</c></b> — 3×
/// its fire cutoff with two corroborators is NOT capped at 1.49, alone it is 1.0, and a merely-saturated anomaly
/// with the same corroborators IS capped (the #3584 pin, PostgreSQL twin); the ratio families never escaping; the
/// CPU fact grading only a measured capacity percent, at the fleet ladder's two lines, pinned equal to
/// <c>ServerHealthThresholds</c>; the load confirmer refusing a raw reading; every PostgreSQL constant in
/// <c>AnomalyThresholds</c> carrying a lineage marker; and the prose — sigma, baseline and sample count on a
/// trusted fact, "first occurrence, no baseline yet" and NO sigma or multiple on a low-quality one, "the engine
/// measured" and never "spent" or "estimated from sampling" on the Aurora wait profile.</para>
///
/// <para><b>Gated e2e</b> (<c>DARLING_TEST_PG</c>): 30 days of one-minute <c>pg_database_stats</c>,
/// <c>pg_session_states</c> and five-minute <c>pg_cpu_utilization</c> on an <c>aurora-postgres</c>-stamped server,
/// steady for 30 days and spiking in the last four hours; through the REAL <c>analyze_server</c>: a TPS anomaly at
/// the display cap, corroborated by the session and CPU anomalies and the measured CPU confirmer, storied above
/// 1.5 (the lone-anomaly cap does not hold an extreme, corroborated one); the CPU anomaly folded onto the
/// <c>PG_CPU_PERCENT</c> story's incident; the first-occurrence deadlock-rate anomaly folded onto
/// <c>PG_DEADLOCK_RATE</c>'s; and every anomaly fact carrying the gate's metadata with a <c>threshold_lineage</c>
/// verdict — 1 on the TPS and CPU anomalies (floors and fallbacks fleet-measured, #3691 2026-09-19), 0 on the session
/// (count floors unmeasured), deadlock-rate and wait-profile (chosen ratio multiple) anomalies.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetAnomalyTests
{
    /* The five v1 metrics (lane 9) plus the WAL-volume arm lane 15 filled (#3691) — each served by exactly one
       PgTargetBaselineProvider arm ending in the one scaffold. Lane 11's I/O arm beside it; lane 12's replay-lag arm is pinned by its own tests. */
    private static readonly string[] s_pgMetricNames =
    [
        MetricNames.PgTps, MetricNames.PgSessionCount, MetricNames.PgDeadlockRate, MetricNames.PgWaitMsPerSec, MetricNames.PgCpu,
        /* v2 (#3691) lane 11: the I/O read-latency arm, filled. */
        MetricNames.PgIoReadLatency,
        MetricNames.PgWalBytesPerSec,
        /* wave 3 (#3691) lane 17: the blocked-sessions point series over the edges and the collection log. */
        MetricNames.PgBlockedSessions,
        /* lane 24 (#3691): stock's SAMPLED wait rate over sampled_ms — its own metric beside the Aurora one. */
        MetricNames.PgSampledWaitMsPerSec,
        /* lane 27 (#3691 v3): the SERVER-WIDE per-call statement mean — one series per server, by the coordinator's ruling. */
        MetricNames.PgStatementMeanMs,
        /* lane 28 (#3691 v3): cores busy per collection over pg_kernel_stats — the self-hosted CPU proxy's bucket. */
        MetricNames.PgCpuBurnCores,
    ];

    private static readonly (string Metric, string Table)[] s_metricTables =
    [
        (MetricNames.PgTps, "pg_database_stats"),
        (MetricNames.PgDeadlockRate, "pg_database_stats"),
        (MetricNames.PgSessionCount, "pg_session_states"),
        (MetricNames.PgWaitMsPerSec, "pg_wait_stats"),
        (MetricNames.PgCpu, "pg_cpu_utilization"),
        (MetricNames.PgIoReadLatency, "pg_io_stats"),
        /* v2 (#3691): lane 12's replay-lag point series — its scaffold / dialect pins live in PgTargetReplicationTests. */
        (MetricNames.PgReplayLagBytes, "pg_replication_stats"),
        (MetricNames.PgWalBytesPerSec, "pg_write_stats"),
        (MetricNames.PgBlockedSessions, "pg_blocking_edges"),
        (MetricNames.PgSampledWaitMsPerSec, "pg_wait_sampling"),
        (MetricNames.PgStatementMeanMs, "pg_statement_stats"),
        (MetricNames.PgCpuBurnCores, "pg_kernel_stats"),
    ];

    /* ───────────────────────── the baselines ───────────────────────── */

    [Fact]
    public void TheServedPgMetricNames_ArePgPrefixed_ServedOnlyByThePgTargetProvider_AndEndInTheOneScaffold()
    {
        Assert.Equal(11, s_pgMetricNames.Distinct(StringComparer.Ordinal).Count());
        foreach (var metric in s_pgMetricNames)
        {
            Assert.StartsWith("pg_", metric, StringComparison.Ordinal);
            /* The SQL Server provider has no arm for a PostgreSQL metric, and the PostgreSQL provider has exactly one. */
            Assert.Null(PgBaselineProvider.GetBaselineQuery(metric));
            var sql = PgTargetBaselineProvider.GetPgTargetBaselineQuery(metric);
            Assert.NotNull(sql);
            Assert.EndsWith(PgBaselineProvider.RobustTierScaffold, sql, StringComparison.Ordinal);
            Assert.Contains("clean AS (", sql, StringComparison.Ordinal);
            Assert.Contains("server_id = $1 AND collection_time >= $2 AND collection_time < $3", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("CURRENT_TIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotMatch(new Regex(@"\bFROM\s+v_"), sql);
        }

        Assert.Null(PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.Cpu));
        Assert.Null(PgTargetBaselineProvider.GetPgTargetBaselineQuery("nope"));

        /* The seam: a protected override, nothing else of the base machinery redeclared (the second seam, the clock
           read #3691 added, is pinned beside it in PgTargetClockTests — exactly two overrides, no state). */
        var resolve = typeof(PgTargetBaselineProvider).GetMethod("ResolveBaselineQuery", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(resolve);
        Assert.Equal(typeof(PgTargetBaselineProvider), resolve!.DeclaringType);
        Assert.Empty(typeof(PgTargetBaselineProvider).GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly));
    }

    /// <summary>
    /// #3691 v2 plumbing: the v2 baselines and detectors exist as REACHABLE stubs until their lane lands — the metric
    /// names are declared and pg_-prefixed, the SQL Server provider has no arm for them, an unfilled PostgreSQL arm
    /// answers null (the same answer as "no arm", so the shared reader reports no baseline rather than a bucket built
    /// from nothing), the root detector awaits each stub after the five v1 detectors, and every stub file carries
    /// the exact marker the content briefs quote (a filled family keeps it, as v1's did). The wave-2 name has no arm
    /// at all. Lanes 11 (I/O read latency), 12 (replay lag) and 15 (WAL volume) have landed: their metrics moved out
    /// of the inert loop (11 and 15 into <see cref="s_pgMetricNames"/>); <c>PgTargetIoTests</c>, <c>PgTargetReplicationTests</c>
    /// and <c>PgTargetWriteTests</c> pin the filled arms' shapes.
    /// </summary>
    [Fact]
    public void TheV2BaselinesAndDetectors_AreReachableInertStubs_EachNamingItsLane()
    {
        /* Lanes 11, 12, 15, 17, 27 and 28 filled pg_io_read_latency, pg_replay_lag_bytes, pg_wal_bytes_per_sec,
           pg_blocked_sessions, pg_statement_mean_ms and pg_cpu_burn_cores (each arm pinned in its own family's tests; the
           served ones also in s_pgMetricNames above); only the wave-2 name still answers null — it has no arm at all. */
        foreach (var metric in new[] { MetricNames.PgAutovacuumWorkers })
        {
            Assert.StartsWith("pg_", metric, StringComparison.Ordinal);
            Assert.DoesNotContain(metric, s_pgMetricNames);
            Assert.Null(PgBaselineProvider.GetBaselineQuery(metric));
            Assert.Null(PgTargetBaselineProvider.GetPgTargetBaselineQuery(metric));
        }
        Assert.Contains(MetricNames.PgBlockedSessions, s_pgMetricNames);
        Assert.Contains(MetricNames.PgStatementMeanMs, s_pgMetricNames);
        Assert.NotNull(PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgReplayLagBytes));
        Assert.Contains(MetricNames.PgWalBytesPerSec, s_pgMetricNames);

        var provider = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.cs");
        Assert.Contains("MetricNames.PgIoReadLatency => IoReadLatencyBaselineQuery(),", provider, StringComparison.Ordinal);
        Assert.Contains("MetricNames.PgReplayLagBytes => ReplayLagBaselineQuery(),", provider, StringComparison.Ordinal);
        Assert.Contains("MetricNames.PgWalBytesPerSec => WalBytesPerSecBaselineQuery(),", provider, StringComparison.Ordinal);
        /* wave 3 (#3691): the blocking arm, routed to the partial lane 17 filled. */
        Assert.Contains("MetricNames.PgBlockedSessions => BlockedSessionsBaselineQuery(),", provider, StringComparison.Ordinal);
        /* v3 (#3691 plumbing): both arms filled — the plan arm by lane 27, the kernel arm by lane 28 — and still routed here. */
        Assert.Contains("MetricNames.PgStatementMeanMs => StatementMeanMsBaselineQuery(),", provider, StringComparison.Ordinal);
        Assert.Contains("MetricNames.PgCpuBurnCores => CpuBurnCoresBaselineQuery(),", provider, StringComparison.Ordinal);
        Assert.DoesNotContain("PgAutovacuumWorkers", provider, StringComparison.Ordinal);

        var detector = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(detector);
        var lastV1 = code.IndexOf("await DetectWaitProfileAnomalies(context, anomalies);", StringComparison.Ordinal);
        Assert.True(lastV1 > 0);
        foreach (var call in new[] { "await DetectIoAnomalies(context, anomalies);", "await DetectReplicationAnomalies(context, anomalies);", "await DetectWalVolumeAnomalies(context, anomalies);", "await DetectBlockingAnomalies(context, anomalies);", "await DetectPlanRegressionAnomalies(context, anomalies);", "await DetectCpuBurnAnomalies(context, anomalies);" })
            Assert.True(code.IndexOf(call, StringComparison.Ordinal) > lastV1, call + " must follow the five v1 detectors");

        foreach (var (file, lane) in new[]
        {
            ("PgTargetAnomalyDetector.Io.cs", 11), ("PgTargetAnomalyDetector.Replication.cs", 12), ("PgTargetAnomalyDetector.Wal.cs", 15),
            ("PgTargetBaselineProvider.Io.cs", 11), ("PgTargetBaselineProvider.Replication.cs", 12), ("PgTargetBaselineProvider.Wal.cs", 15),
            /* wave 3 (#3691 between waves): the blocking family's two Darling-side stubs. */
            ("PgTargetAnomalyDetector.Blocking.cs", 17), ("PgTargetBaselineProvider.Blocking.cs", 17),
            /* v3 (#3691 plumbing): the plan and kernel families' four Darling-side stubs. */
            ("PgTargetAnomalyDetector.Plans.cs", 27), ("PgTargetBaselineProvider.Plans.cs", 27),
            ("PgTargetAnomalyDetector.Kernel.cs", 28), ("PgTargetBaselineProvider.Kernel.cs", 28),
        })
        {
            var text = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", file);
            Assert.Contains($"/* filled by lane {lane}", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void EachCleanCte_ReadsItsCollectorTable_AndOnlyCollectorTablesOrCtes()
    {
        var tables = CollectorCatalog.All.Select(s => s.TargetTable).ToHashSet(StringComparer.Ordinal);
        /* One admission beyond the collector tables (#3691 lane 17), the collector census's own (PgTargetFactCollectorTests):
           the pg_blocked_sessions arm counts the pg_blocking collector's SUCCESS captures from collection_log, because a
           capture that looked and found no edge is a ZERO sample and only the log records that it looked — over the edge
           table alone a quiet month has no baseline and a server that blocks once a week has a baseline of blocking. Not a
           v_ view, not a SQL Server table by another name; nothing else is admitted. */
        tables.Add("collection_log");
        foreach (var (metric, table) in s_metricTables)
        {
            var sql = PgTargetBaselineProvider.GetPgTargetBaselineQuery(metric)!;
            Assert.Contains("FROM " + table, sql, StringComparison.Ordinal);
            AssertFromJoinTargets(sql, tables);
        }
    }

    [Fact]
    public void TheCounterArms_TakeTheResetAwareDifference_PerDatabase_RatedOverTheCollectionsOwnGap()
    {
        foreach (var metric in new[] { MetricNames.PgTps, MetricNames.PgDeadlockRate })
        {
            var sql = PgTargetBaselineProvider.GetPgTargetBaselineQuery(metric)!;
            Assert.Contains("PARTITION BY database_name", sql, StringComparison.Ordinal);
            Assert.Contains("(xact_commit + xact_rollback) - LAG(xact_commit + xact_rollback) OVER series", sql, StringComparison.Ordinal);
            Assert.Contains("deadlocks - LAG(deadlocks) OVER series", sql, StringComparison.Ordinal);
            Assert.Contains("stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series", sql, StringComparison.Ordinal);
            Assert.Contains("ROW_NUMBER() OVER series > 1", sql, StringComparison.Ordinal);
            Assert.Contains("GREATEST(raw_xacts, 0)", sql, StringComparison.Ordinal);
            Assert.Contains("LAG(collection_time) OVER series", sql, StringComparison.Ordinal);
            Assert.Contains("WHERE interval_sec > 0", sql, StringComparison.Ordinal);
            /* Never the log capture for the RATE (adversarial item C). */
            Assert.DoesNotContain("pg_deadlocks", sql, StringComparison.Ordinal);
        }

        Assert.Contains("xacts / interval_sec AS v", PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgTps)!, StringComparison.Ordinal);
        /* Deadlocks per HOUR, so the bucket mean is in the PG_DEADLOCK_RATE fact's unit. */
        Assert.Contains("deadlocks * 3600.0 / interval_sec AS v", PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgDeadlockRate)!, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSessionArm_PicksTheDenormalisedTotal_TheWaitArm_KeepsTheThreeStateIntervalAndExcludesCpu_TheCpuArm_ReadsCapacityNeverRawCpu()
    {
        var sessions = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgSessionCount)!;
        Assert.Contains("MAX(total_sessions)::DOUBLE PRECISION AS v", sessions, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collection_time", sessions, StringComparison.Ordinal);

        var waits = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgWaitMsPerSec)!;
        Assert.Contains("NULLIF(MAX(sample_interval_seconds), 0)", waits, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN MAX(sample_interval_seconds) IS NULL", waits, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time) OVER (ORDER BY collection_time)", waits, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0", waits, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE lower(wait_type) IS DISTINCT FROM 'cpu')", waits, StringComparison.Ordinal);
        Assert.Contains("GREATEST(delta_wait_time_us, 0)", waits, StringComparison.Ordinal);
        /* Only the Aurora measured series in v1 — never the sampled estimate. */
        Assert.DoesNotContain("pg_wait_sampling", waits, StringComparison.Ordinal);

        var cpu = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgCpu)!;
        Assert.Contains("acu_utilization_percent::DOUBLE PRECISION AS v", cpu, StringComparison.Ordinal);
        Assert.Contains("acu_utilization_percent IS NOT NULL", cpu, StringComparison.Ordinal);
        Assert.DoesNotContain("cpu_percent", CSharpSourceWalkerFreeSql(cpu), StringComparison.Ordinal);
    }

    /// <summary>
    /// D10, the <c>BaselineSupplyTests</c> shape: the PostgreSQL raw tables' DEFAULT retention covers the 30-day
    /// baseline window and none of them is in the 4-day raw tier that #1757 found under the SQL Server baselines.
    /// The provider says the dependency in its own doc.
    /// </summary>
    [Fact]
    public void PgBaselineSupply_DefaultRetentionCoversTheWindow_NoSourceIsInTheFourDayRawTier_AndTheProviderStatesTheDependency()
    {
        foreach (var table in s_metricTables.Select(t => t.Table).Distinct(StringComparer.Ordinal))
        {
            /* Schedules are keyed by COLLECTOR name; every table here is its collector's name too except lane 17's
               pg_blocking_edges (collector pg_blocking), so the catalog resolves it. */
            var collector = CollectorCatalog.All.Single(c => c.TargetTable == table).Name;
            Assert.True(
                CollectorScheduleDefaults.All[collector].RetentionDays >= BaselineMath.BaselineWindowDays,
                $"{table} retention ({CollectorScheduleDefaults.All[collector].RetentionDays}d) no longer covers the {BaselineMath.BaselineWindowDays}-day baseline window (D10)");
            Assert.DoesNotContain(table, TimescaleSupport.RawTierCoverage.Select(t => t.Relation));
        }

        var provider = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.cs");
        Assert.Contains("(D10)", provider, StringComparison.Ordinal);
        Assert.Contains("RawTierCoverage", provider, StringComparison.Ordinal);
        Assert.Contains("BaselineServingRawCollectors", provider, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTime.UtcNow", provider, StringComparison.Ordinal);
    }

    /* ───────────────────────── the detector ───────────────────────── */

    private static readonly string[] s_detectorSql =
    [
        PgTargetAnomalyDetector.HasBaselineDataSql,
        PgTargetAnomalyDetector.DatabaseCounterWindowSql,
        PgTargetAnomalyDetector.SessionWindowSql,
        PgTargetAnomalyDetector.CpuWindowSql,
        PgTargetAnomalyDetector.WaitRateWindowSql,
        PgTargetAnomalyDetector.WaitContribWindowSql,
        /* v2 (#3691) lane 11 */
        PgTargetAnomalyDetector.IoLatencyWindowSql,
        PgTargetAnomalyDetector.ReplayLagWindowSql,   /* lane 12 (#3691) */
        /* v2 (#3691) lane 15 */
        PgTargetAnomalyDetector.WalVolumeWindowSql,
        /* wave 3 (#3691) lane 17 */
        PgTargetAnomalyDetector.BlockedSessionsWindowSql,
        /* lane 27 (#3691 v3): the server-wide per-call statement mean */
        PgTargetAnomalyDetector.StatementMeanWindowSql,
        /* lane 24 (#3691): the sampled wait profile's two reads */
        PgTargetAnomalyDetector.SampledWaitRateWindowSql,
        PgTargetAnomalyDetector.SampledWaitContribWindowSql,
        /* lane 28 (#3691 v3): the CPU-burn window read, the collector's text by alias */
        PgTargetAnomalyDetector.CpuBurnWindowSql,
    ];

    [Fact]
    public void EveryDetectorRead_IsPgDialect_ServerScoped_WindowBound_OverCollectorTablesOnly()
    {
        var consts = typeof(PgTargetAnomalyDetector)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name.EndsWith("Sql", StringComparison.Ordinal))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();
        Assert.Equal(s_detectorSql.Order(StringComparer.Ordinal), consts.Order(StringComparer.Ordinal));

        var tables = CollectorCatalog.All.Select(s => s.TargetTable).ToHashSet(StringComparer.Ordinal);
        /* The same one admission as the baseline census above (lane 17): the blocked-sessions window read shares the
           arm's zero rule, so it reads the same log. */
        tables.Add("collection_log");
        foreach (var sql in s_detectorSql)
        {
            var upper = sql.ToUpperInvariant();
            Assert.DoesNotContain("QUALIFY", upper, StringComparison.Ordinal);
            Assert.DoesNotContain("NOW(", upper, StringComparison.Ordinal);
            Assert.DoesNotContain("CURRENT_TIMESTAMP", upper, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
            Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
            AssertFromJoinTargets(sql, tables);
        }

        Assert.Contains("FROM pg_database_stats", PgTargetAnomalyDetector.HasBaselineDataSql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_database_stats", PgTargetAnomalyDetector.DatabaseCounterWindowSql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_session_states", PgTargetAnomalyDetector.SessionWindowSql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_cpu_utilization", PgTargetAnomalyDetector.CpuWindowSql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_wait_stats", PgTargetAnomalyDetector.WaitRateWindowSql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_wait_stats", PgTargetAnomalyDetector.WaitContribWindowSql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_io_stats", PgTargetAnomalyDetector.IoLatencyWindowSql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_replication_stats", PgTargetAnomalyDetector.ReplayLagWindowSql, StringComparison.Ordinal);
        /* Lane 15: the WAL window read IS the collector's read, by alias — one differencing for fact, detector and bucket. */
        Assert.Contains("FROM pg_write_stats", PgTargetAnomalyDetector.WalVolumeWindowSql, StringComparison.Ordinal);
        Assert.Equal(PgTargetFactCollector.PgTargetWalVolumeSql, PgTargetAnomalyDetector.WalVolumeWindowSql);
        /* Lane 17: the window read and the bucket arm share the zero rule — the log's minutes, the edges' distinct pids. */
        Assert.Contains("FROM pg_blocking_edges", PgTargetAnomalyDetector.BlockedSessionsWindowSql, StringComparison.Ordinal);
        Assert.Contains("FROM collection_log", PgTargetAnomalyDetector.BlockedSessionsWindowSql, StringComparison.Ordinal);
        Assert.Contains("collector_name = 'pg_blocking'", PgTargetAnomalyDetector.BlockedSessionsWindowSql, StringComparison.Ordinal);
        Assert.Contains("COUNT(DISTINCT blocked_pid)::DOUBLE PRECISION", PgTargetAnomalyDetector.BlockedSessionsWindowSql, StringComparison.Ordinal);
        /* Lane 24: the sampled profile reads pg_wait_sampling (and counts pg_wait_stats' collections to sit out when both wrote). */
        Assert.Contains("FROM pg_wait_sampling", PgTargetAnomalyDetector.SampledWaitRateWindowSql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_wait_sampling", PgTargetAnomalyDetector.SampledWaitContribWindowSql, StringComparison.Ordinal);

        /* The window reads share the fact reads' shapes. */
        Assert.Contains("stats_reset", PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgTps)!, StringComparison.Ordinal);
        Assert.Contains("(xact_commit + xact_rollback) - LAG(xact_commit + xact_rollback) OVER series", PgTargetAnomalyDetector.DatabaseCounterWindowSql, StringComparison.Ordinal);
        Assert.Contains("MAX(total_sessions)::DOUBLE PRECISION", PgTargetAnomalyDetector.SessionWindowSql, StringComparison.Ordinal);
        Assert.Contains("MAX(acu_utilization_percent) AS peak_capacity_pct", PgTargetAnomalyDetector.CpuWindowSql, StringComparison.Ordinal);
        Assert.Contains("NULLIF(MAX(sample_interval_seconds), 0)", PgTargetAnomalyDetector.WaitRateWindowSql, StringComparison.Ordinal);
        Assert.Contains("IS DISTINCT FROM 'cpu'", PgTargetAnomalyDetector.WaitRateWindowSql, StringComparison.Ordinal);
        Assert.Contains("IS DISTINCT FROM 'cpu'", PgTargetAnomalyDetector.WaitContribWindowSql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_wait_sampling", PgTargetAnomalyDetector.WaitRateWindowSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every FILLED detector is fenced and reads its bucket: the five v1 detectors in the root file, and (since the
    /// #3691 between-waves batch re-pinned this from "five") the v2 detectors in their own partials — I/O (lane
    /// 11), replication (lane 12), WAL volume (lane 15) — and, since lane 17 filled its body, the wave-3 blocking
    /// detector, and since lanes 27 and 28 filled their bodies, the two v3 detectors (plan regression; CPU burn, on the
    /// pair gate like lane 17's). Eleven; every exemption the between-waves batch and the v3 plumbing named is gone with
    /// its stub.
    /// </summary>
    [Fact]
    public void TheDetector_FencesEachOfTenFilledDetectors_DividesTheDeadlockRateByObservedTime_AnchorsOffTheWindow_AndStatesTheA8Residue()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        foreach (var detector in new[] { "DetectTpsAnomalies", "DetectSessionAnomalies", "DetectCpuAnomalies", "DetectDeadlockRateAnomalies", "DetectWaitProfileAnomalies" })
        {
            var start = code.IndexOf("private async Task " + detector + "(", StringComparison.Ordinal);
            Assert.True(start > 0, detector);
            var end = code.IndexOf("\n    private ", start + 1, StringComparison.Ordinal);
            var body = end > start ? code[start..end] : code[start..];
            Assert.Contains("catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))", body, StringComparison.Ordinal);
            Assert.Contains("_baselineProvider.GetBaselineAsync(", body, StringComparison.Ordinal);
        }

        /* The six filled partials: one detector per file, the same fence and the same bucket read. */
        foreach (var (file, detector) in new[] { ("PgTargetAnomalyDetector.Io.cs", "DetectIoAnomalies"), ("PgTargetAnomalyDetector.Replication.cs", "DetectReplicationAnomalies"), ("PgTargetAnomalyDetector.Wal.cs", "DetectWalVolumeAnomalies"), ("PgTargetAnomalyDetector.Blocking.cs", "DetectBlockingAnomalies"), ("PgTargetAnomalyDetector.Plans.cs", "DetectPlanRegressionAnomalies"), ("PgTargetAnomalyDetector.Kernel.cs", "DetectCpuBurnAnomalies") })
        {
            var partialCode = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", file));
            Assert.Contains("private async partial Task " + detector + "(", partialCode, StringComparison.Ordinal);
            Assert.Contains("catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))", partialCode, StringComparison.Ordinal);
            Assert.Contains("_baselineProvider.GetBaselineAsync(", partialCode, StringComparison.Ordinal);
            Assert.DoesNotContain("DateTime.UtcNow", partialCode, StringComparison.Ordinal);
        }
        /* No exemption remains: both v3 stubs were filled (lane 27 the plan one, lane 28 the kernel one), so no detector
           partial on the tree is a Task.CompletedTask stub — a new stub must be named here the day it is declared. */
        foreach (var file in Directory.GetFiles(RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Analysis"), "PgTargetAnomalyDetector.*.cs"))
            Assert.DoesNotContain("=> Task.CompletedTask;", CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file)), StringComparison.Ordinal);

        /* Lane 17's detector is the first PostgreSQL one on the #3653 PAIR gate (peak AND window mean); pinned so a
           later edit back to the peak-only overload is a visible decision. */
        var blockingDetector = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.Blocking.cs"));
        Assert.Contains("baseline, peakBlocked, avgBlocked,", blockingDetector, StringComparison.Ordinal);
        Assert.DoesNotContain("=> Task.CompletedTask;", blockingDetector, StringComparison.Ordinal);
        /* Lane 28's CPU-burn detector is the second on the pair gate — a cores-busy series is spiky by nature. */
        var kernelDetector = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.Kernel.cs"));
        Assert.Contains("baseline, peakCores, meanCores,", kernelDetector, StringComparison.Ordinal);
        Assert.DoesNotContain("=> Task.CompletedTask;", kernelDetector, StringComparison.Ordinal);

        /* #3538 A7: the deadlock rate is per OBSERVED hour, never per nominal window. */
        var deadlock = code[code.IndexOf("DetectDeadlockRateAnomalies(AnalysisContext", StringComparison.Ordinal)..];
        Assert.Contains("context.ObservedDurationMs / 3_600_000.0", deadlock, StringComparison.Ordinal);
        Assert.DoesNotContain("(context.TimeRangeEnd - context.TimeRangeStart).TotalHours", code, StringComparison.Ordinal);

        /* Anchored (#2506): the gate is the window's end minus 30 days, and no clock is read anywhere. */
        Assert.Contains("windowEnd.AddDays(-30)", code, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTime.UtcNow", code, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTime.Now", code, StringComparison.Ordinal);

        /* Every command carries the pass deadline and the token. */
        Assert.Contains("CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds", code, StringComparison.Ordinal);
        Assert.DoesNotContain("ExecuteReaderAsync()", code, StringComparison.Ordinal);
        Assert.DoesNotContain("ExecuteScalarAsync()", code, StringComparison.Ordinal);

        /* The known, deferred peak-vs-per-sample residue is stated, not silently inherited. */
        Assert.Contains("#3538 A8", source, StringComparison.Ordinal);
        Assert.Contains("threshold_lineage", source, StringComparison.Ordinal);
    }

    /* ───────────────────────── membership and the ratio ramp ───────────────────────── */

    [Fact]
    public void EveryAnomalyPgKey_IsInExactlyOneOfDeviationOrRatio_AndTheWaitProfileIsRatio()
    {
        var anomalyKeys = typeof(PgTargetFactKeys).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name != nameof(PgTargetFactKeys.AnomalyPrefix))
            .Select(f => (string)f.GetRawConstantValue()!)
            .Where(PgTargetFactKeys.IsPgAnomalyKey)
            .ToList();
        /* Five v1 anomalies (lane 9) plus the three v2 ones the plumbing registered by shape (#3691: I/O latency,
           replication lag, WAL volume — all z-score, all deviation-scored) plus the wave-3 blocking anomaly the
           between-waves batch registered the same way (blocked sessions per capture — z-score, deviation-scored),
           plus lane 24's stock sampled wait profile (the Aurora profile's ratio shape on another instrument), plus the
           two v3 anomalies the v3 plumbing registered by shape (plan regression — a statement's mean ms; CPU burn —
           cores busy; both z-score, both deviation-scored). */
        Assert.Equal(12, anomalyKeys.Count);
        Assert.True(PgTargetScorer.IsPgRatioAnomalyKey(PgTargetFactKeys.AnomalySampledWaitProfile));
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyBlocking));
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyPlanRegression));
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyCpuBurn));

        foreach (var key in anomalyKeys)
        {
            var deviation = PgTargetScorer.IsDeviationScoredAnomalyKey(key);
            var ratio = PgTargetScorer.IsPgRatioAnomalyKey(key);
            Assert.True(deviation ^ ratio, $"{key} must be in exactly one of the two predicates (deviation={deviation}, ratio={ratio})");
        }

        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyTps));
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalySessionSpike));
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyCpuSpike));
        Assert.True(PgTargetScorer.IsPgRatioAnomalyKey(PgTargetFactKeys.AnomalyDeadlockRate));
        Assert.True(PgTargetScorer.IsPgRatioAnomalyKey(PgTargetFactKeys.AnomalyWaitProfile));
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyIoLatency));
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyReplicationLag));
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyWalVolume));
        Assert.False(PgTargetScorer.IsPgRatioAnomalyKey("ANOMALY_WAIT_PROFILE"));
        Assert.False(PgTargetScorer.IsDeviationScoredAnomalyKey(null));
        Assert.False(PgTargetScorer.IsPgRatioAnomalyKey(null));
    }

    [Theory]
    [InlineData(2.9, 0.0)]
    [InlineData(3.0, 0.5)]
    [InlineData(6.0, 0.75)]
    [InlineData(9.0, 1.0)]
    [InlineData(40.0, 1.0)]
    public void ScoreRatioAnomaly_DeadlockRate_RampsFromTheFiringMultipleToThreeTimesIt_AndStampsTheLineage(double ratio, double expected)
    {
        Assert.Equal(3.0, AnomalyThresholds.PgRatioAnomalyThreshold);
        Assert.Equal(9.0, PgTargetScorer.RatioAnomalySaturation);

        var fact = Anomaly(PgTargetFactKeys.AnomalyDeadlockRate, ("ratio", ratio));
        Assert.Equal(expected, PgTargetScorer.ScoreRatioAnomaly(fact), precision: 9);
        Assert.Equal(0, fact.Metadata["threshold_lineage"]);

        new FactScorer().ScoreAll([fact]);
        Assert.Equal(expected, fact.BaseSeverity, precision: 9);
    }

    [Theory]
    [InlineData(0.9, 0.0)]
    [InlineData(1.0, 0.5)]
    [InlineData(1.5, 0.75)]
    [InlineData(2.0, 1.0)]
    [InlineData(5.0, 1.0)]
    public void ScoreRatioAnomaly_AFirstOccurrence_GradesItsExceedanceOfTheAbsoluteBar_NeverASentinelRatio(double exceedance, double expected)
    {
        /* is_new with a ratio of 0 — the detector writes no sentinel; the SQL Server NoBaselineRatio (100) would read 1.0 everywhere. */
        var deadlock = Anomaly(PgTargetFactKeys.AnomalyDeadlockRate, ("is_new", 1), ("ratio", 0), ("fallback_exceedance", exceedance));
        Assert.Equal(expected, PgTargetScorer.ScoreRatioAnomaly(deadlock), precision: 9);

        var profile = Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("is_new", 1), ("ratio", 0), ("modified_z", 0), ("fallback_exceedance", exceedance));
        Assert.Equal(expected, PgTargetScorer.ScoreRatioAnomaly(profile), precision: 9);
    }

    [Theory]
    [InlineData(4.9, 0.0)]
    [InlineData(5.0, 0.5)]
    [InlineData(10.0, 0.75)]
    [InlineData(15.0, 1.0)]
    [InlineData(25.0, 1.0)]
    public void ScoreRatioAnomaly_WaitProfile_GradesTheModifiedZWhenTheBucketWasRobust_AndTheRatioOtherwise(double modifiedZ, double expected)
    {
        var robust = Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("modified_z", modifiedZ), ("ratio", 50.0));
        Assert.Equal(expected, PgTargetScorer.ScoreRatioAnomaly(robust), precision: 9);

        /* No robust statistic (modified_z 0): the ratio ramp, same shape as the deadlock rate. */
        var classical = Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("modified_z", 0), ("ratio", 6.0));
        Assert.Equal(0.75, PgTargetScorer.ScoreRatioAnomaly(classical), precision: 9);

        Assert.Equal(0.0, PgTargetScorer.ScoreRatioAnomaly(Anomaly("ANOMALY_DEADLOCK_SPIKE", ("ratio", 50.0))));
        Assert.Equal(0.0, PgTargetScorer.ScoreRatioAnomaly(Anomaly(PgTargetFactKeys.AnomalyTps, ("ratio", 50.0))));
    }

    /* ───────────────────────── the extremity escape (#3584, PostgreSQL twin) ───────────────────────── */

    /// <summary>
    /// THE load-bearing pin. A session spike at 3× its robust fire cutoff (10.5σ against 3.5) beside a fired TPS
    /// anomaly and a measured CPU confirmer at the bar: base 1.0 × (1 + 0.3 + 0.3) = 1.6 and NOT capped at 1.49 —
    /// it pages. Alone it is 1.0 (the escape releases the cap, corroboration is still the rule for CRITICAL). The
    /// same two corroborators beside a merely SATURATED spike (7σ, 2× the anchor) hold at 1.49: extreme means "so
    /// far out the ramp ran out of scale", not "the top of the ramp".
    /// </summary>
    [Fact]
    public void ASessionSpikeAtThreeTimesItsCutoff_WithTwoCoFires_IsNotCappedAt149_AloneItIsOne_AndASaturatedOneIsCapped()
    {
        var extreme = Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 10.5), ("fire_threshold", 3.5), ("baseline_low_quality", 0), ("confidence", 1.0));
        var tps = Anomaly(PgTargetFactKeys.AnomalyTps, ("deviation_sigma", 4.0), ("fire_threshold", 3.5));
        var cpu = CpuFact(85, measured: true);

        var facts = new List<Fact> { extreme, tps, cpu };
        new FactScorer().ScoreAll(facts);

        Assert.Equal(1.0, extreme.BaseSeverity, precision: 9);
        Assert.Equal(1.6, extreme.Severity, precision: 9);
        Assert.True(extreme.Severity > 1.49, "the extremity escape did not release the PostgreSQL session spike from the tuning-class cap");
        Assert.Equal(3, extreme.AmplifierResults.Count);
        Assert.Equal(2, extreme.AmplifierResults.Count(r => r.Matched));

        /* Alone: released from the cap, but base maxes at 1.0 — nothing to page on. */
        var alone = Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 10.5), ("fire_threshold", 3.5));
        new FactScorer().ScoreAll([alone]);
        Assert.Equal(1.0, alone.Severity, precision: 9);

        /* Saturated but not extreme: the cap holds however many siblings co-fire. */
        var saturated = Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 7.0), ("fire_threshold", 3.5));
        var capped = new List<Fact>
        {
            saturated,
            Anomaly(PgTargetFactKeys.AnomalyTps, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)),
            CpuFact(85, measured: true),
        };
        new FactScorer().ScoreAll(capped);
        Assert.Equal(1.0, saturated.BaseSeverity, precision: 9);
        Assert.Equal(1.49, saturated.Severity, precision: 9);

        /* The display cap bounds the bar: a 25σ fact fires the escape under any anchor. */
        var atCap = Anomaly(PgTargetFactKeys.AnomalyTps, ("deviation_sigma", AnomalyThresholds.SigmaDisplayCap), ("fire_threshold", 3.5));
        var atCapSet = new List<Fact> { atCap, Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)), CpuFact(85, measured: true) };
        new FactScorer().ScoreAll(atCapSet);
        Assert.Equal(1.6, atCap.Severity, precision: 9);

        /* The low-quality path escapes on its exceedance (3× the absolute bar), never on the meaningless sigma. */
        var young = Anomaly(PgTargetFactKeys.AnomalyCpuSpike, ("deviation_sigma", 0.4), ("fire_threshold", 3.5), ("baseline_low_quality", 1), ("fallback_exceedance", 3.0));
        var youngSet = new List<Fact> { young, Anomaly(PgTargetFactKeys.AnomalyTps, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)), Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)) };
        new FactScorer().ScoreAll(youngSet);
        Assert.Equal(1.0, young.BaseSeverity, precision: 9);
        Assert.Equal(1.6, young.Severity, precision: 9);
    }

    /// <summary>
    /// #3691 (v1 residue, #3689 §5): the wait profile now ESCAPES the 1.49 cap on its own evidence, through the
    /// same three readings its SQL Server twin has — 3× the heavy-tail cutoff on a robust bucket (15σ), 3× its OWN
    /// ratio anchor otherwise (9×), never on <c>is_new</c>. v1 pinned 1.49 here for the same fact set and called it
    /// "by design tonight"; a Lock-storm profile at 40σ with every corroborator lit sat one hundredth under the
    /// page line. The deadlock-rate ratio family stays capped (it has no amplifier arm and reaches CRITICAL through
    /// its never-capped parent), and the SQL Server profile's arm is untouched.
    /// </summary>
    [Fact]
    public void TheWaitProfile_EscapesTheCapWhenExtreme_TheDeadlockRate_NeverDoes_AndHasNoAmplifierArm()
    {
        var profile = Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("modified_z", 40.0), ("ratio", 60.0));
        var lockWait = new Fact { Source = PgTargetSources.WaitsSource, Key = PgTargetFactKeys.WaitKey("Lock", "relation"), Value = 0.5, Metadata = { ["wait_fraction"] = 0.5, ["is_standout"] = 1 } };
        var set = new List<Fact>
        {
            profile, lockWait,
            Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)),
            Anomaly(PgTargetFactKeys.AnomalyTps, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)),
        };
        new FactScorer().ScoreAll(set);
        Assert.Equal(1.0, profile.BaseSeverity, precision: 9);
        Assert.True(profile.AmplifierResults.Count(r => r.Matched) >= 2, "the wait profile's load and named-wait corroborators did not fire");
        Assert.True(profile.Severity > 1.49, $"an extreme, corroborated wait profile must leave the tuning-class cap; scored {profile.Severity}");

        /* The escape's three readings, off the metadata the ramp grades from — and the bar is the PostgreSQL
           profile's own anchors: 15σ robust (3 × HeavyTailModifiedZThreshold), 9× ratio (3 × PgRatioAnomalyThreshold),
           where the SQL Server arm would read 12× (3 × its 4.0 floor). */
        Assert.True(PgTargetScorer.IsExtremeWaitProfileAnomaly(Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("modified_z", 15.0)), 3.0));
        Assert.False(PgTargetScorer.IsExtremeWaitProfileAnomaly(Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("modified_z", 14.9)), 3.0));
        Assert.True(PgTargetScorer.IsExtremeWaitProfileAnomaly(Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("ratio", 9.0)), 3.0));
        Assert.False(PgTargetScorer.IsExtremeWaitProfileAnomaly(Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("ratio", 8.9)), 3.0));
        Assert.False(PgTargetScorer.IsExtremeWaitProfileAnomaly(Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("modified_z", 40.0), ("is_new", 1)), 3.0));
        Assert.False(PgTargetScorer.IsExtremeWaitProfileAnomaly(Anomaly(PgTargetFactKeys.AnomalyDeadlockRate, ("ratio", 60.0)), 3.0));

        /* An un-extreme, corroborated profile is still capped: the escape is the ONLY way past 1.49. */
        var modest = Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("modified_z", 14.0));
        var modestSet = new List<Fact>
        {
            modest,
            new Fact { Source = PgTargetSources.WaitsSource, Key = PgTargetFactKeys.WaitKey("Lock", "relation"), Value = 0.5, Metadata = { ["wait_fraction"] = 0.5, ["is_standout"] = 1 } },
            Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)),
            Anomaly(PgTargetFactKeys.AnomalyTps, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)),
        };
        new FactScorer().ScoreAll(modestSet);
        Assert.True(modest.AmplifierResults.Count(r => r.Matched) >= 2);
        Assert.Equal(1.49, modest.Severity, precision: 9);

        var amplifiers = typeof(PgTargetScorer).GetMethod("Amplifiers", BindingFlags.Static | BindingFlags.NonPublic)!;
        Assert.Empty((System.Collections.IEnumerable)amplifiers.Invoke(null, [PgTargetFactKeys.AnomalyDeadlockRate])!);
        Assert.Equal(3, ((System.Collections.IEnumerable)amplifiers.Invoke(null, [PgTargetFactKeys.AnomalyTps])!).Cast<object>().Count());
        /* The wait profile: the three load siblings, the measured CPU confirmer, and the named-wait corroborator. */
        Assert.Equal(5, ((System.Collections.IEnumerable)amplifiers.Invoke(null, [PgTargetFactKeys.AnomalyWaitProfile])!).Cast<object>().Count());
    }

    /* ───────────────────────── the CPU family and the load confirmer ───────────────────────── */

    [Theory]
    [InlineData(100, false, 0.0)]
    [InlineData(79.9, true, 0.0)]
    [InlineData(80, true, 0.5)]
    [InlineData(87.5, true, 0.75)]
    [InlineData(95, true, 1.0)]
    [InlineData(100, true, 1.0)]
    public void ScoreCpuFact_GradesOnlyAMeasuredCapacityPercent_ZeroBelowTheWarningBar_AndStampsTheLineage(double value, bool measured, double expected)
    {
        var fact = CpuFact(value, measured);
        new FactScorer().ScoreAll([fact]);
        Assert.Equal(expected, fact.BaseSeverity, precision: 9);
        /* Both bars are fleet-measured (#3691, 2026-09-19: 80 ≈ p99.8, 95 ≈ p99.9); the stamp lands graded or not. */
        Assert.Equal(1, fact.Metadata["threshold_lineage"]);

        /* The two bars are the fleet ladder, repeated and pinned equal; the young-store fallback is its Critical line. */
        Assert.Equal(ServerHealthThresholds.CpuWarningPercent, PgTargetScorer.CpuCapacityWarningPercent);
        Assert.Equal(ServerHealthThresholds.CpuCriticalPercent, PgTargetScorer.CpuCapacityCriticalPercent);
        Assert.Equal(ServerHealthThresholds.CpuCriticalPercent, AnomalyThresholds.PgCpuFallbackPct);
        Assert.True(AnomalyThresholds.PgCpuFallbackPct > AnomalyThresholds.PgCpuFloorPct);
    }

    [Fact]
    public void TheLoadConfirmer_ReadsTheCapacityFlagFirst_SoARawReadingOfOneHundredConfirmsNothing()
    {
        var amplifiers = typeof(PgTargetScorer).GetMethod("Amplifiers", BindingFlags.Static | BindingFlags.NonPublic)!;
        var definitions = ((System.Collections.IEnumerable)amplifiers.Invoke(null, [PgTargetFactKeys.AnomalyTps])!).Cast<object>().ToList();
        var confirmer = definitions[^1];
        var predicate = (Func<Dictionary<string, Fact>, bool>)confirmer.GetType().GetProperty("Predicate")!.GetValue(confirmer)!;
        Assert.Equal(PgTargetScorer.LoadAnomalyCoFireBoost, (double)confirmer.GetType().GetProperty("Boost")!.GetValue(confirmer)!);

        Assert.True(predicate(Lookup(CpuFact(80, measured: true))));
        Assert.False(predicate(Lookup(CpuFact(79.9, measured: true))));
        Assert.False(predicate(Lookup(CpuFact(100, measured: false))));
        Assert.False(predicate(Lookup()));

        /* And the sibling arms ask whether the sibling FIRED, never the self-key. */
        var descriptions = definitions.Select(d => (string)d.GetType().GetProperty("Description")!.GetValue(d)!).ToList();
        Assert.DoesNotContain(descriptions, d => d.Contains("Transaction-rate anomaly co-fired", StringComparison.Ordinal));
        Assert.Contains(descriptions, d => d.Contains("Session-count anomaly co-fired", StringComparison.Ordinal));
        Assert.Contains(descriptions, d => d.Contains("Capacity anomaly co-fired", StringComparison.Ordinal));
        Assert.All(descriptions, d => Assert.DoesNotContain("SQL Server", d, StringComparison.Ordinal));
    }

    /// <summary>Every PostgreSQL floor and bar in the shared constants file carries one of the three lineage
    /// markers in the doc comment directly above it — the rule <c>PgTargetThresholdLineageTests</c> enforces on
    /// the scorer partials, applied to the detector's constants, which that scan does not reach.</summary>
    [Fact]
    public void EveryPgConstantInAnomalyThresholds_CarriesALineageMarker()
    {
        var pgConstants = typeof(AnomalyThresholds).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.Name.StartsWith("Pg", StringComparison.Ordinal))
            .Select(f => f.Name)
            .ToList();
        Assert.Equal(
            new[] { "PgBlockedSessionsFallback", "PgBlockedSessionsFloor", "PgCpuBurnCoresFallback", "PgCpuBurnCoresFloor", "PgCpuFallbackPct", "PgCpuFloorPct", "PgDeadlockRateFallbackPerHour", "PgDeadlockRateFloorPerHour", "PgIoLatencyFallbackMs", "PgIoLatencyFloorMs", "PgRatioAnomalyThreshold", "PgSampledWaitProfileFallbackMsPerSec", "PgSessionCountFallback", "PgSessionCountFloor", "PgStatementMeanMsFallback", "PgStatementMeanMsFloor", "PgTpsFallback", "PgTpsFloor", "PgWaitProfileFallbackMsPerSec", "PgWalBytesFallbackPerSec", "PgWalBytesFloorPerSec" },
            pgConstants.Order(StringComparer.Ordinal).ToArray());

        var lines = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "Baselines", "AnomalyThresholds.cs").Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var marker = new Regex(@"\b(measured|engine-defined|unmeasured)\b", RegexOptions.IgnoreCase);
        foreach (var name in pgConstants)
        {
            var at = Array.FindIndex(lines, l => l.Contains("public const double " + name + " ", StringComparison.Ordinal));
            Assert.True(at > 0, name);
            var neighbourhood = string.Join('\n', lines[Math.Max(0, at - 12)..(at + 1)]);
            Assert.Matches(marker, neighbourhood);
        }

        /* No PostgreSQL bar is a SQL Server constant reused by value. */
        Assert.NotEqual(AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.PgTpsFloor);
        Assert.NotEqual(AnomalyThresholds.SessionCountFloor, AnomalyThresholds.PgSessionCountFloor);
        Assert.NotEqual(AnomalyThresholds.SessionCountFallback, AnomalyThresholds.PgSessionCountFallback);
        Assert.NotEqual(AnomalyThresholds.CpuFloorPct, AnomalyThresholds.PgCpuFloorPct);
        Assert.NotEqual(AnomalyThresholds.WaitProfileFallbackMsPerSec, AnomalyThresholds.PgWaitProfileFallbackMsPerSec);
        /* The one alias is of a MEASURED bar, and the interaction-trap rule holds on every pair. */
        Assert.Equal(PgTargetScorer.DeadlockWarnPerHour, AnomalyThresholds.PgDeadlockRateFallbackPerHour);
        Assert.True(AnomalyThresholds.PgTpsFallback > AnomalyThresholds.PgTpsFloor);
        Assert.True(AnomalyThresholds.PgSessionCountFallback > AnomalyThresholds.PgSessionCountFloor);
        Assert.True(AnomalyThresholds.PgDeadlockRateFallbackPerHour > AnomalyThresholds.PgDeadlockRateFloorPerHour);
        Assert.True(AnomalyThresholds.PgIoLatencyFallbackMs > AnomalyThresholds.PgIoLatencyFloorMs);
        /* Lane 15: 1 MiB/s and 16 MiB/s, in BYTES per second (the unit the fact, the window read and the bucket share). */
        Assert.Equal(1024.0 * 1024.0, AnomalyThresholds.PgWalBytesFloorPerSec);
        Assert.Equal(16.0 * 1024.0 * 1024.0, AnomalyThresholds.PgWalBytesFallbackPerSec);
        Assert.True(AnomalyThresholds.PgWalBytesFallbackPerSec > AnomalyThresholds.PgWalBytesFloorPerSec);
        /* Lane 17: counts of distinct blocked sessions in one capture; neither is a SQL Server constant by value. */
        Assert.True(AnomalyThresholds.PgBlockedSessionsFallback > AnomalyThresholds.PgBlockedSessionsFloor);
        Assert.NotEqual(AnomalyThresholds.SessionCountFloor, AnomalyThresholds.PgBlockedSessionsFloor);
    }

    /* ───────────────────────── the advice ───────────────────────── */

    [Fact]
    public void ComposeAnomaly_StatesTheValueSigmaBaselineAndSampleCount_OnATrustedFact_AndFirstOccurrenceWithoutASigma_OnALowQualityOne()
    {
        var tps = Anomaly(PgTargetFactKeys.AnomalyTps, ("peak_tps", 1234), ("deviation_sigma", 6.2), ("fire_threshold", 3.5),
            ("baseline_mean", 300), ("baseline_median", 280), ("baseline_samples", 120));
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyTps, Lookup(tps))!;
        var text = block.Headline + block.Investigation + block.Remediation;
        Assert.Contains("1,234/sec", text, StringComparison.Ordinal);
        Assert.Contains("6.2σ above its 280/sec baseline median for this hour-of-week (over 120 baseline samples)", text, StringComparison.Ordinal);
        Assert.Contains("pg_database_stats", text, StringComparison.Ordinal);
        Assert.Contains("not necessarily a sustained problem", text, StringComparison.Ordinal);
        Assert.DoesNotContain("SQL Server", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Batch", text, StringComparison.Ordinal);

        var young = Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("peak_sessions", 240), ("deviation_sigma", 0.3), ("fire_threshold", 3.5),
            ("baseline_low_quality", 1), ("fallback_exceedance", 1.2), ("baseline_mean", 200), ("baseline_samples", 4));
        var youngBlock = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalySessionSpike, Lookup(young))!;
        Assert.Contains("first occurrence, no baseline yet", youngBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("240 sessions", youngBlock.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("σ", youngBlock.Headline + youngBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("fired on its absolute level", youngBlock.Investigation, StringComparison.Ordinal);

        /* Delegation: the shared entry point answers exactly this block. */
        Assert.Equal(block, FactAdvice.Compose(PgTargetFactKeys.AnomalyTps, Lookup(tps)));
    }

    [Fact]
    public void ComposeAnomaly_DeadlockRate_SaysTheEngineCounted_TheMultiple_AndFirstOccurrenceWithoutAMultiple()
    {
        var ratio = Anomaly(PgTargetFactKeys.AnomalyDeadlockRate, ("current_count", 24), ("current_rate_per_hour", 6), ("observed_hours", 4), ("ratio", 4.0), ("baseline_rate", 1.5));
        ratio.DatabaseName = "appdb";
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyDeadlockRate, Lookup(ratio))!;
        Assert.Contains("about 4× its baseline", block.Headline, StringComparison.Ordinal);
        Assert.Contains("The engine counted 24 deadlocks over 4 observed hours", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("appdb had the most", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("stats_reset honoured", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("get_pg_deadlocks", block.Remediation, StringComparison.Ordinal);

        var first = Anomaly(PgTargetFactKeys.AnomalyDeadlockRate, ("current_count", 1), ("current_rate_per_hour", 5.5), ("observed_hours", 0.2), ("is_new", 1), ("ratio", 0), ("fallback_exceedance", 1.1));
        var firstBlock = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyDeadlockRate, Lookup(first))!;
        Assert.Contains("1 deadlock this window", firstBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("first occurrence, no baseline yet", firstBlock.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("×", firstBlock.Headline + firstBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("deadlock-rate warning tier", firstBlock.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeAnomaly_WaitProfile_NamesTheContributors_SaysTheEngineMeasured_AndNeverSpentOrEstimated()
    {
        var profile = Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("current_ms_per_sec", 1800), ("baseline_mean", 200), ("modified_z", 12.5), ("ratio", 9.0),
            ("contrib_Lock:relation", 900_000), ("contrib_IO:DataFileRead", 500_000), ("contrib_LWLock:WALWrite", 100_000), ("contrib_IPC:BufferIO", 10));
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWaitProfile, Lookup(profile))!;
        var text = block.Headline + block.Investigation + block.Remediation;
        Assert.Contains("12.5σ above its baseline", block.Headline, StringComparison.Ordinal);
        Assert.Contains("led by Lock:relation, IO:DataFileRead, LWLock:WALWrite", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("IPC:BufferIO", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("the engine measured", text, StringComparison.Ordinal);
        Assert.Contains("CPU excluded", text, StringComparison.Ordinal);
        Assert.DoesNotContain("spent", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("estimated from sampling", text, StringComparison.Ordinal);
        Assert.Contains("get_pg_wait_stats", text, StringComparison.Ordinal);

        var classical = Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("current_ms_per_sec", 1800), ("baseline_mean", 200), ("modified_z", 0), ("ratio", 9.0));
        Assert.Contains("about 9× its baseline", PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWaitProfile, Lookup(classical))!.Headline, StringComparison.Ordinal);

        var first = Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("current_ms_per_sec", 1800), ("is_new", 1), ("fallback_exceedance", 3.6));
        var firstBlock = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWaitProfile, Lookup(first))!;
        Assert.Contains("no baseline yet", firstBlock.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("σ", firstBlock.Headline + firstBlock.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("×", firstBlock.Headline + firstBlock.Investigation, StringComparison.Ordinal);

        /* Every static block: non-null for the five, null otherwise, and each names its table. */
        foreach (var (key, table) in new[]
        {
            (PgTargetFactKeys.AnomalyTps, "pg_database_stats"), (PgTargetFactKeys.AnomalySessionSpike, "pg_session_states"),
            (PgTargetFactKeys.AnomalyCpuSpike, "pg_cpu_utilization"), (PgTargetFactKeys.AnomalyDeadlockRate, "pg_stat_database.deadlocks"),
            (PgTargetFactKeys.AnomalyWaitProfile, "pg_wait_stats"),
        })
        {
            var stat = PgTargetAdvice.Static(key);
            Assert.NotNull(stat);
            Assert.Contains(table, stat!.Investigation, StringComparison.Ordinal);
            Assert.Equal(stat, FactAdvice.GetForFactKey(key));
        }
        Assert.Null(PgTargetAdvice.Compose("ANOMALY_PG_NOPE", Lookup()));
        var waitStatic = PgTargetAdvice.Static(PgTargetFactKeys.AnomalyWaitProfile)!;
        Assert.DoesNotContain("spent", waitStatic.Headline + waitStatic.Investigation + waitStatic.Remediation, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// #3691 lane 26: the Aurora composer names the window MEAN the detector has stamped since #3780's peak-AND-mean
    /// gate, in the sampled composer's shape (#3765) — the mean against its routine, its robust sigmas on the gate arm
    /// or its multiple on the ratio arm, and that the whole window sat high. On <c>is_new</c> the bar is on the peak
    /// alone, so the mean is plain context and no gate is claimed. A fact WITHOUT the mean (pre-#3780) composes byte for
    /// byte as before — the sibling pin's fixture, asserted equal to itself with the mean keys absent.
    /// </summary>
    [Fact]
    public void ComposeAnomaly_WaitProfile_NamesTheWindowMeanAgainstItsRoutine_OnlyWhenStamped_AndAPre3780FactIsByteIdentical()
    {
        static Fact Profile(params (string Name, double Value)[] extra) =>
            Anomaly(PgTargetFactKeys.AnomalyWaitProfile, [("current_ms_per_sec", 3200), ("baseline_mean", 200), ("modified_z", 20.2), ("ratio", 16.0), ("contrib_Lock:relation", 900_000), .. extra]);

        /* The gate arm: the mean's robust sigmas against the same routine. */
        var robust = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWaitProfile, Lookup(Profile(("mean_ms_per_sec", 1507.2), ("mean_modified_z", 8.8), ("mean_ratio", 7.5))))!;
        Assert.Contains(" led by Lock:relation. The window's mean rate was 1507.2 ms/sec against a routine of 200 — 8.8 robust sigmas above it — so the whole window sat high, not one delta; the detector fires only when the mean clears its bar as well as the peak. This is a shift in the overall wait profile", robust.Investigation, StringComparison.Ordinal);
        Assert.Contains("20.2σ above its baseline", robust.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("× it", robust.Investigation, StringComparison.Ordinal);

        /* The ratio arm (modified z 0 by construction): the mean's multiple, the stamped mean_ratio first. */
        var classical = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWaitProfile, Lookup(Profile(("modified_z", 0), ("mean_ms_per_sec", 1507.2), ("mean_modified_z", 0), ("mean_ratio", 7.5))))!;
        Assert.Contains("The window's mean rate was 1507.2 ms/sec against a routine of 200 — about 7.5× it — so the whole window sat high, not one delta; the detector fires only when the mean clears its bar as well as the peak.", classical.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("robust sigmas above it", classical.Investigation, StringComparison.Ordinal);
        /* A fact stamped with the mean but no mean_ratio: the quotient is the belt (1507.2 / 200 = 7.5). */
        var unstampedRatio = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWaitProfile, Lookup(Profile(("modified_z", 0), ("mean_ms_per_sec", 1507.2))))!;
        Assert.Contains("about 7.5× it", unstampedRatio.Investigation, StringComparison.Ordinal);

        /* is_new: the mean is context inside the CPU-excluded parenthesis; no sigma, no multiple, no gate claimed. */
        var first = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWaitProfile, Lookup(Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("current_ms_per_sec", 3200), ("is_new", 1), ("fallback_exceedance", 3.6), ("mean_ms_per_sec", 1507.2))))!;
        Assert.Contains("(CPU excluded; the window's mean was about 1507.2 ms/sec), led by the collected wait types.", first.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("whole window sat high", first.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("fires only when the mean", first.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("σ", first.Headline + first.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("×", first.Headline + first.Investigation, StringComparison.Ordinal);

        /* BYTE-IDENTITY without the mean: the sentence is absent and the surrounding text is exactly the sibling pin's. */
        var preGate = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWaitProfile, Lookup(Profile()))!;
        Assert.DoesNotContain("mean rate", preGate.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("mean was", preGate.Investigation, StringComparison.Ordinal);
        Assert.Contains("(CPU excluded) — 20.2 robust sigmas above its 200 ms/sec baseline for this hour-of-week — led by Lock:relation. This is a shift in the overall wait profile, and the named contributors are where to look.", preGate.Investigation, StringComparison.Ordinal);
        Assert.Equal(preGate.Headline, robust.Headline);
        var preGateFirst = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWaitProfile, Lookup(Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("current_ms_per_sec", 3200), ("is_new", 1), ("fallback_exceedance", 3.6))))!;
        Assert.Contains("(CPU excluded), led by the collected wait types.", preGateFirst.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("mean", preGateFirst.Investigation, StringComparison.Ordinal);

        /* The sentence never says "estimated from sampling" or "spent" — this is the engine-measured composer. */
        Assert.DoesNotContain("estimated from sampling", robust.Investigation + classical.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("spent", robust.Investigation + classical.Investigation, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ComposeCpu_StatesTheCapacityPercentAndTheAcus_ReportsTheRawReadingUngraded_AndSaysNotGradedWithoutACapacitySample()
    {
        var fact = CpuFact(90, measured: true);
        fact.Metadata[PgTargetScorer.CpuPeakCapacityPctKey] = 90;
        fact.Metadata[PgTargetScorer.CpuAvgCapacityPctKey] = 62.5;
        fact.Metadata[PgTargetScorer.CpuPeakPercentKey] = 100;
        fact.Metadata[PgTargetScorer.CpuPeakCapacityAcuKey] = 10.8;
        fact.Metadata[PgTargetScorer.CpuMaxConfiguredAcuKey] = 12;
        fact.Metadata[PgTargetScorer.CpuSampleCountKey] = 48;
        new FactScorer().ScoreAll([fact]);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.CpuPercent, Lookup(fact))!;
        Assert.Contains("peaked at 90% of its configured capacity ceiling — 10.8 of 12 configured ACUs at the peak", block.Headline, StringComparison.Ordinal);
        Assert.Contains("averaged 62.5% across 48 five-minute samples", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("The raw cpu_percent peaked at 100% of the capacity currently allocated — a core was pinned — which is reported, not graded.", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("about the 99.8th and 99.9th percentile of the measured fleet's five-minute samples (threshold_lineage = 1)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("get_pg_statement_stats", block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("SQL Server", block.Headline + block.Investigation + block.Remediation, StringComparison.Ordinal);

        var raw = CpuFact(100, measured: false);
        raw.Metadata[PgTargetScorer.CpuPeakPercentKey] = 100;
        var rawBlock = PgTargetAdvice.Compose(PgTargetFactKeys.CpuPercent, Lookup(raw))!;
        Assert.Contains("not graded", rawBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("Unknown, never Healthy", rawBlock.Investigation, StringComparison.Ordinal);

        var anomaly = Anomaly(PgTargetFactKeys.AnomalyCpuSpike, ("peak_capacity_pct", 91), ("peak_cpu_percent", 100), ("deviation_sigma", 25), ("fire_threshold", 3.5), ("baseline_mean", 30), ("baseline_median", 31), ("baseline_samples", 48));
        var anomalyBlock = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyCpuSpike, Lookup(anomaly))!;
        Assert.Contains("91% of the configured capacity ceiling", anomalyBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("25σ above its 31% of the configured capacity ceiling baseline median", anomalyBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("a core was pinned", anomalyBlock.Investigation, StringComparison.Ordinal);

        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.CpuPercent));
        Assert.Equal(PgTargetAdvice.Static(PgTargetFactKeys.CpuPercent), FactAdvice.GetForFactKey(PgTargetFactKeys.CpuPercent));
    }

    /* ───────────────────────── the collector's CPU read ───────────────────────── */

    [Fact]
    public void TheCpuRead_IsOneQueryOverTheCollectorTable_CarriesBothPercentages_AndTheCollectorGradesNeither()
    {
        var sql = PgTargetFactCollector.PgTargetCpuWindowSql;
        Assert.Contains(sql, PgTargetFactCollector.AllSql);
        Assert.Contains("FROM pg_cpu_utilization", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(w.cpu_percent)", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(w.acu_utilization_percent)", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(w.acu_utilization_percent)", sql, StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Cpu.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        /* The collector picks WHICH percentage through the one shared provenance helper and grades nothing —
           a CPU band decision under Darling/ is the FleetCardPostgresCpuTests census's business. */
        Assert.Contains("FleetCpuProvenance.CpuBandInputPercent(", code, StringComparison.Ordinal);
        Assert.Contains("FleetCpuSource.PerformanceInsights", code, StringComparison.Ordinal);
        Assert.DoesNotContain("ServerHealthClassifier.CpuSeverity(", code, StringComparison.Ordinal);
        Assert.DoesNotContain("new ServerHealthMetrics", code, StringComparison.Ordinal);
        Assert.Contains("CommandTimeout = FactCommandTimeoutSeconds", code, StringComparison.Ordinal);
        Assert.Contains("ReportCollectionFailure(ex, context)", code, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.CpuCapacityMeasuredKey", code, StringComparison.Ordinal);
    }

    /* ───────────────────────── the exit criterion, live ───────────────────────── */

    private const string ServerName = "darling-pg-target-anomaly-aurora-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>
    /// v1's exit criterion, end to end: a 30-day planted series → the z anomalies fire against THIS server's own
    /// hour-of-week baseline, corroborate one another, escape the cap, fold onto their regular parents, and reach
    /// the operator as scored, storied, advice-bearing findings through the REAL <c>analyze_server</c>.
    /// </summary>
    [Fact]
    public async Task ThirtyDaysOfHistoryAndAFourHourSpike_YieldCorroboratedAnomaliesAboveOnePointFive_FoldedOntoTheirParents()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the anomaly-family e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.AuroraPostgres, 17, ct);

            /* 30 days + a margin of one-minute samples ending a minute ago; the last 250 minutes are the spike, so
               the tool's own four-hour window (anchored at NOW) sits wholly inside it. Steady state: ~10 tps with a
               deterministic ±1 ripple (sin), 20–22 sessions, 30–33% of capacity. Spike: 60 tps, 120 sessions, 90%
               of capacity — each far past its floor, and each ≥ 25 robust sigmas from a median whose MAD is under 1.
               Deadlocks: none for 30 days, then one every ten minutes over the last 230 minutes (24 increments,
               both ends inclusive) — INSIDE the window only, so the bucket mean is 0 and the anomaly takes the
               first-occurrence path at 24 / 4 h = 6 per observed hour ≥ the measured 5 / h fallback. */
            var end = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            const int minutes = 31 * 24 * 60;
            var start = end.AddMinutes(-minutes);
            const int spikeFrom = minutes - 250;
            const int deadlocksFrom = minutes - 230;

            await PlantSeriesAsync(connection, @"
WITH s AS (
    SELECT n,
           CASE WHEN n >= $5 THEN 3600 ELSE 600 + round(60 * sin(n)) END AS commits,
           CASE WHEN n >= $6 AND n % 10 = 0 THEN 1 ELSE 0 END AS deadlock_inc
    FROM generate_series(0, $7) AS n
)
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'appdb',
       SUM(commits) OVER (ORDER BY n), 0, 100, 9000, 0, 0, SUM(deadlock_inc) OVER (ORDER BY n), NULL
FROM s", start, spikeFrom, deadlocksFrom, minutes, ct);

            await PlantSeriesAsync(connection, @"
INSERT INTO pg_session_states
    (collection_id, collection_time, server_id, server_name, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, FALSE,
       CASE WHEN n >= $5 THEN 120 ELSE 20 + (n % 3) END, 4, 1, 1
FROM generate_series(0, $7) AS n", start, spikeFrom, deadlocksFrom, minutes, ct);

            await PlantSeriesAsync(connection, @"
INSERT INTO pg_cpu_utilization
    (collection_id, collection_time, server_id, server_name, sample_time,
     cpu_percent, acu_utilization_percent, serverless_capacity_acu, max_configured_acu)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, $2 + (n * interval '1 minute'),
       CASE WHEN n >= $5 THEN 100 ELSE 50 + (n % 5) END,
       CASE WHEN n >= $5 THEN 90 ELSE 30 + (n % 4) END,
       CASE WHEN n >= $5 THEN 10.8 ELSE 3.8 END, 12
FROM generate_series(0, $7, 5) AS n", start, spikeFrom, deadlocksFrom, minutes, ct);

            /* ── The baselines alone: every PostgreSQL metric resolves to a robust bucket. */
            var baselines = new PgTargetBaselineProvider(postgres);
            var analysisTime = end.AddHours(-4);
            var tpsBucket = await baselines.GetBaselineAsync(ServerId, MetricNames.PgTps, analysisTime, ct);
            Assert.True(tpsBucket.IsTrustworthy, "the 30-day TPS bucket is not trustworthy");
            Assert.InRange(tpsBucket.Median, 9.0, 11.0);
            Assert.True(tpsBucket.EffectiveRobustSigma > 0);
            var cpuBucket = await baselines.GetBaselineAsync(ServerId, MetricNames.PgCpu, analysisTime, ct);
            Assert.True(cpuBucket.IsTrustworthy);
            Assert.InRange(cpuBucket.Median, 30.0, 33.0);
            var deadlockBucket = await baselines.GetBaselineAsync(ServerId, MetricNames.PgDeadlockRate, analysisTime, ct);
            Assert.Equal(0.0, deadlockBucket.Mean);
            var waitBucket = await baselines.GetBaselineAsync(ServerId, MetricNames.PgWaitMsPerSec, analysisTime, ct);
            Assert.Equal(0, waitBucket.SampleCount);   /* nothing planted: the wait detector sits out */

            /* ── The detector alone, on the collector-shaped context. */
            var context = new AnalysisContext
            {
                ServerId = ServerId, ServerName = ServerName, TimeRangeStart = analysisTime, TimeRangeEnd = end, ServerUtcOffset = TimeSpan.Zero,
                Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 240 },
            };
            var anomalies = await new PgTargetAnomalyDetector(postgres, baselines).DetectAnomaliesAsync(context);
            var keys = anomalies.Select(a => a.Key).ToList();
            Assert.Contains(PgTargetFactKeys.AnomalyTps, keys);
            Assert.Contains(PgTargetFactKeys.AnomalySessionSpike, keys);
            Assert.Contains(PgTargetFactKeys.AnomalyCpuSpike, keys);
            Assert.Contains(PgTargetFactKeys.AnomalyDeadlockRate, keys);
            Assert.DoesNotContain(PgTargetFactKeys.AnomalyWaitProfile, keys);

            var tpsAnomaly = anomalies.Single(a => a.Key == PgTargetFactKeys.AnomalyTps);
            Assert.Equal(60.0, tpsAnomaly.Value, precision: 6);
            Assert.Equal(AnomalyThresholds.SigmaDisplayCap, tpsAnomaly.Metadata["deviation_sigma"]);
            Assert.Equal(AnomalyThresholds.ModifiedZThreshold, tpsAnomaly.Metadata["fire_threshold"]);
            Assert.Equal(0, tpsAnomaly.Metadata["baseline_low_quality"]);
            Assert.Equal(1, tpsAnomaly.Metadata["threshold_lineage"]);   /* PgTpsFloor / PgTpsFallback: fleet-measured 2026-09-19 */
            Assert.Equal(1.0, tpsAnomaly.Metadata["confidence"]);

            var deadlockAnomaly = anomalies.Single(a => a.Key == PgTargetFactKeys.AnomalyDeadlockRate);
            Assert.Equal("appdb", deadlockAnomaly.DatabaseName);
            Assert.Equal(1, deadlockAnomaly.Metadata["is_new"]);
            Assert.Equal(24, deadlockAnomaly.Metadata["current_count"]);
            Assert.Equal(6.0, deadlockAnomaly.Metadata["current_rate_per_hour"], precision: 6);
            Assert.Equal(6.0 / PgTargetScorer.DeadlockWarnPerHour, deadlockAnomaly.Metadata["fallback_exceedance"], precision: 6);

            /* ── THE EXIT CRITERION, through the real analyze_server. */
            var service = new DarlingAnalysisService(postgres);
            var json = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4);
            using (var doc = JsonDocument.Parse(json))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                string RootKey(JsonElement f) => f.GetProperty("root_fact").GetProperty("key").GetString()!;

                var tpsCard = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.AnomalyTps);
                Assert.True(tpsCard.GetProperty("severity").GetDouble() >= 1.5,
                    $"the corroborated, extreme TPS anomaly did not cross the notify floor: {tpsCard.GetProperty("severity").GetDouble()}");
                var advice = tpsCard.GetProperty("advice");
                var text = advice.GetProperty("headline").GetString() + advice.GetProperty("investigation").GetString() + advice.GetProperty("remediation").GetString();
                Assert.Contains("σ above its", text, StringComparison.Ordinal);
                Assert.Contains("/sec", text, StringComparison.Ordinal);
                Assert.Contains("pg_database_stats", text, StringComparison.Ordinal);
                var tools = tpsCard.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.All(tools, t => Assert.StartsWith("get_pg_", t, StringComparison.Ordinal));

                var sessionCard = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.AnomalySessionSpike);
                Assert.True(sessionCard.GetProperty("severity").GetDouble() >= 1.5);

                /* The CPU anomaly folds onto the PG_CPU_PERCENT story (90% ≥ the 80% bar roots it). */
                var cpuParent = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.CpuPercent);
                var cpuAnomaly = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.AnomalyCpuSpike);
                Assert.Equal(cpuParent.GetProperty("incident_id").GetString(), cpuAnomaly.GetProperty("incident_id").GetString());
                Assert.Contains("configured capacity ceiling", cpuParent.GetProperty("advice").GetProperty("headline").GetString(), StringComparison.Ordinal);

                /* The first-occurrence deadlock anomaly folds onto PG_DEADLOCK_RATE (6 / h ≥ the 5 / h tier roots it). */
                var deadlockParent = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.DeadlockRate);
                var deadlockCard = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.AnomalyDeadlockRate);
                Assert.Equal(deadlockParent.GetProperty("incident_id").GetString(), deadlockCard.GetProperty("incident_id").GetString());
                Assert.Contains("first occurrence, no baseline yet", deadlockCard.GetProperty("advice").GetProperty("headline").GetString(), StringComparison.Ordinal);
                Assert.True(deadlockCard.GetProperty("severity").GetDouble() < 1.49);
            }

            /* #3691: get_analysis_facts runs the RESOLVED engine's detector too (CollectAndScoreFactsAsync is
               collector + detector + scorer since then; before it, this read returned no anomaly fact on either
               engine and the gate metadata was reachable only through a finding). So the same planted spike is an
               ANOMALY_PG_TPS fact on the facts read, scored, with the detector's z and sample count and the
               lineage flag, and with `confidence` projected as baseline_confidence the way the tool does for
               every anomaly-source fact. Filtered to the anomaly source, so the set is the detector's alone. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, PgTargetAnomalyDetector.AnomalySource);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var shown = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                Assert.NotEmpty(shown);
                Assert.All(shown, f => Assert.StartsWith(PgTargetFactKeys.AnomalyPrefix, f.GetProperty("key").GetString(), StringComparison.Ordinal));
                Assert.Equal(
                    keys.OrderBy(k => k, StringComparer.Ordinal),
                    shown.Select(f => f.GetProperty("key").GetString()!).OrderBy(k => k, StringComparer.Ordinal));

                var tpsFact = Assert.Single(shown, f => f.GetProperty("key").GetString() == PgTargetFactKeys.AnomalyTps);
                Assert.Equal(60.0, tpsFact.GetProperty("value").GetDouble(), precision: 6);
                Assert.True(tpsFact.GetProperty("severity").GetDouble() > 0, "the TPS anomaly reached the facts read unscored");
                var tpsMetadata = tpsFact.GetProperty("metadata");
                Assert.Equal(AnomalyThresholds.SigmaDisplayCap, tpsMetadata.GetProperty("deviation_sigma").GetDouble());
                Assert.Equal(AnomalyThresholds.ModifiedZThreshold, tpsMetadata.GetProperty("fire_threshold").GetDouble());
                Assert.True(tpsMetadata.GetProperty("baseline_samples").GetDouble() > 0);
                Assert.Equal(1, tpsMetadata.GetProperty("threshold_lineage").GetDouble());   /* PgTpsFloor / PgTpsFallback fleet-measured 2026-09-19 */
                Assert.Equal(1.0, tpsMetadata.GetProperty("baseline_confidence").GetDouble());
                Assert.False(tpsMetadata.TryGetProperty("confidence", out _), "the tool projects an anomaly fact's confidence as baseline_confidence");
            }
            foreach (var anomaly in anomalies)
            {
                /* The verdict per detector: TPS and CPU are gated only on fleet-measured floors and fallbacks (1);
                   the session detector's COUNT floors are unmeasured, and the deadlock-rate detector's firing
                   multiple was read on 2026-09-20 as an empty interval on 22 (server, bucket) pairs — unplaced —
                   with the scorer's ramp span still chosen (0). */
                var measuredGate = anomaly.Key is PgTargetFactKeys.AnomalyTps or PgTargetFactKeys.AnomalyCpuSpike;
                Assert.Equal(measuredGate ? 1 : 0, anomaly.Metadata["threshold_lineage"]);
                Assert.True(anomaly.Metadata.ContainsKey("fire_threshold"), anomaly.Key);
                Assert.True(anomaly.Metadata.ContainsKey("confidence"), anomaly.Key);
                Assert.StartsWith(PgTargetFactKeys.AnomalyPrefix, anomaly.Key, StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ───────────────────────── helpers ───────────────────────── */

    private static Fact Anomaly(string key, params (string Name, double Value)[] metadata)
    {
        var fact = new Fact { Source = PgTargetAnomalyDetector.AnomalySource, Key = key, Value = 1, ServerId = 1 };
        foreach (var (name, value) in metadata)
            fact.Metadata[name] = value;
        return fact;
    }

    private static Fact CpuFact(double value, bool measured) => new()
    {
        Source = PgTargetSources.CpuSource,
        Key = PgTargetFactKeys.CpuPercent,
        Value = value,
        ServerId = 1,
        Metadata = { [PgTargetScorer.CpuCapacityMeasuredKey] = measured ? 1 : 0 },
    };

    private static Dictionary<string, Fact> Lookup(params Fact[] facts)
    {
        var lookup = new Dictionary<string, Fact>(StringComparer.Ordinal);
        foreach (var fact in facts)
            lookup[fact.Key] = fact;
        return lookup;
    }

    /// <summary>The collector census's FROM/JOIN rule (<c>PgTargetFactCollectorTests</c>): every target is a
    /// collector table, the registry, or a CTE of the same statement; never a <c>v_</c> view.</summary>
    private static void AssertFromJoinTargets(string sql, HashSet<string> tables)
    {
        var scanSql = Regex.Replace(sql, @"--[^\n]*", " ");
        scanSql = Regex.Replace(scanSql, @"\bIS\s+(?:NOT\s+)?DISTINCT\s+FROM\b", " ", RegexOptions.IgnoreCase);
        scanSql = Regex.Replace(scanSql, @"\bEXTRACT\s*\(\s*\w+\s+FROM\b", " ", RegexOptions.IgnoreCase);
        var ctes = Regex.Matches(scanSql, @"(?:WITH|,)\s*(\w+)\s+AS\s*\(", RegexOptions.Singleline)
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(scanSql, @"\b(?:FROM|JOIN)\s+(\w+)", RegexOptions.IgnoreCase))
        {
            var target = m.Groups[1].Value;
            Assert.True(tables.Contains(target) || ctes.Contains(target) || target == "servers",
                $"FROM/JOIN target '{target}' resolves to no collector table, the registry, or a CTE in:\n{sql}");
        }
        Assert.DoesNotMatch(new Regex(@"\bFROM\s+v_"), scanSql);
    }

    /// <summary>
    /// The #3691 parity line "Aurora wait-profile detector lacks the peak-AND-mean gate", closed in #3773's shape:
    /// the window read carries the MEAN beside the PEAK in the reader's ordinal order (0 peak, 1 mean, 2 total,
    /// 3 sample count, 4 collection count), the trusted robust arm is the shared <c>AnomalyGate</c> PAIR call with
    /// this family's bar as both floor and fallback (the inline <c>modifiedZ &lt; HeavyTailModifiedZThreshold</c>
    /// gate is gone), the ratio arm asks both ratios, the no-baseline arm stays on the peak alone (#3741's
    /// ruling), and the fact stamps <c>mean_ms_per_sec</c> / <c>mean_ratio</c> / <c>mean_modified_z</c> — the
    /// SAMPLED twin's keys — beside the peak's. No bar value moved.
    ///
    /// <para>The gate's arithmetic is EXECUTED here through the very call the detector makes, on #3773's fixture
    /// (median 200, MAD 100 → robust σ 148.26): a single 3,200 ms/sec collection in a window whose mean sits at
    /// the median (peak 20.2σ, mean 0.08σ) does NOT fire; a sustained shift to 1,500 with the same spike (mean
    /// 8.8σ) fires; the retired peak-only call fires on both, which is the red this pin turns green. The
    /// extremity escape still reads the peak's <c>modified_z</c> on a fact carrying both readings.</para>
    /// </summary>
    [Fact]
    public void TheAuroraWaitProfile_GatesOnPeakAndMean_ThroughTheSharedPairGate_AndStampsBothReadings()
    {
        var sql = PgTargetAnomalyDetector.WaitRateWindowSql;
        /* The OUTER select — the CTE has its own total_wait_ms alias. */
        var projection = sql[sql.LastIndexOf("SELECT ", StringComparison.Ordinal)..];
        var columns = new[] { "AS peak_ms_per_sec", "AS mean_ms_per_sec", "AS total_wait_ms", "AS sample_count", "AS collection_count" };
        var positions = columns.Select(c => projection.IndexOf(c, StringComparison.Ordinal)).ToArray();
        Assert.All(positions, p => Assert.True(p > 0));
        Assert.Equal(positions.Order().ToArray(), positions);
        /* The same guarded arm as the peak's — a NULL-interval collection is NULL to both aggregates. */
        Assert.Contains("MAX(CASE WHEN interval_sec > 0 THEN coalesce(total_wait_ms, 0) / interval_sec END) AS peak_ms_per_sec", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(CASE WHEN interval_sec > 0 THEN coalesce(total_wait_ms, 0) / interval_sec END) AS mean_ms_per_sec", sql, StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var start = code.IndexOf("private async Task DetectWaitProfileAnomalies(", StringComparison.Ordinal);
        var end = code.IndexOf("\n    internal const string AnomalySource", start, StringComparison.Ordinal);
        Assert.True(start > 0 && end > start, "the Aurora wait-profile detector moved");
        var body = code[start..end];
        Assert.Matches(
            @"AnomalyGate\.EvaluateZScore\(\s*baseline,\s*peakRate,\s*meanRate,\s*HeavyTailModifiedZThreshold,\s*HeavyTailModifiedZThreshold,\s*PgWaitProfileFallbackMsPerSec,\s*PgWaitProfileFallbackMsPerSec,\s*SigmaDisplayCap\)",
            body);
        Assert.Contains("if (!decision.Fire) return;", body, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"modifiedZ\s*<\s*HeavyTailModifiedZThreshold", body);
        Assert.Contains("meanRatio < PgRatioAnomalyThreshold", body, StringComparison.Ordinal);
        /* The no-baseline arm: the peak's bar alone, by ruling. */
        Assert.Contains("if (fallbackExceedance < 1.0) return;", body, StringComparison.Ordinal);
        Assert.DoesNotContain("meanRate < PgWaitProfileFallbackMsPerSec", body, StringComparison.Ordinal);
        var rawBody = source[source.IndexOf("private async Task DetectWaitProfileAnomalies(", StringComparison.Ordinal)..];
        rawBody = rawBody[..rawBody.IndexOf("internal const string AnomalySource", StringComparison.Ordinal)];
        Assert.Contains("[\"current_ms_per_sec\"] = peakRate,", rawBody, StringComparison.Ordinal);
        Assert.Contains("[\"mean_ms_per_sec\"] = meanRate,", rawBody, StringComparison.Ordinal);
        Assert.Contains("[\"mean_ratio\"] = meanRatio,", rawBody, StringComparison.Ordinal);
        Assert.Contains("[\"mean_modified_z\"] = meanModifiedZ,", rawBody, StringComparison.Ordinal);

        /* The arithmetic, executed: #3773's fixture through the detector's exact call. */
        var bucket = new BaselineBucket
        {
            Tier = BaselineTier.Full, HourOfDay = 12, DayOfWeek = 3,
            Mean = 200, StdDev = 81.65, Median = 200, Mad = 100,
            SampleCount = 250, DistinctDays = 5, AbsStdDevFloor = 0,
        };
        Assert.True(bucket.IsTrustworthy && bucket.EffectiveRobustSigma > 0);
        Assert.Equal(100 / 0.6745, bucket.EffectiveRobustSigma, precision: 6);
        const double spike = 3_200.0;
        var quietMean = (239 * 200.0 + spike) / 240;    /* one hot collection, the rest at the median */
        var shiftedMean = (239 * 1_500.0 + spike) / 240; /* the profile running heavy across the window */
        Assert.True(BaselineMath.ModifiedZScore(bucket, spike) >= AnomalyThresholds.HeavyTailModifiedZThreshold && spike >= AnomalyThresholds.PgWaitProfileFallbackMsPerSec, "the peak clears on both windows");
        Assert.True(BaselineMath.ModifiedZScore(bucket, quietMean) < AnomalyThresholds.HeavyTailModifiedZThreshold, "the quiet window's mean is what keeps it quiet");
        Assert.True(BaselineMath.ModifiedZScore(bucket, shiftedMean) >= AnomalyThresholds.HeavyTailModifiedZThreshold);

        ZDecisionFor(bucket, spike, quietMean, out var quiet);
        ZDecisionFor(bucket, spike, shiftedMean, out var shifted);
        Assert.False(quiet.Fire, "a single-sample spike over a flat mean must not read as a profile shift");
        Assert.True(shifted.Fire, "a sustained shift with the same peak fires");
        Assert.True(quiet.MeanSigma < AnomalyThresholds.HeavyTailModifiedZThreshold && shifted.MeanSigma >= AnomalyThresholds.HeavyTailModifiedZThreshold);
        /* Red-first, stated: the peak-only overload the arm used to be equivalent to fires on BOTH windows. */
        var peakOnly = AnomalyGate.EvaluateZScore(
            bucket, spike,
            AnomalyThresholds.HeavyTailModifiedZThreshold, AnomalyThresholds.HeavyTailModifiedZThreshold,
            AnomalyThresholds.PgWaitProfileFallbackMsPerSec, AnomalyThresholds.PgWaitProfileFallbackMsPerSec, AnomalyThresholds.SigmaDisplayCap);
        Assert.True(peakOnly.Fire);

        /* The escape and the grade read the PEAK's statistics, unchanged, on a fact that now carries both readings. */
        var paired = Anomaly(PgTargetFactKeys.AnomalyWaitProfile,
            ("current_ms_per_sec", spike), ("mean_ms_per_sec", shiftedMean), ("modified_z", 20.2), ("mean_modified_z", 8.8), ("ratio", 16.0), ("mean_ratio", 7.5));
        Assert.True(PgTargetScorer.IsExtremeWaitProfileAnomaly(paired, 3.0));
        var modest = Anomaly(PgTargetFactKeys.AnomalyWaitProfile,
            ("current_ms_per_sec", spike), ("mean_ms_per_sec", shiftedMean), ("modified_z", 14.9), ("mean_modified_z", 40.0), ("ratio", 16.0), ("mean_ratio", 60.0));
        Assert.False(PgTargetScorer.IsExtremeWaitProfileAnomaly(modest, 3.0), "the escape is the peak's, never the mean's");

        static void ZDecisionFor(BaselineBucket bucket, double peak, double windowMean, out AnomalyGate.ZDecision decision) =>
            decision = AnomalyGate.EvaluateZScore(
                bucket, peak, windowMean,
                AnomalyThresholds.HeavyTailModifiedZThreshold, AnomalyThresholds.HeavyTailModifiedZThreshold,
                AnomalyThresholds.PgWaitProfileFallbackMsPerSec, AnomalyThresholds.PgWaitProfileFallbackMsPerSec, AnomalyThresholds.SigmaDisplayCap);
    }

    /// <summary>
    /// The gate against a real store: two Aurora servers with 31 days of one-minute <c>pg_wait_stats</c> whose
    /// Lock:relation rate cycles 100 / 200 / 300 ms/sec (median 200, MAD 100), then the same four-hour window on
    /// each — QUIET: every collection at the median but one at 3,200 ms/sec (peak 20σ, mean under 1σ) → no
    /// <c>ANOMALY_PG_WAIT_PROFILE</c>; SHIFTED: every collection at 1,500 with the same one hot collection (mean
    /// 8.8σ) → one fact, <c>current_ms_per_sec</c> the peak, <c>mean_ms_per_sec</c> the window mean, both modified
    /// z stamped in the uncapped frame, <c>is_new</c> 0. Before this PR the QUIET window fired.
    /// </summary>
    [Fact]
    public async Task TheAuroraWaitProfile_OneHotCollectionStaysQuiet_ASustainedShiftFires_AgainstDevPostgres()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the wait-profile gate e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteWaitGateRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            const int minutes = 31 * 24 * 60;
            var start = end.AddMinutes(-minutes);
            var windowStart = end.AddHours(-4);
            const int windowFrom = minutes - 240;   /* the window holds minutes windowFrom..minutes: 241 collections */

            foreach (var (id, name, windowMsPerSec) in new[] { (QuietWaitServerId, QuietWaitServerName, 200L), (ShiftedWaitServerId, ShiftedWaitServerName, 1_500L) })
            {
                await PgTargetFactCollectorTests.RegisterServerAsync(connection, id, name, MonitoredEngineKind.AuroraPostgres, 17, ct);

                /* The baseline-data gate: one pg_database_stats row a minute, constant counters. */
                await PlantWaitGateAsync(connection, @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'appdb', 1000, 10, 100, 9000, 0, 0, 0, NULL
FROM generate_series(0, $5) AS n", ct, CollectionIdGenerator.Next() + 4_000_000L, start, id, name, minutes);

                /* One Lock:relation row per collection, stored interval 60 s, so ms/sec = delta_us / 60_000. History
                   cycles 6 / 12 / 18 s a minute (100 / 200 / 300 ms/sec); the window is flat at $7 ms/sec with the
                   LAST collection at 3,200. A CPU row rides beside it to prove the exclusion. */
                await PlantWaitGateAsync(connection, @"
WITH s AS (
    SELECT n,
           CASE WHEN n = $5 THEN 3200 * 60000::bigint
                WHEN n >= $6 THEN $7 * 60000::bigint
                ELSE (100 + 100 * (n % 3)) * 60000::bigint
           END AS lock_us
    FROM generate_series(0, $5) AS n
)
INSERT INTO pg_wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event,
     waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, w.type_id, w.event_id, w.wait_type, w.wait_event,
       1000000, 1000000000000, 10, CASE WHEN w.type_id = 0 THEN 40000000::bigint ELSE s.lock_us END, 60
FROM s
CROSS JOIN (VALUES (3, 300001::bigint, 'Lock', 'relation'), (0, 1::bigint, 'CPU', 'CPU')) AS w(type_id, event_id, wait_type, wait_event)",
                    ct, CollectionIdGenerator.Next() + 5_000_000L, start, id, name, minutes, windowFrom, windowMsPerSec);
            }

            var baselines = new PgTargetBaselineProvider(postgres);
            var bucket = await baselines.GetBaselineAsync(QuietWaitServerId, MetricNames.PgWaitMsPerSec, windowStart, ct);
            Assert.True(bucket.IsTrustworthy, "the 30-day wait bucket is not trustworthy");
            Assert.InRange(bucket.Median, 199.0, 201.0);
            Assert.InRange(bucket.EffectiveRobustSigma, 140.0, 160.0);

            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var quiet = await detector.DetectAnomaliesAsync(WaitGateContext(QuietWaitServerId, QuietWaitServerName, windowStart, end));
            Assert.DoesNotContain(quiet, a => a.Key == PgTargetFactKeys.AnomalyWaitProfile);
            /* Red-first, on the store's own numbers: the peak alone would have fired here. */
            Assert.True(BaselineMath.ModifiedZScore(bucket, 3_200.0) >= AnomalyThresholds.HeavyTailModifiedZThreshold);

            var shifted = await detector.DetectAnomaliesAsync(WaitGateContext(ShiftedWaitServerId, ShiftedWaitServerName, windowStart, end));
            var fact = Assert.Single(shifted, a => a.Key == PgTargetFactKeys.AnomalyWaitProfile);
            Assert.Equal(3_200.0, fact.Metadata["current_ms_per_sec"], precision: 3);
            Assert.Equal((240 * 1_500.0 + 3_200.0) / 241, fact.Metadata["mean_ms_per_sec"], precision: 3);
            Assert.Equal(0, fact.Metadata["is_new"]);
            Assert.Equal(241, fact.Metadata["window_samples"]);
            Assert.True(fact.Metadata["modified_z"] >= AnomalyThresholds.HeavyTailModifiedZThreshold);
            Assert.True(fact.Metadata["mean_modified_z"] >= AnomalyThresholds.HeavyTailModifiedZThreshold, "a fired fact's mean cleared the same cutoff");
            Assert.True(fact.Metadata["mean_modified_z"] < fact.Metadata["modified_z"]);
            Assert.True(fact.Metadata["mean_ratio"] > 0 && fact.Metadata["mean_ratio"] < fact.Metadata["ratio"]);
            Assert.Equal(0, fact.Metadata["threshold_lineage"]);
            Assert.True(fact.Metadata.ContainsKey("contrib_Lock:relation"));
            Assert.False(fact.Metadata.ContainsKey("contrib_CPU:CPU"));
            Assert.False(fact.Metadata.ContainsKey(PgTargetScorer.WaitIsSampledKey));

            /* Lane 26: the composed advice for the detector's real fact names the mean it stamped, against the bucket's
               routine (median 200), with its robust sigmas — the whole window, not the one hot collection. */
            var advice = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWaitProfile, shifted.ToFactLookup())!;
            Assert.Contains($"The window's mean rate was {fact.Metadata["mean_ms_per_sec"].ToString("0.#", System.Globalization.CultureInfo.InvariantCulture)} ms/sec against a routine of {fact.Metadata["baseline_mean"].ToString("0.#", System.Globalization.CultureInfo.InvariantCulture)} — {fact.Metadata["mean_modified_z"].ToString("0.#", System.Globalization.CultureInfo.InvariantCulture)} robust sigmas above it — so the whole window sat high, not one delta", advice.Investigation, StringComparison.Ordinal);
            Assert.Contains("peaked at about 3200 ms of waiting per second", advice.Investigation, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteWaitGateRowsAsync(cleanup, cleanupCt));
        }
    }

    private const string QuietWaitServerName = "darling-pg-target-anomaly-wait-gate-quiet";
    private static readonly int QuietWaitServerId = ServerIdHelper.GetDeterministicHashCode(QuietWaitServerName);
    private const string ShiftedWaitServerName = "darling-pg-target-anomaly-wait-gate-shifted";
    private static readonly int ShiftedWaitServerId = ServerIdHelper.GetDeterministicHashCode(ShiftedWaitServerName);

    private static AnalysisContext WaitGateContext(int serverId, string serverName, DateTime start, DateTime end) => new()
    {
        ServerId = serverId, ServerName = serverName, TimeRangeStart = start, TimeRangeEnd = end, ServerUtcOffset = TimeSpan.Zero,
        Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 241 },
    };

    /// <summary>One planting statement with its positional parameters, every one referenced by its statement.</summary>
    private static async Task PlantWaitGateAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] values)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        foreach (var value in values)
            command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteWaitGateRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id IN ({QuietWaitServerId}, {ShiftedWaitServerId}); " +
            $"DELETE FROM pg_wait_stats WHERE server_id IN ({QuietWaitServerId}, {ShiftedWaitServerId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({QuietWaitServerId}, {ShiftedWaitServerId}); " +
            $"DELETE FROM servers WHERE server_id IN ({QuietWaitServerId}, {ShiftedWaitServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The CPU arm's live SQL with the column list's own name removed, so the "never cpu_percent" pin
    /// reads the READ and not the substring inside <c>acu_utilization_percent</c>'s neighbour.</summary>
    private static string CSharpSourceWalkerFreeSql(string sql) => sql.Replace("acu_utilization_percent", string.Empty, StringComparison.Ordinal);

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task PlantSeriesAsync(NpgsqlConnection connection, string sql, DateTime start, int spikeFrom, int deadlocksFrom, int minutes, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 1_000_000L);
        command.Parameters.AddWithValue(start);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(spikeFrom);
        command.Parameters.AddWithValue(deadlocksFrom);
        command.Parameters.AddWithValue(minutes);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_session_states WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_cpu_utilization WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
