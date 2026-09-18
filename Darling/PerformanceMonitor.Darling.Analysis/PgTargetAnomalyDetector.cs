/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Anomaly detection for a PostgreSQL-target pass (#3542) — the engine-sibling of <see cref="PgAnomalyDetector"/>
/// behind the same <see cref="IAnomalyDetector"/> seam. Same posture throughout: one baseline-data gate ahead of
/// every detector, each detector fenced in its own try so one metric's failure costs the pass that metric and
/// nothing else, and every anomaly fact keyed <c>ANOMALY_PG_*</c> (<see cref="PgTargetFactKeys"/>) so the shared
/// scorer routes it to the PostgreSQL ramps rather than the SQL Server literals (#3584).
///
/// <para>Skeleton only in the plumbing lane: the gate is real (it reads the same table the coverage witness and
/// the data-span gate read), the detector calls are declared in order and return immediately. Lane 9 fills them —
/// TPS z, session z, CPU z (Aurora only), the deadlock-rate ratio and the wait-profile ratio — through the shared
/// <c>AnomalyGate</c>, against <see cref="PgTargetBaselineProvider"/>'s buckets, with floor constants beside the
/// SQL Server ones in <c>AnomalyThresholds</c> carrying their lineage marker and metric names in the shared
/// <c>MetricNames</c> (never a Darling-local twin — <c>SharedBaselineModelPinTests</c>).</para>
/// </summary>
public sealed class PgTargetAnomalyDetector : IAnomalyDetector
{
    private readonly NpgsqlDataSource _postgres;
    private readonly PgTargetBaselineProvider _baselineProvider;
    private readonly ILogger? _logger;

    public PgTargetAnomalyDetector(NpgsqlDataSource postgres, PgTargetBaselineProvider baselineProvider, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _baselineProvider = baselineProvider ?? throw new ArgumentNullException(nameof(baselineProvider));
        _logger = logger;
    }

    /// <summary>
    /// Baseline-data gate: <c>pg_database_stats</c> as canary — the one universal one-minute series (D3), the
    /// same table the coverage witness and the data-span gate read, so the three cannot disagree about whether
    /// this server has history. <c>$2</c> is <c>context.TimeRangeEnd.AddDays(-30)</c>, bound naive-UTC — never a
    /// bare <c>now()</c> — so an anchored pass (#2506) asks about the 30 days before ITS window.
    /// </summary>
    public const string HasBaselineDataSql = @"
SELECT COUNT(*)
FROM pg_database_stats
WHERE server_id = $1 AND collection_time >= $2";

    /// <summary>
    /// The detectors, in order, behind the gate. Each is declared here and stubbed below; lane 9 fills them.
    /// The baseline provider is held so the filled detectors reach the shared bucket machinery through it.
    /// </summary>
    public async Task<List<Fact>> DetectAnomaliesAsync(AnalysisContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var anomalies = new List<Fact>();

        if (!await HasBaselineDataAsync(context.ServerId, context.TimeRangeEnd, context.CancellationToken))
            return anomalies;

        await DetectTpsAnomalies(context, anomalies);
        await DetectSessionAnomalies(context, anomalies);
        await DetectCpuAnomalies(context, anomalies);
        await DetectDeadlockRateAnomalies(context, anomalies);
        await DetectWaitProfileAnomalies(context, anomalies);

        return anomalies;
    }

    /// <summary>The provider the filled detectors read buckets from; exposed for lane 9's detector bodies.</summary>
    internal PgTargetBaselineProvider Baselines => _baselineProvider;

    /// <summary>
    /// <see cref="PgAnomalyDetector"/>'s gate, verbatim but for the SQL: silent on a genuine fault BY DESIGN (an
    /// unreadable canary reads as "no baseline data" and detection sits out the pass), with shutdown residue
    /// excluded (#2299) so a stop mid-gate unwinds to the pass's one Information line instead of masquerading as
    /// an empty baseline.
    /// </summary>
    private async Task<bool> HasBaselineDataAsync(int serverId, DateTime windowEnd, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

            using var cmd = new NpgsqlCommand(HasBaselineDataSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(serverId);
            cmd.Parameters.AddWithValue(AsNaive(windowEnd.AddDays(-30)));

            var count = Convert.ToInt64(await cmd.ExecuteScalarAsync(cancellationToken) ?? 0);
            return count > 0;
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))
        {
            _logger?.LogDebug(
                "[PgTargetAnomalyDetector] Baseline-data gate could not be read for server {ServerId}; anomaly detection sits out this pass: {Message}",
                serverId, ex.Message);
            return false;
        }
    }

    /* ── Detectors: each returns immediately until lane 9 fills it. Each filled body wraps its reads in its
       own try (the per-detector tolerance PgAnomalyDetector documents), sets CommandTimeout on every command,
       passes context.CancellationToken to every store call, and binds every bound naive-UTC. ── */

    /* filled by lane 9 — TPS z (ANOMALY_PG_TPS) off pg_database_stats xact_commit + xact_rollback deltas */
    private static Task DetectTpsAnomalies(AnalysisContext context, List<Fact> anomalies) => Task.CompletedTask;

    /* filled by lane 9 — session-count z (ANOMALY_PG_SESSION_SPIKE) off pg_session_states total_sessions */
    private static Task DetectSessionAnomalies(AnalysisContext context, List<Fact> anomalies) => Task.CompletedTask;

    /* filled by lane 9 — instance CPU z (ANOMALY_PG_CPU_SPIKE) off pg_cpu_utilization; Aurora only, absent on stock */
    private static Task DetectCpuAnomalies(AnalysisContext context, List<Fact> anomalies) => Task.CompletedTask;

    /* filled by lane 9 — deadlock-rate ratio (ANOMALY_PG_DEADLOCK_RATE) off the reset-aware pg_database_stats.deadlocks delta */
    private static Task DetectDeadlockRateAnomalies(AnalysisContext context, List<Fact> anomalies) => Task.CompletedTask;

    /* filled by lane 9 — wait-profile ratio (ANOMALY_PG_WAIT_PROFILE) over the same source the wait facts read */
    private static Task DetectWaitProfileAnomalies(AnalysisContext context, List<Fact> anomalies) => Task.CompletedTask;

    /// <summary>Kind-Unspecified for parameter binds — Npgsql 6+ rejects Kind-Utc against <c>timestamp</c>.</summary>
    private static DateTime AsNaive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
}
