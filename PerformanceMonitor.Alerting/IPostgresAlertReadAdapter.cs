/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The PostgreSQL-only alert reads, kept OFF <see cref="IAlertReadAdapter"/> on purpose.
/// <para>Extending the shared adapter would have forced Lite — which has no PostgreSQL target and no
/// PostgreSQL collectors — to implement three methods that can only ever return empty, leaving permanent
/// dead code in a shipping SKU to satisfy a contract it has no stake in. A separate adapter consulted only
/// for PostgreSQL targets mirrors what collection already does: <c>CollectorCatalog.AppliesTo</c> gates by
/// engine rather than having every definition claim every target.</para>
/// <para>Each method returns the CURRENT state of one Tier 0 outage predictor, already reduced to the
/// worst row per subject. Reduction belongs on the read side because the store queries can do it in one
/// pass, and because the evaluator should decide severity, not shape data.</para>
/// </summary>
public interface IPostgresAlertReadAdapter
{
    /// <summary>
    /// Freeze headroom per database — the worst (oldest) of XID and MultiXact age.
    /// <para>The most consequential condition PostgreSQL has: exhaust transaction IDs and the server stops
    /// accepting writes entirely, and the remedy is hours of vacuuming, so the alert has to fire while
    /// there is still time to act rather than at the cliff.</para>
    /// </summary>
    Task<List<PostgresWraparoundAlertInfo>> GetWraparoundRiskAsync(
        int serverId, CancellationToken cancellationToken = default);

    /// <summary>
    /// What is currently holding back the xmin horizon, and for how long it has been doing so.
    /// <para>Persistence is part of the read rather than the threshold because it is what separates a
    /// chronic holder from a query that ran long, and only the first is worth waking someone for.</para>
    /// </summary>
    Task<PostgresXminHorizonAlertInfo?> GetXminHorizonAsync(
        int serverId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replication slots that are retaining WAL, with whether the pile is still growing.
    /// <para>Growth is the difference between a consumer that is behind and a volume filling in front of
    /// you. Per-instance by nature — slots live on the writer.</para>
    /// </summary>
    Task<List<PostgresSlotAlertInfo>> GetReplicationSlotRiskAsync(
        int serverId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Accumulated wait time per poison wait event over the alert's window (#2711) — an ACCUMULATION, the
    /// one departure from this interface's "current state" convention, because the poison condition is
    /// defined by recent accrual rather than a level (see
    /// <see cref="PostgresAlertEvaluator.PoisonWaitWindowMinutes"/>'s doc comment).
    /// <para>An empty list is the healthy case AND the only possible answer on a target whose engine does
    /// not populate the source (the cumulative wait counters are Aurora-only); the evaluator treats both
    /// as silence, never as failure.</para>
    /// </summary>
    Task<List<PostgresPoisonWaitAlertInfo>> GetPoisonWaitPressureAsync(
        int serverId, CancellationToken cancellationToken = default);
}
