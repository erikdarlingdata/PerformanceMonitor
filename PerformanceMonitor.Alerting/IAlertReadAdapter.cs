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
/// The per-store read surface behind the Phase-5 shared alert engine (slice B) — every collected
/// feed the engine sweep evaluates, so the engine itself (slice D) is store-free. Each method
/// mirrors the semantics and filters of Lite's alert-loop reads EXACTLY (windows, LIMITs, wait-type
/// lists, latest-snapshot-only scoping); implementations over other stores (Darling's Postgres)
/// must reproduce them query-for-query so the same server state produces the same alerts.
/// <para>
/// <c>serverKey</c> is the engine's stable per-server identity, matching
/// <see cref="IAlertStateStore"/>'s convention (Lite/Darling: the deterministic storage-name hash
/// rendered as a string; Dashboard, if it ever converges: its own id) — the engine carries ONE
/// identity type across all three seams.
/// </para>
/// <para>
/// The failed-jobs feed is deliberately NOT part of this adapter: failure outcomes are not in any
/// collected table — hosts run <see cref="FailedJobsQuery"/> live against the monitored server's
/// msdb at alert-check time, on their own connections with their own permission gating (see the
/// <see cref="FailedJobsQuery"/> remarks). A collected-store read surface cannot serve it.
/// </para>
/// </summary>
public interface IAlertReadAdapter
{
    /// <summary>
    /// Recent blocked-process events for the blocking alert, newest first, capped at 200.
    /// CONTRACT: the result INCLUDES the always-on DMV blocking-snapshot fallback rows, merged with
    /// XE blocked-process-report rows preferred — a DMV row appears only where no BPR row covers
    /// the same (blocked, blocker) SPID pair within the same minute (AWS RDS / unset
    /// blocked-process threshold capture nothing via XE). Implementations reproduce Lite's merge
    /// via <see cref="BlockedProcessReportMerge.AppendDmvFallbackRows"/>; DMV rows carry
    /// <see cref="BlockedProcessAlertRow.DmvSnapshotSource"/> and have no report XML.
    /// </summary>
    Task<List<BlockedProcessAlertRow>> GetRecentBlockedProcessReportsAsync(
        string serverKey, int hoursBack, CancellationToken cancellationToken = default);

    /// <summary>
    /// The CURRENT total blocked wait time (#1839): the sum of <c>wait_time_ms</c> across the rows of
    /// the LATEST <c>dmv_blocking_snapshots</c> snapshot for this server, with the distinct blocked-SPID
    /// count. Returns null when the store holds NO snapshot for the server at all.
    /// <para>
    /// A snapshot sum, deliberately — see <see cref="CurrentBlockingWaitResult"/> for why the alert is
    /// level-triggered on one snapshot rather than a rolling window. Implementations select rows by
    /// <c>collection_time = MAX(collection_time)</c> (the snapshot identity the running-jobs and
    /// long-running-query reads already key on), NOT by a time window.
    /// </para>
    /// <para>
    /// FRESHNESS: implementations set <see cref="CurrentBlockingWaitResult.SnapshotIsFresh"/> false when
    /// the snapshot is older than <see cref="CurrentBlockingWaitResult.MaxSnapshotAge"/> at the server's
    /// effective <c>dmv_blocking_snapshot</c> cadence — the #1812 rule, so a collector outage cannot hold
    /// a level-triggered alert active on frozen rows.
    /// </para>
    /// </summary>
    Task<CurrentBlockingWaitResult?> GetCurrentBlockingWaitAsync(
        string serverKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Recent deadlock events for the deadlock alert, newest (by deadlock time) first, capped
    /// at 50. Excluded-database filtering happens in the builder
    /// (<see cref="AlertContextBuilders.IsDeadlockExcluded"/> parses the graph XML), not here.
    /// </summary>
    Task<List<DeadlockAlertRow>> GetRecentDeadlocksAsync(
        string serverKey, int hoursBack, CancellationToken cancellationToken = default);

    /// <summary>
    /// The poison-wait deltas at or above <paramref name="thresholdMs"/> average ms/wait. Mirrors
    /// Lite's read exactly: the newest wait_stats rows (max 3, collected within the last 10
    /// minutes) for THREADPOOL / RESOURCE_SEMAPHORE / RESOURCE_SEMAPHORE_QUERY_COMPILE with
    /// delta_waiting_tasks &gt; 0, THEN the threshold filter applied client-side — the row window
    /// is selected BEFORE thresholding, so a sub-threshold poison wait still occupies its slot,
    /// exactly like the pre-extraction loop's fetch-then-FindAll.
    /// </summary>
    Task<List<PoisonWaitDelta>> GetPoisonWaitDeltasAsync(
        string serverKey, double thresholdMs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Currently-running queries over <paramref name="thresholdMinutes"/> elapsed, longest first,
    /// from the LATEST collection snapshot only (and only if that snapshot is under 10 minutes
    /// old — a stale store must not alert). Mirrors Lite's read exactly: user sessions only
    /// (session_id &gt; 50), the five opt-out noise filters, capped at
    /// <paramref name="maxResults"/> (clamped 1–1000), then rows in
    /// <paramref name="excludedDatabases"/> dropped client-side (case-insensitive; rows with no
    /// database name always pass) — the loop's post-fetch exclusion, moved behind the seam.
    /// </summary>
    Task<List<LongRunningQueryInfo>> GetLongRunningQueriesAsync(
        string serverKey,
        int thresholdMinutes,
        int maxResults,
        bool excludeSpServerDiagnostics,
        bool excludeWaitFor,
        bool excludeBackups,
        bool excludeMiscWaits,
        bool excludeCdc,
        IReadOnlyList<string> excludedDatabases,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// The latest free space per distinct volume (mount point), worst (lowest free %) first, for
    /// the low-disk alert. Files on the same volume share one row (MAX total / MIN free); volumes
    /// with no mount point are excluded, so Azure SQL DB yields an empty list. Threshold
    /// evaluation stays engine-side (<see cref="AlertContextBuilders.GetBreachedVolumes"/>).
    /// </summary>
    Task<List<VolumeFreeSpaceInfo>> GetVolumeFreeSpaceAsync(
        string serverKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// The latest tempdb space snapshot, or null when the store has none for this server.
    /// Threshold evaluation (UsedPercent) stays engine-side.
    /// </summary>
    Task<TempDbSpaceInfo?> GetTempDbSpaceAsync(
        string serverKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// The newest pvs_stats snapshot's per-database persistent version store state, for the
    /// PVS-pressure alert (#1984) — ADR-ON databases only (a database that cannot have a PVS
    /// cannot breach), worst (highest PVS %) first. Empty when the server has no ADR databases or
    /// pvs_stats has not collected yet — the check treats both as nothing-to-evaluate, mirroring
    /// the low-disk alert's empty-on-Azure convention. Threshold evaluation stays engine-side
    /// (<see cref="AlertContextBuilders.GetBreachedPvsDatabases"/>).
    /// </summary>
    Task<List<PvsPressureInfo>> GetPvsPressureAsync(
        string serverKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Currently-running Agent jobs whose duration is at least <paramref name="multiplier"/>x
    /// their historical average, worst (highest % of average) first, capped at 5, from the LATEST
    /// running_jobs snapshot only. Jobs averaging under 60 seconds are excluded (noise floor) —
    /// mirroring Lite's read exactly.
    ///
    /// <para>#1812: the latest snapshot is only evidence when it is FRESH. Implementations compare
    /// MAX(collection_time) against <see cref="AnomalousJobsResult.MaxSnapshotAge"/> at the server's
    /// effective running_jobs cadence, and return <see cref="AnomalousJobsResult.Stale"/> (no rows,
    /// not fresh) when the snapshot is older — a stopped collector, missed cycles, lost msdb access,
    /// or a store reset must not let a historical snapshot read as NOW.</para>
    /// </summary>
    Task<AnomalousJobsResult> GetAnomalousJobsAsync(
        string serverKey, int multiplier, CancellationToken cancellationToken = default);

    /// <summary>
    /// The databases whose collected state DEVIATES from their expected state in the TWO most recent
    /// collections, for the baseline-deviation database-state alert: a two-sample rule so a restart's
    /// RECOVERY_PENDING / RECOVERING transients (and a standby secondary's per-restore RESTORING flicker)
    /// don't fire unless the condition sticks. The read compares the effective state (STANDBY for a
    /// log-shipping secondary, else <c>state_desc</c>) against the per-database expected-state table and
    /// returns the rows where <c>current != expected</c> in both samples and <c>expected</c> is not the
    /// <see cref="DatabaseStateTokens.Ignore"/> sentinel, each carrying both the current and expected state.
    /// <para>
    /// CONTRACT — the read also AUTO-SEEDS, HEALS and PRUNES: any database in the latest snapshot with no
    /// expected-state row yet gets its current effective state recorded as the first-observation baseline,
    /// EXCEPT the states in <see cref="DatabaseStateTokens.NeverBaselinedSqlList"/> — the integrity ones
    /// (which stay pending so they alert rather than learning the bad state as expected) and the transient
    /// ones (RESTORING / RECOVERING, which stay pending SILENTLY until the database settles, so onboarding
    /// mid-restore cannot learn a state nobody chose). An AUTO-seeded baseline that nonetheless records one
    /// of those states — written by an older build, or by re-baselining a database by hand while it was
    /// mid-something — is HEALED to ONLINE once the database's effective state reaches ONLINE, since such a
    /// row is not a baseline anyone chose and would otherwise make the database deviate by being healthy
    /// (#2189). The heal never touches a user override, and never touches an OFFLINE or STANDBY baseline:
    /// those are steady states, and departing one is a real deviation that must still fire. Auto-baselines
    /// for databases that have dropped off the newest snapshot are pruned (user overrides preserved).
    /// Seeding is idempotent (insert-if-absent) and never overwrites a user override or an existing baseline.
    /// </para>
    /// <para>
    /// Empty when the store has no snapshot for this server. Unlike the anomalous-jobs read this is
    /// NOT freshness-gated: a database-state problem is a standing condition, so a stale "still
    /// OFFLINE" snapshot correctly keeps the alert active (cooldown throttles re-fires) rather than
    /// fabricating a recovery.
    /// </para>
    /// </summary>
    Task<List<DatabaseStateInfo>> GetDatabaseStatesAsync(
        string serverKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Forced Query Store plans whose <c>force_failure_count</c> ROSE between the two most recent
    /// collections that carried the plan (#2157) — i.e. the engine is failing to reproduce that plan now.
    ///
    /// <para>The store computes the delta, exactly as <see cref="GetDatabaseStatesAsync"/> returns only
    /// deviating rows: the engine must never see a level. The counter is cumulative AND travels with a
    /// restored database, so a level-based read would alert forever about failures that happened on
    /// hardware the operator may no longer own.</para>
    ///
    /// <para>A counter that DROPPED is an unforce/re-force cycle, not a failure, and is omitted (silent
    /// re-arm). A plan seen for the FIRST time carries no previous sample and is therefore omitted too —
    /// one cycle of delay, deliberately, because "new" is unknowable from a single observation.</para>
    ///
    /// <para>Empty when the store has fewer than two samples for every forced plan. Not freshness-gated,
    /// for the same reason as database state: a failing force is a standing condition, so a stale snapshot
    /// keeps it active rather than fabricating a recovery.</para>
    /// </summary>
    Task<List<ForcePlanFailureInfo>> GetForcePlanFailuresAsync(
        string serverKey, CancellationToken cancellationToken = default);
}
