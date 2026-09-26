/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Versioning;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.WindowsServices;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The 24/7 collection loop (headless plan M2): load darling.json, bootstrap the bundled
/// Postgres first when <c>postgres.managed</c> is true (<see cref="DarlingManagedPostgres"/> —
/// unpack/initdb/start before anything touches the store, stop-on-shutdown only if this
/// process started it), migrate the Postgres store,
/// detect optional TimescaleDB (hypertables + compression when present, plain PG otherwise —
/// see TimescaleSupport), re-seed delta baselines from it (restart continuity — the Postgres
/// twin of Lite's DuckDB
/// seeding, so a service restart doesn't zero the first cycle's deltas), connect and probe each
/// monitored server, ensure the XE sessions, run the on-load config
/// snapshots once, then run every scheduled collector on the shared
/// <see cref="CollectorScheduleDefaults"/> cadence through <see cref="DarlingCollectorRunner"/>.
/// A server that fails to connect is retried every sweep; a collector that errors is logged and
/// retried on its next due time — the loop never dies for one bad cycle. Dispatch mirrors Lite's:
/// the deadlock/blocked-process readers tolerate a missing XE session as zero rows, and
/// trace_flags tolerates denied DBCC as zero rows with a warning. Every successful connect
/// upserts the servers registry and every collector run writes a collection_log row — both
/// failure-isolated (<see cref="DarlingObservability"/>). On top of collection the loop runs
/// the shared alert engine per server every 30 seconds and, since AN3, the analysis pipeline
/// (<see cref="DarlingAnalysisService"/>) per server every 30 minutes with findings routed
/// through the shared <see cref="AnalysisNotificationService"/>.
/// </summary>
public sealed class DarlingWorker : BackgroundService
{
    private static readonly TimeSpan s_sweepInterval = TimeSpan.FromSeconds(15);

    /* The alert engine's evaluation cadence — Lite's overview/alert sweep runs on its 30-second
       status timer (MainWindow.xaml.cs:144), so the headless twin evaluates each connected server
       every 30 seconds too (the collector sweep itself runs every 15). Cooldowns and the
       edge-trigger gates shape delivery on top of this. */
    private static readonly TimeSpan s_alertSweepInterval = TimeSpan.FromSeconds(30);

    /* #3285: the custom-alert evaluation cadence. Deliberately slower than the 30s built-in alert sweep —
       user-authored threshold rules are not poison-wait-urgent, and this halves the fleet compose-query cost.
       Doubles as the per-rule cadence default and the rule-cache TTL; a rule may override its own interval
       (floored at 30s). */
    private static readonly TimeSpan s_customAlertSweepInterval = TimeSpan.FromSeconds(60);

    /* The command plane's poll cadence (Stage 2): a tighter 5-second tick, run on its OWN loop
       independent of the 15-second collection sweep and the 30-second alert sweep, so an operator
       command (pause, test_connect, snapshot_now, ...) is picked up within ~5s and a slow command
       (a test_connect against an unreachable host can block for the connect timeout) never starves
       collection — the two loops share a cancellation token and the guarded server set only. */
    private static readonly TimeSpan s_commandPollInterval = TimeSpan.FromSeconds(5);

    /* The store disk-pressure self-alert's poll cadence (fleet-level, Stage 4). Disk fills slowly, and the
       check is one DriveInfo syscall plus one narrow store_metrics lookup, so 5 minutes is ample and cheap —
       no need to run it on the 30-second alert sweep.

       Cheap is load-bearing here rather than incidental, and #3199 is what it costs when it is not: the size
       number came from pg_database_size_stats, which walks every file in the store, and measured 2,090-3,745 ms on
       a 225 GiB store against a 5 s CommandTimeout floored on 6.2 ms. ~288 nominal iterations a day (this
       gate is start-to-start 5 minutes, stamped before the check runs and quantised to the loop's own
       finish-to-start tick, so the delivered count sits just under that) at ~2.5 s each is ~11.9 min/day of
       this loop's wall time, of which the ~2% that crossed 5 s were the only part that logged anything. */
    private static readonly TimeSpan s_diskCheckInterval = TimeSpan.FromMinutes(5);

    /* The custom-alert-rule health check's cadence (fleet-level, #3304). An integrity heuristic, not an urgent
       page: it re-parses the enabled rules against the live catalog and flags broken / never-firing rules. Cheap
       (reads the evaluator's in-memory rule cache + no-data map), 5 minutes is responsive enough to catch a
       measure drift while the aggregated alert itself is cooldown-limited inside the self-alert evaluator. */
    private static readonly TimeSpan s_customAlertHealthInterval = TimeSpan.FromMinutes(5);

    /* The stale-mute check's cadence (fleet-level, #3306). The condition it judges moves on a scale of DAYS —
       a mute rule with no expiry, in force past every expiry the product offers — so the tick exists for the
       RESOLUTION half rather than the firing half: an operator who deletes the rule should see the alert
       clear promptly, not on the next hour. It costs a scan of the in-memory MuteRuleService cache (a handful
       of rows on any real store) and the alert itself is rate-limited to once a day inside the evaluator,
       so matching its #3304 neighbour costs nothing. */
    private static readonly TimeSpan s_staleMuteCheckInterval = TimeSpan.FromMinutes(5);

    /// <summary>#3514: how often the sweep re-evaluates the web-dashboard TLS certificate's expiry. Hourly is
    /// ample for a day-granularity fact (the evaluator's own daily refire gates the actual firing); the check
    /// itself is a field read and a date compare.</summary>
    private static readonly TimeSpan s_webTlsCheckInterval = TimeSpan.FromHours(1);

    /// <summary>How often the sweep re-evaluates the managed-store settings condition (#4215)
    /// (last-good fallback, a kept hand edit, a rejected value). Every fact behind it is fixed for the life of
    /// this process — only a restart changes any of them — so the tick exists for the RESOLUTION half, the
    /// <see cref="s_staleMuteCheckInterval"/> reasoning exactly: an operator who fixes a rejected value and
    /// restarts should see the alert clear on the FIRST tick after this process comes back up, not an hour
    /// later. The one store read it costs (<c>collect.managed_conf_verdicts</c>, at most eight rows) and the
    /// alert itself is rate-limited to once a day inside the evaluator.</summary>
    private static readonly TimeSpan s_storeSettingsCheckInterval = TimeSpan.FromMinutes(5);

    /* The compression-job self-heal check's cadence (fleet-level, #1581). Compression is a slow archival tier
       and a stuck policy job takes hours to matter, so hourly is ample and cheap (one job_stats read + at most
       one alter_job per stuck job) — no need for the 15s sweep or the 30s alert cadence.

       The PHASE of that hour is not this constant's to choose and is not chosen by "UtcNow + interval" any
       more (#3575): the compression policies this check watches fire at :MM:00 of the wall clock on a fixed
       schedule (#3035), and a check scheduled from the instant of its previous fire slips a few seconds
       every hour and eventually samples one of those :00 instants — which is where a production store's
       false page came from. TimescaleSupport.NextCompressionCheckUtc snaps each due time to :30 past its
       minute, so this interval sets how OFTEN and that phase sets WHEN in the minute.

       The retention re-evaluation (#3812) RIDES this tick rather than owning a second one: same store, same
       domain (TimescaleDB background jobs), same cadence class — a held retention policy costs disk by the
       day, so hourly is far tighter than the "next restart" it replaces and far looser than the 30 s alert
       loop needs to be. It runs AFTER the compression read inside the tick, and that order is #3575's, not
       taste: the compression read must sample at :30 past its minute, and twenty coverage probes ahead of it
       on a large store would slide that sample toward the :00 instant the policies start on. */
    private static readonly TimeSpan s_compressionCheckInterval = TimeSpan.FromHours(1);

    /* The whole-pass budget for the hourly retention re-evaluation (#3812), the #2327 shape: this pass is
       AWAITED on the serial sweep loop, so its worst case stalls per-server dispatch and every fleet-level
       check behind it. Each statement inside TimescaleSupport.EnsureRetentionPoliciesAsync carries the 300 s
       bulk-setup deadline that is right for a first conversion and wrong for a loop — twenty policies at up
       to five statements each is a theoretical hundred-plus statements, and on a store that answers slowly
       without failing that is hours of loop block behind one pass. One linked budget for the WHOLE pass bounds
       it to the same ~5 minutes the store self-metrics sweep accepted for the same reason. On a healthy store
       the pass is seconds: the coverage probe is a chunk-pruned min() per relation (685 ms cold on the largest
       store, #2874) and everything else is a catalog row. A pass cut short leaves its unjudged policies exactly
       as they were and says so; the next hour re-judges them. */
    private static readonly TimeSpan s_retentionReevaluationBudget = TimeSpan.FromMinutes(5);

    /* #4300: the seam-only repair's own child budget, linked to the pass's s_retentionReevaluationBudget
       rather than sharing it unbounded. A seam wide enough to need every one of the pass's five minutes
       would otherwise starve the coverage sweep, the purge trigger and the epoch relaunch for that hour on a
       slow store, every hour, for as long as the seam stays open. Two minutes leaves the rest of the pass
       room to run; a seam that needs more than that closes over several hourly passes instead of one, which
       is the same shape RollupBackfill's own newest-first resume already relies on (RollupBackfill.cs:203) —
       a refresh batch that commits before it is cut short leaves the floor inside already-covered ground, so
       picking the seam back up next hour repeats nothing that already closed. */
    private static readonly TimeSpan s_seamRepairBudget = TimeSpan.FromMinutes(2);

    /* The whole-pass budget for the hourly TimescaleDB availability re-probe (#3815), the #2327 shape and a
       far tighter number than its neighbour above, from a different enclosing constraint. The probe is two
       statements — CREATE EXTENSION IF NOT EXISTS and a one-row pg_extension read — and both carry
       TimescaleSupport's 300 s bulk-setup CommandTimeout, which is right for a first conversion and is ten
       minutes of serial-sweep-loop block here. The bound is the tick's own PHASE: the compression read
       behind this probe samples the job catalog at TimescaleSupport.CompressionCheckPhaseSeconds past the
       minute, half a grid step from the :MM:00 instants the compression policies start on (#3575), so
       anything awaited ahead of it spends that guard band. Ten seconds is a third of the half-step, which
       leaves the sample twenty seconds clear of the next boundary even on the pass where the probe is slow
       AND succeeds — and a probe that succeeds on a reachable store is two sub-second catalog statements, so
       that pass is the pathological one rather than the normal one. A store that cannot answer CREATE
       EXTENSION IF NOT EXISTS inside ten seconds is not one this hour's probe was going to heal; the next
       hour retries, and the latch stays exactly where it was meanwhile. */
    private static readonly TimeSpan s_timescaleReprobeBudget = TimeSpan.FromSeconds(10);

    /* ─────────────── store-object convergence (#3817) ─────────────── */

    /// <summary>
    /// Which segment of the start path a convergence step belongs to, and therefore which connection runs it
    /// and where it sits in the ONE order both callers use.
    ///
    /// <para>The segments exist because the start path interleaves three things that are NOT convergence steps
    /// between the ensures, and two of them have a load-bearing position: the superseded-rollup coverage log
    /// (#3653 Q12) must follow the aggregate ensure, and the materialization-hole repair (#3653 Q10) is
    /// LAUNCHED after that ensure and before the compression ensure so the nightly compression pass meets a
    /// relation the repair has already closed. Splitting the list by segment rather than re-listing the steps
    /// around them is what keeps the runtime order byte-identical to what it was while leaving exactly one
    /// list: the segments are FILTERS over <see cref="s_storeObjectConvergence"/>, so a step cannot be in one
    /// caller's order and not the other's.</para>
    /// </summary>
    internal enum StoreObjectConvergenceStage
    {
        /// <summary>Inside the TimescaleDB gate, before the hole repair is launched — the conversion,
        /// compression, reshape, refresh-converge and aggregate ensures.</summary>
        Timescale,

        /// <summary>Inside the TimescaleDB gate, after the hole repair is launched — the dedup-index sweep and
        /// the aggregate-compression ensure.</summary>
        TimescaleAfterRepairLaunch,

        /// <summary>The UNGATED block: the baseline relations, on every store shape. Deliberately not behind
        /// the availability latch — a relation goes missing three ways and only one of them is "no
        /// TimescaleDB" (the reasoning is at the start path's call site and in <c>BaselineSupplyTests</c>).</summary>
        Ungated,

        /// <summary>The composer/alerting performance tuning: results-invariant covering indexes and the
        /// per-table autovacuum overrides, on every store shape.</summary>
        Tuning,
    }

    /// <summary>Whether a convergence stage needs TimescaleDB: the two the start path runs inside its
    /// TimescaleDB gate. The hourly pass on a plain-PostgreSQL store skips exactly these (#3913).</summary>
    internal static bool NeedsTimescale(StoreObjectConvergenceStage stage) =>
        stage is StoreObjectConvergenceStage.Timescale or StoreObjectConvergenceStage.TimescaleAfterRepairLaunch;

    /// <summary>
    /// Whether a step's return value counts CHANGES it made or OBJECTS it found in place — the difference
    /// between a number the summary line may report as "changed" and one it must not.
    ///
    /// <para>This is a property of the methods as they are written, not a policy: the converge-shaped ones
    /// (<see cref="TimescaleSupport.ConvergeCompressionScheduleAsync"/>,
    /// <see cref="TimescaleSupport.ConvergeContinuousAggregateRefreshAsync"/>,
    /// <see cref="TimescaleSupport.DropStaleContinuousAggregatesAsync"/>,
    /// <see cref="TimescaleSupport.EnsureIntervalDedupMaterializationIndexesAsync"/>,
    /// <see cref="TimescaleSupport.EnsureBaselineFallbackViewsAsync"/>,
    /// <see cref="TimescaleSupport.DropRetiredBaselineAggregatesAsync"/>) return how many objects they altered,
    /// dropped or filled, which is zero on a converged store. The ensure-shaped ones
    /// (<see cref="TimescaleSupport.ConvertToHypertablesAsync"/>,
    /// <see cref="TimescaleSupport.ApplyCompressionPolicyAsync"/>,
    /// <see cref="TimescaleSupport.EnsureContinuousAggregatesAsync"/>,
    /// <see cref="TimescaleSupport.EnsureAggregateCompressionAsync"/>,
    /// <see cref="PgTableTuning.ApplyAsync"/>) return how many objects are in place AFTERWARDS — 51 of 51
    /// hypertables on every pass, changed or not — because they were written to answer "is the store
    /// converged", which is the question a start-path log line asks. Reporting those as changes would make
    /// every hourly line claim the store had just been rebuilt.</para>
    ///
    /// <para>So <c>InPlace</c> steps contribute to the step count and to the failure count and NEVER to the
    /// changed count, and the summary line says so rather than implying a census it cannot take. The honest
    /// alternative — teaching six ensures to return deltas — is a wider change than this issue, and the
    /// per-object INFORMATION lines those methods already write are where a change on one of them is
    /// visible.</para>
    /// </summary>
    internal enum StoreObjectChangeSignal
    {
        /// <summary>The returned count is a number of objects CHANGED this pass; zero means nothing moved.</summary>
        Delta,

        /// <summary>The returned count is a number of objects IN PLACE afterwards, so a change cannot be read
        /// off it. Never counted as changed.</summary>
        InPlace,
    }

    /// <summary>
    /// One store-object convergence step: what it is called in the summary line, which segment runs it, how to
    /// read its return value, and the call itself.
    ///
    /// <para><b>The delegate is <c>EnsureAsync</c> rather than the obvious <c>RunAsync</c>, and that name is
    /// load-bearing rather than a preference.</b> <c>Lite.Tests.QueryStoreServerGateTests</c> holds the Query
    /// Store backfill's lease handoff in BOTH SKUs partly by asserting that a call on a local named
    /// <c>step</c> to a method named <c>RunAsync</c> occurs exactly ONCE in this file — a deliberate count pin,
    /// because the defect it guards (a <c>using</c> moved into or out of the loop body) leaves every
    /// occurrence count in the file invariant, so the span between the acquire and the handoff is all it has
    /// to work with. A second such call here, on an unrelated <c>step</c> of an entirely different kind, turns
    /// that pin red and reports it as a Query Store lease defect. It scans this file as TEXT, which is why the
    /// sentence above describes the call instead of quoting it. The verb is the vocabulary of everything
    /// this delegate actually calls anyway — each is an <c>Ensure*</c> or a <c>Converge*</c>.</para>
    /// </summary>
    internal sealed record StoreObjectConvergenceStep(
        string Name,
        StoreObjectConvergenceStage Stage,
        StoreObjectChangeSignal Signal,
        Func<NpgsqlConnection, ILogger, CancellationToken, Task<int>> EnsureAsync);

    /// <summary>
    /// THE list: every idempotent store-object ensure, in the one order both the start path and the hourly
    /// store-maintenance tick run them (#3817). The start path runs it segment by segment
    /// (<see cref="StoreObjectConvergenceStage"/>) so the three non-convergence steps interleaved with it keep
    /// their positions; the tick runs the whole thing through
    /// <see cref="ConvergeStoreObjectsAsync"/>. Two callers, ONE list — which is the point of the list, because
    /// the failure this closes is a second caller whose order drifts from the first's, and the order here is
    /// load-bearing in four places (each named on the step).
    ///
    /// <para><b>What is NOT here, and why.</b> Five things the start path does inside the same blocks are
    /// deliberately start-path-only, and each is a different reason rather than one rule:</para>
    /// <list type="bullet">
    /// <item><description><see cref="TimescaleSupport.EnsureRetentionPoliciesAsync"/> — already a tenant of this
    /// tick under #3812 (<see cref="ReevaluateRetentionPoliciesAsync"/>), with a pass distinction the ensures
    /// here do not have (a hold that was already a hold is Debug, an arm is Information). Putting it in this
    /// list would run the sweep TWICE an hour and double every transition line it writes.</description></item>
    /// <item><description><see cref="TimescaleSupport.LogSupersededHourlyRollupCoverageAsync"/> — an
    /// INSTRUMENT, not an ensure: it changes nothing, and it writes one Information line per superseded pair.
    /// Hourly that is a wall of no-op lines on a healthy store, which is the objection #3812's own issue
    /// raised against an hourly sweep; the hand-over it reports moves over days, so a line per start is the
    /// grain it is worth reading at.</description></item>
    /// <item><description>The baseline backfill (#1757) and the materialization-hole repair (#3653 Q10) — both
    /// BULK materializations, both deliberately launched-not-awaited on the start path for that reason, and
    /// both already coverage-gated so they no-op once caught up. The issue asks for exactly this exclusion: a
    /// periodic pass must not launch a second one over a first that is still running, and a re-run's cost
    /// scales with history rather than with the catalog.</description></item>
    /// <item><description><see cref="DarlingModuleMap"/>'s table ensure and refresh — the refresh is a DATA
    /// upsert rather than a store object and it ALREADY has a periodic home (the daily purge tick), and the
    /// table ensure is inseparable from it here because the refresh is gated on the bool it returns. A
    /// module_map table that failed to create is also the one item on this list whose absence is not silent:
    /// the daily refresh warns about it every day.</description></item>
    /// </list>
    ///
    /// <para><b>Every step here was read for idempotence rather than assumed idempotent</b>, and the verdicts
    /// are on the steps. The two that are worth an operator's attention: the compression ENABLE statements
    /// (<c>ALTER TABLE ... SET (timescaledb.compress ...)</c>, inside
    /// <see cref="TimescaleSupport.ApplyCompressionPolicyAsync"/> and
    /// <see cref="TimescaleSupport.EnsureCollectionLogHypertableAsync"/>) are re-executed on every pass rather
    /// than skipped under a catalog check, so those two steps are the pass's only unconditional DDL; they take
    /// a brief lock on the hypertable's parent and nothing else. That is measured in the PR body rather than
    /// asserted here, and it is why the summary line carries an elapsed.</para>
    /// </summary>
    private static readonly StoreObjectConvergenceStep[] s_storeObjectConvergence =
    {
        /* if_not_exists => true on create_hypertable; migrate_data has nothing to move on a table that is
           already a hypertable. A catalog check per table on a converged store. */
        new("hypertable conversion", StoreObjectConvergenceStage.Timescale, StoreObjectChangeSignal.InPlace,
            (connection, logger, ct) => TimescaleSupport.ConvertToHypertablesAsync(connection, logger, ct)),

        /* The compression ENABLE is unconditional DDL (see the class remark); add_compression_policy's
           if_not_exists returns -1 for a policy that exists. */
        new("compression policies", StoreObjectConvergenceStage.Timescale, StoreObjectChangeSignal.InPlace,
            (connection, logger, ct) => TimescaleSupport.ApplyCompressionPolicyAsync(connection, logger, ct)),

        /* collection_log is outside the collector catalog, so the two steps above never reach it; same three
           idempotent statements. */
        new("collection_log hypertable", StoreObjectConvergenceStage.Timescale, StoreObjectChangeSignal.InPlace,
            async (connection, logger, ct) => await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, logger, ct) ? 1 : 0),

        /* #1778: AFTER both compression steps, so it covers collection_log in the same pass. Selects only
           policies whose cadence or phase DIFFERS — no rows, no DDL, on a converged store. */
        new("compression schedule converge", StoreObjectConvergenceStage.Timescale, StoreObjectChangeSignal.Delta,
            (connection, logger, ct) => TimescaleSupport.ConvergeCompressionScheduleAsync(connection, logger, ct)),

        /* BEFORE the aggregate ensure: drops old-shape aggregates so the ensure rebuilds them in the
           composer-dimension shape. Probes information_schema.columns first; no-op once reshaped. */
        new("stale aggregate reshape", StoreObjectConvergenceStage.Timescale, StoreObjectChangeSignal.Delta,
            (connection, logger, ct) => TimescaleSupport.DropStaleContinuousAggregatesAsync(connection, logger, ct)),

        /* #3012, and this order is MEASURED rather than preferred: add_continuous_aggregate_policy raises
           22023 against a policy whose window differs instead of skipping like its compression and retention
           siblings, so the converge must precede the ensure or the ensure fails per-aggregate on every
           already-deployed store. Alters only policies that differ. */
        new("aggregate refresh converge", StoreObjectConvergenceStage.Timescale, StoreObjectChangeSignal.Delta,
            (connection, logger, ct) => TimescaleSupport.ConvergeContinuousAggregateRefreshAsync(connection, logger, ct)),

        /* #3745: directly after the window converge, because both are alter_job passes over the same policy
           jobs and this one reaches the tier that one deliberately passes over (daily). It only writes the
           buckets_per_batch key — no initial_start, so the daily tier keeps its finish-to-start scheduling —
           and it selects only policies whose stored value differs, so a converged store issues no DDL. */
        new("aggregate batching converge", StoreObjectConvergenceStage.Timescale, StoreObjectChangeSignal.Delta,
            (connection, logger, ct) => TimescaleSupport.ConvergeContinuousAggregateBatchingAsync(connection, logger, ct)),

        /* CREATE MATERIALIZED VIEW IF NOT EXISTS per aggregate plus its refresh policy (a quiet -1 once the
           converge above has made the windows match). THE step the issue is loudest about: its per-aggregate
           failure isolation is what leaves one rollup family missing on an otherwise healthy store. */
        new("continuous aggregates", StoreObjectConvergenceStage.Timescale, StoreObjectChangeSignal.InPlace,
            (connection, logger, ct) => TimescaleSupport.EnsureContinuousAggregatesAsync(connection, logger, ct)),

        /* #3597: AFTER the aggregates exist. A catalog read that finds nothing on a store already in the
           cheap shape, and a lock-timeout'd transaction per index when it finds one. */
        new("dedup materialization indexes", StoreObjectConvergenceStage.TimescaleAfterRepairLaunch, StoreObjectChangeSignal.Delta,
            (connection, logger, ct) => TimescaleSupport.EnsureIntervalDedupMaterializationIndexesAsync(connection, logger, ct)),

        /* #3581 (and #3620's chunk-width ensure inside it): AFTER the aggregates exist. One state read, then
           an ALTER only where compression is off, a policy only where none exists and a converge only where
           values differ — nothing on a converged store. */
        new("aggregate compression", StoreObjectConvergenceStage.TimescaleAfterRepairLaunch, StoreObjectChangeSignal.InPlace,
            (connection, logger, ct) => TimescaleSupport.EnsureAggregateCompressionAsync(connection, logger, ct)),

        /* #2007/#3653: BEFORE the fallback ensure (hygiene — the retired names left the ensure list, so it
           could not recreate them anyway). Judges each superseded pair against the tier horizon, which is a
           TIME-dependent verdict: today it flips only on a restart, and this is the step that makes "drops
           about five days after the successor arrives" true without one. */
        new("retired baseline relations", StoreObjectConvergenceStage.Ungated, StoreObjectChangeSignal.Delta,
            (connection, logger, ct) => TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, logger, ct)),

        /* #1757: probe-then-create per relation, and it NEVER touches an existing one (a continuous aggregate
           is also a relkind='v' view, so an unconditional CREATE OR REPLACE VIEW here would destroy a
           materialization). The second step the issue is loudest about: a gap here is one anomaly family
           silently returning nothing. */
        new("baseline fallback views", StoreObjectConvergenceStage.Ungated, StoreObjectChangeSignal.Delta,
            (connection, logger, ct) => TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, logger, ct)),
        /* #3899: the store's OWN per-statement timings — pg_stat_statements (preloaded by the managed conf's v13
           block), the SECURITY DEFINER readers get_store_query_stats calls, their grants to the reader roles,
           and the scrub of statements whose text can carry a credential. Ungated: nothing in it depends on
           TimescaleDB. After provisioning on the start path (this segment follows it), so the grantees exist.
           On the hourly pass, on every store shape since #3913, an extension a DBA creates by hand, or a
           function or grant that was dropped, heals within the hour. The PRELOAD is restart-only and lives in the conf, so this step cannot
           load the library, only report that it is not loaded. Counted in place: 1 when the reader is ready, 0
           in any of the named states that are not a failure. */
        new("statement statistics", StoreObjectConvergenceStage.Ungated, StoreObjectChangeSignal.InPlace,
            async (connection, logger, ct) => await StoreStatementStats.EnsureAsync(
                connection,
                PgSchemaGenerator.ConfigSchema,
                [DarlingManagedPostgres.AdminRoleName, DarlingManagedPostgres.ViewerRoleName, DarlingManagedPostgres.McpRoleName],
                logger,
                ct) == StoreStatementStats.SetupOutcome.Ready ? 1 : 0),

        /* #3573 and the composer's covering indexes: CREATE INDEX IF NOT EXISTS (a catalog check) and the
           per-table autovacuum overrides, plus the catalog-FILTERED hypertable insert-tuning sweep, which
           finds nothing once every hypertable carries the reloption. Wrapped whole rather than per-statement
           at its own call site today, which the issue names: one failure costs every index in the pass — so
           on this cadence the next hour retries it, which is the change. */
        new("composer performance tuning", StoreObjectConvergenceStage.Tuning, StoreObjectChangeSignal.InPlace,
            (connection, logger, ct) => PgTableTuning.ApplyAsync(connection, logger, ct)),
    };

    /// <summary>What one convergence pass did, accumulated across its segments so the start path's three
    /// connections still produce ONE summary line.</summary>
    internal sealed class StoreObjectConvergenceTally
    {
        public int Steps { get; set; }

        public List<string> Changed { get; } = new();

        public List<string> Failed { get; } = new();
    }

    /* The whole-pass budget for the hourly store-object convergence (#3817), the #2327 shape and the same
       reasoning as the retention re-evaluation's five minutes beside it: the pass is AWAITED on the serial
       sweep loop, and every statement inside it carries TimescaleSupport's 300 s bulk-setup CommandTimeout,
       which is right for a first hypertable conversion on an adopted store and wrong for something the fleet
       loop waits behind. Fourteen steps over fifty-one hypertables and twenty aggregates is a few hundred
       statements in the worst case, so the per-statement deadline bounds nothing useful here; one linked
       budget for the pass does.

       Five minutes rather than the retention pass's five for a different reason, which is why the number is
       stated rather than shared: on a CONVERGED store this pass is catalog reads and two lock-brief ALTERs,
       measured sub-second on a 2.28.1 rig, so the budget is not sizing the normal case at all — it is the
       bound on the pass that arrives on a store which is NOT converged, where a first aggregate CREATE or a
       first hypertable conversion is real work. That pass is the one a start path would have spent minutes on
       too, and cutting it short costs only the steps it had not reached: they are idempotent, so the next
       hour resumes at the one that was interrupted rather than redoing the ladder. */
    /// <summary>The convergence list, read-only, for tests that build a store the way the product does (#3908).</summary>
    internal static IReadOnlyList<StoreObjectConvergenceStep> StoreObjectConvergence => s_storeObjectConvergence;

    private static readonly TimeSpan s_storeObjectConvergenceBudget = TimeSpan.FromMinutes(5);

    /* The store self-metrics sweep's cadence (fleet-level, #2068). Store growth is a slow signal — the
       series exists to forecast weeks out, and the compression tier only changes state once a day per
       chunk — so hourly matches the compression check it rides beside, and each run is a handful of
       catalog-function reads plus ~30 narrow INSERTs. */
    private static readonly TimeSpan s_storeMetricsInterval = TimeSpan.FromHours(1);

    /* The Query Store backfill worker's tick (#2022): its OWN loop like the command plane, so a slow
       byte-budgeted slice can never delay or starve the collection sweep — the two share only the
       cancellation token and the guarded server snapshot. One slice per server per tick keeps it a
       trickle; the steady state (every tail drained, no holes) costs a candidate query and a few
       MIN() lookups per server per tick, which is why 5 minutes is ample. */
    private static readonly TimeSpan s_queryStoreBackfillInterval = TimeSpan.FromMinutes(5);

    /* The analysis pipeline's per-run budget — Lite's App default hardcoded (AnalysisTimeoutSeconds
       120; not a control-plane knob). The CADENCE (interval), the enabled gate, and the notify gate
       are now control-plane knobs read live from config.Analysis (config_alert_settings' analysis
       columns) — see the loop below. Each run analyzes the last 4 hours (Lite's hoursBack default). */
    private static readonly TimeSpan s_analysisTimeout = TimeSpan.FromSeconds(120);

    /* Clamp the store-driven analysis interval to Lite's accepted range (App clamps 5-360). */
    private const int MinAnalysisIntervalMinutes = 5;
    private const int MaxAnalysisIntervalMinutes = 360;

    /// <summary>
    /// The DEFAULT bounded per-server collection concurrency (the fire-and-track sweep, #1553): at most this
    /// many servers' collection bodies run at once, each opening at most ONE SQL connection (collectors stay
    /// sequential within a body — Lite's RemoteCollectorService shape). 4 clears a 24-server worst case in ~6
    /// waves while the 120s analysis budget stays de-clustered by the cadence jitter, so one slow/hung server
    /// can never head-of-line-block the fleet the way the old strictly-sequential foreach did (the 24-server
    /// field incident).
    ///
    /// <para>#2170: no longer the hard ceiling — an operator knob (config_service.max_concurrent_sweeps, V59)
    /// overrides it, because on a host with headroom watching a large fleet, 4-wide serialization is itself
    /// what makes sweeps queue and the Fleet Health screen report staleness while every collector is healthy
    /// (the reporter's 56-server case). This stays the DEFAULT and the seeded value.</para>
    /// </summary>
    internal const int MaxConcurrentServerSweeps = 4;

    /// <summary>
    /// The gate is constructed at this ceiling and immediately narrowed to the configured width (#2170) —
    /// a <see cref="SemaphoreSlim"/> cannot be resized, so unused permits are drained rather than the
    /// semaphore rebuilt (rebuilding would strand in-flight bodies releasing the old instance). Matches
    /// <see cref="StoreConfigProvider.MaxConcurrentSweepsLimit"/>, the store-read clamp ceiling.
    /// </summary>
    internal const int SweepGateCeiling = 16;

    /* Sweep-gate width state (#2170), all under _gateLock: the gate is built at SweepGateCeiling and its
       effective width is (ceiling - _gateAbsorbed). _gateDesiredAbsorb is where the knob wants that to
       land; a single absorber task closes the gap as in-flight bodies release permits. Holding the counts
       (rather than per-call deltas) is what makes a widen landing mid-narrow safe — see ReconcileSweepGate. */
    private readonly object _gateLock = new();
    private int _gateAbsorbed;
    private int _gateDesiredAbsorb;
    private bool _gateAbsorberRunning;

    /// <summary>
    /// The sweep gate's width right now (#2170) — the ceiling minus what has been absorbed. Reported by the
    /// queued-behind-the-gate diagnostic, which an operator reads while deciding whether to raise the knob,
    /// so it must never print the compile-time default once the knob has moved. Mid-narrow this reads the
    /// TARGET rather than the momentarily-larger real count; that is the honest number to act on.
    /// </summary>
    internal int EffectiveSweepWidth
    {
        get
        {
            lock (_gateLock)
            {
                return SweepGateCeiling - _gateDesiredAbsorb;
            }
        }
    }

    /// <summary>
    /// Seconds an in-flight collection body may go unresolved before the sweep watchdog surfaces it. One
    /// threshold serves both channels below — what differs is WHICH clock it is measured against.
    /// </summary>
    internal const int SweepWatchdogSeconds = 60;

    /// <summary>What (if anything) the in-flight watchdog should surface for one server this sweep.</summary>
    internal enum SweepEpisodeSignal
    {
        /// <summary>Nothing to say — under threshold, or already surfaced once this episode.</summary>
        None,

        /// <summary>The body is EXECUTING and has not finished: a genuine stall. Warning.</summary>
        Hang,

        /// <summary>The body has not started — still queued behind the concurrency gate. Capacity, Info.</summary>
        Queued
    }

    /// <summary>
    /// Pure decision for the in-flight sweep watchdog. Split out so the truth table is unit-pinned rather than
    /// buried in the loop, because getting it wrong is expensive in BOTH directions: attributing gate queue time
    /// to the hang channel fired ~82 warnings/hour at 24 servers behind the N=4 gate on a healthy fleet (burying
    /// the real signal), while dropping the queued case entirely would hide genuine capacity pressure — a body
    /// waiting minutes for a slot IS unserved. So a running body is judged on <paramref name="runningSeconds"/>
    /// (its own execution clock) and a queued one on <paramref name="episodeSeconds"/> (since launch), each
    /// latched to fire once per episode.
    /// </summary>
    internal static SweepEpisodeSignal ClassifySweepEpisode(
        double episodeSeconds,
        bool running,
        double runningSeconds,
        bool alreadyWarned,
        bool alreadyQueuedInfo)
    {
        if (running)
        {
            return !alreadyWarned && runningSeconds >= SweepWatchdogSeconds
                ? SweepEpisodeSignal.Hang
                : SweepEpisodeSignal.None;
        }

        return !alreadyQueuedInfo && episodeSeconds >= SweepWatchdogSeconds
            ? SweepEpisodeSignal.Queued
            : SweepEpisodeSignal.None;
    }

    /// <summary>
    /// #1581 cold-start launch-spread window (seconds). On a service restart the whole fleet's FIRST sweep bodies
    /// would otherwise launch in a single 15s tick and queue behind the <see cref="MaxConcurrentServerSweeps"/>
    /// gate, so the ones that waited past 60s logged "collection body has not completed after 60s" en masse (the
    /// field herd: 366 warnings over ~10 min, nothing actually broken — 0 collector errors, data landed). Each
    /// server's first launch is deferred by a deterministic per-server offset in [0, ColdStartSpreadSeconds); 150s
    /// mirrors the fixed post-connect analysis-phase window (<see cref="CadencePhaseOffset"/>) so no new tuning
    /// knob is introduced. Distinct from the per-collector #1575 seed jitter, which staggers WHICH collectors are
    /// due once a body runs, not WHEN the heavyweight connect body itself launches. A drift tripwire pins it.
    /// </summary>
    internal const int ColdStartSpreadSeconds = 150;

    /// <summary>
    /// The working-set launch-guard threshold, as a fraction of available memory (#1556). Pinned by a test
    /// (a drift tripwire): the launch guard is the fleet-level backstop against the commit-limit exhaustion
    /// the field incident hit, so its threshold must not silently drift.
    /// </summary>
    internal const double MemoryGuardFraction = 0.80;

    /// <summary>
    /// The working-set launch guard (#1556): whether the fleet sweep may launch NEW collection bodies this
    /// tick. Once the process working set crosses <see cref="MemoryGuardFraction"/> of available memory this
    /// returns false, so the launch loop stops STARTING new bodies and lets the in-flight ones drain — the
    /// process backs away from the 0→13GB commit-limit blowout instead of piling on more concurrent
    /// collectors. Purge/disk/analysis/delay keep running (the guard only gates NEW launches). Pure so a unit
    /// test pins the bands and the constant; the caller passes <c>Process.PrivateMemorySize64</c> (the metric
    /// that matched the incident — committed private bytes, not the GC heap) and
    /// <c>GC.GetGCMemoryInfo().TotalAvailableMemoryBytes</c>. A non-positive available figure (an unknown
    /// budget) never blocks collection.
    /// </summary>
    internal static bool ShouldLaunchSweeps(long workingSetBytes, long availableBytes)
    {
        if (availableBytes <= 0)
        {
            return true;
        }

        return workingSetBytes < MemoryGuardFraction * availableBytes;
    }

    /* The shutdown drain budget for the in-flight per-server bodies (#1553 fire-and-track): wait at most this
       long for launched bodies to finish before falling through to the serial command-loop drain. Budgeted
       INSIDE the host's default 30s ShutdownTimeout with headroom for that command-loop drain — a future bump
       MUST respect that ceiling or shutdown starts being force-killed mid-drain. */
    private static readonly TimeSpan s_shutdownDrainBudget = TimeSpan.FromSeconds(15);

    /// <summary>Test hook: the hardcoded per-run analysis budget, pinned against Lite's default.</summary>
    internal static TimeSpan AnalysisTimeout => s_analysisTimeout;

    /* #2299: how long a stopping sweep holds its analysis pass open so the pass can unwind BEFORE
       the loop's data source is disposed at RunCollectionLoopAsync scope exit. The pass observes
       the same stopping token (via AnalysisContext), so this is normally milliseconds; the bound
       exists for a pass stuck inside a store read. Sized WELL INSIDE the 15s s_shutdownDrainBudget
       (this await runs inside a drained sweep body) and the host's 30s ShutdownTimeout. */
    private static readonly TimeSpan s_analysisShutdownGrace = TimeSpan.FromSeconds(5);

    /// <summary>Test hook: the shutdown grace granted to an in-flight analysis pass (#2299).</summary>
    internal static TimeSpan AnalysisShutdownGrace => s_analysisShutdownGrace;

    /// <summary>
    /// The Stage 2 pause gate: whether the collection sweep does work this tick. FALSE while the service is
    /// paused (<c>config_service.paused</c>, mirrored into <c>_paused</c> on reload) — the loop then skips all
    /// collection/alert/analysis/purge work but keeps polling the reload beacon and the command queue, so a
    /// resume un-pauses it on the next tick. Pure so the gate is unit-testable without driving the loop.
    /// </summary>
    internal static bool ShouldRunCollection(bool paused) => !paused;

    /// <summary>
    /// #4299 (two-service pin): the relaunch decision, extracted pure so a second service's own read can be
    /// asserted directly rather than re-driving the whole Periodic pass. Launch a materialization-hole repair
    /// only when BOTH independent guards agree: this process is not already running one
    /// (<paramref name="repairRunningInThisProcess"/>, the in-memory <c>_materializationHoleRepairRunning</c>
    /// flag) AND the store's own repair-epoch stamp is stale under the current postmaster start
    /// (<paramref name="epochCurrentInStore"/> false, read via <see cref="TimescaleSupport.RawRepairEpochMatchesSql"/>).
    /// Either guard alone is not enough — two processes each with the in-memory flag clear would otherwise both
    /// launch — so this method states the AND explicitly rather than leaving it implicit in the call site.
    /// </summary>
    internal static bool ShouldLaunchMaterializationHoleRepair(bool repairRunningInThisProcess, bool epochCurrentInStore)
        => !repairRunningInThisProcess && !epochCurrentInStore;

    /// <summary>
    /// The network-endpoint startup warnings the worker emits AFTER <see cref="DarlingConfig.Validate"/>
    /// passes (darling-network-endpoints) — NEVER inside Validate(), which is all-fatal, so an optional,
    /// default-off endpoint note can never abort collection (D-BYO / D-validate):
    /// <list type="bullet">
    /// <item>BYO mode (<c>managed=false</c>) with any <c>postgres.network.*</c> or <c>mcp.network.*</c>
    /// set — the fields are IGNORED; the operator's own PostgreSQL governs exposure.</item>
    /// <item>Managed mode with an EXPOSED store whose <c>network.role</c> admits <c>admin</c> — names the
    /// <c>config_command</c> / <c>config_monitored_servers</c> / <c>config_notification</c>
    /// service-credential pivot a remote admin connection can reach (D7 — the operator's informed opt-in).</item>
    /// </list>
    /// Pure so a unit test asserts the returned strings without driving the loop or a live logger.
    /// </summary>
    internal static IReadOnlyList<string> GetNetworkStartupWarnings(DarlingConfig config)
    {
        var warnings = new List<string>();

        if (!config.Postgres.Managed)
        {
            /* BYO: network.* is managed-mode only. Warn per section that is set (D-BYO). */
            if (config.Postgres.Network?.IsConfigured == true)
            {
                warnings.Add(
                    "postgres.network.* is set but postgres.managed is false — it is IGNORED in bring-your-own mode; your own PostgreSQL governs its network exposure (pg_hba / listen_addresses / TLS).");
            }

            /* #1804: in a container the mcp/web network blocks ARE honored (the bind ladder's container
               gate), so this notice would be a lie there — the smoke test caught it warning IGNORED in
               the same breath as 'Starting MCP server on 0.0.0.0'. The postgres.network notice above
               stays: the bundled store never runs in BYO mode, container or not. */
            if (config.Mcp.Network?.IsConfigured == true && !Hosting.DarlingHostBinding.IsRunningInContainer)
            {
                warnings.Add(
                    "mcp.network.* is set but postgres.managed is false — the MCP network endpoint is managed-mode (or container, #1804) only, so it is IGNORED; the MCP server stays loopback-only.");
            }

            return warnings;
        }

        /* Managed + the store is genuinely exposed + the network role resolves to admin (D7 pivot warning). */
        var network = config.Postgres.Network;
        if (network is not null
            && DarlingNetwork.IsExposedListenAddress(network.Listen)
            && string.Equals(DarlingNetwork.NormalizeNetworkRole(network.Role), "admin", StringComparison.Ordinal))
        {
            /* "admits" rather than "is": since #2665 the field can name both roles, and NormalizeNetworkRole
               answers 'admin' for that too — correctly, because admin IS reachable. Wording it as "is 'admin'"
               would read as wrong to the operator who wrote "admin,viewer" and invite them to dismiss the one
               warning that matters here. */
            warnings.Add(
                "postgres.network.role admits 'admin' — a REMOTE admin connection can write config_command (the test_connect service-credential pivot), config_monitored_servers, and config_notification (webhook exfil). This is an explicit opt-in; the secure default is 'viewer' (read-only). Only expose admin on a trusted network.");
        }

        return warnings;
    }

    private readonly ILogger<DarlingWorker> _logger;
    private readonly ILoggerFactory _loggerFactory;

    /* Set once by ExecuteAsync before the loop starts; the observability writes need it. */
    private NpgsqlDataSource? _postgres;

    /* #4299: set for the lifetime of a materialization-hole repair this process launched, cleared in a
       finally around the same call — the Periodic pass's "no repair running in this process" half of the
       relaunch gate. A per-process flag, not a store one: the epoch stamp in the store (RawRepairEpochStampSql)
       is what stops a SECOND service from repeating the work; this flag only stops THIS process's own
       Periodic tick from launching a second overlapping repair while one it started is still running. */
    private volatile bool _materializationHoleRepairRunning;

    /* #4299/#4391: the Periodic pass's own launch of RunMaterializationHoleRepairAsync, kept so the same
       shutdown drain that awaits the start-path launch (holeRepair, above) also awaits this one — before
       this field existed the Periodic launch was fired with a bare "_ = ", neither drained on shutdown nor
       observed for a fault, so an exception it threw after the calling tick returned would go unlogged. */
    private Task? _periodicHoleRepair;

    /* #2138 phase 1: the auto force-plan bot, constructed by RunCollectionLoopAsync alongside the
       analysis pieces. Null until then. It holds no executor and this build ships none, so its whole
       output is journal rows — see PlanForceBot and PlanForceNoWritePathTests. */
    private PlanForceBot? _planForceBot;

    /* The live control-plane state (Stage 1): the last-seen config_version reload beacon and the
       current sparse per-collector schedule overrides. Both updated on startup and on each reload;
       read by the schedule-resolution path (TryConnectAsync / RunDueCollectorsAsync) and the reload. */
    private long _lastConfigVersion = -1;

    /* #2918: the compose statement_timeout last WRITTEN onto the viewer/mcp roles, not merely observed.
       Startup provisioning reads the store column itself and applies it, so the baseline is seeded from the
       first store view and a reload re-asserts only on a real change -- a config_version bump fires on any
       config_service or schedule write, and ALTER ROLE is a catalog write we should not pay for a knob
       nobody touched. -1 means "not yet known", which cannot equal any clamped value. */
    private int _appliedComposeStatementTimeoutSeconds = -1;

    /* #3914: this process provisioned the least-privilege roles on the compose distribution's own store, so the
       reload keeps their statement_timeout current as it does on a managed store. Set only on success. */
    private bool _composeStoreRolesProvisioned;
    private IReadOnlyList<ScheduleOverride> _scheduleOverrides = Array.Empty<ScheduleOverride>();

    /* The service-pause flag (Stage 2): read from config_service.paused on every reload and honored by
       the collection loop (skip collection/alert/analysis/purge while paused — Lite's IsPaused gate). Set
       ONLY by the main loop's reload (single writer); the command loop keeps running while paused so a
       resume command is processed. A pause/resume command writes the store, the bump trigger fires the
       reload beacon, and the reload sets this — the same path every other control-plane setting takes. */
    private bool _paused;

    /* Guards structural access to the monitored-server list because the command loop (a concurrent task)
       looks servers up by id while the main loop reconciles (adds/removes) them and the alert / analysis
       plan/failed-job fetchers enumerate them. Only ever held for the microsecond lookup/mutation — never
       across collection I/O — so a long-running command never blocks the collection loop on it. */
    private readonly object _serversLock = new();

    /* Server IDs whose scheduled analysis is currently running — prevents relaunching
       analysis for a server whose previous (possibly hung) pass has not finished
       (Lite's CollectionBackgroundService in-flight guard). The value carries when the pass started
       and how loudly it has been reported, because #2430's defect was that a pass which never
       finishes leaves this marker set FOREVER and the server is then skipped in silence on every
       later cycle. */
    private readonly ConcurrentDictionary<int, AnalysisPassState> _analysisInFlight = new();

    /* How far past its budget an in-flight pass must be before the sweep starts reporting it. The
       ordinary overrun already gets the "exceeded Ns" warning; this is for the pass that ignored the
       cancellation raised at that budget — wedged inside one of the store reads that still take no
       token (see the note on RunAnalysisPassAsync), which no token can reach. */
    private const int StuckAnalysisMultiple = 3;

    /* Reports back off by doubling. A fixed repeat interval would either be slower than the analysis
       cadence (useless) or produce one line per cycle forever (the spam that makes a log unreadable).
       Doubling gives a handful of lines in the first day and a handful per day after — loud enough to
       be noticed, quiet enough to stay noticed. Capped so the shift cannot run away on a service that
       stays up for months, which this one does. */
    private const int StuckAnalysisMaxBackoffDoublings = 20;

    /// <summary>
    /// Bookkeeping for one in-flight analysis pass (#2430). A class, not a struct, so the sweep can
    /// update the report counters in place without a read-modify-write race against the completion
    /// continuation, which only ever removes the whole entry.
    /// </summary>
    private sealed class AnalysisPassState
    {
        public AnalysisPassState(DateTime startedUtc) => StartedUtc = startedUtc;

        public DateTime StartedUtc { get; }

        /// <summary>Cycles this server has lost to the pass still being in flight.</summary>
        public int SkippedCycles { get; set; }

        /// <summary>How many times it has been reported, which is what the backoff doubles on.</summary>
        public int ReportCount { get; set; }
    }

    /* MinValue = the first sweep after startup runs the retention purge, then daily. */
    private DateTime _nextPurgeUtc = DateTime.MinValue;

    /* MinValue = the first sweep after startup evaluates the store disk-pressure self-alert, then every
       s_diskCheckInterval. Fleet-level (one shared store), so it is a single field, not per-server. */
    private DateTime _nextDiskCheckUtc = DateTime.MinValue;

    /* MinValue = the first sweep after startup runs the custom-alert-rule health check (#3304), then every
       s_customAlertHealthInterval. Fleet-level (the rules are a fleet concept), so a single field. */
    private DateTime _nextCustomAlertHealthCheckUtc = DateTime.MinValue;

    /* MinValue = the first sweep after startup evaluates the stale-mute check (#3306), then every
       s_staleMuteCheckInterval. Fleet-level (mute rules are a store-wide concept), so a single field. */
    private DateTime _nextStaleMuteCheckUtc = DateTime.MinValue;

    /* #3514: next due time for the web-dashboard TLS certificate expiry self-alert. Fleet-level (the web
       certificate is a store-wide concept), so a single field like the stale-mute cadence above. */
    private DateTime _nextWebTlsCheckUtc = DateTime.MinValue;

    /* Next due time for the managed store-settings self-alert (#4215). Fleet-level (a managed
       store's settings are a store-wide concept), so a single field like the stale-mute cadence above. */
    private DateTime _nextStoreSettingsCheckUtc = DateTime.MinValue;

    /* MinValue = the first sweep after startup drains the oversized-plan backlog (#3392), then on whatever
       OversizedPlanBacklogSweep.NextSweepDelay returns for what that tick found. Both the cadence and the
       decision live with the sweep rather than here: its own doc derives the interval from the compile ages
       of the plans it fetches, and the empty-target branch from what an empty target list means on the host
       in question. This field is only the stamp. Fleet-level (one field, the sweep iterates servers itself). */
    private DateTime _nextOversizedPlanSweepUtc = DateTime.MinValue;

    /* Re-attempts already spent waiting for a first connected SQL Server runtime (#3405) — the counter
       OversizedPlanBacklogSweep.NextSweepDelay advances and bounds. Zero in steady state: any tick that
       reaches a connected target clears it, and a tick on a host with no sweepable target never spends it.
       Held at the maximum once spent rather than cleared, so a fleet whose targets never connect pays the
       short-wait burst once per process. Read and written only on the fleet-loop thread, like the stamp
       above. */
    private int _oversizedPlanSweepConnectWaits;

    /* The in-flight backlog pass (#3392), tracked so the cadence above cannot start a second one on top of
       it. A pass that is still running when its next slot comes round simply skips that slot: the backlog is
       a best-effort errand and there is nothing in it that a later hour cannot do. */
    private Task? _oversizedPlanSweep;

    /* #4130: the in-flight daily retention purge, fire-and-tracked like the per-server sweeps and the
       oversized-plan backlog above rather than awaited inline. Measured at 346-400s deleting ~839k rows;
       awaited on this loop, that is 346-400s in which NO server's sweep body launches and the whole fleet
       reads stale — the same shape as the oversized-plan backlog's field incident, one maintenance step
       over. Tracked (not fire-and-forget) so the launch loop can see it is still running and skip a second
       launch, and so shutdown can drain it instead of abandoning a live DELETE. */
    private Task? _purgeTask;

    /* MinValue = the first sweep after startup evaluates the compression-job self-heal check (#1581), then
       every s_compressionCheckInterval, pinned to :30 past the minute by TimescaleSupport.NextCompressionCheckUtc
       (#3575) so no steady-state sample lands on the :MM:00 instant the compression policies fire on. The
       first sample is deliberately left unpinned — a restart is when an operator is reading the log and wants
       the store's job health now — and the confirm-read inside ReadStuckPolicyJobsAsync covers it like
       every other sample. Fleet-level (one shared store), so it is a single field, not per-server. Since
       #3812 the same due time also fires the hourly retention re-evaluation
       (ReevaluateRetentionPoliciesAsync), AFTER the compression read so the :30 sample is not pushed by the
       pass ahead of it; since #3815 it fires the availability re-probe
       (ReprobeTimescaleAvailabilityAsync) AHEAD of both, which is why the stamp advances whether or not the
       store is on TimescaleDB — the probe by definition runs while _timescaleAvailable is false, and a due
       time that only moved behind that flag would fire it on every 15-second sweep pass. One stamp, three
       failure-isolated tenants. */
    private DateTime _nextCompressionCheckUtc = DateTime.MinValue;

    /* MinValue = the first loop pass after startup runs the fleet sweep (#3466 lane 2), then on the
       operator-configured cadence (fleet_sweep_interval_minutes, default hourly, clamped by
       FleetSweepEngine.ClampIntervalMinutes). Fleet-level by definition — a sweep is one statement about
       the whole fleet — so a single field. An immediate first sweep after a restart is deliberate: it
       lands inside the engine's post-restart settle window and says so, which re-establishes the timeline
       quickly with an honest startup-transient document rather than leaving an hours-wide gap. */
    private DateTime _nextFleetSweepUtc = DateTime.MinValue;

    /* The in-flight fleet sweep (#3466), tracked so the cadence above cannot start a second one on top of
       it — the oversized-plan backlog's exact shape. A sweep still running when its next slot comes round
       skips that slot; the following one diffs against whatever the last COMPLETE sweep persisted, so
       nothing is lost but the slot. */
    private Task? _fleetSweep;

    /* MinValue = the first sweep after startup records the store self-metrics snapshot (#2068), then every
       s_storeMetricsInterval. Fleet-level (one shared store), so it is a single field, not per-server. NOT
       gated on _timescaleAvailable at the loop: the dimension and whole-store rows apply to plain-PG stores
       too; only the per-hypertable arm inside the sweep needs (and gets) the flag. */
    private DateTime _nextStoreMetricsUtc = DateTime.MinValue;

    /* #2674: per-collector cost on the monitored servers, accumulated in memory and flushed hourly on the
       store-metrics tick. Held here (not in the runner) so its lifetime matches the sweep that drains it. */
    private readonly CollectorCostAccumulator _collectorCost = new();

    /* #4004 review, round 3: "the log-hash key was replaced at start", held for the first pg_log_events run. */
    private readonly LogHashKeyRotationNote _logHashKeyRotation = new();

    /* Fleet-level working-set launch-guard latch (#1556): true once ShouldLaunchSweeps has tripped this
       episode, so its CRITICAL log is emitted ONCE rather than every sweep (the WarnedThisEpisode idiom —
       but fleet-wide: the guard is about the whole process's working set, so it is a single worker field,
       NOT a per-ServerLoopState flag). Cleared when the working set recovers below the threshold. */
    private bool _memoryGuardTrippedThisEpisode;

    /* The store's TimescaleDB availability: seeded by the start-path detection and RE-PROBED on the hourly
       store-maintenance tick for as long as it reads false (#3815). What it records is whether a detection
       ATTEMPT succeeded, not whether the extension exists — the start-path block force-clears it on any
       fault, and the faults that reach it are transient (the store restarting, a failover, a lock held
       elsewhere, a momentary connection failure). A false value nothing revisits runs a genuinely Timescale
       store in plain-PostgreSQL mode for the life of a service designed to run for months, AND switches off
       the store background-job health check that is the only surface that would report it, so the latch is
       re-decided hourly rather than at startup only. Every consumer reads it at call time — the retention
       purge's drop_chunks branch, the self-metrics sweep's hypertable arm, the two provider delegates — so a
       flip mid-run is picked up by each of them on its next pass with no further wiring. */
    private bool _timescaleAvailable;

    /* #3915, #3944: the re-mask pass over store-log rows captured before this build, which normalizes their
       SQL and re-keys their message. The cursor is where the last hourly slice stopped; done once a slice
       reaches the table's end, and then not again this process (new captures are written that way, and the
       pass is idempotent, so the next process's pass is a no-op re-read). */
    private string? _storeLogRemaskCursor;

    private bool _storeLogRemaskDone;

    /* #4012: the re-mask of PostgreSQL deadlock alerts, then deadlock reports, then stored analysis findings'
       deadlock exemplars, stored before #4005 with their SQL raw. The alerts go first because they are found
       through the reports' raw hashes, which the report stage replaces; the findings last, so their read cannot
       hold the reports back. In memory, like the store-log cursor above (#4012's review ruled out a store
       migration): each stage is done after a whole walk that rewrote nothing, and a restart walks again. */
    private readonly PgDeadlockRemask.RemaskProgress _pgDeadlockRemask = new();

    /* #4012: the re-mask keys an alert whose report is gone under the store's log-hash key. Null when the service
       has none; every other stage runs without it (#4012's review, finding 1). */
    private PgLogHashKey? _pgDeadlockRemaskKey;

    /* #3971: a store-log capture that fails for a reason the store will not change on its own warns once per
       process, then logs at Debug: 58P01, no log directory (the Linux compose store logs to stderr and has
       none), and 42501, a login without the log read. get_store_log already reports the gap on the read
       surface, so the hourly Warning only repeated what nobody was going to change. */
    private bool _storeLogCaptureUnavailableWarned;

    /* Stage 4 service self-alerts (collection-stopped, connection lost/restored, capture-down). Built
       once in RunCollectionLoopAsync over the SAME deliverer/history the shared engine uses, so the
       self-alerts inherit its delivery/cooldown/restart-replay. Held as a field because the connection
       edge fires from TryConnectAsync and the reconcile drops per-server state through it. */
    private DarlingSelfAlertEvaluator? _selfAlerts;
    /* The delta calculator, held for the same reason as _selfAlerts (#3540 A4): the reconcile-remove branch
       drops a departing server's baselines and pass window through it, so a server removed and re-added
       inside the gap policy's hour cannot subtract the new identity's counters from the old one's. Built
       and seeded once in RunCollectionLoopAsync, ahead of the runner that shares it. */
    private CollectorDeltaCalculator? _deltas;
    /* Concrete rather than IAlertDeliverer: there is exactly one implementation here and it is constructed
       a few lines from where this is assigned, so the interface bought an indirection per delivered alert
       and no seam (CA1859). */
    private DarlingAlertDeliverer? _alertDeliverer;

    /* The mute check and the cooldown stamps for the PostgreSQL predictors. These ride alongside the shared
       AlertEngine rather than inside it, which is deliberate — but "alongside" was taken to mean "without",
       and the PG path shipped with Muted hardcoded false and no cooldown at all. Every AlertEngine family
       gates on both; a 30-second sweep without them writes ~2,880 history rows a day per breaching subject
       and emails through a mute rule that says not to. */
    private Func<AlertMuteContext, bool>? _isAlertMuted;

    /* #3285: the user-authored custom-alert evaluator. Null when the deployment cannot supply a viewer-role
       pool for it (BYO / non-Windows in this first slice); custom alerts are simply not evaluated there. */
    private CustomAlertEvaluator? _customAlertEvaluator;

    private readonly ConcurrentDictionary<string, DateTime> _lastPostgresAlert = new(StringComparer.Ordinal);

    /* #2711: Postgres Deadlocks/Blocking, mirroring AlertEngine's own field shape for the SQL Server
       versions of these two alerts (RollingCountAlertGate + a watermark + an active flag + a
       last-fired stamp) rather than the simpler single-timestamp cooldown the three Tier 0 predictors
       above use. Deadlocks and blocking are ROLLING-WINDOW COUNTS (the same event can sit in the
       window for the whole hour it takes to age out), which is exactly the shape #1091 fixed for SQL
       Server: a plain "still above threshold" check re-fires the SAME already-reported event every
       cooldown. RollingCountAlertGate is the shared, engine-agnostic fix for that, already proven and
       already living in PerformanceMonitor.Alerting - reusing it here is what keeps this immune to the
       #2704/#2708 class of bug (a cooldown timer with no memory of which data point it last fired on)
       from day one, instead of needing its own follow-up fix later. */
    private readonly ConcurrentDictionary<string, DateTime> _lastPgDeadlockAlert = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _activePgDeadlockAlert = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> _lastAlertedPgDeadlockCount = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTime> _lastPgBlockingAlert = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _activePgBlockingAlert = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> _lastAlertedPgBlockingCount = new(StringComparer.Ordinal);
    /* Long-Running Query is a LIVE-STATE check ("is one running right now"), not a rolling event count like
       Deadlocks/Blocking above — so it needs only a cooldown timestamp and an active flag, the same shape
       AlertEngine itself uses for its own SQL Server Long-Running Query check, not RollingCountAlertGate. */
    private readonly ConcurrentDictionary<string, DateTime> _lastPgLongRunningQueryAlert = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _activePgLongRunningQueryAlert = new(StringComparer.Ordinal);
    /* #2711: Poison Wait is an ACCUMULATION check (how much wait time accrued in the read's own window),
       the same shape as AlertEngine.CheckPoisonWaitsAsync for SQL Server — so it carries that method's
       exact state kit rather than RollingCountAlertGate: a cooldown stamp, an active flag for the
       Detected/Cleared edge, and the #2704 last-fired-on collection_time guard, without which a
       cooldown-elapsed re-read of the SAME still-uncollected store row re-fires on data already reported
       (the collector's delivered cadence and the alert cooldown are independent clocks). Keyed per
       server|metric|subject like _lastPostgresAlert — the two poison events are different incidents with
       different remedies, so one must not consume the other's cooldown (#1140). */
    private readonly ConcurrentDictionary<string, DateTime> _lastPgPoisonWaitAlert = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _activePgPoisonWaitAlert = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTime> _lastPgPoisonWaitCollectionTime = new(StringComparer.Ordinal);
    /* #2716's restart-survival shape for the poison cooldown: seeded once per key from
       IAlertHistoryStore.GetLastAlertTimeAsync (the subject IS the #1140 dedup fingerprint the alert
       fires with), exactly like _postgresAlertHistorySeeded below. The seed also floors the #2704
       collection-time guard: rows collected before the last recorded fire were, by definition, already
       reported by the process that fired it. */
    private readonly ConcurrentDictionary<string, bool> _pgPoisonWaitCooldownSeeded = new(StringComparer.Ordinal);
    /* #3653: per SERVER, the "held on an unobserved window" Debug line has been written for the current
       stretch of collector silence. Set when a sweep holds at least one active poison subject because the
       window had no collector run and no row; removed on the next observed window, so each outage logs
       once rather than every 30 s sweep. Keyed on the server, not the subject, because the silence is the
       collector's, not any one event's. */
    private readonly ConcurrentDictionary<string, bool> _pgPoisonWaitHoldLogged = new(StringComparer.Ordinal);

    /* #2719: CPU is a continuous gauge, so it does not need RollingCountAlertGate, which exists for
       rolling-WINDOW COUNTS (Deadlocks/Blocking) where the same event can sit in the window across several
       sweeps. The cooldown timestamp stays live-state, mirroring AlertEngine's own _lastCpuAlert.

       #3282: the active flag that used to sit beside it is GONE, replaced by the shared
       AlertPersistenceGate's record — which carries the same "an incident is open" bit plus the streak
       that earned it, and is PERSISTED. Both halves of that mattered here. The bool was in-memory only,
       so a restart over a standing condition re-announced an incident the operator already had open; and
       the streak has to live in the same value as the flag, because a caller that can advance one without
       the other is a caller that can lose a signal. Seeded once per key from the same
       config.alert_persistence_state row AlertEngine's SQL Server twin reads, under the same "High CPU"
       metric name — the parity #2719 chose for the metric strings means no second row shape is needed. */
    private readonly ConcurrentDictionary<string, DateTime> _lastPgCpuAlert = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, AlertPersistenceRecord> _pgCpuPersistence = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _pgCpuPersistenceSeeded = new(StringComparer.Ordinal);

    /* #2716: none of the Postgres alerts' watermarks above survive a restart — AlertEngine seeds its
       own SQL Server twins of _lastAlertedPgDeadlockCount/_lastAlertedPgBlockingCount from
       IAlertStateStore on each server's first post-restart sweep (EnsureWatermarksSeededAsync), but
       nothing does the equivalent here, so a restart resets the watermark to 0 and the very next sweep
       re-fires on a deadlock/blocking count still sitting in the rolling window from before the
       restart. Seeded once per (server, metric) key, mirroring AlertEngine's _seededServerKeys. */
    private readonly ConcurrentDictionary<string, bool> _pgDeadlockWatermarkSeeded = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _pgBlockingWatermarkSeeded = new(StringComparer.Ordinal);

    /* #2716: the three Tier 0 predictors' cooldown (_lastPostgresAlert, keyed per server|metric|subject)
       has the same restart gap, but no watermark COUNT to seed — it is a plain last-alerted TIME per
       subject, so IAlertStateStore's int-watermark shape does not fit it. Seeded instead from
       IAlertHistoryStore.GetLastAlertTimeAsync's existing #1154 dedup-key filter (finding.Subject IS
       the #1140 dedup fingerprint these alerts already fire with), which already reconstructs a
       per-fingerprint last-alerted time for the email/webhook cooldowns and needs no new schema.
       Guards one history read per (server, metric, subject) for the life of the process — see the call
       site for why an unconditional per-sweep read would be a real cost, not just noise. */
    private readonly ConcurrentDictionary<string, bool> _postgresAlertHistorySeeded = new(StringComparer.Ordinal);

    /// <summary>
    /// Held for the same reason <see cref="_alertDeliverer"/> is: the Postgres Deadlocks/Blocking alerts
    /// (#2711) need to write a resolution history row on the active→inactive transition, exactly like
    /// <see cref="BuildAlertEngine"/>'s <c>resolutionCallback</c> does for the SQL Server families - a
    /// resolution has no send channel (<see cref="AlertResolution"/>'s own doc comment), so it goes
    /// straight to history rather than through <see cref="_alertDeliverer"/>.
    /// </summary>
    private PgAlertHistoryStore? _historyStore;

    private int _alertCooldownMinutes = 15;

    /* #1560: the live MCP enable/port seam — published to the MCP host's supervisor at startup and on
       every control-plane reload, so the viewer's Settings toggle takes effect without a restart. */
    private readonly McpRuntimeState _mcpState;

    /* #1562: the live WEB dashboard enable/port seam — the twin of _mcpState, published to the web host's
       supervisor at startup and on every control-plane reload so the viewer's Settings toggle takes effect
       without a restart. */
    private readonly WebRuntimeState _webState;

    /// <summary>#3514: the served web-dashboard TLS certificate's expiry, published by the web host and read
    /// each sweep so the certificate-expiry self-alert fires without a restart.</summary>
    private readonly WebTlsCertificateState _webTlsCertState;

    /* #2298: the live monitored-server registry seam — published beside the two above, read by the MCP
       host's plan-fetch resolver so it never re-reads config_monitored_servers as the mcp role (whose
       encrypted_password SELECT-carve fails that whole read). */
    private readonly MonitoredServerRegistryState _registryState;

    /// <summary>#3013: the process counter this worker's own swallowed alert reads are tallied on —
    /// the alert pass entry point, the six PostgreSQL predictor passes, and the store background-job
    /// health reads behind the fleet-scoped self-alerts. The same instance the engine and the
    /// self-alert evaluator are constructed with.</summary>
    private readonly AlertReadFailureCounter _readFailures = AlertReadFailureCounter.Shared;

    /// <summary>#3182: the process-lifetime high-water mark behind the refresh-ceiling staleness finding.
    /// Held HERE rather than inside TimescaleSupport because the lifetime the rate limit needs is this
    /// process — a store whose recorded ceiling has drifted overtakes it on a large share of hourly sweeps,
    /// so a limiter recreated per sweep would limit nothing — and static state would buy that lifetime at
    /// the price of tests that cannot arrange a sequence of readings without leaking it to each other.</summary>
    private readonly RefreshCeilingStalenessWatch _refreshCeilingStaleness = new();

    /* #2953: the collector's own startup verdict, published to the web host so /api/ping can report whether
       collection is actually running WITHOUT reading the store. The other three seams carry control-plane
       values the store is the authority for; this one carries the one fact the store cannot be asked about,
       because the failure it reports is the store being unreachable. Every collection-blocking exit below
       publishes before it returns — a stand-down that completes the worker task successfully, leaves the host
       up and the Windows service reporting Running, and diagnoses itself only in a file log, is otherwise
       indistinguishable from a healthy service on every automated surface there is. */
    private readonly CollectorRuntimeState _collectorState;

    /* #3941: the process's shared baseline tier, handed to every per-pass DarlingAnalysisService so a pass inside an
       analysis hour another pass (or an MCP/web read) already computed reads no 30-day baseline — the same singleton
       the MCP and web hosts hand theirs. */
    private readonly BaselineCache _baselineCache;

    public DarlingWorker(ILogger<DarlingWorker> logger, ILoggerFactory loggerFactory, McpRuntimeState mcpState, WebRuntimeState webState, MonitoredServerRegistryState registryState, CollectorRuntimeState collectorState, WebTlsCertificateState webTlsCertState, BaselineCache baselineCache)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
        _mcpState = mcpState;
        _webState = webState;
        _registryState = registryState;
        _collectorState = collectorState;
        _webTlsCertState = webTlsCertState;
        _baselineCache = baselineCache;
    }

    private sealed class ServerLoopState
    {
        /* Settable so the reconcile can replace a still-connected server's definition on a config
           change (host/auth/excluded-dbs/cost) — paired with dropping Runtime to force a reconnect. */
        public required MonitoredServer Config { get; set; }
        public ServerRuntime? Runtime { get; set; }

        /* ConcurrentDictionary (#1553 D1): with the fire-and-track sweep the per-server body runs on a pool
           thread, so a reload's RecomputeNextDueAsync on the OUTER thread can touch this map concurrently with the
           body's RunDueCollectorsAsync read-and-advance. It is only ever INDEXED by the static collector-catalog
           keys (never enumerated), so a lock-free concurrent map is a drop-in and eliminates the one structure
           the old strict single-threaded invariant (INV-1) existed to protect from tearing. */
        public ConcurrentDictionary<string, DateTime> NextDue { get; } = new(StringComparer.OrdinalIgnoreCase);
        public DateTime NextConnectAttempt { get; set; } = DateTime.MinValue;

        /* #2255: the last connect-failure message logged in FULL, so an unchanged cause repeats as one terse
           line instead of its whole explanation every 60 seconds forever. The field report is a DPAPI decrypt
           failure — permanent by construction, since the blob can never become decryptable on this host — and
           at Warning-with-full-text it buried the log while never once telling the operator anything new.
           Compared on the message rather than the exception type so a changed cause (credential fixed, server
           now genuinely unreachable) prints in full again. */
        public string? LastConnectFailureLogged { get; set; }

        /* #2228: the database-mismatch state last reported for this server, so the tripwire fires on the
           TRANSITION rather than once per connect. A mismatch is a standing misconfiguration — it persists
           until an operator edits the registration — so logging it every reconnect would bury the one line
           that matters, which is how a tripwire gets trained past and stops working. Null = last seen
           correct; the message itself = last seen wrong, compared so a change of mismatch re-reports. */
        public string? LastDatabaseMismatchLogged { get; set; }

        /* MinValue = the first loop pass after connect evaluates alerts immediately. */
        public DateTime NextAlertSweep { get; set; } = DateTime.MinValue;

        /// <summary>
        /// The slowest NON-budgeted collector seen so far in the current sweep body (#2864), or -1 before
        /// any has run. Reset per body, so it describes one sweep rather than the server's history.
        ///
        /// <para>Recorded on every row, not only abandoned ones: a ratio needs a denominator, and the
        /// baseline for 'were this body's ordinary collectors slow' has to come from the same column on
        /// ordinary bodies. Storing it only on failures would rebuild the cross-referencing this exists
        /// to remove.</para>
        /// </summary>
        public int SweepPeerMaxMs { get; set; } = -1;

        /* MinValue = the first loop pass evaluates the Stage 4 service self-alerts (collection-stopped,
           capture-down) immediately. Separate from NextAlertSweep because the self-alert sweep runs for
           a DISCONNECTED server too (collection-stopped is exactly the unreachable-server case), above
           the Runtime-null connect gate. */
        public DateTime NextSelfAlertSweep { get; set; } = DateTime.MinValue;

        /* #3467: the same-statement-pileup sweep's cadence stamp. Its own stamp rather than a rider on
           NextAlertSweep because the two answer to different masters — the alert sweep is engine
           evaluation (master-off stops it entirely) while the pileup sweep is analysis PRODUCTION
           (D0: it runs and persists under master-off; only delivery goes quiet) — and coupling their
           due-times would let a change to one family's cadence silently move the other's. */
        public DateTime NextPileupSweep { get; set; } = DateTime.MinValue;

        /* #3467: the newest query_snapshots collection_time the pileup sweep has already evaluated for
           this server, so a 30-second sweep cadence over a ~60-second collector produces one evaluation
           per snapshot instant rather than re-detecting (and re-persisting) the same pileup. In-memory
           on purpose: after a restart the worst case is one repeated evaluation of the newest snapshot,
           whose finding folds onto the same incident id and whose notification the cooldown (seeded
           from the alert log on first lookup) suppresses. */
        public DateTime LastPileupSnapshotEvaluated { get; set; } = DateTime.MinValue;

        /* MinValue = the first loop pass evaluates the custom-alert rules immediately. Separate from
           NextAlertSweep and above the connect gate for the same reason as the self-alert sweep: a custom
           rule reads the collected store, which exists whether or not the server is currently connected. */
        public DateTime NextCustomAlertSweep { get; set; } = DateTime.MinValue;

        /* Default MinValue; on connect TryConnectAsync PHASES this to now + a sub-2.5-minute per-server offset
           (#1553 jitter site 3) when analysis is enabled, so a fleet restart does not make every freshly
           connected server analysis-due in the same sweep (with N=4 that would cluster 4 analysis passes at
           once). The pipeline's own 24h data-span gate still no-ops it until the store has enough history
           (Lite's GetTotalDataSpanHoursAsync gate), so a fresh server simply re-checks every interval while an
           already-populated store analyzes promptly (within a ~2.5-minute phase spread). Left at MinValue when
           analysis is disabled so re-enabling runs immediately. */
        public DateTime NextAnalysisDue { get; set; } = DateTime.MinValue;

        /* Serializes this server's collector batch so an on-demand snapshot_now (from the command loop)
           and the scheduled sweep (from the main loop) never run the same server's collectors at once —
           which would double-COPY overlapping rows and race the shared delta baselines. The main loop
           try-acquires with a zero timeout (skip this server this sweep if a snapshot holds it); the
           snapshot waits its turn. Binary (1,1). */
        public SemaphoreSlim CollectionGate { get; } = new(1, 1);

        /* Fire-and-track sweep bookkeeping (#1553 D2/D2b) — these FOUR written ONLY by the OUTER sweep thread
           (the launch loop and the shutdown drain); the per-server body NEVER touches them, so there is no
           cross-thread tear to reason about (the torn-read note that applies to the DateTime schedule fields
           above deliberately does NOT apply here). InFlightSweep is this server's currently-running (or
           last-completed) body Task — the launch loop skips relaunch while it is not completed (INV-2: one body
           per server) and the shutdown drain awaits it. SweepStartedUtc is stamped at LAUNCH, so it still
           measures the whole EPISODE (queue + run): a body queued minutes behind the N=4 gate IS unserved, and
           that remains true. WarnedThisEpisode gates the one-Warning-per-episode HANG log and
           QueuedInfoThisEpisode the one-Info-per-episode QUEUED log, so each is surfaced once, not every sweep. */
        public Task? InFlightSweep { get; set; }
        public DateTime SweepStartedUtc { get; set; }
        public bool WarnedThisEpisode { get; set; }
        public bool QueuedInfoThisEpisode { get; set; }

        /* The ONE piece of sweep bookkeeping the BODY writes — and the single reason it is a FIELD rather than a
           property: Interlocked needs a ref to a field. UTC ticks stamped by the body the moment it acquires the
           concurrency gate; 0 while it is still QUEUED behind that gate. Written once per episode by the body
           (Interlocked.Exchange) and read by the outer launch loop (Interlocked.Read) — a long is not guaranteed
           atomic on 32-bit, so BOTH sides go through Interlocked rather than assuming it. The existing three
           fields above keep their outer-thread-only invariant untouched; this is a separate field precisely so
           that invariant does not have to be weakened.

           Why it exists: the 60s watchdog is a HANG detector — "the field incident was HANGS, not throws" — but
           measuring from SweepStartedUtc alone counted GATE QUEUE TIME as hang time, so at 24 servers behind the
           N=4 gate it fired ~82 times/hour on a demonstrably healthy fleet (0 collector errors, data landing) and
           buried the very signal it exists to raise. Splitting run time out restores it: a body merely waiting
           its turn is reported as CAPACITY, never as a hang. */
        public long RunStartedTicks;

        /* #1581 cold-start stagger: the earliest UTC this server's FIRST post-startup sweep body may launch —
           the captured startup instant plus a deterministic per-server CadencePhaseOffset capped at
           ColdStartSpreadSeconds — so a service restart does not launch every server's initial catch-up body in
           one tick and slam the N=4 gate (the field herd that logged "collection body has not completed after
           60s" en masse). Seeded ONCE at construction for the initial fleet (single-threaded startup) and read
           only by the OUTER launch loop; a reconcile-ADDED server keeps the MinValue default and launches
           promptly (a single add is not a herd). Gates ONLY the first launch (InFlightSweep still null), so
           steady-state cadence is untouched. */
        public DateTime FirstSweepDueUtc { get; set; } = DateTime.MinValue;

        /* Retired containment (#1553 D1): set TRUE (write-once) by the reconcile-remove branch when this server
           is disabled/deleted, alongside Runtime=null and _selfAlerts.Forget. A body launched — or gate-queued —
           before the removal re-checks this as its FIRST statement after acquiring the fleet gate and no-ops, so
           a just-disabled server is never connected, never has XE CREATE SESSION DDL run against it, and never
           re-writes self-alert edge state after Forget removed it. volatile because the body reads it on a pool
           thread after the outer thread's write. A remove+re-add mints a FRESH ServerLoopState, so there is no
           reset and no ABA. */
        public volatile bool Retired;

        /* Last-applied enabled state of the OPT-IN long-query completion XE session (#1496), so the
           per-sweep reconcile only opens a connection to run CREATE/DROP DDL when the desired state
           actually changes — null = not yet reconciled (next sweep applies it), true = ensured
           (enabled), false = confirmed dropped (disabled). Reset to null on every (re)connect so a
           stopped Azure database-scoped session is re-ensured; in steady state a default-off collector
           is drop-confirmed once per connect and then skipped every sweep. Written by the per-server
           sweep body and the connect path (both run on the pool thread, one at a time per server —
           INV-2), like the DateTime schedule fields above. */
        public bool? LongQueryTraceApplied { get; set; }

        /* #3754: what the last long-query reconcile could NOT do, carried from the reconcile (which runs
           before the collector sweep and outside any collector run) to the run record the collector sweep
           writes. Two slots because they land on the run two different ways.

           LongQueryTraceFault is set when the reconcile THREW while enabling: on-prem the one CREATE
           failed; on Azure SQL DB the CREATE was refused in EVERY monitored database (DarlingXeSessions
           throws the first failure for that case since #3754). The session exists nowhere the collector
           would read, so RunOneAsync records the run SESSION_MISSING with this message instead of
           dispatching a read that can only return zero rows - the tolerant ring-buffer read cannot tell an
           absent session from a quiet one, which is exactly how the issue's runs recorded SUCCESS. Cleared
           by the next reconcile that does not throw; LongQueryTraceApplied stays null on a throw (existing
           behaviour), so that retry happens on the very next sweep.

           LongQueryTracePartialNote is set when the Azure reconcile succeeded in some databases and was
           refused in others: the run's read of the survivors is real and stays SUCCESS, and this - the
           #2623 partial note naming the refused databases - is merged onto its row so the row cannot pass
           for a server that is quiet. It persists while LongQueryTraceApplied is latched true, because the
           refused databases are not retried until the next (re)connect resets the latch; the note is a
           standing claim about the sessions as they were last reconciled, which is also what it says.

           Both reset with the latch on every (re)connect, and both written only by the per-server body on
           the pool thread (INV-2), like the latch itself. */
        public string? LongQueryTraceFault { get; set; }

        public string? LongQueryTracePartialNote { get; set; }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        /* #2185: an install directory the service account cannot read is diagnosed HERE — first, ahead of
           reading darling.json, and a long way ahead of the managed-Postgres bootstrap. Order is the whole
           point. Every message the reporter saw was downstream of this one: an unreadable tree takes out
           darling.json ("Cannot load configuration") and the bundled PostgreSQL's initdb (an empty Output:
           and a bare loader status) before anything says WHERE the install is. Stated first, it is the line
           above the failure in the log an operator is reading bottom-up.

           Diagnose and continue, deliberately, rather than refuse to start — see DarlingInstallLocation for
           why (the installer asks rather than refuses on an upgrade for the same reason, and a service has
           nobody to ask). Silent unless this process really is running as a Windows service: a console
           test-drive from a Desktop folder runs as the profile owner, reads the tree fine, and is something
           the README suggests doing. */
        if (OperatingSystem.IsWindows())
        {
            DarlingInstallLocation.Report(
                AppContext.BaseDirectory,
                WindowsServiceHelpers.IsWindowsService(),
                _logger);
        }

        DarlingConfig config;
        string configPath;
        /* #2936: same triage as the store-migrate block below, same classifier, same budget. Load() is
           File.Exists then ReadAllText then deserialize, so a sharing violation on a darling.json that an
           installer, the Viewer's Settings save or a config-management tool is mid-write is transient and
           self-heals; a malformed file, an ACL problem, or a file that simply is not there never will.
           The old bare catch could not tell those apart and killed collection for the process lifetime for
           all of them. #2038 already reached this conclusion for the MCP and web hosts, whose supervisors
           retry a failed Load() on a 30 s backoff and whose comments point HERE for the critical case;
           this is that case learning the same lesson. StartupFailureTriage.IsRetryable keeps
           FileNotFoundException terminal even though it is an IOException, which on a first install is the
           likeliest way this ever fails.
           The terminal arm stays a bare catch (Exception), OperationCanceledException included, exactly as
           before — shutdown during Load() still reports the same way it always has. */
        var configRetryBudget = System.Diagnostics.Stopwatch.StartNew();
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                configPath = DarlingConfig.ResolveConfigPath();
                config = DarlingConfig.Load();
                _logger.LogInformation("Loaded configuration from {Path}: {ServerCount} server(s)", configPath, config.Servers.Count);
                break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException
                && attempt < StartupFailureTriage.Attempts
                && configRetryBudget.Elapsed < StartupFailureTriage.RetryBudget
                && StartupFailureTriage.IsRetryable(ex))
            {
                _logger.LogWarning(
                    ex,
                    "Cannot load configuration yet ({Message}) — attempt {Attempt} of {Total}, retrying in " +
                    "{Delay}s. A file another process is mid-write recovers on its own; a missing, malformed " +
                    "or unreadable one does not and is not retried.",
                    ex.Message, attempt, StartupFailureTriage.Attempts,
                    (int)StartupFailureTriage.RetryDelay.TotalSeconds);
                _collectorState.PublishRetrying(
                    CollectorRuntimeState.StartupStep.Configuration, attempt, StartupFailureTriage.Attempts);
                await Task.Delay(StartupFailureTriage.RetryDelay, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Cannot load configuration: {Message}", ex.Message);
                _collectorState.PublishStopped(CollectorRuntimeState.StartupStep.Configuration);
                return;
            }
        }

        if (OperatingSystem.IsWindows())
        {
            TryHardenConfigFile(configPath);
            TryHardenConfigBackups(configPath, _logger);
        }

        var problems = config.Validate();
        if (problems.Count > 0)
        {
            foreach (var problem in problems)
            {
                _logger.LogCritical("Configuration problem: {Problem}", problem);
            }

            /* #2953: every problem, joined, rather than the first — Validate is all-fatal and reports the whole
               set, so a ping body carrying one of several would send an operator to fix a config that still
               does not start. PublishConfigurationProblems takes the config, not this `problems` list — it
               calls Validate itself, so there is no string/list parameter here for a future caller to hand
               exception text instead (#4316 round 1 B1, round 2 L1-r2 a). `problems` above stays local, for
               the per-problem critical log lines just above. */
            _collectorState.PublishConfigurationProblems(config);
            return;
        }

        /* #2339: publish the declared peer stores as soon as a VALIDATED config is in hand, so the web
           dashboard's read dispatch (which reuses the MCP tool methods) discloses the same peers even when
           the MCP endpoint is disabled. The MCP host publishes the identical snapshot from its own load;
           whichever runs first wins and they cannot disagree, both reading darling.json.

           Publish re-validates and refuses rather than trusting the Validate() above: it cannot fire here
           (we already returned on any problem), but the check belongs to the publish, not to this call site,
           because the MCP host reaches Publish WITHOUT ever calling Validate. Logged if it ever does, rather
           than discarded, so an impossible state cannot become a silent one. */
        var peerPublish = DarlingPeerDirectory.Publish(config.Peers);
        foreach (var problem in peerPublish.RefusedProblems)
        {
            _logger.LogCritical("Peer disclosure refused (nothing published): {Problem}", problem);
        }

        /* #3712: the file-level analysis routing knob, published the same way so get_alert_settings can report
           the value this process applies rather than a constant (the MCP host publishes it too). */
        DarlingFileLevelAlertSettings.Publish(config.Analysis);

        /* Network-endpoint caller warnings (darling-network-endpoints, D-BYO / D7) — emitted AFTER
           Validate() passes and NEVER inside it (Validate is all-fatal; an optional-endpoint note must not
           abort startup). Covers BYO-mode network.* being ignored and the network.role=admin pivot risk. */
        foreach (var warning in GetNetworkStartupWarnings(config))
        {
            _logger.LogWarning("{Warning}", warning);
        }

        /* #4220: web.publicBaseUrl shaped like it carries a credential (a query string, fragment, or
           userinfo — e.g. the dashboard's own sign-in link pasted in by mistake). Ruled: sending the link is
           the operator's call, not refused here; one warning at startup is the whole mitigation. */
        var triageLinkWarning = TriageLink.DescribeCredentialShapedBaseWarning(config.Web.PublicBaseUrl);
        if (triageLinkWarning is not null)
        {
            _logger.LogWarning("{Warning}", triageLinkWarning);
        }

        /* Bundled-Postgres bootstrap (the shipped zero-admin default): in managed mode the
           service unpacks/initializes/starts its own Postgres BEFORE the store connection
           below, and the connection string is DERIVED (localhost + port + the generated
           DPAPI credential), never configured. Windows-only, like every DPAPI surface here.
           A bootstrap failure is the existing no-store behavior: LogCritical + clean exit. */
        DarlingManagedPostgres? managedPostgres = null;
        var storeConnectionString = config.Postgres.ConnectionString;

        /* #1706: what the store runtime reconcile did this start, carried past the bootstrap so it can be
           raised as a real self-alert once the alert engine exists. Null when nothing happened, which is
           every ordinary start. */
        DarlingSelfAlertEvaluator.StoreUpgradeReport? storeUpgradeReport = null;

        /* #3908: what this start did to the store's TimescaleDB extension, alerted beside the upgrade report. */
        DarlingSelfAlertEvaluator.StoreTimescaleReport? storeTimescaleReport = null;

        if (config.Postgres.Managed)
        {
            if (!OperatingSystem.IsWindows())
            {
                _logger.LogCritical(
                    "postgres.managed = true requires Windows (the bundled runtime and the DPAPI-protected credential); " +
                    "set postgres.managed = false and point postgres.connectionString at your own PostgreSQL instead.");
                _collectorState.PublishManagedStoreNeedsWindows();
                return;
            }

            managedPostgres = new DarlingManagedPostgres(config.Postgres, _logger);
            /* #2936: the sharpest of the three sites, because the judgment already existed and was being
               thrown away. OpenProbedMaintenanceConnectionAsync inside this bootstrap classifies transient connection
               faults and retries 6 times 2 s apart — and when that runs out it throws, and this catch
               discarded the fact that the failure had been RULED transient. Re-classifying here is what
               makes that verdict mean something.
               StartupFailureTriage is deliberately WIDER than the inner IsTransientConnectionFault, which
               rules that a PostgresException means the server replied and so is never transient. That is
               right at its own site and wrong one layer up: 57P01 and 57P03 are the server replying "going
               down" and "not up yet", which is how a store restart and a crash-recovering store present.
               So a bootstrap the inner layer gave up on can still be retried here, correctly.
               Safe to re-enter: the class documents it and the body shows it — EnsureRuntimeAsync unpacks
               only what is missing, initdb runs only without PG_VERSION, the conf append is
               marker-guarded, and an already-running postmaster is adopted rather than restarted, so "a
               second EnsureRunningAsync against an initialized, running cluster does no initdb, no
               restart, no credential rewrite".
               NOTE this refines a contract DarlingManagedPostgres' own comments still state as
               "throw => service-exit": for a classified-transient throw it is now retry-then-service-exit.
               Terminal throws behave exactly as before. */
            var bootstrapRetryBudget = System.Diagnostics.Stopwatch.StartNew();
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    storeConnectionString = await managedPostgres.EnsureRunningAsync(stoppingToken);
                    storeUpgradeReport = BuildStoreUpgradeReport(managedPostgres.LastUpgradeOutcome);
                    storeTimescaleReport = BuildStoreTimescaleReport(managedPostgres.LastTimescaleOutcome);
                    break;
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex) when (attempt < StartupFailureTriage.Attempts
                    && bootstrapRetryBudget.Elapsed < StartupFailureTriage.RetryBudget
                    && StartupFailureTriage.IsRetryable(ex))
                {
                    _logger.LogWarning(
                        ex,
                        "Managed Postgres bootstrap failed, retrying ({Message}) — attempt {Attempt} of " +
                        "{Total}, retrying in {Delay}s. A transiently locked file or a store still coming " +
                        "up recovers on its own; a broken package or a stale credential does not and is " +
                        "not retried.",
                        ex.Message, attempt, StartupFailureTriage.Attempts,
                        (int)StartupFailureTriage.RetryDelay.TotalSeconds);
                    _collectorState.PublishRetrying(
                        CollectorRuntimeState.StartupStep.ManagedStore, attempt, StartupFailureTriage.Attempts);
                    await Task.Delay(StartupFailureTriage.RetryDelay, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogCritical(ex, "Managed Postgres bootstrap failed: {Message}", ex.Message);
                    _collectorState.PublishStopped(CollectorRuntimeState.StartupStep.ManagedStore);
                    return;
                }
            }
        }

        /* #4215: this start's darling-managed.conf write result, carried past the
           bootstrap the same way storeUpgradeReport/storeTimescaleReport are, so LogStoreHostProfileAsync can
           fold a hand edit's changed keys into the stored verdict rows without re-reading the file. Null on a
           BYO store (managedPostgres itself is null) and on the adopted-listener path (the write never ran). */
        var managedDataDirectory = managedPostgres?.DataDirectory;
        var managedConfWriteResult = OperatingSystem.IsWindows() ? managedPostgres?.LastManagedConfWriteResult : null;

        /* #4215: whether THIS start fell back to darling-managed.conf.last-good, carried out of the
           bootstrap the same way managedConfWriteResult already is — see
           DarlingManagedPostgres.LastStartUsedLastGoodManagedConf. False (never true) on a BYO store, off
           Windows, and on the adopted-listener path, for the same reasons managedConfWriteResult is null
           there. */
        var managedUsedLastGoodConf = OperatingSystem.IsWindows() && (managedPostgres?.LastStartUsedLastGoodManagedConf ?? false);

        /* #4336: this start's darling-managed.conf verification outcome, carried out of the
           bootstrap the same way managedConfWriteResult and managedUsedLastGoodConf already are. Null on a
           BYO store, off Windows, and on the adopted-listener path, for the same reasons those are. */
        var managedConfVerification = OperatingSystem.IsWindows() ? managedPostgres?.LastManagedConfVerification : null;

        try
        {
            await RunCollectionLoopAsync(
                config, storeConnectionString, storeUpgradeReport, storeTimescaleReport,
                managedDataDirectory, managedConfWriteResult, managedUsedLastGoodConf, managedConfVerification, stoppingToken);
        }
        finally
        {
            /* Stop the bundled server ONLY if this process started it — never one the operator
               (or a surviving previous run) owns. Runs on every exit path, including a failed
               migration, AFTER the loop's data source is disposed. The IsWindows re-check is a
               CA1416 guard only — a non-null managedPostgres already implies Windows. */
            if (managedPostgres is not null && OperatingSystem.IsWindows())
            {
                await managedPostgres.StopIfStartedByThisProcessAsync();
            }
        }
    }

    /// <summary>
    /// #1647: locks <c>darling.json</c> down to the posture the DPAPI credential files already get, then
    /// VERIFIES it. The config file is not ordinary config — it holds every monitored server's
    /// <c>encryptedPassword</c>, the MCP bearer token, the web dashboard access token, and in BYO mode the
    /// store connection string. Those blobs are DPAPI <b>LocalMachine</b> scope with an entropy constant that
    /// ships in an open-source repo, so anything that can READ the file can unprotect all of it — the ACL is
    /// the access boundary, exactly as <see cref="DarlingFileSecurity"/> says of the credential files. It never
    /// got one: every harden call site targeted the credential directory, while the config sat beside the
    /// binary, and the documented install (extract the zip to <c>C:\Program Files\PerformanceMonitorDarling</c>) inherits
    /// <c>BUILTIN\Users: Read &amp; Execute</c> from the root DACL. Any local unprivileged user could read it,
    /// decrypt every SQL password, and lift the tokens that unlock the MCP write surface.
    ///
    /// <para><c>allowInteractiveRead: true</c> — the same argument the admin/viewer credentials pass, and
    /// required here: the Viewer (<c>ViewerSettings.ResolveConfigPath</c>) and the CLI verbs run as the
    /// interactive operator and must still read this file.</para>
    ///
    /// <para>Best-effort like every other ACL call (a failure is logged, never fatal — a monitoring service must
    /// not refuse to monitor over a permissions problem), but a file that is STILL readable by
    /// Users/Authenticated Users after the attempt is a <see cref="LogLevel.Critical"/>: at that point the
    /// secrets in it are recoverable by anyone with a local logon.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    private void TryHardenConfigFile(string path)
    {
        try
        {
            DarlingFileSecurity.HardenFile(path, allowInteractiveRead: true);
        }
        catch (Exception ex)
        {
            /* The remediation is spelled out as runnable commands, not described. This exact failure sat in
               a field box's log once per service start for months and nobody acted on it, because knowing
               the ACL is wrong is not the same as knowing what to type — and the service genuinely cannot
               fix it itself: re-ACLing needs WRITE_DAC, which it does not have, and taking ownership needs
               a privilege a virtual service account is not granted. An elevated human is the only actor
               who can resolve this, so give them the three lines. */
            /* Built as ONE argument rather than repeating {Path}: a structured-logging template binds
               placeholders POSITIONALLY, so a repeated name silently consumes the next argument and the
               tail of the message renders empty — in the one log line whose entire job is to be actionable.
               The /grant names the identity the service RUNS AS, not the default virtual account — on an
               install re-homed to a domain account for integrated auth (#1823), granting the virtual
               account would be a fix that cannot work. */
            var remediation =
                $"icacls \"{path}\" /inheritance:d   then   icacls \"{path}\" /remove:g \"BUILTIN\\Users\"   " +
                $"then   icacls \"{path}\" /grant \"{DarlingFileSecurity.ServiceAccountDisplayName}:(F)\"";

            _logger.LogError(
                "Could not restrict the ACL on {Path}{Detail} ({Message}). If the owner is not this service, the " +
                "re-ACL can never succeed — it needs ownership or FullControl — so restarting will not clear this, " +
                "and the service cannot fix it alone: taking ownership needs a privilege a virtual service account " +
                "does not have. From an ELEVATED prompt: {Remediation} — after which this service re-asserts the " +
                "full ACL by itself on the next start.",
                path, DarlingFileSecurity.DescribeOwnerAndExposure(path), ex.Message, remediation);
        }

        if (DarlingFileSecurity.IsReadableByOrdinaryUsers(path))
        {
            _logger.LogCritical(
                "{Path} is READABLE by Users/Authenticated Users/Everyone. It holds every monitored server's " +
                "encrypted password plus the MCP and web access tokens, all protected with machine-scoped DPAPI — " +
                "so any local user who can open this file can recover ALL of it. Remove the inherited read access " +
                "(or move the install out of a world-readable folder such as one created directly under C:\\).",
                path);
        }
    }

    /// <summary>
    /// #1816: the same lockdown for every EXISTING <c>darling.json.bak-*</c> beside the config. The
    /// backup-CREATION path hardens new backups as it writes them (#1786), but backups made before that
    /// fix kept whatever the folder handed them — on the field box that surfaced this, inherited
    /// <c>BUILTIN\Users</c> read, which against machine-scoped DPAPI blobs means any local account could
    /// recover every stored credential and token. The installer's security check flags them but only
    /// prints the fix; this sweep applies it, so no install carries the exposure past its next service
    /// start. <c>allowInteractiveRead: false</c>, matching the creation path — backups are rollback
    /// artifacts, nothing interactive reads them. Static with the logger passed in so the test can drive
    /// it against a scratch directory.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static void TryHardenConfigBackups(string configPath, ILogger logger)
    {
        string[] backups;
        try
        {
            var directory = Path.GetDirectoryName(Path.GetFullPath(configPath));
            if (string.IsNullOrEmpty(directory))
            {
                return;
            }

            backups = Directory.GetFiles(directory, Path.GetFileName(configPath) + ".bak-*");
        }
        catch (Exception ex)
        {
            /* Enumeration failing is not worth failing the start over — the live file's own hardening
               already ran, and the installer's check remains the independent witness. */
            logger.LogWarning("Could not enumerate config backups beside {Path} for ACL hardening: {Message}", configPath, ex.Message);
            return;
        }

        foreach (var backup in backups)
        {
            /* #2093 (ghauan): idempotence gate. The rewrite below needs OWNERSHIP, which an operator's
               manual icacls fix does not transfer — so a backup that had already been hardened by hand
               kept erroring on every start about an exposure that was already closed. The sweep exists
               to close the ordinary-users-can-read gap; when that gap is closed, there is nothing left
               to do and no error to report. The CRITICAL check below remains the independent witness
               for the still-exposed case. */
            if (!DarlingFileSecurity.IsReadableByOrdinaryUsers(backup))
            {
                continue;
            }

            try
            {
                DarlingFileSecurity.HardenFile(backup, allowInteractiveRead: false);
            }
            catch (Exception ex)
            {
                /* Same contract as the live file: best-effort, but the remediation is spelled out as
                   runnable commands so an elevated human can finish the job the service cannot — naming
                   the identity the service runs as, which is not the virtual account on a re-homed install. */
                var remediation =
                    $"icacls \"{backup}\" /inheritance:d   then   icacls \"{backup}\" /remove:g \"BUILTIN\\Users\"   " +
                    $"then   icacls \"{backup}\" /grant \"{DarlingFileSecurity.ServiceAccountDisplayName}:(F)\"";

                logger.LogError(
                    "Could not restrict the ACL on config backup {Path}{Detail} ({Message}). It carries the same " +
                    "DPAPI-protected credentials as the live config. From an ELEVATED prompt: {Remediation}",
                    backup, DarlingFileSecurity.DescribeOwnerAndExposure(backup), ex.Message, remediation);
            }

            if (DarlingFileSecurity.IsReadableByOrdinaryUsers(backup))
            {
                logger.LogCritical(
                    "Config backup {Path} is READABLE by Users/Authenticated Users/Everyone. It holds the same " +
                    "machine-scoped DPAPI credential blobs as the live config — any local user who can open it " +
                    "can recover ALL of them. Remove the inherited read access.",
                    backup);
            }
        }
    }

    /// <summary>
    /// Maps the Windows-only bootstrap's upgrade outcome to the platform-neutral alert payload (#1706).
    /// Null for the ordinary case where the runtime did not move, and null for an extension-only update,
    /// which is a routine maintenance step the log already records rather than something to page about.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static DarlingSelfAlertEvaluator.StoreUpgradeReport? BuildStoreUpgradeReport(
        DarlingStoreUpgrade.StoreUpgradeOutcome outcome)
        => outcome.Status switch
        {
            /* outcome.Message is carried through on SUCCESS too, not just failure: a post-commit bookkeeping
               failure returns Succeeded with the warning there, and dropping it here would leave the alert
               reassuring while the log alarms. The alert is the surface an operator actually receives. */
            DarlingStoreUpgrade.StoreUpgradeStatus.Succeeded => new DarlingSelfAlertEvaluator.StoreUpgradeReport(
                true, outcome.FromMajor, outcome.ToMajor, outcome.FromTimescale, outcome.ToTimescale,
                null, outcome.Message, outcome.UsedLinkMode),
            /* #3927: and what a failure actually put back is carried too. Leaving these at their defaults here
               would hand the alert a clean revert whatever happened, the same dropped-at-the-seam shape the
               success arm's warning once had. */
            DarlingStoreUpgrade.StoreUpgradeStatus.Failed => new DarlingSelfAlertEvaluator.StoreUpgradeReport(
                false, outcome.FromMajor, outcome.ToMajor, outcome.FromTimescale, outcome.ToTimescale,
                outcome.FailedStep, outcome.Message, false,
                RuntimeReverted: outcome.RuntimeReverted,
                ControlFileRestored: outcome.PreUpgradeData == DarlingStoreUpgrade.PreUpgradeDataDirectory.ControlFileRestored,
                DataDirectoryNotRestored: outcome.PreUpgradeData == DarlingStoreUpgrade.PreUpgradeDataDirectory.NotRestored),
            _ => null,
        };

    /// <summary>
    /// Maps the bootstrap's TimescaleDB outcome to the alert payload (#3908). Null unless the extension could not
    /// be moved or is behind the runtime after start: an update that landed is routine maintenance the log
    /// already records, the same call <see cref="BuildStoreUpgradeReport"/> makes.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static DarlingSelfAlertEvaluator.StoreTimescaleReport? BuildStoreTimescaleReport(
        DarlingStoreUpgrade.TimescaleUpdateOutcome outcome)
        => outcome.Status switch
        {
            DarlingStoreUpgrade.TimescaleUpdateStatus.Failed => new DarlingSelfAlertEvaluator.StoreTimescaleReport(
                true, outcome.From, outcome.To, outcome.Message),
            DarlingStoreUpgrade.TimescaleUpdateStatus.Behind => new DarlingSelfAlertEvaluator.StoreTimescaleReport(
                false, outcome.From, outcome.To, outcome.Message),
            _ => null,
        };

    /// <summary>
    /// Maps the web host's published TLS-certificate snapshot to the report the evaluator consumes (#3514):
    /// a null snapshot — nothing served, or <c>Clear()</c>ed when the dashboard stopped — becomes
    /// <c>Configured=false</c> (the evaluator's resolve arm), and a live snapshot carries its validity window,
    /// identity and the host's not-yet-valid verdict (#3517) through unchanged — the verdict is the host's to
    /// make and this mapping must not re-derive or drop it. Pure + static so the null-to-unconfigured seam
    /// pins in a unit test rather than only through the sweep loop — the <see cref="BuildStoreUpgradeReport"/>
    /// precedent, and the seam the #3514 review flagged as previously tested only from the sides.
    /// </summary>
    internal static DarlingSelfAlertEvaluator.WebTlsCertReport BuildWebTlsCertReport(
        WebTlsCertificateState.Snapshot? snapshot)
        => new(
            Configured: snapshot is not null,
            NotBeforeUtc: snapshot?.NotBeforeUtc ?? default,
            NotAfterUtc: snapshot?.NotAfterUtc ?? default,
            Subject: snapshot?.Subject ?? string.Empty,
            Thumbprint: snapshot?.Thumbprint ?? string.Empty,
            RefusedNotYetValid: snapshot?.RefusedNotYetValid ?? false);

    /// <summary>
    /// The once-per-start store host/settings profile log (#4214 ruling 9): logs the host facts and a
    /// verdict for every sizing-relevant setting, then one <c>LogWarning</c> per
    /// <see cref="HostSettingVerdict.StaleAfterHardwareChange"/> setting. Read-only and best-effort — never
    /// throws except on the caller's own <paramref name="stoppingToken"/> (real shutdown) — because ruling 9
    /// requires this to "never fail or delay startup": a bad conf file, a permission problem, or a store
    /// that is merely slow to answer must degrade to "no log line this start," not to a delayed or failed
    /// one. <see cref="ServiceCommandDeadlines.StartupHostProfileSeconds"/> is what bounds "slow" the same
    /// way <see cref="DarlingStoreHostProfile.GatherStoreFactsAsync"/>'s store-size read is deliberately
    /// NEVER reached from here — see <see cref="DarlingStoreHostProfile.GatherStartupProfileAsync"/>.
    /// </summary>
    private async Task LogStoreHostProfileAsync(
        DarlingConfig config, NpgsqlDataSource postgres, string? managedDataDirectory,
        ManagedConfWriteResult? managedConfWriteResult, CancellationToken stoppingToken)
    {
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        budget.CancelAfter(TimeSpan.FromSeconds(ServiceCommandDeadlines.StartupHostProfileSeconds));

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(budget.Token);
            var profile = await DarlingStoreHostProfile.GatherStartupProfileAsync(config.Postgres, connection, budget.Token);

            _logger.LogInformation("Store host profile:\n{Profile}", DarlingStoreHostProfile.FormatStartupProfileText(profile));

            foreach (var setting in profile.Settings)
            {
                if (setting.Verdict == HostSettingVerdict.StaleAfterHardwareChange)
                {
                    _logger.LogWarning(
                        "{Setting} is stale-after-hardware-change: current {Current}, this host would now " +
                        "derive {Derived} ({Source}). Run --check-settings for the full picture.",
                        setting.Name, setting.CurrentValueDisplay, setting.DerivedValueDisplay, setting.SourceDescription);
                }
            }

            /* #4215: on the OWNER connection, right after start, compute and store every owned
               key's verdict (collect.managed_conf_verdicts, V146) so --check-settings, the MCP self-store
               reader and the stale-setting alert can all read it back — the mcp/viewer
               roles have neither file access nor pg_file_settings visibility to compute it themselves. Same
               budget, same try/catch as the read above: never fails or delays startup. BYO stores
               (managedDataDirectory null) and the adopted-listener path skip it — nothing this service wrote
               changed, and there is no NEW conf-write result to fold in. */
            if (config.Postgres.Managed && managedDataDirectory is not null)
            {
                await DarlingStoreHostProfile.ComputeAndStoreManagedConfVerdictsAsync(
                    connection, managedDataDirectory, profile.Memory.EffectiveBytes, profile.DataVolume.FreeBytes,
                    managedConfWriteResult, _logger, budget.Token);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            /* Shutdown — quiet and expected. */
        }
        catch (Exception ex)
        {
            /* Never fails or delays startup (ruling 9's own words) — RunCollectionLoopAsync proceeds to role
               provisioning and the collection loop whether this logged a profile or not. The budget CTS
               surfaces as OperationCanceledException here too (with the SERVICE token untripped, the arm
               above already ruled that out), so it gets the same "did not finish" wording the CLI verb's
               own store-read backstop uses. */
            var reason = ex is OperationCanceledException && budget.IsCancellationRequested
                ? $"did not finish within {ServiceCommandDeadlines.StartupHostProfileSeconds}s"
                : ex.Message;
            _logger.LogWarning(
                "Store host profile check failed at startup ({Reason}) — collection continues; run " +
                "--check-settings to retry.", reason);
        }
    }

    /// <summary>
    /// Everything after the (optional) managed-Postgres bootstrap: store connection, migration,
    /// Timescale adoption, delta seeding, and the collection/alert/analysis loop. Split from
    /// <see cref="ExecuteAsync"/> so the bootstrap's finally can stop the bundled server after
    /// this method's data source is disposed.
    /// </summary>
    private async Task RunCollectionLoopAsync(
        DarlingConfig config,
        string storeConnectionString,
        DarlingSelfAlertEvaluator.StoreUpgradeReport? storeUpgradeReport,
        DarlingSelfAlertEvaluator.StoreTimescaleReport? storeTimescaleReport,
        string? managedDataDirectory,
        ManagedConfWriteResult? managedConfWriteResult,
        bool managedUsedLastGoodConf,
        ManagedConfMigrationOutcome? managedConfVerification,
        CancellationToken stoppingToken)
    {
        /* Carry the collect/config search path on the store connection string BEFORE the data
           source (and its pool) is created, so every pooled physical connection resolves the
           shared SQL's bare table names to the V8 schemas from its very first use — deterministic
           and independent of the pool's connection-open timing relative to PgMigrations'
           best-effort ALTER DATABASE ... SET search_path. Without this a FRESH bring-your-own
           store silently collects nothing until the service is restarted; see
           EnsureStoreSearchPath for the pool-timing root cause. Managed mode already sets it, so
           this is a no-op there. The pin has to sit OUTSIDE EnsureStoreSearchPath (wrapping its
           result rather than the two being one call), so the census below only has to look for the
           pin on the Create line itself — no reassignment in between for a future edit to slip
           an unpinned read behind. */
        await using var postgres = NpgsqlDataSource.Create(
            DarlingStoreConnection.PinSessionTimeZoneUtc(EnsureStoreSearchPath(storeConnectionString)));
        _postgres = postgres;
        /* #2936: a failure here is triaged rather than uniformly terminal. A store that is unreachable for
           a moment — restarting, failing over, still coming up alongside this service — and a sibling
           instance holding the migration advisory lock both succeed seconds later; a rung that cannot
           apply against this store never will. StartupFailureTriage decides which arrived and carries
           the reasoning for where that boundary sits, including why anything it cannot place positively
           stays terminal. This is the LAST of the three startup steps that can kill collection — the config
           load and the managed bootstrap above are the other two, triaged the same way through the same
           predicate — so the degrade-vs-kill question the next block answers out loud is answered here
           too: a transient store failure now degrades to a delayed first cycle, and everything else keeps
           the critical line and the stand-down byte for byte. Both caps are load-bearing: an attempt is not
           quick just because a refused connect is — one that blocks behind a peer's advisory lock can
           spend MigrationLockWaitTimeoutSeconds — so the wall-clock budget is what stops 25 attempts from
           becoming ten hours, and the attempt count is what the warning line reports.
           A FRESH connection per attempt, not a reuse of the old one — its connector is dead after a
           transport failure, the same reason DarlingManagedPostgres.OpenProbedMaintenanceConnectionAsync retries
           the connect and its first query as one unit rather than just the open. Re-entering MigrateAsync is safe because the applier commits
           each rung's DDL and its darling_schema_version stamp in ONE transaction: a rung that failed
           part-way left nothing applied and nothing stamped, and rungs at or below the stamp are skipped,
           so a retry resumes at the rung that failed instead of redoing the ladder. That rests on the
           rungs being transactional, which was measured rather than assumed, because the ladder's
           expensive ones are not plain DDL: V23's own statement set — create_hypertable with
           migrate_data => true, ALTER TABLE SET (timescaledb.compress ...), add_compression_policy — run
           against 200,000 rows on PostgreSQL 17.11 / TimescaleDB 2.29.2 built 139 chunks and a policy job
           inside the transaction and left, after ROLLBACK, a plain table with every row, no chunks, no
           job and no reloptions. No rung uses CREATE INDEX CONCURRENTLY or any other statement that
           cannot be transacted. */
        var storeRetryBudget = System.Diagnostics.Stopwatch.StartNew();
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var migrateConnection = await postgres.OpenConnectionAsync(stoppingToken);
                /* MigrateAsync (logger overload) also best-effort sets the database-default search_path to
                   collect/config for every future connection (V8 security split); a least-privilege BYO
                   login that cannot ALTER DATABASE is warned, not failed — the managed connection strings
                   carry Search Path regardless. */
                var applied = await PgMigrations.MigrateAsync(migrateConnection, _logger, stoppingToken);
                _logger.LogInformation("Postgres store ready (schema v{Version}, {Applied} migration(s) applied)",
                    StorageVersion.SchemaVersion, applied);
                break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException
                && attempt < StartupFailureTriage.Attempts
                && storeRetryBudget.Elapsed < StartupFailureTriage.RetryBudget
                && StartupFailureTriage.IsRetryable(ex))
            {
                /* Every placeholder appears ONCE. A repeated name is not a duplicate here, it is an extra
                   positional slot: LogValuesFormatter numbers placeholders by occurrence, so a template
                   naming four things in five slots throws FormatException out of the logger, and out of
                   this BackgroundService, and StopHost then takes the process down - a retry path that
                   kills the service harder than the failure it was retrying. */
                _logger.LogWarning(
                    ex,
                    "Cannot reach or migrate the Postgres store yet ({Message}) — attempt {Attempt} of " +
                    "{Total}, retrying in {Delay}s. A store that is restarting, failing over or still " +
                    "coming up recovers on its own; after the last attempt this becomes a critical line " +
                    "and collection does not start.",
                    ex.Message, attempt, StartupFailureTriage.Attempts,
                    (int)StartupFailureTriage.RetryDelay.TotalSeconds);
                _collectorState.PublishRetrying(
                    CollectorRuntimeState.StartupStep.Store, attempt, StartupFailureTriage.Attempts);
                await Task.Delay(StartupFailureTriage.RetryDelay, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogCritical(ex, "Cannot reach or migrate the Postgres store: {Message}", ex.Message);
                /* #2953: AFTER the critical line, deliberately. The log line is the diagnosis of record and
                   predates this seam; publishing first would put a new call between the failure and the one
                   message an operator greps for. */
                _collectorState.PublishStopped(CollectorRuntimeState.StartupStep.Store);
                return;
            }
        }

        /* #4348 S1b: the one-time scrub of secrets an older collector build stored in plain text (a
           standby's replication password inside primary_conninfo, most notably). Launched here, right
           after migrations confirm collect.pg_server_config and collect.collector_state exist, and NOT
           gated on TimescaleDB — the scrub works the same on plain PostgreSQL, it just has fewer chunks to
           reason about. Drained with the other background startup work below. */
        var settingScrub = RunPgSettingScrubAsync(postgres, stoppingToken);

        /* #4348: the one-time scrub of collected statement text (collect.pg_statement_text,
           collect.pg_blocking_edges) an older build stored before the shared sensitive-statement filter
           existed. Same launch discipline as the setting scrub immediately above — after migrations
           confirm both target tables exist, not gated on TimescaleDB, drained with the rest of startup
           below. */
        var statementTextScrub = RunPgStatementTextScrubAsync(postgres, stoppingToken);

        /* #4346: the one-time scrub of the legacy plan_force_actions.detail state_unavailable line
           #4326/#4363/#4376 stop new rows from ever carrying. Same launch shape as settingScrub above —
           its own connection, its own catch, drained with the other background startup work below. */
        var planForceDetailScrub = RunPlanForceActionDetailScrubAsync(postgres, stoppingToken);

        /* #4214 ruling 9: the once-per-start store host/settings profile log — host facts, pg_settings and
           the managed conf files only, never the store-size/chunk-total reads --check-settings and the MCP
           read own. Its own short deadline and its own catch: a bad conf file, a permission problem or a
           slow store here must never fail or delay the collection start immediately below it. */
        await LogStoreHostProfileAsync(config, postgres, managedDataDirectory, managedConfWriteResult, stoppingToken);

        /* Least-privilege role provisioning (V8 security hardening), managed mode only: create /
           refresh the admin + viewer login roles and their per-role DPAPI credentials, and grant the
           collect/config privileges — idempotent and self-healing, the conf-append discipline applied
           to roles. Windows-only (DPAPI credential files); a failure degrades (the Viewer cannot
           connect as admin/viewer until a later start succeeds) but never kills collection, which
           connects as the owner. The compose store is the branch below; any other BYO store provisions roles
           out-of-band via tools/provision-roles.sql. */
        if (config.Postgres.Managed && OperatingSystem.IsWindows())
        {
            try
            {
                var dataDirectory = DarlingManagedPostgres.ResolveDataDirectory(config.Postgres);
                /* #2918: record what provisioning actually WROTE onto the roles, not what the store holds
                   afterwards. This runs BEFORE SeedIfEmptyAsync, so on a brand-new store there is no
                   config_service row to read and the roles get the 15 s default while the seed then inserts
                   darling.json's value — seeding the reload baseline from the post-seed view would claim a
                   value the roles never received, and the gate only fires on a difference, so that first-run
                   mismatch would never be corrected. Left at -1 if provisioning throws, so the first reload
                   re-asserts rather than trusting a write that did not land. */
                _appliedComposeStatementTimeoutSeconds =
                    await DarlingManagedRoles.EnsureProvisionedAsync(postgres, dataDirectory, _logger, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(
                    "Least-privilege role provisioning failed — the Viewer's admin/viewer roles may be stale " +
                    "until the next successful start: {Message}", ex.Message);
            }
        }
        else if (!config.Postgres.Managed && Hosting.DarlingHostBinding.IsRunningInContainer)
        {
            /* #3914: the Linux compose distribution's store is the service's own, so it provisions the same roles
               there and the web and MCP hosts connect as viewer and mcp. It decides from the store whether this
               IS that store, publishes the verdict the hosts wait on, and never throws: a refusal or failure
               leaves each host on its role's credential from an earlier start, or on the owner when there is none,
               with the reason. Same reload baseline as managed, and only from a start that provisioned. */
            var verdict = await DarlingStoreLogins.ProvisionComposeStoreAsync(
                postgres, config.Postgres.ConnectionString, _logger, stoppingToken);
            if (verdict.Provisioned)
            {
                _composeStoreRolesProvisioned = true;
                _appliedComposeStatementTimeoutSeconds = verdict.AppliedComposeStatementTimeoutSeconds;
            }
        }

        /* Optional TimescaleDB adoption — runtime setup, deliberately NOT a versioned migration
           (the store must work with or without the extension; migrations stay engine-plain).
           Detected once at startup; when present, the collector tables become hypertables with
           a compression policy (Darling's archival tier) and the daily retention purge below
           switches to drop_chunks. All idempotent, so every restart re-converges. In its own
           try/catch OUTSIDE the critical migrate block: an optional feature failing must degrade
           to plain-PostgreSQL mode, never kill the service.
           This block runs AFTER CREATE EXTENSION (TryEnableAsync), which is why the AUTHORITATIVE
           collection_log conversion (EnsureCollectionLogHypertableAsync) lives here, not in the V23
           migration: MigrateAsync above runs BEFORE the extension exists, so a fresh store's V23
           guard skips the conversion and this heals it (collection_log is outside the collector
           catalog, so the loop calls above never touch it). */

        /* Handle for the background baseline backfill launched inside the block below (#1757); stays null in
           plain-PostgreSQL mode or if the TimescaleDB block faults before reaching it. Drained at shutdown. */
        Task? baselineBackfill = null;

        /* And the materialization-hole repair's (#3653, Q10), launched the same way right after the ensure
           sweep; same lifetime, same drain. */
        Task? holeRepair = null;

        /* #3817: the convergence pass's clock and tally, both declared HERE rather than in the TimescaleDB
           block, because the pass spans four segments across three connections and two of those segments run
           on a plain-PostgreSQL store where the block below never opens its gate. The clock covers every
           segment including the connection opens, which is where a slow store actually spends this pass; the
           tally is what makes the four segments one line instead of four. */
        var startupConvergenceClock = Stopwatch.StartNew();
        var startupConvergence = new StoreObjectConvergenceTally();

        try
        {
            await using var timescaleConnection = await postgres.OpenConnectionAsync(stoppingToken);
            _timescaleAvailable = await TimescaleSupport.TryEnableAsync(timescaleConnection, _logger, stoppingToken);
            if (_timescaleAvailable)
            {
                /* #3817: the store-object ensures that the start path and the hourly store-maintenance tick
                   BOTH run now come from ONE list (s_storeObjectConvergence), walked here segment by segment
                   so the three non-convergence steps interleaved below keep the positions their own issues
                   argued for. Every step, the four load-bearing ordering constraints among them, each step's
                   idempotence verdict and the five things deliberately left start-path-only are documented on
                   that list. What stood here was eleven await lines; this is the same eleven calls in the same
                   order, READ off the list rather than restated, because the defect #3817 closes is precisely
                   a second caller whose order drifts from this one's — and the only way two callers cannot
                   drift is for there to be one order.

                   The tally accumulates across all four segments and all three connections, so a start writes
                   ONE "Store object convergence:" line (after the tuning block below) rather than one per
                   segment, and the hourly pass writes the same line in the same shape. The per-object
                   INFORMATION lines each ensure writes are unchanged and still land here on the start path,
                   which is where an operator reads them. */
                await RunStoreObjectConvergenceSegmentAsync(
                    timescaleConnection, StoreObjectConvergenceStage.Timescale, startupConvergence, stoppingToken);

                /* #3653 (Q12): one line per superseded hourly rollup — where the interval-honest successor's
                   materialized floor stands against the legacy's and against the hourly tier's horizon — so the
                   hand-over from legacy to successor is visible in the log while it happens. An instrument,
                   not a mechanism (nothing here changes a policy or drops a relation; SupersededHourlyRollups
                   states why the legacy trio stays). AFTER the ensure so both relations exist on the start that
                   creates the successors; two min(bucket) reads per pair, failure-isolated, never fatal. */
                await TimescaleSupport.LogSupersededHourlyRollupCoverageAsync(timescaleConnection, _logger, DateTime.UtcNow, stoppingToken);

                /* #3653 (Q10): the materialization-hole repair — for every continuous aggregate, the bucket ranges
                   inside its materialized span where the source holds rows and the aggregate holds none (the
                   pre-outage tail a refresh policy can never reach back to once its window has moved past it),
                   each closed with ONE forced refresh over exactly its bounds, oldest first, capped per aggregate
                   at one policy window's worth of buckets. AFTER the ensure so every aggregate exists, and BEFORE
                   the compression and retention ensures below, on its own connection and NOT awaited — the scan
                   is a few hundred index probes per aggregate and starts at once, but a capped repair on the
                   heaviest aggregate is a policy run's worth of work, and holding a restarted service dark for
                   it is the trade #1757 already declined for the baseline backfill. Ordering against the
                   retention re-arm is not load-bearing (TimescaleSupport.MaterializationHoles states why in
                   full): under #4299's design the raw purge no longer runs on its own schedule at all, so this
                   scan and EnsureRetentionPoliciesAsync can run in either order on the same start without a
                   race — the only trigger for an actual purge is the service's OWN hourly Periodic pass, which
                   cannot fire before the next hourly tick. The first start after the upgrade, which still runs
                   whichever raw job the old scheduled-based code had already armed once, and a DBA's own
                   alter_job/run_job against a raw job, are the two named exceptions to that gate; both are
                   unaffected by this ordering either way. Drained with the command loop at shutdown. */
                holeRepair = RunMaterializationHoleRepairAsync(postgres, stoppingToken);

                /* #3817 segment two: the ensures that must run AFTER the hole repair is LAUNCHED — #3597's
                   dedup-index sweep and #3581's aggregate compression (which carries #3620's chunk-width
                   ensure inside it). Both need the aggregates to exist, and the repair's comment above places
                   itself ahead of both deliberately; splitting the walk here is what preserves that without
                   giving the hole repair a position in a list of idempotent ensures it is not one of. Same
                   list, same tally, next slice. */
                await RunStoreObjectConvergenceSegmentAsync(
                    timescaleConnection, StoreObjectConvergenceStage.TimescaleAfterRepairLaunch, startupConvergence, stoppingToken);

                /* AFTER the CAGGs exist: the tiered retention (raw 4d, hourly HISTORY CAGGs 90d per #1937, daily
                   history kept indefinitely; the interval-dedup and baseline tiers carry their own, #1958). The
                   START-PATH pass: it creates what is missing, converges horizons and judges every policy's
                   coverage gate, and writes one WARNING per held policy plus the "Retention evaluation at
                   startup:" tally. KEPT as-is under #3812, which ADDED the hourly re-evaluation on the
                   compression tick (ReevaluateRetentionPoliciesAsync) rather than moving this; a fresh store
                   still gets its policies before the first collection lands, and a restart is still when an
                   operator is reading the log. */
                await TimescaleSupport.EnsureRetentionPoliciesAsync(timescaleConnection, _logger, stoppingToken);

                /* #1757: the baseline aggregates ship WITH NO DATA and their refresh policy only ever covers
                   the trailing 1-day refresh window, so without this they would answer a 30-day question with
                   a day of supply. DELIBERATELY LAUNCHED, NOT AWAITED: it is a bulk materialization whose
                   cost scales with however much history the store already had, and every step below — the
                   composer tuning, the delta re-seed, the collection loop itself — is sequenced after it.
                   Awaiting it here would take a restarted service dark for as long as the backfill runs,
                   which is exactly when an operator is most likely to be restarting it. Coverage-gated, so it
                   is a no-op on every start after the first and resumes where it left off if cut short.
                   Drained with the command loop at shutdown. */
                baselineBackfill = RunBaselineBackfillAsync(postgres, stoppingToken);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A partially-converted store is fine: DELETE-based retention works on hypertables
               too, so falling back to plain-PG mode is always safe. */
            _timescaleAvailable = false;
            _logger.LogWarning("TimescaleDB setup failed — continuing in plain-PostgreSQL mode: {Message}", ex.Message);
        }

        /* #1757: the provider reads the seven baseline relations BY NAME — a missing one throws,
           ComputeBaselinesAsync swallows it, and that family silently returns an empty baseline. So every
           relation is guaranteed to EXIST here, with a plain view over the same select filling any gap.

           DELIBERATELY UNGATED on _timescaleAvailable. Three ways a gap appears and only one of them is "no
           TimescaleDB": the extension is absent, the TimescaleDB block threw partway, or the block ran fine
           and EnsureContinuousAggregatesAsync's per-aggregate failure isolation left one aggregate unbuilt.
           That last one is the easiest to miss and would take exactly one family down on an otherwise healthy
           store. The call is per-view and probes for an existing relation first, so it never touches a real
           aggregate. Its own connection — the block's is already disposed by here.

           The retirement drop (#2007) rides the same ungated block for the same reason: the retired CPU/IO
           baseline relations exist as CAGGs on TimescaleDB stores and as plain fallback views on
           plain-PostgreSQL stores, and both shapes must go. BEFORE the ensure call, though order is not
           load-bearing — the retired names left BaselineAggregates, so the ensure sweep can never recreate
           them. */
        try
        {
            await using var fallbackConnection = await postgres.OpenConnectionAsync(stoppingToken);
            /* #3817: segment three of the one convergence list — the two baseline steps, in the same order,
               off the same list, so the hourly pass runs them too. The UNGATED property is a property of this
               CALL SITE (after the TimescaleDB block's catch, on every path), not of the list: the steps are
               tagged Ungated and the hourly pass runs them on its own connection for the same reason. */
            await RunStoreObjectConvergenceSegmentAsync(
                fallbackConnection, StoreObjectConvergenceStage.Ungated, startupConvergence, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "Baseline relations could not be checked or backed by plain views — some anomaly baselines may silently return nothing: {Message}",
                ex.Message);
        }

        /* Composer + analyze_*_plan performance tuning (covering indexes + per-table autovacuum-insert override) —
           idempotent RUNTIME setup, NOT a versioned migration: results-invariant perf, so it must not bump
           StorageVersion and gate the Viewer's schema check on indexes it does not need (the role-GUC provisioning
           reasoning). AFTER the TimescaleDB block so the collector tables are already hypertables when indexed
           (CREATE INDEX / ALTER TABLE SET propagate to all chunks), BEFORE collectors so a first build never
           contends with live inserts. Its own try/catch: a failure degrades to un-tuned (slower) queries, never
           fatal (the same optional-feature discipline as the TimescaleDB block above). */
        try
        {
            await using var tuningConnection = await postgres.OpenConnectionAsync(stoppingToken);
            /* #3817: segment four — the tuning pass, off the same list. The whole-block wrap the issue names
               (one failure costing every index in the pass) is unchanged HERE and unchanged on the hourly
               pass, because it is inside PgTableTuning.ApplyAsync's own per-statement isolation rather than
               at this call site; what changes is that a pass which failed is retried within the hour instead
               of at the next restart. */
            await RunStoreObjectConvergenceSegmentAsync(
                tuningConnection, StoreObjectConvergenceStage.Tuning, startupConvergence, stoppingToken);
            // The retained sql_handle->module map (#1568 object_name for OLD query_stats windows the CAGG serves,
            // after procedure_stats raw drops at 4d): create it, then seed it from recent procedure_stats.
            /* NOT a convergence-list step (#3817): the refresh is a DATA upsert that already has a periodic
               home on the daily purge tick, and the table ensure is inseparable from it here because the
               refresh is gated on the bool it returns. */
            if (await DarlingModuleMap.EnsureTableAsync(tuningConnection, _logger, stoppingToken))
            {
                await DarlingModuleMap.RefreshAsync(tuningConnection, _logger, stoppingToken);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("Composer performance tuning failed — queries fall back to un-indexed scans: {Message}", ex.Message);
        }

        /* #3817: the ONE line this start's convergence pass writes, UNCONDITIONALLY — the #3756 discipline,
           and the same line the hourly pass writes (LogStoreObjectConvergence is the single owner of the
           template, so the two cadences cannot drift into two shapes). A start that changed nothing still
           writes it, with zeros, because a pass whose negative outcome is indistinguishable from its
           non-execution has not reported: before this, "did the aggregates get ensured on this start" was
           answered by reading eleven per-object lines and inferring the absence of a twelfth. Written here
           rather than inside the try blocks so it reports every segment including the ones that threw —
           a segment's catch above is what makes its steps' absence visible in this count. */
        LogStoreObjectConvergence(startupConvergence, startupConvergenceClock.ElapsedMilliseconds, startup: true);

        /* Restart continuity: re-seed delta baselines from the store (the Postgres twin of Lite's
           DuckDB seeding) so the first cycle after a service restart produces real deltas instead
           of zeroes. A seed failure logs a warning and collection proceeds with first-cycle-zero. */
        var deltas = new DarlingDeltaCalculator();
        await deltas.SeedFromStoreAsync(postgres, _logger, stoppingToken);
        _deltas = deltas;

        /* Control-plane Stage 1: SEED the config store from darling.json once (idempotent; only empty
           sections), then read the store view and make it authoritative — the held DarlingConfig is
           mutated in place so the alert-settings/capture-plans/analysis seams below all reflect the
           store. Store-unreachable degrades to the darling.json-loaded config (never worse than before).
           The initial server set comes from config_monitored_servers WHERE is_enabled = TRUE (post-seed
           equivalent to darling.json, but store-authoritative); the config_version read here is the
           reload baseline, so the seed's own version bumps do not trigger a spurious first-sweep reload. */
        var configProvider = new StoreConfigProvider(postgres, _logger);
        await configProvider.SeedIfEmptyAsync(config, stoppingToken);
        var initialView = await configProvider.LoadViewAsync(config, stoppingToken);
        IReadOnlyList<MonitoredServer> initialServers = config.Servers;
        if (initialView is not null)
        {
            StoreConfigProvider.ApplyToConfig(config, initialView);
            _scheduleOverrides = initialView.ScheduleOverrides;
            _lastConfigVersion = initialView.ConfigVersion;
            /* Stage 2: honor config_service.paused from the very first sweep (a service that was paused
               before a restart comes back up paused). */
            _paused = initialView.Paused;
            /* Reconcile the observed collect.servers.is_enabled to the desired state up front, so a server
               disabled in the store while the service was down shows disabled even though it never connects
               (never upserts) this run. */
            await DarlingObservability.SyncServerEnabledStatesAsync(postgres, _logger, stoppingToken);
            /* Post-seed the store carries darling.json's servers; fall back to the file only if the store
               read is empty (a partially-seeded store), so the service never starts up monitoring nothing. */
            if (initialView.EnabledServers.Count > 0)
            {
                initialServers = initialView.EnabledServers;
            }
        }

        /* #1560/#1562: publish the effective MCP + web enable/port (store-authoritative when the view loaded,
           else the darling.json values) so the two hosts' supervisors start from the same truth the worker
           holds — including on a store-unreachable boot. Re-published on every reload below. */
        _mcpState.Publish(config.Mcp.Enabled, config.Mcp.Port);
        _webState.Publish(config.Web.Enabled, config.Web.Port);
        /* #2298: publish the effective server set the same way — store-authoritative when the view loaded,
           else the darling.json servers, which is what this run will actually collect from either way. The
           MCP host's plan-fetch resolver reads this instead of re-reading the store as the mcp role. */
        _registryState.Publish(initialServers);

        /* Capture-plans is read live (() => config.CapturePlans) so a store reload of
           config_service.capture_plans is honored on the next collector cycle without rebuilding.
           CollectSchemaChangeEvents is a file-only knob (darling.json), read the same way for symmetry —
           default true keeps every SKU collecting Object DDL; set false to silence a benchmark box's flood. */
        /* #4004: the store's log-hash key, loaded ONCE here and shared by every run that hashes log text (pg_log_events
           on the pg_read_file and RDS routes), so raw_line_hash and statement_fingerprint are keyed with a secret the
           store never holds. Generated when none exists, and replaced only when its directory was found open to other
           users, by whichever look found it so (role provisioning above, a host, or this load), which removes the key
           there and then: null means the file could not be used, the reason is already logged, and those runs refuse
           rather than hash without it. */
        var logHashKeyLoad = DarlingLogHashKeyFile.LoadForService(config, DarlingConfig.ResolveConfigPath(), _logger);
        var logHashKey = logHashKeyLoad.Key;
        /* #4004 review, round 3: a key that replaced one the directory check discarded is noted on the collection-log
           row of the first pg_log_events run after this, and only that run (RunOneAsync takes it). */
        _logHashKeyRotation.Arm(DarlingLogHashKeyFile.RotationNote(logHashKeyLoad));
        var runner = new DarlingCollectorRunner(postgres, deltas, _logger, () => config.CapturePlans, () => config.CollectSchemaChangeEvents,
            () => StoreConfigProvider.ClampTextBudgetMb(config.QueryStoreTextBudgetMb),
            /* #2171: live provider like its siblings — a store reload flipping plan_xml_compression
               takes effect on the next write batch, no restart. */
            compressPlanContent: () => !string.Equals(config.PlanXmlCompression, "none", StringComparison.OrdinalIgnoreCase),
            /* #2862: the procedure_stats plan-capture cadence, clamped at the provider like the V59 budget
               above so the runner never sees an out-of-range interval. A file-only knob today, but read
               live like its siblings, so setting it to 1 restores every-cycle plan capture and promoting
               it to a store column later needs no change here. */
            procedureStatsPlanCycleInterval: () => StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(config.ProcedureStatsPlanCycleInterval),
            /* #3477: the per-collector database scope, resolved live against the SAME _scheduleOverrides
               the cadence gate reads — one source, so the scope a run collects under and the schedule it
               was dispatched under can never come from two different reloads. */
            databaseScope: (collectorName, serverId) => StoreConfigProvider.ResolveDatabaseScope(collectorName, serverId, _scheduleOverrides),
            logHashKey: logHashKey);
        var servers = new List<ServerLoopState>();
        /* #1581 cold-start stagger: capture ONE startup instant so every initial server's first-sweep offset is
           measured from the same base — the deterministic per-server ColdStartFirstSweepDue then spreads the
           fleet's FIRST catch-up bodies across ColdStartSpreadSeconds instead of launching all of them in a
           single sweep tick and slamming the N=4 gate (the field herd). Reconcile-ADDED servers below keep the
           MinValue default and launch promptly. */
        var coldStartInstant = DateTime.UtcNow;
        foreach (var server in initialServers)
        {
            servers.Add(new ServerLoopState
            {
                Config = server,
                FirstSweepDueUtc = ColdStartFirstSweepDue(
                    coldStartInstant, server.ServerId),
            });
        }

        /* Phase-5 slice D: the shared alert engine, wired to the PG-backed stores (V3) and the
           shared email/webhook delivery. Constructed once — the engine holds the per-server
           edge-trigger state for the service's lifetime. The settings/history/webhook pieces
           are hoisted here because the AN3 analysis-notification path below shares them. The mute
           service is hoisted too so a reload can re-LoadAsync() it (closes F16 — the engine holds
           its IsAlertMuted delegate, so refreshing the same instance's cache mutes the next sweep). */
        var alertSettings = new DarlingAlertSettings(config);
        var historyStore = new PgAlertHistoryStore(postgres, _logger);
        var webhookAlertService = new WebhookAlertService(
            alertSettings, DarlingAlertDeliverer.Branding,
            _loggerFactory.CreateLogger<WebhookAlertService>(), historyStore);
        var muteRuleService = new MuteRuleService(
            new PgMuteRuleStore(postgres), _loggerFactory.CreateLogger<MuteRuleService>());
        await LoadMuteRulesAsync(muteRuleService);

        /* The shared record-and-send deliverer — hoisted so BOTH the shared alert engine and the Stage 4
           service self-alerts (DarlingSelfAlertEvaluator) fire through the SAME instance: identical
           email/webhook delivery, per-fingerprint delivery cooldown, and history-seeded restart replay. */
        var deliverer = new DarlingAlertDeliverer(
            alertSettings, historyStore, webhookAlertService, _logger,
            /* #1236: map a fired alert's ServerKey (the storage-name hash the engine keys on) back to the live
               server's per-server delivery override, under the servers lock (mirrors FetchFailedJobsAsync). A
               store reload swaps ServerLoopState.Config, so this always reads the current override. */
            resolveServerOverride: serverKey =>
            {
                if (!int.TryParse(serverKey, NumberStyles.Integer, CultureInfo.InvariantCulture, out var id))
                {
                    return null;
                }

                lock (_serversLock)
                {
                    return servers
                        .Find(s => s.Config.ServerId == id)
                        ?.Config.AlertDeliveryModeOverride;
                }
            });
        var engine = BuildAlertEngine(config, servers, alertSettings, historyStore, muteRuleService, deliverer);

        /* Held for the PostgreSQL predictors, which deliver alongside the shared engine rather than
           through it (see EvaluatePostgresAlertsAsync). Same deliverer instance, so a PostgreSQL alert
           lands in the same history and obeys the same mute rules as an engine-emitted one — the point
           of reusing it rather than building a second delivery path. */
        _alertDeliverer = deliverer;
        /* #2711: the Postgres Deadlocks/Blocking resolution path writes history directly (see
           _historyStore's doc comment) - same instance the engine's own resolutionCallback closure
           over historyStore uses, so a restart-time history read sees both engines' rows regardless of
           which wrote them. */
        _historyStore = historyStore;
        /* Same instance the engine binds, so a mute-rule reload mutes the PostgreSQL predictors on the next
           sweep exactly as it mutes every SQL Server family. */
        _isAlertMuted = muteRuleService.IsAlertMuted;
        _alertCooldownMinutes = alertSettings.CooldownMinutes;

        /* #3285: the user-authored custom-alert evaluator. A rule's compose metric runs on a dedicated
           VIEWER-role pool (which carries the statement_timeout cap and the least-privilege ACL) — never the
           owner pool. #3970: two deployments can supply that pool — a managed Windows store's provisioned
           viewer credential (written by EnsureProvisionedAsync above), or the Linux compose store's own viewer
           role (DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync: this start's, provisioned above, or a
           trusted credential an earlier start left, accepted before use — never the owner login, unlike a
           host's own fallback). A bring-your-own store stays out either way: its viewer role comes from
           tools/provision-roles.sql, which does not create config.record_custom_alert_resolution. */
        string? customAlertViewerConnString = null;
        if (OperatingSystem.IsWindows() && config.Postgres.Managed)
        {
            customAlertViewerConnString = DarlingManagedPostgres.TryBuildViewerConnectionStringFromStoredCredential(config.Postgres);
        }
        else if (!config.Postgres.Managed && Hosting.DarlingHostBinding.IsRunningInContainer)
        {
            customAlertViewerConnString = await DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync(
                config.Postgres.ConnectionString, _logger, stoppingToken);
        }

        await using var customAlertViewerSource =
            customAlertViewerConnString is not null
                ? NpgsqlDataSource.Create(DarlingStoreConnection.PinSessionTimeZoneUtc(customAlertViewerConnString))
                : null;
        if (customAlertViewerSource is not null)
        {
            _customAlertEvaluator = new CustomAlertEvaluator(
                new CustomAlertRuleStore(postgres),
                new CustomAlertStateStore(postgres),
                customAlertViewerSource,
                deliverer,
                muteRuleService.IsAlertMuted,
                /* #3464: the same live master switch every other family consults — a closure over the
                   settings adapter's pass-through read, so a control-plane flip gates the very next sweep. */
                alertsEnabled: () => alertSettings.AlertsEnabled,
                historyStore,
                (int)s_customAlertSweepInterval.TotalSeconds,
                s_customAlertSweepInterval,
                _loggerFactory.CreateLogger<CustomAlertEvaluator>());
            _logger.LogInformation(
                "Custom alert evaluator ready (#3285) — evaluating user rules on the viewer-role pool every {Seconds}s.",
                (int)s_customAlertSweepInterval.TotalSeconds);
        }
        else
        {
            _logger.LogInformation(
                "Custom alert evaluator not started: this deployment has no viewer-role login to evaluate custom alerts with — a managed Windows store or the Linux compose store's own provisioned viewer role are required; a bring-your-own store is not (#3285, #3970).");
        }

        /* Stage 4: the service self-alerts, over the SAME deliverer + history + mute check the engine uses.
           collection-stopped / capture-down are polled from collection_log on the alert cadence below;
           connection lost/restored fire on the connect edges in TryConnectAsync. */
        _selfAlerts = new DarlingSelfAlertEvaluator(
            alertSettings, deliverer, historyStore, muteRuleService.IsAlertMuted, _logger,
            /* V20: the connect-edge Server-Unreachable/Restored delivery honors the notify toggle, read live
               through the same by-reference alertSettings seam a store reload hot-swaps. */
            notifyConnectionChanges: () => alertSettings.NotifyConnectionChanges,
            notifyConnectionDownAtStartup: () => alertSettings.NotifyConnectionDownAtStartup,
            connectionRefireMinutes: () => alertSettings.ConnectionRefireMinutes,
            /* #991: the Availability Group alert family reads its master switch and both thresholds live
               through the same by-reference alertSettings seam, so a store edit takes effect on the next sweep
               without a restart (and the clamps live on the settings properties, not here). */
            notifyAgHealth: () => alertSettings.NotifyAgHealth,
            agLagAlertSeconds: () => alertSettings.AgLagAlertSeconds,
            agRedoQueueAlertKb: () => alertSettings.AgRedoQueueAlertKb,
            agDisconnectRefireMinutes: () => alertSettings.AgDisconnectRefireMinutes,
            /* #2136: the cadence warning threshold, read live like the AG seams (clamped on the property). */
            storeJobCadenceWarnPercent: () => alertSettings.StoreJobCadenceWarnPercent,
            /* #3297 (V119): the Retention Held tiers, read live like the cadence knob above — which is what
               makes them tunable at all. Until this rung they were compile-time constants, so #3296's
               operator got an hourly CRITICAL with nothing in Settings to reach. Clamped on the properties. */
            retentionHoldWarnRatio: () => alertSettings.RetentionHoldWarnRatio,
            retentionHoldCriticalRatio: () => alertSettings.RetentionHoldCriticalRatio,
            /* #3013: the same process counter the shared engine tallies on, so one number covers both
               halves of a server's alert work. */
            readFailures: AlertReadFailureCounter.Shared,
            /* #3500: the opt-in store label, straight from the file's peers block — NOT a Func like the
               store-backed knobs above, because the peers block is file-only and restart-only, so a live
               read would claim a hot-reload the config cannot deliver. Null/blank means the evaluator
               fires under the shipped "Monitor Store" constant, byte-identical to every release before
               the field existed. */
            storeName: config.Peers?.StoreName,
            /* #3580: the two daily documents' delivered-today stamps, in the store's own key/value state
               table, so a restart of this process does not re-announce a digest or rollup the previous
               process delivered an hour ago — and does re-attempt one whose delivery failed. */
            deliveryStamps: new PgSelfAlertDeliveryStampStore(postgres, _logger));

        /* #1706: report this start's store runtime upgrade, now that there IS an alert engine to report it
           through. Fired once, here, and never re-evaluated — the store is down while an upgrade runs, so
           its start could only ever be a log line, and by the time this line is reached both terminal states
           (upgraded, or reverted and still running) have a live store to alert from. */
        if (storeUpgradeReport is not null)
        {
            await _selfAlerts.EvaluateStoreUpgradeAsync(storeUpgradeReport, stoppingToken);
        }

        /* #3908: the store's TimescaleDB extension, the same once-per-start event. */
        if (storeTimescaleReport is not null)
        {
            await _selfAlerts.EvaluateStoreTimescaleAsync(storeTimescaleReport, stoppingToken);
        }

        /* Phase-5 analysis slice AN3: the analysis pipeline's shared pieces, constructed once.
           The plan fetcher resolves a finding's serverId to the CONNECTED runtime's connection
           string (the PgPlanFetcher seam — null for an unknown/disconnected server degrades the
           fetch like Lite's ServerManager miss). The shared AnalysisNotificationService routes
           high-severity findings through DarlingFindingAlertSender (email + webhook + history,
           Lite's cadence); the serverId resolver is Lite's shape (the finding's int id as a
           string), no silencing predicate and no tray sink (headless). */
        var planFetcher = new PgPlanFetcher(
            /* Enumerated from the command loop (analyze_now) as well as the main loop, so guard the read
               against a concurrent reconcile add/remove. Held only for the lookup, never across the fetch. */
            serverId =>
            {
                lock (_serversLock)
                {
                    return servers
                        .Select(s => s.Runtime)
                        .FirstOrDefault(r => r is not null && r.ServerId == serverId)?.ConnectionString;
                }
            },
            _logger);
        /* #3916 PR B: pages wait out a hold-back window, so the flush re-reads the mute registry — a mute
           written inside the window drops the queued page. The read fails OPEN (logged, "not muted"): the
           finding already passed the queue-time mute filter in PgFindingStore. */
        var muteReadStore = new PgFindingStore(postgres, _logger);
        var notificationService = new AnalysisNotificationService(
            new DarlingFindingAlertSender(alertSettings, historyStore, webhookAlertService, _logger),
            alertSettings,
            finding => finding.ServerId.ToString(CultureInfo.InvariantCulture),
            _loggerFactory.CreateLogger<AnalysisNotificationService>(),
            isStoryMuted: async (serverId, storyPathHash) =>
                (await muteReadStore.GetMutedStoryHashesAsync(serverId)).Contains(storyPathHash));

        /* #2138 phase 1: the auto force-plan bot, hooked onto the SCHEDULED analysis pass only (the
           interactive analyze_now command deliberately does not trigger it — an operator poking a
           server should not spend the bot's blast-radius budget). Settings are file-level and OFF by
           default. The bot is constructed with the JOURNAL and nothing else — no executor, no
           connection factory — because phase 1 has no write path at all; the store it writes to is
           the monitoring store, never a monitored server. */
        _planForceBot = new PlanForceBot(
            new PgPlanForceActionStore(postgres, _loggerFactory.CreateLogger<PgPlanForceActionStore>()),
            config.ForcePlanBot.ToSettings(),
            _loggerFactory.CreateLogger<PlanForceBot>());

        /* Command plane (Stage 2): the executor claims/executes/reports config_command rows on its OWN
           5-second loop, concurrent with the collection sweep, so a slow command never stalls collection.
           The host lets snapshot_now/analyze_now reach the LIVE loop (the running server set + runner +
           analysis pieces) without the executor touching that mutable state directly; every other command
           only writes the config.* tables and rides the reload beacon. Launched here and awaited after the
           collection loop stops so both drain cleanly on shutdown. */
        var commandHost = new WorkerCommandHost(this, servers, runner, planFetcher, notificationService, config);
        var serviceInstance = $"{Environment.MachineName}:{Environment.ProcessId.ToString(CultureInfo.InvariantCulture)}";
        var commandExecutor = new DarlingCommandExecutor(postgres, commandHost, serviceInstance, _logger);
        var commandLoop = RunCommandLoopAsync(commandExecutor, stoppingToken);

        /* #2022 phase 2: the Query Store backfill worker, on its own tick (see s_queryStoreBackfillInterval).
           Fills the two windows the live path discards by design — the 60-minute first-contact tail and
           clamp-bounded outage holes — newest-first, byte-budgeted, strictly BELOW the live path's floor, and
           never past the raw tier's horizon. Plan capture reads the same live provider the runner does. */
        /* The extension flag reaches the backfill because its HORIZON depends on it (#3012): on a plain
           PostgreSQL store there are no hourly rollups for a backdated row to fall out of, so the refresh
           term does not apply and that deployment mode keeps the full raw-tier depth. Passed as a provider
           rather than a value so it cannot capture a stale reading. */
        var queryStoreBackfill = new QueryStoreBackfill(postgres, runner, deltas, _logger, () => config.CapturePlans,
            () => StoreConfigProvider.ClampTextBudgetMb(config.QueryStoreTextBudgetMb),
            () => _timescaleAvailable);
        var backfillLoop = RunQueryStoreBackfillLoopAsync(queryStoreBackfill, servers, () => config.QueryStoreBackfillEnabled, stoppingToken);

        /* The fleet concurrency gate (#1553 D2): at most N=4 per-server collection bodies open a SQL connection
           at once, so one slow or hung server cannot head-of-line-block the fleet the way the old strictly
           sequential foreach did. Deliberately NOT disposed (CA2000 suppressed, not "fixed" back by an analyzer
           or a later builder): a body still running past the shutdown drain — or a reconcile-removed server's
           body detached from the drain list — would otherwise reach its finally { gate.Release() } on a disposed
           SemaphoreSlim and throw ObjectDisposedException, faulting an unobserved Task. A SemaphoreSlim needs no
           deterministic disposal unless its AvailableWaitHandle is used, which it never is here. */
        /* #2170: the width is now an operator knob, and a SemaphoreSlim cannot be resized — so the gate's
           MAX is the clamp ceiling while its INITIAL count is the configured width. Later changes move
           between the two: widening Releases permits, narrowing absorbs them as in-flight bodies finish
           (see ReconcileSweepGate), which converges without ever blocking this loop or interrupting a
           running collection.

           Starting AT the configured width rather than at the ceiling matters (review catch): reconciling
           down only STARTS the absorber, so a gate born wide would offer ceiling-many permits for the
           window before it retires them — and a restart with many servers simultaneously due (before the
           #1581 cold-start stagger spreads them) is exactly when that window would be spent. Born narrow,
           the window does not exist. */
        var initialSweepWidth = StoreConfigProvider.ClampConcurrentSweeps(config.MaxConcurrentSweeps);
#pragma warning disable CA2000
        var serverSweepGate = new SemaphoreSlim(initialSweepWidth, SweepGateCeiling);
#pragma warning restore CA2000
        lock (_gateLock)
        {
            /* The permits the gate was never given ARE the absorbed ones — seed both counts so the first
               reconcile computes its delta from reality instead of re-absorbing what was never issued. */
            _gateAbsorbed = SweepGateCeiling - initialSweepWidth;
            _gateDesiredAbsorb = _gateAbsorbed;
        }

        _logger.LogInformation("PerformanceMonitor Darling collection loop started");
        /* #2953: the one publish that clears the failure phases. Set HERE — the last statement before the
           sweep loop's first iteration — and not re-published per cycle: this seam answers "did collection
           start", which is the question no other surface could answer without the store. Whether the CURRENT
           sweep is succeeding is collection_log's question, and by this point collection_log exists to be
           asked. */
        _collectorState.PublishCollecting();

        while (!stoppingToken.IsCancellationRequested)
        {
            /* Control-plane reload beacon: poll config_version at a SAFE point (top of the sweep, never
               mid-collection). On change, re-read the store and hot-swap the live config: the alert /
               SMTP / webhook / capture / analysis settings (via the by-reference DarlingAlertSettings
               seam + the runner's capture provider), the monitored-server set (add/remove/replace
               ServerLoopState), the per-collector schedule overrides + NextDue, and the mute-rule cache. */
            var configVersion = await configProvider.ReadConfigVersionAsync(stoppingToken);
            if (configVersion.HasValue && configVersion.Value != _lastConfigVersion)
            {
                /* The watermark advances AFTER the reload applies, not on detecting the bump. Assigning it
                   first meant a reload whose store read failed had already recorded the version as applied:
                   the operator's change stayed live in the store, absent from this process, and unreported
                   until some LATER write bumped config_version again. Nothing retried it, because the
                   beacon's next tick compared equal.

                   That ordering is why a deadline on this thread is not merely a delay — it DISCARDS the
                   change — which is the asymmetry ServiceCommandDeadlines.SerialLoopSeconds is floored
                   against. Fixing the bound without fixing the ordering would have left the silent-loss
                   path intact and only made it rarer.

                   Failing to apply now leaves the watermark behind, so the very next tick re-detects the
                   same bump and retries. That is right for the transient case this guards (a store restart,
                   a failover, a blip) and is bounded work — one single-row beacon read plus one view read
                   per 15 s tick. StoreConfigProvider rate-limits its own failure log across the streak so a
                   PERSISTENTLY unreachable store does not turn the retry into a log flood.

                   Deliberately NOT a retry POLICY: #2936 owns that question for the adjacent migrate path,
                   where it is a real design decision (which failures are retryable, bounded or forever,
                   re-enter or resume) because that path has no loop of its own and its failure is terminal.
                   This path already re-runs every tick by construction and its failure is recoverable, so
                   the only defect here was the ordering. */
                /* Stamped from the version the reload ACTUALLY applied, not from the beacon's own earlier
                   read. ReloadFromStoreAsync re-reads config_version inside LoadViewAsync, so a write
                   landing in the window between the two makes the applied view NEWER than
                   configVersion.Value — and recording the older number would leave the watermark behind a
                   config that is already live, costing one redundant idempotent re-apply on the next tick.
                   Self-healing rather than lossy, but it is the same class of imprecision as the defect
                   above ("the version recorded as applied must be the version that was applied"), and the
                   startup path at :1179 already does it this way. */
                var appliedVersion = await ReloadFromStoreAsync(configProvider, config, servers, muteRuleService, stoppingToken);
                if (appliedVersion.HasValue)
                {
                    _lastConfigVersion = appliedVersion.Value;

                    /* #2170: the reload swapped the knob into the live config; move the gate to match. Safe
                       here by construction — top of the sweep, and narrowing never preempts a running body.
                       Inside the success branch because a reload that applied nothing changed no knob. */
                    ReconcileSweepGate(serverSweepGate, StoreConfigProvider.ClampConcurrentSweeps(config.MaxConcurrentSweeps), stoppingToken);
                }
            }

            /* Stage 2 pause gate (Lite's IsPaused): while paused, skip ALL collection/alert/analysis/purge
               work but keep looping — the reload beacon above still runs (so a resume, applied via the same
               reload, un-pauses on the very next tick) and the command loop keeps draining commands. A
               resume that reloaded THIS iteration already flipped _paused to false above, so it takes effect
               immediately, not a sweep later. */
            if (!ShouldRunCollection(_paused))
            {
                try
                {
                    await Task.Delay(s_sweepInterval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                continue;
            }

            /* P1: snapshot the server list under the lock so the fire-and-track launch loop below iterates a
               STABLE array while the command loop reconciles the list concurrently (this also keeps the
               deliverer's / plan-fetcher's own locked Find/Select provably race-free). Taken AFTER the reload
               above so this sweep launches against the freshly reconciled set. */
            ServerLoopState[] sweepTargets;
            lock (_serversLock)
            {
                sweepTargets = servers.ToArray();
            }

            /* Fire-and-track launch loop (#1553 D2/D2b): LAUNCH each server's collection body WITHOUT awaiting
               it, so one slow or hung server can no longer stall the fleet or the fleet-level steps below (the
               old foreach awaited every step inline — the 24-server field incident). At most N=4 bodies open a
               connection at once (serverSweepGate, acquired inside the body). All tracking / skip-log state
               (InFlightSweep, SweepStartedUtc, WarnedThisEpisode) is written ONLY here on the outer sweep thread
               — the body never touches it, so there is no cross-thread tear on these fields. */

            /* Working-set launch guard (#1556): before launching ANY new bodies this tick, check the process
               working set against the guard threshold. Over the line, launch NOTHING this sweep so the in-flight
               bodies drain and the process backs away from the commit-limit exhaustion the field incident hit —
               but the purge / disk-pressure / delay steps below keep running. ONE CRITICAL per episode; a
               recovery re-arms and logs at Information. */
            long workingSetBytes;
            using (var currentProcess = System.Diagnostics.Process.GetCurrentProcess())
            {
                workingSetBytes = currentProcess.PrivateMemorySize64;
            }
            var availableMemoryBytes = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
            var mayLaunchSweeps = ShouldLaunchSweeps(workingSetBytes, availableMemoryBytes);
            if (!mayLaunchSweeps && !_memoryGuardTrippedThisEpisode)
            {
                _memoryGuardTrippedThisEpisode = true;
                _logger.LogCritical(
                    "Working set {WorkingSetMb}MB is over {Pct:P0} of {AvailableMb}MB available — PAUSING new collection-body launches this tick so in-flight bodies drain (the #1556 commit-limit backstop). Purge/disk/analysis continue.",
                    workingSetBytes / (1024 * 1024), MemoryGuardFraction, availableMemoryBytes / (1024 * 1024));
            }
            else if (mayLaunchSweeps && _memoryGuardTrippedThisEpisode)
            {
                _memoryGuardTrippedThisEpisode = false;
                _logger.LogInformation(
                    "Working set recovered to {WorkingSetMb}MB of {AvailableMb}MB — resuming collection-body launches.",
                    workingSetBytes / (1024 * 1024), availableMemoryBytes / (1024 * 1024));
            }

            foreach (var server in sweepTargets)
            {
                /* D3: the per-sweep cancellation check HOISTS here from the old inline body — a bare `break`
                   inside the extracted async body would not compile (CS0139), and this launch loop is the loop
                   it belongs to. */
                if (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                /* Working-set guard tripped this tick: stop launching new bodies (evaluated once, before the
                   loop, so the decision applies uniformly to every server this sweep). */
                if (!mayLaunchSweeps)
                {
                    break;
                }

                /* #1581 cold-start stagger: hold this server's FIRST sweep body (InFlightSweep still null) until
                   its deterministic per-server offset elapses, so a service restart does not launch all N
                   servers' initial catch-up bodies in ONE tick — the field herd where 24 servers stamped
                   SweepStartedUtc together, queued behind the N=4 gate, and every one that waited past 60s logged
                   "collection body has not completed after 60s". The offset is bounded by ColdStartSpreadSeconds,
                   so no first launch is deferred beyond that window; once a body has launched (InFlightSweep
                   non-null) this no longer applies, so the relaunch path below and steady-state cadence are
                   untouched. A reconcile-added server has FirstSweepDueUtc == MinValue and launches immediately. */
                if (server.InFlightSweep is null && DateTime.UtcNow < server.FirstSweepDueUtc)
                {
                    continue;
                }

                if (server.InFlightSweep is { IsCompleted: false })
                {
                    /* Still in flight: do NOT relaunch (INV-2, one body per server). The field incident was
                       HANGS, not throws, and the body's catch-all (D3) only covers throws — so the stall is
                       surfaced HERE. The EPISODE (SweepStartedUtc, stamped at launch) still spans queue + run,
                       because a body queued behind the gate genuinely is unserved. But the two halves get
                       DIFFERENT channels: RunStartedTicks is 0 until the body acquires the concurrency gate, so
                       a body merely waiting its turn is CAPACITY pressure, never a hang. Attributing queue time
                       to the hang watchdog fired it ~82 times/hour at 24 servers behind the N=4 gate on a
                       demonstrably healthy fleet, which buried the one signal this warning exists to raise. */
                    var episodeSeconds = (DateTime.UtcNow - server.SweepStartedUtc).TotalSeconds;
                    var runStartedTicks = Interlocked.Read(ref server.RunStartedTicks);
                    var running = runStartedTicks != 0;
                    var runningSeconds = running
                        ? (DateTime.UtcNow - new DateTime(runStartedTicks, DateTimeKind.Utc)).TotalSeconds
                        : 0;

                    _logger.LogDebug(
                        "[{Server}] collection body still in flight after {Elapsed:F0}s ({State}) — skipping this sweep",
                        server.Config.DisplayName,
                        episodeSeconds,
                        running ? FormattableString.Invariant($"running {runningSeconds:F0}s") : "queued for a slot");

                    switch (ClassifySweepEpisode(
                        episodeSeconds, running, runningSeconds, server.WarnedThisEpisode, server.QueuedInfoThisEpisode))
                    {
                        /* HANG — the body is actually EXECUTING and has not finished. ONE Warning per episode.
                           This is the channel that must stay quiet on a healthy fleet so a real stall is seen. */
                        case SweepEpisodeSignal.Hang:
                            server.WarnedThisEpisode = true;
                            _logger.LogWarning(
                                "[{Server}] collection body has not completed after {Elapsed:F0}s of execution — skipping relaunch",
                                server.Config.DisplayName, runningSeconds);
                            break;

                        /* CAPACITY — still QUEUED behind the gate, so nothing is wrong with this server: the
                           fleet is simply wider than the configured sweep width at this moment. Info, once.
                           Reports the EFFECTIVE width, not the compile-time default (#2170 review catch):
                           this line is what an operator reads while deciding whether to raise the knob, so
                           printing 4 after they raised it to 12 would send them chasing a limit that is no
                           longer in force. */
                        case SweepEpisodeSignal.Queued:
                            server.QueuedInfoThisEpisode = true;
                            _logger.LogInformation(
                                "[{Server}] collection body has waited {Elapsed:F0}s for a free slot (fleet concurrency limit {Limit}) — queued, not stalled; it has not started yet",
                                server.Config.DisplayName, episodeSeconds, EffectiveSweepWidth);
                            break;
                    }

                    continue;
                }

                /* The body is null (never launched) or has completed since the last sweep. If we surfaced EITHER
                   channel for this episode, log its resolution with the total elapsed and re-arm before
                   relaunching — the re-arm happens HERE on observing IsCompleted, never in the body's finally
                   (which would race this launch loop). A body that finished under both thresholds was never
                   surfaced, so it simply relaunches. */
                if (server.WarnedThisEpisode || server.QueuedInfoThisEpisode)
                {
                    var ranForSeconds = (DateTime.UtcNow - server.SweepStartedUtc).TotalSeconds;
                    _logger.LogInformation(
                        "[{Server}] collection body completed after {Elapsed:F0}s (episode, including any queue wait)",
                        server.Config.DisplayName, ranForSeconds);
                    server.WarnedThisEpisode = false;
                    server.QueuedInfoThisEpisode = false;
                }

                /* Stamp the launch time BEFORE launching — time spent QUEUED on the gate is part of this
                   episode — and clear the run stamp so this episode starts as QUEUED. The reset must precede the
                   call: ProcessServerSweepAsync runs synchronously up to its gate WaitAsync, so with a free
                   permit the body may stamp RunStartedTicks before this statement returns, and resetting after
                   would erase it. Then fire-and-track: assign the Task to InFlightSweep (so it is observed and
                   the next sweep + the shutdown drain can see it) but do NOT await it here. */
                server.SweepStartedUtc = DateTime.UtcNow;
                Interlocked.Exchange(ref server.RunStartedTicks, 0);
                server.InFlightSweep = ProcessServerSweepAsync(
                    server, engine, runner, planFetcher, notificationService, config, serverSweepGate, stoppingToken);
            }

            /* #4130: fire-and-track, exactly like the per-server sweeps just above and the oversized-plan
               backlog just below — see TryStartScheduledPurge's doc for why the inline await was the
               defect. */
            TryStartScheduledPurge(
                DateTime.UtcNow,
                token => RunScheduledPurgeAsync(postgres, config, token),
                stoppingToken);

            /* #3392: drain the oversized-plan backlog. Its own low-frequency cadence off this loop, beside
               the purge, and deliberately NOT inside the per-server collector rotation: fetching a plan the
               capture cap declined is worth doing eventually and worth nothing if it competes with a live
               collection cycle for that server's wall-clock budget.

               FIRE-AND-TRACK, not awaited — the #1553 launch-loop shape rather than the purge's. The purge
               is awaited because it talks only to the store with bounded statements; this pass opens a
               connection to every monitored server, and its worst case is
               fleet width * MaxPlansPerServerPerTick * PerPlanBudget, which on a 42-server fleet whose
               targets are all timing out is 105 minutes. Awaited, that is 105 minutes in which this loop
               launches no collection bodies at all — the 24-server field incident's exact shape, one
               maintenance step over. Launched and tracked, a slow pass costs only its own next slots.

               sweepTargets is this tick's snapshot, already taken under the servers lock, so the pass
               iterates a stable set. RunAsync is failure-isolated per server and returns quietly on
               cancellation, so the task can complete unobserved without an unhandled fault. */
            if (DateTime.UtcNow >= _nextOversizedPlanSweepUtc
                && (_oversizedPlanSweep is null || _oversizedPlanSweep.IsCompleted))
            {
                /* #3405: count BOTH populations, because an empty target list has two causes that mean
                   opposite things and the pass itself cannot tell them apart. sweepableTargets is the
                   registrations that could ever carry a runtime this pass would visit — read off the
                   registration, since Runtime is null both mid-connect and forever. backlogTargets is the
                   ones carrying one now. Counted in one walk rather than two so the pair describes the same
                   snapshot. */
                var backlogTargets = new List<ServerRuntime>(sweepTargets.Length);
                var sweepableTargets = 0;
                foreach (var target in sweepTargets)
                {
                    if (!OversizedPlanBacklogSweep.IsSweepableTarget(target.Config))
                    {
                        continue;
                    }

                    sweepableTargets++;

                    if (target.Runtime is { } runtime)
                    {
                        backlogTargets.Add(runtime);
                    }
                }

                /* Stamped BEFORE the launch, like every other cadence on this loop: the IsCompleted guard
                   above is what keeps a slow pass from stacking, and it only works against a stamp that has
                   already moved. The delay itself is the sweep's decision, not this loop's. */
                var (sweepDelay, connectWaits) = OversizedPlanBacklogSweep.NextSweepDelay(
                    sweepableTargets, backlogTargets.Count, _oversizedPlanSweepConnectWaits);
                _oversizedPlanSweepConnectWaits = connectWaits;
                _nextOversizedPlanSweepUtc = DateTime.UtcNow.Add(sweepDelay);

                _oversizedPlanSweep = OversizedPlanBacklogSweep.RunAsync(
                    postgres, backlogTargets, _logger, stoppingToken);
            }

            /* Stage 4 fleet-level self-alert: the store disk-pressure backstop. The daily purge is the ONLY
               other maintenance cadence and it is purely time-based (no disk-free check), so on its own the
               store can still fill between purges — this edge-fired condition is the flagship-appropriate
               backstop. Own slow cadence; the master alerts gate + edge-trigger live inside the evaluator. */
            if (DateTime.UtcNow >= _nextDiskCheckUtc)
            {
                _nextDiskCheckUtc = DateTime.UtcNow.Add(s_diskCheckInterval);
                await EvaluateStoreDiskPressureAsync(config, stoppingToken);
            }

            /* #3304: the custom-alert-rule integrity check. A stored rule silently stops compiling when a
               measure drifts out of the catalog (the incremental pg_* buildout), and a rule can compile yet
               never fire (scope resolves to no monitored server, or its measure is always-NULL). Nobody
               "opens" a rule, so a silent gap = an alert an operator believes is armed. Fleet-level, own slow
               cadence; both evaluators are null on deployments without a viewer pool, and each Evaluate* half
               is failure-isolated so a throw never stops the fleet loop. */
            if (_customAlertEvaluator is not null && _selfAlerts is not null
                && DateTime.UtcNow >= _nextCustomAlertHealthCheckUtc)
            {
                _nextCustomAlertHealthCheckUtc = DateTime.UtcNow.Add(s_customAlertHealthInterval);
                await EvaluateCustomAlertRuleHealthAsync(servers, stoppingToken);

                /* #3305: on the same cadence, reconcile persisted state — force-resolve + clean the state of
                   disabled rules (which the enabled-only sweep would orphan) and of servers that have left a
                   rule's scope. Deleted rules are resolved on the delete path (their state cascades away). */
                await ReconcileCustomAlertStateAsync(servers, stoppingToken);
            }

            /* #3306: a mute rule that has outlived its reason. A mute is a deliberate blind spot, and this is
               the only surface that reports one exists, how old it is, and whether it will ever expire —
               otherwise the only way to find one is to already suspect it and call get_mute_rules. Without it
               a rule created to stop a false-positive flood keeps suppressing the alert after the fix ships,
               and the symptom is silence. Judged on the CONJUNCTION (still in force, no expiry, past every expiry the
               product offers), never on permanence alone, which the dialog offers on purpose. Reads the live
               MuteRuleService cache the engine matches against — so it sees exactly what is suppressing
               alerts right now, and needs no store read of its own. Fleet-level, own slow cadence; the
               Evaluate* wrapper is failure-isolated so a throw never stops the fleet loop. */
            if (_selfAlerts is not null && DateTime.UtcNow >= _nextStaleMuteCheckUtc)
            {
                _nextStaleMuteCheckUtc = DateTime.UtcNow.Add(s_staleMuteCheckInterval);
                await _selfAlerts.EvaluateStaleMuteRulesAsync(muteRuleService.GetRules(), stoppingToken);
            }

            /* #3514: the web-dashboard TLS certificate expiry self-alert. The web host loads the certificate
               once at start and publishes its served expiry to WebTlsCertificateState; a headless service can
               run for months without a restart, so the worker re-evaluates that fixed expiry against the clock
               on its own slow cadence and the evaluator warns 30 days out / Critical once lapsed — and Critical
               at once when the snapshot carries the host's not-yet-valid refusal (#3517: the dashboard is
               loopback-only from the start, and the host does not re-decide when the date passes). A null
               snapshot means no LAN TLS certificate to watch. Fleet-level, and the Evaluate* wrapper is
               failure-isolated so a throw never stops the fleet loop. */
            if (_selfAlerts is not null && DateTime.UtcNow >= _nextWebTlsCheckUtc)
            {
                _nextWebTlsCheckUtc = DateTime.UtcNow.Add(s_webTlsCheckInterval);
                await _selfAlerts.EvaluateWebTlsCertificateAsync(
                    BuildWebTlsCertReport(_webTlsCertState.Read()), stoppingToken);
            }

            /* #4215: the managed-store settings self-alert — darling-managed.conf fell back to the
               last-good copy, is hand-edited and kept in force, or PostgreSQL rejected one or more owned
               settings outright. Every
               fact but the rejected-setting list is this start's own in-process state, carried down from
               ExecuteAsync exactly like managedConfWriteResult already is — never re-read, because the writer
               runs on every start and the state is re-derived after a restart. The rejected list is the one
               fact that is store-backed (collect.managed_conf_verdicts, V146), read fresh each tick so a value
               fixed by a later start clears without this process restarting. Fleet-level, own slow cadence;
               the Evaluate* wrapper is failure-isolated so a throw never stops the fleet loop. */
            if (_selfAlerts is not null && DateTime.UtcNow >= _nextStoreSettingsCheckUtc)
            {
                _nextStoreSettingsCheckUtc = DateTime.UtcNow.Add(s_storeSettingsCheckInterval);
                await EvaluateStoreSettingsAsync(config, managedConfWriteResult, managedUsedLastGoodConf, managedConfVerification, stoppingToken);
            }

            /* #1581: the compression-job self-heal backstop. TimescaleDB compression policy jobs can silently
               die (next_start = -infinity) or hang, halting the store's archival tier so uncompressed data grows
               without bound until the disk fills and collection stops for the WHOLE fleet (the field incident).
               Timescale-only — gated one level in since #3815 put the availability re-probe ahead of it on
               this same tick; own hourly cadence; failure-isolated inside EvaluateCompressionJobHealthAsync.

               The next due time is SNAPPED to the wall clock rather than taken from this fire (#3575). The
               dead-job arm the check judges reads next_start = -infinity, which is also what the scheduler
               writes for the few milliseconds at either edge of every healthy run before the worker is, or
               after it stops being, visible as Running — and the policies run at :MM:00 on a fixed schedule,
               so a check that re-anchored itself as "UtcNow + 1 h" on every fire crept a few seconds per hour
               across those instants until, on a production store, it sampled one 53 ms into a 63 ms run and
               paged. NextCompressionCheckUtc puts every steady-state sample at :30 past its minute instead,
               half the grid step from every policy's start in both directions. The read itself now confirms
               a -infinity trip with a second read five seconds later (ReadStuckPolicyJobsAsync), so the
               phase is hardening on top of the fix, not the fix. */
            if (DateTime.UtcNow >= _nextCompressionCheckUtc)
            {
                _nextCompressionCheckUtc = TimescaleSupport.NextCompressionCheckUtc(DateTime.UtcNow, s_compressionCheckInterval);

                /* #3815: the availability re-probe, and the one tenant of this tick that runs OUTSIDE the
                   _timescaleAvailable gate below — because it is the tenant that CORRECTS that flag. Behind
                   the gate it would be unreachable in exactly the state it exists for: a latch reading false
                   cannot be re-opened from inside the block the latch closes. That is why this tick's guard
                   is the due time alone and the flag moved down one level, and why the stamp is taken above
                   the probe rather than behind the flag — on a store whose latch reads false the due time
                   has to advance anyway, or the probe would fire on every 15-second sweep pass instead of
                   hourly.

                   The cost on a store that is genuinely plain PostgreSQL, a fully supported configuration
                   that must not be punished for it: one CREATE EXTENSION IF NOT EXISTS that fails, once an
                   hour, on a pooled connection, saying nothing above Debug. The only other thing this tick
                   runs for such a store is the store-object convergence pass's non-TimescaleDB steps (#3913,
                   the else branch below). */
                if (!_timescaleAvailable)
                {
                    await ReprobeTimescaleAvailabilityAsync(stoppingToken);
                }

                if (_timescaleAvailable)
                {
                    await EvaluateCompressionJobHealthAsync(stoppingToken);

                    /* #3812: the retention coverage gate, re-judged on the RUNNING service. Until this line
                       the only thing that armed a held retention policy was the start-path ensure above, so
                       "the gate releases the hold by itself once the backfill covers raw" was true only after
                       a restart — a store on a stable build sat held indefinitely after a backfill that had
                       worked, with the Retention Held alert still firing and reading like the backfill had
                       failed. Same tick as the compression check (the constant's comment says why this cadence
                       and why this order), each half failure-isolated inside its own method with its own
                       catch, so a retention pass that throws or runs out its budget cannot skip the
                       compression read and a compression fault cannot skip the retention pass. The Retention
                       Held self-alert rides INSIDE the compression method and therefore reads the flags as
                       they stood before this pass: a policy armed here shows as held on this tick's alert read
                       and resolves on the next hour's, one tick of lag on the resolution edge that is stated
                       rather than traded for #3575's phase. The first pass after startup fires within seconds
                       of the start-path ensure (this stamp seeds at MinValue); that pass is deliberately not
                       skipped — its "Retention re-evaluation:" line is the proof the hourly path is wired on
                       this store, visible in the same log window an operator reads after a restart, and it
                       costs twenty catalog rows and twenty chunk-pruned min() reads.

                       THE HOURLY STORE-MAINTENANCE TICK, named. It is the home for every "we decided this at
                       startup and never re-decided it" defect on the store side: #3812 and #3815 are its
                       tenants, and #3816 (job self-heal covers compression only) and #3817 (store-object
                       convergence only at startup) are queued as further ones — not built here. The contract
                       a tenant signs: its own method, its own catch-all, awaited as its own statement in the
                       gated block AFTER the compression read (the #3575 phase argument on
                       s_compressionCheckInterval), in the order it appears; a new tenant is one more await
                       line below this one. A tenant that CORRECTS the gate is the single exception and signs a
                       different contract — it goes above the gate, not below the compression read, because
                       inside it a false flag would block its own correction. #3815 is that case, and the gate
                       has exactly one input, so there is no second one to write. No delegate list yet,
                       deliberately — three tenants do not justify the indirection, and a list would hide the
                       order the phase argument depends on. */
                    await ReevaluateRetentionPoliciesAsync(stoppingToken);

                    /* #3817: the FOURTH tenant, and the one that signs the contract the comment above spells
                       out — its own method, its own catch-all, one awaited statement, LAST. The store-object
                       convergence pass: every idempotent ensure the start path runs, re-run here, so one
                       failed item heals within the hour instead of at the next restart. It is last for the
                       same #3575 reason the retention pass is third: the compression read must sample the job
                       catalog at :30 past the minute, and this pass — the heaviest of the four on a store
                       that is NOT converged — must not be ahead of it pushing that sample toward the :MM:00
                       instant the policies fire on. The ordering is pinned in RetentionReevaluationTests and
                       TimescaleAvailabilityReprobeTests, both of which now name four tenants.

                       Its first pass fires within seconds of the start-path pass (the tick's stamp seeds at
                       MinValue), and that is deliberate for the reason #3812 gives about its own: the
                       "Store object convergence:" line without the "at startup" prefix is the proof the
                       hourly path is wired on THIS store, in the same log window an operator reads after a
                       restart. On a converged store that pass costs catalog reads and two metadata ALTERs. */
                    await ConvergeStoreObjectsAsync(stoppingToken);
                }
                else
                {
                    /* #3913: the same convergence pass on a store WITHOUT TimescaleDB, walking only the steps
                       that run on every store shape (the Ungated and Tuning stages the start path already
                       runs there): the baseline relations, the store's statement statistics, the composer's
                       covering indexes. Before this, nothing re-ran on such a store between restarts, so a
                       dropped fallback view, or an extension a DBA created by hand, waited for the next
                       start. None of the compression-phase reasoning above applies, since a store without
                       TimescaleDB has no policy jobs to sample. */
                    await ConvergeStoreObjectsAsync(stoppingToken, timescaleAvailable: false);
                }
            }

            /* #2068: the store self-metrics sweep. Capacity forecasting previously required ad-hoc
               archaeology over the TimescaleDB chunk catalog, whose raw window is 4 days — a measured 3x
               daily-ingest jump was only visible because the catalog still held both eras. This persists
               the store's own size/compression/growth series (per-hypertable, per-dimension, whole-store)
               into the plain collect.store_metrics table hourly, retention bounded by the sweep's own
               DELETE. Every store shape (the hypertable arm gates on _timescaleAvailable INSIDE);
               failure-isolated inside SweepStoreSelfMetricsAsync like the two checks above. */
            /* #3466 (lane 2): the fleet sweep — the scheduled, stateful whole-fleet report. FIRE-AND-TRACK
               like the oversized-plan backlog, not awaited like the purge: the sweep reads only the loopback
               store, but it reads it once per monitored server, and a stalled store would otherwise hold
               this loop for minutes — the field-herd shape one maintenance step over. Launched and tracked,
               a slow sweep costs only its own next slots (the IsCompleted guard), and RunAsync never faults
               (catch-all inside, quiet on cancellation), so the task can complete unobserved.

               Gated on the sweep's OWN switch and deliberately NOT on config.Alerts.Enabled: sweeps under
               master-off are the muted-mode contract's whole point — the report keeps publishing, carries
               the mute in its header, and writes the would-have-paged ledger. The engine makes no delivery
               call anywhere (delivery is lane 4's daily rollup), so the master-switch census has nothing of
               this path's to count. The cadence is read ONCE into the local that both the stamp and the
               engine's span math use, so a store reload swapping config mid-tick cannot make the document
               claim a span the schedule never used — the retention-hold read-once discipline. Stamped
               BEFORE the launch, like every cadence on this loop. */
            if (config.Alerts.FleetSweepEnabled
                && DateTime.UtcNow >= _nextFleetSweepUtc
                && (_fleetSweep is null || _fleetSweep.IsCompleted))
            {
                var fleetSweepMinutes = FleetSweepEngine.ClampIntervalMinutes(config.Alerts.FleetSweepIntervalMinutes);
                _nextFleetSweepUtc = DateTime.UtcNow.AddMinutes(fleetSweepMinutes);

                /* The whole registered population, connected or not — a sweep is about the fleet, and a
                   server that cannot be reached is exactly the kind of fact it must carry (its store rows
                   go quiet, which the engine bands and explains) rather than skip. */
                var fleetSweepServers = new List<(int ServerId, string ServerName)>(sweepTargets.Length);
                foreach (var target in sweepTargets)
                {
                    fleetSweepServers.Add((target.Config.ServerId, target.Config.DisplayName));
                }

                _fleetSweep = FleetSweepEngine.RunAsync(
                    postgres, fleetSweepServers, TimeSpan.FromMinutes(fleetSweepMinutes),
                    config.Alerts.Enabled, _logger, stoppingToken);
            }

            if (DateTime.UtcNow >= _nextStoreMetricsUtc)
            {
                _nextStoreMetricsUtc = DateTime.UtcNow.Add(s_storeMetricsInterval);

                /* #4012's review: the deadlock re-mask inside the sweep keys an alert whose report is gone under the
                   same log-hash key the runner's log-event runs share. */
                _pgDeadlockRemaskKey = runner.LogHashKey;
                await SweepStoreSelfMetricsAsync(stoppingToken);

                /* #2674: right after the flush wrote the latest hour, evaluate whether any of our collectors
                   regressed in cost on a target — a fleet-level self-alert, failure-isolated like the sweep. */
                if (_selfAlerts is not null)
                {
                    try
                    {
                        await _selfAlerts.EvaluateCollectorCostAsync(_postgres!, stoppingToken);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogDebug(ex, "collector-cost self-alert evaluation failed");
                    }

                    /* #3783: the two store physical-health conditions the sweep just wrote the evidence for —
                       the dimensions' TOAST utilisation (dormant until the store carries pg_freespacemap; the
                       evaluator says why) and the checkpointer's last interval, differenced from the newest
                       two checkpointer rows. Same tick as the sweep on purpose: the rows are seconds old, so
                       the alert judges the hour the sweep measured rather than the one before it. Both
                       master-gated inside and failure-isolated inside; the outer catch is the belt. */
                    try
                    {
                        await _selfAlerts.EvaluateToastSlackAsync(_postgres!, stoppingToken);
                        await _selfAlerts.EvaluateCheckpointerPressureAsync(_postgres!, stoppingToken);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogDebug(ex, "store TOAST slack / checkpointer pressure self-alert evaluation failed");
                    }

                    /* #3466 (lane 4): the fleet sweep's DAILY channel rollup — the delivery half the sweep
                       engine deliberately does not have. Attempted on this same hourly tick because the
                       ceiling is enforced inside (one post per trailing day, and only on a day with
                       something to say — 23 of every 24 ticks cost one dictionary lookup); master-gated
                       inside like every self-alert, so master-off delivers nothing while the sweeps keep
                       publishing to the web feed. Deliberately NOT gated on FleetSweepEnabled: sweeps
                       recorded before the switch went off are still the trailing day's record, and with the
                       sweep off the store simply serves an empty day, which posts nothing. Failure-isolated
                       like its sibling above. */
                    try
                    {
                        await _selfAlerts.EvaluateFleetSweepRollupAsync(_postgres!, stoppingToken);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogDebug(ex, "fleet-sweep rollup evaluation failed");
                    }

                    /* #3712: the third daily document — the analysis singles digest, the once-a-day channel copy
                       of the findings the corroboration gate routed away from the paging channels. Same hourly
                       tick, same one-post-per-trailing-day ceiling enforced inside, same master gate inside,
                       same failure isolation as the two siblings above. */
                    try
                    {
                        await _selfAlerts.EvaluateAnalysisSinglesDigestAsync(_postgres!, stoppingToken);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogDebug(ex, "analysis singles digest evaluation failed");
                    }
                }
            }

            try
            {
                await Task.Delay(s_sweepInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        /* Shutdown drain for the fire-and-track bodies (#1553): the launch loop does NOT await the per-server
           bodies, so on cancellation some may still be running (or queued on the gate). Collect the live ones
           under the servers lock (the command loop reconciles concurrently) and wait up to the drain budget for
           them to finish, bounded so a genuinely hung body cannot hold shutdown open indefinitely. Bodies never
           fault (ProcessServerSweepAsync's catch-all), so Task.WhenAll completes cleanly rather than throwing.
           A reconcile-removed server's body is not in `servers` and so is not awaited here — it detaches, but
           its own catch-all + the never-disposed gate keep it non-fatal. The delay uses CancellationToken.None
           because stoppingToken is already cancelled at this point. */
        List<Task> inFlightSweeps;
        lock (_serversLock)
        {
            inFlightSweeps = servers
                .Select(s => s.InFlightSweep)
                .Where(t => t is not null)
                .Select(t => t!)
                .ToList();
        }

        if (inFlightSweeps.Count > 0)
        {
            await Task.WhenAny(
                Task.WhenAll(inFlightSweeps),
                Task.Delay(s_shutdownDrainBudget, CancellationToken.None));
        }

        /* #4130: drain the in-flight daily purge the same way as the per-server sweeps above, rather than
           abandoning it mid-DELETE. RunTrackedAsync's own try/catch swallows the OperationCanceledException
           that stoppingToken's cancellation raises inside PurgeAsync, so this await completes cleanly
           without throwing — same shape as the sweeps' catch-all. */
        if (_purgeTask is { IsCompleted: false })
        {
            await Task.WhenAny(_purgeTask, Task.Delay(s_shutdownDrainBudget, CancellationToken.None));
        }

        /* Drain the concurrent command loop on shutdown (it observes the same token). */
        try
        {
            await commandLoop;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* #3916 PR B: once the sweeps and the command loop (analyze_now) have drained, nothing enqueues a
           page any more; drop the analysis hold-back queue unstamped. A page queued at stop is not a
           delivery, so nothing holds its story and the next start re-attempts it. */
        notificationService.Dispose();

        /* Drain the Query Store backfill loop the same way (#2022): a slice abandoned mid-shutdown is
           fine — its boundary is derived (or hole-recorded) from what actually landed, so the next
           start resumes exactly where the COPY committed. */
        try
        {
            await backfillLoop;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* Drain the background baseline backfill the same way (#1757). It observes the same token, and its
           own body already swallows everything but cancellation, so this is about not leaving an unobserved
           Task behind — a backfill still running at shutdown is fine to abandon: TimescaleDB commits it in
           per-batch transactions and the coverage gate picks it up from there on the next start. */
        if (baselineBackfill is not null)
        {
            try
            {
                await baselineBackfill;
            }
            catch (OperationCanceledException)
            {
                /* Expected on shutdown. */
            }
        }

        /* And the hole repair (#3653, Q10), for the same reason: a forced refresh cut short at shutdown is
           re-found by the next start's scan, because the scan reads the materialization rather than a ledger
           of what this start meant to do. */
        if (holeRepair is not null)
        {
            try
            {
                await holeRepair;
            }
            catch (OperationCanceledException)
            {
                /* Expected on shutdown. */
            }
        }

        /* #4299/#4391: the Periodic pass's own repair launch, same drain as the start-path one above —
           a repair the Periodic tick started is awaited here too, so shutdown does not race it and any fault
           it throws is observed rather than lost with the task. */
        if (_periodicHoleRepair is not null)
        {
            try
            {
                await _periodicHoleRepair;
            }
            catch (OperationCanceledException)
            {
                /* Expected on shutdown. */
            }
        }

        /* And the setting scrub (#4348 S1b): a run cut short by shutdown left its marker unwritten (the
           marker is only written after every batch completes), so the next start tries again from the top. */
        try
        {
            await settingScrub;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* And the statement-text scrub (#4348), for the same reason. */
        try
        {
            await statementTextScrub;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* And the plan-force-actions detail scrub (#4346), for the same reason. */
        try
        {
            await planForceDetailScrub;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        _logger.LogInformation("PerformanceMonitor Darling collection loop stopped");
    }

    /// <summary>
    /// One server's collection body (#1553 D3), extracted from the old inline sweep loop for the fire-and-track
    /// model (self-alert call rebased to the _postgres field, the loop's continue/break reshaped to a body
    /// return + a hoisted cancellation check): self-alert eval -> connect-or-collect -> alert eval -> analysis, in that order,
    /// SEQUENTIAL within this one server (Lite's RemoteCollectorService shape — parallel across servers,
    /// sequential collectors per server). Runs on a pool thread; the outer launch loop tracks the returned Task
    /// in <see cref="ServerLoopState.InFlightSweep"/> and never awaits it inline, so one slow/hung server cannot
    /// head-of-line-block the fleet. The fleet gate is acquired here (bounding fleet-wide concurrency to
    /// <see cref="MaxConcurrentServerSweeps"/>), and the whole body is wrapped so a hung server cannot starve
    /// the fleet and one server's unexpected throw is CONTAINED (Design Goal 4) rather than killing the loop or
    /// surfacing as an unobserved fire-and-track fault.
    /// </summary>
    private async Task ProcessServerSweepAsync(
        ServerLoopState server,
        AlertEngine engine,
        DarlingCollectorRunner runner,
        PgPlanFetcher planFetcher,
        AnalysisNotificationService notificationService,
        DarlingConfig config,
        SemaphoreSlim gate,
        CancellationToken stoppingToken)
    {
        /* Acquire the fleet concurrency gate OUTSIDE the try (the never-faulting-probe idiom): WaitAsync either
           returns having TAKEN a permit — matched by the finally's Release — or THROWS owning nothing (a cancel
           while queued on shutdown), so the finally can never over-release a permit we do not hold. */
        await gate.WaitAsync(stoppingToken);

        /* The permit is held: this body has STOPPED queueing and STARTED running. Stamp the run start so the
           outer launch loop's watchdog can tell a genuine hang from a body that was merely waiting its turn.
           This is the ONE sweep-bookkeeping field the body writes, via Interlocked (see RunStartedTicks) — the
           three outer-thread-only fields are deliberately left alone. Placed before the Retired check so a
           retired body still reports as "running" for the instant it takes to no-op out, rather than looking
           permanently queued. */
        Interlocked.Exchange(ref server.RunStartedTicks, DateTime.UtcNow.Ticks);

        try
        {
            /* Retired containment (#1553 D1), the AUTHORITATIVE check: a reconcile-remove may have retired this
               server AFTER this body was launched or while it sat QUEUED on the gate. An async method runs
               synchronously only to its first await, so a check BEFORE WaitAsync would evaluate at LAUNCH time
               (Retired still false) and a gate-queued body would never re-check on dequeue — so the check lives
               HERE, as the first statement after acquiring the permit. A retired body no-ops entirely: it never
               connects, never runs XE DDL, and never re-writes self-alert edge state after Forget removed it.
               (The connect path adds a SECOND re-check for the narrow window where removal lands DURING the
               connect I/O; this entry check covers the dominant queued-dequeue path.) */
            if (server.Retired)
            {
                return;
            }

            /* Stage 4 service self-alerts (store-polled): collection-stopped is evaluated for EVERY server —
               connected or not — because an unreachable server has stopped collecting, which is exactly the
               case a headless service must page on. Capture-down is evaluated only for a connected server. Own
               30s cadence; the master alerts gate + edge-trigger live inside the evaluator. Runs ABOVE the
               Runtime-null connect gate so a disconnected server is still checked. Connection lost/restored fire
               on the connect edges in TryConnectAsync. (Uses the _postgres field — the loop-local `postgres` of
               RunCollectionLoopAsync is out of scope in this extracted body.) */
            if (DateTime.UtcNow >= server.NextSelfAlertSweep)
            {
                server.NextSelfAlertSweep = DateTime.UtcNow.Add(s_alertSweepInterval);
                await _selfAlerts!.EvaluateStoreAlertsAsync(
                    _postgres!,
                    server.Config.ServerId,
                    server.Config.DisplayName,
                    connected: server.Runtime is not null,
                    stoppingToken);
            }

            /* #3285: user-authored custom-alert rules, above the connect gate (like the self-alerts) so a rule
               reading the collected store still evaluates for a currently-disconnected server. Its own cadence;
               null on deployments that cannot supply a viewer-role pool. */
            if (_customAlertEvaluator is not null && DateTime.UtcNow >= server.NextCustomAlertSweep)
            {
                server.NextCustomAlertSweep = DateTime.UtcNow.Add(s_customAlertSweepInterval);
                await _customAlertEvaluator.EvaluateServerAsync(
                    server.Config.ServerId,
                    server.Config.StorageName,
                    server.Config.DisplayName,
                    stoppingToken);
            }

            if (server.Runtime is null)
            {
                await TryConnectAsync(server, runner, config, stoppingToken);
                return;
            }

            /* Reconcile the opt-in long-query completion XE session (#1496) to its enabled flag before the
               collector sweep. State-tracked (ServerLoopState.LongQueryTraceApplied), so it only opens a
               connection when the desired state changes — enabling creates the session, disabling DROPS it.
               Runs regardless of whether the collector is due or enabled, because a disabled collector is
               never dispatched by RunDueCollectorsAsync and so the DROP-on-disable has nowhere else to run. */
            await ReconcileLongQueryTraceAsync(server, runner, stoppingToken);

            await RunDueCollectorsAsync(server, runner, stoppingToken);

            /* After the server's collector sweep: evaluate alerts against the freshly collected store — on
               Lite's 30-second overview cadence. */
            if (DateTime.UtcNow >= server.NextAlertSweep)
            {
                server.NextAlertSweep = DateTime.UtcNow.Add(s_alertSweepInterval);
                await EvaluateAlertsAsync(engine, server, config, stoppingToken);
            }

            /* #3467: the same-statement-pileup finding, evaluated at collection cadence rather than the
               analysis interval — the entire point of the finding is that the 20:10:31Z snapshot held
               notify-grade evidence 21 minutes before the scheduled pass described it and ~28 before the
               query_store-fed finding could exist. Alert-sweep cadence (30 s) over a one-minute snapshot
               collector means a pileup is evaluated within one collection cycle of being visible.
               Analysis-side gates, not alert-side: production rides config.Analysis.Enabled (D0 — the
               pass runs and persists whatever the master switch says; delivery alone is gated inside,
               via ShouldNotifyAnalysisFindings). SQL Server targets only — the shape is a
               sys.dm_exec_requests statement pileup, and a PostgreSQL target never writes
               query_snapshots rows. */
            if (config.Analysis.Enabled
                && server.Runtime?.Target.Engine != CollectorTargetEngine.PostgreSql
                && DateTime.UtcNow >= server.NextPileupSweep)
            {
                server.NextPileupSweep = DateTime.UtcNow.Add(s_alertSweepInterval);
                await EvaluateSameStatementPileupAsync(server, config, notificationService, stoppingToken);
            }

            /* AN3: the scheduled analysis pipeline, per-server. The cadence, the enabled gate, and the notify
               gate are now control-plane knobs read LIVE from config.Analysis (a reload takes effect on the next
               tick). When analysis is disabled the pass is skipped and NextAnalysisDue is left in the past, so
               re-enabling runs immediately. The next-due stamp advances up front (Lite's scheduler shape), so a
               timed-out pass is skipped, not retried immediately. Delivery is gated on the master switch
               AND analysis_notifications_enabled (ShouldNotifyAnalysisFindings — #3464; Lite's D0 split
               stands: production unconditional, delivery gated); the interval is clamped to Lite's 5-360
               range. */
            if (config.Analysis.Enabled && DateTime.UtcNow >= server.NextAnalysisDue)
            {
                var intervalMinutes = Math.Clamp(config.Analysis.IntervalMinutes, MinAnalysisIntervalMinutes, MaxAnalysisIntervalMinutes);
                server.NextAnalysisDue = DateTime.UtcNow.AddMinutes(intervalMinutes);

                /* Every engine takes the pass (#3542). Until the PostgreSQL-target analysis engine existed,
                   this site gated on the target engine and wrote a tombstone into analysis_state for a
                   PostgreSQL server instead of running: the pipeline was SQL-Server-shaped, its data-span
                   gate read wait_stats, and a PostgreSQL server_id would have sat at "0 hours, still
                   collecting" for the life of the deployment. That gate is gone, and the invariant that
                   replaced it lives one layer down: the pass ROUTES BY THE REGISTRY'S engine_kind INSIDE
                   DarlingAnalysisService (its ResolveEngineAsync picks the PostgreSQL-target component set
                   and the pg_database_stats span gate for a postgres / aurora-postgres row), so the worker
                   no longer knows or cares which engine it is scheduling. The first real pass then
                   overwrites any tombstone row still standing with the honest state (D7 — the lazy
                   overwrite, no migration). */
                await RunScheduledAnalysisAsync(
                    server, planFetcher, notificationService, ShouldNotifyAnalysisFindings(config), stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            /* Shutdown (a body-internal await observed the token) — quiet and expected. */
        }
        catch (Exception ex)
        {
            /* Design Goal 4 (exception containment — the gap the sweep loop previously acknowledged it had no
               catch-all for): one server's unexpected throw must never kill the collection loop, and a faulted
               fire-and-track Task must never surface as an unobserved exception. Log and isolate; the server
               retries on its next sweep exactly like a failed collector. */
            _logger.LogError("[{Server}] Collection sweep failed: {Message}", server.Config.DisplayName, ex.Message);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Reconciles the OPT-IN long-query completion XE session (#1496) to its resolved enabled flag, once
    /// the desired state differs from what was last applied to this server (tracked in
    /// <see cref="ServerLoopState.LongQueryTraceApplied"/> so steady state — a default-off collector —
    /// opens no connection at all). Enabling creates the server-side session; disabling drops it. A
    /// failure leaves the applied state unchanged so the next sweep retries, and never breaks the sweep.
    /// </summary>
    private async Task ReconcileLongQueryTraceAsync(ServerLoopState server, DarlingCollectorRunner runner, CancellationToken cancellationToken)
    {
        if (server.Runtime is null)
        {
            return;
        }

        /* XE is a SQL Server concept: there is nothing to create or drop on a PostgreSQL target, and the
           un-gated form was the round-2 live catch — ReconcileLongQueryCompletionsAsync builds a
           SqlConnection, the ctor throws "Keyword not supported: 'host'", the catch below skips the latch
           assignment, and because LongQueryTraceApplied resets to null on every connect the failure retried
           EVERY sweep forever (~1,440 warnings/day/server, the same order as the defect this PR fixed).
           Same gate as EnsureAllAsync and FetchFailedJobsAsync, the two doors this class already closed. */
        if (server.Runtime.Target.Engine != CollectorTargetEngine.SqlServer)
        {
            return;
        }

        var serverId = server.Config.ServerId;
        var enabled = StoreConfigProvider.ResolveSchedule("long_query_completions", serverId, _scheduleOverrides).Enabled;

        if (server.LongQueryTraceApplied == enabled)
        {
            return;
        }

        try
        {
            var partialNote = await DarlingXeSessions.ReconcileLongQueryCompletionsAsync(server.Runtime, runner, enabled, _logger, cancellationToken);
            server.LongQueryTraceApplied = enabled;

            /* #3754: a reconcile that returned is one the session exists after - everywhere, or (Azure)
               everywhere it could. Clear the fault, and carry the partial note if there was one. Nulled
               rather than left alone on the DISABLED arm: the collector is not dispatched while disabled,
               so a stale note from an earlier enabled reconcile must not be waiting when it is re-enabled
               and the next reconcile (which runs first, in this same sweep) has already replaced it. */
            server.LongQueryTraceFault = null;
            server.LongQueryTracePartialNote = enabled ? partialNote : null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("[{Server}] Failed to reconcile the long-query completion XE session: {Message}",
                server.Config.DisplayName, ex.Message);

            /* #3754: while ENABLING, a throw means the session could not be created where the collector
               will read - the one CREATE on-prem, or every database on Azure SQL DB (the Azure arm throws
               only for all-refused). Record it so this sweep's run of the collector is classified
               SESSION_MISSING rather than dispatched to read an absent session as zero rows. The database
               name rides on the exception where the Azure arm stamped one (#2997); on-prem there is no
               database to name. A DISABLING failure records nothing here: the collector is not dispatched
               while disabled, so there is no run to be honest on, and the retry the unchanged latch already
               buys is the whole of the remedy. */
            if (enabled)
            {
                var refusedIn = CollectorFaultDatabase.For(ex, fallback: null);
                server.LongQueryTraceFault = refusedIn is null
                    ? $"XE session {LongQueryCompletionsCollector.XeSessionName} could not be created, so no completions can be captured until it is: {ex.Message}"
                    : $"XE session {LongQueryCompletionsCollector.XeSessionName} could not be created in any monitored database (first refusal in [{refusedIn}]), so no completions can be captured until it is: {ex.Message}";
                server.LongQueryTracePartialNote = null;
            }
        }
    }

    /// <summary>
    /// Materializes the baseline aggregates over the history the store already had (#1757), concurrently with
    /// the collection sweep rather than ahead of it. On a fresh store the coverage gate makes this a no-op; on
    /// an upgraded store it is the one-time pass that turns a refresh-window-deep supply into the full
    /// baseline window.
    ///
    /// <para>Takes its OWN connection rather than borrowing the startup one: the caller's connection is scoped
    /// to the TimescaleDB setup block and is disposed the moment that block exits, which is long before this
    /// finishes. Everything is swallowed but cancellation — a store that cannot be backfilled must degrade to
    /// short baselines, never take down collection — and <see cref="TimescaleSupport.BackfillBaselineAggregatesAsync"/>
    /// is itself failure-isolated per aggregate, so this catch is only for the connection-open path.</para>
    /// </summary>
    private async Task RunBaselineBackfillAsync(NpgsqlDataSource postgres, CancellationToken stoppingToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(stoppingToken);
            var backfilled = await TimescaleSupport.BackfillBaselineAggregatesAsync(connection, _logger, stoppingToken);
            if (backfilled > 0)
            {
                _logger.LogInformation(
                    "TimescaleDB: baseline backfill complete — {Backfilled} aggregate(s) materialized over pre-existing history.",
                    backfilled);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "Baseline aggregate backfill could not run — baselines are computed from however much history the aggregates already hold: {Message}",
                ex.Message);
        }
    }

    /// <summary>
    /// #4391: whether a materialization-hole repair pass's summary is clean enough to stamp the repair
    /// epoch — true only when the pass had zero isolated per-aggregate failures. Deferred holes
    /// (<see cref="TimescaleSupport.MaterializationHoleRepairSummary.HolesDeferred"/>) and holes still
    /// remaining after repair do not disqualify a stamp: those are re-measured fresh by the Periodic raw
    /// purge trigger's own hole scan at drop time, so a deferred-but-otherwise-clean pass may still stamp.
    /// </summary>
    internal static bool RepairEpochStampAllowed(TimescaleSupport.MaterializationHoleRepairSummary summary)
        => summary.Failures == 0;

    /// <summary>
    /// Scans every continuous aggregate for materialization holes and closes each with one targeted forced
    /// refresh (#3653, Q10 — <see cref="TimescaleSupport.RepairMaterializationHolesAsync"/>), concurrently with
    /// the rest of startup rather than ahead of it, for the reason <see cref="RunBaselineBackfillAsync"/> gives:
    /// the work is bounded but not small on the largest store, and a restarted service must not go dark for it.
    ///
    /// <para>Its OWN connection, like the backfill's — the startup connection is scoped to the TimescaleDB
    /// block. Everything but cancellation is swallowed here: the pass is failure-isolated per aggregate
    /// already, so this catch is for the connection-open path, and a store whose holes cannot be repaired
    /// degrades to the holes it had, never to a service that did not start.</para>
    ///
    /// <para><b>One INFORMATION line per start, whatever the scan found (#3756).</b> The pass returns its
    /// tally and this method writes it, UNCONDITIONALLY. It is the self-proving-flag class (#3574's
    /// <c>visibility</c>, #3735's <c>collection_health_age_seconds</c>): a check whose negative outcome is
    /// indistinguishable from its non-execution has not reported. The first store to carry the scan showed
    /// the failure: the pass wrote a summary only when it had repaired, deferred or failed something, so a
    /// start that found nothing, a start whose scan threw before its first probe and was swallowed by the
    /// per-aggregate isolation, and a start that never reached the scan all left the same absence, and the
    /// countersign had to accept "zero needed" on inference. Now every outcome writes a DIFFERENT line, all
    /// from this one method so a reader has one place to look: the pass returned — this summary, with zeros
    /// when there were zeros and the isolated-failure count when isolation caught anything (the per-aggregate
    /// WARNING already names the aggregate and the message; the summary's job is the census, so it stays
    /// INFORMATION rather than repeating the failure at WARNING and counting it twice in any warning-rate
    /// read); the pass threw outside its isolation or the connection would not open — the WARNING below;
    /// shutdown cancelled it before it could report — the cancellation line, so the next start's reader knows
    /// this one's scan is not a verdict. A start that never launches it (plain-PostgreSQL mode, or the
    /// TimescaleDB block faulting first) already writes its own line at the launch site's catch; nothing here
    /// can or should speak for that case.</para>
    /// </summary>
    private async Task RunMaterializationHoleRepairAsync(NpgsqlDataSource postgres, CancellationToken stoppingToken)
    {
        _materializationHoleRepairRunning = true;
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(stoppingToken);

            /* #4299: read BEFORE the repair runs, so the value stamped on a clean finish is the postmaster
               start this repair actually ran under, not whatever it is by the time the repair returns. A
               connection-open failure here throws out to the catch below with nothing stamped, which is right —
               a repair that never ran must not make the trigger's epoch check pass. */
            long postmasterEpoch;
            await using (var epochRead = new NpgsqlCommand(TimescaleSupport.PostmasterStartEpochMicrosecondsSql, connection))
            {
                postmasterEpoch = (long)(await epochRead.ExecuteScalarAsync(stoppingToken))!;
            }

            var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, _logger, DateTime.UtcNow, stoppingToken);

            /* #3756: not gated on any count — the zero-hole start is the one this line exists for. Every figure
               is the pass's own (TimescaleSupport.MaterializationHoleRepairSummary states the arithmetic:
               buckets found = repaired + deferred; a range the cap split counts once as found and once on each
               side), and the elapsed is the pass's clock, not this method's connection open. */
            _logger.LogInformation(
                "TimescaleDB: materialization hole scan (#3653 Q10) — {Scanned} aggregate(s) walked, {Skipped} skipped, {HolesFound} hole(s) found spanning {BucketsFound} bucket(s), {HolesRepaired} hole(s) / {BucketsRepaired} bucket(s) repaired ({Forced} needed the forced refresh), {HolesDeferred} hole(s) / {BucketsDeferred} bucket(s) deferred past the cap, {Remaining} bucket(s) still reading as holes after repair, {Failures} isolated failure(s), in {ElapsedMs} ms.",
                summary.AggregatesScanned, summary.AggregatesSkipped, summary.HolesFound, summary.BucketsFound,
                summary.HolesRepaired, summary.BucketsRepaired, summary.HolesForced, summary.HolesDeferred, summary.BucketsDeferred,
                summary.HolesRemaining, summary.Failures, (long)summary.Elapsed.TotalMilliseconds);

            /* #4299/#4391: the completion stamp — reached ONLY here, past both the epoch read and the
               repair call, neither of which threw or was cancelled. Stamped on every one of the three raw jobs
               (TimescaleSupport.RawRelations), one statement per job, each guarded by its own
               IS DISTINCT FROM so a repeat stamp of the same value writes nothing. A stamp failure here is
               logged and swallowed per job — the repair itself already succeeded and reporting that success
               is not conditional on the stamp also landing; a job whose stamp did not take simply stays
               ungated for the Periodic trigger until the next repair tries again.

               The stamp itself is gated on RepairEpochStampAllowed(summary): a repair that ran with one or
               more isolated per-aggregate failures (summary.Failures > 0) must NOT stamp, because the epoch
               is the trigger's promise that the repair covering it finished clean — TimescaleSupport's own
               doc on the stamp says an isolated failure must not make that check pass. Deferred holes and
               holes still remaining after repair (summary.HolesDeferred, summary.HolesRemaining) do NOT block
               the stamp: those are read fresh by the trigger's own hole scan at purge time, which is the gate
               for them; only a failure that means this pass never finished cleanly blocks it here. */
            if (RepairEpochStampAllowed(summary))
            {
                foreach (var relation in TimescaleSupport.RawRelations)
                {
                    try
                    {
                        await using var stamp = new NpgsqlCommand(TimescaleSupport.RawRepairEpochStampSql(relation), connection);
                        stamp.Parameters.AddWithValue(postmasterEpoch);
                        await stamp.ExecuteNonQueryAsync(stoppingToken);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogWarning(
                            "Materialization-hole repair finished, but stamping the repair epoch on {Relation}'s raw retention job failed — the Periodic trigger will not purge it until a later repair stamps it: {Message}",
                            relation, ex.Message);
                    }
                }
            }
            else
            {
                _logger.LogWarning(
                    "Materialization-hole repair finished with {Failures} failure(s); the repair epoch stays unstamped, so the Periodic raw purge keeps holding until a repair completes cleanly.",
                    summary.Failures);
            }
        }
        catch (OperationCanceledException)
        {
            /* #3756: the absence the issue's three did not name. Without this, a scan cut short by shutdown leaves
               exactly the nothing a scan that never ran leaves. Rethrown so the drain's contract is untouched (it
               swallows this as "expected on shutdown"); the line is the whole of the change. */
            _logger.LogInformation(
                "TimescaleDB: materialization hole scan (#3653 Q10) was cancelled before it could report — at shutdown that is expected, and the next start's scan re-finds anything this one left, because the scan reads the materialization rather than a ledger of what this start meant to do.");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "Materialization-hole repair could not run — any pre-outage tail the refresh policies skipped stays unmaterialized until the next start retries: {Message}",
                ex.Message);
        }
        finally
        {
            _materializationHoleRepairRunning = false;
        }
    }

    /// <summary>
    /// Runs <see cref="PgSettingScrub.RunAsync"/> once (#4348 S1b), concurrently with the rest of startup.
    /// Its own connection and its own catch, the same isolation as <see cref="RunMaterializationHoleRepairAsync"/>:
    /// a store this cannot reach degrades to whatever plaintext it already had, never to a service that did
    /// not start. One INFORMATION line whatever the run found, so "nothing to do" (already scrubbed) is
    /// visibly different from "never ran" in the log.
    /// </summary>
    private async Task RunPgSettingScrubAsync(NpgsqlDataSource postgres, CancellationToken stoppingToken)
    {
        try
        {
            var summary = await PgSettingScrub.RunAsync(postgres, _logger, stoppingToken);
            if (summary.AlreadyDone)
            {
                _logger.LogInformation("Postgres setting scrub (#4348): already scrubbed at the current rules version — nothing to do.");
            }
            else
            {
                _logger.LogInformation(
                    "Postgres setting scrub (#4348): {Candidates} candidate row(s) read, {Updated} row(s) redacted across {Days} day(s).",
                    summary.CandidatesRead, summary.RowsUpdated, summary.DaysTouched);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation(
                "Postgres setting scrub (#4348) was cancelled before it could report — at shutdown that is expected, and the next start retries from the top because the marker is only written after every batch completes.");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "Postgres setting scrub (#4348) could not run — any row an older collector build stored unredacted stays as it is until the next start retries: {Message}",
                ex.Message);
        }
    }

    /// <summary>
    /// Runs <see cref="PgStatementTextScrub.RunAsync"/> once (#4348), concurrently with the rest of startup.
    /// Same isolation as <see cref="RunPgSettingScrubAsync"/>: its own connection, its own catch, and a
    /// store this cannot reach retries the scrub at the next start, never blocking the service from
    /// starting.
    /// </summary>
    private async Task RunPgStatementTextScrubAsync(NpgsqlDataSource postgres, CancellationToken stoppingToken)
    {
        try
        {
            var summary = await PgStatementTextScrub.RunAsync(postgres, _logger, stoppingToken);
            if (summary.AlreadyDone)
            {
                _logger.LogInformation("Postgres statement-text scrub (#4348): already scrubbed at the current scrub version — nothing to do.");
            }
            else
            {
                _logger.LogInformation(
                    "Postgres statement-text scrub (#4348): {StatementTextUpdated} pg_statement_text row(s) and {BlockingEdgesUpdated} pg_blocking_edges row(s) updated.",
                    summary.StatementTextRowsUpdated, summary.BlockingEdgesRowsUpdated);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation(
                "Postgres statement-text scrub (#4348) was cancelled before it could report — at shutdown that is expected, and the next start retries from the top because the marker is only written after every batch completes.");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Never the exception TEXT — just the exception type and SQLSTATE (when it is an NpgsqlException),
               the same discipline PgStatementTextScrub's own per-server/per-day catches apply. */
            _logger.LogWarning(
                "Postgres statement-text scrub (#4348) could not run ({ExceptionType}{SqlState}) — the scrub retries at the next start.",
                ex.GetType().Name, ex is NpgsqlException npgsqlEx ? $", SQLSTATE {npgsqlEx.SqlState}" : string.Empty);
        }
    }

    /// <summary>
    /// Runs <see cref="PlanForceActionDetailScrub.RunAsync"/> once (#4346), concurrently with the rest of
    /// startup. Its own connection and its own catch, the same isolation as
    /// <see cref="RunPgSettingScrubAsync"/>: a store this cannot reach keeps whatever legacy text it
    /// already had, never a service that did not start.
    /// </summary>
    private async Task RunPlanForceActionDetailScrubAsync(NpgsqlDataSource postgres, CancellationToken stoppingToken)
    {
        try
        {
            var summary = await PlanForceActionDetailScrub.RunAsync(postgres, _logger, stoppingToken);
            if (summary.AlreadyDone)
            {
                _logger.LogInformation("Plan-force-action detail scrub (#4346): already scrubbed — nothing to do.");
            }
            else
            {
                _logger.LogInformation(
                    "Plan-force-action detail scrub (#4346): {Candidates} candidate row(s) read, {Updated} row(s) redacted.",
                    summary.CandidatesRead, summary.RowsUpdated);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation(
                "Plan-force-action detail scrub (#4346) was cancelled before it could report — at shutdown that is expected, and the next start retries from the top because the marker is only written after every batch completes.");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Never ex.Message here — this scrub's whole reason for existing is a row that carried
               unredacted exception text, and its own failure path must not repeat that mistake. Log the
               exception's type and SQLSTATE only, the same shape SanitizeDetailForAudit's own matched
               templates and CollectionFailure.Describe use. */
            var sqlState = ex is Npgsql.NpgsqlException { SqlState: { Length: > 0 } state } ? state : null;
            _logger.LogWarning(
                "Plan-force-action detail scrub (#4346) could not run — any legacy row stays as it is until the next start retries: {ExceptionType}{SqlState}",
                ex.GetType().Name, sqlState is null ? "" : $", SQLSTATE {sqlState}");
        }
    }

    /// <summary>
    /// Moves the sweep gate to <paramref name="target"/> concurrent servers (#2170). The gate is built at
    /// <see cref="SweepGateCeiling"/> and its width is expressed as how many permits are held OUT of
    /// circulation, so narrowing "absorbs" permits and widening gives them back.
    ///
    /// <para>State is the absorbed COUNT plus a desired count, both under <see cref="_gateLock"/>, rather
    /// than a per-call absorb loop: the first cut had a permit-stealing race (review catch) where a
    /// still-running narrowing task would immediately re-absorb the permit a later widening had just
    /// released, pinning the gate below the configured width. At most one absorber runs, and it re-reads
    /// the desired count under the lock before AND after every wait — so a widening mid-absorb makes the
    /// absorber hand its permit straight back and retire.</para>
    ///
    /// <para>Never blocks the caller and never preempts a running collection: narrowing only takes permits
    /// as in-flight bodies release them, so the effective width converges within about one sweep.</para>
    /// </summary>
    private void ReconcileSweepGate(SemaphoreSlim gate, int target, CancellationToken stoppingToken)
    {
        int toRelease;
        bool startAbsorber;
        lock (_gateLock)
        {
            var desired = SweepGateCeiling - target;
            if (desired == _gateDesiredAbsorb && _gateAbsorbed == desired)
            {
                return;
            }

            _gateDesiredAbsorb = desired;
            toRelease = _gateAbsorbed > desired ? _gateAbsorbed - desired : 0;
            _gateAbsorbed -= toRelease;
            startAbsorber = _gateAbsorbed < desired && !_gateAbsorberRunning;
            if (startAbsorber)
            {
                _gateAbsorberRunning = true;
            }
        }

        if (toRelease > 0)
        {
            gate.Release(toRelease);
            _logger.LogInformation("Fleet sweep width widened to {Target} concurrent servers (#2170 knob)", target);
        }

        if (startAbsorber)
        {
            _logger.LogInformation(
                "Fleet sweep width narrowing to {Target} concurrent servers (#2170 knob) — permits retire as in-flight collections finish",
                target);
            _ = Task.Run(() => AbsorbSweepPermitsAsync(gate, stoppingToken), stoppingToken);
        }
    }

    /// <summary>
    /// The single sweep-gate absorber (#2170): takes permits out of circulation until the absorbed count
    /// reaches the desired count. Re-checks that target around every wait, so a widening that lands while
    /// it is parked on <see cref="SemaphoreSlim.WaitAsync(CancellationToken)"/> is honored — the permit it
    /// was granted goes straight back rather than being stolen from the wider gate.
    /// </summary>
    private async Task AbsorbSweepPermitsAsync(SemaphoreSlim gate, CancellationToken stoppingToken)
    {
        try
        {
            while (true)
            {
                lock (_gateLock)
                {
                    if (_gateAbsorbed >= _gateDesiredAbsorb)
                    {
                        _gateAbsorberRunning = false;
                        return;
                    }
                }

                await gate.WaitAsync(stoppingToken).ConfigureAwait(false);

                var giveBack = false;
                lock (_gateLock)
                {
                    if (_gateAbsorbed >= _gateDesiredAbsorb)
                    {
                        /* Widened while we waited — this permit is no longer surplus. */
                        _gateAbsorberRunning = false;
                        giveBack = true;
                    }
                    else
                    {
                        _gateAbsorbed++;
                    }
                }

                if (giveBack)
                {
                    gate.Release();
                    return;
                }
            }
        }
        catch (OperationCanceledException)
        {
            /* Shutdown — the gate goes away with the process. */
            lock (_gateLock)
            {
                _gateAbsorberRunning = false;
            }
        }
    }

    /// <summary>
    /// The #2022 backfill tick: at most one Query Store backfill slice per CONNECTED server per
    /// interval, sequentially — sequence IS the fleet-wide concurrency bound, so a fleet of slow
    /// slices stretches the tick instead of stacking connections. Servers are snapshotted under
    /// the reconcile lock and only their Runtime is carried out of it; a server that disconnects
    /// mid-tick fails its slice like any other per-server error and is skipped, not fatal.
    /// Deliberately does NOT touch the per-server CollectionGate: taking it would make backfill
    /// delay live collection (the sweep skips a held server), which inverts the issue's own
    /// constraint — a backfill slice is read-only against the monitored server and writes on its
    /// own store connection, so running beside a live sweep is safe.
    /// </summary>
    private async Task RunQueryStoreBackfillLoopAsync(QueryStoreBackfill backfill, List<ServerLoopState> servers, Func<bool> backfillEnabled, CancellationToken stoppingToken)
    {
        /* #2167: transition-logged so a store-config flip is visible in the log exactly once per state
           change, not once per idle cycle. */
        var lastEnabled = true;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(s_queryStoreBackfillInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            /* #2167: the off switch (config_service.query_store_backfill_enabled, V58) — read live each
               cycle via the store-reload seam, so an operator can stop a runaway drain (a freshly restored
               catalog on a cross-region server) without a restart and without touching plan capture. The
               loop keeps ticking while disabled: a re-enable takes effect on the next cycle. */
            var enabled = backfillEnabled();
            if (enabled != lastEnabled)
            {
                _logger.LogInformation(
                    enabled
                        ? "query_store backfill re-enabled via config — resuming on the next cycle"
                        : "query_store backfill DISABLED via config (config_service.query_store_backfill_enabled) — loop idling, in-flight slices finish and no new ones start");
                lastEnabled = enabled;
            }

            if (!enabled)
            {
                continue;
            }

            List<ServerRuntime> runtimes;
            lock (_serversLock)
            {
                runtimes = servers
                    .Where(s => s.Runtime is not null)
                    .Select(s => s.Runtime!)
                    .ToList();
            }

            foreach (var runtime in runtimes)
            {
                if (stoppingToken.IsCancellationRequested)
                {
                    return;
                }

                /* #2148 parity (review catch on the Lite fix): a slice that WEDGES — not throws — used
                   to hold this foreach forever, stalling backfill for the entire fleet with the
                   exception armor below intact. Per-SERVER abandonable steps, so one wedged server is
                   abandoned (loudly) and quarantined until its task actually dies, while every other
                   server's backfill continues. The deadline is a generous multiple of a healthy slice
                   (statement timeout 60s + store writes), so an abandonment is a defect signal. */
                /* #2165: the other half of the gate. Held for the WHOLE slice — which has to mean PAST an
                   abandonment, because that is the one outcome where the statement is genuinely still running
                   on the server and the tick must keep yielding to it. So the lease is HANDED to the step
                   (holdUntilStepEnds) rather than scoped here: a `using` in this loop body releases at the end
                   of the ITERATION, which opened the gate the instant the deadline handed control back and let
                   the tick start its own Query Store collection beside the still-running slice — the #2165
                   overlap, restored by the one case #2148 exists to survive. The step disposes the lease
                   exactly once on every outcome, the moment its own in-flight guard clears, so the gate and
                   the quarantine now open together. Zero-wait, so a tick already collecting simply defers this
                   server's slice to the next five-minute cycle. */
                var gate = _queryStoreGates.GetOrAdd(runtime.ServerId, static _ => new QueryStoreServerGate()).TryAcquire();
                if (gate is null)
                {
                    _logger.LogInformation(
                        "query_store backfill slice on '{Server}' deferred — the tick's Query Store collection is running (#2165)",
                        runtime.Config.DisplayName);
                    continue;
                }

                var step = _backfillSliceSteps.GetOrAdd(runtime.ServerId, static _ => new AbandonableStep());
                var result = await step.RunAsync(
                    () => backfill.RunServerSliceAsync(runtime, stoppingToken),
                    BackfillSliceDeadline,
                    onLateFault: ex => _logger.LogError(ex,
                        "query_store backfill slice on '{Server}' faulted AFTER being abandoned — this is the wedge's own exception (#2148)",
                        runtime.Config.DisplayName),
                    holdUntilStepEnds: gate,
                    cancellationToken: stoppingToken);

                switch (result.Outcome)
                {
                    case AbandonableStepOutcome.Cancelled:
                        return;
                    case AbandonableStepOutcome.Faulted when result.Exception is OperationCanceledException:
                        return;
                    case AbandonableStepOutcome.Faulted:
                        /* One server's slice failing (unreachable, permissions, a mid-tick disconnect) is
                           that server's problem for this tick; the loop and the rest of the fleet continue. */
                        _logger.LogWarning("query_store backfill slice on '{Server}' failed: {Message}",
                            runtime.Config.DisplayName, result.Exception!.Message);
                        break;
                    case AbandonableStepOutcome.Abandoned:
                        _logger.LogError(
                            "query_store backfill slice on '{Server}' exceeded {Deadline}s and was ABANDONED — " +
                            "the fleet's backfill continues; this server's backfill is quarantined until the " +
                            "wedged task ends. Defect signal: report with this log (#2148).",
                            runtime.Config.DisplayName, (int)BackfillSliceDeadline.TotalSeconds);
                        break;
                    case AbandonableStepOutcome.SkippedStillRunning:
                        /* Defence in depth since the gate started outliving abandonment: the guard is only
                           ever held by a run whose lease has not been released yet, so the acquire above
                           refuses first and this loop no longer reaches here. Kept because it is the honest
                           report if that ever stops being true. */
                        _logger.LogError(
                            "query_store backfill slice on '{Server}' skipped — a previously-abandoned slice is still wedged (#2148).",
                            runtime.Config.DisplayName);
                        break;
                }
            }
        }
    }

    /// <summary>
    /// #2165: per-server gates shared by the tick's Query Store pass and the backfill slice, so the two never
    /// run heavy QS text extraction against one server at the same time. Keyed by ServerId and never pruned,
    /// like its <see cref="_backfillSliceSteps"/> sibling — one small object per server ever monitored.
    ///
    /// <para>Both loops must resolve the SAME gate instance for a server, which is what makes this one
    /// dictionary rather than one per loop. Pinned by a test for that reason.</para>
    /// </summary>
    private readonly ConcurrentDictionary<int, QueryStoreServerGate> _queryStoreGates = new();

    /// <summary>
    /// #2717: one <see cref="DetachedCollectorGate"/> per (server, collector) for every collector fired
    /// detached from <see cref="RunDueCollectorsAsync"/>'s sequential body other than query_store (which
    /// keeps its own <see cref="_queryStoreGates"/> because it has a second, orthogonal job — mutual
    /// exclusion against the separate first-contact backfill loop — that a generic gate does not need to
    /// solve). Keyed by collector name as well as server id so two DIFFERENT detached collectors on the
    /// same server never contend for one slot.
    /// </summary>
    private readonly ConcurrentDictionary<(int ServerId, string CollectorName), DetachedCollectorGate> _detachedCollectorGates = new();

    /// <summary>
    /// #2219: whether this is the PostgreSQL statement-stats collector, whose success is what triggers a text
    /// refresh. Compared against the collector's OWN declared name rather than a literal, so renaming it cannot
    /// silently unhook the text path — the same reasoning as <see cref="IsQueryStoreCollector"/>.
    /// </summary>
    internal static bool IsPgStatementStatsCollector(string collectorName) =>
        string.Equals(collectorName, PgStatementStatsCollector.Instance.Name, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// #2165: whether a dispatched collector name is the Query Store collector the gate covers. Compared
    /// against the collector's OWN declared name rather than a literal, so renaming the collector cannot
    /// silently unhook the gate and let the two loops overlap again.
    /// </summary>
    internal static bool IsQueryStoreCollector(string collectorName) =>
        string.Equals(collectorName, QueryStoreCollector.Instance.Name, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// #2717: whether a dispatched collector name is plan_correction — the second collector detached from
    /// the sequential body for the same bimodal-cost reason query_store was in #2701. Compared against the
    /// collector's OWN declared name rather than a literal, for the same renaming-safety reason as
    /// <see cref="IsQueryStoreCollector"/>.
    /// </summary>
    internal static bool IsPlanCorrectionCollector(string collectorName) =>
        string.Equals(collectorName, PlanCorrectionCollector.Instance.Name, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// #3754: whether a dispatched collector name is <c>long_query_completions</c> — the one collector whose
    /// XE session is reconciled OUTSIDE its run (per sweep, following its enabled flag both ways, #1496), so
    /// the run has to be told what that reconcile could not do: <see cref="ServerLoopState.LongQueryTraceFault"/>
    /// and <see cref="ServerLoopState.LongQueryTracePartialNote"/> are read for this collector alone.
    /// Compared against the collector's OWN declared name rather than a literal, for the same renaming-safety
    /// reason as <see cref="IsQueryStoreCollector"/>.
    /// </summary>
    internal static bool IsLongQueryCompletionsCollector(string collectorName) =>
        string.Equals(collectorName, LongQueryCompletionsCollector.Instance.Name, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// #3604: whether a dispatched collector name is <c>pg_wait_sampling</c> — the third collector detached
    /// from the sequential body, and the first detached for a DELIBERATE run length rather than a bimodal
    /// one. Its sampler arm holds its connection for thirty one-second snapshots per cycle by design; awaited
    /// inline that would delay every other collector on a stock PostgreSQL target by half a minute every five,
    /// which is the #2700 starvation with a known cause. Detached behind the generic per-(server, collector)
    /// gate, a still-running window simply skips the tick — and it cannot still be running, because the run is
    /// 30 s and the cadence is 300 s; the gate is there for the day someone lengthens the window. On the
    /// extension arm the run is a 500-row read and the detach costs nothing. Compared against the collector's
    /// OWN declared name, for the renaming-safety reason the two siblings state.
    /// </summary>
    internal static bool IsPgWaitSamplingCollector(string collectorName) =>
        string.Equals(collectorName, PgWaitSamplingCollector.Instance.Name, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// #2219: refreshes this PostgreSQL server's statement text if it is due, and swallows everything if not.
    ///
    /// <para><b>Best-effort by construction.</b> It runs after the statistics have already been collected and
    /// logged, so nothing here can cost a collection: unreadable text is a degraded read, a lost collection is
    /// lost data, and those are not the same severity. Every fault mode — the target refusing
    /// <c>aurora_stat_statements</c>, a store write failing, the cadence query erroring — logs once and leaves
    /// the statistics intact.</para>
    ///
    /// <para><b>Due-ness is asked of the STORE</b> (<see cref="PgStatementText.IsDueSql"/>), not remembered here.
    /// A restart therefore cannot re-fetch the fleet, and two hosts writing one store cannot disagree about when
    /// text was last written. The same <c>now</c> is used for the decision and the rows it stamps, so the cadence
    /// cannot drift against its own timestamps.</para>
    ///
    /// <para>Only for PostgreSQL targets: <c>aurora_stat_statements</c> does not exist elsewhere, and the
    /// statement-stats collector is already engine-gated, so this mirrors that gate rather than trusting it.</para>
    /// </summary>
    private async Task TryRefreshPgStatementTextAsync(ServerRuntime runtime, CancellationToken cancellationToken)
    {
        if (runtime.Target.Engine != CollectorTargetEngine.PostgreSql)
        {
            return;
        }

        try
        {
            var now = PgStatementText.Naive(DateTime.UtcNow);
            var due = now - PgStatementText.RefreshInterval;

            await using (var isDue = _postgres!.CreateCommand(PgStatementText.IsDueSql))
            {
                isDue.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
                isDue.Parameters.AddWithValue(runtime.ServerId);
                isDue.Parameters.AddWithValue(PgStatementText.Naive(due));
                if (await isDue.ExecuteScalarAsync(cancellationToken) is not true)
                {
                    return;
                }
            }

            var (queryIds, texts) = await ReadPgStatementTextAsync(runtime, cancellationToken);
            if (queryIds.Count == 0)
            {
                return;
            }

            var stamps = new DateTime[queryIds.Count];
            Array.Fill(stamps, now);

            await using var upsert = _postgres!.CreateCommand(PgStatementText.UpsertSql);
            upsert.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
            upsert.Parameters.AddWithValue(Enumerable.Repeat(runtime.ServerId, queryIds.Count).ToArray());
            upsert.Parameters.AddWithValue(queryIds.ToArray());
            upsert.Parameters.AddWithValue(texts.ToArray());
            upsert.Parameters.AddWithValue(stamps);
            await upsert.ExecuteNonQueryAsync(cancellationToken);

            _logger.LogInformation(
                "  [{Server}] pg_statement_text => {Count} statement text(s) refreshed (#2219)",
                runtime.Config.DisplayName, queryIds.Count);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* Deliberately broad — see the summary. The statistics for this cycle are already stored and logged;
               losing their text is not worth failing the sweep over. */
            _logger.LogWarning(
                "  [{Server}] pg_statement_text refresh failed, statistics are unaffected: {Message} (#2219)",
                runtime.Config.DisplayName, ex.Message);
        }
    }

    /// <summary>
    /// Reads <c>(queryid, query)</c> from the monitored PostgreSQL server with <c>showtext = true</c> (#2219).
    /// Capped, and ordered by total execution time so a catalog larger than the cap keeps the text for the
    /// queries anyone would actually look at rather than an arbitrary slice.
    /// <para>#2651: the SOURCE is chosen by flavor. This read was Aurora-only, so off Aurora the text table
    /// was never populated at all — which made get_pg_top_queries return a null query_text on every row
    /// forever, and made test_hypothetical_index (#2612) unable to resolve a statement on the one platform
    /// it can be tested against.</para>
    /// </summary>
    private static async Task<(List<long> QueryIds, List<string> Texts)> ReadPgStatementTextAsync(
        ServerRuntime runtime, CancellationToken cancellationToken)
    {
        var queryIds = new List<long>();
        var texts = new List<string>();

        /* The upsert keys on (server_id, queryid) and a batch carrying one queryid twice does not lose one
           row - PostgreSQL aborts the whole statement with 21000, so the non-duplicate rows are lost with
           it. Both source arms already dedupe in SQL, which is where it belongs: the fetch knows which
           duplicate is the costliest and can order for it. This is the backstop, here because the bug being
           fixed WAS a source arm missing that dedupe - #2651 added the vanilla arm with it and left the
           Aurora arm (#2284) without, and for a week every Aurora server stored no text at all. A third
           source can make the same omission; this is the one place all of them funnel through.

           Keep-first is deliberate rather than arbitrary: both arms rank costliest-first, so the first
           occurrence of a queryid is the row the SQL already chose. */
        var seen = new HashSet<long>();

        await using var connection = new Npgsql.NpgsqlConnection(runtime.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new Npgsql.NpgsqlCommand(
            PgStatementText.FetchSqlFor(runtime.Target.IsAurora, runtime.Target.PostgresMajorVersion),
            connection) { CommandTimeout = 60 };
        command.Parameters.AddWithValue(PgStatementTextRowCap);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.IsDBNull(0) || reader.IsDBNull(1))
            {
                continue;
            }

            var queryId = reader.GetInt64(0);
            if (!seen.Add(queryId))
            {
                continue;
            }

            queryIds.Add(queryId);
            texts.Add(reader.GetString(1));
        }

        return (queryIds, texts);
    }

    /// <summary>#2219: the row cap for one text fetch — comfortably above PostgreSQL's default
    /// <c>pg_stat_statements.max</c> of 5,000, so a normally-configured instance is never truncated, while a
    /// pathologically raised setting cannot turn one fetch into an unbounded transfer.</summary>
    private const int PgStatementTextRowCap = 10_000;

    /// <summary>#2148: per-server abandonment guards for the backfill loop — keyed by ServerId so a
    /// removed-and-re-added server reuses its guard (harmless), and a wedged server never blocks its
    /// neighbors. Never pruned: one small object per server ever monitored, bounded by fleet size.</summary>
    private readonly ConcurrentDictionary<int, AbandonableStep> _backfillSliceSteps = new();

    /// <summary>#2148: the hard ceiling one server's backfill slice may hold the fleet loop — a healthy
    /// slice is one 60s-capped statement plus store writes.</summary>
    private static readonly TimeSpan BackfillSliceDeadline = TimeSpan.FromSeconds(300);

    /// <summary>
    /// The command plane's poll loop (Stage 2), run concurrently with the collection sweep on its own
    /// ~5-second tick. Each tick DRAINS every currently-pending command (claim one at a time until the
    /// queue is empty), so a burst of viewer commands is not throttled to one per 5 seconds. Never throws
    /// out — a per-command failure is reported on the row and swallowed by the executor — so the loop lives
    /// for the service's lifetime. Cancellation ends it cleanly.
    /// </summary>
    private async Task RunCommandLoopAsync(DarlingCommandExecutor executor, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                /* Robustness (Stage 2 follow-up): before draining the queue, reclaim any command left
                   in_progress by a crashed/restarted service instance so a Viewer polling it is not stranded
                   forever. Cheap (an indexed UPDATE matching nothing in the normal case) and safe to run
                   every tick — the 5-minute staleness margin can never catch a merely-slow live command. A
                   reclaimed row is marked terminal 'failed' (not re-queued), so the drain below never re-runs
                   a non-idempotent command; see DarlingCommandExecutor.ReclaimStaleCommandsSql. */
                await executor.ReclaimStaleCommandsAsync(stoppingToken);

                while (await executor.PollOnceAsync(stoppingToken))
                {
                    if (stoppingToken.IsCancellationRequested)
                    {
                        return;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                /* Belt-and-suspenders: PollOnceAsync already swallows its own failures, but a truly
                   unexpected throw must not kill the loop. */
                _logger.LogWarning("Command loop tick failed: {Message}", ex.Message);
            }

            try
            {
                await Task.Delay(s_commandPollInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Applies a control-plane reload at a SAFE point (top of a sweep, never mid-collection): re-reads the
    /// store, hot-swaps the held config's alert/SMTP/webhook/capture/analysis settings IN PLACE (the
    /// by-reference DarlingAlertSettings seam + the runner's capture provider reflect it immediately),
    /// reconciles the monitored-server set, recomputes each connected server's NextDue from the fresh
    /// schedule overrides, and reloads the mute-rule cache (F16). Store-unreachable is a no-op — the current
    /// live config stands, never worse than before.
    ///
    /// <para><b>Returns the <c>config_version</c> that was APPLIED</b>, or null when nothing was, so the
    /// caller can both hold the watermark back on a failure and stamp the version the store actually
    /// served rather than the one its own earlier beacon read saw. The line between applied and not is
    /// <c>LoadViewAsync</c>, and it is the right line because that call is the atomic gate: it reads
    /// all five <c>config</c> rows under one try/catch and returns null if any of them throws, so either
    /// nothing was read or <c>ApplyToConfig</c> below has already swapped the whole view into the live
    /// config — a swap that cannot un-happen and that makes the version genuinely applied.</para>
    ///
    /// <para>Everything after that swap is PROPAGATION, and each piece carries its own isolation rather
    /// than relying on this return value: <c>ReassertComposeStatementTimeoutAsync</c> is non-throwing and
    /// advances <c>_appliedComposeStatementTimeoutSeconds</c> only on success, so it self-heals on the next
    /// reload, and <c>SyncServerEnabledStatesAsync</c> catches everything and logs at Debug. A propagation
    /// step that did throw would leave the watermark behind too — the assignment is after the await — and
    /// the retry re-applies idempotently, so the residual window is "applied in memory, not yet mirrored",
    /// which is narrower than the version-discarding one this replaces rather than a new instance of
    /// it.</para>
    /// </summary>
    private async Task<long?> ReloadFromStoreAsync(
        StoreConfigProvider provider, DarlingConfig config, List<ServerLoopState> servers,
        MuteRuleService muteRuleService, CancellationToken cancellationToken)
    {
        var view = await provider.LoadViewAsync(config, cancellationToken);
        if (view is null)
        {
            return null;
        }

        StoreConfigProvider.ApplyToConfig(config, view);
        /* #1560/#1562: the MCP + web host supervisors pick these up within their poll interval — the viewer's
           Settings toggles round-trip to a live start/stop/rebind with no service restart. */
        _mcpState.Publish(config.Mcp.Enabled, config.Mcp.Port);
        _webState.Publish(config.Web.Enabled, config.Web.Port);

        /* #2918: the compose statement_timeout lives on the ROLES, not in a query, so unlike every other
           knob above it does not go live just by landing in the held config — a reload used to observe the
           new value and leave the roles on whatever the last service start wrote. Re-assert it here, but
           ONLY on a real change: a config_version bump fires on any config_service or schedule write, and
           this is a catalog write. Gated exactly as startup provisioning is (managed + Windows, or the compose
           store this process provisioned, #3914), because that is where these roles are known to exist — any
           other store provisions them out-of-band through tools/provision-roles.sql and names them itself, so
           ALTER ROLE viewer here would be guessing.
           The baseline advances only on SUCCESS, so a failed attempt retries on the next reload rather
           than being recorded as applied. */
        if (_postgres is not null
            && DarlingManagedRoles.ShouldReassertComposeStatementTimeout(
                view.ComposeStatementTimeoutSeconds, _appliedComposeStatementTimeoutSeconds,
                config.Postgres.Managed, OperatingSystem.IsWindows(), _composeStoreRolesProvisioned))
        {
            if (await DarlingManagedRoles.ReassertComposeStatementTimeoutAsync(
                    _postgres, view.ComposeStatementTimeoutSeconds, _logger, cancellationToken))
            {
                _appliedComposeStatementTimeoutSeconds = view.ComposeStatementTimeoutSeconds;
            }
        }

        /* #2298: re-publish the server set on every reload, so a server added through add_servers or the
           Viewer reaches the MCP host's plan-fetch resolver on its next resolution — no MCP restart. */
        _registryState.Publish(view.EnabledServers);
        _scheduleOverrides = view.ScheduleOverrides;
        /* Stage 2: honor a pause/resume issued through the store (config_service.paused) — the collection
           loop reads this on its next tick. Single writer (this reload), so no interlock needed. */
        _paused = view.Paused;

        /* Structural reconcile mutates the server list; the command loop reads it concurrently, so hold
           the lock across the add/remove. NextDue recompute mutates only per-server state (safe against a
           concurrent id lookup) so it stays outside the lock. */
        lock (_serversLock)
        {
            ReconcileServers(servers, view.EnabledServers);
        }

        await RecomputeNextDueAsync(servers, cancellationToken);

        /* Mirror the desired enable state onto the observed collect.servers registry so a disable_server
           flips its observed row FALSE even though the disabled server drops out of the loop (stops
           upserting), and an enable_server flips it back. */
        await DarlingObservability.SyncServerEnabledStatesAsync(_postgres!, _logger, cancellationToken);

        await LoadMuteRulesAsync(muteRuleService);

        /* view.ConfigVersion rather than _lastConfigVersion: the caller has not advanced the watermark yet
           (it advances from this method's RETURN value), so the field still holds the PREVIOUS version
           here — and the returned version is this same one, so the log line and the watermark can never
           disagree about what was applied. */
        _logger.LogInformation(
            "Control-plane reload applied (config_version {Version}, {Servers} monitored server(s), paused: {Paused})",
            view.ConfigVersion, servers.Count, _paused);

        return view.ConfigVersion;
    }

    /// <summary>
    /// The ONLY route from this service to <see cref="MuteRuleService.LoadAsync"/> — startup and every
    /// control-plane reload both come through here, so neither path can be guarded while the other is not.
    ///
    /// <para>The service keeps the rules it already read when the store read throws, so the operator's
    /// suppression survives a store blip: that guarantee lives in <c>LoadAsync</c> and holds whatever this
    /// method does. What this method owns is the reporting. A failed reload leaves a cache that is CORRECT
    /// and STALE, and the only way to tell that apart from a cache that is correct and current is an
    /// artefact of the failure — so the event is logged with the number of rules left standing, and counted
    /// on #3013's swallowed-alerting-read surface with a null server key, because
    /// <c>config_mute_rules</c> belongs to the store rather than to any monitored server.</para>
    ///
    /// <para>Counted rather than exempt: this is a store read, on the alert pass's own command deadline,
    /// whose failure changes what the alert engine does on its next sweep. That is the population #3013
    /// measures, and it is the one member of it whose failure makes the fleet report MORE health rather
    /// than less.</para>
    ///
    /// <para>Swallowed here, and it has to be: an unhandled throw at either site takes down a service whose
    /// collection is otherwise healthy, and losing the fleet's monitoring is a larger error than running on
    /// a stale mute set. The elapsed is the store read's alone — <c>LoadAsync</c>'s expiry purge runs only
    /// after a read that succeeded, and swallows its own write failures.</para>
    /// </summary>
    private async Task LoadMuteRulesAsync(MuteRuleService muteRuleService)
    {
        var readClock = Stopwatch.StartNew();
        try
        {
            await muteRuleService.LoadAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Could not reload mute rules after {ElapsedMs} ms — the {Rules} rule(s) already in force "
                + "stay in force until a read succeeds: {Message}",
                readClock.ElapsedMilliseconds, muteRuleService.GetRules().Count, ex.Message);
            _readFailures.RecordReadFailure(null, "mute-rule reload", readClock.ElapsedMilliseconds);
        }
    }

    /// <summary>
    /// Reconciles the live <see cref="ServerLoopState"/> set to the store's desired enabled set, keyed by the
    /// shared server_id. ADD: a new enabled server gets a disconnected state that connects on its next tick
    /// exactly like a startup server (via <see cref="TryConnectAsync"/> — no novel connection logic). REMOVE:
    /// a server no longer enabled/present is dropped; its runtime holds no persistent connection (the
    /// collectors open per run), so dropping the state is a clean disconnect. STAY: the held config is always
    /// refreshed to the desired definition (so a non-connection edit — cost, the #1236 alert-delivery override
    /// — goes live without churn), but the connection + NextDue are preserved unless a connection/collection
    /// field changed (per <see cref="ServerDefinitionEquals"/>), in which case the runtime is dropped so it
    /// reconnects with the new definition through the same startup path.
    /// </summary>
    private void ReconcileServers(List<ServerLoopState> servers, IReadOnlyList<MonitoredServer> desired)
    {
        var desiredById = new Dictionary<int, MonitoredServer>();
        foreach (var d in desired)
        {
            desiredById[d.ServerId] = d;
        }

        for (int i = servers.Count - 1; i >= 0; i--)
        {
            var state = servers[i];
            var id = state.Config.ServerId;
            if (!desiredById.TryGetValue(id, out var desiredServer))
            {
                _logger.LogInformation(
                    "[{Server}] Removed from the monitored set (disabled/deleted) — stopping collection",
                    state.Config.DisplayName);
                state.Runtime = null;
                /* Retired containment (#1553 D1): mark this state retired so any in-flight or gate-queued
                   fire-and-track body for it no-ops at its entry check (and its connect path bails before any
                   durable side-effect) instead of connecting / upserting / running XE DDL against a just-disabled
                   target, or re-writing self-alert edge state after the Forget below. Write-once; a re-add mints
                   a fresh ServerLoopState, so there is no reset and no ABA. */
                state.Retired = true;
                /* Drop the Stage 4 self-alert edge state so a later re-add starts from the Unknown baseline
                   (no stale "was online" / "was stopped" flag carried across a remove+re-add). */
                _selfAlerts?.Forget(id);
                /* And its delta baselines + pass window (#3540 A4), for the same reason: a re-add mints a fresh
                   ServerLoopState, and its first pass must be a first pass, not a subtraction from whatever the
                   removed server's counters were. An in-flight body for the retired state can still make delta
                   calls after this and re-populate the server's cache for one pass; that is the same window the
                   Forget above tolerates, and a re-add inside it is the A5 epoch question, not this one. */
                _deltas?.ClearServer(id);
                servers.RemoveAt(i);
                continue;
            }

            /* Always refresh the held definition so a NON-connection edit — the FinOps cost, or the #1236
               alert-delivery override the deliverer's resolver reads live off state.Config — takes effect on
               this reload without connection churn. Only a connection/collection-affecting change (per
               ServerDefinitionEquals) drops the runtime to reconnect through the startup path. */
            var connectionChanged = !ServerDefinitionEquals(state.Config, desiredServer);
            state.Config = desiredServer;
            if (connectionChanged)
            {
                _logger.LogInformation(
                    "[{Server}] Definition changed — reconnecting with the new configuration; delta baselines forgotten, "
                    + "the first pass on the new connection re-baselines (#3653 A5)", desiredServer.DisplayName);
                state.Runtime = null;
                state.NextConnectAttempt = DateTime.MinValue;
                state.NextDue.Clear();
                /* #3653 A5 (the adjacency #3540 A4 named and left): a same-id reconnect is a new epoch. The
                   fields ServerDefinitionEquals compares are the ones that decide WHICH instance the
                   connection reaches (host, database, auth, intent, subnet failover) or WHAT it collects
                   (excluded databases) — so a change here means the counters the next pass reads may be a
                   different instance's while the server_id, and every baseline cached under it, stays the
                   same. Left cached, the first pass after the reconnect would store one interval of
                   `new instance's total minus old instance's baseline` per key — the positive storm when the
                   new host is the busier one, and the reset branch's (0, 0) only when it is quieter. The
                   remove branch above already forgets for a remove+re-add; this is the same forget for the
                   edit that never removes. The cost is one re-baseline pass on every family after an operator
                   edits a connection field, which is the honest price of not knowing whether the host behind
                   the id changed. The persisted identity pair (collector_state, cpu_utilization) is left for
                   the carrier to compare on its next run: if the new connection reaches the same instance the
                   pair matches and nothing more happens; if it reaches a different one the carrier logs the
                   old/new pair and forgets again — and because the carrier runs after five delta families in
                   the schedule order, that second forget costs those five one more re-baseline pass. Paid once,
                   when an operator points a registration at a different instance, and named here rather than
                   avoided: avoiding it means a store write on this reload path to erase the persisted pair. */
                _deltas?.ClearServer(id);
            }

            desiredById.Remove(id);
        }

        foreach (var addition in desiredById.Values)
        {
            _logger.LogInformation(
                "[{Server}] Added to the monitored set — will connect on the next sweep", addition.DisplayName);
            servers.Add(new ServerLoopState { Config = addition });
        }
    }

    /// <summary>
    /// Recomputes each CONNECTED server's per-collector NextDue from the current schedule overrides after a
    /// reload: a disabled collector is dropped from the schedule; a newly-enabled one (an on-load collector
    /// included — #3929/#3930 give it <see cref="CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes"/>
    /// rather than dropping it) is seeded from its persisted watermark (the #1575
    /// <see cref="ComputeSeededNextDue"/> policy, one lazily batched read per server that gains a new entry); an
    /// existing entry is pulled in to at most now + the (possibly shortened) effective interval so a frequency
    /// change takes effect promptly without over-firing. A server still connecting has no NextDue yet —
    /// <see cref="TryConnectAsync"/> seeds it from the same watermark policy when it connects.
    /// </summary>
    private async Task RecomputeNextDueAsync(List<ServerLoopState> servers, CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        foreach (var server in servers)
        {
            var runtime = server.Runtime;
            if (runtime is null)
            {
                continue;
            }

            /* #1575: the watermark map is read at most ONCE per server, and only if this reload actually
               introduces a NEW collector entry (a just-enabled collector) — a reload that only tweaks existing
               entries costs no extra store round-trip. Lazily populated on the first new entry below. */
            Dictionary<string, DateTime>? watermarks = null;

            foreach (var name in CollectorScheduleDefaults.All.Keys)
            {
                var effective = StoreConfigProvider.ResolveSchedule(name, runtime.ServerId, _scheduleOverrides);
                if (!effective.Enabled)
                {
                    /* ConcurrentDictionary has no Remove(key) — TryRemove is the drop-in for the old Remove. */
                    server.NextDue.TryRemove(name, out _);
                    continue;
                }

                /* #3929/#3930: an on-load collector (frequency 0) is no longer dropped from the schedule here —
                   it ALSO reruns on OnLoadRecaptureMinutes, exactly like the connect-time seed and the
                   due-collector sweep below. */
                var interval = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(effective.FrequencyMinutes);

                if (server.NextDue.TryGetValue(name, out var existing))
                {
                    /* Existing entry KEEPS its already-applied phase, but is pulled in to at most now + the
                       (possibly shortened) interval so a frequency change takes effect promptly without
                       over-firing — unchanged from before. */
                    var capped = now.AddMinutes(interval);
                    server.NextDue[name] = existing < capped ? existing : capped;
                }
                else
                {
                    /* NEW entry — a collector this reload newly enables. Seed it from the persisted watermark
                       (the same #1575 policy as the connect-seed) so a newly-enabled long-frequency collector
                       resumes its real cadence instead of deferring up to a full interval; the small capped
                       jitter still de-clusters an overdue / never-run fleet-wide enable. */
                    watermarks ??= await ReadCollectorWatermarksAsync(_postgres!, runtime.ServerId, _logger, cancellationToken);
                    var lastRun = watermarks.TryGetValue(name, out var w) ? w : (DateTime?)null;
                    var jitter = SeedJitter(runtime.ServerId, interval * 60);
                    server.NextDue[name] = ComputeSeededNextDue(lastRun, interval, now, jitter);
                }
            }
        }
    }

    /// <summary>
    /// Whether two server definitions are identical for the collection loop — the connection-relevant fields
    /// plus the collection-affecting excluded databases. A difference triggers a reconnect on reconcile so the
    /// new definition takes effect. <c>MonthlyCostUsd</c> is deliberately NOT compared: it does not affect
    /// collection at all, and the reload's <see cref="DarlingObservability.SyncServerEnabledStatesAsync"/>
    /// mirrors a cost change straight onto <c>collect.servers</c> (which the FinOps display reads) with no
    /// connection churn — so a cost-only edit must NOT trigger a disconnect+reconnect. Internal so a unit test
    /// can pin exactly that.
    /// </summary>
    internal static bool ServerDefinitionEquals(MonitoredServer a, MonitoredServer b)
        => string.Equals(a.Name, b.Name, StringComparison.Ordinal)
        && string.Equals(a.Host, b.Host, StringComparison.OrdinalIgnoreCase)
        && string.Equals(a.Database, b.Database, StringComparison.OrdinalIgnoreCase)
        && string.Equals(a.Auth, b.Auth, StringComparison.OrdinalIgnoreCase)
        && string.Equals(a.Username, b.Username, StringComparison.Ordinal)
        && string.Equals(a.EncryptedPassword, b.EncryptedPassword, StringComparison.Ordinal)
        && string.Equals(a.Password, b.Password, StringComparison.Ordinal)
        && string.Equals(a.EncryptMode, b.EncryptMode, StringComparison.OrdinalIgnoreCase)
        && a.TrustServerCertificate == b.TrustServerCertificate
        && a.ReadOnlyIntent == b.ReadOnlyIntent
        && a.MultiSubnetFailover == b.MultiSubnetFailover
        && a.ExcludedDatabases.SequenceEqual(b.ExcludedDatabases, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// A deterministic, restart-stable per-server phase offset within a cadence period (#1553 cadence jitter),
    /// used to break the fleet-wide lockstep at cadence boundaries — the field incident re-herded every server
    /// at once, so at each boundary all collectors fired together. The <paramref name="serverId"/> is
    /// <see cref="MonitoredServer.ServerId"/>, which today is an FNV-1a hash
    /// (<see cref="ServerIdHelper.GetDeterministicHashCode"/>) — so a plain modulo spreads it across
    /// <c>[0, period)</c> without any further mixing (an extra multiply was reviewed out as unnecessary — the
    /// input is already avalanched). This is the ONE consumer that wants the value only as a spreading
    /// function rather than as an identity, so if #2218 ever makes ids sequential the extra mixing that was
    /// reviewed out has to come back here: consecutive integers modulo a period do not spread, they line up.
    /// Restart-stable because it is a pure function of the id — no <see cref="Random"/>.
    /// A non-positive period yields no offset (guards the callers where a period could in principle be zero, and
    /// keeps the result well-defined for tests). Applied ONLY at initial cadence stamps, never the steady-state
    /// advance: directly for the on-connect analysis stamp, and — capped at min(interval, 150s) via
    /// <see cref="SeedJitter"/> — as the small de-cluster jitter <see cref="ComputeSeededNextDue"/> adds to an
    /// overdue or never-run collector seed (#1575). Internal so a unit test can pin its shape.
    /// </summary>
    internal static TimeSpan CadencePhaseOffset(int serverId, int periodSeconds)
    {
        if (periodSeconds <= 0)
        {
            return TimeSpan.Zero;
        }

        /* Cast to uint first so a negative FNV hash still maps into [0, period): a signed modulo would yield a
           negative offset and pull the due time into the past. */
        return TimeSpan.FromSeconds((uint)serverId % periodSeconds);
    }

    /// <summary>
    /// The small, bounded per-server seed jitter (#1575): the deterministic <see cref="CadencePhaseOffset"/>
    /// phase, but CAPPED at <c>min(frequencySeconds, 150)</c> so it de-clusters the fleet's overdue / never-run
    /// seeds WITHOUT ever deferring a run by up to a full interval — the coarse full-interval offset applied at
    /// the seed sites was the #1575 starvation bug (a daily collector re-phased up to ~24h forward on every
    /// restart). Mirrors the fixed 150s post-connect analysis-phase jitter. A recently-run collector needs no
    /// jitter at all: its <c>lastRun + interval</c> stamps are already spread across the fleet by when each
    /// server actually last ran. Internal so a unit test can pin the cap.
    /// </summary>
    internal static TimeSpan SeedJitter(int serverId, int frequencySeconds)
        => CadencePhaseOffset(serverId, Math.Min(frequencySeconds, 150));

    /// <summary>
    /// The #1581 cold-start launch time for one server's FIRST post-startup sweep body: the captured
    /// <paramref name="coldStartInstant"/> plus a deterministic per-server <see cref="CadencePhaseOffset"/> capped
    /// at <see cref="ColdStartSpreadSeconds"/>. Spreads the fleet's initial catch-up across that small window so a
    /// service restart does not launch all N servers' first bodies in ONE sweep tick and slam the N=4 gate — the
    /// field herd where the queued bodies crossed 60s and logged "collection body has not completed after 60s" en
    /// masse (nothing broken, but 366 spurious warnings over ~10 min). Distinct from the per-collector #1575 seed
    /// jitter (<see cref="SeedJitter"/> / <see cref="ComputeSeededNextDue"/>), which staggers WHICH collectors are
    /// due once a body runs but not WHEN the heavyweight connect body itself launches — so cold start still
    /// herded. Pure and restart-stable (a function of the id, no <see cref="Random"/>) so a unit test can pin the
    /// spread + bound; applied ONLY to the initial fleet's first launch, never the steady-state advance. Internal
    /// so a unit test can pin its shape.
    /// </summary>
    internal static DateTime ColdStartFirstSweepDue(DateTime coldStartInstant, int serverId)
        => coldStartInstant.Add(CadencePhaseOffset(serverId, ColdStartSpreadSeconds));

    /// <summary>
    /// The pure #1575 seed policy for one collector's first post-connect / newly-enabled due time, decided from
    /// the persisted last-run watermark instead of a full-interval offset. The old seed stamped
    /// <c>now + CadencePhaseOffset(serverId, frequencySeconds)</c> — up to a FULL interval — on EVERY connect,
    /// discarding when the collector actually last ran, so each service restart re-phased every collector up to a
    /// full interval forward and long-frequency collectors (the daily <c>index_object_stats</c>) were starved
    /// across a restart-heavy window. From the collector's last run:
    /// <list type="bullet">
    /// <item>recently run (<c>lastRun + interval</c> still in the future) → wait out the REMAINING interval
    /// (<c>lastRun + interval</c>), resuming the real cadence;</item>
    /// <item>overdue (<c>lastRun + interval</c> already reached) → run promptly at <c>now + jitter</c>;</item>
    /// <item>never run (no watermark) → run promptly at <c>now + jitter</c> (a fresh daily collector runs shortly
    /// after first connect, then daily — not up to 24h later).</item>
    /// </list>
    /// The steady-state advance in <see cref="RunDueCollectorsAsync"/> (exact interval) is unchanged. Pure and
    /// Kind-agnostic (compares by ticks; the caller passes matching UTC values) so the policy is unit-tested
    /// without a live store or a connect. Internal so a unit test can pin the decision table.
    /// </summary>
    internal static DateTime ComputeSeededNextDue(DateTime? lastRunUtc, int frequencyMinutes, DateTime nowUtc, TimeSpan jitter)
    {
        if (lastRunUtc is DateTime lastRun)
        {
            var due = lastRun.AddMinutes(frequencyMinutes);
            return due <= nowUtc ? nowUtc + jitter : due;
        }

        return nowUtc + jitter;
    }

    /// <summary>
    /// One batched round-trip (#1575): the newest <c>collection_time</c> per collector for a server, keyed by
    /// collector_name — the persisted last-run watermark <see cref="ComputeSeededNextDue"/> seeds the schedule
    /// from on connect and on a newly-enabled collector, so a restart resumes the real cadence instead of
    /// re-phasing it forward. ANY status counts (a failed attempt still wrote a row and still reset the cadence
    /// clock, exactly as the steady-state advance retries every interval regardless of outcome). Bare
    /// <c>collection_log</c> resolves through the store connection's collect/config search path, matching the
    /// sibling readers (<c>DarlingSelfAlertEvaluator.ReadCollectionSignalsAsync</c>). The naive-UTC
    /// <c>timestamp</c> value is relabeled Kind=Utc (a relabel, NOT a shift) so every NextDue entry stays
    /// uniformly Kind=Utc. Failure-isolated: a store hiccup returns an EMPTY map so the caller seeds every
    /// collector as never-run (a prompt, jittered run) rather than aborting the connect — an observability read
    /// must never break the collection loop. Internal so a gated live test can seed a row and assert the read.
    /// </summary>
    internal static async Task<Dictionary<string, DateTime>> ReadCollectorWatermarksAsync(
        NpgsqlDataSource postgres, int serverId, ILogger? logger, CancellationToken cancellationToken)
    {
        var watermarks = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(
                "SELECT collector_name, MAX(collection_time) FROM collection_log WHERE server_id = $1 GROUP BY collector_name", connection);
            command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
            command.Parameters.AddWithValue(serverId);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (reader.IsDBNull(1))
                {
                    continue;
                }

                watermarks[reader.GetString(0)] = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Failure-isolated: fall back to never-run (prompt, jittered) seeds — an observability read must
               never break the connect / reload path. */
            logger?.LogDebug(
                "Observability: collector watermark read for server_id {ServerId} failed: {Message}", serverId, ex.Message);
        }

        return watermarks;
    }

    /// <summary>
    /// Ensures the store connection string carries the collect/config search path (the V8 schema
    /// split) so every pooled physical connection resolves the shared SQL's bare table names to the
    /// <c>collect</c>/<c>config</c> schemas from its FIRST use. Managed mode already sets it
    /// (<see cref="DarlingManagedPostgres.SearchPath"/>) — that string is returned unchanged, no
    /// double-set. A bring-your-own connection string usually omits it and would otherwise rely on
    /// the database-default <c>search_path</c> that
    /// <see cref="PgMigrations.MigrateAsync(NpgsqlConnection, ILogger, CancellationToken)"/>
    /// best-effort sets via <c>ALTER DATABASE ... SET search_path</c>.
    ///
    /// <para>That database default only governs the startup search_path of connections established
    /// AFTER the ALTER commits, but the Npgsql pool's physical connections opened around it (for the
    /// migration itself, the hypertable conversion, and the first collection sweep) keep their
    /// pre-ALTER session search_path for their entire lifetime. On a FRESH BYO store that means the
    /// whole first run fails — hypertable conversion, delta seeding, and every collector write hit
    /// <c>42P01: relation "wait_stats" does not exist</c> — and collection only starts working after
    /// a service restart hands out a fresh pool. Carrying the search path on the connection string
    /// itself is deterministic and pool-timing-independent; any login may <c>SET</c> its own
    /// search_path, so this is safe for least-privilege BYO logins too.</para>
    /// </summary>
    internal static string EnsureStoreSearchPath(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        if (string.IsNullOrWhiteSpace(builder.SearchPath))
        {
            /* Append, don't rewrite through the builder: round-1 review on #4285's PR found the builder's
               writer erases a caller keyword set to an explicit empty value (Password='', ...), because it
               emits that as a bare Key=, which its own reader treats as "not set". Appending keeps every
               other keyword byte for byte. The constant below holds no ';', '=' or quote character, so it
               never needs quoting — a plain "Search Path=<value>" is unambiguous.
               DarlingManagedPostgres is annotated [SupportedOSPlatform("windows")] for its DPAPI /
               bundled-cluster surface, but SearchPath is a platform-neutral compile-time constant
               with no runtime dependency, and BYO mode runs on any OS — so the CA1416 cross-platform
               reachability flag is spurious for this const reference. Suppressed narrowly rather than
               forking the constant, keeping managed and BYO byte-identical (a test pins them equal). */
#pragma warning disable CA1416
            return connectionString + ";Search Path=" + DarlingManagedPostgres.SearchPath;
#pragma warning restore CA1416
        }

        return connectionString;
    }

    /// <summary>
    /// Assembles the Phase-5 shared alert engine over Darling's third-party implementations:
    /// darling.json thresholds/SMTP/webhooks (<see cref="DarlingAlertSettings"/>), the Postgres
    /// collected feeds (<see cref="DarlingAlertReadAdapter"/>), the V3 PG watermark/history/mute
    /// stores, Lite-cadence delivery (<see cref="DarlingAlertDeliverer"/> → the shared
    /// EmailSendCore + WebhookAlertService), a live-msdb failed-jobs fetcher on the monitored
    /// server's own connection, and a resolution hook that logs recovered conditions (the
    /// headless stand-in for Lite's tray "Cleared" toasts).
    /// </summary>
    private AlertEngine BuildAlertEngine(
        DarlingConfig config, List<ServerLoopState> servers,
        DarlingAlertSettings alertSettings, PgAlertHistoryStore historyStore,
        MuteRuleService muteRuleService, DarlingAlertDeliverer deliverer)
    {
        var postgres = _postgres!;
        var stateStore = new PgAlertStateStore(postgres, _logger);

        /* The mute service is loaded + owned by the caller (RunCollectionLoopAsync) so a control-plane
           reload can re-LoadAsync() the SAME instance and mute the very next sweep (F16). The engine
           binds its IsAlertMuted delegate, which reads the refreshed cache. The deliverer is hoisted by
           the caller and shared with the Stage 4 self-alerts (same delivery/cooldown/restart-replay). */

        return new AlertEngine(
            alertSettings,
            /* #1812: the adapter's snapshot-freshness bound needs the server's EFFECTIVE running_jobs
               cadence — the same resolution the sweep schedules by, reading the live overrides field so
               a control-plane reload reaches the very next check. */
            new DarlingAlertReadAdapter(
                postgres,
                serverId => StoreConfigProvider.ResolveSchedule("running_jobs", serverId, _scheduleOverrides).FrequencyMinutes,
                /* #1839: the same resolution for the blocking snapshot the total-wait gate reads. */
                serverId => StoreConfigProvider.ResolveSchedule("dmv_blocking_snapshot", serverId, _scheduleOverrides).FrequencyMinutes,
                /* #3848: the same process counter the engine takes below, so a read RETRIED inside the
                   adapter and the same read FAILING in the engine's catch arm land in one bucket under one
                   name. Passed explicitly for the same reason the engine's is — a test builds its own. */
                readFailures: AlertReadFailureCounter.Shared),
            stateStore,
            deliverer,
            muteRuleService.IsAlertMuted,
            failedJobsFetcher: (serverKey, lookbackMinutes, ct) =>
                FetchFailedJobsAsync(servers, serverKey, lookbackMinutes, ct),
            /* #3497: the Long-Running Query card's Agent-job name lookup — the same live-msdb seam shape
               as the failed-jobs feed above, and the same degrade discipline: every failure is an empty
               map, so a denied or broken msdb read costs a card its job NAME and never the card. */
            agentJobStepResolver: (serverKey, keys, ct) =>
                FetchAgentJobStepNamesAsync(servers, serverKey, keys, ct),
            resolutionCallback: async (resolution, _) =>
            {
                /* #1681: same shared shape as the firing line the engine's funnel writes, so an engine
                   alert's TRIGGERED and RESOLVED halves pair up in the log. */
                _logger.LogInformation("{Line}",
                    AlertFiringLog.Resolved(resolution.ServerName, resolution.Title, resolution.Message));
                /* Stage 4 parity-gap fix: record a resolved-flavored history row so an operator reviewing
                   alert history sees the paired "Detected" then "Cleared/Resolved" entries (the Dashboard
                   records these explicitly; Darling previously only logged them). A resolution has no send
                   channel, so this never emails/webhooks; RecordAlertAsync is failure-isolated so it can
                   never break the sweep. */
                await historyStore.RecordAlertAsync(DarlingSelfAlertEvaluator.BuildResolutionRecord(resolution));
            },
            logger: _logger,
            /* #3013: the process counter every swallowed condition read is tallied on. Passed explicitly
               rather than defaulted inside the engine so a test constructs its own and cannot pollute it. */
            readFailures: AlertReadFailureCounter.Shared);
    }

    /// <summary>
    /// Builds this sweep's <see cref="AlertServerSnapshot"/> and runs the engine for one
    /// connected server. The CPU pair mirrors what Lite's overview summary carries (the latest
    /// cpu_utilization_stats sample; total = SQL + other-process, null when no SQL sample);
    /// isOnline is true by definition here (a connected runtime) and suppression is always false
    /// (headless — suppression is an engine INPUT owned by interactive hosts). Failure-isolated:
    /// a failed sweep logs and retries on the next cadence tick, mirroring the collector loop.
    /// </summary>
    private async Task EvaluateAlertsAsync(
        AlertEngine engine, ServerLoopState server, DarlingConfig config, CancellationToken cancellationToken)
    {
        var runtime = server.Runtime;
        if (runtime is null)
        {
            return;
        }

        /* #3013: the latest-CPU read is isolated from the sweep it feeds, for TWO reasons that point the
           same way.

           Correctness of the instrument: this read runs on AlertPassCommandTimeoutSeconds and is the
           first store read of the pass, so under the contention #3013 measures it is the first to fail.
           Inside the sweep's try it took engine.EvaluateServerAsync down with it, which meant
           AlertEngine.EvaluateCoreAsync never ran and never recorded its pass - while the catch below
           still recorded a read failure. Numerator up, denominator unchanged, worst exactly when the
           counter matters most. That is a third route to the same defect the PostgreSQL predictor group
           had: not omission and not placement, but REACHABILITY - a pass site that is real and
           correctly placed and simply never entered.

           Correctness of the ALERTING, which is the bigger half: a single failed CPU read aborted the
           whole shared sweep for this server this tick, so blocking, deadlocks, poison waits,
           long-running queries, TempDB, low disk, PVS, file growth, jobs, database state and forced
           plans were none of them evaluated. The snapshot already documents a null CPU pair as a normal
           input ("null when no SQL sample") and CheckCpuAsync gates on alertCpuValue.HasValue, so
           degrading to (null, null) costs this tick its CPU alert and nothing else. */
        double? sqlCpu = null;
        double? totalCpu = null;
        DateTime? cpuSampleTime = null;

        var cpuReadClock = Stopwatch.StartNew();
        try
        {
            (sqlCpu, totalCpu, cpuSampleTime) = await ReadLatestCpuAsync(runtime.ServerId, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Server}] Latest-CPU read for the alert pass failed after {ElapsedMs} ms: {Message}",
                server.Config.DisplayName, cpuReadClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(
                runtime.ServerId.ToString(CultureInfo.InvariantCulture), LatestCpuReadName, cpuReadClock.ElapsedMilliseconds);
        }

        var sweepReadClock = Stopwatch.StartNew();
        try
        {
            var snapshot = new AlertServerSnapshot(
                runtime.ServerId.ToString(CultureInfo.InvariantCulture),
                runtime.Config.DisplayName,
                IsOnline: true,
                SqlCpuPercent: sqlCpu,
                TotalCpuPercent: totalCpu,
                IsAzureSqlDb: runtime.Target.IsAzureSqlDb,
                Suppressed: false,
                /* #3282: the gate counts breaching SAMPLES, not sweeps — the sweep runs every 30 s and this
                   sample advances about once a minute, so without the instant a re-read would count twice.
                   #3744: the row's UTC twin where the store has one, the local stamp before V134 — resolved
                   in ReadLatestCpuAsync, compared for equality by the gate. */
                CpuSampleTimeUtc: cpuSampleTime);

            await engine.EvaluateServerAsync(snapshot, cancellationToken);
            sweepReadClock.Restart();

            /* PostgreSQL predictors ride alongside rather than inside the shared engine — see
               IPostgresAlertReadAdapter for why the read contract is separate. Gated on the probed engine,
               so a SQL Server target does not pay for a read it can never satisfy, and Lite never sees any
               of it. Awaited AFTER the shared sweep so an existing SQL Server alert is never delayed by a
               PostgreSQL read. */
            if (runtime.Target.Engine == CollectorTargetEngine.PostgreSql)
            {
                await EvaluatePostgresAlertsAsync(runtime, snapshot, config, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Server}] Alert sweep failed after {ElapsedMs} ms: {Message}", server.Config.DisplayName, sweepReadClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(
                runtime.ServerId.ToString(CultureInfo.InvariantCulture),
                "shared engine sweep", sweepReadClock.ElapsedMilliseconds);
        }
    }

    /// <summary>
    /// Evaluates the three PostgreSQL Tier 0 outage predictors and delivers whatever fired.
    /// <para>Failure-isolated from the shared sweep on purpose: these are additive signals, and a broken
    /// PostgreSQL read must not cost a server its CPU or blocking alerts. Recording and mute handling stay
    /// with the deliverer, exactly as for an engine-emitted alert, so a PostgreSQL alert lands in the same
    /// history and obeys the same mute rules as every other one. Gated on the master alerts switch before
    /// anything is read or counted, exactly as the engine sweep and the self-alerts are (#3464).</para>
    /// </summary>
    private async Task EvaluatePostgresAlertsAsync(
        ServerRuntime runtime, AlertServerSnapshot snapshot, DarlingConfig config, CancellationToken cancellationToken)
    {
        if (_postgres is null || _alertDeliverer is null)
        {
            return;
        }

        /* #3464: the master-switch gate this path never had. An earlier version of the #3013 comment
           below named the gap and deliberately left it open as "a question about whether the Tier 0
           predictors are deliberately exempt from the switch"; the answer arrived as an incident. The
           switch's own contract (Darling/README.md, the alerts section) says `enabled: false` turns off
           ALL alert evaluation, and the same document is what an operator silencing a fleet is acting on —
           two OTHER families measured delivering through a fleet-wide mute are the subject of #3464, and
           this path had the identical shape: no consult anywhere between the sweep and _alertDeliverer.
           Master-off now means for the predictors what it means for the engine sweep: no reads, no
           evaluation, no history rows, cooldown state frozen where it stands. Before RecordPass, so the
           denominator stays truthful — a pass that never ran is not counted, which is the SAME rule the
           engine (records after its early return) and EvaluateStoreAlertsAsync (returns before recording)
           already follow. */
        if (!config.Alerts.Enabled)
        {
            return;
        }

        /* #3013: the PostgreSQL predictor group is a THIRD alert evaluation pass, and it has to say so
           or a PostgreSQL target's denominator reports two passes for three. One pass for the whole
           group, not one per check: the six checks below are independently failure-isolated exactly as
           AlertEngine's fourteen Check*Async calls are, and those fourteen are one pass. Isolation
           granularity is not pass granularity. Recorded after the guards above because a pass that cannot
           reach the store — or that the master switch stopped before it ran (#3464) — is not one; that is
           parity with both sibling sites, which this comment once had to disclaim. */
        _readFailures.RecordPass(snapshot.ServerKey);

        var readClock = Stopwatch.StartNew();
        try
        {
            var adapter = new DarlingPostgresAlertReadAdapter(_postgres);

            /* The three feed reads are hoisted into locals rather than awaited inside the Evaluate
               argument list, so each one gets the clock to itself. As arguments they were three
               sequentially awaited reads inside ONE statement, and a client-side cutoff of the third
               reported the sum of all three — a figure above the per-read deadline, which is a reading
               the elapsed has no bucket for. Same evaluation order; the argument list is unchanged. */
            var wraparound = await adapter.GetWraparoundRiskAsync(runtime.ServerId, cancellationToken);
            readClock.Restart();
            var xmin = await adapter.GetXminHorizonAsync(runtime.ServerId, cancellationToken);
            readClock.Restart();
            var slots = await adapter.GetReplicationSlotRiskAsync(runtime.ServerId, cancellationToken);
            readClock.Restart();

            var findings = PostgresAlertEvaluator.Evaluate(wraparound, xmin, slots);

            var now = DateTime.UtcNow;
            var cooldown = TimeSpan.FromMinutes(Math.Max(1, _alertCooldownMinutes));

            foreach (var finding in findings)
            {
                /* Cooldown keyed per SUBJECT, not per metric. Two databases past the wraparound line are two
                   incidents; a metric-level key would have let the first one's stamp suppress the second. */
                var cooldownKey = string.Create(
                    CultureInfo.InvariantCulture,
                    $"{snapshot.ServerKey}|{finding.MetricName}|{finding.Subject}");

                /* #2716: this cooldown has no in-memory entry for a subject this process has never
                   evaluated, which includes every subject after a restart even if it was alerted on
                   moments before. Seed it ONCE per key from history before trusting its absence —
                   finding.Subject IS the #1140 dedup fingerprint this alert already fires with, so
                   GetLastAlertTimeAsync's existing #1154 filter reconstructs exactly the per-subject
                   time this cooldown needs, no new schema required. Guarded by
                   _postgresAlertHistorySeeded so a subject that has never alerted (the common case —
                   most databases never cross the wraparound line) costs one history read per process
                   lifetime, not one per sweep forever. */
                if (!_lastPostgresAlert.ContainsKey(cooldownKey)
                    && _historyStore is not null
                    && _postgresAlertHistorySeeded.TryAdd(cooldownKey, true))
                {
                    var seeded = await _historyStore.GetLastAlertTimeAsync(
                        snapshot.ServerKey, finding.MetricName, dedupKey: finding.Subject);
                    readClock.Restart();
                    if (seeded.HasValue)
                    {
                        _lastPostgresAlert[cooldownKey] = seeded.Value;
                    }
                }

                if (_lastPostgresAlert.TryGetValue(cooldownKey, out var last) && now - last < cooldown)
                {
                    continue;
                }

                /* Stamped even when muted, mirroring AlertEngine: a muted alert still consumes its cooldown,
                   so unmuting does not produce a backlog. */
                _lastPostgresAlert[cooldownKey] = now;

                var muted = _isAlertMuted?.Invoke(new AlertMuteContext
                {
                    ServerName = snapshot.ServerName,
                    MetricName = finding.MetricName,
                    /* The subject is the database for wraparound and the slot/holder for the others, which is
                       what a DatabaseName mute rule is written against. */
                    DatabaseName = finding.Subject,
                }) ?? false;

                await _alertDeliverer.DeliverAsync(
                    new AlertOutcome(
                        snapshot.ServerKey,
                        snapshot.ServerName,
                        finding.MetricName,
                        finding.CurrentValue,
                        finding.ThresholdValue,
                        /* The subject reaches the deliverer as a #1140 incident fingerprint. It was computed
                           by the evaluator and then thrown away (Context: null), so the send-side
                           IncidentCooldown fell back to its metric-level key: two databases past the
                           wraparound line, or two bad slots, collapsed into one incident and the second was
                           silently suppressed for the whole cooldown window. The DedupKey is identity only —
                           no ages or byte counts — so a recurrence of the SAME subject collapses while a
                           different subject does not. */
                        Context: new AlertContext
                        {
                            Incidents = new List<AlertIncident>
                            {
                                new(finding.Subject, new[] { finding.Subject }),
                            },
                        },
                        DetailText: null,
                        finding.NumericCurrentValue,
                        finding.NumericThresholdValue,
                        Muted: muted,
                        finding.Severity,
                        finding.ShortMessage),
                    cancellationToken);
                readClock.Restart();
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Server}] PostgreSQL alert evaluation failed after {ElapsedMs} ms: {Message}",
                runtime.Config.DisplayName, readClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(snapshot.ServerKey, "PostgreSQL outage-predictor reads", readClock.ElapsedMilliseconds);
        }

        /* #2711/#2719: Deadlocks, Blocking, Long-Running Query, Poison Wait and High CPU, each
           independently failure-isolated (own try/catch inside), so a broken read on one never costs the
           three predictors above or any sibling — the same isolation AlertEngine gives its own
           CheckDeadlocksAsync/CheckBlockingAsync/CheckLongRunningQueriesAsync/CheckPoisonWaitsAsync/
           CheckCpuAsync. */
        await EvaluatePgDeadlocksAsync(runtime, snapshot, config, cancellationToken);
        await EvaluatePgBlockingAsync(runtime, snapshot, config, cancellationToken);
        await EvaluatePgLongRunningQueryAsync(runtime, snapshot, config, cancellationToken);
        await EvaluatePgPoisonWaitAsync(runtime, snapshot, config, cancellationToken);
        await EvaluatePgCpuAsync(runtime, snapshot, config, cancellationToken);
    }

    /// <summary>
    /// The tier a PostgreSQL "High CPU" fire wears (#3653, A8e — the PostgreSQL host's half of what #3660
    /// did for <c>AlertEngine.GradeCpuFire</c>): Critical when the CPU health band bands the reading Critical,
    /// Warning otherwise. Pure, so the boundary can be pinned without a store or a deliverer fake, the same
    /// seam <see cref="BuildPgDeadlockIncident"/> is.
    ///
    /// <para><b>It is the card's classifier, called on the card's inputs, not a copy of its ladder.</b>
    /// <see cref="ServerHealthClassifier.CpuSeverity"/> is handed the raw Performance Insights CPU percent and
    /// the ACU utilization percent — the two figures <see cref="EvaluatePgCpuAsync"/> already read off the
    /// sample — on the <see cref="FleetCpuSource.PerformanceInsights"/> arm, and it runs
    /// <see cref="FleetCpuProvenance.CpuBandInputPercent"/> itself to pick the capacity percent (#3281): the
    /// figure this grades is therefore the figure the gate thresholded, by construction rather than by two
    /// call sites agreeing. Re-stating <c>&gt;= 95</c> here against the capacity percent would band the same
    /// number, and would be the second spelling of the ladder that the band's own comment forbids.</para>
    ///
    /// <para><b>What the bar is, and is not.</b> <see cref="ServerHealthThresholds.CpuCriticalPercent"/> is
    /// the ONE ladder the fleet card, <c>get_fleet_overview</c>, <c>/api/fleet</c> and the Performance
    /// Calendar band on (#3539 A2). It is stated against a quantity, not measured on a fleet distribution
    /// the way the deadlock tiers are (#3368) — <c>AlertEngine.GradeCpuFire</c>'s doc says the same of the
    /// SQL Server arm — so this is not a claim that 95% of the configured ceiling is a measured Critical; it
    /// is the claim that the alert row and the card must not disagree about the colour of the same minute.
    /// On the PostgreSQL arm that quantity is percent of the CONFIGURED ACU ceiling (the <c>db.serverless</c>
    /// reading where 100% means the ceiling really is reached), never the percent-of-allocated raw CPU.</para>
    ///
    /// <para><b>The band's other arms map to Warning.</b> Healthy and Warning both: the operator's
    /// <c>CpuThresholdPercent</c> already decided this fire was asked for, and a delivered alert is never
    /// rendered as nothing (the <c>GradeDeadlockFire</c> floor). Unknown — no capacity reading — cannot reach
    /// the fire site at all (<c>breaching</c> requires <c>lastCapacityPercent.HasValue</c>, and the reading
    /// graded is the one that produced it), and maps to Warning too rather than throwing, for the same reason.
    /// An operator whose threshold is 97 sees every fire Critical, which is a coherent reading of a knob above
    /// the bar.</para>
    /// </summary>
    internal static AlertSeverityLevel GradePgCpuFire(double cpuPercent, double? acuUtilizationPercent) =>
        ServerHealthClassifier.CpuSeverity(cpuPercent, acuUtilizationPercent, FleetCpuSource.PerformanceInsights)
            == HealthSeverity.Critical
            ? AlertSeverityLevel.Critical
            : AlertSeverityLevel.Warning;

    /// <summary>
    /// The Postgres High CPU alert (#2719), reading the <c>pg_cpu_utilization</c> table
    /// <see cref="DarlingCollectorRunner.IngestPgCpuAsync"/> fills from AWS Performance Insights. Reuses
    /// <see cref="DarlingAlertSettings.CpuEnabled"/>/<see cref="DarlingAlertSettings.CpuThresholdPercent"/> —
    /// the SAME knobs SQL Server's <c>AlertEngine.CheckCpuAsync</c> reads — rather than a Postgres-specific
    /// pair, so one threshold means the same thing on both engines and an operator tuning it does not have to
    /// find and change it twice. <see cref="DarlingAlertSettings.CpuAlertMode"/> is NOT read: that knob
    /// distinguishes "total server" from "just sqlserver.exe", a SQL-Server-only distinction PI's
    /// <c>os.cpuUtilization.total.avg</c> has no equivalent split for — it is already the one instance-level
    /// number this engine has.
    ///
    /// <para><b>It thresholds CAPACITY HEADROOM, not <c>cpu_percent</c></b> (#3281). <c>cpu_percent</c> is
    /// percent of the capacity CURRENTLY ALLOCATED, and every Aurora PostgreSQL instance in the measured
    /// fleet is <c>db.serverless</c>, where a one-vCPU instance reads exactly 100% whenever one core stays
    /// busy for a minute — the routine trigger for scaling up. Measured cost of thresholding that: 39 of
    /// the last 50 alerts on the PostgreSQL store were High CPU / CPU Resolved pairs minutes apart, firing
    /// at 100% and resolving at 17-26%, across five targets in about eleven hours; at the minute one fired
    /// the instance held 4 of 12 configured ACUs. <c>acu_utilization_percent</c> is percent of the
    /// CONFIGURED ceiling, so the operator's threshold means what it says against it.</para>
    ///
    /// <para><b>No capacity reading does not fire, and does not fall back to the raw CPU.</b> The fallback
    /// is the tempting shape and it silently arms a threshold against percent-of-allocated. Not firing matches
    /// the card, which bands Unknown on the same absence (<c>ServerHealthClassifier.CpuSeverity</c>) — the
    /// two consumers share the metric, so they have to share the rule. The cost is stated rather than
    /// hidden: a PROVISIONED Aurora PostgreSQL instance, whose raw reading IS a fraction of fixed capacity,
    /// would not be alerted on, because an absent ACU sample cannot be told apart from a serverless
    /// instance Performance Insights had no capacity sample for. The measured fleet has no such
    /// instance.</para>
    ///
    /// <para>The metric NAMES are "High CPU" / "CPU Resolved", the SQL Server strings, for the parity
    /// reason above: a mute rule, history filter or dashboard keyed on them means the same condition on
    /// either engine. The rendered text names the figure that crossed and what it is a fraction of, so a
    /// reader is never left inferring either.</para>
    /// </summary>
    private async Task EvaluatePgCpuAsync(
        ServerRuntime runtime, AlertServerSnapshot snapshot, DarlingConfig config, CancellationToken cancellationToken)
    {
        if (_postgres is null || _alertDeliverer is null)
        {
            return;
        }

        var alertSettings = new DarlingAlertSettings(config);

        if (!alertSettings.CpuEnabled)
        {
            /* Not an observation: turning the feature off is not CPU recovering, so the gate is left
               exactly as it stands and re-enabling resumes from the streak that was there. */
            return;
        }

        const string metricName = AlertEngine.CpuPersistenceMetric;
        var key = snapshot.ServerKey;
        var stateStore = new PgAlertStateStore(_postgres, _logger);

        var readClock = Stopwatch.StartNew();
        try
        {
            var now = DateTime.UtcNow;

            /* #3282: seed the gate's record once per key, mirroring AlertEngine's own
               EnsureWatermarksSeededAsync — the same reason #2716 seeds the deadlock/blocking watermarks
               here. Without it a restart forgets an open incident and the first post-restart pass to fill
               the streak announces it again.

               The metric name is the shared AlertEngine.CpuPersistenceMetric ("High CPU"), the same string
               the mute context and the history row use, so this row is the same subject an operator sees in
               config_alert_log. server_id never collides across engines, so one table serves both. */
            if (_pgCpuPersistenceSeeded.TryAdd(key, true))
            {
                var seeded = await stateStore.LoadAlertPersistenceAsync(key, metricName);
                readClock.Restart();
                if (seeded.HasValue)
                {
                    _pgCpuPersistence[key] = seeded.Value;

                    /* An ALREADY-OPEN incident gets its cooldown clock stamped, the same way
                       AlertEngine's own seeding does and for the same reason: the persisted Firing bit
                       stops a second rising edge, but _lastPgCpuAlert is in-memory, so an empty clock plus
                       a still-breaching condition would deliver the standing-condition reminder on the
                       first post-restart pass. Doing this on only one of the two engines would be the
                       shared-seam half-fix this whole issue is about, in miniature. */
                    if (seeded.Value.State.Firing)
                    {
                        _lastPgCpuAlert[key] = now;
                    }
                }
            }

            var priorRecord = _pgCpuPersistence.TryGetValue(key, out var cached)
                ? cached
                : AlertPersistenceRecord.Initial;

            /* The BATCH of readings this pass has not counted yet, oldest first — not just the latest one.
               Performance Insights is sampled at a 60-second period while pg_cpu_utilization is a
               five-minute collector, so roughly five samples arrive together; counting only the newest
               would discard four in five and make CpuBreachSamples take three batches (~15 minutes)
               instead of three minutes. See DarlingPgCpuUtilizationReader.GetSamplesSinceAsync. */
            var samples = await DarlingPgCpuUtilizationReader.GetSamplesSinceAsync(
                _postgres, runtime.ServerId, priorRecord.LastObservedSampleUtc, now, cancellationToken);
            readClock.Restart();

            var record = priorRecord;

            /* AT MOST ONE EDGE PER PASS. The loop stops at the first Fire or Resolve and leaves the rest of
               the batch for the next sweep, 30 seconds later (s_alertSweepInterval) — not the collector's
               five minutes, which is what makes this cheap.

               This is a STRUCTURAL fix rather than a guard, and it replaces one. Accumulating the batch's
               edges into flags and deciding at the end loses information, and it lost it twice: a fire and
               a resolve for the same incident flattened into "both happened", and then — the review catch —
               a pre-existing incident's resolve overwritten by a later, unrelated fire in the same batch,
               so the operator never heard that the incident they had open was over. Two instances, one
               category: a SEQUENCE of edges compressed into a summary. Stopping at the first edge makes the
               category unreachable instead of guarding its members, and it makes this pass the same shape as
               AlertEngine's SQL Server twin, where one sweep is one observation and the fire and resolve
               arms are mutually exclusive by construction.

               The cost is stated rather than hidden: after a restart or a collector gap, a whole excursion
               can arrive as a fire and then a resolve one sweep apart. That is what happened — the condition
               did hold for CpuBreachSamples samples and did then clear — and reporting both is the only
               option that cannot drop an edge, which is the property that failed twice. In steady state the
               30-second sweep sees each sample on its own and this never arises. */
            var outcome = PersistenceOutcome.None;
            DarlingPgCpuUtilizationReader.CpuSample? lastCounted = null;
            double? lastCapacityPercent = null;

            foreach (var sample in samples)
            {
                /* #3281: percent of the CONFIGURED ACU ceiling, which is what the threshold means, reached
                   through the SAME shared decision the two fleet cards band on. Always the Performance
                   Insights arm, because this evaluator exists only for PostgreSQL targets.

                   Read out into a local rather than compared inline: a lifted `double? >= int` is quietly
                   false on null, and "no capacity sample" deserves to be a named state rather than an
                   arithmetic accident. */
                var capacityPercent = FleetCpuProvenance.CpuBandInputPercent(
                    sample.CpuPercent, sample.AcuUtilizationPercent, FleetCpuSource.PerformanceInsights);

                if (!capacityPercent.HasValue)
                {
                    /* NO CAPACITY READING FREEZES THE GATE — never a breach, never a clear. A missing
                       headroom figure is not a measurement of the thing the threshold is against, and the
                       pre-#3282 code treated it as "not exceeded", which resolved an open incident on the
                       strength of a reading it never took. Not firing on it was already the rule (#3281
                       refuses to fall back to percent-of-allocated); not RESOLVING on it is the other half
                       of the same rule.

                       `continue` rather than `break` because this is not an EDGE: ending the pass here
                       would let one capacity-less minute stall the whole batch and every sample behind it.

                       The skipped sample is NOT revisited, and an earlier version of this comment claimed
                       otherwise (review catch). Once any later sample in the batch counts, the watermark
                       advances past this one and `sample_time > $2` excludes it for good — and it could not
                       be corrected anyway: RdsCpuIngestor COPYs new rows keyed off MAX(sample_time) and
                       never updates an inserted one, so there is no backfill path to wait for. Nothing is
                       lost by that, which is the point: a sample with no capacity figure has no
                       contribution to make to a gate counting breaches of a capacity threshold. */
                    continue;
                }

                var evaluation = AlertPersistenceGate.Evaluate(
                    record.State,
                    capacityPercent.Value >= alertSettings.CpuThresholdPercent,
                    AlertEngine.CpuBreachSamples,
                    AlertEngine.CpuClearSamples);

                record = new AlertPersistenceRecord(evaluation.State, sample.SampleTimeUtc);
                lastCounted = sample;
                lastCapacityPercent = capacityPercent;

                if (evaluation.Outcome != PersistenceOutcome.None)
                {
                    outcome = evaluation.Outcome;
                    break;
                }
            }

            if (!record.Equals(priorRecord))
            {
                _pgCpuPersistence[key] = record;
                await stateStore.SaveAlertPersistenceAsync(key, metricName, record);
                readClock.Restart();
            }

            var cooldown = TimeSpan.FromMinutes(Math.Max(1, _alertCooldownMinutes));

            /* THE REMINDER'S LEVEL IS THE LATEST KNOWN READING, not only a reading new to this pass — the
               two are different questions and only the gate cares about newness.

               The batch is empty on most sweeps by design: the sweep is 30 seconds and pg_cpu_utilization
               is a five-minute collector, so about nine sweeps in ten see no new sample. Deriving
               `breaching` from the batch alone therefore skipped the standing-condition reminder on those
               sweeps, which capped its cadence at the COLLECTOR interval instead of the configured
               cooldown. Masked at the default 15-minute cooldown, silent at any cooldown shorter than five
               minutes, and a parity break with AlertEngine.CheckCpuAsync — which computes `breaching`
               unconditionally from the latest reading every sweep — of exactly the kind the comment below
               claims does not exist. Review catch; the pre-batch code had this property for free because it
               re-read the latest reading every sweep, and the batch rewrite dropped it on this engine only.

               So: fall back to that same single-latest read when the batch brought nothing, and use it for
               the reminder decision ONLY. It never advances the gate and never moves the observed-sample
               watermark, because it is not a new observation — it is the answer to "is the condition still
               standing". At most one store read per sweep either way, which is what the pre-batch code
               cost. And it is the read with the 15-minute freshness bound, so a collector that stops does
               not leave the reminder firing forever on an hours-old reading. */
            if (!lastCapacityPercent.HasValue && record.State.Firing)
            {
                var standing = await DarlingPgCpuUtilizationReader.GetLatestAsync(
                    _postgres, runtime.ServerId, now, cancellationToken);
                readClock.Restart();
                if (standing is not null)
                {
                    lastCapacityPercent = FleetCpuProvenance.CpuBandInputPercent(
                        standing.CpuPercent, standing.AcuUtilizationPercent, FleetCpuSource.PerformanceInsights);
                    lastCounted = new DarlingPgCpuUtilizationReader.CpuSample(
                        standing.SampleTimeUtc, standing.CpuPercent, standing.AcuUtilizationPercent,
                        standing.ServerlessCapacityAcu, standing.MaxConfiguredAcu,
                        /* The gate's own reads carry no memory (LatestCpuSql selects none); stated, not defaulted. */
                        Memory: null);
                }
            }

            bool breaching = lastCapacityPercent.HasValue
                && lastCapacityPercent.Value >= alertSettings.CpuThresholdPercent;

            if (record.State.Firing && breaching)
            {
                /* The rising edge is cooldown-gated like every other fire, matching AlertEngine's SQL
                   Server twin exactly. #3282 changed what counts as an incident, deliberately not how the
                   cooldown works, and an engine-specific bypass here would make one threshold mean two
                   things across the two engines — the parity #2719 chose these metric names for. The gate
                   has already imposed CpuBreachSamples samples of delay, and a fire/resolve/fire cycle
                   needs CpuBreachSamples + CpuClearSamples samples, which at the ~60-second sample cadence
                   is about the default cooldown anyway. */
                var cooldownElapsed = !_lastPgCpuAlert.TryGetValue(key, out var last)
                    || now - last >= cooldown;
                if (!cooldownElapsed)
                {
                    return;
                }

                _lastPgCpuAlert[key] = now;

                var muted = _isAlertMuted?.Invoke(new AlertMuteContext
                {
                    ServerName = snapshot.ServerName,
                    MetricName = metricName,
                }) ?? false;

                /* The ACU line only when both halves were sampled: "N of M ACU" with either missing
                   would be a fabricated pair, and the percentage above already carries the answer. */
                var reading = lastCounted!;
                var allocation = reading.ServerlessCapacityAcu.HasValue && reading.MaxConfiguredAcu.HasValue
                    ? $"  Allocated: {reading.ServerlessCapacityAcu:0.#} of {reading.MaxConfiguredAcu:0.#} ACU\n"
                    : string.Empty;

                /* #3653 (A8e, the PostgreSQL host): GRADE the fire from the reading it fired on. Until now
                   this arm passed Severity: null, so every PostgreSQL High CPU row rendered amber by NAME
                   (#3635's replay arm) — a 100%-of-ceiling fire indistinguishable from an 80% one while the
                   fleet card beside it was red — after #3660 gave the SQL Server twin its tier. The grade is
                   the CPU health band's own classifier (ServerHealthClassifier.CpuSeverity) handed the same
                   two figures the card is handed, on the Performance Insights arm, so it bands the SAME
                   capacity percent this gate thresholded (CpuBandInputPercent decides which figure inside
                   the classifier, exactly as it did for `capacityPercent` above) and lands Critical at the
                   band's 95% bar — the card's ladder, not a new number; GradePgCpuFire says what that bar is
                   and is not. The context stays null: this host has exactly one deliverer, and it folds the
                   outcome's tier into the persisted context (#2090, DarlingAlertDeliverer.SendAndRecordAsync)
                   before the row is written — the path every Tier-0 and Poison Wait fire in this file already
                   rides — so the history grids and get_alert_history read the tier this fire wore. */
                var grade = GradePgCpuFire(reading.CpuPercent, reading.AcuUtilizationPercent);

                await _alertDeliverer.DeliverAsync(
                    new AlertOutcome(
                        key,
                        snapshot.ServerName,
                        metricName,
                        $"{lastCapacityPercent!.Value:F0}%",
                        $"{alertSettings.CpuThresholdPercent}%",
                        Context: null,
                        /* Both figures, each labelled with what it is a fraction OF — the whole defect
                           #3281 names is a reader taking one for the other. The sustained-for line is
                           #3282's: without it a reader cannot tell this from the single-sample alert that
                           used to arrive here, and "it has been this way for three samples" is most of
                           what makes the message worth acting on. */
                        DetailText:
                            $"  Capacity: {lastCapacityPercent.Value:F0}% {FleetCpuProvenance.CapacityDenominator}\n"
                            + allocation
                            + $"  Instance CPU: {reading.CpuPercent:F0}% of currently allocated capacity\n"
                            + $"  Threshold: {alertSettings.CpuThresholdPercent}%\n"
                            + $"  Sustained: {AlertEngine.CpuBreachSamples} consecutive samples",
                        NumericCurrentValue: lastCapacityPercent.Value,
                        NumericThresholdValue: alertSettings.CpuThresholdPercent,
                        Muted: muted,
                        Severity: grade,
                        ShortMessage:
                            $"Capacity at {lastCapacityPercent.Value:F0}% {FleetCpuProvenance.CapacityDenominator} "
                            + $"(threshold: {alertSettings.CpuThresholdPercent}%)"),
                    cancellationToken);
                readClock.Restart();
            }
            else if (outcome == PersistenceOutcome.Resolve)
            {
                /* The falling edge is the gate's, after CpuClearSamples consecutive clears, rather than the
                   first sample under the bar. It still says which of the two happened rather than claiming
                   a recovery it did not measure — a subject whose capacity readings stopped arriving
                   altogether never reaches this arm at all now, because a missing reading freezes the gate
                   instead of clearing it. */
                await NotifyPgResolutionAsync(key, snapshot.ServerName, metricName, "CPU Resolved",
                    lastCapacityPercent.HasValue
                        ? $"{snapshot.ServerName}: capacity back to {lastCapacityPercent.Value:F0}% "
                            + FleetCpuProvenance.CapacityDenominator
                        : $"{snapshot.ServerName}: no current capacity reading, so the alert is cleared");
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Server}] PostgreSQL CPU alert evaluation failed after {ElapsedMs} ms: {Message}",
                runtime.Config.DisplayName, readClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(snapshot.ServerKey, "PostgreSQL CPU alert read", readClock.ElapsedMilliseconds);
        }
    }

    /// <summary>
    /// The rolling-1-hour-window Postgres Deadlocks alert (#2711), reusing <see cref="RollingCountAlertGate"/>
    /// (shared with SQL Server's AlertEngine — see the field doc comment on <see cref="_lastPgDeadlockAlert"/>)
    /// so a deadlock already reported cannot re-fire merely because it is still inside the window, and a new
    /// one arriving mid-cooldown is not lost.
    /// <para>Metric names are the EXACT SQL Server strings ("Deadlocks Detected"/"Deadlocks Cleared") rather
    /// than Postgres-prefixed ones — deliberately, for parity: a mute rule, a history filter, or a dashboard
    /// built against "Deadlocks Detected" should not have to know or care which engine a server runs, and
    /// server_id never collides across engines so there is no ambiguity in doing so.</para>
    /// <para>#3444 (V122): the count threshold is <see cref="DarlingAlertSettings.PgDeadlockCountThreshold"/>
    /// — the clamped settings read, deliberately NOT SQL Server's
    /// <see cref="DarlingAlertSettings.DeadlockCountThreshold"/>, whose calibration does not travel across
    /// engines (the V122 rung says why). <see cref="DarlingAlertSettings.DeadlockEnabled"/> gates the check,
    /// the same treatment <see cref="EvaluatePgLongRunningQueryAsync"/> gives its own shared switch: whether
    /// the condition is worth alerting on at all is one preference covering both engines, and the volume at
    /// which it is worth a page is per-engine.</para>
    /// </summary>
    private async Task EvaluatePgDeadlocksAsync(
        ServerRuntime runtime, AlertServerSnapshot snapshot, DarlingConfig config, CancellationToken cancellationToken)
    {
        if (_postgres is null || _alertDeliverer is null)
        {
            return;
        }

        var alertSettings = new DarlingAlertSettings(config);

        if (!alertSettings.DeadlockEnabled)
        {
            return;
        }

        const string metricName = "Deadlocks Detected";
        var key = snapshot.ServerKey;
        var stateStore = new PgAlertStateStore(_postgres, _logger);

        var readClock = Stopwatch.StartNew();
        try
        {
            /* #2716: seed the watermark from the same config_edge_trigger_watermarks row
               AlertEngine's own SQL Server "Deadlocks Detected" twin reads/writes — the parity metric
               name #2711 deliberately chose means no new column or row shape is needed, only a read
               before trusting an in-memory zero. Once per key, mirroring AlertEngine's
               EnsureWatermarksSeededAsync/_seededServerKeys. */
            if (_pgDeadlockWatermarkSeeded.TryAdd(key, true))
            {
                var seeded = await stateStore.LoadEdgeTriggerWatermarkAsync(key, metricName);
                readClock.Restart();
                if (seeded.HasValue)
                {
                    _lastAlertedPgDeadlockCount[key] = seeded.Value;
                }
            }

            var now = DateTime.UtcNow;
            var windowStart = now.AddHours(-AlertEngine.RollingCountWindowHours);

            /* Already deduplicated by deadlock_hash (GROUP BY in DarlingPgDeadlockReader's own SQL) and
               windowed on occurred_at, not collection_time — see that reader's doc comment for why
               collection_time would put a report in the wrong window and move it every cycle. */
            var rows = await DarlingPgDeadlockReader.GetDeadlocksAsync(
                _postgres, runtime.ServerId, windowStart, now, limit: 50, cancellationToken);
            readClock.Restart();
            var count = rows.Count;

            var cooldown = TimeSpan.FromMinutes(Math.Max(1, _alertCooldownMinutes));
            var watermark = _lastAlertedPgDeadlockCount.TryGetValue(key, out var wm) ? wm : 0;
            var cooldownElapsed = !_lastPgDeadlockAlert.TryGetValue(key, out var last) || now - last >= cooldown;

            /* #3444: read ONCE, then handed to the gate and the delivered message alike, so the two cannot
               quote different numbers when a store reload swaps config.Alerts mid-evaluation. The rate tiers
               the fire is GRADED on (#3653, below) are read at the same spot for the same reason. */
            var threshold = alertSettings.PgDeadlockCountThreshold;
            var rateTiers = alertSettings.DeadlockRateThresholds;

            var decision = RollingCountAlertGate.Evaluate(
                count, threshold, watermark, cooldownElapsed, suppressed: false);
            _lastAlertedPgDeadlockCount[key] = decision.Watermark;
            if (decision.Watermark != watermark)
            {
                /* On-change only (#1145's own contract) — persist AFTER the in-memory update so a
                   store failure never desyncs the two; the in-memory watermark still gates this
                   process even if the write is lost, same posture as every other watermark save
                   in this codebase. */
                await stateStore.SaveEdgeTriggerWatermarkAsync(key, metricName, decision.Watermark);
                readClock.Restart();
            }

            var wasActive = _activePgDeadlockAlert.TryGetValue(key, out var activeBefore) && activeBefore;
            _activePgDeadlockAlert[key] = decision.Active;

            if (decision.Fire)
            {
                _lastPgDeadlockAlert[key] = now;

                var muted = _isAlertMuted?.Invoke(new AlertMuteContext
                {
                    ServerName = snapshot.ServerName,
                    MetricName = metricName,
                }) ?? false;

                /* #3653 (A8e, the PostgreSQL host): GRADE the fire the gate already decided on, exactly as
                   #3660 graded the SQL Server twin — the same AlertEngine.GradeDeadlockFire, on the same
                   count over the same window, against the same store-tunable pair. Until now this arm passed
                   Severity: null, so every PostgreSQL Deadlocks Detected row rendered red by NAME (#3635's
                   replay arm): one deadlock and a storm wore one colour. The count IS the hourly rate here
                   (the window is AlertEngine.RollingCountWindowHours, one hour, the same constant the
                   grader divides by), so Critical lands at deadlock_critical_per_hour (shipped 20/hr —
                   #3368's measured tier, inside the empty interval of 14,448 production server-hours) and
                   Warning everywhere else; the band's Healthy maps to Warning too, because the count knob
                   above already decided this fire was asked for and a delivered alert is never rendered as
                   nothing. The tiers are DarlingAlertSettings.DeadlockRateThresholds — the V120 knobs the
                   PostgreSQL fleet card bands this same server on since #3638 — not the shipped Default, so
                   an operator who moved the card's Critical tier sees the alert row move with it.

                   The instrument is the alert's own, deliberately: the deduped pg_deadlocks report count
                   the message quotes, not the pg_stat_database.deadlocks difference the card bands on.
                   Grading a row on a number it does not display would be a second card/alert
                   disagreement in place of the one this ends, and the counter is not in hand here — it is
                   the fleet reader's own read (FleetPgDeadlockSql), so reusing it would cost one more
                   alert-path store read per PostgreSQL server per sweep for a number the row cannot show.
                   The two instruments differ when the log capture misses what the server counted, in
                   which case the row is graded on the smaller, honest number. One stated edge: the read
                   above caps at 50 distinct reports, so the count — and therefore the grade — saturates
                   there; a Critical tier raised past 50/hr is unreachable on this host (the shipped 20 is
                   well under it, and the message quotes the same capped count, so grade and text agree).

                   The tier rides the outcome only, like every graded fire in this file (the Tier-0 trio and
                   Poison Wait pass finding.Severity the same way): this host's one deliverer folds it into
                   the persisted context (#2090) before the row is written, which is what #3635's grids and
                   get_alert_history read. The SQL engine sets both because it serves two SKUs' deliverers;
                   this host serves one. */
                await _alertDeliverer.DeliverAsync(
                    new AlertOutcome(
                        key,
                        snapshot.ServerName,
                        metricName,
                        count.ToString(CultureInfo.InvariantCulture),
                        threshold.ToString(CultureInfo.InvariantCulture),
                        Context: new AlertContext
                        {
                            Incidents = rows.Select(BuildPgDeadlockIncident).ToList(),
                        },
                        DetailText: null,
                        NumericCurrentValue: count,
                        NumericThresholdValue: threshold,
                        Muted: muted,
                        Severity: AlertEngine.GradeDeadlockFire(count, rateTiers),
                        ShortMessage: $"{count} deadlock(s) in the last hour"),
                    cancellationToken);
                readClock.Restart();
            }
            else if (!decision.Active && wasActive)
            {
                await NotifyPgResolutionAsync(key, snapshot.ServerName, metricName, "Deadlocks Cleared",
                    $"{snapshot.ServerName}: No deadlocks in the last hour");
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Server}] PostgreSQL deadlock alert evaluation failed after {ElapsedMs} ms: {Message}",
                runtime.Config.DisplayName, readClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(snapshot.ServerKey, "PostgreSQL deadlock alert read", readClock.ElapsedMilliseconds);
        }
    }

    /// <summary>
    /// The tier a PostgreSQL "Blocking Detected" fire wears (#3653, A8e): Warning, and ONLY Warning — stated
    /// at the fire site rather than left to <c>AlertSeverity.ForMetric</c>'s by-name arm, so the persisted row
    /// says what it fired at and the name decides only for rows written before it did (#3635).
    ///
    /// <para><b>No Critical tier, on purpose, because no measured bar exists to grade one on.</b> The rule
    /// #3660 applied to tempdb Space: existing measured bars only, no new constants without lineage. The
    /// candidates were checked and declined. The SQL Server blocking health band's tiers (#3596) were measured
    /// in <i>reports per server-hour</i> on engine-recorded evidence; this alert counts distinct root blockers
    /// SEEN in a once-a-minute sample of <c>pg_stat_activity</c>, so a waiter present in three consecutive
    /// captures is one wait observed three times, not three reports — the very denominator mismatch that keeps
    /// the PostgreSQL blocking band Unknown-with-reason (<c>Darling/README.md</c>, engine-coverage table).
    /// The operator's <c>PgBlockingCountThreshold</c> is a fire bar, not a tier. And the sampled-shape
    /// distribution an honest PostgreSQL tier would be cut from (share of captures holding a waiter; longest
    /// observed wait) has not been taken — #3539's residue on #3653 names it, and #3601's <c>lock_wait</c>
    /// log events (one line per wait past <c>deadlock_timeout</c>) as the event-grain source a report-rate
    /// tier would read. When that measurement exists it lands here as a second tier with nothing else to
    /// unpick; until then a distinct-blocker count over the operator's bar is a Warning, honestly labelled.
    /// The SQL Server twin (<c>AlertEngine.CheckBlockingAsync</c>) still fires with no tier at all; that
    /// site is the shared engine's and outside this host.</para>
    /// </summary>
    internal const AlertSeverityLevel PgBlockingFireSeverity = AlertSeverityLevel.Warning;

    /// <summary>
    /// The rolling-1-hour-window Postgres Blocking alert (#2711). Counts DISTINCT root blockers, not raw
    /// chain rows — <see cref="DarlingPgBlockingReader.GetPgBlockingChainsDedupedByRootAsync"/> (#2714) already
    /// dedupes by root INSIDE the query, before its own LIMIT, so a single persistent blocker sampled every
    /// cycle for an hour cannot crowd a distinct root out of the row budget the way the raw, severity-ordered
    /// <see cref="DarlingPgBlockingReader.GetPgBlockingChainsAsync"/> could. <see cref="WorstPgBlockingChainPerRoot"/>
    /// below still runs — see its own doc comment for why a second, C#-side dedup remains worth keeping even
    /// though the query no longer needs it to arrive at "one row per root". Same <see cref="RollingCountAlertGate"/>
    /// reuse and parity-named metrics as <see cref="EvaluatePgDeadlocksAsync"/> — see its doc comment for why —
    /// and the same #3444 settings treatment: <see cref="DarlingAlertSettings.PgBlockingCountThreshold"/> is the
    /// count gate, under <see cref="DarlingAlertSettings.BlockingEnabled"/>.
    /// <para>Fires at <see cref="PgBlockingFireSeverity"/> — an explicit Warning, and only Warning (#3653);
    /// that constant's doc says why no Critical tier exists here.</para>
    /// </summary>
    private async Task EvaluatePgBlockingAsync(
        ServerRuntime runtime, AlertServerSnapshot snapshot, DarlingConfig config, CancellationToken cancellationToken)
    {
        if (_postgres is null || _alertDeliverer is null)
        {
            return;
        }

        var alertSettings = new DarlingAlertSettings(config);

        if (!alertSettings.BlockingEnabled)
        {
            return;
        }

        const string metricName = "Blocking Detected";
        var key = snapshot.ServerKey;
        var stateStore = new PgAlertStateStore(_postgres, _logger);

        var readClock = Stopwatch.StartNew();
        try
        {
            /* #2716: same restart-survival seed as EvaluatePgDeadlocksAsync — see its comment. */
            if (_pgBlockingWatermarkSeeded.TryAdd(key, true))
            {
                var seeded = await stateStore.LoadEdgeTriggerWatermarkAsync(key, metricName);
                readClock.Restart();
                if (seeded.HasValue)
                {
                    _lastAlertedPgBlockingCount[key] = seeded.Value;
                }
            }

            var now = DateTime.UtcNow;
            var windowStart = now.AddHours(-AlertEngine.RollingCountWindowHours);

            /* #2714: deduped-by-root BEFORE the row-count LIMIT, not the raw severity-ordered read — a
               single severe root sampled repeatedly across the rolling window could otherwise occupy the
               entire LIMIT budget with repeat samples of itself, pushing a second, genuinely distinct root
               out of the top N before WorstPgBlockingChainPerRoot below ever saw it. That method's own
               per-root dedup is kept regardless, as a no-op safety net now that SQL already hands it one
               row per root — never the only thing standing between a real distinct root and an undercount. */
            var rows = await DarlingPgBlockingReader.GetPgBlockingChainsDedupedByRootAsync(
                _postgres, runtime.ServerId, windowStart, now, limit: 100, cancellationToken);
            readClock.Restart();

            var worstPerRoot = WorstPgBlockingChainPerRoot(rows);
            var count = worstPerRoot.Count;

            var cooldown = TimeSpan.FromMinutes(Math.Max(1, _alertCooldownMinutes));
            var watermark = _lastAlertedPgBlockingCount.TryGetValue(key, out var wm) ? wm : 0;
            var cooldownElapsed = !_lastPgBlockingAlert.TryGetValue(key, out var last) || now - last >= cooldown;

            /* #3444: read ONCE, for the reason EvaluatePgDeadlocksAsync gives at the same spot. */
            var threshold = alertSettings.PgBlockingCountThreshold;

            var decision = RollingCountAlertGate.Evaluate(
                count, threshold, watermark, cooldownElapsed, suppressed: false);
            _lastAlertedPgBlockingCount[key] = decision.Watermark;
            if (decision.Watermark != watermark)
            {
                await stateStore.SaveEdgeTriggerWatermarkAsync(key, metricName, decision.Watermark);
                readClock.Restart();
            }

            var wasActive = _activePgBlockingAlert.TryGetValue(key, out var activeBefore) && activeBefore;
            _activePgBlockingAlert[key] = decision.Active;

            if (decision.Fire)
            {
                _lastPgBlockingAlert[key] = now;

                var muted = _isAlertMuted?.Invoke(new AlertMuteContext
                {
                    ServerName = snapshot.ServerName,
                    MetricName = metricName,
                }) ?? false;

                /* #3653 (A8e, the PostgreSQL host): an explicit tier in place of Severity: null, so the row
                   carries what it fired at (through the deliverer's #2090 fold, as the deadlock arm above
                   explains) rather than leaving #3635's by-name replay arm to imply it. Warning, and only
                   Warning — PgBlockingFireSeverity's doc says which bars were considered and why none
                   qualifies as a Critical tier tonight. */
                await _alertDeliverer.DeliverAsync(
                    new AlertOutcome(
                        key,
                        snapshot.ServerName,
                        metricName,
                        count.ToString(CultureInfo.InvariantCulture),
                        threshold.ToString(CultureInfo.InvariantCulture),
                        Context: new AlertContext
                        {
                            Incidents = worstPerRoot.Select(BuildPgBlockingIncident).ToList(),
                        },
                        DetailText: null,
                        NumericCurrentValue: count,
                        NumericThresholdValue: threshold,
                        Muted: muted,
                        Severity: PgBlockingFireSeverity,
                        ShortMessage: $"{count} blocking session(s)"),
                    cancellationToken);
                readClock.Restart();
            }
            else if (!decision.Active && wasActive)
            {
                await NotifyPgResolutionAsync(key, snapshot.ServerName, metricName, "Blocking Cleared",
                    $"{snapshot.ServerName}: No active blocking");
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Server}] PostgreSQL blocking alert evaluation failed after {ElapsedMs} ms: {Message}",
                runtime.Config.DisplayName, readClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(snapshot.ServerKey, "PostgreSQL blocking alert read", readClock.ElapsedMilliseconds);
        }
    }

    /// <summary>
    /// How far back "the most recent capture" is allowed to reach before it stops counting as "now", for the
    /// Postgres Long-Running Query alert (#2711). See
    /// <see cref="DarlingPgSessionStatesReader.GetCurrentLongRunningSessionsAsync"/>'s doc comment for why this
    /// is the fleet's own staleness convention rather than a tight multiple of the collector's 1-minute
    /// configured cadence. Derived rather than copied (#2794): a recency bound tighter than the fleet's
    /// staleness definition would make this alert silently never fire on exactly the servers whose stretched
    /// sweeps most need it.
    /// </summary>
    private const int PgLongRunningQueryRecencyMinutes = ServerHealthThresholds.CollectionStoppedMinutesDefault;

    /// <summary>
    /// The tier a PostgreSQL "Long-Running Query" fire wears (#3653, A8e): Warning, and ONLY Warning, stated
    /// at the fire site for the reason <see cref="PgBlockingFireSeverity"/> gives.
    ///
    /// <para><b>No Critical tier, because nothing measured supports one.</b> The only bar this alert has is
    /// the operator's <c>LongRunningQueryThresholdMinutes</c>, a fire bar — "N times the threshold" would be
    /// a folklore multiplier with no lineage, the constant the repo's rule refuses. The measured evidence
    /// that exists (the SQL Server twin's population, one production store class, #3653 A5) points the other
    /// way: 191 distinct sessions over 30 minutes in seven days, the p90 seen in 6,192 snapshots — permanent
    /// background requests, which is why that row shapes the alert's rollout as opt-out coverage rather
    /// than a gate; a duration tier cut from such a population would grade the background as the emergency,
    /// and no PostgreSQL distribution has been taken at all. No health band exists for the condition on
    /// either engine to borrow a ladder from. The SQL Server twin
    /// (<c>AlertEngine.CheckLongRunningQueriesAsync</c>) still fires with no tier at all; that site is the
    /// shared engine's and outside this host.</para>
    /// </summary>
    internal const AlertSeverityLevel PgLongRunningQueryFireSeverity = AlertSeverityLevel.Warning;

    /// <summary>
    /// The live-state Postgres Long-Running Query alert (#2711): fires when the most recent
    /// <c>pg_session_states</c> capture shows any session whose CURRENT query has run past
    /// <see cref="IAlertEngineSettings.LongRunningQueryThresholdMinutes"/> — the SAME configured threshold SQL
    /// Server's <c>AlertEngine.CheckLongRunningQueriesAsync</c> uses, read live off <paramref name="config"/>
    /// rather than a separate Postgres-only constant, so changing the one setting changes behavior for both
    /// engines the way one shared "how long is too long" preference should.
    ///
    /// <para><b>Boolean state + cooldown, not <see cref="RollingCountAlertGate"/>.</b> Unlike Deadlocks/Blocking
    /// above, this is not a rolling count of discrete past events — it is "is a condition true right now",
    /// exactly the shape AlertEngine's own SQL Server check already uses (an active flag plus a cooldown
    /// timestamp). Reusing the rolling-count gate here would answer a question this alert does not ask.</para>
    ///
    /// <para>No query-text preview: <c>pg_session_states</c> deliberately stores none (see the collector's
    /// class remarks), so the message identifies the session by pid/database/command tag instead of the
    /// statement text SQL Server's equivalent shows.</para>
    ///
    /// <para><b>The noise opt-outs ride the SAME switches SQL Server's read takes</b> (#3539): the shared
    /// <c>longRunningQueryExcludeBackups</c> drops the dump/restore utilities' sessions, and the shared
    /// <c>excludedDatabases</c> list rides INTO the read as the <c>candidates</c> CTE's third flag, ahead of
    /// the row cap (#3742 — until then it was applied in C# after <c>LIMIT</c>, on this read and on both SQL
    /// Server reads, so an excluded database's sessions consumed the page and the alert came back short or
    /// empty while matches existed; #3772 fixed the SQL Server twins, this the PostgreSQL one). The
    /// unconditional ones — non-client backends (autovacuum, walsender), the
    /// VACUUM/ANALYZE/REINDEX/CLUSTER statements, idle-in-transaction — live in the read's SQL; see
    /// <see cref="DarlingPgSessionStatesReader.CurrentLongRunningSessionsSqlTemplate"/> for each one's SQL
    /// Server sibling and for why the three remaining SQL Server switches have no honest reading here. What
    /// is still reported carries its <c>command_tag</c> on the incident line, so a <c>CREATE</c> that is an
    /// index build reads as what it is rather than being dropped on a guess — the annotate-never-suppress
    /// posture SQL Server's Agent-job name (#3497) takes on its card.</para>
    ///
    /// <para><b>The opt-out knob rides INTO the read here too (#3743, closing #3653 A5 Q5's stated gap).</b>
    /// <c>longRunningQueryExcludedProgramNamePrefixes</c> / <c>longRunningQueryExcludedLogins</c> — the SAME
    /// settings row the SQL Server twin reads, since V135 — are normalised into a
    /// <see cref="LongRunningQueryExclusions"/> on every sweep and translated by the shared
    /// <see cref="LongRunningQueryExclusions.BuildSqlPredicates"/> over <c>application_name</c> (the
    /// <c>program_name</c> twin) and <c>username</c> (<c>pg_stat_activity.usename</c>, the <c>login_name</c>
    /// twin), so an entry means one thing on both engines: prefix for programs, whole name for logins,
    /// case-insensitive, no wildcard grammar. Applied ahead of the row cap for the reason the knob's type
    /// summary gives — a permanent ETL or replication worker under a service role is the longest-running
    /// session by construction and would otherwise fill the five-row page every sweep. Before #3743 the
    /// setting was visible, editable and read back on a PostgreSQL target and did nothing there. The knob is
    /// passed as rendered text and operands rather than as the record because the storage assembly the reader
    /// lives in does not reference the alerting assembly; the translation is still spelled once, in the
    /// builder. The seeded defaults name SQL Server internals (<c>SQLAgent - TSQL JobStep</c>, the two
    /// <c>NT AUTHORITY</c> logins) and match nothing on PostgreSQL by construction — correct, and the
    /// <c>[]</c>-excludes-nothing rule holds; whether this engine wants seeds of its own is a production read
    /// for the monitor seat, not a guess made here.</para>
    ///
    /// <para>The knob's receipt renders on the PostgreSQL incident's context exactly as the SQL Server twin
    /// renders it — <see cref="AlertContextBuilders.BuildLongRunningQueryExclusionItem"/>, same labels
    /// (<c>Excluded Count</c> / <c>Excluded By Program Prefix</c> / <c>Excluded By Login</c>), appended to
    /// <c>Details</c> only when the knob is SET, so an operator who cleared both lists does not read
    /// "Excluded: 0" on every card. Annotation, never suppression: the fire is decided on the rows the read
    /// returned, and the item can only add a line. The database list's receipt (#3742) is the SQL Server
    /// twin's second item, <see cref="AlertContextBuilders.BuildLongRunningQueryExcludedDatabasesItem"/>,
    /// appended after the knob's under the same rule — only when the list is SET, rendered even at 0 then —
    /// so a page that is short because the list removed the longest sessions says so.</para>
    ///
    /// <para>Fires at <see cref="PgLongRunningQueryFireSeverity"/> — an explicit Warning, and only Warning
    /// (#3653); that constant's doc says why no Critical tier exists here.</para>
    /// </summary>
    private async Task EvaluatePgLongRunningQueryAsync(
        ServerRuntime runtime, AlertServerSnapshot snapshot, DarlingConfig config, CancellationToken cancellationToken)
    {
        if (_postgres is null || _alertDeliverer is null)
        {
            return;
        }

        var alertSettings = new DarlingAlertSettings(config);

        if (!alertSettings.LongRunningQueryEnabled)
        {
            return;
        }

        const string metricName = "Long-Running Query";
        var key = snapshot.ServerKey;

        var readClock = Stopwatch.StartNew();
        try
        {
            var thresholdMinutes = alertSettings.LongRunningQueryThresholdMinutes;
            var now = DateTime.UtcNow;

            /* #3743: the opt-out knob, normalised from the same two settings lists AlertEngine's SQL Server
               arm normalises (trim, blanks dropped, case-insensitive dedupe) and translated ONCE by the shared
               builder over this engine's columns; the reader takes the rendered text and its operands,
               numbered from the reader's own first free ordinal. #3742: the shared excludedDatabases list goes
               through the SAME builder call (its five-argument overload, over database_name, operands after the
               login operands) so all three arms are one text in one binding order — the SQL Server twin's call
               in DarlingAlertReadAdapter, over this reader's column constants. An empty knob and an empty list
               render as three FALSE arms and the read's rows are exactly the pre-#3743 rows. */
            var exclusions = LongRunningQueryExclusions.From(
                alertSettings.LongRunningQueryExcludedProgramNamePrefixes, alertSettings.LongRunningQueryExcludedLogins);
            var exclusionSql = exclusions.BuildSqlPredicates(
                DarlingPgSessionStatesReader.ExclusionProgramNameColumn,
                DarlingPgSessionStatesReader.ExclusionLoginNameColumn,
                DarlingPgSessionStatesReader.ExclusionDatabaseNameColumn,
                alertSettings.ExcludedDatabases,
                DarlingPgSessionStatesReader.ExclusionFirstParameterOrdinal);

            var read = await DarlingPgSessionStatesReader.GetCurrentLongRunningSessionsAsync(
                _postgres, runtime.ServerId, thresholdMs: thresholdMinutes * 60_000L, now,
                PgLongRunningQueryRecencyMinutes, limit: alertSettings.LongRunningQueryMaxResults,
                excludeBackups: alertSettings.LongRunningQueryExcludeBackups,
                programPrefixPredicate: exclusionSql.ProgramPrefixPredicate,
                loginPredicate: exclusionSql.LoginPredicate,
                databasePredicate: exclusionSql.DatabasePredicate,
                exclusionOperands: exclusionSql.Operands,
                cancellationToken);
            var rows = read.Sessions;
            readClock.Restart();

            var cooldown = TimeSpan.FromMinutes(Math.Max(1, _alertCooldownMinutes));
            var wasActive = _activePgLongRunningQueryAlert.TryGetValue(key, out var activeBefore) && activeBefore;
            _activePgLongRunningQueryAlert[key] = rows.Count > 0;

            if (rows.Count > 0)
            {
                var cooldownElapsed = !_lastPgLongRunningQueryAlert.TryGetValue(key, out var last) || now - last >= cooldown;
                if (!cooldownElapsed)
                {
                    return;
                }

                _lastPgLongRunningQueryAlert[key] = now;

                var worst = rows[0];
                var elapsedMinutes = worst.QueryDurationMs / 60_000;

                var muted = _isAlertMuted?.Invoke(new AlertMuteContext
                {
                    ServerName = snapshot.ServerName,
                    MetricName = metricName,
                    DatabaseName = worst.DatabaseName,
                }) ?? false;

                /* #3653 (A8e, the PostgreSQL host): an explicit tier in place of Severity: null, for the
                   reason the blocking arm above gives. Warning, and only Warning —
                   PgLongRunningQueryFireSeverity's doc says why. */
                await _alertDeliverer.DeliverAsync(
                    new AlertOutcome(
                        key,
                        snapshot.ServerName,
                        metricName,
                        $"{rows.Count} query(s), longest {elapsedMinutes}m",
                        $"{thresholdMinutes}m",
                        Context: new AlertContext
                        {
                            Incidents = rows.Select(BuildPgLongRunningQueryIncident).ToList(),
                            /* #3743: the knob's receipt, the SQL Server twin's item verbatim (same builder, same
                               labels) and under the same rule — only when the knob is SET; a cleared knob has no
                               footnote. #3742: the database list's receipt beside it, the twin's second item, only
                               when the list is SET. See the method summary. */
                            Details = BuildPgLongRunningQueryExclusionDetails(
                                exclusions, read.ExcludedByProgramPrefix, read.ExcludedByLogin,
                                alertSettings.ExcludedDatabases, read.ExcludedByDatabase),
                        },
                        DetailText: null,
                        NumericCurrentValue: elapsedMinutes,
                        NumericThresholdValue: thresholdMinutes,
                        Muted: muted,
                        Severity: PgLongRunningQueryFireSeverity,
                        ShortMessage: $"pid {worst.Pid} running {elapsedMinutes}m — {worst.CommandTag ?? "(unknown)"}"
                            + (worst.DatabaseName is null ? "" : $" on {worst.DatabaseName}")),
                    cancellationToken);
                readClock.Restart();
            }
            else if (wasActive)
            {
                await NotifyPgResolutionAsync(key, snapshot.ServerName, metricName, "Long-Running Queries Cleared",
                    $"{snapshot.ServerName}: No queries over threshold");
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Server}] PostgreSQL long-running-query alert evaluation failed after {ElapsedMs} ms: {Message}",
                runtime.Config.DisplayName, readClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(snapshot.ServerKey, "PostgreSQL long-running-query alert read", readClock.ElapsedMilliseconds);
        }
    }

    /// <summary>
    /// Pure mapping, pulled out of <see cref="EvaluatePgLongRunningQueryAsync"/> for the same testability
    /// reason as <see cref="BuildPgDeadlockIncident"/>. Dedup key is the synthetic backend id (stable across
    /// samples of the same backend, unlike a reused pid), matching <see cref="BuildPgBlockingIncident"/>'s
    /// convention for the same underlying identity.
    /// </summary>
    internal static AlertIncident BuildPgLongRunningQueryIncident(
        DarlingPgSessionStatesReader.LongRunningSessionRow row) =>
        new(
            row.BackendId.ToString(CultureInfo.InvariantCulture),
            new[]
            {
                $"pid {row.Pid} running {row.QueryDurationMs / 60_000}m ({row.CommandTag ?? "(unknown)"})",
            },
            Database: row.DatabaseName);

    /// <summary>
    /// The PostgreSQL Long-Running Query card's exclusion receipts (#3743, #3742), pulled out of
    /// <see cref="EvaluatePgLongRunningQueryAsync"/> for testability like the incident mapping above: the
    /// SQL Server twin's <see cref="AlertContextBuilders.BuildLongRunningQueryExclusionItem"/> as the card's
    /// first detail item when the knob is set, and NO item when both lists are empty — <c>AlertEngine</c>'s
    /// rule, for its reason (an operator who said "evaluate everything" does not want "Excluded: 0" on
    /// every card); then the twin's <see cref="AlertContextBuilders.BuildLongRunningQueryExcludedDatabasesItem"/>
    /// when the shared <c>excludedDatabases</c> list is set, rendered even at 0 then, and absent when it is
    /// empty (the default), so a fresh install's card is unchanged. Two items rather than one because the two
    /// settings are different instruments with different owners (the builder's doc says why the knob's count
    /// stays the knob's). The counts are the read's, taken ahead of the cap over the same capture as the rows,
    /// and the two items' numbers sum to the sessions the page does not show.
    /// </summary>
    internal static List<AlertDetailItem> BuildPgLongRunningQueryExclusionDetails(
        LongRunningQueryExclusions exclusions, int excludedByProgramPrefix, int excludedByLogin,
        IReadOnlyList<string> excludedDatabases, int excludedByDatabase)
    {
        ArgumentNullException.ThrowIfNull(exclusions);
        ArgumentNullException.ThrowIfNull(excludedDatabases);
        var details = new List<AlertDetailItem>();
        if (!exclusions.IsEmpty)
        {
            details.Add(AlertContextBuilders.BuildLongRunningQueryExclusionItem(exclusions, excludedByProgramPrefix, excludedByLogin));
        }

        if (excludedDatabases.Count > 0)
        {
            details.Add(AlertContextBuilders.BuildLongRunningQueryExcludedDatabasesItem(excludedDatabases, excludedByDatabase));
        }

        return details;
    }

    /// <summary>
    /// The Postgres Poison Wait analogue (#2711): fires when a poison wait event — the IPC
    /// BtreePage/BufferIo pair, chosen from the issue's own fleet research — accumulated enough wait time
    /// across <see cref="PostgresAlertEvaluator.PoisonWaitWindowMinutes"/> to average at least
    /// <see cref="PostgresAlertEvaluator.PoisonWaitWarningAvgWaiters"/> backend(s) continuously stuck.
    ///
    /// <para><b>Gated on the SAME <see cref="IAlertEngineSettings.PoisonWaitEnabled"/> switch SQL Server's
    /// <c>AlertEngine.CheckPoisonWaitsAsync</c> honors</b> — the #2711 Long-Running Query precedent: one
    /// "poison wait alerts on/off" preference, both engines. <c>PoisonWaitThresholdMs</c> is deliberately
    /// NOT reused: it is an avg-ms-per-wait bar, and the issue's research shows the Postgres poison events
    /// average 1-2 ms per wait at six-figure volumes — a shape that bar can never see. The Postgres
    /// threshold is a constant on <see cref="PostgresAlertEvaluator"/> for this first cut: add
    /// configuration when someone actually wants a different number, not speculatively. The
    /// <see cref="EvaluatePgDeadlocksAsync"/>/<see cref="EvaluatePgBlockingAsync"/> count gates started
    /// under that same rule and graduated to settings when #3444 named the operator who wanted one; this
    /// threshold graduates the same way on the same evidence, not before.</para>
    ///
    /// <para><b>Cooldown + active flag + the #2704 unrefreshed-source-row guard, per SUBJECT.</b> This is
    /// an accumulation check like its SQL Server twin, so it inherits that method's exact state kit (see
    /// the field block's doc comment), keyed per server|metric|subject per #1140 — the two poison events
    /// are different incidents. The cooldown seeds from history once per key (#2716), and the seed also
    /// floors the collection-time guard, so a restart cannot re-fire on a window the previous process
    /// already reported.</para>
    ///
    /// <para>On a non-Aurora target the read returns no rows — the cumulative wait counters are
    /// Aurora-only — and no rows is silence, the honest empty. Extending the poison definition to
    /// self-hosted targets via <c>pg_wait_sampling</c> needs its own calibration (sampled counts, not
    /// accumulated time) and its own fleet evidence first.</para>
    ///
    /// <para><b>The Cleared edge requires an OBSERVED window (#3653) — the contract
    /// <c>AlertEngine.CheckPoisonWaitsAsync</c> adopted in #3593, ported.</b> Unwatched is not quiet: a
    /// collector that stops delivering must not make this host announce "Poison Waits Cleared", which is
    /// exactly what the pre-#3653 arm did — every active subject cleared the moment the read came back empty.
    /// On SQL Server the rows are their own witness (THREADPOOL lands a row every cycle); on PostgreSQL they
    /// are not, because since #2694 the <c>pg_wait_stats</c> collector skips an event whose waits delta is
    /// zero — a quiet event and a dead collector both read as "no row for that subject". So the read carries
    /// the collector's own <c>collection_log</c> SUCCESS count over the same window
    /// (<see cref="PostgresPoisonWaitWindow.Observed"/>, the #3537 xmin-horizon witness), and a standing
    /// subject clears only when the window was looked at and the subject is no longer over the bar — which
    /// includes the subject having written NO row (quiet, on an observed window). An unobserved window holds
    /// every active flag where it is and says so once at Debug per server (the #3282 rule for a CPU reading
    /// that stops arriving); the next observed window either re-fires or clears on evidence.</para>
    /// </summary>
    private async Task EvaluatePgPoisonWaitAsync(
        ServerRuntime runtime, AlertServerSnapshot snapshot, DarlingConfig config, CancellationToken cancellationToken)
    {
        if (_postgres is null || _alertDeliverer is null)
        {
            return;
        }

        var alertSettings = new DarlingAlertSettings(config);
        if (!alertSettings.PoisonWaitEnabled)
        {
            return;
        }

        const string metricName = PostgresAlertEvaluator.PoisonWaitMetric;
        var serverKey = snapshot.ServerKey;

        var readClock = Stopwatch.StartNew();
        try
        {
            var adapter = new DarlingPostgresAlertReadAdapter(_postgres);
            var window = await adapter.GetPoisonWaitPressureAsync(runtime.ServerId, cancellationToken);
            readClock.Restart();
            var rows = window.Waits;
            var findings = PostgresAlertEvaluator.EvaluatePoisonWaits(rows);

            var now = DateTime.UtcNow;
            var cooldown = TimeSpan.FromMinutes(Math.Max(1, _alertCooldownMinutes));

            /* Matched back by the evaluator's own subject builder so the guard below cannot drift from
               the findings it protects. */
            var newestCollectionBySubject = new Dictionary<string, DateTime>(StringComparer.Ordinal);
            foreach (var row in rows)
            {
                newestCollectionBySubject[PostgresAlertEvaluator.PoisonWaitSubject(row)] = row.NewestCollectionTime;
            }

            var firingSubjects = new HashSet<string>(StringComparer.Ordinal);

            foreach (var finding in findings)
            {
                firingSubjects.Add(finding.Subject);

                var cooldownKey = string.Create(
                    CultureInfo.InvariantCulture,
                    $"{serverKey}|{finding.MetricName}|{finding.Subject}");

                /* The condition is TRUE regardless of whether this pass delivers — the active flag tracks
                   the condition, not the delivery, or a fire suppressed by cooldown would produce a
                   phantom Cleared next sweep. */
                _activePgPoisonWaitAlert[cooldownKey] = true;

                /* #2716: seed the cooldown from history once per key — finding.Subject is the #1140 dedup
                   fingerprint this alert fires with, so GetLastAlertTimeAsync's #1154 filter reconstructs
                   the per-subject stamp. The same seed floors the #2704 guard: rows collected before the
                   last recorded fire were already reported by whichever process fired it. */
                if (!_lastPgPoisonWaitAlert.ContainsKey(cooldownKey)
                    && _historyStore is not null
                    && _pgPoisonWaitCooldownSeeded.TryAdd(cooldownKey, true))
                {
                    var seeded = await _historyStore.GetLastAlertTimeAsync(
                        serverKey, finding.MetricName, dedupKey: finding.Subject);
                    readClock.Restart();
                    if (seeded.HasValue)
                    {
                        _lastPgPoisonWaitAlert[cooldownKey] = seeded.Value;
                        _lastPgPoisonWaitCollectionTime[cooldownKey] = seeded.Value;
                    }
                }

                /* #2704: only a collection_time newer than the one last fired on counts as a fresh
                   observation. The collector's delivered cadence and the alert cooldown are independent
                   clocks — a cooldown-elapsed re-read of the SAME still-uncollected row is the identical
                   accumulation surfacing twice, not a new observation of a standing condition. */
                var newestCollection = newestCollectionBySubject.TryGetValue(finding.Subject, out var nc)
                    ? nc
                    : DateTime.MinValue;
                var hasFreshCollection = !_lastPgPoisonWaitCollectionTime.TryGetValue(cooldownKey, out var lastCollection)
                    || newestCollection > lastCollection;
                if (!hasFreshCollection)
                {
                    continue;
                }

                if (_lastPgPoisonWaitAlert.TryGetValue(cooldownKey, out var last) && now - last < cooldown)
                {
                    continue;
                }

                /* Stamped even when muted, mirroring AlertEngine: a muted alert still consumes its
                   cooldown, so unmuting does not produce a backlog. */
                _lastPgPoisonWaitAlert[cooldownKey] = now;
                _lastPgPoisonWaitCollectionTime[cooldownKey] = newestCollection;

                var muted = _isAlertMuted?.Invoke(new AlertMuteContext
                {
                    ServerName = snapshot.ServerName,
                    MetricName = finding.MetricName,
                    /* WaitType, not DatabaseName: wait events are instance-wide, and the SQL Server twin's
                       mute rules key on the wait type — the parity metric name only helps if the mute
                       dimension matches too. */
                    WaitType = finding.Subject,
                }) ?? false;

                await _alertDeliverer.DeliverAsync(
                    new AlertOutcome(
                        serverKey,
                        snapshot.ServerName,
                        finding.MetricName,
                        finding.CurrentValue,
                        finding.ThresholdValue,
                        /* The subject as the #1140 incident fingerprint, identity only — same shape and
                           reasoning as the Tier 0 delivery loop above. */
                        Context: new AlertContext
                        {
                            Incidents = new List<AlertIncident>
                            {
                                new(finding.Subject, new[] { finding.CurrentValue }),
                            },
                            /* #4223: the wait type as structured data, the same string the mute context
                               below already keys on (WaitType = finding.Subject) — an alert-notebook reader
                               can branch on it without parsing the subject back out of the incident. */
                            WaitType = finding.Subject,
                        },
                        DetailText: null,
                        finding.NumericCurrentValue,
                        finding.NumericThresholdValue,
                        Muted: muted,
                        finding.Severity,
                        finding.ShortMessage),
                    cancellationToken);
                readClock.Restart();
            }

            /* The Cleared edge, per subject: previously active, no longer over the bar, ON AN OBSERVED WINDOW
               (#3653 — see the method's doc comment). Late by up to one window (the rolling sums age out
               rather than reset), which is accepted — a Cleared that arrives a few minutes conservative
               beats one that flaps with each sweep. An UNOBSERVED window — no collector run logged and no
               row stored inside it — is collector silence, not the server going quiet: every active flag
               holds, and the hold is logged once per server (Debug: the collector's own Collection Stopped
               self-alert is the loud channel for a dead collector; this line only explains why a Cleared
               the operator might expect has not arrived). The once-flag resets on the next observed
               window so a second outage logs again. */
            var activePrefix = string.Create(
                CultureInfo.InvariantCulture, $"{serverKey}|{metricName}|");
            if (!window.Observed)
            {
                if (_activePgPoisonWaitAlert.Any(e => e.Value && e.Key.StartsWith(activePrefix, StringComparison.Ordinal))
                    && _pgPoisonWaitHoldLogged.TryAdd(serverKey, true))
                {
                    _logger.LogDebug(
                        "[{Server}] PostgreSQL poison wait window unobserved: no pg_wait_stats collector run logged and no row stored in the last {WindowMinutes} min — holding the standing alert(s) rather than announcing Cleared on collector silence (#3653)",
                        runtime.Config.DisplayName, PostgresAlertEvaluator.PoisonWaitWindowMinutes);
                }

                return;
            }

            _pgPoisonWaitHoldLogged.TryRemove(serverKey, out _);

            foreach (var entry in _activePgPoisonWaitAlert)
            {
                if (!entry.Value
                    || !entry.Key.StartsWith(activePrefix, StringComparison.Ordinal))
                {
                    continue;
                }

                var subject = entry.Key[activePrefix.Length..];
                if (firingSubjects.Contains(subject))
                {
                    continue;
                }

                _activePgPoisonWaitAlert[entry.Key] = false;
                await NotifyPgResolutionAsync(serverKey, snapshot.ServerName, metricName, "Poison Waits Cleared",
                    $"{snapshot.ServerName}: {subject} accumulated wait back below threshold");
                readClock.Restart();
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Server}] PostgreSQL poison wait alert evaluation failed after {ElapsedMs} ms: {Message}",
                runtime.Config.DisplayName, readClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(snapshot.ServerKey, "PostgreSQL poison wait alert read", readClock.ElapsedMilliseconds);
        }
    }

    /// <summary>
    /// Writes a Postgres Deadlocks/Blocking resolution the same way <see cref="BuildAlertEngine"/>'s
    /// <c>resolutionCallback</c> does for the SQL Server families: a log line via
    /// <see cref="AlertFiringLog.Resolved"/> and a history row via
    /// <see cref="DarlingSelfAlertEvaluator.BuildResolutionRecord"/> — never through
    /// <see cref="_alertDeliverer"/>, because a resolution has no send channel
    /// (<see cref="AlertResolution"/>'s own doc comment). Best-effort: a history-write failure here must
    /// not be allowed to look like the alert itself failed, since the condition genuinely did clear.
    /// </summary>
    private async Task NotifyPgResolutionAsync(
        string serverKey, string serverName, string metricName, string title, string message)
    {
        /* title, not metricName: AlertFiringLog deliberately uses different strings for Fired ("Deadlocks
           Detected") vs Resolved ("Deadlocks Cleared") so the pair is distinguishable without reading the
           log level — every other call site (the resolutionCallback closure above,
           DarlingSelfAlertEvaluator) passes the title-like value here. */
        _logger.LogInformation("{Line}", AlertFiringLog.Resolved(serverName, title, message));

        if (_historyStore is null)
        {
            return;
        }

        try
        {
            await _historyStore.RecordAlertAsync(DarlingSelfAlertEvaluator.BuildResolutionRecord(
                new AlertResolution(serverKey, serverName, metricName, title, message)));
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: a history WRITE, not a condition read. */
            _logger.LogWarning("Could not record Postgres alert resolution for {Server}/{Metric}: {Message}",
                serverName, metricName, ex.Message);
        }
    }

    /// <summary>
    /// Pure mapping, pulled out of <see cref="EvaluatePgDeadlocksAsync"/> so it is testable without a
    /// Postgres connection or an <see cref="IAlertDeliverer"/> fake — the same "pure, testable seam"
    /// reasoning <see cref="CadencePhaseOffset"/> already gets in this file. One <see cref="AlertIncident"/>
    /// per distinct deadlock (rows arrive pre-deduplicated by <c>deadlock_hash</c>, see
    /// <see cref="DarlingPgDeadlockReader.GetDeadlocksAsync"/>'s own doc comment), falling back to pid +
    /// participant count when the victim's statement text was not resolvable (permissions, or a graph
    /// shape the log parser did not recognise).
    /// </summary>
    internal static AlertIncident BuildPgDeadlockIncident(DarlingPgDeadlockReader.PgDeadlockRow row) =>
        new(
            row.DeadlockHash,
            new[]
            {
                /* AlertContextBuilders.TruncateText, not the raw statement: every other query-text field
                   this codebase puts on an AlertIncident (Blocked Query/Blocking Query/Victim SQL/Query,
                   AlertContextBuilders.cs:80,82,165,491,561) goes through it first, and deadlock victim
                   statements are commonly multi-line formatted DML with no SQL-Server-style length cap —
                   without this, a multi-line statement breaks the one-line-per-incident rendering and an
                   unbounded one can bloat the stored context past what Slack/Teams will accept. */
                string.IsNullOrWhiteSpace(row.VictimStatement)
                    ? $"victim pid {row.VictimPid}, {row.ParticipantCount} participant(s)"
                    : AlertContextBuilders.TruncateText(row.VictimStatement!),
            });

    /// <summary>
    /// Which root blocker each captured chain belongs to, worst sample first per root — pulled out of
    /// <see cref="EvaluatePgBlockingAsync"/> for the same testability reason as
    /// <see cref="BuildPgDeadlockIncident"/>.
    /// <para><see cref="EvaluatePgBlockingAsync"/> feeds this from
    /// <see cref="DarlingPgBlockingReader.GetPgBlockingChainsDedupedByRootAsync"/> (#2714), which already
    /// dedupes by root INSIDE the query, ordered worst-first (widest chain, then deepest, then most recent) —
    /// see that method's own doc comment. So the rows arriving here are typically already at most one per
    /// root, and this method's own
    /// "keep the FIRST row seen per root" dedup is now a no-op safety net rather than the only thing standing
    /// between a real distinct root and an undercount — kept because the sentinel-identity handling below
    /// (<c>RootBackendId == 0</c>) is still load-bearing regardless of which reader supplies the rows, and
    /// because nothing stops a future call site from wiring this to the raw, non-deduped
    /// <see cref="DarlingPgBlockingReader.GetPgBlockingChainsAsync"/> again (guarded by
    /// <c>EvaluatePgBlockingAsync_CallsTheDedupedByRootReader_NotTheRawOne</c>).</para>
    /// <para><b><c>RootBackendId == 0</c> is the vanished-blocker sentinel, and it needs its OWN identity to
    /// dedupe against, not the raw backend id.</b> <c>PgBlockingCollector</c> writes
    /// <c>coalesce(blocker.backend_id, 0)</c> when the root's own row had already left
    /// <c>pg_stat_activity</c> by capture time, so every genuinely different vanished-root incident shares
    /// the literal value 0 — <c>DarlingPgBlockingReader</c>'s own <c>recurrence</c> CTE excludes
    /// <c>blocking_backend_id &lt;&gt; 0</c> for the identical reason. Two failure modes sit on either side
    /// of this, and both were caught by review before shipping:
    /// <list type="bullet">
    /// <item>Grouping by the raw <c>RootBackendId</c> (as if 0 were a real id) collapses two UNRELATED
    /// vanished-root incidents into one entry and merges their fingerprints — an undercount.</item>
    /// <item>Never deduping sentinel rows at all re-introduces the #1091/#2704/#2708 re-fire class for
    /// exactly this case: the SAME persisting vanished-root block, sampled every sweep, would add a new
    /// list entry every cycle, so <see cref="RollingCountAlertGate"/>'s watermark keeps climbing and the
    /// alert re-fires every cooldown for one ongoing incident.</item>
    /// </list>
    /// The fix is <c>RootPid</c> as the sentinel case's dedup identity — the same value
    /// <see cref="BuildPgBlockingIncident"/> already folds into that case's <c>DedupKey</c> — which narrows
    /// the risk to pid reuse inside one rolling 1-hour window, far smaller than either failure mode
    /// above.</para>
    /// </summary>
    internal static List<DarlingPgBlockingReader.PgBlockingChainRow> WorstPgBlockingChainPerRoot(
        IReadOnlyList<DarlingPgBlockingReader.PgBlockingChainRow> rows)
    {
        var result = new List<DarlingPgBlockingReader.PgBlockingChainRow>();
        var seenRealBackendIds = new HashSet<long>();
        var seenSentinelPids = new HashSet<int>();

        foreach (var row in rows)
        {
            /* Never deduping the sentinel at all (an earlier version of this method) traded one bug for
               another: the SAME persisting vanished-root block, sampled every sweep, would then add a NEW
               list entry every cycle — RollingCountAlertGate's watermark keeps climbing as long as the
               count keeps climbing, re-firing "Blocking Detected" every cooldown for what is one ongoing
               incident (exactly the #1091/#2704/#2708 class this whole design exists to be immune to,
               reintroduced specifically for this case). Deduping the sentinel by RootPid instead is the
               narrower, correct trade: BuildPgBlockingIncident already treats RootPid as the sentinel
               case's usable identity (it is folded into that case's DedupKey below), and pid reuse inside
               one rolling 1-hour window is a far smaller risk than guaranteed re-alerting on every sweep
               for any persisting vanished-root block. */
            var isNew = row.RootBackendId == 0
                ? seenSentinelPids.Add(row.RootPid)
                : seenRealBackendIds.Add(row.RootBackendId);

            if (isNew)
            {
                result.Add(row);
            }
        }

        return result;
    }

    /// <summary>
    /// Pure mapping, pulled out of <see cref="EvaluatePgBlockingAsync"/> for the same testability reason as
    /// <see cref="BuildPgDeadlockIncident"/>. The dedup key is the root's synthetic backend identity (stable
    /// across samples of the same backend, unlike a reused pid — see the collector's own doc comment), not
    /// the pid alone.
    /// </summary>
    internal static AlertIncident BuildPgBlockingIncident(DarlingPgBlockingReader.PgBlockingChainRow row) =>
        new(
            /* The vanished-blocker sentinel (RootBackendId == 0, see WorstPgBlockingChainPerRoot's doc
               comment) needs a DedupKey too, not just a place in the list: IncidentCooldown.BuildKeys
               (PerformanceMonitor.Notifications/IncidentCooldown.cs) does incidents.Select(i =>
               i.DedupKey).Distinct() to build one cooldown key per fingerprint, so two genuinely distinct
               sentinel incidents both keyed "0" would collapse into one cooldown slot downstream — an
               unrelated PRIOR vanished-root incident's cooldown silently suppressing a genuinely NEW one's
               delivery, even though WorstPgBlockingChainPerRoot correctly kept both as separate list
               entries. Folding in RootPid and CapturedAt makes the sentinel case's key unique per incident
               the same way a real backend id already is on its own. */
            row.RootBackendId == 0
                ? string.Create(CultureInfo.InvariantCulture, $"0-pid{row.RootPid}-{row.CapturedAt:O}")
                : row.RootBackendId.ToString(CultureInfo.InvariantCulture),
            new[]
            {
                $"root pid {row.RootPid} blocking {row.TotalVictims} session(s)"
                    + (row.Databases.Length > 0 ? $" in [{string.Join(", ", row.Databases)}]" : string.Empty)
                    /* AlertContextBuilders.TruncateText — same reasoning as BuildPgDeadlockIncident's
                       VictimStatement: root queries are commonly multi-line and otherwise unbounded. */
                    + (string.IsNullOrWhiteSpace(row.RootQuery)
                        ? string.Empty
                        : $": {AlertContextBuilders.TruncateText(row.RootQuery!)}"),
            },
            Database: row.Databases.Length > 0 ? row.Databases[0] : null);

    /// <summary>
    /// The newest collected CPU sample for one server, the read that gates CPU alerting. $1 server_id.
    ///
    /// <para><b>Ordered on <c>collection_time</c>, the hypertable's own partition column.</b>
    /// <c>TimescaleSupport.CreateHypertableSql</c> partitions every collector table on its
    /// <c>PrefixTimeColumnName</c>, and <c>CpuUtilizationCollector</c> does not override the
    /// <c>collection_time</c> default, so <c>sample_time</c> is an ordinary payload column carrying neither
    /// an index nor partition affinity. Ordering on the dimension is what earns TimescaleDB's ORDERED
    /// ChunkAppend: it walks chunks newest-first and stops at the first one that yields a row, so every older
    /// chunk plans as <c>never executed</c> and the read costs the same against thirty chunks as against one,
    /// compressed chunks included. Ordering on <c>sample_time</c> instead appends every chunk and top-N sorts
    /// the server's whole retained history to return a single row — 47,752 rows and 839 buffers at thirty
    /// one-day chunks, once per server per alert tick.</para>
    ///
    /// <para><b>The <c>sample_time</c> tiebreak is load-bearing, not decoration.</b> One poll writes up to 60
    /// ring-buffer samples under a single <c>collection_time</c>, and without it the read returns whichever of
    /// them the index reaches first — measured 59 minutes stale. The batch with the newest
    /// <c>collection_time</c> is always the one holding the newest <c>sample_time</c>, because the collector's
    /// watermark IS <c>sample_time</c>, so a poll only ever inserts samples above the previous high-water
    /// mark.</para>
    ///
    /// <para><b>No time predicate, deliberately.</b> <c>sample_time</c> is the monitored server's LOCAL wall
    /// clock on the ring-buffer arm and UTC on the Azure SQL DB arm — two frames in one column, pinned in both
    /// directions by <c>CollectorTimestampFrameTests</c>. So <c>sample_time &gt; now() - INTERVAL '...'</c>
    /// compares two different clocks and returns ZERO rows for every server behind the store's, which reads as
    /// "this server has no CPU data" rather than as an error: no exception, no log line, CPU alerting simply
    /// stops. <c>collection_time</c> IS naive UTC and a bound on it would be frame-correct, but it buys nothing
    /// here — ordered append already touches one chunk — and costs two failure modes of its own. It drops a
    /// server that has stopped reporting out of alerting entirely, and a bare <c>now()</c> is a
    /// <c>timestamptz</c> whose comparison against a naive column is re-framed by the STORE session's own
    /// TimeZone (measured: a 1-hour window becomes 4 hours on an <c>America/New_York</c> store, and would
    /// invert east of UTC). An ORDER BY carries no clock frame at all. Internal so the shape and the
    /// same-row-across-offsets behaviour are both pinned by test.</para>
    ///
    /// <para><b>Both stamps, and the UTC twin is the identity where the row has one (#3744; V134, #3653
    /// item 13).</b> Since that rung every row carries the same instant in UTC (<c>sample_time_utc</c>) beside
    /// the local stamp, and the windowed CPU reads prefer it. This read projects both, and
    /// <see cref="ReadLatestCpuAsync"/> hands the gate <c>sample_time_utc ?? sample_time</c> as the CPU alert
    /// gate's OBSERVATION IDENTITY (#3282): the stored UTC instant on every row written since the rung, the
    /// local stamp on a pre-rung row (the twin is NULL there; nothing was backfilled, for the reason V134
    /// gives). #3730 left this read on the local stamp on purpose and wrote the trap down: the gate then
    /// compared identities by <c>&gt;</c>, so a frame that changed once at the upgrade would have read every
    /// first post-rung UTC instant as OLDER than the last local one on every server EAST of UTC and frozen
    /// CPU alerting for one offset's worth of hours, and counted one stale sample as fresh west of it. The same
    /// <c>&gt;</c> was also why the gate went blind for the repeated hour after every autumn fall-back on a
    /// non-UTC server — the local clock runs backwards and every new sample read as stale. #3744 changed the
    /// gate's test to EQUALITY (<c>AlertEngine.ObservePersistenceAsync</c> says why order was the wrong test):
    /// a different instant is a different sample whichever clock stamped it, so the frame switch costs at most
    /// one extra count of one sample per server, once, and the identity can be the honest UTC instant.</para>
    ///
    /// <para><b>The ORDER BY stays on the local stamp.</b> The tiebreak orders rows INSIDE one batch, where
    /// every row shares a frame (a poll writes the twin on every row or on none), so it carries no frame
    /// problem; ordering on the twin, or on a <c>COALESCE</c> of the two, would compare a pre-rung LOCAL stamp
    /// against a post-rung UTC one across batches and, east of UTC, sort a stale pre-rung row as newest for one
    /// offset's worth of hours after the upgrade. What the tiebreak does leave is the one-batch fall-back quirk
    /// <c>TimeHonestyRungTests</c> shows: inside the single poll that straddles the transition the EDT-side
    /// sample sorts as newest by local stamp, a reading stale by one sample, once a year — and with the gate on
    /// equality that costs nothing downstream, because the next batch's identities differ from it. Pinned by
    /// <c>TimeHonestyRungTests</c> and <c>LatestCpuReadShapeTests</c>.</para>
    /// </summary>
    internal const string LatestCpuSql = @"
SELECT sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time, sample_time_utc
FROM cpu_utilization_stats
WHERE server_id = $1
ORDER BY collection_time DESC, sample_time DESC
LIMIT 1";

    /// <summary>
    /// Runs <see cref="LatestCpuSql"/> and shapes it for the snapshot — Lite's overview read
    /// (LocalDataService.Overview.cs:37-51) against the raw PG table, and the
    /// ServerSummaryItem.TotalCpuPercent derivation (:140-141): total = SQL + (other ?? 0),
    /// null when there is no SQL sample (Azure SQL DB stores other as 0; Linux stores NULL).
    /// </summary>
    private Task<(double? SqlCpu, double? TotalCpu, DateTime? SampleTime)> ReadLatestCpuAsync(
        int serverId, CancellationToken cancellationToken)
        /* #3854: the seventh read on the alert pass, and the only one of the seven that lives outside the
           self-alert evaluator — routed through the SAME shared seam for the same reason. It runs on
           AlertPassCommandTimeoutSeconds (the comment at its call site says so, and calls it the first store
           read of the pass, so under contention it is the first to fail), and its swallowed failure is
           counted on this same counter under LatestCpuReadName. A retry seam that covered the evaluator's
           six and left this one would leave the pass's FIRST read as its only unretried one. */
        => DarlingAlertReadAdapter.ExecuteWithOneRetryAsync(
            token => ReadLatestCpuCoreAsync(serverId, token),
            serverId.ToString(CultureInfo.InvariantCulture),
            LatestCpuReadName,
            _readFailures,
            Task.Delay,
            cancellationToken);

    /// <summary>
    /// The counter's name for the latest-CPU read, one constant so the retry count and the failure count
    /// cannot spell it two ways (#3854).
    /// </summary>
    internal const string LatestCpuReadName = "latest-CPU read";

    /// <summary>The read itself, byte-identical to what shipped before #3854 routed it through the seam.</summary>
    private async Task<(double? SqlCpu, double? TotalCpu, DateTime? SampleTime)> ReadLatestCpuCoreAsync(int serverId, CancellationToken cancellationToken)
    {
        double? sqlCpu = null;
        double? otherCpu = null;
        DateTime? sampleTime = null;

        await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(
            LatestCpuSql, connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            sqlCpu = reader.IsDBNull(0) ? null : Convert.ToDouble(reader.GetValue(0), CultureInfo.InvariantCulture);
            otherCpu = reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture);
            /* #3282: the sample's own instant, which is the persistence gate's observation identity. #3744:
               the row's UTC twin (V134's sample_time_utc, ordinal 3) where the store has one, the local stamp
               (ordinal 2) on a pre-rung row — the same `??` Lite's MainWindow.AlertEngine applies to its
               overview read's two columns, so both SKUs hand the shared gate the same identity for the same
               row. Left Kind=Unspecified as both come off `timestamp` columns (the twin is naive UTC by the
               store-wide convention; the local stamp is the server's wall clock on the ring-buffer arm and UTC
               on Azure's): the value is only ever compared for EQUALITY against the one this same read stored
               last sweep, so coercing either to Kind=Utc would buy nothing and, through Npgsql's timestamptz
               inference on the way back into the store, could shift the persisted copy by the host's offset. */
            var sampleTimeUtc = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);
            var sampleTimeLocal = reader.IsDBNull(2) ? (DateTime?)null : reader.GetDateTime(2);
            sampleTime = sampleTimeUtc ?? sampleTimeLocal;
        }

        double? totalCpu = sqlCpu.HasValue ? sqlCpu.Value + (otherCpu ?? 0) : null;
        return (sqlCpu, totalCpu, sampleTime);
    }

    /// <summary>
    /// Gathers the store disk-pressure sample and hands it to the Stage 4 evaluator (fleet-level). The store
    /// size is context only, read from the hourly self-metrics series rather than measured here (#3199); the
    /// store volume's free/total space is
    /// resolved from the MANAGED data directory's drive — the bundled store this service owns and must protect.
    /// In bring-your-own mode the store can be a remote Postgres whose disk the service cannot see, so
    /// free/total stay null and the evaluator no-ops (never a false alarm — the operator owns their own
    /// PostgreSQL's disk monitoring, consistent with the BYO posture elsewhere). Failure-isolated: a bad
    /// sample logs at Debug and skips this tick, never breaking the loop.
    /// </summary>
    private async Task EvaluateStoreDiskPressureAsync(DarlingConfig config, CancellationToken cancellationToken)
    {
        long? storeSizeBytes = await ReadStoreSizeBytesAsync(cancellationToken);

        long? freeBytes = null;
        long? totalBytes = null;
        if (config.Postgres.Managed && OperatingSystem.IsWindows())
        {
            try
            {
                var dataDirectory = DarlingManagedPostgres.ResolveDataDirectory(config.Postgres);
                var root = Path.GetPathRoot(Path.GetFullPath(dataDirectory));
                if (!string.IsNullOrEmpty(root))
                {
                    var drive = new DriveInfo(root);
                    if (drive.IsReady)
                    {
                        freeBytes = drive.TotalFreeSpace;
                        totalBytes = drive.TotalSize;
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* Best-effort: an unreadable drive just means no disk signal this tick. */
                /* NOT counted by #3013's swallowed-read counter: a local filesystem read, not a store read. #3013's
                   mechanism is store latency crossing the alert pass's deadline, which has no bearing on DriveInfo. */
                _logger.LogDebug("Store disk-pressure check: could not read the store volume free space: {Message}", ex.Message);
            }
        }

        /* EvaluateDiskPressureAsync (not ApplyDiskPressureAsync) so a throwing seam — e.g. a mute rule's
           Matches() in the pre-deliver mute check — is isolated inside the evaluator, exactly like the
           per-server EvaluateStoreAlertsAsync. This sweep-loop body has no catch-all of its own, so an
           un-isolated throw here would stop collection for the whole fleet. */
        await _selfAlerts!.EvaluateDiskPressureAsync(freeBytes, totalBytes, storeSizeBytes, cancellationToken);
    }

    /// <summary>
    /// #4215: builds the <see cref="DarlingSelfAlertEvaluator.StoreSettingsReport"/> and hands it to
    /// <see cref="DarlingSelfAlertEvaluator.EvaluateStoreSettingsAsync"/>, mirroring
    /// <see cref="EvaluateStoreDiskPressureAsync"/>'s split between "gather the facts here" and "judge them in
    /// the evaluator". <paramref name="managedConfWriteResult"/> and <paramref name="managedUsedLastGoodConf"/>
    /// are this start's own in-process facts, carried down unchanged from <c>ExecuteAsync</c>; the rejected-value
    /// list is the one genuine store read, isolated in <see cref="ReadRejectedManagedConfSettingNamesAsync"/> so
    /// a throw there degrades to <c>null</c> (unknown) rather than stopping the fleet loop:
    /// unlike <see cref="UsedLastGoodConf"/>/<see cref="HandEdited"/>, a rejected-value row is one
    /// of the three conditions the alert fires on, so "empty" and "unknown" cannot share a representation.
    /// </summary>
    private async Task EvaluateStoreSettingsAsync(
        DarlingConfig config, ManagedConfWriteResult? managedConfWriteResult, bool managedUsedLastGoodConf,
        ManagedConfMigrationOutcome? managedConfVerification,
        CancellationToken cancellationToken)
    {
        var isManagedStore = config.Postgres.Managed && OperatingSystem.IsWindows();
        IReadOnlyList<string>? rejectedSettingNames = isManagedStore
            ? await ReadRejectedManagedConfSettingNamesAsync(cancellationToken)
            : [];

        var report = new DarlingSelfAlertEvaluator.StoreSettingsReport(
            isManagedStore, managedUsedLastGoodConf, managedConfWriteResult?.HandEdited ?? false, rejectedSettingNames,
            managedConfVerification);

        /* EvaluateStoreSettingsAsync (not ApplyStoreSettingsAsync) so a throwing seam — the shared mute check —
           is isolated inside the evaluator, exactly like the disk-pressure call just above. */
        await _selfAlerts!.EvaluateStoreSettingsAsync(report, cancellationToken);
    }

    /// <summary>Every setting name <c>collect.managed_conf_verdicts</c> (V146) currently holds as
    /// <see cref="HostSettingVerdict.RejectedValue"/> — the store-settings self-alert's one real store read,
    /// and one of the three conditions the alert fires on. Returns <c>null</c> (unknown), not an empty list,
    /// on a failed read: a <c>RejectedValue</c> row is judgeable evidence, not context, so this read is counted
    /// on #3013's swallowed-read counter like the other alert reads — <see cref="ReadStoreSizeBytesAsync"/>'s
    /// Debug/empty posture is for a read that is ONLY context and does not apply here.</summary>
    private async Task<IReadOnlyList<string>?> ReadRejectedManagedConfSettingNamesAsync(CancellationToken cancellationToken)
    {
        var readClock = Stopwatch.StartNew();
        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);
            readClock.Restart();
            var stored = await DarlingStoreHostProfile.ReadStoredManagedConfVerdictsAsync(connection, cancellationToken);
            return stored
                .Where(static row => row.Verdict == HostSettingVerdict.RejectedValue)
                .Select(static row => row.SettingName)
                .ToArray();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "Store settings self-alert: could not read stored managed-conf verdicts after {ElapsedMs} ms — "
                + "the rejected-settings condition stays unresolved this tick: {Message}",
                readClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(null, "store settings self-alert (managed-conf verdicts)", readClock.ElapsedMilliseconds);
            return null;
        }
    }

    /// <summary>
    /// #3304: the fleet-level custom-alert-rule integrity check. Asks the <see cref="CustomAlertEvaluator"/> to
    /// re-parse the enabled rules against the live catalog and flag broken / never-firing ones (the current
    /// monitored (server_id, storage name) set feeds the 0-server-scope case, including #3350 tag scope), then hands the report to
    /// <see cref="DarlingSelfAlertEvaluator.EvaluateCustomRuleHealthAsync"/>, which raises ONE aggregated
    /// self-health alert. Both halves are failure-isolated internally (BuildHealthReportAsync returns null on a
    /// load error and this method then HOLDS the standing alert rather than resolving it; the Evaluate* wrapper
    /// contains a throwing mute seam), matching the disk-pressure posture — this sweep-loop body has no catch-all
    /// of its own. Only called when both evaluators are non-null (guarded at the call site).
    /// </summary>
    private async Task EvaluateCustomAlertRuleHealthAsync(List<ServerLoopState> servers, CancellationToken cancellationToken)
    {
        // #3350: (server_id, storage name) pairs so the tag-scope 0-server case can be resolved by id (the tag
        // membership is keyed on server_id) while the 'servers' scope still matches on the storage name.
        var monitoredServers = servers
            .Where(s => !s.Retired)
            .Select(s => (s.Config.ServerId, s.Config.StorageName))
            .ToList();

        var report = await _customAlertEvaluator!.BuildHealthReportAsync(monitoredServers, cancellationToken);

        // Null = the rule load failed this tick; HOLD the standing alert (never resolve on uncertainty).
        if (report is not null)
        {
            await _selfAlerts!.EvaluateCustomRuleHealthAsync(report, cancellationToken);
        }
    }

    /// <summary>
    /// #3305: the fleet-level custom-alert-state reconcile. Force-resolves and cleans the persisted state of
    /// DISABLED rules (whose open incidents the evaluator's enabled-only sweep would otherwise orphan forever)
    /// and of servers that have left a rule's scope. Deleted rules are handled on the delete path instead
    /// (their state cascades away). Hands the evaluator the current fleet — server id → (storage name for the
    /// scope check, display name for the recovery row); a server absent from it has left monitoring. Runs on
    /// the fleet-global health cadence; the evaluator's method is failure-isolated per row so this never stops
    /// the sweep. Only called when the custom-alert evaluator is non-null (guarded at the call site).
    /// </summary>
    private async Task ReconcileCustomAlertStateAsync(List<ServerLoopState> servers, CancellationToken cancellationToken)
    {
        var monitored = servers
            .Where(s => !s.Retired)
            .ToDictionary(s => s.Config.ServerId, s => (s.Config.StorageName, s.Config.DisplayName));

        await _customAlertEvaluator!.ReconcileStateAsync(monitored, cancellationToken);
    }

    /// <summary>
    /// The store database's on-disk size in bytes — context for the disk-pressure alert text, read from the
    /// whole-store row the hourly self-metrics sweep records
    /// (<see cref="StoreSelfMetrics.LatestStoreSizeSql"/>). Failure-isolated to null (Debug) so a transient
    /// store hiccup never breaks the disk-pressure check.
    ///
    /// <para><b>Why this is not <c>pg_database_size_stats</c> (#3199).</b> This method is one of the ten commands
    /// awaited inline on the collection loop's serial thread, under
    /// <see cref="ServiceCommandDeadlines.SerialLoopSeconds"/> — and running <c>pg_database_size_stats</c> here
    /// made it the only one of the ten whose cost scaled with the store, against a bound floored on 6.2 ms
    /// measured on a 4.05 GB fixture. On a 225 GiB production store the same call measured
    /// <b>3,177 ms</b> — 64% of the 5 s bound, 1.57x headroom where the derivation claimed ~806x.</para>
    ///
    /// <para><b>The cancelled statements are the visible 2%; the cost is the other 98%.</b> Thirteen
    /// samples on that store spanned 2,090-3,745 ms (mean ~2.5 s) and NONE crossed 5 s, while cancels for
    /// this statement ran 0-9 a day over six days — mean 5.8 against a nominal ~288 iterations, so 2.0%.
    /// The remaining 98% cost ~2.5 s each and leave no trace at all: under the deadline so no cancel, not a
    /// collector run so no <c>collection_log</c> row. That is ~11.9 minutes a day of this loop's wall time
    /// spent computing a number the store already holds, and it is the finding — not the eight log lines
    /// that started it, and it does not depend on knowing what ends a cluster, the question #3199's own
    /// correction left open.</para>
    ///
    /// <para><b>What that stall does and does not displace, because the difference matters.</b> This check
    /// runs AFTER the per-server launches fan out in the same tick, and
    /// <see cref="SweepWatchdogSeconds"/> clocks per-server BODIES from their own launch rather than
    /// clocking this loop — so 2.5 s here consumes no watchdog budget and delays no already-launched body.
    /// What it does is stretch one collection tick in twenty from <see cref="s_sweepInterval"/> to ~17.5 s,
    /// pushing that tick's remaining maintenance and the NEXT tick's launches out by that much on a
    /// single-threaded loop whose delivered cadence already runs behind its schedule at fleet scale. The
    /// bound genuinely at risk is the command's own: at 42-75% of a 5 s <c>CommandTimeout</c> floored on
    /// millisecond reads, anything competing takes the rest.</para>
    ///
    /// <para><b>The walk is relocated, not eliminated.</b> The row this reads is written by
    /// <see cref="StoreSelfMetrics.StoreInsertSql"/>, which runs <c>pg_database_size_stats</c> itself — so the
    /// store-wide walk still happens, hourly, under
    /// <see cref="StoreSelfMetrics.SweepTimeoutSeconds"/> (300 s, ~120x the mean measured cost, sized by
    /// #2317 against a production store rather than a fixture). That sweep is awaited on this same thread,
    /// so what changes is the frequency and the budget, not the isolation: ~312 executions a day become 24,
    /// and the ~11.9 min/day leaves the 5 s regime specifically. The ~31,000x is the single read's latency
    /// on this path (3,177 ms against 0.101 ms cold, same store) and is never the change's overall
    /// effect.</para>
    /// </summary>
    private async Task<long?> ReadStoreSizeBytesAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(StoreSelfMetrics.LatestStoreSizeSql, connection) { CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds };
            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result is null || result == DBNull.Value ? null : Convert.ToInt64(result, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* NOT counted by #3013's swallowed-read counter: this read is CONTEXT for the alert text, not the
               evidence the alert is judged on - that is freeBytes/totalBytes above. Losing it costs the message a
               number; it does not make the condition unjudgeable. */
            _logger.LogDebug("Store disk-pressure check: could not read the recorded store size: {Message}", ex.Message);
            return null;
        }
    }

    /// <summary>
    /// The #1581 policy-job self-heal check (fleet-level, hourly, Timescale-only): read every stuck POLICY
    /// job (<see cref="TimescaleSupport.ReadStuckPolicyJobsAsync(NpgsqlConnection, DateTime, ILogger, CancellationToken)"/>)
    /// and hand them to the self-alert evaluator's re-arm-once/escalate machine, wired to
    /// <see cref="TimescaleSupport.TryRearmJobAsync"/> on the SAME open connection. One stuck job whose
    /// <c>next_start</c> went <c>-infinity</c> silently halts a whole tier of the store — the field incident — so
    /// this makes it visible AND self-heals it. Failure-isolated at the worker level too (the connection open is
    /// OUTSIDE the evaluator's own isolation): a store hiccup logs and skips this check, never aborting the sweep —
    /// mirroring the purge / disk-check isolation.
    ///
    /// <para><b>The method KEEPS its #1581 name while the check it performs is no longer compression-only
    /// (#3816), and that is deliberate.</b> Its name and its POSITION on this tick are pinned as source text
    /// by three sibling issues' tests — <c>RefreshCeilingStalenessTests</c> holds the signature,
    /// <c>RetentionReevaluationTests</c> holds that #3812's retention re-evaluation is awaited AFTER it, and
    /// <c>TimescaleAvailabilityReprobeTests</c> holds that #3815's re-probe runs BEFORE it and outside the
    /// availability gate. Those three facts are the tick's ordering contract; renaming the method to match
    /// this issue's vocabulary would rewrite all three pins to say the same thing about a different
    /// identifier, and the ordering is what they are for.</para>
    ///
    /// <para>What the widening means here: compression, continuous-aggregate refresh and retention policies
    /// all reach the same machine on the same pass, banded per family by the evaluator (different metric
    /// name, severity and text each) — and every pass now writes one unconditional summary line, so a tick
    /// that found nothing can still be shown to have run.</para>
    ///
    /// <para><b>The stuck-job read may hold this method for <see cref="TimescaleSupport.StuckPolicyJobConfirmDelay"/>
    /// (#3575)</b>, and only on a pass where a job's <c>-infinity</c> arm tripped: the read re-executes its query
    /// after that delay and reports the job only if the arm still trips, because TimescaleDB's view assembles
    /// <c>next_start</c> and <c>job_status</c> from independent sources and reads the dead-job shape for a few
    /// milliseconds at either edge of every healthy run. This method is awaited on the serial sweep loop, so
    /// that five seconds is a once-an-hour worst case paid only when there was something to confirm; the
    /// budgeting argument is on the constant. The evaluator downstream receives a list that has already been
    /// confirmed and does not second-guess it.</para>
    /// </summary>
    private async Task EvaluateCompressionJobHealthAsync(CancellationToken cancellationToken)
    {
        var readClock = Stopwatch.StartNew();
        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);
            readClock.Restart();

            /* #1778: report what compression is DOING before deciding whether anything is stuck. The field
               could see hours-long compressions only in hindsight, by their effect on disk; this puts a
               running compression, its elapsed time, and any eligible-chunk backlog in the log while it is
               still happening. Read on the same connection and the same hourly cadence — the check that
               already exists for this exact subsystem, rather than a second timer for one log line. */
            TimescaleSupport.LogCompressionActivity(
                await TimescaleSupport.ReadCompressionActivityAsync(connection, _logger, cancellationToken),
                DateTime.UtcNow,
                _logger);
            readClock.Restart();

            /* #3044: the heaviest hourly refresh's runtime against the SLOT it has to fit inside, which is a
               different bound from #2136's below and the one the compression phase grid rests on. The build-time
               assertion on HeaviestHourlyRefreshObservedCeilingSeconds bounds a CONSTANT; the thing it bounds is
               a runtime that moves with data volume, so it fires when someone edits the constant and never when
               reality changes underneath it. This is that bound applied to the live figure, on the sweep that
               already reads the job catalog — no new collector, no new timer, and the per-run history is already
               in collect.store_metrics. A log line rather than a band or an alert: see
               TimescaleSupport.LogHeaviestRefreshSlotHeadroom for why, including why #2136's knob happening to
               equal one slot today is not a substitute. */
            var heaviestRefresh = await TimescaleSupport.ReadHeaviestRefreshRuntimeAsync(
                connection, _logger, cancellationToken);
            TimescaleSupport.LogHeaviestRefreshSlotHeadroom(heaviestRefresh, _logger);

            /* #3182: whether either ceiling CONSTANT has been overtaken, which is a different finding from
               the band above and is levelled and rate-limited separately. The band answers "does this run
               fit in the window the grid gives it"; this answers "is the number the grid was DERIVED from
               still a maximum". The defect it exists for is that the answer to the second can be NO while
               the first says InsideSlot and logs at Debug — a recorded ceiling was overtaken on roughly half
               the runs of its own job and nothing anywhere said so. Called BESIDE the band watch rather than
               inside its switch precisely so it is reachable from every band. */
            if (heaviestRefresh is not null)
            {
                TimescaleSupport.LogRefreshCeilingStaleness(
                    TimescaleSupport.HeaviestRefreshCeilingConstantName,
                    TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds,
                    heaviestRefresh.View,
                    heaviestRefresh.LastRunSeconds,
                    _refreshCeilingStaleness,
                    _logger);
            }

            /* One restart per I/O boundary, not per statement: the two log calls above are synchronous, so a
               restart between them would hand the catch below a ~0 ms elapsed for the READ that actually
               faulted. Raised by review. */
            readClock.Restart();

            /* And the same question for the OTHER twelve hourly refreshes, which had no live reading keyed
               to a view anywhere in the product before #3182. Their ceiling is what
               CompressionPhaseGuardMinutes' DECLARED width is checked against (#3188), so a light refresh
               running past that width leaves a compression policy able to start while the refresh still
               holds AccessShareLock — #3012's convoy. It used to be the width's INPUT, rounded up to a whole
               minute, and the difference is which way the failure goes: a longer light refresh widened the
               width silently, and now it goes red. One constant covers all twelve, so the rate limit is
               keyed on the constant and the loop cannot make it twelve times looser than it reads. */
            foreach (var lightRefresh in await TimescaleSupport.ReadOtherHourlyRefreshRuntimesAsync(
                connection, _logger, cancellationToken))
            {
                TimescaleSupport.LogRefreshCeilingStaleness(
                    TimescaleSupport.OtherRefreshCeilingConstantName,
                    TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds,
                    lightRefresh.View,
                    lightRefresh.LastRunSeconds,
                    _refreshCeilingStaleness,
                    _logger);

                /* #3185: and whether the run outran the gap the band gave its own COST CLASS, which is a
                   third fact with a third remedy. The band separates the members whose runs are long enough
                   for CPU and I/O contention to matter and leaves the rest one minute apart, and which
                   members those are is derived from each view's GROUP BY rather than listed — so the one
                   thing that can go silently wrong is a view being slow for a reason its group key does not
                   show. That is not visible to any build-time rule, and it is not visible to the ceiling
                   watch above either: a bounded member at 100 s outran its 60 s step while sitting a long
                   way under the 226.8 s ceiling. Same read, same reading, same rate limiter — keyed on the
                   constant the reading falsified, so the class questions do not suppress each other. */
                TimescaleSupport.LogLightRefreshSpacingBreach(
                    lightRefresh.View,
                    lightRefresh.LastRunSeconds,
                    _refreshCeilingStaleness,
                    _logger);
            }

            readClock.Restart();

            /* #3816: one read, every policy family this product owns — compression (unscoped, as since
               #1581), plus collect-scoped continuous-aggregate refresh and retention. Same statement, same
               confirm-read, same hourly cadence and the SAME connection: the widening is in the WHERE and in
               what the evaluator does with the family, not in what this tick costs. The reading carries the
               stuck list AND the census of every job it looked at, because the unconditional summary line
               and the total_failures arm are both about the jobs that are fine. */
            var policyJobs = await TimescaleSupport.ReadStuckPolicyJobsAsync(
                connection, DateTime.UtcNow, _logger, cancellationToken);
            readClock.Restart();
            await _selfAlerts!.EvaluatePolicyJobsAsync(
                policyJobs,
                jobId => TimescaleSupport.TryRearmJobAsync(connection, jobId, _logger, cancellationToken),
                cancellationToken);
            readClock.Restart();

            /* #2136: the Store Job Over Cadence check rides the same connection and hourly cadence — a
               background job whose last successful run reached the warning share of its own schedule
               interval is the store outgrowing its job schedule, the number an onboarding wave moves
               first. Same isolation posture: the evaluator wraps itself, and this whole method's catch
               is the backstop. */
            var cadenceReadings = await TimescaleSupport.ReadJobCadenceReadingsAsync(
                connection, _logger, cancellationToken);
            readClock.Restart();

            /* #2813: the Retention Held check rides the same connection and hourly cadence. A retention
               policy the #1680/#1877 coverage gate has paused reports total_failures = 0 and a plausible
               last run — it is not failing, it is stopped — so it is invisible to every stored metric and
               went unnoticed on the production store for 16 days while that tier grew to 4.5x its horizon.
               Judged on the CONSEQUENCE (held AND the tier past its own horizon), never on the paused flag
               alone, which is the normal state of every freshly created policy. Same isolation posture. */
            var retentionHolds = await TimescaleSupport.ReadRetentionHoldReadingsAsync(
                connection, _logger, cancellationToken);
            readClock.Restart();

            /* #4299: the three raw jobs are excluded from JobCadenceReadSql (they are never
               timescaledb_information.jobs.scheduled, so job_stats carries no meaningful last_run_duration
               for them) and take their #2136 cadence from the SERVICE'S OWN trigger instead — the #2136
               cadence check takes its cadence from the trigger. The duration is the last RECORDED
               "ran" outcome's elapsed_ms (TimescaleSupport.RawLastPurgeRecord, read below via the SAME
               rawPurgeOverHorizon reading the over-horizon check judges — reused, not re-read); the interval
               is the worker's own hourly Periodic pass, s_compressionCheckInterval, because that pass is the
               ONLY caller of the raw trigger (TriggerRawPurgeCoreAsync). No record, or a last outcome other
               than "ran", yields no cadence reading — being over horizon with no successful run is the #4299
               alert's job, not cadence's; a raw job that has never run at all must not read as running at
               0% of its interval, and a job whose last recorded attempt failed must not be graded on a
               duration that never happened. Merged into the list #2136's evaluator already iterates rather
               than a parallel check, so the raw jobs share JobOverCadence's exact firing, cooldown and
               resolution machinery instead of a second copy of it. */
            var rawPurgeOverHorizon = await TimescaleSupport.ReadRawPurgeOverHorizonReadingsAsync(
                connection, retentionHolds, _logger, cancellationToken);
            readClock.Restart();

            var rawCadenceReadings = RawCadenceReadings(rawPurgeOverHorizon, s_compressionCheckInterval);

            await _selfAlerts!.EvaluateStoreJobCadenceAsync(
                cadenceReadings.Concat(rawCadenceReadings).ToList(), cancellationToken);
            readClock.Restart();

            await _selfAlerts!.EvaluateRetentionHoldsAsync(retentionHolds, cancellationToken);
            readClock.Restart();

            /* #4299: the raw purge over-horizon check rides the SAME connection, hourly cadence and
               retentionHolds/rawPurgeOverHorizon readings as the checks just above — reused, not re-read, so
               this tick's raw rows are the exact ones EvaluateRetentionHoldsAsync and the cadence merge just
               judged. */
            await _selfAlerts!.EvaluateRawPurgeOverHorizonAsync(rawPurgeOverHorizon, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown — quiet and expected. */
        }
        catch (Exception ex)
        {
            /* WHAT THIS SITE'S ELAPSED CAN AND CANNOT CLAIM, stated because the shared finding sentence
               frames every entry against the alert pass's own command deadline and this one does not fit
               that frame.

               Each TimescaleSupport.Read*Async above catches `Exception ex when (ex is not
               OperationCanceledException)` internally, logs at Debug and returns an empty result, and so
               do the _selfAlerts Evaluate* wrappers. So a timeout on one of the reads this entry is NAMED
               for never reaches here — it is swallowed one level down. What reaches here is the connection
               open, a cancellation, or a genuine bug. And those reads run on
               TimescaleSupport.JobCatalogReadTimeoutSeconds (30 s), not
               DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds (10 s), so even when one did surface
               the 10 s bound would be the wrong thing to compare it to.

               The measurement stays, because a figure for the operation that actually faulted is still
               worth having and the clock boundaries above make it one operation's. What does not stay is
               any claim that it discriminates a client cutoff from a store fault at this site. */
            _logger.LogError("Compression-job health check failed after {ElapsedMs} ms: {Message}", readClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(
                null, "store background-job health reads (compression, job cadence, retention holds)", readClock.ElapsedMilliseconds);
        }
    }

    /// <summary>
    /// #4299: the raw-cadence merge pulled out of <see cref="EvaluateCompressionJobHealthAsync"/> as its
    /// own seam so it pins directly, without a connection. Behaviour is unchanged from the inline LINQ it
    /// replaces: only readings whose last recorded purge outcome is <c>"ran"</c> AND carries an elapsed-ms
    /// value become a cadence reading, at the given interval.
    /// </summary>
    internal static IReadOnlyList<StoreJobCadenceReading> RawCadenceReadings(
        IReadOnlyList<RawPurgeOverHorizonReading> readings, TimeSpan interval) =>
        readings
            .Where(reading => reading.LastPurge is { Outcome: "ran", ElapsedMs: long })
            .Select(reading => new StoreJobCadenceReading(
                reading.JobId,
                $"policy_retention {reading.HypertableName}",
                reading.LastPurge!.ElapsedMs,
                (long)interval.TotalMilliseconds))
            .ToList();

    /// <summary>
    /// The #3815 TimescaleDB availability re-probe (fleet-level, on the hourly store-maintenance tick, and
    /// only while <see cref="_timescaleAvailable"/> reads <c>false</c>): re-run
    /// <see cref="TimescaleSupport.TryEnableAsync"/> on a connection of its own and flip the latch when the
    /// store answers that the extension is there after all.
    ///
    /// <para><b>Why the call site is ABOVE the flag's gate and not inside it.</b> The latch decides whether
    /// the store background-job health check runs at all, and that check is the only surface that reports a
    /// dead compression job (#1581), a job running past its cadence (#2136) or a held retention policy
    /// (#2813). Put the correction inside the block the latch gates and it is unreachable in precisely the
    /// state it exists for — the false value suppresses its own repair, and the service is left with the
    /// #1581 field incident's backstop switched off and no way to say so. So the tick's guard is the due time
    /// alone, this runs first, and the compression read and the retention pass sit one level in behind the
    /// flag this may have just flipped.</para>
    ///
    /// <para><b>A DEDICATED connection, and nothing touches it after a <c>false</c> (#1922).</b>
    /// <c>CREATE EXTENSION IF NOT EXISTS timescaledb</c> TERMINATES the backend when the library is on disk
    /// but missing from <c>shared_preload_libraries</c>, and <see cref="TimescaleSupport.TryEnableAsync"/>
    /// turns that into <c>false</c> like any other failure — so its contract reads as "carry on in
    /// plain-PostgreSQL mode" while the connection it was handed is dead. This method is written to the same
    /// rule the start-path block is: a connection of its own out of the worker's pool, the result checked
    /// before anything else uses it, and the <c>false</c> arm returning straight to the <c>await using</c>
    /// that disposes it. Adding a second statement on <c>connection</c> below that check reintroduces the
    /// masking, on an hourly cadence rather than once a start.</para>
    ///
    /// <para><b>What the flip does, and what it deliberately leaves alone.</b> It restores the READS: the
    /// job health check, the <c>drop_chunks</c> branch of the daily purge, the self-metrics sweep's
    /// per-hypertable rows and the two provider delegates all consult the field at call time, so each picks
    /// the flip up on its next pass with no further wiring. It does NOT re-run the setup sequence — hypertable
    /// conversion, compression policies, continuous aggregates, retention policies. Those stay on the start
    /// path, the recovery line says so, and putting them on a cadence is #3817's subject, whose whole content
    /// is the ordering and the fire-and-forget backfill that sequence carries. Restoring the checks is where
    /// the damage in #3815 is: a store that was converted by an earlier start and lost only the flag is fully
    /// healed by the flip alone, and a store that was never converted at least regains the surface that can
    /// report it.</para>
    ///
    /// <para><b>The logging is split by TRANSITION, which is why the logger handed down is null.</b>
    /// <see cref="TimescaleSupport.TryEnableAsync"/> writes its own Information line on every outcome, which
    /// is right once at a start and is a line an hour forever on a store that is genuinely plain PostgreSQL —
    /// a fully supported configuration. So this passes no logger and says it itself: the recovery at
    /// Information, because a service that has been silently degraded since its start must announce that it
    /// no longer is; the unchanged pass at Debug, because "still plain PostgreSQL" is the same sentence the
    /// start already wrote. Failure-isolated with the three outcomes the retention pass uses — shutdown
    /// quiet, the budget its own WARNING, anything else a WARNING naming the message — and no rethrow, so
    /// the sweep loop never sees this pass fail.</para>
    /// </summary>
    private async Task ReprobeTimescaleAvailabilityAsync(CancellationToken cancellationToken)
    {
        var probeClock = Stopwatch.StartNew();
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(s_timescaleReprobeBudget);

        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(budget.Token);
            var available = await TimescaleSupport.TryEnableAsync(connection, null, budget.Token);
            if (!available)
            {
                _logger.LogDebug(
                    "TimescaleDB re-probe after {ElapsedMs} ms: still unavailable, so the store stays in plain-PostgreSQL mode and the next hourly store-maintenance tick probes again.",
                    probeClock.ElapsedMilliseconds);
                return;
            }

            _timescaleAvailable = true;
            _logger.LogInformation(
                "TimescaleDB is available after all - the store reports the extension present while this service has been running in plain-PostgreSQL mode, so the availability latch flips back on this tick WITHOUT a restart (#3815). The store background-job health check that latch gates - the compression-job self-heal (#1581), Store Job Over Cadence (#2136), Retention Held (#2813) and Raw Purge Over Horizon (#4299) - has been skipped on every tick since the start-path detection came back false, and runs again immediately after this line. Hypertable conversion, compression policies, continuous aggregates and the composer's covering indexes are re-converged on this same tick by the store-object convergence pass (#3817), which runs a few statements after this line and writes its own 'Store object convergence:' summary — so anything the start path left unbuilt while the latch was false is rebuilt now, not at the next restart. This pass took {ElapsedMs} ms, connection acquisition included.",
                probeClock.ElapsedMilliseconds);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown - quiet and expected. The next start probes this from scratch. */
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested)
        {
            _logger.LogWarning(
                "TimescaleDB re-probe exceeded its {BudgetSeconds}s budget after {ElapsedMs} ms and was cut short - the availability latch keeps the value it had and the next hourly tick probes again. A store that cannot answer CREATE EXTENSION IF NOT EXISTS and a one-row pg_extension read inside that budget is the finding here, not the extension.",
                (long)s_timescaleReprobeBudget.TotalSeconds, probeClock.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "TimescaleDB re-probe could not run after {ElapsedMs} ms - the availability latch keeps the value it had and the next hourly tick probes again: {Message}",
                probeClock.ElapsedMilliseconds, ex.Message);
        }
    }

    /// <summary>
    /// The #3812 hourly retention re-evaluation (fleet-level, Timescale-only, on the compression tick): run
    /// <see cref="TimescaleSupport.EnsureRetentionPoliciesAsync(NpgsqlConnection, ILogger, TimescaleSupport.RetentionSweepPass, CancellationToken)"/>
    /// as the <see cref="TimescaleSupport.RetentionSweepPass.Periodic"/> pass on a connection from the worker's
    /// pool. The start-path pass's <c>timescaleConnection</c> is scoped to the setup block and disposed after
    /// it, which is why this cannot simply reuse it and why the method exists at all rather than a second line
    /// beside the startup call.
    ///
    /// <para><b>What the pass does is the start path's, exactly.</b> Create what is missing (paused), converge
    /// horizons, measure every policy's coverage, arm the Covered ones and hold the Short ones. Every step is
    /// idempotent and was tested as such before it was scheduled (the method's own summary says how). What
    /// differs is what it SAYS: a hold that was already a hold is Debug here, a policy ARMED this pass is
    /// Information, a policy RE-HELD this pass is Warning, and the one line every pass writes whatever it found
    /// is <c>Retention re-evaluation: N policies held, M armed this pass, K unchanged</c> — the #3756
    /// discipline, so an operator can see the gate was re-judged without a restart marker.</para>
    ///
    /// <para><b>Failure-isolated at the worker level, with three distinct outcomes that read differently.</b>
    /// The pass completed — its own tally line, inside the method. The pass would not open a connection or
    /// threw outside its per-policy isolation — the WARNING below, naming the elapsed and the message. The pass
    /// ran out its <see cref="s_retentionReevaluationBudget"/> — a different WARNING, because a store that
    /// cannot answer twenty catalog reads and twenty chunk-pruned <c>min()</c> probes in five minutes is the
    /// finding, not the retention. Shutdown cancellation is quiet: the next start's own pass re-judges every
    /// policy, so a pass cut short by stopping the service has nothing to report. Not counted by #3013's
    /// swallowed-read counter: this is a maintenance ACTION, not an alert read whose failure would leave a
    /// condition unjudged — the Retention Held alert's own read is inside the compression method and counts
    /// there.</para>
    ///
    /// <para>The budget is ONE linked token for the WHOLE pass (#2327's shape), not a per-statement deadline:
    /// the sweep's statements carry <c>TimescaleSupport</c>'s 300 s bulk-setup bound each, which is right for
    /// a first conversion and, multiplied by a hundred statements on a slow store, wrong for something awaited
    /// on the serial sweep loop. A cancellation from that budget propagates out of the sweep's per-policy
    /// isolation by design (<c>when (ex is not OperationCanceledException)</c>), so the policies it had not
    /// reached keep their state and the WARNING here is the pass's line for the hour.</para>
    /// </summary>
    private async Task ReevaluateRetentionPoliciesAsync(CancellationToken cancellationToken)
    {
        var passClock = Stopwatch.StartNew();
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(s_retentionReevaluationBudget);

        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(budget.Token);

            /* #4300: the seam-only repair runs BEFORE the coverage sweep below, on the SAME connection, so a
               seam it closes this tick is already gone by the time EnsureRetentionPoliciesAsync re-judges
               coverage a moment later — the gate can release in this very tick rather than waiting for next
               hour's pass to notice. It reuses the exact per-target body the start-path walk runs
               (TimescaleSupport.RepairMaterializationSeamsAsync), restricted to legacy-paired targets and
               their seam window alone; a target with an already-closed seam is a cheap skip (one span read,
               one raw-floor read) — there is no heavier scan to avoid running twice. Failure-isolated: a
               throw here must not stop the coverage sweep or the purge trigger below, both of which are the
               pass's other, independent jobs.

               Skipped entirely while _materializationHoleRepairRunning is true: the start-path (or a stale-
               epoch relaunch) full walk covers every seam this seam-only pass would, and running both at once
               would refresh the same aggregate on two connections at the same time for no gain. */
            if (_materializationHoleRepairRunning)
            {
                _logger.LogDebug("Retention re-evaluation: skipping the seam-only repair this pass — a full materialization-hole repair is already running in this process and covers the same seam.");
            }
            else
            {
                /* #4300: the seam repair's own child budget (s_seamRepairBudget), linked to the pass's budget
                   rather than sharing it unbounded — a seam wide enough to spend the whole pass budget would
                   otherwise starve the coverage sweep, the purge trigger and the epoch relaunch below for that
                   hour, on every hour the seam stays open on a slow store. */
                using var seamBudget = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
                seamBudget.CancelAfter(s_seamRepairBudget);

                try
                {
                    var seamSummary = await TimescaleSupport.RepairMaterializationSeamsAsync(connection, _logger, DateTime.UtcNow, seamBudget.Token);
                    if (seamSummary.BucketsRepaired > 0 || seamSummary.BucketsDeferred > 0 || seamSummary.Failures > 0)
                    {
                        _logger.LogInformation(
                            "Retention re-evaluation: seam repair closed {BucketsRepaired} bucket(s) across {HolesRepaired} hole(s) this pass, {BucketsDeferred} bucket(s) left for a later pass (past this pass's per-aggregate cap), {Failures} isolated failure(s).",
                            seamSummary.BucketsRepaired, seamSummary.HolesRepaired, seamSummary.BucketsDeferred, seamSummary.Failures);
                    }
                    else
                    {
                        _logger.LogDebug("Retention re-evaluation: seam repair found nothing to close this pass.");
                    }
                }
                /* #4300: the seam's OWN budget ran out — not the pass's outer budget, and not shutdown — so
                   the seam repair is cut short here and the rest of the pass (the coverage sweep, the purge
                   trigger, the epoch relaunch) still runs this tick. A cut-short refresh is resume-safe: the
                   batches that committed before the cut ran newest-first, so the floor sits inside ground this
                   pass already covered and next hour's seam-only pass picks up exactly where this one left off
                   (RollupBackfill.cs's own newest-first resume argument, ~:203). The count that follows is a
                   cheap re-read (span plus raw-floor per legacy-paired target, the same two probes the seam
                   repair's own skip check makes), not a re-walk, so a store stuck deferring the same seam for
                   a day is visible from this WARNING alone without anyone re-running the repair by hand. */
                catch (OperationCanceledException) when (seamBudget.IsCancellationRequested && !budget.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                {
                    /* #4300: the re-read itself is failure-isolated from the rest of the pass — a throw here
                       (a catalog read that times out, a connection already in a bad state after the budget
                       cut it off mid-statement) must not stop the coverage sweep, the purge trigger or the
                       epoch relaunch below, the exact isolation this whole catch exists to preserve. An
                       unreadable count logs as "unknown" rather than ending the pass here. */
                    string hoursDeferred;
                    try
                    {
                        hoursDeferred = (await TimescaleSupport.CountOpenSeamHoursAsync(connection, DateTime.UtcNow, budget.Token)).ToString(System.Globalization.CultureInfo.InvariantCulture);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        hoursDeferred = "unknown";
                    }

                    _logger.LogWarning(
                        "Retention re-evaluation: seam repair paused at the {BudgetMinutes}-minute budget; resumes next hour; {HoursDeferred} hour(s) still deferred.",
                        (long)s_seamRepairBudget.TotalMinutes, hoursDeferred);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(
                        "Retention re-evaluation: the seam repair could not run this pass — any legacy/successor seam a prior outage opened stands until a later pass retries: {Message}",
                        ex.Message);
                }
            }

            await TimescaleSupport.EnsureRetentionPoliciesAsync(
                connection, _logger, TimescaleSupport.RetentionSweepPass.Periodic, budget.Token);

            /* #4299: the service's own raw-purge trigger — ONLY reached from this Periodic pass, never
               from the start path. TriggerRawPurgeAsync re-reads the sweep's own verdict (config->>'darling_armed',
               written moments ago by the call above) rather than threading it through as a parameter, so a
               future caller of EnsureRetentionPoliciesAsync cannot accidentally skip the trigger by forgetting
               to wire a return value through. */
            await TriggerRawPurgeAsync(connection, budget.Token);

            /* #4299/#4391: the relaunch — when the epoch does not match the CURRENT postmaster start on
               ANY raw job and no repair this process launched is still running, start a new one. Checked
               against ALL THREE raw relations, not just the first: RunMaterializationHoleRepairAsync stamps
               all three under the SAME postmaster-start value in the same pass ON A CLEAN COMPLETION, but a
               repair that failed on (or was cancelled during) the second or third relation stamps only the
               ones it reached, leaving the rest stale — reading only relation zero would then miss that and
               never relaunch. A store that has never had a repair run under this start (every key null) reads
               the same "no match" as a store whose repair is simply stale. _materializationHoleRepairRunning is
               this PROCESS's own guard against launching a second overlapping repair; the epoch stamp in the
               store is the separate guard (RawRepairEpochStampSql's IS DISTINCT FROM) that stops a SECOND
               SERVICE from repeating the work — the two are independent and both are checked here because
               either alone is not enough: two processes each with the flag clear would otherwise both launch. */
            if (!_materializationHoleRepairRunning && TimescaleSupport.RawRelations.Count > 0)
            {
                bool epochCurrent = true;
                var staleRelations = new List<string>();
                foreach (var relation in TimescaleSupport.RawRelations)
                {
                    await using var epochCheck = new NpgsqlCommand(
                        TimescaleSupport.RawRepairEpochMatchesSql(relation), connection);
                    var value = await epochCheck.ExecuteScalarAsync(budget.Token);
                    var relationCurrent = value is bool b && b;
                    epochCurrent &= relationCurrent;
                    if (!relationCurrent)
                    {
                        staleRelations.Add(relation);
                    }
                }

                if (ShouldLaunchMaterializationHoleRepair(_materializationHoleRepairRunning, epochCurrent) && _postgres is not null)
                {
                    _logger.LogInformation(
                        "Retention re-evaluation: the repair epoch is stale under the current postmaster start for {StaleRelations} and no repair is running in this process — launching one now (Periodic pass only).",
                        string.Join(", ", staleRelations));

                    /* Deliberately NOT awaited, the same posture the start path takes for the identical call:
                       a capped repair on the heaviest aggregate is a policy run's worth of work, and this pass
                       has three more tenants to reach this hour. _materializationHoleRepairRunning is set at
                       the top of the method and cleared in its finally, so the NEXT Periodic tick (an hour on)
                       sees it running and does not launch a second one; TriggerRawPurgeAsync above already saw
                       the stale epoch this tick and skipped the purge, which is correct — the repair this
                       launches has not finished yet, so nothing this tick should have purged. */
                    _periodicHoleRepair = RunMaterializationHoleRepairAsync(_postgres, cancellationToken);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown — quiet and expected. The next start's own pass re-judges every policy. */
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested)
        {
            _logger.LogWarning(
                "Retention re-evaluation exceeded its {BudgetSeconds}s budget after {ElapsedMs} ms and was cut short - the policies it had not reached keep the state they were in and are re-judged next hour (and at the next start). A store that cannot answer twenty catalog reads and twenty chunk-pruned min() probes inside that budget is the finding here, not the retention.",
                (long)s_retentionReevaluationBudget.TotalSeconds, passClock.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Retention re-evaluation could not run after {ElapsedMs} ms - every held policy stays held until the next hour retries (or the next start): {Message}",
                passClock.ElapsedMilliseconds, ex.Message);
        }
    }

    /// <summary>
    /// #4299: the Periodic pass's trigger for the three raw jobs' own purge — reached only from
    /// <see cref="ReevaluateRetentionPoliciesAsync"/>, on the SAME connection and immediately after
    /// <see cref="TimescaleSupport.EnsureRetentionPoliciesAsync(NpgsqlConnection, ILogger, TimescaleSupport.RetentionSweepPass, CancellationToken)"/>
    /// has run this pass's coverage sweep, so the drop decision below is measured fresh, not read back from a
    /// value the sweep merely wrote. For each raw relation, in the same pass:
    /// <list type="bullet">
    /// <item>a FRESH Covered verdict — <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/>, called again
    /// here rather than trusting <c>config-&gt;&gt;'darling_armed'</c> the sweep just above wrote: <c>darling_armed</c>
    /// is the readers' and alerts' standing verdict and is deliberately left as it was on anything OTHER than
    /// Covered (#1877's bounded-depth-cap posture), so an Unknown probe (one that throws, times out, or comes
    /// back empty) leaves a PRIOR pass's <c>true</c> in place; a purge gated on that stale value would drop
    /// chunks on a coverage state this pass itself could not confirm. <c>IsRawTierDropSafeAsync</c> answers
    /// true ONLY on a Covered verdict measured THIS call — Short and Unknown both answer false — so the purge
    /// can never run on a verdict older than the pass that is about to act on it;</item>
    /// <item>a repair finished under the CURRENT <c>pg_postmaster_start_time()</c> — <see cref="TimescaleSupport.RawRepairEpochMatchesSql"/>;</item>
    /// <item>no hole in the range the purge is about to drop — <see cref="TimescaleSupport.HoleFreeThroughAsync"/>
    /// over every registered aggregate whose source is this raw relation, from the oldest raw chunk's
    /// <c>range_start</c> to <c>now() - drop_after</c>. A successor <see cref="TimescaleSupport.ResolveMaterializationAsync"/>
    /// cannot resolve (mid-rebuild, renamed, or dropped) is NOT skipped as though it were hole-free: an
    /// unresolvable successor means the gate cannot tell whether the range it should be covering is intact,
    /// which is exactly the state <c>hole-free</c> must never mean. This records <c>gate_unknown</c> and blocks
    /// the purge, the same fail-closed posture as an Unknown coverage verdict.</item>
    /// </list>
    /// All hold → <see cref="TimescaleSupport.RunRetentionPurgeJobAsync"/>. One INFORMATION line per raw
    /// job either way, naming which gate it failed (or that the run itself failed).
    ///
    /// <para>Deferred ranges are passed empty: a deferred range is still a hole (the source has rows, the
    /// materialization has none), so the fresh hole scan below finds it.</para>
    /// </summary>
    private Task TriggerRawPurgeAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
        => TriggerRawPurgeCoreAsync(connection, _logger, cancellationToken);

    /// <summary>
    /// #4299: the exact body of <see cref="TriggerRawPurgeAsync"/>, extracted as an internal static
    /// method so <c>Darling.Tests</c> (already reachable via this project's <c>InternalsVisibleTo</c>) can
    /// drive it directly against a live store without standing up a whole <see cref="DarlingWorker"/>. No
    /// behaviour change: the instance method above is now a one-line forward to this, with <c>_logger</c>
    /// threaded through as the <paramref name="logger"/> parameter.
    /// </summary>
    internal static async Task TriggerRawPurgeCoreAsync(NpgsqlConnection connection, ILogger logger, CancellationToken cancellationToken)
    {
        var emptyDeferred = Array.Empty<(DateTime Start, DateTime End)>();

        foreach (var relation in TimescaleSupport.RawRelations)
        {
            try
            {
                /* Measured THIS call, not read back from config->>'darling_armed' — that key stays as the
                   readers' and alerts' standing verdict and can hold a PRIOR pass's true through an Unknown
                   probe (#1877). The purge decision needs this pass's own fresh answer, so it calls the same
                   probe IsRawTierDropSafeAsync arms against, again, right here. */
                var covered = await TimescaleSupport.IsRawTierDropSafeAsync(connection, relation, cancellationToken);

                if (!covered)
                {
                    logger.LogInformation(
                        "Raw retention purge for {Relation} did not run this pass — not covered (a fresh measurement this pass found Short or Unknown for it).",
                        relation);
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "not_covered", null, null, logger, cancellationToken);
                    continue;
                }

                bool epochMatches;
                await using (var epochRead = new NpgsqlCommand(TimescaleSupport.RawRepairEpochMatchesSql(relation), connection))
                {
                    var value = await epochRead.ExecuteScalarAsync(cancellationToken);
                    epochMatches = value is bool b && b;
                }

                if (!epochMatches)
                {
                    logger.LogInformation(
                        "Raw retention purge for {Relation} did not run this pass — the repair epoch is stale or missing (no repair has finished under the CURRENT postmaster start yet).",
                        relation);
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "epoch_stale", null, null, logger, cancellationToken);
                    continue;
                }

                /* The scan's window is [oldest chunk range_start, now - drop_after], which contains every
                   whole chunk drop_chunks would actually drop (a chunk drops only once its range_end <=
                   now - drop_after). Mixing range_start (timestamptz) with now() AT TIME ZONE 'UTC' is safe
                   here because this connection pins Timezone=UTC, so the subtraction and every later
                   comparison against it stay in UTC with no local-offset step. */
                long jobId;
                DateTime dropFrom;
                DateTime dropTo;
                await using (var range = new NpgsqlCommand($@"
SELECT j.job_id,
       (SELECT min(c.range_start) FROM timescaledb_information.chunks AS c WHERE c.hypertable_schema = 'collect' AND c.hypertable_name = '{relation}'),
       now() AT TIME ZONE 'UTC' - (j.config->>'drop_after')::interval
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = '{relation}'", connection))
                {
                    await using var reader = await range.ExecuteReaderAsync(cancellationToken);
                    if (!await reader.ReadAsync(cancellationToken) || await reader.IsDBNullAsync(1, cancellationToken))
                    {
                        /* No job row, or raw holds no chunks at all — nothing to drop, nothing to gate. */
                        logger.LogInformation(
                            "Raw retention purge for {Relation} did not run this pass — no chunks to evaluate (the job row or the oldest chunk could not be read).",
                            relation);
                        await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "no_chunks", null, null, logger, cancellationToken);
                        continue;
                    }

                    jobId = reader.GetInt64(0);
                    dropFrom = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
                    dropTo = DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc);
                }

                var holeFree = true;
                var successorUnresolved = false;
                foreach (var target in TimescaleSupport.MaterializationHoleTargets)
                {
                    if (!string.Equals(target.Source, relation, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, target.View, cancellationToken);
                    if (materialization is null)
                    {
                        /* An unresolvable successor (mid-rebuild, renamed, or dropped) is NOT hole-free — the
                           gate cannot tell whether the range it should be covering is intact, so it must not
                           read as clean. Fail closed, same as an Unknown coverage verdict above. */
                        successorUnresolved = true;
                        holeFree = false;
                        break;
                    }

                    if (!await TimescaleSupport.HoleFreeThroughAsync(
                        connection, target, materialization.Value, dropFrom, dropTo, emptyDeferred, cancellationToken))
                    {
                        holeFree = false;
                        break;
                    }
                }

                if (successorUnresolved)
                {
                    logger.LogInformation(
                        "Raw retention purge for {Relation} did not run this pass — a successor's materialization could not be resolved (mid-rebuild, renamed, or dropped), so coverage cannot be confirmed hole-free.",
                        relation);
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "gate_unknown", null, null, logger, cancellationToken);
                    continue;
                }

                if (!holeFree)
                {
                    logger.LogInformation(
                        "Raw retention purge for {Relation} did not run this pass — a hole was found in the range about to be dropped.",
                        relation);
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "hole", null, null, logger, cancellationToken);
                    continue;
                }

                var runClock = Stopwatch.StartNew();
                var outcome = await TimescaleSupport.RunRetentionPurgeJobAsync(connection, jobId, logger, cancellationToken);
                runClock.Stop();
                if (!outcome.Ran)
                {
                    logger.LogInformation(
                        "Raw retention purge for {Relation} did not run this pass — the run itself failed with SqlState {SqlState} (see the warning above naming the timeout or error).",
                        relation, outcome.SqlState ?? "(none)");
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "run_failed", outcome.SqlState, null, logger, cancellationToken);
                }
                else
                {
                    logger.LogInformation(
                        "Raw retention purge for {Relation} ran this pass — covered, epoch matched the current postmaster start, and no hole in the dropped range.",
                        relation);
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "ran", null, runClock.ElapsedMilliseconds, logger, cancellationToken);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* #4391: the trigger's own gate check failing partway through (a timeout or error reading
                   coverage, the epoch, chunk ranges, or a successor's holes) is not a routine "not covered
                   this pass" outcome the way the continues above are — it means the gate itself could not
                   answer, so it is logged at Warning rather than Information, and recorded as gate_error so
                   the record distinguishes "the gate said no" from "the gate could not run". The record call
                   is wrapped so a failure recording the outcome never escapes this catch and skips the next
                   relation in the loop. */
                logger.LogWarning(
                    "Raw retention purge for {Relation} did not run this pass — the trigger's own gate check failed: {Message}",
                    relation, ex.Message);

                try
                {
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(
                        connection, relation, "gate_error", (ex as PostgresException)?.SqlState, null, logger, CancellationToken.None);
                }
                catch (Exception recordEx) when (recordEx is not OperationCanceledException)
                {
                    logger.LogDebug("Could not record the gate_error purge outcome for {Relation}: {Message}", relation, recordEx.Message);
                }
            }
        }
    }

    /// <summary>
    /// The #3817 hourly store-object convergence (fleet-level, on the store-maintenance tick): run the WHOLE
    /// <see cref="s_storeObjectConvergence"/> list, in the list's order, each step failure-isolated, and write
    /// the one summary line. The fourth tenant of this tick and the last statement in its
    /// <see cref="_timescaleAvailable"/> gate.
    ///
    /// <para><b>The lie this closes.</b> Every step in that list ran exactly once, at service start, each one
    /// failure-isolated per item — which is the right posture and is exactly what makes a single failure
    /// invisible and permanent. One missing continuous aggregate is one rollup family gone; one missing
    /// baseline relation is one anomaly family silently returning an empty baseline (the provider reads them by
    /// name and swallows the 42P01); an un-applied tuning pass is every composer query back on un-indexed
    /// scans. On a service that runs for months, "until the next restart" and "forever" are the same sentence.
    /// The code already knew: the ungated baseline-fallback block exists BECAUSE a startup step can silently
    /// leave one relation unbuilt — and it was itself a startup step that can silently leave one relation
    /// unbuilt.</para>
    ///
    /// <para><b>Why the whole list on one connection here, when the start path uses three.</b> The start path's
    /// three connections are an artefact of its three try/catch blocks, whose boundaries carry meaning there
    /// (the TimescaleDB block degrades the latch; the ungated block runs on every path; the tuning block
    /// degrades to un-tuned queries). This pass has ONE outcome — the store is converged or this hour's
    /// attempt says what it could not do — so it has one connection, one budget and one catch, and the
    /// segments collapse. The steps' own per-item isolation is untouched and is still what keeps one failed
    /// aggregate from costing the other nineteen.</para>
    ///
    /// <para><b>Cost on a converged store, which is the case that must be cheap.</b> Every step is a catalog
    /// read plus DDL only where something is missing: <c>if_not_exists</c> on the hypertable and policy
    /// creates, <c>IF NOT EXISTS</c> on the aggregate and index creates, probe-then-create on the baseline
    /// relations, and differ-only <c>alter_job</c> on the three converges. Two statements are unconditional
    /// rather than catalog-gated — the <c>ALTER TABLE ... SET (timescaledb.compress ...)</c> enable inside
    /// <see cref="TimescaleSupport.ApplyCompressionPolicyAsync"/> and its <c>collection_log</c> twin — and
    /// they are the reason this pass takes any locks at all; both are metadata-only on a table that already
    /// carries the reloption. Measured on a converged rig (TimescaleDB 2.28.1): the figure is in the PR body
    /// and the summary line carries the elapsed on every pass, which is the honest way to keep that claim
    /// current on a real store rather than on a rig.</para>
    ///
    /// <para><b>The phase grid is not moved by running this hourly.</b> Creating a missing aggregate or policy
    /// here runs the same DDL the start path would have run, and every phase it assigns is deterministic on
    /// the registry — <see cref="TimescaleSupport.RefreshPhaseMinutesFor(string)"/> and
    /// <see cref="TimescaleSupport.TryCompressionPhaseMinutesFor"/> are pure functions of the view or table name,
    /// so the slot a policy lands on does not depend on WHEN the ensure ran. Two <c>initial_start</c>
    /// expressions are clock-relative — the raw compression policy's "next hour plus this table's phase
    /// minutes" and the aggregate compression band's "N nights after the next UTC midnight" — so a policy
    /// created by an hourly pass rather than by a start takes its own first run from that pass's clock. That
    /// is the same drift a restart at a different time of day already produces, it affects only a policy this
    /// store did not have, and the MINUTE (the grid slot the phase argument protects) is identical either
    /// way.</para>
    ///
    /// <para><b>Failure-isolated with the three outcomes this tick's tenants all use</b> (#3812's shape, and
    /// the same reason): shutdown is quiet, the budget gets its own WARNING naming the budget, and anything
    /// else gets a WARNING naming the message. Nothing rethrows — the sweep loop must never see this pass
    /// fail. A pass cut short leaves the steps it had not reached exactly as they were; they are idempotent,
    /// so the next hour resumes rather than restarting the ladder. Not counted by #3013's swallowed-read
    /// counter: this is a maintenance ACTION, not an alert read whose failure would leave a condition
    /// unjudged.</para>
    /// </summary>
    private async Task ConvergeStoreObjectsAsync(CancellationToken cancellationToken, bool timescaleAvailable = true)
    {
        var passClock = Stopwatch.StartNew();
        var tally = new StoreObjectConvergenceTally();
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(s_storeObjectConvergenceBudget);

        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(budget.Token);
            foreach (var step in s_storeObjectConvergence)
            {
                /* #3913: the ONE filter the hourly pass has. On a store without TimescaleDB, a step that needs
                   it is skipped rather than run to fail; every other step runs, as it does at start. */
                if (!timescaleAvailable && NeedsTimescale(step.Stage))
                {
                    continue;
                }

                await RunStoreObjectConvergenceStepAsync(connection, step, tally, _logger, budget.Token);
            }

            LogStoreObjectConvergence(tally, passClock.ElapsedMilliseconds, startup: false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown — quiet and expected. The next start's own pass runs every step again. */
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested)
        {
            _logger.LogWarning(
                "Store object convergence exceeded its {BudgetSeconds}s budget after {ElapsedMs} ms and was cut short — {Steps} step(s) ran, {Changed} changed, and the steps it had not reached are exactly as they were and are retried next hour (and at the next start). Every step is idempotent, so the next pass resumes at the one this pass was inside rather than redoing the ones behind it. A CONVERGED store answers this pass in well under a second; a store that spends five minutes in it is either building something real for the first time — a first hypertable conversion or a first aggregate on an adopted store — or is too slow to answer its own catalog, and which one it is shows in the per-object lines above.",
                (long)s_storeObjectConvergenceBudget.TotalSeconds, passClock.ElapsedMilliseconds, tally.Steps, tally.Changed.Count);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Store object convergence could not run after {ElapsedMs} ms — every store object stays exactly as it is (a missing rollup family stays missing, a missing baseline relation keeps returning nothing) until the next hour retries or the service restarts: {Message}",
                passClock.ElapsedMilliseconds, ex.Message);
        }
    }

    /// <summary>
    /// One SEGMENT of <see cref="s_storeObjectConvergence"/> on the start path — the steps tagged
    /// <paramref name="stage"/>, in the list's order, on the connection that segment's try block owns
    /// (#3817). The hourly pass has no segments and walks the list whole; this exists only because the start
    /// path interleaves three non-convergence steps between the ensures and two of those positions are
    /// load-bearing.
    ///
    /// <para>NOT wrapped in a catch of its own: each segment's call site already sits inside the try that
    /// decides what a failure THERE degrades to (the TimescaleDB latch, the baseline gap, un-tuned queries),
    /// and those three outcomes read differently for reasons their own issues argued. Adding a fourth catch
    /// here would convert a TimescaleDB-block fault into a silently-skipped segment and leave
    /// <c>_timescaleAvailable</c> true.</para>
    /// </summary>
    private async Task RunStoreObjectConvergenceSegmentAsync(
        NpgsqlConnection connection,
        StoreObjectConvergenceStage stage,
        StoreObjectConvergenceTally tally,
        CancellationToken cancellationToken)
    {
        foreach (var step in s_storeObjectConvergence)
        {
            if (step.Stage != stage)
            {
                continue;
            }

            await RunStoreObjectConvergenceStepAsync(connection, step, tally, _logger, cancellationToken);
        }
    }

    /// <summary>
    /// One convergence step, failure-isolated, its outcome recorded in <paramref name="tally"/> (#3817) — the
    /// #1775 shape, and the ONE place a step's result is judged, so the start path and the hourly pass cannot
    /// disagree about what counts as changed or failed.
    ///
    /// <para>A step that THROWS writes its own WARNING here and is counted as failed. That is a second warning
    /// on top of whatever the step's own per-item isolation already wrote, and deliberately so: the step's
    /// internal warnings name one item, this one names the step that did not complete, and the difference
    /// matters — a per-aggregate failure is one family, a throw out of
    /// <see cref="TimescaleSupport.EnsureContinuousAggregatesAsync"/> itself is all twenty. Cancellation is
    /// rethrown, so the budget and shutdown reach the pass's own catches rather than being recorded as a
    /// failure of every step.</para>
    ///
    /// <para><see cref="StoreObjectChangeSignal"/> is what keeps the changed count honest: only the six
    /// steps whose return value IS a change count can contribute to it, and the rest are counted as steps
    /// that ran. The alternative reads "changed: hypertable conversion, compression policies, continuous
    /// aggregates" on every hour of a store that has not changed since July.</para>
    ///
    /// <para><b>Static and internal, deliberately</b>, taking its logger rather than reading the field: this
    /// is the one seam in the pass whose behaviour can be DRIVEN rather than read off the source. The
    /// property that matters — a step which throws on one pass is retried on the next, counted failed then
    /// changed — needs two passes over the same step and nothing else, so
    /// <c>StoreObjectConvergenceStepBehaviourTests</c> drives it with a fake step instead of standing up a
    /// host, a store and a clock. Everything else about the pass is a call site or a list entry, which is
    /// textual and pinned as such.</para>
    /// </summary>
    internal static async Task RunStoreObjectConvergenceStepAsync(
        NpgsqlConnection connection,
        StoreObjectConvergenceStep step,
        StoreObjectConvergenceTally tally,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        try
        {
            var count = await step.EnsureAsync(connection, logger, cancellationToken);
            tally.Steps++;
            if (step.Signal == StoreObjectChangeSignal.Delta && count > 0)
            {
                tally.Changed.Add($"{step.Name} {count.ToString(CultureInfo.InvariantCulture)}");
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            tally.Steps++;
            tally.Failed.Add(step.Name);
            logger.LogWarning(
                "Store object convergence step '{Step}' failed — whatever it had not yet ensured stays unbuilt until the next hourly pass or the next start retries it (the step's own lines above name any individual object it did isolate): {Message}",
                step.Name, ex.Message);
        }
    }

    /// <summary>
    /// The ONE line a convergence pass writes, on both cadences, whatever it found (#3817, the #3756
    /// discipline). One owner so the start-path and hourly lines cannot drift into two shapes; the only
    /// difference between them is which of the two literal templates the <paramref name="startup"/> switch
    /// picks, exactly as <see cref="TimescaleSupport.EnsureRetentionPoliciesAsync"/>'s pass switch does.
    ///
    /// <para>A pass that changed NOTHING still writes it, because that is the pass this line exists for: a
    /// check whose negative outcome is indistinguishable from its non-execution has not reported, and before
    /// this the only evidence a convergence pass had run was eleven per-object lines and the absence of a
    /// twelfth. The changed list names the steps rather than counting them, because "2 changed" sends an
    /// operator back through the per-object lines to find out which.</para>
    /// </summary>
    private void LogStoreObjectConvergence(StoreObjectConvergenceTally tally, long elapsedMs, bool startup)
    {
        var changed = tally.Changed.Count == 0 ? "none" : string.Join(", ", tally.Changed);
        var failed = tally.Failed.Count == 0 ? "none" : string.Join(", ", tally.Failed);

        if (startup)
        {
            _logger.LogInformation(
                "Store object convergence at startup: {Steps} steps, {Changed} changed ({ChangedNames}), {Failed} failed ({FailedNames}), {ElapsedMs} ms",
                tally.Steps, tally.Changed.Count, changed, tally.Failed.Count, failed, elapsedMs);
        }
        else
        {
            _logger.LogInformation(
                "Store object convergence: {Steps} steps, {Changed} changed ({ChangedNames}), {Failed} failed ({FailedNames}), {ElapsedMs} ms",
                tally.Steps, tally.Changed.Count, changed, tally.Failed.Count, failed, elapsedMs);
        }
    }

    /// <summary>
    /// The #2068 store self-metrics sweep (fleet-level, hourly): one <see cref="StoreSelfMetrics"/> run —
    /// per-hypertable size/compression rows (Timescale stores only, gated on the cached
    /// <see cref="_timescaleAvailable"/> flag INSIDE the sweep so plain-PG stores still record their
    /// dimension + whole-store rows), the payload-dimension size/row-count rows, the whole-store summary
    /// row, and the series' own bounded retention DELETE. Failure-isolated at the worker level like the
    /// disk-pressure and compression checks: a store hiccup logs and skips this tick, never aborting the
    /// sweep loop, and the series simply gains a one-hour gap.
    ///
    /// <para>Two more self-telemetry passes ride the same tick, connection and budget: the #3021
    /// <see cref="StoreLogSweep"/> read of the store's own server log, and the #2674 collector-cost flush.
    /// The shared budget is what bounds the whole tick — three passes on one
    /// <see cref="StoreSelfMetrics.SweepTimeoutSeconds"/> linked CTS, not one each.</para>
    /// </summary>
    private async Task SweepStoreSelfMetricsAsync(CancellationToken cancellationToken)
    {
        /* #2327 review catch: this sweep is AWAITED on the main loop, unlike the fire-and-track
           per-server sweeps — so its worst case stalls per-server dispatch and the disk-pressure and
           compression checks with it. The budget is therefore ONE SweepTimeoutSeconds for the WHOLE
           sweep (a linked CTS), not per statement: worst-case loop block stays ~5 minutes, comparable
           to the old default's 5 x 30s, instead of the 25 minutes five sequential 300s statements
           could take against a genuinely wedged store. The per-statement CommandTimeout inside
           StoreSelfMetrics stays as the belt for callers that pass no token. */
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(TimeSpan.FromSeconds(StoreSelfMetrics.SweepTimeoutSeconds));

        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(budget.Token);

            /* #3923: its OWN narrow catch, ahead of the store-log capture below and on the same reasoning
               that catch already carries - a PostgresException the store permanently rejects here (catalog
               drift on a bring-your-own TimescaleDB, a revoked privilege) must not cost the store-log
               census or the collector-cost flush below it every hour forever. #3918 was exactly this for
               one statement (a catch-all census failing 42703 on TimescaleDB 2.29+); the sizing pass shared
               the OUTER catch then, so fixing that one statement left the next one the store rejects able to
               take the whole tick down again. Narrower than the capture's catch below: a command TIMEOUT
               still falls through to the outer catch (unchanged one-hour-gap handling), and so does a fault
               that actually broke the connection - a server-REJECTED statement leaves the session idle and
               usable, which is what the two passes after it need. */
            try
            {
                await StoreSelfMetrics.SweepAsync(connection, _timescaleAvailable, DateTime.UtcNow, _logger, budget.Token);
            }
            catch (PostgresException ex) when (!PgBaselineProvider.IsCommandTimeout(ex)
                                               && connection.State == ConnectionState.Open)
            {
                _logger.LogError(
                    "Store self-metrics sweep failed ({SqlState}), so this hour's size rows are missing; the "
                    + "store-log census and collector-cost flush on the same tick still run: {Message}",
                    ex.SqlState, ex.Message);
            }

            /* #3021: the store reading its OWN server log, on the same hourly tick and the same connection.
               It rides this cadence rather than carrying its own for two reasons. The log grows slowly (a
               production day is ~1,400 entries worth classifying), so an hourly bucket is the finest grain
               the census can honestly report; and sharing the tick makes the two self-telemetry series -
               what the store WEIGHS and what it COMPLAINED about - land on the same timestamp grid, which is
               how they get read side by side.

               ITS OWN catch, unlike the two passes either side of it, and that asymmetry is the point. This
               is the only pass here whose PRIVILEGE is not guaranteed: reading the log needs
               pg_read_server_files plus an explicit GRANT on pg_read_binary_file, which the managed store's
               bootstrap superuser has and a bring-your-own store's owner may not have given. Sharing the
               outer catch would let that one permanent condition cost the collector-cost flush below it
               every hour forever - a new failure in a pass that was working. Warning rather than Error for
               the same reason: on a store that cannot grant it this repeats hourly and would otherwise
               pollute every "errors in the last hour" count with a condition nobody is going to change.
               The durable record is on the read surface, where get_store_log reports zero captures as
               not_collected and names the privilege. */
            try
            {
                await StoreLogSweep.SweepAsync(connection, DateTime.UtcNow, _logger, budget.Token);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var unavailable = ex is PostgresException
                {
                    SqlState: PostgresErrorCodes.UndefinedFile or PostgresErrorCodes.InsufficientPrivilege,
                };
                if (unavailable && _storeLogCaptureUnavailableWarned)
                {
                    _logger.LogDebug("Store log capture is still unavailable: {Message}", ex.Message);
                }
                else
                {
                    _logger.LogWarning(
                        "Store log capture failed, so this hour's store-log census is missing (get_store_log "
                        + "reports the capture gap). Reading the store's own log needs pg_read_server_files and "
                        + "EXECUTE on pg_read_binary_file; a bring-your-own store may not grant them: {Message}",
                        ex.Message);
                    _storeLogCaptureUnavailableWarned |= unavailable;
                }
            }

            /* #3971: Npgsql closes THIS connection outright on the capture's ERROR rather than leaving it
               idle-but-usable - reproduced against a plain PostgreSQL 18.4 store on Npgsql 10.0.3, where a
               pg_ls_dir fault (58P01) takes connection.State straight to Closed. The re-mask and the
               collector-cost flush below run on this SAME connection, so a store that cannot grant the
               capture's privilege, or has no log directory at all (the Linux compose store's shape), lost
               both of them every hour before this - get_collector_cost never got a row however long that
               store ran. Reopened rather than swapped for a fresh one: this connection came from the pool
               through _postgres, so OpenAsync pulls a new physical connection under the same object the
               `await using` above already owns and disposes. A capture that succeeded, or failed WITHOUT
               closing the connection, finds it already Open here and this is a no-op. */
            if (connection.State != ConnectionState.Open)
            {
                /* A Broken connection must be closed before it can open again; closing a Closed one is a no-op. */
                await connection.CloseAsync();
                await connection.OpenAsync(budget.Token);
            }

            /* #3915, #3944: rows stored before this build kept their entries whole (an ERROR's STATEMENT line with
               its literals), for the capture's 400-day retention. Their SQL is normalized one bounded slice per
               tick until the table's end, then not again this process; a new capture is normalized on write. Its
               own catch, like the capture's, and its own time cap (#3920's review): neither a failure nor a slow
               slice may cost the collector-cost flush below. */
            if (!_storeLogRemaskDone)
            {
                using var remaskBudget = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
                remaskBudget.CancelAfter(StoreLogSweep.RemaskSliceBudget);
                try
                {
                    var (next, examined, rewritten) = await StoreLogSweep.RemaskStoredEventsAsync(
                        connection, _storeLogRemaskCursor, remaskBudget.Token);
                    _storeLogRemaskCursor = next;
                    _storeLogRemaskDone = next is null;
                    if (rewritten > 0)
                    {
                        _logger.LogInformation(
                            "Store log: re-masked {Rewritten} of {Examined} stored row(s) captured before this build, so their SQL literals no longer reach get_store_log and their messages group by shape{Remaining}.",
                            rewritten, examined, next is null ? "" : "; the rest follow on the next hourly passes");
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException
                    || (remaskBudget.IsCancellationRequested && !budget.IsCancellationRequested))
                {
                    _logger.LogWarning(
                        "Store log: re-masking rows captured before this build failed, and is retried next hour: {Message}",
                        ex.Message);
                }
            }

            /* #4012: PostgreSQL deadlock reports stored before #4005 keep their SQL raw, with a hash over the raw
               graph, for pg_deadlocks' 90 days, and a deadlock alert fired before it keeps the same in its history
               row, and an analysis finding its exemplars. Every read normalizes them; a direct SELECT does not.
               Page after page until the slice's own cap (#4012's review: a page an hour took about 11 days for the
               reports alone), alerts, then reports, then findings, with the store-log slice's own-catch, own-cap
               posture: neither a failure nor a slow slice may cost the collector-cost flush below. RunAsync ends
               quietly on its cap and keeps each walk's cursor; a stage's own failures are counted and logged there.
               Gated on Pending, not Done (#4036's round-2 review, finding 1): a stage that gave up counts as done,
               and RunAsync is where it is tried again a day later, so it must still be called. */
            if (_pgDeadlockRemask.Pending)
            {
                if (connection.State != ConnectionState.Open)
                {
                    await connection.CloseAsync();
                    await connection.OpenAsync(budget.Token);
                }

                /* #4012's review, finding 1: no key is no reason to skip the pass. The key file can be unusable for
                   good (an untrusted directory or ACL, an unreadable file, DPAPI after the service moved machines),
                   and the reports, alerts, findings and finding alerts need no key; only the alert-key stage waits
                   for one, and RunAsync says so once. */
                using var remaskBudget = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
                remaskBudget.CancelAfter(PgDeadlockRemask.SliceBudget);
                try
                {
                    await PgDeadlockRemask.RunAsync(connection, _pgDeadlockRemask, _pgDeadlockRemaskKey, _logger, remaskBudget.Token);
                }
                catch (Exception ex) when (ex is not OperationCanceledException
                    || (remaskBudget.IsCancellationRequested && !budget.IsCancellationRequested))
                {
                    _logger.LogWarning(
                        "PostgreSQL deadlocks: re-masking alerts, reports and findings stored before this build failed, and is retried next hour: {Message}",
                        ex.Message);
                }
            }

            /* The flush runs on this same connection, which a canceled slice above can leave closed. */
            if (connection.State != ConnectionState.Open)
            {
                await connection.CloseAsync();
                await connection.OpenAsync(budget.Token);
            }

            /* #2674: reuse the same hourly connection and budget — one aggregate row per (server, collector)
               for the window, plus the accumulator's own bounded retention DELETE. */
            await _collectorCost.FlushAsync(connection, DateTime.UtcNow, _logger, budget.Token);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown — quiet and expected. */
        }
        catch (Exception ex)
        {
            /* #2317: a command TIMEOUT and a genuine fault are the same Npgsql message here ("Exception
               while reading from stream" — the #2294 lesson), and on the dogfood box that costume produced
               ~5 fake network-fault ERRORs a day. Name each cause; both are one-hour series gaps that
               self-heal on the next tick. The budget CTS surfaces as OperationCanceledException — with
               the SERVICE token untripped that can only be the sweep budget, so it takes the timeout
               arm too. */
            /* NOT counted by #3013's swallowed-read counter, in either arm: this sweep WRITES the
               self-metrics series. No alert is judged on its result — the store self-alerts read their own
               evidence in EvaluateCompressionJobHealthAsync, which IS counted. */
            if (PgBaselineProvider.IsCommandTimeout(ex) || (ex is OperationCanceledException && budget.IsCancellationRequested))
            {
                _logger.LogError(
                    "Store self-metrics sweep did not finish within its {Timeout}s command timeout — this tick's metrics are skipped and the series gains a one-hour gap (the store side logs this as 'canceling statement due to user request'). If it repeats, the store's sizing queries have outgrown the timeout: {Message}",
                    StoreSelfMetrics.SweepTimeoutSeconds, ex.Message);
            }
            else
            {
                _logger.LogError("Store self-metrics sweep failed: {Message}", ex.Message);
            }
        }
    }

    /// <summary>
    /// Whether a finished analysis pass may route its findings to the notification channels: the master
    /// alerts switch AND the analysis family toggle, the exact idiom the connection-change gate
    /// established (<c>!_settings.AlertsEnabled || !_notifyConnectionChanges()</c> —
    /// DarlingSelfAlertEvaluator.ApplyConnectionOutcomeAsync). #3464's first measured bypass was this
    /// decision made from the family toggle ALONE: an Analysis INFO card reached a live paging channel
    /// 38 minutes into a fleet-wide mute, because "delivery gated on analysis_notifications_enabled"
    /// had quietly REPLACED the master consult rather than adding to it. The split itself is unchanged
    /// and deliberate (Lite's D0): the pass runs and persists findings whatever this returns — the
    /// master switch's own contract (Darling/README.md, the alerts section) promises exactly that,
    /// "turns off all alert evaluation and analysis finding notifications (the analysis itself
    /// still runs and persists findings)" — so the Recommendations tab keeps filling while the channels
    /// stay silent, and the toggle remains the narrower knob for fleets that want alerting without
    /// analysis mail. One predicate, called by BOTH analysis entry points (the scheduled tick and
    /// analyze_now), so a third entry point has one right thing to call and the census test one shape to
    /// pin. Static and pure so the truth table is testable without a worker.
    /// </summary>
    internal static bool ShouldNotifyAnalysisFindings(DarlingConfig config) =>
        config.Alerts.Enabled && config.Analysis.NotificationsEnabled;

    /// <summary>
    /// #3467: one same-statement-pileup evaluation for one server — the collection-cadence analysis
    /// finding. Reads the newest active-query snapshot window (query_snapshots alone — the #2296
    /// deployments have no Query Store to read, so nothing here may key on one), hands it to the
    /// shared detector, and routes any detection through the SAME finding machinery the scheduled
    /// pass uses: mute-filter → materialize → drill-down → persist → notify-if-delivering. Riding
    /// PgFindingStore means the finding inherits everything the scheduled findings already have —
    /// the mute registry, the (story_path_hash, incident_id) occurrence folding that makes episode
    /// two of the same statement a recurrence on episode one's trail, the notification service's
    /// severity gate (notify_severity), its incident-keyed cooldown, and the #2054
    /// worsening-re-notify — and adds no delivery SEMANTICS of its own. It is a new delivery call
    /// site, which is a different thing and is accounted for as one: the single NotifyAsync below sits
    /// under ShouldNotifyAnalysisFindings(config), carries its own #3465 census entry (Inline), and is
    /// pinned in both directions by AlertMasterSwitchSurfaceTests.
    ///
    /// <para><b>Achieved latency for the measured incident</b>: the pileup snapshot landed 20:10:31Z;
    /// this evaluation runs within the 30-second sweep stamp of the collector write, so the finding —
    /// severity 2.0 against the 1.5 gate — exists and notifies by ~20:11Z, while the episode (which
    /// self-cleared around 20:11) is still alive. The scheduled anomalies arrived 20:31:48Z capped at
    /// 1.0; the query_store-fed PLAN_REGRESSION could not exist before ~20:38Z.</para>
    ///
    /// <para>Failure containment: a store fault costs one log line and this cycle's evaluation — never
    /// the sweep body's remaining passes. InsertFindingsAsync throws by design on a rolled-back batch
    /// (#2448); that throw is contained here because the per-instant dedup stamp advances only after a
    /// successful persist, so the next sweep re-derives the SAME instant rather than waiting for a
    /// fresh one — occurrence folding absorbs the re-persist, the incident-keyed cooldown absorbs the
    /// re-notify — which is precisely what a live-signature finding can afford that a scheduled pass
    /// cannot.</para>
    /// </summary>
    private async Task EvaluateSameStatementPileupAsync(
        ServerLoopState server,
        DarlingConfig config,
        AnalysisNotificationService notificationService,
        CancellationToken stoppingToken)
    {
        var runtime = server.Runtime;
        if (runtime is null)
        {
            return;
        }

        try
        {
            /* Window floor: lookback + staleness margin behind "now", so the newest instant's full
               baseline window is covered even when the newest snapshot itself is a few minutes old.
               The detector applies the real staleness rule against the newest instant it finds. */
            var reader = new PgPileupSnapshotReader(_postgres!, _logger);
            var floor = DateTime.UtcNow.AddMinutes(
                -(SameStatementPileupDetector.BaselineLookbackMinutes + SameStatementPileupDetector.StaleSnapshotCutoffMinutes));
            var rows = await reader.ReadWindowAsync(runtime.ServerId, floor, stoppingToken);
            if (rows.Count == 0)
            {
                return;
            }

            /* One evaluation per snapshot instant: the 30-second sweep outpaces the one-minute
               collector, and re-running the detector over an already-evaluated instant would persist
               duplicate occurrence rows for the same evidence. The stamp advances only once the
               instant's OUTCOME is settled — evaluated clean, fully muted, or persisted — never
               before the persist: stamped ahead of InsertFindingsAsync, a transient store fault would
               mark a firing instant "done" and lose its finding for good if the episode cleared
               before the next collector write. Left behind on a fault, the next sweep re-derives the
               same instant instead (the 30-second sweep usually gets its retry in before the
               one-minute collector replaces the instant), and the machinery downstream absorbs the
               replay: a duplicate persist folds onto the same (story_path_hash, incident_id) trail,
               a duplicate notify dies in the incident-keyed cooldown. */
            var latest = DateTime.MinValue;
            foreach (var row in rows)
            {
                if (row.CollectionTime > latest)
                {
                    latest = row.CollectionTime;
                }
            }

            if (latest <= server.LastPileupSnapshotEvaluated)
            {
                return;
            }

            var detections = SameStatementPileupDetector.Evaluate(runtime.StorageName, rows, DateTime.UtcNow);
            if (detections.Count == 0)
            {
                server.LastPileupSnapshotEvaluated = latest;
                return;
            }

            /* The scheduled pass's two-phase persist, transplanted: mute-filter materializes the
               surviving findings (the STORAGE identity, same as every collected row), the drill-down
               evidence is attached between the phases, and the insert commits the set or nothing. */
            var context = new AnalysisContext
            {
                ServerId = runtime.ServerId,
                ServerName = runtime.StorageName,
                TimeRangeStart = latest.AddMinutes(-SameStatementPileupDetector.BaselineLookbackMinutes),
                TimeRangeEnd = latest,
                CancellationToken = stoppingToken,
                ShutdownToken = stoppingToken,
            };

            var findingStore = new PgFindingStore(_postgres!, _logger);
            var findings = await findingStore.FilterMutedFindingsAsync(
                detections.Select(d => d.Story).ToList(), context);
            if (findings.Count == 0)
            {
                server.LastPileupSnapshotEvaluated = latest;
                return;
            }

            var drillDownByHash = detections.ToDictionary(d => d.Story.StoryPathHash, d => d.DrillDown);
            foreach (var finding in findings)
            {
                if (drillDownByHash.TryGetValue(finding.StoryPathHash, out var drillDown))
                {
                    finding.DrillDown = drillDown;
                }
            }

            await findingStore.InsertFindingsAsync(findings, context);
            server.LastPileupSnapshotEvaluated = latest;

            _logger.LogWarning(
                "[{Server}] Same-statement pileup detected: {Count} finding(s) at snapshot {Snapshot:u}, peak severity {Severity:F2}",
                server.Config.DisplayName, findings.Count, latest, findings.Max(f => f.Severity));

            /* Delivery, gated exactly like the scheduled pass's findings: master AND family, one
               predicate (#3464). Below the gate, the notification service applies notify_severity,
               the incident-keyed cooldown, and worsening re-notify — this path adds no delivery
               semantics of its own. */
            if (ShouldNotifyAnalysisFindings(config))
            {
                await notificationService.NotifyAsync(findings);
            }
        }
        catch (OperationCanceledException)
        {
            /* Shutdown — quiet and expected. */
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "[{Server}] Same-statement pileup evaluation failed — skipped this cycle; the next sweep re-evaluates a fresher window: {Message}",
                server.Config.DisplayName, ex.Message);
        }
    }

    /// <summary>
    /// Runs the AN3 analysis pipeline for one connected server and routes the findings to the
    /// shared notification path — Lite's CollectionBackgroundService.RunAnalysisIfDueAsync
    /// per-server body transplanted: the in-flight guard skips a server whose previous
    /// (possibly hung) pass has not finished; a FRESH DarlingAnalysisService per run
    /// (IsAnalyzing is a single instance flag, so a shared instance whose task is abandoned on
    /// timeout would block analysis for every other server); the 120-second timeout moves the
    /// loop on without clearing the in-flight marker (the continuation clears it only when the
    /// task truly finishes, so a hung server is not relaunched); findings are persisted inside
    /// AnalyzeAsync and only routed to the notification channels when delivery is on. The
    /// finding identity is the STORAGE name + its hash id — the same identity the collectors
    /// stamp on every row (Lite's GetServerNameForStorage semantics), so findings join the
    /// collected data; the alert engine's DisplayName snapshot identity is deliberately not
    /// used here.
    /// </summary>
    private async Task RunScheduledAnalysisAsync(
        ServerLoopState server,
        PgPlanFetcher planFetcher,
        AnalysisNotificationService notificationService,
        bool notifyFindings,
        CancellationToken stoppingToken)
    {
        var runtime = server.Runtime;
        if (runtime is null)
        {
            return;
        }

        /* #2138: the force-plan bot rides the scheduled pass's findings — same evidence the operator
           sees, no second analysis. server.Config (not runtime.Config) so a store-reload change to
           the per-server opt-in is honored on the next pass. A disabled bot returns immediately, so
           hooking it unconditionally costs a delegate allocation and a bool test. */
        var planForceBot = _planForceBot;
        Func<IReadOnlyList<AnalysisFinding>, Task>? postPassHook =
            planForceBot is not null
                ? findings => planForceBot.RunAfterAnalysisAsync(runtime, server.Config, findings, stoppingToken)
                : null;

        /* The scheduled caller discards the outcome — the analyze_now command maps it to a result. */
        await RunAnalysisPassAsync(
            runtime.ServerId, runtime.StorageName, server.Config.DisplayName,
            planFetcher, notificationService, notifyFindings, postPassHook, stoppingToken);
    }

    /// <summary>Terminal states of one analysis pass — surfaced to the analyze_now command result.</summary>
    private enum AnalysisPassStatus { Ran, Skipped, TimedOut, InsufficientData, Error }

    private sealed record AnalysisPassResult(AnalysisPassStatus Status, int FindingCount, string? Message);

    /// <summary>
    /// One analysis pass for a server — the shared core of the scheduled sweep and the <c>analyze_now</c>
    /// command. Lite's per-server body: the in-flight guard skips a server whose previous (possibly hung)
    /// pass has not finished; a FRESH <see cref="DarlingAnalysisService"/> per run (IsAnalyzing is a single
    /// instance flag); the 120-second timeout moves on without clearing the in-flight marker (the
    /// continuation clears it only when the task truly finishes, so a hung server is not relaunched);
    /// findings persist inside AnalyzeAsync and route to the notification channels only when delivery is on
    /// (Lite's D0 split). Returns the terminal state so the command path can report it; the scheduled caller
    /// ignores the return. Analyzes the STORAGE identity (Lite's GetServerNameForStorage), so a disconnected
    /// but previously-collected server can still be analyzed on demand (its stored data drives the pass).
    /// </summary>
    private async Task<AnalysisPassResult> RunAnalysisPassAsync(
        int serverId,
        string storageName,
        string displayName,
        PgPlanFetcher planFetcher,
        AnalysisNotificationService notificationService,
        bool notifyFindings,
        Func<IReadOnlyList<AnalysisFinding>, Task>? postPassHook,
        CancellationToken stoppingToken)
    {
        if (!_analysisInFlight.TryAdd(serverId, new AnalysisPassState(DateTime.UtcNow)))
        {
            ReportStuckAnalysis(serverId, displayName);
            return new AnalysisPassResult(AnalysisPassStatus.Skipped, 0, "analysis is already running for this server");
        }

        CancellationTokenSource? passCts = null;
        var passStarted = false;

        try
        {
            var analysisService = new DarlingAnalysisService(_postgres!, planFetcher, _logger, _baselineCache);

            /* #2430: the TOKEN is the budget now; the Task.Delay below is only this sweep's patience.
               Before this, AnalyzeAsync received the STOPPING token and nothing else, so the timeout
               abandoned the wait without cancelling any work — and since the marker below is released
               only on true completion, a pass that never finished left this server skipped in silence
               for the life of the process.

               Arming the CTS before the task exists means Cancel can never race the continuation that
               disposes it. There is deliberately no Task.Run here, unlike the Lite twin: DuckDB
               implements no async execution, so Lite's pass ran its whole collection phase inline and
               the race could not fire at all, while Npgsql is genuinely async and hands this thread
               back at the first read. That difference is also why this defect stayed invisible on
               Darling — a slow pass never took the sweep down with it, it just went quiet. */
            passCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            passCts.CancelAfter(s_analysisTimeout);
            var cts = passCts;

            /* Both tokens, and they mean different things: the first is what the reads observe, the
               second is the only one that still means "the host is stopping". Handing the armed token
               to the classifier alone would log every ordinary overrun as "abandoned at shutdown". */
            var analyzeTask = analysisService.AnalyzeAsync(
                serverId, storageName, hoursBack: 4, cts.Token, stoppingToken);

            /* Clear the in-flight marker only when the task truly finishes — not
               when the timeout below moves us on — so a hung server is not relaunched. */
            _ = analyzeTask.ContinueWith(
                completed =>
                {
                    _analysisInFlight.TryRemove(serverId, out _);
                    cts.Dispose();
                },
                TaskScheduler.Default);

            /* From here the continuation owns the marker and the token source, and neither
               ContinueWith nor the call above throws, so there is no window in which the pass exists
               with nothing committed to cleaning up after it. */
            passStarted = true;

            /* Wait the budget PLUS the unwind grace, so a pass that honours its cancellation is seen
               finishing here rather than racing this sweep's own timer. Losing that race now carries
               real information: the pass was asked to stop and did not. */
            var finished = await Task.WhenAny(
                analyzeTask, Task.Delay(s_analysisTimeout + s_analysisShutdownGrace, stoppingToken));

            if (stoppingToken.IsCancellationRequested)
            {
                /* #2299: the pass observes the same token (AnalysisContext.CancellationToken), so
                   hold this sweep open for a bounded grace and let it unwind — the loop's data
                   source is disposed when the sweeps drain, and before this hold it was disposed
                   UNDERNEATH the still-running pass, which cost a clean stop seven ERRORs. A pass
                   that outlives the grace keeps running into the disposal; its residue is then
                   classified as shutdown (Information) by the pass itself, and the in-flight
                   marker keeps it from being relaunched either way. */
                try
                {
                    /* CancellationToken.None on purpose: stoppingToken has already FIRED — passing
                       it would cancel this wait instantly and defeat the grace. */
                    await analyzeTask.WaitAsync(s_analysisShutdownGrace, CancellationToken.None);
                }
                catch (OperationCanceledException)
                {
                    /* Shutdown — quiet and expected. */
                }
                catch (TimeoutException)
                {
                    _logger.LogDebug(
                        "[{Server}] Analysis pass did not unwind within {Grace}s of shutdown — its residue is classified as shutdown, not fault",
                        displayName, (int)s_analysisShutdownGrace.TotalSeconds);
                }
                return new AnalysisPassResult(AnalysisPassStatus.Skipped, 0, "service is stopping");
            }

            if (finished != analyzeTask)
            {
                _logger.LogWarning(
                    "[{Server}] Analysis exceeded {Timeout}s and has not unwound the cancellation raised at that budget — skipped this cycle. This server stays skipped while the pass is in flight, and is reported again if it stays that way; every other server is unaffected.",
                    displayName, (int)s_analysisTimeout.TotalSeconds);
                return new AnalysisPassResult(AnalysisPassStatus.TimedOut, 0, $"analysis exceeded {(int)s_analysisTimeout.TotalSeconds}s");
            }

            /* Analysis already persisted its findings inside AnalyzeAsync. Only route them
               to the notification channels when delivery is on (Lite's D0 split: production
               unconditional, delivery gated). */
            var findings = await analyzeTask;

            /* A pass that ended early unwound as asked and returned nothing, so there is nothing to
               route — say so, rather than letting it read as a clean all-clear, which is what the old
               code did for every timed-out pass that came back before the sweep gave up on it.

               READ the pass's own classification rather than re-deriving one here (review, #2430). "No
               findings and the budget token has fired" is equally true of a genuine fault that landed
               after the budget expired, and calling that a timeout would bury the pass's ERROR under a
               Warning saying it merely ran out of time. The pass classified this once and logged the
               single line for it, so this adds no second line of its own — it only turns the answer
               into the terminal state analyze_now reports. */
            if (analysisService.EndedEarlyAs is AnalysisAbandonKind ending)
            {
                return ending switch
                {
                    AnalysisAbandonKind.Shutdown =>
                        new AnalysisPassResult(AnalysisPassStatus.Skipped, 0, "service is stopping"),
                    AnalysisAbandonKind.Timeout =>
                        new AnalysisPassResult(AnalysisPassStatus.TimedOut, 0,
                            $"analysis exceeded {(int)s_analysisTimeout.TotalSeconds}s"),
                    _ => new AnalysisPassResult(AnalysisPassStatus.Error, 0,
                        "analysis failed — the pass logged the fault"),
                };
            }

            if (notifyFindings)
            {
                await notificationService.NotifyAsync(findings);
            }

            /* #2138: the force-plan bot's post-analysis pass (scheduled runs only — the analyze_now
               command passes null). Failure-isolated twice over: the bot isolates its own seams, and
               this wrap keeps any residue from reclassifying a perfectly good analysis pass. */
            if (postPassHook is not null)
            {
                try
                {
                    await postPassHook(findings);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(
                        "[{Server}] Post-analysis force-plan bot pass failed: {Message}", displayName, ex.Message);
                }
            }

            /* Persist the pass's data-state determination (V19 marker) so the Viewer's Recommendations
               tab shows a reason instead of a false all-clear on a zero-finding read: true + the
               engine's message when the pass hit the 24h data-span gate ("still collecting"); false +
               the engine's message when the pass cleared the gate but the window itself collected zero
               facts (#3524/#3551 — a dead-collector shape, "collection appears broken"; false-with-a-
               message is a shape only this arm writes, so the viewer distinguishes it without a schema
               change); cleared (false + null) when a real pass completed on measured facts, which is
               how both miss markers self-heal. Failure-isolated like the other observability writes.
               Only the REAL terminal states write it — a Skipped/TimedOut/Error pass (handled above /
               in the catch) leaves the last known marker untouched. */
            /* #3691: the sweep's own pass result, the row an operator reads in the analysis state. A pass
               that could not read a fact family still RAN and still counted its findings honestly — it just
               counted them over less evidence than it looks like, and the message is the only place that
               says so. Composed, never overwritten: the window-empty arm below already has a sentence. */
            var collectionCaveat = analysisService.LastCollectionFailures.Count > 0
                ? CollectionCaveats.Describe(analysisService.LastCollectionFailures, analysisService.LastCollectionFamilyCount)
                : null;
            if (analysisService.InsufficientDataMessage is string insufficient)
            {
                await DarlingObservability.WriteAnalysisStateAsync(
                    _postgres!, serverId, insufficientData: true, insufficient, _logger, stoppingToken);
                return new AnalysisPassResult(AnalysisPassStatus.InsufficientData, 0, insufficient);
            }

            if (analysisService.WindowEmptyMessage is string windowEmpty)
            {
                await DarlingObservability.WriteAnalysisStateAsync(
                    _postgres!, serverId, insufficientData: false, windowEmpty, _logger, stoppingToken);
                return new AnalysisPassResult(AnalysisPassStatus.Ran, 0, CollectionCaveats.Compose(windowEmpty, collectionCaveat));
            }

            await DarlingObservability.WriteAnalysisStateAsync(
                _postgres!, serverId, insufficientData: false, null, _logger, stoppingToken);
            return new AnalysisPassResult(AnalysisPassStatus.Ran, findings.Count, collectionCaveat);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            /* Shutting down — the loop's own cancellation check ends the sweep. */
            return new AnalysisPassResult(AnalysisPassStatus.Skipped, 0, "service is stopping");
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Server}] Analysis failed: {Message}", displayName, ex.Message);

            /* If the pass was never launched (e.g. the service ctor threw), no continuation exists to
               clear the marker or release the token source — do both here, or this server is skipped
               forever, which is the very defect this method is being fixed for. Once the pass IS
               running the continuation owns them, and clearing them here would pull the token out
               from under a live pass and re-admit that server next cycle on top of it. The old code
               cleared unconditionally; it got away with it only because every path that could reach
               here after launch had already completed the task. */
            if (!passStarted)
            {
                _analysisInFlight.TryRemove(serverId, out _);
                passCts?.Dispose();
            }

            return new AnalysisPassResult(AnalysisPassStatus.Error, 0, ex.Message);
        }
    }

    /// <summary>
    /// #2430: reports a server whose analysis pass is still in flight from an earlier cycle. The
    /// in-flight guard is deliberately released only on true completion — that is what stops a hung
    /// server piling up passes — but it also means a pass that never completes leaves the marker set
    /// for the life of the service, and every later cycle skipped that server with nothing said at all.
    /// The cancellation now raised at the budget clears the great majority of those; what remains is
    /// the pass wedged in a store read that takes no token, and this is what makes THAT visible rather
    /// than silent.
    ///
    /// <para>A permanently-skipped server that is loudly skipped is a far smaller bug than one silently
    /// skipped: the first costs findings and says so, the second looks exactly like a server with
    /// nothing wrong with it.</para>
    /// </summary>
    private void ReportStuckAnalysis(int serverId, string displayName)
    {
        if (!_analysisInFlight.TryGetValue(serverId, out var state))
        {
            /* Finished between the TryAdd above and this read — it was never stuck. */
            return;
        }

        state.SkippedCycles++;

        var inFlightFor = DateTime.UtcNow - state.StartedUtc;
        var reportAfter =
            (s_analysisTimeout * StuckAnalysisMultiple) *
            Math.Pow(2, Math.Min(state.ReportCount, StuckAnalysisMaxBackoffDoublings));

        if (inFlightFor < reportAfter)
        {
            return;
        }

        state.ReportCount++;

        _logger.LogError(
            "[{Server}] Analysis has been in flight for {Minutes:F0} minutes — over {Multiple}x its {Timeout}s budget — and did not stop when cancelled at that budget. {Skipped} analysis cycle(s) have been skipped for this server since, and every later cycle is skipped too until the pass unwinds or the service restarts. Analysis for every other server is unaffected.",
            displayName, inFlightFor.TotalMinutes, StuckAnalysisMultiple,
            (int)s_analysisTimeout.TotalSeconds, state.SkippedCycles);
    }

    /// <summary>
    /// The <c>analyze_now</c> command handler (Recommendations "Generate now", control-plane form): forces
    /// an immediate analysis pass for one monitored server, bypassing its NextAnalysisDue wait, and maps the
    /// terminal outcome to a command result. Shares the in-flight guard with the scheduled sweep, so it
    /// no-ops (reported "already running") rather than racing a pass in flight for the same server.
    /// </summary>
    private async Task<CommandOutcome> RunAnalyzeNowAsync(
        List<ServerLoopState> servers,
        PgPlanFetcher planFetcher,
        AnalysisNotificationService notificationService,
        DarlingConfig config,
        int serverId,
        CancellationToken cancellationToken)
    {
        ServerLoopState? server;
        lock (_serversLock)
        {
            server = servers.Find(s => s.Config.ServerId == serverId);
        }

        if (server is null)
        {
            return new CommandOutcome(false, "server not monitored", JsonError($"no monitored server with server_id {serverId}"));
        }

        /* "Generate now" takes the same pass every engine does (#3542). This door used to carry its own
           PostgreSQL arm — the scheduled tick's tombstone re-written here so an operator's click could not
           overwrite it with the generic "still collecting" text — and it went with the tick's gate: the
           pass routes by the registry's engine_kind inside DarlingAnalysisService, so the honest answer for
           a PostgreSQL target is now the pass's own result, and there is no tombstone left to protect. */

        /* postPassHook: null — analyze_now is an interactive diagnostic, and the force-plan bot only
           rides the SCHEDULED cadence so an operator poking a server cannot spend its action budget.
           Findings still come back in the command result under master-off — the operator asked a question
           and gets the answer — but the notification CHANNELS stay silent (#3464): an on-demand pass must
           not be the one analysis entry point that can page through a mute. */
        var result = await RunAnalysisPassAsync(
            serverId, server.Config.StorageName, server.Config.DisplayName,
            planFetcher, notificationService, ShouldNotifyAnalysisFindings(config), postPassHook: null, cancellationToken);

        return result.Status switch
        {
            AnalysisPassStatus.Ran => new CommandOutcome(true, "analysis complete",
                JsonSerializer.Serialize(new { success = true, server = server.Config.DisplayName, findings = result.FindingCount })),
            AnalysisPassStatus.InsufficientData => new CommandOutcome(true, "insufficient data",
                JsonSerializer.Serialize(new { success = true, server = server.Config.DisplayName, message = result.Message })),
            AnalysisPassStatus.Skipped => new CommandOutcome(false, "analysis already running", JsonError(result.Message ?? "skipped")),
            AnalysisPassStatus.TimedOut => new CommandOutcome(false, "analysis timed out", JsonError(result.Message ?? "timed out")),
            _ => new CommandOutcome(false, "analysis failed", JsonError(result.Message ?? "error")),
        };
    }

    /// <summary>A failure result_json body: <c>{ "success": false, "error": ... }</c>.</summary>
    private static string JsonError(string error) => JsonSerializer.Serialize(new { success = false, error });

    /// <summary>
    /// #4130: the launch-loop decision for the daily retention purge, extracted so it is testable without
    /// driving the whole fleet loop. Fires and tracks <paramref name="startPurge"/> in <see cref="_purgeTask"/>
    /// exactly like the per-server sweeps (<see cref="ServerLoopState.InFlightSweep"/>) and the oversized-plan
    /// backlog (<see cref="_oversizedPlanSweep"/>) — never awaited here, so this returns immediately whether
    /// or not it launched anything.
    ///
    /// <para>The purge was measured at 346-400s deleting ~839k rows. Awaited inline on this loop (the pre-4130
    /// shape), that is 346-400s in which the loop launches NO server sweeps and the whole fleet reads
    /// stale — the same shape as the oversized-plan backlog's own field incident, one maintenance step
    /// over.</para>
    ///
    /// <para>Owns <c>_nextPurgeUtc</c>'s update too, rather than leaving it to the caller: on a launch it
    /// advances the stamp by the full 24h cadence, and when the due time arrived but the previous purge is
    /// still running it leaves the stamp untouched, so the next launch-loop tick tries again rather than
    /// silently pushing the purge out by a whole day. A slow purge that overruns one 24h cycle simply gets
    /// its next attempt on the very next tick once it completes, rather than piling up a second instance on
    /// top of the first. Returns whether it launched.</para>
    /// </summary>
    internal bool TryStartScheduledPurge(
        DateTime nowUtc, Func<CancellationToken, Task> startPurge, CancellationToken stoppingToken)
    {
        if (nowUtc < _nextPurgeUtc)
        {
            return false;
        }

        if (_purgeTask is { IsCompleted: false })
        {
            _logger.LogInformation(
                "daily retention purge was still running at its next scheduled time — skipping this launch; "
                + "it will be retried on the next tick once the current run completes");
            return false;
        }

        _nextPurgeUtc = nowUtc.AddHours(24);
        _purgeTask = RunTrackedAsync(startPurge, stoppingToken);
        return true;
    }

    /// <summary>
    /// Wraps a purge delegate so <see cref="_purgeTask"/> can never fault unobserved (the launch loop never
    /// awaits it, so an unhandled fault here would otherwise surface only as an UnobservedTaskException at
    /// GC time). <see cref="OperationCanceledException"/> is swallowed too — that is the normal shutdown-drain
    /// outcome once <c>stoppingToken</c> is cancelled, not a failure to log as one.
    /// </summary>
    private async Task RunTrackedAsync(Func<CancellationToken, Task> startPurge, CancellationToken stoppingToken)
    {
        try
        {
            await startPurge(stoppingToken);
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown drain. */
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "daily retention purge failed");
        }
    }

    /// <summary>
    /// The daily retention purge's actual work — <see cref="DarlingRetention.PurgeAsync"/>, then the AN3
    /// findings cleanup, the #1652 log-file sweep, and the module-map refresh — moved out of the launch loop
    /// body so <see cref="TryStartScheduledPurge"/> can fire-and-track it as one delegate. Order and behavior
    /// are unchanged from the pre-4130 inline sequence.
    /// </summary>
    private async Task RunScheduledPurgeAsync(NpgsqlDataSource postgres, DarlingConfig config, CancellationToken stoppingToken)
    {
        /* Honor fleet-wide retention overrides (config_collector_schedules, server_id NULL) layered
           on CollectorScheduleDefaults; a per-server override can't apply to a shared-table purge.
           Empty overrides (Stage 1 seeds none) resolve to the defaults — identical behavior. */
        var overrides = _scheduleOverrides;
        await DarlingRetention.PurgeAsync(
            postgres, _timescaleAvailable, _logger, stoppingToken,
            name => StoreConfigProvider.ResolveFleetRetentionDays(name, overrides),
            config.PlanContentRetentionDays);

        /* AN3: findings retention. Both apps' finding stores declare a cleanup but neither
           app schedules it (Lite's DuckDB archive-reset bounds it incidentally); a 24/7
           service must actually invoke it or analysis_findings grows unbounded. Rides the
           daily purge; never throws (logs + degrades). The horizon is the shared base window
           rather than a literal of the same value, so findings stay worth exactly as long as
           the metric data they are correlated against instead of holding at 30 on their own
           if that window ever moves. */
        await new PgFindingStore(postgres, _logger).CleanupOldFindingsAsync(
            retentionDays: DarlingRetention.DataRetentionBaseDays);

        /* #1652: sweep the service's own rolling log files. The provider swept only in its
           constructor, so a service up for months — the normal case — swept once at startup and
           never again while writing a file a day. Rides the daily purge like every other
           maintenance chore; static + best-effort, so the worker needs no reference to the
           provider the host owns and a locked file can never break the tick. */
        DarlingFileLoggerProvider.SweepOldFiles(DarlingFileLoggerProvider.DefaultLogDirectory());

        /* Keep the retained sql_handle->module map current (object_name attribution for old query_stats
           CAGG windows). Rides the daily purge; failure-isolated inside RefreshAsync. */
        await using var moduleMapConnection = await postgres.OpenConnectionAsync(stoppingToken);
        await DarlingModuleMap.RefreshAsync(moduleMapConnection, _logger, stoppingToken);

        /* #4211: the raw hypertable chunk-interval reconcile. Rides the daily purge tick like every other
           maintenance chore above — first pass after startup (TryStartScheduledPurge's own MinValue seed),
           then every 24h. TimescaleDB-only (chunk_time_interval has no meaning on plain PostgreSQL) and
           behind its own off switch; never throws — a bad reading on one run degrades to "no changes" on the
           next, the same failure shape as the AN3 cleanup just above. */
        if (_timescaleAvailable && config.RawChunkIntervalReconcileEnabled)
        {
            try
            {
                var budgetBytes = await ResolveRawChunkIntervalBudgetBytesAsync(postgres, config, stoppingToken);
                if (budgetBytes is double resolvedBudget)
                {
                    await using var reconcileConnection = await postgres.OpenConnectionAsync(stoppingToken);
                    await RawChunkIntervalReconciler.ReconcileAsync(
                        reconcileConnection, resolvedBudget, DateTime.UtcNow, _logger, stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                /* Expected on shutdown drain. */
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "raw chunk interval reconcile failed");
            }
        }
    }

    /// <summary>
    /// Budget B for <see cref="RawChunkIntervalReconciler.ReconcileAsync"/> (#4211 ruling decision 2). A
    /// managed store reads the SAME raw RAM figure <see cref="DarlingManagedPostgres.DeriveMemorySettings"/>
    /// sizes Postgres itself from — <see cref="DarlingStoreHostProfile.GatherHostFacts"/>'s
    /// <c>Memory.TotalBytes</c>, which on Windows is already the reused
    /// <see cref="DarlingManagedPostgres.TryReadWindowsPhysicalMemoryBytes"/> read (ruling 2's "reuse the
    /// authoritative read", never a second <c>GlobalMemoryStatusEx</c>), quantized the same way — NOT
    /// <see cref="DarlingManagedPostgres.MemorySettings.SharedBuffersMb"/>, which is capped at 1 GB for the
    /// co-located / Windows 487 mitigation (#1559) and is a real setting, not a sizing ceiling (see
    /// <see cref="RawChunkIntervalPlanner.ManagedBudgetBytes"/>'s remarks). A bring-your-own store instead reads
    /// <c>shared_buffers</c>/<c>effective_cache_size</c> live off <c>pg_settings</c>, through
    /// <c>pg_size_bytes(current_setting(...))</c> so the GUC's storage unit (blocks, kB, whatever) never has to
    /// be parsed by hand. Returns null only when the BYO read fails (an unreachable store on this tick) —
    /// the caller then skips the whole reconcile pass rather than plan against a made-up budget.
    ///
    /// <para>Internal, not private, so a live test can drive the bring-your-own <c>pg_settings</c> path against
    /// a real connection without constructing the whole worker — the same reach
    /// <see cref="DarlingManagedPostgres.TryReadWindowsPhysicalMemoryBytes"/> is internal for.</para>
    /// </summary>
    internal static async Task<double?> ResolveRawChunkIntervalBudgetBytesAsync(
        NpgsqlDataSource postgres, DarlingConfig config, CancellationToken cancellationToken)
    {
        if (config.Postgres.Managed)
        {
            var hostFacts = DarlingStoreHostProfile.GatherHostFacts();
            return RawChunkIntervalPlanner.ManagedBudgetBytes(DarlingManagedPostgres.QuantizeRam(hostFacts.Memory.TotalBytes));
        }

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            "SELECT pg_size_bytes(current_setting('shared_buffers')), pg_size_bytes(current_setting('effective_cache_size'))",
            connection)
        { CommandTimeout = 30 };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return RawChunkIntervalPlanner.BringYourOwnBudgetBytes(reader.GetInt64(0), reader.GetInt64(1));
        }

        return null;
    }

    /// <summary>
    /// The <c>purge_now</c> command handler (the daily retention purge on demand): runs
    /// <see cref="DarlingRetention.PurgeAsync"/> over the shared store immediately and reports the tables +
    /// rows purged. Fleet-wide over the SHARED tables (no target server). When
    /// <paramref name="customRetentionDays"/> is set it purges every collector to that horizon (a
    /// <c>_ =&gt; customDays</c> resolver — PurgeAsync clamps a sub-1-day horizon at its destructive sink, so a
    /// bad custom-N can never wipe a table); otherwise it uses the SAME fleet resolver the scheduled daily
    /// purge uses (<see cref="StoreConfigProvider.ResolveFleetRetentionDays"/> over the live overrides). Takes
    /// NO collection gate: unlike snapshot_now a purge writes no collector state and races no delta baseline,
    /// and PurgeAsync is idempotent + failure-isolated per table, so it may safely overlap the daily sweep.
    /// </summary>
    private async Task<CommandOutcome> RunPurgeNowAsync(DarlingConfig config, int? customRetentionDays, CancellationToken cancellationToken)
    {
        /* Reference read of the live overrides, matching the daily purge caller (never held under a lock —
           the reload swaps the whole list atomically). */
        var overrides = _scheduleOverrides;
        Func<string, int> resolver = customRetentionDays is int days
            ? _ => days
            : name => StoreConfigProvider.ResolveFleetRetentionDays(name, overrides);

        var summary = await DarlingRetention.PurgeAsync(
            _postgres!, _timescaleAvailable, _logger, cancellationToken, resolver,
            config.PlanContentRetentionDays);

        _logger.LogInformation(
            "purge_now purged {Tables} table(s), {Rows} row(s)/chunk(s){Custom}",
            summary.TablesPurged, summary.TotalPurged,
            customRetentionDays is int cd ? $" (custom retention {cd}d)" : string.Empty);

        var json = JsonSerializer.Serialize(new
        {
            success = true,
            tablesPurged = summary.TablesPurged,
            rowsPurged = summary.TotalPurged,
            rowsDeleted = summary.RowsDeleted,
            chunksDropped = summary.ChunksDropped,
            customRetentionDays,
        });
        return new CommandOutcome(true, "purge complete", json);
    }

    /// <summary>
    /// The worker's <see cref="IDarlingCommandHost"/> adapter (Stage 2): lets the command executor reach the
    /// two imperative commands that need the LIVE loop (snapshot_now / analyze_now) without the executor
    /// holding the worker's mutable loop state. Captures the running server set + collector runner + analysis
    /// pieces created in <see cref="RunCollectionLoopAsync"/>; the worker methods it calls take the
    /// <see cref="_serversLock"/> for the server lookup.
    /// </summary>
    private sealed class WorkerCommandHost : IDarlingCommandHost
    {
        private readonly DarlingWorker _worker;
        private readonly List<ServerLoopState> _servers;
        private readonly DarlingCollectorRunner _runner;
        private readonly PgPlanFetcher _planFetcher;
        private readonly AnalysisNotificationService _notificationService;
        private readonly DarlingConfig _config;

        public WorkerCommandHost(
            DarlingWorker worker, List<ServerLoopState> servers, DarlingCollectorRunner runner,
            PgPlanFetcher planFetcher, AnalysisNotificationService notificationService, DarlingConfig config)
        {
            _worker = worker;
            _servers = servers;
            _runner = runner;
            _planFetcher = planFetcher;
            _notificationService = notificationService;
            _config = config;
        }

        public Task<CommandOutcome> SnapshotNowAsync(int serverId, CancellationToken cancellationToken)
            => _worker.RunSnapshotAsync(_servers, _runner, serverId, cancellationToken);

        public Task<CommandOutcome> AnalyzeNowAsync(int serverId, CancellationToken cancellationToken)
            => _worker.RunAnalyzeNowAsync(_servers, _planFetcher, _notificationService, _config, serverId, cancellationToken);

        public Task<CommandOutcome> PurgeNowAsync(int? customRetentionDays, CancellationToken cancellationToken)
            => _worker.RunPurgeNowAsync(_config, customRetentionDays, cancellationToken);

        public Task<CommandOutcome> FetchPlanAsync(int serverId, PlanFetchRequest request, CancellationToken cancellationToken)
            => _worker.RunFetchPlanAsync(_servers, _planFetcher, serverId, request, cancellationToken);

        public Task<CommandOutcome> ExecuteActualPlanAsync(int serverId, ActualPlanRequest request, CancellationToken cancellationToken)
            => _worker.RunExecuteActualPlanAsync(_servers, serverId, request, cancellationToken);

        public Task<CommandOutcome> FetchActiveQueriesLiveAsync(int serverId, CancellationToken cancellationToken)
            => _worker.RunFetchActiveQueriesLiveAsync(_servers, _runner, serverId, cancellationToken);

        public Task<CommandOutcome> TestHypotheticalIndexAsync(int serverId, HypotheticalIndexRequest request, CancellationToken cancellationToken)
            => _worker.RunTestHypotheticalIndexAsync(_servers, serverId, request, cancellationToken);
    }

    /// <summary>
    /// The engine's live-msdb failed-jobs feed: runs the shared <see cref="FailedJobsQuery"/> on
    /// the monitored server's own connection. Gated !IsAzureSqlDb (the engine also gates on the
    /// snapshot; there is deliberately NO msdb-access probe — Phase-5 review F11) and degrades
    /// exactly like the Dashboard's caller: a login that cannot SELECT the msdb job tables
    /// raises SqlException 229/297/300/916 → Info + empty list; any other failure → Warning +
    /// empty list — a permission gap or transient error never fails the alert cycle.
    /// </summary>
    private async Task<List<FailedJobInfo>> FetchFailedJobsAsync(
        List<ServerLoopState> servers, string serverKey, int lookbackMinutes, CancellationToken cancellationToken)
    {
        /* Lookup only — held briefly under the lock (the command loop reconciles the list concurrently),
           then the connection I/O runs outside it. */
        ServerRuntime? runtime;
        lock (_serversLock)
        {
            runtime = servers
                .Select(s => s.Runtime)
                .FirstOrDefault(r => r is not null
                    && string.Equals(r.ServerId.ToString(CultureInfo.InvariantCulture), serverKey, StringComparison.Ordinal));
        }

        /* Engine first: msdb, SQL Agent and the whole FailedJobsQuery are SQL Server concepts, and this
           opens a SqlConnection below. On a PostgreSQL target it threw "Keyword not supported: 'host'" once
           per alert cycle. The IsAzureSqlDb arm stays for the same reason it always did — Azure SQL DB has
           no msdb either. */
        if (runtime is null
            || runtime.Target.Engine != CollectorTargetEngine.SqlServer
            || runtime.Target.IsAzureSqlDb)
        {
            return new List<FailedJobInfo>();
        }

        try
        {
            using var connection = new SqlConnection(runtime.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            using var command = new SqlCommand(FailedJobsQuery.Sql, connection) { CommandTimeout = 10 };
            command.Parameters.Add(new SqlParameter(FailedJobsQuery.LookbackMinutesParameter, SqlDbType.Int) { Value = lookbackMinutes });
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            return await FailedJobsQuery.ReadAsync(reader, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (SqlException ex) when (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
        {
            /* #2512: routed through the shared set rather than a fourth copy of it — this one had
               916 but neither 262 nor 8189, which is the drift the set exists to end. Widening is safe
               in the only direction that matters here: every number in it means the login cannot read
               what it asked for, and the response is to return no jobs rather than fail the alert
               cycle. A 262 naming msdb is exactly this case and used to fall through to the warning.
               Expected for read-only monitoring accounts; hit every alert cycle, so Info. The named
               remedy is direct table SELECTs, NOT SQLAgentReaderRole: that role gates the sp_help_job*
               interface only and confers nothing on the base tables this query reads — a #1823 field
               box had the role and still landed here every cycle. */
            _logger.LogInformation("[{Server}] Skipping recently-failed-job check (needs SELECT on msdb.dbo.sysjobs and sysjobhistory — SQLAgentReaderRole alone is not enough; see the monitoring-login grants in the README): {Message}",
                runtime.Config.DisplayName, ex.Message);
            return new List<FailedJobInfo>();
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: this reads the MONITORED SERVER's msdb over its own
               connection and its own timeout, not the store over the alert pass's deadline. It is a swallowed alert
               read, but not one #3013's mechanism can produce, and pooling the two would put a target-side outage
               in a number the operator reads as store contention. */
            _logger.LogWarning("[{Server}] Recently-failed-job check errored: {Message}",
                runtime.Config.DisplayName, ex.Message);
            return new List<FailedJobInfo>();
        }
    }

    /// <summary>
    /// The engine's #3497 Agent-job name lookup: one <see cref="AgentJobStepQuery"/> round trip against
    /// the monitored server's msdb for the (job, step) pairs a firing Long-Running Query card is about to
    /// show. Gated and degraded exactly like <see cref="FetchFailedJobsAsync"/> one method up, because it
    /// is the same kind of read against the same tables' neighborhood: engine-gated first (msdb, SQL Agent
    /// and the job-step program_name are SQL Server concepts — a PostgreSQL target must not pay for a
    /// SqlConnection it can never open), no Azure SQL DB (no msdb there), permission-denied → Info + empty
    /// map (expected for read-only monitoring logins; the alert cycle proceeds and the card renders the
    /// unresolved form with the raw job-id marker — ANNOTATION, NEVER SUPPRESSION: the card itself already
    /// fired before this read ran), anything else → Warning + empty map.
    /// </summary>
    private async Task<IReadOnlyDictionary<AgentJobStepKey, AgentJobStepNames>> FetchAgentJobStepNamesAsync(
        List<ServerLoopState> servers, string serverKey, IReadOnlyList<AgentJobStepKey> keys, CancellationToken cancellationToken)
    {
        /* Lookup only — held briefly under the lock (the command loop reconciles the list concurrently),
           then the connection I/O runs outside it. */
        ServerRuntime? runtime;
        lock (_serversLock)
        {
            runtime = servers
                .Select(s => s.Runtime)
                .FirstOrDefault(r => r is not null
                    && string.Equals(r.ServerId.ToString(CultureInfo.InvariantCulture), serverKey, StringComparison.Ordinal));
        }

        if (keys.Count == 0
            || runtime is null
            || runtime.Target.Engine != CollectorTargetEngine.SqlServer
            || runtime.Target.IsAzureSqlDb)
        {
            return new Dictionary<AgentJobStepKey, AgentJobStepNames>();
        }

        try
        {
            using var connection = new SqlConnection(runtime.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            using var command = new SqlCommand(AgentJobStepQuery.BuildSql(keys.Count), connection) { CommandTimeout = 10 };
            for (int i = 0; i < keys.Count; i++)
            {
                /* Bound as uniqueidentifier so the ENGINE's type system does the compare — the byte-order
                   conversion already happened once, in AgentJobStepQuery.TryParseProgramName, and binding a
                   string here would invite a second, divergent spelling of it. */
                command.Parameters.Add(new SqlParameter(AgentJobStepQuery.JobIdParameter(i), SqlDbType.UniqueIdentifier) { Value = keys[i].JobId });
                command.Parameters.Add(new SqlParameter(AgentJobStepQuery.StepIdParameter(i), SqlDbType.Int) { Value = keys[i].StepId });
            }

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            return await AgentJobStepQuery.ReadAsync(reader, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (SqlException ex) when (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
        {
            /* Same shared set, same reasoning, same remedy naming as the failed-jobs arm: every number in
               it means the login cannot read what it asked for, the response is to answer nothing rather
               than fail the alert cycle, and SQLAgentReaderRole is NOT the remedy (it gates sp_help_job*
               only). Info because a read-only monitoring login hits this on every Agent-annotated fire. */
            _logger.LogInformation("[{Server}] Skipping the Agent job-name lookup for the Long-Running Query card (needs SELECT on msdb.dbo.sysjobs and sysjobsteps — SQLAgentReaderRole alone is not enough; see the monitoring-login grants in the README). The card renders the unresolved form: {Message}",
                runtime.Config.DisplayName, ex.Message);
            return new Dictionary<AgentJobStepKey, AgentJobStepNames>();
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter, for FetchFailedJobsAsync's exact reason: this
               reads the MONITORED SERVER's msdb, not the store, and pooling the two would put a target-side
               outage in a number the operator reads as store contention. */
            _logger.LogWarning("[{Server}] Agent job-name lookup for the Long-Running Query card errored — the card renders the unresolved form: {Message}",
                runtime.Config.DisplayName, ex.Message);
            return new Dictionary<AgentJobStepKey, AgentJobStepNames>();
        }
    }

    private async Task TryConnectAsync(ServerLoopState server, DarlingCollectorRunner runner, DarlingConfig config, CancellationToken cancellationToken)
    {
        if (DateTime.UtcNow < server.NextConnectAttempt)
        {
            return;
        }

        try
        {
            var runtime = await DarlingServerConnector.ConnectAsync(server.Config, _logger, cancellationToken);
            server.Runtime = runtime;

            /* #2255: cleared on success so a LATER failure prints in full even when it carries the same
               message as one from before this connect. Without this, a fixed-then-broken-again cause would be
               suppressed as a repeat of something the operator had already scrolled past. */
            server.LastConnectFailureLogged = null;

            /* #2228: the tripwire. The connection just told us which database it actually landed in, so this
               is the one moment the registration's claim can be checked against the server's own answer.
               Identity is registration-derived and never verified against the connection, so without this a
               registration pointing somewhere else collects that other database's rows under its own id,
               indefinitely and silently — and if a sibling registration names that database too, both collect
               it and the history is duplicated under two identities (#2220's byte-identical graphs).

               ERROR, not Warning: nothing clears this on its own and every sweep in the meantime stores
               mis-attributed rows. Logged on the TRANSITION so a standing misconfiguration does not bury
               itself. */
            var mismatch = DarlingServerConnector.DescribeDatabaseMismatch(
                server.Config.Database, runtime.ConnectedDatabase, server.Config.DisplayName);

            if (!string.Equals(server.LastDatabaseMismatchLogged, mismatch, StringComparison.Ordinal))
            {
                server.LastDatabaseMismatchLogged = mismatch;
                if (mismatch is not null)
                {
                    _logger.LogError("[{Server}] {Mismatch}", server.Config.DisplayName, mismatch);
                }
                else
                {
                    /* The transition BACK is worth one line too: it is the confirmation that an operator's
                       edit actually took, which otherwise requires trusting silence. */
                    _logger.LogInformation(
                        "[{Server}] now connected to the database it is registered for ('{Database}') — the " +
                        "earlier mismatch (#2228) is resolved.",
                        server.Config.DisplayName, runtime.ConnectedDatabase);
                }
            }
            /* Force the long-query trace (#1496) to re-reconcile on the next sweep after every (re)connect:
               an Azure database-scoped session can stop on reconnect, so a still-"applied" flag would
               otherwise skip restarting it. Cheap — the reconcile no-ops unless the desired state differs
               from what actually exists (ensure is IF-NOT-running START; drop is IF EXISTS). */
            server.LongQueryTraceApplied = null;
            /* #3754: and the two run-record slots the reconcile fills, for the same reason - they describe
               the sessions as of the LAST reconcile, and the next one is about to run against a fresh
               connection. A fault left standing here would classify the first post-reconnect run
               SESSION_MISSING before the reconcile had a chance to succeed; a stale partial note would
               name databases the reconnect may have just fixed. */
            server.LongQueryTraceFault = null;
            server.LongQueryTracePartialNote = null;
            /* Capture the id once, while the connection is freshly established and non-null: an on-load
               RunOneAsync below can drop server.Runtime on a mid-collection connection-level failure, so any
               later read of server.Runtime.ServerId (the schedule resolve, the connection edge) would NRE. */
            var serverId = runtime.ServerId;

            /* Retired containment (#1553 D1), the CONNECT-path re-check: a reconcile-remove may have retired this
               server while this connect body sat QUEUED on the fleet gate or ran the connect I/O (3+ min under
               distress). The connect SUCCEEDED, but a just-disabled server must incur ZERO durable side-effects —
               so bail BEFORE the registry upsert, the XE CREATE SESSION DDL, and the Server-Restored edge,
               dropping the runtime we just took. The entry check in ProcessServerSweepAsync covers the queued-
               dequeue case; this closes the window where removal lands DURING the connect itself. */
            if (server.Retired)
            {
                server.Runtime = null;
                return;
            }

            _logger.LogInformation("[{Server}] Connected (major {Major}, edition {Edition}, server_id {ServerId})",
                server.Config.DisplayName,
                runtime.Target.SqlMajorVersion,
                runtime.Target.IsAzureSqlDb ? "AzureSqlDb" : runtime.Target.IsAzureManagedInstance ? "ManagedInstance" : "Box",
                serverId);

            /* Stage 4: the offline->online connection edge (Server Restored) — fired HERE, right after the
               connection is established and BEFORE the on-load collectors run, using the captured serverId.
               The connect succeeded regardless of what the on-load collectors do next, so this is the correct
               point to record "online" (and it can't NRE on a Runtime the on-load loop might drop). Fires only
               if the server was previously seen offline; the first-ever connect is a silent baseline (the
               state machine mirrors the Dashboard's skip-first-check). */
            await _selfAlerts!.ApplyConnectionOutcomeAsync(
                serverId, server.Config.DisplayName, online: true, error: null, cancellationToken);

            /* Same edge, second consumer: discard anything we concluded about this server while it was
               unreachable. An Azure SQL DB firewall rejection or failover is reported with the same error
               numbers as "this login may not read master", so a verdict formed during the outage can be
               wrong — and used to persist until the service restarted, quietly degrading database-scoped
               collection (#1506). */
            runner.OnServerReconnected(serverId);

            await DarlingObservability.UpsertServerAsync(_postgres!, runtime, _logger, cancellationToken);

            /* Extended Events are a SQL Server feature. Ungated, this ran SqlClient against a PostgreSQL
               target on every connect and logged "Failed to ensure XE sessions: Keyword not supported:
               'host'. - deadlock/blocked-process collection will read zero rows until resolved" — a warning
               that is both alarming and meaningless on an engine that has no XE, on a target whose deadlock
               collectors are engine-gated off anyway. Confirmed on a live PostgreSQL target. */
            if (runtime.Target.Engine == CollectorTargetEngine.SqlServer)
            {
                await DarlingXeSessions.EnsureAllAsync(runtime, runner, _logger, cancellationToken);
            }

            /* On-load config snapshots (effective FrequencyMinutes 0) run once per connect, then every
               scheduled collector becomes immediately due — mirrors Lite's server-open behavior. The
               effective schedule layers config_collector_schedules overrides on CollectorScheduleDefaults;
               a collector disabled by an override is neither run on-load nor scheduled.

               These on-load runs are NOT under the per-server CollectionGate (unlike the scheduled sweep and
               snapshot_now). Safe today: this path only runs while Runtime is null, and a snapshot_now needs
               a non-null Runtime, so the two never overlap for one server. If TryConnect's timing ever changes
               so a connect can race a snapshot, gate this loop too. */
            var now = DateTime.UtcNow;
            /* #1575: seed each scheduled collector's first post-connect due time from its persisted last-run
               watermark so a restart RESUMES the real cadence instead of re-phasing it up to a full interval
               forward (which starved long-frequency collectors like the daily index_object_stats across a
               restart-heavy window). ONE batched round-trip reads every collector's MAX(collection_time) for this
               server; ComputeSeededNextDue turns each into a due time — a recently-run collector waits out the
               remaining interval, an overdue / never-run one runs promptly under a small per-server jitter that
               de-clusters the fleet WITHOUT the old full-interval defer (#1553's anti-herd intent, capped). The
               steady-state advance in RunDueCollectorsAsync stays on the exact interval. */
            var watermarks = await ReadCollectorWatermarksAsync(_postgres!, serverId, _logger, cancellationToken);
            foreach (var name in CollectorScheduleDefaults.All.Keys)
            {
                /* The SAME pre-dispatch engine gate the scheduled sweep applies (see RunDueCollectorsAsync),
                   which this loop never got. Without it, the on-load pass dispatches every foreign-engine
                   collector once per connect: a PostgreSQL target ran server_config, database_config,
                   database_scoped_config, trace_flags and server_properties as T-SQL and logged five fake
                   SUCCESS rows with zero rows collected — confirmed on a live PostgreSQL target. Those rows
                   feed the health bands and analysis, which key on status, so a fake success is worse than an
                   error. Re-read from server.Runtime because a preceding RunOneAsync in this loop can have
                   nulled it on a connection-level failure. */
                if (server.Runtime is null
                    || !CollectorCatalog.EngineMatches(name, server.Runtime.Target)
                    /* And the within-engine gate, on EVERY engine — same reasoning as the scheduled sweep,
                       and extended to SQL Server with it (#2579). This loop runs on every connect and
                       reconnect, so leaving it PostgreSQL-scoped would keep landing fake SUCCESS rows for
                       gated-off SQL Server collectors at exactly the moments an operator is watching. */
                    || !CollectorCatalog.AppliesTo(name, server.Runtime.Target))
                {
                    continue;
                }

                /* Captured serverId, not server.Runtime.ServerId: an earlier on-load RunOneAsync in this loop
                   can null server.Runtime on a connection-level failure, which would otherwise NRE here. */
                var effective = StoreConfigProvider.ResolveSchedule(name, serverId, _scheduleOverrides);
                if (!effective.Enabled)
                {
                    continue;
                }

                if (effective.FrequencyMinutes == 0)
                {
                    /* null, not the live mark: the on-load dispatch is not a scheduled sweep body and
                       never resets it, so folding it in would mix a previous body's bookkeeping
                       into these rows - the cross-body contamination the reset exists to prevent. */
                    await RunOneAsync(server, runner, name, peerMaxAtDispatchMs: null, cancellationToken);

                    /* #3929/#3930: ALSO becomes due again on CollectorScheduleDefaults.OnLoadRecaptureMinutes,
                       seeded from the SAME pre-dispatch watermark used below - the run just above updates it
                       moments from now, but seeding from the watermark read at the top of this method means a
                       collector that already ran recently (a quick reconnect) is not re-phased a full day
                       forward. Without this, a long-lived connection never re-captures: its on-load config
                       snapshot ages out of retention (#3930) and a cleared trace flag has nothing to overwrite
                       its stale ON row (#3929) until the next reconnect. */
                    var onLoadInterval = CollectorScheduleDefaults.OnLoadRecaptureMinutes;
                    var onLoadLastRun = watermarks.TryGetValue(name, out var w0) ? w0 : (DateTime?)null;
                    var onLoadJitter = SeedJitter(serverId, onLoadInterval * 60);
                    server.NextDue[name] = ComputeSeededNextDue(onLoadLastRun, onLoadInterval, now, onLoadJitter);
                }
                else
                {
                    var lastRun = watermarks.TryGetValue(name, out var w) ? w : (DateTime?)null;
                    var jitter = SeedJitter(serverId, effective.FrequencyMinutes * 60);
                    server.NextDue[name] = ComputeSeededNextDue(lastRun, effective.FrequencyMinutes, now, jitter);
                }
            }

            /* Phase the first scheduled analysis over a SMALL fixed sub-2.5-minute window (#1553 jitter site 3):
               at a fleet restart every freshly connected server would otherwise become analysis-due in the same
               sweep, and with N=4 concurrency that clusters 4 analysis passes at once. A deterministic per-server
               offset de-clusters them while keeping post-restart analysis prompt (state-M3). The window is a
               fixed 150s, NOT the full analysis interval — full-interval phasing would break the "already-
               populated store analyzes promptly" promise by up to the interval, and is redundant since analysis
               already runs inside the N=4-bounded body. Left at MinValue when analysis is disabled so re-enabling
               still runs immediately (the sweep gates on config.Analysis.Enabled). */
            server.NextAnalysisDue = config.Analysis.Enabled
                ? DateTime.UtcNow.Add(CadencePhaseOffset(serverId, 150))
                : DateTime.MinValue;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            server.Runtime = null;
            server.NextConnectAttempt = DateTime.UtcNow.AddSeconds(60);
            /* #2255: full text on a NEW cause, one line while it persists. A credential that cannot be
               decrypted on this host is not a transient connect failure, so its explanation is worth Error
               once and worth almost nothing on the 1,440th repeat. */
            var failure = ex.Message;
            if (!string.Equals(server.LastConnectFailureLogged, failure, StringComparison.Ordinal))
            {
                server.LastConnectFailureLogged = failure;
                if (ex is InvalidOperationException && failure.Contains("DPAPI-decrypt", StringComparison.Ordinal))
                {
                    /* Error, not Warning: nothing about this clears on its own, so it needs an operator. */
                    _logger.LogError("[{Server}] Connect failed and will keep failing until fixed: {Message}",
                        server.Config.DisplayName, failure);
                }
                else
                {
                    _logger.LogWarning("[{Server}] Connect failed, retrying in 60s: {Message}",
                        server.Config.DisplayName, failure);
                }
            }
            else
            {
                _logger.LogWarning("[{Server}] Connect still failing, retrying in 60s (same cause as logged above)",
                    server.Config.DisplayName);
            }

            /* Stage 4: the online->offline connection edge (Server Unreachable) — fires once when a
               previously-connected server can no longer be reached; a repeated failed reconnect does NOT
               re-fire (the state machine dedups). server_id comes from the CONFIG rather than the runtime,
               because Runtime is null here by definition -- and post-#2218 the config carries the STORED id,
               so an alert on a server that has never once connected keys on the same identity its collected
               history does. */
            await _selfAlerts!.ApplyConnectionOutcomeAsync(
                server.Config.ServerId,
                server.Config.DisplayName, online: false, error: ex.Message, cancellationToken);
        }
    }

    private async Task RunDueCollectorsAsync(ServerLoopState server, DarlingCollectorRunner runner, CancellationToken cancellationToken)
    {
        var runtime = server.Runtime;
        if (runtime is null)
        {
            return;
        }

        /* Non-blocking: if an on-demand snapshot_now holds this server's gate, skip its scheduled sweep
           this pass (the due collectors run next sweep) — the main loop NEVER blocks on the gate, so a
           long snapshot cannot starve collection of the OTHER servers. */
        if (!await server.CollectionGate.WaitAsync(0, cancellationToken))
        {
            return;
        }

        try
        {
            var now = DateTime.UtcNow;

            /* #2864: the peer high-water mark describes ONE body. Reset here rather than decayed, because
               the comparison it feeds is 'were this sweep's other collectors slow', and a mark carried
               across bodies would answer a different question with the same number. */
            server.SweepPeerMaxMs = -1;
            foreach (var name in CollectorScheduleDefaults.All.Keys)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                /* Wrong-engine collectors are dropped BEFORE dispatch, not gated inside the runner: a
                   definition whose dialect the target does not speak must leave no trace at all. The
                   runner's own CollectorCatalog.AppliesTo check returns 0 rows, which RunOneAsync would
                   record as SUCCESS — fine for the handful of Azure-gated collectors, but with a second
                   engine in the catalog every target would log a fake success per foreign collector per
                   cycle (most are 1-minute), flooding collection_log and feeding phantom successes to the
                   health bands and analysis, which key on status. This is Darling's equivalent of Lite's
                   pre-dispatch SKIPPED path: no dispatch, no log row, no NextDue churn. */
                if (!CollectorCatalog.EngineMatches(name, runtime.Target))
                {
                    continue;
                }

                /* WITHIN-engine gates get the same treatment, on EVERY engine.
                   EngineMatches above drops the wrong DIALECT; it says nothing about a collector that is
                   right-dialect but inapplicable to this particular target — pg_wait_stats reads Aurora's own
                   wait instrumentation, so on stock PostgreSQL it dispatched, came back with 0 rows, and
                   RunOneAsync recorded SUCCESS. At a 1-minute cadence that is ~1,440 fake successes a day per
                   server, and the PR promised "a graceful skip with an explanation" instead. (pg_statement_stats
                   was the second such collector until #2625 gave it a vanilla pg_stat_statements path; it now
                   applies to every PostgreSQL target and reaches this gate on none of them.)

                   #2579 EXTENDS THIS TO SQL SERVER, which the PostgreSQL change deliberately left alone as
                   "its own decision" because it changes a shipping SKU's log semantics for the Azure-gated
                   collectors. Here is that decision, and what settled it is that the cost turned out to be
                   the opposite of cosmetic. On an AWS RDS fleet the SQL Server gates are not a handful: 84
                   instances x agent_status and running_jobs x a 5-minute cadence is ~24,000 rows a day that
                   say SUCCESS about collectors deliberately not running. A gated-off run recorded as SUCCESS
                   is byte-identical to a real one — same status, zero rows, no note — so nothing downstream
                   can tell them apart. That is not merely noise: it is the shape the whole miss vocabulary
                   exists to prevent, and it read as evidence of working collection convincingly enough to
                   produce an issue and a PR built on it before the 0ms durations gave it away.

                   No log row is the honest outcome, and it is not silent: --test-connection names exactly
                   which collectors do not apply to a target, and why, before the service ever runs. */
                if (!CollectorCatalog.AppliesTo(name, runtime.Target))
                {
                    continue;
                }

                /* Effective schedule = config_collector_schedules override layered on the code default.
                   A disabled collector is skipped; the frequency the NextDue stamp advances by is the
                   EFFECTIVE one, so an override takes effect immediately. An on-load collector (freq 0) is
                   NOT skipped here (#3929/#3930): it also reruns on
                   CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes, in addition to the immediate
                   on-connect run TryConnectAsync's on-load loop still does — a long-lived connection would
                   otherwise never refresh it again, aging its config snapshot out of retention (#3930) and
                   leaving a cleared trace flag with no later row to overwrite its stale ON one (#3929). */
                var effective = StoreConfigProvider.ResolveSchedule(name, runtime.ServerId, _scheduleOverrides);
                if (!effective.Enabled
                    || !server.NextDue.TryGetValue(name, out var due)
                    || now < due)
                {
                    continue;
                }

                var interval = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(effective.FrequencyMinutes);
                server.NextDue[name] = now.AddMinutes(interval);

                /* #2700: query_store is split off this sequential body rather than awaited inline. Its
                   run time is bimodal — a heavy batch runs 100-230+ seconds against a ~5-35s mean, on its
                   own 5-minute cadence — and every OTHER due collector in this foreach (query_stats,
                   procedure_stats, wait_stats, all 1-minute cadence) would otherwise queue behind that one
                   await for the rest of the body's duration. Worse, this body IS the unit the outer launch
                   loop will not relaunch while it is still running (INV-2, "one body per server"), so a
                   single heavy query_store run stalled the server's ENTIRE collection for its duration, not
                   just query_store's own row — confirmed via get_collection_health's BODY_OVERRUN
                   diagnostic as the mechanism that pushed several servers' last_collection stale enough to
                   false-trip the fleet's 15-minute Offline threshold while every collector was otherwise
                   healthy (zero failures, a pure scheduling overrun). Fire-and-forget is safe here
                   specifically because RunOneAsync already gates query_store through the per-server
                   QueryStoreServerGate (#2165) — a still-in-flight previous tick skips rather than
                   overlapping — and query_store's own window is watermark-driven (#1960), so a detached run
                   that outlives this sweep resumes correctly from its own gate rather than dropping rows.
                   RunOneAsync's catch-all already contains every fault but cancellation, so
                   RunDetachedAsync exists only to keep a shutdown-time OperationCanceledException from
                   surfacing as an unobserved task exception. */
                /* #2717: plan_correction gets the identical treatment for the identical reason. Its own
                   SQL is already correctly seek-based (#2687) and averages ~1 second, but on a server
                   whose Query Store carries the same workload-class distinct-plan-population signature
                   already root-caused for query_store on multi-03/OMEGA, it can spike to 20+ seconds — the
                   same bimodal shape, just a smaller worst case. Detached the same way, through the
                   generic DetachedCollectorGate (#2717) rather than query_store's own gate, which has an
                   orthogonal second job (excluding the backfill loop) this collector does not share.
                   plan_correction's recommendation-set read is DMV-driven with no persisted watermark, but
                   sys.dm_db_tuning_recommendations is re-read whole on every successful pass regardless —
                   a skipped tick simply re-reads the same (or since-refreshed) live set next time, the
                   same "defers, does not drop" property #1960 gives query_store's watermark. */
                /* #2864 review: snapshot the peer mark HERE, at dispatch, and hand it to the run. Reading it
                   at completion is correct only for the sequential arm; a detached run finishes 100-230s
                   later, by which time the 15s sweep has reset and rebuilt the mark from unrelated ticks. */
                var peerMaxAtDispatchMs = PeerMaxOrNull(server);
                /* #3604: pg_wait_sampling is the third, and the reason is different in kind — see
                   IsPgWaitSamplingCollector: a deliberate 30 s sampling window, not a bimodal tail. */
                if (IsQueryStoreCollector(name) || IsPlanCorrectionCollector(name) || IsPgWaitSamplingCollector(name))
                {
                    _ = RunDetachedAsync(server, runner, name, peerMaxAtDispatchMs, cancellationToken);
                }
                else
                {
                    await RunOneAsync(server, runner, name, peerMaxAtDispatchMs, cancellationToken);
                }
            }
        }
        finally
        {
            server.CollectionGate.Release();
        }
    }

    /// <summary>
    /// The <c>snapshot_now</c> command handler (Lite's Live Snapshot, control-plane form): runs EVERY
    /// enabled collector for one connected server immediately, bypassing the schedule, and reports the
    /// collectors run + total rows. Serialized against the scheduled sweep by the per-server
    /// <see cref="ServerLoopState.CollectionGate"/> so the two never double-collect. Waits its turn for the
    /// gate (unlike the main loop, which skips) because an explicit operator snapshot should not be dropped.
    /// </summary>
    private async Task<CommandOutcome> RunSnapshotAsync(
        List<ServerLoopState> servers, DarlingCollectorRunner runner, int serverId, CancellationToken cancellationToken)
    {
        ServerLoopState? server;
        lock (_serversLock)
        {
            server = servers.Find(s => s.Config.ServerId == serverId);
        }

        if (server is null)
        {
            return new CommandOutcome(false, "server not monitored", JsonError($"no monitored server with server_id {serverId}"));
        }

        if (server.Runtime is null)
        {
            return new CommandOutcome(false, "server not connected",
                JsonError($"server '{server.Config.DisplayName}' is not currently connected — snapshot skipped"));
        }

        await server.CollectionGate.WaitAsync(cancellationToken);
        try
        {
            /* Runtime can be dropped by a concurrent connection failure between the check above and here;
               re-read under the gate. */
            var runtime = server.Runtime;
            if (runtime is null)
            {
                return new CommandOutcome(false, "server not connected",
                    JsonError($"server '{server.Config.DisplayName}' disconnected before the snapshot ran"));
            }

            var collectorsRun = 0;
            var totalRows = 0;
            foreach (var name in CollectorScheduleDefaults.All.Keys)
            {
                cancellationToken.ThrowIfCancellationRequested();

                /* Honor a disabled-by-override collector (mirrors the on-load/scheduled gates); run every
                   enabled one NOW regardless of frequency or NextDue — that is what "snapshot" means. */
                var effective = StoreConfigProvider.ResolveSchedule(name, runtime.ServerId, _scheduleOverrides);
                if (!effective.Enabled)
                {
                    continue;
                }

                /* The THIRD dispatch loop, and it got neither engine gate in the first round: an operator
                   snapshot against a PostgreSQL target dispatched every SQL Server collector, whose
                   AppliesTo early-return yields zero rows and lands a burst of fake SUCCESS in
                   collection_log — the phantom-success class the other two loops (on-load, scheduled
                   sweep) were gated against. Same predicate, and since #2579 the same every-engine scoping:
                   an operator-triggered snapshot against an RDS target would otherwise land its own burst of
                   fake successes for the msdb-gated collectors. */
                if (!CollectorCatalog.EngineMatches(name, runtime.Target)
                    || !CollectorCatalog.AppliesTo(name, runtime.Target))
                {
                    continue;
                }

                /* null for the same reason as the on-load loop: an operator snapshot is not a body. */
                totalRows += await RunOneAsync(server, runner, name, peerMaxAtDispatchMs: null, cancellationToken);
                collectorsRun++;
            }

            _logger.LogInformation("[{Server}] snapshot_now ran {Collectors} collector(s), {Rows} row(s)",
                server.Config.DisplayName, collectorsRun, totalRows);
            var json = JsonSerializer.Serialize(new
            {
                success = true,
                server = server.Config.DisplayName,
                collectorsRun,
                rows = totalRows,
            });
            return new CommandOutcome(true, "snapshot complete", json);
        }
        finally
        {
            server.CollectionGate.Release();
        }
    }

    /// <summary>
    /// The <c>fetch_plan</c> command handler (headless-plan live-plan wave): reads an execution plan from one
    /// monitored server's LIVE plan cache and returns the plan XML — the mechanism the viewer uses to fetch a
    /// plan for ANY process in a deadlock graph / blocked-process report (by its sql_handle) or the
    /// currently-cached plan for a query-grid row (by its plan_handle), neither of which the store holds. The
    /// actual cache read is delegated to the SAME <see cref="PgPlanFetcher"/> the analysis pipeline already uses
    /// (it resolves the serverId to the connected runtime's connection string) — the plan_handle path is its
    /// existing <see cref="PgPlanFetcher.FetchPlanXmlAsync"/>, the sql_handle path its
    /// <see cref="PgPlanFetcher.FetchPlanBySqlHandleAsync"/>. Unlike snapshot_now it takes NO collection gate: a
    /// DMV plan read touches no collector state and writes nothing, so it can run concurrently with a scheduled
    /// sweep. The up-front lookup gives a precise "not monitored" / "not connected" outcome (mirroring
    /// RunSnapshotAsync); a plan that has aged out of the cache — or any fetch error the fetcher swallows to null
    /// per its analysis-safe contract — is reported as a clean "not in cache" (the command itself succeeded).
    /// </summary>
    private async Task<CommandOutcome> RunFetchPlanAsync(
        List<ServerLoopState> servers, PgPlanFetcher planFetcher, int serverId,
        PlanFetchRequest request, CancellationToken cancellationToken)
    {
        /* Lookup under the lock (the command loop reconciles the list concurrently); the fetcher re-resolves the
           serverId to the runtime connection string itself, so this only gates the precise not-monitored /
           not-connected outcomes. Held only for the microsecond lookup. */
        ServerLoopState? server;
        bool connected;
        string displayName;
        lock (_serversLock)
        {
            server = servers.Find(s => s.Config.ServerId == serverId);
            connected = server?.Runtime is not null;
            displayName = server?.Config.DisplayName ?? serverId.ToString(CultureInfo.InvariantCulture);
        }

        if (server is null)
        {
            return new CommandOutcome(false, "server not monitored", JsonError($"no monitored server with server_id {serverId}"));
        }

        if (!connected)
        {
            return new CommandOutcome(false, "server not connected",
                JsonError($"server '{displayName}' is not currently connected — the live plan cache can only be read from a connected server"));
        }

        /* #2443: both arms now take the SAME token. The by-sql_handle arm always had it; the
           by-plan_handle arm did not, so a cancelled fetch_plan command kept a session open on the
           monitored server for whichever key the caller happened to use. */
        var planXml = request.UsePlanHandle
            ? await planFetcher.FetchPlanXmlAsync(serverId, request.PlanHandle!, cancellationToken)
            : await planFetcher.FetchPlanBySqlHandleAsync(
                serverId, request.DatabaseName, request.SqlHandle!,
                request.StatementStartOffset, request.StatementEndOffset, cancellationToken);

        if (string.IsNullOrEmpty(planXml))
        {
            _logger.LogInformation("[{Server}] fetch_plan: the requested plan is not in the cache", displayName);
            /* Succeeded (the fetch ran) but the plan is gone — the viewer shows a "not in cache" info, distinct
               from a failure. planXml is null so the viewer's parse hits the not-in-cache branch. */
            return new CommandOutcome(true, "not in cache",
                JsonSerializer.Serialize(new { success = true, planXml = (string?)null }));
        }

        _logger.LogInformation("[{Server}] fetch_plan returned a {Length}-char plan", displayName, planXml.Length);
        return new CommandOutcome(true, "plan fetched",
            JsonSerializer.Serialize(new { success = true, planXml }));
    }

    /// <summary>The SQL command timeout (seconds) for the live active-queries DMV read. A "what is running now"
    /// snapshot should return quickly; the shared collector query sets <c>LOCK_TIMEOUT 1000</c> and runs under
    /// READ UNCOMMITTED, so 30s is a wide margin. Bounded well under the viewer's
    /// <c>ActiveQueriesLiveTimeout</c> poll budget so the viewer sees a real "timed out" outcome rather than a
    /// poll miss, and so a wedged read cannot pin the single-threaded command loop past the stale-command reaper.</summary>
    public const int ActiveQueriesFetchTimeoutSeconds = 30;

    /// <summary>
    /// <c>test_hypothetical_index</c> (#2612): plan one stored statement with and without a candidate index.
    ///
    /// <para>The statement text is resolved HERE, from this product's own <c>pg_statement_text</c> store,
    /// keyed by the queryid the caller named. It is never taken from the caller — the request carries an
    /// identifier and a candidate, and nothing else reaches SQL.</para>
    ///
    /// <para>PostgreSQL only, and it says so rather than failing obscurely on a SQL Server target: hypopg
    /// and <c>EXPLAIN (GENERIC_PLAN)</c> have no SQL Server equivalent, and the candidate this answers about
    /// comes from a PostgreSQL-only collector.</para>
    /// </summary>
    private async Task<CommandOutcome> RunTestHypotheticalIndexAsync(
        List<ServerLoopState> servers, int serverId, HypotheticalIndexRequest request, CancellationToken cancellationToken)
    {
        ServerLoopState? server;
        ServerRuntime? runtime;
        string displayName;
        lock (_serversLock)
        {
            server = servers.Find(s => s.Config.ServerId == serverId);
            runtime = server?.Runtime;
            displayName = server?.Config.DisplayName ?? serverId.ToString(CultureInfo.InvariantCulture);
        }

        if (server is null)
        {
            return new CommandOutcome(false, "server not monitored", JsonError($"no monitored server with server_id {serverId}"));
        }

        if (runtime is null)
        {
            return new CommandOutcome(false, "server not connected",
                JsonError($"server '{displayName}' is not currently connected — a hypothetical index has to be tested against the server's own statistics, so there is nothing to answer from while it is unreachable"));
        }

        if (runtime.Target.Engine != CollectorTargetEngine.PostgreSql)
        {
            return new CommandOutcome(false, "not a PostgreSQL target",
                JsonError($"server '{displayName}' is not PostgreSQL. Hypothetical indexes come from the hypopg extension and the plan comparison needs EXPLAIN (GENERIC_PLAN); neither has a SQL Server equivalent, and the index candidate this answers about comes from a PostgreSQL-only collector."));
        }

        if (!request.TryGetQueryId(out var queryId))
        {
            return new CommandOutcome(false, "invalid queryid", JsonError("queryid must be a signed 64-bit integer sent as a STRING"));
        }

        string? statementText;
        await using (var lookup = _postgres!.CreateCommand(
            "SELECT query_text FROM collect.pg_statement_text WHERE server_id = $1 AND queryid = $2"))
        {
            lookup.CommandTimeout = ServiceCommandDeadlines.CommandPlaneSeconds;
            lookup.Parameters.AddWithValue(serverId);
            lookup.Parameters.AddWithValue(queryId);
            statementText = (await lookup.ExecuteScalarAsync(cancellationToken)) as string;
        }

        if (string.IsNullOrWhiteSpace(statementText))
        {
            return new CommandOutcome(false, "statement text not captured",
                JsonError($"no statement text is stored for queryid {queryId} on '{displayName}'. Text is refreshed on its own cadence and a major-version upgrade re-keys every queryid, so a statement first seen minutes ago genuinely has none yet — there is nothing to re-plan until it does."));
        }

        try
        {
            await using var connection = new NpgsqlConnection(runtime.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            var result = await Targets.HypotheticalIndexExperiment.RunAsync(
                connection, statementText, request.BuildCreateIndexStatement(),
                runtime.Target.PostgresMajorVersion, cancellationToken);

            _logger.LogInformation(
                "[{Server}] test_hypothetical_index on {Schema}.{Table}: planner would {Verdict} it ({Before:N2} -> {After:N2})",
                displayName, request.SchemaName, request.TableName,
                result.PlannerWouldUseIt ? "USE" : "NOT use", result.CostBefore, result.CostAfter);

            return new CommandOutcome(true, "hypothetical index tested", JsonSerializer.Serialize(new
            {
                server = displayName,
                queryid = request.QueryId,
                candidate = request.BuildCreateIndexStatement(),
                planner_would_use_it = result.PlannerWouldUseIt,
                estimated_cost_before = result.CostBefore,
                estimated_cost_after = result.CostAfter,
                hypothetical_index_name = result.HypotheticalIndexName,
                explanation = result.Explanation,
                plan_before = result.PlanBeforeJson,
                plan_after = result.PlanAfterJson,
            }));
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (PostgresException ex)
        {
            /* Reported rather than thrown, and the SQLSTATE travels: 42P01 here means the table named in the
               candidate does not exist, which is a caller mistake, and 42501 means the login cannot plan
               against it — two different conversations that a bare failure would merge. */
            return new CommandOutcome(false, "experiment failed",
                JsonError($"planning on '{displayName}' failed: {ex.MessageText} (SQLSTATE {ex.SqlState})"));
        }
        catch (Exception ex)
        {
            return new CommandOutcome(false, "experiment failed",
                JsonError($"planning on '{displayName}' failed: {ex.Message}"));
        }
    }

    /// <summary>
    /// The <c>fetch_active_queries</c> command handler (headless-plan live-snapshot wave): reads the LIVE
    /// running-request DMV snapshot from one monitored server on demand and returns the rows — the on-demand,
    /// live counterpart of the collector's stored hourly snapshot, for the viewer's Current Active Queries tab.
    /// The query and shredding are REUSED from the shared <see cref="QuerySnapshotsCollector"/> (via
    /// <see cref="DarlingCollectorRunner.FetchRowsAsync"/>), so the live rows carry the SAME columns as the
    /// stored ones. Read-only — <c>sys.dm_exec_requests</c>/<c>sessions</c> + the sql_text / query_plan DMFs — so
    /// unlike <see cref="RunExecuteActualPlanAsync"/> it is not consent-class and takes NO collection gate (a DMV
    /// read touches no collector state and writes nothing, so it runs concurrently with a scheduled sweep). The
    /// up-front lookup gives a precise "not monitored" / "not connected" outcome (mirroring
    /// <see cref="RunFetchPlanAsync"/>); a timeout / permission gap / SQL error is caught and reported as a
    /// legible outcome rather than a raw exception (mirroring <see cref="RunExecuteActualPlanAsync"/>).
    /// </summary>
    private async Task<CommandOutcome> RunFetchActiveQueriesLiveAsync(
        List<ServerLoopState> servers, DarlingCollectorRunner runner, int serverId, CancellationToken cancellationToken)
    {
        /* Resolve the runtime under the lock (the command loop reconciles the list concurrently); ServerRuntime is
           immutable, so capturing the reference and using it after the lock is safe. Held only for the lookup. */
        ServerLoopState? server;
        ServerRuntime? runtime;
        string displayName;
        lock (_serversLock)
        {
            server = servers.Find(s => s.Config.ServerId == serverId);
            runtime = server?.Runtime;
            displayName = server?.Config.DisplayName ?? serverId.ToString(CultureInfo.InvariantCulture);
        }

        if (server is null)
        {
            return new CommandOutcome(false, "server not monitored", JsonError($"no monitored server with server_id {serverId}"));
        }

        if (runtime is null)
        {
            return new CommandOutcome(false, "server not connected",
                JsonError($"server '{displayName}' is not currently connected — the live active queries can only be read from a connected server"));
        }

        try
        {
            var rows = await runner.FetchRowsAsync(
                QuerySnapshotsCollector.Instance, runtime, ActiveQueriesFetchTimeoutSeconds, cancellationToken);
            _logger.LogInformation("[{Server}] fetch_active_queries returned {Count} running request(s)", displayName, rows.Count);
            return new CommandOutcome(true, "active queries fetched", ActiveQueriesLivePayload.Serialize(rows, DateTime.UtcNow));
        }
        catch (OperationCanceledException)
        {
            /* Service shutdown mid-read — let the poller's catch stop the loop cleanly. */
            throw;
        }
        catch (SqlException ex) when (ex.Number == -2)
        {
            return new CommandOutcome(false, "timed out",
                JsonError($"reading the live active queries on '{displayName}' did not finish within the {ActiveQueriesFetchTimeoutSeconds}s budget and was cancelled"));
        }
        catch (SqlException ex) when (IsPermissionError(ex))
        {
            return new CommandOutcome(false, "permission denied",
                JsonError($"the monitoring login for '{displayName}' lacks permission to read the active-query DMVs (VIEW SERVER STATE is required). (SQL error {ex.Number}: {ex.Message})"));
        }
        catch (SqlException ex)
        {
            return new CommandOutcome(false, "sql error",
                JsonError($"reading the live active queries on '{displayName}' failed (SQL error {ex.Number}): {ex.Message}"));
        }
        catch (Exception ex)
        {
            return new CommandOutcome(false, "error",
                JsonError($"reading the live active queries on '{displayName}' failed: {ex.Message}"));
        }
    }

    /// <summary>The store lookup that resolves the actual-plan command's <c>query_stats</c> IDENTIFIER (server_id +
    /// query_hash + database_name) to the latest captured query text + estimated plan XML — the SERVICE's own
    /// copy, so no SQL text ever rides on the command payload. Serves Top Queries, Query-Stats history, and the
    /// FinOps High Impact grid. The third column (isolation level) is NULL here (query_stats does not capture it).
    /// Public const so a test can pin its shape ($1 server_id, $2 query_hash, $3 database_name).</summary>
    public const string ResolveStoredQueryForActualPlanSql = @"
SELECT query_text, query_plan_xml, NULL::text AS transaction_isolation_level, query_plan_gz
FROM v_query_stats
WHERE server_id = $1
AND   query_hash = $2
AND   database_name = $3
AND   query_text IS NOT NULL
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>The <c>query_store_stats</c> resolver — the Query Store history surface's identifier (server_id +
    /// database_name + query_id) to its captured query text + stored plan. $1 server_id, $2 database_name, $3
    /// query_id. Isolation is NULL (Query Store does not capture it).
    /// <para>
    /// The plan-presence tiebreak leads the sort (#1556): query_store now dedupes plan XML to ONE runtime-stats
    /// interval per plan_id per cycle (NULL on the others), so a plain <c>collection_time DESC</c> could land on
    /// a newer NULL-plan interval of the same query. <c>(query_plan_text IS NOT NULL) DESC</c> first prefers the
    /// row that actually carries the plan; <c>collection_time DESC</c> then breaks ties toward the newest. The
    /// query_text is the same query either way, so this is strictly more robust — the sibling stored-plan
    /// readers' semantics.
    /// </para></summary>
    /* #2150: the text now comes from collect.query_store_text and only FALLS BACK to the fact row's own
       column, which is where it lived before the cutover — pre-cutover rows keep working unchanged, and
       post-cutover rows (NULL inline) resolve from the side table. The lookup is keyed on exactly the
       identifier this resolver already has, and all three keys are the statement's own parameters, so it
       is an uncorrelated scalar subquery: resolved once, then named once by the derived table so the
       IS NOT NULL filter can test the RESOLVED text rather than the raw column. Testing the raw column
       is the trap — it would exclude every post-cutover row, the whole set this change exists to serve. */
    public const string ResolveStoredQueryStoreForActualPlanSql = @"
SELECT r.query_text,
       r.query_plan_text,
       NULL::text AS transaction_isolation_level,
       NULL::bytea AS query_plan_gz
FROM
(
    SELECT
        COALESCE
        (
            (
                SELECT x.query_sql_text
                FROM query_store_text AS x
                WHERE x.server_id = $1
                AND   x.database_name = $2
                AND   x.query_id = $3
            ),
            s.query_text
        ) AS query_text,
        s.query_plan_text,
        s.collection_time
    FROM query_store_stats AS s
    WHERE s.server_id = $1
    AND   s.database_name = $2
    AND   s.query_id = $3
) AS r
WHERE r.query_text IS NOT NULL
ORDER BY (r.query_plan_text IS NOT NULL) DESC, r.collection_time DESC
LIMIT 1";

    /// <summary>The <c>query_snapshots</c> resolver — the Wait drill-down surface's identifier (server_id +
    /// collection_time + session_id) to that captured request's query text, plan (live preferred), and isolation
    /// level. $1 server_id, $2 collection_time, $3 session_id. The exact-timestamp match keys the one snapshot
    /// the row represents.</summary>
    public const string ResolveStoredSnapshotForActualPlanSql = @"
SELECT query_text, COALESCE(live_query_plan, query_plan), transaction_isolation_level, NULL::bytea AS query_plan_gz
FROM query_snapshots
WHERE server_id = $1
AND   collection_time = $2
AND   session_id = $3
AND   query_text IS NOT NULL
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>The command timeout (seconds) for the actual-plan re-execution. Bounded so a runaway query
    /// cannot pin the single-threaded command loop or outlive the stale-command reaper; comfortably under the
    /// viewer's 3-minute poll budget so the viewer sees a real "timed out" outcome rather than a poll miss.</summary>
    public const int ActualPlanCaptureTimeoutSeconds = 120;

    /// <summary>
    /// The <c>execute_actual_plan</c> command handler: RE-EXECUTES a stored query against one monitored server
    /// with SET STATISTICS XML ON to capture its ACTUAL plan, and returns the plan XML. Unlike every other
    /// worker-delegated command this WRITES to the target — re-running an INSERT/UPDATE/DELETE/MERGE re-applies
    /// its changes (the viewer gates this behind informed consent). The command payload is an IDENTIFIER ONLY
    /// (<see cref="ActualPlanRequest"/>): the query text + estimated plan XML are resolved HERE from the service's
    /// OWN store (<see cref="ResolveStoredQueryForActualPlanSql"/>), never from the payload, so a command writer
    /// can never smuggle arbitrary SQL onto a target. The query runs as the server's stored monitoring credential;
    /// the least-privilege VIEW SERVER STATE login has no SELECT on user tables, so a permission failure is caught
    /// and surfaced legibly (as are timeouts), rather than a raw SQL exception.
    /// </summary>
    private async Task<CommandOutcome> RunExecuteActualPlanAsync(
        List<ServerLoopState> servers, int serverId, ActualPlanRequest request, CancellationToken cancellationToken)
    {
        /* Resolve the connection under the lock (the command loop reconciles the list concurrently). Capture the
           immutable connection string + Azure flag as locals — never hold the runtime across the execute. */
        bool serverExists;
        string? connectionString;
        bool isAzureSqlDb;
        string displayName;
        lock (_serversLock)
        {
            var server = servers.Find(s => s.Config.ServerId == serverId);
            serverExists = server is not null;
            connectionString = server?.Runtime?.ConnectionString;
            isAzureSqlDb = server?.Runtime?.Target.IsAzureSqlDb ?? false;
            displayName = server?.Config.DisplayName ?? serverId.ToString(CultureInfo.InvariantCulture);
        }

        if (!serverExists)
        {
            return new CommandOutcome(false, "server not monitored", JsonError($"no monitored server with server_id {serverId}"));
        }

        if (string.IsNullOrEmpty(connectionString))
        {
            return new CommandOutcome(false, "server not connected",
                JsonError($"server '{displayName}' is not currently connected — the actual plan can only be captured from a connected server"));
        }

        if (_postgres is null)
        {
            return new CommandOutcome(false, "store unavailable", JsonError("the Postgres store is not available to resolve the query text"));
        }

        /* Resolve the query text + estimated plan + isolation level from the store BY THE IDENTIFIER (the
           collector's own capture) — the identifier-only contract: the payload named a stored row; the service
           supplies the text. The SQL + bound parameters are chosen by the identifier kind (query_stats /
           query_store_stats / query_snapshots). */
        string? queryText = null;
        string? estimatedPlanXml = null;
        string? isolationLevel = null;
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            await using var command = new NpgsqlCommand(ResolveActualPlanSql(request.Source), connection);
            command.CommandTimeout = ServiceCommandDeadlines.ActualPlanResolveSeconds;
            BindActualPlanResolveParameters(command, serverId, request);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                queryText = reader.IsDBNull(0) ? null : reader.GetString(0);
                /* #2069: query_stats plans written since V54 ride as gzip bytes (column 3) with the
                   text column NULL; the other two resolvers bind NULL::bytea there, so text-else-gz
                   is the one rule for all three source kinds. */
                estimatedPlanXml = PayloadDimensions.ResolveContent(
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.IsDBNull(3) ? null : reader.GetFieldValue<byte[]>(3));
                isolationLevel = reader.IsDBNull(2) ? null : reader.GetString(2);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CommandOutcome(false, "store error",
                JsonError($"could not resolve the stored query text for the actual-plan request: {ex.Message}"));
        }

        if (string.IsNullOrWhiteSpace(queryText))
        {
            return new CommandOutcome(false, "no stored query",
                JsonError($"no stored query text was found for this {DescribeActualPlanIdentifier(request)} on '{displayName}' — the actual plan is captured by re-executing the stored query text, which is not available"));
        }

        /* RE-EXECUTE via the SHARED executor (SET STATISTICS XML ON), as the server's stored credential. */
        try
        {
            var planXml = await ActualPlanExecutor.ExecuteForActualPlanAsync(
                connectionString,
                request.DatabaseName ?? "",
                queryText,
                estimatedPlanXml,
                isolationLevel: isolationLevel,
                isAzureSqlDb: isAzureSqlDb,
                timeoutSeconds: ActualPlanCaptureTimeoutSeconds,
                cancellationToken,
                productName: "SQL Server Performance Monitor Darling");

            if (string.IsNullOrEmpty(planXml))
            {
                _logger.LogInformation("[{Server}] execute_actual_plan: the query ran but no plan was captured", displayName);
                return new CommandOutcome(true, "no plan captured",
                    JsonSerializer.Serialize(new { success = true, planXml = (string?)null }));
            }

            _logger.LogInformation("[{Server}] execute_actual_plan captured a {Length}-char actual plan", displayName, planXml.Length);
            return new CommandOutcome(true, "actual plan captured",
                JsonSerializer.Serialize(new { success = true, planXml }));
        }
        catch (OperationCanceledException)
        {
            /* Service shutdown mid-execute — let the poller's catch stop the loop cleanly. */
            throw;
        }
        catch (SqlException ex) when (ex.Number == -2)
        {
            return new CommandOutcome(false, "timed out",
                JsonError($"the query did not finish within the {ActualPlanCaptureTimeoutSeconds}s actual-plan capture budget on '{displayName}' and was cancelled"));
        }
        catch (SqlException ex) when (IsPermissionError(ex))
        {
            return new CommandOutcome(false, "permission denied",
                JsonError($"the monitoring login for '{displayName}' lacks permission to execute this query. The least-privilege monitoring login has VIEW SERVER STATE for DMV reads but no SELECT/DML on the queried objects, so the actual plan cannot be captured. (SQL error {ex.Number}: {ex.Message})"));
        }
        catch (SqlException ex)
        {
            return new CommandOutcome(false, "sql error",
                JsonError($"executing the query on '{displayName}' to capture the actual plan failed (SQL error {ex.Number}): {ex.Message}"));
        }
        catch (Exception ex)
        {
            return new CommandOutcome(false, "error",
                JsonError($"executing the query on '{displayName}' to capture the actual plan failed: {ex.Message}"));
        }
    }

    /// <summary>Picks the store-resolution SQL for the actual-plan request's identifier kind.</summary>
    private static string ResolveActualPlanSql(ActualPlanSource source) => source switch
    {
        ActualPlanSource.QueryStats => ResolveStoredQueryForActualPlanSql,
        ActualPlanSource.QueryStore => ResolveStoredQueryStoreForActualPlanSql,
        ActualPlanSource.QuerySnapshot => ResolveStoredSnapshotForActualPlanSql,
        _ => throw new InvalidOperationException($"no store resolver for actual-plan source {source}"),
    };

    /// <summary>Binds the store-resolution parameters ($1 server_id, then the identifier's $2/$3) for the request's
    /// identifier kind. The snapshot's collection_time binds as a naive-UTC timestamp (Unspecified), matching how
    /// the collector stores it.</summary>
    private static void BindActualPlanResolveParameters(NpgsqlCommand command, int serverId, ActualPlanRequest request)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        switch (request.Source)
        {
            case ActualPlanSource.QueryStats:
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = request.QueryHash! });
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = request.DatabaseName ?? "" });
                break;
            case ActualPlanSource.QueryStore:
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = request.DatabaseName ?? "" });
                command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = request.QueryId!.Value });
                break;
            case ActualPlanSource.QuerySnapshot:
                command.Parameters.Add(new NpgsqlParameter<DateTime>
                {
                    TypedValue = DateTime.SpecifyKind(request.SnapshotCollectionTime!.Value, DateTimeKind.Unspecified),
                });
                command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = request.SnapshotSessionId!.Value });
                break;
            default:
                throw new InvalidOperationException($"no store resolver for actual-plan source {request.Source}");
        }
    }

    /// <summary>A human description of the request's identifier for the "no stored query" message.</summary>
    private static string DescribeActualPlanIdentifier(ActualPlanRequest request) => request.Source switch
    {
        ActualPlanSource.QueryStats => $"query (query_hash {request.QueryHash})",
        ActualPlanSource.QueryStore => $"Query Store query (query_id {request.QueryId})",
        ActualPlanSource.QuerySnapshot => $"query snapshot (session {request.SnapshotSessionId})",
        _ => "row",
    };

    /// <summary>
    /// Where the missing extension has to be created, named when we know it (#2638).
    /// </summary>
    private static string WhereToCreateIt(string? connectedDatabase)
        => string.IsNullOrWhiteSpace(connectedDatabase)
            ? "in the connected database (CREATE EXTENSION ...). "
            : $"in database '{connectedDatabase}', which is the one this collector connects to — an "
              + "extension installed in a DIFFERENT database on the same cluster is invisible from here, "
              + $"so run CREATE EXTENSION in '{connectedDatabase}'. ";

    /// <summary>
    /// The self-hosted log readers (#3239; three since #3601). Their dispatch entries send Aurora and RDS to
    /// the log-API ingestors, so a target-side PostgresException under any of the names comes from the
    /// pg_read_file route.
    /// Consulted by two arms: the 42501 grant-pair sentence (#3239) and the 58P01 missing-file sentence
    /// (#3410), which is why it names the ROUTE rather than either fault.
    /// </summary>
    private static bool ReadsServerLogWithPgReadFile(string collectorName)
        => collectorName is "pg_deadlocks" or "pg_plan_capture" or "pg_log_events";

    /// <summary>
    /// Where the pg_read_file grants have to be issued, named when we know it (#3239) — function ACLs are
    /// per-database catalogs, the same fact <see cref="WhereToCreateIt"/> names for extensions. Measured
    /// on the 20260910 dogfood soak: the pair issued in the wrong database leaves a failure identical to
    /// no grant at all.
    /// </summary>
    private static string WhereToGrantIt(string? connectedDatabase)
        => string.IsNullOrWhiteSpace(connectedDatabase)
            ? "in the database this collector connects to."
            : $"in database '{connectedDatabase}', the one this collector connects to — issued in a "
              + "DIFFERENT database on the same cluster, they change nothing here.";

    /// <summary>
    /// The extensions <paramref name="collectorName"/> declares it cannot run without
    /// (<see cref="ICollectorSchemaInfo.RequiredPgExtensions"/>, #3191) — the seam #3240's classification
    /// consults. Empty for a collector the catalog does not know, which keeps an unknown name on the
    /// generic missing-object arm rather than inventing a dependency for it.
    /// </summary>
    private static IReadOnlyList<PgExtensionDependency> RequiredExtensionsOf(string collectorName) =>
        CollectorCatalog.Find(collectorName)?.RequiredPgExtensions ?? Array.Empty<PgExtensionDependency>();

    /// <summary>
    /// The sentence an <c>EXTENSION_MISSING</c> row carries (#3240): the raw error, the DECLARED extension
    /// by name, the exact <c>CREATE EXTENSION</c> to run and where (#2638's per-database caution kept — an
    /// extension installed in a different database on the same cluster is invisible from here), and the
    /// <c>shared_preload_libraries</c> restart when the declaration says installing costs one. Ends on the
    /// same retry promise the generic arm makes, because it is the same machinery: the collector retries
    /// every cycle and starts collecting on the first one after the extension exists.
    /// </summary>
    private static string ExtensionMissingExplanation(
        PostgresException ex, IReadOnlyList<PgExtensionDependency> required, string? connectedDatabase)
    {
        var names = string.Join(", ", required.Select(r => r.ExtensionName));
        var create = string.Join("; ", required.Select(r => $"CREATE EXTENSION {r.ExtensionName}"));
        var noun = required.Count == 1 ? "extension" : "extensions";

        var where = string.IsNullOrWhiteSpace(connectedDatabase)
            ? $"run {create} in the connected database (extensions are per-database). "
            : $"run {create} in database '{connectedDatabase}', which is the one this collector connects "
              + "to — an extension installed in a DIFFERENT database on the same cluster is invisible "
              + "from here. ";

        var preload = required.Any(r => r.InstallKind == PgExtensionInstallKind.SharedPreloadLibraries)
            ? $"The module also has to be in shared_preload_libraries first, which takes a server restart "
              + "(a parameter-group change plus a reboot on Aurora/RDS) and leaves it inert until then "
              + "whatever else is installed. "
            : string.Empty;

        return $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — the {names} {noun} this collector reads is "
            + "not installed on this target. This is NOT a missing grant, and no GRANT will change it: "
            + where
            + preload
            + "Recorded as a named non-fatal skip rather than an error so it does not fill the log every "
            + "cycle; the collector retries every cycle and starts collecting on the first one after the "
            + $"{noun} exists.";
    }

    /// <summary>
    /// The object a 42P01 / 42883 names, as its bare last segment (#3818): <c>relation
    /// "public.pg_stat_statements_info" does not exist</c> yields <c>pg_stat_statements_info</c>;
    /// <c>function aurora_stat_statements(boolean) does not exist</c> yields <c>aurora_stat_statements</c>.
    /// Null when the message has neither shape - a non-English <c>lc_messages</c>, or a message this did not
    /// anticipate - which keeps the caller on the arm it would have taken before this existed rather than
    /// guessing. The relation and function fields Npgsql exposes are NOT consulted: PostgreSQL fills them for
    /// constraint and datatype errors, not for a name it could not resolve.
    /// </summary>
    internal static string? MissingObjectNamedBy(PostgresException ex)
    {
        var text = ex.MessageText ?? string.Empty;

        var relation = System.Text.RegularExpressions.Regex.Match(
            text, "^relation \"(?<name>[^\"]+)\" does not exist", System.Text.RegularExpressions.RegexOptions.CultureInvariant);
        var function = System.Text.RegularExpressions.Regex.Match(
            text, "^function (?<name>[^\\s(]+)\\(", System.Text.RegularExpressions.RegexOptions.CultureInvariant);

        var qualified = relation.Success ? relation.Groups["name"].Value
            : function.Success ? function.Groups["name"].Value
            : null;

        if (qualified is null)
        {
            return null;
        }

        var lastDot = qualified.LastIndexOf('.');
        return lastDot >= 0 ? qualified[(lastDot + 1)..] : qualified;
    }

    /// <summary>
    /// The declared companion (<see cref="PgExtensionDependency.Companions"/>) a missing-object fault names,
    /// with the extension it belongs to - or null when the fault names the extension's base object, an
    /// undeclared object, or nothing this can read (#3818). Ordinal on the bare name, both sides lowercase
    /// by declaration and by PostgreSQL's own folding.
    /// </summary>
    internal static (PgExtensionDependency Extension, PgExtensionCompanionObject Companion)? MissingCompanionOf(
        PostgresException ex, IReadOnlyList<PgExtensionDependency> required)
    {
        var missing = MissingObjectNamedBy(ex);
        if (missing is null)
        {
            return null;
        }

        foreach (var extension in required)
        {
            foreach (var companion in extension.Companions)
            {
                if (string.Equals(companion.ObjectName, missing, StringComparison.Ordinal))
                {
                    return (extension, companion);
                }
            }
        }

        return null;
    }

    /// <summary>
    /// The sentence a missing COMPANION object carries (#3818): the raw error, the object the server actually
    /// named, what the catalog says about the extension, and the remedy that follows FROM THAT STATE. Not
    /// <see cref="ExtensionMissingExplanation"/>, whose sentence says the extension is not installed and gives
    /// <c>CREATE EXTENSION</c> plus a preload restart: on the upgraded-fleet case that motivated #3818 the
    /// extension was installed, preloaded and readable, and all three of those were false.
    ///
    /// <para><b>Four states, four sentences (#3830), because one remedy for all of them was wrong on the
    /// fleet it shipped for.</b> #3818's sentence had a single remedy, <c>ALTER EXTENSION ... UPDATE</c>,
    /// inferred from the ordering property alone - the base object resolved, therefore the extension is
    /// present, therefore it is present at an old version. The middle step does not survive the Aurora
    /// flavor: there the base object that resolved is <c>aurora_stat_statements()</c>, which is not the
    /// extension's at all and needs none, so "it resolved" licenses nothing about <c>pg_extension</c>. The 23
    /// clusters that produced #3818 were exactly that case - no row, never created, the collector productive
    /// throughout - and they were told to update an extension they did not have.</para>
    ///
    /// <para><b>So the state ARRIVES rather than being inferred.</b> <paramref name="extensionRow"/> is the
    /// <c>pg_extension</c> row the connect-time read observed, scoped to the database this fault came from,
    /// and <see cref="PgExtensionCompanionObject.VerdictFrom"/> draws the verdict from it here. When nothing
    /// was observed the sentence falls back to the two-possibilities form, which is the most #3818's
    /// inference could honestly claim - a hedge, kept only where there is no answer to prefer over it.</para>
    /// </summary>
    /// <param name="extensionRow">The <c>pg_extension</c> row for the declared extension, as observed at
    /// connect. <c>default</c> (not observed) is the safe value and produces the fallback sentence.</param>
    /// <param name="isAurora">Whether the target is Aurora, which decides only what an ABSENT extension
    /// costs - together with the collector's declared <c>AuroraNativeAlternative</c>.</param>
    private static string CompanionMissingExplanation(
        PostgresException ex,
        PgExtensionDependency extension,
        PgExtensionCompanionObject companion,
        string? connectedDatabase,
        PgExtensionRowObservation extensionRow,
        bool isAurora)
    {
        var where = string.IsNullOrWhiteSpace(connectedDatabase)
            ? "in the connected database"
            : $"in database '{connectedDatabase}'";

        var head = $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — the missing object is {companion.ObjectName}, "
            + $"which is NOT the {extension.ExtensionName} extension's base object";

        const string tail = "This is "
            + "NOT a missing grant, and no GRANT will change it. Recorded as a non-fatal skip rather than an "
            + "error so it does not fill the log every cycle; the collector retries every cycle.";

        /* The row is only usable if it was read in the database this fault came from: pg_extension is per
           database, and a RunsPerDatabase collector faults in databases the connect-time read never saw. A
           mismatch degrades to "not observed", which is the fallback sentence, never a claim about the
           wrong catalog. */
        var verdict = companion.VerdictFrom(extensionRow.InDatabase(connectedDatabase));

        switch (verdict)
        {
            /* NO ROW. The extension was never created in that database, so there is no version to update:
               ALTER EXTENSION would raise, and CREATE EXTENSION is the only statement that changes
               anything. Whether it is WORTH running is the flavor's question, and the collector answers it
               by declaring what it reads on Aurora instead. */
            case PgExtensionCompanionVerdict.NoRow when isAurora && extension.AuroraNativeAlternative is { } auroraSource:
                return head + $". pg_extension has NO row for {extension.ExtensionName} {where}: the extension "
                    + "was never created there, so it is not present at any version and there is nothing for "
                    + $"ALTER EXTENSION {extension.ExtensionName} UPDATE to update. This target is Aurora, where "
                    + $"this collector's source is {auroraSource} — which needs no extension — so the collector "
                    + $"is not waiting on one: running CREATE EXTENSION {extension.ExtensionName} {where} is "
                    + $"OPTIONAL, and what it buys is {companion.ObjectName} and nothing else. " + tail;

            case PgExtensionCompanionVerdict.NoRow:
                return head + $". pg_extension has NO row for {extension.ExtensionName} {where}: the extension "
                    + "was never created there, so it is not present at any version and there is nothing for "
                    + $"ALTER EXTENSION {extension.ExtensionName} UPDATE to update. The remedy is CREATE EXTENSION "
                    + $"{extension.ExtensionName} {where}"
                    + (extension.InstallKind == PgExtensionInstallKind.SharedPreloadLibraries
                        ? $", and {extension.ExtensionName} has to be in shared_preload_libraries before that "
                          + "does anything — a parameter-group change and a restart. "
                        : ". ")
                    + tail;

            /* BELOW the companion's version: #3818's case, and now stated as a fact read out of the catalog
               rather than as one of two possibilities. */
            case PgExtensionCompanionVerdict.BelowCompanionVersion:
                return head + $": that resolved, so the extension IS installed {where} and this is not the "
                    + "extension missing. pg_extension says it is at catalog version "
                    + $"{extensionRow.ExtensionVersion}, below the {companion.SinceExtensionVersion} whose update "
                    + $"script creates {companion.ObjectName} — an engine upgraded in place or restored keeps the "
                    + "version the extension was created at until someone runs ALTER EXTENSION "
                    + $"{extension.ExtensionName} UPDATE {where} — a statement, with no restart and no "
                    + "shared_preload_libraries change; RDS and Aurora do not run it for you. " + tail;

            /* AT OR ABOVE: the update everything else would have recommended is already done, so recommending
               it again is the wrong-remedy defect in its third form. */
            case PgExtensionCompanionVerdict.AtOrAboveCompanionVersion:
                return head + $": that resolved, so the extension IS installed {where} and this is not the "
                    + "extension missing. pg_extension says it is at catalog version "
                    + $"{extensionRow.ExtensionVersion}, which already carries {companion.ObjectName} "
                    + $"({companion.SinceExtensionVersion} and later), so no ALTER EXTENSION "
                    + $"{extension.ExtensionName} UPDATE is owed and it would change nothing. The object is "
                    + "installed outside the schema the query text names (a relocatable extension lives in "
                    + "whatever schema it was created in; check pg_extension.extnamespace), or the error text "
                    + "above names a reason of its own. " + tail;

            /* NOTHING OBSERVED — or a version string nothing can rank. The two-possibilities sentence, which
               is everything the fault alone supports. */
            default:
                return head + ": that resolved, so the "
                    + $"extension IS installed {where} and this is not the extension missing. {companion.ObjectName} "
                    + $"is created by the extension's {companion.SinceExtensionVersion} update script, so either the "
                    + $"extension is present at a catalog version below {companion.SinceExtensionVersion} (an engine "
                    + "upgraded in place or restored keeps the version the extension was created at until someone "
                    + $"runs ALTER EXTENSION {extension.ExtensionName} UPDATE {where} — a statement, with no restart "
                    + "and no shared_preload_libraries change; RDS and Aurora do not run it for you) or the object "
                    + "is installed outside the schema the query text names (a relocatable extension lives in whatever "
                    + "schema it was created in; check pg_extension.extversion and extnamespace). " + tail;
        }
    }
    /// <summary>
    /// #4046 part 1c: the general handler's one call for a fault on a log-tail read (#4051 round-2 review). It
    /// returns <see cref="LogTailUndecodableByteExplanation"/>'s sentence, or null, and it drops a stale
    /// pg_read_binary_file verdict through <see cref="DarlingCollectorRunner.ForgetStaleReadBinaryFileVerdict"/>.
    /// The sentence is built first, because it reads the verdict that the second step can drop. Nothing here
    /// allocates on the way to null for a fault that is not a <see cref="PostgresException"/>, because the general
    /// handler is also the OutOfMemoryException landing pad.
    ///
    /// <para>#4251 round-1 review, M1: also drops a stale pg_file_settings verdict through
    /// <see cref="DarlingCollectorRunner.ForgetStaleFileSettingsVerdict"/> — a no-op here in practice, since a
    /// pg_server_config 42501 classifies PERMISSIONS and is caught before reaching this general handler (see
    /// the other call at the PERMISSIONS arm below), but called anyway so this stays the one place every
    /// stale-verdict check for a fault this handler sees is reached from, the same way the read-binary-file
    /// one already is.</para>
    /// </summary>
    internal static string? LogTailGeneralFault(Exception ex, string collectorName, ServerRuntime runtime)
    {
        var explanation = LogTailUndecodableByteExplanation(ex, collectorName, runtime);
        DarlingCollectorRunner.ForgetStaleReadBinaryFileVerdict(collectorName, ex, runtime);
        DarlingCollectorRunner.ForgetStaleFileSettingsVerdict(collectorName, ex, runtime);
        return explanation;
    }

    /// <summary>
    /// #4046 part 1c: the sentence for a log-tail read that PostgreSQL refused because of one byte, or null for
    /// any other fault. <c>pg_read_file()</c> returns text, which PostgreSQL checks before this process sees a
    /// byte. A failed login can plant a bad byte in the FATAL message's unescaped %u/%d echo, under any
    /// log_line_prefix, and that byte fails the WHOLE tail read for as long as it sits inside the window. Every
    /// reader sharing PgServerLogTail's tail CTE goes blind, not just the one that read this cycle.
    ///
    /// <para><b>Two sentences.</b> On a UTF8 or SQL_ASCII database the fault is a 22021, and the sentence names
    /// the pg_read_binary_file grant that moves the reader to the byte route. On any other encoding the byte
    /// route does not apply (<see cref="PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding"/>), and the
    /// fault is a 22021 (EUC encodings) or a 22P05 (a WIN1252 byte with no UTF-8 equivalent). There the sentence
    /// says that the grant does not help and names #4062 (#4051 round-2 review, L-1).</para>
    ///
    /// <para><b>ERROR, not PERMISSIONS (#4051 review M1).</b> The general handler records this under ERROR with
    /// the sentence as its message, rather than PostgresFaultOutcome giving it a PERMISSIONS arm. Anyone who can
    /// attempt a login can plant the byte, so the blinding has to count in error_count and the daily error
    /// figures. A PERMISSIONS row drops out of both, and a reader an attacker blinds most of the week would
    /// still band HEALTHY whenever one cycle got through.</para>
    ///
    /// <para>Null for a proven write to the STORE (#3111's rule, #4051 review L3): a 22021 the store's COPY
    /// raises is not about the target's log. The checks run cheapest first and allocate nothing on the way to
    /// null, because the general handler is also the OutOfMemoryException landing pad.</para>
    /// </summary>
    internal static string? LogTailUndecodableByteExplanation(Exception ex, string collectorName, ServerRuntime runtime)
    {
        if (ex is not PostgresException { SqlState: "22021" or "22P05" } pg
            || !ReadsServerLogWithPgReadFile(collectorName)
            || CollectorFaultCopyPhase.IsProvenStoreWrite(ex))
        {
            return null;
        }

        const string Planted = " A client can plant such a byte with nothing more than a failed login. The role or "
            + "database name that the client sends lands unescaped in the FATAL message (#4046).";

        if (pg.SqlState == "22P05" || PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding(runtime.StorageName))
        {
            return $"{pg.MessageText} (SQLSTATE {pg.SqlState}). The log tail that this cycle read contains a byte "
                + "that this database's encoding cannot pass to this collector, so PostgreSQL refused the whole read."
                + Planted
                + " Granting pg_read_binary_file does not help on this database. The binary route decodes the log in "
                + "the database's own server_encoding, but this collector does not map this database's encoding "
                + "(EUC_TW, EUC_JIS_2004, LATIN6, LATIN8, LATIN10, MULE_INTERNAL, or one this runtime cannot resolve). "
                + "The read fails until the line with the byte leaves the 4 MB tail window.";
        }

        return $"{pg.MessageText} (SQLSTATE {pg.SqlState}). The log tail that this cycle read contains a byte that "
            + "is not valid UTF-8, so PostgreSQL refused the whole read. pg_read_file() returns text, and PostgreSQL "
            + "checks text before this collector sees a row, even when only one byte in the 4 MB window is bad."
            + Planted
            + " Grant EXECUTE ON FUNCTION pg_read_binary_file(text, bigint, bigint) to the monitoring role "
            + WhereToGrantIt(CollectorFaultDatabase.For(ex, runtime.ConnectedDatabase))
            + " From the next cycle, this collector reads the same bytes as bytea, which PostgreSQL does not check. "
            + "An invalid byte then shows as U+FFFD instead of stopping the read.";
    }

    /// <summary>
    /// Maps a PostgreSQL fault to a collection_log status plus the sentence an operator needs.
    /// <para>PERMISSIONS is the non-fatal-degradation bucket for the cases whose absent thing the code
    /// cannot name — a denied grant, an undeclared missing object, a disabled feature — and the MESSAGE
    /// distinguishes them, the same division the Azure service-objective hint already uses. A missing
    /// object on a collector that DECLARES the extension it reads is not one of those: since #3240 it gets
    /// its own <c>EXTENSION_MISSING</c> status, so the health surfaces can band it apart from a grant
    /// problem instead of hinting at <c>pg_monitor</c>. A missing FILE on one of the two log readers is
    /// not one of those either — there the absent thing IS nameable, because the path came from the
    /// server's own answers, so its arm names it (#3410). Returning "ERROR" means "let the general handler
    /// have it", which keeps the genuinely unexpected loud.</para>
    /// </summary>
    /// <param name="extensionRow">#3830: the <c>pg_extension</c> row a PostgreSQL connect observed for the
    /// declared extension, which the companion arm needs and the exception cannot carry. <c>default</c> means
    /// nothing was observed, and every arm treats that as "no answer" rather than as "no row".</param>
    /// <param name="isAurora">#3830: whether the target is Aurora, which decides what an ABSENT extension
    /// costs a collector that declares an Aurora-native alternative.</param>
    internal static (string Status, string Explanation) PostgresFaultOutcome(
        PostgresException ex,
        string collectorName,
        string? connectedDatabase = null,
        PgExtensionRowObservation extensionRow = default,
        bool isAurora = false)
    {
        var fault = PostgresTargetProvider.Instance.Classify(
            ex, CollectorCatalog.YieldsOnLockTimeout(collectorName));

        return fault switch
        {
            /* #3239: the two self-hosted log readers are the exception the general sentence below used to
               deny to their faces. Reading the log with pg_read_file needs the pg_read_server_files role
               AND an explicit EXECUTE grant — measured on #2566, the role alone does NOT carry it, because
               the function's ACL is postgres=X/postgres — and the EXECUTE half lives in each database's
               own catalog, so the grants only count in the database this collector connects to. The old
               hint said pg_monitor covers everything, which sent an operator in a circle: the product's
               own changelog knew better. */
            CollectorTargetFault.Permissions when ReadsServerLogWithPgReadFile(collectorName) => ("PERMISSIONS",
                $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — this collector reads the server log with "
                + "pg_read_file(), which pg_monitor does NOT cover. The monitoring login needs BOTH the "
                + "pg_read_server_files role AND an explicit GRANT EXECUTE ON FUNCTION pg_read_file(text), "
                + "pg_read_file(text, bigint, bigint), pg_read_file(text, bigint, bigint, boolean) — the "
                + "role alone does not carry EXECUTE, because the function's ACL is postgres=X/postgres. "
                + "EXECUTE grants live in each database's own catalog, so issue them "
                + WhereToGrantIt(connectedDatabase)
                + " While issuing that grant, also GRANT EXECUTE ON FUNCTION pg_read_binary_file(text, "
                + "bigint, bigint) to the same role (#4046): a byte that is not valid UTF-8 makes "
                + "pg_read_file() fail the whole tail read, pg_read_binary_file() does not, and the "
                + "collector switches to it on its own once it is granted."),

            CollectorTargetFault.Permissions => ("PERMISSIONS",
                $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — the monitoring login lacks a grant this "
                + "source needs. pg_monitor covers every collector here; check that it is granted."),

            /* #3410, the half #3414 left open: 58P01 and 42501 were told apart in the classifier —
               Permissions versus Unclassified — and nowhere the operator looks. Both rendered as a failed
               collector, so the natural first move was the grant chase, which is right for 42501 and a
               dead end here: no GRANT changes a missing file. Scoped to the two log readers because for
               them a missing file is a nameable state of the source — the path was built from the server's
               own answers moments earlier — while a 58P01 anywhere else is genuinely unexpected and must
               stay loud, which the default arm below still is. Keyed on Unclassified deliberately: the
               classifier LEAVES 58P01 there so every other collector keeps the loud default, and this arm
               is the exception being made, not a reclassification. */
            CollectorTargetFault.Unclassified when ReadsServerLogWithPgReadFile(collectorName) && ex.SqlState == "58P01" =>
                ("PERMISSIONS",
                    $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — the server named the exact file it could "
                    + "not OPEN, and that path is the fact to act on: 58P01 is a missing FILE, not a missing "
                    + "grant, and no GRANT changes it — a denial is 42501 and gets different advice. The "
                    + "path is built from the server's own answers, current_setting('log_directory') joined "
                    + "to a name pg_ls_logdir() returned in the same statement, and it resolves on the "
                    + "MONITORED server's filesystem, not the monitoring host's. A one-off is a file removed "
                    + "between the listing and the read and heals on the next cycle; one that repeats every "
                    + "cycle means the directory log_directory names no longer holds what the listing said, "
                    + "which is worth a look at that server's setting and what actually sits behind it. A "
                    + "server whose logging_collector is off does not land here — that is detected first "
                    + "and recorded as its own named state."),

            /* #4046 part 1c: a 22021 on a log-tail reader has no arm here on purpose. It keeps ERROR, and the
               general handler records it with LogTailUndecodableByteExplanation's sentence instead. */

            /* #3240: a missing source object on a collector that DECLARES its extension dependency
               (ICollectorSchemaInfo.RequiredPgExtensions, #3191) is not ambiguous — the absent thing is
               that extension, installing it is the remedy, and no grant changes anything. Recorded under
               its own status so Collection Health bands it EXTENSION_MISSING rather than NO_PERMISSIONS
               with a pg_monitor hint, and the sentence names the extension instead of describing the
               error's shape. Undeclared sources keep the arm below: a version-gated relation, or an
               extension-owned object nothing declares, still presents as 42P01/42883, and for those the
               generic sentence is the honest one. */
            /* #3818, the exception to #3240's inference, checked FIRST because it is the narrower claim: when the
               object the server could not find is a COMPANION the collector declared beside its base object,
               the extension is present - the base object resolved before the companion was looked up - and
               "not installed" would be false. The remedy is ALTER EXTENSION ... UPDATE, not CREATE EXTENSION,
               and the status is the general non-fatal-degradation bucket rather than EXTENSION_MISSING,
               because that status word says the extension is missing and every health surface reads it as an
               optional module left uninstalled - a legitimate resting state. A previously productive collector
               dark on a version-gated companion is not resting. The text is where the truth goes, as it is on
               the generic arm below. */
            CollectorTargetFault.ObjectMissing when MissingCompanionOf(ex, RequiredExtensionsOf(collectorName)) is { } found =>
                (CollectorRuntimePrecondition.DegradedStatus,
                    CompanionMissingExplanation(
                        ex, found.Extension, found.Companion, connectedDatabase, extensionRow, isAurora)),

            CollectorTargetFault.ObjectMissing when RequiredExtensionsOf(collectorName) is { Count: > 0 } required =>
                (CollectorRuntimePrecondition.ExtensionMissingStatus,
                    ExtensionMissingExplanation(ex, required, connectedDatabase)),

            /* 42P01 / 42883: the relation or function is not there. Overwhelmingly an extension that was
               never created in the connected database rather than anything to do with privileges. */
            /* #2638: the database is NAMED. Extensions are per-database, and on a real fleet one was
               installed in a different database on the same cluster from the one this collector connects
               to — so an operator who checked the obvious database found it already there and concluded
               the collector was broken. "Create it somewhere" is not an instruction; naming the database
               makes it one. Threaded from ServerRuntime.ConnectedDatabase; when that is unknown the
               sentence degrades to what it always said rather than inventing a name. */
            CollectorTargetFault.ObjectMissing => ("PERMISSIONS",
                $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — the source object does not exist on this "
                + "target. This is NOT a missing grant: it is normally an extension that was never created "
                + WhereToCreateIt(connectedDatabase)
                + "The collector will keep degrading until it is. Recorded as a non-fatal skip rather than "
                + "an error so it does not fill the log every cycle."),

            /* 0A000 / 55000 / 55006: the server will not do this, permanently or by configuration —
               pg_stat_wal on Aurora, or an optimized-reads cache that is switched off. */
            CollectorTargetFault.FeatureDisabled => ("PERMISSIONS",
                $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — this source is unsupported or disabled on "
                + "this server. NOT a missing grant. Aurora does not implement some community sources at "
                + "all, and others are gated by a parameter group. Recorded as a non-fatal skip because it "
                + "will not change until the platform or the parameter group does."),

            CollectorTargetFault.LockTimeoutYield => ("YIELDED",
                $"Lock-timeout yield (SQLSTATE {ex.SqlState}): the collector's lock-timeout guard fired "
                + "rather than waiting in a blocking chain. One sweep skipped; evidence of lock contention "
                + "on the monitored server, not a monitoring failure."),

            /* Everything else belongs to the general handler, which logs ERROR and (for
               ConnectionFatal) forces the reprobe. A command timeout no longer arrives here as raw
               transport text — see PostgresTimeoutExplanation and the arm that calls it — but it
               still lands on ERROR, because a deadline that collected nothing IS a collection
               failure and must stay inside the error counts that feed collector health. */
            _ => ("ERROR", ex.Message),
        };
    }

    /// <summary>
    /// The sentence a PostgreSQL command timeout gets instead of the transport's seven words (#2997).
    ///
    /// <para><b>Why this is not folded into <see cref="PostgresFaultOutcome"/>.</b> Two reasons, and
    /// neither is that a client-side deadline cannot produce a <see cref="PostgresException"/> — it can,
    /// when the server's <c>57014</c> response beats the tearing stream. The first is the PARAMETER TYPE:
    /// that method takes a <see cref="PostgresException"/>, and the shape a client-side deadline
    /// normally arrives in is an <c>NpgsqlException</c> wrapping a <c>TimeoutException</c> with no
    /// SQLSTATE at all, so it cannot be reached through a SQLSTATE map however wide that map grew. The
    /// second is that the answer is not a function of the code: the split below turns on the message
    /// text, which a switch over SQLSTATE cannot express whatever it is handed.</para>
    ///
    /// <para><b>The status stays ERROR.</b> The store has five and none of them means "ran out of time";
    /// PERMISSIONS is the non-fatal-degradation bucket and would wrongly exclude this from the error
    /// counts, the health band and the collection-failure self-alerts, which is precisely where a
    /// collector that has never once succeeded belongs. What changes is that the row is now readable:
    /// the message names the collector, the database, the mechanism and the measured elapsed time, and
    /// the caller records that elapsed figure instead of a literal zero.</para>
    ///
    /// <para><paramref name="origin"/> splits the deadlines that all classify as
    /// <see cref="CollectorTargetFault.CommandTimeout"/>, because the remedy is on a different machine for
    /// each. <b>The SQLSTATE does not carry that split and the message text does</b>, which is the reverse
    /// of what the code alone suggests: 57014 is <c>query_canceled</c>, which PostgreSQL raises for the
    /// target's <c>statement_timeout</c>, for a <c>pg_cancel_backend()</c>, and for the client
    /// CancelRequest Npgsql sends when its own <c>CommandTimeout</c> expires — while
    /// <c>canceling statement due to statement timeout</c> and
    /// <c>canceling statement due to user request</c> are different strings. So the parameter is not a bool
    /// over the code: that expression is
    /// <see cref="PgBaselineProvider.IsCommandTimeout"/>'s first disjunct, where it correctly identifies
    /// OUR deadline, and one expression cannot mean both. <see cref="CollectorFaultCancelOrigin.For"/>
    /// reads the text instead, and reports <see cref="PostgresCancelSource.Unproven"/> for every wording it
    /// does not recognise so that a translated or reworded message costs the weaker sentence rather than
    /// the wrong machine.</para>
    /// </summary>
    internal static string PostgresTimeoutExplanation(
        string collectorName, string? connectedDatabase, long elapsedMs, CollectorFaultCancelOrigin origin)
    {
        var where = string.IsNullOrWhiteSpace(connectedDatabase)
            ? "the connected database"
            : $"database '{connectedDatabase}'";

        /* Invariant culture on the grouping separator: this string is read by operators and compared
           across rows, and a machine-dependent thousands separator makes two identical durations look
           like different ones. */
        var elapsed = elapsedMs.ToString("N0", CultureInfo.InvariantCulture);

        return origin.Source switch
        {
            PostgresCancelSource.TargetStatementTimeout =>
                $"{collectorName} on {where} was CANCELLED BY THE SERVER after {elapsed} ms (SQLSTATE "
                + $"{CollectorFaultCancelOrigin.QueryCanceled}) — the target's own statement_timeout "
                + "expired, so the deadline that fired is on the monitored server, not here. Nothing was "
                + "collected this cycle: this is NOT 'there was nothing to collect'.",

            /* "its command timeout", never the name of a knob. This arm fires for EVERY PostgreSQL
               collector classified as CommandTimeout, and only two of them set
               CommandTimeoutSecondsOverride at all - the rest fall back to
               DarlingCollectorRunner.CommandTimeoutSeconds. Naming the override would therefore be
               false for most collectors that can reach here, and would send an operator looking for a
               setting that collector does not have. The MEASURED elapsed time above says what the
               deadline actually was, which is the more useful number anyway: it is what applied,
               rather than what was configured somewhere.

               The no-SQLSTATE clause is why this sentence belongs to this arm alone and is not the
               fall-through for everything that is not the server's statement_timeout: it describes the
               shape Npgsql produces when its own deadline fires and it never reaches the server's error
               response. A cancel that DID arrive as SQLSTATE 57014 would be contradicted by its own
               explanation. */
            PostgresCancelSource.OurCommandDeadline =>
                $"{collectorName} on {where} hit its CLIENT-SIDE command deadline after {elapsed} ms — "
                + "its command timeout expired and Npgsql cancelled the read mid-stream, which is why the "
                + "transport reports 'Exception while reading from stream' with no SQLSTATE. The statement "
                + "was still running when it was cut off, so the work asked for does not fit the deadline; "
                + "raising the deadline is the wrong half of that and shrinking the work is the right one. "
                + "Nothing was collected this cycle: this is NOT 'there was nothing to collect'.",

            /* The honest arm, and the one this method exists to be able to reach. A 57014 whose wording is
               not the statement_timeout one was cancelled by SOMETHING, and the code names neither the
               machine nor the knob; the wording is quoted rather than interpreted, because a human reading
               a translated or reworded message can place it and this code cannot. It rules the target's
               statement_timeout OUT, which is the actionable half - the alternative on offer is a
               confident sentence about whichever machine the coin landed on.

               The code is DESCRIBED as PostgreSQL's behaviour, never attributed to this fault, and that
               distinction is load-bearing. Unproven is reachable for a fault carrying no SQLSTATE at all -
               CollectorFaultCancelOrigin.For is total on purpose, so that it does not rest on the arm's
               filter - and a sentence reading "(SQLSTATE 57014)" would then assert a code the fault never
               carried. Resting the SENTENCE on the filter instead of the classifier would be this same
               defect one layer up: a confident claim true only because of something a caller elsewhere
               happens to do. The two arms above assert nothing beyond the CommandTimeout classification
               this whole method is handed, which is the caller's to guarantee for all three. */
            _ => $"{collectorName} on {where} was CANCELLED after {elapsed} ms, and this fault does not "
                 + "say WHOSE deadline fired. PostgreSQL raises SQLSTATE "
                 + $"{CollectorFaultCancelOrigin.QueryCanceled} for the target's own statement_timeout, "
                 + "for a pg_cancel_backend() aimed at the backend, and for the client CancelRequest this "
                 + "service's own command deadline sends: one code, three producers, and only the wording "
                 + $"tells them apart. The server said {QuotedCancelWording(origin.ServerText)}, which is "
                 + "not its statement_timeout wording — so do not change statement_timeout on the target "
                 + "on the strength of this row. Nothing was collected this cycle: this is NOT 'there was "
                 + "nothing to collect'.",
        };
    }

    /// <summary>
    /// The server's own cancel wording, quoted for an operator to read, degrading to a phrase when the
    /// error carried none — the rule <c>PostgresTimeoutExplanation</c> already follows for an unknown
    /// database. Quoting an empty string would present "the server said nothing" as "the server said
    /// ''".
    /// </summary>
    private static string QuotedCancelWording(string? serverText)
        => string.IsNullOrWhiteSpace(serverText) ? "nothing at all" : $"'{serverText}'";

    /// <summary>
    /// True when a SqlException is a permission denial — the expected failure when the least-privilege monitoring
    /// login (VIEW SERVER STATE only) re-executes a query that reads/writes user objects. Detected by the known
    /// permission error numbers or a "permission was denied" / "does not have permission" message, so the handler
    /// can surface a clear cause instead of a raw exception.
    /// </summary>
    private static bool IsPermissionError(SqlException ex)
    {
        foreach (SqlError error in ex.Errors)
        {
            if (error.Number is 229 or 230 or 262 or 297 or 300 or 301 or 916 or 33044)
            {
                return true;
            }

            if (error.Message.Contains("permission was denied", StringComparison.OrdinalIgnoreCase)
                || error.Message.Contains("does not have permission", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// This body's peer high-water mark for the collection_log write (#2864), or null before any
    /// non-budgeted collector has run in it.
    ///
    /// <para>NULL rather than the -1 the field carries, because the column means 'no peer had run yet when
    /// this row was written' - true for the first collector of every body - and a stored -1 would be a
    /// magic number every reader had to know to filter. The sentinel is right in memory, where it must not
    /// collide with a real 0 ms peer; NULL is right in the store, where absence has its own value.</para>
    /// </summary>
    private static int? PeerMaxOrNull(ServerLoopState server) =>
        server.SweepPeerMaxMs >= 0 ? server.SweepPeerMaxMs : null;

    /// <summary>
    /// Runs one collector for a server and logs its outcome to collection_log. Returns the rows written
    /// (0 on skip/permissions/error) so an on-demand snapshot can tally them; the scheduled/on-load callers
    /// simply discard the count.
    /// </summary>
    /// <param name="peerMaxAtDispatchMs">
    /// The sweep body's peer high-water mark AS AT DISPATCH (#2864 review), or null when this call is not
    /// part of a scheduled body. Passed in rather than re-read from <c>server.SweepPeerMaxMs</c> at
    /// completion because <c>query_store</c> and <c>plan_correction</c> are dispatched FIRE-AND-FORGET:
    /// their runs outlive the body by 100-230s while the 15s sweep resets and rebuilds the mark several
    /// times over, so a value read at completion describes some unrelated later tick. Those two are among
    /// the heavies this diagnostic exists to explain, so reading it late is wrong exactly where it matters.
    /// </param>
    private async Task<int> RunOneAsync(ServerLoopState server, DarlingCollectorRunner runner, string collectorName, int? peerMaxAtDispatchMs, CancellationToken cancellationToken)
    {
        var runtime = server.Runtime;
        if (runtime is null || !s_dispatch.TryGetValue(collectorName, out var run))
        {
            return 0;
        }

        /* #2165: the tick's Query Store pass and the backfill slice both do heavy QS text extraction, and
           they used to be free to run against the SAME server at once — measured as ~128 MB in flight on a
           4-core box, because a big catalog arriving triggers BOTH loops. Gated HERE because this is the one
           funnel that has the runtime and the collector name together. Never waits: see QueryStoreServerGate
           for why blocking a shared fleet loop would recreate the #2148 wedge through a lock. Skipping is safe
           for this collector because its window is watermark-driven (#1960) — the next pass resumes from the
           same boundary, so a skipped pass defers rows rather than dropping them. */
        using var queryStoreGate = IsQueryStoreCollector(collectorName)
            ? _queryStoreGates.GetOrAdd(runtime.ServerId, static _ => new QueryStoreServerGate()).TryAcquire()
            : QueryStoreServerGate.NotGated;

        if (queryStoreGate is null)
        {
            _logger.LogInformation(
                "  [{Server}] query_store skipped this tick — its Query Store backfill slice is mid-flight (#2165). " +
                "Resumes next tick from the same watermark; no rows are lost.",
                server.Config.DisplayName);
            return 0;
        }

        /* #2717: the generic sibling of the gate above, for collectors detached from the sequential body
           for the same bimodal-cost reason as query_store but with no second loop to exclude — see
           DetachedCollectorGate's own doc comment. Keyed by (server, collector name), so a future third
           collector detached this way needs only its own IsXCollector check added to this condition; the
           dictionary already generalizes. A held gate here means a previous detached tick for THIS
           collector on THIS server has not finished — skip is safe because every collector detached this
           way is picked specifically for having no wall-clock-derived window (plan_correction re-reads
           the live DMV set whole on every pass, so a skip just re-reads it, possibly refreshed, next time).
           NotGated (mirroring QueryStoreServerGate's) collapses this to a single null check below — a
           future third collector needs only its own IsXCollector check added to this one condition,
           never a second one to keep in sync. */
        using var detachedGate = IsPlanCorrectionCollector(collectorName) || IsPgWaitSamplingCollector(collectorName)
            ? _detachedCollectorGates.GetOrAdd((runtime.ServerId, collectorName), static _ => new DetachedCollectorGate()).TryAcquire()
            : DetachedCollectorGate.NotGated;

        if (detachedGate is null)
        {
            _logger.LogInformation(
                "  [{Server}] {Collector} skipped this tick — a previous detached run has not finished (#2717). " +
                "Re-reads the live set next tick; no rows are lost.",
                server.Config.DisplayName, collectorName);
            return 0;
        }

        /* #2997: wall clock for the whole run, read ONLY by the fault arms below. The success path
           keeps reporting the runner's own measured sql/storage split and never consults this — a
           wall clock that included dispatch overhead would quietly widen three published numbers
           (collection_log.duration_ms, sql_duration_ms and the collector_cost series) to satisfy a
           log line.

           The fault arms have no split to report, because a fault by definition interrupted whichever
           phase was running, and what they recorded instead was three literal zeros. That made a
           300-second death indistinguishable from an instant one and sent two separate investigations
           after a connection that dies at open. One honest total beats three precise zeros.

           WHICH SLOT each arm reads this into follows where the time was actually spent, not
           convenience: an arm whose statement reached the target and was refused THERE passes it as
           sqlMs, and an arm that never queried the target at all passes it as storageMs. The RDS/PI
           authorization arms are the second kind, and take the accounting their own success paths
           already use — see IngestRdsPlansAsync, IngestRdsDeadlocksAsync and IngestRdsCpuAsync, which
           file an HTTPS round trip as storage time so that one target's sql_duration_ms cannot mean
           something different from every other target's. duration_ms is sqlMs + storageMs either way,
           so the wall clock is honest under both; only the decomposition differs. */
        var runClock = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            /* #3754: the long-query completion session is ensured by the per-sweep reconcile, not by this
               run, and until now nothing carried the reconcile's failure here. So on a target where the
               CREATE was refused in every database the collector still ran, its tolerant ring-buffer read
               found no session and returned zero rows - which is also what a quiet session returns - and
               the run recorded SUCCESS, sweep after sweep, while the service log said why it could not
               work. When the reconcile recorded that the session exists nowhere this collector would read,
               the honest run is SESSION_MISSING with the reconcile's message, and the read is not worth
               its connections: throwing the same exception RunXeTolerantAsync raises for a permission-
               denied session lands in the same arm below, so the row, the log line and the classification
               are the ones an operator already knows from the deadlock and blocked-process collectors. */
            if (IsLongQueryCompletionsCollector(collectorName) && server.LongQueryTraceFault is { } traceFault)
            {
                throw new DarlingXeSessionMissingException(traceFault);
            }

            var result = await run(runner, runtime, cancellationToken);

            /* #3754, the partial case: the Azure reconcile created the session in some databases and was
               refused in others. The run just read the survivors and its SUCCESS is a real success - but
               its row has to say that the refused databases are not in it, or a zero here reads as a quiet
               server. The #2623 partial note the reconcile composed rides the same channel every other
               partial loss uses (MergeNotes, so a probe-failure or per-database note the runner itself
               composed is kept beside it). HostNote rather than Note: Note is computed from HostNote plus
               the definition's measurements, and setting the composed value would lose the measurements. */
            if (IsLongQueryCompletionsCollector(collectorName) && server.LongQueryTracePartialNote is { } partialNote)
            {
                result = result with { HostNote = EnumeratedCollectorDriver.MergeNotes(result.HostNote, partialNote) };
            }

            /* #4004 review, round 3: the first pg_log_events run after a start that replaced a discarded log-hash key
               carries that on its row, through the same HostNote channel; taking it clears it, so no later run does. */
            result = _logHashKeyRotation.ApplyTo(collectorName, result);

            /* #4046: a log read that skipped lines stamped outside the target's UTC log_timezone carries their count;
               this puts the sentence that names the setting and the issue beside it, on the same HostNote channel. */
            result = DarlingCollectorRunner.WithForeignZoneLinesNote(result);

            /* #4058 L1: a plan-capture run that skipped forged captures (a NULL query id or duration out of
               the guarded CASE chain) carries their count; this puts the sentence that names the issue
               beside it, on the same HostNote channel, following the ForeignZoneLines pattern above. */
            result = DarlingCollectorRunner.WithForgedCaptureNote(result);

            /* #4058 item 1: a deadlock-capture run that skipped RAISE-shaped entries (not written by
               PostgreSQL's own DeadLockReport) carries their count; this puts the sentence that names the
               issue beside it, on the same HostNote channel, following the ForgedCapture pattern above. */
            result = DarlingCollectorRunner.WithRaiseShapedDeadlocksSkippedNote(result);

            /* #4046: the once-a-day nudge for a self-hosted target still on the text route, BEFORE it ever
               hits the 22021 byte — gated to pg_log_events alone so a target with all three log-tail
               collectors scheduled gets one note a day, not whichever of the three wins the 24-hour gate.
               Never fires for Aurora/RDS: they reach the log through the AWS API and never populate
               PgReadBinaryFileCapability's cache, so WithReadBinaryFileAdvisoryNote's cache read finds
               nothing for them. */
            if (string.Equals(collectorName, "pg_log_events", StringComparison.Ordinal))
            {
                result = DarlingCollectorRunner.WithReadBinaryFileAdvisoryNote(result, runtime);
            }

            /* #3102: Debug, which is BELOW the default filter's Information, for the same reason the
               per-database fault split takes its arm's level — see LogPerDatabaseFaultSplit's remarks. A
               timing on the default log with no reason beside it is the shape to avoid, and a run that
               SUCCEEDED has no reason beside it at all. The level is the whole of the choice: the message,
               its gates and its numbers are untouched, so the parsers outside this repo see the identical
               text the moment the level is turned up.

               Not deleted and not aggregated, because the sample IS the artifact here: attributing one
               server's one cycle needs that cycle's own numbers, and a periodic distribution cannot answer
               "what did this collector do at 04:12". Reachable per-namespace rather than all-or-nothing, so
               measuring the store-write path does not also turn on every other Debug line in the process
               (Darling/README.md, "Logs"). */
            _logger.LogDebug("  [{Server}] {Collector} => {Rows} rows (sql:{SqlMs}ms, pg:{PgMs}ms)",
                server.Config.DisplayName, collectorName, result.Rows, result.SqlMs, result.StorageMs);

            /* #3653 A5: the identity-epoch account, if this run's definition saw one. The definition has the
               row (old and new start time, old and new name) and no logger; this loop has the logger and
               no row; the calculator both hold is where the sentence waits (CollectorDeltaCalculator
               .DrainDiscontinuities). Information, not Debug: a forgotten baseline set is a measurement
               discontinuity an operator reading a flat minute on the charts needs to be able to find, and
               it happens on the order of once per restart or failover, never per cycle. Drained on every
               run — a dictionary probe — so the line lands beside the run that observed it and is never
               re-logged by the next. The same run's collection_log row carries the count
               (identity_epoch_changes=1 / statements_epoch_changes=1) as the marker in the store; Lite's
               twin is the same drain in RemoteCollectorService.RunCollectorAsync. Worded as "drained after"
               rather than "observed by": a carrier run that observed the epoch and then failed at its store
               write leaves the sentence queued for the next successful run on the server, whichever
               collector that is. */
            if (_deltas is not null)
            {
                foreach (var discontinuity in _deltas.DrainDiscontinuities(runtime.ServerId))
                {
                    _logger.LogInformation("  [{Server}] {Discontinuity} (#3653 A5; drained after {Collector})",
                        server.Config.DisplayName, discontinuity, collectorName);
                }
            }

            /* #2851: the server-scoped phase split rides its OWN line, for the same reason #2811's fetch
               sub-splits do — the line above is parsed by tooling outside this repo, and "don't break the
               parser" outranks "one line to grep". Gated on the MEASURED flag rather than on a value being
               non-zero: the enumerated path's `PerItemOpenMs > 0` gate cannot tell a genuinely instant open
               from a path that emits no split at all, and this one must.

               wm: sits OUTSIDE the sum and says so, because on this path it genuinely is outside: the
               watermark read runs before the sql: stopwatch starts. Printing it inside a `sql:N = ...`
               decomposition would have made it a permanent 0 and taught every future reader that a store
               read #2796 clocked at 50s cold is free. */
            if (result.ServerPhasesMeasured)
            {
                _logger.LogDebug(
                    "  [{Server}] {Collector} sql:{SqlMs}ms = open:{OpenMs}ms + drain:{DrainMs}ms + other:{OtherMs}ms (wm:{WatermarkMs}ms store-side, outside sql)",
                    server.Config.DisplayName, collectorName, result.SqlMs,
                    result.ServerOpenMs, result.ServerDrainMs, result.ServerOtherMs, result.ServerWatermarkMs);
            }

            /* result.Note is null for an ordinary run — the message column stays null as before. It is set
               only for a successful-but-empty run worth explaining (today: an enumeration that listed zero
               databases, #1837). The status stays SUCCESS, and every health/band read keys on status rather
               than on error_message, so the note is inert outside the Collection Log detail grid.

               The ONE exception is a cycle the #2673 whole-server wall-clock budget abandoned, which reaches
               here on the same path because it returns normally rather than throwing. It is not a successful
               empty run: it stored nothing and advanced no watermark, so recording it SUCCESS made it the
               newest success in ReadCollectionSignalsAsync's status IN ('SUCCESS', 'SKIPPED') — a collector
               abandoning every cycle read as perpetually fresh — and put its message in the #1837 note
               channel, whose whole claim is that the run succeeded. Same reasoning as the RDS/PI
               authorization arms below: nothing was read, so it must not be recorded as a successful empty
               read. */
            var status = EnumeratedCollectorDriver.ClassifyReturnedRun(result.Abandoned);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, status, result.Rows, result.SqlMs, result.StorageMs, result.Note,
                result.Fanout, result.ServerPhases, result.Drain, result.FetchPhases, peerMaxAtDispatchMs, _logger, cancellationToken);

            /* #2864 item 3: fold THIS run into the body's peer high-water mark, AFTER its own row is
               written so a collector is never its own peer. The mark answers the question that took
               manual cross-referencing of neighbouring rows to answer before: when a heavy collector
               blows its budget, were the ORDINARY collectors in that same body slow too?

               Population A - one genuinely large query - runs beside peers at or below their baseline;
               a measured sweep had wait_stats at 1ms and latch_stats at 1ms while query_store took
               71,977ms for 12,557 rows. Population B is sweep-wide degradation, where the same light
               collectors ran 34-47x their baseline BEFORE the heavy ones burned their budget. Same
               stored shape, opposite causes, and only the peers tell them apart.

               Budgeted collectors are excluded because they are the heavy ones being explained - a
               mark that included procedure_stats would be dominated by exactly the run in question.
               Asked of the catalog rather than a name list here: the list would be right until a fifth
               collector earned a budget and silently wrong after. */
            if (!CollectorCatalog.HasWallClockBudget(collectorName))
            {
                server.SweepPeerMaxMs = (int)Math.Min(int.MaxValue, Math.Max(server.SweepPeerMaxMs, result.SqlMs));
            }

            /* #2674: record this run's cost for the hourly collector_cost aggregate — the same numbers that
               go to collection_log, kept as a compact per-(server, collector) series for the cost panel. */
            _collectorCost.Record(runtime.ServerId, collectorName, result.Rows, result.SqlMs, result.StorageMs);

            /* #2219: statement TEXT rides alongside the statement stats, on its own hourly cadence. Hung off the
               stats collector's success rather than given its own loop because it is meaningless without those
               rows and must never run against a server whose stats collection is failing — one less loop that
               can be independently wrong. Best-effort: a text fetch that fails leaves the statistics collected
               and logs, because unreadable text is a degraded read while a failed collection is lost data. */
            if (IsPgStatementStatsCollector(collectorName))
            {
                await TryRefreshPgStatementTextAsync(runtime, cancellationToken);
            }
            return result.Rows;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (DarlingXeSessionMissingException ex)
        {
            /* The blocking/deadlock XE session is missing and couldn't be created — log a distinct
               SESSION_MISSING status the Stage 4 Capture Down self-alert reads. Non-fatal, like a
               permissions skip: log it and continue the rest of the sweep. RunXeTolerantAsync already
               classified this (wrapping an XE 297/15151/'XE session' into the distinct type), so this
               catch and the SqlException permissions filter below match disjoint types — their relative
               order is not load-bearing; it only needs to precede the general Exception catch (which would
               otherwise mislabel it ERROR). A non-XE 297 arrives as a plain SqlException and correctly
               hits the permissions filter.

               #3754: the second producer is the pre-dispatch check at the top of the try, for
               long_query_completions alone - the reconcile recorded that its session could not be created
               anywhere this run would read, so the run is classified here without opening a connection.
               Same type, same arm, same row shape; only the message's origin differs. */
            _logger.LogWarning("  [{Server}] {Collector} => XE session missing (capture down): {Message}",
                server.Config.DisplayName, collectorName, ex.Message);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, "SESSION_MISSING", 0, runClock.ElapsedMilliseconds, 0, ex.Message, fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (RdsLogUnavailableException ex) when (ex.IsAuthorizationFailure)
        {
            /* #2633: the AWS call was DENIED, so nothing was read. Degraded to PERMISSIONS rather than
               ERROR for the same reason a 42501 from the pg_read_file route is — a least-privilege
               deployment is an expected state an operator can act on, and screaming every cycle about it
               would bury real faults — but it must NOT be recorded as a successful empty read, which is
               what returning zero rows used to make it.

               Only the authorization case lands here. A throttle, a failover or an endpoint that stopped
               resolving falls through to the general handler and stays loud, because a permanent-sounding
               status on a transient fault is how an outage gets read as a configuration choice. */
            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: the RDS log API refused the call",
                server.Config.DisplayName, collectorName);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, "PERMISSIONS", 0, 0, runClock.ElapsedMilliseconds,
                $"{ex.Message} — the MONITORING HOST's IAM role lacks a grant this source needs, which is "
                + "not a database grant: plan capture on managed PostgreSQL reads the server log through "
                + "the RDS API, so the role needs rds:DescribeDBLogFiles and rds:DownloadDBLogFilePortion "
                + "on the target instance. Nothing was read this cycle — this is NOT 'no plans were "
                + "captured'.",
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PiMetricsUnavailableException ex) when (ex.IsAuthorizationFailure)
        {
            /* #2719, same shape as the RdsLogUnavailableException handler above and for the same reason:
               the AWS call was DENIED, so no CPU reading was pulled this cycle, and that must read as
               PERMISSIONS rather than a SUCCESS row claiming PI was read and simply had nothing new. */
            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: the RDS/PI API refused the call",
                server.Config.DisplayName, collectorName);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, "PERMISSIONS", 0, 0, runClock.ElapsedMilliseconds,
                $"{ex.Message} — the MONITORING HOST's IAM role lacks a grant this source needs, which is "
                + "not a database grant: instance CPU on managed PostgreSQL reads AWS Performance Insights, "
                + "so the role needs rds:DescribeDBInstances, rds:DescribeDBClusters and "
                + "pi:GetResourceMetrics on the target instance. Nothing was read this cycle — this is NOT "
                + "'CPU is idle'.",
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PgLogTimezoneUnsupportedException ex)
        {
            /* #2993: this target's log_timezone is not UTC, so the lines in its server log are stamped in
               local time and cannot be stored against UTC. Both readers that assemble log entries through
               PgLogEntryAssembler (pg_log_events and pg_deadlocks) land here, which is why the message names
               the log, not deadlocks.

               PERMISSIONS for the same reason the PostgresException FeatureDisabled arm below is: none of
               the store's five statuses means "this target is configured in a way this source cannot be
               read under", so the non-fatal degradation bucket carries it and the MESSAGE is where the
               truth goes. It also earns that bucket on the merits — the cause is a setting on the
               monitored server, an operator can clear it, and CollectorRuntimePrecondition's PERMISSIONS
               arm already frames the condition as satisfiable and re-derives it on every read.

               Not ERROR: nothing is broken on the monitoring side, and a parameter group does not change
               because we shouted about it once a minute. Not a SUCCESS row with zero rows, which is what
               dropping the unusable blocks quietly would have produced — a target whose log is in the
               wrong zone reading as a target with no deadlocks.

               Both transports arrive here: the pg_read_file route throws out of the collector's ReadAsync,
               the RDS log-API route out of the ingestor's Extract.

               That shared arrival is also why this is the ONE fault arm that records zeros rather than
               runClock's wall clock. The slot rule at the stopwatch's declaration keys on whether the
               target was queried, and the two transports answer that oppositely: pg_read_file IS a target
               query and belongs in sqlMs, while the RDS log API is an HTTPS round trip that belongs in
               storageMs. The exception carries only ObservedZone, so this catch cannot tell which one it
               is, and either slot would be wrong for half the fleet. A zero here understates a duration;
               a guessed slot would misattribute one, and this store's readers decompose sql vs storage
               per target. Threading the transport onto PgLogTimezoneUnsupportedException is what would
               let this arm join the rule. */
            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: the server log is stamped '{Zone}', not UTC",
                server.Config.DisplayName, collectorName, ex.ObservedZone);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, "PERMISSIONS", 0, 0, 0, ex.Message,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PgLoggingCollectorOffException ex)
        {
            /* #3410's third state: logging_collector is off, so the target writes its log to stderr and
               there is no file for the pg_read_file route to read. Not 42501's missing grant and not
               58P01's missing file — neither a grant nor a path change can produce a file the server is
               not writing — so it takes the timezone arm's disposition for the timezone arm's reasons:
               PERMISSIONS, because it is a setting on the monitored server that an operator can change and
               CollectorRuntimePrecondition's arm already frames it as satisfiable and re-derived every
               cycle; not ERROR, because nothing is broken on the monitoring side and a deliberate logging
               destination does not change because we shouted about it once a cycle; not a SUCCESS row with
               zero rows, which would read as a server with no deadlocks and nothing slow. The message is
               the exception's own — it names the setting and the restart — which is the same
               not-collected-with-the-reason answer the store's own log read gives when its directory
               listing comes back empty.

               Unlike the timezone arm this one CAN name its slot: only the pg_read_file route returns the
               marker row — the RDS transport runs no SQL and its platform keeps the logging collector on —
               so the elapsed time is a target query and belongs in sqlMs. */
            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: logging_collector is off, so the target writes no server log files",
                server.Config.DisplayName, collectorName);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, "PERMISSIONS", 0, runClock.ElapsedMilliseconds, 0, ex.Message,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PgNoStderrLogFileException ex)
        {
            /* #3997's narrower sibling to the arm above: logging_collector is ON, but log_destination
               carries no stderr format, so the shared tail's newest CTE excluded every file it saw as a
               csvlog/jsonlog sibling and there is nothing left for the pg_read_file route to read. Same
               disposition as PgLoggingCollectorOffException and for the same reasons — a setting on the
               monitored server, satisfiable and re-derived every cycle, not broken on the monitoring side,
               and a SUCCESS row with zero rows would read as a target with nothing to report rather than
               one this route cannot read at all. Same slot rule too: only the pg_read_file route produces
               this marker, so the elapsed time is a target query and belongs in sqlMs. */

            /* #4053 review L1: the cache said stderr-only, but no stderr-format file is left — the far more
               likely explanation is csvlog/jsonlog was just added and the cache has not caught up, not that
               log_destination emptied entirely. Drop the verdict so the next cycle re-probes instead of
               reading no stderr file (and, on the csvlog route, no rows) for up to an hour.

               #4053 review L1 (round 2): this marker is also thrown by pg_deadlocks and pg_plan_capture when
               they read the stderr tail (csvlog off, or a jsonlog-only target, which they don't read yet).
               Their no-stderr-file fault says nothing about a csvlog or jsonlog verdict, so it must not drop
               one. Gate on RoutedCollectors, the same set the runner
               keys the probe itself on, so only a collector this cache actually informs can invalidate it.

               #4053 a2 review W1: and only when the cached verdict said stderr-only. A missing stderr file
               contradicts that verdict and nothing else. On a jsonlog-only target the deadlock and plan
               collectors (still stderr readers) hit this arm every cycle, and clearing a jsonlog verdict there
               would re-probe the target on every routed run. */
            var cacheKey = DarlingCollectorRunner.ReadBinaryFileCacheKey(runtime);
            if (PgLogFormatCapability.RoutedCollectors.Contains(collectorName)
                && PgLogFormatCapability.CachedVerdictIsStderrOnly(cacheKey))
            {
                PgLogFormatCapability.Invalidate(cacheKey);
            }

            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: log_destination carries no stderr-format file for this target",
                server.Config.DisplayName, collectorName);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, "PERMISSIONS", 0, runClock.ElapsedMilliseconds, 0, ex.Message,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PgNoCsvlogFileException ex)
        {
            /* #4053 part a1b's own narrow state: pg_log_events is on the csvlog route (log_destination
               includes csvlog), logging_collector is on, but no .csv file has appeared under log_directory
               yet — most likely csvlog was only just added to the destinations and the syslogger has not
               rolled a file since the reload. Same disposition as PgNoStderrLogFileException and for the
               same reasons: a setting on the monitored server, satisfiable and re-derived every cycle, not
               broken on the monitoring side. Only the csvlog route produces this marker, so the elapsed
               time is a target query and belongs in sqlMs. */

            /* #4053 review L1: the cache said csvlog was one of the destinations, but no .csv file exists.
               The most likely explanation once the syslogger has had time to roll a file is that csvlog was
               REMOVED from log_destination since the cache checked — the exception's own message text
               ("was only just added") is aimed at the fresher case, but this fault is exactly the evidence a
               stale cache needs. Drop the verdict so the next cycle re-probes; a genuinely fresh csvlog
               addition simply gets the same true verdict back next time.

               #4053: every csvlog reader throws this marker (pg_log_events, pg_deadlocks and pg_plan_capture on
               the self-hosted route; the RDS ingestors on a stale csv listing). The RoutedCollectors gate is
               applied here too, for the same reason as the sibling arm above: one guard the cache trusts, not
               two that must agree by accident. */
            if (PgLogFormatCapability.RoutedCollectors.Contains(collectorName))
            {
                PgLogFormatCapability.Invalidate(DarlingCollectorRunner.ReadBinaryFileCacheKey(runtime));
            }

            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: no csvlog file has appeared for this target yet",
                server.Config.DisplayName, collectorName);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, "PERMISSIONS", 0, runClock.ElapsedMilliseconds, 0, ex.Message,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PgNoJsonlogFileException ex)
        {
            /* #4053 part a2's own narrow state: pg_log_events is on the jsonlog route (log_destination
               includes jsonlog), logging_collector is on, but no .json file has appeared under log_directory
               yet — most likely jsonlog was only just added to the destinations and the syslogger has not
               rolled a file since the reload. Same disposition as PgNoCsvlogFileException and
               PgNoStderrLogFileException, and for the same reasons: a setting on the monitored server,
               satisfiable and re-derived every cycle, not broken on the monitoring side. Only the jsonlog
               route produces this marker, so the elapsed time is a target query and belongs in sqlMs. */

            /* Same #4053 review L1 reasoning as the csvlog arm above: the cache said jsonlog was one of the
               destinations, but no .json file exists — drop the verdict so the next cycle re-probes rather
               than reading no rows for up to an hour, gated on RoutedCollectors so only a collector this
               cache actually informs can invalidate it. */
            if (PgLogFormatCapability.RoutedCollectors.Contains(collectorName))
            {
                PgLogFormatCapability.Invalidate(DarlingCollectorRunner.ReadBinaryFileCacheKey(runtime));
            }

            _logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: no jsonlog file has appeared for this target yet",
                server.Config.DisplayName, collectorName);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, "PERMISSIONS", 0, runClock.ElapsedMilliseconds, 0, ex.Message,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (SqlException ex) when (ex.Number == 1222 && CollectorCatalog.YieldsOnLockTimeout(collectorName))
        {
            /* The 1-second LOCK_TIMEOUT guard doing its job (#1805): the snapshot sweep stepped aside
               instead of joining a blocking chain on the monitored server. Not a collection failure —
               the next sweep sees current state — so it records as YIELDED: its own status, excluded
               from the error counts that feed collector health, the daily health band, and the
               collection-failure self-alerts, and readable as evidence of lock contention on the
               TARGET rather than a monitoring fault. Same classification Lite applies — parity is the
               point. A 1222 from a collector without the guard flag falls through to the general
               catch below, unchanged. This filter and the permissions filter match disjoint
               conditions, so their relative order is not load-bearing; both only need to precede the
               general Exception catch. */
            _logger.LogInformation("  [{Server}] {Collector} => YIELDED - 1s lock-timeout guard fired (target lock contention)",
                server.Config.DisplayName, collectorName);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, "YIELDED", 0, runClock.ElapsedMilliseconds, 0,
                $"Lock-timeout yield (SQL error #{ex.Number}): the 1-second LOCK_TIMEOUT guard fired rather than waiting in a blocking chain. One snapshot sweep skipped; evidence of lock contention on the monitored server, not a monitoring failure.",
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (SqlException ex) when (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
        {
            /* Same Azure explanation Lite appends (#1631): error 300 on Azure SQL Database is a service
               objective limit phrased as a permission denied on 'master', which reads as a missing GRANT
               and sends people looking for one that cannot be issued. Appended, so the raw error stays
               searchable. Parity is the point — a Darling operator gets the identical sentence Lite gives.
               8189 is sys.traces' own denial ("You do not have permission to run 'SYS.TRACES'", ALTER
               TRACE missing): a legitimate least-privilege choice (#1823) — ALTER TRACE is not read-only —
               so default_trace_events must degrade as PERMISSIONS, not scream ERROR every cycle.
               #2512: the number set moved to SqlServerPermissionErrors, shared with Lite's catch and
               with SqlServerTargetProvider.Classify, and gained 262 — "permission denied in database
               'tempdb'", the #2150 denial that used to record ERROR every cycle and is the reason
               tempdb_stats was gated off Azure SQL Database at all. */
            var message = ex.Message + AzureDmvPermissionHint.For(
                ex.Number, server.Runtime?.Target.IsAzureSqlDb == true, ex.Message);

            _logger.LogWarning("  [{Server}] {Collector} => insufficient permissions ({Number}): {Message}",
                server.Config.DisplayName, collectorName, ex.Number, message);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, "PERMISSIONS", 0, runClock.ElapsedMilliseconds, 0, message, fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        /* #2997: the database is read off the EXCEPTION first, falling back to the runtime's own. #2638
           added the database name to the ObjectMissing sentence precisely so an operator would not go
           looking in the wrong one - and for a RunsPerDatabase collector the runtime's field is the wrong
           one, because it holds whatever database the initial probe landed on rather than the database
           this fault came from. See CollectorFaultDatabase. */
        catch (PostgresException ex) when (
            PostgresFaultOutcome(
                ex,
                collectorName,
                CollectorFaultDatabase.For(ex, runtime.ConnectedDatabase),
                runtime.PgStatStatementsExtension,
                runtime.Target.IsAurora)
                is { Status: not "ERROR" } outcome)
        {
            /* PostgreSQL faults classified by SQLSTATE through the same ITargetProvider.Classify the
               engine seam already exposes, so the runner and the provider cannot disagree about what an
               error means.

               Without this the general catch below claimed every one of them, and a PERSISTENT condition
               would log ERROR every single cycle forever: pg_statement_stats against a database where the
               extension was never created (42P01), a source Aurora does not implement at all (0A000), a
               feature switched off in the parameter group (55006). Those are the exact PostgreSQL analogue
               of the 8189 sys.traces denial above, which degrades to PERMISSIONS for the same reason —
               it is a real, operator-actionable state, not a monitoring fault, and burying it in a
               once-a-minute error is how it gets ignored.

               The message says WHICH kind it is rather than leaving "PERMISSIONS" to imply a missing
               GRANT, following the AzureDmvPermissionHint precedent: the status is the store's
               non-fatal-degradation bucket, the text is where the truth goes. */
            var (status, explanation) = (outcome.Status, outcome.Explanation);

            /* #4051 review L1: a 42501 while the binary route is in use means that grant is gone, so the
               cached verdict must not outlive it by up to an hour. */
            DarlingCollectorRunner.ForgetStaleReadBinaryFileVerdict(collectorName, ex, runtime);

            /* #4251 round-1 review, M1: the real landing spot for a pg_server_config 42501 — PostgresFaultOutcome
               classifies SqlState 42501 PERMISSIONS for every collector (PostgresTargetProvider.Classify), so
               this fault never reaches the general catch below, and a call only in LogTailGeneralFault would
               never fire for it. Dropping the stale verdict here is what lets the next cycle re-probe instead
               of losing the whole pg_settings snapshot until the hour runs out. */
            DarlingCollectorRunner.ForgetStaleFileSettingsVerdict(collectorName, ex, runtime);

            if (status == "YIELDED")
            {
                _logger.LogInformation("  [{Server}] {Collector} => YIELDED - {Explanation}",
                    server.Config.DisplayName, collectorName, explanation);
            }
            else
            {
                _logger.LogWarning("  [{Server}] {Collector} => {Status} ({SqlState}): {Message}",
                    server.Config.DisplayName, collectorName, status, ex.SqlState, explanation);
            }

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, status, 0, runClock.ElapsedMilliseconds, 0, explanation, fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        /* yieldsOnLockTimeout: false is deliberate, and matches the general catch below rather than the
           PostgresException arm above. That flag only ever decides the 55P03 branch inside Classify, so it
           cannot change a CommandTimeout answer - passing the collector's real value here would read as
           though the yield question were relevant to this filter, and it is not. Both of the plain-Exception
           filters in this method ask a single narrow question of the classifier and pass false for the same
           reason; the SQLSTATE arm passes the real value because it dispatches on the whole fault map,
           55P03 included. */
        catch (Exception ex) when (
            server.Runtime?.Target.Engine == CollectorTargetEngine.PostgreSql
            /* The arm may only claim what the filter guarantees. Classify's first branch answers
               CommandTimeout for ANY TimeoutException, and EnumeratedCollectorDriver.ItemBudgetException
               throws a BARE one for the in-process per-item wall-clock budget - a monitoring-side cut
               that never reached the database and has nothing to do with Npgsql. Without this term the
               authored sentence below would tell an operator that Npgsql cancelled a read mid-stream
               about a budget this service abandoned on its own. Latent today, because no PostgreSQL
               collector declares PerItemWallClockBudget, which is exactly the kind of "true when written"
               that stops being true without anyone revisiting the sentence.

               NpgsqlException covers BOTH real deadlines and only those: PostgresException derives from
               it (verified against Npgsql 10.0.3), so SQLSTATE 57014 still lands here, while the bare
               TimeoutException falls through to the general catch - which classifies it CommandTimeout
               too, so it still forces no reprobe, and now reports the budget's own message and the real
               elapsed time rather than a borrowed narrative. */
            && ex is NpgsqlException
            /* #3111: and NOT a write to the STORE. The engine term above was standing in for "this fault
               came from reading the target", and for a store write that proxy is simply false — the store
               connection is Npgsql whatever the target's engine is, so a COPY timeout satisfies both terms
               above and lands here. Observed on a real fault: a start-phase COPY timeout under store-side
               contention arrives as an NpgsqlException wrapping a TimeoutException, classifies
               CommandTimeout, and reached this arm.

               What it then got was the client-side sentence below, whose remedy is target-read-specific:
               "Npgsql cancelled the read mid-stream ... the work asked for does not fit the deadline ...
               shrinking the work is the right one." Shrinking a read is not the remedy for a COPY blocked
               on a store-side lock, and a confident wrong instruction is worse than a vague one because an
               operator acts on it.

               #3095's phase axis is the discriminator this filter lacked: a recorded phase is only ever
               written by a COPY into the store, so it identifies a store write BY CONSTRUCTION rather than
               by inference. Excluded here rather than given a store-side sentence of its own, so that one
               fault reports one way: the general arm below already renders the phase into both the app log
               and the collection_log row, and it is where this same fault already lands on a SQL Server
               target. A second authored sentence would make the identical store fault read differently
               depending on the monitored target's engine, which the store write has nothing to do with.

               Only the PROVEN population moves. An unstamped fault keeps #2997's sentence, so the post-COPY
               dimension flush and commit (#1767) are still described as target reads — see
               IsProvenStoreWrite for why that residual is the affordable direction. */
            && !CollectorFaultCopyPhase.IsProvenStoreWrite(ex)
            && PostgresTargetProvider.Instance.Classify(ex, yieldsOnLockTimeout: false)
               == CollectorTargetFault.CommandTimeout)
        {
            /* #2997: a PostgreSQL deadline, authored rather than left as transport text.
               pg_index_bloat died here eleven consecutive times reporting only "Exception while
               reading from stream" — seven words, no collector, no database, no duration — which is
               indistinguishable from a dropped socket and was read as one twice.

               Deliberately AFTER the PostgresException arm above and not merged into it: this arm has
               to catch a plain Exception, because a client-side deadline normally arrives as an
               NpgsqlException wrapping a TimeoutException, carrying no SQLSTATE for a SQLSTATE map to
               switch on. Normally, not always — Npgsql enforces its deadline by sending a CancelRequest,
               so the server's 57014 error response can arrive before the stream tears and OUR deadline
               then surfaces as a PostgresException. A 57014 reaches the arm above first, where
               PostgresFaultOutcome maps it to ERROR and so declines it, which is what lets both shapes
               land here. Which one arrives is a RACE, so the exception's type cannot be what tells the
               two deadlines apart, and neither can the SQLSTATE that three unrelated producers share:
               CollectorFaultCancelOrigin reads what the server actually said.

               NO reprobe, and that is the same care the general catch takes: the provider classifies
               this CommandTimeout rather than ConnectionFatal precisely so a slow statement cannot
               force a reconnect and turn a tuning problem into a reconnect storm. ConnectionFatal is
               left to the general catch below, which is where the reprobe lives; classifying it here
               would take the reprobe away and re-create the bug that arm exists to fix. */
            var elapsedMs = runClock.ElapsedMilliseconds;
            var explanation = PostgresTimeoutExplanation(
                collectorName,
                /* Off the exception, not the runtime: pg_index_bloat fans out per database and opens its
                   own connection for each, so the runtime's initial-probe database is not the one that
                   ran out of time. Naming the wrong database confidently is worse than naming none. */
                CollectorFaultDatabase.For(ex, runtime.ConnectedDatabase),
                elapsedMs,
                /* Off the exception for the same reason, one line on: the machine whose deadline fired is
                   read from what the server actually said, not inferred from the SQLSTATE - which is
                   shared by the target's statement_timeout, an external cancel and our own. Naming the
                   wrong MACHINE confidently is the same defect as naming the wrong database, and this
                   arm's whole job is to be the row an operator acts on. */
                CollectorFaultCancelOrigin.For(ex));

            _logger.LogError("  [{Server}] {Collector} => ERROR (timeout): {Message}",
                server.Config.DisplayName, collectorName, explanation);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, "ERROR", 0, elapsedMs, 0, explanation,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (Exception ex)
        {
            /* #3095: the COPY phase is named when the fault carries one. This arm is where a collector's
               binary COPY into the STORE lands, on EITHER engine, and that uniformity is deliberate: it
               reaches neither the SQLSTATE arm above (it is not a PostgresException) nor the
               PostgreSQL-target timeout arm (#3111 excludes a proven store write from it, since the store
               connection is Npgsql whatever the target's engine is and the fault would otherwise satisfy
               that arm's every term on a PostgreSQL target). "Exception while reading from stream" is all
               it said: the two COPY phases carry separate deadlines and produce that same string, so the
               message could not say which of them had been reached.

               Computed once and used for BOTH the app log and the collection_log row, because the stored
               row is the instrument any measurement of this population reads; naming the phase only in the
               app log would leave the store's own error rows as ambiguous as they are today.

               Total by construction: a fault with no phase — which is every fault that is not a COPY —
               gets the very string it has now, with nothing allocated. That matters here specifically,
               because this arm is also the OutOfMemoryException landing pad. */
            var message = CollectorFaultCopyPhase.Describe(ex);

            /* #4046 part 1c: a planted byte on a log-tail read keeps ERROR, with its own sentence, and drops a
               stale pg_read_binary_file verdict so a grant made in answer takes effect next cycle. */
            message = LogTailGeneralFault(ex, collectorName, runtime) ?? message;

            _logger.LogError("  [{Server}] {Collector} => ERROR: {Message}",
                server.Config.DisplayName, collectorName, message);

            /* A dead connection poisons every collector — force a reconnect + reprobe. The Postgres arm
               matters as much as the SQL Server one and is deliberately NARROWER than "any
               PostgresException": a cancelled statement (57014) is not a dead socket whichever side
               cancelled it, and dropping the connection over one would turn a tuning problem into a
               reconnect storm. Only the 08 class and the shutdown/unavailability codes qualify, which is
               exactly what the provider's ConnectionFatal means. */
            if ((ex is SqlException sqlEx && (sqlEx.Class >= 20 || sqlEx.Number == -2))
                /* ANY exception on a PostgreSQL target, not just a PostgresException. The pre-filter was the
                   bug: a dead socket surfaces as a plain NpgsqlException with no SQLSTATE — the provider
                   already classifies that as ConnectionFatal, and the call site could not reach it. So the
                   runtime stayed "connected", Server Unreachable never fired, and every collector errored
                   forever. Asymmetric with the SqlClient arm, which does reach its own classifier. */
                || (server.Runtime?.Target.Engine == CollectorTargetEngine.PostgreSql
                    && PostgresTargetProvider.Instance.Classify(ex, yieldsOnLockTimeout: false)
                       == CollectorTargetFault.ConnectionFatal))
            {
                server.Runtime = null;
                server.NextConnectAttempt = DateTime.UtcNow.AddSeconds(60);
                _logger.LogWarning("[{Server}] Connection-level failure — will reconnect", server.Config.DisplayName);
            }

            /* Best-effort store write (#1556): this is also the OutOfMemoryException landing pad (OOM is an
               Exception and no earlier catch claims it), and under an OOM this handler's own LogCollectionAsync
               allocation can itself fail. A throw HERE would fault the fire-and-track body task instead of being
               the isolated, already-logged ERROR above — so swallow a secondary failure (nothing is allocated in
               the catch, to stay safe under the very condition it guards against). The LogError above already
               recorded the fault to the app log, so no signal is lost. */
            try
            {
                /* #2997 item 3: the run's real elapsed time, not the literal zero this arm used to
                   store. duration_ms is sqlMs + storageMs, so three zeros made every failure look
                   instantaneous — a 300-second timeout and a refused connection stored the identical
                   row, and the number that separates them is the only one an operator needs first.
                   Read from the stopwatch rather than from the exception, because most faults carry
                   no duration at all. */
                await DarlingObservability.LogCollectionAsync(
                    _postgres!, runtime, collectorName, "ERROR", 0, runClock.ElapsedMilliseconds, 0, message, fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            }
            catch
            {
                /* Intentionally empty — see the comment above. Do not add logging here; it must not throw. */
            }

            return 0;
        }
    }

    /// <summary>
    /// #2717 generalized this from query_store's own name (<c>RunDetachedQueryStoreAsync</c>): shared by
    /// every collector fired detached from <see cref="RunDueCollectorsAsync"/>'s sequential body — see
    /// the two call sites for why each one qualifies. <see cref="RunOneAsync"/>'s own catch-all already
    /// contains every fault but cancellation, so this wrapper exists only to keep a shutdown-time
    /// <see cref="OperationCanceledException"/> from surfacing as an unobserved task exception, the same
    /// containment every other fire-and-track body in this file gets.
    /// </summary>
    private async Task RunDetachedAsync(
        ServerLoopState server, DarlingCollectorRunner runner, string collectorName, int? peerMaxAtDispatchMs, CancellationToken cancellationToken)
    {
        try
        {
            await RunOneAsync(server, runner, collectorName, peerMaxAtDispatchMs, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            /* Shutdown — expected, and safe to abandon: every collector detached this way is picked
               specifically for having no wall-clock-derived window (query_store's is watermark-driven,
               #1960; plan_correction re-reads the live DMV set whole on every pass), so a run dropped
               here resumes correctly — from the same watermark, or by re-reading the current set — on
               the next start. */
        }
    }

    private delegate Task<CollectorRunResult> DispatchEntry(DarlingCollectorRunner runner, ServerRuntime server, CancellationToken cancellationToken);

    /// <summary>Test hook: the collector names the worker can dispatch (pinned against the catalog).</summary>
    internal static IReadOnlyCollection<string> DispatchedCollectorNames => s_dispatch.Keys.ToArray();

    /// <summary>
    /// Collector-name dispatch — the Darling twin of Lite's RunCollectorAsync switch, one typed
    /// entry per shared definition, with Lite's forwarder tolerances mirrored: the XE readers
    /// treat a missing/inaccessible session as zero rows, trace_flags treats denied DBCC as zero
    /// rows with a warning.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, DispatchEntry> s_dispatch = new Dictionary<string, DispatchEntry>(StringComparer.OrdinalIgnoreCase)
    {
        ["wait_stats"] = (r, s, ct) => r.RunAsync(WaitStatsCollector.Instance, s, ct),
        ["latch_stats"] = (r, s, ct) => r.RunAsync(LatchStatsCollector.Instance, s, ct),
        ["spinlock_stats"] = (r, s, ct) => r.RunAsync(SpinlockStatsCollector.Instance, s, ct),
        ["cpu_scheduler_stats"] = (r, s, ct) => r.RunAsync(CpuSchedulerStatsCollector.Instance, s, ct),
        ["plan_cache_stats"] = (r, s, ct) => r.RunAsync(PlanCacheStatsCollector.Instance, s, ct),
        ["tempdb_stats"] = (r, s, ct) => r.RunAsync(TempDbStatsCollector.Instance, s, ct),
        ["memory_grant_stats"] = (r, s, ct) => r.RunAsync(MemoryGrantsCollector.Instance, s, ct),
        ["cpu_utilization"] = (r, s, ct) => r.RunAsync(CpuUtilizationCollector.Instance, s, ct),
        ["memory_stats"] = (r, s, ct) => r.RunAsync(MemoryStatsCollector.Instance, s, ct),
        ["memory_clerks"] = (r, s, ct) => r.RunAsync(MemoryClerksCollector.Instance, s, ct),
        ["memory_pressure_events"] = (r, s, ct) => r.RunAsync(MemoryPressureEventsCollector.Instance, s, ct),
        ["file_io_stats"] = (r, s, ct) => r.RunAsync(FileIoStatsCollector.Instance, s, ct),
        ["server_properties"] = (r, s, ct) => r.RunAsync(ServerPropertiesCollector.Instance, s, ct),
        ["server_config"] = (r, s, ct) => r.RunAsync(ServerConfigCollector.Instance, s, ct),
        ["database_config"] = (r, s, ct) => r.RunAsync(DatabaseConfigCollector.Instance, s, ct),
        ["database_states"] = (r, s, ct) => r.RunAsync(DatabaseStateCollector.Instance, s, ct),
        ["trace_flags"] = RunTraceFlagsTolerantAsync,
        ["database_scoped_config"] = (r, s, ct) => r.RunAsync(DatabaseScopedConfigCollector.Instance, s, ct),
        ["query_store_health"] = (r, s, ct) => r.RunAsync(QueryStoreHealthCollector.Instance, s, ct),
        ["session_stats"] = (r, s, ct) => r.RunAsync(SessionStatsCollector.Instance, s, ct),
        ["session_summary_stats"] = (r, s, ct) => r.RunAsync(SessionSummaryStatsCollector.Instance, s, ct),
        ["waiting_tasks"] = (r, s, ct) => r.RunAsync(WaitingTasksCollector.Instance, s, ct),
        ["procedure_stats"] = (r, s, ct) => r.RunAsync(ProcedureStatsCollector.Instance, s, ct),
        ["running_jobs"] = (r, s, ct) => r.RunAsync(RunningJobsCollector.Instance, s, ct),
        ["perfmon_stats"] = (r, s, ct) => r.RunAsync(PerfmonStatsCollector.Instance, s, ct),
        ["dmv_blocking_snapshot"] = (r, s, ct) => r.RunAsync(DmvBlockingSnapshotCollector.Instance, s, ct),
        ["database_size_stats"] = (r, s, ct) => r.RunAsync(DatabaseSizeStatsCollector.Instance, s, ct),
        ["index_object_stats"] = (r, s, ct) => r.RunAsync(IndexObjectStatsCollector.Instance, s, ct),
        ["query_stats"] = (r, s, ct) => r.RunAsync(QueryStatsCollector.Instance, s, ct),
        ["query_snapshots"] = (r, s, ct) => r.RunAsync(QuerySnapshotsCollector.Instance, s, ct),
        ["query_store"] = (r, s, ct) => r.RunAsync(QueryStoreCollector.Instance, s, ct),
        ["deadlocks"] = (r, s, ct) => RunXeTolerantAsync(DeadlocksCollector.Instance, r, s, ct),
        ["blocked_process_report"] = (r, s, ct) => RunXeTolerantAsync(BlockedProcessReportCollector.Instance, r, s, ct),
        ["long_query_completions"] = (r, s, ct) => RunXeTolerantAsync(LongQueryCompletionsCollector.Instance, r, s, ct),
        ["system_health_events"] = (r, s, ct) => r.RunAsync(SystemHealthEventsCollector.Instance, s, ct),
        ["default_trace_events"] = (r, s, ct) => r.RunAsync(DefaultTraceEventsCollector.Instance, s, ct),
        ["job_history"] = (r, s, ct) => r.RunAsync(JobHistoryCollector.Instance, s, ct),
        ["agent_status"] = (r, s, ct) => r.RunAsync(AgentStatusCollector.Instance, s, ct),
        ["ag_replica_states"] = (r, s, ct) => r.RunAsync(AgReplicaStatesCollector.Instance, s, ct),
        ["ag_database_replica_states"] = (r, s, ct) => r.RunAsync(AgDatabaseReplicaStatesCollector.Instance, s, ct),
        ["plan_correction"] = (r, s, ct) => r.RunAsync(PlanCorrectionCollector.Instance, s, ct),
        ["pvs_stats"] = (r, s, ct) => r.RunAsync(PvsStatsCollector.Instance, s, ct),
        /* PostgreSQL. Dispatch is by name and engine-agnostic; the engine gate upstream in
           RunDueCollectorsAsync means this lambda is only ever reached for a Postgres target. */
        ["pg_wait_stats"] = (r, s, ct) => r.RunAsync(PgWaitStatsCollector.Instance, s, ct),
        ["pg_statement_stats"] = (r, s, ct) => r.RunAsync(PgStatementStatsCollector.Instance, s, ct),
        ["pg_wraparound_stats"] = (r, s, ct) => r.RunAsync(PgWraparoundStatsCollector.Instance, s, ct),
        ["pg_server_config"] = (r, s, ct) => r.RunAsync(PgServerConfigCollector.Instance, s, ct),
        /* TWO TRANSPORTS, one table, same reason as pg_plan_capture below: self-hosted reads the server
           log with pg_read_file; Aurora and RDS have no filesystem and pg_read_server_files is not
           grantable, so those go through the AWS log API instead. This branch was the missing half — the
           collector's own AppliesTo returns true for every target on the assumption the route is "chosen
           at dispatch", but before this there was no dispatch branch, so every Aurora target fell through
           to the pg_read_file route and failed PERMISSIONS 100% of the time (no grant fixes a filesystem
           that is not there). */
        ["pg_deadlocks"] = (r, s, ct) =>
            s.Target.IsAurora || s.Target.IsAwsRds
                ? r.IngestRdsDeadlocksAsync(s, ct)
                : r.RunAsync(PgDeadlocksCollector.Instance, s, ct),
        /* TWO TRANSPORTS, one table, the third time (#3601): the classified log-event pipeline reads the
           same server log by the same two roads as its two siblings above and below, and the classifier
           both roads feed is one instance, so the rows are the same whichever road the text took. */
        ["pg_log_events"] = (r, s, ct) =>
            s.Target.IsAurora || s.Target.IsAwsRds
                ? r.IngestRdsLogEventsAsync(s, ct)
                : r.RunAsync(PgLogEventsCollector.Instance, s, ct),
        ["pg_xmin_horizon"] = (r, s, ct) => r.RunAsync(PgXminHorizonCollector.Instance, s, ct),
        ["pg_replication_slots"] = (r, s, ct) => r.RunAsync(PgReplicationSlotsCollector.Instance, s, ct),
        ["pg_autovacuum_stats"] = (r, s, ct) => r.RunAsync(PgAutovacuumStatsCollector.Instance, s, ct),
        ["pg_io_stats"] = (r, s, ct) => r.RunAsync(PgIoStatsCollector.Instance, s, ct),
        ["pg_blocking"] = (r, s, ct) => r.RunAsync(PgBlockingCollector.Instance, s, ct),
        ["pg_database_stats"] = (r, s, ct) => r.RunAsync(PgDatabaseStatsCollector.Instance, s, ct),
        ["pg_index_usage_stats"] = (r, s, ct) => r.RunAsync(PgIndexUsageStatsCollector.Instance, s, ct),
        ["pg_table_bloat_stats"] = (r, s, ct) => r.RunAsync(PgTableBloatStatsCollector.Instance, s, ct),
        ["pg_session_states"] = (r, s, ct) => r.RunAsync(PgSessionStatesCollector.Instance, s, ct),
        ["pg_plan_capture_readiness"] = (r, s, ct) => r.RunAsync(PgPlanCaptureReadinessCollector.Instance, s, ct),
        ["pg_write_stats"] = (r, s, ct) => r.RunAsync(PgWriteStatsCollector.Instance, s, ct),
        ["pg_extension_availability"] = (r, s, ct) => r.RunAsync(PgExtensionAvailabilityCollector.Instance, s, ct),
        ["pg_lock_stats"] = (r, s, ct) => r.RunAsync(PgLockStatsCollector.Instance, s, ct),
        ["pg_wait_sampling"] = (r, s, ct) => r.RunAsync(PgWaitSamplingCollector.Instance, s, ct),
        ["pg_kernel_stats"] = (r, s, ct) => r.RunAsync(PgKernelStatsCollector.Instance, s, ct),
        ["pg_predicate_stats"] = (r, s, ct) => r.RunAsync(PgPredicateStatsCollector.Instance, s, ct),
        /* TWO TRANSPORTS, one table. Self-hosted reads the server log with pg_read_file; Aurora and RDS
           have no filesystem and pg_read_server_files is not grantable, so those go through the AWS log
           API instead (#2538). The collector's own AppliesTo excludes managed targets, so without this
           branch they would simply never capture a plan - and would look like they had nothing to say
           rather than like they were on a different road. */
        ["pg_plan_capture"] = (r, s, ct) =>
            s.Target.IsAurora || s.Target.IsAwsRds
                ? r.IngestRdsPlansAsync(s, ct)
                : r.RunAsync(PgPlanCaptureCollector.Instance, s, ct),
        ["pg_column_stats"] = (r, s, ct) => r.RunAsync(PgColumnStatsCollector.Instance, s, ct),
        ["pg_replication_stats"] = (r, s, ct) => r.RunAsync(PgReplicationStatsCollector.Instance, s, ct),
        ["pg_buffer_usage"] = (r, s, ct) => r.RunAsync(PgBufferUsageCollector.Instance, s, ct),
        ["pg_index_bloat"] = (r, s, ct) => r.RunAsync(PgIndexBloatCollector.Instance, s, ct),
        /* ONE TRANSPORT, unconditionally — unlike pg_deadlocks/pg_plan_capture above, there is no
           pg_read_file-shaped fallback for a self-hosted target, because PostgreSQL exposes no
           instance-level CPU signal at all (#2719, see PgCpuUtilizationCollector's doc comment). A
           self-hosted host resolves to nothing in IngestPgCpuAsync's RdsEndpoint.TryParse and the ingestor
           no-ops, the same "not this transport" answer RdsLogSource itself gives a non-RDS host. */
        ["pg_cpu_utilization"] = (r, s, ct) => r.IngestPgCpuAsync(s, ct),
        /* #3691 (V136): the per-database size series, an ordinary SQL collector over the shared catalog.
           (The rung's other series, host memory, has no entry here: it is six columns on the CPU row the
           pg_cpu_utilization entry above already writes from the same Performance Insights call.) */
        ["pg_database_size_stats"] = (r, s, ct) => r.RunAsync(PgDatabaseSizeStatsCollector.Instance, s, ct),
    };

    /// <summary>
    /// Signals that a blocking/deadlock XE session is missing or inaccessible so the reader returned no
    /// events. <see cref="RunXeTolerantAsync"/> throws it, <see cref="RunOneAsync"/> catches it and logs a
    /// distinct <c>SESSION_MISSING</c> collection_log status. Before Stage 4 this case swallowed to zero
    /// rows and logged SUCCESS — indistinguishable from a genuinely idle session, so the Capture Down
    /// self-alert had no signal to read.
    /// </summary>
    private sealed class DarlingXeSessionMissingException : Exception
    {
        public DarlingXeSessionMissingException(string message, Exception inner) : base(message, inner) { }

        /// <summary>
        /// #3754: the reconcile-side case, where the session could not be CREATED (rather than read) and the
        /// exception that said so was caught a sweep phase earlier in <c>ReconcileLongQueryTraceAsync</c>.
        /// Only its message survives to the run, on <see cref="ServerLoopState.LongQueryTraceFault"/>, so
        /// there is no inner exception to carry - and the arm that catches this reads the message alone.
        /// </summary>
        public DarlingXeSessionMissingException(string message) : base(message) { }
    }

    private static async Task<CollectorRunResult> RunXeTolerantAsync<TRow>(
        ICollectorDefinition<TRow> definition, DarlingCollectorRunner runner, ServerRuntime server, CancellationToken cancellationToken)
    {
        try
        {
            return await runner.RunAsync(definition, server, cancellationToken);
        }
        catch (SqlException ex) when (ex.Number == 297 || ex.Number == 15151 || ex.Message.Contains("XE session"))
        {
            /* XE session not found or not accessible. Lite swallows this to zero rows, but a headless
               service (like the Dashboard) must surface it: raise it as a distinct SESSION_MISSING
               collection_log status so the Stage 4 Capture Down self-alert can detect that blocking/
               deadlock capture is non-functional. RunOneAsync catches this and continues the sweep, so
               collection is still tolerant — only the logged status differs from a real zero-row success. */
            throw new DarlingXeSessionMissingException(ex.Message, ex);
        }
    }

    private static async Task<CollectorRunResult> RunTraceFlagsTolerantAsync(
        DarlingCollectorRunner runner, ServerRuntime server, CancellationToken cancellationToken)
    {
        try
        {
            return await runner.RunAsync(TraceFlagsCollector.Instance, server, cancellationToken);
        }
        catch (SqlException)
        {
            /* DBCC may be denied — degrade to zero rows, mirrors Lite's warning path. */
            return new CollectorRunResult(0, 0, 0, CollectorContext.NoMeasurements);
        }
    }
}
