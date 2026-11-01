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
using ServerClock = PerformanceMonitor.Analysis.Baselines.ServerClock;
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
/// trace_flags has no tolerance of its own (#5633 item 19: a denied DBCC is a PERMISSIONS run, never a 0-row SUCCESS). Every successful connect
/// upserts the servers registry and every collector run writes a collection_log row — both
/// failure-isolated (<see cref="DarlingObservability"/>). On top of collection the loop runs
/// the shared alert engine per server every 30 seconds and, since AN3, the analysis pipeline
/// (<see cref="DarlingAnalysisService"/>) per server every 30 minutes with findings routed
/// through the shared <see cref="AnalysisNotificationService"/>.
/// </summary>
public sealed partial class DarlingWorker : BackgroundService
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

    /// <summary>#5654: the external alert drain's cadence. A queued alert waits at most this long, plus its pass, for a send.</summary>
    private static readonly TimeSpan s_externalAlertInterval = TimeSpan.FromSeconds(15);

    /// <summary>#5632 Phase A: the investigation expiry sweep's tick (ruling R4).</summary>
    private static readonly TimeSpan s_investigationSweepInterval = TimeSpan.FromSeconds(60);

    /// <summary>#6001: the evidence capture's tick. Short, so a High CPU request's evidence is frozen soon after the open and a
    /// pending section is picked up soon after its collector lands.</summary>
    private static readonly TimeSpan s_investigationEvidenceInterval = TimeSpan.FromSeconds(20);

    /* The store disk-pressure self-alert's poll cadence (fleet-level, Stage 4). Disk fills slowly, and the
       check is one volume free-space read (a few file-system calls, no directory walk) plus one narrow
       store_metrics lookup, so 5 minutes is ample and cheap —
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

    /* #5654: how often the external-alert delivery check reads the count of rows the drain gave up on. The count looks back
       DarlingSelfAlertEvaluator.ExternalAlertsWindow (an hour), so a slower tick only delays the alert and its clear by
       minutes; the read is one grouped count over a table the retention job keeps short. */
    private static readonly TimeSpan s_externalAlertsCheckInterval = TimeSpan.FromMinutes(5);

    /// <summary>#3514: how often the sweep re-evaluates the web-dashboard TLS certificate's expiry. Hourly is
    /// ample for a day-granularity fact (the evaluator's own daily refire gates the actual firing); the check
    /// itself is a field read and a date compare.</summary>
    private static readonly TimeSpan s_webTlsCheckInterval = TimeSpan.FromHours(1);

    /// <summary>#4732: how often the sweep reads the fleet-gate counts, judges "Collection Falling Behind" and decides
    /// whether the hourly log line is due. The counts cover a sliding hour in one-minute buckets, so a minute is the
    /// finest cadence that can change the answer.</summary>
    private static readonly TimeSpan s_fleetGateCheckInterval = TimeSpan.FromMinutes(1);

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

    /// <summary>#4732: the longest the compression check's stamp is written ahead of now: <see cref="TimescaleSupport.NextCompressionCheckUtc"/>
    /// floors <c>now + s_compressionCheckInterval</c> to its minute and adds the phase, so it is at most the interval plus the phase.</summary>
    internal static readonly TimeSpan CompressionCheckSpan =
        s_compressionCheckInterval + TimeSpan.FromSeconds(TimescaleSupport.CompressionCheckPhaseSeconds);

    /// <summary>#4732: the longest the fleet sweep's stamp can sit ahead of now. It is written on a grid of the interval read
    /// at each fire (<see cref="FleetSweepEngine.ClampIntervalMinutes"/>), and the operator can change that between fires, so
    /// the span is the largest value the clamp allows, not the current one: a shorter span would wake the sweep early.</summary>
    internal static readonly TimeSpan FleetSweepStampSpan = TimeSpan.FromMinutes(FleetSweepCadence.IntervalMinutesCeiling);

    /// <summary>The daily retention purge's cadence: the stamp is written as now plus this, and it is the span its due check uses (#4732).</summary>
    private static readonly TimeSpan s_purgeInterval = TimeSpan.FromHours(24);

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

    /* The whole-attempt budget for the hourly retry of a failed start-path role provisioning. The attempt is
       three credential-file reads, one connection, a stored-secret read, a settings read and one DDL batch, each
       command carrying the bootstrap deadline (ServiceCommandDeadlines.BootstrapSeconds, 60 s), and that batch
       was measured at under a tenth of a second on a store of production size. Two minutes is two of those
       deadlines: a store that cannot answer inside it is not one this hour's attempt was going to heal, and the
       next hour tries again. It runs behind the compression read and every other tenant of the tick, so it
       spends none of the compression check's phase guard band. */
    private static readonly TimeSpan s_roleProvisioningRetryBudget = TimeSpan.FromMinutes(2);

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
        Func<NpgsqlConnection, ILogger, CancellationToken, Task<int>> EnsureAsync,
        Func<NpgsqlConnection, ILogger, CancellationToken, Task<int>>? HourlyEnsureAsync = null);

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
    /// <item><description><see cref="DarlingModuleMap"/>'s table ensure — a cheap DDL that the start path runs
    /// for readers. The map's refresh is a DATA upsert rather than a store object, and since #5578 it no longer
    /// runs on the start path at all: it has periodic homes that fire in the sweep loop's first pass, after the
    /// collectors start (the daily purge tick, and the hourly tick's own incremental tenant). A module_map table
    /// that failed to create is also the one item on this list whose absence is not silent: the table ensure
    /// warns when it fails, and the refreshes warn about it every run.</description></item>
    /// </list>
    ///
    /// <para><b>Every step here was read for idempotence rather than assumed idempotent</b>, and the verdicts
    /// are on the steps. The two that are worth an operator's attention: the compression ENABLE statements
    /// (<c>ALTER TABLE ... SET (timescaledb.compress ...)</c>, inside
    /// <see cref="TimescaleSupport.ApplyCompressionPolicyAsync"/> and
    /// <see cref="TimescaleSupport.EnsureCollectionLogHypertableAsync"/>) take an ACCESS EXCLUSIVE lock even
    /// when they change nothing, so since #3817 each is issued only for a table whose compression settings
    /// differ from what the product wants, and a converged store's pass issues none. collection_log's waits at
    /// most <see cref="TimescaleSupport.HourlyDdlLockTimeout"/> for its lock (#4951). The summary
    /// line carries an elapsed so a slow pass shows.</para>
    /// </summary>
    private static readonly StoreObjectConvergenceStep[] s_storeObjectConvergence =
    {
        /* if_not_exists => true on create_hypertable; migrate_data has nothing to move on a table that is
           already a hypertable. A catalog check per table on a converged store. */
        new("hypertable conversion", StoreObjectConvergenceStage.Timescale, StoreObjectChangeSignal.InPlace,
            (connection, logger, ct) => TimescaleSupport.ConvertToHypertablesAsync(connection, logger, ct)),

        /* The compression ENABLE runs only for a table whose settings differ (#3817, see the class remark);
           add_compression_policy's if_not_exists returns -1 for a policy that exists. */
        new("compression policies", StoreObjectConvergenceStage.Timescale, StoreObjectChangeSignal.InPlace,
            (connection, logger, ct) => TimescaleSupport.ApplyCompressionPolicyAsync(connection, logger, ct),
            (connection, logger, ct) => TimescaleSupport.ApplyCompressionPolicyAsync(connection, logger, hourly: true, ct)),

        /* collection_log is outside the collector catalog, so the two steps above never reach it; the same three
           idempotent statements, with its own segmentby and a bounded lock wait on its ALTER (#4951). */
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

        /* #6070: a daily whose parent is another daily keeps a window one bucket wider than the 3-day one, so the
           day its parent writes after it ran is still in its window the next run. config only (start_offset),
           no schedule, so the daily tier keeps finish-to-start. Before the ensure for the same 22023 reason as
           the window converge above. */
        new("daily child window converge", StoreObjectConvergenceStage.Timescale, StoreObjectChangeSignal.Delta,
            (connection, logger, ct) => TimescaleSupport.ConvergeDailyChildRefreshWindowAsync(connection, logger, ct)),

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
                [DarlingManagedPostgres.AdminRoleName, DarlingManagedPostgres.ViewerRoleName, DarlingManagedPostgres.WebRoleName, DarlingManagedPostgres.McpRoleName],
                logger,
                ct) == StoreStatementStats.SetupOutcome.Ready ? 1 : 0),

        /* #3573 and the composer's covering indexes: CREATE INDEX IF NOT EXISTS (a catalog check) and the
           per-table autovacuum overrides, plus the catalog-FILTERED hypertable insert-tuning sweep, which
           finds nothing once every hypertable carries the reloption. Wrapped whole rather than per-statement
           at its own call site today, which the issue names: one failure costs every index in the pass — so
           on this cadence the next hour retries it, which is the change. */
        new("composer performance tuning", StoreObjectConvergenceStage.Tuning, StoreObjectChangeSignal.InPlace,
            (connection, logger, ct) => PgTableTuning.ApplyAsync(connection, logger, ct),
            (connection, logger, ct) => PgTableTuning.ApplyAsync(connection, logger, hourly: true, ct)),

        /* #5571: the day partitions of the two Query Store interval tables. A table that is not promoted yet is
           converged first: arm the legacy CHECK when there is none, try ONE promote when it is valid, and re-arm it with
           a later S when the table is still not promoted and S is close, so the CHECK never refuses a current row
           while the table waits for its promotion (the VALIDATE itself runs on the background task, never here).
           Then create every missing day through today + 3 (the DEFAULT drain is the default) and drop whole expired
           days (and the legacy table once it is bounded below the cutoff). No ANALYZE here: the daily one is the
           background task's, because it samples the 91 GB legacy table too on PostgreSQL 16 and 17. Catalog-only, 5 s
           lock_timeout per DDL, no sleeping. Last in the list and in the Tuning segment, so on the start path it runs
           BEFORE the collectors and every hour after that, inside this pass's budget: one lock timeout costs an hour,
           not the 24 h purge's day. One table's failure never stops the other; a failure is thrown after both ran so
           the pass counts it. Counted as a delta: partitions created or dropped, and an arm, re-arm or promotion. */
        new("query store interval partitions", StoreObjectConvergenceStage.Tuning, StoreObjectChangeSignal.Delta,
            async (connection, logger, ct) =>
            {
                var pass = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, DateTime.UtcNow, logger, ct);
                if (pass.Failed > 0)
                {
                    throw new InvalidOperationException(
                        $"{pass.Failed} Query Store interval table(s) failed partition maintenance; each failure is logged above and the next pass retries.");
                }

                return pass.Changed;
            }),
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

    /* #4823: the checkpointer's cumulative sync time is read once a minute, so the hourly Store Checkpointer Pressure
       evaluation can judge the LONGEST single sync as well as the hour's average (a difference of two hourly rows is
       the hour's total, which an average over the hour's checkpoints spreads thin). One trivial single-row read; the
       budget bounds the whole sample, connection wait included, because it is awaited on the main loop. */
    private static readonly TimeSpan s_checkpointSyncSampleInterval = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan s_checkpointSyncSampleBudget = TimeSpan.FromSeconds(10);

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

    /* #4710: the gate a connect attempt runs in, instead of the fleet gate above. SqlClient takes about 15 s to
       fail for every kind of dead server, so an attempt inside the fleet gate held a collection slot for those
       15 s and about 22 down servers filled the default gate. The attempt reads only the monitored server, never
       the store, so this fixed-width gate does not spend the store's connection pool. Never scaled with the core
       count: the fleet gate's ceiling of 16 is tied to that pool. */
    private readonly SemaphoreSlim _connectProbeGate = new(ServerConnectProbe.GateWidth, ServerConnectProbe.GateWidth);

    /// <summary>Test seam: replaces the connect attempt. Null in production, which connects for real.</summary>
    internal Func<MonitoredServer, CancellationToken, Task<ServerRuntime>>? ConnectOverride { get; set; }

    /// <summary>Test seam: replaces the fresh-connection probe that settles a command timeout (#4710).</summary>
    internal Func<ServerRuntime, CancellationToken, Task<bool>>? LivenessProbeOverride { get; set; }

    /// <summary>
    /// Test seam (#4938): replaces what one collector run does once its permits are held, so a test can block a
    /// run and watch the scheduling around it with no store and no monitored server. It sits after every limit
    /// <see cref="RunOneAsync"/> applies, so a limit that is held while the replacement runs is observable. It is
    /// given the runtime the run is about to work on, which for a daily run is the one read again after its wait
    /// for a permit, so a test can see which connection a run got. Null in production, which runs the collector
    /// for real. A negative
    /// return stands for a run that failed (the real run records its failure and returns 0).
    /// </summary>
    internal Func<ServerLoopState, ServerRuntime, string, CancellationToken, Task<int>>? RunOneBodyOverride { get; set; }

    /// <summary>
    /// Test seam (#6186): with <see cref="RunOneBodyOverride"/> set, a non-null answer here stands for the monitored server
    /// refusing that collector (a PERMISSIONS run with this message). The override's run is recorded the way the real
    /// run's outcome is, so the backoff sees it.
    /// </summary>
    internal Func<string, string?>? RunOneRefusalOverride { get; set; }

    /// <summary>
    /// The fleet sweep gate (#5941), set once the collection loop builds it. A <c>snapshot_now</c> takes a permit from it
    /// like a scheduled pass does. Null before the loop starts (and in tests that do not stand one in): no permit is taken.
    /// </summary>
    internal SemaphoreSlim? SweepGate { get; set; }

    /// <summary>
    /// Test seam: awaited by <see cref="RecomputeNextDueAsync"/> right after it reads the persisted watermarks for a
    /// collector, before it writes that collector's schedule, so a test can land a run's record in that window (#5033).
    /// Null in production.
    /// </summary>
    internal Func<ServerLoopState, string, Task>? AfterRecomputeWatermarkReadForTest { get; set; }

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
    /// The memory launch guard's threshold rule (#1556): whether the fleet sweep may launch NEW collection bodies
    /// this pass. Once the process's memory figure crosses <see cref="MemoryGuardFraction"/> of the limit it is
    /// measured against this returns false, so the launch loop stops STARTING new bodies and lets the in-flight
    /// ones drain, and the process backs away from memory exhaustion instead of piling on more concurrent
    /// collectors. Purge/disk/analysis/delay keep running (the guard only gates NEW launches). Pure so a unit
    /// test pins the bands and the constant. The caller is <see cref="LaunchMemoryGuard"/>, which chooses the figure
    /// and the limit per platform, because what each platform kills on differs:
    /// <list type="bullet">
    /// <item>Windows: <c>Process.PrivateMemorySize64</c> (committed private bytes, not the GC heap; the metric that
    /// matched the 13GB commit-limit incident, and the commit charge is what the commit limit kills on) against
    /// <c>GC.GetGCMemoryInfo().TotalAvailableMemoryBytes</c>.</item>
    /// <item>Linux: the process's resident memory (<c>VmRSS</c>), not <c>PrivateMemorySize64</c>, which the runtime
    /// reads as <c>VmData</c> (every private writable mapping, resident or not; #5479 measured 1426MB against 810MB
    /// resident). The limit is the cgroup memory limit when one is set, else total RAM: the figure the kernel's
    /// out-of-memory kill compares, not the GC's 75% budget of it.</item>
    /// </list>
    /// A non-positive limit (an unknown budget) never blocks collection.
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

    /// <summary>#5307: one warning per unknown key under the network sections, naming its full path and never its value.</summary>
    internal static IReadOnlyList<string> GetUnknownNetworkKeyWarnings(DarlingConfig config) =>
        config.UnknownNetworkKeys
            .Select(path => $"darling.json: '{path}' is not a recognized setting and is ignored; check its spelling against darling.sample.json.")
            .ToList();

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
                    "mcp.network.* is set but postgres.managed is false. The MCP network endpoint is available only with the bundled store (postgres.managed = true) or the container distribution, so it is ignored and the MCP server stays on loopback.");
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

    /* #5916: the per-server and per-database threshold overrides the engine and the two PostgreSQL count checks resolve
       through. Loaded at start and on every control-plane reload (the table is on the config_version beacon); empty until then,
       which is the global threshold everywhere. */
    private DarlingThresholdOverrides _thresholdOverrides = new();
    /* #5366: the service's password key at run time, set once at start; null before it. */
    private DarlingPasswordKeyRuntime? _passwordKeyRuntime;

    /// <summary>#5479: the clock the fleet pass's overrun check reads; a test sets it to age a running pass without waiting.</summary>
    internal Func<DateTime> SelfAlertPassClock { get; set; } = static () => DateTime.UtcNow;

    private RepeatLogGate? _repeatLog;

    /// <summary>#7067: the gate for warnings a collector repeats every cycle, and the clock the connect-failure lines
    /// are timed on. A test steps <see cref="RepeatLogGate.Clock"/>. Created on first use: a test may build the worker
    /// without running its constructor.</summary>
    internal RepeatLogGate RepeatLog => LazyInitializer.EnsureInitialized(ref _repeatLog, static () => new RepeatLogGate());

    /// <summary>#5479: lets a live test give the fleet pass the production self-alert evaluator that ExecuteAsync builds.</summary>
    internal DarlingSelfAlertEvaluator? SelfAlertsForTests
    {
        set => _selfAlerts = value;
    }

    /// <summary>#5378: lets a live test give a worker the store ExecuteAsync would, so a run's row can be read back.</summary>
    internal NpgsqlDataSource? StoreForTests
    {
        set => _postgres = value;
    }

    /// <summary>
    /// #4961: this install's id, the eight characters that tell its Extended Events sessions from another install's on
    /// a server both monitor. Made (or read back) once at start, after the store's migrations and before any worker,
    /// so everything a worker starts sees it set; null only before that point.
    /// </summary>
    private string? _installId;

    /// <summary>The install id (#4961), for the code that names this install's sessions on a monitored server.</summary>
    internal string? CurrentInstallId => _installId;

    /// <summary>
    /// #4535: the plan analyzer's per-rule config (darling.json's optional "analyzer" section), set
    /// once at startup config load, same lifetime as <see cref="_capturePlans"/>-style file knobs.
    /// Null (section omitted) is <see cref="PerformanceMonitor.PlanAnalysis.AnalyzerConfig.Default"/>.
    /// </summary>
    private PerformanceMonitor.PlanAnalysis.AnalyzerConfig? _analyzerConfig;

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
    private volatile Task? _periodicHoleRepair;

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

    /* The managed store's data directory while the start-path role provisioning has failed and not yet succeeded;
       null when it succeeded or does not apply. Non-null makes the hourly store-maintenance tick retry it
       (RetryRoleProvisioningAsync), because provisioning otherwise runs once per start: a failed attempt left the
       roles, and the credential files the web and MCP hosts wait for, unset until the next restart. Written by
       the start path before the loop and then only by that tick, so it is never written concurrently. */
    private string? _roleProvisioningRetryDirectory;
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

    /* #5824: how many scheduled analysis passes are running right now inside a sweep body, so inside a fleet collection
       slot (ProcessServerSweepAsync). A body that waited long for a slot says how many of the slots were held by a pass
       when it got one. Interlocked. */
    private int _analysisPassesInSlot;

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

    /// <summary>The once-per-24-hours rule for the <c>collect.query_store_text</c> prune, shared by the scheduled passes (a pass the wall budget stopped brings the next one forward to within the hour).</summary>
    private readonly QueryTextPruneGate _queryTextPruneGate = new();

    /* MinValue = the first sweep after startup evaluates the store disk-pressure self-alert, then every
       s_diskCheckInterval. Fleet-level (one shared store), so it is a single field, not per-server. */
    private DateTime _nextDiskCheckUtc = DateTime.MinValue;

    /* MinValue = the first sweep after startup runs the custom-alert-rule health check (#3304), then every
       s_customAlertHealthInterval. Fleet-level (the rules are a fleet concept), so a single field. */
    private DateTime _nextCustomAlertHealthCheckUtc = DateTime.MinValue;

    /* MinValue = the first sweep after startup evaluates the stale-mute check (#3306), then every
       s_staleMuteCheckInterval. Fleet-level (mute rules are a store-wide concept), so a single field. */
    private DateTime _nextStaleMuteCheckUtc = DateTime.MinValue;

    /* MinValue = the first sweep after startup runs the external-alert delivery check (#5654), then every
       s_externalAlertsCheckInterval. Fleet-level, so a single field. */
    private DateTime _nextExternalAlertsCheckUtc = DateTime.MinValue;

    /* #3514: next due time for the web-dashboard TLS certificate expiry self-alert. Fleet-level (the web
       certificate is a store-wide concept), so a single field like the stale-mute cadence above. */
    private DateTime _nextWebTlsCheckUtc = DateTime.MinValue;

    /* #4732: the fleet collection gate's counts over the last hour (slots run, slots skipped, queue waits), the
       cadence the worker reads them on, and when it writes their log line. Nullable because a test that builds a
       worker without running its constructor never sets it, and the recording sites tolerate that. */
    private readonly FleetGateStats? _fleetGateStats;
    private readonly FleetGateLogCadence? _fleetGateLog = new();
    private DateTime _nextFleetGateCheckUtc = DateTime.MinValue;

    /* #4732: the instant before which a due collector slot is not counted as skipped. The sweep loop raises it when
       it comes back from a sleep, a stall, a clock step or a pause, and every per-server collection body reads it
       where it records its slot, so the count only covers slots that came due while the loop was running. */
    private readonly SkipCreditFloor _skipCreditFloor = new();

    /// <summary>Test hook (#4938): the schedule overrides every collector's schedule resolves from, which a test sets to
    /// give a collector a run time without a store.</summary>
    internal IReadOnlyList<ScheduleOverride> ScheduleOverridesForTest
    {
        get => _scheduleOverrides;
        set => _scheduleOverrides = value;
    }

    /// <summary>Test hook (#4938): the floor the pass reads to decide whether a skipped slot counts.</summary>
    internal SkipCreditFloor SkipCreditFloorForTest => _skipCreditFloor;

    /// <summary>Test hook (#4938): the fleet gate's slot counts.</summary>
    internal FleetGateStats? FleetGateStatsForTest => _fleetGateStats;

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

    /* #5985: the query-tag map step (QueryTagMapBuilder), on its own due stamp and tracked task like the oversized-plan
       sweep above. Fleet-level: the builder iterates the listed servers itself and keeps its extend cursors in memory.
       A pass still running when its next slot comes round makes that slot skip, so two passes never overlap. */
    private DateTime _nextQueryTagMapPassUtc = DateTime.MinValue;
    private Task? _queryTagMapPass;
    private QueryTagMapBuilder? _queryTagMapBuilder;
    private static readonly TimeSpan s_queryTagMapPassInterval = TimeSpan.FromMinutes(1);

    /* #5479: the in-flight per-server self-alert pass (Collection Stopped and the other store-polled self-alerts, and the
       custom-alert rules), fire-and-tracked from the sweep loop whether or not any collection body launches. One at a
       time: a pass still running at the next tick makes that tick skip, so a server is never evaluated by two threads. */
    private Task? _selfAlertPass;

    /* #5642: the in-flight engine pass (the shared alert engine's per-server reads and the pileup finding), fire-and-tracked
       from the same tick as the self-alert pass but in a single-flight slot of its own, chained after the tick's
       self-alert step. A slow alert read holds this slot, never the self-alert one, so Collection Stopped keeps its 30 s
       beat. A pass still running at the next tick makes that tick skip the engine pass. */
    private Task? _alertEnginePass;

    /* The engine pass's own overrun Warning latch and start time, like the self-alert pass's two fields below. Written and
       read on the sweep loop's thread only. */
    private bool _alertEnginePassOverranWarned;

    private DateTime _alertEnginePassStartedUtc;

    /* Set when the running pass has outrun the 30 s server cadence and a tick has said so, cleared when the next pass
       starts: one Warning per pass, not one per 15-second tick. */
    private bool _selfAlertPassOverranWarned;

    /* When the in-flight pass started, read by the skipping tick to tell a pass that is merely past one tick from one
       that has outrun the cadence. Written and read on the sweep loop's thread only. */
    private DateTime _selfAlertPassStartedUtc;

    /* #4130: the in-flight daily retention purge, fire-and-tracked like the per-server sweeps and the
       oversized-plan backlog above rather than awaited inline. Measured at 346-400s deleting ~839k rows;
       awaited on this loop, that is 346-400s in which NO server's sweep body launches and the whole fleet
       reads stale — the same shape as the oversized-plan backlog's field incident, one maintenance step
       over. Tracked (not fire-and-forget) so the launch loop can see it is still running and skip a second
       launch, and so shutdown can drain it instead of abandoning a live DELETE.

       #4825: purge_now runs in this SAME slot (TryStartPurgeNow), so a manual purge and the daily one can
       never overlap, and a manual purge is drained at shutdown like the daily one. The slot is now read and
       set from two threads (the launch loop for the daily purge, the command loop for purge_now), so the
       check-then-set in both launchers holds _purgeTaskLock. */
    private Task? _purgeTask;

    /// <summary>Test hook (#5592): whether the purge slot is held by a pass that has not completed.</summary>
    internal bool PurgeSlotBusyForTest { get { lock (_purgeTaskLock) { return _purgeTask is { IsCompleted: false }; } } }
    private readonly object _purgeTaskLock = new();

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

    /* #4970: the in-flight hourly store-maintenance tick. Only the launch loop writes it. */
    private Task? _storeMaintenanceTick;

    /* #4970: the in-flight hourly store self-metrics tick. Only the launch loop writes it. */
    private Task? _storeMetricsTick;

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

    /* #4823: the once-a-minute checkpointer sync-time sample. MinValue = the first sample is taken on the first pass,
       which only sets the sampler's baseline. The sampler is in memory: a service restart starts a new baseline. */
    private DateTime _nextCheckpointSyncSampleUtc = DateTime.MinValue;
    private readonly CheckpointSyncSampler _checkpointSyncSampler = new();

    /* #2674: per-collector cost on the monitored servers, accumulated in memory and flushed hourly on the
       store-metrics tick. Held here (not in the runner) so its lifetime matches the sweep that drains it. */
    private readonly CollectorCostAccumulator _collectorCost = new();

    /* #4004 review, round 3: "the log-hash key was replaced at start", held for the first pg_log_events run. */
    private readonly LogHashKeyRotationNote _logHashKeyRotation = new();

    /// <summary>#4004: lets a test arm the rotation note the way the service start does, then run the worker's real run path.</summary>
    internal LogHashKeyRotationNote LogHashKeyRotationForTests => _logHashKeyRotation;

    /// <summary>
    /// Test seam: with this set, <see cref="RunOneAsync"/> dispatches every collector through it instead of the collector's own
    /// run, and everything after the dispatch (the notes, the collection_log row) is the real path. Null in production.
    /// </summary>
    internal Func<string, ServerRuntime, CancellationToken, Task<CollectorRunResult>>? DispatchOverrideForTests { get; set; }

    /* The fleet-level memory launch guard (#1556, #5479): one per process, the whole process's memory rather than
       a per-server state, so it is a single worker field and not a ServerLoopState flag. It owns the trip, the
       garbage collection that releases it, the hold warnings and the release (see LaunchMemoryGuard). The setter
       is the seam a test uses to make the guard hold: hand it a guard whose sampler reads over the line. */
    private LaunchMemoryGuard _launchMemoryGuard;

    /// <summary>The memory launch guard the collection loop asks once per pass. A test replaces it, before the loop
    /// starts, with a guard built over a sampler that reads over the line, to make the guard hold with no real
    /// memory pressure.</summary>
    internal LaunchMemoryGuard LaunchGuard
    {
        get => _launchMemoryGuard;
        set => _launchMemoryGuard = value ?? throw new ArgumentNullException(nameof(value));
    }

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
    private volatile bool _timescaleAvailable;

    /* #4659: the one fence the collector runner's Query Store COPY and the alert read adapter's saved
       forced-plan failure answer share. */
    private readonly QueryStoreWriteFence _queryStoreWriteFence = new();

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
    private bool _storeStatementHistoryWarned;

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

    /* The collector runner, kept for the reconcile, which clears a changed server's RDS endpoint verdicts on it. */
    private DarlingCollectorRunner? _runner;

    /* #5518: the Query Store backfill, held so a removed server's cached database list can be dropped with it. */
    private QueryStoreBackfill? _queryStoreBackfill;

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

    /// <summary>#7082: Poison Wait subjects (server|metric|subject) whose current Warning episode already wrote its one held
    /// digest row. Cleared when the subject pages or drops under the Warning bar, so the next episode writes its own.</summary>
    private readonly ConcurrentDictionary<string, bool> _heldPgPoisonWaitEpisode = new(StringComparer.Ordinal);

    /// <summary>#7082: per server, when a held "capacity at the ceiling, low instance CPU" High CPU row was last written, so
    /// the digest gets at most one such row per cooldown per server.</summary>
    private readonly ConcurrentDictionary<string, DateTime> _lastPgCpuHeldRow = new(StringComparer.Ordinal);

    /// <summary>#7082: per server, the deadlock reports (identity to first-seen time) already counted into the signature
    /// store, so a report the window re-reads every sweep is counted once.</summary>
    private readonly ConcurrentDictionary<string, Dictionary<string, DateTime>> _pgDeadlockCounted = new(StringComparer.Ordinal);

    /// <summary>#7082: per server, the deadlock statement-pair keys that were new (not in the 30-day store) and have not yet
    /// been announced by a page. Held in memory until a page goes out, because the store already counts them.</summary>
    private readonly ConcurrentDictionary<string, HashSet<string>> _pgDeadlockUnannouncedNew = new(StringComparer.Ordinal);

    /// <summary>#6005: the deadlock rollup that is also the investigation agent gate. Set when the alert pipeline is built. The
    /// PostgreSQL deadlock count tells it which statement pairs are new, as the SQL Server rollup does, so the gate admits the
    /// investigation request of a page about a first-ever pair.</summary>
    private PerformanceMonitor.Darling.Service.Investigations.DarlingDeadlockRollup? _deadlockRollup;
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

    /* #4795: the failure streaks behind "a PostgreSQL alert whose every channel failed is tried again sooner
       than its cooldown", the twin of AlertEngine's own _failedSends (#4752) for the SQL Server families. The
       six PostgreSQL families stamp their cooldown BEFORE they deliver, so a send that reached nobody (an HTTP
       429 or 5xx, a timeout, an unreachable mail server) used to leave the alert silent for the whole
       cooldown. Keyed (metric name, the family's own cooldown key). In memory only: a restart starts every
       streak over, which is the safe direction, because the first retry after a restart is only ever earlier. */
    private readonly FailedSendBackoff _pgFailedSends = new();

    /// <summary>
    /// Held for the same reason <see cref="_alertDeliverer"/> is: the Postgres Deadlocks/Blocking alerts
    /// (#2711) need to write a resolution history row on the active→inactive transition, exactly like
    /// <see cref="BuildAlertEngine"/>'s <c>resolutionCallback</c> does for the SQL Server families. Since #5825 the
    /// resolution goes through <see cref="_alertDeliverer"/>'s resolution send, which writes the same row; this store
    /// is the fallback when no deliverer was built.
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

    /// <summary>#5288: the MCP endpoint's served TLS certificate, published by the MCP host and read in the same
    /// sweep as <see cref="_webTlsCertState"/>, so the "MCP TLS Certificate Expiring" self-alert fires without a
    /// restart. Null when the worker is built without it (a test harness, never the service): the MCP half of the
    /// sweep is then skipped, exactly as if the MCP host never published a certificate.</summary>
    private readonly McpTlsCertificateState? _mcpTlsCertState;

    /* #2298: the live monitored-server registry seam — published beside the two above, read by the MCP
       host's plan-fetch resolver so it never re-reads config_monitored_servers as the mcp role (whose
       encrypted_password SELECT-carve fails that whole read). */
    /// <summary>
    /// The targets the Azure master duplicate skip considers, from the live store-enabled set and identified
    /// by store id. A registry not yet published gives an empty list, so nothing is skipped and no event is lost.
    /// </summary>
    internal static IReadOnlyList<AlertTargetIdentity> LiveAlertTargets(IReadOnlyList<MonitoredServer>? live) =>
        live is null
            ? Array.Empty<AlertTargetIdentity>()
            : live.Select(s => new AlertTargetIdentity(
                s.ServerId.ToString(CultureInfo.InvariantCulture), s.Host, s.Database, Enabled: true, s.ReadOnlyIntent)).ToList();

    /// <summary>
    /// The databases an analysis pass for this runtime skips because they are monitored as their own targets:
    /// the same list the alert sweep uses, for an Azure SQL Database master target only (null otherwise).
    /// </summary>
    internal static IReadOnlyList<string>? AnalysisSeparatelyMonitoredDatabases(
        bool isAzureSqlDb, string selfServerId, string host, string? database, IReadOnlyList<MonitoredServer>? live)
    {
        if (!isAzureSqlDb) return null;
        var list = AzureMasterScope.SeparatelyMonitoredDatabases(isAzureSqlDb, selfServerId, host, database, LiveAlertTargets(live));
        return list.Count == 0 ? null : list;
    }

    /// <summary>
    /// The newest stored engine edition for a server, when the registry row agrees it is an Azure SQL Database.
    /// Two writers record an edition and they can disagree: the connect probe stamps <c>servers.sql_engine_edition</c>
    /// (<c>DarlingObservability.UpsertServerSql</c>, on every connect, so a re-pointed registration corrects it at
    /// once), and the <c>server_properties</c> collector appends <c>engine_edition</c> on its own schedule. This
    /// returns no row unless <c>servers.sql_engine_edition</c> is 5, so a pair that disagrees reads as not an
    /// Azure master, the same rule the fleet card and the Viewer apply (<c>ViewerDataService.IsAzureMasterSql</c>).
    /// </summary>
    internal const string StoredEngineEditionSql = @"
SELECT sp.engine_edition
FROM server_properties sp
JOIN servers s ON s.server_id = sp.server_id
WHERE sp.server_id = $1
  AND s.sql_engine_edition = 5
ORDER BY sp.collection_time DESC
LIMIT 1";

    /// <summary>
    /// The same list for a caller that has the live registry but no probed runtime (the MCP and web hosts). The
    /// server stands for an Azure SQL Database target when its newest stored <c>server_properties</c> row has
    /// <c>engine_edition</c> 5, the value the probe stored, so a private endpoint, a sovereign cloud or a DNS alias
    /// agrees with the worker path. No row, a NULL edition or any other edition (a managed instance is 8) gives
    /// null, as does an unknown server or a list with nothing to skip.
    /// </summary>
    internal static async Task<IReadOnlyList<string>?> AnalysisSeparatelyMonitoredDatabasesAsync(
        int serverId, MonitoredServerRegistryState.Snapshot? registry, NpgsqlDataSource postgres,
        CancellationToken cancellationToken)
    {
        if (registry is null || !registry.ById.TryGetValue(serverId, out var server)) return null;
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(StoredEngineEditionSql, connection)
        { CommandTimeout = McpCommandDeadlines.ReadSeconds };
        command.Parameters.AddWithValue(serverId);
        var edition = await command.ExecuteScalarAsync(cancellationToken);
        var isAzureSqlDb = edition is not null and not DBNull && Convert.ToInt32(edition, CultureInfo.InvariantCulture) == 5;
        return AnalysisSeparatelyMonitoredDatabases(
            isAzureSqlDb, serverId.ToString(CultureInfo.InvariantCulture), server.Host, server.Database, registry.Servers);
    }

    private IReadOnlyList<string>? AnalysisSeparatelyMonitoredDatabases(ServerRuntime? runtime) =>
        runtime is null ? null : AnalysisSeparatelyMonitoredDatabases(
            runtime.Target.IsAzureSqlDb, runtime.ServerId.ToString(CultureInfo.InvariantCulture),
            runtime.Config.Host, runtime.Config.Database, _registryState?.Read()?.Servers);

    private readonly MonitoredServerRegistryState _registryState;

    /* #5632 step 7: where the worker publishes its store owner data source for the force and unforce tools. Null in a test-built worker. */
    private readonly StoreOwnerSource? _ownerSource;
    /* #6066: the store's pseudonym key, read over the owner connection the first time a metadataOnly webhook token needs it. */
    private readonly PseudonymKeySource? _pseudonymKeys;
    /* #7041: where the running evaluator is published for send_daily_health_report_now. Null in a test-constructed worker. */
    private readonly DailyHealthReportHub? _dailyReportHub;

    /// <summary>
    /// #7041: the Daily Health Report builder the scheduled send and <c>send_daily_health_report_now</c> share. This is the one place the real
    /// builder is wired in; null means the report is never built (the schedule check returns at once and the tool answers not_available).
    /// </summary>
    private static PerformanceMonitor.Darling.Service.Reports.DailyHealthReportBuilder CreateDailyHealthReportSource(Npgsql.NpgsqlDataSource postgres) =>
        new PerformanceMonitor.Darling.Service.Reports.DailyHealthReportBuilder(postgres);

    /// <summary>
    /// True when another registration in the published registry carries this one's display name (ordinal), so
    /// its dedup key takes the store id. The population is the registry snapshot's enabled servers
    /// (<c>StoreConfigView.EnabledServers</c>, config-side enabled); the MCP filter reads the store's
    /// <c>servers WHERE is_enabled</c>, which <c>SyncServerEnabledStatesAsync</c> mirrors from the same flag on
    /// every reload, so the two agree once a reload has run. An unpublished registry shares nothing.
    /// </summary>
    internal static bool ServerNameIsShared(MonitoredServerRegistryState.Snapshot? registry, MonitoredServer config) =>
        registry is not null && registry.SharedDisplayNames.Contains(config.DisplayName);

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

    /// <summary>The shared baseline cache this host holds (#5950): a test compares it with the container's one instance.</summary>
    internal BaselineCache SharedBaselineCache => _baselineCache;

    /* #4442 scope 2: the same process-lifetime accumulator DarlingWebEndpoints.MapAll and RunComposedPanelAsync
       record into — the DI container hands this worker and the web host the SAME singleton instance, so a read
       recorded by either side is drained by this worker's hourly flush. */
    private readonly ReadLatencyAccumulator _readLatency;

    /* #5097: the slow-read record's queue; its one writer runs from the startup path once the store is migrated. */
    private readonly SlowReadLog? _slowReads;

    /* #5506 part 3: the read admission the MCP and web hosts share. The worker only builds its long-read table (at start
       and at each hourly flush); its own reads never go through admission. */
    private readonly ReadAdmission? _readAdmission;

    public DarlingWorker(ILogger<DarlingWorker> logger, ILoggerFactory loggerFactory, McpRuntimeState mcpState, WebRuntimeState webState, MonitoredServerRegistryState registryState, CollectorRuntimeState collectorState, WebTlsCertificateState webTlsCertState, BaselineCache baselineCache, ReadLatencyAccumulator readLatency, SlowReadLog? slowReads = null, McpTlsCertificateState? mcpTlsCertState = null, ReadAdmission? readAdmission = null, StoreOwnerSource? ownerSource = null, PseudonymKeySource? pseudonymKeys = null, DailyHealthReportHub? dailyReportHub = null)
    {
        _ownerSource = ownerSource;
        _pseudonymKeys = pseudonymKeys;
        _dailyReportHub = dailyReportHub;
        _slowReads = slowReads;
        _readAdmission = readAdmission;
        _mcpTlsCertState = mcpTlsCertState;
        _logger = logger;
        _thresholdOverrides = new DarlingThresholdOverrides(logger);
        _loggerFactory = loggerFactory;
        _mcpState = mcpState;
        _webState = webState;
        _registryState = registryState;
        _collectorState = collectorState;
        _webTlsCertState = webTlsCertState;
        _baselineCache = baselineCache;
        _readLatency = readLatency;
        _launchMemoryGuard = LaunchMemoryGuard.CreateDefault(logger);

        /* #5597: the gate's counts read the floor's monotonic uptime when each slot is recorded, so which slots the alert leaves out
           is decided then, not by the wall-clock minute they were stamped in. */
        _fleetGateStats = new FleetGateStats(static () => DateTime.UtcNow, () => _skipCreditFloor.Uptime);

        /* #5592: the retention drain's "is collection behind" read, built from the fleet gate's own counts. */
        _collectionPressure = new CollectionPressure(_fleetGateStats);
    }

    /* #5592: the retention drain's pressure read (see CollectionPressure). */
    private readonly CollectionPressure _collectionPressure;

    /// <summary>Test hook (#5592): the pressure read the retention drain takes.</summary>
    internal CollectionPressure CollectionPressureForTest => _collectionPressure;

    /// <summary>
    /// #4938: what one daily collector's run-time stamp was computed from, kept beside the stamp in
    /// <see cref="ServerLoopState.NextDue"/>. A reload that finds the same run time and the same clock leaves the stamp
    /// alone; a different run time or clock computes the slot again. <see cref="LastRunUtc"/> is when the collector last
    /// ran (the persisted last-run mark at connect, the hand-off time after that), the input the slot is computed from.
    /// </summary>
    internal sealed record RunTimeSlot(int RunAtMinute, int IntervalMinutes, string ClockId, DateTime? LastRunUtc);

    /// <summary>
    /// #4938: a server's wall clock as the worker holds it: the clock itself and an identity that changes when the server's
    /// own clock changes (its zone, or its fixed offset), so a slot is computed again only when the clock did change. The
    /// identity is the clock's zone id, which a fixed offset and UTC also have; a daylight-saving step inside one zone
    /// does not change it, because the zone already follows that step.
    /// </summary>
    internal sealed class ServerClockStamp
    {
        public ServerClockStamp(ServerClock clock)
        {
            ArgumentNullException.ThrowIfNull(clock);
            Clock = clock;
            Id = clock.AsTimeZone().Id;
            ToUtc = clock.ToUtc;
        }

        /// <summary>UTC: what a server has until its first <c>server_properties</c> or <c>pg_server_config</c> row is read.</summary>
        public static ServerClockStamp Utc { get; } = new(ServerClock.Utc);

        public ServerClock Clock { get; }

        public string Id { get; }

        /// <summary>The server-local to UTC conversion the run-time rules take.</summary>
        public Func<DateTime, DateTime> ToUtc { get; }
    }

    /// <summary>#4938: the run time and the clock a collector's slot is computed with.</summary>
    internal readonly record struct RunTimeRule(int RunAtMinute, int ServerId, Func<DateTime, DateTime> LocalToUtc);

    /// <summary>#4938: what the server pass does with a collector that has a run time.</summary>
    internal enum RunTimeAction
    {
        /// <summary>Its slot has not come: leave it.</summary>
        NotDue,

        /// <summary>Its slot is here, inside the grace: hand it off.</summary>
        HandOff,

        /// <summary>Its slot is behind it by more than the grace: skip that day, hand nothing off.</summary>
        SkipDay,
    }

    /// <summary>#4938: one decision of <see cref="DarlingWorker.StepRunTimeCollector"/>: the action, the stamp to write, and the
    /// skipped slots to record (1 for a day the sweep loop lost while it was running, else 0).</summary>
    internal readonly record struct RunTimeStep(RunTimeAction Action, DateTime NextDue, long Skipped);

    /// <summary>#4938: what a hand-off of a collector that has a run time carries to the run: the slot as it stands and the
    /// next stamp. The run records them, and counts the slot as served, only once it has taken its (server, collector) slot.</summary>
    internal sealed record RunTimeHandOff(RunTimeSlot Slot, DateTime NextDue);

    /// <summary>#4938: whether one collector run succeeded, for a caller that must know (the at-connect run of an on-load
    /// collector). A zero-row run is a success; a run that failed, or never started, is not.</summary>
    internal sealed class RunOutcome
    {
        public bool Succeeded { get; set; }
    }

    /// <summary>The failed-jobs list the body read from msdb at <paramref name="FetchedUtc"/> (#5642).</summary>
    internal sealed record MsdbCacheEntry(DateTime FetchedUtc, List<FailedJobInfo> Jobs);

    internal sealed class ServerLoopState
    {
        /* Settable so the reconcile can replace a still-connected server's definition on a config
           change (host/auth/excluded-dbs/cost) — paired with dropping Runtime to force a reconnect. */
        public required MonitoredServer Config { get; set; }
        public ServerRuntime? Runtime { get; set; }

        /* #5642: the runtime this server last connected with, kept when the connection drops (a liveness failure, a
           memory-pressure drop) so the fleet pass still evaluates its alerts from the store with the last known
           target: a server under enough pressure to drop is the case that needs the alert. Cleared when the server is
           removed, retired or its definition changes, because that target is no longer the server's. */
        public ServerRuntime? LastKnownRuntime { get; set; }

        /// <summary>The runtime the alert step evaluates with: the live one, else the last known one (#5642).</summary>
        public ServerRuntime? AlertRuntime => Runtime ?? LastKnownRuntime;

        /* #5642: the instant (naive UTC) the CURRENT connection began collecting. A restart and a reconnect each set a
           new value; a drop leaves the last one in place. Handed to the alert engine as AlertServerSnapshot.WatchingSinceUtc. */
        public DateTime? WatchingSinceUtc { get; set; }

        /* #5642: the msdb facts the alert engine reads, filled by the collection BODY (after its collectors) and only
           served by the fleet pass, which makes store reads and nothing else. The failed-jobs list (with the instant it
           was read), the lookback the engine last asked for, the Agent job step names resolved so far, and the step
           keys a card asked for that are not resolved yet. */
        public MsdbCacheEntry? FailedJobsCache { get; set; }
        public int FailedJobsLookbackWanted { get; set; }
        public ConcurrentDictionary<AgentJobStepKey, AgentJobStepNames> StepNamesCache { get; } = new();
        public ConcurrentDictionary<AgentJobStepKey, byte> StepNamesWanted { get; } = new();
        public DateTime NextMsdbRefresh { get; set; } = DateTime.MinValue;

        /* ConcurrentDictionary (#1553 D1): with the fire-and-track sweep the per-server body runs on a pool
           thread, so a reload's RecomputeNextDueAsync on the OUTER thread can touch this map concurrently with the
           body's RunDueCollectorsAsync read-and-advance. It is only ever INDEXED by the static collector-catalog
           keys (never enumerated), so a lock-free concurrent map is a drop-in and eliminates the one structure
           the old strict single-threaded invariant (INV-1) existed to protect from tearing. */
        public ConcurrentDictionary<string, DateTime> NextDue { get; } = new(StringComparer.OrdinalIgnoreCase);

        /* #6186: the collectors this server keeps refusing with the same answer, and how long each waits for its next try. In
           memory, so a restart (and a changed definition, which clears NextDue beside it) tries every collector once at its
           normal interval. */
        public RefusedCollectorBackoff RefusedBackoff { get; } = new();

        /* #4938: what a daily collector's run-time stamp in NextDue was computed from: the run time, the clock and the
           last run. One entry per collector that has a run time on this server, replaced whole (the record is
           immutable) so a reader never sees half of an update. Indexed by collector name only, like NextDue. */
        public ConcurrentDictionary<string, RunTimeSlot> RunTimeSlots { get; } = new(StringComparer.OrdinalIgnoreCase);

        /* #5033: held only around an in-memory read-modify-write of NextDue and RunTimeSlots together, so the run's
           record, a reload's recompute and the pass's seeds never leave the two maps describing different slots.
           Never held across an await (the C# compiler refuses an await inside a lock block). */
        public object ScheduleLock { get; } = new();

        /* #5479: the held-slot watermarks, one per collector, while the memory launch guard holds collection off. Read and
           written ONLY by the sweep loop's own thread (CountHeldSlots), so a plain map is enough. */
        public Dictionary<string, HeldSlotMark> HeldSlotMarks { get; } = new(StringComparer.OrdinalIgnoreCase);

        /* #5735: the slots this server has lost since its last collection_cycle row, fed by the three places that step over a
           slot and flushed by the body (DarlingCycleSkip). */
        public CycleSkipAccumulator CycleSkips { get; } = new();

        /* #5735: the last finished body's length and its slowest collector, and the time the current body queued for a fleet
           slot. Written by the body's own chain (bodies never overlap on one server), read by the next body when it words the
           cause of the slots it stepped over. */
        public long LastBodyMs;
        public string? LastBodySlowestName;
        public long LastBodySlowestMs;
        public long LastGateWaitMs;

        /* #5824: the last finished body's scheduled analysis pass length (0 when no pass ran in it), and how many analysis
           passes held fleet slots when the current body got its slot. LastBodyMs ends with the collectors, before the
           analysis pass, so without these a slot held by the pass read as "no single cause". */
        public long LastAnalysisMs;
        public int LastGateWaitAnalysisHolders;

        /// <summary>#5597: the UTC ticks at which the connect body finished seeding this server's collector due stamps (0 =
        /// never). The stamps are seeded from a clock read before the body's on-load snapshots, so a slot that came due
        /// while that body ran could not have run; the slot count starts at this instant
        /// (<see cref="SkipCreditFloor.Skipped(DateTime, DateTime, TimeSpan, DateTime)"/>).</summary>
        public long SeedFinishedTicks;

        private ServerClockStamp _clock = ServerClockStamp.Utc;

        /// <summary>#4938: this server's wall clock as last read from the store: UTC until a clock is read. Held per
        /// server so a run-time collector's slot is computed from memory, never from a query on a tick.</summary>
        public ServerClockStamp Clock
        {
            get => Volatile.Read(ref _clock);
            set => Volatile.Write(ref _clock, value);
        }

        public DateTime NextConnectAttempt { get; set; } = DateTime.MinValue;

        /* #4710: failed connect attempts in a row, so the retry delay grows (ServerConnectBackoff) while a
           server stays down instead of costing a full connect timeout every 60 seconds forever. Zero on a
           successful connect and on a definition edit. */
        public int ConsecutiveConnectFailures { get; set; }

        /* #2255: the last connect-failure message logged in FULL, so an unchanged cause repeats as one terse
           line instead of its whole explanation every 60 seconds forever. The field report is a DPAPI decrypt
           failure — permanent by construction, since the blob can never become decryptable on this host — and
           at Warning-with-full-text it buried the log while never once telling the operator anything new.
           Compared on the message rather than the exception type so a changed cause (credential fixed, server
           now genuinely unreachable) prints in full again. */
        public string? LastConnectFailureLogged { get; set; }

        /* #7067: the connect failures since the server last connected. The retry line used to repeat on every attempt
           (about every 5 minutes for a server that is switched off); now the "still failing" line is written at most once
           an hour and carries these figures, and the line logged on reconnect says how long the server was down. Null /
           zero when the server is connected. */
        public DateTime? ConnectDownSinceUtc { get; set; }

        public int ConnectAttemptsSinceDown { get; set; }

        /// <summary>When a connect-failure line (the first, a changed cause, or a "still failing" line) was last written.</summary>
        public DateTime LastConnectFailureLineUtc { get; set; }

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

        /* One of the TWO pieces of sweep bookkeeping the BODY writes (the other is ConnectStartedTicks, just
           below) — and the single reason each is a FIELD rather than a property: Interlocked needs a ref to a
           field. UTC ticks stamped by the body the moment it acquires the concurrency gate; 0 while it is still
           QUEUED behind that gate. Written once per episode by the body
           (Interlocked.Exchange) and read by the outer launch loop (Interlocked.Read) — a long is not guaranteed
           atomic on 32-bit, so BOTH sides go through Interlocked rather than assuming it. The existing three
           fields above keep their outer-thread-only invariant untouched; these are separate fields precisely so
           that invariant does not have to be weakened.

           Why it exists: the 60s watchdog is a HANG detector — "the field incident was HANGS, not throws" — but
           measuring from SweepStartedUtc alone counted GATE QUEUE TIME as hang time, so at 24 servers behind the
           N=4 gate it fired ~82 times/hour on a demonstrably healthy fleet (0 collector errors, data landing) and
           buried the very signal it exists to raise. Splitting run time out restores it: a body merely waiting
           its turn is reported as CAPACITY, never as a hang. */
        public long RunStartedTicks;

        /* #4710: UTC ticks stamped by the body just before its connect attempt and cleared, in a finally, when
           the attempt ends (a shutdown cancel included); 0 while the body is not in its connect stage. The
           attempt runs BEFORE the body asks for a fleet permit, so RunStartedTicks is still 0 for its whole
           length. Without this field the in-flight check would call a body inside a 15 s connect (or waiting
           for one of the connect gate's slots) "queued for a slot" and blame the fleet concurrency limit for a
           slow or unreachable server. Same discipline as RunStartedTicks: written by the body through
           Interlocked, read by the outer launch loop through Interlocked.Read, and reset at launch before the
           body starts. The launch loop reads it only for a body that is not running. */
        public long ConnectStartedTicks;

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
           refused databases are retried only by the hourly create pass (LongQueryTraceAppliedAtUtc) or the
           next (re)connect; the note is a standing claim about the sessions as they were last reconciled,
           which is also what it says.

           Both reset with the latch on every (re)connect, and both written only by the per-server body on
           the pool thread (INV-2), like the latch itself. */
        public string? LongQueryTraceFault
        {
            get => _longQueryTraceFault;
            set
            {
                _longQueryTraceFault = value;
                if (value is null)
                {
                    LongQueryTraceFaultIsPermission = false;
                }
            }
        }

        private string? _longQueryTraceFault;

        /* #5378: the kept fault above is a permission denial (the login lacks ALTER ANY EVENT SESSION, error 15247 and its
           kin), so the run records PERMISSIONS rather than SESSION_MISSING, and the reconcile does not repeat the create DDL
           on every sweep. Cleared with the fault. */
        public bool LongQueryTraceFaultIsPermission { get; set; }

        public string? LongQueryTracePartialNote { get; set; }

        /* The state key the last applied reconcile ran under (LongQueryTraceDatabases.StateKey). On Azure SQL
           Database the trace's databases depend on the #3477 scope, the exclusions and the databases monitored
           as their own servers, so a change to any of them re-runs the reconcile. Null on every other engine.
           Reset with the latch on every (re)connect. */
        public string? LongQueryTraceAppliedKey { get; set; }

        /* When the last reconcile that created the session ran. While the trace is on and latched, the create side
           runs again once this is LongQueryTraceDatabases.RetryInterval old, so a session dropped from outside
           comes back: the read returns no rows for an absent session, the same as for a quiet one. Reset with the
           latch on every (re)connect. */
        public DateTime? LongQueryTraceAppliedAtUtc { get; set; }

        /* Failed cleanup passes in a row, for LongQueryTraceDatabases.DropAttemptCap, and the clock for the hourly
           attempts after the cap. Reset on every (re)connect. */
        public LongQueryTraceDropRetry LongQueryTraceDropRetry { get; } = new();

        /* #4964: true once a pass that was creating the trace's session has logged its failure at Warning, until a pass
           succeeds or the server reconnects. The create side retries on every sweep, on purpose: each attempt records the
           fault again, so collection health reads SESSION_MISSING. What changes is the log level of the repeats, from Warning
           to Debug. A pass while the trace is off neither reads it nor sets it, because the drop side has its own cap
           (LongQueryTraceDropRetry). Reset on every (re)connect. */
        public bool LongQueryTraceCreateWarned { get; set; }

        /* #4961: when the always-on deadlock and blocked-process sessions were last ensured on this server: at connect, then
           once an hour (AlwaysOnXeSessions.EnsureInterval), so a session dropped from outside comes back within the hour. Null
           means due. Reset on every (re)connect, where the connect path ensures and stamps it again. */
        public DateTime? XeSessionsEnsuredAtUtc { get; set; }

        /* #4964: the collectors that have already logged their missing-session line at Warning on this server (the long-query,
           deadlock and blocked-process collectors raise it). Their runs fail on every sweep, on purpose: each one records
           SESSION_MISSING again, so collection health reads it. What changes is the level of the repeated line, from Warning to
           Debug, until a run of that collector succeeds. In memory, so a restart warns again. */
        public XeSessionMissingWarnings XeSessionMissingWarnings { get; } = new();

        /* #5640 (H2): the message of the last ensure of the client-timeout session that the login was refused (no ALTER ANY EVENT
           SESSION, or no CREATE ANY DATABASE EVENT SESSION on Azure SQL Database), or null. While it is set the attention run records
           PERMISSIONS with it instead of reading a session that cannot exist, which would record SESSION_MISSING and fire a Capture
           Down for a least-privilege posture. Replaced by every ensure (at connect, then hourly), so a grant clears it within the
           hour or at the next reconnect. Written on the pool thread only (INV-2), like the long-query fault. */
        public string? AttentionXePermissionFault { get; set; }

        /* #6893: what the last ensure left for the deadlock and blocked-process runs. A session whose create was refused for permission
           records PERMISSIONS, one that failed for any other reason records SESSION_MISSING, and neither read happens: the join to the
           session views returns no rows for an absent session, which a run records as a SUCCESS with zero rows, so collection health
           read HEALTHY and the Capture Down self-alert could not fire. Replaced by every ensure (at connect, then hourly), so the
           first one that succeeds clears it. On Azure SQL Database the fault is set only when the create failed in every database; a
           failure in some leaves a partial note on the run instead. Written on the pool thread only (INV-2). */
        public DarlingXeSessions.CaptureEnsureOutcome CaptureEnsure { get; set; } = new();

        /* #5640 (L5): whether the last ensure ran with the attention collector enabled. An ensure that skipped the client-timeout
           session because the collector was off leaves this false, so turning the collector on makes the next sweep ensure it at
           once and not after the hour. Reset on every (re)connect. */
        public bool AttentionXeEnsuredWhileEnabled { get; set; }

        /* #4961: the long-query latch and its hourly create clock, cleared when a collector run sees the instance's
           identity move (ForgetLongQueryTraceLatchOnRestart), so the next sweep runs the whole reconcile. Not the fault,
           the partial note or the retry count: the reconcile that follows replaces them. */
        internal void ForgetLongQueryTraceLatch()
        {
            LongQueryTraceApplied = null;
            LongQueryTraceAppliedKey = null;
            LongQueryTraceAppliedAtUtc = null;

            /* #4961: and the always-on sessions' clock, so the next sweep ensures them as well: an instance restart can leave
               them stopped or gone, and the hour is not waited out. */
            XeSessionsEnsuredAtUtc = null;
        }
    }

    /// <summary>
    /// The hourly ensure of the always-on deadlock and blocked-process sessions (#4961). The connect path ensures them and
    /// stamps <see cref="ServerLoopState.XeSessionsEnsuredAtUtc"/>; the sweep calls this, and the ensure runs again once the
    /// stamp is <see cref="AlwaysOnXeSessions.EnsureInterval"/> old. On-premises, Managed Instance and RDS that creates a
    /// missing server-scoped session and starts a stopped one, under the shared names as ever. On Azure SQL Database it also
    /// carries the per-database choice (shared or own session) and its switch back. A server that is not SQL Server has no
    /// Extended Events and is left alone.
    /// </summary>
    internal static async Task EnsureAlwaysOnXeSessionsAsync(
        ServerLoopState server, DarlingCollectorRunner runner, DateTime utcNow, ILogger logger, CancellationToken cancellationToken,
        bool attentionEnabled = true)
    {
        /* #4961: a retired server's ensure would create the sessions its removal has just dropped. */
        if (server.Runtime is null
            || server.Retired
            || server.Runtime.Target.Engine != CollectorTargetEngine.SqlServer)
        {
            return;
        }

        /* #5640 (L5): the client-timeout session follows its collector's switch. A collector that was just turned on does not wait
           out the hour for its session. */
        var attentionJustEnabled = attentionEnabled && !server.AttentionXeEnsuredWhileEnabled;
        if (!attentionJustEnabled && !AlwaysOnXeSessions.EnsureIsDue(server.XeSessionsEnsuredAtUtc, utcNow))
        {
            return;
        }

        /* Stamped before the ensure, so a server that refuses it is asked once an hour and not on every sweep. */
        server.XeSessionsEnsuredAtUtc = utcNow;
        server.AttentionXeEnsuredWhileEnabled = attentionEnabled;
        var captureOutcome = new DarlingXeSessions.CaptureEnsureOutcome();
        var attentionFault = await DarlingXeSessions.EnsureAllAsync(server.Runtime, runner, logger, cancellationToken, attentionEnabled, captureOutcome);
        server.AttentionXePermissionFault = attentionFault?.Message;
        server.CaptureEnsure = captureOutcome;
    }
}
