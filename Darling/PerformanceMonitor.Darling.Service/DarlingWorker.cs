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

    /* The command plane's poll cadence (Stage 2): a tighter 5-second tick, run on its OWN loop
       independent of the 15-second collection sweep and the 30-second alert sweep, so an operator
       command (pause, test_connect, snapshot_now, ...) is picked up within ~5s and a slow command
       (a test_connect against an unreachable host can block for the connect timeout) never starves
       collection — the two loops share a cancellation token and the guarded server set only. */
    private static readonly TimeSpan s_commandPollInterval = TimeSpan.FromSeconds(5);

    /* The store disk-pressure self-alert's poll cadence (fleet-level, Stage 4). Disk fills slowly, and the
       check is one pg_database_size + one DriveInfo syscall, so 5 minutes is ample and cheap — no need to
       run it on the 30-second alert sweep. */
    private static readonly TimeSpan s_diskCheckInterval = TimeSpan.FromMinutes(5);

    /* The compression-job self-heal check's cadence (fleet-level, #1581). Compression is a slow archival tier
       and a stuck policy job takes hours to matter, so hourly is ample and cheap (one job_stats read + at most
       one alter_job per stuck job) — no need for the 15s sweep or the 30s alert cadence. */
    private static readonly TimeSpan s_compressionCheckInterval = TimeSpan.FromHours(1);

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

    /* MinValue = the first sweep after startup evaluates the compression-job self-heal check (#1581), then
       every s_compressionCheckInterval. Fleet-level (one shared store), so it is a single field, not
       per-server; only consulted when _timescaleAvailable. */
    private DateTime _nextCompressionCheckUtc = DateTime.MinValue;

    /* MinValue = the first sweep after startup records the store self-metrics snapshot (#2068), then every
       s_storeMetricsInterval. Fleet-level (one shared store), so it is a single field, not per-server. NOT
       gated on _timescaleAvailable at the loop: the dimension and whole-store rows apply to plain-PG stores
       too; only the per-hypertable arm inside the sweep needs (and gets) the flag. */
    private DateTime _nextStoreMetricsUtc = DateTime.MinValue;

    /* #2674: per-collector cost on the monitored servers, accumulated in memory and flushed hourly on the
       store-metrics tick. Held here (not in the runner) so its lifetime matches the sweep that drains it. */
    private readonly CollectorCostAccumulator _collectorCost = new();

    /* Fleet-level working-set launch-guard latch (#1556): true once ShouldLaunchSweeps has tripped this
       episode, so its CRITICAL log is emitted ONCE rather than every sweep (the WarnedThisEpisode idiom —
       but fleet-wide: the guard is about the whole process's working set, so it is a single worker field,
       NOT a per-ServerLoopState flag). Cleared when the working set recovers below the threshold. */
    private bool _memoryGuardTrippedThisEpisode;

    /* Set once at startup by the TimescaleSupport detection (cached per data source — the
       extension can't appear or vanish under a running service without a restart anyway);
       branches the retention purge onto drop_chunks. */
    private bool _timescaleAvailable;

    /* Stage 4 service self-alerts (collection-stopped, connection lost/restored, capture-down). Built
       once in RunCollectionLoopAsync over the SAME deliverer/history the shared engine uses, so the
       self-alerts inherit its delivery/cooldown/restart-replay. Held as a field because the connection
       edge fires from TryConnectAsync and the reconcile drops per-server state through it. */
    private DarlingSelfAlertEvaluator? _selfAlerts;
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

    /* #2719: same LIVE-STATE shape as Long-Running Query above — CPU is a continuous gauge, so a cooldown
       timestamp and an active flag are enough; it does not need RollingCountAlertGate, which exists for
       rolling-WINDOW COUNTS (Deadlocks/Blocking) where the same event can sit in the window across several
       sweeps. Mirrors AlertEngine's own _activeCpuAlert/_lastCpuAlert shape for SQL Server's High CPU. */
    private readonly ConcurrentDictionary<string, DateTime> _lastPgCpuAlert = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _activePgCpuAlert = new(StringComparer.Ordinal);

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

    /* #2298: the live monitored-server registry seam — published beside the two above, read by the MCP
       host's plan-fetch resolver so it never re-reads config_monitored_servers as the mcp role (whose
       encrypted_password SELECT-carve fails that whole read). */
    private readonly MonitoredServerRegistryState _registryState;

    public DarlingWorker(ILogger<DarlingWorker> logger, ILoggerFactory loggerFactory, McpRuntimeState mcpState, WebRuntimeState webState, MonitoredServerRegistryState registryState)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
        _mcpState = mcpState;
        _webState = webState;
        _registryState = registryState;
    }

    private sealed class ServerLoopState
    {
        /* Settable so the reconcile can replace a still-connected server's definition on a config
           change (host/auth/excluded-dbs/cost) — paired with dropping Runtime to force a reconnect. */
        public required MonitoredServer Config { get; set; }
        public ServerRuntime? Runtime { get; set; }

        /* Set once per process after the PostgreSQL analysis-state row is written, so the explanation is
           recorded without rewriting the same row every analysis interval forever. */
        public bool PostgresAnalysisStateWritten { get; set; }

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
        try
        {
            configPath = DarlingConfig.ResolveConfigPath();
            config = DarlingConfig.Load();
            _logger.LogInformation("Loaded configuration from {Path}: {ServerCount} server(s)", configPath, config.Servers.Count);
        }
        catch (Exception ex)
        {
            _logger.LogCritical("Cannot load configuration: {Message}", ex.Message);
            return;
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

        /* Network-endpoint caller warnings (darling-network-endpoints, D-BYO / D7) — emitted AFTER
           Validate() passes and NEVER inside it (Validate is all-fatal; an optional-endpoint note must not
           abort startup). Covers BYO-mode network.* being ignored and the network.role=admin pivot risk. */
        foreach (var warning in GetNetworkStartupWarnings(config))
        {
            _logger.LogWarning("{Warning}", warning);
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

        if (config.Postgres.Managed)
        {
            if (!OperatingSystem.IsWindows())
            {
                _logger.LogCritical(
                    "postgres.managed = true requires Windows (the bundled runtime and the DPAPI-protected credential); " +
                    "set postgres.managed = false and point postgres.connectionString at your own PostgreSQL instead.");
                return;
            }

            managedPostgres = new DarlingManagedPostgres(config.Postgres, _logger);
            try
            {
                storeConnectionString = await managedPostgres.EnsureRunningAsync(stoppingToken);
                storeUpgradeReport = BuildStoreUpgradeReport(managedPostgres.LastUpgradeOutcome);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogCritical("Managed Postgres bootstrap failed: {Message}", ex.Message);
                return;
            }
        }

        try
        {
            await RunCollectionLoopAsync(config, storeConnectionString, storeUpgradeReport, stoppingToken);
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
    /// binary, and the documented install (extract the zip to <c>C:\PerformanceMonitorDarling</c>) inherits
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
            DarlingStoreUpgrade.StoreUpgradeStatus.Failed => new DarlingSelfAlertEvaluator.StoreUpgradeReport(
                false, outcome.FromMajor, outcome.ToMajor, outcome.FromTimescale, outcome.ToTimescale,
                outcome.FailedStep, outcome.Message, false),
            _ => null,
        };

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
        CancellationToken stoppingToken)
    {
        /* Carry the collect/config search path on the store connection string BEFORE the data
           source (and its pool) is created, so every pooled physical connection resolves the
           shared SQL's bare table names to the V8 schemas from its very first use — deterministic
           and independent of the pool's connection-open timing relative to PgMigrations'
           best-effort ALTER DATABASE ... SET search_path. Without this a FRESH bring-your-own
           store silently collects nothing until the service is restarted; see
           EnsureStoreSearchPath for the pool-timing root cause. Managed mode already sets it, so
           this is a no-op there. */
        storeConnectionString = EnsureStoreSearchPath(storeConnectionString);
        await using var postgres = NpgsqlDataSource.Create(storeConnectionString);
        _postgres = postgres;
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
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogCritical("Cannot reach or migrate the Postgres store: {Message}", ex.Message);
            return;
        }

        /* Least-privilege role provisioning (V8 security hardening), managed mode only: create /
           refresh the admin + viewer login roles and their per-role DPAPI credentials, and grant the
           collect/config privileges — idempotent and self-healing, the conf-append discipline applied
           to roles. Windows-only (DPAPI credential files); a failure degrades (the Viewer cannot
           connect as admin/viewer until a later start succeeds) but never kills collection, which
           connects as the owner. BYO stores provision roles out-of-band via tools/provision-roles.sql. */
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

        try
        {
            await using var timescaleConnection = await postgres.OpenConnectionAsync(stoppingToken);
            _timescaleAvailable = await TimescaleSupport.TryEnableAsync(timescaleConnection, _logger, stoppingToken);
            if (_timescaleAvailable)
            {
                await TimescaleSupport.ConvertToHypertablesAsync(timescaleConnection, _logger, stoppingToken);
                await TimescaleSupport.ApplyCompressionPolicyAsync(timescaleConnection, _logger, stoppingToken);
                await TimescaleSupport.EnsureCollectionLogHypertableAsync(timescaleConnection, _logger, stoppingToken);

                /* #1778: the two calls above carry the compression tick on the CREATE, but if_not_exists makes
                   that a no-op against a policy this store already has — so a store that ever ran an older
                   build keeps TimescaleDB's 12-hour default forever and its newest closed chunk stays
                   uncompressed for up to half a day. This retunes the existing policies. AFTER both, so it
                   covers collection_log (outside the collector catalog) in the same pass; a no-op on every
                   start after the first, since it only selects policies whose interval differs. */
                await TimescaleSupport.ConvergeCompressionScheduleAsync(timescaleConnection, _logger, stoppingToken);
                // Reshape: drop stale old-shape QS / procedure_stats CAGGs FIRST so the ensure below rebuilds them
                // in the composer-dimension shape (no-op once reshaped, and on a fresh store nothing matches).
                await TimescaleSupport.DropStaleContinuousAggregatesAsync(timescaleConnection, _logger, stoppingToken);
                await TimescaleSupport.EnsureContinuousAggregatesAsync(timescaleConnection, _logger, stoppingToken);
                // AFTER the CAGGs exist: the tiered retention (raw 4d, hourly HISTORY CAGGs 90d per #1937, daily
                // history kept indefinitely; the interval-dedup and baseline tiers carry their own, #1958).
                await TimescaleSupport.EnsureRetentionPoliciesAsync(timescaleConnection, _logger, stoppingToken);

                /* #1757: the baseline aggregates ship WITH NO DATA and their refresh policy only ever covers
                   the trailing 3 days, so without this they would answer a 30-day question with 3 days of
                   supply. DELIBERATELY LAUNCHED, NOT AWAITED: it is a bulk materialization whose cost scales
                   with however much history the store already had, and every step below this block — the
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
            await TimescaleSupport.DropRetiredBaselineAggregatesAsync(fallbackConnection, _logger, stoppingToken);
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(fallbackConnection, _logger, stoppingToken);
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
            await PgTableTuning.ApplyAsync(tuningConnection, _logger, stoppingToken);
            // The retained sql_handle->module map (#1568 object_name for OLD query_stats windows the CAGG serves,
            // after procedure_stats raw drops at 4d): create it, then seed it from recent procedure_stats.
            if (await DarlingModuleMap.EnsureTableAsync(tuningConnection, _logger, stoppingToken))
            {
                await DarlingModuleMap.RefreshAsync(tuningConnection, _logger, stoppingToken);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("Composer performance tuning failed — queries fall back to un-indexed scans: {Message}", ex.Message);
        }

        /* Restart continuity: re-seed delta baselines from the store (the Postgres twin of Lite's
           DuckDB seeding) so the first cycle after a service restart produces real deltas instead
           of zeroes. A seed failure logs a warning and collection proceeds with first-cycle-zero. */
        var deltas = new DarlingDeltaCalculator();
        await deltas.SeedFromStoreAsync(postgres, _logger, stoppingToken);

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
        var runner = new DarlingCollectorRunner(postgres, deltas, _logger, () => config.CapturePlans, () => config.CollectSchemaChangeEvents,
            () => StoreConfigProvider.ClampTextBudgetMb(config.QueryStoreTextBudgetMb),
            /* #2171: live provider like its siblings — a store reload flipping plan_xml_compression
               takes effect on the next write batch, no restart. */
            compressPlanContent: () => !string.Equals(config.PlanXmlCompression, "none", StringComparison.OrdinalIgnoreCase),
            /* #2862: the procedure_stats plan-capture cadence, clamped at the provider like the V59 budget
               above so the runner never sees an out-of-range interval. A file-only knob today, but read
               live like its siblings, so setting it to 1 restores every-cycle plan capture and promoting
               it to a store column later needs no change here. */
            procedureStatsPlanCycleInterval: () => StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(config.ProcedureStatsPlanCycleInterval));
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
            new PgMuteRuleStore(postgres, _logger), _loggerFactory.CreateLogger<MuteRuleService>());
        await muteRuleService.LoadAsync();

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
            storeJobCadenceWarnPercent: () => alertSettings.StoreJobCadenceWarnPercent);

        /* #1706: report this start's store runtime upgrade, now that there IS an alert engine to report it
           through. Fired once, here, and never re-evaluated — the store is down while an upgrade runs, so
           its start could only ever be a log line, and by the time this line is reached both terminal states
           (upgraded, or reverted and still running) have a live store to alert from. */
        if (storeUpgradeReport is not null)
        {
            await _selfAlerts.EvaluateStoreUpgradeAsync(storeUpgradeReport, stoppingToken);
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
        var notificationService = new AnalysisNotificationService(
            new DarlingFindingAlertSender(alertSettings, historyStore, webhookAlertService, _logger),
            alertSettings,
            finding => finding.ServerId.ToString(CultureInfo.InvariantCulture),
            _loggerFactory.CreateLogger<AnalysisNotificationService>());

        /* #2138 phase 1: the auto force-plan bot, hooked onto the SCHEDULED analysis pass only (the
           interactive analyze_now command deliberately does not trigger it — an operator poking a
           server should not spend the bot's blast-radius budget). Settings are file-level and OFF by
           default. The bot is constructed with the JOURNAL and nothing else — no executor, no
           connection factory — because phase 1 has no write path at all; the store it writes to is
           the monitoring store, never a monitored server. */
        _planForceBot = new PlanForceBot(
            new PgPlanForceActionStore(postgres),
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
        var queryStoreBackfill = new QueryStoreBackfill(postgres, runner, deltas, _logger, () => config.CapturePlans,
            () => StoreConfigProvider.ClampTextBudgetMb(config.QueryStoreTextBudgetMb));
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
                _lastConfigVersion = configVersion.Value;
                await ReloadFromStoreAsync(configProvider, config, servers, muteRuleService, stoppingToken);

                /* #2170: the reload swapped the knob into the live config; move the gate to match. Safe here
                   by construction — top of the sweep, and narrowing never preempts a running body. */
                ReconcileSweepGate(serverSweepGate, StoreConfigProvider.ClampConcurrentSweeps(config.MaxConcurrentSweeps), stoppingToken);
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

            if (DateTime.UtcNow >= _nextPurgeUtc)
            {
                _nextPurgeUtc = DateTime.UtcNow.AddHours(24);
                /* Honor fleet-wide retention overrides (config_collector_schedules, server_id NULL) layered
                   on CollectorScheduleDefaults; a per-server override can't apply to a shared-table purge.
                   Empty overrides (Stage 1 seeds none) resolve to the defaults — identical behavior. */
                var overrides = _scheduleOverrides;
                await DarlingRetention.PurgeAsync(
                    postgres, _timescaleAvailable, _logger, stoppingToken,
                    name => StoreConfigProvider.ResolveFleetRetentionDays(name, overrides),
                    config.PlanContentRetentionDays);

                /* AN3: findings retention. Both apps' finding stores declare a 30-day cleanup
                   but neither app schedules it (Lite's DuckDB archive-reset bounds it
                   incidentally); a 24/7 service must actually invoke it or analysis_findings
                   grows unbounded. Rides the daily purge; never throws (logs + degrades). */
                await new PgFindingStore(postgres, _logger).CleanupOldFindingsAsync(retentionDays: 30);

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

            /* #1581: the compression-job self-heal backstop. TimescaleDB compression policy jobs can silently
               die (next_start = -infinity) or hang, halting the store's archival tier so uncompressed data grows
               without bound until the disk fills and collection stops for the WHOLE fleet (the field incident).
               Timescale-only; own hourly cadence; failure-isolated inside EvaluateCompressionJobHealthAsync. */
            if (_timescaleAvailable && DateTime.UtcNow >= _nextCompressionCheckUtc)
            {
                _nextCompressionCheckUtc = DateTime.UtcNow.Add(s_compressionCheckInterval);
                await EvaluateCompressionJobHealthAsync(stoppingToken);
            }

            /* #2068: the store self-metrics sweep. Capacity forecasting previously required ad-hoc
               archaeology over the TimescaleDB chunk catalog, whose raw window is 4 days — a measured 3x
               daily-ingest jump was only visible because the catalog still held both eras. This persists
               the store's own size/compression/growth series (per-hypertable, per-dimension, whole-store)
               into the plain collect.store_metrics table hourly, retention bounded by the sweep's own
               DELETE. Every store shape (the hypertable arm gates on _timescaleAvailable INSIDE);
               failure-isolated inside SweepStoreSelfMetricsAsync like the two checks above. */
            if (DateTime.UtcNow >= _nextStoreMetricsUtc)
            {
                _nextStoreMetricsUtc = DateTime.UtcNow.Add(s_storeMetricsInterval);
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

        /* Drain the concurrent command loop on shutdown (it observes the same token). */
        try
        {
            await commandLoop;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

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

            /* AN3: the scheduled analysis pipeline, per-server. The cadence, the enabled gate, and the notify
               gate are now control-plane knobs read LIVE from config.Analysis (a reload takes effect on the next
               tick). When analysis is disabled the pass is skipped and NextAnalysisDue is left in the past, so
               re-enabling runs immediately. The next-due stamp advances up front (Lite's scheduler shape), so a
               timed-out pass is skipped, not retried immediately. Delivery is gated on
               analysis_notifications_enabled (Lite's D0 split: production unconditional, delivery gated) —
               replacing the old alerts.enabled gate; the interval is clamped to Lite's 5-360 range. */
            if (config.Analysis.Enabled && DateTime.UtcNow >= server.NextAnalysisDue)
            {
                var intervalMinutes = Math.Clamp(config.Analysis.IntervalMinutes, MinAnalysisIntervalMinutes, MaxAnalysisIntervalMinutes);
                server.NextAnalysisDue = DateTime.UtcNow.AddMinutes(intervalMinutes);

                /* The analysis pipeline is SQL-Server-shaped: its facts come from wait_stats, query_stats,
                   cpu_utilization_stats and friends, none of which a PostgreSQL target ever writes. Running it
                   anyway is not harmless. RunAnalysisPassAsync takes a serverId and a storage name — not the
                   target — so it cannot gate itself, and it would read those tables, find nothing, hit the
                   24-hour data-span gate and persist insufficient_data = true. FOREVER: those tables will
                   never have rows for a PostgreSQL server_id, so the Recommendations tab would say "still
                   collecting" for the life of the deployment, which is the one thing analysis_state exists to
                   distinguish from a genuine all-clear. Plus a fresh DarlingAnalysisService and up to a
                   120-second pass per target per interval, producing nothing.

                   So: skip the pass, and say why ONCE rather than leaving the tab silent. The message is the
                   honest state — not "still collecting", which is a lie about a young deployment. */
                if (server.Runtime?.Target.Engine == CollectorTargetEngine.PostgreSql)
                {
                    if (!server.PostgresAnalysisStateWritten)
                    {
                        server.PostgresAnalysisStateWritten = true;
                        await DarlingObservability.WriteAnalysisStateAsync(
                            _postgres!,
                            server.Runtime.ServerId,
                            insufficientData: true,
                            message: PostgresAnalysisNotApplicable,
                            _logger,
                            stoppingToken);
                    }
                }
                else
                {
                    await RunScheduledAnalysisAsync(
                        server, planFetcher, notificationService, config.Analysis.NotificationsEnabled, stoppingToken);
                }
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
            await DarlingXeSessions.ReconcileLongQueryCompletionsAsync(server.Runtime, runner, enabled, _logger, cancellationToken);
            server.LongQueryTraceApplied = enabled;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("[{Server}] Failed to reconcile the long-query completion XE session: {Message}",
                server.Config.DisplayName, ex.Message);
        }
    }

    /// <summary>
    /// Materializes the baseline aggregates over the history the store already had (#1757), concurrently with
    /// the collection sweep rather than ahead of it. On a fresh store the coverage gate makes this a no-op; on
    /// an upgraded store it is the one-time pass that turns a 3-day supply into the full baseline window.
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
                /* #2165: the other half of the gate. Held for the WHOLE slice, and taken outside the
                   AbandonableStep so an abandoned-but-still-wedged slice keeps the gate closed — the tick must
                   keep yielding while that statement is genuinely still running on the server, which is exactly
                   the case the abandonment leaves behind. Zero-wait, so a tick already collecting simply defers
                   this server's slice to the next five-minute cycle. */
                var gate = _queryStoreGates.GetOrAdd(runtime.ServerId, static _ => new QueryStoreServerGate()).TryAcquire();
                if (gate is null)
                {
                    _logger.LogInformation(
                        "query_store backfill slice on '{Server}' deferred — the tick's Query Store collection is running (#2165)",
                        runtime.Config.DisplayName);
                    continue;
                }

                using var backfillGate = gate;

                var step = _backfillSliceSteps.GetOrAdd(runtime.ServerId, static _ => new AbandonableStep());
                var result = await step.RunAsync(
                    () => backfill.RunServerSliceAsync(runtime, stoppingToken),
                    BackfillSliceDeadline,
                    onLateFault: ex => _logger.LogError(ex,
                        "query_store backfill slice on '{Server}' faulted AFTER being abandoned — this is the wedge's own exception (#2148)",
                        runtime.Config.DisplayName),
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
    /// </summary>
    private async Task ReloadFromStoreAsync(
        StoreConfigProvider provider, DarlingConfig config, List<ServerLoopState> servers,
        MuteRuleService muteRuleService, CancellationToken cancellationToken)
    {
        var view = await provider.LoadViewAsync(config, cancellationToken);
        if (view is null)
        {
            return;
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
           this is a catalog write. Gated exactly as startup provisioning is (managed + Windows), because
           that is where these roles are known to exist — a BYO store provisions them out-of-band through
           tools/provision-roles.sql and names them itself, so ALTER ROLE viewer here would be guessing.
           The baseline advances only on SUCCESS, so a failed attempt retries on the next reload rather
           than being recorded as applied. */
        if (_postgres is not null
            && DarlingManagedRoles.ShouldReassertComposeStatementTimeout(
                view.ComposeStatementTimeoutSeconds, _appliedComposeStatementTimeoutSeconds,
                config.Postgres.Managed, OperatingSystem.IsWindows()))
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

        await muteRuleService.LoadAsync();

        _logger.LogInformation(
            "Control-plane reload applied (config_version {Version}, {Servers} monitored server(s), paused: {Paused})",
            _lastConfigVersion, servers.Count, _paused);
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
                    "[{Server}] Definition changed — reconnecting with the new configuration", desiredServer.DisplayName);
                state.Runtime = null;
                state.NextConnectAttempt = DateTime.MinValue;
                state.NextDue.Clear();
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
    /// reload: a disabled or on-load-only (freq 0) collector is dropped from the schedule; a newly-enabled one is
    /// seeded from its persisted watermark (the #1575 <see cref="ComputeSeededNextDue"/> policy, one lazily
    /// batched read per server that gains a new entry); an existing entry is pulled in to at most now + the
    /// (possibly shortened) effective interval so a frequency change takes effect promptly without over-firing. A
    /// server still connecting has no NextDue yet — <see cref="TryConnectAsync"/> seeds it from the same watermark
    /// policy when it connects.
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
                if (!effective.Enabled || effective.FrequencyMinutes == 0)
                {
                    /* ConcurrentDictionary has no Remove(key) — TryRemove is the drop-in for the old Remove. */
                    server.NextDue.TryRemove(name, out _);
                    continue;
                }

                if (server.NextDue.TryGetValue(name, out var existing))
                {
                    /* Existing entry KEEPS its already-applied phase, but is pulled in to at most now + the
                       (possibly shortened) interval so a frequency change takes effect promptly without
                       over-firing — unchanged from before. */
                    var capped = now.AddMinutes(effective.FrequencyMinutes);
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
                    var jitter = SeedJitter(runtime.ServerId, effective.FrequencyMinutes * 60);
                    server.NextDue[name] = ComputeSeededNextDue(lastRun, effective.FrequencyMinutes, now, jitter);
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
            /* DarlingManagedPostgres is annotated [SupportedOSPlatform("windows")] for its DPAPI /
               bundled-cluster surface, but SearchPath is a platform-neutral compile-time constant
               with no runtime dependency, and BYO mode runs on any OS — so the CA1416 cross-platform
               reachability flag is spurious for this const reference. Suppressed narrowly rather than
               forking the constant, keeping managed and BYO byte-identical (a test pins them equal). */
#pragma warning disable CA1416
            builder.SearchPath = DarlingManagedPostgres.SearchPath;
#pragma warning restore CA1416
            return builder.ConnectionString;
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
                serverId => StoreConfigProvider.ResolveSchedule("dmv_blocking_snapshot", serverId, _scheduleOverrides).FrequencyMinutes),
            stateStore,
            deliverer,
            muteRuleService.IsAlertMuted,
            failedJobsFetcher: (serverKey, lookbackMinutes, ct) =>
                FetchFailedJobsAsync(servers, serverKey, lookbackMinutes, ct),
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
            logger: _logger);
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

        try
        {
            var (sqlCpu, totalCpu) = await ReadLatestCpuAsync(runtime.ServerId, cancellationToken);
            var snapshot = new AlertServerSnapshot(
                runtime.ServerId.ToString(CultureInfo.InvariantCulture),
                runtime.Config.DisplayName,
                IsOnline: true,
                SqlCpuPercent: sqlCpu,
                TotalCpuPercent: totalCpu,
                IsAzureSqlDb: runtime.Target.IsAzureSqlDb,
                Suppressed: false);

            await engine.EvaluateServerAsync(snapshot, cancellationToken);

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
            _logger.LogError("[{Server}] Alert sweep failed: {Message}", server.Config.DisplayName, ex.Message);
        }
    }

    /// <summary>
    /// Evaluates the three PostgreSQL Tier 0 outage predictors and delivers whatever fired.
    /// <para>Failure-isolated from the shared sweep on purpose: these are additive signals, and a broken
    /// PostgreSQL read must not cost a server its CPU or blocking alerts. Recording and mute handling stay
    /// with the deliverer, exactly as for an engine-emitted alert, so a PostgreSQL alert lands in the same
    /// history and obeys the same mute rules as every other one.</para>
    /// </summary>
    private async Task EvaluatePostgresAlertsAsync(
        ServerRuntime runtime, AlertServerSnapshot snapshot, DarlingConfig config, CancellationToken cancellationToken)
    {
        if (_postgres is null || _alertDeliverer is null)
        {
            return;
        }

        try
        {
            var adapter = new DarlingPostgresAlertReadAdapter(_postgres);

            var findings = PostgresAlertEvaluator.Evaluate(
                await adapter.GetWraparoundRiskAsync(runtime.ServerId, cancellationToken),
                await adapter.GetXminHorizonAsync(runtime.ServerId, cancellationToken),
                await adapter.GetReplicationSlotRiskAsync(runtime.ServerId, cancellationToken));

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
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Server}] PostgreSQL alert evaluation failed: {Message}",
                runtime.Config.DisplayName, ex.Message);
        }

        /* #2711/#2719: Deadlocks, Blocking, Long-Running Query, Poison Wait and High CPU, each
           independently failure-isolated (own try/catch inside), so a broken read on one never costs the
           three predictors above or any sibling — the same isolation AlertEngine gives its own
           CheckDeadlocksAsync/CheckBlockingAsync/CheckLongRunningQueriesAsync/CheckPoisonWaitsAsync/
           CheckCpuAsync. */
        await EvaluatePgDeadlocksAsync(runtime, snapshot, cancellationToken);
        await EvaluatePgBlockingAsync(runtime, snapshot, cancellationToken);
        await EvaluatePgLongRunningQueryAsync(runtime, snapshot, config, cancellationToken);
        await EvaluatePgPoisonWaitAsync(runtime, snapshot, config, cancellationToken);
        await EvaluatePgCpuAsync(runtime, snapshot, config, cancellationToken);
    }

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
            return;
        }

        const string metricName = "High CPU";
        var key = snapshot.ServerKey;

        try
        {
            var now = DateTime.UtcNow;
            var reading = await DarlingPgCpuUtilizationReader.GetLatestAsync(_postgres, runtime.ServerId, now, cancellationToken);

            var cooldown = TimeSpan.FromMinutes(Math.Max(1, _alertCooldownMinutes));
            var wasActive = _activePgCpuAlert.TryGetValue(key, out var activeBefore) && activeBefore;
            var exceeded = reading is not null && reading.CpuPercent >= alertSettings.CpuThresholdPercent;
            _activePgCpuAlert[key] = exceeded;

            if (exceeded)
            {
                var cooldownElapsed = !_lastPgCpuAlert.TryGetValue(key, out var last) || now - last >= cooldown;
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

                await _alertDeliverer.DeliverAsync(
                    new AlertOutcome(
                        key,
                        snapshot.ServerName,
                        metricName,
                        $"{reading!.CpuPercent:F0}%",
                        $"{alertSettings.CpuThresholdPercent}%",
                        Context: null,
                        DetailText: $"  Total CPU: {reading.CpuPercent:F0}%\n  Threshold: {alertSettings.CpuThresholdPercent}%",
                        NumericCurrentValue: reading.CpuPercent,
                        NumericThresholdValue: alertSettings.CpuThresholdPercent,
                        Muted: muted,
                        Severity: null,
                        ShortMessage: $"Total CPU at {reading.CpuPercent:F0}% (threshold: {alertSettings.CpuThresholdPercent}%)"),
                    cancellationToken);
            }
            else if (wasActive)
            {
                await NotifyPgResolutionAsync(key, snapshot.ServerName, metricName, "CPU Resolved",
                    reading is null
                        ? $"{snapshot.ServerName}: CPU back below threshold"
                        : $"{snapshot.ServerName}: Total CPU back to {reading.CpuPercent:F0}%");
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Server}] PostgreSQL CPU alert evaluation failed: {Message}",
                runtime.Config.DisplayName, ex.Message);
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
    /// </summary>
    private async Task EvaluatePgDeadlocksAsync(
        ServerRuntime runtime, AlertServerSnapshot snapshot, CancellationToken cancellationToken)
    {
        if (_postgres is null || _alertDeliverer is null)
        {
            return;
        }

        const string metricName = "Deadlocks Detected";
        var key = snapshot.ServerKey;
        var stateStore = new PgAlertStateStore(_postgres, _logger);

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
            var count = rows.Count;

            var cooldown = TimeSpan.FromMinutes(Math.Max(1, _alertCooldownMinutes));
            var watermark = _lastAlertedPgDeadlockCount.TryGetValue(key, out var wm) ? wm : 0;
            var cooldownElapsed = !_lastPgDeadlockAlert.TryGetValue(key, out var last) || now - last >= cooldown;

            var decision = RollingCountAlertGate.Evaluate(
                count, PgDeadlockCountThreshold, watermark, cooldownElapsed, suppressed: false);
            _lastAlertedPgDeadlockCount[key] = decision.Watermark;
            if (decision.Watermark != watermark)
            {
                /* On-change only (#1145's own contract) — persist AFTER the in-memory update so a
                   store failure never desyncs the two; the in-memory watermark still gates this
                   process even if the write is lost, same posture as every other watermark save
                   in this codebase. */
                await stateStore.SaveEdgeTriggerWatermarkAsync(key, metricName, decision.Watermark);
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

                await _alertDeliverer.DeliverAsync(
                    new AlertOutcome(
                        key,
                        snapshot.ServerName,
                        metricName,
                        count.ToString(CultureInfo.InvariantCulture),
                        PgDeadlockCountThreshold.ToString(CultureInfo.InvariantCulture),
                        Context: new AlertContext
                        {
                            Incidents = rows.Select(BuildPgDeadlockIncident).ToList(),
                        },
                        DetailText: null,
                        NumericCurrentValue: count,
                        NumericThresholdValue: PgDeadlockCountThreshold,
                        Muted: muted,
                        Severity: null,
                        ShortMessage: $"{count} deadlock(s) in the last hour"),
                    cancellationToken);
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
            _logger.LogError("[{Server}] PostgreSQL deadlock alert evaluation failed: {Message}",
                runtime.Config.DisplayName, ex.Message);
        }
    }

    /// <summary>
    /// The rolling-1-hour-window Postgres Blocking alert (#2711). Counts DISTINCT root blockers, not raw
    /// chain rows — <see cref="DarlingPgBlockingReader.GetPgBlockingChainsDedupedByRootAsync"/> (#2714) already
    /// dedupes by root INSIDE the query, before its own LIMIT, so a single persistent blocker sampled every
    /// cycle for an hour cannot crowd a distinct root out of the row budget the way the raw, severity-ordered
    /// <see cref="DarlingPgBlockingReader.GetPgBlockingChainsAsync"/> could. <see cref="WorstPgBlockingChainPerRoot"/>
    /// below still runs — see its own doc comment for why a second, C#-side dedup remains worth keeping even
    /// though the query no longer needs it to arrive at "one row per root". Same <see cref="RollingCountAlertGate"/>
    /// reuse and parity-named metrics as <see cref="EvaluatePgDeadlocksAsync"/> — see its doc comment for why.
    /// </summary>
    private async Task EvaluatePgBlockingAsync(
        ServerRuntime runtime, AlertServerSnapshot snapshot, CancellationToken cancellationToken)
    {
        if (_postgres is null || _alertDeliverer is null)
        {
            return;
        }

        const string metricName = "Blocking Detected";
        var key = snapshot.ServerKey;
        var stateStore = new PgAlertStateStore(_postgres, _logger);

        try
        {
            /* #2716: same restart-survival seed as EvaluatePgDeadlocksAsync — see its comment. */
            if (_pgBlockingWatermarkSeeded.TryAdd(key, true))
            {
                var seeded = await stateStore.LoadEdgeTriggerWatermarkAsync(key, metricName);
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

            var worstPerRoot = WorstPgBlockingChainPerRoot(rows);
            var count = worstPerRoot.Count;

            var cooldown = TimeSpan.FromMinutes(Math.Max(1, _alertCooldownMinutes));
            var watermark = _lastAlertedPgBlockingCount.TryGetValue(key, out var wm) ? wm : 0;
            var cooldownElapsed = !_lastPgBlockingAlert.TryGetValue(key, out var last) || now - last >= cooldown;

            var decision = RollingCountAlertGate.Evaluate(
                count, PgBlockingCountThreshold, watermark, cooldownElapsed, suppressed: false);
            _lastAlertedPgBlockingCount[key] = decision.Watermark;
            if (decision.Watermark != watermark)
            {
                await stateStore.SaveEdgeTriggerWatermarkAsync(key, metricName, decision.Watermark);
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

                await _alertDeliverer.DeliverAsync(
                    new AlertOutcome(
                        key,
                        snapshot.ServerName,
                        metricName,
                        count.ToString(CultureInfo.InvariantCulture),
                        PgBlockingCountThreshold.ToString(CultureInfo.InvariantCulture),
                        Context: new AlertContext
                        {
                            Incidents = worstPerRoot.Select(BuildPgBlockingIncident).ToList(),
                        },
                        DetailText: null,
                        NumericCurrentValue: count,
                        NumericThresholdValue: PgBlockingCountThreshold,
                        Muted: muted,
                        Severity: null,
                        ShortMessage: $"{count} blocking session(s)"),
                    cancellationToken);
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
            _logger.LogError("[{Server}] PostgreSQL blocking alert evaluation failed: {Message}",
                runtime.Config.DisplayName, ex.Message);
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

        try
        {
            var thresholdMinutes = alertSettings.LongRunningQueryThresholdMinutes;
            var now = DateTime.UtcNow;

            var rows = await DarlingPgSessionStatesReader.GetCurrentLongRunningSessionsAsync(
                _postgres, runtime.ServerId, thresholdMs: thresholdMinutes * 60_000L, now,
                PgLongRunningQueryRecencyMinutes, limit: alertSettings.LongRunningQueryMaxResults, cancellationToken);

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
                        },
                        DetailText: null,
                        NumericCurrentValue: elapsedMinutes,
                        NumericThresholdValue: thresholdMinutes,
                        Muted: muted,
                        Severity: null,
                        ShortMessage: $"pid {worst.Pid} running {elapsedMinutes}m — {worst.CommandTag ?? "(unknown)"}"
                            + (worst.DatabaseName is null ? "" : $" on {worst.DatabaseName}")),
                    cancellationToken);
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
            _logger.LogError("[{Server}] PostgreSQL long-running-query alert evaluation failed: {Message}",
                runtime.Config.DisplayName, ex.Message);
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
    /// threshold is a constant on <see cref="PostgresAlertEvaluator"/> for this first cut, the same
    /// reasoning as <see cref="PgDeadlockCountThreshold"/>.</para>
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

        try
        {
            var adapter = new DarlingPostgresAlertReadAdapter(_postgres);
            var rows = await adapter.GetPoisonWaitPressureAsync(runtime.ServerId, cancellationToken);
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
                        },
                        DetailText: null,
                        finding.NumericCurrentValue,
                        finding.NumericThresholdValue,
                        Muted: muted,
                        finding.Severity,
                        finding.ShortMessage),
                    cancellationToken);
            }

            /* The Cleared edge, per subject: previously active, no longer over the bar. Late by up to one
               window (the rolling sums age out rather than reset), which is accepted — a Cleared that
               arrives a few minutes conservative beats one that flaps with each sweep. */
            var activePrefix = string.Create(
                CultureInfo.InvariantCulture, $"{serverKey}|{metricName}|");
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
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("[{Server}] PostgreSQL poison wait alert evaluation failed: {Message}",
                runtime.Config.DisplayName, ex.Message);
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
            _logger.LogWarning("Could not record Postgres alert resolution for {Server}/{Metric}: {Message}",
                serverName, metricName, ex.Message);
        }
    }

    /// <summary>Rolling-window count threshold for the Postgres Deadlocks alert (#2711) — 1, matching SQL
    /// Server's own observed default via <see cref="IAlertEngineSettings.DeadlockCountThreshold"/>. A
    /// constant rather than a setting for this first cut, the same reasoning
    /// <see cref="PostgresAlertEvaluator"/>'s own doc comment gives for its three thresholds: add
    /// configuration when someone actually wants a different number, not speculatively.</summary>
    private const int PgDeadlockCountThreshold = 1;

    /// <summary>Rolling-window count threshold for the Postgres Blocking alert (#2711) — same reasoning
    /// as <see cref="PgDeadlockCountThreshold"/>.</summary>
    private const int PgBlockingCountThreshold = 1;

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
    /// The latest collected CPU sample for the snapshot — Lite's overview read
    /// (LocalDataService.Overview.cs:37-51) against the raw PG table, and the
    /// ServerSummaryItem.TotalCpuPercent derivation (:140-141): total = SQL + (other ?? 0),
    /// null when there is no SQL sample (Azure SQL DB stores other as 0; Linux stores NULL).
    /// </summary>
    private async Task<(double? SqlCpu, double? TotalCpu)> ReadLatestCpuAsync(int serverId, CancellationToken cancellationToken)
    {
        double? sqlCpu = null;
        double? otherCpu = null;

        await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(@"
SELECT sqlserver_cpu_utilization, other_process_cpu_utilization
FROM cpu_utilization_stats
WHERE server_id = $1
ORDER BY sample_time DESC
LIMIT 1", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            sqlCpu = reader.IsDBNull(0) ? null : Convert.ToDouble(reader.GetValue(0), CultureInfo.InvariantCulture);
            otherCpu = reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture);
        }

        double? totalCpu = sqlCpu.HasValue ? sqlCpu.Value + (otherCpu ?? 0) : null;
        return (sqlCpu, totalCpu);
    }

    /// <summary>
    /// Gathers the store disk-pressure sample and hands it to the Stage 4 evaluator (fleet-level). The store
    /// size (<c>pg_database_size</c>, context only) is always readable; the store volume's free/total space is
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
    /// The store database's on-disk size in bytes (<c>pg_database_size</c>) — context for the disk-pressure
    /// alert text, the same read the Viewer's status bar uses. Failure-isolated to null (Debug) so a transient
    /// store hiccup never breaks the disk-pressure check.
    /// </summary>
    private async Task<long?> ReadStoreSizeBytesAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand("SELECT pg_database_size(current_database())", connection) { CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds };
            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result is null || result == DBNull.Value ? null : Convert.ToInt64(result, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug("Store disk-pressure check: could not read pg_database_size: {Message}", ex.Message);
            return null;
        }
    }

    /// <summary>
    /// The #1581 compression-job self-heal check (fleet-level, hourly, Timescale-only): read every stuck
    /// COMPRESSION-policy job (<see cref="TimescaleSupport.ReadStuckCompressionJobsAsync"/>) and hand them to the
    /// self-alert evaluator's re-arm-once/escalate machine, wired to <see cref="TimescaleSupport.TryRearmJobAsync"/>
    /// on the SAME open connection. One stuck job whose <c>next_start</c> went <c>-infinity</c> silently halts the
    /// store's archival tier — the field incident — so this makes it visible AND self-heals it. Failure-isolated
    /// at the worker level too (the connection open is OUTSIDE the evaluator's own isolation): a store hiccup logs
    /// and skips this check, never aborting the sweep — mirroring the purge / disk-check isolation.
    /// </summary>
    private async Task EvaluateCompressionJobHealthAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);

            /* #1778: report what compression is DOING before deciding whether anything is stuck. The field
               could see hours-long compressions only in hindsight, by their effect on disk; this puts a
               running compression, its elapsed time, and any eligible-chunk backlog in the log while it is
               still happening. Read on the same connection and the same hourly cadence — the check that
               already exists for this exact subsystem, rather than a second timer for one log line. */
            TimescaleSupport.LogCompressionActivity(
                await TimescaleSupport.ReadCompressionActivityAsync(connection, _logger, cancellationToken),
                DateTime.UtcNow,
                _logger);

            var stuckJobs = await TimescaleSupport.ReadStuckCompressionJobsAsync(
                connection, DateTime.UtcNow, _logger, cancellationToken);
            await _selfAlerts!.EvaluateCompressionJobsAsync(
                stuckJobs,
                jobId => TimescaleSupport.TryRearmJobAsync(connection, jobId, _logger, cancellationToken),
                cancellationToken);

            /* #2136: the Store Job Over Cadence check rides the same connection and hourly cadence — a
               background job whose last successful run reached the warning share of its own schedule
               interval is the store outgrowing its job schedule, the number an onboarding wave moves
               first. Same isolation posture: the evaluator wraps itself, and this whole method's catch
               is the backstop. */
            var cadenceReadings = await TimescaleSupport.ReadJobCadenceReadingsAsync(
                connection, _logger, cancellationToken);
            await _selfAlerts!.EvaluateStoreJobCadenceAsync(cadenceReadings, cancellationToken);

            /* #2813: the Retention Held check rides the same connection and hourly cadence. A retention
               policy the #1680/#1877 coverage gate has paused reports total_failures = 0 and a plausible
               last run — it is not failing, it is stopped — so it is invisible to every stored metric and
               went unnoticed on the production store for 16 days while that tier grew to 4.5x its horizon.
               Judged on the CONSEQUENCE (held AND the tier past its own horizon), never on the paused flag
               alone, which is the normal state of every freshly created policy. Same isolation posture. */
            var retentionHolds = await TimescaleSupport.ReadRetentionHoldReadingsAsync(
                connection, _logger, cancellationToken);
            await _selfAlerts!.EvaluateRetentionHoldsAsync(retentionHolds, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown — quiet and expected. */
        }
        catch (Exception ex)
        {
            _logger.LogError("Compression-job health check failed: {Message}", ex.Message);
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
            await StoreSelfMetrics.SweepAsync(connection, _timescaleAvailable, DateTime.UtcNow, _logger, budget.Token);

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
            var analysisService = new DarlingAnalysisService(_postgres!, planFetcher, _logger);

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

            /* Persist the pass's insufficient-data determination (V19 marker) so the Viewer's
               Recommendations tab shows "still collecting" instead of a false all-clear on a young
               deployment: true + the engine's message when the pass hit the 24h data-span gate, cleared
               (false) when a real pass completed on enough data. Failure-isolated like the other
               observability writes. Only the two REAL terminal states write it — a Skipped/TimedOut/Error
               pass (handled above / in the catch) leaves the last known marker untouched. */
            if (analysisService.InsufficientDataMessage is string insufficient)
            {
                await DarlingObservability.WriteAnalysisStateAsync(
                    _postgres!, serverId, insufficientData: true, insufficient, _logger, stoppingToken);
                return new AnalysisPassResult(AnalysisPassStatus.InsufficientData, 0, insufficient);
            }

            await DarlingObservability.WriteAnalysisStateAsync(
                _postgres!, serverId, insufficientData: false, null, _logger, stoppingToken);
            return new AnalysisPassResult(AnalysisPassStatus.Ran, findings.Count, null);
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

        /* The operator door the scheduled-path gate (see the PostgreSql arm in the analysis tick) did not
           cover: "Generate now" against a PostgreSQL target ran the full SQL-Server-shaped pass, which
           found nothing, persisted the GENERIC insufficient_data message, and thereby OVERWROTE the honest
           engine tombstone the scheduled arm wrote — the Recommendations tab regressed from "does not
           apply, use the PG reads" back to "still collecting" the moment an operator clicked the button.
           Same decision, same honest answer, re-written here so the tombstone survives the click. */
        if (server.Runtime?.Target.Engine == CollectorTargetEngine.PostgreSql)
        {
            /* Mirror the scheduled arm's once-latch so the tick does not re-write what this just wrote. */
            server.PostgresAnalysisStateWritten = true;
            await DarlingObservability.WriteAnalysisStateAsync(
                _postgres!,
                server.Runtime.ServerId,
                insufficientData: true,
                message: PostgresAnalysisNotApplicable,
                _logger,
                cancellationToken);

            return new CommandOutcome(true, "analysis not applicable",
                JsonSerializer.Serialize(new
                {
                    success = true,
                    server = server.Config.DisplayName,
                    message = "Analysis is SQL-Server-shaped and does not apply to a PostgreSQL target; "
                        + "use the get_pg_* MCP reads and the outage-predictor alerts instead.",
                }));
        }

        /* postPassHook: null — analyze_now is an interactive diagnostic, and the force-plan bot only
           rides the SCHEDULED cadence so an operator poking a server cannot spend its action budget. */
        var result = await RunAnalysisPassAsync(
            serverId, server.Config.StorageName, server.Config.DisplayName,
            planFetcher, notificationService, config.Analysis.NotificationsEnabled, postPassHook: null, cancellationToken);

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
            _logger.LogWarning("[{Server}] Recently-failed-job check errored: {Message}",
                runtime.Config.DisplayName, ex.Message);
            return new List<FailedJobInfo>();
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
                   A disabled or on-load-only (freq 0) collector is skipped; the frequency the NextDue stamp
                   advances by is the EFFECTIVE one, so an override takes effect immediately. */
                var effective = StoreConfigProvider.ResolveSchedule(name, runtime.ServerId, _scheduleOverrides);
                if (!effective.Enabled
                    || effective.FrequencyMinutes == 0
                    || !server.NextDue.TryGetValue(name, out var due)
                    || now < due)
                {
                    continue;
                }

                server.NextDue[name] = now.AddMinutes(effective.FrequencyMinutes);

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
                if (IsQueryStoreCollector(name) || IsPlanCorrectionCollector(name))
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
    /// Maps a PostgreSQL fault to a collection_log status plus the sentence an operator needs.
    /// <para>The store has five statuses and none of them is "this feature is not installed", so the
    /// non-fatal-degradation bucket (PERMISSIONS) carries those cases and the MESSAGE distinguishes them —
    /// the same division the Azure service-objective hint already uses. Returning "ERROR" means "let the
    /// general handler have it", which keeps the genuinely unexpected loud.</para>
    /// </summary>
    internal static (string Status, string Explanation) PostgresFaultOutcome(
        PostgresException ex, string collectorName, string? connectedDatabase = null)
    {
        var fault = PostgresTargetProvider.Instance.Classify(
            ex, CollectorCatalog.YieldsOnLockTimeout(collectorName));

        return fault switch
        {
            CollectorTargetFault.Permissions => ("PERMISSIONS",
                $"{ex.MessageText} (SQLSTATE {ex.SqlState}) — the monitoring login lacks a grant this "
                + "source needs. pg_monitor covers every collector here; check that it is granted."),

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

            /* Everything else — including a command timeout and a fatal connection error — belongs to the
               general handler, which logs ERROR and (for ConnectionFatal) forces the reprobe. */
            _ => ("ERROR", ex.Message),
        };
    }

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
        using var detachedGate = IsPlanCorrectionCollector(collectorName)
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

        try
        {
            var result = await run(runner, runtime, cancellationToken);
            _logger.LogInformation("  [{Server}] {Collector} => {Rows} rows (sql:{SqlMs}ms, pg:{PgMs}ms)",
                server.Config.DisplayName, collectorName, result.Rows, result.SqlMs, result.StorageMs);

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
                _logger.LogInformation(
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
               hits the permissions filter. */
            _logger.LogWarning("  [{Server}] {Collector} => XE session missing (capture down): {Message}",
                server.Config.DisplayName, collectorName, ex.Message);

            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, collectorName, "SESSION_MISSING", 0, 0, 0, ex.Message, fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
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
                _postgres!, runtime, collectorName, "PERMISSIONS", 0, 0, 0,
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
                _postgres!, runtime, collectorName, "PERMISSIONS", 0, 0, 0,
                $"{ex.Message} — the MONITORING HOST's IAM role lacks a grant this source needs, which is "
                + "not a database grant: instance CPU on managed PostgreSQL reads AWS Performance Insights, "
                + "so the role needs rds:DescribeDBInstances, rds:DescribeDBClusters and "
                + "pi:GetResourceMetrics on the target instance. Nothing was read this cycle — this is NOT "
                + "'CPU is idle'.",
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
                _postgres!, runtime, collectorName, "YIELDED", 0, 0, 0,
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
                _postgres!, runtime, collectorName, "PERMISSIONS", 0, 0, 0, message, fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (PostgresException ex) when (
            PostgresFaultOutcome(ex, collectorName, runtime.ConnectedDatabase) is { Status: not "ERROR" } outcome)
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
                _postgres!, runtime, collectorName, status, 0, 0, 0, explanation, fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
            return 0;
        }
        catch (Exception ex)
        {
            _logger.LogError("  [{Server}] {Collector} => ERROR: {Message}",
                server.Config.DisplayName, collectorName, ex.Message);

            /* A dead connection poisons every collector — force a reconnect + reprobe. The Postgres arm
               matters as much as the SQL Server one and is deliberately NARROWER than "any
               PostgresException": a statement_timeout (57014) is a slow query, not a dead socket, and
               dropping the connection over one would turn a tuning problem into a reconnect storm. Only
               the 08 class and the shutdown/unavailability codes qualify, which is exactly what the
               provider's ConnectionFatal means. */
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
                await DarlingObservability.LogCollectionAsync(
                    _postgres!, runtime, collectorName, "ERROR", 0, 0, 0, ex.Message, fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: peerMaxAtDispatchMs, _logger, cancellationToken);
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
    };

    /// <summary>
    /// The analysis_state message for a PostgreSQL target, shared by the SCHEDULED pass and the manual
    /// "Generate now" path.
    /// <para>One constant because there were two hand-maintained copies and they had already drifted: adding
    /// <c>get_pg_blocking</c> to the scheduled one left the manual one listing seven tools, so an operator
    /// clicking Generate now got different guidance from the same product depending on which door they came
    /// through. The list grows with every PostgreSQL read, which guarantees the drift recurs.</para>
    /// </summary>
    internal const string PostgresAnalysisNotApplicable =
        "Scheduled analysis does not apply to a PostgreSQL target: its findings are derived from SQL Server "
        + "collectors (waits, query stats, CPU) that this engine does not populate. This is not "
        + "\"still collecting\" — use the PostgreSQL MCP reads (get_pg_wait_stats, get_pg_top_queries, "
        + "get_pg_autovacuum_health, get_pg_wraparound_risk, get_pg_xmin_horizon, get_pg_replication_slots, "
        + "get_pg_io_stats, get_pg_blocking) and the three outage-predictor alerts instead.";

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
            return new CollectorRunResult(0, 0, 0);
        }
    }
}
