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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Stage 4 of the Darling control plane — the SERVICE's self-alerts: the "is my collection actually
/// working" conditions that matter most for an unattended 24/7 headless service where nobody is
/// watching a dashboard. Four conditions — the first three reframe a Dashboard health check onto Darling's
/// own signals; the fourth (Store Disk Pressure) is net-new and guards the service's OWN store — all routed
/// through the SAME <see cref="IAlertDeliverer"/> the shared alert engine uses (so they inherit its
/// email/webhook delivery, per-fingerprint delivery cooldown, and restart replay) and the SAME
/// <c>config_alert_log</c> history store:
/// <list type="number">
/// <item><b>Collection Stopped / collector failure</b> — the server's <c>collection_log</c> shows no
///   SUCCESS within a staleness window OR the last N runs all failed (reframes the Dashboard's
///   "Collection Stopped", <c>MainWindow.AlertEngine.cs</c> + <c>NocHealth.GetCollectionStoppedAsync</c>,
///   onto Darling's collection_log instead of msdb Agent-job state).</item>
/// <item><b>Server Unreachable / Restored</b> — fired on the connect edge in
///   <see cref="DarlingWorker"/>'s loop (online→offline and back), the headless twin of the
///   Dashboard's <c>NotifyOnConnectionLost</c>/<c>Restored</c> (<c>MainWindow.xaml.cs</c>). Uses the
///   Dashboard's exact metric names ("Server Unreachable" / "Server Restored") so the alert history
///   vocabulary and the shared <c>AlertSeverity</c> map (Critical / green RESOLVED) match cross-app.</item>
/// <item><b>Capture Down</b> — a missing/denied blocking-deadlock XE session, surfaced from the
///   <c>SESSION_MISSING</c> collection_log status the tolerant XE readers now write (reframes the
///   Dashboard's #1086 "Capture Down", <c>NocHealth.GetMissingCaptureSessionsAsync</c>).</item>
/// <item><b>Store Disk Pressure</b> — the volume hosting the Darling store is nearly full. Unlike the
///   other three (per monitored server), this is a FLEET-level condition polled once per sweep from the
///   store itself (<c>pg_database_size</c> for context) and the store volume's free space: when a headless
///   service's disk fills, collection and every write stop for the WHOLE fleet, and nobody is watching. The
///   flagship-appropriate maintenance backstop the daily time-based purge otherwise lacks — deliberately
///   NOT Lite's 512MB archive-then-reset (Postgres has no single-file INSERT cliff, and a blanket reset
///   would nuke every tenant of the shared store).</item>
/// <item><b>Availability Group health</b> (#991) — four conditions over the AG collectors' latest snapshot:
///   a replica changed role ("AG Failover"), a replica lost or regained its connection to the primary
///   ("AG Replica Disconnected"/"AG Replica Reconnected"), a secondary fell behind by lag seconds or redo
///   queue ("AG Sync Fell Behind"), and data movement for a database was suspended ("AG Database Suspended").
///   VANTAGE MATTERS: <c>sys.dm_hadr_*</c> on a SECONDARY carries only that replica's own rows, so a
///   monitored secondary yields a one-row self-view of its AG rather than the whole topology (measured on a
///   clusterless AG). Nothing here assumes it can see every replica from any node — the rules simply judge
///   whatever rows arrive, keyed per ag+replica — but full AG coverage requires monitoring the primary.
///   Gated on the V35 <c>notify_ag_health</c> master switch, with the two thresholds
///   (<c>ag_lag_alert_seconds</c>, <c>ag_redo_queue_alert_kb</c>) store-backed alongside it. Keyed per AG
///   grain rather than per server, because a server hosts many replicas and databases.</item>
/// </list>
/// Each condition is EDGE-TRIGGERED (in-memory active flag + the shared alert cooldown for the polled
/// conditions; a per-server connection state machine for the connect edge) so it fires once on the
/// transition, not every sweep — exactly the Dashboard's <c>_activeXAlert</c>/<c>_lastXAlert</c> shape.
/// On recovery a "…Resumed"/"…Restored"/"…Resolved" row is written to alert history (closing the audit loop
/// the same way the engine's resolution callback now does — see <see cref="BuildResolutionRecord"/>).
/// Gated on the master <c>alerts.enabled</c> switch — plus, for the connect edge, the V20
/// <c>notify_connection_changes</c> toggle (Lite's <c>App.NotifyConnectionChanges</c> twin); the
/// collection-stopped / capture-down / disk-pressure thresholds stay sensible hardcoded defaults.
/// </summary>
internal sealed class DarlingSelfAlertEvaluator
{
    /* No successful collection within this window (a server that HAS collected before) reads as
       stopped — matches the Dashboard's CollectionStaleThresholdMinutes (NocHealth.cs). The frequent
       Darling collectors run every ~1 minute, so 30 minutes of no success is unambiguously dead; the
       connection-lost alert covers the fast path for an unreachable server, and the consecutive-failure
       check below covers the fast path for a server that is connected but erroring every collector. */
    internal static readonly TimeSpan StaleWindow = TimeSpan.FromMinutes(30);

    /* The last N logged runs all failing (no SUCCESS/SKIPPED among them) fires "Collection Stopped"
       faster than the staleness backstop when a connected server's collectors are erroring on every
       cycle. 10 spans a couple of minutes of total failure across the frequently-scheduled collectors. */
    internal const int ConsecutiveFailureThreshold = 10;

    /* Store Disk Pressure fires when the store volume drops below this percent free — a percentage (not an
       absolute floor) so it scales from a small managed box to a large fleet disk; 10% is the universal DBA
       "act now" threshold for a database volume, and mirrors the shared engine's target-server low-disk
       percent (LowDiskThresholdPercent). Percent-only by design (defaults over speculative config — a GB
       floor is a trivial follow-up if an operator ever wants one). The condition no-ops when free space is
       undeterminable (a remote BYO store), so it never false-alarms; the managed store's own volume is the
       case it exists to protect. */
    internal const double DiskFreeWarnPercent = 10.0;

    private readonly IAlertEngineSettings _settings;
    private readonly IAlertDeliverer _deliverer;
    private readonly IAlertHistoryStore _historyStore;
    private readonly Func<AlertMuteContext, bool> _isAlertMuted;
    private readonly ILogger? _logger;
    private readonly Func<DateTime> _utcNow;

    /* The connection-change notify gate (V20), read live so a store reload takes effect on the next connect
       edge. A Func rather than a settings-interface member because NotifyConnectionChanges is a Darling-specific
       concrete DarlingAlertSettings knob, not on the shared IAlertEngineSettings (the DeliveryMode precedent);
       the Func seam also keeps the test fakes — which implement only IAlertEngineSettings — untouched. Defaults
       to always-on when unsupplied (preserving the pre-V20 behavior). */
    private readonly Func<bool> _notifyConnectionChanges;

    /// <summary>#1659 opt-in seams, read live like <see cref="_notifyConnectionChanges"/>.</summary>
    private readonly Func<bool> _notifyConnectionDownAtStartup;
    private readonly Func<int> _connectionRefireMinutes;

    /// <summary>The Availability Group alert seams (#991, V35), read live through the same by-reference
    /// settings the store reload hot-swaps — the <see cref="_notifyConnectionChanges"/> discipline. The
    /// master switch gates all four AG conditions; the two thresholds are already clamped by
    /// <see cref="DarlingAlertSettings"/>, so a hand-edited store row cannot drive a nonsense window.</summary>
    private readonly Func<bool> _notifyAgHealth;

    /// <summary>#1696 (V37) re-fire interval for "AG Replica Disconnected", read live like the other AG
    /// seams. 0 = off, the shipped default.</summary>
    private readonly Func<int> _agDisconnectRefireMinutes;
    private readonly Func<int> _agLagAlertSeconds;
    private readonly Func<long> _agRedoQueueAlertKb;

    /// <summary>When the last down alert fired per server — the re-fire clock for
    /// <see cref="ConnectionAlertPolicy"/> (#1659). Stamped on every down alert DELIVERED (not merely
    /// decided: a decision suppressed by the notify toggles must not consume the re-fire window), cleared
    /// on a delivered Restored.</summary>
    private readonly ConcurrentDictionary<string, DateTime> _lastConnectionDownAlertUtc = new();

    /* Edge state, keyed by the engine's serverKey (the server_id as an invariant string — the same
       identity the deliverer/history/watermark stores use). In-memory only, exactly like the shared
       engine's active-condition flags; the restart replay protection is the deliverer's own
       history-seeded email/webhook cooldown, not these. */
    private readonly ConcurrentDictionary<string, bool> _activeCollectionStopped = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastCollectionStoppedAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activeCaptureDown = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastCaptureDownAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activeAgentDown = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastAgentDownAlert = new();

    /// <summary>Servers on which SQL Agent has been OBSERVED RUNNING at least once — the capability gate for
    /// "Agent Not Running". Memoized only once true, so the store probe behind it stops after the first
    /// positive and a server that genuinely runs Agent costs one extra read, once.</summary>
    private readonly ConcurrentDictionary<string, bool> _agentEverSeenRunning = new();
    private readonly ConcurrentDictionary<string, ConnectionState> _connectionState = new();

    /* Store Disk Pressure edge state. FLEET-level (one shared store, not per server), so it is keyed by a
       single fixed sentinel (DiskKey) rather than a serverId — never dropped by Forget (that is per-server).
       Dictionaries (not a plain bool) purely to reuse the same TryRemove-recovery + CooldownElapsed helpers
       the per-server conditions use. */
    private readonly ConcurrentDictionary<string, bool> _activeDiskPressure = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastDiskPressureAlert = new();

    /// <summary>
    /// The free-percent level the last Store Disk Pressure alert reported (#2101) — the worsening
    /// watermark <see cref="LowDiskAlertGate.ShouldAlert"/> compares against, exactly the engine's
    /// <c>_lastAlertedLowDiskPercent</c> idiom. Cleared on recovery so the next breach is fresh.
    /// </summary>
    private readonly ConcurrentDictionary<string, double> _lastAlertedDiskPressurePercent = new();

    /* #2674: the tool's OWN collectors regressing in cost on a monitored server. Keyed cost:{serverId}:{collector};
       value is the server name so a resolution has it without a second read. Same active-flag + CooldownElapsed
       idiom as the per-server conditions above. Thresholds are hardcoded conservative defaults (this class's
       stated posture): a collector must have cost at least CostRegressionBaselineFloorMs/day on average over at
       least three prior days, and its latest day must exceed that baseline by CostRegressionFactor, before it
       alerts — so a cheap collector, or a new one, cannot trip it. */
    private readonly ConcurrentDictionary<string, string> _activeCostRegression = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastCostRegressionAlert = new();

    /// <summary>#2707: the <c>collect.collector_cost</c> row this key last fired on, keyed the same as
    /// <see cref="_lastCostRegressionAlert"/>. This evaluator is meant to run once per hourly store-metrics
    /// tick, but if that tick's cadence ever outpaces the cooldown — or the hourly flush itself lags a tick —
    /// re-asking <see cref="Mcp.DarlingCollectorCostReader.GetCostRegressionsAsync"/> hands back the exact
    /// same <c>latest_ms</c> computed from the exact same underlying hourly rows, and a cooldown-elapsed check
    /// alone cannot tell that answer from a genuinely new one. Mirrors #2704's
    /// <c>PoisonWaitDelta.CollectionTime</c> fix for the identical shape of bug.</summary>
    private readonly ConcurrentDictionary<string, DateTime> _lastCostRegressionDataPoint = new();
    private const double CostRegressionFactor = 2.0;
    private const long CostRegressionBaselineFloorMs = 1000;
    private static readonly TimeSpan CostRegressionBaselineWindow = TimeSpan.FromDays(14);

    /// <summary>The fixed key for the fleet-level Store Disk Pressure edge (not a real server).</summary>
    private const string DiskKey = "store";

    /* Compression-job self-heal edge state (#1581). FLEET-level like disk pressure (one shared store), but
       MULTI-keyed by job_id (a store has many compression policy jobs). The state is the re-arm-once/escalate
       machine: ReArmed = detected + re-armed once this episode (a later still-stuck reading is a RE-HANG);
       Escalated = re-hung after self-heal, or the re-arm itself failed — a human signal, so the service STOPS
       re-arming and only re-fires the CRITICAL on cooldown. An entry is dropped (and a resolution row written)
       when the job stops being stuck. Keyed by the CompressionKeyPrefix + job_id so the alert serverKey never
       collides with a real server_id (an int hash) — the deliverer's #1236 int.TryParse override no-ops on it,
       exactly like the non-numeric DiskKey. */
    private enum CompressionJobHealth { ReArmed, Escalated }
    private readonly ConcurrentDictionary<string, CompressionJobHealth> _compressionJobState = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTime> _lastCompressionJobAlert = new(StringComparer.Ordinal);

    /// <summary>The alert metric name every compression-job self-alert fires under (first-detection + escalation
    /// re-fires share it, so the deliverer's per-metric cooldown and the recovery resolution correlate cleanly;
    /// escalation is distinguished by the message, not a second metric name).</summary>
    private const string CompressionJobMetric = "Compression Job Stuck";

    /// <summary>Prefixes the fleet-level compression-job alert serverKey so it never parses as a server_id.</summary>
    private const string CompressionKeyPrefix = "compressjob:";

    /* Store Job Over Cadence edge state (#2136). FLEET-level like disk pressure, MULTI-keyed by job_id like
       the compression machine, but a STANDING condition (the AG Sync Fell Behind idiom): active flag +
       cooldown re-fire while a job's last run keeps breaching its share of the schedule interval, one
       "Store Job Cadence Recovered" resolution when a later run comes back under. */
    private readonly ConcurrentDictionary<string, bool> _activeJobOverCadence = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTime> _lastJobOverCadenceAlert = new(StringComparer.Ordinal);

    /// <summary>The #2136 alert metric name — the Warning and Critical tiers share it (severity carries the
    /// tier), so the deliverer's per-metric cooldown and the recovery resolution correlate cleanly.</summary>
    internal const string JobCadenceMetric = "Store Job Over Cadence";

    /// <summary>Prefixes the fleet-level cadence alert serverKey so it never parses as a server_id.</summary>
    private const string JobCadenceKeyPrefix = "storejob:";

    /// <summary>#2136: the Warning tier's percent-of-cadence threshold, read live through the same
    /// by-reference settings seam as the AG thresholds (the clamp lives on DarlingAlertSettings).
    /// The Critical tier is FIXED at 100: a job outrunning its own cadence compounds refresh lag.</summary>
    private readonly Func<int> _storeJobCadenceWarnPercent;

    /* Availability Group edge state (#991), keyed by a COMPOSITE of serverId + the AG grain — an AG condition
       is per replica (ag + replica) or per database (ag + database + replica), not per server, so one server's
       two lagging databases must track (and recover) independently. Only the alert HISTORY is per server: every
       AG alert fires under the real server_id as its serverKey, so per-server delivery overrides (#1236), mute
       rules and history correlation keep working; the AG grain lives in the alert text. In-memory like every
       sibling condition — the restart replay protection is the deliverer's history-seeded cooldown. */
    private readonly ConcurrentDictionary<string, string> _agReplicaRole = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> _agReplicaConnectedState = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _activeAgSyncBehind = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTime> _lastAgSyncBehindAlert = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _agDatabaseSuspended = new(StringComparer.Ordinal);

    /// <summary>Which monitored server currently judges each Availability Group, and how good its view is
    /// (#1696). Every replica is visible from every node, so a fully-monitored 3-node AG previously reported
    /// one failover THREE times. One server owns each AG; a server with a strictly better vantage takes over
    /// (a secondary yielding to the primary, whose view is the only complete one). Fleet-level, so it is
    /// deliberately NOT dropped by <see cref="Forget"/> the way the per-server state is — see there.</summary>
    private readonly ConcurrentDictionary<string, (int ServerId, AgVantage Vantage)> _agAuthority =
        new(StringComparer.Ordinal);

    /// <summary>When "AG Replica Disconnected" last DELIVERED per ag+replica — the #1659 re-fire clock
    /// (V37). Stamped on delivery only, so a decision suppressed by the master switch cannot consume the
    /// window; cleared on reconnect.</summary>
    private readonly ConcurrentDictionary<string, DateTime> _lastAgDisconnectAlert = new(StringComparer.Ordinal);

    /* The AG alert metric names and every pure AG decision now live in the shared
       PerformanceMonitor.Common.AgAlertPolicy, so Lite fires the SAME names off the SAME rules (#1696) — the
       ConnectionAlertPolicy discipline. This evaluator keeps only the edge STATE and the delivery; the
       aliases below just keep the fire sites readable. */
    internal const string AgFailoverMetric = AgAlertPolicy.FailoverMetric;
    internal const string AgReplicaDisconnectedMetric = AgAlertPolicy.ReplicaDisconnectedMetric;
    internal const string AgReplicaReconnectedMetric = AgAlertPolicy.ReplicaReconnectedMetric;
    internal const string AgSyncFellBehindMetric = AgAlertPolicy.SyncFellBehindMetric;
    internal const string AgDatabaseSuspendedMetric = AgAlertPolicy.DatabaseSuspendedMetric;

    /// <summary>
    /// Separates the parts of a composite AG state key. A UNIT SEPARATOR (U+001F) rather than a printable
    /// character because AG / database / replica names are SQL Server identifiers, which may contain any
    /// printable character when delimited — so a printable separator could let two different (ag, database,
    /// replica) triples collide on one key. Never logged or displayed.
    /// </summary>
    private const char AgKeySeparator = '\u001f';


    /* Whether the service has successfully connected to this server at least once THIS process-run. Guards
       collection-stopped: unlike the Dashboard (whose target-side collection_log keeps filling regardless of
       the app), Darling IS the collector, so the service's own downtime makes collection_log stale. Without
       this guard a service restart after >30 min of downtime would false-alarm "Collection Stopped" on a
       perfectly healthy server before its first fresh collection lands. Gating on a prior successful connect
       makes collection-stopped a clean "was collecting, then stopped" transition (the same philosophy as the
       connection-lost edge and the Dashboard's skip-first-check) rather than a judgement on pre-restart data. */
    private readonly ConcurrentDictionary<string, bool> _hasBeenOnline = new();

    public DarlingSelfAlertEvaluator(
        IAlertEngineSettings settings,
        IAlertDeliverer deliverer,
        IAlertHistoryStore historyStore,
        Func<AlertMuteContext, bool> isAlertMuted,
        ILogger? logger = null,
        Func<DateTime>? utcNow = null,
        Func<bool>? notifyConnectionChanges = null,
        Func<bool>? notifyConnectionDownAtStartup = null,
        Func<int>? connectionRefireMinutes = null,
        Func<bool>? notifyAgHealth = null,
        Func<int>? agLagAlertSeconds = null,
        Func<long>? agRedoQueueAlertKb = null,
        Func<int>? agDisconnectRefireMinutes = null,
        Func<int>? storeJobCadenceWarnPercent = null)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _deliverer = deliverer ?? throw new ArgumentNullException(nameof(deliverer));
        _historyStore = historyStore ?? throw new ArgumentNullException(nameof(historyStore));
        _isAlertMuted = isAlertMuted ?? throw new ArgumentNullException(nameof(isAlertMuted));
        _logger = logger;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _notifyConnectionChanges = notifyConnectionChanges ?? (() => true);
        _notifyConnectionDownAtStartup = notifyConnectionDownAtStartup ?? (() => false);
        _connectionRefireMinutes = connectionRefireMinutes ?? (() => 0);
        /* Unsupplied AG seams fall back to the V35 DDL defaults (master switch on, 5-minute lag, redo queue
           off) so an evaluator built without them behaves like a store at its shipped defaults. */
        _notifyAgHealth = notifyAgHealth ?? (() => true);
        _agLagAlertSeconds = agLagAlertSeconds ?? (() => 300);
        _agRedoQueueAlertKb = agRedoQueueAlertKb ?? (() => 0);
        _agDisconnectRefireMinutes = agDisconnectRefireMinutes ?? (() => 0);
        /* Unsupplied falls back to the V57 DDL default, so an evaluator built without the seam behaves
           like a store at its shipped defaults (the AG-seam discipline). */
        _storeJobCadenceWarnPercent = storeJobCadenceWarnPercent ?? (() => 25);
    }

    private enum ConnectionState
    {
        /* Never yet observed — the baseline. Unknown→online/offline never fires (mirrors the
           Dashboard's "skip the first check" so a server that is simply down at startup does not
           page; only a transition FROM a known state does). */
        Unknown,
        Online,
        Offline
    }

    /* ---------------- store-polled self-alerts (collection-stopped + capture-down) ---------------- */

    /// <summary>
    /// Evaluates the store-polled self-alerts for one server from its <c>collection_log</c>. Gated on the
    /// master alerts switch (mirrors the engine's early return). Collection-stopped runs for EVERY server
    /// whether or not it is currently connected (an unreachable server has stopped collecting — that is
    /// exactly the case to catch); capture-down runs only for a connected server (its XE collectors only
    /// run then). Failure-isolated per condition so a bad store read never breaks the loop.
    /// </summary>
    public async Task EvaluateStoreAlertsAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, bool connected, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        /* Only judge collection-stopped once the service has actually collected from this server this run
           (see _hasBeenOnline) — otherwise pre-restart / pre-re-add stale rows would false-alarm before the
           first fresh collection lands. */
        if (_hasBeenOnline.ContainsKey(Key(serverId)))
        {
            try
            {
                /* #2107: store-backed window/threshold (clamped on read); the constants remain
                   only as the shipped defaults. */
                var (lastSuccess, recentRuns, recentSuccess) =
                    await ReadCollectionSignalsAsync(postgres, serverId, _settings.CollectionFailureThreshold, cancellationToken);
                bool stopped = IsCollectionStopped(
                    lastSuccess, recentRuns, recentSuccess, _utcNow(),
                    SettingsStaleWindow, _settings.CollectionFailureThreshold, out var reason);
                await ApplyCollectionStoppedAsync(serverId, serverName, stopped, reason, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger?.LogError("[{Server}] Collection-health self-alert failed: {Message}", serverName, ex.Message);
            }
        }

        if (!connected)
        {
            return;
        }

        try
        {
            var missing = await ReadMissingCaptureSessionsAsync(postgres, serverId, cancellationToken);
            await ApplyCaptureDownAsync(serverId, serverName, missing, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("[{Server}] Capture-down self-alert failed: {Message}", serverName, ex.Message);
        }

        try
        {
            /* Agent Not Running (#1433 Phase 2): the collected agent_status snapshot says the target's SQL
               Agent service is stopped. Only a FRESH reading judges — a stale row (collection lagging) yields
               null, so the collection-stopped alert owns staleness and this never false-alarms on old data. */
            var (agentCollectionTimeUtc, agentRunning) = await ReadLatestAgentStatusAsync(postgres, serverId, cancellationToken);
            bool? freshRunning = agentRunning.HasValue
                && agentCollectionTimeUtc.HasValue
                && _utcNow() - agentCollectionTimeUtc.Value < SettingsStaleWindow
                    ? agentRunning
                    : null;

            /* Capability gate: only a server that has been seen RUNNING Agent can report it stopped. The current
               reading is itself the cheapest possible evidence, so a running Agent short-circuits the probe;
               otherwise ask the collected history once and memoize the positive. */
            var agentKey = Key(serverId);
            if (freshRunning == true)
            {
                _agentEverSeenRunning[agentKey] = true;
            }

            if (!_agentEverSeenRunning.TryGetValue(agentKey, out var everRan) || !everRan)
            {
                everRan = await HasAgentEverBeenSeenRunningAsync(postgres, serverId, cancellationToken);
                if (everRan)
                {
                    _agentEverSeenRunning[agentKey] = true;
                }
            }

            await ApplyAgentNotRunningAsync(serverId, serverName, freshRunning, everRan, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("[{Server}] Agent-not-running self-alert failed: {Message}", serverName, ex.Message);
        }

        /* Availability Group health (#991). Skipped entirely when the master AG switch is off, so a fleet that
           runs no AGs — the common case — pays NOTHING for this on the per-server sweep: the read below is the
           only work, and it does not happen. Like agent_status, only a FRESH snapshot judges; a stale one (the
           collector lagging, or an AG that stopped existing so the collector now writes zero rows) yields no
           signal, and the collection-stopped alert owns staleness. */
        if (_notifyAgHealth())
        {
            try
            {
                /* Each grain is gated on its OWN snapshot time: the two AG collectors are scheduled
                   independently, so one being disabled or broken must not let the other's fresh timestamp
                   vouch for its stale rows. */
                var (replicaTimeUtc, replicas) =
                    await ReadLatestAgReplicaStatesAsync(postgres, serverId, cancellationToken);
                if (IsFresh(replicaTimeUtc))
                {
                    await ApplyAgReplicaHealthAsync(serverId, serverName, replicas, cancellationToken);
                }

                var (databaseTimeUtc, databases) =
                    await ReadLatestAgDatabaseReplicaStatesAsync(postgres, serverId, cancellationToken);
                if (IsFresh(databaseTimeUtc))
                {
                    await ApplyAgDatabaseHealthAsync(serverId, serverName, databases, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger?.LogError("[{Server}] Availability-Group self-alert failed: {Message}", serverName, ex.Message);
            }
        }

        /* A snapshot only judges while it is fresh; a missing one (no AGs, or the collector has never run) and
           a stale one are both "no signal", exactly as agent_status is treated above. */
        bool IsFresh(DateTime? snapshotUtc) =>
            snapshotUtc.HasValue && _utcNow() - snapshotUtc.Value < SettingsStaleWindow;
    }

    /// <summary>#2107: the staleness window the sweep actually uses — store-backed, clamped on
    /// read; <see cref="StaleWindow"/> remains only as the shipped default.</summary>
    private TimeSpan SettingsStaleWindow => TimeSpan.FromMinutes(_settings.CollectionStaleMinutes);

    /// <summary>
    /// Pure collection-stopped decision from the three store signals — no I/O, so it pins directly.
    /// A NEVER-succeeded server (<paramref name="lastSuccessUtc"/> null) is deliberately NOT flagged by
    /// the staleness backstop: a freshly-added or never-connected server must not be misread as "stopped"
    /// (the connection-lost alert covers that). It IS flagged by the consecutive-failure path if it has
    /// actually run and failed N times.
    /// </summary>
    internal static bool IsCollectionStopped(
        DateTime? lastSuccessUtc, int recentRunCount, int recentSuccessCount, DateTime nowUtc, out string reason)
        => IsCollectionStopped(lastSuccessUtc, recentRunCount, recentSuccessCount, nowUtc, StaleWindow, ConsecutiveFailureThreshold, out reason);

    /// <summary>#2107: the configurable form — the sweep passes the store-backed window and
    /// threshold; the constant overload keeps the shipped defaults for the tests pinning them.</summary>
    internal static bool IsCollectionStopped(
        DateTime? lastSuccessUtc, int recentRunCount, int recentSuccessCount, DateTime nowUtc,
        TimeSpan staleWindow, int consecutiveFailureThreshold, out string reason)
    {
        /* Fast path: the most-recent N runs all failed. */
        if (recentRunCount >= consecutiveFailureThreshold && recentSuccessCount == 0)
        {
            reason = $"The last {recentRunCount.ToString(CultureInfo.InvariantCulture)} collector runs all failed — no data is landing.";
            return true;
        }

        /* Backstop: a server that HAS collected before but hasn't succeeded within the staleness window. */
        if (lastSuccessUtc.HasValue && nowUtc - lastSuccessUtc.Value >= staleWindow)
        {
            int minutes = (int)(nowUtc - lastSuccessUtc.Value).TotalMinutes;
            reason = $"No successful collection in {minutes.ToString(CultureInfo.InvariantCulture)} minutes — the collectors are failing or the server is unreachable.";
            return true;
        }

        reason = "";
        return false;
    }

    /// <summary>
    /// Edge-applies the collection-stopped decision (mirrors the Dashboard's
    /// <c>_activeCollectionStoppedAlert</c>/<c>_lastCollectionStoppedAlert</c>): fire once on entry, re-fire
    /// only after the alert cooldown while it persists, and write ONE "Collection Resumed" history row on
    /// recovery. Testable directly with a recording deliverer + a controllable clock.
    /// </summary>
    internal async Task ApplyCollectionStoppedAsync(
        int serverId, string serverName, bool stopped, string reason, CancellationToken cancellationToken)
    {
        var key = Key(serverId);
        var now = _utcNow();

        if (stopped)
        {
            _activeCollectionStopped[key] = true;
            if (CooldownElapsed(_lastCollectionStoppedAlert, key, now))
            {
                _lastCollectionStoppedAlert[key] = now;
                await FireAsync(
                    key, serverName, "Collection Stopped", reason, "collecting",
                    detail: reason + " A headless service has no dashboard to watch, so this is the primary " +
                        "signal that a server's data has gone stale. Check the service log and the server's " +
                        "reachability, credentials, and collector permissions.",
                    severity: AlertSeverityLevel.Critical,
                    shortMessage: reason,
                    /* #1881: a prose diagnosis, not a measurement — and the two branches' sentences lead
                       with different units (a run count, or minutes). See AlertMetricClassifier.IsStateOnly. */
                    numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                    cancellationToken);
            }
        }
        else if (_activeCollectionStopped.TryRemove(key, out var was) && was)
        {
            await RecordResolutionAsync(new AlertResolution(
                key, serverName, "Collection Stopped",
                "Collection Resumed", $"{serverName}: Data collection is running again"), cancellationToken);
        }
    }

    /// <summary>
    /// Edge-applies capture-down (mirrors the Dashboard's <c>_activeCaptureDownAlert</c>): gated on
    /// blocking OR deadlock alerts being enabled (the alerts this protects — if the operator wants those,
    /// they need to know when the data feeding them stops existing). Fire once on entry, re-fire only after
    /// the cooldown, write ONE "Capture Restored" row on recovery.
    /// </summary>
    internal async Task ApplyCaptureDownAsync(
        int serverId, string serverName, IReadOnlyList<string> missing, CancellationToken cancellationToken)
    {
        if (!_settings.BlockingEnabled && !_settings.DeadlockEnabled)
        {
            return;
        }

        var key = Key(serverId);
        var now = _utcNow();

        if (missing.Count > 0)
        {
            _activeCaptureDown[key] = true;
            if (CooldownElapsed(_lastCaptureDownAlert, key, now))
            {
                _lastCaptureDownAlert[key] = now;
                var list = string.Join(" and ", missing);
                await FireAsync(
                    key, serverName, "Capture Down", list, "session running",
                    detail: $"The {list} Extended Events session(s) are missing and could not be created. " +
                        "Blocking/deadlock data is NOT being captured, so those alerts can never fire. Check the " +
                        "collection log for the SESSION_MISSING detail (usually a permissions problem: " +
                        "ALTER ANY EVENT SESSION on-prem, CREATE ANY DATABASE EVENT SESSION on Azure SQL DB).",
                    severity: AlertSeverityLevel.Critical,
                    shortMessage: $"{list} capture is not running — XE session missing",
                    /* Which capture is missing ("Blocking and Deadlock") against "session running". */
                    numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                    cancellationToken);
            }
        }
        else if (_activeCaptureDown.TryRemove(key, out var was) && was)
        {
            await RecordResolutionAsync(new AlertResolution(
                key, serverName, "Capture Down",
                "Capture Restored", $"{serverName}: Blocking/deadlock capture is running again"), cancellationToken);
        }
    }

    /// <summary>
    /// FLEET-level (not per-server): the tool's OWN collectors regressing in cost ON the monitored servers
    /// (#2674) — the self-monitoring that makes a collector "sticking out" on a target page us instead of
    /// hiding in a log. Reads <c>collect.collector_cost</c> for per-(server, collector) pairs whose latest
    /// day's target-side query time exceeds their own baseline (see the thresholds above), fires once per pair
    /// on entry, re-fires on the cooldown while it stays regressed, and resolves the moment it drops back.
    /// Called once per cycle from the worker's hourly store-metrics tick, AFTER the flush that writes the
    /// latest hour. Testable directly with a recording deliverer + a controllable clock.
    /// </summary>
    public async Task EvaluateCollectorCostAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        List<Mcp.DarlingCollectorCostReader.CostRegression> regressions;
        try
        {
            regressions = await Mcp.DarlingCollectorCostReader.GetCostRegressionsAsync(
                postgres, _utcNow() - CostRegressionBaselineWindow,
                CostRegressionBaselineFloorMs, CostRegressionFactor, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A self-alert read that fails must not take the loop down — it is best-effort telemetry. */
            _logger?.LogDebug(ex, "collector-cost regression evaluation failed");
            return;
        }

        await ApplyCostRegressionsAsync(regressions, cancellationToken);
    }

    /// <summary>The apply half of <see cref="EvaluateCollectorCostAsync"/>, split out so the fire-once /
    /// re-fire / resolve lifecycle is unit-testable with a recording deliverer and a controllable clock,
    /// with the regression set fabricated rather than read from a store.</summary>
    internal async Task ApplyCostRegressionsAsync(
        IReadOnlyList<Mcp.DarlingCollectorCostReader.CostRegression> regressions, CancellationToken cancellationToken)
    {
        var now = _utcNow();
        var current = new HashSet<string>(StringComparer.Ordinal);

        foreach (var regression in regressions)
        {
            var key = $"cost:{regression.ServerId}:{regression.CollectorName}";
            current.Add(key);
            _activeCostRegression[key] = regression.ServerName;

            /* #2707: the reader's own latest_metric_time is the newest collect.collector_cost row folded
               into LatestMs — a cooldown-elapsed re-ask against a hourly flush that hasn't landed a new row
               yet must wait for that row rather than re-fire on a total it already reported. Same shape as
               #2704's collection-time gate on Poison Wait. */
            bool hasFreshDataPoint = !_lastCostRegressionDataPoint.TryGetValue(key, out var lastDataPoint)
                || regression.LatestMetricTime > lastDataPoint;

            if (hasFreshDataPoint && CooldownElapsed(_lastCostRegressionAlert, key, now))
            {
                _lastCostRegressionAlert[key] = now;
                _lastCostRegressionDataPoint[key] = regression.LatestMetricTime;
                var ratio = regression.BaselineMs > 0 ? regression.LatestMs / regression.BaselineMs : 0;
                await FireAsync(
                    key, regression.ServerName, "Collector Cost Regression",
                    currentValue: $"{regression.LatestMs:N0} ms/day",
                    thresholdValue: $"{regression.BaselineMs:N0} ms/day baseline x {CostRegressionFactor:N1}",
                    detail: $"The '{regression.CollectorName}' collector's OWN query time on {regression.ServerName} rose to " +
                        $"{regression.LatestMs:N0} ms/day, {ratio:N1}x its {CostRegressionBaselineWindow.TotalDays:N0}-day baseline of " +
                        $"{regression.BaselineMs:N0} ms/day. This is the MONITORING TOOL's cost on the target, not the server's own " +
                        $"workload - the tool is spending more time on that server than it used to. get_collector_cost with " +
                        $"collector_name={regression.CollectorName} shows the trend.",
                    severity: AlertSeverityLevel.Warning,
                    shortMessage: $"{regression.CollectorName} collection cost on {regression.ServerName} is {ratio:N1}x its baseline",
                    numericCurrentValue: regression.LatestMs, numericThresholdValue: regression.BaselineMs * CostRegressionFactor,
                    cancellationToken);
            }
        }

        /* Resolve any pair that was regressing and no longer is (edge recovery, one history row). */
        foreach (var key in _activeCostRegression.Keys.ToArray())
        {
            if (!current.Contains(key) && _activeCostRegression.TryRemove(key, out var serverName))
            {
                _lastCostRegressionAlert.TryRemove(key, out _);
                _lastCostRegressionDataPoint.TryRemove(key, out _);
                var parts = key.Split(':', 3);
                var collector = parts.Length == 3 ? parts[2] : key;
                await RecordResolutionAsync(new AlertResolution(
                    key, serverName, "Collector Cost Regression",
                    "Cost Regression Cleared", $"{serverName}: {collector} collection cost is back within its baseline"), cancellationToken);
            }
        }
    }

    /// <summary>
    /// Edge-applies "Agent Not Running" (#1433 Phase 2) from the target's latest FRESH agent_status snapshot
    /// (mirrors the sibling per-server conditions' edge shape): fire once on entry, re-fire only after the
    /// alert cooldown while the Agent stays stopped, and write ONE "Agent Restarted" history row on recovery.
    /// <paramref name="agentRunningFresh"/> is null when there is no fresh reading (never collected, or the
    /// snapshot is stale — the collection-stopped alert owns staleness), in which case the standing state is
    /// left untouched so a lagging feed neither fires nor spuriously clears. Gated on the master alerts switch.
    /// Testable directly with a recording deliverer + a controllable clock.
    ///
    /// <para><b>Capability gate (<paramref name="agentEverSeenRunning"/>).</b> A stopped Agent only alerts on a
    /// server where Agent has been observed RUNNING at least once. This alert began life in the full Dashboard,
    /// which collected THROUGH SQL Agent jobs — there, Agent down meant collection down, so it was a
    /// collection-dependency alarm and firing on sight was right. Lite and Darling collect in-process and have no
    /// Agent dependency whatsoever, so all that survives is a customer-WORKLOAD signal: "the jobs you rely on
    /// stopped running". That signal is meaningless on a server that never ran Agent jobs — a container built
    /// with Agent off, Express, a Linux-minimal image — and without this gate every such target nags on every
    /// sweep, forever, with an alert whose remedy ("start the Agent service") the operator deliberately declined.
    /// Observed on the AG fixture containers: identical Critical alerts every 5 minutes from first contact.</para>
    ///
    /// <para>This is the same first-sighting-silent discipline the connection edge below already applies, and the
    /// baseline is durable because it is DERIVED from collected history rather than remembered in process (see
    /// <see cref="HasAgentEverBeenSeenRunningAsync"/>) — a restart must not un-learn that a real server runs
    /// Agent, or a genuinely stopped Agent would go quiet exactly when it matters.</para>
    /// </summary>
    internal async Task ApplyAgentNotRunningAsync(
        int serverId, string serverName, bool? agentRunningFresh, bool agentEverSeenRunning, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        /* No fresh reading — don't judge (neither fire nor clear a standing alert). */
        if (agentRunningFresh is not bool running)
        {
            return;
        }

        var key = Key(serverId);
        var now = _utcNow();

        /* Never seen running: this server does not use Agent, so there is no workload signal to report. Stay
           silent WITHOUT recording a standing down-state, so a later first start is a plain baseline rather than
           a spurious "Agent Restarted" recovery for an alert that never fired. */
        if (!running && !agentEverSeenRunning)
        {
            return;
        }

        if (!running)
        {
            _activeAgentDown[key] = true;
            if (CooldownElapsed(_lastAgentDownAlert, key, now))
            {
                _lastAgentDownAlert[key] = now;
                await FireAsync(
                    key, serverName, "Agent Not Running", "Stopped", "Running",
                    detail: "The SQL Server Agent service on this server is stopped. Scheduled jobs — backups, " +
                        "index and statistics maintenance, integrity checks, log shipping — will NOT run until it " +
                        "is restarted, and a headless service has no dashboard to warn you. Start the SQL Server " +
                        "Agent service and set its startup type to Automatic so it survives a host reboot.",
                    severity: AlertSeverityLevel.Critical,
                    shortMessage: "SQL Server Agent service is stopped — scheduled jobs will not run",
                    /* "Stopped" against "Running". */
                    numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                    cancellationToken);
            }
        }
        else if (_activeAgentDown.TryRemove(key, out var was) && was)
        {
            await RecordResolutionAsync(new AlertResolution(
                key, serverName, "Agent Not Running",
                "Agent Restarted", $"{serverName}: SQL Server Agent service is running again"), cancellationToken);
        }
    }

    /* ---------------- connection lost / restored (connect-edge driven) ---------------- */

    /// <summary>
    /// Applies a connect-attempt outcome for one server and fires the connection edge. Called from the
    /// loop's <see cref="DarlingWorker.TryConnectAsync"/> success (online) and failure (offline) branches.
    /// The per-server state machine fires "Server Unreachable" only on a genuine online→offline transition
    /// and "Server Restored" only on offline→online — a repeated failed reconnect (offline→offline) does
    /// NOT re-fire, and the first-ever outcome (Unknown→online/offline) is a silent baseline, mirroring the
    /// Dashboard's skip-first-check. Both edges are FULL alerts (email/webhook, Dashboard parity — the
    /// restore is not a silent resolution). State is tracked even while alerts are disabled so re-enabling
    /// resumes from the correct baseline; only delivery is gated — on the master switch AND the V20
    /// connection-change notify toggle (<see cref="DarlingAlertSettings.NotifyConnectionChanges"/>).
    /// The delivery portion is failure-isolated (a throwing mute-check can't propagate out of the un-guarded
    /// sweep loop and stop the fleet); the state machine advances first, so isolation never corrupts the edge.
    /// </summary>
    public async Task ApplyConnectionOutcomeAsync(
        int serverId, string serverName, bool online, string? error, CancellationToken cancellationToken)
    {
        var key = Key(serverId);
        var previous = _connectionState.TryGetValue(key, out var s) ? s : ConnectionState.Unknown;
        _connectionState[key] = online ? ConnectionState.Online : ConnectionState.Offline;

        /* Record that collection is now possible for this server this run — arms the collection-stopped
           check (tracked regardless of the alerts switch, so re-enabling has a correct baseline). */
        if (online)
        {
            _hasBeenOnline[key] = true;
        }

        /* The shared policy (#1659) replaces the inline edge machine: same edge-only semantics by default,
           plus the two OPT-INs — announce a server already down on its first-ever attempt, and re-announce a
           standing outage every N minutes. One definition with Lite (ConnectionAlertPolicy), the
           SqlErrorClassification discipline. */
        var decision = ConnectionAlertPolicy.Decide(
            previousOnline: previous switch
            {
                ConnectionState.Online => true,
                ConnectionState.Offline => false,
                _ => null,
            },
            online,
            _notifyConnectionDownAtStartup(),
            _connectionRefireMinutes() is int refire && refire > 0 ? TimeSpan.FromMinutes(refire) : null,
            _lastConnectionDownAlertUtc.TryGetValue(key, out var lastDown) ? lastDown : null,
            _utcNow());

        /* Delivery is gated on the master switch AND the connection-change notify toggle (V20); the state
           machine above already advanced, so toggling either off then back on resumes from the correct
           baseline rather than replaying a stale edge — the same "track always, deliver conditionally"
           posture the master switch already had. (The re-fire clock is stamped only on DELIVERY, below, so a
           suppressed decision does not consume the window.) */
        if (!_settings.AlertsEnabled || !_notifyConnectionChanges())
        {
            return;
        }

        /* Isolate the DELIVERY portion (the state machine already advanced above, so wrapping only the fire can
           never corrupt the edge). FireAsync's pre-deliver mute-check seam (_isAlertMuted → a mute rule's
           Matches()) is NOT internally isolated, and this method is called straight from the un-guarded sweep
           loop (DarlingWorker.TryConnectAsync) — whose OWN catch RE-CALLS this with online:false — so a throwing
           mute-check here would propagate out of that catch and stop collection for the whole fleet. Same
           isolation the sibling self-alerts use (EvaluateStoreAlertsAsync / EvaluateDiskPressureAsync).
           Cancellation still propagates. */
        try
        {
            if (decision == ConnectionAlertDecision.Restored)
            {
                /* Severity null → the shared AlertSeverity map renders "Server Restored" green/RESOLVED. */
                await FireAsync(
                    key, serverName, "Server Restored", "Online", "Online",
                    detail: $"{serverName}: connection restored",
                    severity: null,
                    shortMessage: "connection restored",
                    /* "Online" both sides. */
                    numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                    cancellationToken);
                _lastConnectionDownAlertUtc.TryRemove(key, out _);
            }
            else if (decision is ConnectionAlertDecision.Lost
                     or ConnectionAlertDecision.AlreadyDownAtFirstSight
                     or ConnectionAlertDecision.StillDown)
            {
                /* Every down flavor delivers under the SAME "Server Unreachable" metric name — downstream
                   automation (the #1659 reporter's webhook-driven auto-heal loop) matches on it, and a
                   re-fire exists precisely to re-trigger that match. The detail says which flavor. */
                var reason = string.IsNullOrWhiteSpace(error) ? "Connection failed" : error!;
                var detail = decision switch
                {
                    ConnectionAlertDecision.AlreadyDownAtFirstSight =>
                        $"Already unreachable when monitoring started: {reason}",
                    ConnectionAlertDecision.StillDown =>
                        $"Still unreachable (re-alerting every {_connectionRefireMinutes()} min): {reason}",
                    _ => reason,
                };
                await FireAsync(
                    key, serverName, "Server Unreachable", reason, "Online",
                    detail: detail,
                    severity: AlertSeverityLevel.Critical,
                    shortMessage: reason,
                    /* #1881: the value is the DRIVER'S error message, whose numbers are error codes and
                       timeouts ("Login timeout expired", "error 10060"). Whatever the parser found in one
                       was never a measurement of this server's reachability. */
                    numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                    cancellationToken);
                _lastConnectionDownAlertUtc[key] = _utcNow();
            }
            /* None → steady state or silent baseline. */
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("[{Server}] Connection-change self-alert delivery failed: {Message}", serverName, ex.Message);
        }
    }

    /* ---------------- Availability Group health (#991, store-polled) ---------------- */

    /* The readings, the AgSyncJudgement outcome and the pure decisions (IsFailover, DecideConnection,
       DecideSuspension, JudgeSync) all moved to PerformanceMonitor.Common.AgAlertPolicy so Lite judges by
       the identical rules (#1696). What stays here is what is genuinely Darling-shaped: the per-grain edge
       STATE, the store reads, the cooldown, and delivery through the shared deliverer. */

    /// <summary>
    /// Edge-applies the two REPLICA-grain AG conditions from the latest snapshot — "AG Failover" (role_desc
    /// changed since the previous sweep) and "AG Replica Disconnected"/"AG Replica Reconnected"
    /// (connected_state_desc crossed DISCONNECTED). Both are pure transitions per ag+replica, and the FIRST
    /// sighting of a replica is a silent BASELINE (the <c>ConnectionAlertPolicy</c> discipline): a service that
    /// starts up must not page "failover" simply because it has never seen the role before, nor "disconnected"
    /// for a replica that was already down when monitoring began — a genuine change from a known state is the
    /// signal. A NULL state string is skipped rather than treated as a transition, so WSFC quorum loss (which
    /// nulls the catalog views wholesale) cannot spray alerts for every replica at once.
    /// Gated on the master alerts switch AND <c>notify_ag_health</c>, the sibling gate shape
    /// <see cref="ApplyCaptureDownAsync"/> uses for its blocking/deadlock opt-in. Testable directly with a
    /// recording deliverer + a controllable clock.
    /// </summary>
    internal async Task ApplyAgReplicaHealthAsync(
        int serverId, string serverName, IReadOnlyList<AgReplicaReading> replicas, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled || !_notifyAgHealth())
        {
            return;
        }

        foreach (var replica in replicas)
        {
            /* #1696 fleet-side de-dup: one monitored server judges each AG, so a fully-monitored 3-node AG
               reports a failover ONCE instead of once per node that can see it. A server with a strictly
               better vantage takes over — a secondary yielding to the primary, whose view is the only
               complete one. The state key drops serverId for the same reason: whoever is authoritative
               reads and writes the SAME edge state, so authority moving cannot re-baseline and lose an
               alert, and cannot double-fire one either. */
            if (!IsAuthoritativeFor(serverId, replica.AgName, replicas))
            {
                continue;
            }

            var key = AgReplicaKey(replica.AgName, replica.ReplicaServerName);

            /* --- 1. Failover: the role changed since we last looked. --- */
            if (!string.IsNullOrEmpty(replica.RoleDesc))
            {
                _agReplicaRole.TryGetValue(key, out var previousRole);
                bool failover = AgAlertPolicy.IsFailover(previousRole, replica.RoleDesc);
                _agReplicaRole[key] = replica.RoleDesc;

                if (failover)
                {
                    await FireAsync(
                        Key(serverId), serverName, AgFailoverMetric, replica.RoleDesc, previousRole!,
                        detail: $"Availability Group '{replica.AgName}': replica {replica.ReplicaServerName} changed " +
                            $"role from {previousRole} to {replica.RoleDesc}. A role change is either a failover somebody " +
                            "performed or an automatic one the cluster performed after a health-check timeout — if nobody " +
                            "initiated it, the previous primary had a problem worth finding. Confirm the new primary is the " +
                            "node you want serving the workload, check the WSFC cluster log for the failover reason, and " +
                            "verify that backups, index maintenance and integrity checks run against the new primary.",
                        severity: AlertSeverityLevel.Warning,
                        shortMessage: $"{replica.ReplicaServerName} in AG {replica.AgName} changed role from " +
                            $"{previousRole} to {replica.RoleDesc}",
                        /* Role descs both sides ("SECONDARY" -> "PRIMARY"). */
                        numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                        cancellationToken);
                }
            }

            /* --- 2. Disconnected / reconnected. --- */
            if (!string.IsNullOrEmpty(replica.ConnectedStateDesc))
            {
                _agReplicaConnectedState.TryGetValue(key, out var previousState);

                /* #1696 (V37): "AG Replica Disconnected" was a pure edge, so a replica that stayed
                   disconnected for a week announced it ONCE. The #1659 treatment: re-announce every N
                   minutes while it is still down (0 = off, the shipped default, so nothing starts
                   re-alerting on upgrade). Re-fires deliver under the SAME metric name, because webhook
                   automation keyed on it is exactly what a re-fire exists to re-trigger.

                   #2426 moved the combined decision into the shared policy rather than leaving it as an
                   expression here: Lite grew the same knob, and the parts with sharp corners — a re-fire
                   must not double up with the edge that just fired, and an unrecognized state string must
                   not count as down — are precisely the parts that drift when written twice. What stays
                   here is what is genuinely this app's: the stamp, and the delivery it is stamped on. */
                int refireMinutes = _agDisconnectRefireMinutes();
                var connection = AgAlertPolicy.DecideConnection(
                    previousState,
                    replica.ConnectedStateDesc,
                    refireMinutes > 0 ? TimeSpan.FromMinutes(refireMinutes) : null,
                    _lastAgDisconnectAlert.TryGetValue(key, out var lastDown) ? lastDown : (DateTime?)null,
                    _utcNow());
                _agReplicaConnectedState[key] = replica.ConnectedStateDesc;

                bool stillDisconnected = connection == AgConnectionDecision.StillDisconnected;

                if (connection == AgConnectionDecision.Disconnected || stillDisconnected)
                {
                    /* #2426: the re-fire says so in the DETAIL, not only in the short message. ShortMessage is
                       the interactive toast body and reaches neither the history row nor the email —
                       DarlingAlertDeliverer forwards DetailText — so an operator opening Alert Detail on the
                       sixth re-announcement read text byte-identical to the first notice, which is precisely
                       the "a week-long outage reads like a blip" problem this knob exists to end. The
                       connection re-fire above already bakes it into detail; this is its AG twin, worded to
                       match Lite's so the two SKUs' history rows say the same thing. */
                    var opening = stillDisconnected
                        ? $"Availability Group '{replica.AgName}': replica {replica.ReplicaServerName} is STILL " +
                            $"DISCONNECTED from the primary (re-alerting every {refireMinutes.ToString(CultureInfo.InvariantCulture)} min)."
                        : $"Availability Group '{replica.AgName}': replica {replica.ReplicaServerName} is DISCONNECTED " +
                            "from the primary.";

                    await FireAsync(
                        Key(serverId), serverName, AgReplicaDisconnectedMetric, replica.ConnectedStateDesc, "CONNECTED",
                        detail: opening +
                            " A disconnected replica receives no log at all, so it falls " +
                            "further behind every second and cannot be failed over to without losing whatever the primary " +
                            "has committed since. If it is a synchronous-commit replica, the primary also loses its " +
                            "automatic-failover partner. Check the replica's SQL Server service, the availability endpoint " +
                            "(TCP 5022 by default) and its firewall rule, the WSFC quorum, and the network between the nodes.",
                        severity: AlertSeverityLevel.Critical,
                        shortMessage: stillDisconnected
                            ? $"{replica.ReplicaServerName} in AG {replica.AgName} is STILL disconnected from the primary"
                            : $"{replica.ReplicaServerName} in AG {replica.AgName} is disconnected from the primary",
                        /* Connected-state descs both sides ("DISCONNECTED" against "CONNECTED"). */
                        numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                        cancellationToken);

                    /* Stamped on DELIVERY, never on the decision: an alert suppressed by the master switch
                       must not consume the re-fire window (the #1659 discipline). */
                    _lastAgDisconnectAlert[key] = _utcNow();
                }
                else if (connection == AgConnectionDecision.Reconnected)
                {
                    /* Severity null → the shared AlertSeverity map renders the reconnect green/RESOLVED, the same
                       treatment "Server Restored" gets on the connect edge. */
                    await FireAsync(
                        Key(serverId), serverName, AgReplicaReconnectedMetric, replica.ConnectedStateDesc, "CONNECTED",
                        detail: $"Availability Group '{replica.AgName}': replica {replica.ReplicaServerName} is connected " +
                            "to the primary again. It is still behind by whatever accumulated while it was gone — watch the " +
                            "send and redo queues until they drain before you count it as a failover target again.",
                        severity: null,
                        shortMessage: $"{replica.ReplicaServerName} in AG {replica.AgName} reconnected",
                        numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                        cancellationToken);
                    _lastAgDisconnectAlert.TryRemove(key, out _);
                }
            }
        }
    }

    /// <summary>
    /// Applies the two DATABASE-grain AG conditions from the latest snapshot, per ag+database+replica:
    /// <list type="bullet">
    /// <item><b>AG Database Suspended</b> — a pure <c>is_suspended</c> false→true transition (first sighting is
    /// a silent baseline), carrying <c>suspend_reason_desc</c>. Recovery writes one "AG Data Movement Resumed"
    /// history row, the sibling conditions' recovery shape.</item>
    /// <item><b>AG Sync Fell Behind</b> — a standing CONDITION rather than an edge, so it takes the evaluator's
    /// active-flag + <see cref="CooldownElapsed"/> idiom: fire once when a database starts breaching, re-fire
    /// only after the alert cooldown while it keeps breaching, and write one "AG Sync Recovered" row when it
    /// catches up. Each database tracks independently, so a second database falling behind fires on its own
    /// merits instead of hiding behind the first.</item>
    /// </list>
    /// Recovery fires only for a database this sweep MEASURED as caught up (see
    /// <see cref="AgSyncJudgement"/>) — never merely for one that stopped breaching, which is also what a
    /// suspend or a quorum-loss NULL looks like. Gated on the master alerts switch AND <c>notify_ag_health</c>.
    /// <para>State for a replica or database that disappears from the snapshot entirely (removed from the AG)
    /// is deliberately left in place rather than swept: an absent row is no signal, the same as a stale one,
    /// and <see cref="Forget"/> clears everything when the server leaves the monitored set.</para>
    /// Testable directly with a recording deliverer + a controllable clock.
    /// </summary>
    internal async Task ApplyAgDatabaseHealthAsync(
        int serverId, string serverName, IReadOnlyList<AgDatabaseReading> databases, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled || !_notifyAgHealth())
        {
            return;
        }

        var now = _utcNow();
        int lagThresholdSeconds = _agLagAlertSeconds();
        long redoThresholdKb = _agRedoQueueAlertKb();
        var measuredCaughtUp = new HashSet<string>(StringComparer.Ordinal);

        foreach (var database in databases)
        {
            /* Same #1696 de-dup as the replica grain: without it a fully-monitored 3-node AG reports the
               same database's lag once per node. */
            if (!IsAuthoritativeFor(serverId, database.AgName))
            {
                continue;
            }

            var key = AgDatabaseKey(database.AgName, database.DatabaseName, database.ReplicaServerName);

            /* --- 3. Data movement suspended (edge). --- */
            if (database.IsSuspended is bool suspended)
            {
                bool seen = _agDatabaseSuspended.TryGetValue(key, out var wasSuspended);
                var suspension = AgAlertPolicy.DecideSuspension(seen ? wasSuspended : null, suspended);
                _agDatabaseSuspended[key] = suspended;

                if (suspension == AgSuspensionDecision.Suspended)
                {
                    var suspendReason = string.IsNullOrWhiteSpace(database.SuspendReasonDesc)
                        ? "no reason reported"
                        : database.SuspendReasonDesc!;
                    await FireAsync(
                        Key(serverId), serverName, AgDatabaseSuspendedMetric, suspendReason, "SYNCHRONIZING",
                        detail: $"Availability Group '{database.AgName}': data movement for database " +
                            $"{database.DatabaseName} on replica {database.ReplicaServerName} is SUSPENDED " +
                            $"({suspendReason}). While movement is suspended the secondary receives nothing AND the " +
                            "primary cannot truncate its transaction log, so the primary's log grows until its disk " +
                            "fills — this is a primary-side outage risk, not just a secondary-side one. Fix the " +
                            "underlying cause, then resume it with ALTER DATABASE " +
                            $"[{database.DatabaseName}] SET HADR RESUME.",
                        severity: AlertSeverityLevel.Warning,
                        shortMessage: $"Data movement for {database.DatabaseName} in AG {database.AgName} is " +
                            $"suspended ({suspendReason})",
                        /* suspend_reason_desc against "SYNCHRONIZING". */
                        numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                        cancellationToken,
                        context: AgDatabaseContext(database, ("Suspend Reason", suspendReason)));
                }
                else if (suspension == AgSuspensionDecision.Resumed)
                {
                    await RecordResolutionAsync(new AlertResolution(
                        Key(serverId), serverName, AgDatabaseSuspendedMetric,
                        "AG Data Movement Resumed",
                        $"{serverName}: data movement for {database.DatabaseName} in AG {database.AgName} on replica " +
                        $"{database.ReplicaServerName} has resumed"), cancellationToken);
                }
            }

            /* --- 4. Sync fell behind (standing condition). --- */
            var judgement = AgAlertPolicy.JudgeSync(database, lagThresholdSeconds, redoThresholdKb, out var behindReason);
            if (judgement == AgSyncJudgement.CaughtUp)
            {
                measuredCaughtUp.Add(key);
            }
            else if (judgement == AgSyncJudgement.Behind)
            {
                _activeAgSyncBehind[key] = true;
                if (CooldownElapsed(_lastAgSyncBehindAlert, key, now))
                {
                    _lastAgSyncBehindAlert[key] = now;
                    await FireAsync(
                        Key(serverId), serverName, AgSyncFellBehindMetric, behindReason, "caught up",
                        detail: behindReason + " A secondary that trails the primary is a data-loss window: an " +
                            "automatic failover cannot complete until it catches up, and a forced failover throws away " +
                            "everything still queued. Look at the network throughput between the replicas, the " +
                            "secondary's redo thread (it is single-threaded per database on older versions and is " +
                            "easily starved by CPU or storage latency on the secondary), and whether something on the " +
                            "primary — an index rebuild, a bulk load, a long transaction — is generating log faster " +
                            "than the secondary can consume it. Read the figures for what they measure: the lag " +
                            "seconds are how STALE the secondary's last hardened log is, not how much data is queued " +
                            "behind it, so on a quiet group a large value can simply mean nothing has been written " +
                            "recently. While data movement is suspended the redo queue is frozen at its last reading, " +
                            "and log_send_queue_size — the actual backlog measure — reports nothing at all.",
                        severity: AlertSeverityLevel.Warning,
                        shortMessage: behindReason,
                        /* #1881: JudgeSync's reason is prose that breaches on EITHER lag seconds OR redo-queue
                           KB, so the number the parser lifted out of it meant seconds on some rows and
                           kilobytes on others — and on an AG or database whose name carries a digit
                           ("Sales2024"), neither. #1846 already classified this metric state-only for exactly
                           that reason; this is the write side finally agreeing with it. */
                        numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                        cancellationToken,
                        context: AgDatabaseContext(database));
                }
            }
        }

        /* Recovery is driven off the databases this sweep MEASURED as caught up — never off "everything tracked
           that did not breach". Those are different sets, and the difference is the bug: a lagging database that
           becomes SUSPENDED, or whose columns go NULL under quorum loss, stops breaching without recovering, so
           the looser rule would announce "has caught up with the primary" in the same sweep that reports it
           suspended. Iterating the measured set also makes cross-server resolution structurally impossible —
           every key in it came from THIS server's snapshot — rather than something a prefix check has to catch. */
        foreach (var key in measuredCaughtUp)
        {
            _lastAgSyncBehindAlert.TryRemove(key, out _);
            if (_activeAgSyncBehind.TryRemove(key, out _))
            {
                await RecordResolutionAsync(new AlertResolution(
                    Key(serverId), serverName, AgSyncFellBehindMetric,
                    "AG Sync Recovered",
                    $"{serverName}: {DescribeAgDatabaseKey(key)} has caught up with the primary"), cancellationToken);
            }
        }
    }

    /// <summary>The #2109 discrete-facts context for a database-scoped AG alert — the shared
    /// builder keyed off the reading, so the fact names cannot drift from Lite's.</summary>
    private static AlertContext AgDatabaseContext(AgDatabaseReading database, params (string, string)[] extras)
        => AgAlertContexts.ForDatabase(database.DatabaseName, database.AgName, database.ReplicaServerName, extras);

    /// <summary>The prefix every AG state key for one server starts with — the scope for
    /// <see cref="Forget"/> and for the per-server recovery sweep.</summary>
    /* AG edge state is keyed by the AG GRAIN ALONE, deliberately without the serverId (#1696). An AG is one
       object no matter how many of its nodes we monitor, so whichever server is authoritative reads and
       writes the same state — which is what makes authority able to move (a secondary yielding to the
       primary) without re-baselining and losing an alert, or double-firing one. */
    private static string AgReplicaKey(string agName, string replicaServerName) =>
        agName + AgKeySeparator + replicaServerName;

    private static string AgDatabaseKey(string agName, string databaseName, string replicaServerName) =>
        agName + AgKeySeparator + databaseName + AgKeySeparator + replicaServerName;

    /// <summary>
    /// Whether <paramref name="serverId"/> is the server that judges <paramref name="agName"/> right now.
    /// The first server to report an AG claims it; a server with a STRICTLY better vantage takes over, so a
    /// secondary's one-row self-view yields to the primary's complete one as soon as the primary is
    /// monitored. Ties keep the incumbent, so authority does not oscillate between equally-placed nodes.
    /// </summary>
    private bool IsAuthoritativeFor(int serverId, string agName, IReadOnlyList<AgReplicaReading> snapshot)
    {
        var vantage = AgAlertPolicy.ClassifyVantage(snapshot, agName);
        var claimed = _agAuthority.AddOrUpdate(
            agName,
            _ => (serverId, vantage),
            (_, current) => current.ServerId == serverId
                ? (serverId, vantage)
                : (vantage > current.Vantage ? (serverId, vantage) : current));

        return claimed.ServerId == serverId;
    }

    /// <summary>
    /// The database-grain check. The replica grain runs first in the same sweep and has normally already
    /// decided who owns this AG, so this defers to the incumbent rather than re-classifying. When nothing has
    /// claimed the AG — the replica-grain snapshot was missing or stale while the database one is fresh — the
    /// caller claims it at the weakest vantage, so the group is still judged by SOMEBODY rather than by
    /// nobody. Judging once from a poor vantage beats silence; that is the direction that keeps alerts.
    /// </summary>
    private bool IsAuthoritativeFor(int serverId, string agName)
    {
        var claimed = _agAuthority.GetOrAdd(agName, _ => (serverId, AgVantage.Remote));
        return claimed.ServerId == serverId;
    }

    /// <summary>Renders a database-grain AG key back into prose for a recovery message. The key is built here
    /// and never escaped, so this is a straight positional split; an unexpected shape degrades to the raw key
    /// rather than throwing inside a recovery write.</summary>
    private static string DescribeAgDatabaseKey(string key)
    {
        var parts = key.Split(AgKeySeparator);
        return parts.Length == 3
            ? $"database {parts[1]} in AG {parts[0]} on replica {parts[2]}"
            : key;
    }

    /* ---------------- store disk pressure (fleet-level, polled) ---------------- */

    /// <summary>
    /// Pure disk-pressure decision: the store volume is under pressure when its FREE space is below
    /// <see cref="DiskFreeWarnPercent"/> of the volume total. No I/O, so it pins directly. A non-positive
    /// total is treated as "can't tell" (false — the caller also guards this). The percentage scales across
    /// disk sizes; see the constant for why it is percent-only.
    /// <para><paramref name="percentFree"/> is the measurement the alert is ABOUT, handed back so the fire
    /// site can store it as a real numeric instead of leaving the history store to find it again by
    /// scanning <paramref name="reason"/> for digits (#1881). It is computed whenever the total is usable,
    /// including when the volume is comfortable and the answer is false — the caller only uses it on the
    /// firing path, but returning a number that is 0 for "no pressure" and 0 for "a full volume" would put
    /// the one dangerous ambiguity this metric must never have back into the signature.</para>
    /// </summary>
    internal static bool IsDiskPressure(long freeBytes, long totalBytes, out string reason, out double percentFree)
        => IsDiskPressure(freeBytes, totalBytes, DiskFreeWarnPercent, out reason, out percentFree);

    /// <summary>#2107: the configurable form — the sweep passes the store-backed
    /// <c>SelfDiskFreeWarnPercent</c>; the constant-threshold overload keeps the shipped default
    /// for the tests pinning it.</summary>
    internal static bool IsDiskPressure(long freeBytes, long totalBytes, double warnPercent, out string reason, out double percentFree)
    {
        if (totalBytes <= 0)
        {
            reason = "";
            percentFree = 0;
            return false;
        }

        percentFree = (double)freeBytes / totalBytes * 100.0;
        if (percentFree < warnPercent)
        {
            reason = $"The monitor store's disk volume has only {percentFree.ToString("0.#", CultureInfo.InvariantCulture)}% free ({FormatGb(freeBytes)} of {FormatGb(totalBytes)}).";
            return true;
        }

        reason = "";
        return false;
    }

    /// <summary>
    /// The isolating entry point the worker's disk-pressure sweep calls — the fleet-level twin of
    /// <see cref="EvaluateStoreAlertsAsync"/> for the store-polled conditions. Wraps
    /// <see cref="ApplyDiskPressureAsync"/> in the SAME failure isolation the sibling store-alerts use, so a
    /// throwing seam — most notably the pre-deliver mute check (<c>_isAlertMuted</c> → a mute rule's
    /// <c>Matches</c>), which unlike Deliver/RecordResolution is NOT internally isolated — can never propagate
    /// out of the (otherwise un-guarded) collection sweep loop and stop collection for the whole fleet.
    /// Cancellation still propagates.
    /// </summary>
    public async Task EvaluateDiskPressureAsync(
        long? freeBytes, long? totalBytes, long? storeSizeBytes, CancellationToken cancellationToken)
    {
        try
        {
            await ApplyDiskPressureAsync(freeBytes, totalBytes, storeSizeBytes, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Store disk-pressure self-alert failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Edge-applies the fleet-level Store Disk Pressure condition from a store-volume sample: fire once on
    /// entry, re-fire only after the alert cooldown while it persists, and write ONE "Store Disk Pressure
    /// Resolved" history row on recovery (mirrors the per-server conditions' edge shape). Gated on the master
    /// alerts switch. NO-OPS when free/total are null — a remote BYO store whose volume the service can't see —
    /// so it never false-alarms; the managed store's own volume is what it exists to protect.
    /// <paramref name="storeSizeBytes"/> (pg_database_size) is context for the alert text only, never the
    /// trigger. Internal (tested directly, like the sibling Apply methods); the worker calls the isolating
    /// <see cref="EvaluateDiskPressureAsync"/>. Testable directly with a recording deliverer + a controllable clock.
    /// </summary>
    internal async Task ApplyDiskPressureAsync(
        long? freeBytes, long? totalBytes, long? storeSizeBytes, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        /* Can't determine the store volume's free space (remote BYO store, or the drive was not ready) — no
           signal, so neither fire nor clear a standing alert. */
        if (freeBytes is not long free || totalBytes is not long total || total <= 0)
        {
            return;
        }

        var now = _utcNow();
        /* #2107: store-backed threshold (clamped on read); the constant remains only as the
           shipped default. */
        double warnPercent = _settings.SelfDiskFreeWarnPercent;
        bool pressure = IsDiskPressure(free, total, warnPercent, out var reason, out var percentFree);

        if (pressure)
        {
            _activeDiskPressure[DiskKey] = true;

            /* #2101: a standing breach at an UNCHANGED level must not re-notify every cooldown — a
               store volume parked at 7% free is one condition, not a condition per 15 minutes. The
               same #754 worsening gate the target-server volume alert runs behind: fire on entry,
               re-fire only when free% has dropped at least the margin below the last-alerted level
               (still cooldown-limited), one resolution on recovery. This is THE self-alert with a
               real measurement, which is what makes the gate fit here and deliberately NOT on the
               state-only siblings (Collection Stopped / Agent Not Running / Capture Down) — those
               have no level to worsen, and their per-cooldown "still broken" reminder is wanted. */
            double? lastAlertedPercent =
                _lastAlertedDiskPressurePercent.TryGetValue(DiskKey, out var lastPct) ? lastPct : (double?)null;
            if (LowDiskAlertGate.ShouldAlert(percentFree, lastAlertedPercent)
                && CooldownElapsed(_lastDiskPressureAlert, DiskKey, now))
            {
                _lastDiskPressureAlert[DiskKey] = now;
                _lastAlertedDiskPressurePercent[DiskKey] = percentFree;
                var storeText = storeSizeBytes is long size ? $" The store currently holds {FormatGb(size)}." : "";
                await FireAsync(
                    DiskKey, "Monitor Store", "Store Disk Pressure", reason,
                    $"{warnPercent.ToString("0.#", CultureInfo.InvariantCulture)}% free",
                    detail: reason + storeText + " When the store volume fills, collection and every write stop " +
                        "for the WHOLE fleet, and a headless service has no dashboard to warn you. Free space on the " +
                        "store volume, shorten retention (config_collector_schedules), enable TimescaleDB compression, " +
                        "or move the store to a larger disk.",
                    severity: AlertSeverityLevel.Critical,
                    shortMessage: reason,
                    /* #1881: THE ONE SELF-ALERT WITH A REAL MEASUREMENT, and the one metric
                       AlertMetricClassifier.IsStateOnly must never list — percent-free is genuinely what
                       this alert is about, and a genuine 0 means a FULL volume. It stored the right number
                       before only because its sentence happens to reach the percent first; passing it
                       explicitly means an operator's volume path ("D2:\\") can no longer get there first,
                       and the stored value stops depending on prose word order. The threshold is a real
                       bound too, which is what separates this metric from every sibling above. */
                    numericCurrentValue: percentFree, numericThresholdValue: warnPercent,
                    cancellationToken);
            }
        }
        else if (_activeDiskPressure.TryRemove(DiskKey, out var was) && was)
        {
            _lastAlertedDiskPressurePercent.TryRemove(DiskKey, out _);
            await RecordResolutionAsync(new AlertResolution(
                DiskKey, "Monitor Store", "Store Disk Pressure",
                "Store Disk Pressure Resolved", "Monitor store volume free space recovered"), cancellationToken);
        }
    }

    /* ---------------- compression-job self-heal (fleet-level, polled — #1581) ---------------- */

    /// <summary>
    /// The alert metric name the store runtime upgrade fires under (#1706). A WEBHOOK AUTOMATION KEY like
    /// its siblings, so it is a const and must stay stable across releases.
    /// </summary>
    internal const string StoreUpgradeMetric = "Store Runtime Upgrade";

    /// <summary>Fleet-level key, non-numeric so it never collides with a real server_id (the DiskKey shape).</summary>
    private const string StoreUpgradeKey = "storeupgrade";

    /// <summary>
    /// What one service start's store runtime upgrade did — a platform-neutral copy of the Windows-only
    /// bootstrap's outcome, so the alert path does not have to reference a Windows-attributed type.
    /// </summary>
    internal sealed record StoreUpgradeReport(
        bool Succeeded,
        int FromMajor,
        int ToMajor,
        string? FromTimescale,
        string? ToTimescale,
        string? FailedStep,
        string? FailureMessage,
        bool WithoutRollbackCopy);

    /// <summary>
    /// Reports the outcome of a store runtime upgrade, ONCE per service start (#1706). Unlike every sibling
    /// condition this is not an edge machine: an upgrade is a discrete event that already happened, so there
    /// is no state to track and nothing to recover from — it is fired at the first opportunity the alert
    /// engine exists and never re-evaluated.
    ///
    /// <para>The timing is forced and worth stating: the store is DOWN while an upgrade runs, so the START of
    /// one can only ever be a log line. Both terminal states happen with a live store — a success on the new
    /// major, a failure back on the old one — which is exactly why the outcome is carried out of the
    /// bootstrap and raised here rather than attempted from inside it.</para>
    /// </summary>
    public async Task EvaluateStoreUpgradeAsync(StoreUpgradeReport report, CancellationToken cancellationToken)
    {
        if (report is null || !_settings.AlertsEnabled)
        {
            return;
        }

        try
        {
            var timescale = string.IsNullOrEmpty(report.ToTimescale)
                ? string.Empty
                : $" TimescaleDB {report.FromTimescale ?? "(none)"} -> {report.ToTimescale}.";

            if (report.Succeeded)
            {
                /* A post-commit bookkeeping failure arrives as Succeeded WITH a message. When it is present
                   the reassuring rollback sentence is not merely incomplete, it is wrong — the retention
                   marker is the very thing that failed — so it is replaced rather than appended to, and the
                   severity escalates to Critical. An operator receiving "upgraded, all tidy" while the log
                   says otherwise is the failure this whole review round was about. */
                var degraded = !string.IsNullOrWhiteSpace(report.FailureMessage);

                var rollback = degraded
                    /* The retention counter is DESIGNED to tolerate a missing marker — the sweep reads an
                       absent counter as 1, so the copy simply ages out one service start later than usual.
                       Saying it never ages out would send an operator to delete a multi-gigabyte directory by
                       hand for no reason. What IS true, and is the actual signal: the countdown cannot
                       advance while whatever blocked the write is still blocking it, because the sweep's own
                       counter write fails the same way. */
                    ? $" BUT post-upgrade bookkeeping did NOT complete: {report.FailureMessage}. The store itself is fine and running on PostgreSQL {report.ToMajor} — this is about cleanup, not data. The retained copy's countdown simply starts on the next service start, so it ages out one start later than usual; it only stays put if whatever blocked the write is still blocking it. Check free disk space on the store volume, clear the underlying problem, and remove the directory by hand only if it is still there after a couple of starts."
                    : report.WithoutRollbackCopy
                        ? " The upgrade ran in hard-link mode, so there is NO rollback copy of the pre-upgrade store — the only way back is a restore from backup."
                        : " The pre-upgrade data directory is kept as a rollback copy for the next couple of service starts, then deleted automatically.";

                await FireAsync(
                    StoreUpgradeKey, "Monitor Store", StoreUpgradeMetric,
                    degraded ? $"PostgreSQL {report.ToMajor} (cleanup incomplete)" : $"PostgreSQL {report.ToMajor}",
                    $"PostgreSQL {report.FromMajor}",
                    detail: $"The monitor's own store was upgraded in place from PostgreSQL {report.FromMajor} to {report.ToMajor}.{timescale} " +
                        "Collection was paused for the duration and has resumed. The upgraded store was verified before this alert: server version, " +
                        "TimescaleDB extension version, and a real read of a collector table." + rollback,
                    /* Warning for a clean upgrade — the store was still OFFLINE for minutes and there is a
                       corresponding hole in every collector's history, which is worth attention even though
                       nothing went wrong. CRITICAL when bookkeeping failed, because that one needs somebody
                       to actually go and look. */
                    severity: degraded ? AlertSeverityLevel.Critical : AlertSeverityLevel.Warning,
                    shortMessage: degraded
                        ? $"store upgraded to PostgreSQL {report.ToMajor}, but post-upgrade cleanup did NOT complete"
                        : $"store upgraded to PostgreSQL {report.ToMajor}",
                    /* #1881: both sides are PostgreSQL MAJOR VERSIONS ("PostgreSQL 18" against "PostgreSQL
                       17"), which the history store used to record as this alert's current value and
                       threshold — a column of measurements in which some of the measurements were 18. A
                       version is an identity, not a quantity, and an in-place store upgrade measures
                       nothing; the versions stay in the text, where they read as versions. */
                    numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                    cancellationToken);
                return;
            }

            await FireAsync(
                StoreUpgradeKey, "Monitor Store", StoreUpgradeMetric,
                $"PostgreSQL {report.FromMajor} (upgrade failed)", $"PostgreSQL {report.ToMajor}",
                detail: $"The monitor's own store FAILED to upgrade from PostgreSQL {report.FromMajor} to {report.ToMajor}, at step '{report.FailedStep}': {report.FailureMessage} " +
                    $"The store reverted to PostgreSQL {report.FromMajor} and is collecting normally — no data was lost, because the pre-upgrade data directory is never modified until the upgrade succeeds. " +
                    "The service will NOT retry this same package automatically, so the store stays on its current major until someone acts. " +
                    "Investigate before the next release: a store that cannot move forward accumulates the version drift this machinery exists to end.",
                severity: AlertSeverityLevel.Critical,
                shortMessage: $"store upgrade to PostgreSQL {report.ToMajor} FAILED at {report.FailedStep} — reverted, still running",
                /* Versions again, on the failure path — see the success branch above. */
                numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Store runtime upgrade self-alert failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// The isolating entry point the worker's hourly compression-job health sweep calls — the fleet-level twin
    /// of <see cref="EvaluateDiskPressureAsync"/>. Wraps <see cref="ApplyCompressionJobsStuckAsync"/> in the SAME
    /// failure isolation the sibling store-alerts use, so a throwing seam — the pre-deliver mute check, or the
    /// caller-supplied re-arm delegate — can never propagate out of the (otherwise un-guarded) collection sweep
    /// loop and stop collection for the whole fleet. Cancellation still propagates.
    /// </summary>
    public async Task EvaluateCompressionJobsAsync(
        IReadOnlyList<StuckCompressionJob> stuckJobs,
        Func<long, Task<bool>> rearmAsync,
        CancellationToken cancellationToken)
    {
        try
        {
            await ApplyCompressionJobsStuckAsync(stuckJobs, rearmAsync, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Compression-job health self-alert failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// The isolating entry point for the #2136 Store Job Over Cadence check — rides the worker's hourly
    /// compression-job health sweep (same connection, same Timescale gate). Same failure isolation as
    /// <see cref="EvaluateCompressionJobsAsync"/>; cancellation still propagates.
    /// </summary>
    public async Task EvaluateStoreJobCadenceAsync(
        IReadOnlyList<StoreJobCadenceReading> jobs, CancellationToken cancellationToken)
    {
        try
        {
            await ApplyStoreJobCadenceAsync(jobs, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Store-job cadence self-alert failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Applies the fleet-level Store Job Over Cadence condition (#2136) from the latest job readings: a
    /// background job whose last SUCCESSFUL run consumed at least the warning share of its own schedule
    /// interval is living too close to its ceiling — these runtimes scale SERIALLY with raw volume (the
    /// finalize hash-aggregate runs in one process), so an onboarding wave moves them first, and a job
    /// that outgrows its cadence compounds refresh lag silently. Tiers: WARNING at the store-backed knob
    /// (V57, default 25), CRITICAL fixed at 100 — past 100 the job is still running when its next run is
    /// due, which is no longer "close to" the ceiling but through it. A STANDING condition (the AG Sync
    /// Fell Behind idiom): fire once on breach, re-fire only after the alert cooldown while it persists,
    /// one "Store Job Cadence Recovered" resolution row when a later run comes back under the warning
    /// threshold. A job with no schedule interval or no completed run yet has no cadence to breach and is
    /// skipped without touching its standing state (no signal, the agent-status discipline). Gated on the
    /// master alerts switch. Internal so it pins directly with a recording deliverer + controllable clock.
    /// </summary>
    internal async Task ApplyStoreJobCadenceAsync(
        IReadOnlyList<StoreJobCadenceReading> jobs, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        var now = _utcNow();
        int warnPercent = _storeJobCadenceWarnPercent();

        foreach (var job in jobs)
        {
            if (job.ScheduleIntervalMs <= 0 || job.LastRunDurationMs is not long durationMs)
            {
                continue;
            }

            var key = job.JobId.ToString(CultureInfo.InvariantCulture);
            double percent = 100.0 * durationMs / job.ScheduleIntervalMs;
            var label = string.IsNullOrEmpty(job.JobName) ? $"job {key}" : $"{job.JobName} [{key}]";

            if (percent >= warnPercent)
            {
                _activeJobOverCadence[key] = true;
                if (CooldownElapsed(_lastJobOverCadenceAlert, key, now))
                {
                    _lastJobOverCadenceAlert[key] = now;
                    bool critical = percent >= 100.0;
                    await FireAsync(
                        JobCadenceKeyPrefix + key, "Monitor Store", JobCadenceMetric,
                        $"{percent:F0}% of schedule interval", $"{warnPercent}%",
                        detail: $"Store background {label} last ran for {durationMs / 1000.0:F0}s against a " +
                            $"{job.ScheduleIntervalMs / 1000.0:F0}s schedule interval ({percent:F0}%). " +
                            (critical
                                ? "The job now takes at least as long as its own cadence, so runs back up behind each " +
                                  "other and everything it maintains (continuous-aggregate freshness, compression, " +
                                  "retention) falls further behind every cycle. "
                                : "These runtimes scale with raw data volume, so this is the early warning that the " +
                                  "store is outgrowing its job schedule — an onboarding wave moves this number first. ") +
                            "Compare the job's duration series in collect.store_metrics (object_kind = " +
                            "'background_job') to see the trend, and either reduce raw volume, extend the job's " +
                            "schedule_interval deliberately, or scale the store host.",
                        severity: critical ? AlertSeverityLevel.Critical : AlertSeverityLevel.Warning,
                        shortMessage: $"{label} ran {percent:F0}% of its schedule interval",
                        numericCurrentValue: Math.Round(percent, 1),
                        numericThresholdValue: critical ? 100 : warnPercent,
                        cancellationToken);
                }
            }
            else if (_activeJobOverCadence.TryRemove(key, out var was) && was)
            {
                await RecordResolutionAsync(new AlertResolution(
                    JobCadenceKeyPrefix + key, "Monitor Store", JobCadenceMetric,
                    "Store Job Cadence Recovered",
                    $"Monitor Store: {label} is back under {warnPercent}% of its schedule interval"), cancellationToken);
            }
        }
    }

    /// <summary>
    /// Edge-applies the fleet-level compression-job self-heal machine (#1581) from the set of currently-stuck
    /// compression jobs the worker detected. Per job, in one check:
    /// <list type="bullet">
    /// <item><b>First detection</b> — re-arm it ONCE (<paramref name="rearmAsync"/> → <c>alter_job</c>), then
    /// FIRE a CRITICAL "self-healed" alert. If the re-arm itself fails (usually a permission problem the service
    /// cannot fix), go straight to Escalated and FIRE an "auto-re-arm FAILED" alert instead — so we neither loop
    /// alter_job nor stay silent.</item>
    /// <item><b>Re-hang</b> (still stuck after a prior self-heal) — ESCALATE: FIRE a CRITICAL "re-hung after
    /// self-heal" alert and STOP re-arming (looping alter_job on a job that re-hangs is a product-bug signal for
    /// a human).</item>
    /// <item><b>Already escalated</b> — never re-arm; re-fire the CRITICAL only on the alert cooldown.</item>
    /// </list>
    /// A job that is no longer stuck has RECOVERED: its state is dropped and one "Compression Job Recovered"
    /// resolution row is written (the sibling conditions' edge shape). Gated on the master alerts switch.
    /// Re-arm happens at most ONCE per job per check (only on the first-detection transition). Internal so it
    /// pins directly with a recording deliverer, a controllable clock, and a fake re-arm delegate.
    /// </summary>
    internal async Task ApplyCompressionJobsStuckAsync(
        IReadOnlyList<StuckCompressionJob> stuckJobs,
        Func<long, Task<bool>> rearmAsync,
        CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        var now = _utcNow();
        var stillStuck = new HashSet<string>(StringComparer.Ordinal);

        foreach (var job in stuckJobs)
        {
            var key = job.JobId.ToString(CultureInfo.InvariantCulture);
            stillStuck.Add(key);

            var label = string.IsNullOrEmpty(job.HypertableName)
                ? $"compression job {key}"
                : $"compression job {key} on {job.HypertableName}";

            if (!_compressionJobState.TryGetValue(key, out var state))
            {
                /* First detection this episode: re-arm ONCE, then alert on the outcome. */
                bool rearmed = await rearmAsync(job.JobId);
                _lastCompressionJobAlert[key] = now;
                if (rearmed)
                {
                    _compressionJobState[key] = CompressionJobHealth.ReArmed;
                    await FireAsync(
                        CompressionKeyPrefix + key, "Monitor Store", CompressionJobMetric,
                        job.Reason, "running on schedule",
                        detail: $"TimescaleDB {label} was stuck ({job.Reason}) and has been automatically re-armed " +
                            "(alter_job next_start => now). A stuck compression policy halts the store's archival tier, so " +
                            "uncompressed data grows without bound until the disk fills and collection stops for the WHOLE " +
                            "fleet, and a headless service has no dashboard to warn you. If it re-hangs the service will " +
                            "escalate and stop auto-re-arming — investigate the TimescaleDB background-worker health.",
                        severity: AlertSeverityLevel.Critical,
                        shortMessage: $"{label} was stuck — auto-re-armed",
                        /* #1881: job.Reason is elapsed minutes when a run HUNG and a scheduler state with no
                           duration at all when next_start is -infinity, so the stored number meant minutes on
                           some rows and nothing on others. The minutes stay in the reason text and the detail,
                           where the unit is spelled out. Threshold is the phrase "running on schedule". */
                        numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                        cancellationToken);
                }
                else
                {
                    /* The re-arm failed — usually the store login does not own the job. Treat as escalated so we
                       never loop alter_job on it, and page: a human must re-arm it (or grant ownership). */
                    _compressionJobState[key] = CompressionJobHealth.Escalated;
                    await FireAsync(
                        CompressionKeyPrefix + key, "Monitor Store", CompressionJobMetric,
                        job.Reason, "running on schedule",
                        detail: $"TimescaleDB {label} is stuck ({job.Reason}) and the service could NOT re-arm it — " +
                            "alter_job failed, usually because the store login does not own the job. Compression is halted, " +
                            "so the store will grow without bound until the disk fills. Re-arm it manually as the store owner " +
                            "(SELECT alter_job(<job_id>, next_start => now())), or grant ownership, then investigate why it hung.",
                        severity: AlertSeverityLevel.Critical,
                        shortMessage: $"{label} stuck — auto-re-arm FAILED",
                        numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                        cancellationToken);
                }
            }
            else if (state == CompressionJobHealth.ReArmed)
            {
                /* Still stuck after last check's self-heal = a RE-HANG. Escalate and STOP re-arming (looping
                   alter_job on a job that re-hangs just churns — it is a product-bug signal for a human). */
                _compressionJobState[key] = CompressionJobHealth.Escalated;
                _lastCompressionJobAlert[key] = now;
                await FireAsync(
                    CompressionKeyPrefix + key, "Monitor Store", CompressionJobMetric,
                    job.Reason, "running on schedule",
                    detail: $"TimescaleDB {label} is STILL stuck ({job.Reason}) after an automatic re-arm last cycle — it " +
                        "re-hung, so the service has STOPPED auto-re-arming it. This is a product-bug signal: the compression " +
                        "background worker is failing to make progress. Investigate the PostgreSQL/TimescaleDB logs and the " +
                        "background-worker settings; compression stays halted until it is fixed.",
                    severity: AlertSeverityLevel.Critical,
                    shortMessage: $"{label} re-hung after self-heal — escalated",
                    numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                    cancellationToken);
            }
            else
            {
                /* Already escalated: never re-arm again; keep paging on the cooldown while it stays stuck. */
                if (CooldownElapsed(_lastCompressionJobAlert, key, now))
                {
                    _lastCompressionJobAlert[key] = now;
                    await FireAsync(
                        CompressionKeyPrefix + key, "Monitor Store", CompressionJobMetric,
                        job.Reason, "running on schedule",
                        detail: $"TimescaleDB {label} remains stuck ({job.Reason}) after escalation — still not compressing. " +
                            "Manual intervention is required; the service will not auto-re-arm it.",
                        severity: AlertSeverityLevel.Critical,
                        shortMessage: $"{label} still stuck after escalation",
                        numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                        cancellationToken);
                }
            }
        }

        /* Recovery: any job we were tracking that is no longer stuck has recovered — drop its state and write
           one resolution row (the sibling conditions' edge shape). ToList() so the removal does not mutate the
           collection under enumeration. */
        foreach (var key in _compressionJobState.Keys.ToList())
        {
            if (stillStuck.Contains(key))
            {
                continue;
            }

            _compressionJobState.TryRemove(key, out _);
            _lastCompressionJobAlert.TryRemove(key, out _);
            await RecordResolutionAsync(new AlertResolution(
                CompressionKeyPrefix + key, "Monitor Store", CompressionJobMetric,
                "Compression Job Recovered",
                $"TimescaleDB compression job {key} is running on schedule again"), cancellationToken);
        }
    }

    /// <summary>Drops all edge state for a server removed from the monitored set (reconcile), so a later
    /// re-add starts fresh at the Unknown baseline rather than inheriting a stale connection/active flag.</summary>
    public void Forget(int serverId)
    {
        var key = Key(serverId);
        _activeCollectionStopped.TryRemove(key, out _);
        _lastCollectionStoppedAlert.TryRemove(key, out _);
        _activeCaptureDown.TryRemove(key, out _);
        _lastCaptureDownAlert.TryRemove(key, out _);
        _activeAgentDown.TryRemove(key, out _);
        _lastAgentDownAlert.TryRemove(key, out _);
        _connectionState.TryRemove(key, out _);
        _hasBeenOnline.TryRemove(key, out _);

        /* AG state is keyed by the AG GRAIN, not by server (#1696), so there is deliberately nothing here to
           drop: an Availability Group outlives any one of its monitored nodes, and another node may still be
           watching it. Dropping the edge state on removal would re-baseline a group that is still monitored
           and silently swallow the next failover.

           What DOES belong to the departing server is its claim to judge an AG. Releasing it lets a
           surviving node take over on its next sweep; the edge state it inherits is the same state, so the
           handover neither re-baselines nor double-fires. If the removed server was the group's only
           monitor, the state simply goes quiet — no snapshots arrive, so nothing fires. */
        foreach (var entry in _agAuthority)
        {
            if (entry.Value.ServerId == serverId)
            {
                _agAuthority.TryRemove(entry.Key, out _);
            }
        }
    }

    /* ---------------- store reads ---------------- */

    /// <summary>
    /// One round trip for the collection-stopped signals: the newest SUCCESS/SKIPPED time across all of the
    /// server's collectors (a SKIPPED is a healthy no-op, matching the viewer's GetCollectionHealthAsync),
    /// plus — over the most recent <paramref name="recentWindow"/> logged runs — the run count and the
    /// success count (feeding the consecutive-failure fast path). Static + parameterized so the gated live
    /// test can seed rows and assert the raw signals directly.
    /// </summary>
    internal static async Task<(DateTime? LastSuccessUtc, int RecentRunCount, int RecentSuccessCount)> ReadCollectionSignalsAsync(
        NpgsqlDataSource postgres, int serverId, int recentWindow, CancellationToken cancellationToken)
    {
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(@"
SELECT
    (SELECT MAX(collection_time)
     FROM collection_log
     WHERE server_id = $1
     AND   status IN ('SUCCESS', 'SKIPPED'))                                        AS last_success,
    (SELECT COUNT(*)
     FROM (SELECT log_id FROM collection_log WHERE server_id = $1 ORDER BY log_id DESC LIMIT $2) r) AS recent_runs,
    (SELECT COUNT(*)
     FROM (SELECT status FROM collection_log WHERE server_id = $1 ORDER BY log_id DESC LIMIT $2) r
     WHERE r.status IN ('SUCCESS', 'SKIPPED'))                                       AS recent_success", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(recentWindow);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return (null, 0, 0);
        }

        DateTime? lastSuccess = reader.IsDBNull(0)
            ? null
            : DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc);
        int recentRuns = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture);
        int recentSuccess = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture);
        return (lastSuccess, recentRuns, recentSuccess);
    }

    /// <summary>
    /// The blocking/deadlock XE collectors whose LATEST run logged <c>SESSION_MISSING</c> — the session is
    /// absent and couldn't be created, so capture is non-functional even though the tolerant reader "succeeds"
    /// with zero rows. The Darling twin of the Dashboard's <c>GetMissingCaptureSessionsAsync</c>, on Darling's
    /// collector names. Returns the friendly capture names ("Blocking" / "Deadlock").
    /// </summary>
    internal static async Task<IReadOnlyList<string>> ReadMissingCaptureSessionsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        var missing = new List<string>();

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(@"
SELECT x.collector_name
FROM
(
    SELECT
        cl.collector_name,
        cl.status,
        ROW_NUMBER() OVER (PARTITION BY cl.collector_name ORDER BY cl.log_id DESC) AS n
    FROM collection_log AS cl
    WHERE cl.server_id = $1
    AND   cl.collector_name IN ('deadlocks', 'blocked_process_report')
) AS x
WHERE x.n = 1
AND   x.status = 'SESSION_MISSING'
ORDER BY x.collector_name", connection);
        command.Parameters.AddWithValue(serverId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var collectorName = reader.GetString(0);
            missing.Add(collectorName == "deadlocks" ? "Deadlock" : "Blocking");
        }

        return missing;
    }

    /// <summary>
    /// Has SQL Agent EVER been observed running on this server, across the retained <c>agent_status</c> history?
    /// The capability gate behind "Agent Not Running" — see <see cref="ApplyAgentNotRunningAsync"/> for why a
    /// server that never ran Agent must never be told to start it.
    ///
    /// <para>Derived from collected evidence rather than tracked in process ON PURPOSE: a service restart must
    /// not un-learn that a real server runs Agent, which is exactly when an in-memory baseline would go quiet —
    /// Agent stops, the service restarts, and the alert that should fire never does.</para>
    ///
    /// <para>Bounded by that table's retention, which is the honest reading of the question: a server whose Agent
    /// has not run once in the whole retained window is, for alerting purposes, a server that does not use Agent.
    /// It re-arms by itself the moment Agent runs again.</para>
    /// </summary>
    internal static async Task<bool> HasAgentEverBeenSeenRunningAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(@"
SELECT EXISTS
(
    SELECT 1
    FROM agent_status
    WHERE server_id = $1
    AND   agent_running
)", connection);
        command.Parameters.AddWithValue(serverId);
        return (bool)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    /// <summary>
    /// The target's latest collected SQL Agent status (#1433 Phase 2): the newest <c>agent_status</c> row's
    /// collection time (UTC) and its <c>agent_running</c> flag. Returns (null, null) when the collector has
    /// never landed a row for the server (Azure SQL DB, msdb denied, or not yet collected) — the caller reads
    /// that as "no signal" and does not judge. Static + parameterized so the gated live test can seed a row
    /// and assert the raw signal.
    /// </summary>
    internal static async Task<(DateTime? CollectionTimeUtc, bool? AgentRunning)> ReadLatestAgentStatusAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(@"
SELECT agent_running, collection_time
FROM agent_status
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1", connection);
        command.Parameters.AddWithValue(serverId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return (null, null);
        }

        bool? running = reader.IsDBNull(0) ? null : reader.GetBoolean(0);
        DateTime? collectionTime = reader.IsDBNull(1)
            ? null
            : DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
        return (collectionTime, running);
    }

    /// <summary>
    /// The server's LATEST <c>ag_replica_states</c> snapshot (#991) — the rows at that table's own
    /// MAX(collection_time), covered by the collector migration's (server_id, collection_time) index, plus that
    /// timestamp as the caller's freshness signal.
    /// <para>ONE STATEMENT PER COMMAND, deliberately. The two grains were briefly read as two statements over a
    /// single command to save a round trip; that cannot work here. Npgsql only splits multi-statement text into
    /// a batch when it parses the SQL for NAMED placeholders — with POSITIONAL (<c>$1</c>) parameters, which is
    /// what every read in this file uses, it sends the text as one extended-protocol Parse, and PostgreSQL
    /// rejects that with "cannot insert multiple commands into a prepared statement". Failure-isolated as this
    /// path is, that would have been an error logged per server per sweep with the whole alert family silently
    /// dead. Two commands, matching the three sibling reads above; <see cref="NpgsqlBatch"/> would restore the
    /// single round trip but has no precedent in this codebase, and the AG read is skipped entirely when the
    /// master switch is off, so an AG-free fleet pays nothing either way.</para>
    /// <para>Rows whose <c>ag_name</c> or <c>replica_server_name</c> is NULL are DROPPED: those are the identity
    /// of the alert's state key, and under WSFC quorum loss they can read NULL. A row we cannot key is a row we
    /// cannot track an edge for, so keying it under a placeholder identity would invent transitions. Zero rows
    /// is the NORMAL result on a server with no Availability Groups.</para>
    /// Static + parameterized so a gated live test can seed rows and assert the raw signals.
    /// </summary>
    internal static async Task<(DateTime? CollectionTimeUtc, IReadOnlyList<AgReplicaReading> Replicas)> ReadLatestAgReplicaStatesAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        var replicas = new List<AgReplicaReading>();
        DateTime? newest = null;

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(@"
SELECT ag_name, replica_server_name, role_desc, connected_state_desc, is_local, collection_time
FROM ag_replica_states
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM ag_replica_states WHERE server_id = $1)
ORDER BY ag_name, replica_server_name", connection);
        command.Parameters.AddWithValue(serverId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            newest = Newest(newest, reader, 5);
            if (reader.IsDBNull(0) || reader.IsDBNull(1))
            {
                continue;
            }

            replicas.Add(new AgReplicaReading(
                AgName: reader.GetString(0),
                ReplicaServerName: reader.GetString(1),
                RoleDesc: reader.IsDBNull(2) ? null : reader.GetString(2),
                ConnectedStateDesc: reader.IsDBNull(3) ? null : reader.GetString(3),
                IsLocal: reader.IsDBNull(4) ? null : reader.GetBoolean(4)));
        }

        return (newest, replicas);
    }

    /// <summary>
    /// The server's LATEST <c>ag_database_replica_states</c> snapshot (#991), with its OWN
    /// MAX(collection_time). Separate from <see cref="ReadLatestAgReplicaStatesAsync"/> — and separately
    /// freshness-gated by the caller — because the two AG collectors carry independent schedule entries: a
    /// disabled or failing database-grain collector must not have the replica grain's healthy timestamp vouch
    /// for its stale rows, which would keep re-firing "AG Sync Fell Behind" off days-old lag readings.
    /// Same NULL-identity drop and same one-statement-per-command rule as the replica read.
    /// </summary>
    internal static async Task<(DateTime? CollectionTimeUtc, IReadOnlyList<AgDatabaseReading> Databases)> ReadLatestAgDatabaseReplicaStatesAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        var databases = new List<AgDatabaseReading>();
        DateTime? newest = null;

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(@"
SELECT ag_name, database_name, replica_server_name, secondary_lag_seconds, redo_queue_size,
       is_suspended, suspend_reason_desc, collection_time
FROM ag_database_replica_states
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM ag_database_replica_states WHERE server_id = $1)
ORDER BY ag_name, database_name, replica_server_name", connection);
        command.Parameters.AddWithValue(serverId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            newest = Newest(newest, reader, 7);
            if (reader.IsDBNull(0) || reader.IsDBNull(1) || reader.IsDBNull(2))
            {
                continue;
            }

            databases.Add(new AgDatabaseReading(
                AgName: reader.GetString(0),
                DatabaseName: reader.GetString(1),
                ReplicaServerName: reader.GetString(2),
                SecondaryLagSeconds: reader.IsDBNull(3) ? null : reader.GetInt64(3),
                RedoQueueSizeKb: reader.IsDBNull(4) ? null : reader.GetInt64(4),
                IsSuspended: reader.IsDBNull(5) ? null : reader.GetBoolean(5),
                SuspendReasonDesc: reader.IsDBNull(6) ? null : reader.GetString(6)));
        }

        return (newest, databases);
    }

    /// <summary>Running maximum of a snapshot's <c>collection_time</c> column, as naive UTC.</summary>
    private static DateTime? Newest(DateTime? running, NpgsqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            return running;
        }

        var candidate = DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc);
        return running is null || candidate > running ? candidate : running;
    }

    /* ---------------- shared helpers ---------------- */

    /// <summary>
    /// Builds a resolved-flavored history row for a recovered condition — the Darling twin of the
    /// Dashboard's explicit "…Cleared/Resolved/Restored" <c>RecordAlert</c> rows. Used by BOTH this
    /// evaluator's self-alert recoveries (Collection Resumed / Capture Restored) and the shared alert
    /// engine's resolution callback (CPU Resolved, Blocking Cleared, …) so an operator reviewing alert
    /// history sees the paired "Detected" then "Cleared" entries. Never email/webhook (a resolution has no
    /// send channel — the deliverer's fire path is untouched): recorded as a delivered tray/history row.
    /// </summary>
    public static AlertHistoryRecord BuildResolutionRecord(AlertResolution resolution) => new(
        resolution.ServerKey, resolution.ServerName, resolution.Title,
        CurrentValueText: "resolved", ThresholdValueText: "",
        NumericCurrentValue: null, NumericThresholdValue: null,
        AlertSent: true, NotificationType: "tray", SendError: null,
        Muted: false, DetailText: resolution.Message, ContextJson: null);

    /// <summary>
    /// The explicit "this metric has no measurement" value for the history stores' NOT NULL
    /// <c>current_value</c>/<c>threshold_value</c> columns — the write-side half of
    /// <see cref="AlertMetricClassifier.IsStateOnly"/>'s contract (#1881).
    ///
    /// <para>Passing it beats leaving the numeric null and letting the text fallback land on 0. The
    /// fallback (<c>AlertValueParser.ParseOrDefault</c>) scans to the first digit ANYWHERE in the display
    /// text, so it returns 0 only for text that happens to carry no digit — which is not a property any
    /// of these producers controls. "Server Unreachable" passes the driver's error message, "AG Sync Fell
    /// Behind" passes prose with the lag seconds in it, and every AG alert embeds object names an
    /// operator is free to call "SQL01" or "Sales2024". Stating the sentinel here makes the stored value
    /// a decision rather than a coincidence, and it is what lets the read side treat a stored 0 on these
    /// metrics as meaning "no value" instead of guessing.</para>
    /// </summary>
    private const double StateOnlyValue = 0;

    /// <param name="numericCurrentValue">The measurement behind <paramref name="currentValue"/>'s display
    /// text, or <see cref="StateOnlyValue"/> when the metric has none. DELIBERATELY REQUIRED rather than
    /// defaulted to null: a default would let a new self-alert fall into the text-parsing fallback
    /// silently, which is exactly how #1881's metrics came to store a PostgreSQL major version and a
    /// digit lifted out of a server name. Making the compiler ask means every fire site answers
    /// "is this a number?" on purpose. Only Store Disk Pressure answers yes.</param>
    /// <param name="numericThresholdValue">The bound behind <paramref name="thresholdValue"/>, on the same
    /// terms. Almost every self-alert's threshold is an English phrase ("collecting", "Online", "running
    /// on schedule"), not a bound.</param>
    /* The optional context TRAILS the cancellation token so the dozens of existing positional call
       sites stay untouched — only the callers that have discrete facts to carry (#2109: the AG
       database alerts) name it. */
    private async Task FireAsync(
        string serverKey, string serverName, string metricName, string currentValue, string thresholdValue,
        string detail, AlertSeverityLevel? severity, string shortMessage,
        double? numericCurrentValue, double? numericThresholdValue, CancellationToken cancellationToken,
        AlertContext? context = null)
    {
        /* Same mute treatment as the engine: a muted self-alert is still recorded (flagged muted) but its
           channels are skipped — the deliverer honors AlertOutcome.Muted. */
        bool muted = _isAlertMuted(new AlertMuteContext { ServerName = serverName, MetricName = metricName });

        /* #1681: log the FIRING, not just the recovery. RecordResolutionAsync has always logged at Information,
           so the service log showed "… Recovered" with nothing before it — which reads as a spontaneous
           recovery from a condition that never happened, and is worse than logging neither half. Every
           self-alert went through here silently: compression-job stuck, disk pressure, capture-down,
           agent-not-running, collection-health. Warning rather than Information because a firing self-alert is
           by definition something wrong with the monitor itself, and because it must stand out from the
           Information-level recovery it will eventually pair with. Muted alerts are logged too — muting
           suppresses the notification channels, not the operator's ability to find it in the log afterwards. */
        _logger?.LogWarning(
            "{Line}",
            AlertFiringLog.Fired(
                serverName, metricName, severity?.ToString() ?? "Warning", shortMessage, muted));

        await _deliverer.DeliverAsync(new AlertOutcome(
            serverKey, serverName, metricName, currentValue, thresholdValue,
            Context: context, DetailText: detail,
            NumericCurrentValue: numericCurrentValue, NumericThresholdValue: numericThresholdValue,
            Muted: muted, Severity: severity, ShortMessage: shortMessage), cancellationToken);
    }

    private async Task RecordResolutionAsync(AlertResolution resolution, CancellationToken cancellationToken)
    {
        /* #1681: the RESOLVED half, in the shared shape so it greps together with the TRIGGERED line the
           firing wrote. resolution.Title is the recovery's own name ("Capture Restored"); the metric it
           clears is carried separately, and the pair is readable either way. */
        _logger?.LogInformation("{Line}",
            AlertFiringLog.Resolved(resolution.ServerName, resolution.Title, resolution.Message));
        try
        {
            await _historyStore.RecordAlertAsync(BuildResolutionRecord(resolution));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* An audit-row write must never break the loop (RecordAlertAsync is already failure-isolated;
               this is belt-and-suspenders for any other IAlertHistoryStore). */
            _logger?.LogError("Failed to record resolution '{Title}': {Message}", resolution.Title, ex.Message);
        }
    }

    private bool CooldownElapsed(ConcurrentDictionary<string, DateTime> lastFired, string key, DateTime now) =>
        !lastFired.TryGetValue(key, out var last)
        || now - last >= TimeSpan.FromMinutes(_settings.CooldownMinutes);

    private static string Key(int serverId) => serverId.ToString(CultureInfo.InvariantCulture);

    /// <summary>Human-readable GiB for disk-pressure alert text (binary GiB, 2 dp).</summary>
    private static string FormatGb(long bytes) =>
        (bytes / 1024.0 / 1024.0 / 1024.0).ToString("0.##", CultureInfo.InvariantCulture) + " GB";
}
