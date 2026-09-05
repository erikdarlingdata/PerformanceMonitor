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
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The Phase-5 shared alert engine (slice D): Lite's <c>MainWindow.CheckPerformanceAlerts</c>
/// TRANSPLANTED behind the three seams — <see cref="IAlertEngineSettings"/> (thresholds),
/// <see cref="IAlertReadAdapter"/> (collected feeds), <see cref="IAlertStateStore"/>
/// (restart-surviving watermarks) — with every fired alert emitted through
/// <see cref="IAlertDeliverer"/>. Same alert order, same gating order per alert
/// (enabled flag → data fetch → threshold compare → edge-trigger state → mute check → cooldown →
/// deliver → record state), same edge-trigger/cooldown/mute semantics, line-cited per check.
/// The headless Darling service is the first consumer; Lite forwards in a later slice.
/// <para>
/// Deliberate NON-transplants (UI-coupled Lite behavior that stays app-side):
/// tray toast RENDERING and the <c>_trayService</c> null gate (the per-metric toast BODY ships as
/// <see cref="AlertOutcome.ShortMessage"/> because it needs per-row data the other display fields
/// don't carry); the server-tab badge flags (#754/#749 <c>_badgeLowDisk</c>/<c>_badgeFailedJob</c>
/// + acknowledgement clearing — the two standing conditions they derive from are surfaced on the
/// returned <see cref="AlertSweepResult"/>); the #1141 Summary-vs-Per-event delivery split (an
/// <see cref="IAlertDeliverer"/> concern per its contract); and the #1236 per-server delivery-mode
/// override (same seam). Lite's tray-only "Resolved"/"Cleared" toasts surface through the optional
/// resolution callback (<see cref="AlertResolution"/>) with Lite's exact strings — they never
/// touch the deliverer because Lite records no history row for them. Line citations per check
/// refer to the pre-forwarding Lite loop (the transplant source, retrievable from git history).
/// </para>
/// <para>
/// Two documented adaptations of the store reads: (1) Lite's loop received precomputed rolling
/// blocking/deadlock counts from its overview summary query; the engine derives them from ONE
/// adapter fetch instead (blocking keeps Lite's XE-preferred count semantics — the XE row count,
/// falling back to the merged count when zero XE rows — and the fetched rows then serve the
/// excluded-database recount and the fired alert's context, so the numbers can't disagree within
/// a sweep). Counts therefore inherit the adapter caps (200/50) and the deadlock read's
/// collection-time window. (2) When the blocking/deadlock fetch itself fails, the engine skips
/// that check for the sweep (state untouched) — mirroring the try/catch-and-move-on shape of
/// Lite's other checks — rather than running the gate against a fabricated zero count, which
/// would reset the watermark and later re-fire.
/// </para>
/// <para>
/// THREAD-SAFETY: the engine is a long-lived singleton per host. Evaluations for the SAME server
/// are serialized internally (per-key gate), so a host that overlaps sweeps cannot interleave one
/// server's state updates; DIFFERENT servers may evaluate concurrently (all state lives in
/// concurrent dictionaries). Hosts should still call sequentially per server — the gate is a
/// guarantee, not an invitation.
/// </para>
/// </summary>
public sealed class AlertEngine
{
    /* The persisted-watermark row keys (#1145) — Lite's MainWindow.xaml.cs:111-112 constants,
       shared so Lite's existing config_edge_trigger_watermarks rows seed this engine unchanged. */
    public const string BlockingWatermarkMetric = "Blocking Detected";
    public const string DeadlockWatermarkMetric = "Deadlocks Detected";

    /* #2362: the remaining fingerprinted alerts. Same names their FireAsync/mute contexts use, so the
       accumulator's per-fingerprint state lives under the metric an operator already knows. Forced Plan
       Failing is deliberately absent: it builds a bare context and never calls AlertIncidentRenderer.Apply,
       so it carries no dedup keys for the accumulator to key on. */
    public const string LongRunningQueryWatermarkMetric = "Long-Running Query";
    public const string VolumeFreeSpaceWatermarkMetric = "Volume Free Space";
    public const string PvsWatermarkMetric = "Version Store (PVS)";
    public const string FileGrowthWatermarkMetric = "Database File Growth";
    public const string AnomalousJobWatermarkMetric = "Long-Running Job";
    public const string FailedJobWatermarkMetric = "Failed Agent Job";

    /// <summary>
    /// The rolling window both count gates read, in hours (#1091's "in the last hour"). Named because
    /// #2216's occurrence accumulator has to agree with it: its staleness horizon is what stops a row
    /// stranded by a crash from being trusted on the same fingerprint's NEXT incident, and the only value
    /// that makes that judgement correct is the window itself — inside the window a persisted row is
    /// describing the very events the gauge is still counting, outside it the row cannot be. Two literals
    /// that must match are two literals that will eventually not.
    /// </summary>
    public const int RollingCountWindowHours = 1;

    /* #2216: rows untouched for longer than the read window are treated as absent by the accumulator. */
    private static readonly TimeSpan OccurrenceStaleAfter = TimeSpan.FromHours(RollingCountWindowHours);

    private readonly IAlertEngineSettings _settings;
    private readonly IAlertReadAdapter _readAdapter;
    private readonly IAlertStateStore _stateStore;
    private readonly IAlertDeliverer _deliverer;
    private readonly Func<AlertMuteContext, bool> _isAlertMuted;
    private readonly Func<string, int, CancellationToken, Task<List<FailedJobInfo>>>? _failedJobsFetcher;
    private readonly Func<AlertResolution, CancellationToken, Task>? _resolutionCallback;
    private readonly ILogger? _logger;
    private readonly Func<DateTime> _utcNow;

    /* Per-serverKey evaluation gate: serializes EvaluateServerAsync for the SAME server. */
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _serverGates = new();

    /* One-time per-serverKey watermark seeding from the state store (#1145) — the per-key twin of
       Lite's bulk SeedEdgeTriggerWatermarksAsync (MainWindow.xaml.cs:1563). */
    private readonly ConcurrentDictionary<string, bool> _seededServerKeys = new();

    /* Cooldown timestamps — Lite's MainWindow.xaml.cs:56-63,90 dictionaries, keyed serverKey.
       In-memory only, exactly like Lite (the restart protection is the persisted watermarks plus
       the deliverer's own email/webhook cooldown seeds, not these). */
    private readonly ConcurrentDictionary<string, DateTime> _lastCpuAlert = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastBlockingAlert = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastBlockingWaitAlert = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastDeadlockAlert = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastPoisonWaitAlert = new();

    /* The collection_time of the wait_stats row(s) last actually fired on. Read adapters answer
       "what's the newest poison-wait row within the last 10 minutes", which is independent of
       whether it is NEW since the previous ask — at fleet load the collector's delivered cadence
       can lag the alert cooldown (PerformanceMonitor's own dogfooding on prod-pos-use2-monitor-01
       caught byte-identical duplicate alerts ~5-7 minutes apart), so the cooldown elapsing is not
       proof a fresh observation exists. Poison wait is deliberately NOT level-triggered like CPU
       (which resamples live every sweep): a delta is one collector cycle's computation, and
       reading it twice is the same event surfacing twice, not two observations of a standing
       condition. Gate re-fire on BOTH the cooldown AND a newer collection_time than last fired. */
    private readonly ConcurrentDictionary<string, DateTime> _lastPoisonWaitCollectionTime = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastLongRunningQueryAlert = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastTempDbSpaceAlert = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastLowDiskAlert = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastFailedJobAlert = new();

    /* Keyed per job *run* ({serverKey}:{jobId}:{startTime:O}) so it grows without bound; stale
       entries are pruned each pass — Lite's MainWindow.xaml.cs:63 + AlertEngine.cs:564-573. */
    private readonly ConcurrentDictionary<string, DateTime> _lastLongRunningJobAlert = new();

    /* Active-condition flags driving the resolved/cleared transitions —
       Lite's MainWindow.xaml.cs:78-89. */
    private readonly ConcurrentDictionary<string, bool> _activeCpuAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activeBlockingAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activeBlockingWaitAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activeDeadlockAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activePoisonWaitAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activeLongRunningQueryAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activeTempDbSpaceAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activeLowDiskAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activeLongRunningJobAlert = new();

    /* Worst free-% captured at the last low-disk alert (#754 follow-up) — Lite's
       MainWindow.xaml.cs:88; gated by LowDiskAlertGate; removed on resolve. */
    private readonly ConcurrentDictionary<string, double> _lastAlertedLowDiskPercent = new();

    /* #1984 — the PVS twins of the low-disk trio: cooldown watermark, standing-condition flag
       for the resolved transition, and the PvsAlertGate worsening watermark. */
    private readonly ConcurrentDictionary<string, DateTime> _lastPvsAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activePvsAlert = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastFileGrowthAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activeFileGrowthAlert = new();
    private readonly ConcurrentDictionary<string, double> _lastAlertedPvsPercent = new();

    /* Rolling-count edge-trigger watermarks (#1091) — Lite's MainWindow.xaml.cs:103-104;
       persisted through IAlertStateStore on change (#1145). */
    private readonly ConcurrentDictionary<string, int> _lastAlertedBlockingCount = new();
    private readonly ConcurrentDictionary<string, int> _lastAlertedDeadlockCount = new();

    /* Newest already-alerted failed-job run time (SERVER-LOCAL) — Lite's MainWindow.xaml.cs:96;
       persisted through IAlertStateStore on change (#1145 parity). */
    private readonly ConcurrentDictionary<string, DateTime> _lastAlertedFailedJobTime = new();

    /* Database-state alert (offline/unhealthy) — a PER-DATABASE standing condition. The active set is
       the databases currently alerting on this server (outer keyed serverKey; the inner set is only
       ever touched under this server's evaluation gate, so a plain HashSet is safe). The cooldown dict
       is keyed per database (serverKey + "|" + dbName) so each database throttles independently, and
       an entry is removed when its database recovers. In-memory only, like the other family state. */
    private readonly ConcurrentDictionary<string, HashSet<string>> _activeDatabaseStateAlerts = new();

    /* #2166: keyed per (server, database, STATE) as a tuple rather than a delimited string. Per-state
       because a chosen state now goes quiet indefinitely, so letting one state's clock rate-limit a
       transition to a DIFFERENT state is a silence rather than a delay — and the state it would silence is
       SUSPECT. Structural rather than concatenated because clearing a database's clocks means matching on
       two of the three parts, and a string key makes that a prefix match: SQL Server permits '|' in a
       database name, so `Foo|Bar` would collide with `Foo` under any delimiter a sysname can contain. */
    private readonly ConcurrentDictionary<(string Server, string Database, string State), DateTime> _lastDatabaseStateAlert = new();

    /* #2157: per-PLAN active set and cooldowns. The alerting unit is one forced plan, not one server —
       two plans failing on the same database are independent conditions that resolve independently.
       Keyed by the internal plan key but VALUED with the plan's identity, because the resolution has to
       name the plan in an operator-readable way: a bare key set left the recovery message reading
       'forceplan:Sales:11:22 no longer failing to force' in every email and webhook (review catch). */
    private readonly ConcurrentDictionary<string, Dictionary<string, ForcePlanFailureInfo>> _activeForcePlanAlerts = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastForcePlanAlert = new();

    /// <param name="settings">Live threshold surface — read every sweep, never cached.</param>
    /// <param name="readAdapter">The collected alert feeds (slice B seam).</param>
    /// <param name="stateStore">Restart-surviving watermark persistence (#1145).</param>
    /// <param name="deliverer">Record-and-send seam — the engine never touches SMTP/history itself.</param>
    /// <param name="isAlertMuted">
    /// Mute check — Lite/Darling pass <c>MuteRuleService.IsAlertMuted</c>. A muted alert is still
    /// delivered to the deliverer (flagged <see cref="AlertOutcome.Muted"/>) so the host records
    /// it without sending, exactly Lite's flow.
    /// </param>
    /// <param name="failedJobsFetcher">
    /// The live msdb failed-jobs feed (serverKey, lookbackMinutes, ct) — NOT a collected read, so
    /// it stays host-supplied: hosts run <see cref="FailedJobsQuery"/> on their own connections
    /// and degrade failures to an empty list. Null disables the failed-jobs check entirely.
    /// </param>
    /// <param name="resolutionCallback">
    /// Optional condition-recovered hook (see <see cref="AlertResolution"/>). Null = resolutions
    /// are tracked but not reported (state transitions still occur).
    /// </param>
    /// <param name="logger">Optional diagnostics logger.</param>
    /// <param name="utcNow">Test seam for the cooldown clock; production leaves it null (UtcNow).</param>
    /// <param name="readFailures">
    /// Where a SWALLOWED condition read is counted (#3013). Every per-check catch below logs and skips —
    /// correctly, because firing on absent evidence fabricates an alert and resolving on it fabricates a
    /// recovery — but the skip reached no surface a person reads, so the alert pass could go blind one
    /// condition at a time behind a green health read. Null leaves the counting off and changes nothing
    /// else; production passes <see cref="AlertReadFailureCounter.Shared"/>, and tests that want to
    /// observe the counting pass their own instance rather than touching that one.
    /// </param>
    public AlertEngine(
        IAlertEngineSettings settings,
        IAlertReadAdapter readAdapter,
        IAlertStateStore stateStore,
        IAlertDeliverer deliverer,
        Func<AlertMuteContext, bool> isAlertMuted,
        Func<string, int, CancellationToken, Task<List<FailedJobInfo>>>? failedJobsFetcher = null,
        Func<AlertResolution, CancellationToken, Task>? resolutionCallback = null,
        ILogger? logger = null,
        Func<DateTime>? utcNow = null,
        AlertReadFailureCounter? readFailures = null)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _readAdapter = readAdapter ?? throw new ArgumentNullException(nameof(readAdapter));
        _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
        _deliverer = deliverer ?? throw new ArgumentNullException(nameof(deliverer));
        _isAlertMuted = isAlertMuted ?? throw new ArgumentNullException(nameof(isAlertMuted));
        _failedJobsFetcher = failedJobsFetcher;
        _resolutionCallback = resolutionCallback;
        _logger = logger;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _readFailures = readFailures;
    }

    /// <summary>#3013: the swallowed-read counter, or null when nothing is counting.</summary>
    private readonly AlertReadFailureCounter? _readFailures;

    /// <summary>
    /// Runs one full alert sweep for one server — Lite's <c>CheckPerformanceAlerts(summary)</c>.
    /// Per-server serialized (see class remarks). Channel/store failures never escape (the
    /// deliverer and state store contracts absorb them; per-check fetch failures are logged and
    /// skip that check for the sweep); only cancellation propagates. Returns what the sweep
    /// OBSERVED (see <see cref="AlertSweepResult"/>) so interactive hosts can drive their
    /// standing-condition badges; headless hosts ignore the result.
    /// </summary>
    public async Task<AlertSweepResult> EvaluateServerAsync(AlertServerSnapshot snapshot, CancellationToken cancellationToken = default)
    {
        if (snapshot is null)
        {
            throw new ArgumentNullException(nameof(snapshot));
        }

        /* Master switch — Lite's AlertEngine.cs:38 (the _trayService null gate is UI-only).
           NotEvaluated mirrors Lite's early return: the host leaves badge state untouched. */
        if (!_settings.AlertsEnabled)
        {
            return AlertSweepResult.NotEvaluated;
        }

        var gate = _serverGates.GetOrAdd(snapshot.ServerKey, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken);
        try
        {
            return await EvaluateCoreAsync(snapshot, cancellationToken);
        }
        finally
        {
            gate.Release();
        }
    }

    private async Task<AlertSweepResult> EvaluateCoreAsync(AlertServerSnapshot snapshot, CancellationToken ct)
    {
        var key = snapshot.ServerKey;
        var serverName = snapshot.ServerName;
        var now = _utcNow();                                                        /* Lite AlertEngine.cs:41 */
        var alertCooldown = TimeSpan.FromMinutes(_settings.CooldownMinutes);        /* :57 */
        bool suppressed = snapshot.Suppressed;                                      /* :60 (suppressPopups) */

        /* #3013: the denominator for this server's swallowed-read count, recorded HERE rather than in
           EvaluateServerAsync so the master-switch-off early return does not count a pass that never
           looked at the store. */
        _readFailures?.RecordPass(key);

        await EnsureWatermarksSeededAsync(key, ct);

        await CheckCpuAsync(snapshot, key, serverName, now, alertCooldown, suppressed, ct);
        await CheckBlockingAsync(key, serverName, now, alertCooldown, suppressed, ct);
        await CheckDeadlocksAsync(key, serverName, now, alertCooldown, suppressed, ct);
        await CheckPoisonWaitsAsync(key, serverName, now, alertCooldown, suppressed, ct);
        await CheckLongRunningQueriesAsync(key, serverName, now, alertCooldown, suppressed, ct);
        await CheckTempDbSpaceAsync(key, serverName, now, alertCooldown, suppressed, ct);
        bool lowDiskConditionPresent = await CheckLowDiskAsync(key, serverName, now, alertCooldown, suppressed, ct);
        await CheckPvsPressureAsync(key, serverName, now, alertCooldown, suppressed, ct);
        await CheckFileGrowthAsync(key, serverName, now, alertCooldown, suppressed, ct);
        await CheckAnomalousJobsAsync(key, serverName, now, alertCooldown, suppressed, ct);
        bool failedJobConditionPresent = await CheckFailedJobsAsync(snapshot, key, serverName, now, alertCooldown, suppressed, ct);
        await CheckDatabaseStateAsync(key, serverName, now, alertCooldown, suppressed, ct);
        await CheckForcePlanFailuresAsync(key, serverName, now, alertCooldown, suppressed, ct);

        return new AlertSweepResult(true, lowDiskConditionPresent, failedJobConditionPresent);
    }

    /* ---------------- watermark seeding (#1145) ---------------- */

    /// <summary>
    /// Per-key twin of Lite's startup <c>SeedEdgeTriggerWatermarksAsync</c>
    /// (MainWindow.xaml.cs:1563-1594): loads the persisted blocking/deadlock count watermarks and
    /// the failed-job time watermark before this server's first sweep, so a host restart doesn't
    /// re-fire (and re-post webhooks for) events still lingering in the rolling window. Seeded
    /// once per key; a seed failure logs and proceeds unseeded, exactly like Lite.
    /// </summary>
    private async Task EnsureWatermarksSeededAsync(string key, CancellationToken ct)
    {
        if (_seededServerKeys.ContainsKey(key))
        {
            return;
        }

        try
        {
            var blocking = await _stateStore.LoadEdgeTriggerWatermarkAsync(key, BlockingWatermarkMetric);
            if (blocking.HasValue)
            {
                _lastAlertedBlockingCount[key] = blocking.Value;
            }

            var deadlock = await _stateStore.LoadEdgeTriggerWatermarkAsync(key, DeadlockWatermarkMetric);
            if (deadlock.HasValue)
            {
                _lastAlertedDeadlockCount[key] = deadlock.Value;
            }

            var failedJob = await _stateStore.LoadFailedJobWatermarkAsync(key);
            if (failedJob.HasValue)
            {
                _lastAlertedFailedJobTime[key] = failedJob.Value;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Failed to seed edge-trigger watermarks for {ServerKey}: {Message}", key, ex.Message);
            _readFailures?.RecordReadFailure(key, "edge-trigger watermark seed");
        }

        _seededServerKeys[key] = true;
    }

    /* ---------------- CPU (Lite AlertEngine.cs:62-114) ---------------- */

    private async Task CheckCpuAsync(
        AlertServerSnapshot snapshot, string key, string serverName,
        DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        /* Mode selection INSIDE the engine — ServerSummaryItem.CpuPercentForAlert semantics
           (Lite LocalDataService.Overview.cs:143-144): Total → TotalCpuPercent ?? CpuPercent;
           SqlOnly → CpuPercent. */
        var alertCpuValue = _settings.CpuAlertMode == CpuAlertMode.TotalServer
            ? (snapshot.TotalCpuPercent ?? snapshot.SqlCpuPercent)
            : snapshot.SqlCpuPercent;
        string cpuMetricLabel = _settings.CpuAlertMode == CpuAlertMode.TotalServer ? "Total CPU" : "SQL CPU"; /* :64 */
        bool cpuExceeded = _settings.CpuEnabled
            && alertCpuValue.HasValue
            && alertCpuValue.Value >= _settings.CpuThresholdPercent;                /* :65-67 */

        if (cpuExceeded)
        {
            _activeCpuAlert[key] = true;                                            /* :71 */
            if (!suppressed && CooldownElapsed(_lastCpuAlert, key, now, alertCooldown)) /* :72 */
            {
                var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "High CPU" }; /* :74 */
                bool isMuted = _isAlertMuted(muteCtx);                              /* :75 */
                _lastCpuAlert[key] = now;                                           /* :76 — stamped even when muted */

                var cpuDetailText = $"  {cpuMetricLabel}: {alertCpuValue:F0}%\n  Threshold: {_settings.CpuThresholdPercent}%"; /* :89 */

                /* :91-98 — CPU passes no context; ShortMessage = the toast body of :84 minus the
                   server-name prefix. The numerics are REQUIRED, not optional (#1830): the ported
                   no-numerics form left the history stores parsing "87% (Total CPU)", which fails on
                   the parenthesized label, so every High CPU row stored current_value 0 in Lite AND
                   Darling while the toast/email/webhook text stayed correct. HasValue is guaranteed
                   here — cpuExceeded requires it. */
                await FireAsync(new AlertOutcome(
                    key, serverName, "High CPU",
                    $"{alertCpuValue:F0}% ({cpuMetricLabel})",
                    $"{_settings.CpuThresholdPercent}%",
                    Context: null, DetailText: cpuDetailText,
                    NumericCurrentValue: alertCpuValue, NumericThresholdValue: _settings.CpuThresholdPercent,
                    Muted: isMuted, Severity: null,
                    ShortMessage: $"{cpuMetricLabel} at {alertCpuValue:F0}% (threshold: {_settings.CpuThresholdPercent}%)"), ct);
            }
        }
        else if (_activeCpuAlert.TryGetValue(key, out var wasCpu) && wasCpu)        /* :101 */
        {
            _activeCpuAlert[key] = false;                                           /* :103 */
            /* :107 — resolve announced only while the alert is still enabled and unsuppressed
               (disabling flips cpuExceeded false; neither means CPU actually recovered). */
            if (!suppressed && _settings.CpuEnabled)
            {
                await NotifyResolutionAsync(new AlertResolution(
                    key, serverName, "High CPU",
                    "CPU Resolved",                                                 /* :110 */
                    $"{serverName}: {cpuMetricLabel} back to {alertCpuValue:F0}%"), ct); /* :111 */
            }
        }
    }

    /* ---------------- blocking (Lite AlertEngine.cs:116-194) ---------------- */

    private async Task CheckBlockingAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        List<BlockedProcessAlertRow>? blockingRows = null;
        int effectiveBlockingCount = 0;

        if (_settings.BlockingEnabled)
        {
            try
            {
                /* ONE fetch serves the rolling count, the excluded-database recount (:118-133),
                   and the fired alert's context (:172) — see class remarks adaptation (1). */
                blockingRows = await _readAdapter.GetRecentBlockedProcessReportsAsync(key, hoursBack: RollingCountWindowHours, ct);

                /* Lite's overview count semantics (LocalDataService.Overview.cs:74-77): prefer the
                   XE blocked-process-report count; fall back to the DMV snapshot count when the XE
                   count is zero (AWS RDS / unset blocked-process threshold). The merged adapter
                   list contains all XE rows plus only uncovered DMV rows, so when no XE row exists
                   the merged count IS the DMV count. */
                int xeCount = blockingRows.Count(r => r.Source == BlockedProcessAlertRow.XeReportSource);
                effectiveBlockingCount = xeCount > 0 ? xeCount : blockingRows.Count;

                /* :118-127 — with excluded databases configured and the raw count at/over the
                   threshold, recount only rows outside the excluded set (no-database rows pass). */
                if (_settings.ExcludedDatabases.Count > 0
                    && effectiveBlockingCount >= _settings.BlockingCountThreshold)
                {
                    effectiveBlockingCount = blockingRows
                        .Count(r => string.IsNullOrEmpty(r.DatabaseName) ||
                            !_settings.ExcludedDatabases.Any(e =>
                                string.Equals(e, r.DatabaseName, StringComparison.OrdinalIgnoreCase)));
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                /* :129-132 shape — log and skip this check for the sweep (class remarks
                   adaptation (2)): never run the gate on a fabricated zero count. */
                _logger?.LogError("Failed to check blocking for {Server}: {Message}", serverName, ex.Message);
                _readFailures?.RecordReadFailure(key, "blocking");
                return;
            }
        }

        /* Edge-trigger the rolling 1-hour count (#1091) — :135-150. */
        int blockingWatermark = _lastAlertedBlockingCount.TryGetValue(key, out var labc) ? labc : 0; /* :138 */
        bool blockingCooldownElapsed = CooldownElapsed(_lastBlockingAlert, key, now, alertCooldown); /* :139 */
        var blockingDecision = _settings.BlockingEnabled
            ? RollingCountAlertGate.Evaluate(effectiveBlockingCount, _settings.BlockingCountThreshold, blockingWatermark, blockingCooldownElapsed, suppressed)
            : new RollingCountAlertGate.Decision(false, false, 0);                  /* :140-142 */
        _lastAlertedBlockingCount[key] = blockingDecision.Watermark;                /* :143 */
        if (blockingDecision.Watermark != blockingWatermark)                        /* :147 — persist on change (#1145) */
        {
            await _stateStore.SaveEdgeTriggerWatermarkAsync(key, BlockingWatermarkMetric, blockingDecision.Watermark); /* :149 */
        }

        bool wasBlockingActive = _activeBlockingAlert.TryGetValue(key, out var wasBlocking) && wasBlocking; /* :152 */
        _activeBlockingAlert[key] = blockingDecision.Active;                        /* :153 */

        /* #2216: observe THIS sweep's fingerprints, whether or not an alert is delivered. Outside the Fire
           branch deliberately — see ObserveOccurrencesAsync: counting only at delivery time lets an event
           that ages out during a cooldown mask an arrival, and the total undercounts by exactly the number
           of events the window retired while nobody was looking. Skipped when the gate is disabled or the
           fetch failed (blockingRows null), because there is no observation to make. */
        var blockingOccurrences = default(OccurrenceTotals);
        if (blockingRows is not null)
        {
            blockingOccurrences = await ObserveOccurrencesAsync(
                key, BlockingWatermarkMetric,
                AlertContextBuilders.BlockingIncidents(serverName, blockingRows, _settings.ExcludedDatabases),
                now);
        }

        if (blockingDecision.Fire)                                                  /* :155 */
        {
            var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Blocking Detected" }; /* :157 */
            bool isMuted = _isAlertMuted(muteCtx);                                  /* :158 */
            _lastBlockingAlert[key] = now;                                          /* :159 */

            /* :172-173 — Lite's BuildBlockingContextAsync refetches the same rows; the engine
               reuses this sweep's fetch (identical query/window). */
            var blockingContext = AlertContextBuilders.BuildBlockingContext(
                serverName, blockingRows, _settings.ExcludedDatabases, blockingOccurrences.Decorate);
            var detailText = AlertContextBuilders.ContextToDetailText(blockingContext);

            /* :175-183 — SendDetectedAlertAsync's #1141/#1236 delivery-mode fan-out is an
               IAlertDeliverer concern; the engine emits one outcome. ShortMessage = the toast body
               of :167. Numerics carried explicitly (#1830): the count text happens to parse today,
               but the stored value must not depend on parse luck. */
            await FireAsync(new AlertOutcome(
                key, serverName, "Blocking Detected",
                effectiveBlockingCount.ToString(),
                _settings.BlockingCountThreshold.ToString(),
                blockingContext, detailText,
                NumericCurrentValue: effectiveBlockingCount, NumericThresholdValue: _settings.BlockingCountThreshold,
                Muted: isMuted, Severity: blockingContext?.SeverityOverride,
                ShortMessage: $"{effectiveBlockingCount} blocking session(s)"), ct);
        }
        else if (!blockingDecision.Active && wasBlockingActive)                     /* :185 */
        {
            /* #2216: the incident is over, so its per-fingerprint counters are too — the next incident's
               total should start from 1 with a start time that says so. When this sweep OBSERVED (rows
               fetched), the observation above already recorded that: an empty window yields an empty state
               set, which the replace-the-set contract writes as a delete. Rows are null only when the gate
               is DISABLED — a fetch failure returns before reaching here — and turning the alert off should
               still drop the counters rather than leave them for the staleness horizon. */
            if (blockingRows is null)
            {
                await ClearOccurrencesAsync(key, BlockingWatermarkMetric);
            }

            if (!suppressed && _settings.BlockingEnabled)                           /* :187 */
            {
                await NotifyResolutionAsync(new AlertResolution(
                    key, serverName, "Blocking Detected",
                    "Blocking Cleared",                                             /* :190 */
                    $"{serverName}: No active blocking"), ct);                      /* :191 */
            }
        }

        /* #1839 — the second, independent blocking gate, evaluated here so it can reuse THIS sweep's
           blocked-process rows for its content instead of refetching them. Deliberately downstream of
           the count gate's fetch-failure `return` above: when the store can't answer for blocked
           processes it can't answer for blocking snapshots either, and firing a wait alert with no
           incident content is worse than skipping the sweep (state untouched, same as every other
           check's failure shape). */
        await CheckBlockingWaitAsync(key, serverName, now, alertCooldown, suppressed, blockingRows, ct);
    }

    /* ---------------- per-fingerprint occurrence counters (#2216) ---------------- */

    /// <summary>
    /// Observes one sweep's incidents for a metric: loads the persisted per-fingerprint state, accumulates
    /// this sweep's window counts into it, persists when there is something to write, and returns the totals
    /// for the fired alert to attach.
    ///
    /// <para>Called on EVERY sweep that successfully fetched rows — NOT only the sweeps that deliver. That is
    /// the whole reason the accumulator keeps a mark separate from
    /// <see cref="RollingCountAlertGate"/>'s: observing only at delivery time makes the two marks advance at
    /// the same cadence, and then every event that ages out of the window during a cooldown masks an arrival
    /// and the total silently undercounts. A sweep's grouping is UNCAPPED for the same reason the observation
    /// is unconditional — the render path's top-N cap is a display budget, and a fingerprint outside it still
    /// has a live incident whose state must not be dropped.</para>
    ///
    /// <para>Failure-isolated at both ends: a store that cannot answer yields an empty map, which the
    /// accumulator treats as first contact — every total equals its window count, exactly the pre-#2216
    /// information. An alert that is already firing must never be lost to bookkeeping.</para>
    /// </summary>
    private async Task<OccurrenceTotals> ObserveOccurrencesAsync(
        string key, string metricName, IReadOnlyList<AlertIncident> incidents, DateTime now)
    {
        IReadOnlyDictionary<string, IncidentOccurrenceState> persisted;
        try
        {
            persisted = await _stateStore.LoadIncidentOccurrencesAsync(key, metricName)
                ?? EmptyOccurrenceStates;
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: an occurrence total is bookkeeping ABOUT an alert,
               not the condition read the alert is judged on. The check still fires or resolves on its own
               evidence when this fails, so alerting did not go blind — it lost a count. */
            _logger?.LogWarning("Could not load incident occurrences for {Metric}: {Message}", metricName, ex.Message);
            persisted = EmptyOccurrenceStates;
        }

        var result = IncidentOccurrenceAccumulator.Accumulate(incidents, persisted, now, OccurrenceStaleAfter);

        if (result.Changed)
        {
            await SaveOccurrencesAsync(key, metricName, result.States);
        }

        return new OccurrenceTotals(result.States);
    }

    /// <summary>
    /// Records the falling edge: the metric has no incidents left, so its counters are cleared and the next
    /// incident starts from 1 with a fresh start time. An empty set IS the clear — see
    /// <see cref="IAlertStateStore.SaveIncidentOccurrencesAsync"/>.
    /// </summary>
    private Task ClearOccurrencesAsync(string key, string metricName) =>
        SaveOccurrencesAsync(key, metricName, EmptyOccurrenceStates);

    private async Task SaveOccurrencesAsync(
        string key, string metricName, IReadOnlyDictionary<string, IncidentOccurrenceState> states)
    {
        try
        {
            await _stateStore.SaveIncidentOccurrencesAsync(key, metricName, states);
        }
        catch (Exception ex)
        {
            /* A dropped write costs accuracy on the next delivery's total — that fingerprint reads as new
               and restarts, with a start time saying so — never a missed or duplicated alert. */
            /* NOT counted by #3013's counter: a WRITE, and the counter is about reads the alert pass performs
               and swallows. Logged at Warning for the same reason. */
            _logger?.LogWarning("Could not persist incident occurrences for {Metric}: {Message}", metricName, ex.Message);
        }
    }

    private static readonly IReadOnlyDictionary<string, IncidentOccurrenceState> EmptyOccurrenceStates =
        new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal);

    /// <summary>
    /// This sweep's per-fingerprint totals, ready for the fired alert's incidents to pick up.
    ///
    /// <para>The accounting is already DONE by the time this exists — <see cref="Decorate"/> is a pure
    /// lookup, not a second accumulation. That split is what keeps the arithmetic honest: the counting
    /// happens once per sweep against the store, and the render path merely reads it. An earlier shape had
    /// the builder's decorator do the accumulating, which meant it only ran on the sweeps that delivered an
    /// alert and only for the incidents that survived the render cap.</para>
    /// </summary>
    private readonly struct OccurrenceTotals
    {
        private readonly IReadOnlyDictionary<string, IncidentOccurrenceState> _states;

        internal OccurrenceTotals(IReadOnlyDictionary<string, IncidentOccurrenceState> states) =>
            _states = states;

        /// <summary>
        /// The builder's pre-render hook. Attaches each incident's total; an incident with no state (a blank
        /// fingerprint, or the vanishingly unlikely case of the render path grouping to a key the sweep's
        /// grouping did not produce) is passed through carrying null, which reads as "no total available"
        /// rather than a fabricated zero.
        /// </summary>
        internal IReadOnlyList<AlertIncident> Decorate(IReadOnlyList<AlertIncident> incidents)
        {
            if (_states is null || _states.Count == 0)
            {
                return incidents;
            }

            var decorated = new List<AlertIncident>(incidents.Count);
            foreach (var incident in incidents)
            {
                decorated.Add(
                    incident is not null
                    && !string.IsNullOrEmpty(incident.DedupKey)
                    && _states.TryGetValue(incident.DedupKey, out var state)
                        ? incident with
                        {
                            TotalOccurrences = state.TotalOccurrences,
                            IncidentStartedUtc = state.IncidentStartedUtc,
                            /* #2361: LastObservedUtc already exists on the state -- it is the value the
                               staleness horizon compares against so a flat incident does not expire itself.
                               It simply never reached the incident. This is a projection, not a new
                               measurement, which is why it rides the same hook as the two above it. */
                            LastEventUtc = state.LastObservedUtc,
                        }
                        : incident!);
            }

            return decorated;
        }
    }

    /* ---------------- blocking wait time (#1839) ---------------- */

    /// <summary>
    /// The total-blocked-wait gate: LEVEL-triggered on the sum of <c>wait_time_ms</c> in the latest
    /// blocking snapshot, mirroring the High CPU mechanism above (active flag → cooldown re-fire while
    /// still above → resolve on the way down) rather than the count gate's rolling-window edge trigger.
    /// The two answer different questions — a count cannot distinguish one session blocked for an hour
    /// from one blocked for a second — so this reports under its OWN metric name, keeping mute rules,
    /// history rows and cooldown state from tangling with "Blocking Detected".
    /// <para>
    /// Both gates sit under <see cref="IAlertEngineSettings.BlockingEnabled"/>: turning blocking alerts
    /// off must silence both, exactly as a user reading one toggle would expect. With the threshold at
    /// its shipped 0 the adapter read never happens at all.
    /// </para>
    /// </summary>
    private async Task CheckBlockingWaitAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed,
        List<BlockedProcessAlertRow>? blockingRows, CancellationToken ct)
    {
        int thresholdSeconds = _settings.BlockingWaitSecondsThreshold;
        bool enabled = _settings.BlockingEnabled && thresholdSeconds > 0;

        CurrentBlockingWaitResult? current = null;
        if (enabled)
        {
            try
            {
                current = await _readAdapter.GetCurrentBlockingWaitAsync(key, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                /* Log and skip for the sweep — state untouched, so a transient store error neither
                   fires nor resolves (the same adaptation (2) shape as the count gate). */
                _logger?.LogError("Failed to check blocking wait time for {Server}: {Message}", serverName, ex.Message);
                _readFailures?.RecordReadFailure(key, "blocking wait time");
                return;
            }
        }

        /* A stale snapshot is NOT evidence (#1812's rule): it neither fires nor holds the alert
           active — see CurrentBlockingWaitResult for why staleness resolves here but not for jobs. */
        long thresholdMs = (long)thresholdSeconds * 1000L;
        bool exceeded = enabled
            && current is { SnapshotIsFresh: true }
            && current.TotalWaitMs >= thresholdMs;

        if (exceeded)
        {
            _activeBlockingWaitAlert[key] = true;
            if (!suppressed && CooldownElapsed(_lastBlockingWaitAlert, key, now, alertCooldown))
            {
                var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Blocking Wait Time" };
                bool isMuted = _isAlertMuted(muteCtx);
                _lastBlockingWaitAlert[key] = now;                                  /* stamped even when muted */

                /* Same incident content the count gate ships, built from the rows this sweep already
                   fetched — an operator who gets this alert gets today's Blocking Detected detail. */
                var blockingContext = AlertContextBuilders.BuildBlockingContext(serverName, blockingRows, _settings.ExcludedDatabases);
                var detailText = AlertContextBuilders.ContextToDetailText(blockingContext);

                /* REAL numerics (#1830): the display text is prose ("745s across 3 blocked session(s)"),
                   which no history-store parser could turn back into a number — the value has to travel
                   as a number or every history row lands at 0, which is the defect #1830 just fixed. */
                double totalWaitSeconds = current!.TotalWaitSeconds;
                await FireAsync(new AlertOutcome(
                    key, serverName, "Blocking Wait Time",
                    $"{totalWaitSeconds:F0}s across {current.BlockedSessionCount} blocked session(s)",
                    $"{thresholdSeconds}s",
                    blockingContext, detailText,
                    NumericCurrentValue: totalWaitSeconds, NumericThresholdValue: thresholdSeconds,
                    Muted: isMuted, Severity: blockingContext?.SeverityOverride,
                    ShortMessage: $"{totalWaitSeconds:F0}s total blocked wait across {current.BlockedSessionCount} session(s) (threshold: {thresholdSeconds}s)"), ct);
            }
        }
        else if (_activeBlockingWaitAlert.TryGetValue(key, out var wasActive) && wasActive)
        {
            _activeBlockingWaitAlert[key] = false;
            /* Announced only while the gate is still on — disabling it, or zeroing the threshold, flips
               `exceeded` false without blocking having actually cleared (the CPU check's rule). */
            if (!suppressed && enabled)
            {
                await NotifyResolutionAsync(new AlertResolution(
                    key, serverName, "Blocking Wait Time",
                    "Blocking Wait Cleared",
                    $"{serverName}: Total blocked wait back under {thresholdSeconds}s"), ct);
            }
        }
    }

    /* ---------------- deadlocks (Lite AlertEngine.cs:196-271) ---------------- */

    private async Task CheckDeadlocksAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        List<DeadlockAlertRow>? deadlockRows = null;
        int effectiveDeadlockCount = 0;

        if (_settings.DeadlockEnabled)
        {
            try
            {
                /* ONE fetch serves the rolling count, the excluded-database recount (:198-211),
                   and the fired alert's context (:249) — class remarks adaptation (1). */
                deadlockRows = await _readAdapter.GetRecentDeadlocksAsync(key, hoursBack: RollingCountWindowHours, ct);
                effectiveDeadlockCount = deadlockRows.Count;

                /* :198-205 — recount excluding deadlocks whose processes ALL ran in excluded
                   databases (graph-XML parse via the shared IsDeadlockExcluded). */
                if (_settings.ExcludedDatabases.Count > 0
                    && effectiveDeadlockCount >= _settings.DeadlockCountThreshold)
                {
                    effectiveDeadlockCount = deadlockRows
                        .Count(r => !AlertContextBuilders.IsDeadlockExcluded(r, _settings.ExcludedDatabases));
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                /* :207-210 shape — log and skip (class remarks adaptation (2)). */
                _logger?.LogError("Failed to check deadlocks for {Server}: {Message}", serverName, ex.Message);
                _readFailures?.RecordReadFailure(key, "deadlocks");
                return;
            }
        }

        /* Edge-trigger the rolling 1-hour count (#1091) — :213-227. */
        int deadlockWatermark = _lastAlertedDeadlockCount.TryGetValue(key, out var ladc) ? ladc : 0; /* :216 */
        bool deadlockCooldownElapsed = CooldownElapsed(_lastDeadlockAlert, key, now, alertCooldown); /* :217 */
        var deadlockDecision = _settings.DeadlockEnabled
            ? RollingCountAlertGate.Evaluate(effectiveDeadlockCount, _settings.DeadlockCountThreshold, deadlockWatermark, deadlockCooldownElapsed, suppressed)
            : new RollingCountAlertGate.Decision(false, false, 0);                  /* :218-220 */
        _lastAlertedDeadlockCount[key] = deadlockDecision.Watermark;                /* :221 */
        if (deadlockDecision.Watermark != deadlockWatermark)                        /* :224 — persist on change (#1145) */
        {
            await _stateStore.SaveEdgeTriggerWatermarkAsync(key, DeadlockWatermarkMetric, deadlockDecision.Watermark); /* :226 */
        }

        bool wasDeadlockActive = _activeDeadlockAlert.TryGetValue(key, out var wasDeadlock) && wasDeadlock; /* :229 */
        _activeDeadlockAlert[key] = deadlockDecision.Active;                        /* :230 */

        /* #2216: observe every sweep — see the blocking twin above for why this cannot sit inside Fire. */
        var deadlockOccurrences = default(OccurrenceTotals);
        if (deadlockRows is not null)
        {
            deadlockOccurrences = await ObserveOccurrencesAsync(
                key, DeadlockWatermarkMetric,
                AlertContextBuilders.DeadlockIncidents(serverName, deadlockRows, _settings.ExcludedDatabases),
                now);
        }

        if (deadlockDecision.Fire)                                                  /* :232 */
        {
            var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Deadlocks Detected" }; /* :234 */
            bool isMuted = _isAlertMuted(muteCtx);                                  /* :235 */
            _lastDeadlockAlert[key] = now;                                          /* :236 */

            /* :249-250 — context from this sweep's fetch. */
            var deadlockContext = AlertContextBuilders.BuildDeadlockContext(
                serverName, deadlockRows, _settings.ExcludedDatabases, deadlockOccurrences.Decorate);
            var detailText = AlertContextBuilders.ContextToDetailText(deadlockContext);

            /* :252-260 — ShortMessage = the toast body of :244. Numerics carried explicitly (#1830):
               the count text happens to parse today, but the stored value must not depend on parse luck. */
            await FireAsync(new AlertOutcome(
                key, serverName, "Deadlocks Detected",
                effectiveDeadlockCount.ToString(),
                _settings.DeadlockCountThreshold.ToString(),
                deadlockContext, detailText,
                NumericCurrentValue: effectiveDeadlockCount, NumericThresholdValue: _settings.DeadlockCountThreshold,
                Muted: isMuted, Severity: deadlockContext?.SeverityOverride,
                ShortMessage: $"{effectiveDeadlockCount} deadlock(s) in the last hour"), ct);
        }
        else if (!deadlockDecision.Active && wasDeadlockActive)                     /* :262 */
        {
            /* #2216: the falling edge — see the blocking twin above for why this is only the disabled-gate
               case; an observed empty window already cleared itself. */
            if (deadlockRows is null)
            {
                await ClearOccurrencesAsync(key, DeadlockWatermarkMetric);
            }

            if (!suppressed && _settings.DeadlockEnabled)                           /* :264 */
            {
                await NotifyResolutionAsync(new AlertResolution(
                    key, serverName, "Deadlocks Detected",
                    "Deadlocks Cleared",                                            /* :267 */
                    $"{serverName}: No deadlocks in the last hour"), ct);           /* :268 */
            }
        }
    }

    /* ---------------- poison waits (Lite AlertEngine.cs:273-339) ---------------- */

    private async Task CheckPoisonWaitsAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.PoisonWaitEnabled)                                           /* :274 */
        {
            return;
        }

        try
        {
            var triggered = await _readAdapter.GetPoisonWaitDeltasAsync(key, _settings.PoisonWaitThresholdMs, ct); /* :278 */

            if (triggered.Count > 0)
            {
                _activePoisonWaitAlert[key] = true;                                 /* :282 */

                /* The read adapter's own window can hand back the SAME wait_stats row(s) across
                   multiple sweeps when the collector lags the cooldown — see the field's own
                   doc comment. Only a collection_time newer than the one last fired on counts as
                   a fresh observation; a cooldown-elapsed re-ask against an unrefreshed row must
                   wait for the NEXT sweep rather than re-fire on data it already reported. */
                var newestCollectionTime = triggered.Max(w => w.CollectionTime);
                bool hasFreshCollection = !_lastPoisonWaitCollectionTime.TryGetValue(key, out var lastCollectionTime)
                    || newestCollectionTime > lastCollectionTime;

                if (!suppressed && hasFreshCollection && CooldownElapsed(_lastPoisonWaitAlert, key, now, alertCooldown)) /* :283 */
                {
                    var worst = triggered[0];                                       /* :285 */
                    var allWaitNames = string.Join(", ", triggered.ConvertAll(w => $"{w.WaitType} ({w.AvgMsPerWait:F0}ms)")); /* :286 */

                    /* :288-293 — mute keys on the worst (highest avg ms/wait) triggered wait type;
                       same documented limitation as Lite. */
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Poison Wait", WaitType = worst.WaitType };
                    bool isMuted = _isAlertMuted(muteCtx);
                    _lastPoisonWaitAlert[key] = now;                                /* :294 */
                    _lastPoisonWaitCollectionTime[key] = newestCollectionTime;

                    var poisonContext = AlertContextBuilders.BuildPoisonWaitContext(triggered); /* :307 */
                    var detailText = AlertContextBuilders.ContextToDetailText(poisonContext);   /* :308 */

                    /* :310-320. ShortMessage = the toast body of :302. */
                    await FireAsync(new AlertOutcome(
                        key, serverName, "Poison Wait",
                        allWaitNames,
                        $"{_settings.PoisonWaitThresholdMs}ms avg",
                        poisonContext, detailText,
                        NumericCurrentValue: worst.AvgMsPerWait,
                        NumericThresholdValue: _settings.PoisonWaitThresholdMs,
                        Muted: isMuted, Severity: poisonContext?.SeverityOverride,
                        ShortMessage: $"{worst.WaitType} avg {worst.AvgMsPerWait:F0}ms/wait"), ct);
                }
            }
            else if (_activePoisonWaitAlert.TryGetValue(key, out var wasPoisonWait) && wasPoisonWait) /* :323 */
            {
                _activePoisonWaitAlert[key] = false;                                /* :325 */
                if (!suppressed)                                                    /* :326 */
                {
                    await NotifyResolutionAsync(new AlertResolution(
                        key, serverName, "Poison Wait",
                        "Poison Waits Cleared",                                     /* :329 */
                        $"{serverName}: Poison wait avg below threshold"), ct);     /* :330 */
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Failed to check poison waits for {Server}: {Message}", serverName, ex.Message); /* :337 */
            _readFailures?.RecordReadFailure(key, "poison waits");
        }
    }

    /* ---------------- long-running queries (Lite AlertEngine.cs:341-411) ---------------- */

    private async Task CheckLongRunningQueriesAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.LongRunningQueryEnabled)                                     /* :342 */
        {
            return;
        }

        try
        {
            var longRunning = await _readAdapter.GetLongRunningQueriesAsync(       /* :346 */
                key,
                _settings.LongRunningQueryThresholdMinutes,
                _settings.LongRunningQueryMaxResults,
                _settings.LongRunningQueryExcludeSpServerDiagnostics,
                _settings.LongRunningQueryExcludeWaitFor,
                _settings.LongRunningQueryExcludeBackups,
                _settings.LongRunningQueryExcludeMiscWaits,
                _settings.LongRunningQueryExcludeCdc,
                _settings.ExcludedDatabases,
                ct);

            /* #2362: observe every sweep, OUTSIDE the fire branch — the #2216 reasoning, which applies
               identically here: counting only at delivery lets an event that ages out during a cooldown mask
               an arrival. The list is UNCAPPED while the render below is capped, so a fingerprint outside the
               displayed top N keeps its total instead of restarting. */
            var lrqOccurrences = await ObserveOccurrencesAsync(
                key, LongRunningQueryWatermarkMetric, AlertContextBuilders.LongRunningQueryIncidents(serverName, longRunning), now);
            if (longRunning.Count > 0)
            {
                _activeLongRunningQueryAlert[key] = true;                           /* :350 */
                if (!suppressed && CooldownElapsed(_lastLongRunningQueryAlert, key, now, alertCooldown)) /* :351 */
                {
                    var worst = longRunning[0];                                     /* :353 */
                    var elapsedMinutes = worst.ElapsedSeconds / 60;                 /* :354 — integer division, exactly Lite */
                    /* :355-356 — the query-text preview feeds ShortMessage (the toast body). */
                    var preview = AlertContextBuilders.TruncateText(worst.QueryText, 80);
                    var previewSuffix = string.IsNullOrEmpty(preview) ? "" : $" — {preview}";

                    var muteCtx = new AlertMuteContext                              /* :358-364 */
                    {
                        ServerName = serverName,
                        MetricName = "Long-Running Query",
                        DatabaseName = worst.DatabaseName,
                        QueryText = worst.QueryText
                    };
                    bool isMuted = _isAlertMuted(muteCtx);                          /* :365 */
                    _lastLongRunningQueryAlert[key] = now;                          /* :366 */

                    var lrqContext = AlertContextBuilders.BuildLongRunningQueryContext(serverName, longRunning, lrqOccurrences.Decorate); /* :379 */
                    var detailText = AlertContextBuilders.ContextToDetailText(lrqContext);                       /* :380 */

                    /* :382-392. ShortMessage = the toast body of :374. */
                    await FireAsync(new AlertOutcome(
                        key, serverName, "Long-Running Query",
                        $"{longRunning.Count} query(s), longest {elapsedMinutes}m",
                        $"{_settings.LongRunningQueryThresholdMinutes}m",
                        lrqContext, detailText,
                        NumericCurrentValue: elapsedMinutes,
                        NumericThresholdValue: _settings.LongRunningQueryThresholdMinutes,
                        Muted: isMuted, Severity: lrqContext?.SeverityOverride,
                        ShortMessage: $"Session #{worst.SessionId} running {elapsedMinutes}m{previewSuffix}"), ct);
                }
            }
            else if (_activeLongRunningQueryAlert.TryGetValue(key, out var wasLongRunning) && wasLongRunning) /* :395 */
            {
                _activeLongRunningQueryAlert[key] = false;
                await ClearOccurrencesAsync(key, LongRunningQueryWatermarkMetric);                          /* :397 */
                if (!suppressed)                                                    /* :398 */
                {
                    await NotifyResolutionAsync(new AlertResolution(
                        key, serverName, "Long-Running Query",
                        "Long-Running Queries Cleared",                             /* :401 */
                        $"{serverName}: No queries over threshold"), ct);           /* :402 */
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Failed to check long-running queries for {Server}: {Message}", serverName, ex.Message); /* :409 */
            _readFailures?.RecordReadFailure(key, "long-running queries");
        }
    }

    /* ---------------- tempdb space (Lite AlertEngine.cs:413-473) ---------------- */

    private async Task CheckTempDbSpaceAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.TempDbSpaceEnabled)                                          /* :414 */
        {
            return;
        }

        try
        {
            var tempDb = await _readAdapter.GetTempDbSpaceAsync(key, ct);           /* :418 */

            if (tempDb != null && tempDb.UsedPercent >= _settings.TempDbSpaceThresholdPercent) /* :420 */
            {
                _activeTempDbSpaceAlert[key] = true;                                /* :422 */
                if (!suppressed && CooldownElapsed(_lastTempDbSpaceAlert, key, now, alertCooldown)) /* :423 */
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "tempdb Space" }; /* :425 */
                    bool isMuted = _isAlertMuted(muteCtx);                          /* :426 */
                    _lastTempDbSpaceAlert[key] = now;                               /* :427 */

                    var tempDbContext = AlertContextBuilders.BuildTempDbSpaceContext(tempDb); /* :440 */
                    var detailText = AlertContextBuilders.ContextToDetailText(tempDbContext); /* :441 */

                    /* :443-453. ShortMessage = the toast body of :435. */
                    await FireAsync(new AlertOutcome(
                        key, serverName, "tempdb Space",
                        $"{tempDb.UsedPercent:F0}% used ({tempDb.TotalReservedMb:F0} MB)",
                        $"{_settings.TempDbSpaceThresholdPercent}%",
                        tempDbContext, detailText,
                        NumericCurrentValue: tempDb.UsedPercent,
                        NumericThresholdValue: _settings.TempDbSpaceThresholdPercent,
                        Muted: isMuted, Severity: tempDbContext?.SeverityOverride,
                        ShortMessage: $"tempdb {tempDb.UsedPercent:F0}% used"), ct);
                }
            }
            else if (_activeTempDbSpaceAlert.TryGetValue(key, out var wasTempDb) && wasTempDb) /* :456 */
            {
                _activeTempDbSpaceAlert[key] = false;                               /* :458 */
                if (!suppressed)                                                    /* :459 */
                {
                    var pct = tempDb != null ? $"{tempDb.UsedPercent:F0}%" : "N/A"; /* :461 */
                    await NotifyResolutionAsync(new AlertResolution(
                        key, serverName, "tempdb Space",
                        "tempdb Space Resolved",                                    /* :463 */
                        $"{serverName}: tempdb usage back to {pct}"), ct);          /* :464 */
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Failed to check TempDB space for {Server}: {Message}", serverName, ex.Message); /* :471 */
            _readFailures?.RecordReadFailure(key, "TempDB space");
        }
    }

    /* ---------------- volume free space (Lite AlertEngine.cs:475-555) ---------------- */

    /// <returns>
    /// True when at least one volume is breached this sweep — the standing condition Lite's #754
    /// tab badge derives from (:487 <c>curBadgeLowDisk</c>), computed BEFORE the worsening/cooldown/
    /// suppression gates. False when the check is disabled or the read failed.
    /// </returns>
    private async Task<bool> CheckLowDiskAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.LowDiskEnabled)                                              /* :476 */
        {
            return false;
        }

        bool conditionPresent = false;
        try
        {
            var volumes = await _readAdapter.GetVolumeFreeSpaceAsync(key, ct);      /* :480 */
            var breached = AlertContextBuilders.GetBreachedVolumes(volumes, _settings.LowDiskThresholdPercent, _settings.LowDiskThresholdGb); /* :481 */
            conditionPresent = breached.Count > 0;                                  /* :487 — feeds the sweep result */

            /* #2362: observe every sweep, OUTSIDE the fire branch — the #2216 reasoning, which applies
               identically here: counting only at delivery lets an event that ages out during a cooldown mask
               an arrival. The list is UNCAPPED while the render below is capped, so a fingerprint outside the
               displayed top N keeps its total instead of restarting. */
            var lowDiskOccurrences = await ObserveOccurrencesAsync(
                key, VolumeFreeSpaceWatermarkMetric, AlertContextBuilders.VolumeFreeSpaceIncidents(serverName, breached), now);
            if (breached.Count > 0)
            {
                var worst = breached[0];                                            /* :489 */
                _activeLowDiskAlert[key] = true;                                    /* :490 */
                double? lastLowDiskPercent =
                    _lastAlertedLowDiskPercent.TryGetValue(key, out var lowDiskPct) ? lowDiskPct : (double?)null; /* :491-492 */
                /* :493-497 — #754 follow-up: notify only on a fresh or worsening breach. */
                if (!suppressed
                    && LowDiskAlertGate.ShouldAlert(worst.FreePercent, lastLowDiskPercent)
                    && CooldownElapsed(_lastLowDiskAlert, key, now, alertCooldown))
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Volume Free Space" }; /* :499 */
                    bool isMuted = _isAlertMuted(muteCtx);                          /* :500 */
                    _lastLowDiskAlert[key] = now;                                   /* :501 */
                    _lastAlertedLowDiskPercent[key] = worst.FreePercent;            /* :502 */

                    var lowDiskContext = AlertContextBuilders.BuildVolumeFreeSpaceContext(serverName, breached, lowDiskOccurrences.Decorate); /* :515 */
                    /* :516-522 — #1136: grade WARNING normally, CRITICAL when critically low. */
                    if (lowDiskContext is not null && LowDiskAlertGate.IsCriticallyLow(
                        worst.FreePercent, worst.FreeGb, _settings.DiskCriticalFreePercent, _settings.DiskCriticalFreeGb))
                    {
                        lowDiskContext.SeverityOverride = AlertSeverityLevel.Critical;
                    }
                    var detailText = AlertContextBuilders.ContextToDetailText(lowDiskContext); /* :523 */

                    /* :525-535. ShortMessage = the toast body of :510. */
                    await FireAsync(new AlertOutcome(
                        key, serverName, "Volume Free Space",
                        $"{worst.MountPoint} {worst.FreePercent:F0}% free ({worst.FreeGb:F1} GB)",
                        AlertContextBuilders.FormatLowDiskThreshold(_settings.LowDiskThresholdPercent, _settings.LowDiskThresholdGb),
                        lowDiskContext, detailText,
                        NumericCurrentValue: worst.FreePercent,
                        NumericThresholdValue: _settings.LowDiskThresholdPercent,
                        Muted: isMuted, Severity: lowDiskContext?.SeverityOverride,
                        ShortMessage: $"{worst.MountPoint} {worst.FreePercent:F0}% free ({worst.FreeGb:F1} GB)"), ct);
                }
            }
            else if (_activeLowDiskAlert.TryGetValue(key, out var wasLowDisk) && wasLowDisk) /* :538 */
            {
                _activeLowDiskAlert[key] = false;
                await ClearOccurrencesAsync(key, VolumeFreeSpaceWatermarkMetric);                                   /* :540 */
                _lastAlertedLowDiskPercent.TryRemove(key, out _);                   /* :541 */
                if (!suppressed)                                                    /* :542 */
                {
                    await NotifyResolutionAsync(new AlertResolution(
                        key, serverName, "Volume Free Space",
                        "Volume Free Space Resolved",                               /* :545 */
                        $"{serverName}: All volumes back above threshold"), ct);    /* :546 */
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Failed to check volume free space for {Server}: {Message}", serverName, ex.Message); /* :553 */
            _readFailures?.RecordReadFailure(key, "volume free space");
        }

        return conditionPresent;
    }

    /* ---------------- persistent version store (#1984) ---------------- */

    /// <summary>
    /// The ADR persistent-version-store twin of <see cref="CheckLowDiskAsync"/>: reads the newest
    /// pvs_stats snapshot's ADR databases, breaches on PVS percent-of-database AND the GB floor
    /// (<see cref="AlertContextBuilders.GetBreachedPvsDatabases"/>), names the worst database with
    /// up to five breaching in the context, and re-fires only on a fresh or worsening breach
    /// (<see cref="PvsAlertGate"/>) — a large PVS stays allocated even after its cause clears, so
    /// without the gate a recovered incident would re-notify every cooldown for hours. No severity
    /// tier: MS documents no "critical" PVS level, and inventing one is the folklore the collector
    /// deliberately avoided. Level-triggered with a resolved transition when no database breaches.
    /// </summary>
    private async Task CheckPvsPressureAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.PvsEnabled || _settings.PvsThresholdPercent <= 0)
        {
            return;
        }

        try
        {
            var databases = await _readAdapter.GetPvsPressureAsync(key, ct);
            var breached = AlertContextBuilders.GetBreachedPvsDatabases(databases, _settings.PvsThresholdPercent, _settings.PvsFloorGb);

            /* #2362: observe every sweep, OUTSIDE the fire branch — the #2216 reasoning, which applies
               identically here: counting only at delivery lets an event that ages out during a cooldown mask
               an arrival. The list is UNCAPPED while the render below is capped, so a fingerprint outside the
               displayed top N keeps its total instead of restarting. */
            var pvsOccurrences = await ObserveOccurrencesAsync(
                key, PvsWatermarkMetric, AlertContextBuilders.PvsPressureIncidents(serverName, breached), now);
            if (breached.Count > 0)
            {
                var worst = breached[0];
                _activePvsAlert[key] = true;
                double? lastPvsPercent =
                    _lastAlertedPvsPercent.TryGetValue(key, out var pvsPct) ? pvsPct : (double?)null;
                if (!suppressed
                    && PvsAlertGate.ShouldAlert(worst.PvsPercent, lastPvsPercent)
                    && CooldownElapsed(_lastPvsAlert, key, now, alertCooldown))
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Version Store (PVS)" };
                    bool isMuted = _isAlertMuted(muteCtx);
                    _lastPvsAlert[key] = now;
                    _lastAlertedPvsPercent[key] = worst.PvsPercent;

                    var pvsContext = AlertContextBuilders.BuildPvsPressureContext(serverName, breached, pvsOccurrences.Decorate);
                    var detailText = AlertContextBuilders.ContextToDetailText(pvsContext);

                    await FireAsync(new AlertOutcome(
                        key, serverName, "Version Store (PVS)",
                        $"{worst.DatabaseName} PVS {worst.PvsPercent:F0}% of database ({worst.PvsGb:F1} GB)",
                        AlertContextBuilders.FormatPvsThreshold(_settings.PvsThresholdPercent, _settings.PvsFloorGb),
                        pvsContext, detailText,
                        NumericCurrentValue: worst.PvsPercent,
                        NumericThresholdValue: _settings.PvsThresholdPercent,
                        Muted: isMuted, Severity: null,
                        ShortMessage: $"{worst.DatabaseName} PVS {worst.PvsPercent:F0}% of database ({worst.PvsGb:F1} GB)"), ct);
                }
            }
            else if (_activePvsAlert.TryGetValue(key, out var wasPvs) && wasPvs)
            {
                _activePvsAlert[key] = false;
                await ClearOccurrencesAsync(key, PvsWatermarkMetric);
                _lastAlertedPvsPercent.TryRemove(key, out _);
                if (!suppressed)
                {
                    await NotifyResolutionAsync(new AlertResolution(
                        key, serverName, "Version Store (PVS)",
                        "Version Store (PVS) Resolved",
                        $"{serverName}: All version stores back below threshold"), ct);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Failed to check PVS pressure for {Server}: {Message}", serverName, ex.Message);
            _readFailures?.RecordReadFailure(key, "PVS pressure");
        }
    }

    /* ---------------- database file growth (#2349) ---------------- */

    /// <summary>
    /// The gap between <c>tempdb Space</c> and <c>Volume Free Space</c>: a file that has grown large but has
    /// not yet filled its disk.
    ///
    /// <para><b>Why neither existing alert can express it.</b> <c>tempdb Space</c> fires on
    /// reserved ÷ (reserved + unallocated) — autogrowth adds unallocated extents, so the denominator grows with
    /// the file and the percentage FALLS as tempdb balloons. It answers "is tempdb internally full right now",
    /// which is a real question and structurally not this one. <c>Volume Free Space</c> fires on the
    /// consequence, by which point a restart is already overdue, and cannot attribute the space to one file.</para>
    ///
    /// <para><b>Two gates, both graded per server.</b> <c>config_alert_settings</c> is a single global row, so
    /// an absolute MB threshold is unusable across a fleet whose normal tempdb sizes differ by an order of
    /// magnitude. The RISE gate is the event (#2157's reasoning: a level alone re-pages every cooldown about a
    /// size that has been true since Tuesday, which trains people to mute it); the LEVEL gate is the file as a
    /// share of its volume, which self-scales to each server's disk layout.</para>
    ///
    /// <para>Observation sits OUTSIDE the fire branch, like blocking's (#2216/#2362): counting only at delivery
    /// lets a file that stops breaching during a cooldown mask the next one.</para>
    /// </summary>
    private async Task CheckFileGrowthAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.FileGrowthEnabled)
        {
            return;
        }

        try
        {
            var files = await _readAdapter.GetDatabaseFileGrowthAsync(
                key, _settings.FileGrowthLookbackMinutes, ct);

            var breached = AlertContextBuilders.GetBreachedFiles(
                files, _settings.FileGrowthRiseMb, _settings.FileGrowthVolumePercent);

            var fileGrowthOccurrences = await ObserveOccurrencesAsync(
                key, FileGrowthWatermarkMetric,
                AlertContextBuilders.FileGrowthIncidents(serverName, breached), now);

            if (breached.Count > 0)
            {
                var worst = breached[0];
                _activeFileGrowthAlert[key] = true;

                if (!suppressed && CooldownElapsed(_lastFileGrowthAlert, key, now, alertCooldown))
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Database File Growth" };
                    bool isMuted = _isAlertMuted(muteCtx);
                    _lastFileGrowthAlert[key] = now;

                    var context = AlertContextBuilders.BuildFileGrowthContext(
                        serverName, breached, fileGrowthOccurrences.Decorate);
                    var detailText = AlertContextBuilders.ContextToDetailText(context);

                    /* The headline names the file, its size and its share of the volume — the three facts that
                       decide whether this is worth getting up for. The rise is in the card. */
                    var headline =
                        $"{worst.DatabaseName}.{worst.FileName} is {worst.TotalSizeGb:F1} GB "
                        + $"({worst.VolumePercent:F0}% of {worst.VolumeMountPoint}), "
                        + $"grew {worst.GrowthGb:F1} GB in {worst.GrowthWindowMinutes:F0} min";

                    await FireAsync(new AlertOutcome(
                        key, serverName, "Database File Growth",
                        headline,
                        $"rise ≥ {_settings.FileGrowthRiseMb} MB or file ≥ {_settings.FileGrowthVolumePercent}% of volume",
                        context, detailText,
                        NumericCurrentValue: worst.VolumePercent,
                        NumericThresholdValue: _settings.FileGrowthVolumePercent,
                        Muted: isMuted, Severity: null,
                        ShortMessage: headline), ct);
                }
            }
            else if (_activeFileGrowthAlert.TryGetValue(key, out var wasGrowing) && wasGrowing)
            {
                _activeFileGrowthAlert[key] = false;
                await ClearOccurrencesAsync(key, FileGrowthWatermarkMetric);

                if (!suppressed)
                {
                    await NotifyResolutionAsync(new AlertResolution(
                        key, serverName, "Database File Growth",
                        "Database File Growth Resolved",
                        $"{serverName}: no file is growing past the threshold or filling its volume"), ct);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Failed to check database file growth for {Server}: {Message}", serverName, ex.Message);
            _readFailures?.RecordReadFailure(key, "database file growth");
        }
    }

    /* ---------------- anomalous Agent jobs (Lite AlertEngine.cs:557-632) ---------------- */

    private async Task CheckAnomalousJobsAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.LongRunningJobEnabled)                                       /* :558 */
        {
            return;
        }

        try
        {
            var jobsResult = await _readAdapter.GetAnomalousJobsAsync(key, _settings.LongRunningJobMultiplier, ct); /* :562 */

            /* #1812: a stale latest snapshot is NO evidence, in either direction. Firing on it re-alerts
               a historical run every cooldown forever (the per-run cooldown key deliberately expires each
               pass below); resolving on it fabricates "jobs cleared" out of a collector that merely
               stopped reporting. Leave the active-state flag untouched — when a fresh snapshot returns,
               evaluation resumes and fires or resolves from real evidence. */
            if (!jobsResult.SnapshotIsFresh)
            {
                _logger?.LogDebug("Long-running-job check skipped for {Server}: the latest running_jobs snapshot is stale (no current evidence)", serverName);
                return;
            }

            var anomalousJobs = jobsResult.Jobs;

            /* :564-573 — the per-run cooldown dict grows without bound; drop entries aged past
               the cooldown each pass (scans ALL servers' entries, exactly like Lite). */
            foreach (var staleJobKey in _lastLongRunningJobAlert
                         .Where(kv => now - kv.Value >= alertCooldown)
                         .Select(kv => kv.Key)
                         .ToList())
            {
                _lastLongRunningJobAlert.TryRemove(staleJobKey, out _);
            }

            /* #2362: observe every sweep, OUTSIDE the fire branch — the #2216 reasoning, which applies
               identically here: counting only at delivery lets an event that ages out during a cooldown mask
               an arrival. The list is UNCAPPED while the render below is capped, so a fingerprint outside the
               displayed top N keeps its total instead of restarting. */
            var jobOccurrences = await ObserveOccurrencesAsync(
                key, AnomalousJobWatermarkMetric, AlertContextBuilders.AnomalousJobIncidents(serverName, anomalousJobs), now);
            if (anomalousJobs.Count > 0)
            {
                _activeLongRunningJobAlert[key] = true;                             /* :577 */
                var worst = anomalousJobs[0];                                       /* :578 */
                var jobKey = $"{key}:{worst.JobId}:{worst.StartTime:O}";            /* :579 */

                if (!suppressed && (!_lastLongRunningJobAlert.TryGetValue(jobKey, out var lastJob) || now - lastJob >= alertCooldown)) /* :581 */
                {
                    var currentMinutes = worst.CurrentDurationSeconds / 60;         /* :583 — feeds ShortMessage (the toast body) */
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Long-Running Job", JobName = worst.JobName }; /* :585 */
                    bool isMuted = _isAlertMuted(muteCtx);                          /* :586 */
                    _lastLongRunningJobAlert[jobKey] = now;                         /* :587 */

                    var jobContext = AlertContextBuilders.BuildAnomalousJobContext(serverName, anomalousJobs, jobOccurrences.Decorate); /* :600 */
                    var detailText = AlertContextBuilders.ContextToDetailText(jobContext);                     /* :601 */

                    /* :603-613. ShortMessage = the toast body of :595. */
                    await FireAsync(new AlertOutcome(
                        key, serverName, "Long-Running Job",
                        $"{anomalousJobs.Count} job(s) exceeding {_settings.LongRunningJobMultiplier}x average",
                        $"{_settings.LongRunningJobMultiplier}x historical avg",
                        jobContext, detailText,
                        NumericCurrentValue: (double)(worst.PercentOfAverage ?? 0),
                        NumericThresholdValue: _settings.LongRunningJobMultiplier * 100,
                        Muted: isMuted, Severity: jobContext?.SeverityOverride,
                        ShortMessage: $"{worst.JobName} at {worst.PercentOfAverage:F0}% of avg ({currentMinutes}m)"), ct);
                }
            }
            else if (_activeLongRunningJobAlert.TryGetValue(key, out var wasJob) && wasJob) /* :616 */
            {
                _activeLongRunningJobAlert[key] = false;
                await ClearOccurrencesAsync(key, AnomalousJobWatermarkMetric);                            /* :618 */
                if (!suppressed)                                                    /* :619 */
                {
                    await NotifyResolutionAsync(new AlertResolution(
                        key, serverName, "Long-Running Job",
                        "Long-Running Jobs Cleared",                                /* :622 */
                        $"{serverName}: No jobs exceeding threshold"), ct);         /* :623 */
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Failed to check anomalous jobs for {Server}: {Message}", serverName, ex.Message); /* :630 */
            _readFailures?.RecordReadFailure(key, "anomalous jobs");
        }
    }

    /* ---------------- failed Agent jobs (Lite AlertEngine.cs:634-717) ---------------- */

    /// <returns>
    /// True when the fetcher returned at least one failure in the lookback window — the standing
    /// condition Lite's #749 tab badge derives from (:663 <c>curBadgeFailedJob</c>), computed
    /// BEFORE the watermark/cooldown/suppression gates. False when the check is disabled, the
    /// server is offline/Azure SQL DB, or the fetch failed.
    /// </returns>
    private async Task<bool> CheckFailedJobsAsync(
        AlertServerSnapshot snapshot, string key, string serverName,
        DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.FailedJobEnabled || _failedJobsFetcher is null)              /* :639 */
        {
            return false;
        }

        bool conditionPresent = false;
        try
        {
            /* :649-653 — Lite gates on online + non-Azure-SQL-DB + HasMsdbAccess. The engine
               gates on the snapshot's online + IsAzureSqlDb flags; the msdb-access probe is
               deliberately NOT part of the seam (Phase-5 review F11) — hosts degrade a denied
               msdb read to an empty list inside the fetcher instead. Failures are point-in-time
               events: no "cleared" notification, watermark-dedup only (:634-638). */
            if (!snapshot.IsOnline || snapshot.IsAzureSqlDb)
            {
                return false;
            }

            var failedJobs = await _failedJobsFetcher(key, _settings.FailedJobLookbackMinutes, ct); /* :657 */
            conditionPresent = failedJobs.Count > 0;                                /* :663 — feeds the sweep result */

            /* #2362: observe every sweep, OUTSIDE the fire branch — the #2216 reasoning, which applies
               identically here: counting only at delivery lets an event that ages out during a cooldown mask
               an arrival. The list is UNCAPPED while the render below is capped, so a fingerprint outside the
               displayed top N keeps its total instead of restarting. */
            var failedJobOccurrences = await ObserveOccurrencesAsync(
                key, FailedJobWatermarkMetric, AlertContextBuilders.FailedJobIncidents(serverName, failedJobs), now);

            /* No ClearOccurrencesAsync counterpart, and that is not an omission: a failed job is an EVENT,
               not a condition that resolves, so this check has no else-branch to clear from. The accumulator's
               staleness horizon is the cleanup path here -- a fingerprint whose gauge stops moving for longer
               than the horizon expires itself, which is exactly the shape an event stream needs. */
            if (failedJobs.Count > 0)
            {
                var newestFailure = failedJobs.Max(j => j.RunDateTime);             /* :665 */
                bool hasWatermark = _lastAlertedFailedJobTime.TryGetValue(key, out var lastFailure); /* :666 */
                bool hasNewFailure = !hasWatermark || newestFailure > lastFailure;  /* :667 */

                if (hasNewFailure && !suppressed &&
                    CooldownElapsed(_lastFailedJobAlert, key, now, alertCooldown))  /* :669-670 */
                {
                    var mostRecent = failedJobs[0]; /* ORDER BY run_datetime DESC — :672 */
                    var jobNames = string.Join(", ", failedJobs.Select(j => j.JobName).Distinct().Take(3)); /* :673 */

                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Failed Agent Job", JobName = mostRecent.JobName }; /* :675 */
                    bool isMuted = _isAlertMuted(muteCtx);                          /* :676 */
                    _lastFailedJobAlert[key] = now;                                 /* :677 */
                    _lastAlertedFailedJobTime[key] = newestFailure;                 /* :678 */
                    /* :679-682 — persist the SERVER-LOCAL watermark on-change only (#1145 parity). */
                    await _stateStore.SaveFailedJobWatermarkAsync(key, newestFailure);

                    var failedJobContext = AlertContextBuilders.BuildFailedJobContext(serverName, failedJobs, failedJobOccurrences.Decorate); /* :695 */
                    var detailText = AlertContextBuilders.ContextToDetailText(failedJobContext);               /* :696 */

                    /* :698-708. ShortMessage = the toast body of :690. */
                    await FireAsync(new AlertOutcome(
                        key, serverName, "Failed Agent Job",
                        $"{failedJobs.Count} job failure(s) in last {_settings.FailedJobLookbackMinutes}m — {jobNames}",
                        $"last {_settings.FailedJobLookbackMinutes}m",
                        failedJobContext, detailText,
                        NumericCurrentValue: failedJobs.Count,
                        NumericThresholdValue: 0,
                        Muted: isMuted, Severity: failedJobContext?.SeverityOverride,
                        ShortMessage: $"{failedJobs.Count} job failure(s) — {jobNames}"), ct);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter, and the only counted-looking site that is
               deliberately not counted. Nothing reachable in this try is a STORE read. The fetcher reads
               the MONITORED SERVER's msdb over its own connection and timeout - the same population
               DarlingWorker.FetchFailedJobsAsync is exempted for, and counting it here while exempting it
               there would put a target-side outage into a number an operator reads as store contention.
               The one store operation in the block is SaveFailedJobWatermarkAsync, a WRITE, and both
               implementations swallow it (PgAlertStateStore and DuckDbAlertHistoryStore each log "Could
               not persist failed-job watermark" without rethrowing), so it cannot reach this catch at
               all. The write sitting inside this try where the blocking, deadlock and database-state
               checks keep theirs outside is a real asymmetry and an inert one. */
            _logger?.LogError("Failed to check failed jobs for {Server}: {Message}", serverName, ex.Message); /* :715 */
        }

        return conditionPresent;
    }

    /* ---------------- database state (offline / unhealthy) ---------------- */

    /// <summary>
    /// Fires when a monitored database's current state DEVIATES from its expected state — the
    /// expected state being the auto-seeded first-observation baseline or the operator's per-database
    /// override (a log-shipping secondary baselines at STANDBY and so never alerts; an "ignore"
    /// override opts a database out entirely). The store computes the deviating set (a two-sample rule)
    /// and does the baseline/ignore comparison (see <see cref="IAlertReadAdapter.GetDatabaseStatesAsync"/>); this
    /// method owns the per-database fire/cooldown/resolution and mute gating. PER-DATABASE: each
    /// deviating database fires and cools down independently, and emits a "recovered" resolution when
    /// its state returns to expected. Severity is graded at the fire site
    /// (<see cref="DatabaseStateTokens.SeverityFor"/>): CRITICAL for the integrity-failure states,
    /// WARNING otherwise. The shared <see cref="IAlertEngineSettings.ExcludedDatabases"/> list is
    /// honoured (parity with the other database-scoped alerts). The read is not freshness-gated (a
    /// standing condition).
    /// </summary>
    private async Task CheckDatabaseStateAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.DatabaseStateEnabled)
        {
            return;
        }

        List<DatabaseStateInfo> deviations;
        try
        {
            deviations = await _readAdapter.GetDatabaseStatesAsync(key, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* Log-and-skip, like the other collected reads: never resolve an active database on a
               failed fetch (that would fabricate a recovery), and never fire on absent evidence. */
            _logger?.LogError("Failed to check database state for {Server}: {Message}", serverName, ex.Message);
            _readFailures?.RecordReadFailure(key, "database state");
            return;
        }

        var excluded = _settings.ExcludedDatabases;

        /* The store already filtered to deviations (current != expected, not ignored); here we only
           drop databases on the shared excluded list, for parity with the other database-scoped alerts.
           Per-database keys (this dict and the active set below) are ORDINAL — case-sensitive — to match
           the stores' case-sensitive expected-state joins, so a database can't key differently here than
           it does in the baseline table. The excluded-databases list stays case-insensitive, matching the
           other alerts' treatment of that user-facing list. */
        var current = new Dictionary<string, DatabaseStateInfo>(StringComparer.Ordinal);
        foreach (var db in deviations)
        {
            if (string.IsNullOrWhiteSpace(db.StateDesc) || string.IsNullOrWhiteSpace(db.DatabaseName))
            {
                continue;
            }

            if (excluded.Count > 0 && excluded.Any(e => string.Equals(e, db.DatabaseName, StringComparison.OrdinalIgnoreCase)))
            {
                continue;
            }

            current[db.DatabaseName] = db;
        }

        /* Inner set is only touched under this server's evaluation gate (see class remarks), so a
           plain HashSet is safe; the outer dictionary is concurrent across servers. */
        var active = _activeDatabaseStateAlerts.GetOrAdd(key, _ => new HashSet<string>(StringComparer.Ordinal));

        foreach (var (dbName, db) in current)
        {
            active.Add(dbName);
            /* Keyed per database AND per STATE (#2166). It used to be per database, which was survivable when
               every deviation re-fired every cooldown: a transition suppressed by the previous state's
               cooldown re-announced on the next tick anyway. Now that a chosen state goes quiet
               indefinitely, that suppression would be permanent for the length of a cooldown window — so a
               database going OFFLINE and then SUSPECT inside one window could have its SUSPECT transition
               swallowed, which is precisely the integrity case this alert must never go quiet about. Each
               state now rate-limits itself and cannot borrow another's clock. */
            var cooldownKey = (Server: key, Database: dbName, State: db.StateDesc);
            /* #2166: for the states an operator usually CHOSE (a parked OFFLINE, a secondary flickering
               RESTORING), repetition is noise — alert on the transition and stay quiet until the state
               changes. Compared against the PERSISTED last-alerted state, so a service restart cannot
               re-announce every parked database. The integrity states skip this entirely: nobody parks a
               database in SUSPECT, so their repetition is the signal and the cooldown still governs.

               A host that does not persist the memory (Lite today) reports empty here, every deviation
               reads as new, and behavior is exactly as it was before this change. */
            var alreadyAnnounced =
                DatabaseStateTokens.RepeatsAreNoise(db.StateDesc)
                && string.Equals(db.LastAlertedState, db.StateDesc, StringComparison.OrdinalIgnoreCase);

            if (!suppressed && !alreadyAnnounced && CooldownElapsed(_lastDatabaseStateAlert, cooldownKey, now, alertCooldown))
            {
                var severity = DatabaseStateTokens.SeverityFor(db.StateDesc);
                var stateText = DatabaseStateTokens.Humanize(db.StateDesc);
                /* An empty expected state means this database was first observed in a critical state and
                   has no accepted baseline yet (see IAlertReadAdapter.GetDatabaseStatesAsync) — surface it
                   as a first-observation alert rather than "expected UNKNOWN". */
                bool pending = string.IsNullOrEmpty(db.ExpectedState);
                var expectedText = pending ? "(no baseline yet)" : DatabaseStateTokens.Humanize(db.ExpectedState);
                var muteCtx = new AlertMuteContext
                {
                    ServerName = serverName,
                    MetricName = DatabaseStateTokens.MetricName,
                    DatabaseName = dbName
                };
                bool isMuted = _isAlertMuted(muteCtx);
                _lastDatabaseStateAlert[cooldownKey] = now; /* stamped even when muted, like the others */

                var detailText = pending
                    ? $"  Database: {dbName}\n  Current: {stateText}\n  First observed in a critical state — no baseline established yet."
                    : $"  Database: {dbName}\n  Expected: {expectedText}\n  Current: {stateText}";
                var shortMessage = pending
                    ? $"{dbName} first observed {stateText} (no baseline yet)"
                    : $"{dbName} changed to {stateText} (expected {expectedText})";

                /* #2109: the same fields the prose carries, as discrete facts — this alert fired with
                   Context: null, which left the database name reachable only by parsing the title. */
                var stateContext = new AlertContext();
                stateContext.Details.Add(new AlertDetailItem
                {
                    Heading = dbName,
                    Fields = new()
                    {
                        ("Database", dbName),
                        ("Current State", stateText),
                        ("Expected State", expectedText)
                    }
                });

                await FireAsync(new AlertOutcome(
                    key, serverName, DatabaseStateTokens.MetricName,
                    $"{dbName}: {stateText}",
                    expectedText,
                    Context: stateContext, DetailText: detailText,
                    NumericCurrentValue: null, NumericThresholdValue: null,
                    Muted: isMuted, Severity: severity,
                    ShortMessage: shortMessage), ct);

                /* Stamped AFTER delivery so a failed fire is retried next cycle rather than silenced, and
                   written for every state rather than only the edge-triggered ones, so that reclassifying a
                   state later has correct history to work from.

                   NOT stamped when MUTED, which is the one place this memory and the cooldown beside it must
                   disagree. The cooldown is rate limiting and applies whether or not anyone was told; this
                   memory means "the operator has been told about this state", and under a mute they have not.
                   Stamping it anyway made a mute permanent: the four edge-triggered states gate all future
                   firing on this value, so muting a parked database, then REMOVING the mute, left
                   LastAlertedState equal to the current state forever and the alert never returned — the
                   operator's mute silently became irreversible for as long as the state held. Skipping the
                   stamp costs a repeat inside the mute (invisible by definition, and exactly the pre-#2166
                   cooldown behavior) and keeps unmuting meaningful. */
                if (!isMuted)
                {
                    await _stateStore.SaveDatabaseStateAlertedAsync(key, dbName, db.StateDesc);
                }
            }
        }

        /* Databases that were alerting but no longer deviate (state returned to expected, or the
           operator re-baselined / set the override to match) — announce a per-database recovery and
           drop their cooldown. Guarded by the master enable (we're past the early return), matching
           the other families' "only announce recovery while still enabled". */
        if (active.Count > 0)
        {
            var recovered = active.Where(d => !current.ContainsKey(d)).ToList();

            /* Every recovered database's clocks are dropped in ONE pass over the cooldown map, not one pass
               each (#2166). The key is per-state, so a single removal per database would leave its other
               states' stamps behind to rate-limit a future episode against a cooldown that started before the
               recovery — but the map holds every server's entries, so scanning it per database made the sweep
               O(recovered x everything tracked) where the old string key was an O(1) remove. Hoisting it back
               to one scan keeps the correctness and drops a factor. Matching on two tuple parts rather than a
               string prefix is what keeps a database named 'Foo|Bar' from being swept when 'Foo' recovers. */
            if (recovered.Count > 0)
            {
                /* ORDINAL, like `current` and `active` above and for the same reason: per-database keys here
                   must be case-SENSITIVE to match the stores' case-sensitive expected-state joins. A
                   case-insensitive set would let recovering `Foo` clear `foo`'s per-state stamps on a
                   case-sensitive collation where both exist — resetting the only quiet mechanism an integrity
                   state has, which is the same collision class the tuple key just removed for '|'. */
                var recoveredSet = new HashSet<string>(recovered, StringComparer.Ordinal);
                foreach (var stamped in _lastDatabaseStateAlert.Keys)
                {
                    if (string.Equals(stamped.Server, key, StringComparison.Ordinal)
                        && recoveredSet.Contains(stamped.Database))
                    {
                        _lastDatabaseStateAlert.TryRemove(stamped, out _);
                    }
                }
            }

            foreach (var dbName in recovered)
            {
                active.Remove(dbName);

                /* #2166 falling edge: forget the announced state as well as the in-memory cooldown, or the
                   edge only ever triggers once per database. Cleared even when suppressed — suppression
                   governs whether operators are TOLD about a transition, never whether the engine keeps
                   accurate state, and leaving a stale memory behind would swallow the next real episode. */
                await _stateStore.ClearDatabaseStateAlertedAsync(key, dbName);

                if (!suppressed)
                {
                    await NotifyResolutionAsync(new AlertResolution(
                        key, serverName, DatabaseStateTokens.MetricName,
                        "Database State Resolved",
                        $"{serverName}: {dbName} back to expected state"), ct);
                }
            }
        }
    }

    /// <summary>
    /// Forced Query Store plans the engine is currently failing to reproduce (#2157). The adapter returns
    /// only plans whose <c>force_failure_count</c> ROSE since the previous collection, so every row here is
    /// a live failure rather than accumulated history — see
    /// <see cref="IAlertReadAdapter.GetForcePlanFailuresAsync"/> for why a level would be wrong.
    ///
    /// <para>Why it deserves an alert at all: when a force fails, the query keeps running on whatever plan
    /// the optimizer picks. Nothing else in the product witnesses that — the operator's mitigation is
    /// silently not in effect, and the only trace is a counter climbing inside Query Store.</para>
    ///
    /// <para>Standing condition with per-plan resolution, mirroring the database-state family: while a plan
    /// keeps failing it re-fires on the cooldown, and when it stops appearing it announces a recovery.</para>
    /// </summary>
    private async Task CheckForcePlanFailuresAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.ForcePlanFailureEnabled)
        {
            return;
        }

        List<ForcePlanFailureInfo> failures;
        try
        {
            failures = await _readAdapter.GetForcePlanFailuresAsync(key, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* Log-and-skip, like every other collected read: never resolve an active plan on a failed
               fetch (that would fabricate a recovery), and never fire on absent evidence. */
            _logger?.LogError("Failed to check forced-plan failures for {Server}: {Message}", serverName, ex.Message);
            _readFailures?.RecordReadFailure(key, "forced-plan failures");
            return;
        }

        var excluded = _settings.ExcludedDatabases;

        /* Per-PLAN keys are ORDINAL for the same reason the database-state family's are: the stores compare
           database names case-sensitively, so a plan must not key differently here than it does there. The
           excluded-databases list stays case-insensitive, matching how every alert treats that user list. */
        var current = new Dictionary<string, ForcePlanFailureInfo>(StringComparer.Ordinal);
        foreach (var failure in failures)
        {
            if (string.IsNullOrWhiteSpace(failure.DatabaseName) || failure.PlanId <= 0)
            {
                continue;
            }

            if (excluded.Count > 0 && excluded.Any(e => string.Equals(e, failure.DatabaseName, StringComparison.OrdinalIgnoreCase)))
            {
                continue;
            }

            current[ForcePlanTokens.PlanKey(failure.DatabaseName, failure.QueryId, failure.PlanId)] = failure;
        }

        var active = _activeForcePlanAlerts.GetOrAdd(key, _ => new Dictionary<string, ForcePlanFailureInfo>(StringComparer.Ordinal));

        foreach (var (planKey, failure) in current)
        {
            active[planKey] = failure;
            var cooldownKey = key + "|" + planKey;
            if (!suppressed && CooldownElapsed(_lastForcePlanAlert, cooldownKey, now, alertCooldown))
            {
                var reasonText = ForcePlanTokens.HumanizeReason(failure.FailureReason);
                var forcingText = string.IsNullOrWhiteSpace(failure.ForcingType) ? "unknown" : failure.ForcingType.Trim();
                var muteCtx = new AlertMuteContext
                {
                    ServerName = serverName,
                    MetricName = ForcePlanTokens.MetricName,
                    DatabaseName = failure.DatabaseName
                };
                bool isMuted = _isAlertMuted(muteCtx);
                _lastForcePlanAlert[cooldownKey] = now; /* stamped even when muted, like the others */

                var detailText =
                    $"  Database: {failure.DatabaseName}\n" +
                    $"  Query / Plan: {failure.QueryId} / {failure.PlanId}\n" +
                    $"  Forcing: {forcingText}\n" +
                    $"  Reason: {reasonText}\n" +
                    $"  New failures since last collection: {failure.FailureDelta} (total {failure.TotalFailures})\n" +
                    "  The query is running on the optimizer's plan, not the forced one.";

                /* #2109 discipline: the same facts the prose carries, as discrete fields, so a consumer
                   never has to parse the title to learn which plan this is about. */
                var context = new AlertContext();
                context.Details.Add(new AlertDetailItem
                {
                    Heading = $"{failure.DatabaseName} query {failure.QueryId} plan {failure.PlanId}",
                    Fields = new()
                    {
                        ("Database", failure.DatabaseName),
                        ("Query ID", failure.QueryId.ToString(CultureInfo.InvariantCulture)),
                        ("Plan ID", failure.PlanId.ToString(CultureInfo.InvariantCulture)),
                        ("Forcing Type", forcingText),
                        ("Failure Reason", reasonText),
                        ("New Failures", failure.FailureDelta.ToString(CultureInfo.InvariantCulture)),
                        ("Total Failures", failure.TotalFailures.ToString(CultureInfo.InvariantCulture))
                    }
                });

                await FireAsync(new AlertOutcome(
                    key, serverName, ForcePlanTokens.MetricName,
                    $"{failure.DatabaseName}: plan {failure.PlanId} failing to force ({reasonText})",
                    reasonText,
                    Context: context, DetailText: detailText,
                    NumericCurrentValue: failure.FailureDelta, NumericThresholdValue: null,
                    Muted: isMuted, Severity: ForcePlanTokens.SeverityFor(failure),
                    ShortMessage: $"{failure.DatabaseName} plan {failure.PlanId} failed to force {failure.FailureDelta}x ({reasonText})"), ct);
            }
        }

        /* Plans that were alerting and no longer are: the counter stopped rising, because the force was
           removed, the plan became reproducible again, or the query stopped running. All three mean "no
           longer failing", which is what the recovery says — deliberately not claiming it was fixed. */
        if (active.Count > 0)
        {
            var recovered = active.Where(p => !current.ContainsKey(p.Key)).ToList();
            foreach (var (planKey, lastSeen) in recovered)
            {
                active.Remove(planKey);
                _lastForcePlanAlert.TryRemove(key + "|" + planKey, out _);
                if (!suppressed)
                {
                    /* Named from the identity we stored when it fired, never from the internal key: an
                       operator reads this in a toast, an email and a history row. */
                    await NotifyResolutionAsync(new AlertResolution(
                        key, serverName, ForcePlanTokens.MetricName,
                        "Forced Plan Failing Resolved",
                        $"{serverName}: {lastSeen.DatabaseName} query {lastSeen.QueryId} plan {lastSeen.PlanId} no longer failing to force"), ct);
                }
            }
        }
    }

    /* ---------------- helpers ---------------- */

    /// <summary>
    /// Lite's per-check cooldown test: no prior fire, or the cooldown has elapsed.
    ///
    /// <para>Generic in the KEY type only (#2166) so a family whose cooldown is scoped by more than one thing
    /// can key it structurally instead of concatenating a string. Every existing caller is string-keyed and
    /// infers unchanged; the database-state family keys by (server, database, state), where a string key
    /// would need a delimiter no <c>sysname</c> can contain — and SQL Server permits <c>|</c>.</para>
    /// </summary>
    private static bool CooldownElapsed<TKey>(
        ConcurrentDictionary<TKey, DateTime> lastFired, TKey key, DateTime now, TimeSpan cooldown)
        where TKey : notnull =>
        !lastFired.TryGetValue(key, out var last) || now - last >= cooldown;

    /// <summary>
    /// Delivers one fired alert AND logs it (#1681). Every family routes through here rather than calling
    /// the deliverer directly, so a tenth family cannot be added that silently skips the log — which is
    /// exactly how the nine below ended up firing silently while their RESOLUTIONS were logged, leaving an
    /// operator's log showing "… Cleared" with nothing before it.
    ///
    /// <para>Logged at Warning: a fired alert is by definition something wrong on a monitored server, and it
    /// has to stand out from the Information-level resolution it will eventually pair with. The wording comes
    /// from the shared <see cref="AlertFiringLog"/> so the engine, Darling's self-alerts and Lite's direct
    /// senders all read identically.</para>
    ///
    /// <para>The log happens BEFORE delivery on purpose. Delivery does I/O (SMTP, webhooks, a history-row
    /// write) and swallows its own failures, so logging afterwards would lose the record of an alert whose
    /// delivery hung or failed — and that alert is precisely the one an operator later goes looking for.</para>
    /// </summary>
    private async Task FireAsync(AlertOutcome outcome, CancellationToken ct)
    {
        _logger?.LogWarning(
            "{Line}",
            AlertFiringLog.Fired(
                outcome.ServerName,
                outcome.MetricName,
                outcome.Severity?.ToString() ?? "Warning",
                outcome.ShortMessage,
                outcome.Muted));

        await _deliverer.DeliverAsync(outcome, ct);
    }

    /// <summary>
    /// Reports a condition-recovered transition to the optional host callback. Callback failures
    /// are logged and swallowed — a broken toast/log hook must not abort the sweep.
    /// </summary>
    private async Task NotifyResolutionAsync(AlertResolution resolution, CancellationToken ct)
    {
        if (_resolutionCallback is null)
        {
            return;
        }

        try
        {
            await _resolutionCallback(resolution, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: this is the DELIVERY path, not a condition read.
               A failed delivery is a different fact with a different remedy, and #3013 deliberately left
               alerting on the alerting out of scope as its own decision. */
            _logger?.LogError("Alert resolution callback failed for {Server} / {Metric}: {Message}",
                resolution.ServerName, resolution.MetricName, ex.Message);
        }
    }
}
