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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
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
    private readonly Func<string, IReadOnlyList<AgentJobStepKey>, CancellationToken, Task<IReadOnlyDictionary<AgentJobStepKey, AgentJobStepNames>>>? _agentJobStepResolver;
    private readonly Func<AlertResolution, CancellationToken, Task>? _resolutionCallback;

    /// <summary>
    /// The #3497 degrade arm's value: a resolver that THREW gets this instead of null, because the two
    /// mean different things to the card — null says "no resolution was attempted" (render exactly as
    /// before #3497), empty says "a resolution ran and answered nothing" (render the unresolved form,
    /// which still states the Agent-job fact the parse alone establishes and carries the raw marker).
    /// </summary>
    private static readonly IReadOnlyDictionary<AgentJobStepKey, AgentJobStepNames> s_noAgentJobNames =
        new Dictionary<AgentJobStepKey, AgentJobStepNames>();
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

    /* The collection_time of the newest wait_stats row inside the window last actually fired on. Read
       adapters answer "how much poison wait accumulated over the last 10 minutes, and when was the
       newest row" (#3539 A4 — before that, "what's the newest poison-wait row within the last 10
       minutes"), which is independent of whether anything is NEW since the previous ask — at fleet
       load the collector's delivered cadence can lag the alert cooldown (PerformanceMonitor's own
       dogfooding on <monitor-host> caught byte-identical duplicate alerts ~5-7 minutes apart), so the
       cooldown elapsing is not proof a fresh observation exists. Poison wait is deliberately NOT
       level-triggered like CPU (which resamples live every sweep): the window sum changes only when
       the collector lands a row, and re-reading it between collections is the same computation
       surfacing twice, not two observations of a standing condition. Gate re-fire on BOTH the
       cooldown AND a newer collection_time than last fired.

       #3282 note, so nobody reads the contrast above as "CPU needs no freshness guard": CPU has one
       now too, and for a DIFFERENT reason. Here freshness stops a stale window being re-reported;
       there it makes the persistence gate count SAMPLES rather than sweeps. Poison wait is still not
       behind that gate, and since #3539 for a better reason than the units mismatch: the rolling
       window IS its persistence. Firing needs 600 s of wait to have accumulated inside ten minutes,
       and clearing needs the window's sum to fall back under that bar, which takes up to a full
       window as rows age out — so one quiet delta cannot clear it and one loud delta cannot fire it
       without the volume to back it. The gate would be a second persistence rule over a measure that
       already carries one (A5's gate rollout to other conditions is a separate #3539 item). */
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

    /* #3282: CPU's active flag is GONE from this family and replaced by the persistence gate's record,
       which carries the same "an incident is open" bit plus the streak that earned it. Two reasons it
       could not stay a bool here. It has to survive a restart — an in-memory flag meant the first
       post-restart sweep over a standing condition re-announced an incident the operator already had open
       — and the streak has to live in the same value as the flag, because a caller that can advance one
       without the other is a caller that can lose a signal.

       Seeded once per key from IAlertStateStore alongside the watermarks, then written through on change.
       The cache is authoritative WITHIN the process: EvaluateServerAsync serializes per server, so the
       read-modify-write below cannot interleave for one key.

       #3653 (A5): tempdb Space is the SECOND arm behind the gate, and its active flag left this family
       for exactly the reasons CPU's did — the persisted record IS the "incident open" bit, and a bool
       beside it would be a second source of truth for the same fact. One record cache per gated metric,
       each a row under its own metric name in the same (server, metric) persistence table, all driven
       through ObservePersistenceAsync — one mechanism, as AlertPersistenceGate's header asks; a third
       consumer is a third dictionary, a third seed line and a third pair of constants, nothing else.

       #3653 (A5, Q4): Blocking Wait Time is that third consumer. Its _activeBlockingWaitAlert flag left for
       the same reason the other two did — the persisted Firing bit is the one "incident open" fact — and its
       row sits under the alert's own metric name in the same table. */
    private readonly ConcurrentDictionary<string, AlertPersistenceRecord> _cpuPersistence = new();
    private readonly ConcurrentDictionary<string, AlertPersistenceRecord> _tempDbPersistence = new();
    private readonly ConcurrentDictionary<string, AlertPersistenceRecord> _blockingWaitPersistence = new();
    private readonly ConcurrentDictionary<string, bool> _activeBlockingAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activeDeadlockAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activePoisonWaitAlert = new();
    private readonly ConcurrentDictionary<string, bool> _activeLongRunningQueryAlert = new();
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

    /* #3636: per server, the OBSERVATION each breached file was last alerted on — keyed "database|file", the
       same key FileGrowthIncidents fingerprints on — so the RISE gate fires once per collection rather than
       once per cooldown. The file-growth condition had no per-file state before this (its DTO says so: "no
       per-file state to keep, survive a restart, or leak"), only the per-server cooldown clock and active
       flag above; this sits beside them at file grain because files enter and leave the breached set
       independently and a per-server stamp would let a threshold change mid-observation silence a file that
       had never been reported. Stamped on FIRE only — including a muted fire — never on a pass that merely
       saw the file (the #3579 seen ≠ alerted lesson: a collection landing inside the cooldown must still
       fire when it elapses). A file that leaves the breached set loses its entry, and the server's recovery
       clears the map, so a later episode starts with no memory exactly as the cooldown clock does.

       In-memory only, like the forced-plan memory it copies: a restart empties it, so the first pass after
       one may re-fire once for an observation still in the window — which is exactly what the cooldown
       clock, also emptied, already did before #3636, so the guard never makes a restart noisier than it was. */
    private readonly ConcurrentDictionary<string, Dictionary<string, DateTime>> _lastAlertedFileGrowthObservation = new();
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
       'forceplan:Sales:11:22 no longer failing to force' in every email and webhook (review catch).

       #3579: the value also remembers the OBSERVATION each plan was last alerted on (the newer sighting's
       collection_time), beside the identity rather than in a third dictionary, so the condition keeps one
       per-plan state and one cooldown clock. The two are different memories: LastSeen is overwritten on
       every pass the plan appears (it exists for the recovery message), LastAlertedObservedAtUtc only when
       the plan actually fires (it exists so the same observation cannot fire twice). Folding them into one
       stamp was the tempting shortcut and would have been wrong in the direction of silence: a new
       collection landing INSIDE the cooldown would have been recorded as seen and then never fired.

       In-memory only, like the rest of this family: a restart empties it, so the first pass after one may
       re-fire once for an observation still in the window — which is exactly what the cooldown clock,
       also emptied, already did before #3579, so the guard never makes a restart noisier than it was. */
    private readonly ConcurrentDictionary<string, Dictionary<string, ForcePlanActivePlan>> _activeForcePlanAlerts = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastForcePlanAlert = new();

    /// <summary>
    /// One forced plan the engine currently holds active (#2157), with the #3579 observation memory. Mutable
    /// by design: the pass updates it in place under the per-server evaluation gate, the same way the
    /// database-state family mutates its per-server HashSet.
    /// </summary>
    private sealed class ForcePlanActivePlan
    {
        public ForcePlanActivePlan(ForcePlanFailureInfo lastSeen)
        {
            LastSeen = lastSeen;
        }

        /// <summary>The row most recently returned for this plan — the identity the recovery message is
        /// named from. Refreshed every pass the plan appears.</summary>
        public ForcePlanFailureInfo LastSeen { get; set; }

        /// <summary>The <see cref="ForcePlanFailureInfo.ObservedAtUtc"/> of the row this plan last FIRED on
        /// (#3579), or null when it has not fired since it entered the active set or the adapter supplied no
        /// stamp. Set on fire only — including a muted fire, which stamps it exactly as it stamps the
        /// cooldown — never on a pass that merely saw the plan.</summary>
        public DateTime? LastAlertedObservedAtUtc { get; set; }
    }

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
    /// <param name="agentJobStepResolver">
    /// #3497's live msdb job-name lookup (serverKey, parsed job/step keys, ct) — host-supplied for the
    /// same reason as <paramref name="failedJobsFetcher"/>: job names live in the monitored server's
    /// msdb, not in any collected table. Hosts run <see cref="AgentJobStepQuery.BuildSql"/> on their own
    /// connections and degrade every failure (permission denied, transient, deleted job) to an empty
    /// map. Called only inside the Long-Running Query FIRE branch — one msdb round trip per delivered
    /// card, never per sweep — and only for sessions whose program_name parses as an Agent job step.
    /// Null disables the annotation entirely: the card renders byte-identically to before #3497.
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
        Func<string, IReadOnlyList<AgentJobStepKey>, CancellationToken, Task<IReadOnlyDictionary<AgentJobStepKey, AgentJobStepNames>>>? agentJobStepResolver = null,
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
        _agentJobStepResolver = agentJobStepResolver;
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

        var readClock = Stopwatch.StartNew();
        try
        {
            var blocking = await _stateStore.LoadEdgeTriggerWatermarkAsync(key, BlockingWatermarkMetric);
            readClock.Restart();
            if (blocking.HasValue)
            {
                _lastAlertedBlockingCount[key] = blocking.Value;
            }

            var deadlock = await _stateStore.LoadEdgeTriggerWatermarkAsync(key, DeadlockWatermarkMetric);
            readClock.Restart();
            if (deadlock.HasValue)
            {
                _lastAlertedDeadlockCount[key] = deadlock.Value;
            }

            var failedJob = await _stateStore.LoadFailedJobWatermarkAsync(key);
            if (failedJob.HasValue)
            {
                _lastAlertedFailedJobTime[key] = failedJob.Value;
            }

            /* #3282: the CPU persistence gate's record, seeded here for the same reason the watermarks
               are — so the first post-restart sweep knows an incident is already open (and does not
               re-announce it) and resumes the streak instead of restarting it. A store with no row hands
               back null and the subject starts at AlertPersistenceRecord.Initial, which is also what a
               host with no persistence gets; either way the gate arms from zero rather than misfiring. */
            readClock.Restart();
            var cpuPersistence = await _stateStore.LoadAlertPersistenceAsync(key, CpuPersistenceMetric);
            if (cpuPersistence.HasValue)
            {
                _cpuPersistence[key] = cpuPersistence.Value;

                /* An incident that was ALREADY OPEN gets its cooldown clock stamped as if it had just been
                   announced. The persisted Firing bit stops the gate producing a second rising edge, but
                   the cooldown dictionaries are in-memory by design (see their field comment), so an empty
                   clock plus a still-breaching condition would deliver the standing-condition REMINDER on
                   the first post-restart sweep — an identical High CPU message seconds after a restart,
                   which is a re-announcement whatever it is called internally. Stamping makes the
                   reminder wait a full cooldown, which is what an operator who already has the incident
                   open would expect. Nothing can be lost: if the condition is still breaching when the
                   cooldown elapses the reminder fires then, and if it cleared while the service was down
                   the first ClearSamples clear samples resolve it. */
                if (cpuPersistence.Value.State.Firing)
                {
                    _lastCpuAlert[key] = _utcNow();
                }
            }

            /* #3653 (A5): the tempdb Space gate's record — the same seed, the same cooldown stamp on an
               already-open incident, for the same reasons the CPU block above states; a second metric name
               is a second row in the same (server, metric) table, so nothing new is stored and no rung was
               needed. */
            readClock.Restart();
            var tempDbPersistence = await _stateStore.LoadAlertPersistenceAsync(key, TempDbSpacePersistenceMetric);
            if (tempDbPersistence.HasValue)
            {
                _tempDbPersistence[key] = tempDbPersistence.Value;
                if (tempDbPersistence.Value.State.Firing)
                {
                    _lastTempDbSpaceAlert[key] = _utcNow();
                }
            }

            /* #3653 (A5, Q4): the Blocking Wait Time gate's record — the third row under the third metric name,
               same seed, same cooldown stamp on an already-open incident, same reasons. */
            readClock.Restart();
            var blockingWaitPersistence = await _stateStore.LoadAlertPersistenceAsync(key, BlockingWaitPersistenceMetric);
            if (blockingWaitPersistence.HasValue)
            {
                _blockingWaitPersistence[key] = blockingWaitPersistence.Value;
                if (blockingWaitPersistence.Value.State.Firing)
                {
                    _lastBlockingWaitAlert[key] = _utcNow();
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Failed to seed edge-trigger watermarks for {ServerKey} after {ElapsedMs} ms: {Message}", key, readClock.ElapsedMilliseconds, ex.Message);
            _readFailures?.RecordReadFailure(key, "edge-trigger watermark seed", readClock.ElapsedMilliseconds);
        }

        _seededServerKeys[key] = true;
    }

    /* ---------------- CPU (Lite AlertEngine.cs:62-114) ---------------- */

    /// <summary>
    /// Consecutive breaching CPU SAMPLES required before High CPU is an incident (#3282). Three, which is
    /// about three minutes: the SQL Server figure comes off the SCHEDULER_MONITOR ring buffer, whose
    /// entries are about a minute apart (<c>CpuUtilizationCollector</c>'s own #2749 note measures "a real
    /// ~60s gap to the next"), and the <c>cpu_utilization</c> collector is scheduled every minute.
    ///
    /// <para><b>Derived from the measured excursion lengths, not picked.</b> Every High CPU / CPU Resolved
    /// pair delivered on a 42-server fleet in the 24 hours to 2026-09-11 was between 42 and 147 seconds
    /// long, median 87 s; the worst of them held at or above the 80% bar for exactly two consecutive
    /// one-minute samples before falling back to a 20% baseline. #3282 measured the same shape
    /// independently (55 s and 87 s on SQL Server, a 30-second pair, and one target at 99% back to 23%
    /// inside two minutes). Three samples is therefore the first bar above all of it, and the shortest
    /// excursion that could still be TRUE when a human opens the message — which is the only thing that
    /// makes a CPU page actionable rather than archaeology.</para>
    ///
    /// <para>Deliberately a constant rather than a setting, following <c>PostgresAlertEvaluator</c>'s
    /// stated position for its own thresholds: the product adds configuration when someone wants a
    /// different number, not speculatively, and a knob here means a new <c>config_alert_settings</c>
    /// column, a migration, a required <see cref="IAlertEngineSettings"/> member, Settings-window work and
    /// MCP plumbing. Half of that is worse than none: #3314 is open precisely because a
    /// delivery-governing number exists in the store and is unreachable from
    /// <c>get_alert_settings</c>/<c>update_alert_settings</c>, and adding a second such number is the one
    /// outcome to avoid. Public so it can be cited and pinned, and so raising it is a one-line diff.</para>
    /// </summary>
    public const int CpuBreachSamples = 3;

    /// <summary>
    /// Consecutive clearing CPU samples required to resolve an open High CPU incident (#3282). Two, and
    /// deliberately fewer than <see cref="CpuBreachSamples"/>: the two costs are not symmetric. A late
    /// resolve leaves a stale open incident, which is mildly annoying; an early resolve announces a
    /// recovery that the next sample contradicts, and a resolve/fire pair is exactly the noise this issue
    /// exists to remove. Two is the smallest value that survives one sample dipping under the bar during a
    /// real saturation event (#3282's "fired at 99% and resolved to 23% within two minutes" is that shape);
    /// three would hold incidents open a further minute and buy nothing.
    /// </summary>
    public const int CpuClearSamples = 2;

    /// <summary>
    /// The (server, metric) key the CPU gate's state is persisted under. The SAME string the mute context,
    /// the history row and the resolve all use, so an operator reading <c>config_alert_log</c> and an
    /// operator reading the persistence table are looking at one metric, not two spellings of it.
    /// </summary>
    public const string CpuPersistenceMetric = "High CPU";

    /// <summary>
    /// Consecutive breaching tempdb Space SAMPLES required before the reserved percentage is an incident
    /// (#3653 A5). Three collected samples: at the shipped <c>tempdb_stats</c> cadence of one minute that is
    /// about three minutes, and at the light tier's five-minute cadence about fifteen — the count is per
    /// COLLECTION, never per 30 s alert sweep, because <see cref="TempDbSpaceInfo.CollectionTimeUtc"/>
    /// keeps a re-read of the last row from counting twice (the same fresh-sample rule the CPU gate uses).
    ///
    /// <para><b>Derived from the measured flap, not picked.</b> Measured on one production store class —
    /// 43 servers over 14 days: one server fired tempdb Space at or above the 80% bar 22 times, and EVERY
    /// run was 1–4 breaching samples spanning at most 206 seconds before the percentage fell back under the
    /// bar. Twenty-two pages and twenty-two resolves in two weeks about a tempdb that was never in trouble
    /// for longer than three and a half minutes. K = 3 keeps 2 of those 22: the two runs that held for
    /// three or more consecutive collections, which are the only two an operator could still have seen
    /// as TRUE when the message arrived. K = 2 would have kept the two-sample runs too, which is most of
    /// the noise; K = 4 would have dropped both real ones and bought nothing the measurement asked for.</para>
    ///
    /// <para>The pre-gate arm fired on the FIRST breaching read and resolved on the first clear one, which
    /// is precisely the shape the 22 fire/resolve pairs describe. Same reasoning as
    /// <see cref="CpuBreachSamples"/> for why this is a constant and not a knob: the number is a
    /// measurement, not a preference, and a setting here is a store column, a rung, a settings member and
    /// MCP plumbing that #3314 already shows the cost of doing by halves.</para>
    /// </summary>
    public const int TempDbSpaceBreachSamples = 3;

    /// <summary>
    /// Consecutive clearing tempdb Space samples required to resolve an open incident (#3653 A5). ONE,
    /// which is the pre-gate behaviour kept on purpose: the arm has always announced the resolve on the
    /// first read under the bar, and this change touches only the rising edge.
    ///
    /// <para><b>Why not the CPU gate's two.</b> <see cref="CpuClearSamples"/> is 2 because #3282 MEASURED the
    /// shape it guards against — a 99% sample followed one minute later by 23% inside a real saturation
    /// event, i.e. one sample dipping under the bar mid-incident. No such measurement exists for tempdb:
    /// the #3653 read counted breach RUNS (1–4 consecutive breaching samples, 22 of them) and said
    /// nothing about dips inside a run, so there is no evidence that a resolve here is ever contradicted
    /// by the next sample. Reserved tempdb is also a slower gauge than scheduler CPU — it moves when
    /// allocation units are freed, not per ring-buffer tick — which is a reason to expect fewer mid-incident
    /// dips, not more. Picking 2 anyway would be the folklore constant the lineage rule exists to refuse.
    /// If the fleet shows resolve/re-fire pairs inside one tempdb event, this is the number that moves,
    /// with the measurement beside it.</para>
    /// </summary>
    public const int TempDbSpaceClearSamples = 1;

    /// <summary>
    /// The (server, metric) key the tempdb Space gate's state is persisted under — the same string the mute
    /// context, the history row and the resolve use, for the reason <see cref="CpuPersistenceMetric"/>
    /// gives. A second metric name is a second row in the existing persistence table (V118 keyed it
    /// <c>(server_id, metric_name)</c> for exactly this), so the rollout needed no rung.
    /// </summary>
    public const string TempDbSpacePersistenceMetric = "tempdb Space";

    /// <summary>
    /// Consecutive breaching blocking SNAPSHOTS required before a total blocked wait at or above
    /// <see cref="IAlertEngineSettings.BlockingWaitSecondsThreshold"/> is an incident — unless one snapshot
    /// alone clears <see cref="BlockingWaitSingleSnapshotMultiplier"/> times the bar, which fires at once
    /// (#3653 A5, ruling Q4). Three collected snapshots at the shipped one-minute <c>dmv_blocking_snapshot</c>
    /// cadence is about three minutes of blocking; the count is per COLLECTION (the snapshot's
    /// <c>collection_time</c> is the observation identity), never per 30 s alert sweep.
    ///
    /// <para><b>Why a disjunction and not the tempdb gate's plain K.</b> Measured on one production store
    /// class — 43 servers: 102 Blocking Wait Time episodes, and 97 of them were a SINGLE snapshot at the
    /// collector's ~72 s cadence, including the p99 event: 1,509 s of summed blocked wait across 145
    /// sessions, gone by the next collection. A pure consecutive gate keeps only whichever of the remaining
    /// five ran three collections or more and drops exactly the episodes an operator most needs to hear
    /// about — the short, severe pile-ups. The
    /// twin fleet's hour-quantised arcs DO run consecutive, so K = 3 is right for them and wrong for the
    /// severe singles; the single-snapshot arm at 3× the bar is what admits those. Erik ruled N = 3× and
    /// K = 3 on #3653; neither is a knob, for the reason <see cref="CpuBreachSamples"/> gives.</para>
    ///
    /// <para><b>Consecutive means adjacent collections, and the collector makes that harder here than for
    /// tempdb.</b> <c>dmv_blocking_snapshot</c> writes rows only while something is blocked, so a quiet
    /// cycle writes nothing and the store's "latest snapshot" simply stops advancing. Two breaching snapshots
    /// with a quiet cycle between them are two episodes, and counting them as consecutive would re-create
    /// the single-snapshot flap three episodes at a time. The arm therefore restarts a partial streak when a
    /// breaching snapshot lands more than <see cref="BlockingWaitEpisodeGapFactor"/> cadences after the last
    /// one it counted (<see cref="CurrentBlockingWaitResult.CadenceMinutes"/>); an OPEN incident is not
    /// restarted by a short gap — it holds, as the level-triggered arm always held, until a fresh snapshot
    /// under the bar or a stale one resolves it.</para>
    /// </summary>
    public const int BlockingWaitBreachSamples = 3;

    /// <summary>
    /// Consecutive clearing blocking observations required to resolve an open Blocking Wait Time incident
    /// (#3653 A5). ONE, the pre-gate behaviour kept on purpose and for the same reason
    /// <see cref="TempDbSpaceClearSamples"/> gives: the #3653 read counted breach episodes and said nothing
    /// about mid-incident dips, so a larger number here would be a constant without a measurement. A clear
    /// is EITHER a fresh snapshot under the bar OR a stale snapshot — the #1812 rule this arm has always
    /// had (see <see cref="CurrentBlockingWaitResult"/>): rows exist only while blocking exists, so a
    /// snapshot that has aged past three cycles is the collector saying the blocking ended, not an absence
    /// of evidence. A stale clear has no collection time of its own to be keyed on (the stale row's is the
    /// one already counted), so it is observed per sweep — which with a clear count of one is the same
    /// instant the arm resolved at before the gate.
    /// </summary>
    public const int BlockingWaitClearSamples = 1;

    /// <summary>
    /// The single-snapshot arm's multiplier (#3653 A5, ruling Q4): ONE fresh snapshot whose total blocked
    /// wait is at or above this many times <see cref="IAlertEngineSettings.BlockingWaitSecondsThreshold"/>
    /// fires immediately, without waiting for <see cref="BlockingWaitBreachSamples"/>. It still goes
    /// THROUGH the gate — as an observation whose bar is one sample — so the record opens the incident the
    /// same way the consecutive arm would, and the subsequent clear is honest: the operator gets one
    /// Cleared for one page, whichever arm produced it.
    /// </summary>
    public const int BlockingWaitSingleSnapshotMultiplier = 3;

    /// <summary>
    /// How many collector cadences may separate two breaching snapshots before the second is a NEW episode
    /// rather than the next consecutive collection (#3653 A5) — see <see cref="BlockingWaitBreachSamples"/>
    /// for why this arm needs the rule at all. 1.5: a skipped quiet cycle puts the next snapshot at least two
    /// cadences out (2.0 ×), and the measured collector ran at ~72 s against a 60 s schedule (1.2 ×), so the
    /// bar sits between the two shapes it has to separate. A collector that overruns past 1.5 × restarts a
    /// streak that was in fact consecutive, which UNDER-fires — the direction every gate in this class
    /// chooses when it must choose, and the single-snapshot arm still covers the severe case regardless.
    /// </summary>
    public const double BlockingWaitEpisodeGapFactor = 1.5;

    /// <summary>
    /// The (server, metric) key the Blocking Wait Time gate's state is persisted under — the alert's own
    /// metric name, the spelling its mute context, history row and resolve already use, for the reason
    /// <see cref="CpuPersistenceMetric"/> gives. A third metric name is a third row in the existing
    /// persistence table (V118 keyed it <c>(server_id, metric_name)</c>), so the rollout needed no rung.
    /// </summary>
    public const string BlockingWaitPersistenceMetric = "Blocking Wait Time";

    /// <summary>
    /// The <c>Fired By</c> detail value when the delivered snapshot alone is at or above
    /// <see cref="BlockingWaitSingleSnapshotMultiplier"/> × the bar (#3653 A5). Built from the constant so the
    /// token cannot say <c>3x</c> after the multiplier moves; pinned to the literal Erik ruled on in the tests.
    /// </summary>
    public static readonly string BlockingWaitFiredBySingleSnapshot = $"single_snapshot_{BlockingWaitSingleSnapshotMultiplier}x";

    /// <summary>
    /// The <c>Fired By</c> detail value when the delivered snapshot is over the bar but under the single-snapshot
    /// multiple, so the incident is open through the K-consecutive arm (#3653 A5). On the opening edge this is
    /// exact — the bar the gate was handed was <see cref="BlockingWaitBreachSamples"/>. On a cooldown reminder it
    /// describes the snapshot being delivered: an incident that opened at 3× and is now continuing at 1.5× is
    /// being HELD by the gate on a breaching sample, which is the consecutive arm's rule, so the token names
    /// the rule that admitted this delivery rather than a history the record does not keep.
    /// </summary>
    public static readonly string BlockingWaitFiredByConsecutive = $"consecutive_k{BlockingWaitBreachSamples}";

    /// <summary>
    /// Row budget for the #3495 fire-time active-session probe. The read orders by elapsed DESC and the
    /// maintenance shapes are long-running by nature — a backup or rebuild burning enough CPU to matter
    /// has been at it for minutes while an OLTP session's elapsed is milliseconds — so the sessions this
    /// probe exists to find sort to the FRONT and fifty rows is generous headroom over any plausible
    /// concurrent-maintenance count, not a coverage bet. Bounded at all because the probe runs inside
    /// the fire branch of a paging alert: the page must never wait on an unbounded read of a busy
    /// server's whole session list.
    /// </summary>
    public const int ActiveMaintenanceProbeMaxRows = 50;

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

        if (!_settings.CpuEnabled)
        {
            /* The disabled case is not an observation and never was: flipping the feature off is not the
               CPU recovering, so the gate is left exactly as it stands and no resolve is announced (the
               pre-#3282 code reached the same outcome through its own _settings.CpuEnabled guard on the
               resolve arm). Re-enabling resumes from the streak that was there. */
            return;
        }

        if (!alertCpuValue.HasValue)
        {
            /* NO-DATA FREEZES THE GATE — never a breach, never a clear. The pre-#3282 code fell through to
               its resolve arm here, so a CPU sample that simply went missing announced a recovery nobody
               measured, rendered "<server>: Total CPU back to %" because the value it interpolated was
               null. Freezing is the same call CustomAlertEvaluator makes on a null scalar, and the same
               reason every per-check catch in this class logs and skips: resolving on absent evidence
               fabricates a recovery exactly as firing on it fabricates an alert. */
            return;
        }

        bool breaching = alertCpuValue.Value >= _settings.CpuThresholdPercent;      /* :65-67 */

        /* The observation is the ring-buffer SAMPLE, identified by snapshot.CpuSampleTimeUtc: the sweep runs
           on s_alertSweepInterval (30 s) while that sample advances about once a minute, so without the
           instant the SAME sample would advance the streak on consecutive sweeps and CpuBreachSamples would
           be reached inside 90 seconds — shorter than every excursion #3282 measured, i.e. the defect intact
           behind a gate that looked like it fixed it. The fresh-sample rule, the null-instant fallback and
           the skip-a-write-when-nothing-moved rule live in ObservePersistenceAsync since #3653 (A5) put a
           second arm behind the same gate. The instant is the row's UTC twin where the store has one and the
           target's local stamp before V134 (#3744); the gate compares it for EQUALITY, so which clock stamped
           it is not a gate input — see ObservePersistenceAsync for why order was the wrong test. */
        var (outcome, incidentOpen) = await ObservePersistenceAsync(
            _cpuPersistence, CpuPersistenceMetric, key, snapshot.CpuSampleTimeUtc, breaching,
            CpuBreachSamples, CpuClearSamples);

        if (incidentOpen && breaching)
        {
            if (!suppressed && CooldownElapsed(_lastCpuAlert, key, now, alertCooldown)) /* :72 */
            {
                var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "High CPU" }; /* :74 */
                bool isMuted = _isAlertMuted(muteCtx);                              /* :75 */
                _lastCpuAlert[key] = now;                                           /* :76 — stamped even when muted */

                /* #3495: name the active backup on the card. Everything the annotation needs was knowable
                   at fire time from the same active-session surface the operator ended up reading by hand
                   (get_active_queries' underlying table — the query_snapshots read the LRQ alert already
                   rides), so this is one read of data the store already holds on the same sweep: no new
                   collector, no new cadence. Inside the fire branch deliberately — cooldown-bounded, so a
                   quiet sweep pays nothing — and taken for muted fires too, so the history row carries the
                   same detail the delivered card would have.

                   ANNOTATION, NEVER SUPPRESSION: the threshold compare, the persistence gate, the tiers and
                   the fire above are all decided before this read exists; it can only ever ADD a line.

                   Every filter is off and the exclusion list empty, on purpose: the LRQ alert's noise
                   opt-outs exist to keep maintenance OFF that alert (excludeBackups drops BACKUPTHREAD/
                   BACKUPIO waits), and this probe wants exactly the population those filters remove — the
                   backup filtered out of the long-running alert is precisely what this line names. An
                   excluded DATABASE stays visible too: the exclusion setting governs alert noise, and a
                   backup of an excluded database still burns this server's CPU. Threshold 0 = every session
                   in the latest fresh snapshot; the read's own 10-minute staleness floor still applies, so a
                   dead collector cannot dress an old backup up as a live one. The #3653 (Q5) opt-out knob is
                   passed as None for the same reason the five filters are off: an operator excludes their
                   permanent background requests from the LONG-RUNNING alert, and a backup run by an excluded
                   service login is still the thing burning this server's CPU.

                   Log-and-degrade: a failed annotation read costs the card its maintenance line and NOTHING
                   else — the alert already fired above this read in every sense that matters, and the empty
                   string leaves cpuDetailText byte-identical to the pre-#3495 card. Counted on #3013's
                   surface because it IS a store read the alert pass swallowed; Warning rather than Error
                   because the CONDITION was evaluated correctly — only the annotation went blind. */
                string maintenanceDetail = "";
                var maintenanceClock = Stopwatch.StartNew();
                try
                {
                    var activeSessions = await _readAdapter.GetLongRunningQueriesAsync(
                        key,
                        thresholdMinutes: 0,
                        maxResults: ActiveMaintenanceProbeMaxRows,
                        excludeSpServerDiagnostics: false,
                        excludeWaitFor: false,
                        excludeBackups: false,
                        excludeMiscWaits: false,
                        excludeCdc: false,
                        Array.Empty<string>(),
                        LongRunningQueryExclusions.None,
                        ct);
                    maintenanceDetail = AlertContextBuilders.BuildActiveMaintenanceDetail(activeSessions.Sessions);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning("Could not read active sessions for the High CPU card's maintenance annotation on {Server} after {ElapsedMs} ms — the alert fires without it: {Message}",
                        serverName, maintenanceClock.ElapsedMilliseconds, ex.Message);
                    _readFailures?.RecordReadFailure(key, "CPU maintenance annotation", maintenanceClock.ElapsedMilliseconds);
                }

                var cpuDetailText = $"  {cpuMetricLabel}: {alertCpuValue:F0}%\n  Threshold: {_settings.CpuThresholdPercent}%{maintenanceDetail}"; /* :89 + #3495 */

                /* :91-98 — ShortMessage = the toast body of :84 minus the server-name prefix. The numerics
                   are REQUIRED, not optional (#1830): the ported no-numerics form left the history stores
                   parsing "87% (Total CPU)", which fails on the parenthesized label, so every High CPU row
                   stored current_value 0 in Lite AND Darling while the toast/email/webhook text stayed
                   correct. alertCpuValue.HasValue is guaranteed here — the null arm above returns.

                   #3653 (A8e): CPU used to pass NO context, and so no tier — every row rendered amber by
                   name, a 100% fire indistinguishable from an 80% one while the card beside it was red. The
                   context now exists for exactly one member: the grade (GradeCpuFire — the CPU health
                   band's Critical bar). Details stay empty and DetailText stays the hand-built block above,
                   byte-identical, so every channel renders what it rendered before plus the tier. Set on
                   BOTH the context and the outcome for the reason the deadlock and Poison Wait sites give:
                   Lite's deliverer persists only the context, Darling's folds the outcome in. */
                var cpuGrade = GradeCpuFire(alertCpuValue.Value);
                await FireAsync(new AlertOutcome(
                    key, serverName, "High CPU",
                    $"{alertCpuValue:F0}% ({cpuMetricLabel})",
                    $"{_settings.CpuThresholdPercent}%",
                    Context: new AlertContext { SeverityOverride = cpuGrade }, DetailText: cpuDetailText,
                    NumericCurrentValue: alertCpuValue, NumericThresholdValue: _settings.CpuThresholdPercent,
                    Muted: isMuted, Severity: cpuGrade,
                    ShortMessage: $"{cpuMetricLabel} at {alertCpuValue:F0}% (threshold: {_settings.CpuThresholdPercent}%)"), ct);
            }
        }
        else if (outcome == PersistenceOutcome.Resolve)                              /* :101 */
        {
            /* The FALLING EDGE now comes from the gate rather than from a single sample dropping under the
               bar, which is the other half of #3282: the pre-gate code resolved on the first clear sample,
               so a 93%-for-two-minutes excursion produced a fire and a resolve 147 seconds apart and an
               operator got both before either meant anything. The gate fires this exactly once, after
               CpuClearSamples consecutive clears. Still gated on !suppressed, exactly as before. */
            if (!suppressed)
            {
                await NotifyResolutionAsync(new AlertResolution(
                    key, serverName, "High CPU",
                    "CPU Resolved",                                                 /* :110 */
                    $"{serverName}: {cpuMetricLabel} back to {alertCpuValue:F0}%"), ct); /* :111 */
            }
        }
    }

    /// <summary>
    /// Advances one gated metric's persistence record by one observation and returns the edge it produced
    /// plus whether an incident is open afterwards. The ONE consumer-side mechanism over
    /// <see cref="AlertPersistenceGate"/> for every built-in arm this engine gates — High CPU (#3282), tempdb
    /// Space (#3653 A5) and Blocking Wait Time (#3653 A5, Q4) today — so the rules below are stated once and
    /// cannot drift between arms. The bar is a PARAMETER per observation, not per arm: the blocking arm hands
    /// in a bar of one sample for a snapshot that clears its single-snapshot multiple and its K otherwise, which
    /// is how a disjunction rides one gate without the gate learning a second rule.
    ///
    /// <para><b>An observation counts only when it is a sample this subject has not counted yet.</b> The
    /// gate counts consecutive breaching SAMPLES, the sweep runs on <c>s_alertSweepInterval</c> (30 s), and
    /// every gated reading here comes off a collected row that advances at the collector's cadence (CPU's
    /// ring-buffer sample about once a minute; <c>tempdb_stats</c> once a minute at the shipped default,
    /// every five at the light tier). Without <paramref name="sampleUtc"/> the SAME row would advance the
    /// streak on consecutive sweeps and a K-sample bar would be reached inside K × 30 s of wall clock
    /// regardless of how many samples were actually taken — the measured flap intact behind a gate that
    /// looked like it fixed it. A sample instant EQUAL to
    /// <see cref="AlertPersistenceRecord.LastObservedSampleUtc"/> is therefore not an observation at all:
    /// the streak holds, nothing is written, and the caller still learns whether an incident is open so the
    /// standing-condition reminder can run on a sweep that brought no new sample.</para>
    ///
    /// <para><b>"Not counted yet" is decided by EQUALITY, not by order (#3744).</b> Until #3744 the rule was
    /// <c>sampleUtc &gt; LastObservedSampleUtc</c>, and the difference between the two predicates is exactly
    /// the set of observations whose instant reads OLDER than the one recorded. Every caller hands this method
    /// the identity of the NEWEST stored row (each read is an <c>ORDER BY … DESC LIMIT 1</c> or a
    /// <c>MAX(collection_time)</c> over a table its collector only ever appends to — at the store's clock, or
    /// above its watermark), so between two sweeps that identity can stay equal
    /// (the same row — not fresh) or change to a different row's; the only way it can change to an EARLIER
    /// value is a clock running backwards behind the stamp, and both real cases of that are new samples that
    /// MUST count. The first is the autumn fall-back: <c>cpu_utilization_stats.sample_time</c> is the monitored
    /// server's LOCAL wall clock, which repeats an hour once a year, so under <c>&gt;</c> every sample in the
    /// repeated hour read as stale and the CPU gate neither advanced a streak nor cleared an incident for that
    /// hour on every non-UTC server — at the one instant nobody is watching. The second is the frame change at
    /// the V134 upgrade (#3730): the CPU reads now prefer the row's UTC twin (<c>sample_time_utc</c>) as the
    /// identity, so a record persisted by a pre-#3744 build holds a LOCAL instant and the first post-upgrade
    /// sweep hands over a UTC one — east of UTC that reads older by the offset, and <c>&gt;</c> would have frozen
    /// the gate for one offset's worth of hours on the half of the world where it is morning, which is why
    /// #3730 left the identity on the local stamp and wrote the trap down. Equality is frame-blind: a
    /// different instant is a different observation whichever clock stamped it, so the frame switch costs at
    /// most ONE extra count of one sample per server, once (the same sample read as local 12:00 and then as
    /// UTC 17:00 is two identities — the accepted one-time re-anchor), and a fall-back costs nothing at all.
    /// No persisted frame tag was needed for that, which matters because
    /// <c>config.alert_persistence_state</c> cannot grow a column without a rung. The residual failure was
    /// weighed and is stated: should a NEWEST row ever be deleted from under the gate (nothing does that —
    /// retention removes the oldest), the read would fall back to an older row, equality would count it once,
    /// and the streak would run ONE sample ahead, where <c>&gt;</c> would have held it until a newer row landed.
    /// One extra count in a path that does not exist against an hour of silence in one that runs every year is
    /// not a close call. Pinned in both directions by <c>AlertEngineTests</c>' fall-back and upgrade fixtures
    /// (fresh) and its repeated-instant fixture (not fresh).</para>
    ///
    /// <para><b>A null sample instant counts every sweep instead</b> (see
    /// <see cref="AlertServerSnapshot.CpuSampleTimeUtc"/> and <see cref="TempDbSpaceInfo.CollectionTimeUtc"/>
    /// for which hosts supply none): the persistence is then weaker, but the alert still fires, and silence
    /// is the one failure a monitoring product cannot distinguish from health.</para>
    ///
    /// <para>Where two samples land between sweeps the older one is skipped, so a sustained excursion can
    /// need one extra sample to reach the bar. That undercounts and therefore UNDER-fires, which is the
    /// correct direction for an alert that pages — the same reasoning <c>PostgresAlertEvaluator</c>'s
    /// poison-wait window states for its own partial coverage.</para>
    ///
    /// <para><b>The write is skipped when nothing moved.</b> Value equality on the record is what makes
    /// that safe: on the vast majority of sweeps nothing about the subject changes, and a store write per
    /// server per gated metric per sweep would be 84 pointless upserts a minute on the measured fleet.</para>
    ///
    /// <para>NOT the gate for absent data. A caller with no reading must return BEFORE calling this — a
    /// null is neither a breach nor a clear, and both arms say so at their own null check.</para>
    /// </summary>
    /// <param name="records">The per-server record cache for this metric, seeded from the store at startup.</param>
    /// <param name="metricName">The <c>(server, metric)</c> key the record is persisted under.</param>
    /// <param name="sampleUtc">
    /// The observation's sample instant — the NEWEST stored row's identity, compared for equality only — or
    /// null for a host that has none. Naive UTC for every arm today, except the CPU arm on a row written before
    /// V134 (the target's local clock; see <see cref="AlertServerSnapshot.CpuSampleTimeUtc"/>).
    /// </param>
    /// <returns>The edge this observation produced, and whether the subject is firing after it.</returns>
    private async Task<(PersistenceOutcome Outcome, bool IncidentOpen)> ObservePersistenceAsync(
        ConcurrentDictionary<string, AlertPersistenceRecord> records, string metricName, string key,
        DateTime? sampleUtc, bool breaching, int breachSamples, int clearSamples)
    {
        var priorRecord = records.TryGetValue(key, out var cached) ? cached : AlertPersistenceRecord.Initial;

        /* != and not > — a different instant is a different sample whichever clock stamped it; the summary
           above says why order was the wrong test (#3744). */
        bool freshSample = !sampleUtc.HasValue
            || !priorRecord.LastObservedSampleUtc.HasValue
            || sampleUtc.Value != priorRecord.LastObservedSampleUtc.Value;

        if (!freshSample)
        {
            return (PersistenceOutcome.None, priorRecord.State.Firing);
        }

        var evaluation = AlertPersistenceGate.Evaluate(priorRecord.State, breaching, breachSamples, clearSamples);
        var nextRecord = new AlertPersistenceRecord(evaluation.State, sampleUtc ?? priorRecord.LastObservedSampleUtc);

        if (!nextRecord.Equals(priorRecord))
        {
            records[key] = nextRecord;
            await SavePersistenceAsync(key, metricName, nextRecord);
        }

        return (evaluation.Outcome, nextRecord.State.Firing);
    }

    /// <summary>
    /// Persists one server's gate record for one metric (#3282; generalised over the metric for #3653 A5),
    /// absorbing store failures the way every other state write in this class does: the gate has already
    /// decided this observation from the in-memory record, so a dropped write costs the streak across a
    /// restart and never an alert.
    /// </summary>
    private async Task SavePersistenceAsync(string key, string metricName, AlertPersistenceRecord record)
    {
        try
        {
            await _stateStore.SaveAlertPersistenceAsync(key, metricName, record);
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's counter, and Warning rather than Error — the same call
               SaveOccurrencesAsync makes for the same reason. That counter is about READS the alert pass
               performs and swallows, measured against a denominator of alert passes; a write in its
               numerator would read as the pass going blind on a condition when in fact the condition was
               evaluated correctly and only the memory of it was lost. The seed LOAD above is a read and is
               counted there. One catch block for every gated metric, so the census exemption is one entry. */
            _logger?.LogWarning("Could not persist the {Metric} persistence gate for {ServerKey}: {Message}", metricName, key, ex.Message);
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
            var readClock = Stopwatch.StartNew();
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
                _logger?.LogError("Failed to check blocking for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message);
                _readFailures?.RecordReadFailure(key, "blocking", readClock.ElapsedMilliseconds);
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
    /// blocking snapshot, mirroring the High CPU mechanism above (incident open → cooldown re-fire while
    /// still above → resolve on the way down) rather than the count gate's rolling-window edge trigger.
    /// The two answer different questions — a count cannot distinguish one session blocked for an hour
    /// from one blocked for a second — so this reports under its OWN metric name, keeping mute rules,
    /// history rows and cooldown state from tangling with "Blocking Detected".
    /// <para>
    /// Both gates sit under <see cref="IAlertEngineSettings.BlockingEnabled"/>: turning blocking alerts
    /// off must silence both, exactly as a user reading one toggle would expect. With the threshold at
    /// its shipped 0 the adapter read never happens at all.
    /// </para>
    /// <para>
    /// Since #3653 (A5, ruling Q4) the FIRE decision sits behind the shared <see cref="AlertPersistenceGate"/>
    /// as a DISJUNCTION: one fresh snapshot at or above <see cref="BlockingWaitSingleSnapshotMultiplier"/> × the
    /// bar fires at once; anything between the bar and that multiple must hold for
    /// <see cref="BlockingWaitBreachSamples"/> consecutive collections. The pre-gate arm fired on the first
    /// snapshot over the bar and resolved on the first under it, which on the measured store class was 102
    /// episodes, 97 of them one snapshot long — a page and a Cleared per collector cycle. The fire names the
    /// arm that admitted it (<c>Fired By</c>) so a reader of the card, the history row or
    /// <c>get_alert_history</c> can tell a severe pile-up from a sustained one.
    /// </para>
    /// </summary>
    private async Task CheckBlockingWaitAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed,
        List<BlockedProcessAlertRow>? blockingRows, CancellationToken ct)
    {
        int thresholdSeconds = _settings.BlockingWaitSecondsThreshold;
        bool enabled = _settings.BlockingEnabled && thresholdSeconds > 0;

        if (!enabled)
        {
            /* Not an observation (the CPU arm's rule): switching blocking alerts off, or zeroing this
               threshold, is not the blocking clearing. The gate is left exactly as it stands — an open
               incident stays open and un-announced, a partial streak keeps its count — and re-enabling
               resumes from there. The pre-gate arm reached the same silence through its `!suppressed &&
               enabled` guard on the resolve, but forgot the incident while doing so; the persisted record
               does not. */
            return;
        }

        CurrentBlockingWaitResult? current;
        var readClock = Stopwatch.StartNew();
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
            _logger?.LogError("Failed to check blocking wait time for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message);
            _readFailures?.RecordReadFailure(key, "blocking wait time", readClock.ElapsedMilliseconds);
            return;
        }

        if (current is null)
        {
            /* NO-DATA FREEZES THE GATE — never a breach, never a clear (#3653 A5, the tempdb arm's rule). A
               store with no blocking snapshot for this server at all is the shipped state of a server that
               has never blocked, and for one with an open incident it means the rows went away (a retention
               pass, a reset store) — an absence of evidence either way. The pre-gate arm read null as "not
               above" and would have announced a Cleared off it; resolving on absent evidence fabricates a
               recovery exactly as firing on it would fabricate an alert. Distinct from a STALE snapshot,
               which is evidence and is handled below. */
            return;
        }

        long thresholdMs = (long)thresholdSeconds * 1000L;

        /* THE TWO KINDS OF CLEAR, and why a stale snapshot is one of them here when a missing tempdb row is
           not (#1812's rule, kept through the gate). dmv_blocking_snapshot writes rows only while something
           is blocked, so "the latest snapshot" after the blocking ends never advances again — it just ages.
           A snapshot older than three cycles is therefore the collector's only way of saying the blocking is
           over, and holding a level-triggered incident open on it would re-page every cooldown about
           blocking that ended an hour ago (the failure CurrentBlockingWaitResult names). A stale clear has no
           collection instant of its own — the stale row's is the breach already counted — so it is observed
           with a null instant, i.e. per sweep, and with BlockingWaitClearSamples = 1 the first stale sweep
           resolves: the same instant the arm resolved at before the gate. The resolve message keeps the
           threshold rather than quoting the stale total, which is not a current number. Observed only for a
           subject the gate has actually counted (a partial streak or an open incident): a server whose only
           snapshot is a week-old one it never breached on has nothing to clear, and observing it anyway
           would write one "0 breaches, 1 clear" row per such server for no reader. */
        if (!current.SnapshotIsFresh)
        {
            if (!_blockingWaitPersistence.TryGetValue(key, out var stalePrior)
                || (!stalePrior.State.Firing && stalePrior.State.ConsecutiveBreaches == 0))
            {
                return;
            }

            var (staleOutcome, _) = await ObservePersistenceAsync(
                _blockingWaitPersistence, BlockingWaitPersistenceMetric, key, sampleUtc: null, breaching: false,
                BlockingWaitBreachSamples, BlockingWaitClearSamples);

            if (staleOutcome == PersistenceOutcome.Resolve && !suppressed)
            {
                await NotifyResolutionAsync(new AlertResolution(
                    key, serverName, "Blocking Wait Time",
                    "Blocking Wait Cleared",
                    $"{serverName}: Total blocked wait back under {thresholdSeconds}s"), ct);
            }

            return;
        }

        bool breaching = current.TotalWaitMs >= thresholdMs;
        bool singleSnapshot = breaching && current.TotalWaitMs >= thresholdMs * BlockingWaitSingleSnapshotMultiplier;

        /* CONSECUTIVE MEANS ADJACENT. A breaching snapshot that lands more than BlockingWaitEpisodeGapFactor
           cadences after the last one this gate counted had at least one quiet collection between them — a
           cycle in which the collector found nothing to write — so it starts a NEW episode and the partial
           streak restarts at this sample. Only a streak that has not fired is restarted: an OPEN incident
           holds across a short gap exactly as the level-triggered arm always held it (its clears are a fresh
           sub-bar snapshot or a stale one, above), so a sustained event with one quiet minute inside it is
           still one incident. Reset in place rather than through the gate because the gate has no "start
           over" primitive and adding one for a rule only this collector's row shape needs would be the
           general mechanism bending to one arm. The record's LastObservedSampleUtc is kept so the freshness
           rule below still sees THIS snapshot as new. A host that supplies no cadence never restarts, and
           the streak degrades to "breaching snapshots inside the freshness window" — stated, not silent. */
        if (breaching
            && current.CadenceMinutes is int cadenceMinutes && cadenceMinutes > 0
            && _blockingWaitPersistence.TryGetValue(key, out var priorRecord)
            && !priorRecord.State.Firing
            && priorRecord.State.ConsecutiveBreaches > 0
            && priorRecord.LastObservedSampleUtc is DateTime lastCounted
            && current.SnapshotTime - lastCounted > TimeSpan.FromMinutes(cadenceMinutes * BlockingWaitEpisodeGapFactor))
        {
            _blockingWaitPersistence[key] = new AlertPersistenceRecord(PersistenceState.Initial, priorRecord.LastObservedSampleUtc);
        }

        /* THE DISJUNCTION, through one gate: the bar handed to the gate is ONE sample when this snapshot alone
           is at or above the single-snapshot multiple and BlockingWaitBreachSamples otherwise. A one-sample bar
           fires on the first fresh breaching observation and opens the incident on the same record the
           consecutive arm would have opened it on, so the clear that follows is the same clear — the
           "still RECORDS the observation" half of the ruling. The gate caps the running breach count at the
           bar it was handed, so a 3× fire leaves ConsecutiveBreaches at 1 in the persisted row; the count is
           the gate's working memory and nothing renders it, and the Firing bit is what a restart reads. */
        var (outcome, incidentOpen) = await ObservePersistenceAsync(
            _blockingWaitPersistence, BlockingWaitPersistenceMetric, key, current.SnapshotTime, breaching,
            singleSnapshot ? 1 : BlockingWaitBreachSamples, BlockingWaitClearSamples);

        if (incidentOpen && breaching)
        {
            if (!suppressed && CooldownElapsed(_lastBlockingWaitAlert, key, now, alertCooldown))
            {
                var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Blocking Wait Time" };
                bool isMuted = _isAlertMuted(muteCtx);
                _lastBlockingWaitAlert[key] = now;                                  /* stamped even when muted */

                /* Same incident content the count gate ships, built from the rows this sweep already
                   fetched — an operator who gets this alert gets today's Blocking Detected detail. The gate
                   item is PREPENDED so the first thing a reader of the card or of get_alert_history's
                   context_json sees is which arm admitted this delivery and the numbers it was judged on;
                   the blocked-process rows may be absent (a DMV-only episode has no report), and the item
                   exists regardless, so the context is never null on a fire from this arm. */
                var blockingContext = AlertContextBuilders.BuildBlockingContext(serverName, blockingRows, _settings.ExcludedDatabases)
                    ?? new AlertContext();
                blockingContext.Details.Insert(0, AlertContextBuilders.BuildBlockingWaitGateItem(
                    current, thresholdSeconds, singleSnapshot ? BlockingWaitFiredBySingleSnapshot : BlockingWaitFiredByConsecutive));
                var detailText = AlertContextBuilders.ContextToDetailText(blockingContext);

                /* REAL numerics (#1830): the display text is prose ("745s across 3 blocked session(s)"),
                   which no history-store parser could turn back into a number — the value has to travel
                   as a number or every history row lands at 0, which is the defect #1830 just fixed. */
                double totalWaitSeconds = current.TotalWaitSeconds;
                await FireAsync(new AlertOutcome(
                    key, serverName, "Blocking Wait Time",
                    $"{totalWaitSeconds:F0}s across {current.BlockedSessionCount} blocked session(s)",
                    $"{thresholdSeconds}s",
                    blockingContext, detailText,
                    NumericCurrentValue: totalWaitSeconds, NumericThresholdValue: thresholdSeconds,
                    Muted: isMuted, Severity: blockingContext.SeverityOverride,
                    ShortMessage: $"{totalWaitSeconds:F0}s total blocked wait across {current.BlockedSessionCount} session(s) (threshold: {thresholdSeconds}s)"), ct);
            }
        }
        else if (outcome == PersistenceOutcome.Resolve)
        {
            /* The falling edge, from the gate: produced exactly once, on the first fresh snapshot under the
               bar while an incident is open (BlockingWaitClearSamples). Still gated on !suppressed, exactly
               as before; `enabled` is true here by construction (the disabled arm returned above). */
            if (!suppressed)
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
            var readClock = Stopwatch.StartNew();
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
                _logger?.LogError("Failed to check deadlocks for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message);
                _readFailures?.RecordReadFailure(key, "deadlocks", readClock.ElapsedMilliseconds);
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

            /* #3653 (A8e): GRADE the fire the gate already decided on. Until now this alert carried no tier,
               so every row rendered by NAME — red for one deadlock and red for a hundred — while the fleet
               card beside it banded the same hour Healthy / Warning / Critical on the measured rate tiers
               (#3368: 5/hr is the 99.94th percentile of 14,448 measured server-hours; 20/hr sits inside the
               empty interval between the routine mode's ceiling of 15 and the smallest storm's 90). One
               instrument, both surfaces: the same classifier, the same window the count was taken over
               (RollingCountWindowHours — one hour, so count and rate are one number here and the band's
               one-hour minimum window is met by construction), the same store-tunable pair. The COUNT knob
               still decides WHETHER it fires; the rate tiers decide only how it is coloured, so an operator
               with a count threshold of 1 keeps every fire and a run of 25 in an hour finally reads as the
               storm it is. Warning is the floor rather than the band's Healthy: a fire the operator asked
               for is never rendered as nothing. The tier rides on the context (the #3635 row projection the
               grids and get_alert_history read) AND on the outcome (Lite's deliverer does not fold the
               outcome's severity into the context; Darling's does — setting both makes the two SKUs agree
               by construction, the Poison Wait precedent). */
            deadlockContext ??= new AlertContext();
            deadlockContext.SeverityOverride = GradeDeadlockFire(effectiveDeadlockCount, _settings.DeadlockRateThresholds);
            var detailText = AlertContextBuilders.ContextToDetailText(deadlockContext);

            /* :252-260 — ShortMessage = the toast body of :244. Numerics carried explicitly (#1830):
               the count text happens to parse today, but the stored value must not depend on parse luck. */
            await FireAsync(new AlertOutcome(
                key, serverName, "Deadlocks Detected",
                effectiveDeadlockCount.ToString(),
                _settings.DeadlockCountThreshold.ToString(),
                deadlockContext, detailText,
                NumericCurrentValue: effectiveDeadlockCount, NumericThresholdValue: _settings.DeadlockCountThreshold,
                Muted: isMuted, Severity: deadlockContext.SeverityOverride,
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

    /* ---------------- poison waits (Lite AlertEngine.cs:273-339; reshaped by #3539 A4) ---------------- */

    /// <summary>
    /// The SQL Server Poison Wait alert, since #3539 A4 the accumulation shape its PostgreSQL twin
    /// (<c>DarlingWorker.EvaluatePgPoisonWaitAsync</c>) has fired on since #2711: the read sums every
    /// collector delta inside <see cref="PoisonWaitEvaluator.WindowMinutes"/> per poison wait type, and
    /// <see cref="PoisonWaitEvaluator.EvaluateSqlServer(IReadOnlyList{PoisonWaitAccumulation})"/> grades the
    /// sum against the shared bars — Warning at one task continuously starved across the window, Critical at
    /// ten. The retired shape judged ONE collector row's avg-ms-per-wait against
    /// <see cref="IAlertEngineSettings.PoisonWaitThresholdMs"/>, presence-flat CRITICAL: a single 600 ms wait
    /// paged and a storm of thousands of 8 ms THREADPOOL waits slept. The measured basis is on the evaluator.
    ///
    /// <para><b>One alert per server, as before</b>, not one per wait type like the PostgreSQL host: the
    /// delivery shape (metric-level cooldown fallback, detail items with no incidents, mute keyed on the worst
    /// wait type — Lite's documented limitation) is what every downstream reader of this alert already
    /// understands, and the graded severity is the worst wait type's. Changing the per-server contract is a
    /// separate decision from changing what the number means.</para>
    ///
    /// <para><b>The clear arm requires an observation.</b> A standing alert clears when the window holds at
    /// least one poison-type collector row AND no type is over the bar — which, because the sum is rolling,
    /// happens only once enough of the wait has aged out, up to a full window late (the PostgreSQL twin
    /// accepts the same lateness). An EMPTY read — no wait_stats rows for any poison type in ten minutes — is
    /// the collector not delivering, not the server going quiet: THREADPOOL has lifetime wait on any server
    /// that has been up long enough to fire this alert, so its row is written every cycle. An absent
    /// measurement holds the flag where it is (the #3282 rule for a CPU reading that stops arriving); the
    /// retired shape announced "Cleared" on that same silence.</para>
    /// </summary>
    private async Task CheckPoisonWaitsAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.PoisonWaitEnabled)                                           /* :274 */
        {
            return;
        }

        var readClock = Stopwatch.StartNew();
        try
        {
            var accumulated = await _readAdapter.GetPoisonWaitAccumulationAsync(key, PoisonWaitEvaluator.WindowMinutes, ct);
            readClock.Restart();

            var findings = PoisonWaitEvaluator.EvaluateSqlServer(accumulated);

            if (findings.Count > 0)
            {
                _activePoisonWaitAlert[key] = true;                                 /* :282 */

                /* The read adapter's window can hand back the SAME newest row across multiple sweeps when
                   the collector lags the cooldown — see the field's own doc comment. Only a collection_time
                   newer than the one last fired on counts as a fresh observation; a cooldown-elapsed re-ask
                   against an unrefreshed window must wait for the NEXT collector row rather than re-fire on
                   a sum it already reported. Taken over the FIRING types only, as before: a quiet type's
                   newer row is not a new observation of the type that is over the bar. */
                var newestCollectionTime = findings.Max(f => f.NewestCollectionTime);
                bool hasFreshCollection = !_lastPoisonWaitCollectionTime.TryGetValue(key, out var lastCollectionTime)
                    || newestCollectionTime > lastCollectionTime;

                if (!suppressed && hasFreshCollection && CooldownElapsed(_lastPoisonWaitAlert, key, now, alertCooldown)) /* :283 */
                {
                    var worst = findings[0];                                        /* :285 — worst-first from the evaluator */
                    var allWaitNames = string.Join(", ", findings.ConvertAll(f => f.CurrentValueClause)); /* :286 */

                    /* :288-293 — mute keys on the worst (highest severity, then most accumulated wait)
                       firing wait type; same documented limitation as Lite. */
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Poison Wait", WaitType = worst.WaitType };
                    bool isMuted = _isAlertMuted(muteCtx);
                    _lastPoisonWaitAlert[key] = now;                                /* :294 */
                    _lastPoisonWaitCollectionTime[key] = newestCollectionTime;

                    /* One detail item per firing type, no incidents — the shape IncidentDeliveryFilter
                       documents for this metric (metric-level cooldown fallback). The severity rides on the
                       context's override as well as the outcome, because the channel builders read the
                       context (the #1136 low-disk grading path) while Darling's deliverer folds the outcome's
                       in only when the context has none (#2090); setting both makes the two agree by
                       construction on both SKUs. */
                    /* #4223: the worst-graded wait type as structured data, the same value the mute
                       context above already keys on — an alert-notebook reader can branch on it without
                       parsing the detail heading or the prose. */
                    var poisonContext = new AlertContext { SeverityOverride = worst.Severity, WaitType = worst.WaitType };
                    foreach (var finding in findings)
                    {
                        poisonContext.Details.Add(new AlertDetailItem
                        {
                            Heading = finding.WaitType,
                            Fields = new()
                            {
                                ("Accumulated wait", string.Create(CultureInfo.InvariantCulture,
                                    $"{finding.AccumulatedSeconds:N0} s over the last {PoisonWaitEvaluator.WindowMinutes} min")),
                                ("Avg tasks stuck", string.Create(CultureInfo.InvariantCulture, $"{finding.AvgWaiters:N2}")),
                                ("Waits completed", string.Create(CultureInfo.InvariantCulture, $"{finding.AccumulatedWaits:N0}")),
                                ("Severity", finding.Severity == AlertSeverityLevel.Critical ? "CRITICAL" : "WARNING"),
                                ("Remedy", PoisonWaitEvaluator.SqlServerRemedyFor(finding.WaitType)),
                            }
                        });
                    }
                    var detailText = AlertContextBuilders.ContextToDetailText(poisonContext);   /* :308 */

                    /* :310-320. ShortMessage = the toast body of :302. Numerics are the worst type's
                       accumulated milliseconds against the bar it crossed, also in milliseconds — the unit
                       the history formatter already renders this metric in, and the PostgreSQL twin's
                       exact numeric pair. */
                    await FireAsync(new AlertOutcome(
                        key, serverName, "Poison Wait",
                        allWaitNames,
                        worst.ThresholdValue,
                        poisonContext, detailText,
                        NumericCurrentValue: worst.AccumulatedWaitMs,
                        NumericThresholdValue: worst.NumericThresholdValue,
                        Muted: isMuted, Severity: worst.Severity,
                        ShortMessage: worst.ShortMessage), ct);
                    readClock.Restart();
                }
            }
            else if (accumulated.Count > 0                                          /* observed AND quiet — see the doc comment */
                && _activePoisonWaitAlert.TryGetValue(key, out var wasPoisonWait) && wasPoisonWait) /* :323 */
            {
                _activePoisonWaitAlert[key] = false;                                /* :325 */
                if (!suppressed)                                                    /* :326 */
                {
                    await NotifyResolutionAsync(new AlertResolution(
                        key, serverName, "Poison Wait",
                        "Poison Waits Cleared",                                     /* :329 */
                        string.Create(CultureInfo.InvariantCulture,
                            $"{serverName}: Poison wait accumulated over the last {PoisonWaitEvaluator.WindowMinutes} minutes back below threshold")), ct); /* :330 */
                    readClock.Restart();
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Failed to check poison waits for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message); /* :337 */
            _readFailures?.RecordReadFailure(key, "poison waits", readClock.ElapsedMilliseconds);
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

        var readClock = Stopwatch.StartNew();
        try
        {
            /* #3653 (A5, Q5): the opt-out knob rides INTO the read, ahead of the row cap — see
               LongRunningQueryExclusions for why a post-read filter would blind the alert. Normalised here
               from the two raw settings lists on every sweep (trim, blanks dropped, case-insensitive dedupe)
               so a Settings-window string and an MCP array mean the same thing to both stores. The lists
               arrive SEEDED from each host (the job-step program prefix and the two NT AUTHORITY logins the
               production read found) unless an operator cleared them; the engine does not re-seed an empty
               list, because present-and-empty is the operator's decision. */
            var exclusions = LongRunningQueryExclusions.From(
                _settings.LongRunningQueryExcludedProgramNamePrefixes, _settings.LongRunningQueryExcludedLogins);
            var read = await _readAdapter.GetLongRunningQueriesAsync(              /* :346 */
                key,
                _settings.LongRunningQueryThresholdMinutes,
                _settings.LongRunningQueryMaxResults,
                _settings.LongRunningQueryExcludeSpServerDiagnostics,
                _settings.LongRunningQueryExcludeWaitFor,
                _settings.LongRunningQueryExcludeBackups,
                _settings.LongRunningQueryExcludeMiscWaits,
                _settings.LongRunningQueryExcludeCdc,
                _settings.ExcludedDatabases,
                exclusions,
                ct);
            var longRunning = read.Sessions;
            readClock.Restart();

            /* #2362: observe every sweep, OUTSIDE the fire branch — the #2216 reasoning, which applies
               identically here: counting only at delivery lets an event that ages out during a cooldown mask
               an arrival. The list is UNCAPPED while the render below is capped, so a fingerprint outside the
               displayed top N keeps its total instead of restarting. */
            var lrqOccurrences = await ObserveOccurrencesAsync(
                key, LongRunningQueryWatermarkMetric, AlertContextBuilders.LongRunningQueryIncidents(serverName, longRunning), now);
            readClock.Restart();
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

                    /* #3497: name the Agent job on the card. FIRE-time resolution, deliberately, over
                       capture-time: no schema change on either store, and the degrade is per-card rather
                       than baked into collected rows — a failed msdb lookup costs THIS card the job name
                       and the next fire tries again. Only the sessions the card will SHOW are parsed (the
                       builder's own display cap), deduped so N sessions of one job cost one key, and the
                       host answers all keys in ONE msdb round trip (AgentJobStepQuery.BuildSql). Inside
                       the cooldown-gated fire branch, so a quiet sweep pays nothing.

                       ANNOTATION, NEVER SUPPRESSION — the #3495 contract, sibling card: the read above,
                       the threshold, the fingerprint observation and the fire decision are all made before
                       this exists; it can only ever add a field. The keys never reach
                       LongRunningQueryIncidents, so the fingerprint (query_hash) is untouched by
                       construction — a card that re-fires with a different elapsed, or with the job name
                       freshly resolved, folds into the same incident it always did.

                       Degrade arms, each distinct on purpose: no resolver wired, or no Agent sessions
                       shown → agentJobNames stays NULL and the builder renders the pre-#3497 card
                       byte-identically; a resolver that THREW → the empty map, so parsed sessions render
                       the unresolved form with the raw job-id marker (the host's own permission arm
                       already degrades a denied msdb read to an empty map before it can throw — this
                       catch is the belt over resolver bugs and transport faults). NOT counted on #3013's
                       surface: that counter is store reads, and this reads the MONITORED SERVER's msdb —
                       the same exemption FetchFailedJobsAsync documents. */
                    IReadOnlyDictionary<AgentJobStepKey, AgentJobStepNames>? agentJobNames = null;
                    if (_agentJobStepResolver is not null)
                    {
                        var agentKeys = new List<AgentJobStepKey>();
                        int shownCount = Math.Min(AlertContextBuilders.LongRunningQueryDisplayCap, longRunning.Count);
                        for (int i = 0; i < shownCount; i++)
                        {
                            if (AgentJobStepQuery.TryParseProgramName(longRunning[i].ProgramName, out var jobKey)
                                && !agentKeys.Contains(jobKey))
                            {
                                agentKeys.Add(jobKey);
                            }
                        }

                        if (agentKeys.Count > 0)
                        {
                            /* The resolver is its own timed operation: without a Restart, a resolver
                               fault would record the store read's elapsed on top of its own — the
                               clock-to-itself rule the census holds every counted block to. */
                            readClock.Restart();
                            try
                            {
                                agentJobNames = await _agentJobStepResolver(key, agentKeys, ct);
                            }
                            catch (OperationCanceledException) when (ct.IsCancellationRequested)
                            {
                                throw;
                            }
                            catch (Exception ex)
                            {
                                _logger?.LogWarning("Could not resolve Agent job names for the Long-Running Query card on {Server} — the card renders the unresolved form: {Message}",
                                    serverName, ex.Message);
                                agentJobNames = s_noAgentJobNames;
                            }
                        }
                    }

                    /* The resolver's time must not ride into whatever the block awaits next: a delivery
                       fault after this point should record its own elapsed, not the msdb lookup's on top
                       — the clock-to-itself rule, applied on the operation's EXIT as well as its entry. */
                    readClock.Restart();

                    var lrqContext = AlertContextBuilders.BuildLongRunningQueryContext(serverName, longRunning, lrqOccurrences.Decorate, agentJobNames); /* :379 + #3497 */

                    /* #3653 (A5, Q5): the knob's own evidence on the card — how many sessions it removed this
                       evaluation, split by the arm that removed them — so an operator can see it working and
                       see which default did the work; a setting whose only effect is a page NOT arriving is
                       one nobody can verify. Only when the knob is SET (it is, by default, on both SKUs — the
                       seeds are set): an operator who cleared BOTH lists has said "evaluate everything", and an
                       "Excluded: 0" line on their every card would be noise about nothing. Appended, never
                       prepended — the sessions are the alert; this is a footnote about the ones that are not.
                       ANNOTATION, NEVER SUPPRESSION, like #3497's job names: the fire is decided above and
                       this can only add a line. */
                    if (!exclusions.IsEmpty && lrqContext is not null)
                    {
                        lrqContext.Details.Add(AlertContextBuilders.BuildLongRunningQueryExclusionItem(exclusions, read.ExcludedByProgramPrefix, read.ExcludedByLogin));
                    }

                    /* #3742: the shared excludedDatabases list's own receipt, beside the knob's. The read now
                       applies that list AHEAD of the cap too (it used to be dropped client-side after LIMIT, so
                       an excluded database's sessions could consume the whole page and the alert came back
                       short or empty while matches existed), and a page that is short for that reason should
                       say so. Only when the list is SET — it is empty by default, so a fresh install's card is
                       unchanged — and, like the knob's item, rendered even at 0 for an operator who set it: a
                       setting whose only effect is an absence needs a line that says the absence was nothing.
                       Appended after the knob's item: the sessions are the alert; these are footnotes about the
                       ones that are not. ANNOTATION, NEVER SUPPRESSION. */
                    if (_settings.ExcludedDatabases.Count > 0 && lrqContext is not null)
                    {
                        lrqContext.Details.Add(AlertContextBuilders.BuildLongRunningQueryExcludedDatabasesItem(_settings.ExcludedDatabases, read.ExcludedByDatabase));
                    }

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
                    readClock.Restart();
                }
            }
            else if (_activeLongRunningQueryAlert.TryGetValue(key, out var wasLongRunning) && wasLongRunning) /* :395 */
            {
                _activeLongRunningQueryAlert[key] = false;
                await ClearOccurrencesAsync(key, LongRunningQueryWatermarkMetric);                          /* :397 */
                readClock.Restart();
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
            _logger?.LogError("Failed to check long-running queries for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message); /* :409 */
            _readFailures?.RecordReadFailure(key, "long-running queries", readClock.ElapsedMilliseconds);
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

        var readClock = Stopwatch.StartNew();
        try
        {
            var tempDb = await _readAdapter.GetTempDbSpaceAsync(key, ct);           /* :418 */
            readClock.Restart();

            if (tempDb == null)
            {
                /* NO-DATA FREEZES THE GATE — never a breach, never a clear (#3653 A5). The pre-gate arm fell
                   through to its resolve branch here and announced "tempdb reserved space back to N/A": a
                   recovery nobody measured, rendered with the value it could not read. A store with no
                   tempdb_stats row for this server (a collector that stopped, a server whose tempdb collector
                   is off) is an absence of evidence, and resolving on absent evidence fabricates a recovery
                   exactly as firing on it would fabricate an alert — the same call the CPU arm makes on a
                   null sample and every per-check catch in this class makes by logging and skipping. The
                   streak (and an open incident) hold until a real row arrives. */
                return;
            }

            /* #3653 (A5): the FIRE decision sits behind the shared persistence gate, K = TempDbSpaceBreachSamples
               consecutive breaching COLLECTIONS, identified by the row's collection_time so a 30 s sweep that
               re-reads the last collected row is not a second observation (the measured flap is 1–4 samples;
               three sweeps of one sample would have re-created it behind the gate). Everything downstream of
               the decision is unchanged: the threshold compare, the Warning grade, the cooldown-governed
               reminder, the card, and the resolve's falling edge — which now comes from the gate rather than
               from the first sub-threshold read, though with TempDbSpaceClearSamples = 1 that is the same
               instant it always was. The in-memory active flag is gone; the record's Firing bit is the one
               "incident open" fact, and it survives a restart. */
            bool breaching = tempDb.ReservedPercent >= _settings.TempDbSpaceThresholdPercent; /* :420 */
            var (outcome, incidentOpen) = await ObservePersistenceAsync(
                _tempDbPersistence, TempDbSpacePersistenceMetric, key, tempDb.CollectionTimeUtc, breaching,
                TempDbSpaceBreachSamples, TempDbSpaceClearSamples);
            readClock.Restart();

            if (incidentOpen && breaching)
            {
                if (!suppressed && CooldownElapsed(_lastTempDbSpaceAlert, key, now, alertCooldown)) /* :423 */
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "tempdb Space" }; /* :425 */
                    bool isMuted = _isAlertMuted(muteCtx);                          /* :426 */
                    _lastTempDbSpaceAlert[key] = now;                               /* :427 */

                    var tempDbContext = AlertContextBuilders.BuildTempDbSpaceContext(tempDb); /* :440 */

                    /* #3653 (A8e): graded WARNING explicitly, and ONLY Warning — stated rather than left to the
                       by-name map, so the row carries the tier it fired at like every other graded site and
                       the map's arm styles only pre-#3653 replays. No Critical tier, on purpose: the product
                       has no measured "tempdb nearly full" bar to cite. The candidates were checked and
                       declined — the target-volume family's DiskCriticalFreePercent / DiskCriticalFreeGb are
                       operator knobs about a VOLUME's free space (and the tempdb volume is already that
                       alert's business); the store's Disk Pressure percent + GB floor (#3528) is a fire bar
                       for the monitor's own disk, not a tier; and the percent this alert measures has two
                       denominators (#2515: the growth ceiling where one exists, the allocation where not),
                       so a single "95% reserved" would mean two different distances from failure. Inventing
                       one would be exactly the folklore constant the repo's lineage rule exists to refuse.
                       When a measured bar exists it lands here as a second tier; until then 100% reserved is
                       a Warning the operator configured at TempDbSpaceThresholdPercent, honestly labelled. */
                    tempDbContext ??= new AlertContext();
                    tempDbContext.SeverityOverride = AlertSeverityLevel.Warning;
                    var detailText = AlertContextBuilders.ContextToDetailText(tempDbContext); /* :441 */

                    /* :443-453. ShortMessage = the toast body of :435. */
                    await FireAsync(new AlertOutcome(
                        key, serverName, "tempdb Space",
                        $"{tempDb.ReservedPercent:F0}% reserved ({tempDb.TotalReservedMb:F0} MB)",
                        $"{_settings.TempDbSpaceThresholdPercent}%",
                        tempDbContext, detailText,
                        NumericCurrentValue: tempDb.ReservedPercent,
                        NumericThresholdValue: _settings.TempDbSpaceThresholdPercent,
                        Muted: isMuted, Severity: tempDbContext.SeverityOverride,
                        ShortMessage: $"tempdb {tempDb.ReservedPercent:F0}% reserved"), ct);
                    readClock.Restart();
                }
            }
            else if (outcome == PersistenceOutcome.Resolve)                          /* :456 */
            {
                /* The falling edge, from the gate: produced exactly once, on the first fresh clear sample of
                   an open incident (TempDbSpaceClearSamples). Still gated on !suppressed, exactly as before.
                   The "N/A" arm of the old message is gone with the null-resolve above — tempDb is non-null
                   here by construction, so the percentage is always the one that was measured. */
                if (!suppressed)                                                    /* :459 */
                {
                    await NotifyResolutionAsync(new AlertResolution(
                        key, serverName, "tempdb Space",
                        "tempdb Space Resolved",                                    /* :463 */
                        $"{serverName}: tempdb reserved space back to {tempDb.ReservedPercent:F0}%"), ct); /* :461,:464 */
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Failed to check TempDB space for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message); /* :471 */
            _readFailures?.RecordReadFailure(key, "TempDB space", readClock.ElapsedMilliseconds);
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
        var readClock = Stopwatch.StartNew();
        try
        {
            var volumes = await _readAdapter.GetVolumeFreeSpaceAsync(key, ct);      /* :480 */
            readClock.Restart();
            var breached = AlertContextBuilders.GetBreachedVolumes(volumes, _settings.LowDiskThresholdPercent, _settings.LowDiskThresholdGb); /* :481 */
            conditionPresent = breached.Count > 0;                                  /* :487 — feeds the sweep result */

            /* #2362: observe every sweep, OUTSIDE the fire branch — the #2216 reasoning, which applies
               identically here: counting only at delivery lets an event that ages out during a cooldown mask
               an arrival. The list is UNCAPPED while the render below is capped, so a fingerprint outside the
               displayed top N keeps its total instead of restarting. */
            var lowDiskOccurrences = await ObserveOccurrencesAsync(
                key, VolumeFreeSpaceWatermarkMetric, AlertContextBuilders.VolumeFreeSpaceIncidents(serverName, breached), now);
            readClock.Restart();
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
                    readClock.Restart();
                }
            }
            else if (_activeLowDiskAlert.TryGetValue(key, out var wasLowDisk) && wasLowDisk) /* :538 */
            {
                _activeLowDiskAlert[key] = false;
                await ClearOccurrencesAsync(key, VolumeFreeSpaceWatermarkMetric);                                   /* :540 */
                readClock.Restart();
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
            _logger?.LogError("Failed to check volume free space for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message); /* :553 */
            _readFailures?.RecordReadFailure(key, "volume free space", readClock.ElapsedMilliseconds);
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

        var readClock = Stopwatch.StartNew();
        try
        {
            var databases = await _readAdapter.GetPvsPressureAsync(key, ct);
            readClock.Restart();
            var breached = AlertContextBuilders.GetBreachedPvsDatabases(databases, _settings.PvsThresholdPercent, _settings.PvsFloorGb);

            /* #2362: observe every sweep, OUTSIDE the fire branch — the #2216 reasoning, which applies
               identically here: counting only at delivery lets an event that ages out during a cooldown mask
               an arrival. The list is UNCAPPED while the render below is capped, so a fingerprint outside the
               displayed top N keeps its total instead of restarting. */
            var pvsOccurrences = await ObserveOccurrencesAsync(
                key, PvsWatermarkMetric, AlertContextBuilders.PvsPressureIncidents(serverName, breached), now);
            readClock.Restart();
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
                    readClock.Restart();
                }
            }
            else if (_activePvsAlert.TryGetValue(key, out var wasPvs) && wasPvs)
            {
                _activePvsAlert[key] = false;
                await ClearOccurrencesAsync(key, PvsWatermarkMetric);
                readClock.Restart();
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
            _logger?.LogError("Failed to check PVS pressure for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message);
            _readFailures?.RecordReadFailure(key, "PVS pressure", readClock.ElapsedMilliseconds);
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
    ///
    /// <para><b>The RISE gate fires once per OBSERVATION, not once per cooldown (#3636).</b> The rise is a stored
    /// fact about two collections — the newest sample and the oldest inside the window — and the
    /// <c>database_size_stats</c> collector lands one per HOUR, while this check runs every ~30 s and the
    /// cooldown is 5 minutes: at pass granularity "still growing" and "no new data yet" are indistinguishable,
    /// so the pre-#3636 loop re-fired on every cooldown expiry against the SAME two rows — up to twelve cards
    /// for one growth event before the next collection replaced the observation. #3579 found this mechanism
    /// first in the forced-plan alert (5-minute cadence, six cards measured on one production store) and its
    /// <c>CheckForcePlanFailuresAsync</c> remarks carry the fuller explanation; this is that guard one
    /// condition over. The contract now: <b>a file whose only breach is the rise gate fires once per new
    /// <see cref="DatabaseFileGrowthInfo.ObservedAtUtc"/></b> — the engine remembers, per (server, database,
    /// file), the stamp it last fired on and declines to fire the same stamp again regardless of cooldown; a
    /// newer stamp with a rise fires (a file growing across successive hourly collections still re-fires, each
    /// collection being a new observation), the cooldown still rate-limits those, and recovery is unchanged.
    /// A file breaching the LEVEL gate is a standing level and re-fires on the cooldown exactly as before — the
    /// guard never consults it. A row without a stamp falls back to the pre-#3636 cooldown-repeat rather than to
    /// silence.</para>
    /// </summary>
    private async Task CheckFileGrowthAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.FileGrowthEnabled)
        {
            return;
        }

        var readClock = Stopwatch.StartNew();
        try
        {
            var files = await _readAdapter.GetDatabaseFileGrowthAsync(
                key, _settings.FileGrowthLookbackMinutes, ct);
            readClock.Restart();

            /* #3539 A8c: the rise knob is MB per HOUR and the lookback is the window that rate is averaged over,
               so the builder scales the bar to the window rather than comparing the raw in-window delta against
               a number whose meaning would otherwise change with the other knob. */
            var breached = AlertContextBuilders.GetBreachedFiles(
                files, _settings.FileGrowthRiseMb, _settings.FileGrowthVolumePercent, _settings.FileGrowthLookbackMinutes);

            var fileGrowthOccurrences = await ObserveOccurrencesAsync(
                key, FileGrowthWatermarkMetric,
                AlertContextBuilders.FileGrowthIncidents(serverName, breached), now);
            readClock.Restart();

            if (breached.Count > 0)
            {
                var worst = breached[0];
                _activeFileGrowthAlert[key] = true;

                /* #3636: the observation guard, per file. A file that leaves the breached set loses its memory
                   here (a later re-entry is a new episode, like a forced plan that recovers and fails again); a
                   file that stays is news when it breaches the LEVEL gate — a standing level, re-fired by design
                   — or when its rise carries a stamp NEWER than the one this file last fired on. "Not newer"
                   rather than "equal", so an older stamp (nothing produces one today; a deleted newest row or a
                   clock step would) is not mistaken for news, and a null on either side never matches: a
                   stampless row keeps the cooldown-repeat, and a file that has not fired yet is always eligible.
                   The card is per server and carries every breached file, so ONE file with news is enough to
                   send it and every file on it is then stamped as reported. */
                var alertedObservations = _lastAlertedFileGrowthObservation.GetOrAdd(key, _ => new Dictionary<string, DateTime>(StringComparer.Ordinal));
                var breachedKeys = new HashSet<string>(breached.Select(FileGrowthObservationKey), StringComparer.Ordinal);
                foreach (var departed in alertedObservations.Keys.Where(k => !breachedKeys.Contains(k)).ToList())
                {
                    alertedObservations.Remove(departed);
                }

                bool anyNewObservation = breached.Any(f =>
                    AlertContextBuilders.BreachesLevelGate(f, _settings.FileGrowthVolumePercent)
                    || !(f.ObservedAtUtc is { } observedAt
                         && alertedObservations.TryGetValue(FileGrowthObservationKey(f), out var lastAlertedAt)
                         && observedAt <= lastAlertedAt));

                if (!suppressed && anyNewObservation && CooldownElapsed(_lastFileGrowthAlert, key, now, alertCooldown))
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Database File Growth" };
                    bool isMuted = _isAlertMuted(muteCtx);
                    _lastFileGrowthAlert[key] = now;
                    foreach (var f in breached)
                    {
                        /* #3636: stamped even when muted, like the cooldown — the operator muted the server's
                           file growth, not the engine's memory of which observation it already reported. */
                        if (f.ObservedAtUtc is { } reported)
                        {
                            alertedObservations[FileGrowthObservationKey(f)] = reported;
                        }
                    }

                    var context = AlertContextBuilders.BuildFileGrowthContext(
                        serverName, breached, fileGrowthOccurrences.Decorate);
                    var detailText = AlertContextBuilders.ContextToDetailText(context);

                    /* The headline names the file, its size and its share of the volume — the three facts that
                       decide whether this is worth getting up for. The rise is in the card. */
                    var headline =
                        $"{worst.DatabaseName}.{worst.FileName} is {worst.TotalSizeGb:F1} GB "
                        + $"({worst.VolumePercent:F0}% of {worst.VolumeMountPoint}), "
                        + $"grew {worst.GrowthGb:F1} GB in {worst.GrowthWindowMinutes:F0} min";

                    /* The threshold line states the rate AND the window it was averaged over, in the same unit
                       phrase the Settings windows and the card use (#3539 A8c) — and the megabytes that rate
                       amounts to inside the window, which is the number the card's "Growth" figure was held to.
                       The card's own rate is over the MEASURED span, which can be narrower than the window on a
                       server that started collecting recently; naming the in-window bar is what lets the two be
                       compared without knowing that. */
                    var riseBarMb = AlertContextBuilders.FileGrowthRiseBarMb(
                        _settings.FileGrowthRiseMb, _settings.FileGrowthLookbackMinutes);
                    await FireAsync(new AlertOutcome(
                        key, serverName, "Database File Growth",
                        headline,
                        $"rise ≥ {_settings.FileGrowthRiseMb} {AlertContextBuilders.FileGrowthRiseUnit} averaged over {_settings.FileGrowthLookbackMinutes} min "
                        + $"(≥ {riseBarMb:F0} MB in the window) or file ≥ {_settings.FileGrowthVolumePercent}% of volume",
                        context, detailText,
                        NumericCurrentValue: worst.VolumePercent,
                        NumericThresholdValue: _settings.FileGrowthVolumePercent,
                        Muted: isMuted, Severity: null,
                        ShortMessage: headline), ct);
                    readClock.Restart();
                }
            }
            else if (_activeFileGrowthAlert.TryGetValue(key, out var wasGrowing) && wasGrowing)
            {
                _activeFileGrowthAlert[key] = false;
                /* #3636: the recovery drops the server's observation memory with it, deliberately — a file
                   that recovers and later grows again is a new episode and starts with no memory, exactly as
                   the cooldown clock beside it does when the condition next fires. */
                _lastAlertedFileGrowthObservation.TryRemove(key, out _);
                await ClearOccurrencesAsync(key, FileGrowthWatermarkMetric);
                readClock.Restart();

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
            _logger?.LogError("Failed to check database file growth for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message);
            _readFailures?.RecordReadFailure(key, "database file growth", readClock.ElapsedMilliseconds);
        }
    }

    /// <summary>#3636: the per-file key of the observation memory — <c>database|file</c>, the same key
    /// <see cref="AlertContextBuilders.FileGrowthIncidents"/> fingerprints on, so the memory and the occurrence
    /// accumulator agree about what a "file" is (eight tempdb data files are eight files; a log file running
    /// away is a different incident from its data files).</summary>
    private static string FileGrowthObservationKey(DatabaseFileGrowthInfo f) => f.DatabaseName + "|" + f.FileName;

    /* ---------------- anomalous Agent jobs (Lite AlertEngine.cs:557-632) ---------------- */

    private async Task CheckAnomalousJobsAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.LongRunningJobEnabled)                                       /* :558 */
        {
            return;
        }

        var readClock = Stopwatch.StartNew();
        try
        {
            var jobsResult = await _readAdapter.GetAnomalousJobsAsync(key, _settings.LongRunningJobMultiplier, ct); /* :562 */
            readClock.Restart();

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
            readClock.Restart();
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
                    readClock.Restart();
                }
            }
            else if (_activeLongRunningJobAlert.TryGetValue(key, out var wasJob) && wasJob) /* :616 */
            {
                _activeLongRunningJobAlert[key] = false;
                await ClearOccurrencesAsync(key, AnomalousJobWatermarkMetric);                            /* :618 */
                readClock.Restart();
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
            _logger?.LogError("Failed to check anomalous jobs for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message); /* :630 */
            _readFailures?.RecordReadFailure(key, "anomalous jobs", readClock.ElapsedMilliseconds);
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

                    var failedJobContext = AlertContextBuilders.BuildFailedJobContext(
                        serverName, failedJobs, failedJobOccurrences.Decorate,
                        windowEndUtc: now, lookbackMinutes: _settings.FailedJobLookbackMinutes); /* :695 */
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
        var readClock = Stopwatch.StartNew();
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
            _logger?.LogError("Failed to check database state for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message);
            _readFailures?.RecordReadFailure(key, "database state", readClock.ElapsedMilliseconds);
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

                var shortMessage = pending
                    ? $"{dbName} first observed {stateText} (no baseline yet)"
                    : $"{dbName} changed to {stateText} (expected {expectedText})";

                /* #2109: the same facts as discrete fields — this alert fired with Context: null, which
                   left the database name reachable only by parsing the title. These three fields are the
                   canonical copy and the detail text is their flattening, as at every other engine fire
                   site, so no channel receives the same fact under two labels. The no-baseline case needs
                   no field of its own: expectedText above IS "(no baseline yet)" whenever pending, and
                   ShortMessage says it too. */
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

                var detailText = AlertContextBuilders.ContextToDetailText(stateContext);

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
    /// <para><b>Standing condition with per-plan resolution, firing once per OBSERVATION (#3579).</b> The
    /// original contract read "while a plan keeps failing it re-fires on the cooldown, mirroring the
    /// database-state family", and that is the right contract for a condition whose input is re-measured
    /// every pass. This condition's input is not: it is a stored delta between the two newest COLLECTIONS,
    /// which changes only when the collector lands a row (the query_store cadence, 5 minutes by default and
    /// ~15 on the store that found this), while the engine re-asks every ~30 s and the cooldown is 5 minutes.
    /// Cooldown shorter than cadence means the cooldown expires several times against the SAME two rows, and
    /// at pass granularity "still failing" and "no new data yet" are indistinguishable — the pre-#3579 loop
    /// took the second for the first. Measured on one production store, one plan: the raw series was
    /// 0, 0, 1 (forced AUTO), 0, 0 across five collections at 03:47 / 04:02 / 04:18 / 04:50 / 05:01, and the
    /// engine fired SIX times between the 04:18 and 04:50 collections — 04:18:53, 04:24:31, 04:30:04,
    /// 04:35:33, 04:40:49, 04:46:03, one per cooldown — every card honestly reading New 1 / Total 1, because
    /// every pass re-read the same 04:02→04:18 rise. The arithmetic was right; the repetition was the defect.
    /// The channel saw two of the six only because webhook-side throttling ate the rest.</para>
    ///
    /// <para>The contract now: <b>a plan fires once per new observation that shows a rise</b> — the engine
    /// remembers, per plan, the <see cref="ForcePlanFailureInfo.ObservedAtUtc"/> it last fired on and
    /// declines to fire the same stamp again regardless of cooldown; a newer stamp with a rise fires (a plan
    /// failing across successive collections still re-fires, each collection being a new observation), the
    /// cooldown still rate-limits those, and a plan that stops appearing announces a recovery exactly as
    /// before. The poison-wait family took the same guard for the same shape under #2704; this is that
    /// guard at plan grain. A row without a stamp falls back to the pre-#3579 cooldown-repeat rather than
    /// to silence.</para>
    /// </summary>
    private async Task CheckForcePlanFailuresAsync(
        string key, string serverName, DateTime now, TimeSpan alertCooldown, bool suppressed, CancellationToken ct)
    {
        if (!_settings.ForcePlanFailureEnabled)
        {
            return;
        }

        List<ForcePlanFailureInfo> failures;
        var readClock = Stopwatch.StartNew();
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
            _logger?.LogError("Failed to check forced-plan failures for {Server} after {ElapsedMs} ms: {Message}", serverName, readClock.ElapsedMilliseconds, ex.Message);
            _readFailures?.RecordReadFailure(key, "forced-plan failures", readClock.ElapsedMilliseconds);
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

        var active = _activeForcePlanAlerts.GetOrAdd(key, _ => new Dictionary<string, ForcePlanActivePlan>(StringComparer.Ordinal));

        foreach (var (planKey, failure) in current)
        {
            if (active.TryGetValue(planKey, out var plan))
            {
                plan.LastSeen = failure;
            }
            else
            {
                plan = new ForcePlanActivePlan(failure);
                active[planKey] = plan;
            }

            /* #3579: the observation guard. The row's stamp is the collection that produced the rise; if it
               is not NEWER than the one this plan last fired on, this pass is re-reading an observation the
               operator already has a card for, and no amount of elapsed cooldown makes it a second event.
               "Not newer" rather than "equal" so the contract reads as stated — fire once per NEW
               observation — and an older stamp (nothing produces one today; a deleted newest row or a clock
               step would) is not mistaken for news. A null on either side never matches: a stampless row
               keeps the cooldown-repeat, and a plan that has not fired yet is always eligible. */
            bool sameObservation =
                failure.ObservedAtUtc is { } observedAt
                && plan.LastAlertedObservedAtUtc is { } lastAlertedAt
                && observedAt <= lastAlertedAt;

            var cooldownKey = key + "|" + planKey;
            if (!suppressed && !sameObservation && CooldownElapsed(_lastForcePlanAlert, cooldownKey, now, alertCooldown))
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
                plan.LastAlertedObservedAtUtc = failure.ObservedAtUtc; /* #3579: and so is the observation */

                /* #2109 discipline: the same facts the prose carries, as discrete fields, so a consumer
                   never has to parse the title to learn which plan this is about.
                   #3297: and the fields are now the only carrier — the detail text is their flattening, as
                   at every other engine fire site, rather than a hand-authored restatement that would
                   deliver the same facts twice on every channel. The consequence the prose stated and the
                   fields did not is a field now. */
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
                        ("Total Failures", failure.TotalFailures.ToString(CultureInfo.InvariantCulture)),
                        ("Effect", "the query is running on the optimizer's plan, not the forced one")
                    }
                });

                var detailText = AlertContextBuilders.ContextToDetailText(context);

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
            foreach (var (planKey, recoveredPlan) in recovered)
            {
                var lastSeen = recoveredPlan.LastSeen;
                /* Removing the plan drops its #3579 observation memory with it, deliberately: a plan that
                   recovers and later fails again is a new episode and starts with no memory, exactly as the
                   cooldown clock beside it does. */
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
    /// The tier a "Deadlocks Detected" fire wears (#3653, A8e): Critical when the window's deadlock RATE
    /// reaches the deadlock health band's Critical tier, Warning otherwise. The band's own classifier,
    /// over the engine's own window — <see cref="RollingCountWindowHours"/> is one hour, so the count IS
    /// the hourly rate and the band's one-hour minimum window is met — and the same store-tunable pair the
    /// fleet card and calendar band on (<see cref="IAlertEngineSettings.DeadlockRateThresholds"/>). The
    /// band's Healthy and Warning both map to Warning here: the count knob already decided this fire was
    /// asked for, and a delivered alert is never rendered as nothing. Its Unknown arm is unreachable on a
    /// non-null count over a window at the minimum, and would map to Warning too.
    /// </summary>
    public static AlertSeverityLevel GradeDeadlockFire(int deadlockCount, DeadlockRateThresholds tiers) =>
        ServerHealthClassifier.DeadlockSeverity(deadlockCount, TimeSpan.FromHours(RollingCountWindowHours), tiers)
            == HealthSeverity.Critical
            ? AlertSeverityLevel.Critical
            : AlertSeverityLevel.Warning;

    /// <summary>
    /// The tier a "High CPU" fire wears (#3653, A8e): Critical at the CPU health band's Critical bar
    /// (<see cref="ServerHealthThresholds.CpuCriticalPercent"/>, 95% of a fixed host), Warning otherwise.
    ///
    /// <para><b>Which bar, and what its lineage is.</b> The product has two CPU ladders: the analysis
    /// scorer's <c>CPU_SQL_PERCENT</c> pair (75, 95), a scoring formula with no stated measurement, and the
    /// health band's (80, 95), the ONE ladder the fleet card, <c>get_fleet_overview</c>, <c>/api/fleet</c>
    /// and the Performance Calendar's high-CPU day count all band on, pinned across both SKUs by #3539 A2 so
    /// "high CPU" means one thing on the card and the day cell. Neither pair is a fleet percentile the way the
    /// deadlock tiers are (#3368) — the band's own doc states its cutoffs against a quantity, not a
    /// distribution — so this is not a claim that 95% is measured; it is the claim that the alert row and
    /// the card must not disagree about the colour of the same minute. Before #3653 a 100% fire rendered the
    /// same amber as an 80% one while the card beside it was red. If a measured CPU Critical bar is ever
    /// established, the band's constant is where it lands, and this follows it.</para>
    ///
    /// <para>The quantity is the engine's <c>alertCpuValue</c> — SQL Server's ring-buffer percent of a FIXED
    /// host (this arm never sees a PostgreSQL target: the PI/ACU reading is banded by the Darling host's own
    /// CPU arm, and <c>cpu_utilization</c> has no row for one). The operator's <c>CpuThresholdPercent</c>
    /// still decides WHETHER it fires — an operator who set 97 sees every fire Critical, which is a coherent
    /// reading of a threshold above the Critical bar, the same way the deadlock record's Critical tier is
    /// not floored at its Warning tier.</para>
    /// </summary>
    public static AlertSeverityLevel GradeCpuFire(double cpuPercent) =>
        cpuPercent >= ServerHealthThresholds.CpuCriticalPercent
            ? AlertSeverityLevel.Critical
            : AlertSeverityLevel.Warning;

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
