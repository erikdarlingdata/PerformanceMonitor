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
using System.Text;
using System.Text.Json;
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
/// watching a dashboard. The FOUNDING conditions are listed below — the first three reframe a Dashboard
/// health check onto Darling's own signals; Store Disk Pressure was net-new and guards the service's OWN
/// store — and every condition added since is sectioned in the body under its own issue number. The list is
/// not a census and deliberately carries no count, because a numeral here goes stale silently every time a
/// condition is added and nothing checks it. All of them route through the SAME
/// <see cref="IAlertDeliverer"/> the shared alert engine uses (so they inherit its email/webhook delivery,
/// per-fingerprint delivery cooldown, and restart replay) and the SAME <c>config_alert_log</c> history
/// store:
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
///   store itself (its last recorded size, for context) and the store volume's free space: when a headless
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
       check below covers the fast path for a server that is connected but erroring every collector.
       Derived from the shared default so the display's Offline band and this alert describe the SAME
       condition (#2794) — CollectionStoppedThresholdAgreementTests pins the agreement. */
    internal static readonly TimeSpan StaleWindow =
        TimeSpan.FromMinutes(ServerHealthThresholds.CollectionStoppedMinutesDefault);

    /* The last N logged runs all failing (no SUCCESS/SKIPPED among them) fires "Collection Stopped"
       faster than the staleness backstop when a connected server's collectors are erroring on every
       cycle. 10 spans a couple of minutes of total failure across the frequently-scheduled collectors. */
    internal const int ConsecutiveFailureThreshold = 10;

    /* Store Disk Pressure fires when the store volume drops below this percent free — a percentage so it
       scales from a small managed box to a large fleet disk; 10% is the universal DBA "act now" threshold
       for a database volume, and mirrors the shared engine's target-server low-disk percent
       (LowDiskThresholdPercent). The condition no-ops when free space is undeterminable (a remote BYO
       store), so it never false-alarms; the managed store's own volume is the case it exists to protect. */
    internal const double DiskFreeWarnPercent = 10.0;

    /* #3528: the percent's GB floor — pressure additionally requires free space BELOW this many GB, an AND
       qualifier so a big volume at a low percent (400 GB free on a 4 TB store) never pages CRITICAL. This
       was "percent-only by design (a GB floor is a trivial follow-up if an operator ever wants one)"; #3528
       is that want. The composition is PvsFloorGb's (percent triggers, the floor keeps it honest, 0 removes
       the floor), deliberately NOT the target-volume pair's OR — there the GB dimension ADDS fires, which
       would make this alert noisier, the opposite of the complaint. 50 puts the crossover at a 500 GB
       volume: below that the percent governs exactly as before; above it, 50 GB free is the line. */
    internal const double DiskFreeWarnFloorGb = 50.0;

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
       least three prior days, and its latest day's cost PER RUN must exceed its run-weighted baseline per run by
       CostRegressionFactor, before it alerts — so a cheap collector, or a new one, cannot trip it.
       #2846: the comparison is per RUN, not per day. Daily totals are runs x cost-per-run, so a cadence
       recovery — more of the same work, each unit cheaper — used to read as a regression. It fired 3,259 times
       over 612 pairs in one day, 53% of them on collectors whose per-run cost had FALLEN.
       #3316: the two gates measure DIFFERENT UNITS, so the daily-total floor does not constrain the per-run
       ratio — a total-cost floor is cleared by VOLUME, so a collector averaging 3 ms per run clears
       CostRegressionBaselineFloorMs on run count alone and is then judged by a ratio on that 3 ms. Such a
       firing is TRUTHFUL (both sides are means over many runs, so this is not rounding) and useless: the
       measured case doubled 3.0 -> 6.1 ms per run over 50 runs, which costs 0.16 s a day. So a third gate
       asks what the regression COSTS — the per-run rise times the volume it is paid on — and requires
       CostRegressionAddedMsFloor of it. That is unit-consistent with the ratio, and unlike a minimum
       per-run baseline it still reports a 3 ms collector that runs often enough for the rise to matter.
       #3462: that floor is recalibrated from the fleet's own per-collector daily-cost distribution (see
       its declaration) after an 8.7 s/day card broke #3316's empty-band assumption, and the digest's
       movers read now shares it — evaluated in the reads themselves, so both delivery surfaces inherit
       one definition of material.
       #3440: the ratio is measured against the baseline's UPPER EDGE as well as its mean. A heavy
       collector's own spread exceeds CostRegressionFactor on natural variation — per-run p95/avg measured
       at 2.03x to 5.04x across index_object_stats, procedure_stats and query_store on one fleet — so
       against a mean baseline a normal upper-mode day cleared the factor by construction, and the
       CostRegressionAddedMsFloor cannot screen those because the expense that makes a collector bimodal
       makes its upper mode's excess large. The reader now also returns the p95 of the PRIOR days' per-run
       cost and requires the factor on that too. The two are an AND, so this alert can only fire on a
       subset of what it fired on before, and CostRegression.ThresholdMsPerRun is the bound the fired alert
       reports so the threshold a reader falsifies against is the one that selected the row. */
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
    /// <summary>
    /// #3192: the figure this condition fires on is <c>collect.collector_cost.total_sql_ms</c>, which is the
    /// driver's SQL slice — and on the enumerated path that slice contains the per-item watermark refresh and
    /// the deferred plan/text fetches, all of which touch the monitoring STORE. So a <c>query_store</c>
    /// regression here can be the store getting slower rather than the target, and the alert used to say
    /// flatly that it was "cost on the target". Appended rather than folded into the sentence above so the
    /// text stays one substitution away from being re-worded, and stated on the alert itself because that is
    /// where the reader is when the inference gets made.
    /// </summary>
    private const string CostIsNotAllTargetSide =
        "NOTE: on collectors that fetch plan XML or statement text (query_store), part of this figure is the "
        + "monitoring STORE's own probe and write rather than the monitored server - they run inside the same "
        + "per-item stopwatch. get_collection_log's sql_store_ms attributes it per run; this series carries no "
        + "phase split.";

    private const double CostRegressionFactor = 2.0;
    private const long CostRegressionBaselineFloorMs = 1000;

    /// <summary>#3316/#3462: the minimum ADDED cost per day, in ms, before a per-run regression is worth
    /// reporting on EITHER delivery surface — the page and the digest movers list both gate on it, taken
    /// from this one constant so the two cannot disagree about what material means.
    ///
    /// <para><b>The #3316 calibration this replaces claimed a band the fleet has since disproved.</b> Its
    /// 41-hour sample put real regressions at 21.8-22.6 s/day and truthful-unactionable ones at
    /// 0.16-3.6 s/day, and set 5 s in the gap claiming "more than 4x headroom on both sides". On
    /// 2026-09-15 a production server paged hourly on a 5.5x <c>database_size_stats</c> regression whose
    /// whole-day total was 8.7 seconds and whose ADDED cost — this floor's own unit — was 7.1 s/day,
    /// 1.42x the floor: truthful, unactionable, and comfortably selected — while the same day's genuine
    /// exhibits added 224 and 377 s/day. The unactionable mass reaches at least 7.1 s/day of added cost,
    /// so the empty band is (7.1, 21.8) and 5 s sits below it, not inside it.</para>
    ///
    /// <para><b>15 s/day is derived from the fleet distribution of per-collector daily cost, not from that
    /// one card (#3462).</b> Measured over 7 days on two stores — 42 and 43 servers of one production
    /// store class — the MEDIAN (server, collector) pair costs 13.7 and 13.1 s/day to exist at all, and
    /// the cheapest 17 of 39 collectors each cost under 4 s/day. A regression whose entire added footprint
    /// is less than the median collector's whole daily bill is lost in the fleet's own operating mass; at
    /// 15 s the floor sits just above both medians and inside the measured empty band — 2.1x above its
    /// noise edge and 1.45x under the smallest real catch. The headroom is honestly thinner than #3316
    /// claimed for 5 s, because the band itself is thinner than #3316 believed: both of its edges are now
    /// measured rather than extrapolated.</para>
    ///
    /// <para>Internal so <c>CollectorCostMaterialityFloorTests</c> can run the SHIPPED value against the
    /// shipped query: the 2026-09-15 exhibits and #3316's smallest catch bracket this constant, so a retune
    /// outside the measured band turns a fixture red rather than silently re-admitting the noise or
    /// dropping the catches.</para></summary>
    internal const long CostRegressionAddedMsFloor = 15000;
    private static readonly TimeSpan CostRegressionBaselineWindow = TimeSpan.FromDays(14);

    /// <summary>
    /// #3443: the fan-out that separates the one cost shape worth interrupting somebody for from the ones
    /// worth reading in the morning. A collector reaches the paging channel only when the regression is a
    /// property of the COLLECTOR rather than of a server: it was selected on at least this many servers AND
    /// on more than half the servers that collector actually ran on in the window
    /// (<see cref="CollectorCostDenominatorWindow"/>). Everything else is reported by
    /// <see cref="CollectorCostDigestMetric"/> instead, with the comparison context an alert card cannot
    /// carry.
    ///
    /// <para><b>The majority half is a comparison, not a threshold.</b> The collector's code and the
    /// monitoring store are shared across the fleet, so a deployment or store-side cause raises the cost
    /// everywhere the collector runs; one server's slow day raises it on one server. "More than half"
    /// is the weakest statement that says the rise is the common case rather than the exception, and there
    /// is no number in it to tune — 22 of 43 servers and 2 of 3 are the same rule.</para>
    ///
    /// <para><b>The floor of 2 exists because a majority of one is one.</b> A collector that ran on a single
    /// server would otherwise satisfy the majority rule on that server alone, which is exactly the shape
    /// being demoted.</para>
    ///
    /// <para><b>Measured, and the measurement is uncomfortable enough to state.</b> Over 8 days on a
    /// 43-server production store this condition produced 937 firings across 143 (collector, day)
    /// combinations; the widest fan-out any collector reached was 10 servers of 43, so NONE of the 937
    /// would have paged under this rule and all 937 would have been digest lines. That is the intended
    /// outcome rather than a coincidence — the shape this page is for is a build or a store change reaching
    /// the whole fleet (#2133/#2150), which is rare by construction — but it does mean the paging half has
    /// no live firing to validate against, and its fixtures are the only demonstration that it still
    /// fires.</para>
    /// </summary>
    private const int CostRegressionFleetWideMinServers = 2;

    /// <summary>
    /// The window the fan-out DENOMINATOR is measured over — how many servers each collector ran on. A
    /// trailing 24 hours rather than the current UTC day, because a day-aligned denominator is a partial
    /// count for the whole first hour after midnight and the majority rule would read a smaller fleet than
    /// the collector has. Named in the fired alert text, because a ratio whose denominator the reader cannot
    /// see is not falsifiable.
    ///
    /// <para>It is a DIFFERENT window from the regression read's own <c>latest_day</c> numerator, and that
    /// asymmetry is deliberate: the numerator has to stay the predicate's own grain (this evaluator does not
    /// own that predicate), and the denominator has to be a full count. For a collector that runs many times
    /// an hour the two populations are the same 43 servers either way; for a once-daily collector the
    /// denominator is the servers it ran on in the last day, which is the same set.</para>
    /// </summary>
    private static readonly TimeSpan CollectorCostDenominatorWindow = TimeSpan.FromHours(24);

    /// <summary>
    /// The metric name the collector-cost DIGEST fires under (#3443). A WEBHOOK AUTOMATION KEY like its
    /// siblings — the alert-history grids, the severity map and any downstream consumer key on it — so it is
    /// a const and must stay stable across releases.
    /// </summary>
    internal const string CollectorCostDigestMetric = "Collector Cost Digest";

    /// <summary>Fleet-level key, non-numeric so it never collides with a real server_id (the
    /// <see cref="DiskKey"/> shape).</summary>
    private const string CollectorCostDigestKey = "costdigest";

    /// <summary>
    /// How often the digest is sent. Daily, and for <see cref="StaleMuteRefire"/>'s reason rather than a new
    /// one: the shared alert cooldown is clamped to at most two hours, and the fact this reports is a day's
    /// per-run cost against a multi-day baseline, which does not change twelve times a day.
    ///
    /// <para><b>Not the same argument as #3306's, though, and the difference is the point of #3443.</b>
    /// <see cref="StaleMuteRefire"/> makes a standing fact livable by re-stating it less often on the same
    /// channel; a stale mute has one actionable step and an alert card holds it. This interval is doing
    /// something else: it is the period over which findings are COLLECTED INTO ONE DOCUMENT, because the
    /// action a cost movement invites is "look at a distribution and decide", which does not fit on a card
    /// and cannot be taken from a phone at 3am. A longer interval alone would have produced the same
    /// unactionable card less often.</para>
    ///
    /// <para><b>Gated on DELIVERED-TODAY in the store, not fired-today in memory (#3580).</b> This shipped
    /// as "in-memory, like every sibling's interval, and a restart costs one extra digest" — the failure
    /// class <see cref="StaleMuteRefire"/> and #3430's <c>RepeatDeliveryBudget</c> both accept, on the
    /// argument that an extra copy of a report is the cheapest possible failure. The v3.8.0 install night
    /// priced it: three stores restarted once each, and the channel carried SIX re-announcements among ~23
    /// overnight posts — a quarter of the channel was this document and the rollup, twice — because a fresh
    /// process has an empty <see cref="_lastCostDigest"/> and the gate was answering "has THIS PROCESS sent
    /// one today" when the reader's question is "has one been DELIVERED today". The same night showed the
    /// case the fix must keep: a pair whose delivery had FAILED (a transport fault, not a suppression) was
    /// correctly re-attempted after the restart and landed; the restart was the recovery.</para>
    ///
    /// <para>So the gate now reads a delivery stamp from <see cref="ISelfAlertDeliveryStampStore"/> (the
    /// store's existing key/value state table, no rung) and skips while <c>now - stamp</c> is inside this
    /// interval, restart or not; and the stamp is written only when the deliverer reports a disposition
    /// other than <see cref="AlertDelivery.ChannelFailed"/>. A failed delivery writes nothing, so the next
    /// tick — restart or not — retries; the in-memory dictionary remains as a same-process fast path (23 of
    /// 24 ticks still cost one lookup and no store read) but is no longer the authority. A store fault on
    /// the stamp falls back to that fast path and warns — fail-open toward delivering, the direction every
    /// store-fault posture in this evaluator already takes, because the alternative (skip on an unreadable
    /// stamp) would let a store hiccup silence a daily document, and the memory gate still bounds the
    /// fallback at one copy per process. The nothing-is-lost half of the original argument still holds:
    /// the digest is recomputed from the store every time rather than accumulated in process.</para>
    /// </summary>
    internal static readonly TimeSpan CollectorCostDigestInterval = TimeSpan.FromDays(1);

    /// <summary>How many movers the digest spells out, on <see cref="MaxListedStaleMuteRules"/>' reasoning —
    /// one bounded message, not a wall of text. Measured: 177 pairs on one 43-server store and 103 on
    /// another were eligible on the same day, so the cap is doing real work and the digest states the
    /// population it selected from rather than implying it showed everything.</summary>
    private const int MaxListedCostMovers = 20;

    /// <summary>How many collectors the digest's heaviest-first census spells out.</summary>
    private const int MaxListedCostHeaviest = 10;

    /// <summary>When the digest was last known DELIVERED — the <see cref="_lastStaleMuteAlert"/> idiom, one
    /// fixed key, but since #3580 a CACHE of the store's stamp rather than the authority: filled from the
    /// stamp on the first tick that has to ask, and from the fire itself on a delivery the deliverer did
    /// not report failed. A digest has no active flag and no resolution edge: it is a report of a
    /// measurement, not a condition that can be entered and left, so there is nothing to clear.</summary>
    private readonly ConcurrentDictionary<string, DateTime> _lastCostDigest = new();

    /// <summary>
    /// The metric name the fleet sweep's DAILY ROLLUP fires under (#3466 lane 4). A WEBHOOK AUTOMATION KEY
    /// like its siblings — the alert-history grids, the severity map's declared INFO arm and any downstream
    /// consumer key on it — so it is a const and must stay stable across releases.
    /// </summary>
    internal const string FleetSweepRollupMetric = "Fleet Sweep Rollup";

    /// <summary>Fleet-level key, non-numeric so it never collides with a real server_id (the
    /// <see cref="DiskKey"/> shape).</summary>
    private const string FleetSweepRollupKey = "sweeprollup";

    /// <summary>
    /// How often the sweep rollup is sent — and unlike every sibling interval, this one is a RULING rather
    /// than a tuning choice: the #3466 delivery contract bounds the sweep feature's channel presence at ONE
    /// post per day, a CEILING and not a default ("a daily digest channel notification is fine, but not an
    /// hourly one"). The web feed is the workhorse at full sweep cadence; report-class content any finer than
    /// daily in a paging channel trains operators to ignore the channel, which is the failure mode half the
    /// alerting issues exist to undo. The interval is also the rollup's COVERED SPAN: each post summarizes
    /// the trailing day and only the trailing day, so what a post claims to cover and how often one can
    /// arrive are the same number and cannot drift apart.
    ///
    /// <para><b>Gated on delivered-today in the store, like <see cref="CollectorCostDigestInterval"/> and
    /// for the same night's reason (#3580).</b> This shipped in-memory with "a restart costs one extra
    /// rollup" accepted as the cheapest failure; the install night's census — three restarts, six
    /// re-announcements, this document being three of them — is what that acceptance cost, and the digest's
    /// remarks carry the arc. The one-post-per-day CEILING above is a ruling, and a gate that a restart
    /// resets is a ceiling the deployment procedure breaches on every install. The rollup takes the same
    /// stamp store, the same failed-writes-nothing rule and the same fail-open fallback, under its own key.
    /// Still recomputed from the sweep store every time rather than accumulated in process, so nothing is
    /// lost in either direction.</para>
    /// </summary>
    internal static readonly TimeSpan FleetSweepRollupInterval = TimeSpan.FromDays(1);

    /// <summary>How many band transitions the rollup spells out — the <see cref="MaxListedCostMovers"/>
    /// reasoning: one bounded message. A churning fleet at hourly cadence can move bands dozens of times a
    /// day, and the web timeline is where that day is READ; the rollup states the total beside the cap so
    /// it never implies it showed everything.</summary>
    private const int MaxListedRollupTransitions = 20;

    /// <summary>How many watch-item events (per direction) the rollup spells out.</summary>
    private const int MaxListedRollupWatchEvents = 10;

    /// <summary>How many servers a would-have-paged family names before eliding — the ledger block must
    /// stay readable on the fleet-wide day it exists for.</summary>
    private const int MaxListedRollupLedgerServers = 10;

    /// <summary>When the rollup was last known DELIVERED — the <see cref="_lastCostDigest"/> idiom, one
    /// fixed key, and since #3580 the same cache-of-the-stamp role rather than the authority. A rollup is a
    /// report of a period, not a condition: no active flag, no resolution edge, nothing to clear.</summary>
    private readonly ConcurrentDictionary<string, DateTime> _lastSweepRollup = new();

    /// <summary>
    /// The metric name the ANALYSIS SINGLES DIGEST fires under (#3712) — the third daily document, beside the
    /// collector-cost digest and the fleet-sweep rollup. A WEBHOOK AUTOMATION KEY like its siblings (the
    /// alert-history grids, the severity map's declared INFO arm, the family census and any downstream
    /// consumer key on it), so it is a const and must stay stable across releases.
    /// </summary>
    internal const string AnalysisSinglesDigestMetric = "Analysis Singles Digest";

    /// <summary>Fleet-level key, non-numeric so it never collides with a real server_id (the
    /// <see cref="DiskKey"/> shape).</summary>
    private const string AnalysisSinglesDigestKey = "singlesdigest";

    /// <summary>
    /// How often the singles digest is sent. Daily, and the interval is also its COVERED SPAN, the rollup's
    /// exact reasoning: what the post claims to cover and how often one can arrive are one number. It exists
    /// because of what #3712 measured — forty-plus uncorroborated anomaly pages in seven hours on a large
    /// production fleet, interleaved with the handful that needed a human — and the action a lone-fact
    /// anomaly invites is "look at the day's singles as a distribution and decide which to promote", which is
    /// a document to read, not a card to act on at 3am. The findings themselves are live on the web and MCP
    /// surfaces the instant they fire; this is the once-a-day channel copy, and its ceiling.
    ///
    /// <para>Gated on delivered-today in the store like both siblings (#3580): the same stamp store, the same
    /// failed-writes-nothing rule, the same fail-open fallback, under its own key. Recomputed from the
    /// ledger every time rather than accumulated in process, so a restart loses nothing in either
    /// direction.</para>
    /// </summary>
    internal static readonly TimeSpan AnalysisSinglesDigestInterval = TimeSpan.FromDays(1);

    /// <summary>How many top movers the singles digest spells out, on <see cref="MaxListedCostMovers"/>'
    /// reasoning — one bounded message that states the population it selected from.</summary>
    private const int MaxListedSinglesMovers = 10;

    /// <summary>How many servers a family block names before eliding, and how many singles a server line
    /// names — the ledger block's <see cref="MaxListedRollupLedgerServers"/> reasoning: readable on the
    /// fleet-wide day it exists for.</summary>
    private const int MaxListedSinglesServersPerFamily = 10;
    private const int MaxListedSinglesPerServer = 3;

    /// <summary>When the singles digest was last known DELIVERED — the <see cref="_lastCostDigest"/> idiom:
    /// one fixed key, a cache of the store's stamp rather than the authority (#3580). A digest is a report of
    /// a period, not a condition: no active flag, no resolution edge, nothing to clear.</summary>
    private readonly ConcurrentDictionary<string, DateTime> _lastSinglesDigest = new();

    /// <summary>The fixed key for the fleet-level Store Disk Pressure edge (not a real server).</summary>
    private const string DiskKey = "store";

    /// <summary>The alert metric name the fleet-level Store Disk Pressure edge fires under. A WEBHOOK
    /// AUTOMATION KEY like its siblings, and the triage map (#2768) keys on it too, so it is a const and
    /// must stay stable across releases.</summary>
    internal const string DiskPressureMetric = "Store Disk Pressure";

    /// <summary>The resolution title the Store Disk Pressure recovery edge records into
    /// <c>config_alert_log.metric_name</c>. The triage map (#2768) aliases it back to
    /// <see cref="DiskPressureMetric"/>, so it is a const for the same reason.</summary>
    internal const string DiskPressureResolvedMetric = "Store Disk Pressure Resolved";

    /// <summary>
    /// The synthetic server label every FLEET-LEVEL store self-alert fires under — each condition in this
    /// class whose subject is the store or its configuration rather than one monitored server, plus each
    /// one's resolution edge. Described rather than listed, because a list here goes stale silently every
    /// time a condition is added and nothing checks it.
    /// The monitoring store is not a SQL Server instance and is not in the monitored-server registry,
    /// so this string deliberately resolves to NOTHING: <c>DarlingServerResolver</c> cannot match it, and the
    /// deliverer's #1236 int.TryParse override no-ops on it exactly like the non-numeric <see cref="DiskKey"/>.
    ///
    /// <para>A CONST rather than twelve repeated literals since #2768: the triage endpoint has to recognise
    /// this exact label to know an alert is fleet-level and serve store reads instead of per-server ones.
    /// While it was a bare literal that page ran <c>get_server_summary</c> / <c>get_collection_health</c> /
    /// <c>get_collection_log</c> against an unresolvable server and rendered three resolver errors on every
    /// store alert. Renaming it must stay in step with <c>DarlingTriageEndpoint.IsFleetLevelStoreServer</c>,
    /// which is why both sides now read one symbol.</para>
    ///
    /// <para>Since #3500 this is the DEFAULT rather than the only spelling: an operator running several
    /// stores can set <c>peers.storeName</c> and every fleet-level self-alert fires under that label instead,
    /// through <see cref="EffectiveStoreLabel"/> and the <see cref="_storeLabel"/> field — the ONE seam every
    /// fire site reads, pinned from source by the tests, so a thirteenth family cannot quietly hardcode this
    /// constant back in. Unset stays byte-identical to before the field existed.</para>
    /// </summary>
    internal const string StoreServerLabel = "Monitor Store";

    /// <summary>
    /// Resolves the configured <c>peers.storeName</c> into the label the fleet-level self-alerts fire under
    /// (#3500): the trimmed name when one is set, else <see cref="StoreServerLabel"/>. PURE and the ONLY
    /// place the fallback decision lives, so the evaluator and any other consumer cannot answer it two ways.
    /// A storeName spelled exactly as the constant resolves TO the constant — same bytes, same fingerprints,
    /// no re-key — rather than being treated as an opt-in that changes nothing but the key shape.
    /// </summary>
    internal static string EffectiveStoreLabel(string? configuredStoreName)
    {
        var trimmed = (configuredStoreName ?? "").Trim();
        return trimmed.Length == 0 || string.Equals(trimmed, StoreServerLabel, StringComparison.Ordinal)
            ? StoreServerLabel
            : trimmed;
    }

    /// <summary>
    /// The label every fleet-level self-alert fires under — <see cref="StoreServerLabel"/> unless the
    /// operator opted into <c>peers.storeName</c> (#3500). Resolved ONCE at construction because the peers
    /// block is file-only and restart-only (unlike the hot-reloading store-backed knobs, whose seams are
    /// <c>Func</c>s for that reason): a <c>Func</c> here would claim a liveness the config cannot deliver.
    /// </summary>
    private readonly string _storeLabel;

    /// <summary>
    /// The delivery identity key for one fleet-level self-alert family (#3500): the family key itself when
    /// the label is the shipped constant, else the label prefixed on — <c>"dc1-monitor-01:store"</c>. The
    /// serverKey is what the delivery fingerprint's no-incident fallback concatenates with the metric
    /// (<c>WebhookAlertService.DerivePagerDutyDedupKey</c>: <c>{serverKey}:{metric}</c>), so WITHOUT this the
    /// opted-in label would change every card while two stores' work items kept colliding on the identical
    /// key — the exact half of #3500 that bites. The family key stays inside the qualified form because the
    /// per-object families (retention, compression, job cadence) rely on it to keep per-object cooldowns and
    /// history identities apart; only the LABEL is new. History storage is unaffected either way: every
    /// non-numeric key already collapses into the write-only server_id 0 bucket (#3456), and the cooldown
    /// seed already refuses to read it, so qualifying the key re-keys nothing but the fingerprint — which is
    /// the re-key the operator accepted by setting the field.
    /// </summary>
    private string StoreKey(string familyKey) =>
        _storeLabel == StoreServerLabel ? familyKey : _storeLabel + ":" + familyKey;

    /* Custom-alert-rule health edge state (#3304). FLEET-level like disk pressure (the rules are a fleet
       concept, not per-server), so a single fixed sentinel key. Standing condition (the AG-Sync-Fell-Behind /
       Collection-Stopped idiom): active flag + cooldown re-fire while ANY custom rule is broken or never-firing,
       one resolution when all rules are healthy again. The report itself is rebuilt each check by the
       CustomAlertEvaluator; this evaluator only decides fire/hold/resolve and renders the (already-sanitized)
       rule list. */
    private readonly ConcurrentDictionary<string, bool> _activeCustomRuleHealth = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastCustomRuleHealthAlert = new();

    /// <summary>The fixed key for the fleet-level custom-alert-rule-health edge (not a real server); non-numeric
    /// so the deliverer's #1236 int.TryParse override no-ops on it exactly like <see cref="DiskKey"/>.</summary>
    private const string CustomRuleHealthKey = "customalerts";

    /// <summary>The alert metric name the custom-alert-rule-health self-alert fires under (#3304). A WEBHOOK
    /// AUTOMATION KEY like its siblings, so it is a const and must stay stable across releases. Classified as a
    /// count metric by <c>AlertMetricClassifier</c> (the value is the number of unhealthy rules).</summary>
    internal const string CustomRuleHealthMetric = "Custom Alert Rules Unhealthy";

    /// <summary>The resolution title recorded when every custom rule is healthy again. Carries a recognized
    /// resolution suffix ("Recovered") so the shared <c>AlertMetricClassifier.IsResolution</c> styles it green.</summary>
    internal const string CustomRuleHealthResolvedMetric = "Custom Alert Rules Recovered";

    /// <summary>How many unhealthy rules the aggregated alert lists by name before eliding the rest — a pg_*
    /// rename can break many rules at once, and the whole point is ONE bounded alert, not a wall of text.</summary>
    private const int MaxListedUnhealthyRules = 20;

    /* Stale mute-rule edge state (#3306). FLEET-level like custom-rule health (mute rules are a store-wide
       concept, not per-server), so a single fixed sentinel key, and a STANDING condition: active flag +
       cooldown re-fire while any mute rule is still suppressing without a bound, one resolution when none
       is. The rule set is handed in by the worker from the live MuteRuleService cache — this evaluator only
       decides fire/hold/resolve and renders the (sanitized) list. */
    private readonly ConcurrentDictionary<string, bool> _activeStaleMute = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastStaleMuteAlert = new();

    /// <summary>The fixed key for the fleet-level stale-mute edge (not a real server); non-numeric so the
    /// deliverer's #1236 int.TryParse override no-ops on it exactly like <see cref="DiskKey"/>.</summary>
    private const string StaleMuteKey = "mutestale";

    /// <summary>The alert metric name the stale-mute self-alert fires under (#3306). A WEBHOOK AUTOMATION
    /// KEY like its siblings, so it is a const and must stay stable across releases. Classified as a count
    /// metric by <c>AlertMetricClassifier</c> (the value is the number of unbounded rules past the age).</summary>
    internal const string StaleMuteMetric = "Stale Mute Rules";

    /// <summary>The resolution title recorded when no unbounded mute rule is old enough to report any more —
    /// every one of them was deleted, disabled, or given an expiry. Carries a recognized resolution suffix
    /// ("Cleared") so the shared <c>AlertMetricClassifier.IsResolution</c> styles it green.</summary>
    internal const string StaleMuteResolvedMetric = "Stale Mute Rules Cleared";

    /// <summary>
    /// How long a mute rule with NO expiry may be in force before this reports it (#3306).
    ///
    /// <para><b>Derived from the product's own expiry options, not picked.</b> The mute dialog offers
    /// "1 hour", "24 hours", "7 days" and "Never" (<c>ViewerAppSettings.MuteRuleDefaultExpiration</c>), so
    /// the last of those is the longest BOUND an operator could have chosen. A rule that has outlived it, with no
    /// bound at all, has by the product's own standard outlasted every expiry it offered — which is a
    /// different statement from a threshold someone liked the sound of. It also sits clear of the case this
    /// must not report: a mute made deliberately this morning to stop a flood while a fix ships, whose
    /// author is still watching it.</para>
    ///
    /// <para><b>Why not read <c>MuteRuleDefaultExpiration</c> itself.</b> It is a VIEWER app setting — a
    /// per-install UI preference that prefills a dialog — and it is not in <c>config_alert_settings</c>, so
    /// the headless service does not have it and could not honor a change to it. A compile-time constant
    /// rather than a store-backed knob because a knob needs a migration rung, and it belongs in the control
    /// plane the next time one is going in anyway. The Retention Held tiers are the worked example of that
    /// step being taken (#3297, V119); this one is still outstanding.</para>
    /// </summary>
    internal static readonly TimeSpan StaleMuteAge = TimeSpan.FromDays(7);

    /// <summary>
    /// How long this condition waits before re-stating itself while a stale rule is still there — its OWN
    /// interval, and the only condition in this class that does not re-fire on the shared alert cooldown.
    ///
    /// <para><b>Because the fact changes on a scale of DAYS.</b> Every sibling re-fires on
    /// <c>IAlertEngineSettings.CooldownMinutes</c> (shipped default 5, clamped to at most 120), which is a
    /// reasonable standing reminder for an episodic condition. This one's subject is a rule's CREATION DATE
    /// measured against a seven-day bound: it is identical on every sweep and changes only when an operator
    /// edits a rule. On the shipped defaults the shared cooldown gives a history row every five minutes and
    /// a notification every fifteen, forever, about that — which is the channel flood a permanent mute rule
    /// is usually created to prevent, arriving from the thing that reports the mute.</para>
    ///
    /// <para><b>It stayed daily when the mute arrived (#3348), on reasoning that never rested on being
    /// unsuppressible.</b> The interval was first chosen while this alert could not be silenced at all, so
    /// the obvious reading is that an off switch makes the shared cooldown safe again. It does not, for two
    /// reasons independent of suppressibility. The timescale argument above is one. The other is that a
    /// five-minute cadence would make the explicit mute the only survivable configuration: the single way to
    /// quiet it would be to mute it permanently, so the cadence would manufacture exactly the blind spot the
    /// condition exists to report. Daily keeps the alert livable WITHOUT the mute, which is what leaves the
    /// mute a real choice rather than a forced one.</para>
    ///
    /// <para>Daily: unmistakable as a standing reminder, and bounded at one a day. It cannot be configured
    /// for the <see cref="StaleMuteAge"/> reason — a knob needs a migration rung this change is not taking.
    /// The alert cooldown's own ceiling is two hours, so this dominates it under every setting rather than
    /// only under the default, and there is no configuration in which the two disagree about which wins.</para>
    /// </summary>
    internal static readonly TimeSpan StaleMuteRefire = TimeSpan.FromDays(1);

    /// <summary>How many stale rules the aggregated alert lists before eliding the rest, on
    /// <see cref="MaxListedUnhealthyRules"/>' reasoning — one bounded alert, not a wall of text.</summary>
    private const int MaxListedStaleMuteRules = 20;

    /* -------- web dashboard TLS certificate expiry (#3514) -------- */

    /// <summary>The fixed fleet-level key for the web-dashboard TLS certificate expiry edge (not a real
    /// server); non-numeric so the deliverer's #1236 int.TryParse no-ops on it, like <see cref="StaleMuteKey"/>.</summary>
    private const string WebTlsCertKey = "webtlscert";

    private readonly ConcurrentDictionary<string, bool> _activeWebTlsCert = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastWebTlsCertAlert = new();

    /// <summary>The metric the web-dashboard TLS certificate expiry self-alert fires under (#3514). A WEBHOOK
    /// AUTOMATION KEY like its siblings — a const, stable across releases. State-only in the numeric columns:
    /// an expiry is a DATE, an identity rather than a quantity (the <see cref="StoreUpgradeMetric"/> precedent),
    /// so the human-readable expiry rides the text and no measurement column turns negative the day it matters.</summary>
    internal const string WebTlsCertExpiryMetric = "Web TLS Certificate Expiring";

    /// <summary>The resolution title recorded when the served certificate is healthy again — renewed past the
    /// warning window, or TLS no longer configured. Carries a recognized resolution suffix ("Renewed") so
    /// <c>AlertMetricClassifier.IsResolution</c> styles it green.</summary>
    internal const string WebTlsCertRenewedMetric = "Web TLS Certificate Renewed";

    /// <summary>How long before expiry this begins to warn — the SAME window the web host's startup log uses
    /// (<see cref="Hosting.DarlingWebTls.ExpiryWarningDays"/>), so the two surfaces agree to the day and a
    /// reader who saw the startup line sees the same threshold here.</summary>
    internal static readonly TimeSpan WebTlsCertWarnWindow = TimeSpan.FromDays(Hosting.DarlingWebTls.ExpiryWarningDays);

    /// <summary>How long this condition waits before re-stating itself while the certificate is still inside
    /// the warning window — its OWN daily interval, for the <see cref="StaleMuteRefire"/> reason: the fact is a
    /// fixed expiry date measured against the clock, identical every sweep, so the shared 5-to-120-minute
    /// cooldown would flood the channel about a date that changes only when the operator renews. Daily
    /// dominates the cooldown's two-hour ceiling under every setting.</summary>
    internal static readonly TimeSpan WebTlsCertRefire = TimeSpan.FromDays(1);

    /// <summary>Length cap for one stale rule's operator-authored reason in the alert detail. Generous
    /// enough to carry a real sentence, bounded so <see cref="MaxListedStaleMuteRules"/> lines cannot grow
    /// the body without limit.</summary>
    private const int MaxStaleMuteReasonLength = 160;

    /// <summary>Length cap for one stale rule's rendered <c>MuteRule.Summary</c>. Also operator-authored in
    /// part — its pattern fields are free text — so it is sanitized and capped like the reason.</summary>
    private const int MaxStaleMuteSummaryLength = 160;

    /* Compression-job self-heal edge state (#1581). FLEET-level like disk pressure (one shared store), but
       MULTI-keyed by job_id (a store has many compression policy jobs). The state is the re-arm-once/escalate
       machine: ReArmed = detected + re-armed once this episode (a later still-stuck reading is a RE-HANG);
       Escalated = re-hung after self-heal, or the re-arm itself failed — a human signal, so the service STOPS
       re-arming and only re-fires the CRITICAL on cooldown. An entry is dropped (and a resolution row written)
       when the job stops being stuck. Keyed by the CompressionKeyPrefix + job_id so the alert serverKey never
       collides with a real server_id (an int hash) — the deliverer's #1236 int.TryParse override no-ops on it,
       exactly like the non-numeric DiskKey. */
    /* AwaitingSchedulerRetry (#3591): a -infinity row on a TimescaleDB whose scheduler recovers it by itself
       (StuckPolicyJob.SchedulerRetries) — seen once, not re-armed, not paged; a second consecutive
       sighting escalates. The other two states are #1581's. */
    /* #3816: the state is now keyed by job_id across EVERY policy family, not only compression, and each
       entry remembers which family it fired under — see PolicyJobEpisode for why the recovery edge needs
       that. One dictionary rather than one per family because a job_id is unique within a store. */
    private enum PolicyJobHealth { ReArmed, Escalated, AwaitingSchedulerRetry }
    private readonly ConcurrentDictionary<string, PolicyJobEpisode> _policyJobState = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTime> _lastPolicyJobAlert = new(StringComparer.Ordinal);

    /// <summary>The alert metric name every compression-job self-alert fires under (first-detection + escalation
    /// re-fires share it, so the deliverer's per-metric cooldown and the recovery resolution correlate cleanly;
    /// escalation is distinguished by the message, not a second metric name).</summary>
    internal const string CompressionJobMetric = "Compression Job Stuck";

    /// <summary>Prefixes the fleet-level compression-job alert serverKey so it never parses as a server_id.</summary>
    private const string CompressionKeyPrefix = "compressjob:";

    /// <summary>
    /// #3816: the metric name a dead or hung CONTINUOUS-AGGREGATE REFRESH policy fires under.
    ///
    /// <para><b>A new name beside <see cref="CompressionJobMetric"/> rather than a widening of it.</b> A
    /// metric name is a PERSISTED IDENTITY: it is the string in every <c>config_alert_log</c> row, the string
    /// a mute rule matches, the key a notification route resolves, and the key the triage page's read map is
    /// built on. Renaming "Compression Job Stuck" to something family-neutral would orphan every existing
    /// history row and silently break every mute rule written against it on a deployed store, so the
    /// compression family keeps its exact string and its exact key prefix. And a REFRESH job's page must not
    /// arrive under a name that says compression: an operator who mutes one condition would have muted the
    /// other, and the two have different urgency, different remedies and different blast radius.</para>
    ///
    /// <para><b>One tier per name is the second reason to split.</b> This one is always Critical and
    /// <see cref="RetentionJobStuckMetric"/> is always Warning, so each name's arm in
    /// <c>AlertSeverity.ForMetric</c> is a faithful replay colour for every row it will ever style — the
    /// #3635 defect (a history row wearing the colour its NAME implies rather than the tier it fired at) is
    /// unreachable here by construction, where one mixed-tier name would have walked into it.</para>
    /// </summary>
    internal const string RefreshJobStuckMetric = "Refresh Job Stuck";

    /// <summary>#3816: the resolution title when a refresh job runs on schedule again. Carries a recognized
    /// resolution suffix ("Recovered") so <c>AlertMetricClassifier.IsResolution</c> styles it green, exactly
    /// like "Compression Job Recovered".</summary>
    internal const string RefreshJobRecoveredMetric = "Refresh Job Recovered";

    /// <summary>Prefixes the refresh-job alert serverKey so it never parses as a server_id — and so a job id
    /// that was a refresh policy on one deployment and a compression policy on another cannot inherit the
    /// other family's open alert row.</summary>
    private const string RefreshKeyPrefix = "refreshjob:";

    /// <summary>#3816: the metric name a dead or hung RETENTION policy fires under. See
    /// <see cref="RefreshJobStuckMetric"/> for why this is an addition rather than a widening, and the
    /// evaluator's family table for why this one is the Warning of the three.</summary>
    internal const string RetentionJobStuckMetric = "Retention Job Stuck";

    /// <summary>#3816: the resolution title when a retention job runs on schedule again.</summary>
    internal const string RetentionJobRecoveredMetric = "Retention Job Recovered";

    /// <summary>Prefixes the retention-job alert serverKey. Deliberately NOT
    /// <c>RetentionHoldKeyPrefix</c> ("retentionhold:"): a dead retention job and a HELD one are different
    /// conditions with different remedies — the hold is the coverage gate working and clears with a backfill,
    /// this is the scheduler having abandoned an ARMED policy — and sharing a key would let one resolve the
    /// other's alert row.</summary>
    private const string RetentionKeyPrefix = "retentionjob:";

    /* #3816: the total_failures arm's baselines. The DELTA is what this arm judges, and nothing in the
       product persisted a per-pass copy of these counters where the evaluator could reach it, so the
       previous pass's value is held in memory here — one long per policy job, on an hourly cadence. The
       first pass after a service start therefore establishes a BASELINE and cannot fire: a non-measurement
       neither fires nor resolves (the checkpointer arm's NoPrevious discipline). The alternative — reading
       the previous collect.store_metrics sample back out — is a second store read per hour to re-learn a
       number this process had in its hand an hour ago, and it would still be a non-measurement on the first
       pass after a restart. */
    private readonly ConcurrentDictionary<string, long> _policyJobFailureBaseline = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTime> _lastPolicyJobFailureAlert = new(StringComparer.Ordinal);

    /// <summary>
    /// #3816: the metric name the <c>total_failures</c> arm fires under — a policy job that FAILED since the
    /// previous hourly sample and whose last run reports <c>Failed</c>.
    ///
    /// <para><b>A third name, because it is a third condition and not a tier of the other two.</b> A job that
    /// fails and retries is alive: the scheduler is running it, <c>next_start</c> is finite, and the stuck
    /// arms are correctly silent. It is also invisible everywhere else — #2136's cadence read filters to
    /// <c>last_run_status = 'Success'</c> (the check gets quieter as the job gets sicker, as the issue puts
    /// it), #2813 judges a PAUSED policy, and <c>total_failures</c> has ridden the V56 telemetry since then
    /// with nothing alerting on it. Fired at INFORMATION (no severity override, the declared INFO arm in
    /// <c>AlertSeverity.ForMetric</c> — the #3443/#3783 idiom), because the useful response is to read the
    /// PostgreSQL log, not to wake anybody: a failing job that then stops being run at all arrives as one of
    /// the two Critical/Warning names above.</para>
    /// </summary>
    internal const string PolicyJobFailingMetric = "Store Job Failing";

    /// <summary>Prefixes the failure-arm alert serverKey.</summary>
    private const string PolicyJobFailingKeyPrefix = "jobfailing:";

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

    /* Retention Held edge state (#2813). FLEET-level, MULTI-keyed by retention job_id, STANDING like the
       cadence condition: active flag + cooldown re-fire while the gate keeps a policy paused past its
       horizon, one "Retention Hold Cleared" resolution when it arms or comes back under. */
    private readonly ConcurrentDictionary<string, bool> _activeRetentionHold = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTime> _lastRetentionHoldAlert = new(StringComparer.Ordinal);

    /// <summary>The #2813 alert metric name — Warning and Critical share it (severity carries the tier), so
    /// the deliverer's per-metric cooldown and the resolution correlate cleanly.</summary>
    internal const string RetentionHoldMetric = "Retention Held";

    /// <summary>The resolution title <see cref="RetentionHoldMetric"/> clears with. A constant (rather than
    /// the inline literal it was) because the triage endpoint's <c>ResolutionAliases</c> folds this title onto
    /// its firing metric (#3833), and a string that exists in two files with no shared symbol is how the four
    /// store families after #2768 drifted out of that fold in the first place.</summary>
    internal const string RetentionHoldClearedMetric = "Retention Hold Cleared";

    /// <summary>Prefixes the fleet-level retention-hold alert serverKey so it never parses as a server_id.</summary>
    private const string RetentionHoldKeyPrefix = "retentionhold:";

    /// <summary>
    /// #2813 WARNING tier, SHIPPED DEFAULT — how many times its own configured horizon a HELD tier must be
    /// holding before the hold has cost enough to say so.
    ///
    /// <para>#3297 moved the live value into <c>config_alert_settings.retention_hold_warn_ratio</c> (V119),
    /// so this is the seed and the unsupplied-seam fallback, not what the check reads: the decision reads
    /// <see cref="_retentionHoldWarnRatio"/>. Field-reported on #3296 — the operator received an hourly
    /// CRITICAL and found nothing in Settings matching "Retention Held" or "Monitor Store".</para>
    ///
    /// <para>Taken from <see cref="TimescaleSupport.RetentionHoldWarnRatioDefault"/> rather than restated,
    /// where the measurement that bounds it on both sides lives — including the correction that healthy
    /// chunk granularity reaches <b>1.4x</b> in production, not the ~1.25x the arithmetic predicts.</para>
    /// </summary>
    internal const double RetentionHoldWarnRatio = TimescaleSupport.RetentionHoldWarnRatioDefault;

    /// <summary>#2813 CRITICAL tier, SHIPPED DEFAULT: double the warning ratio, and store-backed since V119
    /// for its sibling's reason. The live value is <see cref="_retentionHoldCriticalRatio"/>.</summary>
    internal const double RetentionHoldCriticalRatio = TimescaleSupport.RetentionHoldCriticalRatioDefault;

    /// <summary>#3297: the Retention Held WARNING tier, read live through the same by-reference settings seam
    /// as the AG thresholds and the #2136 cadence knob (the clamp lives on <c>DarlingAlertSettings</c>).
    /// Every retention-hold decision AND every threshold this check states back to the operator goes through
    /// these two seams — a bare <see cref="RetentionHoldWarnRatio"/> in the fire path would judge on the
    /// shipped default while <c>get_alert_settings</c> reported the store's, and a bare one in the message
    /// would name a threshold the engine is not using.</summary>
    private readonly Func<double> _retentionHoldWarnRatio;

    /// <summary>#3297: the Retention Held CRITICAL tier, read live like its warning sibling.</summary>
    private readonly Func<double> _retentionHoldCriticalRatio;

    /* Raw Purge Over Horizon edge state (#4299). FLEET-level, MULTI-keyed by raw job_id, STANDING
       like Retention Held: active flag + cooldown re-fire while a raw relation stays over-horizon with a
       last-recorded outcome that is not "ran", one "Raw Purge Over Horizon Cleared" resolution when a later
       record says "ran" and the ratio is back under. A SEPARATE metric from Retention Held so its standing
       state does not collide with the generic hold's — a raw relation reads BOTH conditions' readings from
       the same RetentionHoldReading.OverHorizonRatio, but this one fires on the RECORDED REASON the trigger
       did not run, not on the armed flag alone (darling_armed=true does not suppress it: it fires WHATEVER
       the recorded verdict says). */
    private readonly ConcurrentDictionary<string, bool> _activeRawPurgeOverHorizon = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTime> _lastRawPurgeOverHorizonAlert = new(StringComparer.Ordinal);

    /// <summary>The #4299 alert metric name — deliberately distinct from <see cref="RetentionHoldMetric"/>
    /// so the two conditions' standing state and cooldowns never collide.</summary>
    internal const string RawPurgeOverHorizonMetric = "Raw Purge Over Horizon";

    /// <summary>The resolution title <see cref="RawPurgeOverHorizonMetric"/> clears with.</summary>
    internal const string RawPurgeOverHorizonClearedMetric = "Raw Purge Over Horizon Cleared";

    /// <summary>Prefixes the fleet-level raw-purge-over-horizon alert serverKey so it never parses as a server_id.</summary>
    private const string RawPurgeOverHorizonKeyPrefix = "rawpurgehorizon:";

    /* -------- the store's own TOAST slack and checkpointer (#3783) -------- */

    /* Store TOAST Slack edge state (#3783). FLEET-level (the dimensions are the store's own tables), MULTI-keyed
       by dimension table name (two today: query_text_dim, query_plan_dim), STANDING like Retention Held:
       active flag + a DAILY re-fire while a dimension's TOAST file stays under-utilised past the floor, one
       "Store TOAST Slack Cleared" resolution when the reclaim lands (or the file drops under the floor). */
    private readonly ConcurrentDictionary<string, bool> _activeToastSlack = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTime> _lastToastSlackAlert = new(StringComparer.Ordinal);

    /// <summary>The #3783 TOAST-slack metric name. A WEBHOOK AUTOMATION KEY like its siblings — a const, stable
    /// across releases. Fired with NO severity override, so <c>AlertSeverity.ForMetric</c>'s declared INFO arm
    /// styles it: the condition is a report about disk the operator may choose to reclaim in a maintenance
    /// window, not a condition the on-call must act on tonight, and INFO is the one tier the rendering layer
    /// has for that (the Collector Cost Digest reasoning). Classified as a percent by the value it carries.</summary>
    internal const string ToastSlackMetric = "Store TOAST Slack";

    /// <summary>The resolution title when a dimension's TOAST utilisation is back over the bar or its file is
    /// under the floor. "Cleared" so <c>AlertMetricClassifier.IsResolution</c> styles it green (the Retention
    /// Hold Cleared spelling).</summary>
    internal const string ToastSlackClearedMetric = "Store TOAST Slack Cleared";

    /// <summary>Prefixes the fleet-level TOAST-slack alert serverKey so it never parses as a server_id.</summary>
    private const string ToastSlackKeyPrefix = "toastslack:";

    /// <summary>
    /// Under this utilisation a dimension's TOAST file is SLACK (#3783): 50 %. The measured case was 40 %
    /// (154 GB holding ~61 GB of live chunks on one production store class, ~93 GB of slack left by the V54
    /// text→gz conversion plus ~755 k rows a day cycling through row-capped deletes); its near-twin on the same
    /// build sat at ~64 % and was NOT the finding. Half is the line between "a file with the ordinary breathing
    /// room a churning table keeps" and "a file more empty than full", and it is also the point at which a
    /// VACUUM FULL rebuild — which copies the LIVE data into a fresh file — costs less disk than the slack it
    /// returns, so the reclaim pays for its own working space.
    ///
    /// <para><b>Why a constant and not a <c>config_alert_settings</c> knob.</b> A knob needs a migration rung,
    /// and the rule is one un-landed rung at a time; V137 is the rung this condition rides on and it added
    /// the columns, not a threshold. The Retention Held tiers are the worked example of the knob being added
    /// later (#3297, V119) once a store had a reason to tune it; this pair follows the same road when one
    /// does. Paired with <see cref="ToastSlackFileFloorBytes"/> so a small dimension is never a finding.</para>
    /// </summary>
    internal const double ToastSlackUtilisationBarPercent = 50.0;

    /// <summary>
    /// The file-size floor under which slack is not reported however low the utilisation (#3783): 10 GiB — the
    /// issue's "10 GB", in the binary unit the tool's byte prose uses. A 2 GB dimension at 30 % is 1.4 GB of
    /// slack, which is real and not worth an ACCESS EXCLUSIVE lock on the store's plan dimension to recover;
    /// the finding this exists for was ~93 GB. Ten is well under the smallest production plan dimension seen
    /// (67 GB) and well over any store a maintenance-window reclaim would not be worth scheduling for. Not a
    /// knob, for <see cref="ToastSlackUtilisationBarPercent"/>'s reason.
    /// </summary>
    internal const long ToastSlackFileFloorBytes = 10L << 30;

    /// <summary>
    /// How long the slack condition waits before re-stating itself while a file stays slack — its OWN daily
    /// interval, the <see cref="StaleMuteRefire"/> reasoning exactly: the fact is a file's utilisation, which
    /// moves on a scale of DAYS (slack accumulates with the delete cycle and is returned only by a VACUUM FULL
    /// the operator schedules), so the shared 5-to-120-minute cooldown would say the same sentence about the
    /// same file every sweep. Daily keeps an INFO reminder livable without a mute, which is what leaves the
    /// mute a real choice; it dominates the cooldown's two-hour ceiling under every setting.
    /// </summary>
    internal static readonly TimeSpan ToastSlackRefire = TimeSpan.FromDays(1);

    /* Store Checkpointer Pressure edge state (#3783). FLEET-level (one store, one checkpointer), a single fixed
       key, STANDING like Store Job Over Cadence: active flag + shared-cooldown re-fire while each new hourly
       interval keeps breaching, one "Store Checkpointer Pressure Recovered" resolution when an interval comes
       back clean. The shared cooldown rather than a daily interval, deliberately: every breaching hour is a NEW
       hour of fsync storms the store's readers sat inside, not a restatement of a standing fact. */
    private readonly ConcurrentDictionary<string, bool> _activeCheckpointerPressure = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTime> _lastCheckpointerPressureAlert = new(StringComparer.Ordinal);

    /// <summary>The #3783 checkpointer metric name. A WEBHOOK AUTOMATION KEY like its siblings. Fired with NO
    /// severity override so the declared INFO arm styles it, for <see cref="ToastSlackMetric"/>'s reason: the
    /// two levers are configuration the maintainer weighs, not a page. The value it carries is the interval's
    /// average sync milliseconds PER CHECKPOINT (#4037), not the interval's summed sync milliseconds.</summary>
    internal const string CheckpointerPressureMetric = "Store Checkpointer Pressure";

    /// <summary>The resolution title when an interval reads clean again. "Recovered" for the classifier.</summary>
    internal const string CheckpointerPressureRecoveredMetric = "Store Checkpointer Pressure Recovered";

    /// <summary>The fixed key for the fleet-level checkpointer edge (not a real server); non-numeric so the
    /// deliverer's #1236 int.TryParse override no-ops on it exactly like <see cref="DiskKey"/>.</summary>
    private const string CheckpointerKey = "checkpointer";

    /// <summary>
    /// Milliseconds PER CHECKPOINT (#4037; originally #3783) past which the checkpointer's sync (fsync) phase
    /// is PRESSURE: 10,000. The MCP host's read deadline is the bound this is derived from — a checkpoint
    /// whose fsync phase runs past ten seconds is one whose I/O stall can outlast a read the deadline kills,
    /// which is exactly what happened: three unattributed read kills on a production store in one day all sat
    /// inside sync phases of 25.2 s and 14.0 s. Judged on <c>SyncMs / (timed + requested)</c> — the interval's
    /// AVERAGE sync milliseconds per checkpoint — rather than the interval's summed sync milliseconds: the
    /// series stores the cumulative counter and an hourly interval on the default five-minute
    /// <c>checkpoint_timeout</c> covers about twelve timed checkpoints, so judging the sum against a
    /// per-checkpoint bar breached on a healthy store whose checkpoints each synced five to eight seconds and
    /// never recovered (#4037's own measured population). The second arm, <c>checkpoints_requested &gt; 0</c>,
    /// has no threshold to tune: one WAL-forced checkpoint in an hour says the store outran <c>max_wal_size</c>.
    /// Both arms are judged on an interval with no postmaster restart inside it: across one, the shutdown
    /// checkpoint is in the requested count and in the phase times alike, so neither is judged (#3955); and the
    /// average arm needs a TIMED count on both samples of the pair (V140), so a row from before that rung
    /// leaves the average unmeasured rather than falling back to the old sum.
    /// Not a knob, for <see cref="ToastSlackUtilisationBarPercent"/>'s reason.
    /// </summary>
    internal const long CheckpointSyncBarMs = 10_000;

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
        Func<int>? storeJobCadenceWarnPercent = null,
        Func<double>? retentionHoldWarnRatio = null,
        Func<double>? retentionHoldCriticalRatio = null,
        AlertReadFailureCounter? readFailures = null,
        string? storeName = null,
        ISelfAlertDeliveryStampStore? deliveryStamps = null,
        Func<TimeSpan, CancellationToken, Task>? retryDelay = null)
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
        /* Unsupplied falls back to the shipped default, so an evaluator built without the seam behaves
           like a store at its shipped defaults (the AG-seam discipline). Taken from the constant rather
           than restated (#3060): a literal here is a third copy of the same number that a moved refresh
           grid would leave behind, and this one would fire past the slot silently. */
        _storeJobCadenceWarnPercent =
            storeJobCadenceWarnPercent ?? (() => TimescaleSupport.RefreshSlotPercentOfHourlyCadence);
        /* #3297: unsupplied falls back to the V119 column defaults, so an evaluator built without the seams
           behaves like a store at its shipped defaults — the AG-seam discipline, and taken from the shared
           constants rather than restated for the #3060 reason the cadence fallback above gives. */
        _retentionHoldWarnRatio =
            retentionHoldWarnRatio ?? (() => TimescaleSupport.RetentionHoldWarnRatioDefault);
        _retentionHoldCriticalRatio =
            retentionHoldCriticalRatio ?? (() => TimescaleSupport.RetentionHoldCriticalRatioDefault);
        _readFailures = readFailures;
        /* #3500: unsupplied (or blank) falls back to the shipped constant, so an evaluator built without the
           seam behaves like a store that never opted in — the AG-seam discipline, and the byte-identical
           promise the opt-in stands on. */
        _storeLabel = EffectiveStoreLabel(storeName);
        /* #3580: unsupplied means the two daily documents gate on process memory alone — the pre-#3580
           behavior, and what every test harness that does not care about restarts gets. Production
           passes the store-backed stamps. */
        _deliveryStamps = deliveryStamps;
        /* #3854: the retry pause, injectable for the reason the adapter's is — a pin asserts the seam
           waited AlertPassRetryDelaySeconds as a VALUE rather than by spending two real seconds. Production
           gets Task.Delay, which is what the adapter defaults to as well. */
        _retryDelay = retryDelay ?? Task.Delay;
    }

    /// <summary>
    /// The pause between a retried store read's two attempts (#3854), injectable so the seam's pins cost no
    /// wall-clock time. See <see cref="DarlingAlertReadAdapter.ExecuteWithOneRetryAsync{T}(Func{CancellationToken, Task{T}}, string, string, AlertReadFailureCounter, Func{TimeSpan, CancellationToken, Task}, CancellationToken)"/>.
    /// </summary>
    private readonly Func<TimeSpan, CancellationToken, Task> _retryDelay;

    /// <summary>
    /// Runs one of this type's store reads through the alert pass's SHARED retry seam (#3854): on a command
    /// timeout and nothing else, the read is re-asked once, <see cref="DarlingAlertReadAdapter.AlertPassRetryDelaySeconds"/>
    /// later, and the retry is tallied on this evaluator's own <see cref="_readFailures"/>.
    ///
    /// <para><b>The seam is the adapter's, deliberately — one truth, not two.</b> #3848 gave the twelve reads
    /// on <see cref="DarlingAlertReadAdapter"/> a retry; the seven reads this alert pass issues outside that
    /// type — the six below and the worker's latest-CPU read — run on the SAME
    /// <see cref="DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds"/> deadline and are counted on the
    /// same <see cref="AlertReadFailureCounter"/>, so they want the same retry rather than their own. A
    /// second seam here would be a second copy of one decision, free to disagree with the first the moment
    /// either grew an arm — the argument #3848 already makes against fourteen copies inside one file,
    /// applied one scope out. So this forwards to the adapter's static overload and adds nothing but this
    /// type's counter and pause.</para>
    ///
    /// <para><b>What the shared discrimination deliberately excludes</b>, and why these reads want exactly
    /// that: <see cref="DarlingAlertReadAdapter.IsCommandTimeout"/> is true for a
    /// <see cref="TimeoutException"/> anywhere in the chain and for NOTHING else. It is false for a
    /// <see cref="Npgsql.PostgresException"/> at every level — the store's own <c>statement_timeout</c> at
    /// SQLSTATE <c>57014</c> is the backend ANSWERING, and an identical second attempt buys an identical
    /// answer — false for a cancellation through the pass token, because a retry that outlives an orderly
    /// stop while holding a fleet-sweep permit is guaranteed useless, and false for a reset socket or a torn
    /// stream, which are connection faults rather than a read taking too long and which this pass's count
    /// should keep saying out loud. The population the retry is FOR is this store's own write bands: a
    /// Collection Signals or Agent Status read that crosses ten seconds inside a checkpoint fsync tail or a
    /// compression band, which used to skip one cycle unretried.</para>
    ///
    /// <para>Every read below is a thin non-async forwarder over a private <c>…CoreAsync</c> sibling holding
    /// its body verbatim, the shape <c>EveryStoreReadOnTheEvaluator_GoesOutThroughTheSeam</c> derives from
    /// source — so an eighth read written in the old shape reds on the day it is written rather than
    /// shipping unretried and silently blind.</para>
    /// </summary>
    /// <param name="serverId">
    /// The server whose bucket a retry lands in, keyed exactly as <see cref="Key(int)"/> spells it for this
    /// type's failure counts, so one read cannot carry two spellings across the two figures.
    /// </param>
    private Task<T> ReadWithOneRetryAsync<T>(
        Func<CancellationToken, Task<T>> read, int serverId, string readName, CancellationToken passToken)
        => DarlingAlertReadAdapter.ExecuteWithOneRetryAsync(
            read, Key(serverId), readName, _readFailures, _retryDelay, passToken);

    /// <summary>
    /// Where the two daily documents' DELIVERED-TODAY stamps live across restarts (#3580), or null when the
    /// process-memory gate is the only gate. See <see cref="CollectorCostDigestInterval"/> for the arc.
    /// </summary>
    private readonly ISelfAlertDeliveryStampStore? _deliveryStamps;

    /// <summary>
    /// Where a SWALLOWED self-alert store read is counted (#3013), or null when nothing is counting.
    /// Only the conditions that READ the store here increment it; the fleet-scoped conditions are handed
    /// their evidence as parameters, so their reads are counted at their own sites in
    /// <c>DarlingWorker</c> — see the exemption notes at each catch.
    /// </summary>
    private readonly AlertReadFailureCounter? _readFailures;

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

        /* #3013: this server's second alert pass of the sweep, counted after the master-switch return so a
           pass that never looked at the store is not in the denominator. */
        _readFailures?.RecordPass(Key(serverId));

        /* Only judge collection-stopped once the service has actually collected from this server this run
           (see _hasBeenOnline) — otherwise pre-restart / pre-re-add stale rows would false-alarm before the
           first fresh collection lands. */
        if (_hasBeenOnline.ContainsKey(Key(serverId)))
        {
            var collectionReadClock = Stopwatch.StartNew();
            try
            {
                /* #2107: store-backed window/threshold (clamped on read); the constants remain
                   only as the shipped defaults. */
                var (lastSuccess, recentRuns, recentSuccess) =
                    await ReadCollectionSignalsAsync(postgres, serverId, _settings.CollectionFailureThreshold, cancellationToken);
                collectionReadClock.Restart();
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
                _logger?.LogError("[{Server}] Collection-health self-alert failed after {ElapsedMs} ms: {Message}", serverName, collectionReadClock.ElapsedMilliseconds, ex.Message);
                _readFailures?.RecordReadFailure(Key(serverId), CollectionSignalsReadName, collectionReadClock.ElapsedMilliseconds);
            }
        }

        if (!connected)
        {
            return;
        }

        var captureReadClock = Stopwatch.StartNew();
        try
        {
            var missing = await ReadMissingCaptureSessionsAsync(postgres, serverId, cancellationToken);
            captureReadClock.Restart();
            await ApplyCaptureDownAsync(serverId, serverName, missing, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("[{Server}] Capture-down self-alert failed after {ElapsedMs} ms: {Message}", serverName, captureReadClock.ElapsedMilliseconds, ex.Message);
            _readFailures?.RecordReadFailure(Key(serverId), MissingCaptureSessionsReadName, captureReadClock.ElapsedMilliseconds);
        }

        var agentReadClock = Stopwatch.StartNew();
        try
        {
            /* Agent Not Running (#1433 Phase 2): the collected agent_status snapshot says the target's SQL
               Agent service is stopped. Only a FRESH reading judges — a stale row (collection lagging) yields
               null, so the collection-stopped alert owns staleness and this never false-alarms on old data. */
            var (agentCollectionTimeUtc, agentRunning) = await ReadLatestAgentStatusAsync(postgres, serverId, cancellationToken);
            agentReadClock.Restart();
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
                agentReadClock.Restart();
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
            _logger?.LogError("[{Server}] Agent-not-running self-alert failed after {ElapsedMs} ms: {Message}", serverName, agentReadClock.ElapsedMilliseconds, ex.Message);
            _readFailures?.RecordReadFailure(Key(serverId), AgentStatusReadName, agentReadClock.ElapsedMilliseconds);
        }

        /* Availability Group health (#991). Skipped entirely when the master AG switch is off, so a fleet that
           runs no AGs — the common case — pays NOTHING for this on the per-server sweep: the read below is the
           only work, and it does not happen. Like agent_status, only a FRESH snapshot judges; a stale one (the
           collector lagging, or an AG that stopped existing so the collector now writes zero rows) yields no
           signal, and the collection-stopped alert owns staleness. */
        if (_notifyAgHealth())
        {
            var agReadClock = Stopwatch.StartNew();
            try
            {
                /* Each grain is gated on its OWN snapshot time: the two AG collectors are scheduled
                   independently, so one being disabled or broken must not let the other's fresh timestamp
                   vouch for its stale rows. */
                var (replicaTimeUtc, replicas) =
                    await ReadLatestAgReplicaStatesAsync(postgres, serverId, cancellationToken);
                agReadClock.Restart();
                if (IsFresh(replicaTimeUtc))
                {
                    await ApplyAgReplicaHealthAsync(serverId, serverName, replicas, cancellationToken);
                    agReadClock.Restart();
                }

                var (databaseTimeUtc, databases) =
                    await ReadLatestAgDatabaseReplicaStatesAsync(postgres, serverId, cancellationToken);
                agReadClock.Restart();
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
                _logger?.LogError("[{Server}] Availability-Group self-alert failed after {ElapsedMs} ms: {Message}", serverName, agReadClock.ElapsedMilliseconds, ex.Message);
                _readFailures?.RecordReadFailure(Key(serverId), AgStateReadName, agReadClock.ElapsedMilliseconds);
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
    /// day's query time exceeds their own baseline (see the thresholds above), fires once per pair
    /// on entry, re-fires on the cooldown while it stays regressed, and resolves the moment it drops back.
    ///
    /// <para>"ON the monitored servers" is the series' intent and not always what it measures (#3192): the
    /// figure rolls up the driver's SQL slice, which on the enumerated path contains the store's own
    /// plan/text probe and write-back. So a <c>query_store</c> regression here can be the STORE getting
    /// slower rather than the target, and the fired alert says so — see
    /// <see cref="CostIsNotAllTargetSide"/>, which exists because this doc and that text have to agree.</para>
    /// Called once per cycle from the worker's hourly store-metrics tick, AFTER the flush that writes the
    /// latest hour. Gated on the master alerts switch before the first store read (#3464). Testable
    /// directly with a recording deliverer + a controllable clock.
    /// </summary>
    public async Task EvaluateCollectorCostAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        /* #3464: the master-switch gate, up front and before the first store read — the shape
           EvaluateStoreAlertsAsync established ("mirrors the engine's early return"). This method had NO
           consult ahead of the paging apply: the only AlertsEnabled on this path guarded the digest branch
           below, which runs AFTER ApplyCostRegressionsAsync, so the one self-alert family that pages
           fleet-wide was also the one that delivered 60 minutes into a fleet-wide mute while the engine
           sweep's server_alert_passes counters sat frozen. Master-off here means what it means for the
           engine: nothing is read, nothing is evaluated, nothing is recorded, and the fire/resolve state
           freezes where it stands — the regressions are a property of stored rows, so re-enabling resumes
           from the same answer the store would have given all along. */
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        List<Mcp.DarlingCollectorCostReader.CostRegression> regressions;
        var readClock = Stopwatch.StartNew();
        try
        {
            regressions = await Mcp.DarlingCollectorCostReader.GetCostRegressionsAsync(
                postgres, _utcNow() - CostRegressionBaselineWindow,
                CostRegressionBaselineFloorMs, CostRegressionFactor, CostRegressionAddedMsFloor,
                cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A self-alert read that fails must not take the loop down — it is best-effort telemetry. */
            _logger?.LogDebug(ex, "collector-cost regression evaluation failed after {ElapsedMs} ms", readClock.ElapsedMilliseconds);
            _readFailures?.RecordReadFailure(null, "collector-cost regression self-alert", readClock.ElapsedMilliseconds);
            return;
        }

        /* #3443: the fan-out denominator — how many servers each collector actually ran on. This is
           get_collector_cost's OWN ranked read (GetTopAsync), not a private query, so the digest's census
           and the routing denominator are the same numbers the MCP surface serves and cannot disagree with
           it. One aggregate over 24 hours of an hourly table. */
        List<Mcp.DarlingCollectorCostReader.CollectorCostSummaryRow> census;
        var censusClock = Stopwatch.StartNew();
        try
        {
            census = await Mcp.DarlingCollectorCostReader.GetTopAsync(
                postgres, _utcNow() - CollectorCostDenominatorWindow, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Without the denominator there is no routing decision to make, so this tick does NOTHING
               rather than guessing. Falling back to "route everything to the page" would reinstate the
               delivery this change exists to end; falling back to "route nothing" would fire a spurious
               resolution for every pair that was paging. Skipping keeps _activeCostRegression and every
               interval untouched, and the regressions are a property of stored rows rather than of this
               moment — the next tick reads the same answer. */
            _logger?.LogDebug(ex, "collector-cost census read failed after {ElapsedMs} ms", censusClock.ElapsedMilliseconds);
            _readFailures?.RecordReadFailure(null, "collector-cost census self-alert", censusClock.ElapsedMilliseconds);
            return;
        }

        var routing = RouteCostRegressions(regressions, census);
        await ApplyCostRegressionsAsync(routing.Paging, cancellationToken);

        /* The digest's own interval, checked here so 23 of every 24 hourly ticks do no extra store work —
           the delivered-today gate (#3580): memory first, the stamp only when memory cannot answer. The
           master switch already returned above, before the first store read (#3464); AlertsEnabled is
           ALSO checked inside the apply, like every sibling, so a direct caller cannot skip it. */
        if (await DocumentDeliveredInsideIntervalAsync(
                _lastCostDigest, CollectorCostDigestKey, PgSelfAlertDeliveryStampStore.CostDigestStateKey,
                CollectorCostDigestInterval, _utcNow(), "collector-cost digest", cancellationToken))
        {
            return;
        }

        List<Mcp.DarlingCollectorCostReader.CostMover> movers;
        var moverClock = Stopwatch.StartNew();
        try
        {
            movers = await Mcp.DarlingCollectorCostReader.GetCostMoversAsync(
                postgres, _utcNow() - CostRegressionBaselineWindow, CostRegressionAddedMsFloor,
                MaxListedCostMovers, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogDebug(ex, "collector-cost digest read failed after {ElapsedMs} ms", moverClock.ElapsedMilliseconds);
            _readFailures?.RecordReadFailure(null, "collector-cost digest self-alert", moverClock.ElapsedMilliseconds);
            return;
        }

        await ApplyCollectorCostDigestAsync(movers, census, cancellationToken);
    }

    /// <summary>The apply half of <see cref="EvaluateCollectorCostAsync"/>, split out so the fire-once /
    /// re-fire / resolve lifecycle is unit-testable with a recording deliverer and a controllable clock,
    /// with the regression set fabricated rather than read from a store. Gated on the master alerts
    /// switch, like every sibling apply (#3464 — this was the one that was not).</summary>
    internal async Task ApplyCostRegressionsAsync(
        IReadOnlyList<Mcp.DarlingCollectorCostReader.CostRegression> regressions, CancellationToken cancellationToken)
    {
        /* #3464: the consult this apply never had, and the measured half of that issue: with
           alerts_enabled read back false on both SQL Server stores, a Collector Cost Regression reached
           the live paging channel an hour into the mute, because every sibling apply gates here and this
           one delivered through FireAsync with no master consult anywhere between the hourly tick and the
           channel. The gate is the sibling shape — no evaluation, no history rows, cooldown and
           active-edge state frozen — rather than the connection edge's track-always split, because unlike
           a connection edge there is no cross-sweep state machine here to corrupt: the fire/resolve edge
           is rebuilt from stored rows the moment the switch comes back on. */
        if (!_settings.AlertsEnabled)
        {
            return;
        }

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
                var ratio = regression.BaselineMsPerRun > 0
                    ? regression.LatestMsPerRun / regression.BaselineMsPerRun
                    : 0;
                var threshold = regression.ThresholdMsPerRun(CostRegressionFactor);
                await FireAsync(
                    key, regression.ServerName, "Collector Cost Regression",
                    currentValue: $"{regression.LatestMsPerRun:N1} ms/run",
                    /* #3441's rule extended by #3462: the reported threshold is the one that actually selected
                       the row, and since #3462 that is a conjunction in two units — the per-run bound AND the
                       added-cost-per-day floor — so both are stated, or a reader falsifying the arithmetic
                       would reconstruct a looser predicate than the one that fired. */
                    thresholdValue: $"{threshold:N1} ms/run (the greater of its {regression.BaselineMsPerRun:N1} ms/run mean " +
                        $"and its {regression.BaselineP95MsPerRun:N1} ms/run daily p95, x {CostRegressionFactor:N1}), " +
                        $"worth at least {CostRegressionAddedMsFloor / 1000.0:N0} s/day of added collection time",
                    detail: $"The '{regression.CollectorName}' collector's OWN query time on {regression.ServerName} rose to " +
                        $"{regression.LatestMsPerRun:N1} ms per run, {ratio:N1}x its {CostRegressionBaselineWindow.TotalDays:N0}-day " +
                        $"baseline of {regression.BaselineMsPerRun:N1} ms per run ({regression.LatestRuns:N0} runs totalling " +
                        $"{regression.LatestMs:N0} ms so far today, adding {regression.AddedMsPerDay / 1000.0:N1} s of collection " +
                        $"time a day at that volume - a regression is reported only when that added cost reaches " +
                        $"{CostRegressionAddedMsFloor / 1000.0:N0} s/day, the materiality floor both delivery surfaces share, so this " +
                        $"is not a few hundred milliseconds of nothing (#3462)). It also cleared {threshold:N1} ms per run, the factor on the HIGHER of " +
                        $"that mean and the p95 of its OWN daily per-run cost over the window " +
                        $"({regression.BaselineP95MsPerRun:N1} ms) - so this is not the collector's own upper mode on a " +
                        $"normal slow day (#3440). " +
                        $"This is the MONITORING TOOL's own cost, not the " +
                        $"server's workload - each individual run is costing more than it used to. Measured PER RUN (#2846) so " +
                        $"a cadence change cannot read as a cost change. get_collector_cost with " +
                        $"collector_name={regression.CollectorName} shows the trend. {CostIsNotAllTargetSide}",
                    severity: AlertSeverityLevel.Warning,
                    shortMessage: $"{regression.CollectorName} collection cost on {regression.ServerName} is {ratio:N1}x its per-run baseline",
                    numericCurrentValue: regression.LatestMsPerRun,
                    numericThresholdValue: threshold,
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
                    "Cost Regression Cleared",
                    $"{serverName}: {collector} collection cost is no longer rising on a majority of the servers it runs on"),
                    cancellationToken);
            }
        }
    }

    /// <summary>
    /// #3443: which of the regressions the predicate selected reach the PAGING channel, and which are
    /// reported by <see cref="CollectorCostDigestMetric"/> instead. PURE — no clock, no I/O — so the routing
    /// is pinnable without a host, and it decides nothing about detection: every row in
    /// <paramref name="regressions"/> appears in one of the two lists.
    ///
    /// <para><b>Why the channel and not the threshold.</b> This condition has needed two successive gates to
    /// stop it reporting things nobody can act on — #3316's materiality floor and #3440's dispersion bound —
    /// and both are correct fixes to real defects. Neither changes what the surviving message asks a reader
    /// to do. Take the firing that produced #3440 at its best, with #3440 shipped and the ratio genuinely
    /// meaningful: a once-daily collector cost 11.1 extra seconds today, on the monitoring tool's own
    /// overhead, against a 60,000 ms sweep budget. (#3462's recalibrated floor now screens that particular
    /// magnitude before routing ever sees it; the argument stands at any magnitude the floor admits —
    /// 21.8 s/day, the smallest real catch #3316 measured, clears it and still needs nothing before
    /// morning.) Nothing is degraded, no data is lost, no monitored server
    /// is affected, and nothing needs doing before morning. That is a report. The one cost shape that is
    /// both real and urgent is a different quantity — the same collector rising across the fleet at once,
    /// which is a deployment or a store-side change rather than one server's slow day — and that is what
    /// <see cref="CostRegressionFleetWideMinServers"/> and the majority rule select.</para>
    ///
    /// <para><b>The demotion fails toward the REPORT, deliberately, which is the opposite of this
    /// product's usual direction.</b> Delivery decisions elsewhere (#3430's budget) resolve every
    /// uncertainty to "post", because the cost of failing that way is one extra message and the cost of
    /// failing the other way is an unannounced incident. Here an unannounced finding is impossible: a
    /// collector missing from <paramref name="census"/> — so its denominator is unknown — still appears in
    /// the digest, in full, with more context than the alert card carried. #3462's floor on the digest does
    /// not narrow that guarantee: the movers read gates on the SAME constant this predicate does, and the
    /// regression's added cost IS the mover's magnitude, so every row this routing ever receives is material
    /// by construction and stays digest-visible. So the safe direction is the
    /// quiet one, and it is safe only BECAUSE the report exists. If the digest were ever removed this rule
    /// would have to invert.</para>
    ///
    /// <para><b>Nothing is aggregated away on the paging side.</b> A fleet-wide regression still fires
    /// per (server, collector), carrying every figure the predicate computed for that pair, because #1154's
    /// rule is that a never-announced incident must not be swallowed by a key shared with another server and
    /// #3430's budget already folds the RE-tellings into one carrier plus a roster. A page that named the
    /// collector once and summarised the servers would have had to decide which pair's numbers to show.</para>
    /// </summary>
    /// <param name="regressions">Everything the regression predicate selected this tick.</param>
    /// <param name="census">
    /// The fan-out denominator: <c>get_collector_cost</c>'s own ranked read over
    /// <see cref="CollectorCostDenominatorWindow"/>, whose <c>ServerCount</c> is how many servers each
    /// collector ran on. A collector absent from it has no denominator and is report-only.
    /// </param>
    internal static CostRegressionRouting RouteCostRegressions(
        IReadOnlyList<Mcp.DarlingCollectorCostReader.CostRegression> regressions,
        IReadOnlyList<Mcp.DarlingCollectorCostReader.CollectorCostSummaryRow> census)
    {
        var paging = new List<Mcp.DarlingCollectorCostReader.CostRegression>();
        var reportOnly = new List<Mcp.DarlingCollectorCostReader.CostRegression>();

        /* Distinct SERVERS per collector, not row count: the predicate returns one row per
           (server, collector), but counting rows would let a duplicated row inflate a fan-out. */
        var regressedServers = new Dictionary<string, HashSet<int>>(StringComparer.Ordinal);
        foreach (var regression in regressions)
        {
            if (!regressedServers.TryGetValue(regression.CollectorName, out var servers))
            {
                servers = new HashSet<int>();
                regressedServers[regression.CollectorName] = servers;
            }

            servers.Add(regression.ServerId);
        }

        var collectedServers = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var row in census)
        {
            /* Highest wins on a duplicated collector name, so a duplicate cannot shrink a denominator and
               make the majority rule easier to satisfy. */
            if (!collectedServers.TryGetValue(row.CollectorName, out var existing) || row.ServerCount > existing)
            {
                collectedServers[row.CollectorName] = row.ServerCount;
            }
        }

        foreach (var regression in regressions)
        {
            var regressed = regressedServers[regression.CollectorName].Count;

            /* A collector absent from the census has no denominator, and "unknown" must not read as a
               fleet of ZERO: zero satisfies the majority comparison for any numerator at all, so folding
               the miss into a default of 0 pages exactly the pair with the least evidence behind it. The
               TryGetValue is therefore part of the condition — no denominator, no majority claim, and the
               pair goes to the digest in full (the fail-toward-the-report direction the remarks argue). */
            if (collectedServers.TryGetValue(regression.CollectorName, out var collected)
                && regressed >= CostRegressionFleetWideMinServers
                && regressed * 2 > collected)
            {
                paging.Add(regression);
            }
            else
            {
                reportOnly.Add(regression);
            }
        }

        return new CostRegressionRouting(paging, reportOnly);
    }

    /// <summary>
    /// Where each selected regression goes (#3443). One value carrying the whole answer so a caller cannot
    /// read one list and silently drop the other, and so the invariant that matters — every input row is in
    /// exactly one of the two — is assertable on a single return.
    /// </summary>
    internal readonly record struct CostRegressionRouting(
        IReadOnlyList<Mcp.DarlingCollectorCostReader.CostRegression> Paging,
        IReadOnlyList<Mcp.DarlingCollectorCostReader.CostRegression> ReportOnly);

    /// <summary>
    /// FLEET-level (#3443): the collector-cost DIGEST — one message, once per
    /// <see cref="CollectorCostDigestInterval"/>, carrying every (server, collector) pair whose per-run cost
    /// moved MATERIALLY against its own baseline in either direction (#3462 — the movers read shares
    /// <see cref="CostRegressionAddedMsFloor"/> with the paging predicate, on the move's magnitude), ranked
    /// by the collection time the move adds or removes per day, plus the heaviest collectors over the same
    /// window, which carry no floor at all — expensive-without-moving is that section's whole catch (#2862).
    ///
    /// <para><b>It is a report, and the product says so in the only tier it has for saying it.</b> Fired
    /// with no severity, which routes <see cref="AlertSeverity.ForMetric"/> to the per-metric map, where
    /// this metric has an explicit INFO arm: badge INFO, blue, in email and in all four webhook shapes. That
    /// arm is DECLARED rather than a fall-through on purpose — an unmapped metric renders INFO-blue too, and
    /// the #1136/#2090 work was a sweep to eliminate exactly that accident, so a digest relying on the
    /// accident would be "fixed" into a WARNING by the next such sweep.</para>
    ///
    /// <para><b>No resolution edge, unlike every sibling.</b> Retention Held, Store Disk Pressure, Stale
    /// Mute Rules and the rest are CONDITIONS: they are entered and left, so a recovery row closes the audit
    /// loop. A digest is a measurement of a period. There is no state to leave and nothing to announce the
    /// clearing of, so it has no active flag and writes no "Cleared" row — which is also why the 302
    /// <c>Cost Regression Cleared</c> rows the demoted half produced over 8 days on one store simply stop
    /// existing rather than moving somewhere.</para>
    ///
    /// <para><b>Empty sends nothing.</b> A digest whose two sections are both empty is a message that says a
    /// read returned no rows, which is the channel noise this issue is about. The store had no eligible
    /// pairs and no collector cost at all in the window, and that is <c>get_collection_health</c>'s
    /// question, not this one's.</para>
    ///
    /// <para>Muted through the shared seam like its siblings — <see cref="StaleMuteMetric"/> is the one
    /// condition that decides its own mute, and for a reason (it reports mutes) that does not apply
    /// here. Internal so it pins directly with a recording deliverer and a controllable clock.</para>
    /// </summary>
    internal async Task ApplyCollectorCostDigestAsync(
        IReadOnlyList<Mcp.DarlingCollectorCostReader.CostMover> movers,
        IReadOnlyList<Mcp.DarlingCollectorCostReader.CollectorCostSummaryRow> census,
        CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        var now = _utcNow();
        if (await DocumentDeliveredInsideIntervalAsync(
                _lastCostDigest, CollectorCostDigestKey, PgSelfAlertDeliveryStampStore.CostDigestStateKey,
                CollectorCostDigestInterval, now, "collector-cost digest", cancellationToken))
        {
            return;
        }

        if (movers.Count == 0 && census.Count == 0)
        {
            return;
        }

        var (shortMessage, detail) = RenderCollectorCostDigest(movers, census);

        /* #3580: the stamp is written AFTER the fire and only on a delivery the deliverer did not report
           failed — it used to be written before the fire, unconditionally, which is the "fired-today" gate
           this issue retires. A fire that throws before the deliverer is reached (the mute seam) delivered
           nothing either, and takes the same path: no stamp, the next tick retries. */
        var delivery = await FireAsync(
            StoreKey(CollectorCostDigestKey), _storeLabel, CollectorCostDigestMetric,
            currentValue: movers.Count.ToString(CultureInfo.InvariantCulture),
            /* There is no threshold. Saying so in the string is the point of the string: this surface
               exists because the same figures under a threshold could not be judged from a card. The
               numeric below is the 0 the NOT NULL column demands. */
            thresholdValue: "no threshold (report)",
            detail: detail,
            /* No override: the per-metric map's declared INFO arm decides. See the remarks. */
            severity: null,
            shortMessage: shortMessage,
            /* A genuine whole number — the count of movers listed (AlertMetricClassifier renders it as a
               count, the Stale Mute Rules shape). */
            numericCurrentValue: movers.Count,
            numericThresholdValue: 0,
            cancellationToken,
            /* #3834: the same rows the prose lists, as structured items — the prose above is unchanged and
               still leads every channel (it carries the report-not-incident framing and the materiality
               floor, which no field list states), and these give every surface that can render a table
               something to render: the Viewer's dialog, the Teams fact sets, Slack's field sections and
               {{context_json}}. Carries no Incidents, so this report stays outside per-event splitting. */
            context: BuildCollectorCostDigestContext(movers, census));

        await RecordDocumentDeliveredAsync(
            _lastCostDigest, CollectorCostDigestKey, PgSelfAlertDeliveryStampStore.CostDigestStateKey,
            delivery, now, "collector-cost digest", cancellationToken);
    }

    /// <summary>
    /// Renders the digest (#3443). PURE and static, so the DOCUMENT a human reads is assertable — a pin over
    /// this output can check that the figures on one line reconcile with each other, which a pin over the
    /// producer's fields one at a time cannot see.
    ///
    /// <para><b>Every figure a reader needs to judge the move is on the line, and the product judges
    /// one thing only: that the move was worth listing at all (#3462).</b> Latest cost per run, the run-weighted baseline, the ratio between them, the p95 and
    /// the worst single day of the baseline's own per-run costs, how many runs the latest figure averages,
    /// the worst single run in it, and the signed seconds per day the move is worth. The dispersion pair is
    /// what makes the ratio judgeable: a collector whose own worst prior day was 17,935 ms/run is not
    /// remarkable at 17,548 today, and nothing but those two numbers side by side says so.</para>
    ///
    /// <para><b>The heaviest-first census is load-bearing, not decoration.</b> A ranking by MOVEMENT cannot
    /// surface a collector that is expensive without being newly expensive, and that is how the most
    /// expensive collector this product has ever had was actually found (#2862: <c>procedure_stats</c> at
    /// 98.1M ms/day over 17,869 runs, 5,490 ms/run, which was noticed by reading a cost ranking rather than
    /// by any alert). A digest that could not surface it would have lost the better of this metric's two
    /// real catches.</para>
    ///
    /// <para><b>Summary statistics over the window rather than a per-day series.</b> Twenty movers times a
    /// week of daily figures is a wall of numbers in a channel message; the p95, the worst day and the
    /// baseline day count are that week, compressed, and the closing line names
    /// <c>get_collector_cost</c> as where the series itself lives.</para>
    /// </summary>
    /* Internal rather than private so the pin over the prose can compare the SHIPPED fire's detail_text
       against this renderer's own output (#3834) — the two sibling documents' renderers are already
       internal for the same reason. */
    internal static (string ShortMessage, string Detail) RenderCollectorCostDigest(
        IReadOnlyList<Mcp.DarlingCollectorCostReader.CostMover> movers,
        IReadOnlyList<Mcp.DarlingCollectorCostReader.CollectorCostSummaryRow> census)
    {
        /* Read off the rows rather than recomputed: every row of one read carries the same denominator, and
           a digest that states "N of M" has to take the M from the answer the N came out of. */
        var eligible = movers.Count == 0 ? 0 : movers[0].EligiblePairs;
        var shortMessage = string.Create(CultureInfo.InvariantCulture,
            $"Collector cost digest: {movers.Count} of {eligible} (server, collector) pairs moved materially against their own baseline");

        var sb = new StringBuilder();
        sb.Append(shortMessage).Append('.');
        sb.Append(string.Create(CultureInfo.InvariantCulture,
            $" This is a REPORT, not an incident: it is the monitoring tool's own query time on the monitored"
            + $" servers, nothing here is degraded, no data is lost and nothing needs doing before morning."
            + $" Ranked by the collection time each move adds or removes per day, so a stable heavy collector"
            + $" sorts low and a cheap one that moved on high volume sorts high. Only moves worth at least"
            + $" {CostRegressionAddedMsFloor / 1000.0:N0} s/day in either direction are listed - the same materiality floor the paging"
            + $" condition applies, inherited from the metric so a few hundred milliseconds of movement is not"
            + $" restated here as news (#3462) - but there is no ratio factor and no dispersion bound: the"
            + $" baseline's own p95 and worst day are on every line for the reader to judge. A NEGATIVE figure"
            + $" is a collector that got CHEAPER, which the paging condition cannot report at all."));

        if (movers.Count > 0)
        {
            sb.Append("\nMoved most (per-run cost against its own ")
              .Append(movers[0].BaselineDays.ToString(CultureInfo.InvariantCulture))
              .Append("-day baseline where stated):");
        }

        foreach (var mover in movers)
        {
            sb.Append(string.Create(CultureInfo.InvariantCulture,
                $"\n- {mover.CollectorName} on {mover.ServerName}: {mover.LatestMsPerRun:N1} ms/run vs a"
                + $" {mover.BaselineMsPerRun:N1} ms/run baseline ({mover.Ratio:N2}x) over {mover.BaselineDays:N0} prior days"
                + $" whose own p95 was {mover.BaselineP95MsPerRun:N1} ms/run and worst day {mover.BaselineWorstDayMsPerRun:N1} ms/run;"
                + $" {mover.LatestRuns:N0} runs, worst single run {mover.LatestWorstMs:N0} ms"
                + $" -> {mover.AddedMsPerDay / 1000.0:+0.0;-0.0;0.0} s/day"));
        }

        if (census.Count > 0)
        {
            /* The census is the routing denominator's read reused (GetTopAsync over
               CollectorCostDenominatorWindow), NOT the movers' 14-day baseline — so the header names the
               trailing 24 hours rather than claiming "the same window" as the section above it, which
               told the reader the two rankings shared a baseline they do not (#3448 review). The figure
               is spelled from the constant so the words cannot drift from the read. */
            sb.Append(string.Create(CultureInfo.InvariantCulture,
                $"\nHeaviest collectors over the trailing {CollectorCostDenominatorWindow.TotalHours:N0} hours,"
                + $" every server together - a collector can be expensive without having moved, and a movement"
                + $" ranking cannot see that (#2862):"));
        }

        var listed = 0;
        foreach (var row in census)
        {
            if (listed >= MaxListedCostHeaviest)
            {
                break;
            }

            sb.Append(string.Create(CultureInfo.InvariantCulture,
                $"\n= {row.CollectorName}: {row.TotalSqlMs:N0} ms over {row.RunCount:N0} runs on {row.ServerCount:N0} servers"
                + $" ({row.AvgSqlMs:N0} ms/run, worst single run {row.MaxSqlMs:N0} ms)"));
            listed++;
        }

        var remaining = census.Count - listed;
        if (remaining > 0)
        {
            sb.Append("\n+ ").Append(remaining.ToString(CultureInfo.InvariantCulture))
              .Append(" more collectors (see get_collector_cost).");
        }

        sb.Append(
            "\nThe figures are summary statistics over the window, not a per-day series - get_collector_cost"
            + " with a collector_name is where the series lives, and get_collection_health is where a"
            + " collector's runs, failures and abandonment rate live. ");
        sb.Append(CostIsNotAllTargetSide);

        return (shortMessage, sb.ToString());
    }

    /* ------------------------- #3834: the three reports' structured rows ------------------------- */

    /// <summary>
    /// The collector-cost digest's rows as an <see cref="AlertContext"/> (#3834) — ADDITIVE beside the prose
    /// <see cref="RenderCollectorCostDigest"/> returns, never a replacement for it.
    ///
    /// <para><b>Why both.</b> The prose is doing work a table cannot: it states that this is a REPORT and not
    /// an incident, that nothing is degraded and nothing needs doing before morning, and it names the
    /// materiality floor and the absence of a ratio factor. A reader handed thirteen rows of milliseconds and
    /// no sentence reasonably concludes something is wrong. The rows are doing work the paragraph cannot: the
    /// top mover's run count being 1 (one daily run getting more expensive, not a rate over hundreds of
    /// executions — a different problem with a different fix) and the least-material row's 45x worst-run
    /// outlier are both IN the prose and neither survives being read as prose. So the sentence leads and the
    /// structure follows, and every surface renders whichever it can: the Viewer's dialog binds these items
    /// as label/value pairs instead of falling back to a 45-character-wide box, the Teams card renders fact
    /// sets, Slack renders field sections, and <c>{{context_json}}</c> finally carries the figures to the
    /// automation that was parsing prose for them.</para>
    ///
    /// <para><b>The prose is unchanged to the byte, and that is what keeps the delivery gate working.</b>
    /// <see cref="AlertDetailText.ProseForDelivery"/> suppresses an alert's prose only when it is textually
    /// EQUAL to <see cref="AlertDetailText.Flatten"/> of its context — the engine alerts, whose detail text IS
    /// their flattened context. This document's prose is an essay with sentences no field list contains, so
    /// the equality can never fire and the prose keeps delivering on every channel exactly as it did. The
    /// fields carry the same figures under the same names the prose speaks, which is duplication a reader
    /// benefits from rather than noise: one is the argument, the other is the table.</para>
    ///
    /// <para><b>The caps are the prose's caps, and a bound that binds says so.</b> Movers arrive already
    /// capped at <see cref="MaxListedCostMovers"/> by the read; the census is cut at
    /// <see cref="MaxListedCostHeaviest"/> here exactly as the prose cuts it, and the remainder gets its own
    /// heading-only item — the <c>+N more</c> line the paragraph prints, as a row, so a surface rendering ONLY
    /// the rows cannot imply it showed everything. PURE and static like the renderer beside it, so the rows a
    /// human reads are assertable against the figures the prose prints.</para>
    /// </summary>
    internal static AlertContext BuildCollectorCostDigestContext(
        IReadOnlyList<Mcp.DarlingCollectorCostReader.CostMover> movers,
        IReadOnlyList<Mcp.DarlingCollectorCostReader.CollectorCostSummaryRow> census)
    {
        var context = new AlertContext();

        /* One item per (server, collector) mover, in the ranking the alert already applied — nothing is
           re-sorted, so the row a reader finds first is the row the prose lists first. The field ORDER is
           the reading order the issue's worked example settled on: the delta first, because "how much
           collection time did this move" is the question the ranking answers, then the pair the ratio is
           between, then the dispersion that makes the ratio judgeable, then the volume the per-run figure
           averages over. */
        foreach (var mover in movers)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture, $"{mover.CollectorName} on {mover.ServerName}"),
                Fields =
                {
                    ("Delta", string.Create(CultureInfo.InvariantCulture, $"{mover.AddedMsPerDay / 1000.0:+0.0;-0.0;0.0} s/day")),
                    ("Per run", string.Create(CultureInfo.InvariantCulture, $"{mover.LatestMsPerRun:N1} ms")),
                    ("Baseline", string.Create(CultureInfo.InvariantCulture,
                        $"{mover.BaselineMsPerRun:N1} ms over {mover.BaselineDays:N0} prior days")),
                    ("Ratio", string.Create(CultureInfo.InvariantCulture, $"{mover.Ratio:N2}x")),
                    ("Baseline p95", string.Create(CultureInfo.InvariantCulture, $"{mover.BaselineP95MsPerRun:N1} ms")),
                    ("Baseline worst", string.Create(CultureInfo.InvariantCulture, $"{mover.BaselineWorstDayMsPerRun:N1} ms")),
                    ("Runs", string.Create(CultureInfo.InvariantCulture, $"{mover.LatestRuns:N0}")),
                    ("Worst run", string.Create(CultureInfo.InvariantCulture, $"{mover.LatestWorstMs:N0} ms")),
                }
            });
        }

        /* The heaviest-first census, cut where the prose cuts it. Expensive-without-moving is this section's
           whole catch (#2862), and a movement ranking cannot see it — so the rows carry it too rather than
           leaving the structured reader with only the movers. */
        var listed = 0;
        foreach (var row in census)
        {
            if (listed >= MaxListedCostHeaviest)
            {
                break;
            }

            context.Details.Add(new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture, $"Heaviest: {row.CollectorName}"),
                Fields =
                {
                    ("Total", string.Create(CultureInfo.InvariantCulture, $"{row.TotalSqlMs:N0} ms")),
                    ("Runs", string.Create(CultureInfo.InvariantCulture, $"{row.RunCount:N0}")),
                    ("Servers", string.Create(CultureInfo.InvariantCulture, $"{row.ServerCount:N0}")),
                    ("Per run", string.Create(CultureInfo.InvariantCulture, $"{row.AvgSqlMs:N0} ms")),
                    ("Worst run", string.Create(CultureInfo.InvariantCulture, $"{row.MaxSqlMs:N0} ms")),
                }
            });
            listed++;
        }

        var remaining = census.Count - listed;
        if (remaining > 0)
        {
            /* A heading-only item, the shape AlertContextBuilders already uses for a stated remainder
               (BuildBlockingContext's "+N more distinct blocking incident(s)"): a surface that renders only
               rows must not be able to read this document as complete. */
            context.Details.Add(new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture,
                    $"+{remaining:N0} more collectors not listed (see get_collector_cost)")
            });
        }

        return context;
    }

    /// <summary>
    /// The fleet-sweep rollup's rows as an <see cref="AlertContext"/> (#3834) — the digest's argument applied
    /// to the second document: additive beside the prose, one item per thing that HAPPENED, every cap the
    /// prose applies applied here with its remainder stated as a row.
    ///
    /// <para>Band transitions come first because the rollup's own headline ranks them first, then the watch
    /// items that opened and closed, then the would-have-paged ledger's families — the three lists a reader
    /// scans. The rollup's aggregate counts (sweeps, muted sweeps, liveness incidents, unreadable items) lead
    /// as a single summary item rather than being spread across the rows: they describe the WINDOW, not any
    /// one row in it, and a surface that renders rows alone would otherwise lose the fact that some covered
    /// sweeps could not prove their instruments — quiet is not clean, and that sentence has to survive into
    /// the structure.</para>
    /// </summary>
    internal static AlertContext BuildFleetSweepRollupContext(
        FleetSweepRollupFacts facts, DateTime spanStartUtc, DateTime spanEndUtc)
    {
        var context = new AlertContext();

        /* The window and its aggregates as one leading item — the frame the #2506 echo discipline requires a
           report to carry, in the structure as well as in the sentence: a report that does not say what
           window it covers invites the reader to assume a different one. */
        var summary = new AlertDetailItem
        {
            Heading = "Rollup window",
            Fields =
            {
                ("Window start (UTC)", string.Create(CultureInfo.InvariantCulture, $"{spanStartUtc:o}")),
                ("Window end (UTC)", string.Create(CultureInfo.InvariantCulture, $"{spanEndUtc:o}")),
                ("Sweeps", string.Create(CultureInfo.InvariantCulture, $"{facts.Sweeps:N0}")),
                ("Band transitions", string.Create(CultureInfo.InvariantCulture, $"{facts.Transitions.Count:N0}")),
                ("Watch opened", string.Create(CultureInfo.InvariantCulture, $"{facts.Opened.Count:N0}")),
                ("Watch closed", string.Create(CultureInfo.InvariantCulture, $"{facts.Closed.Count:N0}")),
                ("Liveness incidents", string.Create(CultureInfo.InvariantCulture, $"{facts.LivenessIncidents:N0}")),
                ("Muted sweeps", string.Create(CultureInfo.InvariantCulture, $"{facts.MutedSweeps:N0}")),
                ("Unreadable items", string.Create(CultureInfo.InvariantCulture, $"{facts.UnreadableItems:N0}")),
            }
        };

        if (facts.NewestBands.Count > 0)
        {
            summary.Fields.Add(("Fleet as of newest readable sweep", string.Join(", ", facts.NewestBands.Select(b =>
                string.Create(CultureInfo.InvariantCulture, $"{b.Key} {b.Value}")))));
        }

        context.Details.Add(summary);

        var listedTransitions = 0;
        foreach (var transition in facts.Transitions)
        {
            if (listedTransitions >= MaxListedRollupTransitions)
            {
                break;
            }

            var item = new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture, $"Band transition: {transition.Server}"),
                Fields =
                {
                    ("Server", transition.Server),
                    ("From", transition.From),
                    ("To", transition.To),
                }
            };
            if (transition.Reason is not null)
            {
                item.Fields.Add(("Reason", transition.Reason));
            }

            context.Details.Add(item);
            listedTransitions++;
        }

        if (facts.Transitions.Count > listedTransitions)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture,
                    $"+{facts.Transitions.Count - listedTransitions:N0} more band transitions (see the web timeline)")
            });
        }

        AppendWatchItems(context, "opened", facts.Opened);
        AppendWatchItems(context, "closed", facts.Closed);

        /* The ledger only exists when a covered sweep ran muted; its rows are what the silence cost, and the
           prose renders them per family with the server names capped. Same shape here. */
        foreach (var family in facts.Ledger)
        {
            var servers = string.Join(", ", family.Servers.Take(MaxListedRollupLedgerServers));
            if (family.Servers.Count > MaxListedRollupLedgerServers)
            {
                servers += string.Create(CultureInfo.InvariantCulture,
                    $" +{family.Servers.Count - MaxListedRollupLedgerServers:N0} more servers");
            }

            context.Details.Add(new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture, $"Would have paged: {family.Family}"),
                Fields =
                {
                    ("Family", family.Family),
                    ("Rows", string.Create(CultureInfo.InvariantCulture, $"{family.Rows:N0}")),
                    ("Servers", servers),
                }
            });
        }

        return context;
    }

    /// <summary>One watch-event section's rows ("opened" or "closed"), capped and with the remainder stated
    /// as its own heading-only item — <see cref="AppendWatchSection"/>'s discipline, in structure.</summary>
    private static void AppendWatchItems(
        AlertContext context, string verb, IReadOnlyList<RollupWatchEvent> events)
    {
        var listed = 0;
        foreach (var item in events)
        {
            if (listed >= MaxListedRollupWatchEvents)
            {
                break;
            }

            context.Details.Add(new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture, $"Watch {verb}: {item.ItemKey}"),
                Fields =
                {
                    ("Item", item.ItemKey),
                    ("Subject", item.Subject),
                    ("Condition", item.Condition),
                }
            });
            listed++;
        }

        if (events.Count > listed)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture,
                    $"+{events.Count - listed:N0} more watch items {verb} (see the watch-item worklist)")
            });
        }
    }

    /// <summary>
    /// The analysis singles digest's rows as an <see cref="AlertContext"/> (#3834) — the third document, on the
    /// same terms: additive beside the prose, the alert's own ranking preserved, every cap's remainder stated
    /// as a row.
    ///
    /// <para>One item per top mover by severity, each carrying the KEY a page would have carried (the
    /// alert-history <c>metric_name</c> and the story hash <c>get_analysis_findings</c> resolves), the gate's
    /// reason and the #1140 dedup fingerprints — which is the whole point of structuring this one: an operator
    /// promoting a single to a drill-down needs those identifiers by name, and an automation consuming
    /// <c>{{context_json}}</c> was previously left to parse them out of a sentence. The by-family blocks follow
    /// as one item per family, the per-family server list capped as the prose caps it.</para>
    ///
    /// <para>The dedup keys ride as a FIELD rather than as <see cref="AlertContext.Incidents"/>: these are the
    /// fingerprints of OTHER alerts' findings, quoted so they can be looked up, not incidents of this report —
    /// and populating Incidents here would enter this document into per-event splitting and the incident
    /// delivery filter, which is a paging mechanism this report deliberately stays outside of.</para>
    /// </summary>
    internal static AlertContext BuildAnalysisSinglesDigestContext(AnalysisSinglesDigestFacts facts)
    {
        var context = new AlertContext();

        context.Details.Add(new AlertDetailItem
        {
            Heading = "Singles summary",
            Fields =
            {
                ("Singles", string.Create(CultureInfo.InvariantCulture, $"{facts.Singles:N0}")),
                ("Servers", string.Create(CultureInfo.InvariantCulture, $"{facts.Servers:N0}")),
                ("Ledger rows", string.Create(CultureInfo.InvariantCulture, $"{facts.Rows:N0}")),
                ("Families", string.Create(CultureInfo.InvariantCulture, $"{facts.Families.Count:N0}")),
            }
        });

        var rank = 0;
        foreach (var single in facts.TopMovers)
        {
            rank++;
            var item = new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture, $"#{rank} {single.Server} — {single.Family}"),
                Fields =
                {
                    ("Server", single.Server),
                    ("Family", single.Family),
                    ("Key", single.Key),
                    ("Severity", string.Create(CultureInfo.InvariantCulture, $"{single.Severity:F2}")),
                    ("Last recorded (UTC)", string.Create(CultureInfo.InvariantCulture, $"{single.LastRecordedUtc:o}")),
                }
            };
            if (!string.IsNullOrEmpty(single.Reason))
            {
                item.Fields.Add(("Routing reason", single.Reason));
            }
            if (single.DedupKeys.Count > 0)
            {
                item.Fields.Add(("Dedup keys", string.Join(", ", single.DedupKeys)));
            }

            context.Details.Add(item);
        }

        if (facts.Singles > facts.TopMovers.Count)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture,
                    $"+{facts.Singles - facts.TopMovers.Count:N0} more singles below the top {MaxListedSinglesMovers}, listed by family")
            });
        }

        foreach (var family in facts.Families)
        {
            var item = new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture, $"Family: {family.Family}"),
                Fields =
                {
                    ("Singles", string.Create(CultureInfo.InvariantCulture, $"{family.Singles:N0}")),
                    ("Servers", string.Create(CultureInfo.InvariantCulture, $"{family.Servers.Count:N0}")),
                }
            };

            foreach (var server in family.Servers.Take(MaxListedSinglesServersPerFamily))
            {
                var keys = string.Join("; ", server.Singles.Take(MaxListedSinglesPerServer).Select(s =>
                    string.Create(CultureInfo.InvariantCulture, $"{s.Key} {s.Severity:F2}")));
                if (server.Singles.Count > MaxListedSinglesPerServer)
                {
                    keys += string.Create(CultureInfo.InvariantCulture,
                        $" +{server.Singles.Count - MaxListedSinglesPerServer:N0} more");
                }

                item.Fields.Add((server.Server, keys));
            }

            if (family.Servers.Count > MaxListedSinglesServersPerFamily)
            {
                item.Fields.Add(("Not listed",
                    string.Create(CultureInfo.InvariantCulture,
                        $"+{family.Servers.Count - MaxListedSinglesServersPerFamily:N0} more servers")));
            }

            context.Details.Add(item);
        }

        return context;
    }

    /* ------------------------- #3466 lane 4: the fleet sweep's daily rollup ------------------------- */

    /// <summary>
    /// FLEET-level (#3466 lane 4): the fleet sweep's DAILY CHANNEL ROLLUP - one message, at most once per
    /// <see cref="FleetSweepRollupInterval"/>, summarizing the trailing day of sweeps: the band transitions,
    /// the watch items that opened and closed, the sweeps that could not prove their instruments, and -
    /// whenever any covered sweep ran under the alert master switch - the would-have-paged ledger those
    /// sweeps carried, rendered explicitly so the operator sees what the silence cost. The digest machinery
    /// reused whole (#3448's publish leg, per the spec): the same daily-interval dedup, the same
    /// empty-sends-nothing gate, the same declared INFO arm, the same shared mute seam, the same funnel.
    ///
    /// <para><b>Gated on the master switch up front, before the first store read (#3464)</b> - and here the
    /// gate IS the delivery contract rather than merely joining it: the sweep ENGINE deliberately keeps
    /// running under master-off (the muted-mode contract - the report surface is never blinded), so this
    /// method is the one and only place the sweep feature touches a channel, and master-off means it
    /// touches nothing. The gate does not consume the interval, so re-enabling does not leave a day of
    /// silence behind it: the first tick after re-enable delivers a rollup covering ITS trailing day,
    /// muted sweeps included.</para>
    ///
    /// <para><b>The catch-up semantics are deliberately narrow, and stated so nobody widens them by
    /// accident.</b> A rollup only ever covers the trailing day. A mute longer than a day therefore has
    /// muted days that never get a channel post of their own - by design: a post recapping an unbounded
    /// backlog would be either unbounded or silently truncated, and the flood it delivers on re-enable is
    /// the exact thing an operator's mute usually exists to prevent. The sweep documents are the PERMANENT
    /// record - every muted sweep stands in the web feed and <c>get_sweep_reports</c> with its mute header
    /// and its ledger, for as long as retention holds it - and the first post-re-enable rollup states how
    /// many of ITS covered sweeps ran muted, so the operator is told there is history to read rather than
    /// left to discover it.</para>
    ///
    /// <para>Called from the worker's hourly store-metrics tick beside the collector-cost evaluation; 23 of
    /// every 24 ticks cost one dictionary lookup, and the first tick after a start costs one stamp read
    /// instead of a re-announcement (#3580 — <see cref="DocumentDeliveredInsideIntervalAsync"/>). Testable
    /// through <see cref="ApplyFleetSweepRollupAsync"/> with a recording deliverer, a controllable clock and
    /// an in-memory stamp store.</para>
    /// </summary>
    public async Task EvaluateFleetSweepRollupAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        /* #3464: the master gate before the first store read - the EvaluateCollectorCostAsync shape.
           ALSO consulted inside the apply, like every sibling, so a direct caller cannot skip it. */
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        var now = _utcNow();
        /* #3580: delivered-today, memory first and the stamp when memory cannot answer. */
        if (await DocumentDeliveredInsideIntervalAsync(
                _lastSweepRollup, FleetSweepRollupKey, PgSelfAlertDeliveryStampStore.FleetSweepRollupStateKey,
                FleetSweepRollupInterval, now, "fleet-sweep rollup", cancellationToken))
        {
            return;
        }

        var spanStartUtc = now - FleetSweepRollupInterval;

        List<FleetSweepRun> runs;
        List<FleetSweepLedgerSpanEntry> ledger;
        Dictionary<int, string> serverNames;
        var readClock = Stopwatch.StartNew();
        try
        {
            /* One clock, restarted per read: the failure surface census requires the elapsed a fault
               records to belong to the operation that faulted, not to everything the try ran first. */
            runs = await FleetSweepStore.GetSweepsBySpanForRollupAsync(postgres, spanStartUtc, now, cancellationToken);
            readClock.Restart();
            ledger = await FleetSweepStore.GetWouldHavePagedBySpanAsync(postgres, spanStartUtc, now, cancellationToken);
            readClock.Restart();
            serverNames = await FleetSweepStore.GetSweepServerNamesBySpanAsync(postgres, spanStartUtc, now, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A failed read skips the tick WITHOUT consuming the interval, and it is counted: the rollup
               posts nothing on an empty day by design, so a fault folded into "empty" would convert an
               unreadable store into a permanently quiet channel - the quiet-is-not-clean misreading at the
               delivery end. The next hourly tick asks again. */
            _logger?.LogDebug(ex, "fleet-sweep rollup read failed after {ElapsedMs} ms", readClock.ElapsedMilliseconds);
            _readFailures?.RecordReadFailure(null, "fleet-sweep rollup self-alert", readClock.ElapsedMilliseconds);
            return;
        }

        await ApplyFleetSweepRollupAsync(runs, ledger, serverNames, spanStartUtc, now, cancellationToken);
    }

    /// <summary>
    /// The apply half of <see cref="EvaluateFleetSweepRollupAsync"/>, split out so the fire/dedup lifecycle
    /// is unit-testable with a recording deliverer, a controllable clock and fixture rows - the
    /// <see cref="ApplyCollectorCostDigestAsync"/> shape, seam for seam.
    ///
    /// <para><b>A day with nothing to say posts NOTHING - not a one-line all-clear.</b> Argued rather than
    /// assumed, because the alternative is defensible and was considered: the digest precedent is exact
    /// ("empty sends nothing" - a message that says a read returned no rows is the channel noise #3443
    /// exists to end), and the owner's cadence ruling names the failure mode (report-class content in a
    /// paging channel trains operators to ignore the channel). The clincher is quiet-is-not-clean: an
    /// honest all-clear would have to carry the instrument-liveness proof behind it, and a daily post
    /// carrying proof-of-quiet is a report card - the exact thing the ruling bounds. The affirmative
    /// all-clear already exists where it can afford its evidence: the web feed, every sweep, at full
    /// cadence, each document proving its own instruments. Channel silence is therefore backed by
    /// instrument-proved quiet on the record surface, never by absence. And a quiet day does not consume
    /// the interval, so the first day with something to say is not delayed by the quiet day evaluated
    /// before it.</para>
    ///
    /// <para><b>What counts as something to say</b> is <see cref="FleetSweepRollupFacts.HasReportableContent"/>:
    /// a band transition, a watch item opening or closing, a sweep that could not prove its instruments, a
    /// sweep document that did not parse (an unreadable day must not read as a quiet one), or any covered
    /// sweep having run under master-off - the last one reportable even with an EMPTY ledger, because
    /// "muted, and nothing would have paged" is a statement the operator is owed where silence would read
    /// as "nothing was checked".</para>
    ///
    /// <para><b>No resolution edge, no severity override</b> - the digest's reasoning verbatim: a rollup is
    /// a measurement of a period, not a condition, and it fires with <c>severity: null</c> so the
    /// per-metric map's DECLARED INFO arm decides (#3443's precedent - declared rather than left to the
    /// identical fall-through, so the next #1136/#2090-style sweep cannot "fix" it into a WARNING). Muted
    /// through the shared seam like every sibling.</para>
    /// </summary>
    internal async Task ApplyFleetSweepRollupAsync(
        IReadOnlyList<FleetSweepRun> runs,
        IReadOnlyList<FleetSweepLedgerSpanEntry> ledger,
        IReadOnlyDictionary<int, string> serverNames,
        DateTime spanStartUtc,
        DateTime spanEndUtc,
        CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        var now = _utcNow();
        if (await DocumentDeliveredInsideIntervalAsync(
                _lastSweepRollup, FleetSweepRollupKey, PgSelfAlertDeliveryStampStore.FleetSweepRollupStateKey,
                FleetSweepRollupInterval, now, "fleet-sweep rollup", cancellationToken))
        {
            return;
        }

        var facts = ExtractRollupFacts(runs, ledger, serverNames);
        if (!facts.HasReportableContent)
        {
            return;
        }

        var (shortMessage, detail) = RenderFleetSweepRollup(facts, spanStartUtc, spanEndUtc);

        /* #3580: stamped after the fire, and only on a delivery not reported failed — the digest's rule. */
        var delivery = await FireAsync(
            StoreKey(FleetSweepRollupKey), _storeLabel, FleetSweepRollupMetric,
            currentValue: facts.Sweeps.ToString(CultureInfo.InvariantCulture),
            /* There is no threshold - the digest's exact posture, stated in the string because the NOT NULL
               column demands a value and "report" is the honest one. */
            thresholdValue: "no threshold (report)",
            detail: detail,
            /* No override: the per-metric map's declared INFO arm decides. See the remarks. */
            severity: null,
            shortMessage: shortMessage,
            /* The count of sweeps the rollup covers - a genuine whole number (AlertMetricClassifier renders
               it as a count, the digest's shape). */
            numericCurrentValue: facts.Sweeps,
            numericThresholdValue: 0,
            cancellationToken,
            /* #3834: the digest's structured rows, on the same terms — see BuildFleetSweepRollupContext. */
            context: BuildFleetSweepRollupContext(facts, spanStartUtc, spanEndUtc));

        await RecordDocumentDeliveredAsync(
            _lastSweepRollup, FleetSweepRollupKey, PgSelfAlertDeliveryStampStore.FleetSweepRollupStateKey,
            delivery, now, "fleet-sweep rollup", cancellationToken);
    }

    /* ------------------------- #3712: the analysis singles digest ------------------------- */

    /// <summary>
    /// FLEET-level (#3712): the ANALYSIS SINGLES DIGEST — the third daily document, beside the collector-cost
    /// digest and the fleet-sweep rollup: one message, at most once per <see cref="AnalysisSinglesDigestInterval"/>,
    /// naming every analysis finding the corroboration gate routed to the digest in the trailing day —
    /// "Anomaly singles: N across M servers" — grouped by family then server, with the top movers by
    /// severity, each carrying the same dedup keys a page would have carried (design point 3), so an operator
    /// can promote any single to a full drill-down without archaeology.
    ///
    /// <para><b>Why this document exists.</b> The first day the recalibrated engine ran on a large production
    /// fleet it paged forty-plus times in seven hours on anomaly stories at severity 1.5–1.9 and confidence
    /// 0.20–0.35 — lone facts, mostly true and mostly redundant with a page something else had already sent.
    /// <c>AnalysisNotificationService</c> now routes those to the digest road: persisted, visible on the web
    /// and MCP surfaces the instant they fire, recorded in the ledger with the reason, and delivered to no
    /// paging channel. This is the one channel copy that road gets, once a day, as a distribution to read
    /// rather than a card to act on.</para>
    ///
    /// <para><b>The source is the LEDGER, not the findings table</b> (<see cref="AnalysisSinglesDigestReader"/>):
    /// a digest-routed ledger row exists for exactly the findings the gate decided about, one per
    /// fresh-or-worsening story, and it already carries the reason and the fingerprints. Rows are deduplicated
    /// per (server, story) at the newest severity, so a story re-recorded after a restart counts once.</para>
    ///
    /// <para><b>Gated on the master switch up front and inside</b>, like every sibling; <b>empty sends
    /// nothing</b> and does not consume the interval (the rollup's argument verbatim — a report that says a
    /// read returned no rows is the channel noise #3443 exists to end); a failed read <b>skips the tick
    /// WITHOUT consuming the interval</b> and is counted into the #3013 census, because a fault folded into
    /// "no singles today" would convert an unreadable store into a permanently quiet document. Called from the
    /// worker's hourly store-metrics tick beside the two siblings; 23 of every 24 ticks cost one dictionary
    /// lookup. Testable through <see cref="ApplyAnalysisSinglesDigestAsync"/> with a recording deliverer, a
    /// controllable clock and fixture rows.</para>
    /// </summary>
    public async Task EvaluateAnalysisSinglesDigestAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        var now = _utcNow();
        if (await DocumentDeliveredInsideIntervalAsync(
                _lastSinglesDigest, AnalysisSinglesDigestKey, PgSelfAlertDeliveryStampStore.AnalysisSinglesDigestStateKey,
                AnalysisSinglesDigestInterval, now, "analysis singles digest", cancellationToken))
        {
            return;
        }

        var spanStartUtc = now - AnalysisSinglesDigestInterval;

        List<AnalysisSinglesDigestReader.DigestRoutedRow> rows;
        var readClock = Stopwatch.StartNew();
        try
        {
            rows = await AnalysisSinglesDigestReader.GetDigestRoutedRowsAsync(postgres, spanStartUtc, now, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A failed read skips the tick WITHOUT consuming the interval, and it is counted: the digest posts
               nothing on an empty day by design, so a fault folded into "empty" would convert an unreadable
               store into a permanently quiet document — the quiet-is-not-clean misreading at the delivery
               end. The next hourly tick asks again. */
            _logger?.LogDebug(ex, "analysis singles digest read failed after {ElapsedMs} ms", readClock.ElapsedMilliseconds);
            _readFailures?.RecordReadFailure(null, "analysis singles digest self-alert", readClock.ElapsedMilliseconds);
            return;
        }

        await ApplyAnalysisSinglesDigestAsync(rows, spanStartUtc, now, cancellationToken);
    }

    /// <summary>
    /// The apply half of <see cref="EvaluateAnalysisSinglesDigestAsync"/>, split out so the fire/dedup
    /// lifecycle is unit-testable with a recording deliverer, a controllable clock and fixture rows — the
    /// <see cref="ApplyFleetSweepRollupAsync"/> shape, seam for seam. No resolution edge and no severity
    /// override, for the digest's reason: a report of a period fires with <c>severity: null</c> so the
    /// per-metric map's DECLARED INFO arm decides. Muted through the shared seam like every sibling.
    /// </summary>
    internal async Task ApplyAnalysisSinglesDigestAsync(
        IReadOnlyList<AnalysisSinglesDigestReader.DigestRoutedRow> rows,
        DateTime spanStartUtc,
        DateTime spanEndUtc,
        CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        var now = _utcNow();
        if (await DocumentDeliveredInsideIntervalAsync(
                _lastSinglesDigest, AnalysisSinglesDigestKey, PgSelfAlertDeliveryStampStore.AnalysisSinglesDigestStateKey,
                AnalysisSinglesDigestInterval, now, "analysis singles digest", cancellationToken))
        {
            return;
        }

        var facts = ExtractSinglesDigestFacts(rows);
        if (facts.Singles == 0)
        {
            return;
        }

        var (shortMessage, detail) = RenderAnalysisSinglesDigest(facts, spanStartUtc, spanEndUtc);

        /* #3580: stamped after the fire, and only on a delivery not reported failed — the digest's rule. */
        var delivery = await FireAsync(
            StoreKey(AnalysisSinglesDigestKey), _storeLabel, AnalysisSinglesDigestMetric,
            currentValue: facts.Singles.ToString(CultureInfo.InvariantCulture),
            /* There is no threshold — the digest's exact posture, stated in the string because the NOT NULL
               column demands a value and "report" is the honest one. */
            thresholdValue: "no threshold (report)",
            detail: detail,
            /* No override: the per-metric map's declared INFO arm decides. */
            severity: null,
            shortMessage: shortMessage,
            /* The count of distinct singles the digest covers — a genuine whole number (AlertMetricClassifier
               renders it as a count, the digest's shape). */
            numericCurrentValue: facts.Singles,
            numericThresholdValue: 0,
            cancellationToken,
            /* #3834: the digest's structured rows, on the same terms — see
               BuildAnalysisSinglesDigestContext, including why the dedup keys ride as a field rather than
               as Incidents. */
            context: BuildAnalysisSinglesDigestContext(facts));

        await RecordDocumentDeliveredAsync(
            _lastSinglesDigest, AnalysisSinglesDigestKey, PgSelfAlertDeliveryStampStore.AnalysisSinglesDigestStateKey,
            delivery, now, "analysis singles digest", cancellationToken);
    }

    /// <summary>One uncorroborated finding the digest names: the server, the family (the finding's category,
    /// parsed from its metric name), the KEY a page would have carried (<c>Analysis: {category} [{hash8}]</c> —
    /// the same <c>metric_name</c> the ledger row and <c>get_alert_history</c> spell, and the short story hash
    /// <c>get_analysis_findings</c> resolves), its newest severity, the gate's reason, the #1140 dedup
    /// fingerprints its context carried, and when it was last recorded.</summary>
    internal sealed record AnalysisSingle(
        string Server, string Family, string Key, double Severity, string Reason,
        IReadOnlyList<string> DedupKeys, DateTime LastRecordedUtc);

    /// <summary>One server's singles inside a family block, newest-severity-first.</summary>
    internal sealed record SinglesServer(string Server, IReadOnlyList<AnalysisSingle> Singles);

    /// <summary>One family block: how many singles, on how many servers, and the per-server lines.</summary>
    internal sealed record SinglesFamily(string Family, int Singles, IReadOnlyList<SinglesServer> Servers);

    /// <summary>
    /// Everything one singles digest says, extracted pure from the span's ledger rows — so the empty
    /// decision and the render read one value and the tests drive both with fixtures. <see cref="Singles"/>
    /// and <see cref="Servers"/> are distinct counts over the deduplicated population; <see cref="Rows"/> is
    /// the raw row count, stated in the document when it differs so a reader can see the dedup happened.
    /// </summary>
    internal sealed record AnalysisSinglesDigestFacts(
        int Singles,
        int Servers,
        int Rows,
        IReadOnlyList<AnalysisSingle> TopMovers,
        IReadOnlyList<SinglesFamily> Families);

    /// <summary>
    /// Extracts the digest's facts from the span's digest-routed rows — PURE (no clock, no I/O). Rows are
    /// deduplicated per (server, key): the newest row's severity and reason win, because a story that kept
    /// firing is one single, not one per re-record. The family is the category between the metric name's
    /// <c>Analysis: </c> prefix and its <c> [</c> hash bracket, the shape <c>FindingMessageFormatter.MetricName</c>
    /// writes; a name that does not parse keeps its whole text as the family, so nothing is dropped for
    /// having an unexpected shape. Reason and fingerprints come off the row's context JSON through the shared
    /// serializer; a row whose context does not parse keeps its place with an empty reason and no keys.
    /// </summary>
    internal static AnalysisSinglesDigestFacts ExtractSinglesDigestFacts(
        IReadOnlyList<AnalysisSinglesDigestReader.DigestRoutedRow> rows)
    {
        if (rows is null)
        {
            throw new ArgumentNullException(nameof(rows));
        }

        /* Oldest-first input (the reader's ORDER BY), so the LAST write for a key is the newest row. */
        var byKey = new Dictionary<(int ServerId, string Key), AnalysisSingle>();
        foreach (var row in rows)
        {
            if (row is null || string.IsNullOrEmpty(row.MetricName))
            {
                continue;
            }

            var reason = AlertContextSerializer.TryReadRouting(row.ContextJson)?.Reason ?? string.Empty;
            var dedupKeys = ReadDedupKeys(row.ContextJson);
            var server = string.IsNullOrEmpty(row.ServerName)
                ? row.ServerId.ToString(CultureInfo.InvariantCulture)
                : row.ServerName;

            byKey[(row.ServerId, row.MetricName)] = new AnalysisSingle(
                server, FamilyOf(row.MetricName), row.MetricName, row.Severity, reason, dedupKeys, row.AlertTime);
        }

        var singles = byKey.Values.ToList();
        var topMovers = singles
            .OrderByDescending(s => s.Severity)
            .ThenBy(s => s.Server, StringComparer.Ordinal)
            .ThenBy(s => s.Key, StringComparer.Ordinal)
            .Take(MaxListedSinglesMovers)
            .ToList();

        /* Families ordered most-populous first, then by name; servers inside a family the same way; singles
           on a server severity-desc. Deterministic, so the document a human reads is assertable. */
        var families = singles
            .GroupBy(s => s.Family, StringComparer.Ordinal)
            .Select(g => new SinglesFamily(
                g.Key,
                g.Count(),
                g.GroupBy(s => s.Server, StringComparer.Ordinal)
                    .Select(sg => new SinglesServer(
                        sg.Key,
                        sg.OrderByDescending(s => s.Severity).ThenBy(s => s.Key, StringComparer.Ordinal).ToList()))
                    .OrderByDescending(sg => sg.Singles.Count)
                    .ThenBy(sg => sg.Server, StringComparer.Ordinal)
                    .ToList()))
            .OrderByDescending(f => f.Singles)
            .ThenBy(f => f.Family, StringComparer.Ordinal)
            .ToList();

        return new AnalysisSinglesDigestFacts(
            singles.Count,
            singles.Select(s => s.Server).Distinct(StringComparer.Ordinal).Count(),
            rows.Count,
            topMovers,
            families);
    }

    /// <summary>The finding's category out of <c>Analysis: {category} [{hash8}]</c>, or the whole name when
    /// it is not that shape.</summary>
    internal static string FamilyOf(string metricName)
    {
        const string prefix = "Analysis: ";
        if (!metricName.StartsWith(prefix, StringComparison.Ordinal))
        {
            return metricName;
        }

        var bracket = metricName.IndexOf(" [", prefix.Length, StringComparison.Ordinal);
        var end = bracket < 0 ? metricName.Length : bracket;
        var category = metricName[prefix.Length..end].Trim();
        return category.Length == 0 ? metricName : category;
    }

    /// <summary>The #1140 dedup fingerprints a row's context carries, in order — the keys a page would have
    /// delivered under <c>{{dedup_key}}</c>. Empty for a row with no incidents or an unparseable context.</summary>
    private static IReadOnlyList<string> ReadDedupKeys(string? contextJson)
    {
        if (!AlertContextSerializer.TryDeserialize(contextJson, out var context) || context.Incidents is not { Count: > 0 })
        {
            return Array.Empty<string>();
        }

        return context.Incidents
            .Select(i => i.DedupKey)
            .Where(k => !string.IsNullOrEmpty(k))
            .Distinct(StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>
    /// Renders the singles digest (#3712). PURE and static, so the DOCUMENT a human reads is assertable — the
    /// rollup's discipline. The short message is the headline the issue asked for; the detail states the
    /// span, what a single IS and where the full finding lives, then the top movers by severity (each with
    /// its key, its reason and its dedup fingerprints), then the by-family blocks. Every cap states the
    /// remainder it did not list, so the digest never implies it showed everything.
    /// </summary>
    internal static (string ShortMessage, string Detail) RenderAnalysisSinglesDigest(
        AnalysisSinglesDigestFacts facts, DateTime spanStartUtc, DateTime spanEndUtc)
    {
        var shortMessage = string.Create(CultureInfo.InvariantCulture,
            $"Anomaly singles: {facts.Singles} across {facts.Servers} servers — uncorroborated findings routed to this digest, not paged");

        var sb = new StringBuilder();
        sb.Append(shortMessage).Append('.');
        sb.Append(string.Create(CultureInfo.InvariantCulture,
            $" This is a REPORT, not an incident (#3712): the once-a-day channel copy of the analysis findings"
            + $" the corroboration gate kept off the paging channels between {spanStartUtc:o} and {spanEndUtc:o}"
            + $" — the trailing day only. A single is a finding at or above the notify severity with ONE fact in"
            + $" its chain and no matched co-fire check; it was persisted the instant it fired and is readable now"
            + $" in get_analysis_findings (by the story hash in its key) and get_alert_history (notification_type"
            + $" 'digest', with routing_reason). A single that gains corroboration pages at that moment as a new"
            + $" firing; nothing here waits on this digest."));

        if (facts.Rows != facts.Singles)
        {
            sb.Append(string.Create(CultureInfo.InvariantCulture,
                $"\n{facts.Rows} ledger rows collapsed to {facts.Singles} distinct singles (a story re-recorded"
                + $" after a service restart counts once, at its newest severity)."));
        }

        sb.Append(string.Create(CultureInfo.InvariantCulture,
            $"\nTop movers by severity ({Math.Min(facts.TopMovers.Count, MaxListedSinglesMovers)} of {facts.Singles}):"));
        var rank = 0;
        foreach (var single in facts.TopMovers)
        {
            rank++;
            sb.Append(string.Create(CultureInfo.InvariantCulture,
                $"\n{rank}. {single.Server} — {single.Key} — severity {single.Severity:F2}"));
            if (!string.IsNullOrEmpty(single.Reason))
            {
                sb.Append(" — ").Append(single.Reason);
            }
            if (single.DedupKeys.Count > 0)
            {
                sb.Append(" — dedup: ").Append(string.Join(", ", single.DedupKeys));
            }
        }
        if (facts.Singles > facts.TopMovers.Count)
        {
            sb.Append(string.Create(CultureInfo.InvariantCulture,
                $"\n+ {facts.Singles - facts.TopMovers.Count} more singles below the top {MaxListedSinglesMovers}, listed by family below."));
        }

        sb.Append(string.Create(CultureInfo.InvariantCulture,
            $"\nBy family ({facts.Families.Count} families):"));
        foreach (var family in facts.Families)
        {
            sb.Append(string.Create(CultureInfo.InvariantCulture,
                $"\n- {family.Family}: {family.Singles} singles across {family.Servers.Count} servers"));
            foreach (var server in family.Servers.Take(MaxListedSinglesServersPerFamily))
            {
                sb.Append(string.Create(CultureInfo.InvariantCulture, $"\n    {server.Server}: "));
                sb.Append(string.Join("; ", server.Singles.Take(MaxListedSinglesPerServer).Select(s =>
                    string.Create(CultureInfo.InvariantCulture, $"{s.Key} {s.Severity:F2}"))));
                if (server.Singles.Count > MaxListedSinglesPerServer)
                {
                    sb.Append(string.Create(CultureInfo.InvariantCulture,
                        $" + {server.Singles.Count - MaxListedSinglesPerServer} more"));
                }
            }
            if (family.Servers.Count > MaxListedSinglesServersPerFamily)
            {
                sb.Append(string.Create(CultureInfo.InvariantCulture,
                    $"\n    + {family.Servers.Count - MaxListedSinglesServersPerFamily} more servers"));
            }
        }

        sb.Append("\nTo promote a single: open its finding on the web surface or read it with get_analysis_findings;"
            + " the key above is its alert-history metric_name and the dedup fingerprints are the ones a page"
            + " would have delivered. To page every notify-worthy finding again, set the route to 'page' in the"
            + " Viewer's Settings > Automated Analysis or through update_alert_settings (analysis.uncorroborated_route;"
            + " live within one collection sweep) - the store column wins over darling.json's analysis.uncorroboratedRoute,"
            + " which governs only while the column is NULL.");

        return (shortMessage, sb.ToString());
    }

    /* ------------------------- #3580: the daily documents' delivered-today gate ------------------------- */

    /// <summary>
    /// Whether a daily document was DELIVERED inside its interval as of <paramref name="now"/> — the gate
    /// both documents' evaluate and apply halves consult (#3580). The store SEEDS process memory after a
    /// start; memory serves the process; every delivery writes both — the shape #981 gave the email
    /// cooldown (<c>IAlertHistoryStore.GetLastEmailSentUtcAsync</c> seeds it across restart), applied to
    /// the one gate that was still memory-only.
    ///
    /// <para><b>Memory answers whenever it holds anything.</b> 23 of every 24 hourly ticks fall inside the
    /// interval of a delivery this process already knows about, and those ticks cost one dictionary lookup
    /// and no store round-trip — the cost promise the documents' callers make. Memory is also allowed to
    /// answer "outside the interval, deliver" without re-asking the store, because there is ONE writer per
    /// store and it is this process: once memory is seeded the store can never hold a newer stamp than
    /// memory does, so a second read could only return what memory already knows. That is also why the
    /// evaluate half's pre-check and the apply half's own check cost one store read between them and not
    /// two — the first seeds, the second finds memory populated.</para>
    ///
    /// <para><b>The stamp store is asked ONCE per document per process — on the first tick, when memory is
    /// empty</b> — and whatever it answers is cached, including "nothing": a stamp inside the interval
    /// gates; a stamp outside it lets the document proceed to its reads and stays cached, so a subsequent
    /// failed delivery leaves memory pointing at the last REAL delivery rather than at nothing; no row, or
    /// a read that failed, caches <see cref="NoDeliveryKnown"/>, which reads as "deliver" from then on.
    /// Caching the empty answer is exact under the single-writer fact above — a store that had no row for
    /// this process's first tick cannot gain one except through this process, which would populate memory
    /// directly — and it is what keeps the cost model honest in the fault case as well as the happy one: the
    /// evaluate half's pre-check asks, and the apply half's own check, seconds later on the same tick,
    /// finds memory populated whether the store answered, was empty, or threw. Re-asking on a fault would
    /// log the same failure twice and count it twice in the #3013 census on every tick the fault persisted,
    /// for one logical failure. The retry the install night's recovery case needs is unaffected: a document
    /// that has never delivered reads "deliver" from memory on every tick until a delivery lands.</para>
    ///
    /// <para><b>A stamp read that fails falls OPEN to memory, warns, and is counted.</b> Fail-open toward
    /// delivering is the direction every store-fault posture in this evaluator already takes for its
    /// documents — the rollup's read fault "skips the tick WITHOUT consuming the interval" so an unreadable
    /// store cannot become a permanently quiet channel; the digest's does the same — and it is bounded: the
    /// memory gate still holds within the process once one delivery lands, so a store that cannot answer
    /// costs at most one extra copy per process, which is exactly the pre-#3580 posture and not a spam
    /// path. Failing CLOSED (skip when the stamp cannot be read) would let a store hiccup silence a daily
    /// document, the worse failure. Counted into #3013's census because it is a store read the alert pass
    /// performed, failed and swallowed, and the census exists so that population is not invisible; the
    /// warning beside it names the document that could not ask. Counted ONCE per process, per the
    /// paragraph above — the census measures faults the pass met, and this pass meets this one once.</para>
    ///
    /// <para>No store configured (<see cref="_deliveryStamps"/> null) is the pre-#3580 gate exactly: memory
    /// only.</para>
    /// </summary>
    private async Task<bool> DocumentDeliveredInsideIntervalAsync(
        ConcurrentDictionary<string, DateTime> lastDelivered, string memoryKey, string stampKey,
        TimeSpan interval, DateTime now, string documentName, CancellationToken cancellationToken)
    {
        if (lastDelivered.TryGetValue(memoryKey, out var known))
        {
            return known != NoDeliveryKnown && now - known < interval;
        }

        if (_deliveryStamps is null)
        {
            return false;
        }

        DateTime? stamped;
        var stampClock = Stopwatch.StartNew();
        try
        {
            stamped = await _deliveryStamps.GetDeliveredAtUtcAsync(stampKey, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* One read name for both documents, because the #3013 census keys a counted site on a LITERAL
               name with its own clock and this is one site serving two callers; the log line beside it
               names the document, so the actionable half is not lost — it is a line away. The sentinel
               is what makes this warning and this count fire once per process rather than once per check:
               the apply half's own gate, seconds from now, finds memory populated and does not re-ask. */
            _logger?.LogWarning(ex,
                "{Document} delivery stamp could not be read after {ElapsedMs} ms; gating on process memory from here, which re-announces once per restart until the store answers",
                documentName, stampClock.ElapsedMilliseconds);
            _readFailures?.RecordReadFailure(null, "daily-document delivery-stamp self-alert", stampClock.ElapsedMilliseconds);
            lastDelivered[memoryKey] = NoDeliveryKnown;
            return false;
        }

        if (stamped is not DateTime deliveredAt)
        {
            lastDelivered[memoryKey] = NoDeliveryKnown;
            return false;
        }

        lastDelivered[memoryKey] = deliveredAt;
        return now - deliveredAt < interval;
    }

    /// <summary>
    /// What <see cref="DocumentDeliveredInsideIntervalAsync"/> caches when the store was asked and had no
    /// answer — no row, or a read that threw — so the store is asked once per document per process and
    /// never re-asked on the same tick by the apply half (#3580). Reads as "deliver": the gate compares it
    /// by identity before the interval arithmetic, so it can never be mistaken for a real stamp, and the
    /// first delivery that lands replaces it with a real one. <see cref="DateTime.MinValue"/> rather than a
    /// nullable value because the dictionaries are the pre-#3580 shape and every sibling gate in this file
    /// keys on presence; a value that means "asked, nothing known" keeps presence meaning "asked".
    /// </summary>
    private static readonly DateTime NoDeliveryKnown = DateTime.MinValue;

    /// <summary>
    /// Records that a daily document was DELIVERED at <paramref name="now"/> — into process memory and,
    /// when a store is configured, into the delivery stamp — unless the deliverer reported the send
    /// <see cref="AlertDelivery.ChannelFailed"/> (#3580).
    ///
    /// <para><b>"Failed" is the one disposition that writes nothing, and the rule is stated as that one
    /// exclusion on purpose.</b> It is the disposition the install night's recovery case wore: a channel
    /// was attempted and came back unsuccessful, so nothing reached a reader, and the next tick — restart
    /// or not — must retry. Every other answer is either a delivery (<see cref="AlertDelivery.Sent"/>) or
    /// the product's own decision that nothing is owed: <see cref="AlertDelivery.ChannelMuted"/> (a mute
    /// rule chose the silence), <see cref="AlertDelivery.ChannelNoneConfigured"/> (no channel exists to
    /// deliver to — the history row IS the delivery, and retrying hourly would write 24 rows a day for
    /// nothing), <see cref="AlertDelivery.ChannelThrottled"/> (a copy went out inside the cooldown, so this
    /// one is not owed) and <see cref="AlertDelivery.ChannelFolded"/> (reported on another delivery's
    /// roster). A <c>null</c> report — a deliverer that does not report, or one whose outer isolation
    /// caught something outside the channels — is "unreported", not "failed", and stamps: that is how
    /// every fire before #3580 was treated, and a deliverer that KNOWS a send failed says so. Stating the
    /// exclusion rather than an allow-list means a disposition added later defaults to the quiet side; one
    /// that means "not delivered and owed" has to be added here by name.</para>
    ///
    /// <para><b>The stamp write is failure-isolated and NOT counted</b> — a write, not a condition read,
    /// the <see cref="RecordResolutionAsync"/> distinction. Memory is stamped first, so a store that will
    /// not take the write still gates this process; the warning says the next restart will re-announce.
    /// The instant recorded is the evaluator's <paramref name="now"/>, the controllable clock, so a test
    /// can place the stamp and the interval compare is against the same clock it was written from.</para>
    /// </summary>
    private async Task RecordDocumentDeliveredAsync(
        ConcurrentDictionary<string, DateTime> lastDelivered, string memoryKey, string stampKey,
        AlertDelivery? delivery, DateTime now, string documentName, CancellationToken cancellationToken)
    {
        if (delivery is { Channel: AlertDelivery.ChannelFailed })
        {
            _logger?.LogWarning(
                "{Document} delivery failed ({Error}); no delivery stamp written, so the next tick retries it",
                documentName, delivery.SendError ?? "no error text");
            return;
        }

        lastDelivered[memoryKey] = now;

        if (_deliveryStamps is null)
        {
            return;
        }

        try
        {
            await _deliveryStamps.RecordDeliveredAtUtcAsync(stampKey, now, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: a stamp WRITE, not a condition read. */
            _logger?.LogWarning(ex,
                "{Document} was delivered but its delivery stamp could not be written; process memory gates it until the next restart, which will re-announce it",
                documentName);
        }
    }

    /// <summary>One band transition a covered sweep reported: the server by name (the documents carry names
    /// on transitions), the two bands, and the stated reason when the new band needed defending.</summary>
    internal sealed record RollupTransition(string Server, string From, string To, string? Reason);

    /// <summary>One watch-item event (opened or closed) a covered sweep reported, with the subject already
    /// resolved for a channel render: the server name from the span's verdicts, "the fleet" for the
    /// fleet-scope sentinel, or the bare id stated as such when no verdict named it.</summary>
    internal sealed record RollupWatchEvent(string Subject, string ItemKey, string Condition);

    /// <summary>One would-have-paged family aggregated across the span's muted sweeps: how many ledger rows
    /// it produced and which servers (resolved names, sorted) produced them.</summary>
    internal sealed record RollupLedgerFamily(string Family, int Rows, IReadOnlyList<string> Servers);

    /// <summary>
    /// Everything one rollup says, extracted pure from the day's run rows, the span's ledger rows and the
    /// span's names map - so the reportable-content decision and the render read one value and the tests
    /// drive both with fixtures. The counts are TOTALS; the render caps what it lists and states the
    /// remainder, so a figure here and a line count there can legitimately differ only by a stated
    /// "+ N more".
    /// </summary>
    internal sealed record FleetSweepRollupFacts(
        int Sweeps,
        int MutedSweeps,
        int LivenessIncidents,
        int UnreadableItems,
        IReadOnlyList<RollupTransition> Transitions,
        IReadOnlyList<RollupWatchEvent> Opened,
        IReadOnlyList<RollupWatchEvent> Closed,
        IReadOnlyList<RollupLedgerFamily> Ledger,
        int LedgerRows,
        int LedgerServers,
        IReadOnlyList<KeyValuePair<string, int>> NewestBands)
    {
        /// <summary>Whether the day earned a channel post - see <see cref="ApplyFleetSweepRollupAsync"/>'s
        /// remarks for why each member of this disjunction is reportable and why their absence is NOT an
        /// all-clear post. An empty day (zero sweeps) is vacuously false through every term.</summary>
        public bool HasReportableContent =>
            Transitions.Count > 0 || Opened.Count > 0 || Closed.Count > 0
            || LivenessIncidents > 0 || MutedSweeps > 0 || UnreadableItems > 0;
    }

    /// <summary>
    /// Extracts the rollup's facts from the day's rows - PURE (no clock, no I/O), so
    /// <c>FleetSweepRollupTests</c> drives every branch with fixtures. The run columns carry the mute header
    /// and the liveness verdict directly; the transitions and watch events are parsed out of each run's own
    /// document (<c>changes.band_transitions</c>, <c>watch.opened</c>, <c>watch.closed</c>) - the engine's
    /// persisted report is the authority on what each sweep SAID, and re-deriving transitions from verdict
    /// rows here would be a second opinion that could disagree with it. Anything unreadable is COUNTED
    /// (<see cref="FleetSweepRollupFacts.UnreadableItems"/>) rather than skipped silently - a document that
    /// does not parse, a band census carrying a count no int holds, a watch entry missing its item or a
    /// usable server id - and the count is itself reportable: an unreadable day must not render as a quiet
    /// one. One counter for every shape, stated as such in the render, because the operator's next move is
    /// the same whichever piece was unreadable: read the sweep documents on the record surface. Runs are
    /// processed oldest-first whatever order the read served, so the transition list reads chronologically
    /// and the band census standing at the end is the newest READABLE sweep's.
    /// </summary>
    internal static FleetSweepRollupFacts ExtractRollupFacts(
        IReadOnlyList<FleetSweepRun> runs,
        IReadOnlyList<FleetSweepLedgerSpanEntry> ledger,
        IReadOnlyDictionary<int, string> serverNames)
    {
        var ordered = runs.OrderBy(r => r.SweptAtUtc).ThenBy(r => r.SweepId).ToList();

        var muted = 0;
        var liveness = 0;
        var unreadable = 0;
        var transitions = new List<RollupTransition>();
        var opened = new List<RollupWatchEvent>();
        var closed = new List<RollupWatchEvent>();
        IReadOnlyList<KeyValuePair<string, int>> newestBands = Array.Empty<KeyValuePair<string, int>>();

        foreach (var run in ordered)
        {
            if (!run.AlertsEnabled)
            {
                muted++;
            }

            if (!run.InstrumentsAlive)
            {
                liveness++;
            }

            try
            {
                using var document = JsonDocument.Parse(run.ReportJson);
                var root = document.RootElement;

                if (root.ValueKind != JsonValueKind.Object)
                {
                    unreadable++;
                    continue;
                }

                if (root.TryGetProperty("changes", out var changes)
                    && changes.ValueKind == JsonValueKind.Object
                    && changes.TryGetProperty("band_transitions", out var bandTransitions)
                    && bandTransitions.ValueKind == JsonValueKind.Array)
                {
                    foreach (var transition in bandTransitions.EnumerateArray())
                    {
                        var server = StringProperty(transition, "server");
                        var from = StringProperty(transition, "from");
                        var to = StringProperty(transition, "to");
                        if (server is not null && from is not null && to is not null)
                        {
                            transitions.Add(new RollupTransition(server, from, to, StringProperty(transition, "reason")));
                        }
                    }
                }

                if (root.TryGetProperty("watch", out var watch) && watch.ValueKind == JsonValueKind.Object)
                {
                    unreadable += AppendWatchEvents(watch, "opened", serverNames, opened);
                    unreadable += AppendWatchEvents(watch, "closed", serverNames, closed);
                }

                /* Ordered oldest-first, so the last readable band census standing is the newest one - the
                   "where the fleet ended the day" line. */
                if (root.TryGetProperty("fleet", out var fleet)
                    && fleet.ValueKind == JsonValueKind.Object
                    && fleet.TryGetProperty("bands", out var bands)
                    && bands.ValueKind == JsonValueKind.Object)
                {
                    var census = new List<KeyValuePair<string, int>>();
                    var censusReadable = true;
                    foreach (var band in bands.EnumerateObject())
                    {
                        if (band.Value.ValueKind != JsonValueKind.Number)
                        {
                            continue;
                        }

                        /* TryGetInt32, not GetInt32: a band count is engine-written and int-sized, so a
                           JSON number an int cannot hold is a corrupt census - and GetInt32 answers it
                           with FormatException/OverflowException, which the JsonException-only catch
                           below would NOT swallow. An escape here kills the whole rollup tick for up to
                           a day, the exact silent outage the unreadable counter exists to prevent. The
                           corrupt census is counted unreadable like a corrupt document, and the
                           fleet-as-of line keeps the newest sweep whose census WAS readable. */
                        if (!band.Value.TryGetInt32(out var count))
                        {
                            censusReadable = false;
                            break;
                        }

                        census.Add(new KeyValuePair<string, int>(band.Name, count));
                    }

                    if (censusReadable)
                    {
                        newestBands = census.OrderBy(b => b.Key, StringComparer.Ordinal).ToList();
                    }
                    else
                    {
                        unreadable++;
                    }
                }
            }
            catch (JsonException)
            {
                /* A sweep document that did not parse becomes the rollup's own evidence: the store read
                   above is the counted site, and this arm CONVERTS the fault into the unreadable count
                   the rollup reports (an unreadable day must not read as a quiet one) rather than
                   swallowing it. Counting it as a read failure would double-book the one read. */
                unreadable++;
            }
        }

        var families = ledger
            .GroupBy(entry => entry.AlertFamily, StringComparer.Ordinal)
            .OrderBy(g => g.Key, StringComparer.Ordinal)
            .Select(g => new RollupLedgerFamily(
                g.Key,
                g.Count(),
                g.Select(entry => SubjectName(entry.ServerId, serverNames))
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(name => name, StringComparer.Ordinal)
                    .ToList()))
            .ToList();

        return new FleetSweepRollupFacts(
            ordered.Count, muted, liveness, unreadable,
            transitions, opened, closed,
            families, ledger.Count,
            ledger.Select(entry => entry.ServerId).Distinct().Count(),
            newestBands);
    }

    /// <summary>
    /// Renders the rollup - PURE and static like <see cref="RenderCollectorCostDigest"/>, so the DOCUMENT a
    /// human reads is assertable: <c>FleetSweepRollupTests</c> parses this output and holds its printed
    /// figures to each other (the header's counts against the lines rendered, the ledger's total against
    /// its family lines, the mute sentence against the sweep count it claims). Every section renders only
    /// when it has something to say; the frame states the covered window explicitly (the #2506 echo
    /// discipline - a report that does not say what window it covers invites the reader to assume a
    /// different one) and names the web feed as the record surface, because the rollup is the day's
    /// CEILING, not the day.
    /// </summary>
    internal static (string ShortMessage, string Detail) RenderFleetSweepRollup(
        FleetSweepRollupFacts facts, DateTime spanStartUtc, DateTime spanEndUtc)
    {
        var shortMessage = string.Create(CultureInfo.InvariantCulture,
            $"Fleet sweep rollup: {facts.Sweeps} sweeps - {facts.Transitions.Count} band transitions,"
            + $" {facts.Opened.Count} watch items opened, {facts.Closed.Count} closed,"
            + $" {facts.LivenessIncidents} liveness incidents, {facts.MutedSweeps} muted sweeps");

        var sb = new StringBuilder();
        sb.Append(shortMessage).Append('.');
        sb.Append(string.Create(CultureInfo.InvariantCulture,
            $" This is a REPORT, not an incident - the fleet sweep's one channel post for the day (#3466's"
            + $" delivery ruling: the web feed's Fleet Sweeps timeline is the workhorse at full cadence, and"
            + $" channel delivery is bounded at one daily rollup). It covers {spanStartUtc:o} to"
            + $" {spanEndUtc:o} - the trailing day only, never a recap of older history, because the sweep"
            + $" documents are the permanent record and this is their ceiling."));

        if (facts.NewestBands.Count > 0)
        {
            sb.Append("\nThe fleet as of the newest readable covered sweep: ");
            sb.Append(string.Join(", ", facts.NewestBands.Select(b =>
                string.Create(CultureInfo.InvariantCulture, $"{b.Key} {b.Value}"))));
            sb.Append('.');
        }

        if (facts.MutedSweeps > 0)
        {
            sb.Append(string.Create(CultureInfo.InvariantCulture,
                $"\n{facts.MutedSweeps} of the {facts.Sweeps} covered sweeps ran with the alert master"
                + $" switch OFF. Delivery was off while they ran; the sweeps kept publishing to the web feed"
                + $" with the mute stated on every document - the muted-mode contract."));

            if (facts.LedgerRows > 0)
            {
                sb.Append(string.Create(CultureInfo.InvariantCulture,
                    $"\nThe would-have-paged ledger those sweeps carried - what the silence cost:"
                    + $" {facts.LedgerRows} rows across {facts.LedgerServers} servers and"
                    + $" {facts.Ledger.Count} families:"));

                foreach (var family in facts.Ledger)
                {
                    sb.Append(string.Create(CultureInfo.InvariantCulture,
                        $"\n- {family.Family}: {family.Rows} rows on "));
                    sb.Append(string.Join(", ", family.Servers.Take(MaxListedRollupLedgerServers)));
                    if (family.Servers.Count > MaxListedRollupLedgerServers)
                    {
                        sb.Append(string.Create(CultureInfo.InvariantCulture,
                            $" + {family.Servers.Count - MaxListedRollupLedgerServers} more servers"));
                    }
                }

                sb.Append("\nEach row carries its evidence on the sweep document it rode in on - the figures"
                    + " and the thresholds they crossed live there, not here.");
            }
            else
            {
                sb.Append("\nThose sweeps carried an EMPTY would-have-paged ledger - muted, and nothing the"
                    + " sweep's scoring banded Critical. That is a statement, not an absence: the check was"
                    + " made on every muted sweep.");
            }
        }

        if (facts.LivenessIncidents > 0)
        {
            sb.Append(string.Create(CultureInfo.InvariantCulture,
                $"\n{facts.LivenessIncidents} covered sweeps could NOT prove their instruments were alive."
                + $" Quiet is not clean - read those sweeps' liveness blocks before believing any quiet card"
                + $" they carry."));
        }

        if (facts.UnreadableItems > 0)
        {
            sb.Append(string.Create(CultureInfo.InvariantCulture,
                $"\n{facts.UnreadableItems} unreadable items across the covered sweeps' documents - whole"
                + $" documents or single entries that did not parse - counted here so an unreadable day"
                + $" cannot read as a quiet one."));
        }

        if (facts.Transitions.Count > 0)
        {
            sb.Append("\nBand transitions, chronological:");
            var listed = 0;
            foreach (var transition in facts.Transitions)
            {
                if (listed >= MaxListedRollupTransitions)
                {
                    break;
                }

                sb.Append(string.Create(CultureInfo.InvariantCulture,
                    $"\n- {transition.Server}: {transition.From} -> {transition.To}"));
                if (transition.Reason is not null)
                {
                    sb.Append(" (").Append(transition.Reason).Append(')');
                }

                listed++;
            }

            if (facts.Transitions.Count > listed)
            {
                sb.Append(string.Create(CultureInfo.InvariantCulture,
                    $"\n+ {facts.Transitions.Count - listed} more band transitions (see the web timeline)."));
            }
        }

        AppendWatchSection(sb, "opened", facts.Opened);
        AppendWatchSection(sb, "closed", facts.Closed);

        sb.Append("\nEvery sweep in full - verdicts, evidence, liveness blocks and the ledger rows' own"
            + " figures - lives on the Fleet Sweeps web page and get_sweep_reports. This rollup is the"
            + " day's ceiling, not the record.");

        return (shortMessage, sb.ToString());
    }

    /// <summary>One watch-event section ("opened" or "closed"), rendered only when it has rows, capped with
    /// a stated remainder like every list in this document.</summary>
    private static void AppendWatchSection(StringBuilder sb, string verb, IReadOnlyList<RollupWatchEvent> events)
    {
        if (events.Count == 0)
        {
            return;
        }

        sb.Append("\nWatch items ").Append(verb).Append(':');
        var listed = 0;
        foreach (var item in events)
        {
            if (listed >= MaxListedRollupWatchEvents)
            {
                break;
            }

            sb.Append(string.Create(CultureInfo.InvariantCulture,
                $"\n- {item.ItemKey} on {item.Subject}: {item.Condition}"));
            listed++;
        }

        if (events.Count > listed)
        {
            sb.Append(string.Create(CultureInfo.InvariantCulture,
                $"\n+ {events.Count - listed} more (see the watch-item worklist)."));
        }
    }

    /// <summary>The watch arrays' entries, resolved to render-ready events. The documents carry watch
    /// subjects by bare id (the engine's vocabulary); the span's verdict names resolve them, the fleet
    /// sentinel is named as the fleet, and a server no verdict named degrades to its id stated as such -
    /// honest, never invented. Returns how many entries were UNREADABLE - missing their item, or carrying
    /// a server id no int holds - so the caller counts them on the one unreadable counter rather than this
    /// helper dropping them silently: a watch event the rollup cannot render is still a watch event the
    /// operator was owed.</summary>
    private static int AppendWatchEvents(
        JsonElement watch, string property, IReadOnlyDictionary<int, string> serverNames, List<RollupWatchEvent> events)
    {
        if (!watch.TryGetProperty(property, out var array) || array.ValueKind != JsonValueKind.Array)
        {
            return 0;
        }

        var unreadable = 0;
        foreach (var entry in array.EnumerateArray())
        {
            var itemKey = StringProperty(entry, "item");
            if (itemKey is null
                || !entry.TryGetProperty("server_id", out var serverId)
                || serverId.ValueKind != JsonValueKind.Number
                || !serverId.TryGetInt32(out var serverIdValue))
            {
                unreadable++;
                continue;
            }

            events.Add(new RollupWatchEvent(
                SubjectName(serverIdValue, serverNames),
                itemKey,
                StringProperty(entry, "condition") ?? string.Empty));
        }

        return unreadable;
    }

    private static string SubjectName(int serverId, IReadOnlyDictionary<int, string> serverNames) =>
        serverId == FleetSweepStore.FleetScopeServerId
            ? "the fleet"
            : serverNames.TryGetValue(serverId, out var name)
                ? name
                : string.Create(CultureInfo.InvariantCulture, $"server id {serverId}");

    private static string? StringProperty(JsonElement element, string property) =>
        element.ValueKind == JsonValueKind.Object
        && element.TryGetProperty(property, out var value)
        && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

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
            /* NOT counted by #3013's swallowed-read counter: the DELIVERY path, not a condition read. */
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

                /* #3653 A5 asked whether this edge should also forget the server's delta baselines
                   (_deltas.ClearServer), and the answer is no, deliberately. Two reasons, both about WHICH
                   server. First, this edge fires from the AUTHORITATIVE vantage for the AG (#1696 de-dup above),
                   which is a monitored connection that can SEE the role change — not necessarily the one whose
                   counters moved; `serverId` here names the observer. Second, a role change is a fact about a
                   replica, not about which instance a connection reaches: a registration pointed at a node
                   directly keeps reading that node's cumulative DMVs through the failover, the counters are
                   continuous, and forgetting them would throw one honest interval away on every replica of the
                   AG at the moment the charts matter most. The registration that DOES change instance — one
                   pointed at the listener — reports a different @@SERVERNAME and sqlserver_start_time after the
                   failover, and that pair is what the identity-epoch carrier (CpuUtilizationCollector ->
                   ServerEpoch) compares every minute against the persisted one; the forget happens there, on
                   the server whose counters actually moved, and this edge stays an alert about a role. */
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
    /// Pure disk-pressure decision at the SHIPPED defaults: the store volume is under pressure when its
    /// FREE space is below <see cref="DiskFreeWarnPercent"/> of the volume total AND below
    /// <see cref="DiskFreeWarnFloorGb"/> absolute (#3528 — see the floor constant for the composition).
    /// No I/O, so it pins directly. A non-positive total is treated as "can't tell" (false — the caller
    /// also guards this).
    /// <para><paramref name="percentFree"/> is the measurement the alert is ABOUT, handed back so the fire
    /// site can store it as a real numeric instead of leaving the history store to find it again by
    /// scanning <paramref name="reason"/> for digits (#1881). It is computed whenever the total is usable,
    /// including when the volume is comfortable and the answer is false — the caller only uses it on the
    /// firing path, but returning a number that is 0 for "no pressure" and 0 for "a full volume" would put
    /// the one dangerous ambiguity this metric must never have back into the signature.</para>
    /// </summary>
    internal static bool IsDiskPressure(long freeBytes, long totalBytes, out string reason, out double percentFree)
        => IsDiskPressure(freeBytes, totalBytes, DiskFreeWarnPercent, DiskFreeWarnFloorGb, out reason, out percentFree);

    /// <summary>#2107: the percent-only form — floor disabled, kept for the tests that pin the percent
    /// edge on its own. The sweep calls the two-knob overload below.</summary>
    internal static bool IsDiskPressure(long freeBytes, long totalBytes, double warnPercent, out string reason, out double percentFree)
        => IsDiskPressure(freeBytes, totalBytes, warnPercent, 0.0, out reason, out percentFree);

    /// <summary>#3528: the configurable form — the sweep passes the store-backed
    /// <c>SelfDiskFreeWarnPercent</c> AND <c>SelfDiskFreeWarnGb</c> (0 = no floor).</summary>
    internal static bool IsDiskPressure(long freeBytes, long totalBytes, double warnPercent, double floorGb, out string reason, out double percentFree)
    {
        if (totalBytes <= 0)
        {
            reason = "";
            percentFree = 0;
            return false;
        }

        percentFree = (double)freeBytes / totalBytes * 100.0;
        double freeGb = freeBytes / (1024.0 * 1024.0 * 1024.0);
        if (percentFree < warnPercent && (floorGb <= 0 || freeGb < floorGb))
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
            /* NOT counted by #3013's swallowed-read counter: this method is handed its evidence as parameters and
               performs no store read — the catch covers the apply/deliver half. The reads that FEED it are counted
               at their own sites in DarlingWorker. */
            _logger?.LogError("Store disk-pressure self-alert failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Edge-applies the fleet-level Store Disk Pressure condition from a store-volume sample: fire once on
    /// entry, re-fire only after the alert cooldown while it persists, and write ONE "Store Disk Pressure
    /// Resolved" history row on recovery (mirrors the per-server conditions' edge shape). Gated on the master
    /// alerts switch. NO-OPS when free/total are null — a remote BYO store whose volume the service can't see —
    /// so it never false-alarms; the managed store's own volume is what it exists to protect.
    /// <paramref name="storeSizeBytes"/> is the last size the hourly self-metrics sweep recorded — context
    /// for the alert text only, never the trigger, which is why the sentence it renders names the sample
    /// rather than claiming the current byte count (#3199).
    /// Internal (tested directly, like the sibling Apply methods); the worker calls the isolating
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
        /* #2107/#3528: store-backed thresholds (clamped on read); the constants remain only as the
           shipped defaults. */
        double warnPercent = _settings.SelfDiskFreeWarnPercent;
        double floorGb = _settings.SelfDiskFreeWarnGb;
        bool pressure = IsDiskPressure(free, total, warnPercent, floorGb, out var reason, out var percentFree);

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
                /* Names the sample rather than claiming currency: the size is the self-metrics series'
                   newest whole-store row, not a live measurement (#3199 — measuring it live cost a
                   filesystem walk on the collection loop's serial thread every five minutes, 3,177 ms of
                   a 5 s bound on a 225 GiB store). "currently" would be a claim this value cannot make.
                   The word "hourly" is deliberately absent too: that is the sweep's CADENCE, and measured
                   gaps in the series run past it, so naming it here would imply an age the row does not
                   carry. */
                var storeText = storeSizeBytes is long size
                    ? $" The store measured {FormatGb(size)} at its last self-metrics sample."
                    : "";
                await FireAsync(
                    StoreKey(DiskKey), _storeLabel, DiskPressureMetric, reason,
                    /* #3528: the threshold string names BOTH gates when the floor is active, so the history
                       row's threshold column states the condition that actually fired. */
                    floorGb > 0
                        ? $"{warnPercent.ToString("0.#", CultureInfo.InvariantCulture)}% free and under {floorGb.ToString("0.#", CultureInfo.InvariantCulture)} GB"
                        : $"{warnPercent.ToString("0.#", CultureInfo.InvariantCulture)}% free",
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
                StoreKey(DiskKey), _storeLabel, DiskPressureMetric,
                DiskPressureResolvedMetric, "Monitor store volume free space recovered"), cancellationToken);
        }
    }

    /* ---------------- custom-alert-rule health (fleet-level, polled — #3304) ---------------- */

    /// <summary>
    /// Isolating wrapper for the fleet-level custom-alert-rule-health self-alert (#3304), mirroring
    /// <see cref="EvaluateDiskPressureAsync"/>: a throwing seam (e.g. a mute rule's <c>Matches()</c>) is
    /// contained here so it can never stop the worker's fleet-global maintenance pass. The worker calls THIS;
    /// tests call the isolated <see cref="ApplyCustomRuleHealthAsync"/> directly.
    /// </summary>
    public async Task EvaluateCustomRuleHealthAsync(CustomAlertHealthReport report, CancellationToken cancellationToken)
    {
        try
        {
            await ApplyCustomRuleHealthAsync(report, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: this method is handed its evidence (the report)
               as a parameter and performs no store read — the catch covers the apply/deliver half, exactly
               like the sibling store self-alert wrappers. The read that BUILDS the report lives in
               CustomAlertEvaluator, outside this alert-pass census. */
            _logger?.LogError("Custom-alert rule-health self-alert failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Edge-applies the fleet-level "some custom alert rules are broken or never firing" condition from the
    /// <see cref="CustomAlertHealthReport"/> the <see cref="CustomAlertEvaluator"/> builds: fire once on entry,
    /// re-fire only after the alert cooldown while any rule stays unhealthy, and write ONE resolution row when
    /// every rule is healthy again (the Collection-Stopped standing-condition edge shape). ONE alert aggregates
    /// ALL unhealthy rules — a <c>pg_*</c> rename can break many at once, and one-alert-per-rule would be an
    /// alert storm. Gated on the master alerts switch. The rule names/errors in the report are ALREADY
    /// newline-stripped + length-capped by the evaluator, and the detail NEVER contains the compiled SQL — only
    /// the rule id/name and the catalog parse error. Internal (tested directly, like the sibling Apply methods)
    /// with a recording deliverer + a controllable clock.
    /// </summary>
    internal async Task ApplyCustomRuleHealthAsync(CustomAlertHealthReport report, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled || report is null)
        {
            return;
        }

        var now = _utcNow();
        if (report.HasIssues)
        {
            _activeCustomRuleHealth[CustomRuleHealthKey] = true;

            /* Standing condition: fire on entry, re-fire only per cooldown while unhealthy. The CURRENT report
               is rendered each time, so a rule that breaks later shows up on the next re-fire. */
            if (CooldownElapsed(_lastCustomRuleHealthAlert, CustomRuleHealthKey, now))
            {
                _lastCustomRuleHealthAlert[CustomRuleHealthKey] = now;
                var (shortMessage, detail) = RenderCustomRuleHealth(report);
                await FireAsync(
                    StoreKey(CustomRuleHealthKey), _storeLabel, CustomRuleHealthMetric,
                    currentValue: report.TotalIssues.ToString(CultureInfo.InvariantCulture),
                    thresholdValue: "0",
                    detail: detail,
                    severity: AlertSeverityLevel.Warning,
                    shortMessage: shortMessage,
                    /* The count of unhealthy rules is a genuine whole number (AlertMetricClassifier renders it
                       as a count); the healthy bound is 0. */
                    numericCurrentValue: report.TotalIssues,
                    numericThresholdValue: 0,
                    cancellationToken);
            }
        }
        else if (_activeCustomRuleHealth.TryRemove(CustomRuleHealthKey, out var was) && was)
        {
            _lastCustomRuleHealthAlert.TryRemove(CustomRuleHealthKey, out _);
            await RecordResolutionAsync(new AlertResolution(
                StoreKey(CustomRuleHealthKey), _storeLabel, CustomRuleHealthMetric,
                CustomRuleHealthResolvedMetric,
                "All custom alert rules compile and are firing-eligible again"), cancellationToken);
        }
    }

    /// <summary>
    /// Renders the aggregated health alert's one-line summary and its multi-line <c>detail_text</c> from an
    /// (already-sanitized) report. Each rule occupies its own line led by <c>"- Rule &lt;id&gt;"</c>, which
    /// cannot be read as a mute-context label by <see cref="AlertMuteContext.PopulateFromDetailText"/>; the list
    /// is capped at <see cref="MaxListedUnhealthyRules"/> with a "+N more" tail so one <c>pg_*</c> rename cannot
    /// produce an unbounded body. The compiled SQL is never included — only the rule id/name and the reason.
    /// </summary>
    private static (string ShortMessage, string Detail) RenderCustomRuleHealth(CustomAlertHealthReport report)
    {
        var shortMessage = string.Create(CultureInfo.InvariantCulture,
            $"{report.TotalIssues} custom alert rule(s) need attention: {report.BrokenRules.Count} no longer compile, {report.NeverFiringRules.Count} armed but never fire");

        var sb = new StringBuilder();
        sb.Append(shortMessage).Append('.');

        var listed = 0;
        void AppendSection(string heading, IReadOnlyList<CustomAlertRuleHealthIssue> issues)
        {
            if (issues.Count == 0 || listed >= MaxListedUnhealthyRules)
            {
                return;
            }

            sb.Append('\n').Append(heading).Append(':');
            foreach (var issue in issues)
            {
                if (listed >= MaxListedUnhealthyRules)
                {
                    break;
                }

                /* Leading "- Rule <id>" never matches a PopulateFromDetailText label; name/reason are pre-sanitized. */
                sb.Append("\n- Rule ").Append(issue.RuleId.ToString(CultureInfo.InvariantCulture))
                  .Append(" \"").Append(issue.RuleName).Append("\": ").Append(issue.Reason);
                listed++;
            }
        }

        AppendSection("Broken (will not fire)", report.BrokenRules);
        AppendSection("Armed but never fires", report.NeverFiringRules);

        var remaining = report.TotalIssues - listed;
        if (remaining > 0)
        {
            sb.Append("\n+ ").Append(remaining.ToString(CultureInfo.InvariantCulture)).Append(" more (see the service log).");
        }

        return (shortMessage, sb.ToString());
    }

    /* ---------------- stale mute rules (fleet-level, polled — #3306) ---------------- */

    /// <summary>
    /// Isolating wrapper for the fleet-level stale-mute self-alert (#3306), mirroring
    /// <see cref="EvaluateCustomRuleHealthAsync"/>. The worker calls THIS; tests call the isolated
    /// <see cref="ApplyStaleMuteRulesAsync"/> directly.
    /// </summary>
    public async Task EvaluateStaleMuteRulesAsync(
        IReadOnlyList<MuteRule> rules, CancellationToken cancellationToken)
    {
        try
        {
            await ApplyStaleMuteRulesAsync(rules, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: the rules arrive as a parameter from the live
               MuteRuleService cache and this method performs no store read. */
            _logger?.LogError("Stale-mute self-alert failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Edge-applies the fleet-level "a mute rule has outlived its reason" condition (#3306): a rule that is
    /// ENABLED, has NO expiry, and was created longer than <see cref="StaleMuteAge"/> ago.
    ///
    /// <para><b>Why this condition exists at all.</b> A mute is a deliberate blind spot in a monitoring
    /// tool, and nothing else in this product reports that one is there. The only way to discover a mute is
    /// to ask <c>get_mute_rules</c>, which requires already suspecting it — so a mute whose justification
    /// has expired keeps suppressing a now-correct alert, and the symptom is silence. It is #2813's Retention
    /// Held shape one surface over: a correct, deliberate pause that reads as health once made.</para>
    ///
    /// <para><b>All three halves are required.</b> Unbounded alone is not a finding — an operator may mean it,
    /// and the product's own dialog offers "Never". Old alone is not either: a rule with an expiry has a
    /// reviewer built in, namely the expiry. And a disabled rule suppresses nothing, so it is not a blind
    /// spot however old and however unbounded. It is the conjunction — still suppressing, no bound, and past
    /// every bound the product offers — that says the justification was never revisited.</para>
    ///
    /// <para><b>This never deletes or expires a rule.</b> Silent un-muting is its own incident: the channel
    /// this was protecting floods unannounced, which is exactly the outcome a permanent rule was chosen to
    /// avoid. Permanence plus visibility, so the operator decides.</para>
    ///
    /// <para>Severity follows BLAST RADIUS, not age. A rule that constrains nothing
    /// (<see cref="MuteRule.MatchesEveryAlert"/>) suppresses every alert on the store, so the fleet reads
    /// healthy for want of anything being reported — CRITICAL. A rule scoped to a metric, a server or a
    /// pattern hides one signal — WARNING.</para>
    ///
    /// <para>A STANDING condition like Custom Alert Rules Unhealthy: fire once on entry, re-state it while
    /// any rule qualifies, and ONE resolution row when none does. Unlike those siblings it re-states on its
    /// own <see cref="StaleMuteRefire"/> rather than the shared alert cooldown — see there for why a
    /// days-scale fact needs its own, longer interval. Gated on the master alerts switch. Internal
    /// so it pins directly with a recording deliverer and a controllable clock.</para>
    /// </summary>
    internal async Task ApplyStaleMuteRulesAsync(
        IReadOnlyList<MuteRule> rules, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        var now = _utcNow();

        /* Unbounded, in force, and older than every expiry the product offers. Ordered oldest-first so the
           elided tail is the least interesting end.

           "In force" reduces to Enabled here: a rule with no expiry cannot have lapsed, so the expiry test
           that belongs in the general predicate would be dead code in this one — and it reads MuteRule's
           IsExpired, which consults DateTime.UtcNow rather than this evaluator's injected clock. Dropping it
           leaves the whole condition judged on ONE clock, which is what makes the age assertions mean what
           they say. A rule carrying ANY expiry is excluded either way, past or future: a bound is a bound,
           and a rule that has one reviews itself. */
        var stale = new List<MuteRule>();
        foreach (var rule in rules)
        {
            if (rule is null || !rule.Enabled || rule.ExpiresAtUtc.HasValue)
            {
                continue;
            }

            if (now - rule.CreatedAtUtc >= StaleMuteAge)
            {
                stale.Add(rule);
            }
        }

        stale.Sort(static (a, b) => a.CreatedAtUtc.CompareTo(b.CreatedAtUtc));

        if (stale.Count == 0)
        {
            if (_activeStaleMute.TryRemove(StaleMuteKey, out var was) && was)
            {
                _lastStaleMuteAlert.TryRemove(StaleMuteKey, out _);
                await RecordResolutionAsync(new AlertResolution(
                    StoreKey(StaleMuteKey), _storeLabel, StaleMuteMetric, StaleMuteResolvedMetric,
                    "No mute rule is suppressing alerts without an expiry any more"), cancellationToken);
            }

            return;
        }

        _activeStaleMute[StaleMuteKey] = true;

        /* Standing condition: fire on entry, re-fire only per StaleMuteRefire while any rule qualifies. The
           CURRENT set is rendered each time, so a rule that ages past the bound later shows up on the next
           re-fire. Its OWN interval rather than the shared CooldownElapsed the siblings use, because the
           fact it reports changes on a scale of days — see StaleMuteRefire. */
        if (_lastStaleMuteAlert.TryGetValue(StaleMuteKey, out var lastFired)
            && now - lastFired < StaleMuteRefire)
        {
            return;
        }

        _lastStaleMuteAlert[StaleMuteKey] = now;
        bool blanket = stale.Exists(static r => r.MatchesEveryAlert);
        var (shortMessage, detail) = RenderStaleMuteRules(stale, now, blanket);

        await FireAsync(
            StoreKey(StaleMuteKey), _storeLabel, StaleMuteMetric,
            currentValue: stale.Count.ToString(CultureInfo.InvariantCulture),
            thresholdValue: "0",
            detail: detail,
            severity: blanket ? AlertSeverityLevel.Critical : AlertSeverityLevel.Warning,
            shortMessage: shortMessage,
            /* The count of stale rules is a genuine whole number (AlertMetricClassifier renders it as a
               count); the healthy bound is 0. */
            numericCurrentValue: stale.Count,
            numericThresholdValue: 0,
            cancellationToken,
            context: null,
            /* The ONE condition that decides its own mute rather than asking the shared seam, because the
               seam returns a single boolean over every rule and cannot say which rule answered — see
               FindExplicitMute. Always non-null, so the seam is never consulted for this metric. */
            muted: FindExplicitMute(rules, now) is not null);
    }

    /// <summary>
    /// The one mute rule that deliberately silences this alert, or null — the EXPLICIT/incidental split that
    /// gives the condition an off switch without letting it switch itself off (#3348).
    ///
    /// <para><b>Why it cannot just ask the shared seam.</b> <c>_isAlertMuted</c> answers one boolean over
    /// every rule, so it cannot say WHICH rule answered. A rule that constrains nothing matches every alert
    /// on the store, this one included, and it is also the shape with the largest blast radius — so an
    /// affirmative seam answer is as easily a fleet-wide silence as a decision about this condition, and
    /// honoring it would lose the report precisely where it matters most. The surviving history row is the
    /// surface nobody reads without already suspecting the mute, which is the blind spot rather than a fix
    /// for it.</para>
    ///
    /// <para><b>What "explicit" means, and why it is the matcher's own answer rather than an assertion.</b>
    /// The metric dimension is EXACT full-string equality (<see cref="MuteRule.NamesMetric"/>, the same
    /// comparison <see cref="MuteRule.MatchesAt"/> applies) — unlike the four <c>*Pattern</c> dimensions
    /// there is no substring, glob or regex form of a metric constraint. So a rule either spells
    /// <see cref="StaleMuteMetric"/> out or does not constrain metrics at all, and no rule can match this
    /// alert incidentally while looking deliberate. A pattern that happened to cover the name would NOT
    /// count and cannot arise: there is no such shape to write.</para>
    ///
    /// <para><b>Self-suppression stays impossible by construction, not by care.</b> The only input to the
    /// decision is a rule that names this metric, and a blanket rule names nothing — so a blanket mute
    /// cannot reach this decision at all, whatever else it silences. The narrowing is also strictly
    /// one-directional: an explicitly-naming rule still has to pass the FULL matcher against this alert's
    /// real context, so a rule naming the metric but scoped to some monitored server does not suppress a
    /// fleet-level condition whose server is the store's label (<see cref="StoreServerLabel"/>, or the
    /// opted-in <c>peers.storeName</c> — #3500's mute-rule coupling: the context below carries the SAME label
    /// the fired row does, so a rule scoped to either spelling matches exactly the rows that spelling names),
    /// and one carrying a database or wait pattern does not either — this alert has no such dimension to
    /// match.</para>
    ///
    /// <para>Judged on the evaluator's injected clock via <see cref="MuteRule.MatchesAt"/>, so the rule's
    /// expiry is read on the same instant the staleness ages are, and an operator's explicit mute lapses
    /// exactly when its bound says. A muted alert is still RECORDED — the history row lands every re-fire,
    /// naming the very rule that silenced the channels, and <c>get_mute_rules</c> lists it with its reason.
    /// That is the audit trail an operator gets for this decision, and it is what makes the decision
    /// answerable rather than invisible.</para>
    /// </summary>
    private MuteRule? FindExplicitMute(IReadOnlyList<MuteRule> rules, DateTime now)
    {
        var context = new AlertMuteContext
        {
            ServerName = _storeLabel,
            MetricName = StaleMuteMetric
        };

        foreach (var rule in rules)
        {
            if (rule is null || !rule.NamesMetric(StaleMuteMetric))
            {
                continue;
            }

            if (rule.MatchesAt(context, now))
            {
                return rule;
            }
        }

        return null;
    }

    /// <summary>
    /// Renders the stale-mute alert's one-line summary and its multi-line <c>detail_text</c>. Each rule
    /// occupies its own line led by <c>"- Rule &lt;id&gt;"</c>, which cannot be read as a mute-context label
    /// by <see cref="AlertMuteContext.PopulateFromDetailText"/>; the operator-authored reason and the
    /// rendered match summary are newline-stripped and capped through
    /// <see cref="CustomAlertEvaluator.SanitizeDisplayText"/> — the SAME sanitizer #3304 uses — because
    /// either one could otherwise carry a forged label line. The list is capped at
    /// <see cref="MaxListedStaleMuteRules"/> with a "+N more" tail.
    /// </summary>
    private static (string ShortMessage, string Detail) RenderStaleMuteRules(
        List<MuteRule> stale, DateTime nowUtc, bool blanket)
    {
        var oldestDays = (nowUtc - stale[0].CreatedAtUtc).TotalDays;
        var shortMessage = string.Create(CultureInfo.InvariantCulture,
            $"{stale.Count} mute rule(s) have been suppressing alerts with no expiry for over {StaleMuteAge.TotalDays:F0} days (oldest {oldestDays:F0} days)");

        var sb = new StringBuilder();
        sb.Append(shortMessage).Append('.');
        sb.Append(
            blanket
                ? " At least one of them constrains nothing, so it suppresses EVERY alert on this store -"
                  + " this store reads healthy because nothing is being reported, not because nothing is wrong."
                : " Each one hides a specific alert.");
        sb.Append(
            " A mute is a deliberate blind spot and nothing else in this product reports that one exists, so"
            + " the question this alert asks is whether the reason each rule was created for is still true."
            + " Nothing has been un-muted: a silent expiry would flood the delivery channel unannounced,"
            + " which is usually why the rule was made permanent in the first place. Review them with"
            + " get_mute_rules and delete or re-scope the ones whose reason has passed.");

        var listed = 0;
        foreach (var rule in stale)
        {
            if (listed >= MaxListedStaleMuteRules)
            {
                break;
            }

            var ageDays = (nowUtc - rule.CreatedAtUtc).TotalDays;
            var summary = CustomAlertEvaluator.SanitizeDisplayText(rule.Summary, MaxStaleMuteSummaryLength);
            var reason = CustomAlertEvaluator.SanitizeDisplayText(rule.Reason, MaxStaleMuteReasonLength);

            /* Leading "- Rule <id>" never matches a PopulateFromDetailText label; the two operator-authored
               values on the line are sanitized above. */
            sb.Append("\n- Rule ").Append(CustomAlertEvaluator.SanitizeDisplayText(rule.Id, 64))
              .Append(string.Create(CultureInfo.InvariantCulture, $": {ageDays:F0} days old, never expires, matches "))
              .Append(summary.Length == 0 ? "(matches all alerts)" : summary);
            if (reason.Length > 0)
            {
                sb.Append(" [reason: ").Append(reason).Append(']');
            }

            listed++;
        }

        var remaining = stale.Count - listed;
        if (remaining > 0)
        {
            sb.Append("\n+ ").Append(remaining.ToString(CultureInfo.InvariantCulture)).Append(" more (see get_mute_rules).");
        }

        return (shortMessage, sb.ToString());
    }

    /* ---------------- managed store settings needing attention (fleet-level, polled — #4215) ---------------- */

    /// <summary>
    /// What DarlingWorker knows, once per sweep tick, about this managed store's <c>darling-managed.conf</c>
    /// outcome (#4215). <paramref name="IsManagedStore"/> false means a bring-your-own store or a non-Windows host:
    /// nothing here was ever written by this service, so the alert never fires and every other field is
    /// meaningless. <paramref name="UsedLastGoodConf"/> and <paramref name="HandEdited"/> are THIS START's
    /// in-process facts (<see cref="DarlingManagedPostgres.LastStartUsedLastGoodManagedConf"/> and
    /// <see cref="ManagedConfWriteResult.HandEdited"/>) — nothing persists them, so they are re-derived from
    /// the writer every start, the same way <see cref="StoreUpgradeReport"/> already is.
    /// <paramref name="RejectedSettingNames"/> is the one fact that IS store-backed: every
    /// <see cref="HostSettingVerdict.RejectedValue"/> row <c>collect.managed_conf_verdicts</c> (V146) is
    /// currently holding, read fresh each tick since a rejected value fixed by a later start replaces that
    /// row without this process restarting. <c>null</c> means the read FAILED this tick — unknown, not
    /// empty — so the evaluator neither fires nor resolves on this condition alone and instead keeps
    /// whatever the family's rejected-settings state already was (the read's
    /// own worker-side catch used to collapse a failure to an empty list, which made one bad read able to
    /// write a false "Store Settings Resolved" when rejected settings were the only condition standing).
    /// </summary>
    internal sealed record StoreSettingsReport(
        bool IsManagedStore,
        bool UsedLastGoodConf,
        bool HandEdited,
        IReadOnlyList<string>? RejectedSettingNames,
        ManagedConfMigrationOutcome? Verification = null);

    private readonly ConcurrentDictionary<string, bool> _activeStoreSettings = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastStoreSettingsAlert = new();

    /// <summary>The fixed key for the fleet-level store-settings edge (not a real server); non-numeric so the
    /// deliverer's #1236 int.TryParse override no-ops on it, like <see cref="StaleMuteKey"/>.</summary>
    private const string StoreSettingsKey = "storesettings";

    /// <summary>The alert metric name the store-settings self-alert fires under (#4215). A WEBHOOK AUTOMATION
    /// KEY like its siblings, so it is a const and must stay stable across releases. Classified as a count
    /// metric by <c>AlertMetricClassifier</c> (the value is the number of conditions in force).</summary>
    internal const string StoreSettingsMetric = "Store Settings Need Attention";

    /// <summary>The resolution title recorded when none of the four conditions holds any more. Carries a
    /// recognized resolution suffix ("Resolved") so the shared <c>AlertMetricClassifier.IsResolution</c>
    /// styles it green.</summary>
    internal const string StoreSettingsResolvedMetric = "Store Settings Resolved";

    /// <summary>How often a standing store-settings condition re-states itself — the <see cref="StaleMuteRefire"/>
    /// reasoning exactly: none of the four facts moves inside a run (only a restart changes any of them), so
    /// re-stating on the shared per-cycle cooldown would just repeat the same sentence every sweep.</summary>
    internal static readonly TimeSpan StoreSettingsRefire = TimeSpan.FromDays(1);

    /// <summary>
    /// Isolating wrapper for the fleet-level store-settings self-alert (#4215), mirroring
    /// <see cref="EvaluateStaleMuteRulesAsync"/>. The worker calls THIS; tests call the isolated
    /// <see cref="ApplyStoreSettingsAsync"/> directly.
    /// </summary>
    public async Task EvaluateStoreSettingsAsync(StoreSettingsReport report, CancellationToken cancellationToken)
    {
        try
        {
            await ApplyStoreSettingsAsync(report, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: the report is a parameter DarlingWorker already
               built (its one store read, the rejected-verdict list, is isolated at its own call site); this
               method performs no store read of its own. */
            _logger?.LogError("Store settings self-alert failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Edge-applies the "a managed store's settings need an operator's attention" condition (#4215, plus
    /// #4336's failed-verification condition): fires while any of four independent facts about THIS start holds — <paramref
    /// name="report"/>'s <c>UsedLastGoodConf</c> (the freshly rendered file failed <c>postgres -C</c>
    /// validation, or the write itself failed, and this start ran on the last file that proved it could start
    /// PostgreSQL), <c>HandEdited</c> (an operator's hand edit of <c>darling-managed.conf</c> is kept in
    /// force rather than overwritten), one or more stored verdicts being <see
    /// cref="HostSettingVerdict.RejectedValue"/> (PostgreSQL itself refused a value this host's own formula
    /// derived), or <c>Verification</c>'s <c>Status</c> being <see cref="ManagedConfVerificationStatus.Failed"/>
    /// (the after-write snapshot did not match <c>pg_file_settings</c>, and the backup or previous verified
    /// <c>darling-managed.conf</c> was restored). <see cref="HostSettingVerdict.PendingRestart"/> and
    /// <see cref="HostSettingVerdict.StaleAfterHardwareChange"/> do NOT fire this — a value waiting on a
    /// restart it will cleanly apply, or one merely different from what today's hardware would now derive, is
    /// not evidence anything is WRONG the way a fallback, a kept override or an outright rejection is.
    /// <see cref="ManagedConfVerificationStatus.Unknown"/> does not fire this alone either, by the same rule
    /// as a null <c>RejectedSettingNames</c>.
    ///
    /// <para>Never fires on a bring-your-own store or off Windows (<paramref name="report"/>'s
    /// <c>IsManagedStore</c> false): nothing here was ever written by this service, so there is no fact to
    /// report.</para>
    ///
    /// <para>A STANDING condition like its siblings: fire on entry, re-state per
    /// <see cref="StoreSettingsRefire"/> while any fact still holds — the CURRENT set is rendered every
    /// re-fire, so a rejected value fixed by a later start clears from the very next tick's report — and ONE
    /// resolution when none does. Gated on the master alerts switch. Internal so it pins directly with a
    /// recording deliverer and a controllable clock.</para>
    /// </summary>
    internal async Task ApplyStoreSettingsAsync(StoreSettingsReport report, CancellationToken cancellationToken)
    {
        if (report is null || !_settings.AlertsEnabled || !report.IsManagedStore)
        {
            return;
        }

        var now = _utcNow();
        var reasons = new List<string>();
        if (report.UsedLastGoodConf)
        {
            reasons.Add(
                "this start ran PostgreSQL on the last-good copy of darling-managed.conf because the freshly "
                + "rendered file failed postgres -C validation (or could not be written)");
        }

        if (report.HandEdited)
        {
            reasons.Add("darling-managed.conf is hand-edited, and the edit is being kept in force rather than overwritten");
        }

        /* #4336: a failed verification is a fourth, independent condition. Failed means a
           mismatch was found between the after-snapshot and pg_file_settings and the restore already ran
           (postgresql.conf when a backup path exists, otherwise the previous verified darling-managed.conf).
           This condition always states plainly when Status is Failed; Unknown is handled below with the
           other unknown-read fact. */
        if (report.Verification is { Status: ManagedConfVerificationStatus.Failed } verification)
        {
            var mismatchedList = string.Join(", ", verification.MismatchedKeys);
            var restoredFrom = verification.BackupPath is not null
                ? FormattableString.Invariant($"postgresql.conf was restored from {verification.BackupPath}")
                : "the previous verified darling-managed.conf was restored";
            var runningNote = verification.Step == ManagedConfMigrationStep.B
                ? "; the running server keeps the new values until its next restart"
                : string.Empty;
            reasons.Add(FormattableString.Invariant(
                $"verifying darling-managed.conf failed (step {verification.Step}): {mismatchedList} did not match pg_file_settings; {restoredFrom}{runningNote}"));
        }

        /* #4215, extended to the verification read (#4336): null means the rejected-settings read FAILED this tick, and Verification's
           Status being Unknown means the before/after snapshot itself could not be taken — both UNKNOWN, not
           empty. An unknown condition neither fires nor resolves on its own; it keeps the family's CURRENT
           state and lets the other conditions decide. If any of those is already true, the family is firing
           regardless of what the unknown read says, so fall through to the normal fire/re-state path below
           without ever stating a fact we didn't read. If none is true, there is nothing else to decide this
           tick: return without touching _activeStoreSettings or _lastStoreSettingsAlert, so an active alert
           stays active (no resolve, no stale re-fire) and an inactive family stays inactive (no fire). A
           successful read — including one that comes back empty — always states exactly what it found. */
        var otherConditionsActive = reasons.Count > 0;
        var anyUnknown = report.RejectedSettingNames is null
            || report.Verification?.Status == ManagedConfVerificationStatus.Unknown;

        if (report.RejectedSettingNames is null)
        {
            if (!otherConditionsActive)
            {
                return;
            }
        }
        else if (report.RejectedSettingNames.Count > 0)
        {
            var rejectedList = string.Join(", ", report.RejectedSettingNames);
            reasons.Add(string.Create(CultureInfo.InvariantCulture,
                $"PostgreSQL rejected the managed value for {report.RejectedSettingNames.Count} owned setting(s): {rejectedList}"));
        }

        if (anyUnknown && reasons.Count == 0)
        {
            return;
        }

        if (reasons.Count == 0)
        {
            if (_activeStoreSettings.TryRemove(StoreSettingsKey, out var was) && was)
            {
                _lastStoreSettingsAlert.TryRemove(StoreSettingsKey, out _);
                await RecordResolutionAsync(new AlertResolution(
                    StoreKey(StoreSettingsKey), _storeLabel, StoreSettingsMetric, StoreSettingsResolvedMetric,
                    "darling-managed.conf started clean, carries no kept-in-force hand edit, and PostgreSQL "
                    + "rejects none of its owned settings"), cancellationToken);
            }

            return;
        }

        _activeStoreSettings[StoreSettingsKey] = true;

        /* Standing condition: fire on entry, re-state only per StoreSettingsRefire while any fact still
           holds — its OWN interval rather than the shared cooldown, the StaleMuteRefire reasoning: none of
           these four facts moves faster than a restart. */
        if (_lastStoreSettingsAlert.TryGetValue(StoreSettingsKey, out var lastFired)
            && now - lastFired < StoreSettingsRefire)
        {
            return;
        }

        _lastStoreSettingsAlert[StoreSettingsKey] = now;

        var detail = string.Join(". ", reasons) + ". Run --check-settings for the full picture.";
        var shortMessage = FormattableString.Invariant($"{reasons.Count} managed-store setting condition(s) need attention");

        await FireAsync(
            StoreKey(StoreSettingsKey), _storeLabel, StoreSettingsMetric,
            currentValue: reasons.Count.ToString(CultureInfo.InvariantCulture),
            thresholdValue: "0",
            detail: detail,
            severity: AlertSeverityLevel.Warning,
            shortMessage: shortMessage,
            /* The count of conditions in force is a genuine whole number; the healthy bound is 0 — the
               "Custom Alert Rules Unhealthy" / "Stale Mute Rules" shape. */
            numericCurrentValue: reasons.Count,
            numericThresholdValue: 0,
            cancellationToken);
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
        bool WithoutRollbackCopy,
        /* #3927: what a FAILED upgrade actually put back, read only when Succeeded is false. The two data
           directory flags are exclusive: pg_upgrade's hard-link rename of the old cluster's control file was
           undone, or the directory could not be put back at all. The defaults are the clean revert, which is
           what every failure report meant before these existed. */
        bool RuntimeReverted = true,
        bool ControlFileRestored = false,
        bool DataDirectoryNotRestored = false);

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
                    StoreKey(StoreUpgradeKey), _storeLabel, StoreUpgradeMetric,
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

            /* #3927: a failure may claim only what it actually put back. This used to say "reverted ... collecting
               normally, no data was lost, because the pre-upgrade data directory is never modified until the
               upgrade succeeds" for every failure. In hard-link mode that last clause stops being true once
               pg_upgrade begins linking and renames the old cluster's pg_control, and a revert refused under a
               server the upgrade could not stop is not a revert. Each shape states its own facts and nothing
               more. */
            string aftermath;
            string shortAftermath;
            if (report.DataDirectoryNotRestored)
            {
                /* No store starts in this shape, so this text cannot actually reach anyone: the alert engine
                   needs the store. It is written anyway, because a false claim that is only safe while it is
                   unreachable is one refactor away from being delivered. */
                aftermath =
                    "The store CANNOT start on either runtime until its pre-upgrade data directory is put back by hand. The upgrade had changed it " +
                    "(in hard-link mode pg_upgrade renames the old cluster's global\\pg_control to global\\pg_control.old, and a failed directory swap can leave the whole directory under its retained name), " +
                    "and putting it back automatically failed. The data is intact on disk and nothing needs restoring from a backup. The service log's CRITICAL entries name the exact file or directory to move" +
                    (report.RuntimeReverted ? "." : ", and the previous PostgreSQL runtime has to be put back by hand as well.");
                shortAftermath = "the store CANNOT start until its data directory is put back by hand";
            }
            else if (!report.RuntimeReverted)
            {
                /* The one not-reverted shape that can deliver this at all: the service is up, so it attached to a
                   server that was already running, because the binaries it would start itself cannot open the
                   store. The next start has no such server to lean on. */
                aftermath =
                    $"The previous PostgreSQL {report.FromMajor} runtime could NOT be put back, so the service's pg-runtime\\pgsql still holds the PostgreSQL {report.ToMajor} binaries, which cannot open this store. The store's data is intact. " +
                    "Collection is running only because a PostgreSQL server was already running on the store's data directory (most likely the old cluster this upgrade started and could not stop) and the service attached to it. The NEXT service start will fail. " +
                    "Before restarting the service, stop that server, then move pg-runtime\\pgsql aside and move the rescued runtime in pg-runtime-prev\\pgsql into its place. The service log's CRITICAL entries name the exact paths.";
                shortAftermath = "runtime NOT reverted, act before the next restart";
            }
            else
            {
                aftermath =
                    $"The store reverted to PostgreSQL {report.FromMajor} and is collecting normally, and no data was lost" +
                    (report.ControlFileRestored
                        ? ". One thing had to be undone first: the upgrade ran in hard-link mode, and pg_upgrade had already renamed the old cluster's global\\pg_control to global\\pg_control.old, which it does so the old cluster cannot be started while the two share files. That rename was undone before the revert, and the new cluster was never started outside pg_upgrade, so the old cluster is intact. "
                        : ", because the upgrade had not modified the pre-upgrade data directory. ") +
                    "The service will NOT retry this same package automatically, so the store stays on its current major until someone acts. " +
                    "Investigate before the next release: a store that cannot move forward accumulates the version drift this machinery exists to end.";
                shortAftermath = "reverted, still running";
            }

            await FireAsync(
                StoreKey(StoreUpgradeKey), _storeLabel, StoreUpgradeMetric,
                $"PostgreSQL {report.FromMajor} (upgrade failed)", $"PostgreSQL {report.ToMajor}",
                detail: $"The monitor's own store FAILED to upgrade from PostgreSQL {report.FromMajor} to {report.ToMajor}, at step '{report.FailedStep}': {report.FailureMessage} " +
                    aftermath,
                severity: AlertSeverityLevel.Critical,
                shortMessage: $"store upgrade to PostgreSQL {report.ToMajor} FAILED at {report.FailedStep} — {shortAftermath}",
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
            /* NOT counted by #3013's swallowed-read counter: the report is a parameter; no store read happens here. */
            _logger?.LogError("Store runtime upgrade self-alert failed: {Message}", ex.Message);
        }
    }

    /* ---------------- the store's TimescaleDB extension (#3908) ---------------- */

    /// <summary>
    /// Fleet-level key for the store's TimescaleDB extension, beside <see cref="StoreUpgradeKey"/>: the same
    /// metric, a different condition, so a start that upgrades the PostgreSQL major and then cannot move the
    /// extension raises both.
    /// </summary>
    private const string StoreTimescaleKey = "storetimescale";

    /// <summary>
    /// What one service start found about the store's TimescaleDB extension (#3908): the update to the runtime's
    /// version failed (<paramref name="Failed"/>), or the extension is behind that version after start and nothing
    /// moved it. <paramref name="FromVersion"/> is what the store is on, null when it could not be read. A
    /// platform-neutral copy of the Windows-only bootstrap's outcome, like <see cref="StoreUpgradeReport"/>.
    /// </summary>
    internal sealed record StoreTimescaleReport(bool Failed, string? FromVersion, string? ToVersion, string? FailureMessage);

    /// <summary>
    /// Reports a store whose TimescaleDB extension is not the runtime's version (#3908), ONCE per service start,
    /// with the discipline of <see cref="EvaluateStoreUpgradeAsync"/>: a start's outcome is an event, fired when
    /// the alert engine first exists and never re-evaluated. Under the same metric as the major upgrade, because
    /// it is the same family and the same operator action. CRITICAL either way: the store runs and collects, but
    /// on an older extension than the release was built and tested with, and a runtime moves TimescaleDB for a
    /// reason, typically a published advisory against the old version.
    /// </summary>
    public async Task EvaluateStoreTimescaleAsync(StoreTimescaleReport report, CancellationToken cancellationToken)
    {
        if (report is null || !_settings.AlertsEnabled)
        {
            return;
        }

        try
        {
            var from = report.FromVersion is null ? "its current TimescaleDB" : $"TimescaleDB {report.FromVersion}";
            var to = report.ToVersion is null ? "the runtime's TimescaleDB" : $"TimescaleDB {report.ToVersion}";
            var reason = string.IsNullOrWhiteSpace(report.FailureMessage)
                ? string.Empty
                : $" Reason: {report.FailureMessage.Trim().TrimEnd('.')}.";

            var state = report.Failed
                ? $"The store is running and collecting on {from}, whose library this runtime still carries, so nothing is down. The update is retried on the next service start."
                : $"The store is running and collecting on {from}. Nothing moved it this start. It moves the next time this service starts the store itself, before anything can connect; a server started by something else, or one this service adopted, keeps this version until then.";

            await FireAsync(
                StoreKey(StoreTimescaleKey), _storeLabel, StoreUpgradeMetric,
                from, to,
                detail: $"The monitor's own store {(report.Failed ? "could not move" : "has not moved")} from {from} to {to}, the version its runtime ships.{reason} {state} " +
                    "Until it moves, the store runs an older extension than the one this release was built and tested with.",
                severity: AlertSeverityLevel.Critical,
                shortMessage: report.Failed
                    ? $"store {to} update FAILED, still running on {from}"
                    : $"store is on {from}, runtime ships {to}",
                /* Versions are identities, not quantities (#1881): they stay in the text. */
                numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: the report is a parameter; no store read happens here. */
            _logger?.LogError("Store TimescaleDB self-alert failed: {Message}", ex.Message);
        }
    }

    /* ---------------- web dashboard TLS certificate expiry (#3514) ---------------- */

    /// <summary>
    /// What the web host knows about its served TLS certificate, carried out to the worker's alert sweep — a
    /// platform-neutral copy of the loaded certificate's facts so the alert path never touches an X.509 type.
    /// <paramref name="Configured"/> is false when there is no LAN TLS certificate to watch (loopback-only, no
    /// <c>tls</c> block, or an unusable one); the other fields are meaningful only when it is true.
    /// <paramref name="RefusedNotYetValid"/> is the host's own load-time verdict (#3517): it judged
    /// <paramref name="NotBeforeUtc"/> still ahead of the clock, refused the certificate, and bound loopback-only
    /// — a decision it does not revisit until its next start, which is why it travels as a flag and is never
    /// re-derived here from the date.
    /// </summary>
    internal sealed record WebTlsCertReport(
        bool Configured,
        DateTimeOffset NotBeforeUtc,
        DateTimeOffset NotAfterUtc,
        string Subject,
        string Thumbprint,
        bool RefusedNotYetValid);

    /// <summary>
    /// The isolating entry point the worker's sweep calls for the web-dashboard TLS certificate expiry
    /// self-alert (#3514) — the fleet-level twin of <see cref="EvaluateStaleMuteRulesAsync"/>. Wraps
    /// <see cref="ApplyWebTlsCertificateAsync"/> in the same failure isolation the sibling store-alerts use, so
    /// a throwing pre-deliver mute check can never propagate out of the collection sweep. Cancellation still
    /// propagates.
    /// </summary>
    public async Task EvaluateWebTlsCertificateAsync(WebTlsCertReport report, CancellationToken cancellationToken)
    {
        try
        {
            await ApplyWebTlsCertificateAsync(report, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: the report is a parameter from the web host's
               in-memory WebTlsCertificateState publish, and this method performs no store read. */
            _logger?.LogError("Web TLS certificate self-alert failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Edge-applies the "the web dashboard's served TLS certificate is expiring" condition (#3514).
    ///
    /// <para><b>Why this exists.</b> When the dashboard is LAN-exposed with a certificate, its expiry was
    /// surfaced only two ways — a startup-log warning inside <see cref="WebTlsCertWarnWindow"/> and the
    /// <c>--status</c> line — both of which require someone to look, on a HEADLESS service that can run for
    /// months without a restart. The certificate is loaded ONCE at start, so as the clock crosses the window
    /// nothing re-fires and the symptom is the dashboard silently dropping to loopback-only the day it lapses.
    /// This re-reads the served certificate's fixed expiry against the evaluator's clock every sweep, so it
    /// warns 30 days out and escalates to Critical once lapsed WITHOUT a restart — the Retention Held /
    /// stale-mute shape: a correct, deliberate posture whose failure reads as silence.</para>
    ///
    /// <para><b>Severity follows what has already happened.</b> Inside the window but not yet lapsed is a
    /// WARNING — the dashboard still serves and there is time to renew. Lapsed is CRITICAL: an expired
    /// certificate fails every TLS handshake, so the LAN dashboard is already unreachable and binds
    /// loopback-only on the next restart.</para>
    ///
    /// <para><b>The not-yet-valid arm (#3517).</b> A certificate whose <c>NotBefore</c> was still ahead when
    /// the host loaded it — a skewed clock, or a certificate minted for a future rotation — is refused by the
    /// host and the dashboard is loopback-only from the start. Its <c>NotAfter</c> is far out, so on the
    /// expiry test alone it read as the healthiest certificate in the fleet and the degrade raised nothing
    /// but a startup log line. This arm fires the SAME family at CRITICAL — the dashboard is exactly as
    /// unreachable as it is when expired — under the same key and metric, so an operator's mute rule and the
    /// deliverer's dedup treat it as the one condition it is: "the configured certificate is not being
    /// served". It fires on the host's carried verdict, NOT on <c>NotBefore</c> against the clock, because
    /// the host does not re-decide when the date passes: it stays loopback-only until it is restarted, and a
    /// date-derived arm would have resolved the alert about a dashboard that was still down.</para>
    ///
    /// <para>A STANDING condition like its siblings: fire on entry, re-state per <see cref="WebTlsCertRefire"/>
    /// while it holds, and ONE resolution when the served certificate is healthy again (renewed past the
    /// window) or TLS is no longer configured — the not-yet-valid arm shares that resolution: the host
    /// re-publishes a usable verdict on its next successful start (a fresh evaluator, so no resolution row is
    /// written, the #3514 in-place-renewal finding), or clears the snapshot when the dashboard is stopped
    /// (the <c>Configured=false</c> arm, which does resolve). Gated on the master alerts switch. Internal so it
    /// pins directly with a recording deliverer and a controllable clock.</para>
    /// </summary>
    internal async Task ApplyWebTlsCertificateAsync(WebTlsCertReport report, CancellationToken cancellationToken)
    {
        if (report is null || !_settings.AlertsEnabled)
        {
            return;
        }

        var now = _utcNow();

        /* The host's verdict, not the clock's: see the method summary. Meaningful only when configured. */
        var refusedNotYetValid = report.Configured && report.RefusedNotYetValid;

        /* Healthy is either "no certificate to watch" or "being served, with more than the warning window
           still to run". The subtraction is DateTime-on-DateTime so it is a pure TimeSpan and never trips the
           DateTimeOffset(...) Kind guard on a test-injected clock. */
        var healthy =
            !report.Configured
            || (!refusedNotYetValid && report.NotAfterUtc.UtcDateTime - now > WebTlsCertWarnWindow);

        if (healthy)
        {
            if (_activeWebTlsCert.TryRemove(WebTlsCertKey, out var was) && was)
            {
                _lastWebTlsCertAlert.TryRemove(WebTlsCertKey, out _);
                /* The configured-and-healthy line names BOTH facts the family alerts on — served, and outside
                   the window — because the active alert it clears may have been either arm (#3517): a
                   dashboard toggled off and on within one supervisor tick re-publishes a now-usable
                   certificate before the sweep ever sees the null, so this is the line a cured
                   not-yet-valid refusal resolves with too. */
                await RecordResolutionAsync(new AlertResolution(
                    StoreKey(WebTlsCertKey), _storeLabel, WebTlsCertExpiryMetric, WebTlsCertRenewedMetric,
                    report.Configured
                        ? "The web dashboard's TLS certificate is being served and is outside the expiry window"
                        : "The web dashboard is no longer serving a TLS certificate to watch"), cancellationToken);
            }

            return;
        }

        _activeWebTlsCert[WebTlsCertKey] = true;

        /* Standing condition: fire on entry, re-state only per WebTlsCertRefire while it holds — its OWN
           interval rather than the shared cooldown, for the StaleMuteRefire reason (a fixed date measured
           against the clock, identical every sweep). */
        if (_lastWebTlsCertAlert.TryGetValue(WebTlsCertKey, out var lastFired)
            && now - lastFired < WebTlsCertRefire)
        {
            return;
        }

        _lastWebTlsCertAlert[WebTlsCertKey] = now;

        var expired = report.NotAfterUtc.UtcDateTime <= now;
        var (shortMessage, detail, currentValue) = RenderWebTlsCert(report, now, expired, refusedNotYetValid);

        await FireAsync(
            StoreKey(WebTlsCertKey), _storeLabel, WebTlsCertExpiryMetric,
            currentValue: currentValue,
            /* The not-yet-valid arm has no window to name — the bar it failed is "valid now". */
            thresholdValue: refusedNotYetValid && !expired
                ? "valid at service start"
                : $"{Hosting.DarlingWebTls.ExpiryWarningDays} days",
            detail: detail,
            /* Critical for BOTH refusals: expired and not-yet-valid leave the LAN dashboard equally unreachable. */
            severity: expired || refusedNotYetValid ? AlertSeverityLevel.Critical : AlertSeverityLevel.Warning,
            shortMessage: shortMessage,
            /* State-only: an expiry is a date, not a quantity — see WebTlsCertExpiryMetric. */
            numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
            cancellationToken);
    }

    /// <summary>Renders the (shortMessage, detail, currentValue) for the web TLS certificate alert. The
    /// subject and thumbprint match the web host's own startup log line, so an operator can tie the alert to
    /// the certificate it named. Pure but for the caller's clock; pinned by tests.
    ///
    /// <para>Expired outranks not-yet-valid when both hold (a refused-at-start certificate the process then
    /// outlived): fixing the clock cannot bring an expired certificate back, so that is the fact to lead
    /// with; the not-yet-valid text below is for the case a clock fix or the right certificate plus a restart
    /// actually cures.</para></summary>
    private static (string ShortMessage, string Detail, string CurrentValue) RenderWebTlsCert(
        WebTlsCertReport report, DateTime now, bool expired, bool refusedNotYetValid)
    {
        var notAfter = report.NotAfterUtc.UtcDateTime;
        var certRef = $"Certificate: subject {report.Subject}, thumbprint {report.Thumbprint}.";

        if (refusedNotYetValid && !expired)
        {
            var notBefore = report.NotBeforeUtc.UtcDateTime;
            var currentValue = $"not valid until {notBefore:u}; not being served";
            var shortMessage =
                $"web dashboard TLS certificate NOT YET VALID (valid from {notBefore:u}) — LAN dashboard is loopback-only";

            /* Two tenses, because the operator reads this on the alert channel at some later hour: while the
               window is still ahead, the clock is the likely culprit and the date is what to check it
               against; once the window has opened, the only thing still wrong is that this process decided
               before it did — and it will not re-decide without a restart, which is the one fact that would
               otherwise surprise them. */
            var clockLine = now < notBefore
                ? $"The window opens {notBefore:u}: if that is in the past by any wall clock you trust, this host's clock "
                  + "is behind; if it is genuinely ahead, the certificate installed is one issued for a future rotation."
                : $"The window opened {notBefore:u}, after the service started — the host judged the certificate once, at load, "
                  + "and stays loopback-only on that verdict until it is restarted.";
            var detail =
                $"The web dashboard's configured TLS certificate was not yet valid when the service started (not valid "
                + $"until {notBefore:u}), so the host refused to serve it and the LAN dashboard is bound LOOPBACK-ONLY — "
                + $"unreachable from the network, and it will not fall back to plain HTTP. {clockLine} Correct the system "
                + "clock or install the currently-valid certificate, then restart the service so the host loads it "
                + $"again. {certRef}";
            return (shortMessage, detail, currentValue);
        }

        if (expired)
        {
            var agoDays = Math.Max(0, (int)Math.Floor((now - notAfter).TotalDays));
            var currentValue = $"expired {notAfter:u}";
            var shortMessage =
                $"web dashboard TLS certificate EXPIRED {notAfter:u} ({agoDays} day{(agoDays == 1 ? string.Empty : "s")} ago)";
            var detail =
                $"The web dashboard's TLS certificate expired on {notAfter:u}. An expired certificate fails every TLS "
                + "handshake, so the LAN dashboard is unreachable now and binds loopback-only on the next service restart. "
                + $"Install a renewed certificate and restart the service. {certRef}";
            return (shortMessage, detail, currentValue);
        }

        var days = Math.Max(0, (int)Math.Ceiling((notAfter - now).TotalDays));
        var plural = days == 1 ? string.Empty : "s";
        var current = $"expires {notAfter:u} (in {days} day{plural})";
        var shortMsg = $"web dashboard TLS certificate expires in {days} day{plural} ({notAfter:u})";
        var det =
            $"The web dashboard's TLS certificate expires on {notAfter:u}, in {days} day{plural}. When it lapses the LAN "
            + "dashboard stops serving (it fails closed to loopback-only, never plain HTTP), so renew it and restart the "
            + $"service before then. {certRef}";
        return (shortMsg, det, current);
    }

    /// <summary>
    /// The isolating entry point the worker's hourly store background-job health sweep calls — the
    /// fleet-level twin of <see cref="EvaluateDiskPressureAsync"/>. Wraps
    /// <see cref="ApplyPolicyJobsStuckAsync"/> in the SAME failure isolation the sibling store-alerts use, so
    /// a throwing seam — the pre-deliver mute check, or the caller-supplied re-arm delegate — can never
    /// propagate out of the (otherwise un-guarded) collection sweep loop and stop collection for the whole
    /// fleet. Cancellation still propagates.
    ///
    /// <para>#3816 changed the parameter from the stuck LIST to the whole <see cref="StorePolicyJobHealth"/>
    /// reading, because two of the four things this pass now does are about the jobs that are FINE: the
    /// unconditional census line, and the failure arm over every job's counters.</para>
    /// </summary>
    public async Task EvaluatePolicyJobsAsync(
        StorePolicyJobHealth reading,
        Func<long, Task<bool>> rearmAsync,
        CancellationToken cancellationToken)
    {
        try
        {
            await ApplyPolicyJobsStuckAsync(reading, rearmAsync, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: the reading is a parameter; the read that
               produces it is counted in DarlingWorker.EvaluateCompressionJobHealthAsync. */
            _logger?.LogError("Store policy-job health self-alert failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// The isolating entry point for the #2136 Store Job Over Cadence check — rides the worker's hourly
    /// compression-job health sweep (same connection, same Timescale gate). Same failure isolation as
    /// <see cref="EvaluatePolicyJobsAsync"/>; cancellation still propagates.
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
            /* NOT counted by #3013's swallowed-read counter: the readings are a parameter; the read that produces
               them is counted in DarlingWorker.EvaluateCompressionJobHealthAsync. */
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
    ///
    /// <para><b>The remedy names one exception, and does not hedge for everyone else (#3060).</b> "Extend
    /// the job's schedule_interval" is right for a compression or retention policy and actively wrong for a
    /// continuous-aggregate refresh, whose interval is also its <c>end_offset</c> — an operator following it
    /// there alters what the store materializes in order to quiet an alert. The branch is
    /// <see cref="TimescaleSupport.ScheduleIntervalDoublesAsEndOffset"/>, keyed on the policy proc, so the
    /// one family that cannot take the advice is told the lever that does work while the other three keep
    /// the concrete sentence. Softening it for all four instead would have made every alert vaguer to fix
    /// one of them.</para>
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
                        StoreKey(JobCadenceKeyPrefix + key), _storeLabel, JobCadenceMetric,
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
                            "'background_job') to see the trend, and " +
                            (TimescaleSupport.ScheduleIntervalDoublesAsEndOffset(job.JobName)
                                ? "either reduce raw volume or scale the store host. Do NOT widen this job's " +
                                  "schedule_interval: on a continuous-aggregate refresh policy it is also the " +
                                  "aggregate's end_offset, so widening it changes what the refresh " +
                                  "materializes — a wider still-filling tail is left unmaterialized, and on " +
                                  "the hourly tier the Query Store backfill horizon derived from it shortens " +
                                  "too. Narrow the refresh window or re-phase the grid instead."
                                : "either reduce raw volume, extend the job's schedule_interval deliberately, " +
                                  "or scale the store host."),
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
                    StoreKey(JobCadenceKeyPrefix + key), _storeLabel, JobCadenceMetric,
                    "Store Job Cadence Recovered",
                    /* The message names the store through the label too (#3500): an opted-in store's
                       resolution prose must not call it by the constant its own rows no longer carry. */
                    $"{_storeLabel}: {label} is back under {warnPercent}% of its schedule interval"), cancellationToken);
            }
        }
    }

    /// <summary>
    /// The isolating entry point for the #2813 Retention Held check — rides the worker's hourly
    /// compression-job health sweep (same connection, same Timescale gate) like its #2136 sibling. Same
    /// failure isolation as <see cref="EvaluateStoreJobCadenceAsync"/>; cancellation still propagates.
    /// </summary>
    public async Task EvaluateRetentionHoldsAsync(
        IReadOnlyList<RetentionHoldReading> policies, CancellationToken cancellationToken)
    {
        try
        {
            await ApplyRetentionHoldsAsync(policies, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: the policies are a parameter; the read that produces
               them is counted in DarlingWorker.EvaluateCompressionJobHealthAsync. */
            _logger?.LogError("Retention-held self-alert failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Applies the fleet-level Retention Held condition (#2813): a retention policy the #1680/#1877 coverage
    /// gate has PAUSED, whose tier has as a result grown past its own configured horizon by
    /// the store's configured warning ratio or more.
    ///
    /// <para><b>Both halves are required, and that is the whole design.</b> Paused alone is normal —
    /// <see cref="TimescaleSupport.EnsureRetentionPoliciesAsync"/> deliberately creates every policy paused
    /// (there is no window in which TimescaleDB would not run a new policy's first check immediately), so
    /// alerting on the flag would fire on every fresh store at every start. Over-horizon alone is normal
    /// too: retention drops whole CHUNKS, so a 4-day policy with 1-day chunks legitimately holds ~5 days.
    /// It is the CONJUNCTION that is unambiguous — the gate is holding this policy AND the tier has already
    /// grown well past what it was meant to keep.</para>
    ///
    /// <para><b>Why a ratio and not a hold duration.</b> Nothing records when a policy was paused, so the
    /// duration is not knowable without persisting state. The ratio measures the same thing from the
    /// consequence end and is strictly more useful: it is the number an operator checks by hand, it is what
    /// makes the cost legible, and it self-scales with the horizon so one threshold serves a 4-day raw tier
    /// and a 35-day baseline tier alike.</para>
    ///
    /// <para>Tiers: WARNING at the store's <c>retention_hold_warn_ratio</c>, CRITICAL at its
    /// <c>retention_hold_critical_ratio</c> (#3297, V119), read live through
    /// <see cref="_retentionHoldWarnRatio"/> / <see cref="_retentionHoldCriticalRatio"/> and defaulting to
    /// <see cref="RetentionHoldWarnRatio"/> / <see cref="RetentionHoldCriticalRatio"/> — the production
    /// incident that motivated this sat at 4.5x (18 days held under a 4-day policy for 16 days) and would
    /// have read CRITICAL on the shipped pair. Both are read ONCE per pass, so one pass cannot judge some
    /// policies on the old pair and the rest on a reloaded one. A critical tier set BELOW the warning tier
    /// is not corrected: every fire is then Critical and the Warning tier is empty, which is what setting it
    /// there asks for. A STANDING condition
    /// like Store Job Over Cadence: fire once on breach, re-fire only on the alert cooldown while it
    /// persists, one "Retention Hold Cleared" resolution when the policy arms or the tier comes back under
    /// the warning ratio. A policy with no chunks, no measurable horizon, or an unreadable span has no
    /// ratio and is skipped without touching its standing state — no signal, the agent-status discipline.
    /// Gated on the master alerts switch. Internal so it pins directly with a recording deliverer and a
    /// controllable clock.</para>
    ///
    /// <para><b>This check never acts.</b> It cannot arm a policy, and deliberately so: arming a held
    /// policy drops the only copy of history no rollup has materialized, which is exactly what the gate
    /// exists to prevent. The release is a backfill, which is an operator decision; this makes the need for
    /// one visible instead of silent.</para>
    /// </summary>
    internal async Task ApplyRetentionHoldsAsync(
        IReadOnlyList<RetentionHoldReading> policies, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        var now = _utcNow();

        /* #3297: read BOTH tiers ONCE per pass, not per policy. A store reload can hot-swap the settings row
           mid-pass, and re-reading per policy would let one pass judge some policies on the old pair and the
           rest on the new one — a mixed reading no configuration ever held. */
        var warnRatio = _retentionHoldWarnRatio();
        var criticalRatio = _retentionHoldCriticalRatio();

        foreach (var policy in policies)
        {
            var key = policy.JobId.ToString(CultureInfo.InvariantCulture);

            /* No ratio = no signal. An armed policy is the healthy case; a policy with no chunks, no
               horizon, or an unreadable span is unmeasured, not innocent — either way it must not touch
               the standing state, or an unreadable probe would silently "resolve" a real hold. */
            if (policy.OverHorizonRatio is not double ratio)
            {
                if (policy.Armed)
                {
                    await ClearRetentionHoldAsync(key, policy, warnRatio, cancellationToken);
                }

                continue;
            }

            var label = string.IsNullOrEmpty(policy.HypertableName)
                ? $"retention job {key}"
                : $"{policy.HypertableName} retention [{key}]";

            if (!policy.Armed && ratio >= warnRatio)
            {
                _activeRetentionHold[key] = true;
                if (CooldownElapsed(_lastRetentionHoldAlert, key, now))
                {
                    _lastRetentionHoldAlert[key] = now;
                    bool critical = ratio >= criticalRatio;
                    double spanDays = (policy.SpanSeconds ?? 0) / 86400.0;
                    await FireAsync(
                        StoreKey(RetentionHoldKeyPrefix + key), _storeLabel, RetentionHoldMetric,
                        $"{ratio:F1}x its {policy.DropAfter} horizon", $"{warnRatio:F1}x",
                        detail: $"Store {label} is HELD PAUSED by the rollup-coverage gate, and the tier now " +
                            $"holds {spanDays:F1} days across {policy.ChunkCount} chunk(s) against a configured " +
                            $"{policy.DropAfter} horizon ({ratio:F1}x). " +
                            (critical
                                ? "The tier is now several times its intended depth and still growing, so this " +
                                  "is the dominant and still-compounding contributor to store size. "
                                : "The gate is working as designed - it will not let retention drop history a " +
                                  "rollup has never materialized - but the hold has lasted long enough to cost " +
                                  "real disk. ") +
                            "The policy arms ITSELF once its consumer covers everything raw holds: the coverage " +
                            "gate is re-judged at startup and on the running service's hourly maintenance tick " +
                            "(EnsureRetentionPoliciesAsync, #3812), so the remedy is one step - run the " +
                            "--backfill-rollups operator action - and the hold clears by itself within about an " +
                            "hour of the backfill completing. Restart the service only if you want it armed " +
                            "immediately; this alert resolves on the tick after the one that arms it. Do NOT arm " +
                            "the policy by hand: the history it holds exists nowhere else, so arming drops the only " +
                            "copy, which is precisely what the gate prevents - and the hourly pass re-holds a " +
                            "hand-armed policy whose coverage is still short. Check the service log for the " +
                            "'HELD PAUSED' line at startup naming which consumer is short, and for the hourly " +
                            "'Retention re-evaluation:' line, whose 'armed this pass' count is the confirmation.",
                        severity: critical ? AlertSeverityLevel.Critical : AlertSeverityLevel.Warning,
                        shortMessage: $"{label} held at {ratio:F1}x its {policy.DropAfter} horizon",
                        numericCurrentValue: Math.Round(ratio, 2),
                        numericThresholdValue: critical ? criticalRatio : warnRatio,
                        cancellationToken);
                }
            }
            else
            {
                await ClearRetentionHoldAsync(key, policy, warnRatio, cancellationToken);
            }
        }
    }

    /// <summary>Drops one retention hold's standing state and records the resolution, but only if it was
    /// actually standing — so a store where nothing is held writes no resolution rows at all.
    ///
    /// <para><paramref name="warnRatio"/> is handed in rather than read here, and that is the point: the
    /// resolution names the threshold the tier came back under, so reading the seam a second time could
    /// report a ratio that never judged this policy if a store reload landed mid-pass — and a bare constant
    /// would name the shipped default on a store that had tuned it.</para></summary>
    private async Task ClearRetentionHoldAsync(
        string key, RetentionHoldReading policy, double warnRatio, CancellationToken cancellationToken)
    {
        if (!_activeRetentionHold.TryRemove(key, out var was) || !was)
        {
            return;
        }

        var label = string.IsNullOrEmpty(policy.HypertableName)
            ? $"retention job {key}"
            : $"{policy.HypertableName} retention [{key}]";
        var why = policy.Armed
            ? "is armed again - its consumer now covers everything the tier holds"
            : $"is back under {warnRatio:F1}x its {policy.DropAfter} horizon";

        await RecordResolutionAsync(new AlertResolution(
            StoreKey(RetentionHoldKeyPrefix + key), _storeLabel, RetentionHoldMetric,
            RetentionHoldClearedMetric,
            /* Label rather than the constant for the #3500 reason the cadence recovery gives. */
            $"{_storeLabel}: {label} {why}"), cancellationToken);
    }

    /// <summary>
    /// The isolating entry point for the #4299 Raw Purge Over Horizon check — rides the SAME hourly
    /// pass that already reads <see cref="RetentionHoldReading"/>s (<see cref="EvaluateRetentionHoldsAsync"/>'s
    /// sibling). Same failure isolation.
    /// </summary>
    public async Task EvaluateRawPurgeOverHorizonAsync(
        IReadOnlyList<RawPurgeOverHorizonReading> readings, CancellationToken cancellationToken)
    {
        try
        {
            await ApplyRawPurgeOverHorizonAsync(readings, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* NOT counted by #3013's swallowed-read counter: the readings are a parameter; the reads that
               produce them are counted in DarlingWorker.EvaluateCompressionJobHealthAsync. */
            _logger?.LogError("Raw-purge-over-horizon self-alert failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Applies the fleet-level Raw Purge Over Horizon condition (#4299): for a raw relation whose
    /// <see cref="RetentionHoldReading.OverHorizonRatio"/> breaches the SAME warn/critical ratio pair
    /// <see cref="ApplyRetentionHoldsAsync"/> judges non-raw policies on, this fires whenever the LAST
    /// RECORDED trigger outcome (<see cref="TimescaleSupport.RecordRawLastPurgeOutcomeAsync"/>) is not
    /// <c>"ran"</c> — unconditionally on the recorded reason, WHATEVER <c>darling_armed</c> says, because
    /// <c>darling_armed=true</c> only means the coverage
    /// gate would allow a purge — it says nothing about whether the SEPARATE hourly trigger actually ran one
    /// (a hole, a stale epoch or a run failure can all block it even while armed). This is the gap the
    /// existing Retention Held alert (#2813) cannot close: that check reads only the armed flag, so a raw
    /// relation that is armed but whose trigger keeps failing reports healthy there while staying held here.
    ///
    /// <para>A raw relation with no recorded outcome yet (a store that has not run a Periodic pass since this
    /// build shipped) reads as unmeasured and is skipped without touching standing state — the same
    /// agent-status discipline <see cref="ApplyRetentionHoldsAsync"/> follows for an unreadable span.</para>
    ///
    /// <para>The alert text names the recorded outcome in plain words: "repair pending"
    /// (<c>epoch_stale</c>), "a hole in the range" (<c>hole</c>), "not covered" (<c>not_covered</c>), or
    /// "the purge failed" with the SqlState (<c>run_failed</c>). A STANDING condition, same idiom as Retention
    /// Held: fire once on breach, re-fire only on cooldown while it persists, one clearing resolution when a
    /// LATER record says <c>"ran"</c> AND the ratio is back under the warning tier — both conditions, so a
    /// relation that just ran once but is still numerically over-horizon (retention drops whole chunks; the
    /// first successful run after a long hold does not instantly return to under-horizon) does not falsely
    /// clear. Gated on the master alerts switch. Internal so it pins directly with a recording deliverer and
    /// a controllable clock.</para>
    /// </summary>
    internal async Task ApplyRawPurgeOverHorizonAsync(
        IReadOnlyList<RawPurgeOverHorizonReading> readings, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        var now = _utcNow();
        var warnRatio = _retentionHoldWarnRatio();
        var criticalRatio = _retentionHoldCriticalRatio();

        foreach (var reading in readings)
        {
            var key = reading.JobId.ToString(CultureInfo.InvariantCulture);

            if (reading.LastPurgeReadFailed)
            {
                _readFailures?.RecordReadFailure(null, RawPurgeOverHorizonReadName, 0);
                continue;
            }

            var label = string.IsNullOrEmpty(reading.HypertableName)
                ? $"raw retention job {key}"
                : $"{reading.HypertableName} raw retention [{key}]";

            var overHorizon = reading.OverHorizonRatio is double ratio && ratio >= warnRatio;
            var lastRan = string.Equals(reading.LastPurge?.Outcome, "ran", StringComparison.Ordinal);
            var recordStale = reading.LastPurge is { } rec && now - rec.At > RawPurgeRecordStaleAfter;

            if (overHorizon && (!lastRan || recordStale))
            {
                _activeRawPurgeOverHorizon[key] = true;
                if (CooldownElapsed(_lastRawPurgeOverHorizonAlert, key, now))
                {
                    _lastRawPurgeOverHorizonAlert[key] = now;
                    var ratioValue = reading.OverHorizonRatio!.Value;
                    bool critical = ratioValue >= criticalRatio;
                    var reasonText = recordStale
                        ? $"the purge trigger has not recorded a pass since {reading.LastPurge!.At:yyyy-MM-dd HH:mm} UTC"
                        : RawPurgeOutcomeReasonText(reading.LastPurge);
                    await FireAsync(
                        StoreKey(RawPurgeOverHorizonKeyPrefix + key), _storeLabel, RawPurgeOverHorizonMetric,
                        $"{ratioValue:F1}x its {reading.DropAfter} horizon", $"{warnRatio:F1}x",
                        detail: $"Store {label} is {ratioValue:F1}x its configured {reading.DropAfter} horizon, " +
                            $"and the last recorded purge-trigger pass did not run it — {reasonText}. " +
                            (critical
                                ? "The tier is now several times its intended depth and still growing. "
                                : "") +
                            "This is the hourly Periodic trigger's own record (#4299), separate from the " +
                            "Retention Held coverage gate: the relation may already read covered and still " +
                            "not be purging if a hole or a run failure keeps blocking the trigger. Check the " +
                            "service log's 'Raw retention purge for' lines for this relation to see the gate " +
                            "the trigger is failing.",
                        severity: critical ? AlertSeverityLevel.Critical : AlertSeverityLevel.Warning,
                        shortMessage: $"{label} over horizon — {reasonText}",
                        numericCurrentValue: Math.Round(ratioValue, 2),
                        numericThresholdValue: critical ? criticalRatio : warnRatio,
                        cancellationToken);
                }
            }
            else if (_activeRawPurgeOverHorizon.TryRemove(key, out var was) && was)
            {
                await RecordResolutionAsync(new AlertResolution(
                    StoreKey(RawPurgeOverHorizonKeyPrefix + key), _storeLabel, RawPurgeOverHorizonMetric,
                    RawPurgeOverHorizonClearedMetric,
                    $"{_storeLabel}: {label} purged successfully and is back under {warnRatio:F1}x its {reading.DropAfter} horizon"), cancellationToken);
            }
        }
    }

    /// <summary>Plain-words rendering of a <see cref="RawLastPurgeRecord"/>'s outcome: the alert names the
    /// reason, one of repair pending, a hole, not covered, or a failed run (with SqlState).
    /// <c>null</c> (never recorded yet) reads as "not covered" — the trigger has not recorded a run for this
    /// relation at all, which is the same unmeasured-as-not-covered posture the coverage gate itself takes.</summary>
    private static string RawPurgeOutcomeReasonText(RawLastPurgeRecord? lastPurge) => lastPurge?.Outcome switch
    {
        "epoch_stale" => "repair pending (the materialization-hole repair epoch is stale or missing)",
        "hole" => "a hole was found in the range about to be dropped",
        "not_covered" => "not covered (the coverage sweep measured Short or Unknown for it)",
        "no_chunks" => "no chunks to evaluate",
        "run_failed" => $"the purge failed (SqlState {lastPurge!.SqlState ?? "(none)"})",
        "gate_unknown" => "a successor rollup could not be resolved, so coverage could not be confirmed",
        "gate_error" => "the trigger's own gate check failed; the service log's 'Raw retention purge for' warning names the error",
        _ => "not covered (no purge-trigger pass has recorded an outcome for it yet)",
    };

    /// <summary>
    /// #4391: how old a last-purge record can be before the Raw Purge Over Horizon alert stops trusting a
    /// recorded <c>"ran"</c> outcome to mean the trigger is still running. Twice the hourly store-maintenance
    /// tick the trigger rides on (<see cref="DarlingWorker.TriggerRawPurgeCoreAsync"/>), so one missed tick
    /// does not false-fire but two in a row does: an old <c>"ran"</c> record must not keep the alert quiet
    /// forever once the trigger itself has stopped running.
    /// </summary>
    internal static readonly TimeSpan RawPurgeRecordStaleAfter = TimeSpan.FromHours(2);

    /// <summary>
    /// The isolating entry point for the #3783 Store TOAST Slack check — rides the worker's hourly store
    /// self-metrics tick, right after the sweep that wrote the rows it reads, the way the collector-cost
    /// check does. Reads the latest-per-object rows through the SAME reader <c>get_store_metrics</c> uses, so
    /// the percentage the alert judged is the one the tool shows. Same failure isolation as
    /// <see cref="EvaluateCollectorCostAsync"/>: a failed read logs, counts as a swallowed read (#3013) and
    /// skips the tick; cancellation propagates. Master-gated up front so master-off reads nothing.
    /// </summary>
    public async Task EvaluateToastSlackAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        List<Mcp.DarlingStoreMetricsReader.StoreMetricRow> latest;
        var readClock = Stopwatch.StartNew();
        try
        {
            latest = await Mcp.DarlingStoreMetricsReader.GetLatestAsync(postgres, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogDebug(ex, "store TOAST slack evaluation failed after {ElapsedMs} ms", readClock.ElapsedMilliseconds);
            _readFailures?.RecordReadFailure(null, "store TOAST slack self-alert", readClock.ElapsedMilliseconds);
            return;
        }

        await ApplyToastSlackAsync(latest, cancellationToken);
    }

    /// <summary>
    /// Applies the fleet-level Store TOAST Slack condition (#3783) from the latest dimension rows: a payload
    /// dimension whose TOAST file is over <see cref="ToastSlackFileFloorBytes"/> and whose MEASURED utilisation
    /// is under <see cref="ToastSlackUtilisationBarPercent"/> is holding slack that ordinary VACUUM returns to
    /// the table and never to the OS — ~93 GB of it on the store that motivated this — and only
    /// <c>--recompress-plan-dim --vacuum-full</c> compacts it. A STANDING condition (the Retention Held idiom):
    /// fire once on breach, re-fire only after <see cref="ToastSlackRefire"/> while it persists, one "Store TOAST
    /// Slack Cleared" resolution when the utilisation is back over the bar or the file is under the floor.
    /// INFORMATIONAL: fired with no severity override so the declared INFO arm styles it — this is a
    /// maintenance-window decision for the maintainer, and the detail says so in those words. Keyed per
    /// dimension table, so the two dims track and clear independently.
    ///
    /// <para><b>The arm is DORMANT wherever live bytes are not measured, and that is pinned rather than
    /// hoped.</b> <see cref="Mcp.DarlingStoreMetricsReader.ToastFacts.IsSlack"/> is false on a NULL utilisation,
    /// and the utilisation is NULL wherever <c>toast_live_bytes</c> is — which on the shipped store is every
    /// row, because <c>pg_freespacemap</c> is available and not installed and the rung ruled the install the
    /// maintainer's dependency decision. A NULL is "unmeasured", never "fine": it neither fires nor RESOLVES a
    /// standing alert (a store that lost the extension would otherwise announce a reclaim that never ran), so
    /// an unmeasured row leaves the standing state exactly as it found it — the agent-status discipline every
    /// sibling follows. The day the maintainer runs <c>CREATE EXTENSION pg_freespacemap</c>, the next sweep
    /// fills the column and this arm judges real numbers with no further change.</para>
    ///
    /// <para><b>This check never acts.</b> It cannot and must not run the reclaim: <c>VACUUM FULL</c> takes an
    /// ACCESS EXCLUSIVE lock on the store's largest table for the whole rebuild and needs free disk for a
    /// full copy of the live data — the issue is explicit that it runs at the maintainer's word in a
    /// maintenance window. This makes the need visible instead of silent. Gated on the master alerts switch.
    /// Internal so it pins directly with a recording deliverer and a controllable clock.</para>
    /// </summary>
    internal async Task ApplyToastSlackAsync(
        IReadOnlyList<Mcp.DarlingStoreMetricsReader.StoreMetricRow> latest, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        var now = _utcNow();

        foreach (var row in latest)
        {
            var facts = Mcp.DarlingStoreMetricsReader.ToastFacts.For(row);
            if (facts is null)
            {
                continue;
            }

            var key = row.ObjectName;

            /* Unmeasured is unmeasured: no fire, no resolve, standing state untouched. */
            if (facts.UtilisationPercent is not double pct || facts.ToastBytes is not long file)
            {
                continue;
            }

            if (Mcp.DarlingStoreMetricsReader.ToastFacts.IsSlack(file, pct))
            {
                _activeToastSlack[key] = true;
                if (!_lastToastSlackAlert.TryGetValue(key, out var last) || now - last >= ToastSlackRefire)
                {
                    _lastToastSlackAlert[key] = now;
                    var live = facts.ToastLiveBytes ?? 0;
                    var slack = Math.Max(file - live, 0);
                    await FireAsync(
                        StoreKey(ToastSlackKeyPrefix + key), _storeLabel, ToastSlackMetric,
                        $"{pct.ToString("0.0", CultureInfo.InvariantCulture)}% of {FormatGb(file)} TOAST file live",
                        $"{ToastSlackUtilisationBarPercent.ToString("0", CultureInfo.InvariantCulture)}% over {FormatGb(ToastSlackFileFloorBytes)}",
                        detail: $"Store dimension {key}'s TOAST file — where the plan XML / statement text actually lives — is " +
                            $"{FormatGb(file)} on disk and holds {FormatGb(live)} of live data " +
                            $"({pct.ToString("0.0", CultureInfo.InvariantCulture)}%); {FormatGb(slack)} is free space INSIDE the file. " +
                            "That slack is what a cycling delete pattern leaves behind: ordinary VACUUM (and autovacuum) returns " +
                            "those pages to the table for reuse but never to the operating system, so the file sits at its " +
                            "high-water mark until it is rebuilt, and pg_total_relation_size keeps reporting it as store size. " +
                            "The reclaim is the existing operator verb --recompress-plan-dim --vacuum-full, run at the " +
                            "maintainer's word in a maintenance window: VACUUM FULL takes an ACCESS EXCLUSIVE lock on the " +
                            "dimension for the whole rebuild (every write that references it waits) and needs free disk for a " +
                            "full copy of the live data while it runs. The service never runs it by itself; this alert re-states " +
                            "itself once a day while the file stays slack and clears when the rebuild lands. The series is " +
                            "collect.store_metrics (object_kind = 'dimension', toast_bytes / toast_live_bytes) and " +
                            "get_store_metrics publishes toast_utilisation_pct per dimension.",
                        /* No override: the per-metric map's declared INFO arm decides (the digest reasoning). */
                        severity: null,
                        shortMessage: $"{key} TOAST file at {pct.ToString("0.0", CultureInfo.InvariantCulture)}% — --recompress-plan-dim --vacuum-full reclaims the slack; maintenance window",
                        numericCurrentValue: pct,
                        numericThresholdValue: ToastSlackUtilisationBarPercent,
                        cancellationToken);
                }
            }
            else if (_activeToastSlack.TryRemove(key, out var was) && was)
            {
                var why = file <= ToastSlackFileFloorBytes
                    ? $"TOAST file is {FormatGb(file)}, under the {FormatGb(ToastSlackFileFloorBytes)} floor"
                    : $"TOAST file is back at {pct.ToString("0.0", CultureInfo.InvariantCulture)}% live, over the {ToastSlackUtilisationBarPercent.ToString("0", CultureInfo.InvariantCulture)}% bar";
                await RecordResolutionAsync(new AlertResolution(
                    StoreKey(ToastSlackKeyPrefix + key), _storeLabel, ToastSlackMetric,
                    ToastSlackClearedMetric,
                    /* Label rather than the constant for the #3500 reason the cadence recovery gives. */
                    $"{_storeLabel}: {key} {why}"), cancellationToken);
            }
        }
    }

    /// <summary>
    /// The isolating entry point for the #3783 Store Checkpointer Pressure check — rides the same hourly
    /// store self-metrics tick as <see cref="EvaluateToastSlackAsync"/>, right after the sweep wrote the newest
    /// checkpointer row, and differences it against the one before through the SAME reader
    /// <c>get_store_metrics</c> publishes from. Same failure isolation and master gate.
    /// </summary>
    public async Task EvaluateCheckpointerPressureAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        if (!_settings.AlertsEnabled)
        {
            return;
        }

        Mcp.DarlingStoreMetricsReader.CheckpointerReading reading;
        var readClock = Stopwatch.StartNew();
        try
        {
            reading = await Mcp.DarlingStoreMetricsReader.GetCheckpointerAsync(postgres, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogDebug(ex, "store checkpointer pressure evaluation failed after {ElapsedMs} ms", readClock.ElapsedMilliseconds);
            _readFailures?.RecordReadFailure(null, "store checkpointer pressure self-alert", readClock.ElapsedMilliseconds);
            return;
        }

        await ApplyCheckpointerPressureAsync(reading, cancellationToken);
    }

    /// <summary>
    /// Applies the fleet-level Store Checkpointer Pressure condition (#3783) from the store's last measured
    /// checkpointer interval: the sync (fsync) phase held more than <see cref="CheckpointSyncBarMs"/> inside the
    /// interval, OR at least one checkpoint was REQUESTED — forced by WAL volume reaching <c>max_wal_size</c>
    /// rather than by the clock. Three unattributed read kills on a production store in one day all sat inside
    /// checkpoint sync phases of 25.2 s and 14.0 s with nothing recording that the checkpointer had been there;
    /// this is that record, as an alert. A STANDING condition (the Store Job Over Cadence idiom): fire once on
    /// breach, re-fire on the shared alert cooldown while each new interval keeps breaching, one "Store
    /// Checkpointer Pressure Recovered" resolution when an interval reads clean. INFORMATIONAL, fired with no
    /// severity override: the two levers are configuration — #3802's WAL sizing (<c>max_wal_size</c> /
    /// <c>checkpoint_completion_target</c>, landed for the managed store as the v12 postgresql.conf block) and
    /// #3745's refresh slicing (smaller aggregate refreshes write less WAL per tick) — which the maintainer
    /// weighs against the store's disk, not a page.
    ///
    /// <para><b>Only an Observed interval is judged.</b> <see cref="Mcp.DarlingStoreMetricsReader.CheckpointerDeltaStatus.Absent"/>
    /// (no row yet), <c>NoPrevious</c> (one row, nothing to subtract), <c>Reset</c> (the counters went
    /// backwards — <c>pg_stat_reset_shared</c> or a restart between sweeps) and <c>Restarted</c> carry no
    /// measurement, and a non-measurement neither fires nor resolves: the standing state is left as found, the
    /// agent-status discipline. Gated on the master alerts switch. Internal so it pins directly with a recording
    /// deliverer and a controllable clock.</para>
    ///
    /// <para><b>An interval that spans a postmaster restart judges neither arm (#3955).</b> PostgreSQL counts the
    /// shutdown checkpoint as requested and keeps the count across the restart, so every service restart that
    /// stopped the store fired this alert as "WAL-forced" pressure the store did not have; and that checkpoint's
    /// own write and sync phases land in the same counters, where nothing can separate them from the live
    /// checkpoints' (a fast shutdown flushes every dirty buffer at once, with every client already gone, so a
    /// long one is not a stall anyone's read sat inside). The reader therefore calls such an interval
    /// <see cref="Mcp.DarlingStoreMetricsReader.CheckpointerDeltaStatus.Restarted"/> and states no delta, and the
    /// gate below treats it like every other non-measurement. The cost is one skipped hourly interval after each
    /// restart; the next interval is judged normally.</para>
    /// </summary>
    internal async Task ApplyCheckpointerPressureAsync(
        Mcp.DarlingStoreMetricsReader.CheckpointerReading reading, CancellationToken cancellationToken)
    {
        if (reading is null)
        {
            throw new ArgumentNullException(nameof(reading));
        }

        if (!_settings.AlertsEnabled || reading.Status != Mcp.DarlingStoreMetricsReader.CheckpointerDeltaStatus.Observed)
        {
            return;
        }

        var now = _utcNow();
        var syncMs = reading.SyncMs ?? 0;
        var writeMs = reading.WriteMs ?? 0;
        var requested = reading.Requested ?? 0;
        var checkpointCount = reading.CheckpointCount;
        var averageSyncMs = reading.AverageSyncMsPerCheckpoint;
        var intervalSeconds = reading.IntervalSeconds ?? 0;
        var intervalMinutes = (intervalSeconds / 60.0).ToString("0.0", CultureInfo.InvariantCulture);
        var barSeconds = (CheckpointSyncBarMs / 1000.0).ToString("0", CultureInfo.InvariantCulture);

        if (reading.IsPressure)
        {
            _activeCheckpointerPressure[CheckpointerKey] = true;
            if (CooldownElapsed(_lastCheckpointerPressureAlert, CheckpointerKey, now))
            {
                _lastCheckpointerPressureAlert[CheckpointerKey] = now;
                var writeSeconds = (writeMs / 1000.0).ToString("0.0", CultureInfo.InvariantCulture);
                var averageOverBar = averageSyncMs is double avg && avg > CheckpointSyncBarMs;
                var averageSecondsText = averageSyncMs is double a ? (a / 1000.0).ToString("0.0", CultureInfo.InvariantCulture) : "unmeasured";
                var arms = (averageOverBar, requested > 0) switch
                {
                    (true, true) => $"average sync {averageSecondsText}s per checkpoint and {requested} WAL-forced checkpoint(s)",
                    (true, false) => $"average sync {averageSecondsText}s per checkpoint",
                    _ => $"{requested} WAL-forced checkpoint(s)",
                };
                await FireAsync(
                    StoreKey(CheckpointerKey), _storeLabel, CheckpointerPressureMetric,
                    $"{arms} over {checkpointCount} checkpoint(s) in {intervalMinutes} min",
                    $"average sync > {barSeconds}s per checkpoint, or any requested checkpoint",
                    detail: $"The store's own checkpointer ran {checkpointCount} checkpoint(s) over the {intervalMinutes} minutes " +
                        $"between the last two self-metrics sweeps, spending {(syncMs / 1000.0).ToString("0.0", CultureInfo.InvariantCulture)}s " +
                        $"total in its sync (fsync) phase and {writeSeconds}s in its write phase — an average of {averageSecondsText}s of sync " +
                        $"per checkpoint — and {requested} of those checkpoints were REQUESTED — forced by WAL volume reaching " +
                        "max_wal_size rather than by checkpoint_timeout. " +
                        (averageOverBar
                            ? $"A per-checkpoint sync average past {barSeconds}s means at least some of this interval's checkpoints ran " +
                              "an I/O stall every reader on the store shares: on one production store, three read kills in a day that " +
                              "nothing else explained all sat inside 25.2 s and 14.0 s sync phases, and the MCP host's read deadline is " +
                              $"the {barSeconds}s this line is drawn at. "
                            : "A requested checkpoint means the store wrote more WAL between checkpoints than max_wal_size " +
                              "allows, so the checkpointer ran early and the next one is closer — checkpoint pressure compounds. ") +
                        "The interval series is collect.store_metrics (object_kind = 'checkpointer'; the stored columns are the " +
                        "server's cumulative counters, and get_store_metrics' checkpointer block publishes the per-interval " +
                        "differences and the same per-checkpoint average). This alert re-fires on the alert cooldown while each " +
                        "new interval breaches and recovers when one reads clean.",
                    /* No override: the per-metric map's declared INFO arm decides (the digest reasoning). */
                    severity: null,
                    shortMessage: $"store checkpointer: {arms} over {checkpointCount} checkpoint(s) in the last {intervalMinutes} min",
                    numericCurrentValue: averageSyncMs is double current ? (long)Math.Round(current) : syncMs,
                    numericThresholdValue: CheckpointSyncBarMs,
                    cancellationToken);
            }
        }
        else if (_activeCheckpointerPressure.TryRemove(CheckpointerKey, out var was) && was)
        {
            var recoveredAverageText = averageSyncMs is double recoveredAvg
                ? (recoveredAvg / 1000.0).ToString("0.0", CultureInfo.InvariantCulture) + "s average sync per checkpoint"
                : "an unmeasured average sync per checkpoint";
            await RecordResolutionAsync(new AlertResolution(
                StoreKey(CheckpointerKey), _storeLabel, CheckpointerPressureMetric,
                CheckpointerPressureRecoveredMetric,
                /* Label rather than the constant for the #3500 reason the cadence recovery gives. */
                $"{_storeLabel}: checkpointer {recoveredAverageText} and {requested} requested checkpoint(s) over {checkpointCount} " +
                $"checkpoint(s) in the last {intervalMinutes} min — under the line again"), cancellationToken);
        }
    }

    /// <summary>
    /// Edge-applies the fleet-level policy-job self-heal machine (#1581, widened to every family by #3816)
    /// from the jobs the worker's read flagged. Per job, in one check:
    /// <list type="bullet">
    /// <item><b>First detection</b> — re-arm it ONCE (<paramref name="rearmAsync"/> → <c>alter_job</c>), then
    /// FIRE its family's "self-healed" alert. If the re-arm itself fails (usually a permission problem the
    /// service cannot fix), go straight to Escalated and FIRE an "auto-re-arm FAILED" alert instead — so we
    /// neither loop alter_job nor stay silent.</item>
    /// <item><b>Re-hang</b> (still stuck after a prior self-heal) — ESCALATE: FIRE a "re-hung after
    /// self-heal" alert and STOP re-arming (looping alter_job on a job that re-hangs is a product-bug signal for
    /// a human).</item>
    /// <item><b>Already escalated</b> — never re-arm; re-fire only on the alert cooldown.</item>
    /// </list>
    /// A job that is no longer stuck has RECOVERED: its state is dropped and one "… Recovered" resolution row
    /// is written under the family it fired as (the sibling conditions' edge shape). Gated on the master
    /// alerts switch. Re-arm happens at most ONCE per job per check (only on the first-detection transition).
    /// Internal so it pins directly with a recording deliverer, a controllable clock, and a fake re-arm
    /// delegate.
    ///
    /// <para><b>#3816 added three things to this machine and changed none of #1581's transitions.</b> The
    /// band (<c>PolicyJobBand</c>) decides the metric name, key prefix, severity and prose per family, so a
    /// dead rollup refresh does not arrive wearing compression's sentence and a mute on one condition is not
    /// a mute on the other; a job that is HELD rather than dead is refused a re-arm by a second gate here as
    /// well as by the reader; every pass writes one unconditional summary line; and the
    /// <c>total_failures</c> arm reports a job that fails WITHOUT ever going <c>-infinity</c>, which no
    /// surface in the product reported before. The state machine, the confirm-read it trusts and the
    /// version-gated re-arm are all unchanged.</para>
    ///
    /// <para><b>What this machine trusts, and what it cost when the trust was misplaced (#3575).</b> This takes
    /// the reading's stuck list as settled fact: first sight re-arms and pages, absence an hour
    /// later posts Recovered. So one false row in the list is not one false message but three — the page, the
    /// idempotent re-arm it narrates, and the recovery of a job that was never unwell — on the alert family
    /// that reports the store's own health. A production store produced exactly that set from a healthy job:
    /// the detector's <c>-infinity</c> arm already guarded on <c>job_status</c>, but TimescaleDB's
    /// <c>job_stats</c> view assembles that status from <c>pg_stat_activity</c> and <c>next_start</c> from the
    /// job-stat row, and for a few milliseconds at either edge of every run the two disagree in exactly the
    /// dead-job shape; the check's sample landed 53 ms into a 63 ms run that succeeded. The fix is upstream
    /// of here and deliberately so: <c>TimescaleSupport.ReadStuckPolicyJobsAsync</c> now confirms a
    /// <c>-infinity</c> trip with a second read five seconds later before a job reaches this list, and the
    /// worker pins its samples to <c>:30</c> past the minute, off the policies' <c>:MM:00</c> run instants.
    /// This method keeps its single-sample semantics — first sight IS first sight — because the input is now
    /// worth that trust, and adding hysteresis here instead would have bought the same protection for an
    /// hour of detection latency on a genuinely dead job.</para>
    ///
    /// <para><b>Which TimescaleDB the dead-job arm's sentence and its re-arm are true on (#3591).</b> "The
    /// scheduler will never run it again" was true of every TimescaleDB below 2.26.4: a persisted
    /// <c>next_start = -infinity</c> was returned to the scheduler as the due time and the job was never due
    /// again — the state #1581 was built against, and the one the re-arm genuinely rescues. Upstream #9360
    /// ("Sanitize <c>DT_NOBEGIN</c> next_start to recover jobs stuck after primary failover", in 2.26.4 and
    /// every 2.27+ release; <c>TimescaleSupport.TimescaleNextStartSanitizedFrom</c>) removed that state. On a
    /// fixed store the only PERSISTENT <c>-infinity</c> is a crashed run — a worker killed between its start
    /// and end marks by a SIGKILL, a crash-restart or a failover — which the scheduler holds in a CRASH
    /// BACKOFF of at least five minutes and, for a compression policy with the default one-hour
    /// <c>retry_period</c>, about an hour (±13 % jitter), and then re-runs by itself. Rows of that kind arrive
    /// here with <see cref="StuckPolicyJob.SchedulerRetries"/> set, and this machine treats them
    /// differently on the evidence of a 2.28.1 rig: re-arming a job in crash backoff does not shorten the
    /// wait, it RESETS it — <c>alter_job</c> refreshes the scheduler's job list and every crash row's backoff
    /// is recomputed from that instant (the un-re-armed sibling crash row moved too), and the re-arm overwrites
    /// the <c>-infinity</c> so the next hourly read would call the job healthy before it had run. A re-arm on
    /// such a row is therefore three untrue messages and a later retry, which is why the first sighting of
    /// one is NOT re-armed and NOT paged: it is logged at Information and remembered as
    /// <c>AwaitingSchedulerRetry</c>. If the same job is still on the arm an hour later — the scheduler's own
    /// retry has not cleared it, because the jittered backoff fell just past the check cadence or because the
    /// job crashed AGAIN and its backoff doubled — it escalates: one Critical page naming a crash the operator
    /// should read the PostgreSQL log for, no re-arm, and the cooldown re-fires of the escalated state. A job
    /// that clears while awaiting the retry is dropped silently, since nothing was paged for it to recover
    /// from. Stores below 2.26.4, and stores whose version could not be read, keep every #1581 semantic
    /// exactly — an unknown version is treated as old, because on an old store the re-arm is the rescue.</para>
    /// </summary>
    internal async Task ApplyPolicyJobsStuckAsync(
        StorePolicyJobHealth reading,
        Func<long, Task<bool>> rearmAsync,
        CancellationToken cancellationToken)
    {
        if (reading is null)
        {
            throw new ArgumentNullException(nameof(reading));
        }

        var census = PolicyJobCensus.Of(reading);

        if (!_settings.AlertsEnabled)
        {
            /* #3756: the summary is unconditional, and "alerts are off" is the one thing it has to say
               differently — a pass that read twelve jobs and deliberately judged none of them must not read
               like a pass that judged twelve and found nothing wrong. */
            LogPolicyJobSummary(census, rearmed: 0, fired: 0, alertsEnabled: false);
            return;
        }

        var now = _utcNow();
        var stillStuck = new HashSet<string>(StringComparer.Ordinal);
        var rearmed = 0;
        var fired = 0;

        foreach (var job in reading.Stuck)
        {
            var key = job.JobId.ToString(CultureInfo.InvariantCulture);
            stillStuck.Add(key);

            /* A family this vocabulary has no sentence for is NOT paged under another family's name (#3816).
               Unreachable from StuckPolicyJobsSql as written — every proc name it admits bands — so reaching
               this is a widening that outran the band table, which is a product defect and says so. Skipping
               is the conservative half of that: an operator who gets "Retention Job Stuck" about a reorder
               policy has been told something false, and a WARNING in the log about an unbanded job is
               findable and harmless. The census still counts it. */
            if (job.Family == TimescaleSupport.StorePolicyJobFamily.Other)
            {
                _logger?.LogWarning(
                    "TimescaleDB policy job {JobId}{Relation} was flagged as stuck ({Reason}) but its family is not one this service has alert text for — not re-armed, not alerted. The detection was widened past the band table; report it (#3816)",
                    key,
                    string.IsNullOrEmpty(job.HypertableName) ? "" : " on " + job.HypertableName,
                    job.Reason);
                continue;
            }

            var band = PolicyJobBand.For(job.Family);
            var label = string.IsNullOrEmpty(job.HypertableName)
                ? $"{band.Noun} {key}"
                : $"{band.Noun} {key} on {job.HypertableName}";

            /* THE SECOND GATE (#3816, guarding #1680/#1877). TimescaleSupport.ClassifyStuckPolicyJobs already
               dropped every HELD row, so this cannot fire today — and it is here because the action on the
               other side of it is DESTRUCTIVE. A held retention policy is paused on purpose until its tier's
               rollups cover raw, the history it holds exists nowhere else, and alter_job(next_start => now())
               on one drops exactly the chunks the coverage gate exists to protect. A regression upstream of
               here — a reader that stops projecting j.scheduled, an upstream view that starts reporting a real
               next_start for a paused job — must not be able to reach a re-arm. It gets a WARNING naming the
               job instead. */
            if (!job.Scheduled)
            {
                /* #4299: one of the three raw retention jobs (TimescaleSupport.RawRelations) is
                   permanently unscheduled by DESIGN, not by the #1680/#1877 coverage gate — its verdict
                   lives in config->>'darling_armed', not in j.scheduled, so reaching here for one of them
                   every hourly pass is expected, not a detector defect. */
                if (!string.IsNullOrEmpty(job.HypertableName) && TimescaleSupport.RawRelations.Contains(job.HypertableName))
                {
                    _logger?.LogDebug(
                        "TimescaleDB {Label} was reported as stuck while NOT scheduled — this is one of the three raw retention jobs, permanently unscheduled by design (#4299), not a #1680/#1877 coverage hold. Nothing re-armed, nothing alerted; no action needed",
                        label);
                    continue;
                }

                _logger?.LogWarning(
                    "TimescaleDB {Label} was reported as stuck while NOT scheduled — a paused policy is a HELD one, not an abandoned job, and re-arming it would drop history the #1680/#1877 coverage gate is protecting. Nothing re-armed, nothing alerted; this is a detector defect worth reporting (#3816)",
                    label);
                continue;
            }

            if (!_policyJobState.TryGetValue(key, out var episode))
            {
                if (job.SchedulerRetries)
                {
                    /* #3591 (and the #3629 measurement behind it): a crash-backoff row on a TimescaleDB whose
                       scheduler re-runs it by itself. Re-arming would RESET that backoff rather than shorten
                       it and would erase the evidence; paging would narrate a self-recovering condition as a
                       rescue. Remember it, say so in the log, and give the scheduler one check cadence.
                       Inherited UNCHANGED by every family #3816 added, and that is the point: the scheduler's
                       crash arm is proc-agnostic, so the argument that made this right for compression is the
                       same argument for a refresh or a retention job. */
                    _policyJobState[key] = new PolicyJobEpisode(PolicyJobHealth.AwaitingSchedulerRetry, job.Family);
                    _logger?.LogInformation(
                        "TimescaleDB {Label} read {Reason}; the scheduler re-runs a crashed job by itself after its crash backoff (at least five minutes, about an hour for a policy's default retry period), and a re-arm would reset that backoff rather than shorten it — not re-armed, not alerted; escalates if still there next check (#3591)",
                        label, job.Reason);
                    continue;
                }

                /* First detection this episode: re-arm ONCE, then alert on the outcome. */
                bool jobWasRearmed = await rearmAsync(job.JobId);
                _lastPolicyJobAlert[key] = now;
                fired++;
                if (jobWasRearmed)
                {
                    rearmed++;
                    _policyJobState[key] = new PolicyJobEpisode(PolicyJobHealth.ReArmed, job.Family);
                    await FireAsync(
                        StoreKey(band.KeyPrefix + key), _storeLabel, band.Metric,
                        job.Reason, "running on schedule",
                        detail: $"TimescaleDB {label} was stuck ({job.Reason}) and has been automatically re-armed " +
                            $"(alter_job next_start => now). {band.Consequence} If it re-hangs the service will " +
                            "escalate and stop auto-re-arming — investigate the TimescaleDB background-worker health. " +
                            "(A next_start of -infinity is permanent only below TimescaleDB 2.26.4; from 2.26.4 on, upstream " +
                            "#9360, the scheduler recovers it by itself and the service leaves such rows to it — if this store " +
                            "is on 2.26.4 or later, its extension version could not be read on this pass.)",
                        severity: band.Severity,
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
                    _policyJobState[key] = new PolicyJobEpisode(PolicyJobHealth.Escalated, job.Family);
                    await FireAsync(
                        StoreKey(band.KeyPrefix + key), _storeLabel, band.Metric,
                        job.Reason, "running on schedule",
                        detail: $"TimescaleDB {label} is stuck ({job.Reason}) and the service could NOT re-arm it — " +
                            "alter_job failed, usually because the store login does not own the job. " +
                            $"{band.Stalled} Re-arm it manually as the store owner " +
                            "(SELECT alter_job(<job_id>, next_start => now())), or grant ownership, then investigate why it hung.",
                        severity: band.Severity,
                        shortMessage: $"{label} stuck — auto-re-arm FAILED",
                        numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                        cancellationToken);
                }
            }
            else if (episode.State == PolicyJobHealth.AwaitingSchedulerRetry)
            {
                /* #3591: an hour on and the scheduler's own retry has not cleared it. Either the jittered backoff
                   landed just past this check, or the job crashed AGAIN and its backoff doubled — both are worth a
                   human reading the PostgreSQL log, and neither is helped by alter_job (which would reset the
                   backoff once more). Escalate: page once now, re-fire on the cooldown, never re-arm. */
                _policyJobState[key] = new PolicyJobEpisode(PolicyJobHealth.Escalated, job.Family);
                _lastPolicyJobAlert[key] = now;
                fired++;
                await FireAsync(
                    StoreKey(band.KeyPrefix + key), _storeLabel, band.Metric,
                    job.Reason, "running on schedule",
                    detail: $"TimescaleDB {label} has sat in the scheduler's crash backoff ({job.Reason}) since at least the previous " +
                        "hourly check, and the scheduler's own retry has not cleared it. A crashed background worker means the " +
                        "PostgreSQL cluster crash-restarted, failed over, or the worker was killed mid-run — read the PostgreSQL log " +
                        "around the job's last_run_started_at for the cause, and check whether it has crashed more than once (each " +
                        "consecutive crash doubles the backoff). The service did NOT re-arm it: on TimescaleDB 2.26.4+ (upstream " +
                        "#9360) alter_job(next_start => now()) against a job in crash backoff resets the backoff instead of " +
                        $"shortening it. {band.Stalled}",
                    severity: band.Severity,
                    shortMessage: $"{label} still in crash backoff an hour on — escalated",
                    numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                    cancellationToken);
            }
            else if (episode.State == PolicyJobHealth.ReArmed)
            {
                /* Still stuck after last check's self-heal = a RE-HANG. Escalate and STOP re-arming (looping
                   alter_job on a job that re-hangs just churns — it is a product-bug signal for a human). */
                _policyJobState[key] = new PolicyJobEpisode(PolicyJobHealth.Escalated, job.Family);
                _lastPolicyJobAlert[key] = now;
                fired++;
                await FireAsync(
                    StoreKey(band.KeyPrefix + key), _storeLabel, band.Metric,
                    job.Reason, "running on schedule",
                    detail: $"TimescaleDB {label} is STILL stuck ({job.Reason}) after an automatic re-arm last cycle — it " +
                        "re-hung, so the service has STOPPED auto-re-arming it. This is a product-bug signal: the background " +
                        $"worker behind this policy is not making progress. {band.Stalled} Investigate the " +
                        "PostgreSQL/TimescaleDB logs and the background-worker settings.",
                    severity: band.Severity,
                    shortMessage: $"{label} re-hung after self-heal — escalated",
                    numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                    cancellationToken);
            }
            else
            {
                /* Already escalated: never re-arm again; keep paging on the cooldown while it stays stuck. */
                if (CooldownElapsed(_lastPolicyJobAlert, key, now))
                {
                    _lastPolicyJobAlert[key] = now;
                    fired++;
                    await FireAsync(
                        StoreKey(band.KeyPrefix + key), _storeLabel, band.Metric,
                        job.Reason, "running on schedule",
                        detail: $"TimescaleDB {label} remains stuck ({job.Reason}) after escalation. {band.Stalled} " +
                            "Manual intervention is required; the service will not auto-re-arm it.",
                        severity: band.Severity,
                        shortMessage: $"{label} still stuck after escalation",
                        numericCurrentValue: StateOnlyValue, numericThresholdValue: StateOnlyValue,
                        cancellationToken);
                }
            }
        }

        /* Recovery: any job we were tracking that is no longer stuck has recovered — drop its state and write
           one resolution row (the sibling conditions' edge shape). ToList() so the removal does not mutate the
           collection under enumeration. */
        foreach (var key in _policyJobState.Keys.ToList())
        {
            if (stillStuck.Contains(key))
            {
                continue;
            }

            _policyJobState.TryRemove(key, out var was);
            _lastPolicyJobAlert.TryRemove(key, out _);

            /* #3816: the resolution must be addressed to the metric name and key its FIRING used, so the
               episode remembers the family. Deriving the family from this pass's rows instead would be
               unanswerable in the one case that matters — the job is not in this pass's stuck list, which is
               why we are here at all — and a resolution under the wrong metric name resolves nothing and
               leaves the real alert row open. */
            var band = PolicyJobBand.For(was.Family);
            if (was.State == PolicyJobHealth.AwaitingSchedulerRetry)
            {
                /* #3591: the scheduler's own retry cleared it and nothing was paged, so there is nothing to
                   resolve — a lone "Recovered" with no preceding alert would be the very message shape #3575
                   removed. The log carries the outcome instead. */
                _logger?.LogInformation(
                    "TimescaleDB {Noun} {JobId} is running on schedule again — the scheduler's own retry cleared its crash backoff; nothing was re-armed or alerted (#3591)",
                    band.Noun,
                    key);
                continue;
            }

            await RecordResolutionAsync(new AlertResolution(
                StoreKey(band.KeyPrefix + key), _storeLabel, band.Metric,
                band.RecoveredMetric,
                $"TimescaleDB {band.Noun} {key} is running on schedule again"), cancellationToken);
        }

        await ApplyPolicyJobFailuresAsync(reading.Jobs, now, cancellationToken);
        LogPolicyJobSummary(census, rearmed, fired, alertsEnabled: true);
    }

    /// <summary>
    /// The <c>total_failures</c> arm (#3816): a policy job whose failure count GREW since the previous hourly
    /// sample and whose last run reports <c>Failed</c>, fired at INFORMATION under
    /// <see cref="PolicyJobFailingMetric"/> with its family's text.
    ///
    /// <para><b>Both halves are required, and each alone is wrong.</b> A grown count alone fires on a job that
    /// failed once and then succeeded — a retry that worked is the scheduler doing its job. A
    /// <c>last_run_status = 'Failed'</c> alone re-fires hourly about ONE failure forever, because the column
    /// keeps saying Failed until the next run: it is a state, not an event. The conjunction reads "it failed
    /// again since we last looked, and it is still failing", which is the only form that is both an event and
    /// a condition.</para>
    ///
    /// <para><b>The first pass after a start establishes the baseline and cannot fire.</b>
    /// <c>total_failures</c> is cumulative since the job was created, so treating an absent baseline as 0
    /// would page about every failure in the store's history the first time the service looked. A
    /// non-measurement neither fires nor resolves — the checkpointer arm's <c>NoPrevious</c> discipline.</para>
    ///
    /// <para><b>A HELD policy is skipped here too.</b> Its counters are frozen because it is not being run, so
    /// it cannot produce a delta; and if one ever did, the sentence to say about it belongs to the hold, not
    /// to this arm.</para>
    ///
    /// <para>Event-shaped, so there is no standing state and no resolution row: what is reported is "N more
    /// failures since the previous sample", which is true when it is said and is not a condition that later
    /// clears. The alert cooldown still gates the re-fire, so a job failing every few minutes cannot outrun
    /// the channel.</para>
    /// </summary>
    private async Task ApplyPolicyJobFailuresAsync(
        IReadOnlyList<PolicyJobRunReading> jobs, DateTime now, CancellationToken cancellationToken)
    {
        /* #3464: the master switch, consulted before the first store write like every sibling apply — the
           baseline below is still advanced so re-enabling alerts does not replay a quiet hour as new failures. */
        if (!_settings.AlertsEnabled)
        {
            foreach (var job in jobs)
            {
                _policyJobFailureBaseline[job.JobId.ToString(CultureInfo.InvariantCulture)] = job.TotalFailures;
            }
            return;
        }

        foreach (var job in jobs)
        {
            var key = job.JobId.ToString(CultureInfo.InvariantCulture);
            var hadBaseline = _policyJobFailureBaseline.TryGetValue(key, out var previous);
            _policyJobFailureBaseline[key] = job.TotalFailures;

            if (job.Held
                || !hadBaseline
                || job.TotalFailures <= previous
                || job.Family == TimescaleSupport.StorePolicyJobFamily.Other)
            {
                continue;
            }

            if (!string.Equals(job.LastRunStatus, "Failed", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!CooldownElapsed(_lastPolicyJobFailureAlert, key, now))
            {
                continue;
            }

            _lastPolicyJobFailureAlert[key] = now;
            var band = PolicyJobBand.For(job.Family);
            var label = string.IsNullOrEmpty(job.RelationName)
                ? $"{band.Noun} {key}"
                : $"{band.Noun} {key} on {job.RelationName}";
            var grew = job.TotalFailures - previous;

            await FireAsync(
                StoreKey(PolicyJobFailingKeyPrefix + key), _storeLabel, PolicyJobFailingMetric,
                $"{grew} new failure(s), {job.TotalFailures} total", "at least one new failure",
                detail: $"TimescaleDB {label} recorded {grew} more failed run(s) since the previous hourly sample " +
                    $"({job.TotalFailures} failures total since the policy was created) and its last run reports Failed. " +
                    $"{band.Consequence} The job is still SCHEDULED — the scheduler is running it and it is failing — " +
                    "which is why neither the dead-job arm (next_start = -infinity) nor the #2136 cadence check (which " +
                    "reads only SUCCESSFUL runs, so it gets quieter as a job gets sicker) reports it. The cause is in the " +
                    "PostgreSQL log around the job's last_run_started_at; the counter series is collect.store_metrics " +
                    "(object_kind = 'background_job', total_failures). Information rather than a page: a job that fails " +
                    "and retries is alive, and a job the scheduler gives up on arrives under its family's own stuck alert.",
                /* No severity override: the DECLARED INFO arm in AlertSeverity.ForMetric decides, the
                   #3443/#3783 idiom. The value IS a measurement — how many more failures — so unlike the
                   stuck family this metric is deliberately not state-only. */
                severity: null,
                shortMessage: $"{label} failed {grew} more time(s) since the last sample",
                numericCurrentValue: grew,
                numericThresholdValue: 1,
                cancellationToken);
        }
    }

    /// <summary>
    /// The one unconditional INFORMATION line every store policy-job evaluation writes (#3816, #3756's
    /// discipline): what was read, per family, and what happened to it.
    ///
    /// <para><b>Why unconditional.</b> Before this, a healthy pass of this check was indistinguishable from a
    /// pass that never ran — the only evidence it produced was an alert, so "no alert" meant either "nothing
    /// is wrong" or "the connection open threw, was swallowed at Debug, and nothing has been checked for
    /// days". #3815's re-probe and #3812's retention re-evaluation each write their own line on this same
    /// tick for exactly that reason; this is its third tenant saying so too.</para>
    ///
    /// <para><b>HELD is reported and never acted on</b>, which is the number that makes the coverage gate
    /// legible on a healthy store: a reader who sees "retention 5, 4 held" knows the gate is holding four
    /// tiers and that nothing in this pass touched them.</para>
    ///
    /// <para><paramref name="fired"/> counts fires ATTEMPTED, not delivered — the mute check and the
    /// per-metric cooldown live downstream inside <c>FireAsync</c>, and a muted alert is still recorded. It is
    /// the count that says what this pass DECIDED, which is what a reader comparing it to the dead/held
    /// numbers wants.</para>
    /// </summary>
    private void LogPolicyJobSummary(PolicyJobCensus census, int rearmed, int fired, bool alertsEnabled)
        => _logger?.LogInformation(
            "Store job health: {Jobs} jobs read (compression {Compression}, refresh {Refresh}, retention {Retention}{Other}), {Dead} dead, {Hung} stuck, {Held} held, {Rearmed} re-armed, {Fired} alerted{Disabled} (#3816)",
            census.Total,
            census.Compression,
            census.Refresh,
            census.Retention,
            census.Other == 0 ? "" : ", other " + census.Other.ToString(CultureInfo.InvariantCulture),
            census.Dead,
            census.Hung,
            census.Held,
            rearmed,
            fired,
            alertsEnabled ? "" : " — alerts are DISABLED, so nothing was judged this pass");

    /// <summary>
    /// What one pass of <c>TimescaleSupport.StuckPolicyJobsSql</c> read, per family, and what the classifier
    /// made of it (#3816) — the summary line's whole content.
    ///
    /// <para>Counted from the pass's OWN census rows rather than from the stuck list, so the two numbers a
    /// reader compares ("12 read, 1 dead") come from one statement and one instant.
    /// <see cref="Dead"/> and <see cref="Hung"/> come from the stuck list's ARM, because which arm fired is
    /// the classifier's answer and not a column: the same <c>-infinity</c> row is a dead job or a run-instant
    /// edge depending on a second read five seconds later, and only the reader knows which.</para>
    /// </summary>
    internal readonly record struct PolicyJobCensus(
        int Total, int Compression, int Refresh, int Retention, int Other, int Held, int Dead, int Hung)
    {
        /// <summary>Counts one pass. A reading with no census rows (the read failed) counts zeroes, which is
        /// how the summary line says "nothing was read" rather than "a store with no jobs".</summary>
        internal static PolicyJobCensus Of(StorePolicyJobHealth reading)
        {
            var jobs = reading.Jobs ?? Array.Empty<PolicyJobRunReading>();
            var stuck = reading.Stuck ?? Array.Empty<StuckPolicyJob>();

            return new PolicyJobCensus(
                jobs.Count,
                jobs.Count(j => j.Family == TimescaleSupport.StorePolicyJobFamily.Compression),
                jobs.Count(j => j.Family == TimescaleSupport.StorePolicyJobFamily.Refresh),
                jobs.Count(j => j.Family == TimescaleSupport.StorePolicyJobFamily.Retention),
                jobs.Count(j => j.Family == TimescaleSupport.StorePolicyJobFamily.Other),
                jobs.Count(j => j.Held),
                stuck.Count(j => j.Arm == StuckPolicyJobArm.NextStartNegativeInfinity),
                stuck.Count(j => j.Arm == StuckPolicyJobArm.RunningPastBound));
        }
    }

    /// <summary>Which state a policy job's episode is in, and the family it fired under (#3816). The family
    /// is remembered because the RECOVERY has to address the metric name and key the firing used, and by then
    /// the job is (by definition) absent from the pass's rows.</summary>
    private readonly record struct PolicyJobEpisode(
        PolicyJobHealth State, TimescaleSupport.StorePolicyJobFamily Family);

    /// <summary>
    /// One family's band (#3816): the persisted metric identity it fires under, its key prefix and resolution
    /// title, its severity, the noun its label is built from, and the two sentences that say what a stall of
    /// THIS family costs.
    ///
    /// <para><b>Why severity differs by family, stated as the choice it is.</b></para>
    /// <list type="bullet">
    /// <item><b>Refresh — CRITICAL, and the worst of the three.</b> The rollup stops advancing, so every
    /// reader of that view is served silently STALE answers (the only one of the three whose consequence is a
    /// wrong answer rather than a cost), the raw tier it summarizes keeps filling, and #1680/#1877's coverage
    /// gate then holds that tier's retention. One dead job, three consequences, and the one an operator meets
    /// first is a retention hold whose named remedy (<c>--backfill-rollups</c>) fixes the symptom and leaves
    /// the cause running.</item>
    /// <item><b>Compression — CRITICAL, unchanged.</b> #1581's judgement byte for byte: an uncompressed
    /// archival tier grows without bound until the disk fills and collection stops for the whole fleet.</item>
    /// <item><b>Retention — WARNING.</b> The store keeps history it was told to drop. That is disk and
    /// nothing else: no reader gets a wrong answer, nothing is lost, and it is the SLOWEST growth of the
    /// three because the tier it stops draining is the compressed one. It is a this-week fact, not a tonight
    /// fact. It does not escalate to Critical by persisting either — an unhealed retention stall does not
    /// become more urgent, it becomes disk, and <see cref="DiskPressureMetric"/> owns that and pages for
    /// it.</item>
    /// </list>
    /// </summary>
    private readonly record struct PolicyJobBand(
        string Metric,
        string RecoveredMetric,
        string KeyPrefix,
        AlertSeverityLevel Severity,
        string Noun,
        string Consequence,
        string Stalled)
    {
        internal static PolicyJobBand For(TimescaleSupport.StorePolicyJobFamily family) => family switch
        {
            TimescaleSupport.StorePolicyJobFamily.Refresh => new PolicyJobBand(
                RefreshJobStuckMetric,
                RefreshJobRecoveredMetric,
                RefreshKeyPrefix,
                AlertSeverityLevel.Critical,
                "refresh job",
                "A rollup has stopped materializing, so every reader of that view is served silently stale " +
                "answers, the raw tier it summarizes keeps filling, and the #1680/#1877 coverage gate will " +
                "HOLD that tier's retention policy for as long as the rollup is short — the retention hold an " +
                "operator sees first is this job's SYMPTOM, and --backfill-rollups clears the symptom while " +
                "leaving this cause in place to do it again.",
                "The rollup stays where it stopped until the refresh runs."),

            TimescaleSupport.StorePolicyJobFamily.Retention => new PolicyJobBand(
                RetentionJobStuckMetric,
                RetentionJobRecoveredMetric,
                RetentionKeyPrefix,
                AlertSeverityLevel.Warning,
                "retention job",
                "The archival tier is stalled: this policy is ARMED and the scheduler has stopped running it, " +
                "so the tier keeps every chunk it was configured to drop and grows for as long as that lasts. " +
                "This is NOT the #2813 Retention Held condition — that one is a policy the rollup-coverage " +
                "gate paused on purpose, which reports scheduled = false and clears with a backfill. This one " +
                "is scheduled and abandoned, and no backfill affects it.",
                "The tier drops nothing until the job runs."),

            /* Compression is the default arm deliberately: it is #1581's family, its text and severity are
               unchanged, and a family added to the enum later that reached here would at worst wear the
               oldest and most conservative of the three sentences. StorePolicyJobFamily.Other never reaches
               this method — both call sites skip it with a WARNING rather than page under a name that does
               not fit. */
            _ => new PolicyJobBand(
                CompressionJobMetric,
                "Compression Job Recovered",
                CompressionKeyPrefix,
                AlertSeverityLevel.Critical,
                "compression job",
                "A stuck compression policy halts the store's archival tier, so uncompressed data grows " +
                "without bound until the disk fills and collection stops for the WHOLE fleet, and a headless " +
                "service has no dashboard to warn you.",
                "Compression stays halted for this relation until the job runs."),
        };
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

    /* #3854: the name each of these reads is given AT THE SEAM is the same one its condition's catch arm
       records a failure under, taken from one constant rather than spelled twice — so a read cannot carry
       one name into the retry count and a different one into the failure count, which is the two-spellings
       hazard AlertReadFailureSurfaceTests pins for the per-server bucket. The granularity is the CATCH
       arm's, not the method's: the agent condition issues two reads inside one try and the AG condition
       two, so each pair shares its condition's name exactly as its failures already do. */

    /// <summary>The counter's name for the collection-stopped condition's read (#3013's spelling).</summary>
    internal const string CollectionSignalsReadName = "collection-health self-alert";

    /// <summary>The counter's name for the capture-down condition's read.</summary>
    internal const string MissingCaptureSessionsReadName = "capture-down self-alert";

    /// <summary>
    /// The counter's name for the agent-not-running condition's TWO reads — the latest status and the
    /// ever-seen-running capability probe share it because they share one catch arm and one condition.
    /// </summary>
    internal const string AgentStatusReadName = "agent-not-running self-alert";

    /// <summary>
    /// The counter's name for the Availability-Group condition's TWO reads, on the same reasoning: the
    /// replica grain and the database grain are separately freshness-gated but failure-isolated together.
    /// </summary>
    internal const string AgStateReadName = "Availability-Group self-alert";

    /// <summary>The counter's name for the Raw Purge Over Horizon condition's last-purge-record read (#4391).</summary>
    internal const string RawPurgeOverHorizonReadName = "raw purge over horizon self-alert";

    /// <summary>
    /// One round trip for the collection-stopped signals: the newest SUCCESS/SKIPPED time across all of the
    /// server's collectors (a SKIPPED is a healthy no-op, matching the viewer's GetCollectionHealthAsync),
    /// plus — over the most recent <paramref name="recentWindow"/> logged runs — the run count and the
    /// success count (feeding the consecutive-failure fast path). Static + parameterized so the gated live
    /// test can seed rows and assert the raw signals directly.
    ///
    /// <para><b>#3496: the recent-N subqueries order by <c>collection_time DESC, log_id DESC</c>, and the
    /// time column must stay FIRST.</b> <c>collection_log</c> is a TimescaleDB hypertable partitioned on
    /// <c>collection_time</c> and carries no index on <c>log_id</c>, so <c>ORDER BY log_id DESC</c> had
    /// exactly one legal plan: append EVERY chunk — decompressing the columnar history — into a top-N sort.
    /// Measured live once the retention horizon filled: ~389,000 rows decompressed and heapsorted across
    /// all 32 chunks to return ten, twice per statement (the status twin repeats the shape), every alert
    /// pass, every 30 seconds, per server. The statement carried its own control: the
    /// <c>MAX(collection_time)</c> arm below — same table, same <c>server_id</c> predicate — resolved in
    /// 0.097 ms, because its sort key is chunk-orderable: ChunkAppend stopped at the newest chunk and 30 of
    /// 32 chunks reported "never executed". Ordering the recent-N arms by the partition column buys the
    /// same early stop, and the property is HORIZON-INDEPENDENT — ChunkAppend stops at the newest chunks no
    /// matter how many chunks the retention horizon accumulates, so a future horizon extension cannot
    /// regress this read back over its deadline. The semantics are identical: a server's <c>log_id</c>
    /// order and its <c>collection_time</c> order agree (<c>CollectionIdGenerator</c> is a
    /// process-monotonic counter re-seeded FORWARD from the clock across restarts), and <c>log_id</c>
    /// stays in the ORDER BY as the deterministic tiebreak within one collection instant. The MAX arm is
    /// deliberately untouched — it is the measured control, and its one residual (a server whose newest
    /// qualifying row is ancient walks deeper before stopping) is the rare case and the right cost to pay
    /// exactly then. This read's growth toward the 90-day retention steady state is what ate the alert
    /// pass's 10 s command deadline margin — a deadline derived from a 1,744.9 ms measured worst case
    /// (<see cref="DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds"/>): the warm mean sat near
    /// 221 ms while the cold/contended excursions clocked 12.0–12.1 s and were swallowed as
    /// <c>instance_read_failures</c>.</para>
    /// </summary>
    internal Task<(DateTime? LastSuccessUtc, int RecentRunCount, int RecentSuccessCount)> ReadCollectionSignalsAsync(
        NpgsqlDataSource postgres, int serverId, int recentWindow, CancellationToken cancellationToken)
        => ReadWithOneRetryAsync(
            token => ReadCollectionSignalsCoreAsync(postgres, serverId, recentWindow, token),
            serverId, CollectionSignalsReadName, cancellationToken);

    /// <summary>The read itself, byte-identical to what shipped before #3854 routed it through the seam.</summary>
    private static async Task<(DateTime? LastSuccessUtc, int RecentRunCount, int RecentSuccessCount)> ReadCollectionSignalsCoreAsync(
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
     FROM (SELECT log_id FROM collection_log WHERE server_id = $1 ORDER BY collection_time DESC, log_id DESC LIMIT $2) r) AS recent_runs,
    (SELECT COUNT(*)
     FROM (SELECT status FROM collection_log WHERE server_id = $1 ORDER BY collection_time DESC, log_id DESC LIMIT $2) r
     WHERE r.status IN ('SUCCESS', 'SKIPPED'))                                       AS recent_success", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
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

    /// <summary>The shipped capture-down statement, a constant so the #3597 pins and the gated plan test read
    /// the text the method executes rather than a copy of it.</summary>
    internal const string MissingCaptureSessionsSql = @"
SELECT x.collector_name
FROM
(
    (SELECT cl.collector_name, cl.status
     FROM collection_log AS cl
     WHERE cl.server_id = $1
     AND   cl.collector_name = 'deadlocks'
     ORDER BY cl.collection_time DESC
     LIMIT 1)
    UNION ALL
    (SELECT cl.collector_name, cl.status
     FROM collection_log AS cl
     WHERE cl.server_id = $1
     AND   cl.collector_name = 'blocked_process_report'
     ORDER BY cl.collection_time DESC
     LIMIT 1)
) AS x
WHERE x.status = 'SESSION_MISSING'
ORDER BY x.collector_name";

    /// <summary>
    /// The blocking/deadlock XE collectors whose LATEST run logged <c>SESSION_MISSING</c> — the session is
    /// absent and couldn't be created, so capture is non-functional even though the tolerant reader "succeeds"
    /// with zero rows. The Darling twin of the Dashboard's <c>GetMissingCaptureSessionsAsync</c>, on Darling's
    /// collector names. Returns the friendly capture names ("Blocking" / "Deadlock").
    ///
    /// <para><b>#3597: one chunk-orderable <c>LIMIT 1</c> per collector, not a window function over the
    /// server's whole history.</b> This read shipped as <c>ROW_NUMBER() OVER (PARTITION BY collector_name
    /// ORDER BY log_id DESC)</c> over every <c>collection_log</c> row the server had for the two collectors —
    /// the #3496 shape, which that fix named and deliberately left: a window function cannot early-stop by
    /// reordering alone. <c>collection_log</c> is a hypertable partitioned on <c>collection_time</c> with no
    /// index on <c>log_id</c>, so the window had exactly one legal plan: decompress EVERY chunk in the
    /// retention horizon for the server, Merge Append them, and number 100 K rows to keep two. Measured on a
    /// rig with 60 days of one server's log across 61 chunks (59 compressed): 9,573 buffers and every chunk
    /// executed, per alert pass, every 30 seconds, per server — the largest read on the pass by two to three
    /// orders of magnitude, and one of the issue's four sites that died at the 10 s deadline while the
    /// interval-hourly refresh starved the store for I/O. The same question asked per collector as
    /// <c>ORDER BY collection_time DESC LIMIT 1</c> lets ChunkAppend order the chunks newest-first and stop
    /// at the first row: 14 buffers, the newest chunk only, 120 of 122 chunk scans never executed — and the
    /// property is horizon-independent, so a longer retention cannot regress it. Two literal arms rather than
    /// a LATERAL over a VALUES list because the collector names are a closed set the alert owns, and a plan
    /// with a literal predicate is the one the gated test can pin.</para>
    ///
    /// <para><b>Why no <c>log_id</c> tiebreak here, when #3496 kept one.</b> The recent-N read orders across ALL
    /// of a server's collectors, where many rows share a collection instant and the id decides among them.
    /// Each arm here is ONE collector on ONE server, and a collector logs one row per run, stamped
    /// <c>DateTime.UtcNow</c> at log time (<c>DarlingObservability.LogCollectionAsync</c>) — two runs of the
    /// same collector on the same server cannot share a microsecond, so there is nothing for a tiebreak to
    /// decide. What it would COST is measured: with <c>, log_id DESC</c> appended, the index on
    /// <c>(server_id, collection_time)</c> cannot serve the second key, and each arm top-N-heapsorts the
    /// server's whole newest chunk (1,158 buffers) instead of stopping at its first hit (5).</para>
    /// </summary>

    internal Task<IReadOnlyList<string>> ReadMissingCaptureSessionsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
        => ReadWithOneRetryAsync(
            token => ReadMissingCaptureSessionsCoreAsync(postgres, serverId, token),
            serverId, MissingCaptureSessionsReadName, cancellationToken);

    /// <summary>The read itself, byte-identical to what shipped before #3854 routed it through the seam.</summary>
    private static async Task<IReadOnlyList<string>> ReadMissingCaptureSessionsCoreAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        var missing = new List<string>();

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(MissingCaptureSessionsSql, connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
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
    internal Task<bool> HasAgentEverBeenSeenRunningAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
        => ReadWithOneRetryAsync(
            token => HasAgentEverBeenSeenRunningCoreAsync(postgres, serverId, token),
            serverId, AgentStatusReadName, cancellationToken);

    /// <summary>The read itself, byte-identical to what shipped before #3854 routed it through the seam.</summary>
    private static async Task<bool> HasAgentEverBeenSeenRunningCoreAsync(
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
)", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
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
    internal Task<(DateTime? CollectionTimeUtc, bool? AgentRunning)> ReadLatestAgentStatusAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
        => ReadWithOneRetryAsync(
            token => ReadLatestAgentStatusCoreAsync(postgres, serverId, token),
            serverId, AgentStatusReadName, cancellationToken);

    /// <summary>The read itself, byte-identical to what shipped before #3854 routed it through the seam.</summary>
    private static async Task<(DateTime? CollectionTimeUtc, bool? AgentRunning)> ReadLatestAgentStatusCoreAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(@"
SELECT agent_running, collection_time
FROM agent_status
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
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
    internal Task<(DateTime? CollectionTimeUtc, IReadOnlyList<AgReplicaReading> Replicas)> ReadLatestAgReplicaStatesAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
        => ReadWithOneRetryAsync(
            token => ReadLatestAgReplicaStatesCoreAsync(postgres, serverId, token),
            serverId, AgStateReadName, cancellationToken);

    /// <summary>The read itself, byte-identical to what shipped before #3854 routed it through the seam.</summary>
    private static async Task<(DateTime? CollectionTimeUtc, IReadOnlyList<AgReplicaReading> Replicas)> ReadLatestAgReplicaStatesCoreAsync(
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
ORDER BY ag_name, replica_server_name", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
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
    internal Task<(DateTime? CollectionTimeUtc, IReadOnlyList<AgDatabaseReading> Databases)> ReadLatestAgDatabaseReplicaStatesAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
        => ReadWithOneRetryAsync(
            token => ReadLatestAgDatabaseReplicaStatesCoreAsync(postgres, serverId, token),
            serverId, AgStateReadName, cancellationToken);

    /// <summary>The read itself, byte-identical to what shipped before #3854 routed it through the seam.</summary>
    private static async Task<(DateTime? CollectionTimeUtc, IReadOnlyList<AgDatabaseReading> Databases)> ReadLatestAgDatabaseReplicaStatesCoreAsync(
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
ORDER BY ag_name, database_name, replica_server_name", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
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
    /// history sees the paired "Detected" then "Cleared" entries. Never email/webhook: a resolution has no
    /// send channel, and the deliverer's fire path is untouched.
    ///
    /// <para>That "no send channel" is stated by <see cref="AlertDelivery.NoChannelApplies"/> rather than
    /// spelled as a delivered row. Saying it with <c>AlertSent: true</c> made <c>alert_sent</c> mean
    /// "delivered" on a fired row and "no channel applies" here, so a reader could not tell the two senses
    /// apart and no aggregate over the column meant anything — the delivered fraction rose with the number
    /// of resolutions. The pairing this method exists for is unaffected: the row is still written, still
    /// carries the resolution title, and still reads as resolved.</para>
    /// </summary>
    public static AlertHistoryRecord BuildResolutionRecord(AlertResolution resolution) => new(
        resolution.ServerKey, resolution.ServerName, resolution.Title,
        CurrentValueText: "resolved", ThresholdValueText: "",
        NumericCurrentValue: null, NumericThresholdValue: null,
        Delivery: AlertDelivery.NoChannelApplies(),
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
    /// <param name="muted">The mute decision, when the caller has ALREADY made it; null (the default, and
    /// every condition but one) asks the shared <c>_isAlertMuted</c> seam for this server and metric, which
    /// is what every sibling wants.
    ///
    /// <para>Only "Stale Mute Rules" decides for itself, and it must, because the seam returns ONE boolean
    /// over every rule and so cannot say WHICH rule answered. A rule that constrains nothing matches every
    /// alert on the store, this one included, so a seam answer of true is as easily a blanket mute as a
    /// decision about this alert — and honoring it would let the condition suppress the only report of its
    /// own subject. That condition scans the rules it already holds for one that NAMES it and passes the
    /// verdict in here instead (#3348). Nothing else may pass this: a caller that hands in a decision it did
    /// not derive from an explicit naming has re-introduced the self-suppression the seam cannot see.</para></param>
    /// <returns>What the deliverer reported the channels did (#3580), or <c>null</c> when it reported
    /// nothing — read by the two daily documents' stamps and ignored by every condition-class caller,
    /// whose lifecycle is edge-driven and owes nothing to a failed send. See
    /// <see cref="IAlertDeliverer.DeliverAndReportAsync"/> for why the report rides a second method.</returns>
    /* The optional context TRAILS the cancellation token so the dozens of existing positional call
       sites stay untouched — only the callers that have discrete facts to carry (#2109: the AG
       database alerts) name it. Same for muted, which defaults to asking the seam like its siblings. */
    private async Task<AlertDelivery?> FireAsync(
        string serverKey, string serverName, string metricName, string currentValue, string thresholdValue,
        string detail, AlertSeverityLevel? severity, string shortMessage,
        double? numericCurrentValue, double? numericThresholdValue, CancellationToken cancellationToken,
        AlertContext? context = null, bool? muted = null)
    {
        /* Same mute treatment as the engine: a muted self-alert is still recorded (flagged muted) but its
           channels are skipped — the deliverer honors AlertOutcome.Muted. A caller that brought its own
           decision does not even ASK, so a throwing Matches() cannot reach it either. */
        bool isMuted = muted
            ?? _isAlertMuted(new AlertMuteContext { ServerName = serverName, MetricName = metricName });

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
                serverName, metricName, severity?.ToString() ?? "Warning", shortMessage, isMuted));

        return await _deliverer.DeliverAndReportAsync(new AlertOutcome(
            serverKey, serverName, metricName, currentValue, thresholdValue,
            Context: context, DetailText: detail,
            NumericCurrentValue: numericCurrentValue, NumericThresholdValue: numericThresholdValue,
            Muted: isMuted, Severity: severity, ShortMessage: shortMessage), cancellationToken);
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
            /* NOT counted by #3013's swallowed-read counter: an audit-row WRITE, not a condition read. */
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
