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

public sealed partial class DarlingWorker
{
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
        MuteRuleService muteRuleService, DarlingAlertDeliverer deliverer,
        PerformanceMonitor.Darling.Service.Investigations.DarlingDeadlockRollup? deadlockRollup = null)
    {
        var postgres = _postgres!;
        var stateStore = new PgAlertStateStore(postgres, _logger);

        /* The mute service is loaded + owned by the caller (RunCollectionLoopAsync) so a control-plane
           reload can re-LoadAsync() the SAME instance and mute the very next sweep (F16). The engine
           binds its IsAlertMuted delegate, which reads the refreshed cache. The deliverer is hoisted by
           the caller and shared with the Stage 4 self-alerts (same delivery/cooldown/restart-replay). */

        var readAdapter =
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
                readFailures: AlertReadFailureCounter.Shared,
                /* #4606: the database-state maintenance sequence's own deadlock-retry log. */
                logger: _logger,
                /* #4659: the same fence the collector runner brackets every Query Store write with. */
                queryStoreWriteFence: _queryStoreWriteFence);

        return new AlertEngine(
            alertSettings,
            readAdapter,
            stateStore,
            deliverer,
            muteRuleService.IsAlertMuted,
            /* #5642: served from the cache the collection body fills, so the alert pass reads the store and nothing
               else. The live read itself is FetchFailedJobsAsync, called by RefreshMsdbCacheAsync. */
            failedJobsFetcher: (serverKey, lookbackMinutes, ct) =>
                Task.FromResult(ServeFailedJobsFromCache(servers, serverKey, lookbackMinutes)),
            /* #3497: the Long-Running Query card's Agent-job name lookup — the same live-msdb seam shape
               as the failed-jobs feed above, and the same degrade discipline: every failure is an empty
               map, so a denied or broken msdb read costs a card its job NAME and never the card. */
            agentJobStepResolver: (serverKey, keys, ct) =>
                Task.FromResult(ServeStepNamesFromCache(servers, serverKey, keys)),
            resolutionCallback: async (resolution, token) =>
            {
                /* #1681: same shared shape as the firing line the engine's funnel writes, so an engine
                   alert's TRIGGERED and RESOLVED halves pair up in the log. */
                _logger.LogInformation("{Line}",
                    AlertFiringLog.Resolved(resolution.ServerName, resolution.Title, resolution.Message));
                /* Stage 4 parity-gap fix: record a resolved-flavored history row so an operator reviewing
                   alert history sees the paired "Detected" then "Cleared/Resolved" entries (the Dashboard
                   records these explicitly; Darling previously only logged them). #5825: the deliverer also sends the
                   clear to the destinations its delivered fire reached when the store sends resolutions, and records
                   the send on the row. A failed send is recorded, not thrown, and RecordAlertAsync is failure-isolated,
                   so neither can break the sweep. */
                await deliverer.DeliverResolutionAsync(resolution, cancellationToken: token);
            },
            logger: _logger,
            /* #5715 part B: a Warning delivered before a restart still lets the Critical through at once. */
            escalationSeedReader: EscalationSeed.ReaderFor(historyStore),
            /* #7068: a standing Volume Free Space or PVS condition is not announced again after a restart. */
            openFireReader: EscalationSeed.OpenFireReaderFor(historyStore),
            /* #3013: the process counter every swallowed condition read is tallied on. Passed explicitly
               rather than defaulted inside the engine so a test constructs its own and cannot pollute it. */
            readFailures: AlertReadFailureCounter.Shared,
            /* #5916: a threshold held for one server or database. Lite passes none. */
            overrides: _thresholdOverrides,
            deadlockRollup: deadlockRollup);
    }

    /// <summary>
    /// Builds this sweep's <see cref="AlertServerSnapshot"/> and runs the engine for one
    /// server with a runtime (#5642: the live one, else the last known one, so a dropped connection is still
    /// evaluated from the store). The CPU pair mirrors what Lite's overview summary carries (the latest
    /// cpu_utilization_stats sample; total = SQL + other-process, null when no SQL sample);
    /// isOnline is always passed true, which is NOT a statement that the server is connected: the only
    /// consumer is the failed-jobs check, which reads the msdb cache and returns empty once the cache is
    /// stale, so a dropped server yields no failed-jobs fire. Suppression is always false
    /// (headless — suppression is an engine INPUT owned by interactive hosts). Failure-isolated:
    /// a failed sweep logs and retries on the next cadence tick, mirroring the collector loop.
    /// </summary>
    private async Task EvaluateAlertsAsync(
        AlertEngine engine, ServerLoopState server, DarlingConfig config, CancellationToken cancellationToken)
    {
        /* #5642: the live runtime, else the last known one, so a server whose connection dropped is still evaluated from
           the store. Null only for a server that never connected or was removed. */
        var runtime = server.AlertRuntime;
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
        DateTime? cpuSampleUtcOnly = null;
        IReadOnlyList<AlertCpuSample>? recentCpuSamples = null;

        var cpuReadClock = Stopwatch.StartNew();
        try
        {
            (sqlCpu, totalCpu, cpuSampleTime, cpuSampleUtcOnly) = await ReadLatestCpuAsync(runtime.ServerId, cancellationToken);

            /* #5642: the recent samples (15 minutes, at most 32, oldest first) the engine feeds through the persistence
               gate one by one. Inside this same counted block on purpose: it is one more read of the same pass step, so a
               failure of either costs this tick its CPU alert and nothing else, and counts once under the latest-CPU name.
               The clock restarts so a fault here records this read's elapsed, not the first read's too. */
            cpuReadClock.Restart();
            recentCpuSamples = await ReadRecentCpuSamplesAsync(runtime.ServerId, DateTime.UtcNow, cancellationToken);
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
                /* Not "connected" (#5642): see the method summary. Gates only the cache-backed failed-jobs check. */
                IsOnline: true,
                SqlCpuPercent: sqlCpu,
                TotalCpuPercent: totalCpu,
                IsAzureSqlDb: runtime.Target.IsAzureSqlDb,
                Suppressed: false,
                /* #3282: the gate counts breaching SAMPLES, not sweeps — the sweep runs every 30 s and this
                   sample advances about once a minute, so without the instant a re-read would count twice.
                   #3744: the row's UTC twin where the store has one, the local stamp before V134 — resolved
                   in ReadLatestCpuAsync, compared for equality by the gate. */
                CpuSampleTimeUtc: cpuSampleTime,
                /* The live store set, by store id: after the first seed the store decides which targets are
                   monitored (darling.json is never rewritten), and a target's name is not unique. */
                SeparatelyMonitoredDatabases: AzureMasterScope.SeparatelyMonitoredDatabases(
                    runtime.Target.IsAzureSqlDb,
                    runtime.ServerId.ToString(CultureInfo.InvariantCulture),
                    runtime.Config.Host,
                    runtime.Config.Database,
                    LiveAlertTargets(_registryState.Read()?.Servers)))
            {
                ServerId = runtime.ServerId,
                /* #5642: the true UTC instant of the latest sample (null on a pre-V134 row), the recent samples, and when
                   this connection began collecting. */
                CpuSampleUtcOnly = cpuSampleUtcOnly,
                RecentCpuSamples = recentCpuSamples,
                WatchingSinceUtc = server.WatchingSinceUtc,
                /* F14: the display name is not unique (a blank name falls back to the host, and two registrations
                   can be typed alike), so the dedup keys collided; the fingerprint adds the store id for
                   exactly the names another registration also carries. */
                ServerNameIsShared = ServerNameIsShared(_registryState.Read(), runtime.Config)
            };

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
    /// The collection time of the row a Tier 0 finding was judged on (#5642): the finding's own database row for
    /// wraparound, its own slot row for slot retention, the winning row for the vacuum horizon. Null when the read
    /// carried none, which gives no age line.
    /// </summary>
    internal static DateTime? Tier0NewestSample(
        PostgresAlertEvaluator.Finding finding,
        IReadOnlyList<PostgresWraparoundAlertInfo>? wraparound,
        PostgresXminHorizonAlertInfo? xmin,
        IReadOnlyList<PostgresSlotAlertInfo>? slots) =>
        finding.MetricName switch
        {
            PostgresAlertEvaluator.WraparoundMetric =>
                wraparound?.FirstOrDefault(w => string.Equals(w.DatabaseName, finding.Subject, StringComparison.Ordinal))?.NewestSampleUtc,
            PostgresAlertEvaluator.XminHorizonMetric => xmin?.NewestSampleUtc,
            PostgresAlertEvaluator.SlotRetentionMetric =>
                slots?.FirstOrDefault(sl => string.Equals(sl.SlotName, finding.Subject, StringComparison.Ordinal))?.NewestSampleUtc,
            _ => null,
        };

    /// <summary>
    /// Runs after a PostgreSQL family's fire (#4795), the twin of <c>AlertEngine.AfterFire</c> (#4752) for the
    /// six families that fire from this file. They stamp their cooldown BEFORE delivery, so an alert whose
    /// every channel failed (<see cref="FailedSendBackoff.EveryChannelFailed"/>) used to be silent for the
    /// whole cooldown. When it did, this counts the failure in <c>_pgFailedSends</c> and back-dates the stamp
    /// so the cooldown opens again after the streak's delay (a minute, doubling, never more than the
    /// cooldown): every family's check is <c>now - last &gt;= cooldown</c>, so
    /// <c>last = now - cooldown + delay</c> opens exactly <c>delay</c> after this fire, and the next sweep
    /// that still sees the condition fires it again. Any other result (delivered, partly delivered, muted,
    /// throttled, folded, unreported) ends the streak and leaves the stamp alone.
    /// <para>Returns true when every channel failed. The families with a second "already reported" marker
    /// (the deadlock and blocking count watermarks, the poison wait collection time) put it back at their own
    /// call site on true; this method only knows the cooldown.</para>
    /// </summary>
    private bool AfterPgFire(
        string family, ConcurrentDictionary<string, DateTime> stamps, string key, DateTime now,
        TimeSpan cooldown, AlertDelivery? delivery) =>
        AfterPgFireCore(_pgFailedSends, _logger, family, stamps, key, now, cooldown, delivery);

    /// <summary>
    /// The body of <see cref="AfterPgFire"/> as an internal static method, so <c>Darling.Tests</c> can drive it
    /// without a whole <see cref="DarlingWorker"/> (the arms that call it are private and read a live store).
    /// The instance method is a one-line forward that threads <c>_pgFailedSends</c> and <c>_logger</c> through.
    /// </summary>
    internal static bool AfterPgFireCore(
        FailedSendBackoff failedSends, ILogger logger, string family,
        ConcurrentDictionary<string, DateTime> stamps, string key, DateTime now,
        TimeSpan cooldown, AlertDelivery? delivery)
    {
        if (!FailedSendBackoff.EveryChannelFailed(delivery))
        {
            failedSends.RecordDelivered(family, key);
            return false;
        }

        var delay = failedSends.RecordFailure(family, key, now, cooldown, out var failures);
        stamps[key] = now - cooldown + delay;
        logger.LogInformation(
            "Every channel failed for {Family} on {Key} (failure {Failures}); trying again in {Delay}",
            family, key, failures, delay);
        return true;
    }

    /// <summary>
    /// The count watermark a Deadlocks or Blocking fire whose every channel failed leaves behind (#4795): the
    /// value from BEFORE the fire, decayed the way <see cref="RollingCountAlertGate"/> decays it (a watermark
    /// above the current count drops to the count). The gate advances the watermark to the fired count, and the
    /// caller saves it, before delivery; left there, the retry sweep would see no new events and the alert
    /// would stay lost until the count rose. At the pre-fire value the retry sees the count above the
    /// watermark and fires again at the same count. The SQL Server twins do the same (#4752).
    /// </summary>
    internal static int PgUnannouncedWatermark(int preFireWatermark, int count) =>
        Math.Min(preFireWatermark, count);

    /// <summary>
    /// Puts a poison wait subject's last-fired-on collection time back to what it was before a fire (#4795):
    /// the prior time when there was one, no entry when the failed fire was the first. The time was recorded
    /// before delivery, and the retry sweep would otherwise read the same collection as already reported and
    /// never fire on it. Mirrors <c>AlertEngine.RestoreAlertedLevel</c>'s prior-or-remove shape.
    /// </summary>
    internal static void RestorePgPoisonCollectionTime(
        ConcurrentDictionary<string, DateTime> collectionTimes, string key, DateTime? prior)
    {
        if (prior is { } time)
        {
            collectionTimes[key] = time;
        }
        else
        {
            collectionTimes.TryRemove(key, out _);
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

                /* #4732: a stamp ahead of the clock — stepped back since it was written, or seeded from history
                   above by a restart after the step — is replaced by this sweep's reading and counted from there. */
                if (LastFiredStamp.TryGet(_lastPostgresAlert, cooldownKey, now, out var last) && now - last < cooldown)
                {
                    continue;
                }

                /* Stamped even when muted, mirroring AlertEngine: a muted alert still consumes its cooldown,
                   so unmuting does not produce a backlog. */
                _lastPostgresAlert[cooldownKey] = now;

                var muted = _isAlertMuted?.Invoke(new AlertMuteContext
                {
                    ServerName = snapshot.ServerName,
                    ServerId = snapshot.ServerId,
                    MetricName = finding.MetricName,
                    /* The subject is the database for wraparound and the slot/holder for the others, which is
                       what a DatabaseName mute rule is written against. */
                    DatabaseName = finding.Subject,
                }) ?? false;

                var delivery = await _alertDeliverer.DeliverAndReportAsync(
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
                        Context: AlertSampleAge.Stamp(
                            new AlertContext
                            {
                                Incidents = new List<AlertIncident>
                                {
                                    new(finding.Subject, new[] { finding.Subject }),
                                },
                            },
                            /* #5642, ruling 6: the age of the row this finding was judged on, counted once at fire
                               time. Wraparound and slot findings are keyed by their own subject (database, slot);
                               the horizon finding comes from the one winning row. */
                            "collection",
                            Tier0NewestSample(finding, wraparound, xmin, slots), now),
                        DetailText: null,
                        finding.NumericCurrentValue,
                        finding.NumericThresholdValue,
                        Muted: muted,
                        finding.Severity,
                        finding.ShortMessage),
                    cancellationToken);
                readClock.Restart();

                /* #4795: the cooldown was stamped above, before the send, so a send that reached no channel
                   would hold this subject for the whole cooldown. Back-date the stamp instead: the subject is
                   tried again after a minute, doubling, never later than the cooldown. This family moves no
                   other marker, so the stamp is all there is to put right. */
                AfterPgFire(finding.MetricName, _lastPostgresAlert, cooldownKey, now, cooldown, delivery);
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

        var alertSettings = new DarlingAlertSettings(config, _logger);

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
                var cooldownElapsed = !LastFiredStamp.TryGet(_lastPgCpuAlert, key, now, out var last)
                    || now - last >= cooldown;
                if (!cooldownElapsed)
                {
                    return;
                }

                _lastPgCpuAlert[key] = now;

                var muted = _isAlertMuted?.Invoke(new AlertMuteContext
                {
                    ServerName = snapshot.ServerName,
                    ServerId = snapshot.ServerId,
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

                var delivery = await _alertDeliverer.DeliverAndReportAsync(
                    new AlertOutcome(
                        key,
                        snapshot.ServerName,
                        metricName,
                        $"{lastCapacityPercent!.Value:F0}%",
                        $"{alertSettings.CpuThresholdPercent}%",
                        Context: AlertSampleAge.Stamp(null, "CPU", reading.SampleTimeUtc, now),
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

                /* #4795: only the cooldown stamp moved before the send. The gate's record (Firing, the
                   streak, the observed-sample watermark) was saved above from the samples, not from the
                   delivery, and it stays Firing, so the retry sweep reaches this same arm as a standing
                   condition and fires once the back-dated stamp opens. Nothing else to put back. */
                AfterPgFire(metricName, _lastPgCpuAlert, key, now, cooldown, delivery);
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

        var alertSettings = new DarlingAlertSettings(config, _logger);

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
            var cooldownElapsed = !LastFiredStamp.TryGet(_lastPgDeadlockAlert, key, now, out var last) || now - last >= cooldown;

            /* #3444: read ONCE, then handed to the gate and the delivered message alike, so the two cannot
               quote different numbers when a store reload swaps config.Alerts mid-evaluation. The rate tiers
               the fire is GRADED on (#3653, below) are read at the same spot for the same reason. */
            var threshold = _thresholdOverrides.ResolveWire(
                DarlingThresholdKeys.PgDeadlockCount, snapshot.ServerKey, null, alertSettings.PgDeadlockCountThreshold);
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
                    ServerId = snapshot.ServerId,
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
                var delivery = await _alertDeliverer.DeliverAndReportAsync(
                    new AlertOutcome(
                        key,
                        snapshot.ServerName,
                        metricName,
                        count.ToString(CultureInfo.InvariantCulture),
                        threshold.ToString(CultureInfo.InvariantCulture),
                        Context: AlertSampleAge.Stamp(
                            new AlertContext
                            {
                                Incidents = rows.Select(BuildPgDeadlockIncident).ToList(),
                            },
                            "deadlock", AlertSampleAge.Newest(rows.Select(r => r.OccurredAtUtc)), now),
                        DetailText: null,
                        NumericCurrentValue: count,
                        NumericThresholdValue: threshold,
                        Muted: muted,
                        Severity: AlertEngine.GradeDeadlockFire(count, rateTiers),
                        ShortMessage: $"{count} {(count == 1 ? "deadlock" : "deadlocks")} in the last hour"),
                    cancellationToken);
                readClock.Restart();

                /* #4795: the cooldown stamp above and the count watermark (moved to this count by the gate,
                   and saved, before the send) were both written ahead of delivery. A send that reached no
                   channel gets the stamp back-dated, and the watermark goes back to the value it had before
                   this fire, in memory and in the saved row: the retry sweep then sees the count above the
                   watermark and fires again at the same count, instead of waiting for the count to rise.
                   The SQL Server twin does the same (#4752). */
                if (AfterPgFire(metricName, _lastPgDeadlockAlert, key, now, cooldown, delivery))
                {
                    var unannouncedWatermark = PgUnannouncedWatermark(watermark, count);
                    _lastAlertedPgDeadlockCount[key] = unannouncedWatermark;
                    if (unannouncedWatermark != decision.Watermark)
                    {
                        await stateStore.SaveEdgeTriggerWatermarkAsync(key, metricName, unannouncedWatermark);
                        readClock.Restart();
                    }
                }
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

        var alertSettings = new DarlingAlertSettings(config, _logger);

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
            var cooldownElapsed = !LastFiredStamp.TryGet(_lastPgBlockingAlert, key, now, out var last) || now - last >= cooldown;

            /* #3444: read ONCE, for the reason EvaluatePgDeadlocksAsync gives at the same spot. */
            var threshold = _thresholdOverrides.ResolveWire(
                DarlingThresholdKeys.PgBlockingCount, snapshot.ServerKey, null, alertSettings.PgBlockingCountThreshold);

            /* #7083: the same page rule as the SQL Server path. A root's event is its chain's worst victim wait;
               the wait bar is the shared blockingWaitSecondsThreshold (and its per-server override). */
            var waitBarSeconds = _thresholdOverrides.ResolveWire(
                "blocking.wait_threshold_seconds", snapshot.ServerKey, null, alertSettings.BlockingWaitSecondsThreshold);
            var worstWaitMs = worstPerRoot.Count == 0 ? 0 : worstPerRoot.Max(r => r.WorstVictimWaitMs);
            var wasActiveBefore = _activePgBlockingAlert.TryGetValue(key, out var activeNow) && activeNow;
            var decision = BlockingPageRule.Evaluate(
                count, threshold, worstWaitMs, waitBarSeconds, watermark, cooldownElapsed, suppressed: false, wasActiveBefore);
            _lastAlertedPgBlockingCount[key] = decision.Watermark;
            if (decision.Watermark != watermark)
            {
                await stateStore.SaveEdgeTriggerWatermarkAsync(key, metricName, decision.Watermark);
                readClock.Restart();
            }

            var wasActive = wasActiveBefore;
            _activePgBlockingAlert[key] = decision.Active;

            if (decision.Fire)
            {
                _lastPgBlockingAlert[key] = now;

                var muted = _isAlertMuted?.Invoke(new AlertMuteContext
                {
                    ServerName = snapshot.ServerName,
                    ServerId = snapshot.ServerId,
                    MetricName = metricName,
                }) ?? false;

                /* #3653 (A8e, the PostgreSQL host): an explicit tier in place of Severity: null, so the row
                   carries what it fired at (through the deliverer's #2090 fold, as the deadlock arm above
                   explains) rather than leaving #3635's by-name replay arm to imply it. Warning, and only
                   Warning — PgBlockingFireSeverity's doc says which bars were considered and why none
                   qualifies as a Critical tier tonight. */
                var delivery = await _alertDeliverer.DeliverAndReportAsync(
                    new AlertOutcome(
                        key,
                        snapshot.ServerName,
                        metricName,
                        count.ToString(CultureInfo.InvariantCulture),
                        threshold.ToString(CultureInfo.InvariantCulture),
                        Context: AlertSampleAge.Stamp(
                            new AlertContext
                            {
                                Incidents = worstPerRoot.Select(BuildPgBlockingIncident).ToList(),
                            },
                            "blocking capture", AlertSampleAge.Newest(worstPerRoot.Select(r => r.CapturedAt)), now),
                        DetailText: null,
                        NumericCurrentValue: count,
                        NumericThresholdValue: threshold,
                        Muted: muted,
                        Severity: PgBlockingFireSeverity,
                        ShortMessage: $"{count} blocking {(count == 1 ? "session" : "sessions")}"),
                    cancellationToken);
                readClock.Restart();

                /* #4795: the deadlock arm explains it — the stamp is back-dated and the count watermark put
                   back to its pre-fire value (in memory and saved) when no channel delivered, so the retry
                   sweep fires again at the same count. */
                if (AfterPgFire(metricName, _lastPgBlockingAlert, key, now, cooldown, delivery))
                {
                    var unannouncedWatermark = PgUnannouncedWatermark(watermark, count);
                    _lastAlertedPgBlockingCount[key] = unannouncedWatermark;
                    if (unannouncedWatermark != decision.Watermark)
                    {
                        await stateStore.SaveEdgeTriggerWatermarkAsync(key, metricName, unannouncedWatermark);
                        readClock.Restart();
                    }
                }
            }
            else if (!decision.Active && wasActive)
            {
                await NotifyPgResolutionAsync(key, snapshot.ServerName, metricName, "Blocking Cleared",
                    BlockingPageRule.ClearedMessage(snapshot.ServerName, threshold, waitBarSeconds));
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

        var alertSettings = new DarlingAlertSettings(config, _logger);

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
                var cooldownElapsed = !LastFiredStamp.TryGet(_lastPgLongRunningQueryAlert, key, now, out var last) || now - last >= cooldown;
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
                    ServerId = snapshot.ServerId,
                    MetricName = metricName,
                    DatabaseName = worst.DatabaseName,
                }) ?? false;

                /* #3653 (A8e, the PostgreSQL host): an explicit tier in place of Severity: null, for the
                   reason the blocking arm above gives. Warning, and only Warning —
                   PgLongRunningQueryFireSeverity's doc says why. */
                var delivery = await _alertDeliverer.DeliverAndReportAsync(
                    new AlertOutcome(
                        key,
                        snapshot.ServerName,
                        metricName,
                        $"{rows.Count} {(rows.Count == 1 ? "query" : "queries")}, longest {elapsedMinutes}m",
                        $"{thresholdMinutes}m",
                        Context: AlertSampleAge.Stamp(
                            new AlertContext
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
                            /* #5642, ruling 6: the capture these rows were read from, counted once at fire time. The
                               same label the SQL Server twin uses for its snapshot. */
                            "query snapshot", AlertSampleAge.Newest(rows.Select(r => r.NewestSampleUtc ?? default)), now),
                        DetailText: null,
                        NumericCurrentValue: elapsedMinutes,
                        NumericThresholdValue: thresholdMinutes,
                        Muted: muted,
                        Severity: PgLongRunningQueryFireSeverity,
                        ShortMessage: $"pid {worst.Pid} running {elapsedMinutes}m — {worst.CommandTag ?? "(unknown)"}"
                            + (worst.DatabaseName is null ? "" : $" on {worst.DatabaseName}")),
                    cancellationToken);
                readClock.Restart();

                /* #4795: a live-state check with no count watermark and no collection-time marker, so the
                   cooldown stamp is the only thing the fire moved. A send that reached no channel gets it
                   back-dated, and the next sweep that still sees a long-running query fires again. */
                AfterPgFire(metricName, _lastPgLongRunningQueryAlert, key, now, cooldown, delivery);
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

        var alertSettings = new DarlingAlertSettings(config, _logger);
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
                        /* #4732: a recorded fire ahead of the clock (it stepped back across the restart) seeds as
                           this sweep's reading, for the cooldown and for the collection floor beside it. The
                           floor would otherwise hold a real collection back until the clock passed the step. */
                        var seededStamp = LastFiredStamp.Settle(seeded.Value, now);
                        _lastPgPoisonWaitAlert[cooldownKey] = seededStamp;
                        _lastPgPoisonWaitCollectionTime[cooldownKey] = seededStamp;
                    }
                }

                /* #2704: only a collection_time newer than the one last fired on counts as a fresh
                   observation. The collector's delivered cadence and the alert cooldown are independent
                   clocks — a cooldown-elapsed re-read of the SAME still-uncollected row is the identical
                   accumulation surfacing twice, not a new observation of a standing condition. */
                var newestCollection = newestCollectionBySubject.TryGetValue(finding.Subject, out var nc)
                    ? nc
                    : DateTime.MinValue;
                /* Whether a time was recorded, and which, is kept for #4795: a fire that reaches no channel
                   puts it back. */
                var hadPriorCollection = _lastPgPoisonWaitCollectionTime.TryGetValue(cooldownKey, out var lastCollection);
                var hasFreshCollection = !hadPriorCollection || newestCollection > lastCollection;
                if (!hasFreshCollection)
                {
                    continue;
                }

                if (LastFiredStamp.TryGet(_lastPgPoisonWaitAlert, cooldownKey, now, out var last) && now - last < cooldown)
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
                    ServerId = snapshot.ServerId,
                    MetricName = finding.MetricName,
                    /* WaitType, not DatabaseName: wait events are instance-wide, and the SQL Server twin's
                       mute rules key on the wait type — the parity metric name only helps if the mute
                       dimension matches too. */
                    WaitType = finding.Subject,
                }) ?? false;

                var delivery = await _alertDeliverer.DeliverAndReportAsync(
                    new AlertOutcome(
                        serverKey,
                        snapshot.ServerName,
                        finding.MetricName,
                        finding.CurrentValue,
                        finding.ThresholdValue,
                        /* The subject as the #1140 incident fingerprint, identity only — same shape and
                           reasoning as the Tier 0 delivery loop above. */
                        Context: AlertSampleAge.Stamp(
                            new AlertContext
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
                            "wait stats",
                            newestCollectionBySubject.TryGetValue(finding.Subject, out var newestWaitCollection) ? newestWaitCollection : null,
                            now),
                        DetailText: null,
                        finding.NumericCurrentValue,
                        finding.NumericThresholdValue,
                        Muted: muted,
                        finding.Severity,
                        finding.ShortMessage),
                    cancellationToken);
                readClock.Restart();

                /* #4795: two markers were written before the send, the cooldown stamp and the collection time
                   this fire counted. A send that reached no channel gets the stamp back-dated, and the
                   collection time goes back to what it was (or to no entry, when this was the first fire for
                   the subject): otherwise the retry sweep reads the same collection as already reported and
                   never fires on it, which is the #2704 guard doing its job on a fire nobody received. */
                if (AfterPgFire(finding.MetricName, _lastPgPoisonWaitAlert, cooldownKey, now, cooldown, delivery))
                {
                    RestorePgPoisonCollectionTime(_lastPgPoisonWaitCollectionTime, cooldownKey, hadPriorCollection ? lastCollection : null);
                }
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
    /// <see cref="DarlingSelfAlertEvaluator.BuildResolutionRecord"/>, written by <see cref="_alertDeliverer"/>'s
    /// resolution send (#5825), which also posts the clear where the delivered fire went when the store sends resolutions. Best-effort: a history-write failure here must
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
            var resolution = new AlertResolution(serverKey, serverName, metricName, title, message);
            /* #5825: through the deliverer, so a delivered fire's clear goes where the fire went. */
            if (_alertDeliverer is not null)
            {
                await _alertDeliverer.DeliverResolutionAsync(resolution);
            }
            else
            {
                await _historyStore.RecordAlertAsync(DarlingSelfAlertEvaluator.BuildResolutionRecord(resolution));
            }
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
                    ? $"victim pid {row.VictimPid}, {row.ParticipantCount} {(row.ParticipantCount == 1 ? "participant" : "participants")}"
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
                $"root pid {row.RootPid} blocking {row.TotalVictims} {(row.TotalVictims == 1 ? "session" : "sessions")}"
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
    /// <para><b>The one-chunk claim holds only for a server with a recent row (#6968).</b> A server whose newest row is old, or that
    /// has none, makes the ordered append visit every chunk between that row and now, and each visited chunk costs a run-time plan
    /// (measured 1,300 buffers for a server five days stale and 4,400 for one with no rows, at 21 daily chunks). So after the first
    /// read of a server the alert pass reads <see cref="LatestCpuSinceSql"/> instead: the same statement bounded below by a
    /// <c>collection_time</c> the previous read returned (<see cref="NewestRowFloor"/>). The bound is a value off the column
    /// itself, so the objections above do not apply: no store clock, no re-framing, and a server that stopped reporting still returns its
    /// last row. This text and <see cref="LatestCpuSinceSql"/> keep the same ORDER BY.</para>
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
SELECT sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time, sample_time_utc, collection_time
FROM cpu_utilization_stats
WHERE server_id = $1
ORDER BY collection_time DESC, sample_time DESC
LIMIT 1";

    /// <summary>
    /// #6968: <see cref="LatestCpuSql"/> bounded below by <c>collection_time</c> $2, a value a previous read of the same server
    /// returned (<see cref="NewestRowFloor"/>), never a clock. The bound lets the planner exclude every older chunk, so the read
    /// opens the newest chunk or two instead of every chunk between a stale or empty server's newest row and now. Same ordering,
    /// same row: the newest row is at or after any row already read.
    /// </summary>
    internal const string LatestCpuSinceSql = @"
SELECT sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time, sample_time_utc, collection_time
FROM cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2
ORDER BY collection_time DESC, sample_time DESC
LIMIT 1";

    /// <summary>Where each server's next latest-CPU read starts (#6968).</summary>
    private readonly NewestRowFloor _latestCpuFloor = new();

    /// <summary>
    /// Runs <see cref="LatestCpuSql"/> and shapes it for the snapshot — Lite's overview read
    /// (LocalDataService.Overview.cs:37-51) against the raw PG table, and the
    /// ServerSummaryItem.TotalCpuPercent derivation (:140-141): total = SQL + (other ?? 0),
    /// null when there is no SQL sample (Azure SQL DB stores other as 0; Linux stores NULL).
    /// </summary>
    private Task<(double? SqlCpu, double? TotalCpu, DateTime? SampleTime, DateTime? SampleUtcOnly)> ReadLatestCpuAsync(
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
    private async Task<(double? SqlCpu, double? TotalCpu, DateTime? SampleTime, DateTime? SampleUtcOnly)> ReadLatestCpuCoreAsync(int serverId, CancellationToken cancellationToken)
    {
        double? sqlCpu = null;
        double? otherCpu = null;
        DateTime? sampleTime = null;
        DateTime? sampleUtcOnly = null;

        await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);
        /* #6968: the shared floor loop remembers where the newest row is; a bounded read that finds nothing means the row the
           bound came from is gone, so it asks again without one (the read that shipped before). */
        await _latestCpuFloor.ReadAsync(serverId, async since =>
        {
            DateTime? newestCollection = null;
            using var command = new NpgsqlCommand(
                since is null ? LatestCpuSql : LatestCpuSinceSql, connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
            command.Parameters.AddWithValue(serverId);
            if (since is { } floor)
            {
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = floor });
            }

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
                /* #5642: only a TRUE UTC instant can give an age; the local fallback above cannot. */
                sampleUtcOnly = sampleTimeUtc;
                newestCollection = reader.GetDateTime(4);
            }

            return newestCollection;
        });

        double? totalCpu = sqlCpu.HasValue ? sqlCpu.Value + (otherCpu ?? 0) : null;
        return (sqlCpu, totalCpu, sampleTime, sampleUtcOnly);
    }

    /// <summary>The counter's name for the recent-samples read (#5642), beside <see cref="LatestCpuReadName"/>.</summary>
    internal const string RecentCpuReadName = "recent-CPU read";

    /// <summary>How far back the recent-samples read looks (#5642): the PostgreSQL check's window.</summary>
    internal static readonly TimeSpan RecentCpuLookback = TimeSpan.FromMinutes(15);

    /// <summary>At most this many recent samples (#5642): the PostgreSQL check's batch bound
    /// (<c>DarlingPgCpuUtilizationReader.GateBatchLimit</c>).</summary>
    internal const int RecentCpuSampleLimit = 32;

    /// <summary>
    /// The CPU samples the engine's persistence gate sees one by one (#5642): the newest 32 rows collected in the last
    /// 15 minutes that carry a true <c>sample_time_utc</c>. The window rides <c>collection_time</c> (the reliable
    /// clock, a naive-UTC bound so the store session's TimeZone cannot re-frame it), newest first so the LIMIT keeps the
    /// newest; the read returns them oldest first. A per-server index descent, never <c>DISTINCT ON</c> (measured more
    /// than 30 times slower than the per-server <c>LIMIT 1</c>).
    /// </summary>
    internal const string RecentCpuSamplesSql = @"
SELECT sample_time_utc, sqlserver_cpu_utilization, other_process_cpu_utilization
FROM cpu_utilization_stats
WHERE server_id = $1
  AND collection_time >= $2
  AND sample_time_utc IS NOT NULL
ORDER BY collection_time DESC, sample_time DESC
LIMIT $3";

    internal Task<IReadOnlyList<AlertCpuSample>> ReadRecentCpuSamplesAsync(
        int serverId, DateTime nowUtc, CancellationToken cancellationToken)
        => DarlingAlertReadAdapter.ExecuteWithOneRetryAsync(
            token => ReadRecentCpuSamplesCoreAsync(serverId, nowUtc, token),
            serverId.ToString(CultureInfo.InvariantCulture),
            RecentCpuReadName,
            _readFailures,
            Task.Delay,
            cancellationToken);

    private async Task<IReadOnlyList<AlertCpuSample>> ReadRecentCpuSamplesCoreAsync(
        int serverId, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var samples = new List<AlertCpuSample>();

        await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(
            RecentCpuSamplesSql, connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(nowUtc - RecentCpuLookback, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(RecentCpuSampleLimit);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var sqlCpu = reader.IsDBNull(1) ? (double?)null : Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture);
            var otherCpu = reader.IsDBNull(2) ? (double?)null : Convert.ToDouble(reader.GetValue(2), CultureInfo.InvariantCulture);
            double? totalCpu = sqlCpu.HasValue ? sqlCpu.Value + (otherCpu ?? 0) : null;
            samples.Add(new AlertCpuSample(reader.GetDateTime(0), sqlCpu, totalCpu));
        }

        /* Newest first out of the store (so the LIMIT keeps the newest); the engine wants them oldest first. */
        samples.Reverse();
        return samples;
    }
}
