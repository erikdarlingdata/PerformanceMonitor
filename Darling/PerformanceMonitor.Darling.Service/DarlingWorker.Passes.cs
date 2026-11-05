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
    ///
    /// <para>#4825: the slot is shared with <c>purge_now</c> (<see cref="TryStartPurgeNow"/>). A manual purge that
    /// is still running when the daily one comes due holds it off exactly like a slow daily run does: the launch
    /// is skipped, the stamp stays where it was, and the next tick tries again. The check and the set happen
    /// under <c>_purgeTaskLock</c> because the other launcher runs on the command loop's thread.</para>
    /// </summary>
    internal bool TryStartScheduledPurge(
        DateTime nowUtc, Func<CancellationToken, Task> startPurge, CancellationToken stoppingToken)
    {
        /* #4732: the stamp below is written as now + s_purgeInterval, so that is the span. */
        if (!StampIsDue(_nextPurgeUtc, s_purgeInterval, nowUtc))
        {
            return false;
        }

        lock (_purgeTaskLock)
        {
            if (_purgeTask is { IsCompleted: false })
            {
                _logger.LogInformation(
                    "a retention purge (the previous daily run, or a manual purge_now) was still running at the "
                    + "daily purge's next scheduled time — skipping this launch; it will be retried on the next "
                    + "tick once the current run completes");
                return false;
            }

            _nextPurgeUtc = nowUtc + s_purgeInterval;
            _purgeTask = RunTrackedAsync(startPurge, stoppingToken);
            return true;
        }
    }

    /// <summary>
    /// How soon after a budget-stopped retention pass ends the next scheduled pass is due (#5592). The launch stamps the
    /// next pass a whole <see cref="s_purgeInterval"/> out, so a drain the wall budget stopped would otherwise wait a day
    /// for the rest. One hour leaves collection the gap the budget already guarantees it.
    /// </summary>
    internal static readonly TimeSpan PurgeContinuationDelay = TimeSpan.FromHours(1);

    /// <summary>
    /// #5592 (#4732): the earlier of two absolute due times. Both arguments are stamps, not a stamp and the clock, so a
    /// wall clock that stepped backwards cannot strand the work here the way a raw compare of a stamp against now does:
    /// a stamp left far in the future by a step back is simply the later one and is replaced, so the continuation is
    /// still due within <see cref="PurgeContinuationDelay"/> of the pass's end (the stamp is not read as due at once,
    /// which would start the next drain with no gap for collection). A stamp already sooner, or equal, is returned as it
    /// is. Internal so a test can pin the table.
    /// </summary>
    internal static DateTime SoonerStamp(DateTime firstUtc, DateTime secondUtc) =>
        firstUtc <= secondUtc ? firstUtc : secondUtc;

    /// <summary>
    /// #5592: called by the scheduled pass and by <c>purge_now</c> when the retention sweep returns. When the sweep
    /// stopped on its wall budget with tables left, the next scheduled pass becomes due at most
    /// <see cref="PurgeContinuationDelay"/> after <paramref name="endUtc"/>: <c>_nextPurgeUtc</c> moves to the sooner of
    /// itself and that time, never later. A sweep that drained every table, failed (its summary carries no tables left) or
    /// was cancelled (it threw, so this is never reached) leaves the stamp alone. The set happens under
    /// <c>_purgeTaskLock</c> like the launchers' check and set, and the slot is still held while this pass runs, so the
    /// continuation cannot start a second pass beside it. Returns whether the stamp was pulled in.
    /// </summary>
    internal bool NotePurgePassEnded(PurgeSummary summary, DateTime endUtc)
    {
        if (!summary.StoppedOnBudget)
        {
            return false;
        }

        var continueAtUtc = endUtc + PurgeContinuationDelay;
        lock (_purgeTaskLock)
        {
            var sooner = SoonerStamp(_nextPurgeUtc, continueAtUtc);
            if (sooner == _nextPurgeUtc)
            {
                return false;
            }

            _nextPurgeUtc = sooner;
        }

        _logger.LogInformation(
            "Retention purge stopped on its time budget with {TablesLeft} table(s) left; the next pass is due at {DueUtc:O} instead of waiting for the daily time",
            summary.TablesLeftOnBudget, continueAtUtc);
        return true;
    }

    /// <summary>
    /// #4825: the <c>purge_now</c> command's launch decision, extracted so it is testable the way
    /// <see cref="TryStartScheduledPurge"/> is. The purge used to run inline on the command loop
    /// (<c>RunCommandLoopAsync</c> runs one command at a time), so every other command waited behind it, and it ran
    /// unpaced, so its deletes could stall collection on every server. It now starts in the SAME slot the daily
    /// purge uses (<see cref="_purgeTask"/>) and this returns at once.
    ///
    /// <para>If that slot is still running, whether the daily purge or an earlier <c>purge_now</c>, nothing
    /// starts: the reply is a success with <c>started: false, alreadyRunning: true</c>. Otherwise it is a success
    /// with <c>started: true</c> and <c>startedAtUtc</c>. Both carry <c>customRetentionDays</c> as asked. It does
    /// not touch <c>_nextPurgeUtc</c> when it starts: a manual purge neither counts as the day's purge nor delays it.
    /// (One that stops on its wall budget with tables left pulls the stamp in when it ends, see
    /// <see cref="NotePurgePassEnded"/>.)</para>
    ///
    /// <para><c>startedAtUtc</c> is this service's <c>DateTime.UtcNow</c>, taken just before the purge starts, as
    /// naive UTC in the round-trip ("o") form: the same clock and the same shape as the <c>collection_time</c> the
    /// run's records are stamped with (<see cref="DarlingObservability.LogRetentionRunAsync"/>). The viewer reads
    /// the run's totals back from collection_log with it as the lower bound, so no viewer or database clock enters
    /// the comparison. The already-running reply has none: nothing started, so there is no run of this caller's to
    /// look for.</para>
    ///
    /// <para><paramref name="stoppingToken"/> must be the SERVICE's stopping token. The token the command loop
    /// hands a command is a per-command one, and the purge outlives the command, so binding it to that one would
    /// cancel the purge the moment the command reported.</para>
    /// </summary>
    internal CommandOutcome TryStartPurgeNow(
        Func<CancellationToken, Task> startPurge, int? customRetentionDays, CancellationToken stoppingToken)
    {
        DateTime startedAtUtc;
        lock (_purgeTaskLock)
        {
            if (_purgeTask is { IsCompleted: false })
            {
                _logger.LogInformation(
                    "purge_now: a retention purge (the daily run, or an earlier purge_now) is already running — starting nothing");
                return new CommandOutcome(
                    true,
                    "purge already running",
                    JsonSerializer.Serialize(new { success = true, started = false, alreadyRunning = true, customRetentionDays }));
            }

            /* #4825: taken before the purge starts, so every record it writes is stamped at or after this. Naive
               UTC (Kind Unspecified), the form collection_time is stored in. */
            startedAtUtc = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            _purgeTask = RunTrackedAsync(startPurge, stoppingToken);
        }

        return new CommandOutcome(
            true,
            "purge started",
            JsonSerializer.Serialize(new
            {
                success = true,
                started = true,
                customRetentionDays,
                startedAtUtc = startedAtUtc.ToString("o", CultureInfo.InvariantCulture),
            }));
    }

    /// <summary>
    /// The hourly store-maintenance tick's launcher (#4970). The tick used to be awaited inline in the collection
    /// launch loop, so a slow pass delayed every sweep launch; it now runs as a tracked background task, one at a
    /// time. When the previous tick is still running at the next due time this hour is skipped: the caller has
    /// already advanced the due stamp, which keeps the :30 phase the compression read depends on (#3575). That
    /// differs on purpose from the daily purge, which retries on the next pass, because a skipped hour costs one
    /// hour and a retry would sample the job catalog off phase.
    /// </summary>
    internal bool TryStartStoreMaintenanceTick(Func<CancellationToken, Task> runTick, CancellationToken stoppingToken)
    {
        if (_storeMaintenanceTick is { IsCompleted: false })
        {
            _logger.LogWarning(
                "the previous hourly store-maintenance tick was still running at this tick's due time — skipping this hour; the next due time keeps its :30 phase");
            return false;
        }

        _storeMaintenanceTick = RunTrackedTickAsync("hourly store-maintenance tick", runTick, stoppingToken);
        return true;
    }

    /// <summary>
    /// The hourly store self-metrics tick's launcher (#4970). The sweep and the self-alert evaluators that judge
    /// its rows used to be awaited inline in the collection launch loop; they now run as one tracked background
    /// task, one at a time. While the store-maintenance tick still runs this returns false WITHOUT advancing the
    /// due stamp or taking the checkpoint window, so the next 15-second pass retries and maintenance keeps
    /// running first. Once it launches (or skips), the stamp advances on the grid. If the previous metrics tick
    /// is still running at this due time the hour is skipped with a Warning, so a late tick never doubles up.
    /// </summary>
    internal bool TryStartStoreMetricsTick(
        DateTime nowUtc,
        Func<CheckpointSyncMax?> takeWindow,
        Func<CheckpointSyncMax?, CancellationToken, Task> runTick,
        CancellationToken stoppingToken)
    {
        if (_storeMaintenanceTick is { IsCompleted: false })
        {
            return false;
        }

        _nextStoreMetricsUtc = NextGridStamp(_nextStoreMetricsUtc, nowUtc, s_storeMetricsInterval);

        if (_storeMetricsTick is { IsCompleted: false })
        {
            _logger.LogWarning(
                "the previous hourly store self-metrics tick was still running at this tick's due time — skipping this hour");
            return false;
        }

        /* #4834: the longest single checkpoint sync the minute samples saw since the last tick, taken ONCE and
           here, so the window closes when the tick LAUNCHES, whatever the sweep goes on to do. A deferral behind
           maintenance moves that close to the first pass after maintenance ends. A skip (above) returns before
           this call, so the window is not taken and carries into the next tick: that tick's window covers both
           hours and its checkpointer row keeps the longest sync of the two, which is the safe direction since
           dropping the window would lose a real long sync. The sweep stores it on the hour's checkpointer row;
           the checkpointer evaluation and get_store_metrics both read it back from that row, so neither is
           handed a window. */
        var checkpointWindow = takeWindow();
        _storeMetricsTick = RunTrackedTickAsync("hourly store self-metrics tick", token => runTick(checkpointWindow, token), stoppingToken);
        return true;
    }

    /// <summary>
    /// The hourly store self-metrics tick's body: the store self-metrics sweep, then the self-alerts that judge
    /// the rows it just wrote, moved out of the launch loop (#4970) unchanged.
    /// </summary>
    private async Task RunStoreMetricsTickAsync(CheckpointSyncMax? checkpointWindow, CancellationToken stoppingToken)
    {
            await SweepStoreSelfMetricsAsync(checkpointWindow, stoppingToken);

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
                   master-gated inside and failure-isolated inside; the outer catch is the belt.
                   #4834: the checkpointer check reads the hour's longest single sync from the row the
                   sweep just wrote, the same value get_store_metrics publishes. */
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

                /* #5450: the daily retained-history audit. Same hourly tick; the once-a-day gate (03:00Z, the previous
                   UTC day, a stamp that survives a restart) is enforced inside, so 23 of every 24 ticks cost a
                   clock check and a dictionary lookup. Master-gated inside, failure-isolated inside; the outer catch
                   is the belt. */
                try
                {
                    await _selfAlerts.EvaluateCollectionHistoryAuditAsync(_postgres!, stoppingToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogDebug(ex, "collection-history audit evaluation failed");
                }
            }
    }

    /// <summary>
    /// Test seam for the per-server self-alert evaluation (#5479): stands in for the evaluator's store-polled pass for one
    /// server, so a test can see which servers the fleet pass evaluates, when, and on which thread. Null in production.
    /// </summary>
    internal Func<ServerLoopState, CancellationToken, Task>? SelfAlertServerOverride { get; set; }

    /// <summary>Test seam (#5642): replaces the engine's alert evaluation for one server in the fleet pass, like
    /// <see cref="SelfAlertServerOverride"/>. Production never sets it.</summary>
    internal Func<ServerLoopState, CancellationToken, Task>? AlertServerOverride { get; set; }

    /// <summary>Test seam (#5642): replaces the same-statement-pileup evaluation for one server in the fleet pass.
    /// Production never sets it.</summary>
    internal Func<ServerLoopState, CancellationToken, Task>? PileupServerOverride { get; set; }

    /// <summary>
    /// Test seam (#5481): the fleet pass's task, so a test that starts a second pass waits for the first to finish rather than
    /// for a counter the pass moves before it ends (a pass still running makes <see cref="TryStartSelfAlertPass"/> return false).
    /// Null before the first pass. Production never reads it.
    /// </summary>
    internal Task? SelfAlertPassForTests => _selfAlertPass;

    /// <summary>Test seam (#5642): the engine pass's task, null until the first one starts. Production never reads it.</summary>
    internal Task? AlertEnginePassForTests => _alertEnginePass;

    /// <summary>
    /// The per-server self-alert pass's launcher (#5479): Collection Stopped and the other store-polled self-alerts, and
    /// the custom-alert rules, for every server in this tick's snapshot. They ran at the top of each server's collection
    /// body, so a body the memory launch guard held off, or one that never finished, silenced them; a fleet that had
    /// stopped collecting paged nobody for 25 hours. Called by the sweep loop every tick whether or not any body
    /// launches, fire-and-tracked like the per-server bodies and the hourly ticks, never awaited. One pass at a time: a
    /// pass still running at the next tick makes that tick skip (one Warning per pass, once it has outrun the 30 s cadence), so no server is ever
    /// evaluated by two threads at once, and the stamps in <see cref="ServerLoopState"/> are written by this pass only.
    /// </summary>
    /// <param name="sweepWidth">The fleet gate's width (<c>max_concurrent_sweeps</c>). The self-alert pass and the engine pass split
    /// <see cref="SelfAlertPassWidthFor"/> between them (<see cref="SelfAlertStepWidthFor"/> and <see cref="AlertEnginePassWidthFor"/>) so their reads stay inside the store pool's reserve.</param>
    /// <returns>True when a new self-alert pass started. The engine pass (#5642) has its own slot, so this says nothing
    /// about it: see <see cref="TryStartAlertEnginePass"/>.</returns>
    internal bool TryStartSelfAlertPass(
        ServerLoopState[] targets, CancellationToken stoppingToken, int sweepWidth = MaxConcurrentServerSweeps,
        AlertEngine? engine = null, DarlingConfig? config = null, AnalysisNotificationService? notifications = null)
    {
        var started = TryStartSelfAlertStep(targets, sweepWidth, stoppingToken);

        /* #5642: the engine's alert reads and the pileup finding run in their own single-flight pass, chained AFTER the
           self-alert step of this tick, so Collection Stopped and its siblings never wait for them. */
        TryStartAlertEnginePass(targets, sweepWidth, engine, config, notifications, stoppingToken);
        return started;
    }

    private bool TryStartSelfAlertStep(ServerLoopState[] targets, int sweepWidth, CancellationToken stoppingToken)
    {
        if (_selfAlertPass is { IsCompleted: false })
        {
            /* A server's cadence is 30 s (s_alertSweepInterval) and the sweep tick is 15 s, so a pass that runs past
               one tick but ends inside the cadence is on time and logs nothing here. Only a pass that has outrun the
               cadence warns, once per pass, with how long it has run. */
            var runningFor = SelfAlertPassClock() - _selfAlertPassStartedUtc;
            if (!_selfAlertPassOverranWarned && runningFor > s_alertSweepInterval)
            {
                _selfAlertPassOverranWarned = true;
                _logger.LogWarning(
                    "the previous per-server self-alert pass has been running for {RunningFor:F0}s, longer than its {Cadence:F0}s cadence, so this tick starts no second one; the running pass is still judging the servers, and the next tick after it ends starts a new pass",
                    runningFor.TotalSeconds, s_alertSweepInterval.TotalSeconds);
            }

            return false;
        }

        _selfAlertPassOverranWarned = false;
        _selfAlertPassStartedUtc = SelfAlertPassClock();
        _selfAlertPass = RunTrackedTickAsync("per-server self-alert pass", token => RunSelfAlertPassAsync(targets, sweepWidth, token), stoppingToken);
        return true;
    }

    /// <summary>
    /// The engine pass's launcher (#5642): the shared alert engine's per-server reads (~22 store reads a server) and the
    /// same-statement-pileup finding, in a single-flight slot of their own. They used to ride the self-alert pass, so
    /// the pass's length, and with it Collection Stopped's cadence, grew with every alert read: a store in a slow write
    /// band stretched it from a few seconds to a minute or more. Now the self-alert step ends and frees its slot first;
    /// this pass waits for the step launched in the same tick (or the one still running) and only then starts its reads,
    /// and a pass still running at the next tick makes that tick skip it, with one Warning per pass once it has outrun
    /// the 30 s cadence, as the self-alert pass does. The servers' alert stamps do not move on a skip, so the next free
    /// tick picks up what was due. Not started when no engine, seam or pileup finding is configured.
    /// </summary>
    internal bool TryStartAlertEnginePass(
        ServerLoopState[] targets, int sweepWidth,
        AlertEngine? engine, DarlingConfig? config, AnalysisNotificationService? notifications, CancellationToken stoppingToken)
    {
        var hasEngine = AlertServerOverride is not null || (engine is not null && config is not null);
        var hasPileup = PileupServerOverride is not null || (config is { Analysis.Enabled: true } && notifications is not null);
        if (!hasEngine && !hasPileup)
        {
            return false;
        }

        if (_alertEnginePass is { IsCompleted: false })
        {
            var runningFor = SelfAlertPassClock() - _alertEnginePassStartedUtc;
            if (!_alertEnginePassOverranWarned && runningFor > s_alertSweepInterval)
            {
                _alertEnginePassOverranWarned = true;
                _logger.LogWarning(
                    "the previous per-server alert engine pass has been running for {RunningFor:F0}s, longer than its {Cadence:F0}s cadence, so this tick starts no second one; the self-alerts (Collection Stopped and the rest) are not waiting on it, and the next tick after it ends starts a new engine pass",
                    runningFor.TotalSeconds, s_alertSweepInterval.TotalSeconds);
            }

            return false;
        }

        _alertEnginePassOverranWarned = false;
        _alertEnginePassStartedUtc = SelfAlertPassClock();
        var selfAlertStep = _selfAlertPass;
        _alertEnginePass = RunTrackedTickAsync("per-server alert engine pass", async token =>
        {
            /* RunTrackedTickAsync swallows the step's own failures, so this wait only ends: it never throws. */
            if (selfAlertStep is not null)
            {
                await selfAlertStep.WaitAsync(token);
            }

            await RunAlertEnginePassAsync(targets, sweepWidth, engine, config, notifications, token);
        }, stoppingToken);
        return true;
    }

    /// <summary>
    /// The pass itself (#5479): each non-retired server, up to <c>sweepWidth</c> at a time, on its own cadence stamps. A
    /// fleet pass that walked the servers one at a time took 3 or more sequential store reads per server (50 to 100 s at
    /// 500 servers), so every tick overran and Collection Stopped lagged the cadence several times over; the same calls
    /// ran inside the bodies under the fleet gate before. The pass runs at <see cref="SelfAlertStepWidthFor"/> of the sweep width (its share of the <see cref="SelfAlertPassWidthFor"/> budget, which keeps its reads inside the store pool's reserve); the engine's reads are the separate pass after it (#5642). Each server is still
    /// visited once per pass by one thread, so its stamps stay single-writer, and only one pass runs at a time.
    /// Collection-stopped is evaluated for EVERY server, connected or not and whether or not it has been seen online
    /// since this service started (#4757), because an unreachable server has stopped collecting, which is exactly the
    /// case a headless service must page on; the evaluator judges its staleness from the later of the last success and
    /// the service start, so a restart's stale rows do not false-alarm a healthy server, and capture-down only for a
    /// connected one. The master alerts gate and the edge trigger live inside the evaluator. The custom-alert rules
    /// (#3285) read the collected store, so they evaluate for a disconnected server too. One server's throw, or a
    /// cancellation that is not the service shutting down, is logged and does not skip the rest.
    /// </summary>
    private async Task RunSelfAlertPassAsync(ServerLoopState[] targets, int sweepWidth, CancellationToken stoppingToken)
    {
        var started = Stopwatch.StartNew();
        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = SelfAlertStepWidthFor(sweepWidth),
            CancellationToken = stoppingToken,
        };

        await Parallel.ForEachAsync(targets, options, async (server, _) =>
        {
            if (server.Retired)
            {
                return;
            }

            try
            {
                if (StampIsDue(server.NextSelfAlertSweep, s_alertSweepInterval, DateTime.UtcNow))
                {
                    server.NextSelfAlertSweep = DateTime.UtcNow.Add(s_alertSweepInterval);
                    if (SelfAlertServerOverride is { } evaluate)
                    {
                        await evaluate(server, stoppingToken);
                    }
                    else if (_selfAlerts is not null && _postgres is not null)
                    {
                        await _selfAlerts.EvaluateStoreAlertsAsync(
                            _postgres,
                            server.Config.ServerId,
                            server.Config.DisplayName,
                            connected: server.Runtime is not null,
                            stoppingToken,
                            engine: server.Runtime?.Target.Engine ?? CollectorTargetEngine.SqlServer);
                    }
                }

                if (_customAlertEvaluator is not null && StampIsDue(server.NextCustomAlertSweep, s_customAlertSweepInterval, DateTime.UtcNow))
                {
                    server.NextCustomAlertSweep = DateTime.UtcNow.Add(s_customAlertSweepInterval);
                    await _customAlertEvaluator.EvaluateServerAsync(
                        server.Config.ServerId,
                        server.Config.StorageName,
                        server.Config.DisplayName,
                        stoppingToken);
                }
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                /* Any exception that is not the service shutting down, a cancellation inside one server's evaluator
                   included (the custom-alert evaluator rethrows every OperationCanceledException): log it and go on, so
                   the servers after it are still evaluated and a repeating one cannot starve them. */
                _logger.LogError(ex, "[{Server}] self-alert pass failed; the remaining servers are still evaluated", server.Config.DisplayName);
            }
        });

        if (started.Elapsed > s_alertSweepInterval)
        {
            _logger.LogInformation(
                "the per-server self-alert pass took {Elapsed:F1}s for {Servers} servers at {Width} at a time, longer than its {Cadence}s cadence",
                started.Elapsed.TotalSeconds, targets.Length, options.MaxDegreeOfParallelism, s_alertSweepInterval.TotalSeconds);
        }
    }

    /// <summary>
    /// The engine pass itself (#5642): for each non-retired server, up to <see cref="AlertEnginePassWidthFor"/> at a time,
    /// the shared engine's alert evaluation and then the same-statement-pileup finding, each on its own 30 s stamp. Runs
    /// in its own single-flight slot after the self-alert step (see <see cref="TryStartAlertEnginePass"/>), so a slow
    /// server's alert reads never delay Collection Stopped. The stamps are written by this pass only. One server's throw
    /// is logged and does not skip the rest.
    /// </summary>
    private async Task RunAlertEnginePassAsync(
        ServerLoopState[] targets, int sweepWidth, AlertEngine? engine, DarlingConfig? config,
        AnalysisNotificationService? notifications, CancellationToken stoppingToken)
    {
        var started = Stopwatch.StartNew();
        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = AlertEnginePassWidthFor(sweepWidth),
            CancellationToken = stoppingToken,
        };

        await Parallel.ForEachAsync(targets, options, async (server, _) =>
        {
            if (server.Retired)
            {
                return;
            }

            try
            {
                /* #5642: the engine's alert evaluation. It ran after the collection sweep inside the body, so a body that
                   never finished silenced every alert on its server. A server whose runtime dropped is still evaluated,
                   from the store, with its last known target (AlertRuntime): the server under enough pressure to drop is
                   the one that needs the alert. */
                if ((AlertServerOverride is not null || (engine is not null && config is not null))
                    && StampIsDue(server.NextAlertSweep, s_alertSweepInterval, DateTime.UtcNow))
                {
                    server.NextAlertSweep = DateTime.UtcNow.Add(s_alertSweepInterval);
                    if (server.AlertRuntime is not null)
                    {
                        if (AlertServerOverride is { } evaluateAlerts)
                        {
                            await evaluateAlerts(server, stoppingToken);
                        }
                        else
                        {
                            await EvaluateAlertsAsync(engine!, server, config!, stoppingToken);
                        }
                    }
                }

                /* #3467, moved here by #5642: the same-statement-pileup finding at the alert cadence. Analysis-side gates
                   (config.Analysis.Enabled; delivery alone is gated inside via ShouldNotifyAnalysisFindings). SQL Server
                   targets only, with a live runtime: the shape is a sys.dm_exec_requests statement pileup, and a
                   PostgreSQL target never writes query_snapshots rows. */
                if ((PileupServerOverride is not null || (config is { Analysis.Enabled: true } && notifications is not null))
                    && server.Runtime is { } pileupRuntime
                    && pileupRuntime.Target.Engine != CollectorTargetEngine.PostgreSql
                    && StampIsDue(server.NextPileupSweep, s_alertSweepInterval, DateTime.UtcNow))
                {
                    server.NextPileupSweep = DateTime.UtcNow.Add(s_alertSweepInterval);
                    if (PileupServerOverride is { } evaluatePileup)
                    {
                        await evaluatePileup(server, stoppingToken);
                    }
                    else
                    {
                        await EvaluateSameStatementPileupAsync(server, config!, notifications!, stoppingToken);
                    }
                }
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                /* Any exception that is not the service shutting down, a cancellation inside one server's evaluator
                   included (the custom-alert evaluator rethrows every OperationCanceledException): log it and go on, so
                   the servers after it are still evaluated and a repeating one cannot starve them. */
                _logger.LogError(ex, "[{Server}] alert engine pass failed; the remaining servers are still evaluated", server.Config.DisplayName);
            }
        });

        if (started.Elapsed > s_alertSweepInterval)
        {
            _logger.LogInformation(
                "the per-server alert engine pass took {Elapsed:F1}s for {Servers} servers at {Width} at a time, longer than its {Cadence}s cadence",
                started.Elapsed.TotalSeconds, targets.Length, options.MaxDegreeOfParallelism, s_alertSweepInterval.TotalSeconds);
        }
    }

    /// <summary>
    /// Counts the collector slots that came due while the memory launch guard held collection off (#5479), once each,
    /// within a pass of their due time, and adds them to the fleet gate's skipped count: a held slot is a skipped slot. A
    /// held pass launches no body, so without this the hour's count read "0 ran, 0 skipped" and Collection Falling Behind
    /// cleared an hour into an outage. The due stamps do not move during a hold, so each (server, collector) keeps a
    /// <see cref="HeldSlotMark"/> and <see cref="HeldSlots.Newly"/> adds only what came due since the last pass. The pass
    /// that launches again raises the skip-credit floor first, so the first bodies count none of these a second time.
    /// A server with a body still in flight is left to that body, which counts what it steps over. That leaves a gap
    /// on purpose: a body in flight for the whole hold adds no held slots, and slots that collector loses after the
    /// release, before that body reaches it, go uncounted by the hold. Counting them here would race the body's own
    /// slot record, and Collection Stopped pages for a server that long without collecting, so the gap stays. Runs on
    /// the sweep loop's thread. Returns the slots counted.
    /// </summary>
    internal long CountHeldSlots(ServerLoopState[] targets, DateTime nowUtc)
    {
        long counted = 0;
        var floor = _skipCreditFloor.Floor;
        foreach (var server in targets)
        {
            var runtime = server.Runtime;
            if (runtime is null || server.Retired || server.InFlightSweep is { IsCompleted: false })
            {
                continue;
            }

            foreach (var name in CollectorScheduleDefaults.All.Keys)
            {
                /* The same filters RunDueCollectorsAsync puts before its slot count: a collector that does not run on this
                   target, or is disabled, or has no stamp, has no slot to lose. */
                if (!CollectorCatalog.EngineMatches(name, runtime.Target)
                    || !CollectorCatalog.AppliesTo(name, runtime.Target))
                {
                    continue;
                }

                var effective = StoreConfigProvider.ResolveSchedule(name, runtime.ServerId, _scheduleOverrides);
                if (!effective.Enabled || !server.NextDue.TryGetValue(name, out var due))
                {
                    continue;
                }

                var interval = TimeSpan.FromMinutes(CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(effective.FrequencyMinutes));

                /* A grid collector's slot is lost when the next one comes due (what a late run steps over); a collector
                   with a run time loses its day once the grace after the stamp has passed (StepRunTimeCollector). */
                var serveWindow = effective.RunAtMinute is not null ? CollectorRunTime.Grace : interval;
                server.HeldSlotMarks.TryGetValue(name, out var mark);
                var newlyHeld = HeldSlots.Newly(due, nowUtc, floor, interval, serveWindow, ref mark);
                counted += newlyHeld;
                server.HeldSlotMarks[name] = mark;
                /* #5735: only fills the server's tally; the body that runs after the release writes the row (this runs on the
                   sweep thread, under memory pressure). */
                server.CycleSkips.AddHeld(name, newlyHeld, due);
            }
        }

        _fleetGateStats?.RecordSkippedSlots(counted);
        return counted;
    }

    /// <summary>
    /// Wraps a tick delegate so the tracked task can never fault unobserved: nothing awaits it while it runs.
    /// <see cref="OperationCanceledException"/> is the normal shutdown outcome and is swallowed.
    /// </summary>
    private async Task RunTrackedTickAsync(string name, Func<CancellationToken, Task> run, CancellationToken ct)
    {
        try
        {
            await run(ct);
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown drain. */
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{Tick} failed", name);
        }
    }

    /// <summary>
    /// The hourly store-maintenance tick's body: the availability re-probe, then compression job health,
    /// retention re-evaluation and store-object convergence, moved out of the launch loop (#4970) unchanged.
    /// </summary>
    private async Task RunStoreMaintenanceTickAsync(CancellationToken stoppingToken)
    {
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
               restart. On a converged store that pass costs catalog reads and two metadata ALTERs.

               #5094: the FIFTH tenant, same contract — its own method, its own catch-all, one awaited
               statement, LAST. The Query Store daily summary builder runs after the convergence pass
               so a store that is still being converged is never made to build summaries first, and a
               builder fault or an expired budget cannot skip any tenant above it. */
            await ConvergeStoreObjectsAsync(stoppingToken);
            await BuildQueryStoreTopDailyAsync(stoppingToken);
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

        /* The retry of a start-path role provisioning that failed. Outside the TimescaleDB gate on
           purpose: provisioning does not depend on the extension, so a plain-PostgreSQL store retries too.
           Directly after the gate, ahead of the later tenants, so it spends none of the compression read's phase.
           Its own method and its own catch-all; nothing to do (and nothing awaited) unless a start failed. */
        await RetryRoleProvisioningAsync(stoppingToken);

        /* #5582: the ninth tenant, same contract - its own method, its own catch-all, one awaited statement. The exact stamp-grain
           rollup of the wide Query Store table (V173) needs no TimescaleDB (its tables are plain and the wide table is a plain
           partitioned table), so it sits outside the gate and runs on every store shape. It sits BEFORE the I/O rollup, not
           after it: the pins keep the I/O rollup directly before the module-map refresh and the PLAN_REGRESSION builder, which
           can run up to a 10-minute tick, as the last await. At most six hours a tick, so a fault or a slow build skips nothing. */
        await BuildQueryStoreComposeStampAsync(stoppingToken);

        /* #5495: the eighth tenant, same contract — its own method, its own catch-all, one awaited statement. The hourly
           PostgreSQL I/O rollup needs no TimescaleDB (its tables are plain), so it sits outside the gate and runs on every
           store shape. It sits BEFORE the module-map refresh and the PLAN_REGRESSION builder, not after them: the pins keep
           the plan-regression builder, which can run up to a 10-minute tick, as the last await, with the refresh directly
           before it, so the rollup (budget-bounded to 2 minutes) goes ahead of both and a fault in it skips nothing. */
        await BuildPgIoStatsHourlyAsync(stoppingToken);

        /* #4605: the sixth tenant, same contract — its own method, its own catch-all, one awaited statement.
           It sits AFTER the gate rather than inside it because it needs no TimescaleDB: procedure_stats and
           module_map are plain tables on every store shape, and the daily refresh already runs on all of them.
           It comes after the gate and the I/O rollup so a store still being converged, or a summary builder that ran
           out its budget, is never made to wait behind it, and a fault here skips nothing above. */
        await RefreshModuleMapRecentAsync(stoppingToken);

        /* #5448: the seventh tenant, same contract — its own method, its own catch-all, one awaited statement, LAST.
           The PLAN_REGRESSION per-day totals builder needs no TimescaleDB either (collect.plan_regression_daily and
           its built table are plain tables), so it sits outside the gate, directly after the module-map refresh: a builder
           that runs out its budget never makes the cheaper tenants above it wait, and a fault here skips nothing. */
        await BuildPlanRegressionDailyAsync(stoppingToken);
    }

    /// <summary>
    /// The hourly store-maintenance tick's sixth tenant (#4605): the incremental module-map refresh, which keeps
    /// the <c>collect.module_map</c> watermark within about an hour of procedure_stats so a reader can trust the
    /// map up to it. Reads only the rows since the last watermark (<see cref="DarlingModuleMap.RefreshRecentAsync"/>),
    /// failure-isolated inside that method and again here. Its own catch-all: nothing here can fail the tick.
    /// </summary>
    private async Task RefreshModuleMapRecentAsync(CancellationToken stoppingToken)
    {
        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(stoppingToken);
            await DarlingModuleMap.RefreshRecentAsync(connection, _logger, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("module_map hourly refresh could not run; the next hourly tick retries: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// The hourly store-maintenance tick's eighth tenant (#5495): builds the hourly rollup of the differenced PostgreSQL I/O
    /// counters that long <c>get_pg_io_stats</c> windows read, one failure-isolated pass (see
    /// <see cref="PgIoStatsHourlyBuilder.RunTickAsync(NpgsqlDataSource, DateTime, ILogger, CancellationToken)"/>). The first pass after
    /// the V170 upgrade is the fill, bounded by time (<see cref="PgIoStatsHourlyBuilder.TickBudget"/>). Its own catch-all: nothing here can fail the tick.
    /// </summary>
    private async Task BuildPgIoStatsHourlyAsync(CancellationToken stoppingToken)
    {
        try
        {
            await PgIoStatsHourlyBuilder.RunTickAsync(_postgres!, DateTime.UtcNow, _logger, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("PostgreSQL I/O hourly rollup could not run; the next hourly tick retries: {Message}", ex.Message);
        }
    }
    /// <summary>
    /// The hourly store-maintenance tick's seventh tenant (#5448): builds the per-day per-plan totals PLAN_REGRESSION reads
    /// for closed days, one failure-isolated pass (see <see cref="PlanRegressionDaily.RunTickAsync(NpgsqlDataSource, DateTime, ILogger, CancellationToken)"/>).
    /// Its own catch-all: nothing here can fail the tick.
    /// </summary>
    private async Task BuildPlanRegressionDailyAsync(CancellationToken stoppingToken)
    {
        try
        {
            await PlanRegressionDaily.RunTickAsync(_postgres!, DateTime.UtcNow, _logger, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("PLAN_REGRESSION daily totals could not run; the next hourly tick retries: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// The hourly store-maintenance tick's ninth tenant (#5582): builds the exact stamp-grain rollup of the wide Query Store table that
    /// the Query Store panels of the composer read for the hours before the last three, one failure-isolated pass (see
    /// <see cref="QueryStoreComposeStamp.RunTickAsync(Npgsql.NpgsqlDataSource, DateTime, int, ILogger, CancellationToken)"/>). At most
    /// <see cref="QueryStoreComposeStamp.MaxBuildsPerTick"/> hours per tick, stale hours first. Its own catch-all: nothing here can fail the tick.
    /// </summary>
    private async Task BuildQueryStoreComposeStampAsync(CancellationToken stoppingToken)
    {
        try
        {
            await QueryStoreComposeStamp.RunTickAsync(
                _postgres!, DateTime.UtcNow, DarlingRetention.QueryStoreIntervalWideRetentionDays, _logger, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("Query Store compose rollup could not run; the next hourly tick retries: {Message}", ex.Message);
        }

        /* #5867: the hours whose build now waits a day between attempts raise the Compose Rollup Hours Stuck self-alert, from its own
           read after the tick. */
        if (_selfAlerts is not null)
        {
            await EvaluateComposeHoursStuckAsync(stoppingToken);
        }
    }

    /// <summary>
    /// #5867: reads the Query Store compose builder's failure record and hands it to
    /// <see cref="DarlingSelfAlertEvaluator.EvaluateComposeHoursStuckAsync"/>. A failed read sends no report, so the standing alert
    /// is held, never resolved on uncertainty, and the next hourly compose tick reads again. Exempt from #3013's swallowed-read
    /// counter rather than counted (unlike #5654's): counting it would also add a read to the fleet-scoped inventory that both SKUs'
    /// get_collection_health descriptions carry.
    /// </summary>
    private async Task EvaluateComposeHoursStuckAsync(CancellationToken cancellationToken)
    {
        IReadOnlyList<QueryStoreComposeStamp.HourFailure> failures;
        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);
            failures = await QueryStoreComposeStamp.ReadFailuresAsync(connection, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "Query Store compose rollup: the failure record could not be read for the self-alert; the standing alert is held this tick: {Message}",
                ex.Message);
            return;
        }

        await _selfAlerts!.EvaluateComposeHoursStuckAsync(failures, cancellationToken);
    }

    /// <summary>
    /// The hourly store-maintenance tick's fifth tenant (#5094): builds the per-day Query Store summary the
    /// long windows of <c>get_query_store_top</c> will read, one failure-isolated pass (see
    /// <see cref="QueryStoreTopDaily.RunTickAsync"/>). Its own catch-all: nothing here can fail the tick.
    /// </summary>
    private async Task BuildQueryStoreTopDailyAsync(CancellationToken stoppingToken)
    {
        try
        {
            await QueryStoreTopDaily.RunTickAsync(
                _postgres!, DateTime.UtcNow, DarlingRetention.QueryStoreIntervalWideRetentionDays, _logger, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("Query Store top daily summary could not run; the next hourly tick retries: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// The plain-text collection_log run-record for <c>purge_now</c>'s raw-table step (#4825): a header line naming
    /// the run, then one line per relation with its outcome and note. Status is WARNING when a relation ended
    /// <c>gate_error</c> or <c>run_failed</c> (the step itself failed), otherwise SUCCESS, because a relation the
    /// gate holds on purpose (<c>not_covered</c>, <c>hole</c>, <c>epoch_stale</c>, <c>gate_unknown</c>) is the gate
    /// doing its job, not a failure to warn about. Pure so it is testable without a store.
    /// </summary>
    internal static (string Status, string Message) BuildRawPurgeNowRunRecord(
        string runLabel, IReadOnlyList<RawPurgeNowEntry> entries)
    {
        var lines = new List<string>(entries.Count + 1) { $"{runLabel}, raw tables:" };
        var failed = false;
        foreach (var entry in entries)
        {
            lines.Add($"{entry.Relation}: {entry.Outcome} - {entry.Note}");
            failed |= entry.Outcome is "gate_error" or "run_failed";
        }

        return (failed ? "WARNING" : "SUCCESS", string.Join("\n", lines));
    }

    /// <summary>One relation's line in <c>purge_now</c>'s raw-table report (#4427, #4825).</summary>
    internal readonly record struct RawPurgeNowEntry(string Relation, string Outcome, string Note);

    /// <summary>
    /// Wraps a purge delegate (the daily one or a <c>purge_now</c>, #4825) so <see cref="_purgeTask"/> can never
    /// fault unobserved (nothing awaits it while it runs, so an unhandled fault here would otherwise surface only
    /// as an UnobservedTaskException at GC time). <see cref="OperationCanceledException"/> is swallowed too — that is the normal shutdown-drain
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
            _logger.LogError(ex, "retention purge failed");
        }
    }

    /// <summary>#6044: the module map repair runs at most once per this span.</summary>
    internal static readonly TimeSpan ModuleMapRepairInterval = TimeSpan.FromHours(24);

    private DateTime? _lastModuleMapRepairUtc;

    /// <summary>#6044: whether the repair is due. Never run (this process), or a run at least
    /// <see cref="ModuleMapRepairInterval"/> ago, or a clock that stepped back past the stamp. Internal so a test can pin it.</summary>
    internal static bool IsModuleMapRepairDue(DateTime? lastSuccessUtc, DateTime nowUtc) =>
        lastSuccessUtc is not { } last || nowUtc < last || nowUtc - last >= ModuleMapRepairInterval;

    /// <summary>#6044: runs <paramref name="repair"/> when due and stamps the run only when it returns true (a failed
    /// or cancelled repair leaves the stamp, so the next pass retries). The purge slot is single, so passes do not overlap.
    /// Returns whether it ran.</summary>
    internal async Task<bool> RunModuleMapRepairIfDueAsync(DateTime nowUtc, Func<CancellationToken, Task<bool>> repair, CancellationToken cancellationToken)
    {
        if (!IsModuleMapRepairDue(_lastModuleMapRepairUtc, nowUtc))
        {
            return false;
        }

        if (await repair(cancellationToken))
        {
            _lastModuleMapRepairUtc = nowUtc;
        }

        return true;
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

        /* #4823: the daily sweep paces its WAL. Its deletes used to run flat out and put 6.89 GB of WAL into
           one checkpoint interval, which stalled collection while that checkpoint caught up. This runs off
           the collection loop (_purgeTask), so a longer, paced purge delays nothing. */
        var purgeSummary = await DarlingRetention.PurgeAsync(
            postgres, _timescaleAvailable, _logger, stoppingToken,
            name => StoreConfigProvider.ResolveFleetRetentionDays(name, overrides),
            config.PlanContentRetentionDays,
            paceWal: true,
            collectionPressure: _collectionPressure,
            queryTextPruneGate: _queryTextPruneGate);

        /* #5592: a pass the wall budget stopped brings the next scheduled pass forward to within the hour. */
        NotePurgePassEnded(purgeSummary, DateTime.UtcNow);

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
           CAGG windows). #6044: the repair is once per 24 hours, not once per pass: a pass the wall budget stopped
           brings the next one forward (#5592), and each repair read 13 to 17 GB, so six a day on a large store.
           The pass still does all its purge work; only this step is skipped. Failure-isolated inside RefreshAsync,
           which reports a failed read so it does not count as a run. #5519: with a watermark it re-reads from
           DailyRepairSlack behind it, not a flat two days; that span is the repair for rows the hourly path
           missed by committing more than WatermarkSlack late. */
        await RunModuleMapRepairIfDueAsync(
            DateTime.UtcNow,
            async token =>
            {
                await using var moduleMapConnection = await postgres.OpenConnectionAsync(token);
                var outcome = new ModuleMapRefreshOutcome();
                await DarlingModuleMap.RefreshAsync(moduleMapConnection, _logger, outcome, token);
                return !outcome.Failed;
            },
            stoppingToken);

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
    /// The <c>purge_now</c> command handler (the daily retention purge on demand): starts
    /// <see cref="RunPurgeNowBackgroundAsync"/> in the daily purge's own slot (<see cref="TryStartPurgeNow"/>)
    /// and answers at once, with <c>started</c> or <c>alreadyRunning</c>. Fleet-wide over the SHARED tables (no
    /// target server). Takes NO collection gate: unlike snapshot_now a purge writes no collector state and races
    /// no delta baseline, and PurgeAsync is idempotent + failure-isolated per table.
    /// </summary>
    private CommandOutcome StartPurgeNow(DarlingConfig config, int? customRetentionDays, CancellationToken stoppingToken)
    {
        return TryStartPurgeNow(
            token => RunPurgeNowBackgroundAsync(_postgres!, _timescaleAvailable, config, customRetentionDays, token),
            customRetentionDays,
            stoppingToken);
    }

    /// <summary>
    /// The work behind <c>purge_now</c> (#4825), run in the background in <see cref="_purgeTask"/>: the paced
    /// <see cref="DarlingRetention.PurgeAsync"/> over the shared store, then the #4427 raw-table step. When
    /// <paramref name="customRetentionDays"/> is set it purges every collector to that horizon (a
    /// <c>_ =&gt; customDays</c> resolver — PurgeAsync clamps a sub-1-day horizon at its destructive sink, so a
    /// bad custom-N can never wipe a table); otherwise it uses the SAME fleet resolver the scheduled daily
    /// purge uses (<see cref="StoreConfigProvider.ResolveFleetRetentionDays"/> over the live overrides).
    ///
    /// <para>The command's reply no longer carries the totals, so they go where the daily purge's already go:
    /// PurgeAsync's own run-record (labelled as a manual purge, naming a custom horizon), and a second run-record
    /// written after the raw step with one line per raw relation. Both are also logged at Information. A service
    /// stop that cuts the run short is logged as one warning naming the run (its label carries a custom horizon)
    /// and saying that the next daily purge uses the configured horizons.
    /// <paramref name="writeRawRunRecord"/> is a TEST SEAM (status, duration ms, message, token): null writes it
    /// to the store with <see cref="DarlingObservability.LogRetentionRunAsync"/>.</para>
    /// </summary>
    internal async Task RunPurgeNowBackgroundAsync(
        NpgsqlDataSource postgres, bool timescaleAvailable, DarlingConfig config, int? customRetentionDays,
        CancellationToken stoppingToken, Func<string, long, string, CancellationToken, Task>? writeRawRunRecord = null)
    {
        /* Reference read of the live overrides, matching the daily purge caller (never held under a lock —
           the reload swaps the whole list atomically). */
        var overrides = _scheduleOverrides;
        Func<string, int> resolver = customRetentionDays is int days
            ? _ => days
            : name => StoreConfigProvider.ResolveFleetRetentionDays(name, overrides);
        var runLabel = DarlingRetention.BuildManualPurgeLabel(customRetentionDays);

        try
        {
            /* #4825: paced, like the daily sweep. This used to run inline on the command loop, unpaced (#4823's
               note), because a paced purge would have held pause, resume and test_connect for as long as the pacing
               takes. It runs off the command loop now, in the daily purge's slot, so it can pace its WAL and delay
               nothing. */
            var summary = await DarlingRetention.PurgeAsync(
                postgres, timescaleAvailable, _logger, stoppingToken, resolver,
                config.PlanContentRetentionDays,
                paceWal: true,
                runLabel: runLabel,
                collectionPressure: _collectionPressure);

            _logger.LogInformation(
                "{Label} purged {Tables} table(s), {Rows} row(s)/chunk(s)",
                runLabel, summary.TablesPurged, summary.TotalPurged);

            /* #5592: a manual pass the wall budget stopped leaves the same rows behind a scheduled one would. */
            NotePurgePassEnded(summary, DateTime.UtcNow);

            /* #4427: the sweep above (DarlingRetention.PurgeAsync) no longer drops the three raw tables on a
               TimescaleDB store — they left its drop path entirely. purge_now must not go silent about them:
               run the SAME gated trigger the daily fleet-loop tick uses (TriggerRawPurgeCoreAsync), on a pooled
               connection, then read back what each relation's pass just decided (or held at). Plain-PostgreSQL
               mode has no gate and no rollups — the sweep's DELETE fallback already purged raw there, so no raw
               record is written rather than reporting on a trigger that never runs off Timescale.

               The whole raw step is wrapped: if opening rawConnection or the trigger itself throws, the sweep
               record above already ran and must not be discarded — the raw record carries a single gate_error line
               rather than the whole purge failing over a step that is, from the caller's side, purely additional
               reporting. A shutdown OperationCanceledException still propagates; that is the service stopping, not
               a raw-step failure to report around. */
            if (!timescaleAvailable)
            {
                return;
            }

            var rawTimer = Stopwatch.StartNew();
            List<RawPurgeNowEntry> rawTables;
            try
            {
                await using var rawConnection = await postgres.OpenConnectionAsync(stoppingToken);

                /* #4427 H1: read the clock BEFORE the trigger runs, on the SAME connection the trigger and the
                   report both use. RecordRawLastPurgeOutcomeAsync never throws (TimescaleSupport, logs at
                   Debug on a write failure) — if THIS pass's write fails, ReadRawLastPurgeStateAsync falls
                   back to whatever the PREVIOUS pass recorded, and without this stamp the report would claim
                   that stale record as this run's decision. Comparing against a time taken right here, rather
                   than DateTime.UtcNow, keeps the comparison honest under clock skew between the app host and
                   the database server. */
                DateTime passStartUtc;
                await using (var nowCommand = new NpgsqlCommand("SELECT now()", rawConnection) { CommandTimeout = 30 })
                {
                    passStartUtc = (DateTime)(await nowCommand.ExecuteScalarAsync(stoppingToken))!;
                }

                await TriggerRawPurgeCoreAsync(rawConnection, _logger, stoppingToken);
                rawTables = await BuildRawTablePurgeNowReportAsync(rawConnection, customRetentionDays, passStartUtc, _logger, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "purge_now's raw-table reporting step failed; the sweep above still ran and is recorded");
                rawTables = new List<RawPurgeNowEntry>
                {
                    new("(all three raw tables)", "gate_error", $"Could not report on the gated raw purge this pass: {ex.Message}"),
                };
            }

            foreach (var entry in rawTables)
            {
                _logger.LogInformation(
                    "{Label} raw table {Relation}: {Outcome} - {Note}",
                    runLabel, entry.Relation, entry.Outcome, entry.Note);
            }

            var (rawStatus, rawMessage) = BuildRawPurgeNowRunRecord(runLabel, rawTables);
            var writeRecord = writeRawRunRecord
                ?? ((status, durationMs, message, token) =>
                    DarlingObservability.LogRetentionRunAsync(postgres, status, 0, durationMs, message, _logger, token));
            await writeRecord(rawStatus, rawTimer.ElapsedMilliseconds, rawMessage, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            /* The service is stopping. RunTrackedAsync swallows this as the normal shutdown drain, and the command
               row already said "purge started", so without this line a purge cut short would leave no trace. It may
               have written its totals record already (a stop during the raw step), or neither record. The next daily
               purge uses the configured horizons, not this run's custom one, and purges whatever this one left. */
            _logger.LogWarning(
                "{Label} was cut short because the service is stopping; its run record may be missing or incomplete. "
                + "The next daily purge uses the configured retention horizons",
                runLabel);
            throw;
        }
    }

    /// <summary>
    /// #4427: <c>purge_now</c>'s raw-table reporting step, extracted so <c>Darling.Tests</c> can drive it
    /// directly against a live store (the same reach <see cref="TriggerRawPurgeCoreAsync"/> already has).
    /// Reads back, per <see cref="TimescaleSupport.RawRelations"/> entry, the outcome the trigger pass just
    /// recorded (<see cref="TimescaleSupport.ReadRawLastPurgeStateAsync"/>) — <c>ran</c>, <c>not_covered</c>,
    /// <c>hole</c>, <c>epoch_stale</c>, <c>gate_unknown</c>, <c>gate_error</c>, <c>no_chunks</c> or
    /// <c>run_failed</c> — with a plain-language note. A record that was never written or could not be read
    /// reports as <c>gate_unknown</c>: the trigger just ran on this same connection, so either state means the
    /// gate's own answer for this relation cannot be confirmed, the same fail-closed reading
    /// <see cref="TriggerRawPurgeCoreAsync"/> gives an unresolvable successor. #4825: the entries are typed
    /// (<see cref="RawPurgeNowEntry"/>) because they now feed the run-record
    /// (<see cref="BuildRawPurgeNowRunRecord"/>) rather than a JSON reply.
    ///
    /// <para><b>#4427 H1:</b> <paramref name="passStartUtc"/> is a timestamp read on <paramref name="connection"/>
    /// BEFORE the trigger ran this pass. <c>RecordRawLastPurgeOutcomeAsync</c> never throws — a failed write is
    /// only logged at Debug — so on its own a failed write means <c>ReadRawLastPurgeStateAsync</c> silently
    /// hands back the PREVIOUS pass's record, and the report would then claim that stale decision as this run's.
    /// Any record whose <c>At</c> is earlier than <paramref name="passStartUtc"/> reports <c>gate_unknown</c>
    /// instead, with a plain "no outcome was recorded for this pass" note — never <c>ran</c> or any other
    /// outcome the trigger did not actually just write.</para>
    ///
    /// <para>When <paramref name="customRetentionDays"/> is set and does not match the gated horizon
    /// (<see cref="TimescaleSupport.RawRetentionSpan"/>), every note gets a trailing sentence: SHORTER means
    /// the value was never applied (the gate, not the requested horizon, decides when raw purges); LONGER
    /// means the same thing for the opposite reason — the gated horizon still applies, so a longer custom
    /// value is not honored either.</para>
    /// </summary>
    internal static async Task<List<RawPurgeNowEntry>> BuildRawTablePurgeNowReportAsync(
        NpgsqlConnection connection, int? customRetentionDays, DateTime passStartUtc, ILogger? logger, CancellationToken cancellationToken)
    {
        var gatedHorizonDays = (int)TimescaleSupport.RawRetentionSpan.TotalDays;
        var customDiffersFromGatedHorizon = customRetentionDays is int days && days != gatedHorizonDays;
        var customBelowGatedHorizon = customRetentionDays is int belowDays && TimeSpan.FromDays(belowDays) < TimescaleSupport.RawRetentionSpan;
        var reports = new List<RawPurgeNowEntry>(TimescaleSupport.RawRelations.Count);

        foreach (var relation in TimescaleSupport.RawRelations)
        {
            var (state, record) = await TimescaleSupport.ReadRawLastPurgeStateAsync(connection, relation, logger, cancellationToken);

            string outcome;
            string note;
            if (record is not null && record.At < passStartUtc)
            {
                /* #4427 H1: a record exists, but it predates this pass's own start — it is the PREVIOUS pass's
                   outcome, not this one's. Report honestly rather than pass it off as "ran this pass". */
                outcome = "gate_unknown";
                note = "No outcome was recorded for this pass; the last recorded outcome is from an earlier pass and is not reported here.";
            }
            else
            {
                outcome = record?.Outcome ?? "gate_unknown";
                note = DescribeRawPurgeNowOutcome(outcome, state);
            }

            if (customDiffersFromGatedHorizon)
            {
                note += customBelowGatedHorizon
                    ? $" Raw tables are purged only at the gated horizon ({gatedHorizonDays} days) and only when the gate passes; the custom retention was not applied to them."
                    : $" Raw tables are purged only at the gated horizon ({gatedHorizonDays} days) and only when the gate passes; the longer custom retention does not apply to them.";
            }

            reports.Add(new RawPurgeNowEntry(relation, outcome, note));
        }

        return reports;
    }

    /// <summary>
    /// Plain-language text for one <see cref="BuildRawTablePurgeNowReportAsync"/> entry's <c>outcome</c> —
    /// the same vocabulary <see cref="TriggerRawPurgeCoreAsync"/> records, read back by
    /// <paramref name="readState"/> (used only to distinguish "never written" from "could not be read" when
    /// <paramref name="outcome"/> fell back to <c>gate_unknown</c> for a missing record).
    /// </summary>
    internal static string DescribeRawPurgeNowOutcome(string outcome, TimescaleSupport.RawLastPurgeReadState readState) => outcome switch
    {
        "ran" => "The gated purge ran this pass.",
        "not_covered" => "Held — the rollup does not yet cover the oldest rows.",
        "hole" => "Held — a hole was found inside the range the gate would have dropped.",
        "epoch_stale" => "Held — no materialization repair has finished under the current start yet.",
        "gate_error" => "Held — the gate's own check failed partway through.",
        "no_chunks" => "Nothing to purge — no chunks exist yet.",
        "run_failed" => "Held — the gate passed, but the purge run itself failed.",
        _ => readState == TimescaleSupport.RawLastPurgeReadState.NeverWritten
            ? "Held — a successor's materialization could not be resolved, or no purge pass has recorded an outcome for it yet."
            : "Held — a successor's materialization could not be resolved, or the last outcome could not be read.",
    };

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
        private readonly CancellationToken _stoppingToken;

        public WorkerCommandHost(
            DarlingWorker worker, List<ServerLoopState> servers, DarlingCollectorRunner runner,
            PgPlanFetcher planFetcher, AnalysisNotificationService notificationService, DarlingConfig config,
            CancellationToken stoppingToken)
        {
            _worker = worker;
            _servers = servers;
            _runner = runner;
            _planFetcher = planFetcher;
            _notificationService = notificationService;
            _config = config;
            _stoppingToken = stoppingToken;
        }

        public Task<CommandOutcome> SnapshotNowAsync(int serverId, IReadOnlyList<string>? collectors, CancellationToken cancellationToken)
            => _worker.RunSnapshotAsync(_servers, _runner, serverId, cancellationToken, collectors);

        public Task<CommandOutcome> AnalyzeNowAsync(int serverId, CancellationToken cancellationToken)
            => _worker.RunAnalyzeNowAsync(_servers, _planFetcher, _notificationService, _config, serverId, cancellationToken);

        public Task<CommandOutcome> PurgeNowAsync(int? customRetentionDays, CancellationToken cancellationToken)
        {
            /* #4825: starts the purge in the background and answers at once. The purge outlives this command,
               so it gets the service's own stopping token — the command's token ends with the command. */
            return Task.FromResult(_worker.StartPurgeNow(_config, customRetentionDays, _stoppingToken));
        }

        public Task<CommandOutcome> FetchPlanAsync(int serverId, PlanFetchRequest request, CancellationToken cancellationToken)
            => _worker.RunFetchPlanAsync(_servers, _planFetcher, serverId, request, cancellationToken);

        public Task<CommandOutcome> ExecuteActualPlanAsync(int serverId, ActualPlanRequest request, CancellationToken cancellationToken)
            => _worker.RunExecuteActualPlanAsync(_servers, serverId, request, cancellationToken);

        public Task<CommandOutcome> FetchActiveQueriesLiveAsync(int serverId, CancellationToken cancellationToken)
            => _worker.RunFetchActiveQueriesLiveAsync(_servers, _runner, serverId, cancellationToken);

        public Task<CommandOutcome> TestHypotheticalIndexAsync(int serverId, HypotheticalIndexRequest request, CancellationToken cancellationToken)
            => _worker.RunTestHypotheticalIndexAsync(_servers, serverId, request, cancellationToken);
    }
}
