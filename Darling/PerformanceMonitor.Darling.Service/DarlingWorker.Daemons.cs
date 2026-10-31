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

    /// <summary>#5003: which collector failures have already had their stack written to the service log since this
    /// process started. Read and written by every concurrent collector run, so it is a concurrent table inside.</summary>
    private readonly CollectorFaultStackLog _collectorFaultStacks = new();

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
    /// #4999: drops every slot in <see cref="_detachedCollectorGates"/> held under this server id, when the server is
    /// removed from the monitored set. The slots are never pruned otherwise, and the id is the registration's, so a
    /// server added back has the removed one's id: a slot its removed state's run still holds (going, or queued for
    /// a permit) would make the new state's first daily run skip, which costs a day. The run that holds the old slot
    /// keeps the object it took and releases it when it ends, and nothing else refers to it by then. Only this
    /// server's slots go: another server's run that is still going keeps its slot. A run of the removed state that is
    /// already past its slot and still going is not stopped, so the added-back server's first run can overlap it once.
    /// </summary>
    private void ForgetDetachedRunSlots(int serverId)
    {
        foreach (var key in _detachedCollectorGates.Keys)
        {
            if (key.ServerId == serverId)
            {
                _detachedCollectorGates.TryRemove(key, out _);
            }
        }
    }

    /// <summary>
    /// #4938: the effective interval, in minutes, from which a collector counts as a daily one: it runs detached
    /// from its server's sequential pass instead of inside it. Applied to the interval the schedule resolves for
    /// that server (<see cref="CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes"/>), so it covers the
    /// five on-load collectors' once-a-day recapture, the catalog's 1440-minute collectors, and any collector an
    /// operator slows to a day or more.
    /// </summary>
    internal const int DailyCollectorIntervalMinutes = SweepPressureReport.DailyCollectorIntervalMinutes;

    /// <summary>
    /// #4938: the most detached daily runs that may be going at once across the whole fleet. Each holds a connection
    /// to its monitored server and one to the store for as long as it runs (index_object_stats took 43 minutes
    /// across 72 databases in a field case), so the cap is what bounds the load that daily work can add beside the
    /// sweep. A run over the cap waits for a permit and is never dropped.
    ///
    /// <para>#4999: this is the ceiling, and the cap in force is never above it. The store's connection pool bounds
    /// it too, because a daily run holds a store connection for its whole run: see <see cref="DailyRunCapFor"/>.</para>
    /// </summary>
    internal const int MaxConcurrentDailyRuns = 16;

    /// <summary>
    /// #4999: how many connections of the store pool the daily-run cap leaves free for everything that is neither a
    /// sweep body nor a daily run: the MCP tools, the web viewer's reads and the alert reads each borrow one for the
    /// length of a query. Eight is a ruled round number, room for a handful of those reads at once, and not a
    /// measured peak. The point of it is which side waits: a daily run that waits for a permit costs nothing, and
    /// a read that waits for a connection is a stall someone sees. It is a constant and not a setting, so the cap
    /// has one rule to explain.
    /// </summary>
    internal const int DailyRunPoolReserve = 8;

    /// <summary>
    /// #4999: how many detached daily runs may go at once, given the store connection pool's size and the sweep
    /// width. Every daily run holds a store connection for its whole run and so does every sweep body, so the runs
    /// get what the pool has left after the sweep and <see cref="DailyRunPoolReserve"/>: the smaller of
    /// <see cref="MaxConcurrentDailyRuns"/> and (pool size - sweep width - reserve), and never below one so daily
    /// collection still happens on a pool that is too small. A pool of 24 and a sweep width of 9 gives 7; a pool of
    /// 100 gives 16. A null pool size means the pool bounds nothing (pooling is off), so the ceiling applies.
    /// </summary>
    internal static int DailyRunCapFor(int? poolMaxSize, int sweepWidth)
    {
        if (poolMaxSize is not { } poolMax)
        {
            return MaxConcurrentDailyRuns;
        }

        return (int)Math.Clamp((long)poolMax - sweepWidth - DailyRunPoolReserve, 1, MaxConcurrentDailyRuns);
    }

    /// <summary>
    /// #5479: how many servers the fleet self-alert pass evaluates at once, given the sweep width: the smaller of the
    /// width and half of <see cref="DailyRunPoolReserve"/> (so at most 4). The pass is nothing but store reads, and they
    /// run beside the sweep bodies and the daily runs, which <see cref="DailyRunCapFor"/> already counts. Those reads are
    /// the alert reads the reserve is documented to cover, so the pass spends at most half of it and the web viewer's and
    /// the MCP tools' reads always keep the other half. A pool of 24 with a sweep width of 16 runs 16 bodies, the pass at
    /// 4, and a daily-run cap of 1: 21 of 24, with 3 left. The same pass at the full width of 16 would need 33.
    /// </summary>
    internal static int SelfAlertPassWidthFor(int sweepWidth) =>
        Math.Max(1, Math.Min(StoreConfigProvider.ClampConcurrentSweeps(sweepWidth), DailyRunPoolReserve / 2));

    /// <summary>
    /// #5642: how many servers the engine's alert reads (and the pileup finding) run at once. The self-alert step and the
    /// engine pass are two single-flight passes that can overlap when an engine pass outlasts a cadence, so they split
    /// the <see cref="SelfAlertPassWidthFor"/> budget instead of each taking all of it: the engine pass takes half
    /// (at least one), <see cref="SelfAlertStepWidthFor"/> takes the rest. At the widest setting that is 2 and 2 of the
    /// 4 the pool's alert share allows, so the web viewer's and the MCP tools' half of the reserve stays free. The one
    /// exception is a sweep width of 1: the budget is 1, both widths floor to 1, and the two passes hold 2 connections.
    /// </summary>
    internal static int AlertEnginePassWidthFor(int sweepWidth) =>
        Math.Max(1, SelfAlertPassWidthFor(sweepWidth) / 2);

    /// <summary>#5642: how many servers the self-alert step (Collection Stopped and its siblings, the custom rules) runs
    /// at once: what <see cref="AlertEnginePassWidthFor"/> leaves of the <see cref="SelfAlertPassWidthFor"/> budget.</summary>
    internal static int SelfAlertStepWidthFor(int sweepWidth) =>
        Math.Max(1, SelfAlertPassWidthFor(sweepWidth) - AlertEnginePassWidthFor(sweepWidth));

    /// <summary>
    /// #4999: the largest number of connections the store's data source will hold open, read from the connection
    /// string the data source was built with, so it is the operator's <c>Maximum Pool Size</c> and not the managed
    /// default. A string that names none gets Npgsql's own default (100). Null when the data source is null or
    /// pooling is off, which leaves the pool bounding nothing.
    /// </summary>
    internal static int? StorePoolMaxSize(NpgsqlDataSource? dataSource)
    {
        if (dataSource is null)
        {
            return null;
        }

        try
        {
            var builder = new NpgsqlConnectionStringBuilder(dataSource.ConnectionString);
            return builder.Pooling ? builder.MaxPoolSize : null;
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    /// <summary>
    /// #4938, #4999: the permits behind the daily-run cap. An instance field and a limiter of its own, never a
    /// waiting mode on <see cref="DetachedCollectorGate"/>, whose try-only contract is what makes a held
    /// single-flight slot mean "skip this tick". A daily run takes its (server, collector) slot first and waits
    /// here second, so a run that is still queued for a permit counts as unfinished and the next tick skips. It
    /// starts at <see cref="MaxConcurrentDailyRuns"/> and <see cref="ApplyDailyRunCap"/> moves it to the cap the
    /// store pool allows once the collection loop knows both.
    /// </summary>
    private readonly DailyRunLimiter _dailyRunPermits = new(MaxConcurrentDailyRuns);

    /// <summary>Test hook (#4999): how many detached daily runs may go at once right now.</summary>
    internal int DailyRunCap => _dailyRunPermits.Cap;

    /// <summary>Test hook (#4999): how many detached daily runs hold a permit right now.</summary>
    internal int DailyRunsHoldingPermits => _dailyRunPermits.InUse;

    /// <summary>
    /// #4999: whether <see cref="ApplyDailyRunCap"/> has logged the cap yet. The first call says what the cap is
    /// at start, every later one only when it moved.
    /// </summary>
    private bool _dailyRunCapLogged;

    /// <summary>
    /// #4999: sets the daily-run cap from the store pool and the sweep width (<see cref="DailyRunCapFor"/>) and
    /// logs the effective cap, once at start and again each time it changes. Called when the collection loop
    /// starts and every time the sweep width moves, so the cap follows the knob. Runs already going keep their
    /// permits when the cap narrows, and a widening starts the runs that were waiting.
    /// </summary>
    internal void ApplyDailyRunCap(int? poolMaxSize, int sweepWidth)
    {
        var cap = DailyRunCapFor(poolMaxSize, sweepWidth);
        var changed = _dailyRunPermits.SetCap(cap);
        if (_dailyRunCapLogged && !changed)
        {
            return;
        }

        _dailyRunCapLogged = true;
        _logger.LogInformation(
            "Daily collectors run at most {Cap} at a time: the smaller of {Ceiling} and the store's connection pool " +
            "({Pool}) minus the sweep width ({Width}) minus {Reserve} kept free for reads, and at least 1 (#4999)",
            cap, MaxConcurrentDailyRuns, poolMaxSize?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "unbounded", sweepWidth, DailyRunPoolReserve);
    }

    /// <summary>
    /// #4938, #4999: the detached runs that have not finished, so a shutdown can wait for them the way it waits for
    /// the per-server bodies. They used to run inside those bodies and so inside that wait; detached, nothing else
    /// holds on to them. That is every daily run and, since #4999, the three collectors detached by name. A run
    /// removes itself when it ends.
    /// </summary>
    private readonly ConcurrentDictionary<Task, byte> _detachedRuns = new();

    /// <summary>Test hook and shutdown-drain input (#4938, #4999): the detached runs that have not finished, daily and by name.</summary>
    internal IReadOnlyCollection<Task> InFlightDetachedRuns => _detachedRuns.Keys.ToArray();

    /// <summary>
    /// #4999: the detached runs that are executing right now, one per (server, collector), which the hang watchdog
    /// (<see cref="WatchDailyRuns"/>) reads each sweep tick: the daily runs, and the three runs detached by name
    /// (query_store, plan_correction, pg_wait_sampling). A daily run is added once it holds its permit, so a run that
    /// is only waiting for one is never in it: queue time is capacity pressure, not a hang, the same split the
    /// sweep watchdog makes (<see cref="ClassifySweepEpisode"/>). A run detached by name takes no permit and is added
    /// as it starts executing. One run per key holds because every run is added AFTER the single-flight gate that
    /// keeps its collector to one run per server, and is removed BEFORE that gate is released: query_store's
    /// per-server <see cref="QueryStoreServerGate"/>, the <see cref="DetachedCollectorGate"/> of plan_correction,
    /// pg_wait_sampling and every daily run. A second run that comes due while the first is going is skipped at
    /// the gate, before it is watched.
    /// </summary>
    private readonly ConcurrentDictionary<(int ServerId, string CollectorName), DailyRunWatch> _dailyRunWatches = new();

    /// <summary>
    /// #4999: starts watching one detached run that is now executing: a daily run that has just taken its permit
    /// (<paramref name="holdsPermit"/>), or a run detached by name, which takes none. Dispose the result when the
    /// run ends and the run is no longer watched.
    /// </summary>
    private DailyRunWatch BeginDailyRunWatch(string serverName, int serverId, string collectorName, bool holdsPermit)
    {
        var watch = new DailyRunWatch(serverId, serverName, collectorName, holdsPermit, DateTime.UtcNow, EndDailyRunWatch);
        _dailyRunWatches[(serverId, collectorName)] = watch;
        return watch;
    }

    /// <summary>
    /// #4999: stops watching a detached run that ended, removing only that run's own entry, and says so when the
    /// watchdog had already warned about it, the way a sweep body's resolution is logged.
    /// </summary>
    private void EndDailyRunWatch(DailyRunWatch watch)
    {
        _dailyRunWatches.TryRemove(new KeyValuePair<(int ServerId, string CollectorName), DailyRunWatch>(
            (watch.ServerId, watch.CollectorName), watch));
        if (!watch.Warned)
        {
            return;
        }

        var elapsedSeconds = (DateTime.UtcNow - watch.StartedUtc).TotalSeconds;
        if (watch.HoldsPermit)
        {
            _logger.LogInformation(
                "[{Server}] {Collector} daily run completed after {Elapsed:F0}s of execution",
                watch.ServerName, watch.CollectorName, elapsedSeconds);
        }
        else
        {
            _logger.LogInformation(
                "[{Server}] {Collector} run completed after {Elapsed:F0}s of execution",
                watch.ServerName, watch.CollectorName, elapsedSeconds);
        }
    }

    /// <summary>
    /// #4999: seconds a detached run may execute before the hang watchdog (<see cref="WatchDailyRuns"/>) reports
    /// it: 15 minutes, for a daily run and for each of the three runs detached by name alike. It is its own number
    /// and not <see cref="SweepWatchdogSeconds"/>'s 60 s, because a detached run is the slow end of the catalog and a
    /// minute is not a stall there. database_config reads every database on its server, so on a server with many
    /// databases a healthy daily run takes minutes, and a 60 s line warned once a day, on every such server, for no
    /// fault; query_store takes 100 to 230 s on a healthy server, so at 60 s it would have warned on every run, which
    /// is why the three detached by name were first left out and are watched now that the line is 15 minutes. A
    /// command deadline cannot be the measure either: a run issues many commands (database_config issues a set per
    /// database), each bounded by its own deadline, so no single deadline bounds a whole run. A run still going after
    /// 15 minutes is long enough past a healthy one to be worth saying so, and while it goes its server's next run
    /// of that collector is skipped, and a daily run also keeps one of the daily-run permits. The sweep keeps its own
    /// 60 s for its own work.
    /// </summary>
    internal const int DailyRunWatchdogSeconds = 15 * 60;

    /// <summary>
    /// #4999: the hang watchdog for detached runs, called once per sweep tick with the current time: the daily
    /// runs and the three runs detached by name (query_store, plan_correction, pg_wait_sampling). A run that has
    /// been executing for <see cref="DailyRunWatchdogSeconds"/> or more gets ONE Warning that names its server and
    /// collector, the same action the sweep watchdog takes for a body that has not finished
    /// (<see cref="ClassifySweepEpisode"/>), on a threshold of its own. Before this a stuck detached run showed only
    /// as stale data, hours later. A run waiting for a permit is not executing and is not reported here, and its
    /// wait is not counted toward the threshold. Returns how many runs it warned about, for the test.
    /// </summary>
    internal int WatchDailyRuns(DateTime nowUtc)
    {
        var warned = 0;
        foreach (var watch in _dailyRunWatches.Values)
        {
            var runningSeconds = (nowUtc - watch.StartedUtc).TotalSeconds;
            if (watch.Warned || runningSeconds < DailyRunWatchdogSeconds)
            {
                continue;
            }

            watch.MarkWarned();
            warned++;
            if (watch.HoldsPermit)
            {
                _logger.LogWarning(
                    "[{Server}] {Collector} daily run has not completed after {Elapsed:F0}s of execution - it keeps one of the {Cap} daily-run permits and its next run is skipped until it ends (#4999)",
                    watch.ServerName, watch.CollectorName, runningSeconds, _dailyRunPermits.Cap);
            }
            else
            {
                /* A run detached by name takes no daily-run permit, so the line does not say it keeps one. */
                _logger.LogWarning(
                    "[{Server}] {Collector} run has not completed after {Elapsed:F0}s of execution - its next run is skipped until it ends (#4999)",
                    watch.ServerName, watch.CollectorName, runningSeconds);
            }
        }

        return warned;
    }

    /// <summary>
    /// #4938: whether a collector whose effective interval is <paramref name="effectiveIntervalMinutes"/> is a
    /// daily one, and so runs detached (see <see cref="DailyCollectorIntervalMinutes"/>). Takes the EFFECTIVE
    /// interval, where an on-load collector's 0 has already become its daily recapture.
    /// </summary>
    internal static bool IsDailyInterval(int effectiveIntervalMinutes) =>
        SweepPressureReport.IsDailyInterval(effectiveIntervalMinutes);

    /// <summary>
    /// #4938, #4999: whether a finished run's cost goes into the sweep body's peer high-water mark (#2864). Not
    /// for a run that ran detached, and not for a budgeted collector. A detached run ran beside the body rather
    /// than in it, finishes minutes to hours after the body that dispatched it, and would put its own duration
    /// into whichever body is running at that moment, which then reads as slow peers that were never slow. That
    /// holds for every detached run, so it is keyed on <see cref="RanDetached"/> and not on the budget:
    /// pg_wait_sampling is detached and carries no budget, and its ~30 s window used to fold in.
    /// </summary>
    internal static bool FoldsIntoSweepPeerMark(string collectorName, bool detachedDaily) =>
        !RanDetached(collectorName, detachedDaily) && !CollectorCatalog.HasWallClockBudget(collectorName);

    /// <summary>
    /// #4999: whether this run ran detached from the sweep body: a daily collector's scheduled run
    /// (<paramref name="detachedDaily"/>, set by the dispatch that detaches it) or one of the three that are
    /// detached by name (<see cref="IsDetachedByName"/>), whose every scheduled run is.
    /// </summary>
    internal static bool RanDetached(string collectorName, bool detachedDaily) =>
        detachedDaily || IsDetachedByName(collectorName);

    /// <summary>
    /// #4999: folds a finished run's SQL time into the sweep body's peer high-water mark when
    /// <see cref="FoldsIntoSweepPeerMark"/> says it counts. The write is here, not inline in the run, so a test
    /// can watch the mark itself: that a detached run leaves it alone is a statement about the value, and the
    /// run that would change it needs a store.
    /// </summary>
    internal static void FoldIntoSweepPeerMark(ServerLoopState server, string collectorName, bool detachedDaily, long sqlMs)
    {
        if (FoldsIntoSweepPeerMark(collectorName, detachedDaily))
        {
            server.SweepPeerMaxMs = (int)Math.Min(int.MaxValue, Math.Max(server.SweepPeerMaxMs, sqlMs));
        }
    }

    /// <summary>
    /// #4938, #4999: keeps a detached run, a daily run or one of the three detached by name, in
    /// <see cref="InFlightDetachedRuns"/> until it ends. The removal is attached after the add, so a run that finished
    /// in between is still removed.
    /// </summary>
    private void TrackDetachedRun(Task run)
    {
        if (run.IsCompleted)
        {
            return;
        }

        _detachedRuns.TryAdd(run, 0);
        _ = run.ContinueWith(
            static (finished, state) => ((ConcurrentDictionary<Task, byte>)state!).TryRemove(finished, out _),
            _detachedRuns,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>
    /// #5985: the shutdown drain's query-tag step. Waits (briefly) for the map pass in flight, which on the stop's cancellation
    /// puts back the sightings its own flush had taken, and then flushes the buffer once more under its own time limit
    /// (<see cref="QueryTagSightingBuffer.ShutdownFlushLimit"/>), because the service's token is already cancelled. Never throws.
    /// </summary>
    internal static async Task DrainQueryTagSightingsAsync(Task? mapPass, QueryTagSightingBuffer sightings)
    {
        if (mapPass is { IsCompleted: false })
        {
            await Task.WhenAny(mapPass, Task.Delay(QueryTagSightingBuffer.ShutdownFlushLimit, CancellationToken.None));
        }

        await sightings.FlushOnShutdownAsync(QueryTagSightingBuffer.ShutdownFlushLimit);
    }

    /// <summary>
    /// The shutdown wait for the work that is still going when the sweep loop ends (#1553): the per-server bodies and
    /// the store ticks the caller collected (<paramref name="inFlight"/>) and, since #4938, the detached daily runs,
    /// which used to run inside those bodies and so inside this wait, and, since #4999, the three collectors that
    /// are detached by name. The stopping token has already cancelled them,
    /// and a run still queued for a daily-run permit ends at once on it. None of them faults (RunDetachedAsync
    /// contains the cancel), so the wait completes cleanly. Bounded by the drain budget, so a run that is genuinely
    /// hung cannot hold shutdown open. A method of its own (#4938) so a test can call it; the shutdown calls it once.
    /// </summary>
    internal async Task DrainInFlightAsync(List<Task> inFlight)
    {
        inFlight.AddRange(InFlightDetachedRuns);

        if (inFlight.Count > 0)
        {
            await Task.WhenAny(
                Task.WhenAll(inFlight),
                Task.Delay(s_shutdownDrainBudget, CancellationToken.None));
        }
    }

    /// <summary>
    /// #4938: takes one of the fleet's daily-run permits, waiting for it when all are in use. Waits on the
    /// service's own token, so only shutdown ends the wait and a run is never dropped for waiting. The wait is
    /// logged once, at Debug, when it starts: a run waits once, so a tick that skips behind it adds no line.
    /// </summary>
    private async Task<IDisposable> AcquireDailyRunPermitAsync(ServerLoopState server, string collectorName, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_dailyRunPermits.TryAcquire())
        {
            _logger.LogDebug(
                "  [{Server}] {Collector} is waiting for one of {Cap} daily-run permits (#4938)",
                server.Config.DisplayName, collectorName, _dailyRunPermits.Cap);
            await _dailyRunPermits.AcquireAsync(cancellationToken);
        }

        return new DailyRunPermit(_dailyRunPermits);
    }

    /// <summary>
    /// #4938, #4999: what a detached daily run works on once it has its permit, or <c>null</c> when it must not run.
    /// The wait can last hours behind other daily runs, so the run asks again everything its dispatch asked, against
    /// the server as it is now: that it is not removed and still connected (the runtime it returns is the current one,
    /// not the one captured at dispatch), that the collector still applies to that connection's target (the engine
    /// half and the within-engine half, both in <see cref="CollectorCatalog.AppliesTo(string, CollectorTargetInfo)"/>),
    /// that its effective schedule still has it enabled, and that collection is not paused (the loop dispatches
    /// nothing while it is). A run that was dispatched before the change and starts after it would otherwise collect
    /// from a server the operator had removed, turned the collector off for, or paused.
    ///
    /// <para>A run skipped for the pause is put back to due. Its dispatch moved the due time a day on, and nothing
    /// else moves a due time when collection resumes (a reload never moves one forward), so left alone it would not
    /// run until tomorrow. The other skips need no such help: a removed server has no schedule, a collector that was
    /// turned off is seeded again from its last run when it is turned on, and a reconnect seeds every collector that
    /// applies.</para>
    /// </summary>
    private ServerRuntime? ReReadAfterPermitWait(ServerLoopState server, string collectorName)
    {
        if (server.Retired || server.Runtime is not { } current)
        {
            return null;
        }

        if (!CollectorCatalog.AppliesTo(collectorName, current.Target)
            || !StoreConfigProvider.ResolveSchedule(collectorName, current.ServerId, _scheduleOverrides).Enabled)
        {
            return null;
        }

        if (!ShouldRunCollection(_paused))
        {
            server.NextDue[collectorName] = DateTime.UtcNow;
            return null;
        }

        return current;
    }

    /// <summary>
    /// #4938: one held daily-run permit. Returns it once and only once, however many times it is disposed, so a
    /// double dispose can never hand a permit back that another run has since taken.
    /// </summary>
    private sealed class DailyRunPermit : IDisposable
    {
        private DailyRunLimiter? _permits;

        public DailyRunPermit(DailyRunLimiter permits) => _permits = permits;

        public void Dispose() => Interlocked.Exchange(ref _permits, null)?.Release();
    }

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

    /// <summary>#5640 (H2): the permission fault the attention run records as PERMISSIONS, or null for any other collector and when the ensure was not denied.</summary>
    internal static string? AttentionPermissionFaultFor(string collectorName, ServerLoopState server) =>
        IsAttentionCollector(collectorName) ? server.AttentionXePermissionFault : null;

    /// <summary>
    /// #6893: the ensure failure the deadlock or blocked-process run records instead of reading a session that does not exist, or null for
    /// any other collector and when the last ensure left the session in place.
    /// </summary>
    internal static DarlingXeSessions.XeEnsureFailure? CaptureEnsureFaultFor(string collectorName, ServerLoopState server) =>
        string.Equals(collectorName, DeadlocksCollector.Instance.Name, StringComparison.OrdinalIgnoreCase) ? server.CaptureEnsure.Deadlock
        : string.Equals(collectorName, BlockedProcessReportCollector.Instance.Name, StringComparison.OrdinalIgnoreCase) ? server.CaptureEnsure.BlockedProcess
        : null;

    /// <summary>#6893: the partial note (Azure SQL Database, the create failed in some databases) merged onto a successful deadlock or blocked-process run, or null.</summary>
    internal static string? CaptureEnsurePartialNoteFor(string collectorName, ServerLoopState server) =>
        string.Equals(collectorName, DeadlocksCollector.Instance.Name, StringComparison.OrdinalIgnoreCase) ? server.CaptureEnsure.DeadlockPartialNote
        : string.Equals(collectorName, BlockedProcessReportCollector.Instance.Name, StringComparison.OrdinalIgnoreCase) ? server.CaptureEnsure.BlockedProcessPartialNote
        : null;

    internal static bool IsAttentionCollector(string collectorName) =>
        string.Equals(collectorName, AttentionCollector.Instance.Name, StringComparison.OrdinalIgnoreCase);

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
    /// #4999: whether a collector runs detached from its server's sequential pass BY NAME: query_store (#2700),
    /// plan_correction (#2717) and pg_wait_sampling (#3604), each for a cost that does not belong in the pass.
    /// The daily collectors are the other half, detached by cadence (<see cref="IsDailyInterval"/>), and the two
    /// together are <see cref="RunsDetached"/>. The parameter is <c>name</c>, as in the dispatch loop: the Lite
    /// suite counts the plan_correction predicate's calls that are spelled with <c>collectorName</c>, to pin that
    /// the gate acquisition tests it once, and this is not that call.
    /// </summary>
    internal static bool IsDetachedByName(string name) =>
        SweepPressureReport.IsDetachedByName(name);

    /// <summary>
    /// #4999: THE test for whether a collector's scheduled run leaves its server's sequential pass: the three
    /// detached by name (<see cref="IsDetachedByName"/>) and every daily collector by cadence
    /// (<see cref="IsDailyInterval"/>, over the EFFECTIVE interval, where an on-load collector's 0 is already its
    /// daily recapture). The pass dispatches by it, and get_collection_health leaves exactly these collectors
    /// out of its sweep-body roll-up by it, so the two cannot disagree about what runs in the pass: a detached
    /// run is not part of the body, and a long run of it is not an overrun of the body.
    /// </summary>
    internal static bool RunsDetached(string name, int effectiveIntervalMinutes) =>
        SweepPressureReport.RunsDetached(name, effectiveIntervalMinutes);

    /// <summary>
    /// #4999: the (server, collector) slot an INLINE run of a daily collector takes: the at-connect run of an on-load
    /// collector (<see cref="RunOnLoadAsync"/>) and an operator snapshot. It is the slot the detached scheduled run
    /// holds for as long as it is going or queued for a permit, so an inline run never starts beside it. Returns the
    /// lease; <c>null</c> when the slot is held, which the caller reads as "that collector's scheduled run is already
    /// going: leave this one out"; and <see cref="DetachedCollectorGate.NotGated"/> for a collector this does not
    /// cover, which is one that does not run detached and one of the three detached by name, whose slot
    /// <see cref="RunOneAsync"/> takes itself (taking it here too would make that run find it held).
    /// </summary>
    private IDisposable? TryTakeInlineDailySlot(int serverId, string collectorName, EffectiveSchedule effective) =>
        RunsDetached(collectorName, CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(effective.FrequencyMinutes))
        && !IsDetachedByName(collectorName)
            ? _detachedCollectorGates.GetOrAdd((serverId, collectorName), static _ => new DetachedCollectorGate()).TryAcquire()
            : DetachedCollectorGate.NotGated;

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

        var fetchClock = Stopwatch.StartNew();
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

            /* #5320: a refresh that worked writes a SUCCESS row under the same name the failure row uses. Collection
               health bands a name from the counts of its rows and the age of its newest success, so a name that
               only ever wrote errors would read failing until the error aged out of the window, whatever the
               refreshes after it did. The row carries the rows stored and the fetch-plus-store time; a due check
               that finds nothing due, and a fetch that returns no statements, write none. */
            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, PgStatementText.CollectorName, "SUCCESS", queryIds.Count, fetchClock.ElapsedMilliseconds, 0,
                errorMessage: null,
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: null, _logger, cancellationToken);
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

            /* #5320: the failure is also an ERROR row in the collection log under the text fetch's own name, so a
               fetch that keeps timing out on the target shows in collection health instead of only in the service
               log. The statistics run's own row stays as it was written: its data was collected. */
            await DarlingObservability.LogCollectionAsync(
                _postgres!, runtime, PgStatementText.CollectorName, "ERROR", 0, fetchClock.ElapsedMilliseconds, 0,
                PgStatementText.DescribeFailure(ex),
                fanout: null, phases: null, drain: null, fetchPhases: null, sweepPeerMaxMs: null, _logger, cancellationToken);
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
    internal static async Task<(List<long> QueryIds, List<string> Texts)> ReadPgStatementTextAsync(
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

        await using var connection = PostgresTargetProvider.CreatePinnedConnection(runtime.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        /* #6143: the view and the Aurora function are named by the schema read on this connection, as the collectors
           name them, never by public. */
        var schemas = new CollectorContext
        {
            ServerId = runtime.ServerId,
            ServerName = runtime.Config.DisplayName,
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
            Target = runtime.Target,
        };
        await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(
            schemas, PgStatementStatsCollector.Instance, connection, logger: null, cancellationToken);
        await using var command = new Npgsql.NpgsqlCommand(
            PgStatementText.FetchSqlFor(runtime.Target.IsAurora, runtime.Target.PostgresMajorVersion, schemas),
            connection) { CommandTimeout = PgStatementText.FetchCommandTimeoutSeconds };
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
    /// #5632 Phase A (ruling R4): every <see cref="s_investigationSweepInterval"/>, <see cref="InvestigationRequestStore.SweepAsync"/>
    /// expires lapsed leases and old requests, through the shared <see cref="InvestigationDispatchCore.SweepOnceAsync"/> (#5820). It honors the service pause as the external alert loop does, and a failed
    /// pass is logged and tried again next tick.
    /// </summary>
    private async Task RunInvestigationSweepLoopAsync(InvestigationRequestStore store,
        PerformanceMonitor.Darling.Service.Investigations.InvestigationEventDelivery events,
        PerformanceMonitor.Darling.Service.Investigations.DarlingDeadlockRollup deadlockRollup,
        PerformanceMonitor.Darling.Service.ChangeWatch.ChangeWatchPass changeWatchPass, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (ShouldRunCollection(_paused))
            {
                /* #6234: its own try, like the passes below. */
                try
                {
                    await changeWatchPass.RunOnceAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Change watch pass failed; it is tried again next pass");
                }

                /* #6005: the deadlock digest, once per 4-hour bucket; its own try, like the two below. */
                try
                {
                    await deadlockRollup.RunDigestAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Deadlock digest failed; it is tried again next pass");
                }

                try
                {
                    await InvestigationDispatchCore.SweepOnceAsync(store, _logger, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Investigation sweep failed; it is tried again next pass");
                }

                /* #6003: its own try, so a failed sweep still delivers what is already journaled, and the reverse. */
                try
                {
                    await events.RunOnceAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Investigation event delivery failed; it is tried again next pass");
                }
            }

            try
            {
                await Task.Delay(s_investigationSweepInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>
    /// #6001: every <see cref="s_investigationEvidenceInterval"/>, the evidence capture freezes the evidence of the High CPU
    /// requests that still need it, from the store's own rows. It honors the service pause as the sweep does, and a failed
    /// pass is logged and tried again next tick.
    /// </summary>
    private async Task RunInvestigationEvidenceLoopAsync(
        PerformanceMonitor.Darling.Service.Investigations.InvestigationEvidenceCapture capture, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (ShouldRunCollection(_paused))
            {
                try
                {
                    await capture.CaptureOnceAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Investigation evidence capture failed; it is tried again next pass");
                }
            }

            try
            {
                await Task.Delay(s_investigationEvidenceInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>
    /// #5654: the external alert drain's loop, one pass every <see cref="s_externalAlertInterval"/> on its own task, beside
    /// the command loop. While the service is paused a tick does nothing at all (no expiry, no reap, no claim, no send), the
    /// same as every other alert family; rows that waited out a pause expire on the first pass after the resume. With the
    /// master switch off a pass still expires and reaps but claims and sends nothing (the switch is read inside the drain).
    /// A failed pass is logged and tried again on the next tick; only cancellation ends the loop.
    /// </summary>
    private async Task RunExternalAlertLoopAsync(ExternalAlertDrain drain, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (ShouldRunCollection(_paused))
            {
                try
                {
                    var pass = await drain.RunPassAsync(stoppingToken);
                    if (pass.Claimed > 0 || pass.Expired > 0)
                    {
                        _logger.LogInformation(
                            "External alerts: claimed {Claimed} (delivered {Delivered}, muted {Muted}, no channel {NoChannel}, retry {Retried}, failed {Failed}, ignored {Ignored}, interrupted {Interrupted}), expired {Expired}",
                            pass.Claimed, pass.Delivered, pass.Muted, pass.NoChannel,
                            pass.Retried, pass.Failed, pass.Ignored, pass.Interrupted, pass.Expired);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "External alert pass failed; it is tried again next pass");
                }
            }

            try
            {
                await Task.Delay(s_externalAlertInterval, stoppingToken);
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
        MuteRuleService muteRuleService, DarlingCollectorRunner runner, CancellationToken cancellationToken)
    {
        var view = await provider.LoadViewAsync(config, cancellationToken);
        if (view is null)
        {
            return null;
        }

        StoreConfigProvider.ApplyToConfig(config, view);
        _collectorState.SigningStatus.Publish(config.Webhooks, config.NotificationRoutes, DateTime.UtcNow); // #5998: presence and dates for the web page, no key text
        DarlingSmtpWarning.Log(config.Smtp, _logger); // the reloaded SMTP settings, same warning as at start
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
        PerformanceMonitor.Darling.Analysis.PgFactCollector.PlanRegressionCandidateCache.Shared.RetainServers(view.EnabledServers.Select(s => s.ServerId));
        _scheduleOverrides = view.ScheduleOverrides;
        /* Stage 2: honor a pause/resume issued through the store (config_service.paused) — the collection
           loop reads this on its next tick. Single writer (this reload), so no interlock needed. */
        _paused = view.Paused;

        /* Structural reconcile mutates the server list; the command loop reads it concurrently, so hold
           the lock across the add/remove. NextDue recompute mutates only per-server state (safe against a
           concurrent id lookup) so it stays outside the lock. */
        List<RemovedLongQueryServer> removedServers;
        lock (_serversLock)
        {
            /* #4961: the reconcile clears a removed server's runtime and drops its state in one synchronous step, so what the
               drop below needs (the definition, the runtime, the long-query latch) is taken first. */
            removedServers = DarlingRemovedServerSessions.Capture(servers, view.EnabledServers, runner);
            ReconcileServers(servers, view.EnabledServers);

            /* #5452: an assumed-role session is kept only while an enabled server names its role and external ID. */
            runner.RetainAwsRoles(servers.Select(state => state.Config.AwsRoleKey).OfType<Targets.AwsRoleKey>());
        }

        /* #4961: a removed server's sessions of this install's go with it, awaited here, after the lock is released: the
           servers' lock is never held across a connection. */
        await DropRemovedServerSessionsAsync(removedServers, runner, cancellationToken);

        await RecomputeNextDueAsync(servers, cancellationToken);

        /* Mirror the desired enable state onto the observed collect.servers registry so a disable_server
           flips its observed row FALSE even though the disabled server drops out of the loop (stops
           upserting), and an enable_server flips it back. */
        await DarlingObservability.SyncServerEnabledStatesAsync(_postgres!, _logger, cancellationToken);

        await LoadMuteRulesAsync(muteRuleService);
        await LoadThresholdOverridesAsync();

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
    /// The removed servers of one reload drop this install's sessions on them (#4961), one at a time, each in one attempt
    /// within <see cref="DarlingRemovedServerSessions.Timeout"/>. The registry already holds the servers that remain, so
    /// the removed one holds nothing back. Nothing it meets stops the reload: the drop logs its own failures.
    /// </summary>
    private async Task DropRemovedServerSessionsAsync(
        List<RemovedLongQueryServer> removedServers, DarlingCollectorRunner runner, CancellationToken stoppingToken)
    {
        if (removedServers.Count == 0)
        {
            return;
        }

        var remaining = _registryState.Read()?.Servers;
        bool TraceOn(int otherId) => StoreConfigProvider.ResolveSchedule("long_query_completions", otherId, _scheduleOverrides).Enabled;
        foreach (var removed in removedServers)
        {
            using var sessionDrop = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            sessionDrop.CancelAfter(DarlingRemovedServerSessions.Timeout);
            /* On premises the guard reads the other registrations' last-known instance names, inside the same timeout. */
            await DarlingRemovedServerSessions.DropAsync(
                removed,
                runner,
                remaining,
                TraceOn,
                token => LongQueryTraceInstanceGuardFor(
                    removed.Runtime.ServerId, remaining, TraceOn, (otherId, carrier) => runner.GetCollectorStateAsync(otherId, carrier, token)),
                _logger,
                sessionDrop.Token);
        }
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
    /// Reloads the threshold overrides (#5916). A failed read keeps the overrides already in force and is counted, as the
    /// mute-rule reload is, so a store blip never drops a tuned threshold back to the global one.
    /// </summary>
    private async Task LoadThresholdOverridesAsync()
    {
        var postgres = _postgres;
        if (postgres is null)
        {
            return;
        }

        var readClock = Stopwatch.StartNew();
        try
        {
            await _thresholdOverrides.LoadAsync(postgres);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Could not reload threshold overrides after {ElapsedMs} ms - the {Overrides} override(s) already in force "
                + "stay in force until a read succeeds: {Message}",
                readClock.ElapsedMilliseconds, _thresholdOverrides.Count, ex.Message);
            _readFailures.RecordReadFailure(null, "threshold-override reload", readClock.ElapsedMilliseconds);
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
                state.LastKnownRuntime = null;
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
                /* The RDS endpoint verdict and fresh login held for this id go with the server: a re-add checks again. */
                _runner?.ForgetRdsVerdicts(id);
                /* #5518: and the backfill's cached database list and the fence's database names for it. */
                _queryStoreBackfill?.ForgetServer(id);
                /* #4999: and its single-flight slots. The id is the registration's, so a re-add carries the same one, and a
                   run of this removed state that is still going, or still queued for a permit (hours, behind other daily
                   runs), would hold the slot the re-added server's first daily run needs. That run would skip, and a
                   skipped daily run waits a day. The old run keeps the slot object it holds and gives it back harmlessly;
                   it ends at its own re-read (Retired) when it still waits for a permit. */
                ForgetDetachedRunSlots(id);
                servers.RemoveAt(i);
                continue;
            }

            /* Always refresh the held definition so a NON-connection edit — the FinOps cost, or the #1236
               alert-delivery override the deliverer's resolver reads live off state.Config — takes effect on
               this reload without connection churn. Only a connection/collection-affecting change (per
               ServerDefinitionEquals) drops the runtime to reconnect through the startup path. */
            var connectionChanged = !ServerDefinitionEquals(state.Config, desiredServer);
            state.Config = desiredServer;
            /* #6771: the plan-capture setting is not a connection field, so an edit must reach the next cycle without a reconnect.
               The collectors read the runtime (set at connect, init-only Config), so the new value goes onto the live runtime too. */
            if (state.Runtime is { } liveRuntime)
            {
                liveRuntime.CapturePlans = desiredServer.CapturePlans;
            }

            if (state.LastKnownRuntime is { } lastRuntime)
            {
                lastRuntime.CapturePlans = desiredServer.CapturePlans;
            }

            if (connectionChanged)
            {
                _logger.LogInformation(
                    "[{Server}] Definition changed — reconnecting with the new configuration; delta baselines forgotten, "
                    + "the first pass on the new connection re-baselines (#3653 A5)", desiredServer.DisplayName);
                state.Runtime = null;
                state.LastKnownRuntime = null;
                state.NextConnectAttempt = DateTime.MinValue;
                state.ConsecutiveConnectFailures = 0;
                state.NextDue.Clear();
                state.RefusedBackoff.Clear();
                /* #3653 A5 (the adjacency #3540 A4 named and left): a same-id reconnect is a new epoch. The
                   fields ServerDefinitionEquals compares are the ones that decide WHICH instance the
                   connection reaches (host, port, engine, database, auth, intent, subnet failover) or WHAT it collects
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
                /* The RDS endpoint verdict and fresh login held for this id belong to the old definition (host, role,
                   credentials), so the next RDS read asks AWS and logs in again. */
                _runner?.ForgetRdsVerdicts(id);
            }

            desiredById.Remove(id);
        }

        foreach (var addition in desiredById.Values)
        {
            _logger.LogInformation(
                "[{Server}] Added to the monitored set — will connect on the next sweep", addition.DisplayName);
            /* #4757: a server enabled while the service is already running may carry collection_log rows from
               an earlier registration (it was disabled across a restart, so the removal Forget above never
               ran in this process). Start collection-stopped watching it from its first pass, not from the
               service start, or those old rows would page at once. Nothing else is held for a new server. */
            _selfAlerts?.Forget(addition.ServerId);
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
    internal async Task RecomputeNextDueAsync(List<ServerLoopState> servers, CancellationToken cancellationToken)
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
               entries costs no extra store round-trip. Lazily populated on the first new entry below.
               #4938: it is also read, still once, when a reload sets, changes or clears a collector's run time and
               the last run is not already held in memory. */
            Dictionary<string, DateTime>? watermarks = null;
            var clock = server.Clock;

            foreach (var name in CollectorScheduleDefaults.All.Keys)
            {
                var effective = StoreConfigProvider.ResolveSchedule(name, runtime.ServerId, _scheduleOverrides);
                if (!effective.Enabled)
                {
                    /* ConcurrentDictionary has no Remove(key) — TryRemove is the drop-in for the old Remove. */
                    lock (server.ScheduleLock)
                    {
                        server.NextDue.TryRemove(name, out _);
                        server.RunTimeSlots.TryRemove(name, out _);
                    }

                    continue;
                }

                /* #3929/#3930: an on-load collector (frequency 0) is no longer dropped from the schedule here —
                   it ALSO reruns on OnLoadRecaptureMinutes, exactly like the connect-time seed and the
                   due-collector sweep below. */
                var interval = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(effective.FrequencyMinutes);
                RunTimeRule? rule = effective.RunAtMinute is int runAt ? new RunTimeRule(runAt, runtime.ServerId, clock.ToUtc) : null;
                var slot = ReadRunTimeSlot(server, name);

                if (server.NextDue.TryGetValue(name, out var existing))
                {
                    if (rule is { } kept && slot is not null
                        && slot.RunAtMinute == kept.RunAtMinute
                        && slot.IntervalMinutes == interval
                        && string.Equals(slot.ClockId, clock.Id, StringComparison.Ordinal))
                    {
                        /* #4938: the run time and the clock are the ones this stamp was computed from, so an edit to some
                           other row leaves it where it is. The cap below would pull a stamp that sits more than one
                           interval ahead (a 25-hour autumn day, or an interval that was just shortened) in to now plus
                           the interval, which moves the run to the time of day of the edit. */
                        continue;
                    }

                    if (rule is { } changed)
                    {
                        /* #4938: a run time that was just set or changed, or a clock that changed, computes the slot again,
                           from the last run held in memory when there is one and the persisted mark when not. */
                        watermarks ??= await ReadCollectorWatermarksAsync(_postgres!, runtime.ServerId, _logger, cancellationToken);
                        if (AfterRecomputeWatermarkReadForTest is { } afterRead)
                        {
                            await afterRead(server, name);
                        }
                        /* #5033: a run recorded while the watermarks were being read is in the slot now, not in the one read
                           above, so the slot is read again under the lock and the later last run wins. `now` is stale after
                           the await, so the time is taken again inside the lock. */
                        lock (server.ScheduleLock)
                        {
                            var nowLocked = DateTime.UtcNow;
                            server.RunTimeSlots.TryGetValue(name, out var currentSlot);
                            var changedLastRun = Newer(currentSlot?.LastRunUtc, slot?.LastRunUtc)
                                ?? (watermarks.TryGetValue(name, out var cw) ? cw : (DateTime?)null);
                            server.NextDue[name] = ComputeSeededNextDue(
                                changedLastRun, interval, nowLocked, SeedJitter(runtime.ServerId, interval * 60), changed);
                            server.RunTimeSlots[name] = new RunTimeSlot(changed.RunAtMinute, interval, clock.Id, changedLastRun);
                        }

                        continue;
                    }

                    if (slot is not null)
                    {
                        /* #4938: the run time was cleared, so the collector returns to the rule it had without one: its
                           last run plus the interval, or now plus the seed jitter when that has passed. */
                        watermarks ??= await ReadCollectorWatermarksAsync(_postgres!, runtime.ServerId, _logger, cancellationToken);
                        if (AfterRecomputeWatermarkReadForTest is { } afterRead)
                        {
                            await afterRead(server, name);
                        }
                        lock (server.ScheduleLock)
                        {
                            var nowLocked = DateTime.UtcNow;
                            server.RunTimeSlots.TryGetValue(name, out var currentSlot);
                            var clearedLastRun = Newer(currentSlot?.LastRunUtc, slot.LastRunUtc)
                                ?? (watermarks.TryGetValue(name, out var xw) ? xw : (DateTime?)null);
                            server.RunTimeSlots.TryRemove(name, out _);
                            server.NextDue[name] = ComputeSeededNextDue(
                                clearedLastRun, interval, nowLocked, SeedJitter(runtime.ServerId, interval * 60));
                        }

                        continue;
                    }

                    /* Existing entry KEEPS its already-applied phase, but is pulled in to at most now + the
                       (possibly shortened) interval so a frequency change takes effect promptly without
                       over-firing — unchanged from before. */
                    var capped = now.AddMinutes(interval);
                    /* #5033: the stamp is re-read and written under the schedule lock, so a run's record that lands
                       between the two cannot be overwritten by the older value. */
                    lock (server.ScheduleLock)
                    {
                        if (server.NextDue.TryGetValue(name, out var existingLocked))
                        {
                            server.NextDue[name] = existingLocked < capped ? existingLocked : capped;
                        }
                    }
                }
                else
                {
                    /* NEW entry — a collector this reload newly enables. Seed it from the persisted watermark
                       (the same #1575 policy as the connect-seed) so a newly-enabled long-frequency collector
                       resumes its real cadence instead of deferring up to a full interval; the small capped
                       jitter still de-clusters an overdue / never-run fleet-wide enable. With a run time (#4938)
                       the seed is the slot instead, so a collector enabled at noon waits for its run time. */
                    watermarks ??= await ReadCollectorWatermarksAsync(_postgres!, runtime.ServerId, _logger, cancellationToken);
                    if (AfterRecomputeWatermarkReadForTest is { } afterRead)
                    {
                        await afterRead(server, name);
                    }
                    var watermark = watermarks.TryGetValue(name, out var w) ? w : (DateTime?)null;
                    var jitter = SeedJitter(runtime.ServerId, interval * 60);
                    lock (server.ScheduleLock)
                    {
                        var nowLocked = DateTime.UtcNow;
                        server.RunTimeSlots.TryGetValue(name, out var currentSlot);
                        var lastRun = Newer(currentSlot?.LastRunUtc, watermark);
                        server.NextDue[name] = ComputeSeededNextDue(lastRun, interval, nowLocked, jitter, rule);
                        if (rule is { } seeded)
                        {
                            server.RunTimeSlots[name] = new RunTimeSlot(seeded.RunAtMinute, interval, clock.Id, lastRun);
                        }
                        else
                        {
                            server.RunTimeSlots.TryRemove(name, out _);
                        }
                    }
                }
            }
        }
    }

    /// <summary>
    /// #5452: the AWS role exception behind <paramref name="ex"/> when it is one an operator can act on (the allow list does
    /// not list the role, its partition is not the target's, STS refused it, the region is disabled), found through any
    /// wrapper; otherwise null. The role arm of the collector run filters on this.
    /// </summary>
    internal static AwsRoleAssumeException? AwsRoleConfigurationFault(Exception ex)
        => AwsRoleAssumeException.Find(ex) is { IsConfiguration: true } found ? found : null;

    /// <summary>
    /// #5452: writes the warning line for a server whose AWS role cannot be used and returns the text for its
    /// <c>collection_log</c> row. Both are the exception's own message, which names the role ARN and whether an external ID is
    /// set and never the ID. One method so the two outputs cannot differ, and so a test can read both.
    /// </summary>
    internal static string AwsRoleFaultNote(ILogger logger, string serverName, string collectorName, AwsRoleAssumeException fault)
    {
        logger.LogWarning("  [{Server}] {Collector} => PERMISSIONS: {Message}", serverName, collectorName, fault.Message);
        return fault.Message;
    }

    /// <summary>
    /// Whether two server definitions are identical for the collection loop — the connection-relevant fields
    /// (host, port, engine, database, auth, credentials, intent), the server's own AWS role and external ID (#5452: a role-only edit
    /// reconnects, so the runtime reads under the new role and the host check starts over), plus the collection-affecting excluded databases. A difference triggers a reconnect on reconcile so the
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
        && !QueryStoreReplicaSetting.Differ(a.QueryStoreReplica, b.QueryStoreReplica)
        && a.Port == b.Port
        && a.TargetEngine == b.TargetEngine
        && a.AwsRoleKey == b.AwsRoleKey
        && a.ExcludedDatabases.SequenceEqual(b.ExcludedDatabases, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// #4732: whether a runtime this body connected still describes the server it was installed on. False when
    /// the held definition no longer equals the one the connect was made with (<see cref="ServerDefinitionEquals"/>:
    /// a cost or alert-delivery edit does not count), and false when the runtime on the state is no longer this one,
    /// which is what <see cref="ReconcileServers"/> leaves behind when it replaces a definition mid-connect. A stale
    /// runtime is discarded rather than used: it would run a pass on the connection string the edit replaced.
    /// Internal so a unit test can pin the table.
    /// </summary>
    internal static bool ConnectedRuntimeIsCurrent(
        MonitoredServer held, MonitoredServer attempted, ServerRuntime? installed, ServerRuntime connected)
        => ReferenceEquals(installed, connected) && ServerDefinitionEquals(held, attempted);

    /// <summary>
    /// #4732: drops a runtime the connect body found stale and makes the next pass connect with the new definition.
    /// The failure count and the backoff are left alone: the edit's reconcile already reset them, and a connect
    /// that succeeded must not put the new definition into backoff.
    /// </summary>
    private void DiscardStaleConnection(ServerLoopState server, ServerRuntime runtime)
    {
        _logger.LogInformation(
            "[{Server}] Definition changed or server removed while connecting - discarding the connection made with the old one; connecting again with the new one if the server is still monitored",
            server.Config.DisplayName);
        if (ReferenceEquals(server.Runtime, runtime))
        {
            server.Runtime = null;
            server.LastKnownRuntime = null;
        }

        server.NextConnectAttempt = DateTime.MinValue;
    }

    /// <summary>
    /// The deterministic, restart-stable per-server phase offset within a cadence period (#1553 cadence jitter).
    /// The rule moved to <see cref="CollectorCadence.CadencePhaseOffset"/> (#4938) so the shared run-time rules in the
    /// collectors project can use it; this forwards to it, so the seed jitter (<see cref="SeedJitter"/>), the
    /// cold-start spread (<see cref="ColdStartFirstSweepDue"/>) and the on-connect analysis stamp, and the pins on
    /// them, keep one spelling and the same values. Applied ONLY at initial cadence stamps, never the steady-state
    /// advance. Internal so a unit test can pin its shape.
    /// </summary>
    internal static TimeSpan CadencePhaseOffset(int serverId, int periodSeconds)
        => CollectorCadence.CadencePhaseOffset(serverId, periodSeconds);

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

    /// <summary>#4732: the longest a connect-retry stamp can sit ahead of now when it is written: the backoff cap plus
    /// its jitter (<see cref="ServerConnectBackoff"/>), so a stamp further out than this came from a clock that
    /// stepped backwards.</summary>
    private static readonly TimeSpan s_connectAttemptStampSpan =
        TimeSpan.FromSeconds(ServerConnectBackoff.CapSeconds * (1.0 + ServerConnectBackoff.JitterFraction));

    /// <summary>
    /// #4732: whether a per-server due stamp written as "then plus <paramref name="interval"/>" has come due at
    /// <paramref name="nowUtc"/>. The stamps are absolute wall-clock times, so a clock that steps backwards leaves
    /// each one ahead by the size of the step and the work waits that long; a stamp more than one interval ahead can
    /// only be that, and counts as due (<see cref="CollectorCadence.ClampDue"/>). A stamp at or below now, and one up
    /// to a full interval ahead, decide exactly as a plain comparison would. Internal so a test can pin the table.
    /// </summary>
    internal static bool StampIsDue(DateTime dueUtc, TimeSpan interval, DateTime nowUtc) =>
        nowUtc >= CollectorCadence.ClampDue(dueUtc, nowUtc, interval);

    /// <summary>
    /// #4652: the next due time for a stamp on this loop, on a fixed grid: the first run (still at MinValue) anchors
    /// the grid at now; after that, the previous due plus the interval, skipping missed slots, so a slow sweep or
    /// the loop's own 15-second tick is never carried into the next slot. #4732: a stamp more than one interval ahead can
    /// only be a wall clock that stepped backwards (<see cref="StampIsDue"/> counts it as due), and it is re-anchored at
    /// now instead of advanced from where it sits, so the stamp written is never more than one interval ahead. Advanced
    /// from a far-future stamp it would stay ahead of the span and fire on every pass.
    /// </summary>
    internal static DateTime NextGridStamp(DateTime currentDue, DateTime now, TimeSpan interval) =>
        currentDue == DateTime.MinValue
            ? now + interval
            : CollectorCadence.NextDue(CollectorCadence.ClampDue(currentDue, now, interval), now, interval);

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
    /// The steady-state advance in <see cref="RunDueCollectorsAsync"/> is grid-based
    /// (<see cref="CollectorCadence.NextDue"/>: previous due time plus the interval). Pure and
    /// Kind-agnostic (compares by ticks; the caller passes matching UTC values) so the policy is unit-tested
    /// without a live store or a connect. Internal so a unit test can pin the decision table.
    /// </summary>
    internal static DateTime ComputeSeededNextDue(DateTime? lastRunUtc, int frequencyMinutes, DateTime nowUtc, TimeSpan jitter,
        RunTimeRule? runTime = null)
    {
        /* #4938: a collector that has a run time replaces all three arms below. Its answer is a slot, the instant on the
           server's clock the run time falls on plus the server's spread (CollectorRunTime.NextDue holds the rules): a
           last run at or after today's slot waits for the next one, a collector that never ran waits for its first, and
           only inside the hour after a slot does one run now. NextDue answers "now" for that case and the seed jitter is
           added here, so the inside-the-grace answer is the one place now + jitter survives. Outside the grace
           now + jitter would step on the quiet hour the run time exists to keep, so it is never returned there. */
        if (runTime is { } rule && CollectorRunTime.AllowsRunAt(frequencyMinutes))
        {
            var slot = CollectorRunTime.NextDue(nowUtc, lastRunUtc, rule.RunAtMinute, frequencyMinutes, rule.ServerId, rule.LocalToUtc);
            return slot <= nowUtc ? nowUtc + jitter : slot;
        }

        if (lastRunUtc is DateTime lastRun)
        {
            var due = lastRun.AddMinutes(frequencyMinutes);
            return due <= nowUtc ? nowUtc + jitter : due;
        }

        return nowUtc + jitter;
    }

    /// <summary>
    /// #4938: the server pass's decision for one collector that has a run time, from its stamp. Three outcomes
    /// (<see cref="RunTimeAction"/>):
    /// <list type="bullet">
    /// <item>the stamp is ahead of now: not due. It is judged with the room a run-time stamp needs
    /// (<see cref="CollectorRunTime.MaxStampAhead"/>: the interval, an hour of spread and a 25-hour autumn day), so a stamp
    /// a day and an hour away is a wait and not a clock that stepped back;</item>
    /// <item>the stamp has come and now is no later than the grace after it: hand it off, and the next stamp is the slot
    /// <c>interval / 1440</c> days on, computed from that date's local time (never the stamp plus 1440 minutes, which is
    /// an hour off after a daylight-saving change);</item>
    /// <item>the stamp has come and now is past the grace: that day is skipped and nothing is handed off, as a missed slot is
    /// skipped everywhere else (#4636). The next stamp is the next slot, or now plus the seed jitter when a later slot's
    /// grace is already running (a host that slept through several days).</item>
    /// </list>
    /// A skipped day counts 1 skipped slot when the sweep loop was running through it, and 0 after a sleep, a pause or a
    /// stopped service: <paramref name="skipCreditFloor"/> is <see cref="SkipCreditFloor.Floor"/>, the instant the loop last
    /// came back from a stretch it was not running, and a slot that came due before it is not the gate's doing. A run that is
    /// handed off is delayed by the fleet cap, never dropped, so it records nothing here. Pure: the pass passes the clock and
    /// the floor in, so every row is tested without a worker. Internal for that.
    /// </summary>
    internal static RunTimeStep StepRunTimeCollector(
        DateTime due, DateTime nowUtc, DateTime skipCreditFloor, int intervalMinutes, RunTimeRule rule, TimeSpan jitter)
    {
        due = CollectorCadence.ClampDue(due, nowUtc, CollectorRunTime.MaxStampAhead(intervalMinutes));
        if (nowUtc < due)
        {
            return new RunTimeStep(RunTimeAction.NotDue, due, 0);
        }

        if (nowUtc <= due + CollectorRunTime.Grace)
        {
            var next = CollectorRunTime.NextDue(nowUtc, nowUtc, rule.RunAtMinute, intervalMinutes, rule.ServerId, rule.LocalToUtc);
            return new RunTimeStep(RunTimeAction.HandOff, next, 0);
        }

        var following = CollectorRunTime.NextDue(nowUtc, due, rule.RunAtMinute, intervalMinutes, rule.ServerId, rule.LocalToUtc);
        return new RunTimeStep(
            RunTimeAction.SkipDay,
            following <= nowUtc ? nowUtc + jitter : following,
            skipCreditFloor <= due ? 1 : 0);
    }

    /// <summary>The floor applied to the watermark read below (#4469): how far back
    /// <see cref="ReadCollectorWatermarksAsync"/> is willing to look for a collector's last run. Two days,
    /// not one, so a floor-excluded row (a true last run 1–2 days back) can't be mistaken for never-run —
    /// see the method's own doc comment for why this is safe.</summary>
    internal static readonly TimeSpan WatermarkFloorLookback = TimeSpan.FromDays(2);

    /// <summary>The bounded statement itself (#4469/#4477 store rung V150), pinned by name so a live/plan
    /// test can assert its text and shape directly rather than re-deriving it from the call site. $1
    /// server_id; $2 the floor (<c>DateTime.UtcNow - WatermarkFloorLookback</c>, bound naive-UTC
    /// (Unspecified) to match the naive <c>timestamp</c> column, so Npgsql sends <c>timestamp</c> and
    /// TimescaleDB excludes the old chunks with no cast on <c>collection_time</c> — the same product-wide
    /// contract as <see cref="BindActualPlanResolveParameters"/>); $3 the requested collector names, bound
    /// as a <c>text[]</c>.
    ///
    /// <para><b>One index-only descent per collector, not a GROUP BY over every matching row (V150).</b>
    /// The prior text was <c>SELECT collector_name, MAX(collection_time) FROM collection_log WHERE
    /// server_id = $1 AND collection_time >= $2 GROUP BY collector_name</c>, which still had to read every
    /// row in the newest chunks for this server (a Bitmap Heap Scan, the 89% of the pre-floor cold cost the
    /// #4469 floor above did not touch) before it could answer, because TimescaleDB has no index shaped to
    /// answer "the newest row per collector" directly. <c>idx_collection_log_watermark (server_id,
    /// collector_name, collection_time DESC)</c> gives it one: for each name in $3, an <c>ORDER BY
    /// collection_time DESC LIMIT 1</c> sub-select is an index-only descent, not a scan. Measured on a rig
    /// shaped like the field (43 servers, ~40 collectors, 15M rows, 9 of 11 chunks compressed): the new
    /// shape runs ~0.65 ms against the old statement's ~24 ms, cold and warm alike — a >30x floor
    /// reduction, because the read goes from "scan every row in the newest chunks for this server" to "one
    /// index descent per collector name." $3 costs nothing extra to produce: both call sites already hold
    /// <c>CollectorScheduleDefaults.All.Keys</c> in the very next line.</para>
    /// </summary>
    internal const string ReadCollectorWatermarksSql = """
        SELECT c.name, (
            SELECT l.collection_time
            FROM collection_log l
            WHERE l.server_id = $1
            AND   l.collector_name = c.name
            AND   l.collection_time >= $2
            ORDER BY l.collection_time DESC
            LIMIT 1
        )
        FROM unnest($3::text[]) AS c(name)
        """;

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
    ///
    /// <para><b>Bounded to <see cref="WatermarkFloorLookback"/>, not the unbounded MAX/GROUP BY this
    /// replaces (#4469).</b> On a busy store the old statement walked every retained <c>collection_log</c>
    /// chunk, compressed ones included, because the newest instant per collector is not known until the
    /// whole table has been read. Back-to-back field EXPLAINs on the busiest measured store: the unbounded
    /// statement, cold, took 4,775 ms (3,166 hit + 21,483 read buffers, 13,957 ms of parallel-worker I/O read
    /// time); adding a literal 2-day floor removed the 30 older compressed chunks from that scan, which had
    /// accounted for only about 11% of the cold I/O (~1,540 ms) — the other 89% (~12,417 ms) was spent in a
    /// Bitmap Heap Scan over the two newest, uncompressed chunks, and the floor keeps those chunks, so that
    /// cost is unchanged. (The bounded run's own 69 ms/20,703-buffers-all-hit number came from running warm,
    /// right after the unbounded run had already pulled the same pages into cache, so it is not a clean
    /// before/after and is not cited as the floor's effect.) The floor still matters: the excluded 11% grows
    /// with retention and chunk count, and a follow-up addresses the dominant newest-chunk cost.</para>
    ///
    /// <para><b>Why 2 days is a safe floor.</b> This read only seeds <see cref="ComputeSeededNextDue"/>,
    /// which only cares whether a collector ran within its OWN interval of "now" — a collector whose true
    /// last run falls outside the floor is, by definition, already overdue on every cadence this product
    /// schedules (the longest recurring cadence in <c>CollectorScheduleDefaults</c> is 1440 minutes = 1 day,
    /// shared by <c>index_object_stats</c>, <c>pg_column_stats</c>, <c>pg_extension_availability</c> and
    /// <c>pg_index_usage_stats</c>). A floor of one day would already catch every such collector's true
    /// last run; two days is a full day of margin so a floor-excluded row (last run 1–2 days back, still
    /// technically inside interval + slop for a collector whose interval is close to a day) cannot be
    /// mistaken for never-run. A collector whose last run predates the floor entirely is seeded as never-run
    /// — the SAME fallback this read already used on a query failure — so it runs promptly under jitter
    /// instead of waiting out a remaining interval it does not actually have left; that is strictly safer
    /// than the alternative (treating it as still-current and never rescheduling it).</para>
    /// </summary>
    internal static async Task<Dictionary<string, DateTime>> ReadCollectorWatermarksAsync(
        NpgsqlDataSource postgres, int serverId, ILogger? logger, CancellationToken cancellationToken)
    {
        var watermarks = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(ReadCollectorWatermarksSql, connection);
            command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.Add(new NpgsqlParameter<DateTime>
            {
                TypedValue = DateTime.SpecifyKind(DateTime.UtcNow - WatermarkFloorLookback, DateTimeKind.Unspecified),
            });
            command.Parameters.Add(new NpgsqlParameter<string[]>
            {
                TypedValue = CollectorScheduleDefaults.All.Keys.ToArray(),
            });

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

    /// <summary>The newest server-scoped <c>TimeZone</c> setting of a PostgreSQL target (#4938), the zone its clock keeps.
    /// A session-scoped source (a client's own <c>TimeZone</c>) and a per-database or per-role setting do not describe the
    /// server and are left out, the rules <c>PgTargetBaselineProvider</c> reads the same row by. $1 server_id; $2 the
    /// oldest snapshot to look at, naive UTC. <see cref="TryReadServerClockAsync"/> runs it up to twice, with the bounds
    /// <see cref="PgTargetFactCollector.ConfigSnapshotLowerBounds"/> hands the analysis side's read of the same row: the
    /// newest day first, which lets TimescaleDB plan the day's chunks instead of every retained one, and every retained
    /// snapshot only when that day held none. A target whose config collector has been off for a week or a season still
    /// has the clock it last reported, and only a target with no snapshot at all has no known clock and reads as UTC.</summary>
    internal const string ReadPgServerClockSql = """
        SELECT c.setting
        FROM pg_server_config AS c
        WHERE c.server_id = $1
        AND   c.collection_time >= $2
        AND   c.name = 'TimeZone'
        AND   c.database_name IS NULL
        AND   c.role_name IS NULL
        AND   c.setting IS NOT NULL
        AND   coalesce(c.source, '') NOT IN ('client', 'session', 'override')
        ORDER BY c.collection_time DESC
        LIMIT 1
        """;

    /// <summary>
    /// #4938: whether a collector's run puts a row in the store that carries its server's clock: <c>server_properties</c>
    /// for a SQL Server target (its time zone and UTC offset) and <c>pg_server_config</c> for a PostgreSQL one (its
    /// <c>TimeZone</c> setting). The worker reads the clock again after such a run, so a clock that arrives or changes is
    /// seen, and never on a tick.
    /// </summary>
    internal static bool CarriesServerClock(string collectorName, CollectorTargetEngine engine) =>
        engine == CollectorTargetEngine.SqlServer
            ? string.Equals(collectorName, ServerPropertiesCollector.Instance.Name, StringComparison.OrdinalIgnoreCase)
            : string.Equals(collectorName, PgServerConfigCollector.Instance.Name, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// #4938: one server's wall clock, from the store: the newest <c>server_properties</c> row for a SQL Server target
    /// (<see cref="DarlingServerClockReader"/>: its zone where it reports one, else its fixed offset) or the newest
    /// <c>pg_server_config</c> <c>TimeZone</c> row for a PostgreSQL one (<see cref="ReadPgServerClockSql"/>); UTC when the
    /// server has no such row yet. Null when the read failed, so a caller keeps the clock it holds: a store hiccup is
    /// not a clock change, and it must never break the connect or the run that asked. Internal so a live test can
    /// seed rows and read.
    /// </summary>
    internal static async Task<ServerClockStamp?> TryReadServerClockAsync(
        NpgsqlDataSource postgres, int serverId, CollectorTargetEngine engine, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            if (engine == CollectorTargetEngine.SqlServer)
            {
                return new ServerClockStamp(await DarlingServerClockReader.ReadAsync(postgres, serverId, cancellationToken));
            }

            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

            /* The newest day first, then every retained snapshot only when that day held none: the bounds the analysis side
               reads the same row with. The day's newest qualifying row is the newest one the unbounded read would find, so
               the two steps cannot disagree; the first only keeps the planner off every retained chunk. A server whose
               snapshots stopped a week ago must not read as UTC, which would move its run-time slot by its offset. */
            foreach (var lowerBound in PgTargetFactCollector.ConfigSnapshotLowerBounds(DateTime.UtcNow))
            {
                using var command = new NpgsqlCommand(ReadPgServerClockSql, connection);
                command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
                command.Parameters.AddWithValue(serverId);
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = lowerBound });

                if (await command.ExecuteScalarAsync(cancellationToken) is string zone && !string.IsNullOrWhiteSpace(zone))
                {
                    return ResolvePgServerClock(serverId, zone.Trim(), logger);
                }
            }

            return new ServerClockStamp(ServerClock.Utc);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug("Observability: server clock read for server_id {ServerId} failed: {Message}", serverId, ex.Message);
            return null;
        }
    }

    /* Servers already warned about an unresolvable PostgreSQL TimeZone, keyed by server id and zone text, so the
       warning is logged once per server and zone rather than on every clock read. */
    private static readonly ConcurrentDictionary<(int ServerId, string Zone), byte> UnresolvedPgZonesWarned = new();

    /// <summary>
    /// #4938: the clock for a PostgreSQL server's <c>TimeZone</c> text. A zone this host cannot resolve (a POSIX string such
    /// as <c>EST5EDT,M3.2.0,M11.1.0</c>, or <c>localtime</c>) reads as UTC, so the run times of that server's collectors
    /// are placed on a UTC clock; that is logged as a warning, once per server and zone text, naming the text.
    /// </summary>
    internal static ServerClockStamp ResolvePgServerClock(int serverId, string zone, ILogger? logger)
    {
        var clock = ServerClock.Resolve(zone, null);
        if (ReferenceEquals(clock, ServerClock.Utc) && UnresolvedPgZonesWarned.TryAdd((serverId, zone), 0))
        {
            logger?.LogWarning(
                "Server {ServerId} reports the PostgreSQL TimeZone '{Zone}', which this host cannot resolve to a time zone; its collector run times use UTC.",
                serverId, zone);
        }

        return new ServerClockStamp(clock);
    }

    /// <summary>
    /// The store data source the MCP and web hosts open (#6630): the operator's string (a configured
    /// <c>postgres.mcpConnectionString</c>/<c>webConnectionString</c> or the owner fallback never runs through the managed
    /// builder) with the collect/config search path set-if-absent, then the application name, then the UTC session pin,
    /// in the order the worker builds its own. Without the path a login that does not own the database, on a database
    /// with no default path, fails every unqualified read with 42P01.
    /// </summary>
    internal static NpgsqlDataSource CreateHostStoreDataSource(string storeConnectionString, string applicationName) =>
        NpgsqlDataSource.Create(
            DarlingStoreConnection.PinSessionTimeZoneUtc(
                DarlingStoreConnection.WithApplicationName(DarlingStoreConnection.EnsureStoreSearchPath(storeConnectionString), applicationName)));
}
