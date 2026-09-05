/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Background service that runs data collection on a 1-minute timer,
/// plus periodic archival and retention cleanup.
/// </summary>
public class CollectionBackgroundService : BackgroundService
{
    private readonly RemoteCollectorService _collectorService;
    private readonly DuckDbInitializer? _duckDb;
    private readonly ServerManager? _serverManager;
    private readonly ArchiveService? _archiveService;
    private readonly RetentionService? _retentionService;
    private readonly AnalysisNotificationService? _notificationService;
    private readonly ILogger<CollectionBackgroundService>? _logger;

    private static readonly TimeSpan CollectionInterval = TimeSpan.FromMinutes(1);
    /* Start at UtcNow so maintenance tasks don't all fire on the very first cycle. */
    private DateTime _lastArchiveTime = DateTime.UtcNow;

    /* #2058: the Query Store backfill tick — one byte-budgeted slice per server per due-tick, on
       Lite's IfDue ladder (the archival/retention/analysis idiom) rather than a separate loop. */
    private DateTime _lastQueryStoreBackfill = DateTime.MinValue;

    /* #2167: last observed state of the backfill switch, so the log records each TRANSITION once instead of
       once per idle tick. Starts true to match the setting's default — a deployment that never touches the
       switch therefore logs nothing about it. */
    private bool _queryStoreBackfillWasEnabled = true;
    private DateTime _lastRetentionTime = DateTime.UtcNow;
    private DateTime _lastAnalysisTime = DateTime.UtcNow;
    private DateTime _lastFindingsCleanupTime = DateTime.UtcNow;
    private DateTime _lastDismissedAlertsCleanupTime = DateTime.UtcNow;

    /* Server IDs whose scheduled analysis is currently running — prevents relaunching
       analysis for a server whose previous (possibly hung) pass has not finished. The value
       carries when the pass started and how loudly it has been reported, because #2412's defect
       was that a pass which never finishes leaves this marker set FOREVER and the server is then
       skipped in silence on every later cycle. */
    private readonly ConcurrentDictionary<int, AnalysisPassState> _analysisInFlight = new();

    /* How far past its budget an in-flight pass must be before the loop starts reporting it. The
       ordinary overrun already gets the "exceeded Ns" warning; this is for the pass that ignored
       the cancellation raised at that budget — wedged inside a single store read, or waiting on
       the store's read lock behind a long archival, neither of which any token can reach. */
    private const int StuckAnalysisMultiple = 3;

    /* Reports back off by doubling. Scheduled analysis runs on a 30-minute default cadence, so a
       fixed repeat interval would either be slower than the cadence (useless) or produce one line
       per cycle forever (the spam that makes a log unreadable). Doubling gives eight lines in the
       first day against forty-eight cycles, and a handful per day after — loud enough to be noticed, quiet enough to
       stay noticed. Capped so the shift cannot run away on a long-lived process. */
    private const int StuckAnalysisMaxBackoffDoublings = 20;

    /* How long the loop gives a cancelled pass to unwind. It serves twice, and the two are not
       the same wait. On the TIMEOUT path it extends the loop's patience past the budget, so a pass
       that honours its cancellation is observed finishing rather than racing the loop's own timer.
       On SHUTDOWN it is the hold below that keeps a pass from being orphaned, which is a wait the
       loop's Task.Delay cannot provide because that delay observes the stopping token and so
       collapses the instant it fires. Same five seconds as the Darling twin's shutdown grace. */
    private static readonly TimeSpan AnalysisUnwindGrace = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Bookkeeping for one in-flight analysis pass. A class, not a struct, so the loop can update
    /// the report counters in place without a read-modify-write race against the completion
    /// continuation (which only ever removes the whole entry).
    /// </summary>
    private sealed class AnalysisPassState
    {
        public AnalysisPassState(DateTime startedUtc) => StartedUtc = startedUtc;

        public DateTime StartedUtc { get; }
        public int SkippedCycles { get; set; }
        public int ReportCount { get; set; }
    }

    /* Archive every hour, retention once per day */
    private static readonly TimeSpan ArchiveInterval = TimeSpan.FromHours(1);

    /// <summary>The backfill worker's cadence (#2058) — Darling's worker ticks at the same 5
    /// minutes; the steady state (every tail drained, no holes) costs a candidate query and a few
    /// MIN() lookups per server.</summary>
    private static readonly TimeSpan QueryStoreBackfillInterval = TimeSpan.FromMinutes(5);

    /* ── #2148: the ladder steps that could HOLD the loop with no bound, made abandonable. ──
       The field failure: one step wedged on an Azure elastic pool right after the 3.4.0 upgrade and
       ALL collection stopped permanently — every step's exception armor intact, because armor bounds
       throws and nothing bounded a hang. The connection check runs under the ladder's own
       analysis-timeout idiom (deadline + in-flight guard, extracted to AbandonableStep); the backfill
       tick's protection lives INSIDE the tick, per server (RemoteCollectorService), so one wedged
       server quarantines only itself. The deadline is a generous multiple of healthy behavior, so an
       abandonment is always a defect signal, never scheduling jitter, and it logs as ERROR. */
    private static readonly TimeSpan ConnectionCheckDeadline = TimeSpan.FromSeconds(90);
    private readonly AbandonableStep _connectionCheckStep = new();
    private static readonly TimeSpan RetentionInterval = TimeSpan.FromHours(24);
    /* Analysis-findings retention purge — daily, matching the parquet-retention cadence
       above and Darling's daily findings-cleanup horizon. */
    private static readonly TimeSpan FindingsCleanupInterval = TimeSpan.FromHours(24);
    /* dismissed_archive_alerts sidecar purge — the same daily cadence as its retention siblings. */
    private static readonly TimeSpan DismissedAlertsCleanupInterval = TimeSpan.FromHours(24);

    /* Size-based trigger — when the database exceeds this size, archive ALL data
       to parquet and reset the database. INSERT performance degrades badly with
       large tables (33x slower at 667MB in testing). Data remains fully queryable
       through the archive views (hot UNION parquet). */
    private const double ArchiveSizeThresholdMb = 512;

    public bool IsPaused { get; set; }
    public DateTime? LastCollectionTime { get; private set; }
    public bool IsCollecting { get; private set; }

    public CollectionBackgroundService(
        RemoteCollectorService collectorService,
        DuckDbInitializer? duckDb = null,
        ArchiveService? archiveService = null,
        RetentionService? retentionService = null,
        ServerManager? serverManager = null,
        AnalysisNotificationService? notificationService = null,
        ILogger<CollectionBackgroundService>? logger = null)
    {
        _collectorService = collectorService;
        _duckDb = duckDb;
        _serverManager = serverManager;
        _archiveService = archiveService;
        _retentionService = retentionService;
        _notificationService = notificationService;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger?.LogInformation("Collection background service started");

        /* Seed delta calculator from DuckDB so restarts don't lose baselines */
        await _collectorService.SeedDeltaCacheAsync();

        /* Wait a few seconds before first collection to let the app initialize */
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            if (!IsPaused)
            {
                /* Check all server connections before collecting */
                if (_serverManager != null)
                {
                    /* #2148: abandonable — a connection check wedged on one server (an Azure token
                       refresh that never returns, a network path that swallows packets) must not stop
                       every server's collection. */
                    var check = await _connectionCheckStep.RunAsync(
                        () => _serverManager.CheckAllConnectionsAsync(), ConnectionCheckDeadline,
                        onLateFault: ex => _logger?.LogError(ex,
                            "Connection check faulted AFTER being abandoned — this is the wedge's own exception (#2148)"),
                        cancellationToken: stoppingToken);
                    LogStepOutcome(check, "Connection check", ConnectionCheckDeadline);
                }

                try
                {
                    IsCollecting = true;
                    await _collectorService.RunDueCollectorsAsync(stoppingToken);
                    LastCollectionTime = DateTime.UtcNow;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Collection cycle failed");
                }
                finally
                {
                    IsCollecting = false;
                }

                /* Periodic archival (time-based or size-based) */
                await RunArchivalIfDueAsync();

                await RunQueryStoreBackfillIfDueAsync(stoppingToken);

                /* Periodic retention cleanup */
                RunRetentionIfDue();

                /* Periodic analysis-findings retention (rolling 30-day purge) */
                await RunFindingsCleanupIfDueAsync();

                /* Periodic dismissed-archive-alert sidecar retention (rolling 180-day purge) */
                await RunDismissedAlertsCleanupIfDueAsync();

                /* Periodic scheduled analysis + high-severity finding notifications */
                await RunAnalysisIfDueAsync(stoppingToken);

                /* Log process memory at the end of each cycle. Lets bug reporters
                   self-report memory without Task Manager, gives us a continuous
                   memory trace for diagnosis, and surfaces regressions in the log
                   that would otherwise need external sampling to detect. Three
                   property reads — negligible overhead at 1-minute cadence. */
                LogProcessMemory();
            }

            try
            {
                await Task.Delay(CollectionInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        _logger?.LogInformation("Collection background service stopped");
    }

    /// <summary>#2058: fills the Query Store history the live path never takes — the 60-minute
    /// first-contact tail and clamp-bounded outage holes — newest-first, strictly behind the live
    /// path's floor, never past the resolved query_store retention. See
    /// RemoteCollectorService.QueryStoreBackfill for the worker itself.</summary>
    private async Task RunQueryStoreBackfillIfDueAsync(CancellationToken stoppingToken)
    {
        /* #2167: the off switch, checked BEFORE the due-time stamp so a disabled backfill does not quietly
           consume its own schedule — flipping it back on runs on the next due tick rather than waiting out
           an interval that elapsed while it was off. Read live from the setting (not captured), so the
           Settings window takes effect without restarting Lite, matching Darling's store-reload behavior. */
        if (!App.QueryStoreBackfillEnabled)
        {
            if (_queryStoreBackfillWasEnabled)
            {
                _queryStoreBackfillWasEnabled = false;
                _logger?.LogInformation("Query Store backfill disabled in settings — idling; in-flight slices finish and no new ones start");
            }

            return;
        }

        if (!_queryStoreBackfillWasEnabled)
        {
            _queryStoreBackfillWasEnabled = true;
            _logger?.LogInformation("Query Store backfill re-enabled in settings — resuming from the stored watermarks");
        }

        if (DateTime.UtcNow - _lastQueryStoreBackfill < QueryStoreBackfillInterval)
        {
            return;
        }

        _lastQueryStoreBackfill = DateTime.UtcNow;

        /* #2148: the hang protection lives INSIDE the tick, per server (see
           RemoteCollectorService.RunQueryStoreBackfillTickAsync) — a wedged slice is abandoned and
           quarantines only ITS server, so the tick itself is bounded by construction. A tick-level
           deadline here was the first cut and was wrong twice over (review catch, round 2): it stalled
           every server's backfill behind one wedge, and it would false-trip as fleet size grows
           because a shared deadline sized for one slice was applied to the sum of all of them. */
        try
        {
            await _collectorService.RunQueryStoreBackfillTickAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            /* Shutdown — quiet. */
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Query Store backfill tick failed");
        }
    }

    /// <summary>One log vocabulary for every abandonable ladder step (#2148): abandonment and
    /// still-wedged skips are ERRORS — the deadlines are generous multiples of healthy behavior, so
    /// either one is a defect signal, and this line is the difference between a diagnosable field
    /// report and "the charts just stopped".</summary>
    private void LogStepOutcome(AbandonableStepResult result, string stepName, TimeSpan deadline)
    {
        switch (result.Outcome)
        {
            case AbandonableStepOutcome.Faulted:
                _logger?.LogError(result.Exception, "{Step} failed", stepName);
                break;
            case AbandonableStepOutcome.Abandoned:
                _logger?.LogError(
                    "{Step} exceeded {Deadline}s and was ABANDONED — collection continues; the step is " +
                    "quarantined until the wedged task ends. This is a defect signal: please report it " +
                    "with this log file (#2148).",
                    stepName, (int)deadline.TotalSeconds);
                break;
            case AbandonableStepOutcome.SkippedStillRunning:
                _logger?.LogError(
                    "{Step} skipped — a previously-abandoned run is still wedged (#2148).", stepName);
                break;
            /* Completed and Cancelled are the quiet outcomes. */
        }
    }

    private async Task RunArchivalIfDueAsync()
    {
        if (_archiveService == null)
        {
            return;
        }

        var timeDue = DateTime.UtcNow - _lastArchiveTime >= ArchiveInterval;
        var sizeDue = _duckDb != null && _duckDb.GetDatabaseSizeMb() >= ArchiveSizeThresholdMb;

        if (!timeDue && !sizeDue)
        {
            return;
        }

        try
        {
            if (sizeDue)
            {
                _logger?.LogInformation("Database size ({SizeMb:F0} MB) exceeds {Threshold} MB — archiving all data and resetting database",
                    _duckDb!.GetDatabaseSizeMb(), ArchiveSizeThresholdMb);
                await _archiveService.ArchiveAllAndResetAsync();
            }
            else
            {
                await _archiveService.ArchiveOldDataAsync(hotDataDays: 7);
            }
            _lastArchiveTime = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Archival cycle failed");
        }
    }

    private void RunRetentionIfDue()
    {
        if (_retentionService == null || DateTime.UtcNow - _lastRetentionTime < RetentionInterval)
        {
            return;
        }

        try
        {
            _retentionService.CleanupOldArchives(retentionMonths: 3);
            _lastRetentionTime = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Retention cleanup failed");
        }
    }

    /// <summary>
    /// Purges analysis findings past the shared retention horizon on a daily cadence.
    /// FindingStore has always declared CleanupOldFindingsAsync, but nothing scheduled it,
    /// so analysis_findings grew until a size-triggered ArchiveAllAndResetAsync incidentally
    /// wiped the WHOLE DuckDB (losing ALL findings, not just aged ones — analysis_findings is
    /// not in ArchiveService.ArchivableTables, so routine archival never touched it). This
    /// reads that horizon from the one place it is named (<see cref="AnalysisRetentionDefaults"/>)
    /// instead of a literal per site: the same rolling window the other editions intend, and the
    /// one Darling's daily purge rides. Gated on _duckDb — the dependency the cleanup needs —
    /// so it is independent of the parquet RetentionService. Never throws: logs and degrades
    /// like the archival/retention ticks above.
    /// </summary>
    private async Task RunFindingsCleanupIfDueAsync()
    {
        if (_duckDb == null || DateTime.UtcNow - _lastFindingsCleanupTime < FindingsCleanupInterval)
        {
            return;
        }

        try
        {
            await new AnalysisService(_duckDb).CleanupAsync(
                retentionDays: AnalysisRetentionDefaults.FindingsRetentionDays);
            _lastFindingsCleanupTime = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Findings cleanup failed");
        }
    }

    /// <summary>
    /// Purges dismissed_archive_alerts rows past their retention horizon on a daily cadence. The
    /// sidecar was the one table in Lite with NO purge path at all (#1651): it is INSERT-only, and
    /// because it is in ArchiveService.PreservedConfigTables it is restored intact after the 512 MB
    /// emergency reset that incidentally bounds everything else — so it grew for the life of the
    /// install. It stays preserved (losing it resurrects every dismissal at once); this just gives it
    /// the age horizon it never had. Kept a SEPARATE tick from the findings cleanup above rather than
    /// folded into it so one purge failing cannot skip the other. Never throws: logs and degrades
    /// like the archival/retention ticks.
    /// </summary>
    private async Task RunDismissedAlertsCleanupIfDueAsync()
    {
        if (_duckDb == null || DateTime.UtcNow - _lastDismissedAlertsCleanupTime < DismissedAlertsCleanupInterval)
        {
            return;
        }

        try
        {
            await new LocalDataService(_duckDb).PurgeOldDismissedArchiveAlertsAsync();
            _lastDismissedAlertsCleanupTime = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Dismissed-archive-alert cleanup failed");
        }
    }

    /// <summary>
    /// Runs the triage engine for each enabled server on the independent
    /// AnalysisIntervalMinutes cadence and persists findings to DuckDB. Production is
    /// gated by App.AnalysisEnabled (default ON, D0); findings are only routed to the
    /// notification channels when App.AnalysisNotificationsEnabled is also on.
    /// </summary>
    private async Task RunAnalysisIfDueAsync(CancellationToken stoppingToken)
    {
        /* Analysis production is gated by AnalysisEnabled, NOT by the notification toggle
           (D0). Skip when disabled, or when dependencies aren't injected in this path. */
        if (!App.AnalysisEnabled ||
            _duckDb == null || _serverManager == null || _notificationService == null)
        {
            return;
        }

        /* D0: deliver findings only when notifications are also enabled. Analysis
           runs+persists regardless; this inner gate controls delivery alone. */
        var notify = App.AnalysisNotificationsEnabled;

        if (DateTime.UtcNow - _lastAnalysisTime < TimeSpan.FromMinutes(App.AnalysisIntervalMinutes))
        {
            return;
        }
        _lastAnalysisTime = DateTime.UtcNow;

        var timeout = TimeSpan.FromSeconds(App.AnalysisTimeoutSeconds);
        var planFetcher = new SqlPlanFetcher(_serverManager);

        foreach (var server in _serverManager.GetEnabledServers())
        {
            if (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            var serverName = RemoteCollectorService.GetServerNameForStorage(server);
            var serverId = RemoteCollectorService.GetDeterministicHashCode(serverName);

            /* Skip a server whose previous analysis is still running — a hung
               connection that outlived its timeout would otherwise pile up tasks. The skip is
               still right; what was wrong is that it used to happen in silence. */
            if (!_analysisInFlight.TryAdd(serverId, new AnalysisPassState(DateTime.UtcNow)))
            {
                ReportStuckAnalysis(serverId, serverName, timeout);
                continue;
            }

            CancellationTokenSource? passCts = null;
            var passStarted = false;

            try
            {
                /* Fresh AnalysisService per server: IsAnalyzing is a single instance
                   flag, so a shared instance whose task is abandoned on timeout would
                   block analysis for every other server. */
                var analysisService = new AnalysisService(_duckDb, planFetcher);

                /* #2412: the TOKEN is the timeout now; the Task.Delay below is only this loop's
                   patience. Two things had to change for the budget to mean anything.

                   The pass has to leave this thread. DuckDB.NET implements no async execution —
                   every read completes synchronously on whichever thread called it — so an
                   AnalyzeAsync invoked here ran the whole fact-collection phase INLINE and handed
                   back an already-completed task. The race below could never fire for the phase
                   that would actually be slow, and a wedged read took the entire collection loop
                   down with it rather than just this server's analysis. Task.Run puts the pass on
                   the pool, which makes the race real and keeps collection alive regardless.

                   And something has to raise the cancellation. CancelAfter does it at the same
                   budget the loop waits on. Arming it BEFORE the task exists means Cancel can
                   never race the completion continuation's Dispose. */
                passCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                passCts.CancelAfter(timeout);

                var cts = passCts;
                var analyzeTask = Task.Run(
                    () => analysisService.AnalyzeAsync(serverId, serverName, hoursBack: 4, cts.Token),
                    CancellationToken.None);

                /* Clear the in-flight marker only when the task truly finishes — not
                   when the timeout below moves us on — so a hung server is not relaunched. */
                _ = analyzeTask.ContinueWith(
                    completed =>
                    {
                        _analysisInFlight.TryRemove(serverId, out _);
                        cts.Dispose();
                    },
                    TaskScheduler.Default);

                /* From here the continuation owns the marker and the token source. Neither
                   Task.Run nor ContinueWith throws, so there is no window where the pass exists
                   without something committed to cleaning up after it. */
                passStarted = true;

                /* Wait the budget PLUS a short grace, so a pass that honours its cancellation is
                   seen finishing here. Losing that race now carries real information: the pass was
                   asked to stop and did not. Note this delay observes the stopping token and so
                   collapses the moment it fires — shutdown is handled by the hold below, not here. */
                var finished = await Task.WhenAny(
                    analyzeTask, Task.Delay(timeout + AnalysisUnwindGrace, stoppingToken));

                if (stoppingToken.IsCancellationRequested)
                {
                    /* Hold briefly for the pass to unwind instead of orphaning it (the Darling
                       twin's #2299 discipline). Offloading onto the pool is what makes this
                       necessary: the store work used to run inline on this thread and so could not
                       still be in flight at this line, and now it can — walking away from it
                       mid-InsertFindingsAsync leaves that write to be torn off by process
                       teardown. CancellationToken.None on purpose: stoppingToken has already
                       FIRED, so passing it here would cancel the wait instantly and defeat the
                       grace entirely. A pass that outlives the hold is left to the in-flight
                       marker, which keeps it from being relaunched either way. */
                    try
                    {
                        await analyzeTask.WaitAsync(AnalysisUnwindGrace, CancellationToken.None);
                    }
                    catch (OperationCanceledException)
                    {
                        /* Shutdown — quiet and expected. */
                    }
                    catch (TimeoutException)
                    {
                        _logger?.LogDebug(
                            "Analysis for {Server} did not unwind within {Grace}s of shutdown — it is abandoned in place",
                            serverName, (int)AnalysisUnwindGrace.TotalSeconds);
                    }

                    break;
                }

                if (finished != analyzeTask)
                {
                    _logger?.LogWarning(
                        "Scheduled analysis for {Server} exceeded {Timeout}s and has not unwound the cancellation raised at that budget — skipped this cycle. The server stays skipped while the pass remains in flight; it will be reported again if it stays that way.",
                        serverName, App.AnalysisTimeoutSeconds);
                    continue;
                }

                /* Analysis already persisted via SaveFindingsAsync inside AnalyzeAsync.
                   Only route findings to the notification channels when delivery is on. */
                var findings = await analyzeTask;

                /* A pass cut short at its budget unwound as asked and returned nothing, so there
                   is nothing to route — say that, rather than letting a cancelled cycle read as a
                   clean all-clear. Gated on the empty result as well as the token so that a pass
                   which genuinely finished in the last instant before the deadline still delivers
                   what it found; the only thing the two conditions can jointly mislabel is a real
                   run that found nothing, whose delivery would have been a no-op anyway. */
                if (findings.Count == 0 && cts.IsCancellationRequested)
                {
                    _logger?.LogWarning(
                        "Scheduled analysis for {Server} exceeded {Timeout}s and was cancelled — no findings this cycle; the next cycle recomputes them",
                        serverName, App.AnalysisTimeoutSeconds);
                    continue;
                }

                if (notify)
                {
                    await _notificationService.NotifyAsync(findings);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Scheduled analysis failed for {Server}", serverName);
                /* If the pass was never launched (e.g. the ctor threw), no continuation exists to
                   clear the marker or release the token source — do both here or this server is
                   skipped forever, which is the very defect this loop is being fixed for. Once the
                   pass IS running the continuation owns them, and clearing them here would both
                   pull the token out from under a live pass and re-admit it next cycle. */
                if (!passStarted)
                {
                    _analysisInFlight.TryRemove(serverId, out _);
                    passCts?.Dispose();
                }
            }
        }
    }

    /// <summary>
    /// #2412: reports a server whose analysis pass is still in flight from an earlier cycle. The
    /// in-flight guard is deliberately released only on true completion — that is what stops a hung
    /// server piling up tasks — but it also means a pass that never completes leaves the marker set
    /// for the life of the process, and every later cycle skipped that server with nothing said. A
    /// cancellation raised at the budget clears the great majority of those; what remains is the
    /// pass wedged where no token reaches, and this is what makes that visible instead of silent.
    /// </summary>
    private void ReportStuckAnalysis(int serverId, string serverName, TimeSpan timeout)
    {
        if (!_analysisInFlight.TryGetValue(serverId, out var state))
        {
            /* Finished between the TryAdd above and this read — it was never stuck. */
            return;
        }

        state.SkippedCycles++;

        var inFlightFor = DateTime.UtcNow - state.StartedUtc;
        var reportAfter =
            (timeout * StuckAnalysisMultiple) *
            Math.Pow(2, Math.Min(state.ReportCount, StuckAnalysisMaxBackoffDoublings));

        if (inFlightFor < reportAfter)
        {
            return;
        }

        state.ReportCount++;

        _logger?.LogError(
            "Scheduled analysis for {Server} has been in flight for {Minutes:F0} minutes — over {Multiple}x its {Timeout}s budget — and did not stop when cancelled at that budget. {Skipped} analysis cycle(s) have been skipped for this server since, and every later cycle is skipped too until the pass unwinds or the app restarts. Analysis for every other server is unaffected.",
            serverName, inFlightFor.TotalMinutes, StuckAnalysisMultiple, App.AnalysisTimeoutSeconds, state.SkippedCycles);
    }

    private void LogProcessMemory()
    {
        try
        {
            using var process = Process.GetCurrentProcess();
            var wsMb = process.WorkingSet64 / 1024 / 1024;
            var privMb = process.PrivateMemorySize64 / 1024 / 1024;
            var gcMb = GC.GetTotalMemory(forceFullCollection: false) / 1024 / 1024;
            _logger?.LogInformation(
                "Process memory: WS={WorkingSetMb} MB, Private={PrivateMb} MB, GC heap={GcMb} MB",
                wsMb, privMb, gcMb);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Failed to read process memory stats");
        }
    }

}
