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
    /// Whether a finished analysis pass may route its findings to the notification channels: the master
    /// alerts switch AND the analysis family toggle, the exact idiom the connection-change gate
    /// established (<c>!_settings.AlertsEnabled || !_notifyConnectionChanges()</c> —
    /// DarlingSelfAlertEvaluator.ApplyConnectionOutcomeAsync). #3464's first measured bypass was this
    /// decision made from the family toggle ALONE: an Analysis INFO card reached a live paging channel
    /// 38 minutes into a fleet-wide mute, because "delivery gated on analysis_notifications_enabled"
    /// had quietly REPLACED the master consult rather than adding to it. The split itself is unchanged
    /// and deliberate (Lite's D0): the pass runs and persists findings whatever this returns — the
    /// master switch's own contract (Darling/README.md, the alerts section) promises exactly that,
    /// "turns off all alert evaluation and analysis finding notifications (the analysis itself
    /// still runs and persists findings)" — so the Recommendations tab keeps filling while the channels
    /// stay silent, and the toggle remains the narrower knob for fleets that want alerting without
    /// analysis mail. One predicate, called by BOTH analysis entry points (the scheduled tick and
    /// analyze_now), so a third entry point has one right thing to call and the census test one shape to
    /// pin. Static and pure so the truth table is testable without a worker.
    /// </summary>
    internal static bool ShouldNotifyAnalysisFindings(DarlingConfig config) =>
        config.Alerts.Enabled && config.Analysis.NotificationsEnabled;

    /// <summary>
    /// #3467: one same-statement-pileup evaluation for one server — the collection-cadence analysis
    /// finding. Reads the newest active-query snapshot window (query_snapshots alone — the #2296
    /// deployments have no Query Store to read, so nothing here may key on one), hands it to the
    /// shared detector, and routes any detection through the SAME finding machinery the scheduled
    /// pass uses: mute-filter → materialize → drill-down → persist → notify-if-delivering. Riding
    /// PgFindingStore means the finding inherits everything the scheduled findings already have —
    /// the mute registry, the (story_path_hash, incident_id) occurrence folding that makes episode
    /// two of the same statement a recurrence on episode one's trail, the notification service's
    /// severity gate (notify_severity), its incident-keyed cooldown, and the #2054
    /// worsening-re-notify — and adds no delivery SEMANTICS of its own. It is a new delivery call
    /// site, which is a different thing and is accounted for as one: the single NotifyAsync below sits
    /// under ShouldNotifyAnalysisFindings(config), carries its own #3465 census entry (Inline), and is
    /// pinned in both directions by AlertMasterSwitchSurfaceTests.
    ///
    /// <para><b>Achieved latency for the measured incident</b>: the pileup snapshot landed 20:10:31Z;
    /// this evaluation runs within the 30-second sweep stamp of the collector write, so the finding —
    /// severity 2.0 against the 1.5 gate — exists and notifies by ~20:11Z, while the episode (which
    /// self-cleared around 20:11) is still alive. The scheduled anomalies arrived 20:31:48Z capped at
    /// 1.0; the query_store-fed PLAN_REGRESSION could not exist before ~20:38Z.</para>
    ///
    /// <para>Failure containment: a store fault costs one log line and this cycle's evaluation — never
    /// the sweep body's remaining passes. InsertFindingsAsync throws by design on a rolled-back batch
    /// (#2448); that throw is contained here because the per-instant dedup stamp advances only after a
    /// successful persist, so the next sweep re-derives the SAME instant rather than waiting for a
    /// fresh one — occurrence folding absorbs the re-persist, the incident-keyed cooldown absorbs the
    /// re-notify — which is precisely what a live-signature finding can afford that a scheduled pass
    /// cannot.</para>
    /// </summary>
    private async Task EvaluateSameStatementPileupAsync(
        ServerLoopState server,
        DarlingConfig config,
        AnalysisNotificationService notificationService,
        CancellationToken stoppingToken)
    {
        var runtime = server.Runtime;
        if (runtime is null)
        {
            return;
        }

        try
        {
            /* Window floor: lookback + staleness margin behind "now", so the newest instant's full
               baseline window is covered even when the newest snapshot itself is a few minutes old.
               The detector applies the real staleness rule against the newest instant it finds. */
            var reader = new PgPileupSnapshotReader(_postgres!, _logger);
            var floor = DateTime.UtcNow.AddMinutes(
                -(SameStatementPileupDetector.BaselineLookbackMinutes + SameStatementPileupDetector.StaleSnapshotCutoffMinutes));
            var rows = await reader.ReadWindowAsync(runtime.ServerId, floor, stoppingToken);
            if (rows.Count == 0)
            {
                return;
            }

            /* One evaluation per snapshot instant: the 30-second sweep outpaces the one-minute
               collector, and re-running the detector over an already-evaluated instant would persist
               duplicate occurrence rows for the same evidence. The stamp advances only once the
               instant's OUTCOME is settled — evaluated clean, fully muted, or persisted — never
               before the persist: stamped ahead of InsertFindingsAsync, a transient store fault would
               mark a firing instant "done" and lose its finding for good if the episode cleared
               before the next collector write. Left behind on a fault, the next sweep re-derives the
               same instant instead (the 30-second sweep usually gets its retry in before the
               one-minute collector replaces the instant), and the machinery downstream absorbs the
               replay: a duplicate persist folds onto the same (story_path_hash, incident_id) trail,
               a duplicate notify dies in the incident-keyed cooldown. */
            var latest = DateTime.MinValue;
            foreach (var row in rows)
            {
                if (row.CollectionTime > latest)
                {
                    latest = row.CollectionTime;
                }
            }

            if (latest <= server.LastPileupSnapshotEvaluated)
            {
                return;
            }

            var detections = SameStatementPileupDetector.Evaluate(runtime.StorageName, rows, DateTime.UtcNow);
            if (detections.Count == 0)
            {
                server.LastPileupSnapshotEvaluated = latest;
                return;
            }

            /* The scheduled pass's two-phase persist, transplanted: mute-filter materializes the
               surviving findings (the STORAGE identity, same as every collected row), the drill-down
               evidence is attached between the phases, and the insert commits the set or nothing. */
            var context = new AnalysisContext
            {
                ServerId = runtime.ServerId,
                ServerName = runtime.StorageName,
                TimeRangeStart = latest.AddMinutes(-SameStatementPileupDetector.BaselineLookbackMinutes),
                TimeRangeEnd = latest,
                /* #5558: the same-statement pileup is a node-local finding (queries that piled up on THIS node), so there is nothing to skip. */
                SecondaryReplicaDatabases = PerformanceMonitor.Darling.Analysis.PgSecondaryReplicaScope.NoneSkipped,
                CancellationToken = stoppingToken,
                ShutdownToken = stoppingToken,
            };

            var findingStore = new PgFindingStore(_postgres!, _logger);
            var findings = await findingStore.FilterMutedFindingsAsync(
                detections.Select(d => d.Story).ToList(), context);
            if (findings.Count == 0)
            {
                server.LastPileupSnapshotEvaluated = latest;
                return;
            }

            var drillDownByHash = detections.ToDictionary(d => d.Story.StoryPathHash, d => d.DrillDown);
            foreach (var finding in findings)
            {
                if (drillDownByHash.TryGetValue(finding.StoryPathHash, out var drillDown))
                {
                    finding.DrillDown = drillDown;
                }
            }

            await findingStore.InsertFindingsAsync(findings, context);
            server.LastPileupSnapshotEvaluated = latest;

            _logger.LogWarning(
                "[{Server}] Same-statement pileup detected: {Count} finding(s) at snapshot {Snapshot:u}, peak severity {Severity:F2}",
                server.Config.DisplayName, findings.Count, latest, findings.Max(f => f.Severity));

            /* Delivery, gated exactly like the scheduled pass's findings: master AND family, one
               predicate (#3464). Below the gate, the notification service applies notify_severity,
               the incident-keyed cooldown, and worsening re-notify — this path adds no delivery
               semantics of its own. */
            if (ShouldNotifyAnalysisFindings(config))
            {
                await notificationService.NotifyAsync(findings);
            }
        }
        catch (OperationCanceledException)
        {
            /* Shutdown — quiet and expected. */
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "[{Server}] Same-statement pileup evaluation failed — skipped this cycle; the next sweep re-evaluates a fresher window: {Message}",
                server.Config.DisplayName, ex.Message);
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

        /* #5824: the scheduled pass is recorded as one collection_log row (AnalysisPassRow), the only record of how long it
           held this server's fleet slot. The analyze_now command maps the outcome to its result instead and writes none. */
        DarlingAnalysisService? passService = null;
        Task? passTask = null;
        var started = Stopwatch.GetTimestamp();
        var result = await RunAnalysisPassAsync(
            runtime.ServerId, runtime.StorageName, server.Config.DisplayName,
            planFetcher, notificationService, notifyFindings, postPassHook, stoppingToken,
            AnalysisSeparatelyMonitoredDatabases(runtime), (service, task) => (passService, passTask) = (service, task));
        var elapsedMs = (long)Stopwatch.GetElapsedTime(started).TotalMilliseconds;

        if (stoppingToken.IsCancellationRequested)
        {
            return;
        }

        var planRegressionMs = PlanRegressionReadOfFinishedPass(passTask, () => passService?.LastPlanRegressionReadMs);
        var (status, detail) = DescribeAnalysisPass(
            result.Status, result.FindingCount, result.Message, elapsedMs, planRegressionMs, insideFleetSlot: true);
        await DarlingObservability.LogAnalysisPassAsync(
            _postgres!, runtime, status, result.FindingCount, elapsedMs, detail, _logger, stoppingToken);
    }

    /// <summary>
    /// #5824: the status word and the detail of a scheduled analysis pass's <c>collection_log</c> row. Never <c>SUCCESS</c>
    /// (see <see cref="AnalysisPassRow"/>). The detail is one line, read per server through <c>get_collection_log</c>: the
    /// outcome, the findings count, the PLAN_REGRESSION read's time, and whether the pass ran inside the server's fleet
    /// collection slot. Pure, so a unit test pins the wording.
    /// </summary>
    internal static (string Status, string Detail) DescribeAnalysisPass(
        AnalysisPassStatus status, int findingCount, string? message, long elapsedMs, long? planRegressionMs, bool insideFleetSlot)
    {
        var word = status switch
        {
            AnalysisPassStatus.Ran or AnalysisPassStatus.InsufficientData => AnalysisPassRow.Analyzed,
            AnalysisPassStatus.TimedOut => AnalysisPassRow.Abandoned,
            AnalysisPassStatus.Error => AnalysisPassRow.Error,
            _ => AnalysisPassRow.Skipped,
        };
        var outcome = status switch
        {
            AnalysisPassStatus.Ran => "ran",
            AnalysisPassStatus.InsufficientData => "ran on too little data",
            AnalysisPassStatus.TimedOut => "timed out",
            AnalysisPassStatus.Error => "failed",
            _ => "did not run",
        };
        var planRegression = planRegressionMs is { } ms
            ? "PLAN_REGRESSION read " + DarlingCycleSkip.Format(ms)
            : "PLAN_REGRESSION read did not run or was not measured";
        var slot = insideFleetSlot ? "inside this server's fleet collection slot" : "outside any fleet collection slot";
        var detail = string.Create(CultureInfo.InvariantCulture,
            $"Scheduled analysis pass {outcome} in {DarlingCycleSkip.Format(elapsedMs)}, {findingCount} finding(s), {planRegression}, {slot}.");
        if (!string.IsNullOrWhiteSpace(message))
        {
            detail += " " + message;
        }

        return (word, detail);
    }

    /// <summary>
    /// #5824: the PLAN_REGRESSION read time of a pass, read only once the pass's task has finished, because the service stamps
    /// it in <c>AnalyzeAsync</c>'s <c>finally</c>. Keyed on the task, not on the status word: a pass that ran past its budget,
    /// unwound and came back (status TimedOut, <c>EndedEarlyAs</c> Timeout) has finished and keeps its real figure; a pass the
    /// sweep stopped waiting for may still be running, so its figure is null rather than whatever the pass has stamped so
    /// far. A pass that never started (no task) is null too.
    /// </summary>
    internal static long? PlanRegressionReadOfFinishedPass(Task? passTask, Func<long?> read) =>
        passTask is not null && passTask.IsCompleted ? read() : null;

    /// <summary>Terminal states of one analysis pass — surfaced to the analyze_now command result.</summary>
    internal enum AnalysisPassStatus { Ran, Skipped, TimedOut, InsufficientData, Error }

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
        CancellationToken stoppingToken,
        IReadOnlyList<string>? separatelyMonitoredDatabases = null,
        Action<DarlingAnalysisService, Task>? onPassStarted = null)
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
            var analysisService = new DarlingAnalysisService(_postgres!, planFetcher, _logger, _baselineCache, _analyzerConfig)
            {
                SeparatelyMonitoredDatabases = separatelyMonitoredDatabases
            };

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
            /* #5633 item 13: the scheduled pass is the ONE caller that writes collect.analysis_collection_caveats
               (on-demand passes -- MCP analyze_server -- compute their caveats and never save them). */
            var analyzeTask = analysisService.AnalyzeAsync(
                serverId, storageName, hoursBack: 4, cts.Token, stoppingToken, persistCollectionCaveats: true);

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
            /* #5824: the scheduled caller reads the pass's PLAN_REGRESSION time off the service, and only once this task has
               finished (PlanRegressionReadOfFinishedPass). */
            onPassStarted?.Invoke(analysisService, analyzeTask);

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

            /* Persist the pass's data-state determination (V19 marker) so the Viewer's Recommendations
               tab shows a reason instead of a false all-clear on a zero-finding read: true + the
               engine's message when the pass hit the 24h data-span gate ("still collecting"); false +
               the engine's message when the pass cleared the gate but the window itself collected zero
               facts (#3524/#3551 — a dead-collector shape, "collection appears broken"; false-with-a-
               message is a shape only this arm writes, so the viewer distinguishes it without a schema
               change); cleared (false + null) when a real pass completed on measured facts, which is
               how both miss markers self-heal. Failure-isolated like the other observability writes.
               Only the REAL terminal states write it — a Skipped/TimedOut/Error pass (handled above /
               in the catch) leaves the last known marker untouched. */
            /* #3691: the sweep's own pass result, the row an operator reads in the analysis state. A pass
               that could not read a fact family still RAN and still counted its findings honestly — it just
               counted them over less evidence than it looks like, and the message is the only place that
               says so. Composed, never overwritten: the window-empty arm below already has a sentence. */
            var collectionCaveat = analysisService.LastCollectionFailures.Count > 0
                ? CollectionCaveats.Describe(analysisService.LastCollectionFailures, analysisService.LastCollectionFamilyCount)
                : null;
            if (analysisService.InsufficientDataMessage is string insufficient)
            {
                await DarlingObservability.WriteAnalysisStateAsync(
                    _postgres!, serverId, insufficientData: true, insufficient, _logger, stoppingToken);
                return new AnalysisPassResult(AnalysisPassStatus.InsufficientData, 0, insufficient);
            }

            if (analysisService.WindowEmptyMessage is string windowEmpty)
            {
                await DarlingObservability.WriteAnalysisStateAsync(
                    _postgres!, serverId, insufficientData: false, windowEmpty, _logger, stoppingToken);
                return new AnalysisPassResult(AnalysisPassStatus.Ran, 0, CollectionCaveats.Compose(windowEmpty, collectionCaveat));
            }

            await DarlingObservability.WriteAnalysisStateAsync(
                _postgres!, serverId, insufficientData: false, null, _logger, stoppingToken);
            return new AnalysisPassResult(AnalysisPassStatus.Ran, findings.Count, collectionCaveat);
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

        /* "Generate now" takes the same pass every engine does (#3542). This door used to carry its own
           PostgreSQL arm — the scheduled tick's tombstone re-written here so an operator's click could not
           overwrite it with the generic "still collecting" text — and it went with the tick's gate: the
           pass routes by the registry's engine_kind inside DarlingAnalysisService, so the honest answer for
           a PostgreSQL target is now the pass's own result, and there is no tombstone left to protect. */

        /* postPassHook: null — analyze_now is an interactive diagnostic, and the force-plan bot only
           rides the SCHEDULED cadence so an operator poking a server cannot spend its action budget.
           Findings still come back in the command result under master-off — the operator asked a question
           and gets the answer — but the notification CHANNELS stay silent (#3464): an on-demand pass must
           not be the one analysis entry point that can page through a mute. */
        var result = await RunAnalysisPassAsync(
            serverId, server.Config.StorageName, server.Config.DisplayName,
            planFetcher, notificationService, ShouldNotifyAnalysisFindings(config), postPassHook: null, cancellationToken,
            AnalysisSeparatelyMonitoredDatabases(server.Runtime));

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
}
