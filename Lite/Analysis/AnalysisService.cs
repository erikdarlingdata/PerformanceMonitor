using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Orchestrates the full analysis pipeline: collect → score → traverse → persist.
/// Can be run on-demand or on a timer. Each run analyzes a single server's data
/// for a given time window and persists the findings.
/// </summary>
public class AnalysisService
{
    private readonly DuckDbInitializer _duckDb;
    private readonly FindingStore _findingStore;
    private readonly DuckDbFactCollector _collector;
    private readonly FactScorer _scorer;
    private readonly RelationshipGraph _graph;
    private readonly InferenceEngine _engine;
    private readonly DrillDownCollector _drillDown;
    private readonly AnomalyDetector _anomalyDetector;
    private readonly BaselineProvider _baselineProvider;
    /// <summary>
    /// Minimum hours of collected data required before analysis will run.
    /// Short collection windows distort fraction-of-period calculations —
    /// 5 seconds of THREADPOOL looks alarming in a 16-minute window.
    /// 24 hours has been validated empirically as sufficient.
    /// </summary>
    internal double MinimumDataHours { get; set; } = 24;

    /// <summary>
    /// Raised after each analysis run completes, providing the findings for UI display.
    /// </summary>
    public event EventHandler<AnalysisCompletedEventArgs>? AnalysisCompleted;

    /// <summary>
    /// Whether an analysis is currently running.
    /// </summary>
    public bool IsAnalyzing { get; private set; }

    /// <summary>
    /// Time of the last completed analysis run.
    /// </summary>
    public DateTime? LastAnalysisTime { get; private set; }

    /// <summary>
    /// Set after AnalyzeAsync if insufficient data was found. Null if enough data exists.
    /// </summary>
    public string? InsufficientDataMessage { get; private set; }

    /// <summary>
    /// Set after AnalyzeAsync when the server PASSED the data-span gate but the collector observed NONE
    /// of the analysis window (#3524, #3538 A2): the coverage witness found no collection interval inside
    /// it. Null otherwise. The gate measures TOTAL history, so a server whose collection died still sails
    /// through it and lands on an unobserved window — which is a dead collector or an unreachable target,
    /// not a healthy server. Callers must not render an empty findings list as an all-clear while this is
    /// set; nothing was measured.
    ///
    /// <para>#3653: this is the UNOBSERVED-window message and nothing else. An observed window over which
    /// the collector emitted no fact leaves it null and runs the pass — that is a measurement with nothing
    /// in it, and <see cref="LastWindowCoverage"/> says how much of the window the all-clear rests on.</para>
    /// </summary>
    public string? WindowEmptyMessage { get; private set; }

    /// <summary>
    /// How much of the last pass's window the collector actually observed (#3538 A2), stamped by the
    /// fact collector and carried out here the way <see cref="WindowEmptyMessage"/> is, because the
    /// findings list cannot say it: a pass over a window with a three-hour hole returns the SAME shape as
    /// a pass over a fully collected one, and only this tells the caller that the rates were divided by
    /// one hour rather than four and that the caveat is owed. Null when the pass never reached
    /// collection (the data-span gate, or a fault before it).
    /// </summary>
    public WindowCoverage? LastWindowCoverage { get; private set; }

    /// <summary>
    /// The fact families whose read FAILED in the last pass (#3691), carried out the way
    /// <see cref="LastWindowCoverage"/> is and for the same reason: the findings list cannot say it. A pass
    /// in which every family failed returns the same empty list as a quiet server, and until now the tools
    /// rendered it as the <c>empty</c> all-clear. Empty on a clean pass and on a pass that never reached
    /// collection; <see cref="LastCollectionFamilyCount"/> is the total the count is stated against.
    /// </summary>
    public IReadOnlyList<CollectionFailure> LastCollectionFailures { get; private set; } = [];

    /// <summary>How many family reads the collector ran in the last pass — the caveat's denominator, stamped
    /// by the collector from its own type (#3691). 0 when the pass never reached collection.</summary>
    public int LastCollectionFamilyCount { get; private set; }

    /// <summary>
    /// How many facts the last pass handed to the scorer — the collector's plus the anomaly detector's
    /// (#3691) — and how many of those came out with a non-zero severity. Both null when the pass never
    /// reached scoring (the data-span gate, the unobserved window, a fault). They exist so an <c>empty</c>
    /// envelope can say which kind of nothing it is: "facts were scored and none fired" (both &gt; 0),
    /// "facts were read and none graded above zero" (count &gt; 0, scored 0), or "no fact was emitted"
    /// (count 0) — which, beside <see cref="LastCollectionFailures"/>, is the difference between a quiet
    /// server and a blind pass.
    /// </summary>
    public int? LastFactCount { get; private set; }

    /// <summary>See <see cref="LastFactCount"/>: of those facts, how many scored above zero.</summary>
    public int? LastFactsScored { get; private set; }

    /// <param name="retentionDaysForCollector">#1757: resolves a collector's configured retention so the
    /// baseline provider can warn when a source table is retained for less than the baseline window. Optional
    /// — null simply disables that warning, which is why every existing caller keeps working unchanged.</param>
    public AnalysisService(
        DuckDbInitializer duckDb,
        IPlanFetcher? planFetcher = null,
        Func<string, int?>? retentionDaysForCollector = null)
    {
        _duckDb = duckDb;
        _findingStore = new FindingStore(duckDb);
        _collector = new DuckDbFactCollector(duckDb);
        _scorer = new FactScorer();
        _graph = new RelationshipGraph();
        _engine = new InferenceEngine(_graph);
        _drillDown = new DrillDownCollector(duckDb, planFetcher);
        _baselineProvider = new BaselineProvider(duckDb, retentionDaysForCollector);
        _anomalyDetector = new AnomalyDetector(duckDb, _baselineProvider);
    }

    /// <summary>
    /// Runs the full analysis pipeline for a server.
    /// Default time range is the last 4 hours.
    /// </summary>
    /// <param name="cancellationToken">#2412: abandons the pass at the scheduler's per-server
    /// budget (and at app shutdown). Optional so the on-demand callers — the Recommendations tab
    /// and the MCP tool, neither of which has a budget to enforce — keep the prior behavior. The
    /// argument order matches the Darling twin's AnalyzeAsync so the two stay transplantable.</param>
    /// <param name="asOfUtc">#2506: moves the END of the window off "now" while
    /// <paramref name="hoursBack"/> stays its LENGTH, so an incident can be analyzed where it happened.
    /// Null — every caller but the anchored MCP tool — is the pre-#2506 behaviour exactly. Anchoring
    /// reaches the whole pipeline through the context, including the anomaly detector's hour-of-day ×
    /// day-of-week baseline, which is keyed off the window rather than off the clock. An anchored pass
    /// does not persist — see <see cref="AnalysisContext.PersistFindings"/>.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last",
        Justification = "The token is at position 4 and the scheduler passes it POSITIONALLY. Moving asOfUtc ahead " +
                        "of it to satisfy the rule would silently rebind that call site's arguments — a compiling " +
                        "change of meaning on the one caller that matters. Appending is the only edit that cannot " +
                        "do that, and the Darling twin keeps the same order so the two stay transplantable.")]
    public async Task<List<AnalysisFinding>> AnalyzeAsync(
        int serverId, string serverName, int hoursBack = 4, CancellationToken cancellationToken = default,
        DateTime? asOfUtc = null)
    {
        var timeRangeEnd = asOfUtc ?? DateTime.UtcNow;
        var timeRangeStart = timeRangeEnd.AddHours(-hoursBack);

        var context = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = timeRangeStart,
            TimeRangeEnd = timeRangeEnd,
            AsOfUtc = asOfUtc,
            CancellationToken = cancellationToken
        };

        return await AnalyzeAsync(context);
    }

    /// <summary>
    /// Runs the full analysis pipeline with a specific context.
    /// </summary>
    public async Task<List<AnalysisFinding>> AnalyzeAsync(AnalysisContext context)
    {
        if (IsAnalyzing)
            return [];

        IsAnalyzing = true;
        InsufficientDataMessage = null;
        WindowEmptyMessage = null;
        LastWindowCoverage = null;
        LastCollectionFailures = [];
        LastCollectionFamilyCount = 0;
        LastFactCount = null;
        LastFactsScored = null;

        try
        {
            /* #2412: a checkpoint ahead of every store-touching stage, so the rule is simply that
               no store read STARTS after the budget has gone. One check at the top of the method
               would not deliver that — each stage below is a many-query phase, and a cancelled
               pass would run out whichever one it was already inside. This first checkpoint earns
               its place even though the read below is a single scalar: an already-cancelled
               context arrives here whenever the budget is very short or the pass queued behind a
               wedged server, and it should not buy a round-trip. The post-enrichment tail (action
               build + insert) carries no check on purpose — by then the expensive work is paid
               for and finishing is what preserves it. */
            context.CancellationToken.ThrowIfCancellationRequested();

            // 0. Check minimum data span — total history, not the analysis window.
            // A server with 100h of total history can be analyzed over a 4h window.
            var dataSpanHours = await GetTotalDataSpanHoursAsync(context.ServerId, context.CancellationToken);
            if (dataSpanHours < MinimumDataHours)
            {
                var needed = MinimumDataHours >= 24
                    ? $"{MinimumDataHours / 24:F1} days"
                    : $"{MinimumDataHours:F0} hours";
                var have = dataSpanHours >= 24
                    ? $"{dataSpanHours / 24:F1} days"
                    : $"{dataSpanHours:F1} hours";

                InsufficientDataMessage =
                    $"Not enough data for reliable analysis. Need {needed} of collected data, " +
                    $"have {have}. Keep the collector running and try again later.";

                AppLogger.Info("AnalysisService",
                    $"Skipping analysis for {context.ServerName}: {dataSpanHours:F1}h data, need {MinimumDataHours}h");

                LastAnalysisTime = DateTime.UtcNow;
                return [];
            }

            context.CancellationToken.ThrowIfCancellationRequested();

            // 1. Collect facts from DuckDB
            var facts = await _collector.CollectFactsAsync(context);
            LastWindowCoverage = context.Coverage;
            LastCollectionFailures = context.CollectionFailures;
            LastCollectionFamilyCount = context.CollectionFamilyCount;

            /* #3691 lane 45: the same two facts into the PROCESS-WIDE ledger, the twin of Darling's write.
               This SKU keeps one analysis service for the app's life, so the instance properties above are
               not blind here the way Darling's are — but get_collection_health reads ONE ledger on both
               products, and a surface that exists on one SKU only is the parity defect the twin review
               blocks. Recorded on EVERY pass, clean ones included: a clean pass is the only evidence that a
               family recovered. */
            CollectionCaveatLedger.Shared.Record(
                context.ServerId, context.ServerName, DateTime.UtcNow,
                context.CollectionFailures, context.CollectionFamilyCount);

            if (context.CollectionFailures.Count > 0)
            {
                /* #3691: the per-site log lines above say each failure as it happened; this one line says
                   what the PASS is missing, so a scheduled pass — which has no payload to carry
                   collection_caveats — still leaves the summary beside the findings it persists. Every
                   envelope the tools render off this pass carries the same sentence. */
                AppLogger.Warn("AnalysisService",
                    $"Collection caveat for {context.ServerName}: {CollectionCaveats.Describe(context.CollectionFailures, context.CollectionFamilyCount)}");
            }

            if (context.ObservedDurationMs <= 0)
            {
                /* #3524: the span gate above passed on LIFETIME history, so an empty WINDOW here means
                   collection stopped producing rows for it — not that the server is healthy. Say so,
                   instead of returning a bare [] that reads exactly like "analyzed and found nothing".

                   #3538 A2 widens the branch to a window with NO OBSERVED COLLECTION TIME even when some
                   facts exist. The facts that survive an unobserved window are the point-in-time ones
                   (server config, trace flags, hardware) — read from the latest row regardless of
                   window — and scoring those alone would produce a pass whose every windowed rate is
                   absent and whose all-clear (or config-only findings) still reads as "analyzed this
                   window". Nothing was measured over the window; the same envelope says so.

                   #3653: the branch is gated on the coverage witness ALONE. Until then it read
                   `facts.Count == 0 || context.ObservedDurationMs <= 0`, and the first half — the original
                   #3524 test, written before any collector stamped coverage — made an OBSERVED window that
                   happened to produce no fact wear this envelope too: "collection appears to have stopped"
                   over a window the witness proves the collector was up for, rendered `unavailable` by both
                   analyze_server tools with a pointer at collection health, and persisted by the worker as
                   the dead-collector marker the Viewer renders. That case falls through to the branch below.
                   This envelope is now for exactly the case its prose is true of: no collection interval
                   landed inside the window. Every collector stamps the witness before its first windowed
                   read (wait_stats on a SQL Server target, pg_database_stats on a PostgreSQL one, #3665), so
                   "unstamped" is not a third state here — Coverage is null only when collection threw, and
                   that unwinds through the catch below, never through this test. */
                /* True when the WINDOW went unobserved but point-in-time facts (config, trace flags,
                   hardware) still read — the case that used to slip past the facts.Count == 0 check. */
                var hasPointInTimeFactsOnly = facts.Count > 0;
                WindowEmptyMessage = hasPointInTimeFactsOnly
                    ? $"The collector observed none of the analysis window " +
                      $"({context.TimeRangeStart:yyyy-MM-dd HH:mm} to {context.TimeRangeEnd:yyyy-MM-dd HH:mm} UTC): " +
                      $"no collection interval landed inside it, even though this server has {dataSpanHours:F1} hours " +
                      $"of total collected history. The {facts.Count} fact(s) that could still be read are point-in-time " +
                      "configuration and state, not measurements of this window. Collection appears to have stopped or " +
                      "broken for this window, so nothing was measured — this is NOT an all-clear."
                    : $"No facts were collected in the analysis window " +
                      $"({context.TimeRangeStart:yyyy-MM-dd HH:mm} to {context.TimeRangeEnd:yyyy-MM-dd HH:mm} UTC) " +
                      $"even though this server has {dataSpanHours:F1} hours of total collected history. " +
                      "Collection appears to have stopped or broken for this window, so nothing was measured " +
                      "— this is NOT an all-clear.";

                AppLogger.Warn("AnalysisService",
                    $"No observed collection in the analysis window for {context.ServerName} " +
                    $"({context.TimeRangeStart:o} to {context.TimeRangeEnd:o}) " +
                    $"despite {dataSpanHours:F1}h of total history — collection may be down ({facts.Count} point-in-time fact(s) only)");

                LastAnalysisTime = DateTime.UtcNow;
                return [];
            }

            if (facts.Count == 0)
            {
                /* #3653: the collector OBSERVED the window — the witness found collection intervals inside
                   it — and emitted no fact over it. Nothing rose to a fact: no wait accrued time, no event
                   table has a row in the window, and no point-in-time family had a latest row to read. That
                   is not the dead-collector shape above, and it must not wear its envelope: WindowEmptyMessage
                   stays null, so both analyze_server tools render whatever this pass returns as `empty` with
                   the coverage block rather than `unavailable`, the worker clears the analysis_state marker
                   instead of writing "collection appears broken", and neither Viewer points at collection
                   health for a collector that is up.

                   The pass CONTINUES rather than returning [] here, because "the collector emitted no fact"
                   is a statement about the collector's reads, not the detector's: the anomaly detector reads
                   the store on its own and still gets its say, and whatever survives scoring is the answer.
                   A zero-finding pass out of here is an all-clear AT THE STATED COVERAGE — the coverage block
                   every tool payload carries says which fraction of the window it rests on, and the line
                   below says the same for the scheduled pass, which has no payload to put it in.

                   Partial coverage cannot arrive here. The collector adds the COLLECTION_GAP context fact
                   whenever the observed fraction is under WindowCoverage.PartialThreshold, so zero facts with
                   observed time means coverage at or above the bar; the partial caveat below is therefore
                   unreachable from this branch by construction, not by luck. Information rather than Warning:
                   nothing is wrong with the collector, and a Warning here would be the old lie at a lower
                   volume. */
                AppLogger.Info("AnalysisService",
                    $"The collector observed the analysis window for {context.ServerName} " +
                    $"({context.TimeRangeStart:o} to {context.TimeRangeEnd:o}) " +
                    $"but emitted no fact over it — {context.Coverage!.Describe()}; nothing rose to a fact, and an all-clear from this pass rests on that coverage");
            }

            if (context.Coverage is { IsPartial: true } partial)
            {
                /* Logged, not just carried: a scheduled pass has no payload to put the caveat in, and
                   the persisted findings from this pass were rated against the observed hours. */
                AppLogger.Warn("AnalysisService",
                    $"Partial collection coverage for {context.ServerName}: {partial.Describe()} — " +
                    "rates in this pass are per observed time, and the COLLECTION_GAP fact records the hole");
            }

            context.CancellationToken.ThrowIfCancellationRequested();

            // 1.5. Detect anomalies (compare analysis window against baseline)
            var anomalies = await _anomalyDetector.DetectAnomaliesAsync(context);
            facts.AddRange(anomalies);

            // 2. Score facts (base severity + amplifiers)
            _scorer.ScoreAll(facts);

            /* #3691: what the scorer saw and what it graded, for the `empty` envelope's two counts. Read
               here, after scoring and before attribution adds its own cards, so "facts_scored" means the
               collector's and detector's facts and nothing composed later. */
            LastFactCount = facts.Count;
            LastFactsScored = facts.Count(f => f.Severity > 0);

            // 2.5. Config → outcome attribution (#3653 A10, Q2). AFTER scoring, never before: the fact
            // is appended with its Information severity preset (ConfigChangeAttribution.InformationSeverity),
            // because FactScorer.ScoreConfigFact returns 0 for a "config" key it does not know and ScoreAll
            // would zero it out of the working set. Its own try inside: a failed snapshot read or compare
            // costs the pass this one card, never the pass. The Darling twin gates on its SQL Server engine
            // set; Lite has only that engine.
            context.CancellationToken.ThrowIfCancellationRequested();
            await AttributeConfigChangesAsync(context, facts);

            // 3. Build stories via graph traversal
            var stories = _engine.BuildStories(facts);

            // 3.5. Freeze value-stated advice (current MAXDOP/CTFP/etc.) into each story's StoryText
            // from the FULL fact set, BEFORE the store copies StoryText onto the finding. This is the
            // only place the raw fact VALUES are in scope; read-back cards then state the numbers
            // (FactAdvice.GetComposedForFinding) instead of generic folklore. No schema change.
            FactAdvice.PopulateStoryText(stories, facts);

            // 3.6. Cluster the run's stories into causally-related incidents (graph-connectivity) and
            // stamp each with its own trackable id, BEFORE the store copies it onto the finding. The
            // grouped surface renders one report per incident; the id fingerprints the incident's
            // primary so the same recurring incident is trackable across runs.
            var incidents = _engine.ClusterIntoIncidents(stories, facts);
            IncidentId.StampClusters(context.ServerName, incidents);

            // 3.7. Fold each ANOMALY_* story into the REGULAR finding that describes the same symptom
            // (same run, same database) by rewriting its stamped incident id onto that parent's — so
            // the anomaly stops rendering as its own card / its own email. No-parent anomalies stay
            // solo; db-scoped object anomalies never cross databases. Presentation-only: nothing is
            // dropped, only the incident tag is reconciled. The facts ride along (#3704) so a
            // maintenance-family anomaly folded onto a fired RUNNING_JOBS' incident can name the job
            // in its frozen StoryText — the name is on the fact (#3693), not on any story.
            AnomalyIncidentReconciler.Reconcile(stories, facts);

            // 3.8. Label the chains that keep a weekly schedule (#3653 item 3, ruling Q3: LABEL, not discount).
            // One store read of the prior three weeks — every chain that fired in this pass's hour×weekday
            // slot on the target's clock, plus every RUNNING_JOBS card in any slot — then RecurrenceLabeler
            // appends one sentence to the frozen StoryText of each story whose chain fired in this slot for
            // three consecutive weeks ("recurring at this hour"), and one to each story tied to a fired job
            // whose slot MOVED since last week ("maintenance window moved"). Severity is untouched, by ruling.
            // Here — after the fold has finished rewriting StoryText and before the store copies it onto the
            // finding — so the label is on the persisted row every card, e-mail and MCP read renders. The
            // reference instant is the window's end: "now" for a scheduled pass, the anchor for an as-of one,
            // so an exploratory pass is labelled relative to the instant it explores. A read the store could
            // not make labels nothing and costs the pass nothing (FindingStore.GetPriorOccurrencesAsync).
            var priorOccurrences = await _findingStore.GetPriorOccurrencesAsync(context, context.TimeRangeEnd);
            RecurrenceLabeler.Label(stories, facts, context.TimeRangeEnd, priorOccurrences);

            context.CancellationToken.ThrowIfCancellationRequested();

            // 4. Mute-filter the stories into the surviving findings WITHOUT inserting yet (the
            //    Darling twin's D2/P2 reorder) — enrichment + action-build happen on the survivors
            //    first so the BUILT RemediationAction is persisted on each row.
            var findings = await _findingStore.FilterMutedFindingsAsync(stories, context);

            context.CancellationToken.ThrowIfCancellationRequested();

            // 5. Enrich the survivors with drill-down data (ephemeral except through the built action).
            await _drillDown.EnrichFindingsAsync(findings, context);

            // 6. Build + attach each finding's RemediationAction from the now drill-down-populated
            //    finding, then persist it as remediation_action_json. The builders REQUIRE
            //    finding.DrillDown, which the store read-back does NOT return — so the BUILT action is
            //    persisted, exactly the artifact the read path deserializes and the Recommendations
            //    reader renders into the copy-paste command. Same shared builders + null-coalescing
            //    order Darling's DarlingAnalysisService uses, so Lite and Darling produce identical
            //    commands. Lite has no in-app executor: the action drives a COPYABLE command only.
            foreach (var finding in findings)
            {
                finding.Remediation =
                    FactRemediation.BuildAction(finding)
                    ?? FactRemediation.BuildRcsiAction(finding)
                    ?? FactRemediation.BuildClearPlanAction(finding)
                    ?? FactRemediation.BuildFileAutogrowthAction(finding) // advisory copy-paste (no handler)
                    ?? FactRemediation.BuildServerConfigAction(finding)   // server-level config — MAXDOP/CTFP/memory
                    ?? FactRemediation.BuildMissingIndexAction(finding);  // missing-index CREATE — copy-paste only
            }

            // 7. Insert the survivors in one batched pass, persisting remediation_action_json —
            //    UNLESS the window was anchored at a past instant (#2506), in which case the pass is
            //    exploratory and writes nothing. The findings are still built, enriched and returned in
            //    full; only the row is withheld, because the row would claim to be a current
            //    observation. AnalysisContext.PersistFindings carries the whole argument.
            if (context.PersistFindings)
            {
                await _findingStore.InsertFindingsAsync(findings, context);
            }

            LastAnalysisTime = DateTime.UtcNow;

            // 8. Notify listeners. Gated with the insert for the same reason and not a weaker one:
            //    this event is how findings reach notification, and an alert about last Tuesday
            //    delivered today is the persistence problem with a shorter fuse.
            if (context.PersistFindings)
            {
                AnalysisCompleted?.Invoke(this, new AnalysisCompletedEventArgs
                {
                    ServerId = context.ServerId,
                    ServerName = context.ServerName,
                    Findings = findings,
                    AnalysisTime = LastAnalysisTime.Value
                });
            }

            AppLogger.Info("AnalysisService",
                $"Analysis complete for {context.ServerName}: {findings.Count} finding(s), " +
                $"highest severity {(findings.Count > 0 ? findings.Max(f => f.Severity) : 0):F2}" +
                (context.PersistFindings ? string.Empty : " (anchored window — exploratory, not persisted)"));

            return findings;
        }
        catch (Exception ex) when (AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* #2443: the predicate moved to AnalysisAbandon.IsExpected, unchanged — signalled token
               AND OperationCanceledException — because the store layer now needs the same judgement
               at forty-odd catch sites, and two copies of it would drift. */
            /* #2412: the pass was abandoned because it outlived its budget (or the app is
               stopping), which is not a fault and must not read as one. Whatever this pass would
               have written is gone; the next scheduled pass recomputes it from the store.

               Both halves of the filter are load-bearing, and the TYPE half especially so here.
               This token fires on TIMEOUT as well as at shutdown, so it is signalled during
               ordinary running — a blanket `Exception` filter would relabel any genuine fault
               that happened to land after the budget elapsed as abandonment and drop it to Info,
               burying the one line of evidence it left. OperationCanceledException is what the
               checkpoints throw, and what DuckDB's own duckdb_interrupt surfaces, so nothing the
               cancellation actually produces is lost by naming it. This is the Darling twin's
               AnalysisShutdown.IsExpectedAbandon discipline — signalled token AND a shape the
               cancellation really produces — narrowed to the one shape that arises here. */
            AppLogger.Info("AnalysisService",
                $"Analysis abandoned for {context.ServerName} — this pass's findings are lost by design; the next pass recomputes them ({ex.Message})");
            return [];
        }
        catch (Exception ex)
        {
            AppLogger.Error("AnalysisService", $"Analysis failed for {context.ServerName}: {ex.Message}");
            return [];
        }
        finally
        {
            IsAnalyzing = false;
        }
    }

    /// <summary>
    /// Runs the collect + detect + score pipeline without graph traversal.
    /// Returns raw scored facts with amplifier details for direct inspection, together with the
    /// window's observed coverage (#3538 A2) — returned rather than parked on a property because this
    /// path has no <see cref="IsAnalyzing"/> guard and two on-demand callers can overlap; a shared
    /// property would let one read the other's window. Coverage is null only when collection threw.
    /// The third element (#3691) is the same context's collection failures and family total, for the
    /// callers' <c>collection_caveats</c> — returned for the same overlap reason, and populated even when
    /// collection threw, so a read that recorded failures before the throw still reports them.
    ///
    /// <para>#2506: <paramref name="asOfUtc"/> anchors the END of the window; null is "now", which is
    /// every caller but the anchored MCP tool. Nothing here persists, so the anchor carries no
    /// write-side question — this is a read that happens to score what it read.</para>
    ///
    /// <para>#3691: the anomaly detector runs here too, over the same context, between collection and
    /// scoring — the order the full pass uses. Until then this read was collector + scorer only, so the
    /// fact set <c>get_analysis_facts</c> sold as "every observation the engine sees" never held an
    /// <c>ANOMALY_*</c> fact, and the gate metadata the detector stamps (deviation_sigma, fire_threshold,
    /// sample_count, baseline_confidence, threshold_lineage) was reachable only through a finding that
    /// had already survived the severity floor; an anomaly that fired and scored under 0.5 was invisible
    /// everywhere. The cost is the detector's baseline reads (one per baselined metric) on top of the
    /// collector's, which is what the full pass already pays for the same answer. The detector is gated
    /// the way the pass gates it: it does not run over a window the collector never observed, because a
    /// deviation is measured against the window's own rate, and an unobserved window has none — the
    /// callers' unobserved envelope keeps describing exactly the point-in-time facts it names.</para>
    /// </summary>
    public async Task<(List<Fact> Facts, WindowCoverage? Coverage, CollectionCaveatState Caveats)> CollectAndScoreFactsAsync(
        int serverId, string serverName, int hoursBack = 4, DateTime? asOfUtc = null)
    {
        var timeRangeEnd = asOfUtc ?? DateTime.UtcNow;
        var timeRangeStart = timeRangeEnd.AddHours(-hoursBack);

        var context = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = timeRangeStart,
            TimeRangeEnd = timeRangeEnd,
            AsOfUtc = asOfUtc
        };

        try
        {
            var facts = await _collector.CollectFactsAsync(context);
            if (context.ObservedDurationMs > 0)
            {
                /* Same detector, same context, same position as the pass (#3691). A collector that
                   observed the window but emitted no fact does not short-circuit this: the detector reads
                   the store on its own and its facts are part of what this read shows. */
                var anomalies = await _anomalyDetector.DetectAnomaliesAsync(context);
                facts.AddRange(anomalies);
            }
            if (facts.Count == 0) return (facts, context.Coverage, CollectionCaveatState.From(context));
            _scorer.ScoreAll(facts);
            return (facts, context.Coverage, CollectionCaveatState.From(context));
        }
        catch (Exception ex)
        {
            AppLogger.Error("AnalysisService", $"Fact collection or anomaly detection failed for {serverName}: {ex.Message}");
            return ([], null, CollectionCaveatState.From(context));
        }
    }

    /// <summary>
    /// Compares analysis of two time periods, returning facts from both for comparison, each with its
    /// window's observed coverage (#3538 A2) so the caller can say when one side was only partly
    /// collected — the case the empty-window caveats never reached, where a half-collected window
    /// produces confident numbers with nothing to flag them.
    ///
    /// <para>#3538 A3: also returns the stored per-server dispersion for the baselined metrics some
    /// compared key is measured in (<see cref="ComparisonBanding.DispersionMetricsFor"/>), keyed by metric
    /// name, so <c>compare_analysis</c> can band a CPU or read-latency delta in the server's own robust
    /// sigma instead of on a flat severity dead-band. The bucket is the comparison window's START hour
    /// × day-of-week — the same coordinate the anomaly detectors read for a pass over that window
    /// (<c>AnomalyDetector</c> passes <c>context.TimeRangeStart</c>), so the default same-hour-yesterday
    /// call reuses the pass's cached buckets and an anchored one recomputes them the way an anchored
    /// <c>analyze_server</c> does. The lookups are fenced separately from collection: a baseline read
    /// that fails must not cost the caller the comparison it was only meant to refine, so it degrades
    /// to an empty map and every key takes the absolute rule — the never-blind fallback the anomaly
    /// gate follows.</para>
    /// </summary>
    public async Task<(List<Fact> BaselineFacts, List<Fact> ComparisonFacts, WindowCoverage? BaselineCoverage, WindowCoverage? ComparisonCoverage, IReadOnlyDictionary<string, BaselineBucket> Dispersion)> ComparePeriodsAsync(
        int serverId, string serverName,
        DateTime baselineStart, DateTime baselineEnd,
        DateTime comparisonStart, DateTime comparisonEnd)
    {
        var baselineContext = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = baselineStart,
            TimeRangeEnd = baselineEnd
        };

        var comparisonContext = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = comparisonStart,
            TimeRangeEnd = comparisonEnd
        };

        try
        {
            var baselineFacts = await _collector.CollectFactsAsync(baselineContext);
            var comparisonFacts = await _collector.CollectFactsAsync(comparisonContext);

            _scorer.ScoreAll(baselineFacts);
            _scorer.ScoreAll(comparisonFacts);

            var dispersion = await LookUpDispersionAsync(serverId, serverName, baselineFacts, comparisonFacts, comparisonStart);

            return (baselineFacts, comparisonFacts, baselineContext.Coverage, comparisonContext.Coverage, dispersion);
        }
        catch (Exception ex)
        {
            AppLogger.Error("AnalysisService", $"Period comparison failed for {serverName}: {ex.Message}");
            return ([], [], null, null, new Dictionary<string, BaselineBucket>());
        }
    }

    /// <summary>
    /// The baseline buckets <see cref="ComparePeriodsAsync"/> hands to the comparison, one per metric some
    /// compared key is measured in. Its own try: see the summary above for why a failed baseline read
    /// degrades to "no dispersion" rather than failing the comparison.
    /// </summary>
    private async Task<IReadOnlyDictionary<string, BaselineBucket>> LookUpDispersionAsync(
        int serverId, string serverName, List<Fact> baselineFacts, List<Fact> comparisonFacts, DateTime comparisonStart)
    {
        var dispersion = new Dictionary<string, BaselineBucket>(StringComparer.Ordinal);
        try
        {
            foreach (var metric in ComparisonBanding.DispersionMetricsFor(baselineFacts, comparisonFacts))
                dispersion[metric] = await _baselineProvider.GetBaselineAsync(serverId, metric, comparisonStart);
        }
        catch (Exception ex)
        {
            AppLogger.Warn("AnalysisService", $"Baseline dispersion lookup failed for {serverName}; compare_analysis bands every key by the absolute rule: {ex.Message}");
            dispersion.Clear();
        }
        return dispersion;
    }

    /// <summary>
    /// Gets the latest findings for a server without running a new analysis.
    /// </summary>
    public async Task<List<AnalysisFinding>> GetLatestFindingsAsync(int serverId)
    {
        return await _findingStore.GetLatestFindingsAsync(serverId);
    }

    /// <summary>
    /// Gets recent findings for a server within the given time range. The MCP findings read
    /// passes <see cref="FindingOccurrences.WindowCoveringLimit"/> so its occurrence stats cover
    /// the whole window; the store's default 100 stays for everyone else.
    ///
    /// <para>#2506: <paramref name="asOfUtc"/> anchors the window's END. This one is a pure read of
    /// rows the SCHEDULED passes already wrote, so anchoring it asks "what did analysis say about this
    /// server at the time" — the only way to see findings the retention sweep has not yet reached but
    /// the default 24-hour window has scrolled past.</para>
    /// </summary>
    public async Task<List<AnalysisFinding>> GetRecentFindingsAsync(
        int serverId, int hoursBack = 24, int limit = 100, DateTime? asOfUtc = null)
    {
        return await _findingStore.GetRecentFindingsAsync(serverId, hoursBack, limit, asOfUtc);
    }

    /// <summary>
    /// Mutes a finding pattern so it won't appear in future runs and returns what the write did — a new row or
    /// an existing one (#3653 A15/A16; the store throws on failure, so <see cref="MuteRegistration.Failed"/> is
    /// the Darling twin's arm). An empty <see cref="AnalysisFinding.StoryPath"/> means the caller knows only the
    /// hash (the MCP entry point does) and the store resolves the path from the retained findings; see
    /// <see cref="FindingStore.MuteStoryAsync"/> for the placeholder it writes when none carries the hash.
    /// </summary>
    public async Task<MuteWriteResult> MuteFindingAsync(AnalysisFinding finding, string? reason = null)
    {
        return await _findingStore.MuteStoryAsync(
            finding.ServerId,
            finding.StoryPathHash,
            string.IsNullOrEmpty(finding.StoryPath) ? null : finding.StoryPath,
            reason);
    }

    /// <summary>
    /// How many stored findings carry <paramref name="storyPathHash"/> for one server (or every server when
    /// <paramref name="serverId"/> is null / the all-servers sentinel 0) — the MCP mute verb's <c>matched_now</c>
    /// disclosure (#3541 A14). A pass-through to <see cref="FindingStore.CountStoredFindingsAsync"/>; see its
    /// note for why this is reported beside the mute and not used to refuse it. Matches the Darling twin.
    /// </summary>
    public Task<long> CountStoredFindingsAsync(int? serverId, string storyPathHash, CancellationToken cancellationToken = default) =>
        _findingStore.CountStoredFindingsAsync(serverId, storyPathHash, cancellationToken);

    /// <summary>
    /// Cleans up old findings beyond the retention period. Defaults to the shared horizon
    /// (<see cref="AnalysisRetentionDefaults.FindingsRetentionDays"/>) so this wrapper and the
    /// store method it forwards to cannot disagree about the window when a caller names none —
    /// the scheduler passes it explicitly, and the tests pass 0 to purge everything.
    /// </summary>
    public async Task CleanupAsync(int retentionDays = AnalysisRetentionDefaults.FindingsRetentionDays)
    {
        await _findingStore.CleanupOldFindingsAsync(retentionDays);
    }

    /// <summary>
    /// The <c>server_config</c> snapshots the config-change attribution diffs (#3653 A10, Q2): every capture
    /// inside the pass window PLUS the last capture before it — the diff baseline, without which a change
    /// first seen on the first in-window capture is invisible (the WINDOWING rule every history reader
    /// follows; see <c>ConfigChangeDiff</c>). Reads the <c>v_server_config</c> view like the Lite fact
    /// collector does. <c>$1</c> server_id, <c>$2</c> window start, <c>$3</c> window end. The Darling twin
    /// (<c>DarlingAnalysisService.ServerConfigSnapshotsForAttributionSql</c>) is the same statement over the
    /// collector table. The subquery's COALESCE covers a server with no capture before the window: the bound
    /// then becomes the window start and the in-window captures are read alone, which the diff answers with
    /// "no change" (it needs two captures) rather than a fabricated one.
    /// </summary>
    internal const string ServerConfigSnapshotsForAttributionSql = @"
SELECT capture_time, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced
FROM v_server_config
WHERE server_id = $1
AND   capture_time <= $3
AND   capture_time >= COALESCE(
        (SELECT MAX(capture_time) FROM v_server_config WHERE server_id = $1 AND capture_time < $2),
        $2)
ORDER BY configuration_name, capture_time";

    /// <summary>
    /// The <c>database_config</c> snapshots the attribution diffs (#3653 A10, slice two): the same window
    /// rule as <see cref="ServerConfigSnapshotsForAttributionSql"/> over the WIDE <c>sys.databases</c> row,
    /// the 27 option columns CAST to VARCHAR in <c>ConfigChangeDiff.DatabaseConfigChangeSettingNames</c>
    /// ORDER, which the diff walks positionally (the Configuration Changes tab's projection,
    /// <c>LocalDataService.ReadDatabaseConfigSnapshotsAsync</c>, bounded below as well as above;
    /// <c>ConfigChangeAttributionTests</c> pins the order against the diff's list). Reads the
    /// <c>v_database_config</c> view like the Lite fact collector does. <c>$1</c> server_id, <c>$2</c> window
    /// start, <c>$3</c> window end. The Darling twin is <c>DarlingAnalysisService.DatabaseConfigSnapshotsForAttributionSql</c>.
    /// </summary>
    internal const string DatabaseConfigSnapshotsForAttributionSql = @"
SELECT capture_time, database_name,
       CAST(state_desc AS VARCHAR), CAST(compatibility_level AS VARCHAR), CAST(collation_name AS VARCHAR),
       CAST(recovery_model AS VARCHAR), CAST(is_read_only AS VARCHAR), CAST(is_auto_close_on AS VARCHAR),
       CAST(is_auto_shrink_on AS VARCHAR), CAST(is_auto_create_stats_on AS VARCHAR),
       CAST(is_auto_update_stats_on AS VARCHAR), CAST(is_auto_update_stats_async_on AS VARCHAR),
       CAST(is_read_committed_snapshot_on AS VARCHAR), CAST(snapshot_isolation_state AS VARCHAR),
       CAST(is_parameterization_forced AS VARCHAR), CAST(is_query_store_on AS VARCHAR),
       CAST(is_encrypted AS VARCHAR), CAST(is_trustworthy_on AS VARCHAR), CAST(is_db_chaining_on AS VARCHAR),
       CAST(is_broker_enabled AS VARCHAR), CAST(is_cdc_enabled AS VARCHAR),
       CAST(is_mixed_page_allocation_on AS VARCHAR), CAST(log_reuse_wait_desc AS VARCHAR),
       CAST(page_verify_option AS VARCHAR), CAST(target_recovery_time_seconds AS VARCHAR),
       CAST(delayed_durability AS VARCHAR), CAST(is_accelerated_database_recovery_on AS VARCHAR),
       CAST(is_memory_optimized_enabled AS VARCHAR), CAST(is_optimized_locking_on AS VARCHAR)
FROM v_database_config
WHERE server_id = $1
AND   capture_time <= $3
AND   capture_time >= COALESCE(
        (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1 AND capture_time < $2),
        $2)
ORDER BY database_name, capture_time";

    /// <summary>
    /// The <c>trace_flags</c> snapshots the attribution set-diffs (#3653 A10, slice two): the same window rule
    /// over the per-enabled-flag rows. The baseline subquery finds the last capture WITH rows before the window
    /// — the collector writes none when no flag is enabled, so a zero-flag capture is invisible here and to the
    /// diff alike (the blind spot <c>ConfigChangeAttribution</c>'s remarks state). <c>$1</c> server_id, <c>$2</c>
    /// window start, <c>$3</c> window end. The Darling twin is <c>DarlingAnalysisService.TraceFlagSnapshotsForAttributionSql</c>.
    /// </summary>
    internal const string TraceFlagSnapshotsForAttributionSql = @"
SELECT capture_time, trace_flag, status, is_global, is_session
FROM v_trace_flags
WHERE server_id = $1
AND   capture_time <= $3
AND   capture_time >= COALESCE(
        (SELECT MAX(capture_time) FROM v_trace_flags WHERE server_id = $1 AND capture_time < $2),
        $2)
ORDER BY capture_time, trace_flag";

    /// <summary>
    /// This server's collected UTC offset for the trace-anchor read (#3740) — the same statement
    /// <c>LocalDataService.GetServerUtcOffsetMinutesAsync</c> runs, inlined because the analysis pass holds a
    /// DuckDB connection and not a <c>LocalDataService</c>. Skips NULL offsets rather than taking the newest
    /// row blindly: the column arrived in schema v42 and a store migrated from earlier holds pre-v42
    /// snapshots that predate it. <c>$1</c> server_id. No row means no offset yet, and the caller treats
    /// local as UTC — <c>McpServerLocalWindow</c>'s decision, for the same reason (a server with no offset
    /// almost always has no server-local rows either).
    /// </summary>
    internal const string ServerUtcOffsetForAttributionSql = @"
SELECT utc_offset_minutes
FROM v_server_properties
WHERE server_id = $1
AND   utc_offset_minutes IS NOT NULL
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>
    /// The default trace's sp_configure lines for the attribution's trace anchor (#3740): every stored
    /// <c>ErrorLog</c> row carrying msg 15457 (<see cref="ConfigChangeAttribution.ReconfigureMessageNumber"/>)
    /// whose event time falls in <c>($2, $3]</c>. Selected on <c>error_number</c>, not on the text — the number
    /// is populated on the row and does not change with the instance's language; the attribution parses the
    /// text afterwards. Reads the <c>v_default_trace_events</c> archive view (hot UNION parquet) like the
    /// System Events read does, so a line that has already aged into parquet still anchors.
    ///
    /// <para><b>The stored <c>event_time</c> is the monitored server's LOCAL wall clock</b> —
    /// <c>fn_trace_gettable</c>'s <c>StartTime</c>, stored raw — while the capture times this span is made of
    /// are naive UTC. Lite de-skews in C# rather than SQL, exactly as <c>LocalDataService.GetDefaultTraceEventsAsync</c>
    /// does: the caller shifts BOTH bounds into the server's frame by the collected offset before binding
    /// them, and subtracts the same offset from each returned row, so the bounds and the values can never
    /// disagree about whose clock they are in (<c>ServerLocalReadFrameDisciplineTests</c> pins the row
    /// de-skew). The Darling twin, <c>DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql</c>, spells
    /// the same de-skew in SQL. One offset covers the span, so a span straddling a DST transition is off by an
    /// hour on its far side — the single-snapshot approximation every reader of this column makes.</para>
    /// </summary>
    internal const string ReconfigureTraceLinesForAttributionSql = @"
SELECT event_time, text_data
FROM v_default_trace_events
WHERE server_id = $1
AND   error_number = 15457
AND   event_time > $2
AND   event_time <= $3
ORDER BY event_time";

    /// <summary>
    /// Step 2.5 of the pass (#3653 A10, Q2 and slice two): if a configuration value was first observed
    /// changed inside the pass window — a <c>sys.configurations</c> setting, a <c>sys.databases</c> option on
    /// one database, or a trace flag — run <see cref="ComparePeriodsAsync"/> over the four hours before the
    /// change and the (clamped) four hours after it, band the result with
    /// <see cref="ComparisonBanding.Compare"/> exactly as <c>compare_analysis</c> would, and append ONE
    /// <c>CONFIG_CHANGED</c> fact carrying the verdict. <see cref="ConfigChangeAttribution"/> holds the
    /// design — why one fact, why Information, which clock "the change" is on, why changes of different
    /// families observed at one connect are one event; this method is the store reads and the wiring, twin to
    /// <c>DarlingAnalysisService.AttributeConfigChangesAsync</c>.
    ///
    /// <para><b>Three families, three reads, one event list.</b> Each family's snapshots are read with the
    /// same window rule, diffed through its <c>ConfigChangeDiff</c> arm, mapped onto
    /// <see cref="ConfigChangeAttribution.SettingChange"/> in one LINQ line per family (the database family
    /// dropping <c>log_reuse_wait_desc</c> through <see cref="ConfigChangeAttribution.IsAttributableDatabaseSetting"/>
    /// — a status, not an option), and grouped into events against ITS OWN capture times (the three collectors
    /// stamp their own <c>capture_time</c>, seconds apart at one connect, so a shared list would put a
    /// same-connect sibling capture where the previous connect belongs).
    /// <see cref="ConfigChangeAttribution.MergeSameConnectEvents"/> then folds same-connect events across
    /// families, and the most recent event is the card's subject.</para>
    ///
    /// <para><b>Two clocks, in order (#3740).</b> The snapshot diff says WHAT changed and when it was first
    /// observed; the default trace's sp_configure line, when the store holds one for the same option in the
    /// span between the two captures, says WHEN it changed. <see cref="ResolveTraceAnchorAsync"/> reads and
    /// joins those lines; the compare and the fact anchor on the trace's time when the join resolves and on
    /// the observation otherwise, and the fact says which. The trace read is the server family's alone — the
    /// other two have no trace subject the store holds (measured; see the attribution's remarks) — so it runs
    /// only when the subject event holds a server setting. It has its own catch inside this method's: a fault
    /// there costs the pass the trace anchor, not the card.</para>
    ///
    /// <para><b>Cost.</b> A compare is two full fact collections (the collector's thirty-one reads, twice)
    /// plus the baseline lookups, inside the pass budget. It runs only when a change is in-window — the
    /// snapshot read alone is the steady-state cost, one small query — and at most once per pass (the most
    /// recent event). The compare's contexts are not wired to the pass token (<see cref="ComparePeriodsAsync"/>'s
    /// public shape is <c>compare_analysis</c>'s); the pass's boundary checks bracket the call.</para>
    ///
    /// <para><b>Its own catch.</b> Like every collector read: a store fault here degrades to "no card" with
    /// one Warning naming why, and an abandonment propagates to the pass's single classified line (#2443).
    /// <see cref="ComparePeriodsAsync"/> swallows its own faults and returns empties with null coverage;
    /// that shape is passed through as <c>compare_unavailable</c> on the fact, so a change is still recorded
    /// when its compare could not run.</para>
    /// </summary>
    private async Task AttributeConfigChangesAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            var snapshots = new List<ConfigChangeDiff.ServerConfigSnapshot>();
            using (var readLock = _duckDb.AcquireReadLock(context.CancellationToken))
            using (var connection = _duckDb.CreateConnection())
            {
                await connection.OpenAsync(context.CancellationToken);
                using var cmd = connection.CreateCommand();
                cmd.CommandText = ServerConfigSnapshotsForAttributionSql;
                cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
                cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
                cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    snapshots.Add(new ConfigChangeDiff.ServerConfigSnapshot(
                        reader.GetDateTime(0),
                        reader.IsDBNull(1) ? "" : reader.GetString(1),
                        reader.IsDBNull(2) ? null : Convert.ToInt64(reader.GetValue(2)),
                        reader.IsDBNull(3) ? null : Convert.ToInt64(reader.GetValue(3)),
                        reader.IsDBNull(4) ? null : reader.GetBoolean(4),
                        reader.IsDBNull(5) ? null : reader.GetBoolean(5)));
                }
            }

            var databaseSnapshots = await ReadDatabaseConfigSnapshotsAsync(context);
            var traceFlagSnapshots = await ReadTraceFlagSnapshotsAsync(context);

            var serverEvents = ConfigChangeAttribution.GroupIntoEvents(
                ConfigChangeDiff.DiffServerConfigChanges(snapshots, context.TimeRangeStart, context.TimeRangeEnd)
                    .Select(c => (c.ChangeTime, new ConfigChangeAttribution.SettingChange(
                        c.ConfigurationName, c.OldValueConfigured, c.NewValueConfigured, c.OldValueInUse, c.NewValueInUse, c.RequiresRestart))),
                snapshots.Select(s => s.CaptureTime));
            var databaseEvents = ConfigChangeAttribution.GroupIntoEvents(
                ConfigChangeDiff.DiffDatabaseConfigChanges(databaseSnapshots, context.TimeRangeStart, context.TimeRangeEnd)
                    .Where(c => ConfigChangeAttribution.IsAttributableDatabaseSetting(c.SettingName))
                    .Select(c => (c.ChangeTime, ConfigChangeAttribution.SettingChange.ForDatabase(c.DatabaseName, c.SettingName, c.OldValue, c.NewValue))),
                databaseSnapshots.Select(s => s.CaptureTime));
            var traceFlagEvents = ConfigChangeAttribution.GroupIntoEvents(
                ConfigChangeDiff.DiffTraceFlagChanges(traceFlagSnapshots, context.TimeRangeStart, context.TimeRangeEnd)
                    .Select(c => (c.ChangeTime, ConfigChangeAttribution.SettingChange.ForTraceFlag(c.TraceFlag, c.ChangeType, c.Scope, c.PreviousStatus, c.NewStatus))),
                traceFlagSnapshots.Select(s => s.CaptureTime));
            var events = ConfigChangeAttribution.MergeSameConnectEvents(serverEvents.Concat(databaseEvents).Concat(traceFlagEvents));
            if (events.Count == 0)
                return;

            var latest = events[0];
            var anchor = (latest.Families & ConfigChangeAttribution.ChangeFamily.ServerConfig) != 0
                ? await ResolveTraceAnchorAsync(context, latest)
                : null;
            var anchorTime = ConfigChangeAttribution.AnchorTime(latest, anchor);
            var windows = ConfigChangeAttribution.WindowsFor(anchorTime, context.TimeRangeEnd);

            context.CancellationToken.ThrowIfCancellationRequested();
            var (before, after, beforeCoverage, afterCoverage, dispersion) = await ComparePeriodsAsync(
                context.ServerId, context.ServerName,
                windows.BeforeStart, windows.BeforeEnd,
                windows.AfterStart, windows.AfterEnd);

            /* Both coverages null is ComparePeriodsAsync's own catch (collection threw); an empty compare
               over OBSERVED windows is the "nothing moved" answer and is banded like any other. */
            var compare = beforeCoverage is null && afterCoverage is null
                ? null
                : ComparisonBanding.Compare(before, after, dispersion, ConfigChangeAttribution.CoverageCaveatFor(beforeCoverage, afterCoverage));

            facts.Add(ConfigChangeAttribution.BuildFact(
                context.ServerId, latest, events.Count - 1, windows, compare, beforeCoverage, afterCoverage, anchor));

            AppLogger.Info("AnalysisService",
                $"Configuration change attributed for {context.ServerName} ({latest.Families}): {string.Join(ConfigChangeAttribution.SettingSeparator, latest.Changes.Select(c => c.Name))} " +
                $"{(anchor is null ? "first observed at" : "changed at")} {anchorTime:u} ({(anchor is null ? "configuration snapshot" : "default trace, msg 15457")}), " +
                $"compare over ±{ConfigChangeAttribution.CompareWindowHours} h ({windows.AfterHoursObserved:0.#} h after so far) — " +
                $"{compare?.Worse ?? 0} worse, {compare?.Better ?? 0} better, {compare?.Stable ?? 0} stable{(compare is null ? " (compare unavailable this pass)" : string.Empty)}");
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Warn("AnalysisService",
                $"Configuration-change attribution failed for {context.ServerName}; the pass continues without the CONFIG_CHANGED card: {ex.Message}");
        }
    }

    /// <summary>
    /// The database family's half of step 2.5's snapshot read (slice two): <see cref="DatabaseConfigSnapshotsForAttributionSql"/>
    /// into the diff's WIDE record, the 27 option columns read positionally after <c>capture_time</c> and
    /// <c>database_name</c> — the same walk <c>LocalDataService.ReadDatabaseConfigSnapshotsAsync</c> makes for
    /// the Configuration Changes tab. A NULL column is a null value the diff compares ordinally against the
    /// other capture's. No catch of its own: a fault here is the caller's, and costs the pass the whole card
    /// rather than a family of it, because a card that silently dropped one family would read as "nothing
    /// changed there".
    /// </summary>
    private async Task<List<ConfigChangeDiff.DatabaseConfigSnapshot>> ReadDatabaseConfigSnapshotsAsync(AnalysisContext context)
    {
        var rows = new List<ConfigChangeDiff.DatabaseConfigSnapshot>();
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = DatabaseConfigSnapshotsForAttributionSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            var values = new string?[ConfigChangeDiff.DatabaseConfigChangeSettingNames.Count];
            for (var i = 0; i < values.Length; i++)
            {
                var ordinal = i + 2; /* capture_time, database_name precede the settings */
                values[i] = reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
            }

            rows.Add(new ConfigChangeDiff.DatabaseConfigSnapshot(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                values));
        }

        return rows;
    }

    /// <summary>
    /// The trace-flag family's half of step 2.5's snapshot read (slice two): <see cref="TraceFlagSnapshotsForAttributionSql"/>
    /// into the diff's per-flag record, the same mapping <c>LocalDataService.ReadTraceFlagSnapshotsAsync</c>
    /// makes. No catch of its own, for the reason <see cref="ReadDatabaseConfigSnapshotsAsync"/> states.
    /// </summary>
    private async Task<List<ConfigChangeDiff.TraceFlagSnapshot>> ReadTraceFlagSnapshotsAsync(AnalysisContext context)
    {
        var rows = new List<ConfigChangeDiff.TraceFlagSnapshot>();
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = TraceFlagSnapshotsForAttributionSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            rows.Add(new ConfigChangeDiff.TraceFlagSnapshot(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : reader.GetBoolean(2),
                reader.IsDBNull(3) ? null : reader.GetBoolean(3),
                reader.IsDBNull(4) ? null : reader.GetBoolean(4)));
        }

        return rows;
    }

    /// <summary>
    /// The trace half of step 2.5 (#3740): the stored sp_configure lines in the span between the two config
    /// captures of <paramref name="change"/>, joined to its settings by
    /// <see cref="ConfigChangeAttribution.ResolveServerConfigTraceAnchor"/>. Null — the observation anchor —
    /// when no line matches (trace off, Azure SQL Database, the row aged out before collection, a
    /// non-English message) AND when the read itself faults: the anchor is an improvement on a card that is
    /// already true without it, so a store fault here is logged at Warning and costs only the anchor. An
    /// abandonment still propagates to the caller's classified line (#2443).
    ///
    /// <para>The offset is read first and used twice — to shift both span bounds into the server's local
    /// frame and to de-skew each returned row back to UTC — off the ONE resolved value, so the bounds and
    /// the values cannot disagree about which clock they are in (the discipline
    /// <c>LocalDataService.GetDefaultTraceEventsAsync</c> states for its own parameter).</para>
    /// </summary>
    private async Task<ConfigChangeAttribution.TraceAnchor?> ResolveTraceAnchorAsync(
        AnalysisContext context, ConfigChangeAttribution.ChangeEvent change)
    {
        try
        {
            var lines = new List<ConfigChangeAttribution.TraceLine>();
            using (var readLock = _duckDb.AcquireReadLock(context.CancellationToken))
            using (var connection = _duckDb.CreateConnection())
            {
                await connection.OpenAsync(context.CancellationToken);

                var offset = 0;
                using (var offsetCmd = connection.CreateCommand())
                {
                    offsetCmd.CommandText = ServerUtcOffsetForAttributionSql;
                    offsetCmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
                    var scalar = await offsetCmd.ExecuteScalarAsync(context.CancellationToken);
                    if (scalar is not null and not DBNull)
                        offset = Convert.ToInt32(scalar);
                }

                using var cmd = connection.CreateCommand();
                cmd.CommandText = ReconfigureTraceLinesForAttributionSql;
                cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
                /* Server-local bounds: the stored event_time is the server's wall clock, so the UTC span is
                   shifted INTO that frame by the collected offset (local = UTC + offset). */
                cmd.Parameters.Add(new DuckDBParameter { Value = change.PreviousCaptureTime.AddMinutes(offset) });
                cmd.Parameters.Add(new DuckDBParameter { Value = change.ChangeTime.AddMinutes(offset) });

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    if (reader.IsDBNull(0))
                        continue;
                    /* De-skew server-local StartTime -> naive-UTC, the same subtraction the System Events read makes. */
                    var eventTimeUtc = reader.GetDateTime(0).AddMinutes(-offset);
                    lines.Add(new ConfigChangeAttribution.TraceLine(
                        eventTimeUtc,
                        reader.IsDBNull(1) ? null : reader.GetString(1)));
                }
            }

            return ConfigChangeAttribution.ResolveServerConfigTraceAnchor(change, lines);
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Warn("AnalysisService",
                $"Default-trace anchor lookup failed for {context.ServerName}; the CONFIG_CHANGED card keeps its observation anchor: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Returns the total span of collected data for a server (no time range filter).
    /// This answers "has this server been monitored long enough?" — separate from
    /// the analysis window. A server with 100 hours of total history can safely
    /// be analyzed over a 4-hour window without dilution.
    /// </summary>
    /* Internal for AnalysisDataSpanTests (#1809): the span must survive an archive/reset, which is
       only observable with a real DuckDB + parquet fixture. */
    internal async Task<double> GetTotalDataSpanHoursAsync(int serverId, CancellationToken cancellationToken = default)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(cancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT EXTRACT(EPOCH FROM (MAX(collection_time) - MIN(collection_time))) / 3600.0
FROM v_wait_stats
WHERE server_id = $1";

            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result == null || result is DBNull)
                return 0;

            return Convert.ToDouble(result);
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, cancellationToken))
        {
            /* A probe failure reads as "no data yet" — EXCEPT an abandonment, which must not be
               allowed to masquerade as a 0-hour history (#2443). That would turn a cancelled pass
               into an insufficient-data SKIP, which is a different and far calmer-looking answer
               than the one the caller is about to log. */
            return 0;
        }
    }

    /// <summary>
    /// The read-time forcing/automatic-plan-correction state for a set of findings' force-plan targets
    /// (#3652), through the analysis service's own DuckDB handle because the MCP analysis tools hold this
    /// service and no <c>LocalDataService</c>. One statement for every target across the findings
    /// (<see cref="ForcePlanTargetStateReader"/>), keyed for <c>FactRemediation.BuildStructuredRemediation</c>.
    /// Returns the dictionary and a null reason on success; on ANY failure returns null and the reason, so
    /// the caller can put "state unavailable: why" on every target rather than fail the read or — worse —
    /// let the verdict read as eligible by silence. An abandonment is not swallowed (#2443).
    /// </summary>
    public async Task<(IReadOnlyDictionary<ForcePlanTargetKey, ForcePlanTargetState>? States, string? UnavailableReason)> TryGetForcePlanTargetStatesAsync(
        int serverId, IEnumerable<AnalysisFinding> findings, CancellationToken cancellationToken = default)
    {
        var targets = new List<ForcePlanTarget>();
        foreach (var finding in findings)
        {
            if (finding.Remediation?.Targets is { Count: > 0 } ts)
                targets.AddRange(ts);
        }

        if (targets.Count == 0)
            return (new Dictionary<ForcePlanTargetKey, ForcePlanTargetState>(ForcePlanTargetKey.Comparer), null);

        try
        {
            using var readLock = _duckDb.AcquireReadLock(cancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);
            using var command = connection.CreateCommand();
            return (await ForcePlanTargetStateReader.ReadAsync(command, serverId, targets, DateTime.UtcNow, cancellationToken), null);
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, cancellationToken))
        {
            return (null, $"the forcing and automatic-plan-correction state read failed ({ex.GetType().Name}: {ex.Message}); eligible reflects only the finding's own evidence. Check get_plan_corrections and sys.query_store_plan before forcing.");
        }
    }
}

/// <summary>
/// Event args for when an analysis run completes.
/// </summary>
public class AnalysisCompletedEventArgs : EventArgs
{
    public int ServerId { get; set; }
    public string ServerName { get; set; } = string.Empty;
    public List<AnalysisFinding> Findings { get; set; } = [];
    public DateTime AnalysisTime { get; set; }
}
