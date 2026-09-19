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
            // dropped, only the incident tag is reconciled.
            AnomalyIncidentReconciler.Reconcile(stories);

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
    /// Runs the collect + score pipeline without graph traversal.
    /// Returns raw scored facts with amplifier details for direct inspection, together with the
    /// window's observed coverage (#3538 A2) — returned rather than parked on a property because this
    /// path has no <see cref="IsAnalyzing"/> guard and two on-demand callers can overlap; a shared
    /// property would let one read the other's window. Coverage is null only when collection threw.
    ///
    /// <para>#2506: <paramref name="asOfUtc"/> anchors the END of the window; null is "now", which is
    /// every caller but the anchored MCP tool. Nothing here persists, so the anchor carries no
    /// write-side question — this is a read that happens to score what it read.</para>
    /// </summary>
    public async Task<(List<Fact> Facts, WindowCoverage? Coverage)> CollectAndScoreFactsAsync(
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
            if (facts.Count == 0) return (facts, context.Coverage);
            _scorer.ScoreAll(facts);
            return (facts, context.Coverage);
        }
        catch (Exception ex)
        {
            AppLogger.Error("AnalysisService", $"Fact collection failed for {serverName}: {ex.Message}");
            return ([], null);
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
