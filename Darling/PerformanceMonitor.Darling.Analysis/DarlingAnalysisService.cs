/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Orchestrates the full analysis pipeline: collect → score → traverse → persist.
/// Can be run on-demand or on a timer. Each run analyzes a single server's data
/// for a given time window and persists the findings.
///
/// <para>
/// Port of Lite's AnalysisService (Phase-5 analysis slice AN3) over the PG pieces — Lite's
/// pipeline ORDER (24h-data-span gate → CollectFacts → DetectAnomalies → ScoreAll →
/// BuildStories → PopulateStoryText → ClusterIntoIncidents + StampClusters → persist →
/// enrich) with the DASHBOARD twin's richer SAVE phase adopted (recommendations rebuild
/// D2/P2): mute-filter WITHOUT inserting → enrich the survivors → build + attach each
/// finding's RemediationAction from the drill-down → insert with
/// <c>remediation_action_json</c> persisted. The V4 schema carries that column and
/// <see cref="PgFindingStore"/> implements the two-phase surface precisely for this.
/// </para>
///
/// <para>
/// Clock semantics are LITE's, not the Dashboard's: the analysis window is built from host
/// UTC and the context times are used as-is (no server-clock probe, no offset math) because
/// Darling's collectors stamp <c>collection_time</c> with the service host's
/// <c>DateTime.UtcNow</c> exactly like Lite's — the windowed reads and the collected rows
/// share one clock already. <c>ServerUtcOffset</c> stays <see cref="TimeSpan.Zero"/>, which
/// <see cref="PgFindingStore.FilterMutedFindingsAsync"/> treats as an identity conversion.
/// </para>
///
/// <para>
/// /* PG port deviations from the twins, the PgFactCollector conventions: the DuckDB
/// initializer / SQL connection string becomes an <see cref="NpgsqlDataSource"/>; Lite's
/// static AppLogger / Dashboard's static Logger become an injected optional
/// <see cref="ILogger"/>; the data-span SQL moves to a <c>public const</c> for the ungated
/// dialect pins (text is Lite's verbatim — <c>EXTRACT(EPOCH FROM ...)</c> is shared-dialect
/// and runs unchanged on Postgres, reading the raw <c>wait_stats</c> table with Lite's
/// multi-server <c>server_id = $1</c> filter, which the single-server Dashboard twin
/// drops). Dashboard's <c>GetServerClockAsync</c>/<c>GetServerLocalNowAsync</c> are
/// deliberately NOT ported — they exist only for its server-local collection clock. */
/// </para>
/// </summary>
public sealed class DarlingAnalysisService
{
    private readonly NpgsqlDataSource _postgres;
    private readonly PgFindingStore _findingStore;
    private readonly PgFactCollector _collector;
    private readonly FactScorer _scorer;
    private readonly RelationshipGraph _graph;
    private readonly InferenceEngine _engine;
    private readonly PgDrillDownCollector _drillDown;
    private readonly PgAnomalyDetector _anomalyDetector;
    private readonly PgBaselineProvider _baselineProvider;
    private readonly ILogger? _logger;

    /// <summary>
    /// Minimum hours of collected data required before analysis will run.
    /// Short collection windows distort fraction-of-period calculations —
    /// 5 seconds of THREADPOOL looks alarming in a 16-minute window.
    /// 24 hours has been validated empirically as sufficient.
    /// </summary>
    internal double MinimumDataHours { get; set; } = 24;

    /// <summary>
    /// Raised after each analysis run completes, providing the findings — the twins' UI
    /// hook, kept so the surface stays twin-shaped (the worker awaits AnalyzeAsync's return
    /// directly, like Lite's scheduler does).
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
    /// How the last pass ended EARLY, or null when it ran through (#2430). Set inside the pass's own
    /// catch, so <see cref="AnalysisAbandonKind.None"/> here means a genuine fault: the pass reached the
    /// catch and the classifier said it was not an abandonment.
    ///
    /// <para>Carried out to the caller because the caller cannot re-derive it. "No findings and the
    /// budget token has fired" is true of a fault as well as of a timeout, and inferring a timeout from
    /// it buries the fault's ERROR under a Warning that says the pass merely ran out of time. The pass
    /// has already classified this once and logged the one line for it; this is how the scheduler reads
    /// that answer instead of guessing at a second one.</para>
    /// </summary>
    public AnalysisAbandonKind? EndedEarlyAs { get; private set; }

    public DarlingAnalysisService(NpgsqlDataSource postgres, IPlanFetcher? planFetcher = null, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
        _findingStore = new PgFindingStore(postgres, logger);
        _collector = new PgFactCollector(postgres, logger);
        _scorer = new FactScorer();
        _graph = new RelationshipGraph();
        _engine = new InferenceEngine(_graph);
        _drillDown = new PgDrillDownCollector(postgres, planFetcher, logger);
        _baselineProvider = new PgBaselineProvider(postgres, logger);
        _anomalyDetector = new PgAnomalyDetector(postgres, _baselineProvider, logger);
    }

    /// <summary>
    /// Runs the full analysis pipeline for a server.
    /// Default time range is the last 4 hours. Host-UTC window (Lite's clock semantics —
    /// Darling's collectors stamp rows with the service host's UTC clock).
    ///
    /// <para>#2506: <paramref name="asOfUtc"/> moves the END of that window off "now" while
    /// <paramref name="hoursBack"/> stays its LENGTH, so an incident can be analyzed where it happened.
    /// Null — every caller but the anchored MCP tool — is the pre-#2506 behaviour exactly. Anchoring
    /// reaches the whole pipeline through the context, including the anomaly detector's hour-of-day ×
    /// day-of-week baseline, which is keyed off the window rather than off the clock; that is what makes
    /// the answer for a past window the same KIND of answer, and not merely a differently-filtered one.
    /// An anchored pass does not persist — see <see cref="AnalysisContext.PersistFindings"/>.</para>
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last",
        Justification = "The two tokens are at positions 4 and 5 and the Darling worker passes them POSITIONALLY. " +
                        "Moving asOfUtc ahead of them to satisfy the rule would silently rebind that call site's " +
                        "arguments — a compiling change of meaning on the one caller that matters. Appending is the " +
                        "only edit that cannot do that, and Lite's twin keeps the same order so the two stay transplantable.")]
    public async Task<List<AnalysisFinding>> AnalyzeAsync(
        int serverId, string serverName, int hoursBack = 4, CancellationToken cancellationToken = default,
        CancellationToken shutdownToken = default, DateTime? asOfUtc = null)
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
            CancellationToken = cancellationToken,

            /* #2430. The fifth argument is what keeps the abandon classification truthful once
               cancellationToken is a BUDGET rather than the stopping token. Defaulting it to None is
               deliberate rather than lazy: an on-demand caller (the MCP analyze_server tool, the
               Viewer) has no service stop to distinguish, so its cancellations are timeouts and
               should read as timeouts. The scheduled worker is the one caller that has both, and it
               is the only one that passes both. */
            ShutdownToken = shutdownToken
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
        EndedEarlyAs = null;

        try
        {
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

                _logger?.LogInformation(
                    "[DarlingAnalysisService] Skipping analysis for {Server}: {Have:F1}h data, need {Need}h",
                    context.ServerName, dataSpanHours, MinimumDataHours);

                LastAnalysisTime = DateTime.UtcNow;
                return [];
            }

            /* #2299: abandon BETWEEN the expensive store stages when the host is stopping. The
               fact collector's per-query catches are deliberately silent, so a stop mid-collect
               cannot unwind from inside it — these boundary checks are what turn the token into
               an exit. The post-enrichment tail (action build + insert) carries no check: by
               then the expensive work is done, and finishing preserves it when the store is
               still up, while a store already gone classifies quietly. */
            context.CancellationToken.ThrowIfCancellationRequested();

            // 1. Collect facts from the Postgres store
            var facts = await _collector.CollectFactsAsync(context);

            if (facts.Count == 0)
            {
                LastAnalysisTime = DateTime.UtcNow;
                return [];
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

            // 4. Mute-filter the stories into the surviving findings (the Dashboard twin's D2/P2
            //    reorder) — WITHOUT inserting yet, so enrichment + action-build happen on the
            //    survivors first and the BUILT RemediationAction is persisted on each row. Muted/
            //    absolution findings are dropped here and never enriched.
            var findings = await _findingStore.FilterMutedFindingsAsync(stories, context);

            // 5. Enrich the survivors with drill-down data (ephemeral except through the built
            //    action; the cheap config drill-downs run below the 0.5 gate inside the collector).
            await _drillDown.EnrichFindingsAsync(findings, context);

            // 6. Build + attach each finding's RemediationAction from the now drill-down-
            //    populated finding (D2). The builders REQUIRE finding.DrillDown, which the
            //    store read-back does not return — so the BUILT action is persisted, exactly
            //    the artifact the alert path serializes into ContextJson. Try the always-safe/
            //    db-config force action first, then the destructive entry points (each gates
            //    internally on RootFactKey + drill-down and returns null when N/A); attach the
            //    first non-null.
            foreach (var finding in findings)
            {
                finding.Remediation =
                    FactRemediation.BuildAction(finding)
                    ?? FactRemediation.BuildRcsiAction(finding)
                    ?? FactRemediation.BuildClearPlanAction(finding)
                    ?? FactRemediation.BuildFileAutogrowthAction(finding) // WS3: advisory only (no handler -> no Apply); carried for the read-time copy-paste
                    ?? FactRemediation.BuildServerConfigAction(finding) // WS3: server-level config — MAXDOP/CTFP/memory
                    ?? FactRemediation.BuildMissingIndexAction(finding); // WS4: missing-index CREATE — copy-paste only
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

            // 8. Notify listeners — the returned/enriched findings (now action-bearing) also
            //    flow back to the caller (the worker), which routes them to the shared
            //    AnalysisNotificationService. Gated with the insert for the same reason and not a
            //    weaker one: this event is how findings reach notification, and an alert about last
            //    Tuesday delivered today is the persistence problem with a shorter fuse.
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

            _logger?.LogInformation(
                "[DarlingAnalysisService] Analysis complete for {Server}: {Count} finding(s), highest severity {Severity:F2}{Exploratory}",
                context.ServerName, findings.Count, findings.Count > 0 ? findings.Max(f => f.Severity) : 0,
                context.PersistFindings ? string.Empty : " (anchored window — exploratory, not persisted)");

            return findings;
        }
        catch (Exception ex)
        {
            /* #2299: the ONE line an abandonment is allowed to cost. The component catches let the
               residue propagate instead of logging it per-metric, so seven ERRORs collapse to a single
               line here — and it states the loss honestly: whatever this pass would have written is
               gone, and the next scheduled pass recomputes it from the store.

               #2430 split that line in two, because the pass token now fires for two very different
               reasons and only one of them is fine. Getting this wrong is the reason the Lite fix could
               not simply be ported: arm the token with a budget while the classifier still asks "are we
               stopping?", and every ordinary overrun on a healthy service reports itself at Information
               as a clean stop — a wrong answer wearing a calm one's clothes, on exactly the signal
               someone would use to decide the budget needs raising.

               Classified ONCE, in the catch body rather than across two exception filters, because the
               three outcomes are one decision and splitting it would mean evaluating it twice and
               letting the halves drift. */
            EndedEarlyAs = AnalysisShutdown.Classify(ex, context.ShutdownToken, context.CancellationToken);

            switch (EndedEarlyAs)
            {
                case AnalysisAbandonKind.Shutdown:
                    _logger?.LogInformation(
                        "[DarlingAnalysisService] Analysis abandoned at shutdown for {Server} — this pass's findings are lost by design; the next pass recomputes them ({Detail})",
                        context.ServerName, ex.Message);
                    break;

                case AnalysisAbandonKind.Timeout:
                    _logger?.LogWarning(
                        "[DarlingAnalysisService] Analysis for {Server} was cancelled at its per-pass budget and unwound as asked — this cycle produces no findings and the next one recomputes them. A pass that keeps hitting this is not finishing inside its budget, which is a server whose analysis is quietly getting less complete, not a stop ({Detail})",
                        context.ServerName, ex.Message);
                    break;

                default:
                    _logger?.LogError("[DarlingAnalysisService] Analysis failed for {Server}: {Message}",
                        context.ServerName, ex.Message);
                    break;
            }

            return [];
        }
        finally
        {
            IsAnalyzing = false;
        }
    }

    /// <summary>
    /// Runs the collect + score pipeline without graph traversal.
    /// Returns raw scored facts with amplifier details for direct inspection.
    ///
    /// <para>#2506: <paramref name="asOfUtc"/> anchors the END of the window; null is "now", which is
    /// every caller but the anchored MCP tool. Nothing here persists, so the anchor carries no
    /// write-side question — this is a read that happens to score what it read.</para>
    /// </summary>
    public async Task<List<Fact>> CollectAndScoreFactsAsync(
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
            if (facts.Count == 0) return facts;
            _scorer.ScoreAll(facts);
            return facts;
        }
        catch (Exception ex)
        {
            _logger?.LogError("[DarlingAnalysisService] Fact collection failed for {Server}: {Message}",
                serverName, ex.Message);
            return [];
        }
    }

    /// <summary>
    /// Compares analysis of two time periods, returning facts from both for comparison.
    /// </summary>
    public async Task<(List<Fact> BaselineFacts, List<Fact> ComparisonFacts)> ComparePeriodsAsync(
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

            return (baselineFacts, comparisonFacts);
        }
        catch (Exception ex)
        {
            _logger?.LogError("[DarlingAnalysisService] Period comparison failed for {Server}: {Message}",
                serverName, ex.Message);
            return ([], []);
        }
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
    /// Mutes a finding pattern so it won't appear in future runs.
    /// </summary>
    public async Task MuteFindingAsync(AnalysisFinding finding, string? reason = null)
    {
        await _findingStore.MuteStoryAsync(
            finding.ServerId, finding.StoryPathHash, finding.StoryPath, reason);
    }

    /// <summary>
    /// Cleans up old findings beyond the retention period.
    /// </summary>
    public async Task CleanupAsync(int retentionDays = 30)
    {
        await _findingStore.CleanupOldFindingsAsync(retentionDays);
    }

    /// <summary>
    /// Lite's data-span query VERBATIM — EXTRACT(EPOCH FROM ...) is shared dialect and runs
    /// unchanged on Postgres. Reads the raw wait_stats table (not the view) like Lite, with
    /// Lite's multi-server server_id filter (the Dashboard twin is single-server and drops it).
    /// Exposed const so Darling.Tests can pin the dialect ungated.
    /// </summary>
    public const string TotalDataSpanSql = @"
SELECT EXTRACT(EPOCH FROM (MAX(collection_time) - MIN(collection_time))) / 3600.0
FROM wait_stats
WHERE server_id = $1";

    /// <summary>
    /// Returns the total span of collected data for a server (no time range filter).
    /// This answers "has this server been monitored long enough?" — separate from
    /// the analysis window. A server with 100 hours of total history can safely
    /// be analyzed over a 4-hour window without dilution.
    /// </summary>
    private async Task<double> GetTotalDataSpanHoursAsync(int serverId, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

            using var cmd = new NpgsqlCommand(TotalDataSpanSql, connection);
            cmd.Parameters.AddWithValue(serverId);

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result == null || result is DBNull)
                return 0;

            return Convert.ToDouble(result);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))
        {
            /* Probe failure reads as "no data yet" — EXCEPT shutdown residue, which must not be
               allowed to masquerade as a 0-hour history (#2299): it propagates to the pass's
               shutdown catch instead of producing a bogus insufficient-data skip. */
            return 0;
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
