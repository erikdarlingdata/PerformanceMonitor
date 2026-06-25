using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Analysis;

/// <summary>
/// Orchestrates the full analysis pipeline: collect → score → traverse → persist.
/// Can be run on-demand or on a timer. Each run analyzes a single server's data
/// for a given time window and persists the findings.
/// Port of Lite's AnalysisService — uses SQL Server instead of DuckDB.
/// </summary>
public class AnalysisService
{
    private readonly string _connectionString;
    private readonly SqlServerFindingStore _findingStore;
    private readonly SqlServerFactCollector _collector;
    private readonly FactScorer _scorer;
    private readonly RelationshipGraph _graph;
    private readonly InferenceEngine _engine;
    private readonly SqlServerDrillDownCollector _drillDown;
    private readonly SqlServerAnomalyDetector _anomalyDetector;
    private readonly SqlServerBaselineProvider _baselineProvider;

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

    public AnalysisService(string connectionString, IPlanFetcher? planFetcher = null)
    {
        _connectionString = connectionString;
        _findingStore = new SqlServerFindingStore(connectionString);
        _collector = new SqlServerFactCollector(connectionString);
        _scorer = new FactScorer();
        _graph = new RelationshipGraph();
        _engine = new InferenceEngine(_graph);
        _drillDown = new SqlServerDrillDownCollector(connectionString, planFetcher);
        _baselineProvider = new SqlServerBaselineProvider(connectionString);
        _anomalyDetector = new SqlServerAnomalyDetector(connectionString, _baselineProvider);
    }

    /// <summary>
    /// Probes the MONITORED server's clock so the analysis window is built in the SAME clock the
    /// collectors stamp rows with (SYSDATETIME, server-local). Returns the server's local "now"
    /// and its UTC offset (SYSDATETIME − SYSUTCDATETIME). Falls back to host UTC + zero offset
    /// when the server is unreachable — degrading to the prior (host-UTC-window) behavior rather
    /// than introducing a new hard failure (the collectors that follow would fail anyway).
    /// </summary>
    private async Task<(DateTime LocalNow, TimeSpan UtcOffset)> GetServerClockAsync()
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT SYSDATETIME(), SYSUTCDATETIME();";
            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var local = reader.GetDateTime(0);
                var utc = reader.GetDateTime(1);
                return (local, local - utc);
            }
        }
        catch (Exception ex)
        {
            Logger.Error($"[AnalysisService] Server clock probe failed; using host UTC: {ex.Message}");
        }
        return (DateTime.UtcNow, TimeSpan.Zero);
    }

    /// <summary>
    /// The monitored server's current LOCAL time, for callers that compute their own analysis
    /// windows (e.g. period comparison). See <see cref="GetServerClockAsync"/>.
    /// </summary>
    public async Task<DateTime> GetServerLocalNowAsync() => (await GetServerClockAsync()).LocalNow;

    /// <summary>
    /// Runs the full analysis pipeline for a server.
    /// Default time range is the last 4 hours.
    /// </summary>
    public async Task<List<AnalysisFinding>> AnalyzeAsync(int serverId, string serverName, int hoursBack = 4)
    {
        // The collectors stamp rows with the SERVER's clock (SYSDATETIME, server-local), so the
        // analysis window MUST be in that same clock — otherwise every windowed read filters
        // server-local data against a host-UTC window and silently misses it on any non-UTC
        // server. The captured offset converts this window back to UTC at persistence time.
        var (serverNow, serverUtcOffset) = await GetServerClockAsync();
        var timeRangeEnd = serverNow;
        var timeRangeStart = timeRangeEnd.AddHours(-hoursBack);

        var context = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = timeRangeStart,
            TimeRangeEnd = timeRangeEnd,
            ServerUtcOffset = serverUtcOffset
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

        try
        {
            // 0. Check minimum data span — total history, not the analysis window.
            // A server with 100h of total history can be analyzed over a 4h window.
            var dataSpanHours = await GetTotalDataSpanHoursAsync();
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

                Logger.Info(
                    $"[AnalysisService] Skipping analysis for {context.ServerName}: {dataSpanHours:F1}h data, need {MinimumDataHours}h");

                LastAnalysisTime = DateTime.UtcNow;
                return [];
            }

            // 1. Collect facts from SQL Server
            var facts = await _collector.CollectFactsAsync(context);

            if (facts.Count == 0)
            {
                LastAnalysisTime = DateTime.UtcNow;
                return [];
            }

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

            // 4. Mute-filter the stories into the surviving findings (P2 reorder) — WITHOUT
            //    inserting yet, so enrichment + action-build happen on the survivors first
            //    and the BUILT RemediationAction is persisted on each row (D2). Muted/
            //    absolution findings are dropped here and never enriched (no enrich-then-
            //    discard; round-3 MODERATE-2).
            var findings = await _findingStore.FilterMutedFindingsAsync(stories, context);

            // 5. Enrich the survivors with drill-down data (ephemeral, not persisted). The
            //    cheap config drill-down runs regardless of severity (D7), so config/RCSI/
            //    db-config actions can build at their true 0.3 severity; the expensive
            //    plan-fetch enrichment stays behind the 0.5 gate inside the collector.
            await _drillDown.EnrichFindingsAsync(findings, context);

            // 6. Build + attach each finding's RemediationAction from the now drill-down-
            //    populated finding (D2). The builders REQUIRE finding.DrillDown, which the
            //    store read-back does not return — so the BUILT action is persisted, exactly
            //    the artifact the alert path serializes into ContextJson. Try the always-safe/
            //    db-config force action first, then the two destructive entry points (each
            //    gates internally on RootFactKey + drill-down and returns null when N/A);
            //    attach the first non-null.
            foreach (var finding in findings)
            {
                finding.Remediation =
                    FactRemediation.BuildAction(finding)
                    ?? FactRemediation.BuildRcsiAction(finding)
                    ?? FactRemediation.BuildClearPlanAction(finding)
                    ?? FactRemediation.BuildFileAutogrowthAction(finding) // WS3: advisory only (no handler -> no Apply); carried for the read-time copy-paste
                    ?? FactRemediation.BuildServerConfigAction(finding) // WS3: server-level config — MAXDOP/CTFP Apply-able, memory advise-only
                    ?? FactRemediation.BuildMissingIndexAction(finding); // WS4: missing-index CREATE — copy-paste only (no handler -> no Apply); carried for the read-time copy-paste
            }

            // 7. Insert the survivors in one batched pass, persisting remediation_action_json
            //    (D2). Reuses PR-1's single-connection + single-schema-check discipline.
            await _findingStore.InsertFindingsAsync(findings, context);

            LastAnalysisTime = DateTime.UtcNow;

            // 8. Notify listeners — the returned/enriched findings (now action-bearing) flow
            //    to the AnalysisCompleted event and, via the scheduler, to NotifyAsync, which
            //    builds its own context from the drill-down. The reorder did not change which
            //    list notify receives.
            AnalysisCompleted?.Invoke(this, new AnalysisCompletedEventArgs
            {
                ServerId = context.ServerId,
                ServerName = context.ServerName,
                Findings = findings,
                AnalysisTime = LastAnalysisTime.Value
            });

            Logger.Info(
                $"[AnalysisService] Analysis complete for {context.ServerName}: {findings.Count} finding(s), " +
                $"highest severity {(findings.Count > 0 ? findings.Max(f => f.Severity) : 0):F2}");

            return findings;
        }
        catch (Exception ex)
        {
            Logger.Error($"[AnalysisService] Analysis failed for {context.ServerName}: {ex.Message}");
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
    /// </summary>
    public async Task<List<Fact>> CollectAndScoreFactsAsync(int serverId, string serverName, int hoursBack = 4)
    {
        // Server-local window (see AnalyzeAsync) so windowed fact reads match the collectors.
        var (serverNow, serverUtcOffset) = await GetServerClockAsync();
        var timeRangeEnd = serverNow;
        var timeRangeStart = timeRangeEnd.AddHours(-hoursBack);

        var context = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = timeRangeStart,
            TimeRangeEnd = timeRangeEnd,
            ServerUtcOffset = serverUtcOffset
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
            Logger.Error($"[AnalysisService] Fact collection failed for {serverName}: {ex.Message}");
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
            Logger.Error($"[AnalysisService] Period comparison failed for {serverName}: {ex.Message}");
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
    /// Gets recent findings for a server within the given time range.
    /// </summary>
    public async Task<List<AnalysisFinding>> GetRecentFindingsAsync(int serverId, int hoursBack = 24)
    {
        return await _findingStore.GetRecentFindingsAsync(serverId, hoursBack);
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
    /// Returns the total span of collected data (no time range filter).
    /// This answers "has this server been monitored long enough?" — separate from
    /// the analysis window. A server with 100 hours of total history can safely
    /// be analyzed over a 4-hour window without dilution.
    /// Dashboard monitors one server per database, so no server_id filtering.
    /// </summary>
    private async Task<double> GetTotalDataSpanHoursAsync()
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT DATEDIFF(SECOND, MIN(collection_time), MAX(collection_time)) / 3600.0
FROM collect.wait_stats;";

            var result = await cmd.ExecuteScalarAsync();
            if (result == null || result is DBNull)
                return 0;

            return Convert.ToDouble(result);
        }
        catch
        {
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
