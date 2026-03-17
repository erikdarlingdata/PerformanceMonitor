using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
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
    /// <summary>
    /// Minimum hours of collected data required before analysis will run.
    /// Short collection windows distort fraction-of-period calculations —
    /// 5 seconds of THREADPOOL looks alarming in a 16-minute window.
    /// Production: 72. Dev/testing: 0.5 (raise before release).
    /// </summary>
    internal double MinimumDataHours { get; set; } = 72;

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

    public AnalysisService(DuckDbInitializer duckDb)
    {
        _duckDb = duckDb;
        _findingStore = new FindingStore(duckDb);
        _collector = new DuckDbFactCollector(duckDb);
        _scorer = new FactScorer();
        _graph = new RelationshipGraph();
        _engine = new InferenceEngine(_graph);
        _drillDown = new DrillDownCollector(duckDb);
    }

    /// <summary>
    /// Runs the full analysis pipeline for a server.
    /// Default time range is the last 4 hours.
    /// </summary>
    public async Task<List<AnalysisFinding>> AnalyzeAsync(int serverId, string serverName, int hoursBack = 4)
    {
        var timeRangeEnd = DateTime.UtcNow;
        var timeRangeStart = timeRangeEnd.AddHours(-hoursBack);

        var context = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = timeRangeStart,
            TimeRangeEnd = timeRangeEnd
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
            var dataSpanHours = await GetTotalDataSpanHoursAsync(context.ServerId);
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

            // 1. Collect facts from DuckDB
            var facts = await _collector.CollectFactsAsync(context);

            if (facts.Count == 0)
            {
                LastAnalysisTime = DateTime.UtcNow;
                return [];
            }

            // 2. Score facts (base severity + amplifiers)
            _scorer.ScoreAll(facts);

            // 3. Build stories via graph traversal
            var stories = _engine.BuildStories(facts);

            // 4. Persist findings (filtering out muted)
            var findings = await _findingStore.SaveFindingsAsync(stories, context);

            // 5. Enrich findings with drill-down data (ephemeral, not persisted)
            await _drillDown.EnrichFindingsAsync(findings, context);

            LastAnalysisTime = DateTime.UtcNow;

            // 5. Notify listeners
            AnalysisCompleted?.Invoke(this, new AnalysisCompletedEventArgs
            {
                ServerId = context.ServerId,
                ServerName = context.ServerName,
                Findings = findings,
                AnalysisTime = LastAnalysisTime.Value
            });

            AppLogger.Info("AnalysisService",
                $"Analysis complete for {context.ServerName}: {findings.Count} finding(s), " +
                $"highest severity {(findings.Count > 0 ? findings.Max(f => f.Severity) : 0):F2}");

            return findings;
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
    /// Returns raw scored facts with amplifier details for direct inspection.
    /// </summary>
    public async Task<List<Fact>> CollectAndScoreFactsAsync(int serverId, string serverName, int hoursBack = 4)
    {
        var timeRangeEnd = DateTime.UtcNow;
        var timeRangeStart = timeRangeEnd.AddHours(-hoursBack);

        var context = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = timeRangeStart,
            TimeRangeEnd = timeRangeEnd
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
            AppLogger.Error("AnalysisService", $"Fact collection failed for {serverName}: {ex.Message}");
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
            AppLogger.Error("AnalysisService", $"Period comparison failed for {serverName}: {ex.Message}");
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
    /// Returns the total span of collected data for a server (no time range filter).
    /// This answers "has this server been monitored long enough?" — separate from
    /// the analysis window. A server with 100 hours of total history can safely
    /// be analyzed over a 4-hour window without dilution.
    /// </summary>
    private async Task<double> GetTotalDataSpanHoursAsync(int serverId)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT EXTRACT(EPOCH FROM (MAX(collection_time) - MIN(collection_time))) / 3600.0
FROM wait_stats
WHERE server_id = $1";

            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });

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
