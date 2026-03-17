using System;
using System.Collections.Generic;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// A scored observation from collected data.
/// </summary>
public class Fact
{
    public string Source { get; set; } = string.Empty;
    public string Key { get; set; } = string.Empty;
    public double Value { get; set; }
    public double BaseSeverity { get; set; }
    public double Severity { get; set; }
    public int ServerId { get; set; }
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Raw metric values for analysis and audit trail.
    /// Keys are metric-specific (e.g., "wait_time_ms", "waiting_tasks_count").
    /// </summary>
    public Dictionary<string, double> Metadata { get; set; } = [];

    /// <summary>
    /// Amplifiers that were evaluated for this fact.
    /// </summary>
    public List<AmplifierResult> AmplifierResults { get; set; } = [];
}

/// <summary>
/// Result of evaluating a single amplifier against the fact set.
/// </summary>
public class AmplifierResult
{
    public string Description { get; set; } = string.Empty;
    public bool Matched { get; set; }
    public double Boost { get; set; }
}

/// <summary>
/// A conditional edge in the relationship graph.
/// </summary>
public class Edge
{
    public string Source { get; set; } = string.Empty;
    public string Destination { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public string PredicateDescription { get; set; } = string.Empty;

    /// <summary>
    /// Evaluates whether this edge should be followed given the current fact set.
    /// </summary>
    public Func<IReadOnlyDictionary<string, Fact>, bool> Predicate { get; set; } = _ => false;
}

/// <summary>
/// A complete analysis story — the path from root symptom to leaf recommendation.
/// </summary>
public class AnalysisStory
{
    public string RootFactKey { get; set; } = string.Empty;
    public double RootFactValue { get; set; }
    public double Severity { get; set; }
    public double Confidence { get; set; }
    public string Category { get; set; } = string.Empty;
    public List<string> Path { get; set; } = [];
    public string StoryPath { get; set; } = string.Empty;
    public string StoryPathHash { get; set; } = string.Empty;
    public string StoryText { get; set; } = string.Empty;
    public string? LeafFactKey { get; set; }
    public double? LeafFactValue { get; set; }
    public int FactCount { get; set; }
    public bool IsAbsolution { get; set; }
}

/// <summary>
/// A persisted finding from a previous analysis run.
/// Maps to the analysis_findings DuckDB table.
/// </summary>
public class AnalysisFinding
{
    public long FindingId { get; set; }
    public DateTime AnalysisTime { get; set; }
    public int ServerId { get; set; }
    public string ServerName { get; set; } = string.Empty;
    public string? DatabaseName { get; set; }
    public DateTime? TimeRangeStart { get; set; }
    public DateTime? TimeRangeEnd { get; set; }
    public double Severity { get; set; }
    public double Confidence { get; set; }
    public string Category { get; set; } = string.Empty;
    public string StoryPath { get; set; } = string.Empty;
    public string StoryPathHash { get; set; } = string.Empty;
    public string StoryText { get; set; } = string.Empty;
    public string RootFactKey { get; set; } = string.Empty;
    public double? RootFactValue { get; set; }
    public string? LeafFactKey { get; set; }
    public double? LeafFactValue { get; set; }
    public int FactCount { get; set; }

    /// <summary>
    /// Drill-down data collected after graph traversal. Ephemeral — not persisted to DuckDB.
    /// Contains supporting detail keyed by category (e.g., "top_deadlocks", "queries_at_spike").
    /// </summary>
    public Dictionary<string, object>? DrillDown { get; set; }
}

/// <summary>
/// A muted finding pattern. Maps to the analysis_muted DuckDB table.
/// </summary>
public class AnalysisMuted
{
    public long MuteId { get; set; }
    public int? ServerId { get; set; }
    public string? DatabaseName { get; set; }
    public string StoryPathHash { get; set; } = string.Empty;
    public string StoryPath { get; set; } = string.Empty;
    public DateTime MutedDate { get; set; }
    public string? Reason { get; set; }
}

/// <summary>
/// A user-configured exclusion filter. Maps to the analysis_exclusions DuckDB table.
/// </summary>
public class AnalysisExclusion
{
    public long ExclusionId { get; set; }
    public string ExclusionType { get; set; } = string.Empty;
    public string ExclusionValue { get; set; } = string.Empty;
    public int? ServerId { get; set; }
    public string? DatabaseName { get; set; }
    public bool IsEnabled { get; set; } = true;
    public DateTime CreatedDate { get; set; }
    public string? Description { get; set; }
}

/// <summary>
/// A severity threshold value. Maps to the analysis_thresholds DuckDB table.
/// </summary>
public class AnalysisThreshold
{
    public long ThresholdId { get; set; }
    public string Category { get; set; } = string.Empty;
    public string FactKey { get; set; } = string.Empty;
    public string ThresholdType { get; set; } = string.Empty;
    public double ThresholdValue { get; set; }
    public int? ServerId { get; set; }
    public string? DatabaseName { get; set; }
    public bool IsEnabled { get; set; } = true;
    public DateTime ModifiedDate { get; set; }
}
