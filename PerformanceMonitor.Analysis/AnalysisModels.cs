using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

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
    /// <summary>The root fact's RAW collected value (the setting/metric — MAXDOP 0, a wait's
    /// fraction-of-period, CPU%, etc.), NOT its severity. <see cref="Severity"/> is the separate
    /// 0–~2 score. Surfaced as MCP root_fact.value and in the notification headline.</summary>
    public double RootFactValue { get; set; }
    public double Severity { get; set; }
    public double Confidence { get; set; }
    public string Category { get; set; } = string.Empty;
    public List<string> Path { get; set; } = [];
    public string StoryPath { get; set; } = string.Empty;
    public string StoryPathHash { get; set; } = string.Empty;
    public string StoryText { get; set; } = string.Empty;
    public string? LeafFactKey { get; set; }
    /// <summary>The leaf fact's RAW collected value (see <see cref="RootFactValue"/>), not severity.</summary>
    public double? LeafFactValue { get; set; }
    public int FactCount { get; set; }
    public bool IsAbsolution { get; set; }

    /// <summary>
    /// Stable id for the incident this story belongs to (correlate-and-focus slice 2). All findings
    /// from one analysis run share it, and it is a fingerprint of the run's PRIMARY (highest-severity)
    /// finding + database, so the same recurring incident keeps one id across runs (trackable). Set by
    /// <see cref="IncidentId.StampStories"/> after stories are built; copied onto the finding + persisted.
    /// Empty for a healthy/absolution-only run.
    /// </summary>
    public string IncidentId { get; set; } = string.Empty;

    /// <summary>
    /// Metadata from the root fact (raw metric values used to assemble the story).
    /// Ephemeral — copied onto the finding for the notification layer, not persisted.
    /// </summary>
    public Dictionary<string, double>? RootFactMetadata { get; set; }

    /// <summary>
    /// Database the root fact pertains to, if any (e.g. BAD_ACTOR_* facts). Copied onto the
    /// finding so recommendation cards can show a database. Null for server-scope stories.
    /// </summary>
    public string? DatabaseName { get; set; }
}

/// <summary>
/// A persisted finding from a previous analysis run.
/// Maps to the analysis_findings table.
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
    /// <summary>Stable id for the incident this finding belongs to — see
    /// <see cref="AnalysisStory.IncidentId"/>. Persisted to analysis_findings.incident_id.</summary>
    public string IncidentId { get; set; } = string.Empty;
    public string StoryText { get; set; } = string.Empty;
    public string RootFactKey { get; set; } = string.Empty;
    /// <summary>The root fact's RAW collected value (the setting/metric), NOT its severity — see
    /// <see cref="AnalysisStory.RootFactValue"/>. Persisted to analysis_findings.root_fact_value.</summary>
    public double? RootFactValue { get; set; }
    public string? LeafFactKey { get; set; }
    /// <summary>The leaf fact's RAW collected value (see <see cref="RootFactValue"/>), not severity.</summary>
    public double? LeafFactValue { get; set; }
    public int FactCount { get; set; }

    /// <summary>
    /// Drill-down data collected after graph traversal. Ephemeral — not persisted.
    /// Contains supporting detail keyed by category (e.g., "top_deadlocks", "queries_at_spike").
    /// </summary>
    public Dictionary<string, object>? DrillDown { get; set; }

    /// <summary>
    /// The built remediation action for this finding (recommendations rebuild D2).
    /// Ephemeral, like <see cref="DrillDown"/>: populated post-enrich on the WRITE path
    /// (AnalysisService builds it from the drill-down-populated finding via FactRemediation),
    /// serialized into the analysis_findings <c>remediation_action_json</c> column, and
    /// deserialized back here on READ. It is NOT a scored field and takes no part in story
    /// scoring/traversal; it exists so the Recommendations surface can drive Apply + the
    /// two-sided consent gate from a finding read back from storage (the builders require a
    /// drill-down that GetRecentFindingsAsync does not return, so the BUILT action is
    /// persisted instead, mirroring the alert path's ContextJson). Null when no execution
    /// shape applies. <see cref="RemediationAction"/> lives in this same assembly.
    /// </summary>
    public RemediationAction? Remediation { get; set; }

    /// <summary>
    /// Metadata from the root fact carried in from <see cref="AnalysisStory.RootFactMetadata"/>.
    /// Ephemeral — used by the notification layer for diagnosis context; not persisted.
    /// In practice this is anomaly-detector baseline context: mean, stddev, tier, hour, dow.
    /// </summary>
    public Dictionary<string, double>? RootFactMetadata { get; set; }
}

/// <summary>
/// A muted finding pattern. Maps to the analysis_muted table.
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
/// A user-configured exclusion filter. Maps to the analysis_exclusions table.
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
/// A severity threshold value. Maps to the analysis_thresholds table.
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
