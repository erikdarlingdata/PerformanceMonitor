using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// One object a summary fact RANKS, in that fact's own units (#3691 lane 43). A summary card names its top
/// few; the list of every object is a tool's job, which is why this is capped at
/// <see cref="FactRanked.MaxObjects"/> and not a page of rows.
///
/// <para><b>The invariant.</b> Entry <c>[0]</c> IS the fact's own subject — <see cref="Fact.ObjectName"/> and
/// <see cref="Fact.Value"/> — so a card has ONE truth about what it is about and the advice never has to
/// decide between two spellings of the worst object. The order is the collector's rank (its read's ORDER BY),
/// never re-sorted here; <c>FactRankedTests</c> pins all of it over the collectors' planted fixtures.</para>
///
/// <para><b>Why <see cref="Figures"/> is a dictionary and not fields.</b> Each family ranks on its own
/// quantity and states its own figures — a ratio and hours for the autovacuum-disabled card, growth bytes /
/// percent / dead tuples for the bloat trend — and a shared record cannot name them all without becoming a
/// union of every family that ever ranks. Doubles for the same reason <see cref="Fact.Metadata"/> is doubles:
/// figures are numbers, names are the record's own <see cref="ObjectName"/>. Null when the rank needs no
/// figures beyond <see cref="Value"/>. The keys are the family's UN-PREFIXED metadata names, so a reader who
/// knows the fact's metadata knows these.</para>
/// </summary>
public sealed record RankedObject(
    string ObjectName,
    string? DatabaseName,
    double Value,
    IReadOnlyDictionary<string, double>? Figures = null);

/// <summary>
/// The cap on <see cref="Fact.Ranked"/>, in one place because it is a DESIGN decision rather than a per-family
/// preference: three is the number #3761's autovacuum-disabled read already chose, and the argument is that the
/// card is a summary, not the list — a collector that wants more rows is asking for a drill-down or a tool
/// (<c>get_pg_autovacuum_health</c>, <c>get_pg_index_usage</c>), both of which exist. Ruled by the maintainer,
/// 2026-09-22.
/// </summary>
public static class FactRanked
{
    /// <summary>How many objects a fact may rank, entry [0] (the fact's own subject) included.</summary>
    public const int MaxObjects = 3;
}

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
    /// Optional object name (schema.table, optionally with an index) for object-scoped facts such as
    /// the ANOMALY_OBJECT_* anomalies — the name the source query selected but the doubles-only
    /// <see cref="Metadata"/> cannot carry. A fact whose read ranked several objects carries the rest of
    /// them, with their own figures, in <see cref="Ranked"/> — this name is that list's first entry.
    /// </summary>
    public string? ObjectName { get; set; }

    /// <summary>
    /// The objects this fact ranks, worst first, entry [0] being the fact's own subject
    /// (<see cref="ObjectName"/> / <see cref="Value"/>) — see <see cref="RankedObject"/> for the invariant and
    /// the cap. Empty for the overwhelming majority of facts: a fact about a server, a setting or a single
    /// object ranks nothing, and an empty list is what byte-identity rests on (the payloads emit
    /// <c>ranked</c> only at two or more entries, and no SQL Server collector sets this at all).
    ///
    /// <para><b>Ephemeral by construction.</b> Only <see cref="AnalysisFinding"/> is persisted;
    /// <c>Fact</c> lives for one pass and <c>get_analysis_facts</c> re-runs the collect+score to answer.
    /// So this needed no migration rung and no store column — the difference between naming the top few on a
    /// card and reading them back later, which is the drill-down's job.</para>
    /// </summary>
    public List<RankedObject> Ranked { get; set; } = [];

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
    /// How many of the root fact's amplifier checks MATCHED — the corroboration count
    /// <see cref="StoryConfidence.Compute(int, int, int)"/> folds into <see cref="Confidence"/> as the
    /// amplifier-share term (#3538 A6). Carried as its own number since #3712 because the notification
    /// layer routes on the corroboration COMPONENTS rather than on the confidence scalar: a page is earned by
    /// a second fact in the chain or by a matched co-fire check, and reading either off the scalar would move
    /// the paging bar the day the formula's weights change. Every amplifier the scorer defines is a predicate
    /// over ANOTHER fact (a sibling anomaly fired, an absolute fact at its bar, a wait significant), so a
    /// match here IS a co-fire. Zero for a root with no catalogue and for the two by-construction 1.0
    /// stories (absolution, the same-statement pileup), which <c>FindingRouting</c> names by root key instead.
    /// Ephemeral — copied onto the finding for the notification layer, not persisted.
    /// </summary>
    public int MatchedAmplifiers { get; set; }

    /// <summary>
    /// How many amplifier checks the scorer DEFINED for the root fact — the denominator of the amplifier
    /// share. Zero means the root has no catalogue (the uncatalogued confidence arm), which is the absence of
    /// evidence rather than evidence against; carried so a routing reason can say "0 of 3 checks matched"
    /// versus "no checks defined". Ephemeral, like <see cref="MatchedAmplifiers"/>.
    /// </summary>
    public int DefinedAmplifiers { get; set; }

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

    /// <summary>
    /// The hops AFTER the root whose fact names a thing — the parked application, the lead blocker, the job, the
    /// table — highest severity first, at most <see cref="FactIdentity.MaxNamedHops"/> of them, as
    /// <see cref="InferenceEngine.BuildStories"/> recorded them off the traversal path (#3691). FactAdvice's
    /// PopulateStoryText composes one sentence per entry onto the root's frozen advice, which is how the name
    /// reaches every card, e-mail and MCP payload without any of them changing; this list is the typed record of
    /// WHICH hops were named, for the composer and for tests. Ephemeral like <see cref="RootFactMetadata"/> — the
    /// sentence in StoryText is what persists. Empty for a one-node story, an absolution, or a chain none of
    /// whose hops names anything (that story's text is byte-identical to what it was).
    /// </summary>
    public List<NamedHop> NamedHops { get; set; } = [];

    /// <summary>
    /// Config-advisory facts that hang off a node ON this story's path by an ACTIVE edge the greedy walk did not
    /// follow — the lever beside the incident (#3691). The traversal takes the single highest-severity edge from
    /// each node, so a mid-path node with two active edges reaches only one of them; before this list, the config
    /// leaf it skipped was left un-consumed and then rooted its OWN one-node card at its advisory severity, beside
    /// the incident it belongs to. Two cards where the truth is one story with a lever. The engine now sweeps those
    /// destinations onto the story and marks them consumed, so they no longer root alone.
    ///
    /// <para>They ride BESIDE the path, never in it: <see cref="Path"/>, <see cref="StoryPath"/>,
    /// <see cref="StoryPathHash"/>, <see cref="LeafFactKey"/> and <see cref="FactCount"/> are what they were, so the
    /// incident identity (the hash every mute, occurrence count and fingerprint keys on) is byte-identical and no
    /// existing story pin moves. A side leaf does not lift <see cref="Severity"/> or <see cref="Confidence"/> either
    /// — it is context, not corroboration; amplifying is the graph's business and lives in the scorer. Highest
    /// severity first, ties by ordinal key, so a pass is deterministic. Ephemeral like
    /// <see cref="RootFactMetadata"/>: the payload's <c>side_leaves</c> array is what a reader sees, and the ONE
    /// sentence <c>FactAdvice.PopulateStoryText</c> appends is what persists. Empty for every story with no skipped
    /// config destination, which is nearly all of them.</para>
    /// </summary>
    public List<string> SideLeafKeys { get; set; } = [];
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
    /// The root fact's matched amplifier count, carried in from <see cref="AnalysisStory.MatchedAmplifiers"/>
    /// (#3712). Ephemeral like <see cref="DrillDown"/>: populated on the WRITE path for the notification
    /// layer's routing gate and NOT persisted — a finding read back from <c>analysis_findings</c> carries 0
    /// here, and a read-side surface that wants the route re-derives it through
    /// <c>FindingRouting.ClassifyPersisted</c>, which says how.
    /// </summary>
    public int MatchedAmplifiers { get; set; }

    /// <summary>The root fact's defined amplifier count, carried in from
    /// <see cref="AnalysisStory.DefinedAmplifiers"/> (#3712). Ephemeral, not persisted.</summary>
    public int DefinedAmplifiers { get; set; }

    /// <summary>
    /// The config levers hanging off this finding's chain, carried in from
    /// <see cref="AnalysisStory.SideLeafKeys"/> (#3691) — the advisory keys the greedy walk could not reach and
    /// that therefore no longer root a card of their own. Ephemeral like <see cref="DrillDown"/> and the
    /// amplifier components: no <c>analysis_findings</c> column, so a finding read back from the store carries
    /// none here, and the sentence <c>FactAdvice.PopulateStoryText</c> appended to
    /// <see cref="StoryText"/> is the part that persists and reaches a read-back card. The
    /// <c>side_leaves</c> array in <c>analyze_server</c> is rendered from this.
    /// </summary>
    public List<string> SideLeafKeys { get; set; } = [];

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
