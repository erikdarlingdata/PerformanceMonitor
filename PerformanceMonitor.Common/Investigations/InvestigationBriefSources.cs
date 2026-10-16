using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>The request a brief is about (#5632 Phase A). Every time is UTC.</summary>
public sealed record InvestigationBriefRequest(
    Guid RequestId,
    int ServerId,
    string ServerName,
    bool IsPostgres,
    string Family,
    string State,
    DateTime ExpiresAtUtc,
    DateTime? LeaseUntilUtc,
    int AlertCount,
    string FirstMetric,
    string? StoryPathHash,
    int AutonomyLevel = 1);

/// <summary>One alert linked to the request, with its stored text and context (<c>config_alert_log</c>).</summary>
public sealed record InvestigationBriefAlert(
    DateTime AlertTimeUtc,
    string MetricName,
    double? CurrentValue,
    double? ThresholdValue,
    bool Muted,
    string? DetailText,
    string? SendError,
    IReadOnlyList<InvestigationBriefContextItem> Context);

/// <summary>One context block of an alert: its heading and its labeled fields (the labels of <c>AlertContextBuilders</c>).</summary>
public sealed record InvestigationBriefContextItem(string Heading, IReadOnlyList<KeyValuePair<string, string>> Fields);

/// <summary>A tool call the brief tells the agent to make, with its arguments and why.</summary>
public sealed record InvestigationNextCall(string Tool, IReadOnlyDictionary<string, string> Arguments, string Why);

/// <summary>One force-plan target of a finding's remediation: the verdict and the named blockers, never the script.</summary>
public sealed record InvestigationBriefRemediationTarget(
    string Database, long QueryId, long PlanId, bool Eligible, IReadOnlyList<string> Blockers, string? StateNote);

/// <summary>A finding's remediation as a verdict (the verb and each target's eligibility and blockers) and guidance.</summary>
public sealed record InvestigationBriefRemediation(
    string Verb, string? Guidance, IReadOnlyList<InvestigationBriefRemediationTarget> Targets);

/// <summary>One of the latest analysis findings for the server, collapsed to its newest occurrence.</summary>
public sealed record InvestigationBriefFinding(
    string StoryPathHash,
    string Category,
    string Headline,
    string? StoryText,
    string RootFactKey,
    double? RootFactValue,
    double Severity,
    double Confidence,
    DateTime AnalysisTimeUtc,
    int Occurrences,
    InvestigationBriefRemediation? Remediation,
    IReadOnlyList<InvestigationNextCall> NextTools);

/// <summary>An earlier report for the same server and family: what an agent said, not reviewed by a person.</summary>
public sealed record InvestigationEarlierReport(
    Guid RequestId,
    DateTime SubmittedAtUtc,
    string AgentId,
    string Verdict,
    string RootCause,
    bool RootCauseFound,
    string? Summary);

/// <summary>A recent change on the monitoring side: a mute rule, an analysis mute, a plan-force action.</summary>
/// <param name="Pattern">A mute rule's query text pattern, masked as statement text; null for other kinds.</param>
public sealed record InvestigationRecentChange(DateTime AtUtc, string Kind, string Text, string? Pattern);

/// <summary>An approved, unexpired operator note about the server.</summary>
public sealed record InvestigationBriefNote(string Author, string? DecidedBy, DateTime ExpiresAtUtc, string Text);

/// <summary>The operator's environment profile text for the server (scope <c>server</c>, <c>tag</c> or <c>fleet</c>).</summary>
public sealed record InvestigationProfileText(string Scope, string Text);

/// <summary>A link an earlier report on the same server and family recorded (#5914): a <c>link_recorded</c> journal row.
/// What an agent said, not checked by a person; the URL is free text.</summary>
public sealed record InvestigationPriorLink(Guid RequestId, DateTime RecordedAtUtc, string Kind, string Url, string? Key);

/// <summary>
/// Everything the composer reads for one brief, already in priority order inside each list: the first alert then the
/// newest, the request's own finding then the rest by the collapse order, everything else newest first.
/// <see cref="PriorLinks"/> and <see cref="RequestUrl"/> (#5914) are Darling's: Lite's reader supplies neither, so a Lite
/// brief carries neither key.
/// </summary>
public sealed record InvestigationBriefSources(
    InvestigationBriefRequest Request,
    IReadOnlyList<InvestigationBriefAlert> Alerts,
    IReadOnlyList<InvestigationBriefFinding> Findings,
    IReadOnlyList<InvestigationEarlierReport> EarlierReports,
    IReadOnlyList<InvestigationRecentChange> RecentChanges,
    IReadOnlyList<InvestigationBriefNote> Notes,
    IReadOnlyList<InvestigationProfileText> Profiles,
    IReadOnlyList<InvestigationPriorLink>? PriorLinks = null,
    string? RequestUrl = null,
    bool Practice = false);
