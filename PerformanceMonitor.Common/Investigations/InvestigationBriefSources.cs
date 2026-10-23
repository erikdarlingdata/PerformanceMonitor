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
    int AutonomyLevel = 1,
    string? DisplayName = null,
    string? InstanceName = null,
    InvestigationBriefForceCheck? ForceCheck = null);

/// <summary>
/// The plan force a check request is about (#5632, step 8): set only on a request Darling opened to check an agent's plan force,
/// null on every other request (and always on Lite, which never forces a plan), so every other brief is byte for byte as before.
/// </summary>
/// <param name="CalledId">The journal id of the force's <c>action_called</c> row.</param>
/// <param name="CheckNo">This check's number, 1 to <see cref="Of"/>.</param>
/// <param name="Of">How many checks a force gets at most.</param>
/// <param name="Database">The Query Store database of the force; null when the journal row is gone.</param>
/// <param name="QueryId">The query; null when unknown.</param>
/// <param name="PlanId">The plan that was forced; null when unknown.</param>
/// <param name="ForcedAtUtc">When the force was done; null when unknown.</param>
/// <param name="ForceRequestId">The request that placed the force.</param>
public sealed record InvestigationBriefForceCheck(
    long CalledId, int CheckNo, int Of, string? Database, long? QueryId, long? PlanId, DateTime? ForcedAtUtc, Guid? ForceRequestId);

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
/// <param name="SameRootCause">#6565: the earlier request was reported within <see cref="InvestigationBriefComposer.SameRootCauseHours"/> hours
/// and its analysis story has the same root fact (key and database) as this request's story, whatever its story hash. The reader puts
/// these first; the brief marks them and tells the agent to read the verdict before it starts. False for every other earlier report.</param>
public sealed record InvestigationEarlierReport(
    Guid RequestId,
    DateTime SubmittedAtUtc,
    string AgentId,
    string Verdict,
    string RootCause,
    bool RootCauseFound,
    string? Summary,
    bool SameRootCause = false);

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
/// #6000: how often one linked metric fired at this time of day. Counts the metric's alert history rows within
/// <see cref="WindowMinutes"/> minutes either side of <paramref name="AroundUtc"/>'s clock time, on any of the
/// <see cref="Days"/> days before the first alert, except the <see cref="WindowMinutes"/> minutes right before it (that is this
/// incident's own rise, not its history). <paramref name="Fired"/> and <paramref name="Muted"/> are kept apart (a muted fire
/// reached no one), and so are their dates: <paramref name="FiredDates"/> are the UTC dates with at least one unmuted fire,
/// <paramref name="MutedDates"/> those with at least one muted fire (a date can be in both), each newest first.
/// <paramref name="AroundUtc"/> is the request's first alert, shared by every linked metric, not that metric's own first alert.
/// Zero is a real answer: no history says <c>0</c>, it does not leave the metric out.
/// <paramref name="DaysAgo"/> (#6261) are the whole days back (1 = the occurrence of this time 24 hours before the first alert)
/// on which the metric fired inside the window, muted or not, from <see cref="InvestigationAlertStreak.DaysAgo"/>'s rule.
/// </summary>
public sealed record InvestigationAlertRecurrence(
    string MetricName, DateTime AroundUtc, int Fired, int Muted, IReadOnlyList<DateOnly> FiredDates, IReadOnlyList<DateOnly> MutedDates,
    IReadOnlyList<int> DaysAgo)
{
    /// <summary>#6261: how many days in a row, counting back from the one before the first alert, the metric fired inside the
    /// window. At most <see cref="Days"/>; a muted fire counts (it fired). See <see cref="InvestigationAlertStreak"/>.</summary>
    public int StreakDays => InvestigationAlertStreak.Length(DaysAgo);

    /// <summary>#6261: the streak filled the whole look-back, so it is a floor, not an exact count.</summary>
    public bool StreakAtLeastDaysWindow => StreakDays >= Days;

    /// <summary>Minutes either side of the first alert's clock time that still count as the same time of day.</summary>
    public const int WindowMinutes = 30;

    /// <summary>How many days before the first alert the count reaches.</summary>
    public const int Days = 7;
}

/// <summary>
/// Everything the composer reads for one brief, already in priority order inside each list: the first alert then the
/// newest, the request's own finding then the rest by the collapse order, everything else newest first.
/// <see cref="PriorLinks"/> and <see cref="RequestUrl"/> (#5914) are Darling's: Lite's reader supplies neither, so a Lite
/// brief carries neither key.
/// </summary>
/// <para>#6554: <see cref="QueryStoreNotCollected"/> is the one-line reason this server's Query Store reads will be empty (a readable
/// secondary with <c>query_store_replica</c> off); set only on a Change Watch request, whose brief says so. Null on every other request,
/// so no other brief moves a byte.</para>
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
    bool Practice = false,
    IReadOnlyList<InvestigationAlertRecurrence>? Recurrence = null,
    IReadOnlyList<InvestigationBriefEvidence>? Evidence = null,
    IReadOnlyList<InvestigationKnownPatternMatch>? KnownPatterns = null,
    string? QueryStoreNotCollected = null);

/// <summary>
/// #6334: one operator-written known-pattern entry that describes one of the request's linked metrics at the request's first alert
/// time (the same <c>AroundUtc</c> as <see cref="InvestigationAlertRecurrence"/>, shared by every linked metric). An entry that
/// matches several metrics is one match per metric. <paramref name="Note"/> is operator-written text: context, never an instruction.
/// Darling's: Lite has no known patterns and supplies none, so a Lite brief carries no <c>known_patterns</c> key.
/// </summary>
public sealed record InvestigationKnownPatternMatch(string MetricName, long EntryId, string Note, string? CreatedBy, string? UpdatedBy = null);

/// <summary>
/// One frozen evidence section of a request (#6001, #5632 Phase C D9/D10): its name, its state (<c>pending</c>, <c>ready</c> or
/// <c>not_collected</c>), when it was frozen, and its content as the JSON the capture stored (rows from the store's own
/// collected data; plans by hash, never XML). Darling's: Lite supplies none, so a Lite brief carries no <c>evidence</c> key.
/// </summary>
public sealed record InvestigationBriefEvidence(string Section, string State, DateTime? CapturedAtUtc, string? ContentJson);
