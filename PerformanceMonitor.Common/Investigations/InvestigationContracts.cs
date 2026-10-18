/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/* #5820: the Level 1 investigation contracts, shared by Darling's PostgreSQL store and Lite's DuckDB store. Moved
   unchanged from Darling.Storage's InvestigationRequestStore.cs. */

/// <summary>One alert handed to the investigation store by the dispatcher (#5632 Phase A). Every time is UTC.</summary>
/// <param name="RouteId">The route whose agent level answered 1; its limits are the next four members.</param>
/// <param name="DedupKeys">Every incident dedup key of the alert (a row is recorded per key); empty when it has none.</param>
/// <param name="Facts">
/// #5821: what the alert's history row will hold, written on its link rows (V188) so the brief's alerts section reads them
/// even when an agent claims the request before that row exists. Null writes NULLs, and the brief then reads the history row.
/// </param>
public sealed record InvestigationAlert(
    int RouteId,
    int ServerId,
    string ServerName,
    string Family,
    string MetricName,
    DateTime AlertTimeUtc,
    IReadOnlyList<string> DedupKeys,
    string? StoryPathHash,
    int MaxPerHour,
    int MaxPerDay,
    int LeaseMinutes,
    int ExpiryMinutes,
    InvestigationAlertFacts? Facts = null)
{
    /// <summary>The level the answering route sends this alert at (#5912): 1 (investigate) or 2 (act). The store stamps it on a
    /// new request and lowers a joined request to it, so the lowest level among a request's routes wins. An init member with
    /// the Level 1 default, so every existing construction keeps its meaning; Lite never sets it.</summary>
    public int AutonomyLevel { get; init; } = 1;

    /// <summary>The alert's severity, <see cref="InvestigationEvents.Warning"/> or <see cref="InvestigationEvents.Critical"/>
    /// (#6003, D6): stamped as a new request's <c>max_severity</c>, and a join above it is an event. Null reads as warning.</summary>
    public string? Severity { get; init; }
}

/// <summary>
/// #5821: the alert's values as its <c>config_alert_log</c> row stores them: <c>current_value</c> and
/// <c>threshold_value</c> as that row's doubles, <c>muted</c>, <c>detail_text</c> and <c>context_json</c>. The two texts are
/// scrubbed where they are written, as the history row's are.
/// </summary>
public sealed record InvestigationAlertFacts(
    double CurrentValue,
    double ThresholdValue,
    bool Muted,
    string? DetailText,
    string? ContextJson);

/// <summary>What <see cref="IInvestigationRequestStore.OpenOrJoinAsync"/> did with an alert.</summary>
public enum InvestigationOpenOutcome
{
    /// <summary>A new request was opened.</summary>
    Opened,

    /// <summary>The alert joined an open or claimed request of the same server and family.</summary>
    Joined,

    /// <summary>The route's hourly or daily limit was reached: nothing was opened.</summary>
    LimitReached,
}

/// <summary>The result of <see cref="IInvestigationRequestStore.OpenOrJoinAsync"/>; the request is null at a limit.</summary>
public sealed record InvestigationOpenResult(InvestigationOpenOutcome Outcome, Guid? RequestId)
{
    /// <summary>#6003: true when the store journaled an event (<c>opened</c>, or a <c>joined</c> that brought a new metric or a
    /// higher severity); a quiet join and a limit journal none.</summary>
    public bool Evented { get; init; }
}

/// <summary>What a claim answered.</summary>
public enum InvestigationClaimOutcome
{
    /// <summary>The request is now claimed under the new claim id.</summary>
    Claimed,

    /// <summary>Another claim holds a live lease.</summary>
    AlreadyClaimed,

    /// <summary>The request is reported, closed or expired, or has used its three claims.</summary>
    NotOpen,

    /// <summary>No request has that id.</summary>
    NotFound,

    /// <summary>The agent id is not 1 to <see cref="InvestigationRules.MaxAgentIdLength"/> characters from
    /// <c>[A-Za-z0-9._:-]</c> (#5632 review round 2, L4): nothing
    /// was read or written.</summary>
    AgentIdInvalid,

    /// <summary>The request already holds <see cref="InvestigationRules.MaxAgentJournalRows"/> journal rows
    /// written by agents: the call is refused and one <c>journal_capped</c> row says so.</summary>
    JournalCapped,
}

/// <summary>The result of a claim. The lease and claim count are set only when <see cref="Outcome"/> is Claimed.</summary>
public sealed record InvestigationClaimResult(
    InvestigationClaimOutcome Outcome, Guid? ClaimId, DateTime? LeaseUntilUtc, int ClaimCount, string? Reason);

/// <summary>What a report submission answered.</summary>
public enum InvestigationSubmitOutcome
{
    /// <summary>The report was stored and the request is reported.</summary>
    Reported,

    /// <summary>The claim id is not the request's current claim: a newer claim superseded it, or it never held one.</summary>
    LeaseLost,

    /// <summary>The request already holds a report.</summary>
    AlreadyReported,

    /// <summary>The request is closed.</summary>
    NotOpen,

    /// <summary>No request has that id.</summary>
    NotFound,

    /// <summary>The agent id is not 1 to <see cref="InvestigationRules.MaxAgentIdLength"/> characters from
    /// <c>[A-Za-z0-9._:-]</c> (#5632 review round 2, L4): nothing
    /// was read or written.</summary>
    AgentIdInvalid,

    /// <summary>The request already holds <see cref="InvestigationRules.MaxAgentJournalRows"/> journal rows
    /// written by agents: the report is refused and one <c>journal_capped</c> row says so.</summary>
    JournalCapped,

    /// <summary>The report set a <c>follow_up_at</c> and the request's route already holds
    /// <see cref="InvestigationReportFields.MaxPendingFollowUpsPerRoute"/> pending follow-ups (#6008): nothing was stored.</summary>
    FollowUpLimit,
}

/// <summary>A report an agent returns under its claim. <see cref="EvidenceJson"/> is a JSON array of at least one item.
/// <see cref="Links"/> (#5914) are checked by <see cref="InvestigationReportLinks.Validate"/> and stored once per URL.
/// <see cref="FamilyOf"/> (#6004) is the first report's request id, which must name a request; <see cref="QueriesRunJson"/>
/// is a JSON array of statement strings within <see cref="InvestigationReportFields"/>'s caps. <see cref="FollowUpAtUtc"/>
/// (#6008) is when Darling opens a follow-up request; <see cref="FollowUpCondition"/> (#6009) is a <c>module:db.schema.object</c> deploy that opens it sooner; the store keeps it on the report's request (Lite ignores it, #6012).</summary>
public sealed record InvestigationReportSubmission(
    Guid RequestId,
    Guid ClaimId,
    string AgentId,
    short ReportFormat,
    string Verdict,
    string RootCause,
    bool RootCauseFound,
    string? Summary,
    string EvidenceJson,
    string? ProposedNote = null,
    IReadOnlyList<InvestigationReportLink>? Links = null,
    Guid? FamilyOf = null,
    string? QueriesRunJson = null,
    DateTime? FollowUpAtUtc = null,
    string? FollowUpCondition = null);

/// <summary>What a lease renewal answered (#5632 Phase A, a claim made with the caller's own claim id).</summary>
/// <param name="Renewed">True when the claim still held a live lease and the lease moved on.</param>
/// <param name="LeaseUntilUtc">The new lease end, capped at the request's expiry; null when not renewed.</param>
public sealed record InvestigationRenewResult(bool Renewed, DateTime? LeaseUntilUtc);

/// <summary>One stored report as <see cref="IInvestigationRequestStore.ReadReportsAsync"/> reads it. <see cref="Links"/> are
/// the request's <c>link_recorded</c> journal rows (#5914), oldest first; null or empty when it has none. <see cref="FamilyOf"/>
/// and <see cref="QueriesRunJson"/> (#6004) are null on a report that gave none, and on every report Lite stores.</summary>
public sealed record InvestigationReportRow(
    Guid RequestId,
    short ReportFormat,
    string AgentId,
    DateTime SubmittedAtUtc,
    string Verdict,
    string RootCause,
    bool RootCauseFound,
    string? Summary,
    string EvidenceJson,
    IReadOnlyList<InvestigationReportLink>? Links = null,
    Guid? FamilyOf = null,
    string? QueriesRunJson = null);

/// <summary>One journal row as <see cref="IInvestigationRequestStore.ReadReportsAsync"/> reads it.</summary>
public sealed record InvestigationJournalRow(
    DateTime EventTimeUtc,
    Guid? RequestId,
    int? ServerId,
    string Event,
    string Actor,
    string? Reason,
    string? DetailJson);

/// <summary>What <see cref="IInvestigationRequestStore.ReadReportsAsync"/> read; <see cref="Brief"/> only for one request.
/// <see cref="RequestsTruncated"/> says the window held more requests than the page (the reports and the journal are
/// those of the page's requests only), and <see cref="JournalTruncated"/> that those requests held more journal rows
/// than the journal page (#5834). <see cref="Feedback"/> is the people's feedback on the page's reports and
/// <see cref="FamilyCounts"/> the false-positive counts per family over the same window (#5913); both are null from a store
/// that keeps no feedback (Lite's).</summary>
public sealed record InvestigationReportsRead(
    IReadOnlyList<InvestigationRequestRow> Requests,
    IReadOnlyList<InvestigationReportRow> Reports,
    IReadOnlyList<InvestigationJournalRow> Journal,
    string? Brief,
    bool RequestsTruncated = false,
    bool JournalTruncated = false,
    IReadOnlyList<InvestigationFeedbackRow>? Feedback = null,
    IReadOnlyList<InvestigationFamilyCounts>? FamilyCounts = null);

/// <summary>
/// One request as the long-poll's catch-up read returns it (#5822): the request id, its route, the server name, the family
/// and when it opened (UTC), and the registry's names (#5999: <see cref="DisplayName"/> always set on Darling,
/// <see cref="InstanceName"/> null once the server has left the registry; both null where the read has no registry join).
/// </summary>
public sealed record InvestigationOpenedRow(
    Guid RequestId, int RouteId, string ServerName, string Family, DateTime OpenedAtUtc,
    string? DisplayName = null, string? InstanceName = null)
{
    /// <summary>#6003: the journal event this row answers (the long-poll reads events, not only opens); null on a row read
    /// from the requests alone, which answers its open.</summary>
    public string? Event { get; init; }

    /// <summary>#6003: the event's id, the cursor's key; <see cref="Guid.Empty"/> keys the row by its request id.</summary>
    public Guid EventId { get; init; }

    /// <summary>#6003: when the event was journaled, the cursor's watermark; null uses <see cref="OpenedAtUtc"/>.</summary>
    public DateTime? EventTimeUtc { get; init; }

    /// <summary>#6003: the journal row's detail (for <c>evidence_ready</c>, the sections to re-read).</summary>
    public string? DetailJson { get; init; }

    /// <summary>The cursor's key for this row: the event id, else the request id.</summary>
    public Guid CursorKey => EventId == Guid.Empty ? RequestId : EventId;

    /// <summary>The cursor's time for this row: the event's time, else the open's.</summary>
    public DateTime CursorTimeUtc => EventTimeUtc ?? OpenedAtUtc;
}

/// <summary>One request as a list reads it, with the state computed from the lease and the expiry, not the stored word.
/// <see cref="PracticeOf"/> (#5918) is the request a practice request replays; null for every other request, and then absent from the answer.
/// <see cref="DisplayName"/> and <see cref="InstanceName"/> (#5999) are the registry's names for the server, read when the row is served so a
/// rename shows on the next read (<see cref="ServerName"/> stays the name the request was opened with). Darling always fills
/// <see cref="DisplayName"/> (it falls back to the opened name) and leaves <see cref="InstanceName"/> null once the server has left the
/// registry; a store built with no server list to read leaves both null. Lite names them from its server list (ServerManager, #6011). A null is absent from the answer.
/// The answer key is <c>instance_name</c>, not <c>host_name</c>, which the MCP data-detail filter numbers as a client host.</summary>
public sealed record InvestigationRequestRow(
    Guid RequestId,
    int RouteId,
    int ServerId,
    string ServerName,
    string Family,
    string FirstMetric,
    DateTime FirstAlertAtUtc,
    DateTime LastAlertAtUtc,
    int AlertCount,
    string State,
    DateTime OpenedAtUtc,
    DateTime ExpiresAtUtc,
    string? ClaimedBy,
    DateTime? LeaseUntilUtc,
    int ClaimCount,
    DateTime? ReportedAtUtc,
    Guid? PracticeOf = null,
    string? DisplayName = null,
    string? InstanceName = null);

/// <summary>What one expiry sweep did.</summary>
public sealed record InvestigationSweepResult(int LeasesLapsed, int Expired)
{
    /// <summary>#6003: how many requests this pass journaled <c>resolved</c> for (D7).</summary>
    public int Resolved { get; init; }

    /// <summary>#6008: how many follow-up requests this pass opened (each with its <c>follow_up</c> event).</summary>
    public int FollowUps { get; init; }
}

/// <summary>
/// The <c>agent_action</c> argument a Level 2 tool carries under the investigations arm (#5912), validated: the request and
/// the live claim the change is made under, the caller's declared agent id, why, and the evidence (shaped and capped like a
/// report's). <c>agent_id</c> is the caller's word; the claim is what confines it to one server and one lease.
/// </summary>
public sealed record AgentAction(Guid RequestId, Guid ClaimId, string AgentId, string Reason, string EvidenceJson);

/// <summary>What a Level 2 change targets (#5912): the server, the object's id when it has one, and the database.</summary>
public sealed record AgentActionTarget(int ServerId, string ServerName, string? Id, string? Database);

/// <summary>A gate refusal (#5912): <c>{status: refused, reason, message}</c>.</summary>
public sealed record AgentActionRefusal(string Reason, string Message);

/// <summary>The gate's refusal codes (#5912), in the order the gate checks them.</summary>
public static class AgentActionCodes
{
    /// <summary>1: no <c>agent_action</c> under the investigations arm.</summary>
    public const string Required = "agent_action_required";

    /// <summary>1: an <c>agent_action</c> field is missing or malformed; the message names the field.</summary>
    public const string Invalid = "agent_action_invalid";

    /// <summary>2: <c>config_service.agent_actions_enabled</c> is off.</summary>
    public const string ActionsOff = "actions_off";

    /// <summary>3: the claim is not live (one message for every cause, so it is not an oracle).</summary>
    public const string ClaimNotLive = "claim_not_live";

    /// <summary>4: the request is a practice request.</summary>
    public const string Practice = "practice";

    /// <summary>5: the request is not stamped Level 2, or its routes no longer answer Level 2.</summary>
    public const string NotLevel2 = "not_level_2";

    /// <summary>6: the change targets a server other than the request's.</summary>
    public const string OtherServer = "other_server";

    /// <summary>8: the request's agent journal is full.</summary>
    public const string JournalCapped = "journal_capped";

    /// <summary>The one <see cref="ClaimNotLive"/> message.</summary>
    public const string ClaimNotLiveMessage =
        "Your claim is not live: it lapsed, was released, its report was submitted, or another agent holds it. " +
        "Changes are allowed only while your claim is live.";
}
