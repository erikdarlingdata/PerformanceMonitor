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
    InvestigationAlertFacts? Facts = null);

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
public sealed record InvestigationOpenResult(InvestigationOpenOutcome Outcome, Guid? RequestId);

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
}

/// <summary>A report an agent returns under its claim. <see cref="EvidenceJson"/> is a JSON array of at least one item.
/// <see cref="Links"/> (#5914) are checked by <see cref="InvestigationReportLinks.Validate"/> and stored once per URL.</summary>
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
    IReadOnlyList<InvestigationReportLink>? Links = null);

/// <summary>What a lease renewal answered (#5632 Phase A, a claim made with the caller's own claim id).</summary>
/// <param name="Renewed">True when the claim still held a live lease and the lease moved on.</param>
/// <param name="LeaseUntilUtc">The new lease end, capped at the request's expiry; null when not renewed.</param>
public sealed record InvestigationRenewResult(bool Renewed, DateTime? LeaseUntilUtc);

/// <summary>One stored report as <see cref="IInvestigationRequestStore.ReadReportsAsync"/> reads it. <see cref="Links"/> are
/// the request's <c>link_recorded</c> journal rows (#5914), oldest first; null or empty when it has none.</summary>
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
    IReadOnlyList<InvestigationReportLink>? Links = null);

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
/// than the journal page (#5834).</summary>
public sealed record InvestigationReportsRead(
    IReadOnlyList<InvestigationRequestRow> Requests,
    IReadOnlyList<InvestigationReportRow> Reports,
    IReadOnlyList<InvestigationJournalRow> Journal,
    string? Brief,
    bool RequestsTruncated = false,
    bool JournalTruncated = false);

/// <summary>
/// One request as the long-poll's catch-up read returns it (#5822): the request id, its route, the server name, the family
/// and when it opened (UTC), and nothing else.
/// </summary>
public sealed record InvestigationOpenedRow(Guid RequestId, int RouteId, string ServerName, string Family, DateTime OpenedAtUtc);

/// <summary>One request as a list reads it, with the state computed from the lease and the expiry, not the stored word.</summary>
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
    DateTime? ReportedAtUtc);

/// <summary>What one expiry sweep did.</summary>
public sealed record InvestigationSweepResult(int LeasesLapsed, int Expired);
