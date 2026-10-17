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

/// <summary>
/// The request events (#6003, D5 and D6 of #5632 Phase C): the investigation journal is the event log, and its rows whose
/// <c>event</c> is one of <see cref="Kinds"/> are the events. Two readers consume them: the signed POST to the agent route and
/// the MCP long-poll. Every other journal row (a claim, a refusal, a quiet join, a delivery mark) is audit only.
/// </summary>
public static class InvestigationEvents
{
    public const string Opened = "opened";

    /// <summary>A join that brought a new metric or a higher severity (D6). Any other join is <see cref="QuietJoin"/>.</summary>
    public const string Joined = "joined";

    /// <summary>Every metric linked to the request has a resolution row after its last alert (D7).</summary>
    public const string Resolved = "resolved";

    public const string Expired = "expired";

    public const string ClaimsExhausted = "claims_exhausted";

    public const string EvidenceReady = "evidence_ready";

    public const string FollowUp = "follow_up";

    public const string DeadlockDigest = "deadlock_digest";

    /// <summary>A join with no new metric and no higher severity: journaled for the audit trail, never an event (D6).</summary>
    public const string QuietJoin = "joined_quiet";

    /// <summary>The journal row that marks one event as delivered to the agent route (once key <see cref="DeliveredOnceKey"/>).</summary>
    public const string Delivered = "event_delivered";

    /// <summary>The journal row that marks an expired request's fallback as sent (once key <see cref="FallbackOnceKey"/>).</summary>
    public const string FallbackSent = "fallback_sent";

    /// <summary>Every event kind, in the order D6 lists them.</summary>
    public static IReadOnlyList<string> Kinds { get; } =
        [Opened, Joined, Resolved, Expired, ClaimsExhausted, EvidenceReady, FollowUp, DeadlockDigest];

    /// <summary>The severities a request's <c>max_severity</c> holds, lowest first.</summary>
    public const string Warning = "warning", Critical = "critical";

    /// <summary>The rank of a stored severity: critical above warning; null or an unknown word ranks lowest.</summary>
    public static int SeverityRank(string? severity) => severity switch
    {
        Critical => 2,
        Warning => 1,
        _ => 0,
    };

    /// <summary>
    /// D6: a join is an event only when it brings a metric the request has not seen, or a severity above the request's
    /// <c>max_severity</c>. Every other join is noise.
    /// </summary>
    public static bool JoinIsEvent(bool newMetric, string? maxSeverity, string? joiningSeverity) =>
        newMetric || SeverityRank(joiningSeverity) > SeverityRank(maxSeverity);

    /// <summary>The once key of an event's delivery mark: a replayed event id is never delivered twice.</summary>
    public static string DeliveredOnceKey(Guid eventId) => "delivered:" + eventId.ToString("D");

    /// <summary>The once key of an expired request's fallback.</summary>
    public static string FallbackOnceKey(Guid requestId) => "fallback:" + requestId.ToString("D");

    /// <summary>The once key of a request's <see cref="Resolved"/> event.</summary>
    public static string ResolvedOnceKey(Guid requestId) => "resolved:" + requestId.ToString("D");
}

/// <summary>
/// One request event as the journal holds it (#6003): the event's own id (sent as <c>webhook-id</c>), its kind and time,
/// the request it is about and that request's route, and the journal row's detail (for <c>evidence_ready</c>, the sections to
/// re-read). <see cref="JournalId"/> orders the rows.
/// </summary>
public sealed record InvestigationEventRow(
    long JournalId, Guid EventId, string Event, DateTime EventTimeUtc, Guid RequestId, int RouteId, int ServerId,
    string ServerName, string Family, string? Reason, string? DetailJson);

/// <summary>An expired request no agent held when it expired, for the D8 fallback.</summary>
public sealed record InvestigationUnclaimedExpiry(
    Guid RequestId, int RouteId, int ServerId, string ServerName, string Family, string FirstMetric, DateTime OpenedAtUtc,
    DateTime ExpiredAtUtc);
