/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;

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

    /// <summary>
    /// #6596: an alert on an agent route whose story the analysis pass labelled "recurring at this hour", so no request was
    /// opened or joined for it. Journaled with no request id, for a person to read; not an event (an agent is not woken for it).
    /// </summary>
    public const string RecurringSkipped = "recurring_skipped";

    /// <summary>
    /// #6596: the once key of a <see cref="RecurringSkipped"/> row: one per route, server, metric and alert hour, so a page
    /// repeated inside the hour writes one row.
    /// </summary>
    public static string RecurringSkippedOnceKey(int routeId, int serverId, string metricName, DateTime alertTimeUtc) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture,
            $"recurring_skipped:{routeId}:{serverId}:{metricName}:{alertTimeUtc:yyyy-MM-ddTHH}");

    /// <summary>#6596: the detail of a <see cref="RecurringSkipped"/> row: the story path hash and the alert's time.</summary>
    public static string RecurringSkippedDetail(string? storyPathHash, DateTime alertTimeUtc) =>
        new JsonObject
        {
            ["story_path_hash"] = storyPathHash,
            ["alert_time"] = DateTime.SpecifyKind(alertTimeUtc, DateTimeKind.Utc).ToString("O", System.Globalization.CultureInfo.InvariantCulture),
        }.ToJsonString();

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

    /// <summary>The once key of a follow-up's event (#6008): one <c>follow_up</c> row per earlier request, whatever replays the sweep.</summary>
    public static string FollowUpOnceKey(Guid earlierRequestId) => "follow_up:" + earlierRequestId.ToString("D");

    /// <summary>The <c>trigger</c> of the <c>follow_up</c> event a plan-force check opens (#5632, step 8).</summary>
    public const string ForceCheckTrigger = "force_check";

    /// <summary>The once key of a plan-force check's event: one <c>follow_up</c> row per force (its <c>action_called</c> journal id) and check number.</summary>
    public static string ForceCheckOnceKey(long calledId, int checkNo) =>
        "force_check:" + calledId.ToString(System.Globalization.CultureInfo.InvariantCulture) + ":" + checkNo.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /* #6003 review: the detail members each event kind may carry out to the agent route and the long-poll. The writers are
       the service's own, but deadlock_digest has none yet (#6005); follow_up's writer is the follow-up sweep (#6008), and a member nobody listed here is
       dropped at the read, so a later writer cannot put text that was never meant to leave the store on the wire. */
    private static readonly Dictionary<string, string[]> DetailMembers = new(StringComparer.Ordinal)
    {
        [Joined] = ["new_metric", "severity"],
        [Expired] = ["unclaimed"],
        [ClaimsExhausted] = ["unclaimed"],
        [EvidenceReady] = ["sections", "not_collected", "pending"],
        /* #6005: the digest names signatures (a hash) and counts, never object names or statements. */
        [DeadlockDigest] = ["window_hours", "total", "signatures"],
        [FollowUp] = ["follow_up_of", "trigger", "force_check_of", "check_no"],
    };

    /// <summary>
    /// The detail an event of kind <paramref name="eventKind"/> sends: only the members its kind lists, as a JSON object, or
    /// null when none is left (or <paramref name="detailJson"/> is not a JSON object). A practice request's event
    /// (<paramref name="practiceOf"/>, the case it replays) also carries <c>practice_of</c>, so a receiver can tell a replay from
    /// a live request (#6003 review). Read by both event readers.
    /// </summary>
    public static string? PublicDetail(string eventKind, string? detailJson, Guid? practiceOf = null)
    {
        var kept = new JsonObject();
        if (!string.IsNullOrWhiteSpace(detailJson) && DetailMembers.TryGetValue(eventKind, out var members))
        {
            JsonObject? source;
            try
            {
                source = JsonNode.Parse(detailJson) as JsonObject;
            }
            catch (JsonException)
            {
                source = null;
            }

            if (source is not null)
            {
                foreach (var member in members)
                {
                    if (source.TryGetPropertyValue(member, out var value))
                    {
                        kept[member] = value?.DeepClone();
                    }
                }
            }
        }

        if (practiceOf is Guid practice)
        {
            kept["practice_of"] = practice.ToString("D");
        }

        return kept.Count == 0 ? null : kept.ToJsonString();
    }

    /// <summary>
    /// The reason an event of kind <paramref name="eventKind"/> sends: the metric that opened or joined the request, which the
    /// service writes from its own alert; null for every other kind (#6003 review, as <see cref="PublicDetail"/>).
    /// </summary>
    public static string? PublicReason(string eventKind, string? reason) =>
        eventKind is Opened or Joined ? reason : null;
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
