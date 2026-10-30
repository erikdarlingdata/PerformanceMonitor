/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;


/// <summary>
/// The store operations of Level 1 investigations (#5632 Phase A, plan v2 section 2 and ruling R4): open or join a request,
/// claim it, submit its report, list requests, and the expiry sweep. Every time is bound as a UTC <c>timestamptz</c>
/// parameter from the injected clock, never a bare <c>now()</c>, so the windows do not depend on the session's zone and a
/// test can move the clock.
///
/// <para><b>Open or join is one transaction.</b> It takes a transaction-scoped advisory lock on the server and family, so two
/// alerts of one incident cannot both open a request; joins an open or claimed request, or a reported request that takes a quiet re-fire (#6778), of that server and family only while
/// the alert fires within 30 minutes of the request's latest alert and no later than 4 hours after its first (#5632: the
/// window slides, see <see cref="InvestigationRules.GroupingWindow"/> and <see cref="InvestigationRules.GroupingWindowCap"/>); and otherwise takes
/// <c>pg_advisory_xact_lock(5632, route_id)</c> over the count of the route's requests and the INSERT (ruling R6), so two
/// families of one route cannot both pass the limit. At a limit it journals <c>limit_reached</c> once per route per hour
/// (the journal's <c>once_key</c>) and opens nothing. Joins never count toward a limit.</para>
///
/// <para><b>Claim is one conditional UPDATE</b> (state open, or claimed with a lapsed lease; fewer than three claims; not
/// expired) with <c>RETURNING</c>, so two agents claiming at once cannot both win. When no row returns, one SELECT says why,
/// and the refusal is journaled once per request and reason. <b>Submit</b> accepts the current claim's report while the
/// request is claimed (its lease live or lapsed) or expired under that claim, until a newer claim supersedes it.
/// <b>List</b> writes nothing, so the SELECT-only web role can serve it. <b>The sweep</b> only expires: it journals each lapsed
/// lease once per claim and moves a request past its expiry, or out of claims, to <c>expired</c> (journaled <c>expired</c> or
/// <c>claims_exhausted</c>, once per request). A release keeps its claim counted. It never reopens a lease;
/// the claim's own WHERE takes a lapsed lease over.</para>
/// </summary>
public sealed partial class InvestigationRequestStore : IInvestigationRequestStore, IInvestigationNoteStore
{
    /// <summary>The first key of the route lock, <c>pg_advisory_xact_lock(5632, route_id)</c>.</summary>
    public const int RouteLockKey = 5632;


    /* #6919: an unregistered external name arrives with server_id 0, so server_id and family alone would join every such alert. For
       server_id 0 the lock text also carries the label ($3) and the external source ($4), the same two values FindJoinSql adds to the
       join key. A registered server keeps the text it always had. */
    internal const string GroupLockSql =
        "SELECT pg_advisory_xact_lock(hashtextextended('investigation:' || $1::text || ':' || $2"
        + " || CASE WHEN $1::int = 0 THEN ':' || $3 || ':' || $4 ELSE '' END, 5632))";

    /* #6919: the source of an External:<source>:<key> metric. A source name matches [a-z][a-z0-9_]{1,30}
       (ck_external_alert_source_name), so it holds no colon and the second colon-separated part is the whole source. Any other metric
       at server_id 0 has no source and reads as empty, so those alerts keep grouping by label and family alone. */
    internal static string ExternalSourceSql(string metric) =>
        $"CASE WHEN {metric} LIKE 'External:%' THEN split_part({metric}, ':', 2) ELSE '' END";

    /// <summary>The source part of an <c>External:&lt;source&gt;:&lt;key&gt;</c> metric name, or empty for any other metric (#6919).</summary>
    internal static string ExternalSourceOf(string metricName)
    {
        const string prefix = "External:";
        if (!metricName.StartsWith(prefix, StringComparison.Ordinal))
        {
            return string.Empty;
        }

        var colon = metricName.IndexOf(':', prefix.Length);
        return colon < 0 ? metricName[prefix.Length..] : metricName[prefix.Length..colon];
    }

    internal const string RouteLockSql = "SELECT pg_advisory_xact_lock($1, $2)";

    /* #5918: a practice request is never joined (a live alert must open its own request) and never counts against a route's limits. */
    /// <summary>The grouping window and its cap as interval literals, built from <see cref="InvestigationRules"/> so the SQL never
    /// holds a second copy of either number (#5632).</summary>
    private static readonly string GroupingWindowSql = string.Create(CultureInfo.InvariantCulture,
        $"interval '{(int)InvestigationRules.GroupingWindow.TotalMinutes} minutes'");
    private static readonly string GroupingWindowCapSql = string.Create(CultureInfo.InvariantCulture,
        $"interval '{(int)InvestigationRules.GroupingWindowCap.TotalMinutes} minutes'");

    /* #5632: the window slides. The request's window_until is its latest alert + 30 minutes, held to its first alert + 4 hours, and each
       join moves it (JoinSql), so the alert-time test ($4 <= window_until) is the whole sliding bound. The wall-clock test stays on the
       open time, on the wall clock ($3 is now), and is widened to the 4-hour cap: comparing an alert time with now would refuse every
       alert stamped more than 30 minutes before it reached the store (a late or replayed alert). */
    internal static readonly string FindJoinSql = @"
SELECT r.request_id,
       r.max_severity,
       EXISTS (SELECT 1 FROM config.investigation_request_alerts AS a
               WHERE a.request_id = r.request_id AND a.metric_name = $5) AS has_metric
FROM config.investigation_requests AS r
WHERE r.server_id = $1
  AND r.family = $2
  AND ($1 <> 0 OR r.server_name = $7 AND " + ExternalSourceSql("r.first_metric") + @" = $8)
  AND r.practice_of IS NULL
  AND r.force_check_of IS NULL
  AND r.trigger_kind = 'alert'
  AND (r.state IN ('open', 'claimed') AND r.expires_at > $3
       OR r.state = 'reported'
          AND EXISTS (SELECT 1 FROM config.investigation_request_alerts AS a2 WHERE a2.request_id = r.request_id AND a2.metric_name = $5)
          AND (CASE WHEN $6 = 'critical' THEN 1 ELSE 0 END) <= (CASE WHEN r.max_severity = 'critical' THEN 1 ELSE 0 END)
          AND NOT EXISTS (SELECT 1 FROM config.investigation_journal AS j WHERE j.request_id = r.request_id AND j.event = 'resolved'))
  AND r.opened_at >= $3 - " + GroupingWindowCapSql + @"
  AND $4 <= r.window_until
ORDER BY r.opened_at DESC
LIMIT 1";

    /// <summary>
    /// The level an alert stamps (#5912): the route's level, except 1 for an alert of a custom rule an agent last wrote
    /// (<c>updated_by</c> <c>agent:&lt;id&gt;</c>), so an agent cannot open its own Level 2 window. Custom rule alerts are named
    /// <c>Custom:&lt;rule id&gt;</c>. <c>{metric}</c> and <c>{level}</c> are the statement's parameter numbers.
    /// </summary>
    internal static string StampLevelSql(int metric, int level) => $@"CASE WHEN EXISTS (
        SELECT 1 FROM config.custom_alert_rules AS c
        WHERE 'Custom:' || c.id::text = ${metric}
          AND c.updated_by LIKE 'agent:%') THEN 1 ELSE ${level}::smallint END";

    /// <summary>A join lowers the request to the joining alert's level (#5912): the lowest level among its routes wins.</summary>
    internal static readonly string JoinSql = @"
UPDATE config.investigation_requests
SET alert_count = alert_count + 1,
    last_alert_at = greatest(last_alert_at, $2),
    window_until = greatest(window_until, least(greatest(last_alert_at, $2) + " + GroupingWindowSql + @", first_alert_at + " + GroupingWindowCapSql + @")),
    autonomy_level = least(autonomy_level, " + StampLevelSql(3, 4) + @"),
    max_severity = $5
WHERE request_id = $1";

    internal const string CountRouteSql = @"
SELECT count(*) FILTER (WHERE opened_at > $2 - interval '1 hour'),
       count(*)
FROM config.investigation_requests
WHERE route_id = $1
  AND practice_of IS NULL
  AND force_check_of IS NULL
  AND opened_at > $2 - interval '1 day'";

    internal static readonly string OpenSql = @"
INSERT INTO config.investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, first_dedup_key, story_path_hash,
     first_alert_at, last_alert_at, window_until, opened_at, expires_at, lease_minutes, autonomy_level, max_severity, trigger_kind)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $9 + " + GroupingWindowSql + @", $10, $10 + make_interval(mins => $11), $12,
        " + StampLevelSql(6, 13) + ", $14, $15)";

    internal const string AlertRowsSql = @"
INSERT INTO config.investigation_request_alerts
    (request_id, alert_time, server_id, metric_name, dedup_key, current_value, threshold_value, muted, detail_text, context_json, severity)
SELECT $1, $2, $3, $4, k, $6, $7, $8, $9, $10, $11
FROM unnest($5::text[]) AS k";

    internal const string JournalSql = @"
INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, reason, detail, once_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)
ON CONFLICT (once_key) WHERE once_key IS NOT NULL DO NOTHING";

    internal const string AgentJournalCountSql = @"
SELECT count(*)
FROM config.investigation_journal
WHERE request_id = $1
  AND actor LIKE 'agent:%'";

    internal const string ClaimSql = @"
UPDATE config.investigation_requests
SET state = 'claimed',
    claimed_by = $2,
    claim_id = $3,
    claimed_at = $4,
    lease_until = $4 + make_interval(mins => lease_minutes),
    claim_count = claim_count + 1
WHERE request_id = $1
  AND (state = 'open' OR (state = 'claimed' AND lease_until < $4))
  AND claim_count < 3
  AND expires_at > $4
RETURNING lease_until, claim_count, server_id";

    /* #6576: the agent that already holds the live lease claims again (it lost its claim_id): the live claim_id comes back and the
       lease renews the way RenewSql renews it. No new claim, so claim_count does not move. $1 request, $2 agent, $3 now. */
    internal const string ReclaimSql = @"
UPDATE config.investigation_requests
SET lease_until = least($3 + make_interval(mins => lease_minutes), expires_at)
WHERE request_id = $1
  AND claimed_by = $2
  AND state = 'claimed'
  AND lease_until >= $3
  AND expires_at > $3
RETURNING claim_id, lease_until, claim_count";

    internal const string ClaimWhySql = @"
SELECT state, lease_until, claim_count, expires_at, server_id
FROM config.investigation_requests
WHERE request_id = $1";

    internal const string SubmitLockSql = @"
SELECT state, claim_id, server_id, first_metric, first_dedup_key, story_path_hash, route_id
FROM config.investigation_requests
WHERE request_id = $1
FOR UPDATE";

    /* #6008: the pending follow-ups of a route: reported requests that set follow_up_at and whose follow-up has not opened. $1 route. */
    internal const string PendingFollowUpsSql = @"
SELECT count(*)
FROM config.investigation_requests AS r
WHERE r.route_id = $1
  AND r.state = 'reported'
  AND r.practice_of IS NULL
  AND r.follow_up_at IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM config.investigation_requests AS f WHERE f.follow_up_of = r.request_id)";

    /* #6008: written only when the report set a time, so a store whose mcp role has no grant on the column still takes every other report. */
    internal const string SetFollowUpSql = @"
UPDATE config.investigation_requests SET follow_up_at = $2 WHERE request_id = $1";

    /* #6009: a deploy condition sits beside the time, written only when the report set one, for the same grant reason. */
    internal const string SetFollowUpConditionSql = @"
UPDATE config.investigation_requests SET follow_up_condition = $2 WHERE request_id = $1";

    /// <summary>
    /// #6009: how far back the due read looks in <c>collect.procedure_stats</c> for a changed module: the rows of the last this-many
    /// minutes. The read runs every sweep (60 s) for each request with a condition, and no index covers the module names, so it must stay
    /// a short window of one collection day (the busiest server wrote about a million rows a day). The collector stamps a watched module's
    /// <c>modify_date</c> on every collection while the follow-up is pending, so a window of at least two collection intervals misses
    /// nothing, and a service that was down simply sees the change on the next pass.
    /// </summary>
    internal const string DeployReadWindowMinutes = "30";

    /// <summary>#6009: how far before a module's change the counts' "before" side reaches.</summary>
    internal const string DeployCountsBeforeHours = "24";

    /* #6008: the reported requests whose follow-up is due and not yet opened, oldest first. Locked, so two passes never open one twice. $1 now.
       #6009: a request that names a module (follow_up_condition = 'module:db.schema.object') is also due when a collection of the last
       DeployReadWindowMinutes carries a modify_date for that module later than the report: a module's modify_date is the time it was last
       altered, so a later one means a deploy since the report, and no baseline is needed. Column 2 is that date (the newest), NULL for a
       request with no condition or no change; the time rule keeps its own end for a deploy that never comes. */
    internal const string DueFollowUpsSql = @"
SELECT r.request_id, d.changed_to
FROM config.investigation_requests AS r
LEFT JOIN LATERAL (
    SELECT max(p.module_modify_date) AS changed_to
    FROM collect.procedure_stats AS p
    WHERE p.server_id = r.server_id
      AND p.collection_time >= ($1 AT TIME ZONE 'UTC') - INTERVAL '" + DeployReadWindowMinutes + @" minutes'
      AND lower(p.database_name) = lower(split_part(substr(r.follow_up_condition, 8), '.', 1))
      AND lower(p.schema_name) = lower(split_part(substr(r.follow_up_condition, 8), '.', 2))
      AND lower(p.object_name) = lower(split_part(substr(r.follow_up_condition, 8), '.', 3))
      AND p.module_modify_date > (r.reported_at AT TIME ZONE 'UTC')
) AS d ON r.follow_up_condition IS NOT NULL
WHERE r.state = 'reported'
  AND r.practice_of IS NULL
  AND r.follow_up_at IS NOT NULL
  AND (r.follow_up_at <= $1 OR d.changed_to IS NOT NULL)
  AND NOT EXISTS (SELECT 1 FROM config.investigation_requests AS f WHERE f.follow_up_of = r.request_id)
ORDER BY r.follow_up_at, r.request_id
LIMIT 50
FOR UPDATE OF r SKIP LOCKED";

    /* #6009: the module's procedure stats around its change ($2, the new modify_date), split by collection_time against it: the 24 hours
       before it and from it up to now ($3). Per side: how many collections, executions, and the CPU and elapsed time those executions
       took (microseconds), and the newest modify_date a row of that side carries (NULL when none does). Runs once per wake and reads a
       bounded span. $1 the request, $2 the change, $3 now. */
    internal const string DeployCountsSql = @"
SELECT (p.collection_time >= ($2 AT TIME ZONE 'UTC')) AS after_change, count(*), COALESCE(sum(p.delta_execution_count), 0)::bigint,
       COALESCE(sum(p.delta_worker_time), 0)::bigint, COALESCE(sum(p.delta_elapsed_time), 0)::bigint, max(p.module_modify_date)
FROM config.investigation_requests AS r
JOIN collect.procedure_stats AS p ON p.server_id = r.server_id
WHERE r.request_id = $1
  AND lower(p.database_name) = lower(split_part(substr(r.follow_up_condition, 8), '.', 1))
  AND lower(p.schema_name) = lower(split_part(substr(r.follow_up_condition, 8), '.', 2))
  AND lower(p.object_name) = lower(split_part(substr(r.follow_up_condition, 8), '.', 3))
  AND p.collection_time >= ($2 AT TIME ZONE 'UTC') - INTERVAL '" + DeployCountsBeforeHours + @" hours'
  AND p.collection_time <= ($3 AT TIME ZONE 'UTC')
GROUP BY 1";

    /* #6009: the module a request waits on, for the evidence section's name. $1 the request. */
    internal const string FollowUpConditionSql = @"
SELECT follow_up_condition FROM config.investigation_requests WHERE request_id = $1";

    /* #6008: the follow-up request copies its source's route, server, family, metric, story path, alert times, life and level, and
       names the source in follow_up_of, as a practice request names its case in practice_of. $1 new id, $2 source, $3 now. */
    internal const string OpenFollowUpSql = @"
INSERT INTO config.investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, story_path_hash, first_alert_at, last_alert_at,
     window_until, opened_at, expires_at, lease_minutes, autonomy_level, alert_count, max_severity, follow_up_of)
SELECT $1, r.route_id, r.server_id, r.server_name, r.family, r.first_metric, r.story_path_hash, r.first_alert_at, r.last_alert_at,
       r.window_until, $3, $3 + (r.expires_at - r.opened_at), r.lease_minutes, r.autonomy_level, r.alert_count, r.max_severity, r.request_id
FROM config.investigation_requests AS r
WHERE r.request_id = $2
RETURNING route_id, server_id";

    /* #6008: the source's alert rows, without the dedup key, so the brief's alerts section reads what the source's read. $1 new, $2 source. */
    internal const string CopyFollowUpAlertsSql = @"
INSERT INTO config.investigation_request_alerts
    (request_id, alert_time, server_id, metric_name, dedup_key, current_value, threshold_value, muted, detail_text, context_json, severity)
SELECT $1, alert_time, server_id, metric_name, NULL, current_value, threshold_value, muted, detail_text, context_json, severity
FROM config.investigation_request_alerts
WHERE request_id = $2";

    internal const string ReportInsertSql = @"
INSERT INTO config.investigation_reports
    (request_id, report_format, agent_id, claim_id, submitted_at, verdict, root_cause, root_cause_found, summary, evidence,
     server_id, metric_name, dedup_key, story_path_hash, family_of, queries_run)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13, $14, $15, $16::jsonb)";

    /* #6004: a report's family_of must name a request; the tool checks it with this before the submit. */
    internal const string RequestExistsSql = @"
SELECT 1 FROM config.investigation_requests WHERE request_id = $1";

    internal const string ReportedSql = @"
UPDATE config.investigation_requests
SET state = 'reported', reported_at = $2, lease_until = NULL
WHERE request_id = $1";

    internal const string SetBriefSql = @"
UPDATE config.investigation_requests
SET brief_format = $3, brief = $4::jsonb
WHERE request_id = $1 AND claim_id = $2 AND state = 'claimed'";

    internal const string ReleaseSql = @"
UPDATE config.investigation_requests
SET state = 'open', claimed_by = NULL, claim_id = NULL, claimed_at = NULL, lease_until = NULL
WHERE request_id = $1 AND claim_id = $2 AND state = 'claimed'";

    /* #5999: the one definition of the registry names an investigation request answers with: the display name falls back to the
       registry's server name and then to the name the request was opened with (so it is never null: that column is NOT NULL), and
       the instance name is the registry's server name, null once the server has left the registry. <paramref name="requests"/> is
       the alias of config.investigation_requests and <paramref name="registry"/> the alias of the LEFT JOINed collect.servers. The
       three reads here and the brief reader alias this text, so the list, the reports, the long-poll and the brief cannot disagree
       about the fallback. Read as the last two ordinals of each statement. */
    public static string RegistryNamesSelect(string requests, string registry) =>
        $"COALESCE({registry}.display_name, {registry}.server_name, {requests}.server_name) AS registry_display_name, "
        + $"{registry}.server_name AS registry_instance_name";

    /* #6551: a change-watch request's alert detail (the text its first alert row carries), NULL for every other request. A
       change-watch request has no row in the alert history, so this is what lets the list and the wait answer triage it before a
       claim. <paramref name="requests"/> is the alias of config.investigation_requests. */
    internal static string ChangeWatchDetailSelect(string requests) =>
        $"CASE WHEN {requests}.trigger_kind = '{InvestigationTriggerKinds.ChangeWatch}' THEN "
        + $"(SELECT a.detail_text FROM config.investigation_request_alerts AS a WHERE a.request_id = {requests}.request_id "
        + "ORDER BY a.alert_time, a.dedup_key LIMIT 1) END AS change_watch_detail";

    /* #5822, #6003: the long-poll's catch-up read. Every request event (InvestigationEvents.Kinds, $4) journaled after $1 on the
       routes in $2 (all routes when NULL), oldest first, whatever the request's state now: it answers what happened, and the
       claim decides what is still claimable. An open is its request's "opened" row. */
    internal static readonly string OpenedSinceSql = $@"
SELECT r.request_id, r.route_id, r.server_name, r.family, r.opened_at,
       {RegistryNamesSelect("r", "sv")},
       j.event_id, j.event, j.event_time, j.detail::text, r.practice_of,
       {ChangeWatchDetailSelect("r")}
FROM config.investigation_journal AS j
JOIN config.investigation_requests AS r ON r.request_id = j.request_id
LEFT JOIN collect.servers AS sv ON sv.server_id = r.server_id
WHERE j.event_time > $1
  AND j.event = ANY ($4)
  AND ($2::integer[] IS NULL OR r.route_id = ANY ($2))
ORDER BY j.event_time, j.event_id
LIMIT $3";

    /* #6003: the events not yet delivered to their agent route, journaled after $1, oldest first. An event counts as delivered
       once its delivery mark (once key delivered:<event_id>) exists, so a replay of the same event id is never sent again. */
    internal const string PendingDeliverySql = @"
SELECT j.journal_id, j.event_id, j.event, j.event_time, r.request_id, r.route_id, r.server_id, r.server_name, r.family,
       j.reason, j.detail::text, r.practice_of
FROM config.investigation_journal AS j
JOIN config.investigation_requests AS r ON r.request_id = j.request_id
WHERE j.event_time > $1
  AND j.event = ANY ($2)
  AND NOT EXISTS (SELECT 1 FROM config.investigation_journal AS d
                  WHERE d.once_key = 'delivered:' || j.event_id::text)
ORDER BY j.journal_id
LIMIT $3";

    /* #6003, D8: requests that expired with no agent holding them, journaled after $1, whose fallback has not been sent. */
    internal const string UnclaimedExpiriesSql = @"
SELECT r.request_id, r.route_id, r.server_id, r.server_name, r.family, r.first_metric, r.opened_at, j.event_time
FROM config.investigation_journal AS j
JOIN config.investigation_requests AS r ON r.request_id = j.request_id
WHERE j.event = 'expired'
  AND j.event_time > $1
  AND r.practice_of IS NULL
  AND COALESCE((j.detail ->> 'unclaimed')::boolean, false)
  AND NOT EXISTS (SELECT 1 FROM config.investigation_journal AS f
                  WHERE f.once_key = 'fallback:' || r.request_id::text)
ORDER BY j.journal_id
LIMIT $2";

    /* #6003, D7: a live request is resolved when EVERY metric linked to it has a resolution row after that metric's last
       alert on the same server. A resolution row is stored under its title ("CPU Resolved"); its context_json names the
       metric it resolves (Resolves, written by DarlingSelfAlertEvaluator.BuildResolutionRecord on every clear, whatever the
       route's SendResolutions says). The member is NOT matched as text (#6003 review round 2): every alert-log write goes
       through DerivedStoreScrub, which re-serializes a context it changed with an encoder that escapes < > & ' + and every
       non-ASCII character, so no one encoding of a metric name is the stored one. The candidate rows come back and C# parses
       each context_json (System.Text.Json reads every escaping); one a text column holds but jsonb refuses (a NUL
       escape) is no problem there. The prefilter is the member's key, which no encoder escapes. A metric with no candidate
       comes back once with a NULL context, so a request with an uncleared metric is seen as unresolved.
       The bound on h.alert_time is the oldest last alert among the candidates. The plan still scans the log; the bound saves
       the per-row work (the Resolves test and the join) on every older row, about 44 ms a pass on a store with 30,000 alert
       rows (warm, and the pass runs once a minute). A live follow-up can pull the oldest last alert back by up to 30 days, so
       the saving is smaller while one is open. It drops no candidate: every resolution row a candidate can use is after that
       candidate's last alert, so after the oldest one. The bound is NOT the clock minus a fixed window:
       a follow-up opens now but copies its source's alert rows with their original times, so its last alert can be weeks old
       (follow_up_at may be 30 days out), and a fixed window would hide a clear older than it and lose the resolved event. */
    internal const string ResolveCandidatesSql = @"
WITH m AS (SELECT a.request_id, a.server_id, a.metric_name, max(a.alert_time) AS last_alert
           FROM config.investigation_request_alerts AS a
           WHERE a.request_id IN (SELECT r.request_id
                                  FROM config.investigation_requests AS r
                                  WHERE r.state IN ('open', 'claimed', 'reported')
                                    AND r.practice_of IS NULL
                                    AND r.opened_at > $1 - interval '2 days')
           GROUP BY a.request_id, a.server_id, a.metric_name)
SELECT m.request_id, m.metric_name, h.context_json
FROM m
LEFT JOIN config.config_alert_log AS h
       ON h.server_id = m.server_id
      AND h.alert_time > m.last_alert
      AND h.alert_time > (SELECT min(last_alert) FROM m)
      AND strpos(h.context_json, 'Resolves') > 0";

    /* Journals resolved once per request (once key resolved:<request_id>). The state is checked again: the candidates were
       read a moment ago. */
    internal const string ResolveInsertSql = @"
INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, once_key)
SELECT $1, r.request_id, r.route_id, r.server_id, 'resolved', 'service', 'resolved:' || r.request_id::text
FROM config.investigation_requests AS r
WHERE r.request_id = ANY($2)
  AND r.state IN ('open', 'claimed', 'reported')
  AND r.practice_of IS NULL
ON CONFLICT (once_key) WHERE once_key IS NOT NULL DO NOTHING";

    internal static readonly string ListSql = $@"
SELECT request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, alert_count,
       effective_state, opened_at, expires_at, claimed_by, lease_until, claim_count, reported_at, practice_of,
       registry_display_name, registry_instance_name, change_watch_detail
FROM (
    SELECT r.*,
           CASE
               WHEN r.state IN ('open', 'claimed') AND r.expires_at <= $1 THEN 'expired'
               WHEN r.state = 'claimed' AND r.lease_until < $1 AND r.claim_count >= 3 THEN 'expired'
               WHEN r.state = 'open' AND r.claim_count >= 3 THEN 'expired'
               WHEN r.state = 'claimed' AND r.lease_until < $1 THEN 'open'
               ELSE r.state
           END AS effective_state,
           {RegistryNamesSelect("r", "sv")},
           {ChangeWatchDetailSelect("r")}
    FROM config.investigation_requests AS r
    LEFT JOIN collect.servers AS sv ON sv.server_id = r.server_id
    WHERE ($3::integer IS NULL OR r.server_id = $3)
      AND ($4::text IS NULL OR r.family = $4)
) AS s
WHERE ($2::text IS NULL OR s.effective_state = $2)
ORDER BY s.opened_at DESC, s.request_id
LIMIT $5";

    /* #5632 Phase A, L6: a claim made with the caller's own claim id renews its lease, capped at the request's expiry. */
    internal const string RenewSql = @"
UPDATE config.investigation_requests
SET lease_until = least($3 + make_interval(mins => lease_minutes), expires_at)
WHERE request_id = $1
  AND claim_id = $2
  AND state = 'claimed'
  AND lease_until >= $3
  AND expires_at > $3
RETURNING lease_until";

    internal const string ClaimHolderSql = @"
SELECT claimed_by
FROM config.investigation_requests
WHERE request_id = $1
  AND claim_id = $2";

    /* get_notification_routes: each route's use, the requests it opened in the last hour and day (the limits' windows). */
    internal const string RouteUseSql = @"
SELECT route_id,
       count(*) FILTER (WHERE opened_at > $1 - interval '1 hour'),
       count(*)
FROM config.investigation_requests
WHERE opened_at > $1 - interval '1 day'
  AND practice_of IS NULL
GROUP BY route_id";

    internal const string RefusalServerSql = @"
SELECT server_id
FROM config.investigation_requests
WHERE request_id = $1";

    /* The note an agent proposes with its report: pending until a person approves it. It carries the request's server,
       story path and first alert, so an approved note reaches the next brief of the same story. */
    internal const string ProposedNoteSql = @"
INSERT INTO config.investigation_notes (server_id, story_path_hash, dedup_key, metric_name, note_text, author, source_request_id)
SELECT r.server_id, r.story_path_hash, r.first_dedup_key, r.first_metric, $2, $3, r.request_id
FROM config.investigation_requests AS r
WHERE r.request_id = $1";

    /* #5903: the pending notes for the approve screen, with the request that proposed each (server name, family). A note past
       its expires_at is not listed: the brief read drops it (state = 'approved' AND expires_at > now), so an approval of it
       would do nothing. $1 the limit, $2 now. */
    internal const string PendingNotesSql = @"
SELECT n.note_id, n.server_id, r.server_name, n.metric_name, r.family, n.story_path_hash, n.note_text, n.author,
       n.source_request_id, n.created_at, n.expires_at
FROM config.investigation_notes AS n
LEFT JOIN config.investigation_requests AS r ON r.request_id = n.source_request_id
WHERE n.state = 'pending' AND n.expires_at > $2
ORDER BY n.created_at, n.note_id
LIMIT $1";

    /* #5903: a decision lands only on a pending note, so a second click or a second viewer changes nothing. $1 note,
       $2 the new state, $3 who. The time is the store's clock (now()), returned for the journal row. */
    internal const string DecideNoteSql = @"
UPDATE config.investigation_notes
SET state = $2, decided_by = $3, decided_at = now()
WHERE note_id = $1 AND state = 'pending'
RETURNING source_request_id, server_id, decided_at";

    internal static string ReportsRequestsSql => ReportsRequestsSelect + @"
WHERE ($2::uuid IS NULL OR r.request_id = $2)
  AND ($3::integer IS NULL OR r.server_id = $3)
  AND ($2::uuid IS NOT NULL OR r.opened_at >= $4)
ORDER BY r.opened_at DESC, r.request_id
LIMIT $5";

    /// <summary>The request columns <see cref="ReportsRequestRow"/> reads, shared by the report read and the feedback export (#6236).
    /// $1 is now, as the state is computed.</summary>
    internal static readonly string ReportsRequestsSelect = $@"
SELECT r.request_id, r.route_id, r.server_id, r.server_name, r.family, r.first_metric, r.first_alert_at, r.last_alert_at,
       r.alert_count,
       CASE
           WHEN r.state IN ('open', 'claimed') AND r.expires_at <= $1 THEN 'expired'
           WHEN r.state = 'claimed' AND r.lease_until < $1 AND r.claim_count >= 3 THEN 'expired'
           WHEN r.state = 'open' AND r.claim_count >= 3 THEN 'expired'
           WHEN r.state = 'claimed' AND r.lease_until < $1 THEN 'open'
           ELSE r.state
       END AS effective_state,
       r.opened_at, r.expires_at, r.claimed_by, r.lease_until, r.claim_count, r.reported_at, r.brief::text, r.practice_of,
       {RegistryNamesSelect("r", "sv")}
FROM config.investigation_requests AS r
LEFT JOIN collect.servers AS sv ON sv.server_id = r.server_id";

    /* #5918: the one definition of "a LIVE practice request": a practice request that reads open or claimed (the computed state,
       as ListSql computes it), so a submitted, released-to-expiry or expired practice is not live. It returns
       (request_id, practice_of, opened_at). <paramref name="nowParameter"/> is the positional parameter that holds now ($1).
       The hider (LivePracticeReplaysSql), the brief reader's earlier reports and links, and the practice store's "already live"
       check all alias this text, so they cannot disagree about what is live. */
    public static string LivePracticeRequestsSql(string nowParameter) => $@"
SELECT s.request_id, s.practice_of, s.opened_at
FROM (
    SELECT r.request_id, r.practice_of, r.opened_at,
           CASE
               WHEN r.state IN ('open', 'claimed') AND r.expires_at <= {nowParameter} THEN 'expired'
               WHEN r.state = 'claimed' AND r.lease_until < {nowParameter} AND r.claim_count >= 3 THEN 'expired'
               WHEN r.state = 'open' AND r.claim_count >= 3 THEN 'expired'
               WHEN r.state = 'claimed' AND r.lease_until < {nowParameter} THEN 'open'
               ELSE r.state
           END AS effective_state
    FROM config.investigation_requests AS r
    WHERE r.practice_of IS NOT NULL
) AS s
WHERE s.effective_state IN ('open', 'claimed')";

    /* #5918: the cases a LIVE practice request replays. $1 now. */
    internal static readonly string LivePracticeReplaysSql =
        "SELECT DISTINCT lp.practice_of FROM (" + LivePracticeRequestsSql("$1") + ") AS lp";

    internal const string ReportsSql = @"
SELECT request_id, report_format, agent_id, submitted_at, verdict, root_cause, root_cause_found, summary, evidence::text,
       family_of, queries_run::text
FROM config.investigation_reports
WHERE request_id = ANY($1)
ORDER BY submitted_at DESC";

    /* #5914: a report's links are its request's link_recorded journal rows, read whole (at most MaxLinks per request), so
       the journal page's limit never drops one. ReportsJournalSql excludes the event for the same reason: these rows are
       served in full here, and leaving them in the capped newest-first page would spend it twice on one fact (a link sorts
       above its own reported row, so 100 reported requests with two links each would fill a 200-row page with links alone). */
    internal const string ReportLinksSql = @"
SELECT request_id, detail::text
FROM config.investigation_journal
WHERE request_id = ANY($1)
  AND event = 'link_recorded'
ORDER BY journal_id";

    internal const string ReportsJournalSql = @"
SELECT event_time, request_id, server_id, event, actor, reason, detail::text
FROM config.investigation_journal
WHERE request_id = ANY($1)
  AND event <> 'link_recorded'
ORDER BY event_time DESC, journal_id DESC
LIMIT $2";

    internal const string SweepLapsedSql = @"
INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason, once_key)
SELECT $1, r.request_id, r.server_id, 'lease_expired', 'service', r.claimed_by, 'lease_expired:' || r.claim_id::text
FROM config.investigation_requests AS r
WHERE r.state = 'claimed' AND r.lease_until < $1
ON CONFLICT (once_key) WHERE once_key IS NOT NULL DO NOTHING";

    /* #6003: the row names the request's route, and its detail says whether an agent held the request when it expired
       (unclaimed), which is what sends the D8 fallback. A claimed request whose lease lapsed counts as unclaimed: the lapse
       is journaled but leaves the state claimed, and a reported request is never due, so no agent was working on it. */
    internal const string SweepExpireSql = @"
WITH due AS (
    SELECT request_id, state = 'open' OR (state = 'claimed' AND lease_until < $1) AS unclaimed
    FROM config.investigation_requests
    WHERE (state IN ('open', 'claimed') AND expires_at <= $1)
       OR (state = 'claimed' AND lease_until < $1 AND claim_count >= 3)
       OR (state = 'open' AND claim_count >= 3)
    FOR UPDATE
),
expired AS (
    UPDATE config.investigation_requests AS r
    SET state = 'expired'
    FROM due AS d
    WHERE r.request_id = d.request_id
    RETURNING r.request_id, r.route_id, r.server_id, r.expires_at > $1 AS exhausted, d.unclaimed
)
INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, detail, once_key)
SELECT $1, e.request_id, e.route_id, e.server_id, CASE WHEN e.exhausted THEN 'claims_exhausted' ELSE 'expired' END, 'service',
       jsonb_build_object('unclaimed', e.unclaimed), 'expired:' || e.request_id::text
FROM expired AS e
ON CONFLICT (once_key) WHERE once_key IS NOT NULL DO NOTHING";

    private readonly NpgsqlDataSource _dataSource;
    private readonly NpgsqlDataSource? _agentActionSource;
    private readonly int _commandTimeoutSeconds;
    private readonly Func<DateTime> _utcNow;

    /// <summary>The data source <see cref="RunAgentActionAsync"/> runs on: the store owner's, when the caller gave one. The journal rows an agent
    /// action writes (<see cref="AgentActionEvents"/>) are the owner's alone, so the whole change, its checks and its rows, runs on that login.</summary>
    private NpgsqlDataSource AgentActionSource => _agentActionSource ?? _dataSource;

    /// <summary>
    /// <paramref name="dataSource"/> must connect as a role that may write the investigation tables: the store owner for
    /// open, join and the sweep; the owner or the <c>mcp</c> role for claim and submit. <paramref name="commandTimeoutSeconds"/>
    /// is applied to every command. <paramref name="utcNow"/> is the clock (tests move it); it defaults to the wall clock.
    /// <paramref name="agentActionSource"/> is the store owner's data source for <see cref="RunAgentActionAsync"/>: the service's MCP tools
    /// pass it, because their own <paramref name="dataSource"/> is the <c>mcp</c> login, which the journal's trigger keeps from writing the
    /// agent action rows (#6569). Null runs the change on <paramref name="dataSource"/>.
    /// </summary>
    public InvestigationRequestStore(NpgsqlDataSource dataSource, int commandTimeoutSeconds, Func<DateTime>? utcNow = null,
        NpgsqlDataSource? agentActionSource = null)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _agentActionSource = agentActionSource;
        _commandTimeoutSeconds = commandTimeoutSeconds;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    private DateTime Now() => DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc);

    /// <summary>
    /// Opens a request for <paramref name="alert"/>, joins it to an open or claimed request (or a reported one, #6778) of the same server and family,
    /// or refuses at the route's limit, in one transaction. See the class remarks for the locks.
    /// </summary>
    /// <remarks>
    /// <para><b>The join is bounded by the window</b> (#5632 review round 1, M2): an alert joins only a request whose state is
    /// <c>open</c> or <c>claimed</c> and that opened at most <see cref="InvestigationRules.GroupingWindow"/> ago. A dedup key never decides a join:
    /// the send path's key falls back to the constant <c>{server}:{metric}</c> for an alert with no incidents, so a key arm
    /// would join every later alert of that metric for the request's whole life, after its brief was composed. The keys are
    /// recorded as the request's alert rows only.</para>
    /// <para><b>A closed request is final. A reported request takes a quiet re-fire (#6778).</b> An alert joins a reported
    /// request only when its metric is already on the request, its severity is not above the request's <c>max_severity</c>, the
    /// request has no <c>resolved</c> journal row (#6003, D7), and the alert is inside the grouping window and the 4-hour cap.
    /// Such a join brings no new metric and no higher severity, so <see cref="InvestigationEvents.JoinIsEvent"/> is false and it
    /// is journaled <c>joined_quiet</c> (<see cref="InvestigationEvents.QuietJoin"/>), which no reader delivers. The state and the report do not change, and a join never counts
    /// against a limit. Anything else opens a NEW request, as before. The claim deadline (<c>expires_at</c>) applies to open and
    /// claimed requests only, because a reported request is past its claim and the 4-hour cap bounds it. The send path's
    /// cooldown cannot decide this: the request is opened or joined before the throttle runs, because the generic webhook's
    /// <c>{{investigation_json}}</c> needs it (#5821), so the store cannot know whether the page will be throttled.</para>
    /// </remarks>
    public async Task<InvestigationOpenResult> OpenOrJoinAsync(InvestigationAlert alert, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(alert);
        var now = Now();
        var alertTime = DateTime.SpecifyKind(alert.AlertTimeUtc, DateTimeKind.Utc);
        var keys = alert.DedupKeys.Where(k => !string.IsNullOrEmpty(k)).Distinct(StringComparer.Ordinal).ToArray();

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await ExecAsync(connection, transaction, GroupLockSql, cancellationToken,
            P(alert.ServerId), P(alert.Family), P(alert.ServerName), P(ExternalSourceOf(alert.MetricName)));

        var severity = AlertSeverityWord(alert.Severity);
        Guid? joinId = null;
        string? maxSeverity = null;
        var newMetric = false;
        /* #6234: a change-watch request is its own incident; it joins nothing, and FindJoinSql never finds one. */
        if (string.Equals(alert.TriggerKind, InvestigationTriggerKinds.Alert, StringComparison.Ordinal))
        {
            await using var find = Command(connection, transaction, FindJoinSql,
                P(alert.ServerId), P(alert.Family), P(now), P(alertTime), P(alert.MetricName), P(severity),
                P(alert.ServerName), P(ExternalSourceOf(alert.MetricName)));
            await using var found = await find.ExecuteReaderAsync(cancellationToken);
            if (await found.ReadAsync(cancellationToken))
            {
                joinId = found.GetGuid(0);
                maxSeverity = found.IsDBNull(1) ? null : found.GetString(1);
                newMetric = !found.GetBoolean(2);
            }
        }

        if (joinId is Guid join)
        {
            /* #6003, D6: a join is an event only when it brings a new metric or a higher severity; any other join is
               journaled as a quiet join, which neither reader delivers. */
            var evented = InvestigationEvents.JoinIsEvent(newMetric, maxSeverity, severity);
            var newMax = InvestigationEvents.SeverityRank(severity) > InvestigationEvents.SeverityRank(maxSeverity)
                ? severity
                : maxSeverity ?? severity;
            await ExecAsync(connection, transaction, JoinSql, cancellationToken, P(join), P(alertTime), P(alert.MetricName),
                P((short)alert.AutonomyLevel), P(newMax));
            await InsertAlertRowsAsync(connection, transaction, join, alert, alertTime, keys, cancellationToken);
            await JournalAsync(connection, transaction, now, join, alert.RouteId, alert.ServerId,
                evented ? InvestigationEvents.Joined : InvestigationEvents.QuietJoin, "service",
                alert.MetricName, evented ? JoinDetail(newMetric, severity) : null, null, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return new InvestigationOpenResult(InvestigationOpenOutcome.Joined, join) { Evented = evented };
        }

        await ExecAsync(connection, transaction, RouteLockSql, cancellationToken, P(RouteLockKey), P(alert.RouteId));

        long lastHour, lastDay;
        await using (var count = Command(connection, transaction, CountRouteSql, P(alert.RouteId), P(now)))
        await using (var reader = await count.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            lastHour = reader.GetInt64(0);
            lastDay = reader.GetInt64(1);
        }

        if (lastHour >= alert.MaxPerHour || lastDay >= alert.MaxPerDay)
        {
            var which = lastHour >= alert.MaxPerHour ? "hourly" : "daily";
            var detail = string.Create(CultureInfo.InvariantCulture,
                $"{{\"limit\":\"{which}\",\"last_hour\":{lastHour},\"last_day\":{lastDay},\"max_per_hour\":{alert.MaxPerHour},\"max_per_day\":{alert.MaxPerDay}}}");
            var onceKey = string.Create(CultureInfo.InvariantCulture, $"limit_reached:{alert.RouteId}:{now:yyyy-MM-ddTHH}");
            await JournalAsync(connection, transaction, now, null, alert.RouteId, alert.ServerId, "limit_reached", "service",
                which, detail, onceKey, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return new InvestigationOpenResult(InvestigationOpenOutcome.LimitReached, null);
        }

        var requestId = Guid.NewGuid();
        await ExecAsync(connection, transaction, OpenSql, cancellationToken,
            P(requestId), P(alert.RouteId), P(alert.ServerId), P(alert.ServerName), P(alert.Family), P(alert.MetricName),
            PNullable(keys.FirstOrDefault()), PNullable(alert.StoryPathHash), P(alertTime), P(now), P(alert.ExpiryMinutes),
            P(alert.LeaseMinutes), P((short)alert.AutonomyLevel), P(severity), P(alert.TriggerKind));
        await InsertAlertRowsAsync(connection, transaction, requestId, alert, alertTime, keys, cancellationToken);
        await JournalAsync(connection, transaction, now, requestId, alert.RouteId, alert.ServerId, "opened", "service",
            alert.MetricName, null, null, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new InvestigationOpenResult(InvestigationOpenOutcome.Opened, requestId) { Evented = true };
    }

    /// <summary>The stored severity word: critical, or warning for anything else (null included).</summary>
    private static string AlertSeverityWord(string? severity) =>
        string.Equals(severity, InvestigationEvents.Critical, StringComparison.Ordinal) ? InvestigationEvents.Critical : InvestigationEvents.Warning;

    /// <summary>What a <c>joined</c> event says it brought: a new metric, a higher severity, or both.</summary>
    private static string JoinDetail(bool newMetric, string severity) =>
        "{\"new_metric\":" + (newMetric ? "true" : "false") + ",\"severity\":\"" + severity + "\"}";

    /// <summary>
    /// Claims <paramref name="requestId"/> for <paramref name="agentId"/> under a new claim id with one conditional UPDATE.
    /// When no row returns, one SELECT says why, and the refusal is journaled once per request and reason (an unknown id
    /// once per hour). An agent id <see cref="InvestigationRules.IsValidAgentId"/> refuses (journaled once per request under
    /// <see cref="InvestigationRules.InvalidAgentId"/>), or a request whose agent journal is full (<see cref="InvestigationRules.MaxAgentJournalRows"/>), is
    /// refused first. The <c>claimed</c> row's reason is null (#5632 review round 2, L4): the claim id is a lease identity,
    /// so it lives only in that row's <c>once_key</c>, which no reader returns.
    /// </summary>
    public Task<InvestigationClaimResult> ClaimAsync(Guid requestId, string agentId, CancellationToken cancellationToken = default) =>
        ClaimAsync(requestId, agentId, allowReclaim: false, cancellationToken);

    /// <summary>The claim, with <paramref name="allowReclaim"/> (#6576, #6586 review): true only when <paramref name="agentId"/> is bound to the caller's named agent token (#6542),
    /// so the same agent id holding the live lease gets its claim id back. An agent id on the legacy token is free text, and claimed_by is
    /// published by list_investigation_requests, so there the same call answers already_claimed.</summary>
    public async Task<InvestigationClaimResult> ClaimAsync(Guid requestId, string agentId, bool allowReclaim, CancellationToken cancellationToken = default)
    {
        if (!InvestigationRules.IsValidAgentId(agentId))
        {
            await RecordRefusalAsync(requestId, InvestigationRules.InvalidAgentId, "claim", "agent_id_invalid", null, cancellationToken);
            return new InvestigationClaimResult(InvestigationClaimOutcome.AgentIdInvalid, null, null, 0, "agent_id_invalid");
        }

        var now = Now();
        var claimId = Guid.NewGuid();
        var actor = "agent:" + agentId;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        /* #6576: before the journal cap, like a renewal: it writes no journal row. The same agent id that holds the live lease gets
           its claim id back; any other agent id falls through to the claim below and is refused as before. */
        if (allowReclaim)
        {
            await using (var reclaim = Command(connection, transaction, ReclaimSql, P(requestId), P(agentId), P(now)))
            await using (var reader = await reclaim.ExecuteReaderAsync(cancellationToken))
            {
                if (await reader.ReadAsync(cancellationToken) && !reader.IsDBNull(0))
                {
                    var heldClaim = reader.GetGuid(0);
                    var heldLease = DateTime.SpecifyKind(reader.GetFieldValue<DateTime>(1), DateTimeKind.Utc);
                    var heldCount = reader.GetInt32(2);
                    await reader.DisposeAsync();
                    await transaction.CommitAsync(cancellationToken);
                    return new InvestigationClaimResult(InvestigationClaimOutcome.Reclaimed, heldClaim, heldLease, heldCount, null);
                }
            }
        }

        if (await JournalCappedAsync(connection, transaction, now, requestId, actor, "claim", cancellationToken))
        {
            await transaction.CommitAsync(cancellationToken);
            return new InvestigationClaimResult(InvestigationClaimOutcome.JournalCapped, null, null, 0, "journal_capped");
        }

        await using (var claim = Command(connection, transaction, ClaimSql, P(requestId), P(agentId), P(claimId), P(now)))
        await using (var reader = await claim.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                var leaseUntil = reader.GetFieldValue<DateTime>(0);
                var claimCount = reader.GetInt32(1);
                var serverId = reader.GetInt32(2);
                await reader.DisposeAsync();
                await JournalAsync(connection, transaction, now, requestId, null, serverId, "claimed", actor,
                    null, null, "claimed:" + claimId.ToString(), cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return new InvestigationClaimResult(InvestigationClaimOutcome.Claimed, claimId, leaseUntil, claimCount, null);
            }
        }

        InvestigationClaimOutcome outcome;
        string reason;
        int count = 0;
        int? server = null;
        await using (var why = Command(connection, transaction, ClaimWhySql, P(requestId)))
        await using (var reader = await why.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                outcome = InvestigationClaimOutcome.NotFound;
                reason = "not_found";
            }
            else
            {
                var state = reader.GetString(0);
                DateTime? leaseUntil = reader.IsDBNull(1) ? null : reader.GetFieldValue<DateTime>(1);
                count = reader.GetInt32(2);
                var expiresAt = reader.GetFieldValue<DateTime>(3);
                server = reader.GetInt32(4);
                (outcome, reason) = state switch
                {
                    "claimed" when leaseUntil >= now && expiresAt > now => (InvestigationClaimOutcome.AlreadyClaimed, "already_claimed"),
                    "reported" => (InvestigationClaimOutcome.NotOpen, "reported"),
                    "closed" => (InvestigationClaimOutcome.NotOpen, "closed"),
                    _ when expiresAt <= now || state == "expired" => (InvestigationClaimOutcome.NotOpen, "expired"),
                    _ when count >= InvestigationRules.MaxClaims => (InvestigationClaimOutcome.NotOpen, "claim_limit"),
                    _ => (InvestigationClaimOutcome.NotOpen, state),
                };
            }
        }

        /* A refusal for an unknown id names no request row, so no request's cap bounds it: it is journaled once per hour
           (#5632 review round 1, L3), not once per guessed id. */
        var onceKey = outcome == InvestigationClaimOutcome.NotFound
            ? string.Create(CultureInfo.InvariantCulture, $"refused:claim:not_found:{now:yyyy-MM-ddTHH}")
            : "refused:claim:" + requestId.ToString() + ":" + reason;
        await JournalAsync(connection, transaction, now, outcome == InvestigationClaimOutcome.NotFound ? null : requestId, null, server,
            "refused", actor, "claim:" + reason, null, onceKey, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new InvestigationClaimResult(outcome, null, null, count, reason);
    }

    /// <summary>
    /// Stores the brief composed for the current claim. False when <paramref name="claimId"/> no longer holds the request.
    /// </summary>
    public async Task<bool> SetBriefAsync(Guid requestId, Guid claimId, short briefFormat, string briefJson, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(briefJson);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, SetBriefSql, P(requestId), P(claimId), P(briefFormat), P(briefJson));
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    /// <summary>
    /// Returns a request whose claim could not be served (the brief failed to compose) to <c>open</c>. The claim stays
    /// counted (#5632 review round 1, L4): a brief that keeps failing cannot claim and release forever; after the third
    /// claim the claim UPDATE refuses and the sweep expires the request, journaling <c>claims_exhausted</c> once.
    /// False when <paramref name="claimId"/> no longer holds the request.
    /// </summary>
    public async Task<bool> ReleaseAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, ReleaseSql, P(requestId), P(claimId));
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    /// <summary>
    /// Accepts the current claim's report while the request is claimed (lease live or lapsed) or expired under that claim,
    /// until a newer claim supersedes it. A refusal is journaled once per request and reason, never per claim id, so guessed
    /// claim ids cannot fill the request's agent journal (#5632 review round 2, L1; an unknown id once per hour). An agent id
    /// <see cref="InvestigationRules.IsValidAgentId"/> refuses, or a request whose agent journal is full (<see cref="InvestigationRules.MaxAgentJournalRows"/>),
    /// is refused first.
    /// </summary>
    public async Task<InvestigationSubmitOutcome> SubmitAsync(InvestigationReportSubmission report, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(report);
        if (!InvestigationRules.IsValidAgentId(report.AgentId))
        {
            await RecordRefusalAsync(report.RequestId, InvestigationRules.InvalidAgentId, "submit", "agent_id_invalid", null, cancellationToken);
            return InvestigationSubmitOutcome.AgentIdInvalid;
        }

        /* #5914: the tool layer's link rule again, before any write. */
        InvestigationReportLinks.ThrowIfInvalid(report.Links, nameof(report));
        /* #6004: and the queries_run caps, never clamped. */
        InvestigationReportFields.ThrowIfInvalid(report.QueriesRunJson, nameof(report));
        var now = Now();

        var actor = "agent:" + report.AgentId;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        string? state = null;
        Guid? currentClaim = null;
        int serverId = 0;
        string? metric = null, dedupKey = null, story = null;
        var routeId = 0;
        await using (var command = Command(connection, transaction, SubmitLockSql, P(report.RequestId)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                state = reader.GetString(0);
                currentClaim = reader.IsDBNull(1) ? null : reader.GetGuid(1);
                serverId = reader.GetInt32(2);
                metric = reader.GetString(3);
                dedupKey = reader.IsDBNull(4) ? null : reader.GetString(4);
                story = reader.IsDBNull(5) ? null : reader.GetString(5);
                routeId = reader.GetInt32(6);
            }
        }

        /* After the row lock, so concurrent submits on one request count in turn. */
        if (state is not null
            && await JournalCappedAsync(connection, transaction, now, report.RequestId, actor, "submit", cancellationToken))
        {
            await transaction.CommitAsync(cancellationToken);
            return InvestigationSubmitOutcome.JournalCapped;
        }

        var outcome = state switch
        {
            null => InvestigationSubmitOutcome.NotFound,
            "reported" => InvestigationSubmitOutcome.AlreadyReported,
            "closed" => InvestigationSubmitOutcome.NotOpen,
            _ when currentClaim != report.ClaimId => InvestigationSubmitOutcome.LeaseLost,
            "claimed" or "expired" => InvestigationSubmitOutcome.Reported,
            _ => InvestigationSubmitOutcome.LeaseLost,
        };

        if (outcome != InvestigationSubmitOutcome.Reported)
        {
            var reason = outcome switch
            {
                InvestigationSubmitOutcome.NotFound => "not_found",
                InvestigationSubmitOutcome.AlreadyReported => "already_reported",
                InvestigationSubmitOutcome.NotOpen => "closed",
                _ => "lease_lost",
            };
            /* An unknown id: once per hour, as for a claim. A known request's refusals are bounded by its agent cap. */
            var onceKey = state is null
                ? string.Create(CultureInfo.InvariantCulture, $"refused:submit:not_found:{now:yyyy-MM-ddTHH}")
                : "refused:submit:" + report.RequestId.ToString() + ":" + reason;
            await JournalAsync(connection, transaction, now, state is null ? null : report.RequestId, null, state is null ? null : serverId,
                "refused", actor, "submit:" + reason, null, onceKey, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return outcome;
        }

        /* #6008: a follow-up time over the route's pending cap refuses the whole report, before anything is written. The route lock
           makes concurrent submits on one route count in turn. */
        if (report.FollowUpAtUtc is not null)
        {
            await ExecAsync(connection, transaction, RouteLockSql, cancellationToken, P(RouteLockKey), P(routeId));
            long pending;
            await using (var count = Command(connection, transaction, PendingFollowUpsSql, P(routeId)))
            {
                pending = (long)(await count.ExecuteScalarAsync(cancellationToken))!;
            }

            if (pending >= InvestigationReportFields.MaxPendingFollowUpsPerRoute)
            {
                return InvestigationSubmitOutcome.FollowUpLimit;
            }
        }

        await ExecAsync(connection, transaction, ReportInsertSql, cancellationToken,
            P(report.RequestId), P(report.ReportFormat), P(report.AgentId), P(report.ClaimId), P(now), P(report.Verdict),
            P(report.RootCause), P(report.RootCauseFound), PNullable(report.Summary), P(report.EvidenceJson), P(serverId),
            PNullable(metric), PNullable(dedupKey), PNullable(story), new NpgsqlParameter<Guid?> { TypedValue = report.FamilyOf, NpgsqlDbType = NpgsqlDbType.Uuid }, PNullable(report.QueriesRunJson));
        if (!string.IsNullOrWhiteSpace(report.ProposedNote))
        {
            await ExecAsync(connection, transaction, ProposedNoteSql, cancellationToken,
                P(report.RequestId), P(report.ProposedNote), P(actor));
        }

        await ExecAsync(connection, transaction, ReportedSql, cancellationToken, P(report.RequestId), P(now));
        if (report.FollowUpAtUtc is { } followUpAt)
        {
            await ExecAsync(connection, transaction, SetFollowUpSql, cancellationToken, P(report.RequestId), P(followUpAt));
            if (report.FollowUpCondition is { } condition)
            {
                await ExecAsync(connection, transaction, SetFollowUpConditionSql, cancellationToken, P(report.RequestId), P(condition));
            }
        }

        await JournalAsync(connection, transaction, now, report.RequestId, null, serverId, "reported", actor,
            report.Verdict, null, null, cancellationToken);
        /* #5914: each link is a link_recorded row under the agent's actor, once per request and URL (the once-key). A
           report is stored once per request, so a request holds at most MaxLinks of them. */
        foreach (var link in InvestigationReportLinks.Distinct(report.Links ?? []))
        {
            await JournalAsync(connection, transaction, now, report.RequestId, null, serverId, InvestigationReportLinks.JournalEvent,
                actor, link.Kind, InvestigationReportLinks.DetailJson(link), InvestigationReportLinks.OnceKey(report.RequestId, link.Url),
                cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return InvestigationSubmitOutcome.Reported;
    }

    /// <summary>True when a request with <paramref name="requestId"/> exists (#6004: what a report's <c>family_of</c> must
    /// name). Writes nothing.</summary>
    /// <inheritdoc />
    public async Task<bool> RequestExistsAsync(Guid requestId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, RequestExistsSql, P(requestId));
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <summary>
    /// Renews the lease of <paramref name="claimId"/> on <paramref name="requestId"/> (#5632 Phase A): only while that claim
    /// holds the request with a live lease, by the route's lease length, never past the request's expiry. Not renewed (a
    /// lapsed lease, a newer claim, a reported or expired request): the caller claims afresh.
    /// </summary>
    public async Task<InvestigationRenewResult> RenewAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, RenewSql, P(requestId), P(claimId), P(Now()));
        var lease = await command.ExecuteScalarAsync(cancellationToken);
        return lease is DateTime until
            ? new InvestigationRenewResult(true, DateTime.SpecifyKind(until, DateTimeKind.Utc))
            : new InvestigationRenewResult(false, null);
    }

    /// <summary>The agent that holds <paramref name="claimId"/> on <paramref name="requestId"/>, or null when that claim
    /// does not hold it (the submit then answers lease_lost or not_found from the store). Writes nothing.</summary>
    public async Task<string?> ClaimHolderAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, ClaimHolderSql, P(requestId), P(claimId));
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    /// <summary>Each route's requests opened in the last hour and the last day, by route id; a route that opened none is
    /// absent. Writes nothing.</summary>
    public async Task<IReadOnlyDictionary<int, (int LastHour, int LastDay)>> RouteUseAsync(CancellationToken cancellationToken = default)
    {
        var use = new Dictionary<int, (int LastHour, int LastDay)>();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, RouteUseSql, P(Now()));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            use[reader.GetInt32(0)] = ((int)reader.GetInt64(1), (int)reader.GetInt64(2));
        }

        return use;
    }

    /// <summary>
    /// #6596: journals an alert on an agent route that opened no request because its story is recurring at this hour
    /// (<see cref="InvestigationEvents.RecurringSkipped"/>). The row has no request id; it names the route, the server, the metric
    /// (as the reason) and the story path hash, once per route, server, metric and alert hour.
    /// </summary>
    public async Task RecordRecurringSkipAsync(InvestigationAlert alert, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(alert);
        var alertTime = DateTime.SpecifyKind(alert.AlertTimeUtc, DateTimeKind.Utc);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await JournalAsync(connection, transaction, Now(), null, alert.RouteId, alert.ServerId, InvestigationEvents.RecurringSkipped,
            "service", alert.MetricName, InvestigationEvents.RecurringSkippedDetail(alert.StoryPathHash, alertTime),
            InvestigationEvents.RecurringSkippedOnceKey(alert.RouteId, alert.ServerId, alert.MetricName, alertTime), cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>
    /// Journals a refusal the tool layer decided (an invalid report, an action proposal at Level 1, an unsupported format)
    /// once per request, operation and reason (#5632 review ruling). An unknown request id is journaled once per hour per
    /// reason, as the store's own refusals are. An agent id <see cref="InvestigationRules.IsValidAgentId"/> refuses is journaled as
    /// <see cref="InvestigationRules.InvalidAgentId"/>.
    /// </summary>
    public async Task RecordRefusalAsync(Guid requestId, string agentId, string operation, string reason, string? detailJson,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operation);
        ArgumentException.ThrowIfNullOrEmpty(reason);
        if (!InvestigationRules.IsValidAgentId(agentId))
        {
            agentId = InvestigationRules.InvalidAgentId;
        }

        var now = Now();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        int? serverId = null;
        await using (var command = Command(connection, transaction, RefusalServerSql, P(requestId)))
        {
            if (await command.ExecuteScalarAsync(cancellationToken) is int server)
            {
                serverId = server;
            }
        }

        var onceKey = serverId is null
            ? string.Create(CultureInfo.InvariantCulture, $"refused:{operation}:not_found:{reason}:{now:yyyy-MM-ddTHH}")
            : "refused:" + operation + ":" + requestId.ToString() + ":" + reason;
        await JournalAsync(connection, transaction, now, serverId is null ? null : requestId, null, serverId, "refused",
            "agent:" + agentId, operation + ":" + reason, detailJson, onceKey, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>
    /// One request by id with its stored brief (#5821, the generic webhook's <c>{{investigation_json}}</c>), the state
    /// computed as <see cref="ListAsync"/> computes it; null when no request has that id. The request half of
    /// <see cref="ReadReportsAsync"/>, without its reports and journal. Writes nothing.
    /// </summary>
    public async Task<(InvestigationRequestRow Row, string? Brief)?> ReadRequestAsync(Guid requestId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, ReportsRequestsSql,
            P(Now()), new NpgsqlParameter<Guid?> { TypedValue = requestId, NpgsqlDbType = NpgsqlDbType.Uuid },
            new NpgsqlParameter<int?> { TypedValue = null, NpgsqlDbType = NpgsqlDbType.Integer }, P(DateTime.MinValue), P(1));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return (ReportsRequestRow(reader), reader.IsDBNull(16) ? null : reader.GetString(16));
    }

    private static InvestigationRequestRow ReportsRequestRow(NpgsqlDataReader reader) => new(
        reader.GetGuid(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetString(3), reader.GetString(4),
        reader.GetString(5), reader.GetFieldValue<DateTime>(6), reader.GetFieldValue<DateTime>(7), reader.GetInt32(8),
        reader.GetString(9), reader.GetFieldValue<DateTime>(10), reader.GetFieldValue<DateTime>(11),
        reader.IsDBNull(12) ? null : reader.GetString(12),
        reader.IsDBNull(13) ? null : reader.GetFieldValue<DateTime>(13),
        reader.GetInt32(14),
        reader.IsDBNull(15) ? null : reader.GetFieldValue<DateTime>(15),
        reader.IsDBNull(17) ? null : reader.GetGuid(17),
        reader.GetString(18),
        reader.IsDBNull(19) ? null : reader.GetString(19));

    /// <summary>
    /// Reads requests with their reports and journal (#5632 Phase A, <c>get_investigation_reports</c>): one request by id
    /// (then its stored brief too), or the requests opened since <paramref name="sinceUtc"/>, optionally on one server. The
    /// state is computed as <see cref="ListAsync"/> computes it. Writes nothing. Each page is read one row past its limit
    /// and trimmed, so the read says when the window held more requests, or those requests more journal rows, than it
    /// returns (#5834, the #3699 shape); the reports and the journal are read for the returned requests only.
    /// </summary>
    public async Task<InvestigationReportsRead> ReadReportsAsync(Guid? requestId, int? serverId, DateTime sinceUtc, int limit,
        int journalLimit, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(journalLimit, 1);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var requests = new List<InvestigationRequestRow>();
        string? brief = null;
        await using (var command = Command(connection, null, ReportsRequestsSql,
            P(Now()), new NpgsqlParameter<Guid?> { TypedValue = requestId, NpgsqlDbType = NpgsqlDbType.Uuid },
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer }, P(sinceUtc), P(limit + 1)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                requests.Add(ReportsRequestRow(reader));
                if (requestId is not null && !reader.IsDBNull(16))
                {
                    brief = reader.GetString(16);
                }
            }
        }

        var requestsTruncated = requests.Count > limit;
        if (requestsTruncated)
        {
            requests.RemoveRange(limit, requests.Count - limit);
        }

        var ids = requests.Select(r => r.RequestId).ToArray();
        var reports = new List<InvestigationReportRow>();
        var journal = new List<InvestigationJournalRow>();
        if (ids.Length == 0)
        {
            /* #5913: this store keeps feedback, so even an empty read says so (no family_counts key would read as "a store that keeps none"). */
            return new InvestigationReportsRead(requests, reports, journal, brief, requestsTruncated, false, [], requestId is null ? [] : null, EvidenceByBasis: true);
        }

        reports.AddRange(await ReadReportsForAsync(connection, ids, cancellationToken));

        await using (var command = Command(connection, null, ReportsJournalSql, P(ids), P(journalLimit + 1)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                journal.Add(new InvestigationJournalRow(
                    reader.GetFieldValue<DateTime>(0), reader.IsDBNull(1) ? null : reader.GetGuid(1),
                    reader.IsDBNull(2) ? null : reader.GetInt32(2), reader.GetString(3), reader.GetString(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetString(6)));
            }
        }

        var journalTruncated = journal.Count > journalLimit;
        if (journalTruncated)
        {
            journal.RemoveRange(journalLimit, journal.Count - journalLimit);
        }

        /* #5913: the people's feedback on these reports, and (for a window, not one request) the false-positive counts per family
           over the same window and server. The V191 table is SELECT-able by every role that reads these tables. */
        var feedbackStore = new InvestigationFeedbackStore(_dataSource, _commandTimeoutSeconds);
        var feedback = await feedbackStore.ReadAsync(connection, ids, cancellationToken);
        var familyCounts = requestId is null ? await feedbackStore.FamilyCountsAsync(connection, sinceUtc, serverId, cancellationToken) : null;
        return new InvestigationReportsRead(requests, reports, journal, brief, requestsTruncated, journalTruncated, feedback, familyCounts, EvidenceByBasis: true);
    }

    /// <summary>The reports of the requests in <paramref name="ids"/> with their links (#5914), as the report read and the feedback
    /// export (#6236) serve them.</summary>
    private async Task<List<InvestigationReportRow>> ReadReportsForAsync(NpgsqlConnection connection, Guid[] ids, CancellationToken cancellationToken)
    {
        var reports = new List<InvestigationReportRow>();
        await using (var command = Command(connection, null, ReportsSql, P(ids)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                reports.Add(new InvestigationReportRow(
                    reader.GetGuid(0), reader.GetInt16(1), reader.GetString(2), reader.GetFieldValue<DateTime>(3), reader.GetString(4),
                    reader.GetString(5), reader.GetBoolean(6), reader.IsDBNull(7) ? null : reader.GetString(7), reader.GetString(8),
                    FamilyOf: reader.IsDBNull(9) ? null : reader.GetGuid(9), QueriesRunJson: reader.IsDBNull(10) ? null : reader.GetString(10)));
            }
        }

        var links = new Dictionary<Guid, List<InvestigationReportLink>>();
        await using (var command = Command(connection, null, ReportLinksSql, P(ids)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                if (InvestigationReportLinks.FromDetail(reader.IsDBNull(1) ? null : reader.GetString(1)) is not { } link)
                    continue;
                var id = reader.GetGuid(0);
                if (!links.TryGetValue(id, out var list))
                    links[id] = list = [];
                list.Add(link);
            }
        }

        if (links.Count > 0)
        {
            for (var i = 0; i < reports.Count; i++)
            {
                if (links.TryGetValue(reports[i].RequestId, out var list))
                    reports[i] = reports[i] with { Links = list };
            }
        }

        return reports;
    }

    /// <summary>
    /// The ids of the cases a live practice request replays (#5918): practice requests that read open or claimed now. The
    /// investigations arm's report read leaves their reports and feedback out while this answers them. Writes nothing.
    /// </summary>
    public async Task<IReadOnlySet<Guid>> LivePracticeReplaysAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, LivePracticeReplaysSql, P(Now()));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var ids = new HashSet<Guid>();
        while (await reader.ReadAsync(cancellationToken))
            ids.Add(reader.GetGuid(0));
        return ids;
    }

    /// <summary>
    /// Lists requests newest first, with the state computed from the lease and the expiry (a lapsed lease reads
    /// <c>open</c>, or <c>expired</c> after three claims; a request past its expiry reads <c>expired</c>). Writes nothing.
    /// Every filter is optional.
    /// </summary>
    public async Task<IReadOnlyList<InvestigationRequestRow>> ListAsync(
        string? state, int? serverId, string? family, int limit, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, ListSql,
            P(Now()), PNullable(state), new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer },
            PNullable(family), P(limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<InvestigationRequestRow>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new InvestigationRequestRow(
                reader.GetGuid(0),
                reader.GetInt32(1),
                reader.GetInt32(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.GetString(5),
                reader.GetFieldValue<DateTime>(6),
                reader.GetFieldValue<DateTime>(7),
                reader.GetInt32(8),
                reader.GetString(9),
                reader.GetFieldValue<DateTime>(10),
                reader.GetFieldValue<DateTime>(11),
                reader.IsDBNull(12) ? null : reader.GetString(12),
                reader.IsDBNull(13) ? null : reader.GetFieldValue<DateTime>(13),
                reader.GetInt32(14),
                reader.IsDBNull(15) ? null : reader.GetFieldValue<DateTime>(15),
                reader.IsDBNull(16) ? null : reader.GetGuid(16),
                reader.GetString(17),
                reader.IsDBNull(18) ? null : reader.GetString(18),
                reader.IsDBNull(19) ? null : reader.GetString(19)));
        }

        return rows;
    }

    /// <summary>
    /// The requests opened after <paramref name="sinceUtc"/> on <paramref name="routeIds"/> (every route when null or empty),
    /// oldest first, at most <paramref name="limit"/> (#5822, the long-poll's catch-up read). Writes nothing.
    /// </summary>
    public async Task<IReadOnlyList<InvestigationOpenedRow>> OpenedSinceAsync(
        DateTime sinceUtc, IReadOnlyCollection<int>? routeIds, int limit, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, OpenedSinceSql,
            P(DateTime.SpecifyKind(sinceUtc, DateTimeKind.Utc)),
            new NpgsqlParameter<int[]?>
            {
                TypedValue = routeIds is { Count: > 0 } ? routeIds.ToArray() : null,
                NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer,
            },
            P(limit),
            new NpgsqlParameter<string[]> { TypedValue = InvestigationEvents.Kinds.ToArray(), NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<InvestigationOpenedRow>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new InvestigationOpenedRow(
                reader.GetGuid(0),
                reader.GetInt32(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetFieldValue<DateTime>(4),
                reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6))
            {
                EventId = reader.GetGuid(7),
                Event = reader.GetString(8),
                EventTimeUtc = reader.GetFieldValue<DateTime>(9),
                /* #6003 review: only the members the event's kind lists leave the store. */
                DetailJson = InvestigationEvents.PublicDetail(reader.GetString(8), reader.IsDBNull(10) ? null : reader.GetString(10),
                    reader.IsDBNull(11) ? null : reader.GetGuid(11)),
                DetailText = reader.IsDBNull(12) ? null : reader.GetString(12),
            });
        }

        return rows;
    }

    /// <summary>
    /// #6003: up to <paramref name="limit"/> request events journaled after <paramref name="sinceUtc"/> that have no delivery
    /// mark yet, oldest first, for the signed POST to the agent route.
    /// </summary>
    public async Task<IReadOnlyList<InvestigationEventRow>> PendingDeliveriesAsync(
        DateTime sinceUtc, int limit, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, PendingDeliverySql,
            P(DateTime.SpecifyKind(sinceUtc, DateTimeKind.Utc)),
            new NpgsqlParameter<string[]> { TypedValue = InvestigationEvents.Kinds.ToArray(), NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text },
            P(limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<InvestigationEventRow>();
        while (await reader.ReadAsync(cancellationToken))
        {
            /* #6003 review: the signed POST carries only the reason and the detail members the event's kind lists. */
            var kind = reader.GetString(2);
            rows.Add(new InvestigationEventRow(
                reader.GetInt64(0), reader.GetGuid(1), kind, reader.GetFieldValue<DateTime>(3),
                reader.GetGuid(4), reader.GetInt32(5), reader.GetInt32(6), reader.GetString(7), reader.GetString(8),
                InvestigationEvents.PublicReason(kind, reader.IsDBNull(9) ? null : reader.GetString(9)),
                InvestigationEvents.PublicDetail(kind, reader.IsDBNull(10) ? null : reader.GetString(10),
                    reader.IsDBNull(11) ? null : reader.GetGuid(11))));
        }

        return rows;
    }

    /// <summary>
    /// #6003: writes <paramref name="row"/>'s delivery mark (once key <see cref="InvestigationEvents.DeliveredOnceKey"/>) and
    /// answers whether THIS call wrote it. The caller sends only on true, so an event id is delivered at most once however
    /// often it is read or replayed; <paramref name="outcome"/> is null for a send about to be made, else why none is.
    /// </summary>
    public Task<bool> MarkDeliveredAsync(InvestigationEventRow row, string? outcome, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);
        return MarkOnceAsync(row.RequestId, row.RouteId, row.ServerId, InvestigationEvents.Delivered,
            outcome is null ? row.Event : row.Event + ": " + outcome, InvestigationEvents.DeliveredOnceKey(row.EventId),
            cancellationToken);
    }

    /// <summary>#6003, D8: up to <paramref name="limit"/> requests that expired unclaimed after <paramref name="sinceUtc"/>
    /// and have no fallback yet, oldest first.</summary>
    public async Task<IReadOnlyList<InvestigationUnclaimedExpiry>> UnclaimedExpiriesAsync(
        DateTime sinceUtc, int limit, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, UnclaimedExpiriesSql,
            P(DateTime.SpecifyKind(sinceUtc, DateTimeKind.Utc)), P(limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<InvestigationUnclaimedExpiry>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new InvestigationUnclaimedExpiry(
                reader.GetGuid(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetString(3), reader.GetString(4),
                reader.GetString(5), reader.GetFieldValue<DateTime>(6), reader.GetFieldValue<DateTime>(7)));
        }

        return rows;
    }

    /// <summary>
    /// #6003, D8: writes the request's fallback mark naming <paramref name="target"/> and answers whether THIS call wrote it;
    /// the caller sends only on true, so a fallback goes out at most once per request.
    /// </summary>
    public Task<bool> MarkFallbackAsync(InvestigationUnclaimedExpiry expiry, string target, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(expiry);
        return MarkOnceAsync(expiry.RequestId, expiry.RouteId, expiry.ServerId, InvestigationEvents.FallbackSent, target,
            InvestigationEvents.FallbackOnceKey(expiry.RequestId), cancellationToken);
    }

    private async Task<bool> MarkOnceAsync(Guid requestId, int routeId, int serverId, string journalEvent, string reason,
        string onceKey, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var written = await JournalAsync(connection, transaction, Now(), requestId, routeId, serverId, journalEvent, "service",
            reason, null, onceKey, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return written > 0;
    }

    /// <summary>
    /// The expiry sweep: journals each lapsed lease once per claim, then moves every request past its expiry, or out of
    /// claims (its last claim's lease lapsed, or released), to <c>expired</c> and journals that once: <c>expired</c> past the
    /// expiry, <c>claims_exhausted</c> before it. It never reopens a lease. Then, in a transaction of its own, it journals
    /// each live request whose every metric has cleared (<see cref="ResolveCandidatesSql"/>), so a failure there cannot hold
    /// back the expiry and its fallback (#6003 review).
    /// </summary>
    public async Task<InvestigationSweepResult> SweepAsync(CancellationToken cancellationToken = default)
    {
        var now = Now();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var lapsed = await ExecAsync(connection, transaction, SweepLapsedSql, cancellationToken, P(now));
        var expired = await ExecAsync(connection, transaction, SweepExpireSql, cancellationToken, P(now));
        await transaction.CommitAsync(cancellationToken);
        await using var resolvedTransaction = await connection.BeginTransactionAsync(cancellationToken);
        var resolved = await SweepResolvedAsync(connection, resolvedTransaction, now, cancellationToken);
        await resolvedTransaction.CommitAsync(cancellationToken);
        /* #6008: its own transaction too, so a follow-up that cannot open holds back neither the expiry nor the resolved journal. */
        await using var followUpTransaction = await connection.BeginTransactionAsync(cancellationToken);
        var followUps = await SweepFollowUpsAsync(connection, followUpTransaction, now, cancellationToken);
        await followUpTransaction.CommitAsync(cancellationToken);
        /* #5632 step 8: the plan-force checks, in a transaction of their own as well. They read the store and open requests, nothing else. */
        await using var forceCheckTransaction = await connection.BeginTransactionAsync(cancellationToken);
        var forceChecks = await SweepForceChecksAsync(connection, forceCheckTransaction, now, cancellationToken);
        await forceCheckTransaction.CommitAsync(cancellationToken);
        return new InvestigationSweepResult(lapsed, expired) { Resolved = resolved, FollowUps = followUps + forceChecks };
    }

    /// <summary>
    /// The <c>deploy_follow_up</c> evidence section of a follow-up a deploy opened (#6009): the module, the change's date, and the
    /// procedure-stats counts for the 24 hours before it and from it to now (collections, executions, average CPU and elapsed
    /// milliseconds per execution), so the brief shows what the module did before and after the change. A ready section, frozen as every
    /// evidence section is. Each side carries the newest <c>modify_date</c> its rows hold, null when none holds one.
    /// </summary>
    private async Task WriteDeployEvidenceAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid followUpId, Guid earlier,
        DateTime changedAt, DateTime now, CancellationToken cancellationToken)
    {
        string? condition;
        await using (var named = Command(connection, transaction, FollowUpConditionSql, P(earlier)))
        {
            condition = Convert.ToString(await named.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        }

        var sides = new Dictionary<bool, (long Collections, long Executions, long Cpu, long Elapsed, DateTime? ModifyDate)>();
        await using (var counts = Command(connection, transaction, DeployCountsSql, P(earlier), P(changedAt), P(now)))
        await using (var reader = await counts.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                sides[reader.GetBoolean(0)] = (reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4),
                    reader.IsDBNull(5) ? null : reader.GetDateTime(5));
            }
        }

        static JsonObject Side(bool after, IReadOnlyDictionary<bool, (long Collections, long Executions, long Cpu, long Elapsed, DateTime? ModifyDate)> sides)
        {
            sides.TryGetValue(after, out var s);
            return new JsonObject
            {
                ["modify_date"] = s.ModifyDate?.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
                ["collections"] = s.Collections,
                ["executions"] = s.Executions,
                ["avg_cpu_ms"] = s.Executions > 0 ? Math.Round(s.Cpu / 1000.0 / s.Executions, 3) : null,
                ["avg_duration_ms"] = s.Executions > 0 ? Math.Round(s.Elapsed / 1000.0 / s.Executions, 3) : null,
            };
        }

        var content = new JsonObject
        {
            ["module"] = condition is { Length: > 7 } ? condition[7..] : condition,
            ["changed_at"] = changedAt.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
            ["before"] = Side(false, sides),
            ["after"] = Side(true, sides),
        };
        await ExecAsync(connection, transaction, EvidenceUpsertSql, cancellationToken,
            P(followUpId), P(DeployEvidenceSection), P("ready"), P(content.ToJsonString()), P(now));
    }

    /// <summary>The evidence section a deploy follow-up carries (#6009).</summary>
    public const string DeployEvidenceSection = "deploy_follow_up";

    /// <summary>
    /// The follow-up sweep (#6008, D16): for each reported request whose <c>follow_up_at</c> has come and that has no follow-up
    /// yet, opens a new request that names it in <c>follow_up_of</c> (the way a practice request names its case) and journals one
    /// <c>follow_up</c> event for the new request, once per earlier request. It runs as the store owner: the journal's trigger
    /// refuses event rows from every other role. The new request's brief reads the earlier report as it reads any earlier report
    /// of the same server and family. Returns the follow-ups opened.
    /// </summary>
    private async Task<int> SweepFollowUpsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime now,
        CancellationToken cancellationToken)
    {
        var due = new List<(Guid Id, DateTime? ChangedTo)>();
        await using (var candidates = Command(connection, transaction, DueFollowUpsSql, P(now)))
        await using (var reader = await candidates.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                due.Add((reader.GetGuid(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1)));
            }
        }

        foreach (var (earlier, changedTo) in due)
        {
            /* #6009: opened by a deploy when the module's modify_date moved, whatever the time says; the time ending is the rest. */
            var deployed = changedTo is not null;
            var followUpId = Guid.NewGuid();
            int routeId, serverId;
            await using (var insert = Command(connection, transaction, OpenFollowUpSql, P(followUpId), P(earlier), P(now)))
            await using (var returned = await insert.ExecuteReaderAsync(cancellationToken))
            {
                await returned.ReadAsync(cancellationToken);
                routeId = returned.GetInt32(0);
                serverId = returned.GetInt32(1);
            }

            await ExecAsync(connection, transaction, CopyFollowUpAlertsSql, cancellationToken, P(followUpId), P(earlier));
            if (deployed)
            {
                await WriteDeployEvidenceAsync(connection, transaction, followUpId, earlier, changedTo!.Value, now, cancellationToken);
            }

            await JournalAsync(connection, transaction, now, followUpId, routeId, serverId, InvestigationEvents.FollowUp, "service",
                null, "{\"follow_up_of\":\"" + earlier.ToString("D") + "\",\"trigger\":\"" + (deployed ? "deploy" : "time") + "\"}", InvestigationEvents.FollowUpOnceKey(earlier), cancellationToken);
        }

        return due.Count;
    }

    /// <summary>
    /// The resolved sweep (#6003, D7): reads each live request's linked metrics with the resolution rows logged since each
    /// metric's last alert, parses their <c>context_json</c> here, and journals <c>resolved</c> for every request whose
    /// metrics all have a row whose top-level <c>Resolves</c> names them. Returns the
    /// journal rows written.
    /// </summary>
    private async Task<int> SweepResolvedAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime now,
        CancellationToken cancellationToken)
    {
        /* Per request: each linked metric, and whether a parsed row names it. */
        var requests = new Dictionary<Guid, Dictionary<string, bool>>();
        await using (var candidates = Command(connection, transaction, ResolveCandidatesSql, P(now)))
        await using (var reader = await candidates.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var requestId = reader.GetGuid(0);
                var metric = reader.GetString(1);
                if (!requests.TryGetValue(requestId, out var metrics))
                {
                    metrics = new Dictionary<string, bool>(StringComparer.Ordinal);
                    requests[requestId] = metrics;
                }

                metrics.TryAdd(metric, false);
                if (!reader.IsDBNull(2) && !metrics[metric] && ResolvesMetric(reader.GetString(2), metric))
                {
                    metrics[metric] = true;
                }
            }
        }

        var cleared = requests.Where(r => r.Value.Values.All(done => done)).Select(r => r.Key).ToArray();
        if (cleared.Length == 0)
        {
            return 0;
        }

        return await ExecAsync(connection, transaction, ResolveInsertSql, cancellationToken, P(now),
            new NpgsqlParameter<Guid[]> { TypedValue = cleared, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Uuid });
    }

    /// <summary>The member of a resolution row's context that names the metric it resolves (<c>DarlingSelfAlertEvaluator.ResolvesMember</c>).</summary>
    private const string ResolvesMember = "Resolves";

    /// <summary>True when <paramref name="contextJson"/> is a JSON object whose own <c>Resolves</c> string is <paramref name="metric"/>.</summary>
    private static bool ResolvesMetric(string contextJson, string metric)
    {
        try
        {
            using var document = System.Text.Json.JsonDocument.Parse(contextJson);
            return document.RootElement.ValueKind == System.Text.Json.JsonValueKind.Object
                && document.RootElement.TryGetProperty(ResolvesMember, out var member)
                && member.ValueKind == System.Text.Json.JsonValueKind.String
                && string.Equals(member.GetString(), metric, StringComparison.Ordinal);
        }
        catch (System.Text.Json.JsonException)
        {
            /* A context that is not JSON names nothing; the row is stored data, not a store read. */
            return false;
        }
    }

    private Task<int> InsertAlertRowsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid requestId,
        InvestigationAlert alert, DateTime alertTime, string[] keys, CancellationToken cancellationToken)
    {
        /* A row per incident dedup key; an alert with no key still gets its one row. */
        var rowKeys = keys.Length == 0 ? new string?[] { null } : keys.Cast<string?>().ToArray();
        var facts = alert.Facts;
        return ExecAsync(connection, transaction, AlertRowsSql, cancellationToken,
            P(requestId), P(alertTime), P(alert.ServerId), P(alert.MetricName),
            new NpgsqlParameter<string?[]> { TypedValue = rowKeys, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text },
            /* #5821: the history row's values, so a brief composed before that row exists still reads them. */
            new NpgsqlParameter<double?> { TypedValue = facts?.CurrentValue, NpgsqlDbType = NpgsqlDbType.Double },
            new NpgsqlParameter<double?> { TypedValue = facts?.ThresholdValue, NpgsqlDbType = NpgsqlDbType.Double },
            new NpgsqlParameter<bool?> { TypedValue = facts?.Muted, NpgsqlDbType = NpgsqlDbType.Boolean },
            new NpgsqlParameter<string?> { TypedValue = DerivedStoreScrub.Text(facts?.DetailText), NpgsqlDbType = NpgsqlDbType.Text },
            new NpgsqlParameter<string?> { TypedValue = DerivedStoreScrub.Json(facts?.ContextJson), NpgsqlDbType = NpgsqlDbType.Text },
            /* #6928: the tier this alert arrived at, so the agent-action gate resolves its route at that tier. */
            new NpgsqlParameter<string?> { TypedValue = AlertSeverityWord(alert.Severity), NpgsqlDbType = NpgsqlDbType.Text });
    }

    /// <summary>
    /// True when <paramref name="requestId"/> already holds <see cref="InvestigationRules.MaxAgentJournalRows"/> rows written by agents; then
    /// journals <c>journal_capped</c> once per request. A count, not a lock: concurrent claims can pass it together, so the
    /// cap is exceeded by at most the calls in flight, each still journaled once by its own key.
    /// </summary>
    private async Task<bool> JournalCappedAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime now,
        Guid requestId, string actor, string operation, CancellationToken cancellationToken)
    {
        long rows;
        await using (var count = Command(connection, transaction, AgentJournalCountSql, P(requestId)))
        {
            rows = (long)(await count.ExecuteScalarAsync(cancellationToken))!;
        }

        if (rows < InvestigationRules.MaxAgentJournalRows)
        {
            return false;
        }

        await JournalAsync(connection, transaction, now, requestId, null, null, "journal_capped", actor, operation,
            null, "journal_capped:" + requestId.ToString(), cancellationToken);
        return true;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<PendingInvestigationNote>> ListPendingNotesAsync(int limit = 200, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, PendingNotesSql, P(Math.Clamp(limit, 1, 1000)), P(Now()));
        var rows = new List<PendingInvestigationNote>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PendingInvestigationNote(
                reader.GetInt64(0),
                reader.IsDBNull(1) ? null : reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.GetString(6),
                reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetGuid(8),
                DateTime.SpecifyKind(reader.GetDateTime(9), DateTimeKind.Utc),
                DateTime.SpecifyKind(reader.GetDateTime(10), DateTimeKind.Utc)));
        }

        return rows;
    }

    /// <inheritdoc />
    public Task<bool> ApproveNoteAsync(long noteId, string decidedBy, CancellationToken cancellationToken = default) =>
        DecideNoteAsync(noteId, true, decidedBy, cancellationToken);

    /// <inheritdoc />
    public Task<bool> RejectNoteAsync(long noteId, string decidedBy, CancellationToken cancellationToken = default) =>
        DecideNoteAsync(noteId, false, decidedBy, cancellationToken);

    /* The one decide statement of Darling's note decisions (#5903): the desktop Viewer, the web routes and the tests all come
       through here, so there is one statement, one clock and one decided_by rule. The update and its journal row share one
       transaction: a decision is never stamped without its audit row. */
    /// <summary>Approves (<paramref name="approve"/>) or rejects a pending note. False when the note is gone or already decided.
    /// Throws <see cref="ArgumentException"/> when <paramref name="decidedBy"/> is not a <c>person:&lt;login&gt;</c> actor
    /// (<see cref="InvestigationPersonWrites.ValidateActor"/>, the guard close and note hold), so no caller approves a note as an agent.
    /// The decision time is the store's clock (<c>now()</c>), as the person close and note use, not the caller's.</summary>
    public async Task<bool> DecideNoteAsync(long noteId, bool approve, string decidedBy, CancellationToken cancellationToken = default)
    {
        if (InvestigationPersonWrites.ValidateActor(decidedBy) is { } refusal)
        {
            throw new ArgumentException(refusal, nameof(decidedBy));
        }

        var state = approve ? "approved" : "rejected";
        var journalEvent = approve ? "note_approved" : "note_rejected";
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        Guid? sourceRequest;
        int? serverId;
        DateTime now;
        await using (var command = Command(connection, transaction, DecideNoteSql, P(noteId), P(state), P(decidedBy)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                return false;
            }

            sourceRequest = reader.IsDBNull(0) ? null : reader.GetGuid(0);
            serverId = reader.IsDBNull(1) ? null : reader.GetInt32(1);
            now = reader.GetFieldValue<DateTime>(2);
        }

        await JournalAsync(connection, transaction, now, sourceRequest, null, serverId, journalEvent, decidedBy,
            "note " + noteId.ToString(CultureInfo.InvariantCulture), null, null, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    private Task<int> JournalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime now, Guid? requestId,
        int? routeId, int? serverId, string journalEvent, string actor, string? reason, string? detailJson, string? onceKey,
        CancellationToken cancellationToken) =>
        ExecAsync(connection, transaction, JournalSql, cancellationToken,
            P(now),
            new NpgsqlParameter<Guid?> { TypedValue = requestId, NpgsqlDbType = NpgsqlDbType.Uuid },
            new NpgsqlParameter<int?> { TypedValue = routeId, NpgsqlDbType = NpgsqlDbType.Integer },
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer },
            P(journalEvent), P(actor), PNullable(reason), PNullable(detailJson), PNullable(onceKey));

    private NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.AddRange(parameters);
        return command;
    }

    private async Task<int> ExecAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql,
        CancellationToken cancellationToken, params NpgsqlParameter[] parameters)
    {
        await using var command = Command(connection, transaction, sql, parameters);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static NpgsqlParameter<T> P<T>(T value) => new NpgsqlParameter<T> { TypedValue = value };

    private static NpgsqlParameter<DateTime> P(DateTime value) =>
        new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Utc), NpgsqlDbType = NpgsqlDbType.TimestampTz };

    private static NpgsqlParameter<string?> PNullable(string? value) =>
        new NpgsqlParameter<string?> { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Text };
}
