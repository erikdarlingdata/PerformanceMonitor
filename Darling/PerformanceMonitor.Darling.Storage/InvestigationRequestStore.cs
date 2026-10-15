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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One alert handed to the investigation store by the dispatcher (#5632 Phase A). Every time is UTC.</summary>
/// <param name="RouteId">The route whose agent level answered 1; its limits are the next four members.</param>
/// <param name="DedupKeys">Every incident dedup key of the alert (a row is recorded per key); empty when it has none.</param>
/// <param name="Facts">
/// #5821: what the alert's history row will hold, written on its link rows (V187) so the brief's alerts section reads them
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

/// <summary>What <see cref="InvestigationRequestStore.OpenOrJoinAsync"/> did with an alert.</summary>
public enum InvestigationOpenOutcome
{
    /// <summary>A new request was opened.</summary>
    Opened,

    /// <summary>The alert joined an open or claimed request of the same server and family.</summary>
    Joined,

    /// <summary>The route's hourly or daily limit was reached: nothing was opened.</summary>
    LimitReached,
}

/// <summary>The result of <see cref="InvestigationRequestStore.OpenOrJoinAsync"/>; the request is null at a limit.</summary>
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

    /// <summary>The agent id is not 1 to <see cref="InvestigationRequestStore.MaxAgentIdLength"/> characters from
    /// <c>[A-Za-z0-9._:-]</c> (#5632 review round 2, L4): nothing
    /// was read or written.</summary>
    AgentIdInvalid,

    /// <summary>The request already holds <see cref="InvestigationRequestStore.MaxAgentJournalRows"/> journal rows
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

    /// <summary>The agent id is not 1 to <see cref="InvestigationRequestStore.MaxAgentIdLength"/> characters from
    /// <c>[A-Za-z0-9._:-]</c> (#5632 review round 2, L4): nothing
    /// was read or written.</summary>
    AgentIdInvalid,

    /// <summary>The request already holds <see cref="InvestigationRequestStore.MaxAgentJournalRows"/> journal rows
    /// written by agents: the report is refused and one <c>journal_capped</c> row says so.</summary>
    JournalCapped,
}

/// <summary>A report an agent returns under its claim. <see cref="EvidenceJson"/> is a JSON array of at least one item.</summary>
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
    string? ProposedNote = null);

/// <summary>What a lease renewal answered (#5632 Phase A, a claim made with the caller's own claim id).</summary>
/// <param name="Renewed">True when the claim still held a live lease and the lease moved on.</param>
/// <param name="LeaseUntilUtc">The new lease end, capped at the request's expiry; null when not renewed.</param>
public sealed record InvestigationRenewResult(bool Renewed, DateTime? LeaseUntilUtc);

/// <summary>One stored report as <see cref="InvestigationRequestStore.ReadReportsAsync"/> reads it.</summary>
public sealed record InvestigationReportRow(
    Guid RequestId,
    short ReportFormat,
    string AgentId,
    DateTime SubmittedAtUtc,
    string Verdict,
    string RootCause,
    bool RootCauseFound,
    string? Summary,
    string EvidenceJson);

/// <summary>One journal row as <see cref="InvestigationRequestStore.ReadReportsAsync"/> reads it.</summary>
public sealed record InvestigationJournalRow(
    DateTime EventTimeUtc,
    Guid? RequestId,
    int? ServerId,
    string Event,
    string Actor,
    string? Reason,
    string? DetailJson);

/// <summary>What <see cref="InvestigationRequestStore.ReadReportsAsync"/> read; <see cref="Brief"/> only for one request.
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

/// <summary>
/// The store operations of Level 1 investigations (#5632 Phase A, plan v2 section 2 and ruling R4): open or join a request,
/// claim it, submit its report, list requests, and the expiry sweep. Every time is bound as a UTC <c>timestamptz</c>
/// parameter from the injected clock, never a bare <c>now()</c>, so the windows do not depend on the session's zone and a
/// test can move the clock.
///
/// <para><b>Open or join is one transaction.</b> It takes a transaction-scoped advisory lock on the server and family, so two
/// alerts of one incident cannot both open a request; joins an open or claimed request of that server and family only while
/// the request is inside its 30-minute window (opened at most 30 minutes ago, and the alert at most 30 minutes after the
/// request's first); and otherwise takes
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
public sealed class InvestigationRequestStore
{
    /// <summary>The first key of the route lock, <c>pg_advisory_xact_lock(5632, route_id)</c>.</summary>
    public const int RouteLockKey = 5632;

    /// <summary>The grouping window: an alert this long after a request's first alert still joins it.</summary>
    public static readonly TimeSpan GroupingWindow = TimeSpan.FromMinutes(30);

    /// <summary>The claims a request may take before it expires.</summary>
    public const int MaxClaims = 3;

    /// <summary>The longest agent id a claim or a report may carry (#5632 review round 1, L3). It is written into
    /// <c>claimed_by</c>, the journal's <c>actor</c> and the report, so the store refuses a longer one for every caller.</summary>
    public const int MaxAgentIdLength = 64;

    /// <summary>The agent a refusal is journaled under when the caller's agent id is not a valid one, so an id built to
    /// read as another journal line (the web panel joins a row's fields with a middle dot) is never written.</summary>
    public const string InvalidAgentId = "invalid";

    /// <summary>
    /// True when <paramref name="agentId"/> is 1 to <see cref="MaxAgentIdLength"/> characters from <c>[A-Za-z0-9._:-]</c>
    /// (#5632 review round 2, L4; V184's check constraint), so it cannot carry a separator, a person's prefix with spaces,
    /// or markup into the journal.
    /// </summary>
    public static bool IsValidAgentId(string? agentId) =>
        !string.IsNullOrEmpty(agentId) && agentId.Length <= MaxAgentIdLength
        && agentId.All(c => c is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9') or '.' or '_' or ':' or '-');

    /// <summary>The journal rows agents may write on one request (#5632 review round 1, L3). A token holder could otherwise
    /// add a row per guessed claim id, each kept 365 days. The next agent call on that request is refused, and one
    /// <c>journal_capped</c> row records it.</summary>
    public const int MaxAgentJournalRows = 200;

    internal const string GroupLockSql =
        "SELECT pg_advisory_xact_lock(hashtextextended('investigation:' || $1::text || ':' || $2, 5632))";

    internal const string RouteLockSql = "SELECT pg_advisory_xact_lock($1, $2)";

    internal const string FindJoinSql = @"
SELECT r.request_id
FROM config.investigation_requests AS r
WHERE r.server_id = $1
  AND r.family = $2
  AND r.state IN ('open', 'claimed')
  AND r.expires_at > $3
  AND r.opened_at >= $3 - interval '30 minutes'
  AND $4 <= r.window_until
ORDER BY r.opened_at DESC
LIMIT 1";

    internal const string JoinSql = @"
UPDATE config.investigation_requests
SET alert_count = alert_count + 1,
    last_alert_at = greatest(last_alert_at, $2)
WHERE request_id = $1";

    internal const string CountRouteSql = @"
SELECT count(*) FILTER (WHERE opened_at > $2 - interval '1 hour'),
       count(*)
FROM config.investigation_requests
WHERE route_id = $1
  AND opened_at > $2 - interval '1 day'";

    internal const string OpenSql = @"
INSERT INTO config.investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, first_dedup_key, story_path_hash,
     first_alert_at, last_alert_at, window_until, opened_at, expires_at, lease_minutes)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $9 + interval '30 minutes', $10, $10 + make_interval(mins => $11), $12)";

    internal const string AlertRowsSql = @"
INSERT INTO config.investigation_request_alerts
    (request_id, alert_time, server_id, metric_name, dedup_key, current_value, threshold_value, muted, detail_text, context_json)
SELECT $1, $2, $3, $4, k, $6, $7, $8, $9, $10
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

    internal const string ClaimWhySql = @"
SELECT state, lease_until, claim_count, expires_at, server_id
FROM config.investigation_requests
WHERE request_id = $1";

    internal const string SubmitLockSql = @"
SELECT state, claim_id, server_id, first_metric, first_dedup_key, story_path_hash
FROM config.investigation_requests
WHERE request_id = $1
FOR UPDATE";

    internal const string ReportInsertSql = @"
INSERT INTO config.investigation_reports
    (request_id, report_format, agent_id, claim_id, submitted_at, verdict, root_cause, root_cause_found, summary, evidence,
     server_id, metric_name, dedup_key, story_path_hash)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13, $14)";

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

    internal const string ListSql = @"
SELECT request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, alert_count,
       effective_state, opened_at, expires_at, claimed_by, lease_until, claim_count, reported_at
FROM (
    SELECT r.*,
           CASE
               WHEN r.state IN ('open', 'claimed') AND r.expires_at <= $1 THEN 'expired'
               WHEN r.state = 'claimed' AND r.lease_until < $1 AND r.claim_count >= 3 THEN 'expired'
               WHEN r.state = 'open' AND r.claim_count >= 3 THEN 'expired'
               WHEN r.state = 'claimed' AND r.lease_until < $1 THEN 'open'
               ELSE r.state
           END AS effective_state
    FROM config.investigation_requests AS r
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

    internal const string ReportsRequestsSql = @"
SELECT r.request_id, r.route_id, r.server_id, r.server_name, r.family, r.first_metric, r.first_alert_at, r.last_alert_at,
       r.alert_count,
       CASE
           WHEN r.state IN ('open', 'claimed') AND r.expires_at <= $1 THEN 'expired'
           WHEN r.state = 'claimed' AND r.lease_until < $1 AND r.claim_count >= 3 THEN 'expired'
           WHEN r.state = 'open' AND r.claim_count >= 3 THEN 'expired'
           WHEN r.state = 'claimed' AND r.lease_until < $1 THEN 'open'
           ELSE r.state
       END AS effective_state,
       r.opened_at, r.expires_at, r.claimed_by, r.lease_until, r.claim_count, r.reported_at, r.brief::text
FROM config.investigation_requests AS r
WHERE ($2::uuid IS NULL OR r.request_id = $2)
  AND ($3::integer IS NULL OR r.server_id = $3)
  AND ($2::uuid IS NOT NULL OR r.opened_at >= $4)
ORDER BY r.opened_at DESC, r.request_id
LIMIT $5";

    internal const string ReportsSql = @"
SELECT request_id, report_format, agent_id, submitted_at, verdict, root_cause, root_cause_found, summary, evidence::text
FROM config.investigation_reports
WHERE request_id = ANY($1)
ORDER BY submitted_at DESC";

    internal const string ReportsJournalSql = @"
SELECT event_time, request_id, server_id, event, actor, reason, detail::text
FROM config.investigation_journal
WHERE request_id = ANY($1)
ORDER BY event_time DESC, journal_id DESC
LIMIT $2";

    internal const string SweepLapsedSql = @"
INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason, once_key)
SELECT $1, r.request_id, r.server_id, 'lease_expired', 'service', r.claimed_by, 'lease_expired:' || r.claim_id::text
FROM config.investigation_requests AS r
WHERE r.state = 'claimed' AND r.lease_until < $1
ON CONFLICT (once_key) WHERE once_key IS NOT NULL DO NOTHING";

    internal const string SweepExpireSql = @"
WITH expired AS (
    UPDATE config.investigation_requests
    SET state = 'expired'
    WHERE (state IN ('open', 'claimed') AND expires_at <= $1)
       OR (state = 'claimed' AND lease_until < $1 AND claim_count >= 3)
       OR (state = 'open' AND claim_count >= 3)
    RETURNING request_id, server_id, expires_at > $1 AS exhausted
)
INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, once_key)
SELECT $1, e.request_id, e.server_id, CASE WHEN e.exhausted THEN 'claims_exhausted' ELSE 'expired' END, 'service',
       'expired:' || e.request_id::text
FROM expired AS e
ON CONFLICT (once_key) WHERE once_key IS NOT NULL DO NOTHING";

    private readonly NpgsqlDataSource _dataSource;
    private readonly int _commandTimeoutSeconds;
    private readonly Func<DateTime> _utcNow;

    /// <summary>
    /// <paramref name="dataSource"/> must connect as a role that may write the investigation tables: the store owner for
    /// open, join and the sweep; the owner or the <c>mcp</c> role for claim and submit. <paramref name="commandTimeoutSeconds"/>
    /// is applied to every command. <paramref name="utcNow"/> is the clock (tests move it); it defaults to the wall clock.
    /// </summary>
    public InvestigationRequestStore(NpgsqlDataSource dataSource, int commandTimeoutSeconds, Func<DateTime>? utcNow = null)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _commandTimeoutSeconds = commandTimeoutSeconds;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    private DateTime Now() => DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc);

    /// <summary>
    /// Opens a request for <paramref name="alert"/>, joins it to an open or claimed request of the same server and family,
    /// or refuses at the route's limit, in one transaction. See the class remarks for the locks.
    /// </summary>
    /// <remarks>
    /// <para><b>The join is bounded by the window</b> (#5632 review round 1, M2): an alert joins only a request whose state is
    /// <c>open</c> or <c>claimed</c> and that opened at most <see cref="GroupingWindow"/> ago. A dedup key never decides a join:
    /// the send path's key falls back to the constant <c>{server}:{metric}</c> for an alert with no incidents, so a key arm
    /// would join every later alert of that metric for the request's whole life, after its brief was composed. The keys are
    /// recorded as the request's alert rows only.</para>
    /// <para><b>A reported or closed request is final.</b> A later alert of the same server and family opens a NEW request,
    /// even inside the same 30 minutes, and that request counts against the route's hourly and daily limits. This is
    /// intended: the agent's report answered the alerts it was briefed on, and a condition that fires again is a new question.</para>
    /// </remarks>
    public async Task<InvestigationOpenResult> OpenOrJoinAsync(InvestigationAlert alert, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(alert);
        var now = Now();
        var alertTime = DateTime.SpecifyKind(alert.AlertTimeUtc, DateTimeKind.Utc);
        var keys = alert.DedupKeys.Where(k => !string.IsNullOrEmpty(k)).Distinct(StringComparer.Ordinal).ToArray();

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await ExecAsync(connection, transaction, GroupLockSql, cancellationToken, P(alert.ServerId), P(alert.Family));

        Guid? joinId = null;
        await using (var find = Command(connection, transaction, FindJoinSql,
                         P(alert.ServerId), P(alert.Family), P(now), P(alertTime)))
        {
            var found = await find.ExecuteScalarAsync(cancellationToken);
            if (found is Guid id)
            {
                joinId = id;
            }
        }

        if (joinId is Guid join)
        {
            await ExecAsync(connection, transaction, JoinSql, cancellationToken, P(join), P(alertTime));
            await InsertAlertRowsAsync(connection, transaction, join, alert, alertTime, keys, cancellationToken);
            await JournalAsync(connection, transaction, now, join, alert.RouteId, alert.ServerId, "joined", "service",
                alert.MetricName, null, null, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return new InvestigationOpenResult(InvestigationOpenOutcome.Joined, join);
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
            P(alert.LeaseMinutes));
        await InsertAlertRowsAsync(connection, transaction, requestId, alert, alertTime, keys, cancellationToken);
        await JournalAsync(connection, transaction, now, requestId, alert.RouteId, alert.ServerId, "opened", "service",
            alert.MetricName, null, null, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new InvestigationOpenResult(InvestigationOpenOutcome.Opened, requestId);
    }

    /// <summary>
    /// Claims <paramref name="requestId"/> for <paramref name="agentId"/> under a new claim id with one conditional UPDATE.
    /// When no row returns, one SELECT says why, and the refusal is journaled once per request and reason (an unknown id
    /// once per hour). An agent id <see cref="IsValidAgentId"/> refuses (journaled once per request under
    /// <see cref="InvalidAgentId"/>), or a request whose agent journal is full (<see cref="MaxAgentJournalRows"/>), is
    /// refused first. The <c>claimed</c> row's reason is null (#5632 review round 2, L4): the claim id is a lease identity,
    /// so it lives only in that row's <c>once_key</c>, which no reader returns.
    /// </summary>
    public async Task<InvestigationClaimResult> ClaimAsync(Guid requestId, string agentId, CancellationToken cancellationToken = default)
    {
        if (!IsValidAgentId(agentId))
        {
            await RecordRefusalAsync(requestId, InvalidAgentId, "claim", "agent_id_invalid", null, cancellationToken);
            return new InvestigationClaimResult(InvestigationClaimOutcome.AgentIdInvalid, null, null, 0, "agent_id_invalid");
        }

        var now = Now();
        var claimId = Guid.NewGuid();
        var actor = "agent:" + agentId;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

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
                    _ when count >= MaxClaims => (InvestigationClaimOutcome.NotOpen, "claim_limit"),
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
    /// <see cref="IsValidAgentId"/> refuses, or a request whose agent journal is full (<see cref="MaxAgentJournalRows"/>),
    /// is refused first.
    /// </summary>
    public async Task<InvestigationSubmitOutcome> SubmitAsync(InvestigationReportSubmission report, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(report);
        if (!IsValidAgentId(report.AgentId))
        {
            await RecordRefusalAsync(report.RequestId, InvalidAgentId, "submit", "agent_id_invalid", null, cancellationToken);
            return InvestigationSubmitOutcome.AgentIdInvalid;
        }

        var now = Now();
        var actor = "agent:" + report.AgentId;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        string? state = null;
        Guid? currentClaim = null;
        int serverId = 0;
        string? metric = null, dedupKey = null, story = null;
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

        await ExecAsync(connection, transaction, ReportInsertSql, cancellationToken,
            P(report.RequestId), P(report.ReportFormat), P(report.AgentId), P(report.ClaimId), P(now), P(report.Verdict),
            P(report.RootCause), P(report.RootCauseFound), PNullable(report.Summary), P(report.EvidenceJson), P(serverId),
            PNullable(metric), PNullable(dedupKey), PNullable(story));
        if (!string.IsNullOrWhiteSpace(report.ProposedNote))
        {
            await ExecAsync(connection, transaction, ProposedNoteSql, cancellationToken,
                P(report.RequestId), P(report.ProposedNote), P(actor));
        }

        await ExecAsync(connection, transaction, ReportedSql, cancellationToken, P(report.RequestId), P(now));
        await JournalAsync(connection, transaction, now, report.RequestId, null, serverId, "reported", actor,
            report.Verdict, null, null, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return InvestigationSubmitOutcome.Reported;
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
    /// Journals a refusal the tool layer decided (an invalid report, an action proposal at Level 1, an unsupported format)
    /// once per request, operation and reason (#5632 review ruling). An unknown request id is journaled once per hour per
    /// reason, as the store's own refusals are. An agent id <see cref="IsValidAgentId"/> refuses is journaled as
    /// <see cref="InvalidAgentId"/>.
    /// </summary>
    public async Task RecordRefusalAsync(Guid requestId, string agentId, string operation, string reason, string? detailJson,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operation);
        ArgumentException.ThrowIfNullOrEmpty(reason);
        if (!IsValidAgentId(agentId))
        {
            agentId = InvalidAgentId;
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
        reader.IsDBNull(15) ? null : reader.GetFieldValue<DateTime>(15));

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
            return new InvestigationReportsRead(requests, reports, journal, brief, requestsTruncated);
        }

        await using (var command = Command(connection, null, ReportsSql, P(ids)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                reports.Add(new InvestigationReportRow(
                    reader.GetGuid(0), reader.GetInt16(1), reader.GetString(2), reader.GetFieldValue<DateTime>(3), reader.GetString(4),
                    reader.GetString(5), reader.GetBoolean(6), reader.IsDBNull(7) ? null : reader.GetString(7), reader.GetString(8)));
            }
        }

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

        return new InvestigationReportsRead(requests, reports, journal, brief, requestsTruncated, journalTruncated);
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
                reader.IsDBNull(15) ? null : reader.GetFieldValue<DateTime>(15)));
        }

        return rows;
    }

    /// <summary>
    /// The expiry sweep: journals each lapsed lease once per claim, then moves every request past its expiry, or out of
    /// claims (its last claim's lease lapsed, or released), to <c>expired</c> and journals that once: <c>expired</c> past the
    /// expiry, <c>claims_exhausted</c> before it. It never reopens a lease.
    /// </summary>
    public async Task<InvestigationSweepResult> SweepAsync(CancellationToken cancellationToken = default)
    {
        var now = Now();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var lapsed = await ExecAsync(connection, transaction, SweepLapsedSql, cancellationToken, P(now));
        var expired = await ExecAsync(connection, transaction, SweepExpireSql, cancellationToken, P(now));
        await transaction.CommitAsync(cancellationToken);
        return new InvestigationSweepResult(lapsed, expired);
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
            new NpgsqlParameter<string?> { TypedValue = DerivedStoreScrub.Json(facts?.ContextJson), NpgsqlDbType = NpgsqlDbType.Text });
    }

    /// <summary>
    /// True when <paramref name="requestId"/> already holds <see cref="MaxAgentJournalRows"/> rows written by agents; then
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

        if (rows < MaxAgentJournalRows)
        {
            return false;
        }

        await JournalAsync(connection, transaction, now, requestId, null, null, "journal_capped", actor, operation,
            null, "journal_capped:" + requestId.ToString(), cancellationToken);
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
