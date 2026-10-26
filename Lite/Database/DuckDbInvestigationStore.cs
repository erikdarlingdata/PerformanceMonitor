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
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// Lite's store of Level 1 investigations (#5820): the DuckDB port of Darling's <c>InvestigationRequestStore</c> behind the
/// shared <see cref="IInvestigationRequestStore"/>. Each statement keeps the name of the Darling statement it ports, so the
/// two can be read side by side. The semantics are Darling's: open or join within the sliding 30-minute window (up to 4 hours after the first alert), the route limits
/// with <c>limit_reached</c> journaled once per route-hour, a claim as one conditional UPDATE, a report accepted from the
/// current claim until a newer claim supersedes it, and a sweep that only expires.
///
/// <para><b>Locking.</b> Darling takes advisory locks (the group and the route) and a row lock (the submit); DuckDB has
/// neither, so every write here holds <see cref="DuckDbInitializer.AcquireWriteLock"/> with the 5-second budget
/// (<see cref="LocalDataService.DefaultWriteLockBudget"/>) for its whole transaction. That lock is process-wide and
/// covers what the three PostgreSQL locks covered: no two writers of this store interleave, and every max + 1 id is
/// assigned under it. The lock is NoRecursion, so no method calls another store method while it holds the lock. A read
/// takes no lock: it assigns no id, and DuckDB gives each connection a consistent snapshot.</para>
///
/// <para><b>Times.</b> Every time is bound from the injected clock, truncated to the microsecond DuckDB keeps, and read
/// back with <see cref="DateTimeKind.Utc"/>. DuckDB's <c>current_timestamp</c> is never used: it is local-zone.</para>
/// </summary>
public sealed partial class DuckDbInvestigationStore : IInvestigationRequestStore, IInvestigationNoteStore
{
    /* Darling's GroupLockSql and RouteLockSql (pg_advisory_xact_lock) have no twin: the write lock each write holds for its
       whole transaction covers them. SubmitLockSql keeps its name; its FOR UPDATE is the write lock. */

    /* #5632: the grouping window and its cap as interval literals, built from InvestigationRules so the SQL holds no second copy. */
    private static readonly string GroupingWindowSql = string.Create(CultureInfo.InvariantCulture,
        $"INTERVAL {(int)InvestigationRules.GroupingWindow.TotalMinutes} MINUTE");
    private static readonly string GroupingWindowCapSql = string.Create(CultureInfo.InvariantCulture,
        $"INTERVAL {(int)InvestigationRules.GroupingWindowCap.TotalMinutes} MINUTE");

    /* #6778: a reported request takes a quiet re-fire (same metric, severity not above max_severity, no resolved journal row, inside the window and cap); the claim deadline applies to open and claimed rows only. */
    /* #5632: the window slides (Darling's twin): window_until is the latest alert + 30 minutes, held to the first alert + 4 hours, and
       the alert-time test ($4 <= window_until) is the sliding bound. The wall-clock test stays on the open time ($3 is now), widened to
       the 4-hour cap, so an alert stamped more than 30 minutes before it reached the store still joins. */
    internal static readonly string FindJoinSql = @"
SELECT r.request_id,
       r.max_severity,
       EXISTS (SELECT 1 FROM investigation_request_alerts AS a
               WHERE a.request_id = r.request_id AND a.metric_name = $5) AS has_metric
FROM investigation_requests AS r
WHERE r.server_id = $1
  AND r.family = $2
  AND (r.state IN ('open', 'claimed') AND r.expires_at > $3
       OR r.state = 'reported'
          AND EXISTS (SELECT 1 FROM investigation_request_alerts AS a2 WHERE a2.request_id = r.request_id AND a2.metric_name = $5)
          AND (CASE WHEN $6 = 'critical' THEN 1 ELSE 0 END) <= (CASE WHEN r.max_severity = 'critical' THEN 1 ELSE 0 END)
          AND NOT EXISTS (SELECT 1 FROM investigation_journal AS j WHERE j.request_id = r.request_id AND j.event = 'resolved'))
  AND r.opened_at >= $3 - " + GroupingWindowCapSql + @"
  AND $4 <= r.window_until
ORDER BY r.opened_at DESC
LIMIT 1";

    internal static readonly string JoinSql = @"
UPDATE investigation_requests
SET alert_count = alert_count + 1,
    last_alert_at = greatest(last_alert_at, $2),
    window_until = greatest(window_until, least(greatest(last_alert_at, $2) + " + GroupingWindowSql + @", first_alert_at + " + GroupingWindowCapSql + @")),
    max_severity = $3
WHERE request_id = $1";

    internal const string CountRouteSql = @"
SELECT count(*) FILTER (WHERE opened_at > $2 - INTERVAL 1 HOUR),
       count(*)
FROM investigation_requests
WHERE route_id = $1
  AND opened_at > $2 - INTERVAL 1 DAY";

    internal static readonly string OpenSql = @"
INSERT INTO investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, first_dedup_key, story_path_hash,
     first_alert_at, last_alert_at, window_until, opened_at, expires_at, lease_minutes, max_severity)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $9 + " + GroupingWindowSql + @", $10, $10 + to_minutes(CAST($11 AS BIGINT)), $12, $13)";

    /* One row per key of the bound list; the ids continue from the table's max under the write lock. */
    internal const string AlertRowsSql = @"
INSERT INTO investigation_request_alerts
    (request_alert_id, request_id, alert_time, server_id, metric_name, dedup_key, current_value, threshold_value, muted,
     detail_text, context_json)
SELECT (SELECT coalesce(max(request_alert_id), 0) FROM investigation_request_alerts) + row_number() OVER (),
       $1, $2, $3, $4, k.key, $6, $7, $8, $9, $10
FROM (SELECT unnest($5::VARCHAR[]) AS key) AS k";

    /* InvestigationDuckDbSpikeTests (a): a NULL once_key never conflicts, and a repeated key inserts nothing, so Darling's
       partial-index arbiter (WHERE once_key IS NOT NULL) is not needed. */
    internal const string JournalSql = @"
INSERT INTO investigation_journal (journal_id, event_time, request_id, route_id, server_id, event, actor, reason, detail, once_key)
VALUES ((SELECT coalesce(max(journal_id), 0) + 1 FROM investigation_journal), $1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (once_key) DO NOTHING";

    /* #5914: every link of one report in one statement. The ids continue from the table's max plus row_number(), the shape of
       the sweep statements below: a max() subquery in a per-row VALUES would not see this statement's own rows. ord keeps the
       order the agent gave the links in, and a repeated once_key writes nothing. */
    internal const string ReportLinksInsertSql = @"
INSERT INTO investigation_journal (journal_id, event_time, request_id, server_id, event, actor, reason, detail, once_key)
SELECT (SELECT coalesce(max(journal_id), 0) FROM investigation_journal) + row_number() OVER (ORDER BY l.ord),
       $1, $2, $3, 'link_recorded', $4, l.kind, l.detail, l.once_key
FROM (SELECT unnest($5::VARCHAR[]) AS kind, unnest($6::VARCHAR[]) AS detail, unnest($7::VARCHAR[]) AS once_key,
             unnest($8::INTEGER[]) AS ord) AS l
ON CONFLICT (once_key) DO NOTHING";

    internal const string AgentJournalCountSql = @"
SELECT count(*)
FROM investigation_journal
WHERE request_id = $1
  AND actor LIKE 'agent:%'";

    internal const string ClaimSql = @"
UPDATE investigation_requests
SET state = 'claimed',
    claimed_by = $2,
    claim_id = $3,
    claimed_at = $4,
    lease_until = $4 + to_minutes(CAST(lease_minutes AS BIGINT)),
    claim_count = claim_count + 1
WHERE request_id = $1
  AND (state = 'open' OR (state = 'claimed' AND lease_until < $4))
  AND claim_count < 3
  AND expires_at > $4
RETURNING lease_until, claim_count, server_id";

    internal const string ClaimWhySql = @"
SELECT state, lease_until, claim_count, expires_at, server_id
FROM investigation_requests
WHERE request_id = $1";

    internal const string SubmitLockSql = @"
SELECT state, claim_id, server_id, first_metric, first_dedup_key, story_path_hash, route_id
FROM investigation_requests
WHERE request_id = $1";

    /* #6012 (Darling's PendingFollowUpsSql, #6008): the pending follow-ups of a route: reported requests that set follow_up_at and
       whose follow-up has not opened. Lite has no practice requests, so there is no practice_of filter. $1 route. */
    internal const string PendingFollowUpsSql = @"
SELECT count(*)
FROM investigation_requests AS r
WHERE r.route_id = $1
  AND r.state = 'reported'
  AND r.follow_up_at IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM investigation_requests AS f WHERE f.follow_up_of = r.request_id)";

    /* Written only when the report set a time. $1 request, $2 time. */
    internal const string SetFollowUpSql = @"
UPDATE investigation_requests SET follow_up_at = $2 WHERE request_id = $1";

    /* The reported requests whose follow-up is due and not yet opened, oldest first (Darling's DueFollowUpsSql without the deploy
       condition: Lite takes the time only). $1 now. */
    internal const string DueFollowUpsSql = @"
SELECT r.request_id
FROM investigation_requests AS r
WHERE r.state = 'reported'
  AND r.follow_up_at IS NOT NULL
  AND r.follow_up_at <= $1
  AND NOT EXISTS (SELECT 1 FROM investigation_requests AS f WHERE f.follow_up_of = r.request_id)
ORDER BY r.follow_up_at, r.request_id
LIMIT 50";

    /* The follow-up request copies its source's route, server, family, metric, story path, alert times, life and level, and names the
       source in follow_up_of (Darling's OpenFollowUpSql). $1 new id, $2 source, $3 now. */
    internal const string OpenFollowUpSql = @"
INSERT INTO investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, story_path_hash, first_alert_at, last_alert_at,
     window_until, opened_at, expires_at, lease_minutes, autonomy_level, alert_count, max_severity, follow_up_of)
SELECT $1, r.route_id, r.server_id, r.server_name, r.family, r.first_metric, r.story_path_hash, r.first_alert_at, r.last_alert_at,
       r.window_until, $3, $3 + (r.expires_at - r.opened_at), r.lease_minutes, r.autonomy_level, r.alert_count, r.max_severity, r.request_id
FROM investigation_requests AS r
WHERE r.request_id = $2
RETURNING route_id, server_id";

    /* The source's alert rows, without the dedup key, so the brief's alerts section reads what the source's read. $1 new, $2 source. */
    internal const string CopyFollowUpAlertsSql = @"
INSERT INTO investigation_request_alerts
    (request_alert_id, request_id, alert_time, server_id, metric_name, dedup_key, current_value, threshold_value, muted, detail_text, context_json)
SELECT (SELECT coalesce(max(request_alert_id), 0) FROM investigation_request_alerts) + row_number() OVER (ORDER BY request_alert_id),
       $1, alert_time, server_id, metric_name, NULL, current_value, threshold_value, muted, detail_text, context_json
FROM investigation_request_alerts
WHERE request_id = $2";

    internal const string ReportInsertSql = @"
INSERT INTO investigation_reports
    (report_id, request_id, report_format, agent_id, claim_id, submitted_at, verdict, root_cause, root_cause_found, summary,
     evidence, server_id, metric_name, dedup_key, story_path_hash, family_of, queries_run)
VALUES ((SELECT coalesce(max(report_id), 0) + 1 FROM investigation_reports), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
        $12, $13, $14, $15, $16)";

    /* #6004, Lite's twin of Darling's RequestExistsSql: a report's family_of must name a request. DuckDB has no foreign key
       here, so this check is the guard; the retention purge can remove that request later (see ReadReportsAsync). */
    internal const string RequestExistsSql = "SELECT 1 FROM investigation_requests WHERE request_id = $1";

    internal const string ReportedSql = @"
UPDATE investigation_requests
SET state = 'reported', reported_at = $2, lease_until = NULL
WHERE request_id = $1";

    internal const string SetBriefSql = @"
UPDATE investigation_requests
SET brief_format = $3, brief = $4
WHERE request_id = $1 AND claim_id = $2 AND state = 'claimed'";

    internal const string ReleaseSql = @"
UPDATE investigation_requests
SET state = 'open', claimed_by = NULL, claim_id = NULL, claimed_at = NULL, lease_until = NULL
WHERE request_id = $1 AND claim_id = $2 AND state = 'claimed'";

    internal const string OpenedSinceSql = @"
SELECT request_id, route_id, server_name, family, opened_at, server_id
FROM investigation_requests
WHERE opened_at > $1
  AND ($2::INTEGER[] IS NULL OR list_contains($2::INTEGER[], route_id))
ORDER BY opened_at, request_id::VARCHAR
LIMIT $3";

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
    FROM investigation_requests AS r
    WHERE ($3::INTEGER IS NULL OR r.server_id = $3)
      AND ($4::VARCHAR IS NULL OR r.family = $4)
) AS s
WHERE ($2::VARCHAR IS NULL OR s.effective_state = $2)
ORDER BY s.opened_at DESC, s.request_id::VARCHAR
LIMIT $5";

    internal const string RenewSql = @"
UPDATE investigation_requests
SET lease_until = least($3 + to_minutes(CAST(lease_minutes AS BIGINT)), expires_at)
WHERE request_id = $1
  AND claim_id = $2
  AND state = 'claimed'
  AND lease_until >= $3
  AND expires_at > $3
RETURNING lease_until";

    internal const string ClaimHolderSql = @"
SELECT claimed_by
FROM investigation_requests
WHERE request_id = $1
  AND claim_id = $2";

    internal const string RouteUseSql = @"
SELECT route_id,
       count(*) FILTER (WHERE opened_at > $1 - INTERVAL 1 HOUR),
       count(*)
FROM investigation_requests
WHERE opened_at > $1 - INTERVAL 1 DAY
GROUP BY route_id";

    internal const string RefusalServerSql = @"
SELECT server_id
FROM investigation_requests
WHERE request_id = $1";

    /* Darling fills created_at and expires_at (90 days) by default; DuckDB's default clock is local-zone, so both are bound. */
    internal const string ProposedNoteSql = @"
INSERT INTO investigation_notes
    (note_id, server_id, story_path_hash, dedup_key, metric_name, note_text, author, source_request_id, state, created_at, expires_at)
SELECT (SELECT coalesce(max(note_id), 0) FROM investigation_notes) + 1,
       r.server_id, r.story_path_hash, r.first_dedup_key, r.first_metric, $2, $3, r.request_id, 'pending', $4,
       $4 + INTERVAL 90 DAY
FROM investigation_requests AS r
WHERE r.request_id = $1";

    /* #5903: the pending notes for the approve screen, oldest first, with the request that proposed each (server name,
       family). Mirrors Darling's PendingNotesSql, expiry filter included: a note past its expires_at is not listed, because the
       brief read drops it and an approval would do nothing. created_at and expires_at are naive UTC. $1 the limit, $2 now. */
    internal const string PendingNotesSql = @"
SELECT n.note_id, n.server_id, r.server_name, n.metric_name, r.family, n.story_path_hash, n.note_text, n.author,
       n.source_request_id, n.created_at, n.expires_at
FROM investigation_notes AS n
LEFT JOIN investigation_requests AS r ON r.request_id = n.source_request_id
WHERE n.state = 'pending' AND n.expires_at > $2
ORDER BY n.created_at, n.note_id
LIMIT $1";

    /* #5903: a decision lands only on a pending note, so a second click changes nothing. $1 note, $2 the new state, $3 who,
       $4 when (bound naive UTC, as the rest of the store binds it). */
    internal const string DecideNoteSql = @"
UPDATE investigation_notes
SET state = $2, decided_by = $3, decided_at = $4
WHERE note_id = $1 AND state = 'pending'
RETURNING source_request_id, server_id";

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
       r.opened_at, r.expires_at, r.claimed_by, r.lease_until, r.claim_count, r.reported_at, r.brief
FROM investigation_requests AS r
WHERE ($2::UUID IS NULL OR r.request_id = $2)
  AND ($3::INTEGER IS NULL OR r.server_id = $3)
  AND ($2::UUID IS NOT NULL OR r.opened_at >= $4)
ORDER BY r.opened_at DESC, r.request_id::VARCHAR
LIMIT $5";

    internal const string ReportsSql = @"
SELECT request_id, report_format, agent_id, submitted_at, verdict, root_cause, root_cause_found, summary, evidence,
       family_of, queries_run
FROM investigation_reports
WHERE list_contains($1::VARCHAR[], request_id::VARCHAR)
ORDER BY submitted_at DESC";

    /* #5914: Darling's ReportLinksSql; ReportsJournalSql below excludes link_recorded as Darling's does. */
    internal const string ReportLinksSql = @"
SELECT request_id, detail
FROM investigation_journal
WHERE list_contains($1::VARCHAR[], request_id::VARCHAR)
  AND event = 'link_recorded'
ORDER BY journal_id";

    internal const string ReportsJournalSql = @"
SELECT event_time, request_id, server_id, event, actor, reason, detail
FROM investigation_journal
WHERE list_contains($1::VARCHAR[], request_id::VARCHAR)
  AND event <> 'link_recorded'
ORDER BY event_time DESC, journal_id DESC
LIMIT $2";

    /* DuckDB has no writable CTE, so Darling's two sweep statements become three under one write lock and one
       transaction: SweepLapsedSql as Darling's; SweepExpireSql's journal INSERT from a SELECT with the UPDATE's own
       predicate; then SweepExpireUpdateSql. The INSERT runs first, while the rows still match. */
    internal const string SweepLapsedSql = @"
INSERT INTO investigation_journal (journal_id, event_time, request_id, server_id, event, actor, reason, once_key)
SELECT (SELECT coalesce(max(journal_id), 0) FROM investigation_journal) + row_number() OVER (ORDER BY r.request_id::VARCHAR),
       $1, r.request_id, r.server_id, 'lease_expired', 'service', r.claimed_by, 'lease_expired:' || r.claim_id::VARCHAR
FROM investigation_requests AS r
WHERE r.state = 'claimed' AND r.lease_until < $1
ON CONFLICT (once_key) DO NOTHING";

    internal const string SweepExpireSql = @"
INSERT INTO investigation_journal (journal_id, event_time, request_id, server_id, event, actor, once_key)
SELECT (SELECT coalesce(max(journal_id), 0) FROM investigation_journal) + row_number() OVER (ORDER BY r.request_id::VARCHAR),
       $1, r.request_id, r.server_id, CASE WHEN r.expires_at > $1 THEN 'claims_exhausted' ELSE 'expired' END, 'service',
       'expired:' || r.request_id::VARCHAR
FROM investigation_requests AS r
WHERE (r.state IN ('open', 'claimed') AND r.expires_at <= $1)
   OR (r.state = 'claimed' AND r.lease_until < $1 AND r.claim_count >= 3)
   OR (r.state = 'open' AND r.claim_count >= 3)
ON CONFLICT (once_key) DO NOTHING";

    internal const string SweepExpireUpdateSql = @"
UPDATE investigation_requests
SET state = 'expired'
WHERE (state IN ('open', 'claimed') AND expires_at <= $1)
   OR (state = 'claimed' AND lease_until < $1 AND claim_count >= 3)
   OR (state = 'open' AND claim_count >= 3)";

    /* Retention, as Darling's (DarlingRetention.InvestigationRetentionDays and InvestigationBriefRetentionDays): a finished
       request and its journal keep a year, a brief 90 days. There is no ON DELETE CASCADE here, so a request's alert rows
       and report go first, then the request, then the journal rows past the cutoff. Notes and profiles are configuration
       and get no pass, as in Darling. */
    internal const int RetentionDays = 365;

    internal const int BriefRetentionDays = 90;

    internal const string RetentionDoomedRequests = @"
SELECT request_id FROM investigation_requests WHERE state NOT IN ('open', 'claimed') AND opened_at < $1";

    internal const string RetentionAlertRowsSql =
        "DELETE FROM investigation_request_alerts WHERE request_id IN (" + RetentionDoomedRequests + ")";

    internal const string RetentionEvidenceSql =
        "DELETE FROM investigation_evidence WHERE request_id IN (" + RetentionDoomedRequests + ")";

    internal const string RetentionReportsSql =
        "DELETE FROM investigation_reports WHERE request_id IN (" + RetentionDoomedRequests + ")";

    /* #6011: Darling's family_of is a foreign key ON DELETE SET NULL. DuckDB has no foreign key here, so a purge clears the id
       of every surviving report whose family request it is about to remove: a family_of that is served always names a request. */
    internal const string RetentionFamilyOfSql =
        "UPDATE investigation_reports SET family_of = NULL WHERE family_of IN (" + RetentionDoomedRequests + ")"
        + " AND request_id NOT IN (" + RetentionDoomedRequests + ")";

    /* #6012: Darling's follow_up_of is a foreign key ON DELETE SET NULL; a purge clears it the way it clears family_of, so a served
       follow_up_of always names a request. A follow-up that is itself doomed goes with the delete. */
    internal const string RetentionFollowUpOfSql =
        "UPDATE investigation_requests SET follow_up_of = NULL WHERE follow_up_of IN (" + RetentionDoomedRequests + ")"
        + " AND request_id NOT IN (" + RetentionDoomedRequests + ")";

    internal const string RetentionRequestsSql =
        "DELETE FROM investigation_requests WHERE state NOT IN ('open', 'claimed') AND opened_at < $1";

    internal const string RetentionJournalSql = "DELETE FROM investigation_journal WHERE event_time < $1";

    internal const string ClearBriefSql =
        "UPDATE investigation_requests SET brief = NULL WHERE brief IS NOT NULL AND opened_at < $1";

    private readonly DuckDbInitializer _db;
    private readonly Func<DateTime> _utcNow;
    private readonly TimeSpan _lockBudget;
    private readonly InvestigationServerNameLookup.Snapshot? _serverNames;

    /// <summary><paramref name="utcNow"/> is the clock (tests move it); it defaults to the wall clock.
    /// <paramref name="lockBudget"/> bounds every wait for the write lock; it defaults to
    /// <see cref="LocalDataService.DefaultWriteLockBudget"/> (5 s), the same budget Darling's store gives its locks.
    /// <paramref name="serverNames"/> (#6011) is Lite's server list, read at each read so a rename shows on the next one; null
    /// leaves <c>display_name</c> and <c>instance_name</c> off every row (a store with no list to read).</summary>
    public DuckDbInvestigationStore(DuckDbInitializer db, Func<DateTime>? utcNow = null, TimeSpan? lockBudget = null,
        InvestigationServerNameLookup.Snapshot? serverNames = null)
    {
        _serverNames = serverNames;
        _db = db ?? throw new ArgumentNullException(nameof(db));
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _lockBudget = lockBudget ?? LocalDataService.DefaultWriteLockBudget;
    }

    private DateTime Now() => Micro(_utcNow());

    /// <summary>A time as DuckDB keeps it: UTC, truncated to the microsecond (InvestigationDuckDbSpikeTests (d)).</summary>
    internal static DateTime Micro(DateTime value)
    {
        var utc = DateTime.SpecifyKind(value, DateTimeKind.Utc);
        return new DateTime(utc.Ticks - utc.Ticks % 10, DateTimeKind.Utc);
    }

    /// <inheritdoc />
    public async Task<InvestigationOpenResult> OpenOrJoinAsync(InvestigationAlert alert, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(alert);
        var now = Now();
        var alertTime = Micro(alert.AlertTimeUtc);
        var keys = alert.DedupKeys.Where(k => !string.IsNullOrEmpty(k)).Distinct(StringComparer.Ordinal).ToArray();

        using var writeLock = _db.AcquireWriteLock(_lockBudget);
        using var connection = await OpenAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();

        var severity = string.Equals(alert.Severity, InvestigationEvents.Critical, StringComparison.Ordinal)
            ? InvestigationEvents.Critical : InvestigationEvents.Warning;
        Guid? joinId = null;
        string? maxSeverity = null;
        var newMetric = false;
        using (var find = Command(connection, FindJoinSql, alert.ServerId, alert.Family, now, alertTime, alert.MetricName, severity))
        using (var found = await find.ExecuteReaderAsync(cancellationToken))
        {
            if (await found.ReadAsync(cancellationToken))
            {
                joinId = found.GetGuid(0);
                maxSeverity = found.IsDBNull(1) ? null : found.GetString(1);
                newMetric = !found.GetBoolean(2);
            }
        }

        if (joinId is Guid join)
        {
            /* #6010, D6 (Darling's #6003): a join is an event only when it brings a metric the request has not seen or a
               severity above its max_severity; any other join is journaled as a quiet join. */
            var evented = InvestigationEvents.JoinIsEvent(newMetric, maxSeverity, severity);
            var newMax = InvestigationEvents.SeverityRank(severity) > InvestigationEvents.SeverityRank(maxSeverity)
                ? severity
                : maxSeverity ?? severity;
            await ExecAsync(connection, JoinSql, cancellationToken, join, alertTime, newMax);
            await InsertAlertRowsAsync(connection, join, alert, alertTime, keys, cancellationToken);
            await JournalAsync(connection, now, join, alert.RouteId, alert.ServerId,
                evented ? InvestigationEvents.Joined : InvestigationEvents.QuietJoin, "service",
                alert.MetricName,
                evented ? "{\"new_metric\":" + (newMetric ? "true" : "false") + ",\"severity\":\"" + severity + "\"}" : null,
                null, cancellationToken);
            transaction.Commit();
            return new InvestigationOpenResult(InvestigationOpenOutcome.Joined, join) { Evented = evented };
        }

        long lastHour, lastDay;
        using (var count = Command(connection, CountRouteSql, alert.RouteId, now))
        using (var reader = await count.ExecuteReaderAsync(cancellationToken))
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
            await JournalAsync(connection, now, null, alert.RouteId, alert.ServerId, "limit_reached", "service",
                which, detail, onceKey, cancellationToken);
            transaction.Commit();
            return new InvestigationOpenResult(InvestigationOpenOutcome.LimitReached, null);
        }

        var requestId = Guid.NewGuid();
        await ExecAsync(connection, OpenSql, cancellationToken,
            requestId, alert.RouteId, alert.ServerId, alert.ServerName, alert.Family, alert.MetricName,
            keys.FirstOrDefault(), alert.StoryPathHash, alertTime, now, alert.ExpiryMinutes, alert.LeaseMinutes,
            string.Equals(alert.Severity, InvestigationEvents.Critical, StringComparison.Ordinal)
                ? InvestigationEvents.Critical : InvestigationEvents.Warning);
        await InsertAlertRowsAsync(connection, requestId, alert, alertTime, keys, cancellationToken);
        await JournalAsync(connection, now, requestId, alert.RouteId, alert.ServerId, "opened", "service",
            alert.MetricName, null, null, cancellationToken);
        transaction.Commit();
        return new InvestigationOpenResult(InvestigationOpenOutcome.Opened, requestId);
    }

    /// <inheritdoc />
    public async Task<InvestigationClaimResult> ClaimAsync(Guid requestId, string agentId, CancellationToken cancellationToken = default)
    {
        if (!InvestigationRules.IsValidAgentId(agentId))
        {
            /* Before the lock: RecordRefusalAsync takes it itself, and the lock does not nest. */
            await RecordRefusalAsync(requestId, InvestigationRules.InvalidAgentId, "claim", "agent_id_invalid", null, cancellationToken);
            return new InvestigationClaimResult(InvestigationClaimOutcome.AgentIdInvalid, null, null, 0, "agent_id_invalid");
        }

        var now = Now();
        var claimId = Guid.NewGuid();
        var actor = "agent:" + agentId;

        using var writeLock = _db.AcquireWriteLock(_lockBudget);
        using var connection = await OpenAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();

        if (await JournalCappedAsync(connection, now, requestId, actor, "claim", cancellationToken))
        {
            transaction.Commit();
            return new InvestigationClaimResult(InvestigationClaimOutcome.JournalCapped, null, null, 0, "journal_capped");
        }

        DateTime? claimedLease = null;
        int claimedCount = 0, claimedServer = 0;
        using (var claim = Command(connection, ClaimSql, requestId, agentId, claimId, now))
        using (var reader = await claim.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                claimedLease = Utc(reader.GetDateTime(0));
                claimedCount = reader.GetInt32(1);
                claimedServer = reader.GetInt32(2);
            }
        }

        if (claimedLease is DateTime leaseUntilClaimed)
        {
            await JournalAsync(connection, now, requestId, null, claimedServer, "claimed", actor,
                null, null, "claimed:" + claimId.ToString(), cancellationToken);
            transaction.Commit();
            return new InvestigationClaimResult(InvestigationClaimOutcome.Claimed, claimId, leaseUntilClaimed, claimedCount, null);
        }

        InvestigationClaimOutcome outcome;
        string reason;
        int count = 0;
        int? server = null;
        using (var why = Command(connection, ClaimWhySql, requestId))
        using (var reader = await why.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                outcome = InvestigationClaimOutcome.NotFound;
                reason = "not_found";
            }
            else
            {
                var state = reader.GetString(0);
                DateTime? leaseUntil = reader.IsDBNull(1) ? null : Utc(reader.GetDateTime(1));
                count = reader.GetInt32(2);
                var expiresAt = Utc(reader.GetDateTime(3));
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

        var onceKey = outcome == InvestigationClaimOutcome.NotFound
            ? string.Create(CultureInfo.InvariantCulture, $"refused:claim:not_found:{now:yyyy-MM-ddTHH}")
            : "refused:claim:" + requestId.ToString() + ":" + reason;
        await JournalAsync(connection, now, outcome == InvestigationClaimOutcome.NotFound ? null : requestId, null, server,
            "refused", actor, "claim:" + reason, null, onceKey, cancellationToken);
        transaction.Commit();
        return new InvestigationClaimResult(outcome, null, null, count, reason);
    }

    /// <inheritdoc />
    public async Task<bool> SetBriefAsync(Guid requestId, Guid claimId, short briefFormat, string briefJson, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(briefJson);
        using var writeLock = _db.AcquireWriteLock(_lockBudget);
        using var connection = await OpenAsync(cancellationToken);
        return await ExecAsync(connection, SetBriefSql, cancellationToken, requestId, claimId, briefFormat, briefJson) == 1;
    }

    /// <inheritdoc />
    public async Task<bool> ReleaseAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default)
    {
        using var writeLock = _db.AcquireWriteLock(_lockBudget);
        using var connection = await OpenAsync(cancellationToken);
        return await ExecAsync(connection, ReleaseSql, cancellationToken, requestId, claimId) == 1;
    }

    /// <inheritdoc />
    /// <remarks>Darling's V184 CHECKs hold the report caps in the database (root cause, evidence size, agent id); DuckDB's
    /// tables carry no such CHECK, so this store refuses a report over a cap with an <see cref="ArgumentException"/> before
    /// it writes anything, where PostgreSQL would raise 23514. The tool validates first, so only a caller that skips it
    /// reaches this.</remarks>
    public async Task<InvestigationSubmitOutcome> SubmitAsync(InvestigationReportSubmission report, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(report);
        if (!InvestigationRules.IsValidAgentId(report.AgentId))
        {
            await RecordRefusalAsync(report.RequestId, InvestigationRules.InvalidAgentId, "submit", "agent_id_invalid", null, cancellationToken);
            return InvestigationSubmitOutcome.AgentIdInvalid;
        }

        var caps = InvestigationToolCore.CapProblems(report.RootCause, report.EvidenceJson);
        if (caps.Count > 0)
        {
            throw new ArgumentException(string.Join(" ", caps.Select(c => c.Problem)), nameof(report));
        }

        InvestigationReportLinks.ThrowIfInvalid(report.Links, nameof(report));
        /* #6004: and the queries_run caps, never clamped (the tool's CapProblems already refused them; this is the store's own). */
        InvestigationReportFields.ThrowIfInvalid(report.QueriesRunJson, nameof(report));

        var now = Now();
        var actor = "agent:" + report.AgentId;

        using var writeLock = _db.AcquireWriteLock(_lockBudget);
        using var connection = await OpenAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();

        string? state = null;
        Guid? currentClaim = null;
        int serverId = 0, routeId = 0;
        string? metric = null, dedupKey = null, story = null;
        using (var command = Command(connection, SubmitLockSql, report.RequestId))
        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
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

        if (state is not null
            && await JournalCappedAsync(connection, now, report.RequestId, actor, "submit", cancellationToken))
        {
            transaction.Commit();
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
            var onceKey = state is null
                ? string.Create(CultureInfo.InvariantCulture, $"refused:submit:not_found:{now:yyyy-MM-ddTHH}")
                : "refused:submit:" + report.RequestId.ToString() + ":" + reason;
            await JournalAsync(connection, now, state is null ? null : report.RequestId, null, state is null ? null : serverId,
                "refused", actor, "submit:" + reason, null, onceKey, cancellationToken);
            transaction.Commit();
            return outcome;
        }

        /* #6012 (Darling's #6008): a follow-up time over the route's pending cap refuses the whole report, before anything is written.
           The write lock held since the top makes concurrent submits count in turn. */
        if (report.FollowUpAtUtc is not null)
        {
            long pending;
            using (var count = Command(connection, PendingFollowUpsSql, routeId))
            {
                pending = Convert.ToInt64(await count.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
            }

            if (pending >= InvestigationReportFields.MaxPendingFollowUpsPerRoute)
            {
                transaction.Commit();
                return InvestigationSubmitOutcome.FollowUpLimit;
            }
        }

        await ExecAsync(connection, ReportInsertSql, cancellationToken,
            report.RequestId, report.ReportFormat, report.AgentId, report.ClaimId, now, report.Verdict,
            report.RootCause, report.RootCauseFound, report.Summary, report.EvidenceJson, serverId,
            metric, dedupKey, story, report.FamilyOf, report.QueriesRunJson);
        if (!string.IsNullOrWhiteSpace(report.ProposedNote))
        {
            await ExecAsync(connection, ProposedNoteSql, cancellationToken, report.RequestId, report.ProposedNote, actor, now);
        }

        await ExecAsync(connection, ReportedSql, cancellationToken, report.RequestId, now);
        if (report.FollowUpAtUtc is { } followUpAt)
        {
            await ExecAsync(connection, SetFollowUpSql, cancellationToken, report.RequestId, followUpAt);
        }

        await JournalAsync(connection, now, report.RequestId, null, serverId, "reported", actor,
            report.Verdict, null, null, cancellationToken);
        /* #5914, Darling's rule in the shared contract: each link a link_recorded row, once per request and URL. Lite's submit
           tool takes links since #6011 (commit and release among the kinds). */
        var links = InvestigationReportLinks.Distinct(report.Links ?? []);
        if (links.Count > 0)
        {
            await ExecAsync(connection, ReportLinksInsertSql, cancellationToken,
                now, report.RequestId, serverId, actor,
                links.Select(l => (string?)l.Kind).ToList(),
                links.Select(l => (string?)InvestigationReportLinks.DetailJson(l)).ToList(),
                links.Select(l => (string?)InvestigationReportLinks.OnceKey(report.RequestId, l.Url)).ToList(),
                Enumerable.Range(0, links.Count).ToList());
        }

        transaction.Commit();
        return InvestigationSubmitOutcome.Reported;
    }

    /// <inheritdoc />
    public async Task<InvestigationRenewResult> RenewAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default)
    {
        using var writeLock = _db.AcquireWriteLock(_lockBudget);
        using var connection = await OpenAsync(cancellationToken);
        using var command = Command(connection, RenewSql, requestId, claimId, Now());
        var lease = await command.ExecuteScalarAsync(cancellationToken);
        return lease is DateTime until
            ? new InvestigationRenewResult(true, Utc(until))
            : new InvestigationRenewResult(false, null);
    }

    /// <inheritdoc />
    public async Task<string?> ClaimHolderAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenAsync(cancellationToken);
        using var command = Command(connection, ClaimHolderSql, requestId, claimId);
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<int, (int LastHour, int LastDay)>> RouteUseAsync(CancellationToken cancellationToken = default)
    {
        var use = new Dictionary<int, (int LastHour, int LastDay)>();
        using var connection = await OpenAsync(cancellationToken);
        using var command = Command(connection, RouteUseSql, Now());
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            use[reader.GetInt32(0)] = ((int)reader.GetInt64(1), (int)reader.GetInt64(2));
        }

        return use;
    }

    /// <summary>
    /// #6596: journals an alert on an agent route that opened no request because its story is recurring at this hour
    /// (<see cref="InvestigationEvents.RecurringSkipped"/>). Darling's <c>InvestigationRequestStore.RecordRecurringSkipAsync</c>:
    /// no request id, the metric as the reason, once per route, server, metric and alert hour.
    /// </summary>
    public async Task RecordRecurringSkipAsync(InvestigationAlert alert, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(alert);
        var alertTime = Micro(alert.AlertTimeUtc);
        using var writeLock = _db.AcquireWriteLock(_lockBudget);
        using var connection = await OpenAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();
        await JournalAsync(connection, Now(), null, alert.RouteId, alert.ServerId, InvestigationEvents.RecurringSkipped,
            "service", alert.MetricName, InvestigationEvents.RecurringSkippedDetail(alert.StoryPathHash, alertTime),
            InvestigationEvents.RecurringSkippedOnceKey(alert.RouteId, alert.ServerId, alert.MetricName, alertTime), cancellationToken);
        transaction.Commit();
    }

    /// <inheritdoc />
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
        using var writeLock = _db.AcquireWriteLock(_lockBudget);
        using var connection = await OpenAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();
        int? serverId = null;
        using (var command = Command(connection, RefusalServerSql, requestId))
        {
            if (await command.ExecuteScalarAsync(cancellationToken) is int server)
            {
                serverId = server;
            }
        }

        var onceKey = serverId is null
            ? string.Create(CultureInfo.InvariantCulture, $"refused:{operation}:not_found:{reason}:{now:yyyy-MM-ddTHH}")
            : "refused:" + operation + ":" + requestId.ToString() + ":" + reason;
        await JournalAsync(connection, now, serverId is null ? null : requestId, null, serverId, "refused",
            "agent:" + agentId, operation + ":" + reason, detailJson, onceKey, cancellationToken);
        transaction.Commit();
    }

    /// <inheritdoc />
    public async Task<bool> RequestExistsAsync(Guid requestId, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenAsync(cancellationToken);
        using var command = Command(connection, RequestExistsSql, requestId);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <inheritdoc />
    public async Task<(InvestigationRequestRow Row, string? Brief)?> ReadRequestAsync(Guid requestId,
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenAsync(cancellationToken);
        using var command = Command(connection, ReportsRequestsSql, Now(), requestId, null, DateTime.MinValue, 1);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return (Named(RequestRow(reader), NamesNow()), reader.IsDBNull(16) ? null : reader.GetString(16));
    }

    /// <inheritdoc />
    public async Task<InvestigationReportsRead> ReadReportsAsync(Guid? requestId, int? serverId, DateTime sinceUtc, int limit,
        int journalLimit, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(journalLimit, 1);
        using var connection = await OpenAsync(cancellationToken);
        var requests = new List<InvestigationRequestRow>();
        var names = NamesNow();
        string? brief = null;
        using (var command = Command(connection, ReportsRequestsSql, Now(), requestId, serverId, Micro(sinceUtc), limit + 1))
        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                requests.Add(Named(RequestRow(reader), names));
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

        var reports = new List<InvestigationReportRow>();
        var journal = new List<InvestigationJournalRow>();
        if (requests.Count == 0)
        {
            return new InvestigationReportsRead(requests, reports, journal, brief, requestsTruncated, EvidenceByBasis: true);
        }

        var ids = requests.Select(r => r.RequestId.ToString("D")).ToList();
        using (var command = Command(connection, ReportsSql, ids))
        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                reports.Add(new InvestigationReportRow(
                    reader.GetGuid(0), reader.GetInt16(1), reader.GetString(2), Utc(reader.GetDateTime(3)), reader.GetString(4),
                    reader.GetString(5), reader.GetBoolean(6), reader.IsDBNull(7) ? null : reader.GetString(7), reader.GetString(8),
                    FamilyOf: reader.IsDBNull(9) ? null : reader.GetGuid(9), QueriesRunJson: reader.IsDBNull(10) ? null : reader.GetString(10)));
            }
        }

        var links = new Dictionary<Guid, List<InvestigationReportLink>>();
        using (var command = Command(connection, ReportLinksSql, ids))
        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
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

        for (var i = 0; i < reports.Count && links.Count > 0; i++)
        {
            if (links.TryGetValue(reports[i].RequestId, out var list))
                reports[i] = reports[i] with { Links = list };
        }

        using (var command = Command(connection, ReportsJournalSql, ids, journalLimit + 1))
        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                journal.Add(new InvestigationJournalRow(
                    Utc(reader.GetDateTime(0)), reader.IsDBNull(1) ? null : reader.GetGuid(1),
                    reader.IsDBNull(2) ? null : reader.GetInt32(2), reader.GetString(3), reader.GetString(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetString(6)));
            }
        }

        var journalTruncated = journal.Count > journalLimit;
        if (journalTruncated)
        {
            journal.RemoveRange(journalLimit, journal.Count - journalLimit);
        }

        return new InvestigationReportsRead(requests, reports, journal, brief, requestsTruncated, journalTruncated, EvidenceByBasis: true);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<InvestigationRequestRow>> ListAsync(
        string? state, int? serverId, string? family, int limit, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        using var connection = await OpenAsync(cancellationToken);
        using var command = Command(connection, ListSql, Now(), state, serverId, family, limit);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var names = NamesNow();
        var rows = new List<InvestigationRequestRow>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(Named(RequestRow(reader), names));
        }

        return rows;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<InvestigationOpenedRow>> OpenedSinceAsync(
        DateTime sinceUtc, IReadOnlyCollection<int>? routeIds, int limit, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        using var connection = await OpenAsync(cancellationToken);
        using var command = Command(connection, OpenedSinceSql,
            Micro(sinceUtc), routeIds is { Count: > 0 } ? routeIds.ToList() : null, limit);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var names = NamesNow();
        var rows = new List<InvestigationOpenedRow>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new InvestigationOpenedRow(
                reader.GetGuid(0), reader.GetInt32(1), reader.GetString(2), reader.GetString(3), Utc(reader.GetDateTime(4))));
            if (names is not null)
            {
                var (display, instance) = InvestigationServerNameLookup.Resolve(names, reader.GetInt32(5), reader.GetString(2));
                rows[^1] = rows[^1] with { DisplayName = display, InstanceName = instance };
            }
        }

        return rows;
    }

    /// <inheritdoc />
    public async Task<InvestigationSweepResult> SweepAsync(CancellationToken cancellationToken = default)
    {
        var now = Now();
        using var writeLock = _db.AcquireWriteLock(_lockBudget);
        using var connection = await OpenAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();
        var lapsed = await ExecAsync(connection, SweepLapsedSql, cancellationToken, now);
        await ExecAsync(connection, SweepExpireSql, cancellationToken, now);
        var expired = await ExecAsync(connection, SweepExpireUpdateSql, cancellationToken, now);
        transaction.Commit();

        /* #6010, D7: its own transaction, so a failure there cannot hold back the expiry above (Darling's order). */
        using var resolvedTransaction = connection.BeginTransaction();
        var resolved = await SweepResolvedAsync(connection, now, cancellationToken);
        resolvedTransaction.Commit();

        /* #6012 (Darling's #6008): its own transaction too, so a follow-up that cannot open holds back neither the expiry nor the
           resolved journal. */
        using var followUpTransaction = connection.BeginTransaction();
        var followUps = await SweepFollowUpsAsync(connection, now, cancellationToken);
        followUpTransaction.Commit();
        return new InvestigationSweepResult(lapsed, expired) { Resolved = resolved, FollowUps = followUps };
    }

    /// <summary>
    /// The follow-up sweep (#6012, Darling's #6008): for each reported request whose <c>follow_up_at</c> has come and that has no
    /// follow-up yet, opens a new request that names it in <c>follow_up_of</c> and journals one <c>follow_up</c> event (trigger
    /// <c>time</c>) for the new request, once per earlier request. The new request's brief reads the earlier report as it reads any
    /// earlier report of the same server and family. Lite has no event delivery, so an agent finds the follow-up in the request list.
    /// Returns the follow-ups opened.
    /// </summary>
    private async Task<int> SweepFollowUpsAsync(DuckDBConnection connection, DateTime now, CancellationToken cancellationToken)
    {
        var due = new List<Guid>();
        using (var candidates = Command(connection, DueFollowUpsSql, now))
        using (var reader = await candidates.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                due.Add(reader.GetGuid(0));
            }
        }

        foreach (var earlier in due)
        {
            var followUpId = Guid.NewGuid();
            int routeId, serverId;
            using (var insert = Command(connection, OpenFollowUpSql, followUpId, earlier, now))
            using (var returned = await insert.ExecuteReaderAsync(cancellationToken))
            {
                await returned.ReadAsync(cancellationToken);
                routeId = returned.GetInt32(0);
                serverId = returned.GetInt32(1);
            }

            await ExecAsync(connection, CopyFollowUpAlertsSql, cancellationToken, followUpId, earlier);
            await JournalAsync(connection, now, followUpId, routeId, serverId, InvestigationEvents.FollowUp, "service",
                null, "{\"follow_up_of\":\"" + earlier.ToString("D") + "\",\"trigger\":\"time\"}", InvestigationEvents.FollowUpOnceKey(earlier), cancellationToken);
        }

        return due.Count;
    }

    /* #6010, D7 (Darling's #6003): a live request is resolved when EVERY metric linked to it has a resolution row after that
       metric's last alert on the same server. A resolution row's context_json names the metric it resolves in its top-level
       Resolves member; the candidate rows come back and C# parses each context (a text match would miss the escapings).
       A metric with no candidate comes back once with a NULL context, so a request with an uncleared metric is unresolved.
       Read through v_config_alert_log (live plus archive) so an archived resolution still counts.
       The bound on h.alert_time is the oldest last alert among the candidates. It saves the per-row work (the Resolves test
       and the join) on every older row, live or archived; the rows are still read, and the saving was measured on Darling's
       PostgreSQL store only. A live follow-up can pull the oldest last alert back by up to 30 days, so the saving is smaller
       while one is open. It drops no candidate: a resolution row a candidate can use is after that candidate's last alert,
       so after the oldest one. It is not the clock minus a fixed window, because a follow-up opens now but copies its
       source's alert rows with their original times, so its last alert can be weeks old. */
    internal const string ResolveCandidatesSql = @"
WITH m AS (SELECT a.request_id, a.server_id, a.metric_name, max(a.alert_time) AS last_alert
           FROM investigation_request_alerts AS a
           WHERE a.request_id IN (SELECT r.request_id
                                  FROM investigation_requests AS r
                                  WHERE r.state IN ('open', 'claimed', 'reported')
                                    AND r.opened_at > $1 - INTERVAL 2 DAY)
           GROUP BY a.request_id, a.server_id, a.metric_name)
SELECT m.request_id, m.metric_name, h.context_json
FROM m
LEFT JOIN v_config_alert_log AS h
       ON h.server_id = m.server_id
      AND h.alert_time > m.last_alert
      AND h.alert_time > (SELECT min(last_alert) FROM m)
      AND strpos(h.context_json, 'Resolves') > 0";

    /* Journals resolved once per request (once key resolved:<request_id>); the state is checked again under the write lock. */
    internal const string ResolveInsertSql = @"
INSERT INTO investigation_journal (journal_id, event_time, request_id, route_id, server_id, event, actor, once_key)
SELECT (SELECT coalesce(max(journal_id), 0) FROM investigation_journal) + row_number() OVER (),
       $1, r.request_id, r.route_id, r.server_id, 'resolved', 'service', 'resolved:' || r.request_id::VARCHAR
FROM investigation_requests AS r
WHERE list_contains($2::VARCHAR[], r.request_id::VARCHAR)
  AND r.state IN ('open', 'claimed', 'reported')
ON CONFLICT (once_key) DO NOTHING";

    private async Task<int> SweepResolvedAsync(DuckDBConnection connection, DateTime now, CancellationToken cancellationToken)
    {
        var requests = new Dictionary<Guid, Dictionary<string, bool>>();
        using (var candidates = Command(connection, ResolveCandidatesSql, now))
        using (var reader = await candidates.ExecuteReaderAsync(cancellationToken))
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

        var cleared = requests.Where(r => r.Value.Values.All(done => done)).Select(r => r.Key).ToList();
        return cleared.Count == 0 ? 0 : await ExecAsync(connection, ResolveInsertSql, cancellationToken, now, cleared.Select(id => id.ToString()).ToArray());
    }

    /// <summary>The member of a resolution row's context that names the metric it resolves (the shared <c>AlertResolutionRecords.ResolvesMember</c>, written by both products).</summary>
    private const string ResolvesMember = PerformanceMonitor.Alerting.AlertResolutionRecords.ResolvesMember;

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

    /// <summary>
    /// The retention pass (#5820, Darling's <c>InvestigationRetentionDays</c> and <c>InvestigationBriefRetentionDays</c>):
    /// clears the brief of every request opened more than 90 days ago, and deletes every finished request (not open or
    /// claimed) opened more than a year ago with its alert rows and report, and the journal rows older than a year. The
    /// children go first: there is no cascade. Returns the requests deleted.
    /// </summary>
    public async Task<int> PurgeAsync(CancellationToken cancellationToken = default)
    {
        var now = Now();
        var cutoff = now.AddDays(-RetentionDays);
        using var writeLock = _db.AcquireWriteLock(_lockBudget);
        using var connection = await OpenAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();
        await ExecAsync(connection, ClearBriefSql, cancellationToken, now.AddDays(-BriefRetentionDays));
        await ExecAsync(connection, RetentionAlertRowsSql, cancellationToken, cutoff);
        await ExecAsync(connection, RetentionEvidenceSql, cancellationToken, cutoff);
        await ExecAsync(connection, RetentionFamilyOfSql, cancellationToken, cutoff);
        await ExecAsync(connection, RetentionFollowUpOfSql, cancellationToken, cutoff);
        await ExecAsync(connection, RetentionReportsSql, cancellationToken, cutoff);
        var requests = await ExecAsync(connection, RetentionRequestsSql, cancellationToken, cutoff);
        await ExecAsync(connection, RetentionJournalSql, cancellationToken, cutoff);
        transaction.Commit();
        return requests;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<PendingInvestigationNote>> ListPendingNotesAsync(int limit = 200, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenAsync(cancellationToken);
        using var command = Command(connection, PendingNotesSql, Math.Clamp(limit, 1, 1000), Now());
        var rows = new List<PendingInvestigationNote>();
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
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
                Utc(reader.GetDateTime(9)),
                Utc(reader.GetDateTime(10))));
        }

        return rows;
    }

    /// <inheritdoc />
    public Task<bool> ApproveNoteAsync(long noteId, string decidedBy, CancellationToken cancellationToken = default) =>
        DecideNoteAsync(noteId, "approved", "note_approved", decidedBy, cancellationToken);

    /// <inheritdoc />
    public Task<bool> RejectNoteAsync(long noteId, string decidedBy, CancellationToken cancellationToken = default) =>
        DecideNoteAsync(noteId, "rejected", "note_rejected", decidedBy, cancellationToken);

    /* The update and its journal row share one transaction under the write lock: a decision is never stamped without its
       audit row. Only a person's action in the viewer reaches this; no MCP tool does. */
    private async Task<bool> DecideNoteAsync(long noteId, string state, string journalEvent, string decidedBy, CancellationToken cancellationToken)
    {
        var refusal = InvestigationNoteRules.ValidateDecidedBy(decidedBy);
        if (refusal is not null)
        {
            throw new ArgumentException(refusal, nameof(decidedBy));
        }

        decidedBy = decidedBy.Trim();
        var now = Now();
        using var writeLock = _db.AcquireWriteLock(_lockBudget);
        using var connection = await OpenAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();
        Guid? sourceRequest;
        int? serverId;
        using (var command = Command(connection, DecideNoteSql, noteId, state, decidedBy, now))
        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                return false;
            }

            sourceRequest = reader.IsDBNull(0) ? null : reader.GetGuid(0);
            serverId = reader.IsDBNull(1) ? null : reader.GetInt32(1);
        }

        await JournalAsync(connection, now, sourceRequest, null, serverId, journalEvent, decidedBy,
            "note " + noteId.ToString(CultureInfo.InvariantCulture), null, null, cancellationToken);
        transaction.Commit();
        return true;
    }

    /// <summary>#6011: the row with the names Lite's server list holds now (see <see cref="InvestigationServerNameLookup"/>).
    /// Read once per call and shared by its rows, so a list is named from one snapshot.</summary>
    private IReadOnlyDictionary<int, InvestigationServerNames>? NamesNow() => _serverNames?.Invoke();

    private static InvestigationRequestRow Named(InvestigationRequestRow row, IReadOnlyDictionary<int, InvestigationServerNames>? names)
    {
        if (names is null) return row;
        var (display, instance) = InvestigationServerNameLookup.Resolve(names, row.ServerId, row.ServerName);
        return row with { DisplayName = display, InstanceName = instance };
    }

    private static InvestigationRequestRow RequestRow(System.Data.Common.DbDataReader reader) => new(
        reader.GetGuid(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetString(3), reader.GetString(4),
        reader.GetString(5), Utc(reader.GetDateTime(6)), Utc(reader.GetDateTime(7)), reader.GetInt32(8),
        reader.GetString(9), Utc(reader.GetDateTime(10)), Utc(reader.GetDateTime(11)),
        reader.IsDBNull(12) ? null : reader.GetString(12),
        reader.IsDBNull(13) ? null : Utc(reader.GetDateTime(13)),
        reader.GetInt32(14),
        reader.IsDBNull(15) ? null : Utc(reader.GetDateTime(15)));

    private Task<int> InsertAlertRowsAsync(DuckDBConnection connection, Guid requestId, InvestigationAlert alert,
        DateTime alertTime, string[] keys, CancellationToken cancellationToken)
    {
        /* A row per incident dedup key; an alert with no key still gets its one row (a list holding one NULL). */
        var rowKeys = keys.Length == 0 ? new List<string?> { null } : keys.Cast<string?>().ToList();
        var facts = alert.Facts;
        return ExecAsync(connection, AlertRowsSql, cancellationToken,
            requestId, alertTime, alert.ServerId, alert.MetricName, rowKeys,
            facts?.CurrentValue, facts?.ThresholdValue, facts?.Muted,
            DerivedStoreScrub.Text(facts?.DetailText), DerivedStoreScrub.Json(facts?.ContextJson));
    }

    /// <summary>True when the request already holds <see cref="InvestigationRules.MaxAgentJournalRows"/> agent-written
    /// rows; then journals <c>journal_capped</c> once per request. Runs under the caller's write lock, so the count and
    /// the write that follows it are not interleaved by another agent call (Darling's count could be passed by calls in
    /// flight; this one cannot).</summary>
    private async Task<bool> JournalCappedAsync(DuckDBConnection connection, DateTime now, Guid requestId, string actor,
        string operation, CancellationToken cancellationToken)
    {
        long rows;
        using (var count = Command(connection, AgentJournalCountSql, requestId))
        {
            rows = Convert.ToInt64(await count.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        }

        if (rows < InvestigationRules.MaxAgentJournalRows)
        {
            return false;
        }

        await JournalAsync(connection, now, requestId, null, null, "journal_capped", actor, operation,
            null, "journal_capped:" + requestId.ToString(), cancellationToken);
        return true;
    }

    private static Task<int> JournalAsync(DuckDBConnection connection, DateTime now, Guid? requestId, int? routeId, int? serverId,
        string journalEvent, string actor, string? reason, string? detailJson, string? onceKey, CancellationToken cancellationToken) =>
        ExecAsync(connection, JournalSql, cancellationToken,
            now, requestId, routeId, serverId, journalEvent, actor, reason, detailJson, onceKey);

    private async Task<DuckDBConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = _db.CreateConnection();
        try
        {
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    private static DuckDBCommand Command(DuckDBConnection connection, string sql, params object?[] values)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var value in values)
        {
            command.Parameters.Add(new DuckDBParameter { Value = value switch
            {
                null => DBNull.Value,
                DateTime time => Micro(time),
                _ => value,
            } });
        }

        return command;
    }

    private static async Task<int> ExecAsync(DuckDBConnection connection, string sql, CancellationToken cancellationToken,
        params object?[] values)
    {
        using var command = Command(connection, sql, values);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static DateTime Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc);
}
