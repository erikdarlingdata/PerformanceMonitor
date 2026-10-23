using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.KnownPatterns;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// Reads a brief's sources from the monitoring store (#5632 Phase A): the request, the first and the latest three of its
/// alerts with their stored text and context, the latest findings for the server (24 hours, collapsed, the request's own
/// story first), earlier reports for the same server and family, approved unexpired notes, the environment profile, and
/// the monitoring-side changes of the last seven days, and how often each linked metric fired at the first alert's time of day (#6000). Every command carries <see cref="CommandTimeoutSeconds"/>.
/// </summary>
public sealed class PgInvestigationBriefSourceReader : IInvestigationBriefSourceReader
{
    /// <summary>Every read's command timeout.</summary>
    public const int CommandTimeoutSeconds = 30;

    /// <summary>How many findings, earlier reports and changes a brief reads at most (the budget then trims).</summary>
    public const int MaxFindings = 5;

    /// <summary>See <see cref="MaxFindings"/>.</summary>
    public const int MaxEarlierReports = 10;

    /// <summary>See <see cref="MaxFindings"/>.</summary>
    public const int MaxChanges = 15;

    /// <summary>How far back recent changes reach.</summary>
    public static readonly TimeSpan ChangeWindow = TimeSpan.FromDays(7);

    internal static readonly string RequestSql = $@"
SELECT r.server_id, r.server_name, r.family, r.state, r.expires_at, r.lease_until, r.alert_count, r.first_metric,
       r.story_path_hash, s.engine_kind, r.first_alert_at, r.first_dedup_key, r.autonomy_level, r.practice_of,
       {InvestigationRequestStore.RegistryNamesSelect("r", "s")}, r.force_check_of, r.force_check_no
FROM config.investigation_requests AS r
LEFT JOIN collect.servers AS s ON s.server_id = r.server_id
WHERE r.request_id = $1";

    /* #5632 step 8: the force a check request is about: the force's action_called row, the request that placed it and the time of its
       action_done row. $1 the journal id. */
    internal const string ForceCheckForceSql = @"
SELECT c.request_id, COALESCE(c.detail->'before'->>'database', c.detail->'target'->>'database'), c.detail->'before'->>'query_id',
       COALESCE(c.detail->'before'->>'plan_id', c.detail->'target'->>'id'), d.event_time
FROM config.investigation_journal AS c
LEFT JOIN LATERAL (
    SELECT x.event_time FROM config.investigation_journal AS x
    WHERE x.request_id = c.request_id AND x.event = 'action_done' AND x.detail->>'called_id' = c.journal_id::text
    ORDER BY x.journal_id DESC LIMIT 1) AS d ON true
WHERE c.journal_id = $1 AND c.event = 'action_called'";

    /* The first alert and the latest three. A request holds a row per alert and per dedup key, so the alerts are made
       distinct first. config_alert_log.alert_time is naive UTC; the link allows five seconds either side and takes the
       nearest row, so a stamp taken a moment apart from the request row still finds its alert.
       #5821: an alert that opens or joins before its page is sent writes its link rows before its history row, so an agent
       that claims while the page is in flight finds no history row yet. The link rows carry the history row's values (V188)
       and are read first; the history row answers only for what the link does not hold (send_error, and every value of a
       link written before V188). */
    internal const string AlertsSql = @"
WITH a AS (
    SELECT DISTINCT ON (alert_time, server_id, metric_name)
           alert_time, server_id, metric_name, current_value, threshold_value, muted, detail_text, context_json
    FROM config.investigation_request_alerts
    WHERE request_id = $1
    ORDER BY alert_time, server_id, metric_name, request_alert_id
), picked AS (
    (SELECT * FROM a ORDER BY alert_time, metric_name LIMIT 1)
    UNION
    (SELECT * FROM a ORDER BY alert_time DESC, metric_name LIMIT 3)
)
SELECT p.alert_time, p.metric_name, coalesce(p.current_value, l.current_value), coalesce(p.threshold_value, l.threshold_value),
       coalesce(p.muted, l.muted, false), coalesce(p.detail_text, l.detail_text), l.send_error,
       coalesce(p.context_json, l.context_json)
FROM picked AS p
LEFT JOIN LATERAL (
    SELECT cal.current_value, cal.threshold_value, cal.muted, cal.detail_text, cal.send_error, cal.context_json
    FROM config_alert_log AS cal
    WHERE cal.server_id = p.server_id
      AND cal.metric_name = p.metric_name
      AND cal.alert_time BETWEEN (p.alert_time AT TIME ZONE 'UTC') - interval '5 seconds'
                             AND (p.alert_time AT TIME ZONE 'UTC') + interval '5 seconds'
    ORDER BY abs(extract(epoch FROM cal.alert_time - (p.alert_time AT TIME ZONE 'UTC')))
    LIMIT 1
) AS l ON true
ORDER BY p.alert_time";

    /* #5918: the earlier reports read no practice request's report (practice is never real history for a live brief) and no
       report of a case a LIVE practice request replays (that report is the practising agent's answer key). "Live" is the
       store's one definition (InvestigationRequestStore.LivePracticeRequestsSql), aliased here, never copied. $5 now. */
    internal static readonly string EarlierReportsSql = @"
SELECT rp.request_id, rp.submitted_at, rp.agent_id, rp.verdict, rp.root_cause, rp.root_cause_found, rp.summary
FROM config.investigation_reports AS rp
JOIN config.investigation_requests AS rq ON rq.request_id = rp.request_id
WHERE rp.server_id = $1
  AND rq.family = $2
  AND rp.request_id <> $3
  AND rq.practice_of IS NULL
  AND NOT EXISTS (SELECT 1 FROM (" + InvestigationRequestStore.LivePracticeRequestsSql("$5") + @") AS lp WHERE lp.practice_of = rp.request_id)
ORDER BY rp.submitted_at DESC
LIMIT $4";

    /* #6565: the reports of other requests on the same server and family, reported in the last SameRootCauseHours hours, whose analysis
       story has the same root fact (key and database) as this request's story. The story hash is a pure hash of the
       story's path, so one chain gives one hash; what differs between two requests is the 30-minute join window (FindJoinSql), and a
       later request on the same fault opens its own request. The root fact (for example the regressed query's hash and database) is what
       the reports share. Computed here, when the brief is composed, from analysis_findings: a request stores only its story hash. The
       root is the newest finding for this request's hash at or before the request's open time: the brief composes at claim time, up
       to the lease after open, and a later analysis run (another database_name, another offender list) must not re-point the match. $1 server, $2 family, $3 this request, $4 limit,
       $5 now (the live-practice filter), $6 this request's story hash (null: no rows), $7 reported since. Same practice exclusions as
       EarlierReportsSql. The key and database do not name a plan regression's query (PLAN_REGRESSION is server-wide), so a row also
       carries this finding's and the candidate's newest remediation_action_json and the reader compares their force targets
       (InvestigationRootFactMatch). $4 is therefore a candidate cap, not the count a brief names. */
    internal static readonly string SameRootCauseReportsSql = @"
WITH root AS (
    SELECT f.root_fact_key, f.database_name, f.remediation_action_json
    FROM analysis_findings AS f
    WHERE f.server_id = $1 AND f.story_path_hash = $6
      AND f.analysis_time <= (SELECT r0.opened_at AT TIME ZONE 'UTC' FROM config.investigation_requests AS r0 WHERE r0.request_id = $3)
    ORDER BY f.analysis_time DESC
    LIMIT 1
)
SELECT rp.request_id, rp.submitted_at, rp.agent_id, rp.verdict, rp.root_cause, rp.root_cause_found, rp.summary,
       root.root_fact_key, root.remediation_action_json,
       (SELECT f3.remediation_action_json FROM analysis_findings AS f3
        WHERE f3.server_id = rq.server_id
          AND f3.story_path_hash = rq.story_path_hash
          AND f3.root_fact_key = root.root_fact_key
          AND f3.database_name IS NOT DISTINCT FROM root.database_name
        ORDER BY f3.analysis_time DESC
        LIMIT 1) AS candidate_remediation_action_json
FROM root
JOIN config.investigation_requests AS rq ON true
JOIN config.investigation_reports AS rp ON rp.request_id = rq.request_id
WHERE rq.server_id = $1
  AND rq.family = $2
  AND rq.request_id <> $3
  AND rq.practice_of IS NULL
  AND rp.submitted_at > $7
  AND NOT EXISTS (SELECT 1 FROM (" + InvestigationRequestStore.LivePracticeRequestsSql("$5") + @") AS lp WHERE lp.practice_of = rp.request_id)
  AND EXISTS (
        SELECT 1 FROM analysis_findings AS f2
        WHERE f2.server_id = rq.server_id
          AND f2.story_path_hash = rq.story_path_hash
          AND f2.root_fact_key = root.root_fact_key
          AND f2.database_name IS NOT DISTINCT FROM root.database_name)
ORDER BY rp.submitted_at DESC
LIMIT $4";

    /// <summary>#6565: how many same-root-cause reports a brief names at most (the newest first).</summary>
    public const int MaxSameRootCauseReports = 3;

    /// <summary>#6565: how many candidate reports the SQL returns before the reader drops a plan regression on another query, so a
    /// run of other-query regressions cannot crowd the real match out of <see cref="MaxSameRootCauseReports"/>.</summary>
    public const int SameRootCauseCandidateCap = 50;

    /* #6001: the evidence the capture froze for the request, in the order the sections were written. */
    internal const string EvidenceSql = @"
SELECT section, state, content::text, captured_at
FROM config.investigation_evidence
WHERE request_id = $1
ORDER BY evidence_id";

    /* The server's notes AND the fleet's (server_id NULL): the viewer's note scope offers "Fleet (every server)", and a
       fleet note no brief read was an approved note no agent would ever see (#5632 review L3). A note has no tag scope
       (no tag column; the viewer's note scope lists only the fleet and servers). The server's own notes sort first, so
       fleet notes never crowd them out of the ten. */
    internal const string NotesSql = @"
SELECT author, decided_by, expires_at, note_text
FROM config.investigation_notes
WHERE (server_id = $1 OR server_id IS NULL)
  AND state = 'approved'
  AND expires_at > $2
  AND ($3::uuid IS NULL OR source_request_id IS DISTINCT FROM $3)
ORDER BY (server_id IS NULL), coalesce(decided_at, created_at) DESC
LIMIT 10";

    internal const string ProfilesSql = @"
SELECT CASE WHEN p.scope_server_id IS NOT NULL THEN 'server' WHEN p.scope_tag_id IS NOT NULL THEN 'tag' ELSE 'fleet' END,
       p.profile_text
FROM config.agent_environment_profiles AS p
WHERE p.scope_server_id = $1
   OR (p.scope_server_id IS NULL AND p.scope_tag_id IS NULL)
   OR p.scope_tag_id IN (SELECT tm.tag_id FROM config.server_tag_map AS tm WHERE tm.server_id = $1)
ORDER BY CASE WHEN p.scope_server_id IS NOT NULL THEN 0 WHEN p.scope_tag_id IS NOT NULL THEN 1 ELSE 2 END, p.profile_id";

    /* Mute rules for this server or every server, analysis mutes for this server or every server, plan-force actions on
       this server, and each monitoring service host that started inside the window with the version it runs, newest first.
       Times in the first three tables are naive UTC; config.service_version's are timestamptz, read as naive UTC.
       #5632: config.service_version keeps one row per host (the version and start of its latest run), so a start inside
       the window is all it can give: an upgrade shows as a start on the new version, and the version before is not kept.
       Alert thresholds give nothing here: config_alert_settings keeps no change time. */
    internal const string ChangesSql = @"
SELECT at, kind, text, pattern
FROM (
    SELECT m.created_at_utc AS at, 'mute_rule' AS kind,
           concat_ws(' ', CASE WHEN m.enabled THEN 'enabled' ELSE 'disabled' END, 'mute rule',
                     'for ' || coalesce(m.metric_name, 'every metric'),
                     CASE WHEN m.expires_at_utc IS NOT NULL THEN 'until ' || to_char(m.expires_at_utc, 'YYYY-MM-DD""T""HH24:MI:SS""Z""') END,
                     CASE WHEN m.reason IS NOT NULL THEN 'reason: ' || m.reason END) AS text,
           m.query_text_pattern AS pattern
    FROM config_mute_rules AS m
    WHERE (m.server_name IS NULL OR m.server_name = $2)
      AND m.created_at_utc >= $3
    UNION ALL
    SELECT am.muted_date, 'analysis_mute',
           concat_ws(' ', 'analysis finding muted:', am.story_path, CASE WHEN am.reason IS NOT NULL THEN 'reason: ' || am.reason END),
           NULL
    FROM analysis_muted AS am
    WHERE (am.server_id IS NULL OR am.server_id = $1)
      AND am.muted_date >= $3
    UNION ALL
    SELECT pf.action_time, 'plan_force_action',
           concat_ws(' ', pf.action, pf.decision, pf.outcome, 'in', pf.database_name, 'query_id', pf.query_id::text, 'plan_id', pf.plan_id::text),
           NULL
    FROM collect.plan_force_actions AS pf
    WHERE pf.server_id = $1
      AND pf.action_time >= $3
    UNION ALL
    SELECT sv.process_started_at AT TIME ZONE 'UTC', 'service_version',
           concat_ws(' ', 'monitoring service on', sv.service_host, 'started running version', sv.product_version),
           NULL
    FROM config.service_version AS sv
    WHERE sv.process_started_at AT TIME ZONE 'UTC' >= $3
) AS c
ORDER BY at DESC
LIMIT $4";

    /* #5914: the links earlier reports on the same server and family recorded in the last PriorLinkDays days, newest
       first: link_recorded journal rows of other requests, whose detail holds {kind, url, key}. */
    internal static readonly string PriorLinksSql = @"
SELECT j.request_id, j.event_time, j.detail::text
FROM config.investigation_journal AS j
JOIN config.investigation_requests AS rq ON rq.request_id = j.request_id
WHERE j.event = 'link_recorded'
  AND rq.server_id = $1
  AND rq.family = $2
  AND j.request_id <> $3
  AND j.event_time >= $4
  AND rq.practice_of IS NULL
  AND NOT EXISTS (SELECT 1 FROM (" + InvestigationRequestStore.LivePracticeRequestsSql("$6") + @") AS lp WHERE lp.practice_of = j.request_id)
ORDER BY j.event_time DESC, j.journal_id DESC
LIMIT $5";

    /* #6000: how often each of the request's linked metrics fired at the first alert's time of day: the metric's history rows
       in the Days days before the first alert, within WindowMinutes minutes either side of its clock time (circular, so a 23:50
       alert counts a 00:10 fire, the next date's), muted apart from unmuted. Both numbers come from InvestigationAlertRecurrence,
       the constants the brief publishes, so the SQL cannot measure one window while the brief states another (#3684).
       What is not history: the first alert's own row and anything at or after it, and any fire in the WindowMinutes minutes
       before it. Those last are this incident's own rise (an episode that spent its route cap opens on a later fire, so the
       earlier fires of the same rise sit just before the anchor); a fire about 24 hours earlier still counts. One index seek per
       metric (idx_config_alert_log_time on server_id, metric_name, alert_time); the clock test filters inside the Days days. A
       metric with no rows still comes back (day NULL), so the brief says zero.
       #6261: k is the whole day number the fire counts toward (1 = 24 hours before the anchor), the streak's unit
       (InvestigationAlertStreak.DaysAgo is the same rule in C#). It is not the date: a 23:50 anchor's window holds a 00:10 fire
       of the anchor's own date, and a 00:10 anchor's holds a 23:50 fire of the day before the one it counts toward. A date can
       so hold two k values, and a (day, muted) two rows; the reader adds a date once.
       Types: config.investigation_request_alerts.alert_time is timestamptz and config_alert_log.alert_time is naive UTC, so the
       anchor is taken AT TIME ZONE 'UTC' (as AlertsSql does for the same pair): every comparison and the ::date are then naive
       UTC and none rides the session TimeZone. Past rows do not change, so this is read at compose time. $1 request, $2 server. */
    internal static readonly string RecurrenceSql = @"
WITH anchor AS (
    SELECT min(alert_time AT TIME ZONE 'UTC') AS t FROM config.investigation_request_alerts WHERE request_id = $1
), m AS (
    SELECT DISTINCT metric_name FROM config.investigation_request_alerts WHERE request_id = $1
)
SELECT m.metric_name, a.t, f.day, f.muted, f.n, f.k
FROM m
CROSS JOIN anchor AS a
LEFT JOIN LATERAL (
    SELECT cal.alert_time::date AS day, cal.muted, round(extract(epoch FROM a.t - cal.alert_time) / 86400)::int AS k, count(*) AS n
    FROM config_alert_log AS cal
    CROSS JOIN LATERAL (SELECT mod(mod(extract(epoch FROM cal.alert_time - a.t)::numeric, 86400) + 86400, 86400) AS d) AS o
    WHERE cal.server_id = $2
      AND cal.metric_name = m.metric_name
      AND cal.alert_time >= a.t - interval '" + InvestigationAlertRecurrence.Days + @" days'
      AND cal.alert_time < a.t - interval '" + InvestigationAlertRecurrence.WindowMinutes + @" minutes'
      AND least(o.d, 86400 - o.d) <= " + (InvestigationAlertRecurrence.WindowMinutes * 60) + @"
    GROUP BY cal.alert_time::date, cal.muted, 3
) AS f ON true
ORDER BY m.metric_name, f.day DESC";

    /// <summary>How far back <c>prior_links</c> reaches (#5914).</summary>
    public const int PriorLinkDays = 30;

    /// <summary>The most prior links a brief is offered (#5914); the composer's cap may keep fewer.</summary>
    public const int MaxPriorLinks = 20;

    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;
    private readonly Func<DateTime> _utcNow;
    private readonly string? _publicBaseUrl;

    /// <summary>A reader over <paramref name="postgres"/>, a role that may read the investigation, alert and analysis tables.
    /// <paramref name="publicBaseUrl"/> (<c>web.publicBaseUrl</c> while the dashboard is on; null otherwise) builds the
    /// brief's <c>request_url</c> (#5914): the triage page of the alert that opened the request.</summary>
    public PgInvestigationBriefSourceReader(NpgsqlDataSource postgres, ILogger? logger = null, Func<DateTime>? utcNow = null,
        string? publicBaseUrl = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _publicBaseUrl = publicBaseUrl;
    }

    /// <inheritdoc />
    public async Task<InvestigationBriefSources?> ReadAsync(Guid requestId, CancellationToken cancellationToken = default)
    {
        var now = DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc);
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        InvestigationBriefRequest? request = null;
        string? requestUrl = null;
        Guid? practiceOf = null;
        InvestigationBriefForceCheck? forceCheck = null;
        await using (var command = Command(connection, RequestSql, requestId))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                request = new InvestigationBriefRequest(
                    requestId,
                    reader.GetInt32(0),
                    reader.GetString(1),
                    MonitoredEngineKind.IsPostgres(reader.IsDBNull(9) ? null : reader.GetString(9)),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetFieldValue<DateTime>(4),
                    reader.IsDBNull(5) ? null : reader.GetFieldValue<DateTime>(5),
                    reader.GetInt32(6),
                    reader.GetString(7),
                    reader.IsDBNull(8) ? null : reader.GetString(8),
                    reader.IsDBNull(12) ? 1 : reader.GetInt16(12),
                    reader.GetString(14),
                    reader.IsDBNull(15) ? null : reader.GetString(15));
                requestUrl = reader.IsDBNull(10) ? null : TriageLink.Build(_publicBaseUrl, request.ServerName, request.FirstMetric,
                    DateTime.SpecifyKind(reader.GetFieldValue<DateTime>(10), DateTimeKind.Utc), reader.IsDBNull(11) ? null : reader.GetString(11));
                practiceOf = reader.IsDBNull(13) ? null : reader.GetGuid(13);
                if (!reader.IsDBNull(16) && !reader.IsDBNull(17))
                    forceCheck = new InvestigationBriefForceCheck(reader.GetInt64(16), reader.GetInt16(17), InvestigationRequestStore.MaxForceChecks,
                        null, null, null, null, null);
            }
        }

        /* #5632 step 8: a check request names its force. A force whose journal rows are gone still leaves the check number. */
        if (request is not null && forceCheck is not null)
        {
            await using var command = new NpgsqlCommand(ForceCheckForceSql, connection) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds };
            command.Parameters.AddWithValue(forceCheck.CalledId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                forceCheck = forceCheck with
                {
                    ForceRequestId = reader.IsDBNull(0) ? null : reader.GetGuid(0),
                    Database = reader.IsDBNull(1) ? null : reader.GetString(1),
                    QueryId = !reader.IsDBNull(2) && long.TryParse(reader.GetString(2), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var q) ? q : null,
                    PlanId = !reader.IsDBNull(3) && long.TryParse(reader.GetString(3), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var p) ? p : null,
                    ForcedAtUtc = reader.IsDBNull(4) ? null : DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc),
                };
            }

            request = request with { ForceCheck = forceCheck };
        }

        if (request is null) return null;

        var alerts = new List<InvestigationBriefAlert>();
        await using (var command = Command(connection, AlertsSql, requestId))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                alerts.Add(new InvestigationBriefAlert(
                    reader.GetFieldValue<DateTime>(0),
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetDouble(2),
                    reader.IsDBNull(3) ? null : reader.GetDouble(3),
                    reader.GetBoolean(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.IsDBNull(6) ? null : reader.GetString(6),
                    Context(reader.IsDBNull(7) ? null : reader.GetString(7))));
            }
        }

        // Priority order: the first alert, then the newest.
        if (alerts.Count > 1)
            alerts = alerts.Take(1).Concat(alerts.Skip(1).Reverse()).ToList();

        var findings = await FindingsAsync(request, cancellationToken).ConfigureAwait(false);

        /* #5918: a practice request's brief holds no answer, so it reads no earlier report (the replayed case's own report is one
           of them) and no prior link, and no note the replayed case's report proposed (NotesSql). */
        var earlier = new List<InvestigationEarlierReport>();
        await using (var command = Command(connection, EarlierReportsSql, request.ServerId, request.Family, requestId,
            practiceOf is null ? MaxEarlierReports : 0, now))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                earlier.Add(new InvestigationEarlierReport(
                    reader.GetGuid(0),
                    reader.GetFieldValue<DateTime>(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetString(4),
                    reader.GetBoolean(5),
                    reader.IsDBNull(6) ? null : reader.GetString(6)));
            }
        }

        /* #6565: a reported request with the same root fact under another story hash comes first, marked, so the brief names it even when
           ten newer reports crowd it out of the list. A practice request reads none (above). */
        if (practiceOf is null && request.StoryPathHash is { } storyHash)
        {
            var sameRoot = new List<InvestigationEarlierReport>();
            await using (var command = Command(connection, SameRootCauseReportsSql, request.ServerId, request.Family, requestId,
                SameRootCauseCandidateCap, now, storyHash, now.AddHours(-InvestigationBriefComposer.SameRootCauseHours)))
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (sameRoot.Count >= MaxSameRootCauseReports ||
                        !InvestigationRootFactMatch.IsSameRootCause(
                            reader.GetString(7),
                            reader.IsDBNull(8) ? null : reader.GetString(8),
                            reader.IsDBNull(9) ? null : reader.GetString(9)))
                        continue;

                    sameRoot.Add(new InvestigationEarlierReport(
                        reader.GetGuid(0),
                        reader.GetFieldValue<DateTime>(1),
                        reader.GetString(2),
                        reader.GetString(3),
                        reader.GetString(4),
                        reader.GetBoolean(5),
                        reader.IsDBNull(6) ? null : reader.GetString(6),
                        SameRootCause: true));
                }
            }

            if (sameRoot.Count > 0)
            {
                var ids = sameRoot.Select(r => r.RequestId).ToHashSet();
                earlier = sameRoot.Concat(earlier.Where(r => !ids.Contains(r.RequestId))).Take(Math.Max(MaxEarlierReports, sameRoot.Count)).ToList();
            }
        }

        var notes = new List<InvestigationBriefNote>();
        await using (var command = Command(connection, NotesSql, request.ServerId, now, (object?)practiceOf ?? DBNull.Value))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                notes.Add(new InvestigationBriefNote(
                    reader.GetString(0),
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.GetFieldValue<DateTime>(2),
                    reader.GetString(3)));
            }
        }

        var profiles = new List<InvestigationProfileText>();
        await using (var command = Command(connection, ProfilesSql, request.ServerId))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                profiles.Add(new InvestigationProfileText(reader.GetString(0), reader.GetString(1)));
        }

        var changes = new List<InvestigationRecentChange>();
        var since = DateTime.SpecifyKind(now - ChangeWindow, DateTimeKind.Unspecified);
        await using (var command = Command(connection, ChangesSql, request.ServerId, request.ServerName, since, MaxChanges))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                changes.Add(new InvestigationRecentChange(
                    DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                    reader.GetString(1),
                    reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3)));
            }
        }

        var priorLinks = new List<InvestigationPriorLink>();
        await using (var command = Command(connection, PriorLinksSql, request.ServerId, request.Family, requestId,
            now.AddDays(-PriorLinkDays), practiceOf is null ? MaxPriorLinks : 0, now))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (InvestigationReportLinks.FromDetail(reader.IsDBNull(2) ? null : reader.GetString(2)) is { } link)
                {
                    priorLinks.Add(new InvestigationPriorLink(reader.GetGuid(0),
                        DateTime.SpecifyKind(reader.GetFieldValue<DateTime>(1), DateTimeKind.Utc), link.Kind, link.Url, link.Key));
                }
            }
        }

        var recurrence = new List<InvestigationAlertRecurrence>();
        await using (var command = Command(connection, RecurrenceSql, requestId, request.ServerId))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            string? metric = null;
            DateTime around = default;
            int fired = 0, muted = 0;
            var firedDates = new List<DateOnly>();
            var mutedDates = new List<DateOnly>();
            var daysAgo = new List<int>();
            void Flush()
            {
                if (metric is not null)
                    recurrence.Add(new InvestigationAlertRecurrence(metric, around, fired, muted, firedDates, mutedDates, daysAgo));
                firedDates = new List<DateOnly>();
                mutedDates = new List<DateOnly>();
                daysAgo = new List<int>();
                fired = 0;
                muted = 0;
            }

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var name = reader.GetString(0);
                if (!string.Equals(name, metric, StringComparison.Ordinal))
                {
                    Flush();
                    metric = name;
                    around = DateTime.SpecifyKind(reader.GetFieldValue<DateTime>(1), DateTimeKind.Utc);
                }

                if (reader.IsDBNull(2)) continue;
                var count = (int)reader.GetInt64(4);
                var date = DateOnly.FromDateTime(reader.GetFieldValue<DateTime>(2));
                /* Rows come newest day first, so each list is newest first. A date can come on two rows (#6261: one per day
                   number k), so it is added once. */
                if (reader.GetBoolean(3))
                {
                    muted += count;
                    if (!mutedDates.Contains(date)) mutedDates.Add(date);
                }
                else
                {
                    fired += count;
                    if (!firedDates.Contains(date)) firedDates.Add(date);
                }

                daysAgo.Add(reader.GetInt32(5));
            }

            Flush();
        }

        var evidence = new List<InvestigationBriefEvidence>();
        await using (var command = Command(connection, EvidenceSql, requestId))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                evidence.Add(new InvestigationBriefEvidence(
                    reader.GetString(0), reader.GetString(1),
                    reader.IsDBNull(3) ? null : DateTime.SpecifyKind(reader.GetFieldValue<DateTime>(3), DateTimeKind.Utc),
                    reader.IsDBNull(2) ? null : reader.GetString(2)));
            }
        }

        /* #6334: the operator's known-pattern entries, loaded once for the brief and matched in memory for each linked metric at the
           first alert's time. `recurrence` has one row per linked metric (a metric with no history still comes back) and every row
           carries the shared first-alert time, so it is the metric list and the anchor, with no query of its own. The load is the
           alert history's (LoadForAlertsAsync): a store older than V214 has no table and gives no entries and no error. Entries
           expired at the alert's time are judged by the matcher, not here. A practice request reads none (the composer drops the
           section for it too): a note can name the cause of the replayed case. */
        var knownPatterns = new List<InvestigationKnownPatternMatch>();
        if (practiceOf is null && recurrence.Count > 0)
        {
            var entries = await DarlingMcpKnownPatternTools.LoadForAlertsAsync(_postgres, cancellationToken).ConfigureAwait(false);
            if (entries.Count > 0)
            {
                foreach (var item in recurrence)
                {
                    foreach (var entry in KnownPatternMatcher.Match(entries, request.ServerId, item.MetricName, item.AroundUtc,
                        message => _logger?.LogWarning("{Message}", message)))
                    {
                        knownPatterns.Add(new InvestigationKnownPatternMatch(item.MetricName, entry.Id, entry.Note, entry.CreatedBy, entry.UpdatedBy));
                    }
                }
            }
        }

        return new InvestigationBriefSources(request, alerts, findings, earlier, changes, notes, profiles,
            priorLinks.Count > 0 ? priorLinks : null, requestUrl, practiceOf is not null,
            recurrence.Count > 0 ? recurrence : null, evidence.Count > 0 ? evidence : null,
            knownPatterns.Count > 0 ? knownPatterns : null,
            await QueryStoreNotCollectedAsync(request, cancellationToken).ConfigureAwait(false));
    }

    /// <summary>#6554: the reason a Change Watch request's Query Store reads will be empty, or null (every other request, and a server whose
    /// Query Store is collected). The wording is <see cref="DarlingRuntimePrecondition.QueryStoreReadableSecondaryMessageAsync"/>'s, the same
    /// text the Query Store reads answer with.</summary>
    private Task<string?> QueryStoreNotCollectedAsync(InvestigationBriefRequest request, CancellationToken cancellationToken) =>
        string.Equals(request.FirstMetric, InvestigationTriggerKinds.ChangeWatchMetric, StringComparison.Ordinal)
            ? DarlingRuntimePrecondition.QueryStoreReadableSecondaryMessageAsync(_postgres, request.ServerId, request.ServerName, cancellationToken)
            : Task.FromResult<string?>(null);

    /// <summary>The latest findings for the server over 24 hours, collapsed per chain, the request's own story first, each
    /// remediation reduced to its verdict and blockers (the store's forcing state read as <c>get_analysis_findings</c> does).</summary>
    private async Task<IReadOnlyList<InvestigationBriefFinding>> FindingsAsync(InvestigationBriefRequest request, CancellationToken cancellationToken)
    {
        var raw = await new PgFindingStore(_postgres, _logger).GetRecentFindingsAsync(
            request.ServerId, 24, FindingOccurrences.WindowCoveringLimit, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (raw.Count == 0) return Array.Empty<InvestigationBriefFinding>();

        var groups = FindingOccurrences.Collapse(raw)
            .OrderBy(g => string.Equals(g.Latest.StoryPathHash, request.StoryPathHash, StringComparison.Ordinal) ? 0 : 1)
            .Take(MaxFindings)
            .ToList();

        var (states, stateNote) = await DarlingForcePlanTargetStateReader.TryReadAsync(
            _postgres, request.ServerId, groups.Select(g => g.Latest), logger: _logger, cancellationToken: cancellationToken).ConfigureAwait(false);

        var result = new List<InvestigationBriefFinding>(groups.Count);
        foreach (var g in groups)
        {
            var f = g.Latest;
            var structured = FactRemediation.BuildStructuredRemediation(f.Remediation, states, stateNote);
            result.Add(new InvestigationBriefFinding(
                f.StoryPathHash,
                f.Category,
                FactAdvice.GetComposedForFinding(f)?.Headline ?? f.RootFactKey,
                f.StoryText,
                f.RootFactKey,
                f.RootFactValue,
                f.Severity,
                f.Confidence,
                DateTime.SpecifyKind(f.AnalysisTime, DateTimeKind.Utc),
                g.Occurrences,
                Remediation(structured),
                NextTools(f)));
        }

        return result;
    }

    /// <summary>The verdict and blockers of a structured remediation; the force, unforce and verify scripts are left out.</summary>
    internal static InvestigationBriefRemediation? Remediation(StructuredRemediation? structured) =>
        structured is null
            ? null
            : new InvestigationBriefRemediation(
                structured.Verb,
                structured.Guidance,
                structured.ForcePlanTargets
                    .Select(t => new InvestigationBriefRemediationTarget(t.Database, t.QueryId, t.PlanId, t.Eligible, t.Blockers, t.StateNote))
                    .ToList());

    /// <summary>A finding's recommended reads, from the same table as <c>analyze_server</c>'s <c>next_tools</c>.</summary>
    internal static IReadOnlyList<InvestigationNextCall> NextTools(AnalysisFinding finding)
    {
        var calls = new List<InvestigationNextCall>();
        using var document = JsonSerializer.SerializeToDocument(ToolRecommendations.GetForStoryPath(StoryKeys.ForFinding(finding)));
        foreach (var element in document.RootElement.EnumerateArray())
        {
            var arguments = new Dictionary<string, string>(StringComparer.Ordinal);
            if (element.TryGetProperty("suggested_params", out var parameters) && parameters.ValueKind == JsonValueKind.Object)
            {
                foreach (var p in parameters.EnumerateObject())
                    arguments[p.Name] = p.Value.ToString();
            }

            calls.Add(new InvestigationNextCall(
                element.GetProperty("tool").GetString() ?? string.Empty,
                arguments,
                element.TryGetProperty("reason", out var reason) ? reason.GetString() ?? string.Empty : string.Empty));
        }

        return calls;
    }

    /// <summary>An alert's stored context as labeled blocks; empty when it has none or it does not parse. The attachment
    /// (a deadlock graph) is left out: the detail reads fetch it.</summary>
    internal static IReadOnlyList<InvestigationBriefContextItem> Context(string? contextJson)
    {
        if (string.IsNullOrEmpty(contextJson) || !AlertContextSerializer.TryDeserialize(contextJson, out var context))
            return Array.Empty<InvestigationBriefContextItem>();
        return context.Details
            .Select(d => new InvestigationBriefContextItem(
                d.Heading,
                d.Fields.Select(f => new KeyValuePair<string, string>(f.Label, f.Value)).ToList()))
            .ToList();
    }

    private static NpgsqlCommand Command(NpgsqlConnection connection, string sql, params object[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection) { CommandTimeout = CommandTimeoutSeconds };
        foreach (var parameter in parameters)
            command.Parameters.AddWithValue(parameter);
        return command;
    }
}
