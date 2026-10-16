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
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// Reads a brief's sources from the monitoring store (#5632 Phase A): the request, the first and the latest three of its
/// alerts with their stored text and context, the latest findings for the server (24 hours, collapsed, the request's own
/// story first), earlier reports for the same server and family, approved unexpired notes, the environment profile, and
/// the monitoring-side changes of the last seven days. Every command carries <see cref="CommandTimeoutSeconds"/>.
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

    internal const string RequestSql = @"
SELECT r.server_id, r.server_name, r.family, r.state, r.expires_at, r.lease_until, r.alert_count, r.first_metric,
       r.story_path_hash, s.engine_kind, r.first_alert_at, r.first_dedup_key, r.practice_of
FROM config.investigation_requests AS r
LEFT JOIN collect.servers AS s ON s.server_id = r.server_id
WHERE r.request_id = $1";

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

    internal const string EarlierReportsSql = @"
SELECT rp.request_id, rp.submitted_at, rp.agent_id, rp.verdict, rp.root_cause, rp.root_cause_found, rp.summary
FROM config.investigation_reports AS rp
JOIN config.investigation_requests AS rq ON rq.request_id = rp.request_id
WHERE rp.server_id = $1
  AND rq.family = $2
  AND rp.request_id <> $3
ORDER BY rp.submitted_at DESC
LIMIT $4";

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
    internal const string PriorLinksSql = @"
SELECT j.request_id, j.event_time, j.detail::text
FROM config.investigation_journal AS j
JOIN config.investigation_requests AS rq ON rq.request_id = j.request_id
WHERE j.event = 'link_recorded'
  AND rq.server_id = $1
  AND rq.family = $2
  AND j.request_id <> $3
  AND j.event_time >= $4
ORDER BY j.event_time DESC, j.journal_id DESC
LIMIT $5";

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
                    reader.IsDBNull(8) ? null : reader.GetString(8));
                requestUrl = reader.IsDBNull(10) ? null : TriageLink.Build(_publicBaseUrl, request.ServerName, request.FirstMetric,
                    DateTime.SpecifyKind(reader.GetFieldValue<DateTime>(10), DateTimeKind.Utc), reader.IsDBNull(11) ? null : reader.GetString(11));
                practiceOf = reader.IsDBNull(12) ? null : reader.GetGuid(12);
            }
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
            practiceOf is null ? MaxEarlierReports : 0))
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
            now.AddDays(-PriorLinkDays), practiceOf is null ? MaxPriorLinks : 0))
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

        return new InvestigationBriefSources(request, alerts, findings, earlier, changes, notes, profiles,
            priorLinks.Count > 0 ? priorLinks : null, requestUrl, practiceOf is not null);
    }

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
