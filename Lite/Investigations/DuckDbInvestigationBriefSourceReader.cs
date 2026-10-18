/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Investigations;

/// <summary>
/// Reads a brief's sources from Lite's DuckDB store (#5820), the twin of Darling's <c>PgInvestigationBriefSourceReader</c>:
/// the request, the first and the latest three of its alerts (the link rows' own facts first, the alert history row only
/// for what the link does not hold), the latest findings for the server (24 hours, collapsed, the request's own story
/// first), earlier reports for the same server and family, approved unexpired notes, the environment profile, and the
/// monitoring-side changes of the last seven days. Two of Darling's change sources have no Lite twin and read as nothing:
/// plan-force actions (Lite never forces a plan; it only reads Query Store) and service starts (Lite is one process on one
/// machine, with no service host table). Each call opens its own connection, and every time is bound to the microsecond
/// and read back as UTC, because the stores keep naive UTC to the microsecond.
/// </summary>
public sealed class DuckDbInvestigationBriefSourceReader : IInvestigationBriefSourceReader
{
    /// <summary>How many findings, earlier reports and changes a brief reads at most (the budget then trims).</summary>
    public const int MaxFindings = 5;

    /// <summary>See <see cref="MaxFindings"/>.</summary>
    public const int MaxEarlierReports = 10;

    /// <summary>See <see cref="MaxFindings"/>.</summary>
    public const int MaxChanges = 15;

    /// <summary>How far back recent changes reach.</summary>
    public static readonly TimeSpan ChangeWindow = TimeSpan.FromDays(7);

    /* #6010: the evidence the capture froze for the request, in the order the sections were written (Darling's EvidenceSql). */
    internal const string EvidenceSql = @"
SELECT section, state, content, captured_at
FROM investigation_evidence
WHERE request_id = $1
ORDER BY evidence_id";

    internal const string RequestSql = @"
SELECT server_id, server_name, family, state, expires_at, lease_until, alert_count, first_metric, story_path_hash
FROM investigation_requests
WHERE request_id = $1";

    /* The first alert and the latest three, as in Darling's reader. A request holds a row per alert and per dedup key, so
       the alerts are made distinct first. The link allows five seconds either side of its time and takes the nearest
       history row, so a stamp taken a moment apart from the request row still finds its alert. The link rows carry the
       history row's values and are read first (#5821: an alert that opens or joins before its page is sent writes its link
       rows before its history row); the history row answers only for send_error and for a link written without facts.
       Both tables hold naive UTC. The history is read through v_config_alert_log (the hot table unioned with the archive),
       so an alert that ordinary archival has already moved to Parquet still answers. */
    internal const string AlertsSql = @"
WITH a AS (
    SELECT DISTINCT ON (alert_time, server_id, metric_name)
           alert_time, server_id, metric_name, current_value, threshold_value, muted, detail_text, context_json
    FROM investigation_request_alerts
    WHERE request_id = $1
    ORDER BY alert_time, server_id, metric_name, request_alert_id
), first_alert AS (
    SELECT * FROM a ORDER BY alert_time, metric_name LIMIT 1
), latest_alerts AS (
    SELECT * FROM a ORDER BY alert_time DESC, metric_name LIMIT 3
), picked AS (
    SELECT * FROM first_alert
    UNION
    SELECT * FROM latest_alerts
)
SELECT p.alert_time, p.metric_name, coalesce(p.current_value, l.current_value), coalesce(p.threshold_value, l.threshold_value),
       coalesce(p.muted, l.muted, false), coalesce(p.detail_text, l.detail_text), l.send_error,
       coalesce(p.context_json, l.context_json)
FROM picked AS p
LEFT JOIN LATERAL (
    SELECT cal.current_value, cal.threshold_value, cal.muted, cal.detail_text, cal.send_error, cal.context_json
    FROM v_config_alert_log AS cal
    WHERE cal.server_id = p.server_id
      AND cal.metric_name = p.metric_name
      AND cal.alert_time BETWEEN p.alert_time - INTERVAL 5 SECOND AND p.alert_time + INTERVAL 5 SECOND
    ORDER BY abs(epoch_us(cal.alert_time) - epoch_us(p.alert_time))
    LIMIT 1
) AS l ON true
ORDER BY p.alert_time, p.metric_name";

    internal const string EarlierReportsSql = @"
SELECT rp.request_id, rp.submitted_at, rp.agent_id, rp.verdict, rp.root_cause, rp.root_cause_found, rp.summary
FROM investigation_reports AS rp
JOIN investigation_requests AS rq ON rq.request_id = rp.request_id
WHERE rp.server_id = $1
  AND rq.family = $2
  AND rp.request_id <> $3
ORDER BY rp.submitted_at DESC
LIMIT $4";

    /* The server's notes AND the fleet's (server_id NULL), the server's own first so fleet notes never crowd them out of
       the ten; a note has no tag scope. Only approved, unexpired notes. */
    internal const string NotesSql = @"
SELECT author, decided_by, expires_at, note_text
FROM investigation_notes
WHERE (server_id = $1 OR server_id IS NULL)
  AND state = 'approved'
  AND expires_at > $2
ORDER BY (server_id IS NULL), coalesce(decided_at, created_at) DESC
LIMIT 10";

    internal const string ProfilesSql = @"
SELECT CASE WHEN p.scope_server_id IS NOT NULL THEN 'server' WHEN p.scope_tag_id IS NOT NULL THEN 'tag' ELSE 'fleet' END,
       p.profile_text
FROM agent_environment_profiles AS p
WHERE p.scope_server_id = $1
   OR (p.scope_server_id IS NULL AND p.scope_tag_id IS NULL)
   OR p.scope_tag_id IN (SELECT tm.tag_id FROM server_tag_map AS tm WHERE tm.server_id = $1)
ORDER BY CASE WHEN p.scope_server_id IS NOT NULL THEN 0 WHEN p.scope_tag_id IS NOT NULL THEN 1 ELSE 2 END, p.profile_id";

    /* Mute rules for this server or every server, and analysis mutes for this server or every server, newest first. Both
       tables hold naive UTC. Darling also reads plan-force actions and service starts here; Lite has neither source. */
    internal const string ChangesSql = @"
SELECT at_utc, kind, text, pattern
FROM (
    SELECT m.created_at_utc AS at_utc, 'mute_rule' AS kind,
           concat_ws(' ', CASE WHEN m.enabled THEN 'enabled' ELSE 'disabled' END, 'mute rule',
                     'for ' || coalesce(m.metric_name, 'every metric'),
                     CASE WHEN m.expires_at_utc IS NOT NULL THEN 'until ' || strftime(m.expires_at_utc, '%Y-%m-%dT%H:%M:%SZ') END,
                     CASE WHEN m.reason IS NOT NULL THEN 'reason: ' || m.reason END) AS text,
           m.query_text_pattern AS pattern
    FROM config_mute_rules AS m
    WHERE (m.server_name IS NULL OR m.server_name = $2)
      AND m.created_at_utc >= $3
    UNION ALL
    SELECT am.muted_date, 'analysis_mute',
           concat_ws(' ', 'analysis finding muted:', am.story_path, CASE WHEN am.reason IS NOT NULL THEN 'reason: ' || am.reason END),
           CAST(NULL AS VARCHAR)
    FROM analysis_muted AS am
    WHERE (am.server_id IS NULL OR am.server_id = $1)
      AND am.muted_date >= $3
) AS c
ORDER BY at_utc DESC
LIMIT $4";

    private readonly DuckDbInitializer _duckDb;
    private readonly FindingStore _findings;
    private readonly Func<DateTime> _utcNow;

    /// <summary>A reader over <paramref name="duckDb"/>, Lite's store.</summary>
    public DuckDbInvestigationBriefSourceReader(DuckDbInitializer duckDb, Func<DateTime>? utcNow = null)
    {
        _duckDb = duckDb ?? throw new ArgumentNullException(nameof(duckDb));
        _findings = new FindingStore(duckDb);
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    /// <inheritdoc />
    public async Task<InvestigationBriefSources?> ReadAsync(Guid requestId, CancellationToken cancellationToken = default)
    {
        var now = DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc);
        InvestigationBriefRequest? request = null;
        var alerts = new List<InvestigationBriefAlert>();
        var earlier = new List<InvestigationEarlierReport>();
        var notes = new List<InvestigationBriefNote>();
        var profiles = new List<InvestigationProfileText>();
        var changes = new List<InvestigationRecentChange>();

        using (_duckDb.AcquireReadLock(cancellationToken))
        {
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            using (var command = Command(connection, RequestSql, requestId))
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    request = new InvestigationBriefRequest(
                        requestId,
                        reader.GetInt32(0),
                        reader.GetString(1),
                        IsPostgres: false,
                        reader.GetString(2),
                        reader.GetString(3),
                        Utc(reader, 4),
                        reader.IsDBNull(5) ? null : Utc(reader, 5),
                        reader.GetInt32(6),
                        reader.GetString(7),
                        reader.IsDBNull(8) ? null : reader.GetString(8));
                }
            }

            if (request is null) return null;

            using (var command = Command(connection, AlertsSql, requestId))
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    alerts.Add(new InvestigationBriefAlert(
                        Utc(reader, 0),
                        reader.GetString(1),
                        reader.IsDBNull(2) ? null : reader.GetDouble(2),
                        reader.IsDBNull(3) ? null : reader.GetDouble(3),
                        reader.GetBoolean(4),
                        reader.IsDBNull(5) ? null : reader.GetString(5),
                        reader.IsDBNull(6) ? null : reader.GetString(6),
                        Context(reader.IsDBNull(7) ? null : reader.GetString(7))));
                }
            }

            using (var command = Command(connection, EarlierReportsSql, request.ServerId, request.Family, requestId, MaxEarlierReports))
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    earlier.Add(new InvestigationEarlierReport(
                        reader.GetGuid(0),
                        Utc(reader, 1),
                        reader.GetString(2),
                        reader.GetString(3),
                        reader.GetString(4),
                        reader.GetBoolean(5),
                        reader.IsDBNull(6) ? null : reader.GetString(6)));
                }
            }

            using (var command = Command(connection, NotesSql, request.ServerId, Micros(now)))
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    notes.Add(new InvestigationBriefNote(
                        reader.GetString(0),
                        reader.IsDBNull(1) ? null : reader.GetString(1),
                        Utc(reader, 2),
                        reader.GetString(3)));
                }
            }

            using (var command = Command(connection, ProfilesSql, request.ServerId))
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    profiles.Add(new InvestigationProfileText(reader.GetString(0), reader.GetString(1)));
            }

            using (var command = Command(connection, ChangesSql, request.ServerId, request.ServerName, Micros(now - ChangeWindow), MaxChanges))
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    changes.Add(new InvestigationRecentChange(
                        Utc(reader, 0),
                        reader.GetString(1),
                        reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                        reader.IsDBNull(3) ? null : reader.GetString(3)));
                }
            }
        }

        var evidence = new List<InvestigationBriefEvidence>();
        using (_duckDb.AcquireReadLock(cancellationToken))
        {
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                using var command = Command(connection, EvidenceSql, requestId);
                using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    evidence.Add(new InvestigationBriefEvidence(
                        reader.GetString(0), reader.GetString(1),
                        reader.IsDBNull(3) ? null : DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc),
                        reader.IsDBNull(2) ? null : reader.GetString(2)));
                }
            }
            catch (DuckDB.NET.Data.DuckDBException)
            {
                /* A file whose v72 step has not run has no evidence table: the brief carries no evidence, as before. */
                evidence.Clear();
            }
        }

        // Priority order: the first alert, then the newest.
        if (alerts.Count > 1)
            alerts = alerts.Take(1).Concat(alerts.Skip(1).Reverse()).ToList();

        // The finding store and the force-plan state take the read lock themselves, so they run outside the scope above.
        var findings = await FindingsAsync(request, now, cancellationToken).ConfigureAwait(false);

        return new InvestigationBriefSources(request, alerts, findings, earlier, changes, notes, profiles,
            Evidence: evidence.Count > 0 ? evidence : null);
    }

    /// <summary>The latest findings for the server over 24 hours, collapsed per chain, the request's own story first, each
    /// remediation reduced to its verdict and blockers (the store's forcing state read as <c>get_analysis_findings</c> does).</summary>
    private async Task<IReadOnlyList<InvestigationBriefFinding>> FindingsAsync(
        InvestigationBriefRequest request, DateTime now, CancellationToken cancellationToken)
    {
        var raw = await _findings.GetRecentFindingsAsync(request.ServerId, 24, FindingOccurrences.WindowCoveringLimit).ConfigureAwait(false);
        if (raw.Count == 0) return Array.Empty<InvestigationBriefFinding>();

        var groups = FindingOccurrences.Collapse(raw)
            .OrderBy(g => string.Equals(g.Latest.StoryPathHash, request.StoryPathHash, StringComparison.Ordinal) ? 0 : 1)
            .Take(MaxFindings)
            .ToList();

        var (states, stateNote) = await TryReadForcePlanStatesAsync(
            request.ServerId, groups.Select(g => g.Latest), now, cancellationToken).ConfigureAwait(false);

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

    /// <summary>The forcing and automatic-plan-correction state behind the findings' force-plan targets, read as
    /// <c>get_analysis_findings</c> reads it (#3652). A failed read gives the reason instead, which lands on every target,
    /// so a verdict never reads as eligible by silence; an abandonment is not swallowed.</summary>
    private async Task<(IReadOnlyDictionary<ForcePlanTargetKey, ForcePlanTargetState>? States, string? UnavailableReason)> TryReadForcePlanStatesAsync(
        int serverId, IEnumerable<AnalysisFinding> findings, DateTime now, CancellationToken cancellationToken)
    {
        var targets = new List<ForcePlanTarget>();
        foreach (var finding in findings)
        {
            if (finding.Remediation?.Targets is { Count: > 0 } ts)
                targets.AddRange(ts);
        }

        if (targets.Count == 0)
            return (new Dictionary<ForcePlanTargetKey, ForcePlanTargetState>(ForcePlanTargetKey.Comparer), null);

        try
        {
            using var readLock = _duckDb.AcquireReadLock(cancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            using var command = connection.CreateCommand();
            return (await ForcePlanTargetStateReader.ReadAsync(command, serverId, targets, now, cancellationToken).ConfigureAwait(false), null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException && !cancellationToken.IsCancellationRequested)
        {
            return (null, $"the forcing and automatic-plan-correction state read failed ({ex.GetType().Name}: {ex.Message}); eligible reflects only the finding's own evidence.");
        }
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

    /// <summary>A stored TIMESTAMP is naive UTC.</summary>
    private static DateTime Utc(DbDataReader reader, int ordinal) =>
        DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc);

    /// <summary>A time as the store keeps it: to the microsecond, with no kind (a tick below it is not stored).</summary>
    private static DateTime Micros(DateTime utc) =>
        new(utc.Ticks - (utc.Ticks % 10), DateTimeKind.Unspecified);

    private static DuckDBCommand Command(DuckDBConnection connection, string sql, params object[] parameters)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var parameter in parameters)
            command.Parameters.Add(new DuckDBParameter { Value = parameter });
        return command;
    }
}
