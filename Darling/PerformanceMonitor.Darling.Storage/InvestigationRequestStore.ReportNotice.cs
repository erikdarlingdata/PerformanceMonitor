/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #6979: a report whose route asks for a notice and that has none yet. <see cref="ReportNotice"/> is the route's
/// <c>report_notice</c> word (never <c>off</c> here), <see cref="HasChange"/> says the request has at least one agent change
/// (an <c>action_called</c> row), and <see cref="RootCause"/> is the report's text as stored (the caller masks it).
/// </summary>
public sealed record ReportNoticeCandidate(
    Guid RequestId, int RouteId, int ServerId, string ServerName, string Verdict, string Metric, string RootCause,
    string ReportNotice, DateTime ReportedAtUtc, bool HasChange);

/// <summary>#6979: one agent change of a request, as a report notice lists it: the tool, the object the call named
/// (<c>target.id</c>, and <c>target.database</c> when the tool gave one) as the journal holds it, and how the call ended.</summary>
public sealed record RequestChangeRow(long CalledId, string Tool, string TargetId, string? Database, AgentChangeResult Result);

public sealed partial class InvestigationRequestStore
{
    /* #6979: the reports whose route asks for a notice, reported after $1, with no notice mark yet, oldest first. The window runs
       on the `reported` journal row, which SubmitAsync writes in the same transaction and with the same time as reported_at (the
       only writer of it): config.investigation_requests has no index on reported_at, and ix_investigation_journal_time bounds the
       read to the last 30 minutes of journal rows whatever the table holds. The request, its report and its route are then
       primary-key or unique-key lookups, and the mark test is the unique once_key index. r.reported_at > $1 is repeated so the
       window is the one the setting names. A practice request is a drill and sends no notice, as it sends no fallback. */
    internal const string PendingReportNoticesSql = @"
SELECT r.request_id, r.route_id, r.server_id, COALESCE(sv.display_name, sv.server_name, r.server_name),
       p.verdict, COALESCE(p.metric_name, r.first_metric), p.root_cause, rt.report_notice, r.reported_at,
       EXISTS (SELECT 1 FROM config.investigation_journal AS c
               WHERE c.request_id = r.request_id AND c.event = 'action_called')
FROM config.investigation_journal AS j
JOIN config.investigation_requests AS r ON r.request_id = j.request_id
JOIN config.investigation_reports AS p ON p.request_id = r.request_id
JOIN config.config_notification_routes AS rt ON rt.route_id = r.route_id
LEFT JOIN collect.servers AS sv ON sv.server_id = r.server_id
WHERE j.event = 'reported'
  AND j.event_time > $1
  AND r.reported_at > $1
  AND r.practice_of IS NULL
  AND rt.report_notice <> 'off'
  AND NOT EXISTS (SELECT 1 FROM config.investigation_journal AS m
                  WHERE m.once_key = 'report_notice:' || r.request_id::text)
ORDER BY j.journal_id
LIMIT $2";

    /// <summary>
    /// #6979: up to <paramref name="limit"/> reports made after <paramref name="sinceUtc"/> whose route's <c>report_notice</c> is
    /// not <c>off</c> and that have no notice mark (<see cref="InvestigationEvents.ReportNoticeOnceKey"/>), oldest first. Writes nothing.
    /// The caller decides which of them qualify under the route's setting.
    /// </summary>
    public async Task<IReadOnlyList<ReportNoticeCandidate>> PendingReportNoticesAsync(
        DateTime sinceUtc, int limit, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, PendingReportNoticesSql,
            P(DateTime.SpecifyKind(sinceUtc, DateTimeKind.Utc)), P(limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<ReportNoticeCandidate>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ReportNoticeCandidate(
                reader.GetGuid(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetString(3), reader.GetString(4),
                reader.GetString(5), reader.GetString(6), reader.GetString(7), reader.GetFieldValue<DateTime>(8), reader.GetBoolean(9)));
        }

        return rows;
    }

    /// <summary>
    /// #6979: the agent changes of <paramref name="requestId"/>, oldest first, each with its tool, object and result. A call with
    /// no <c>action_done</c> or <c>action_failed</c> row is <see cref="AgentChangeResult.Unknown"/>. Writes nothing.
    /// </summary>
    public async Task<IReadOnlyList<RequestChangeRow>> ChangesForRequestAsync(Guid requestId, CancellationToken cancellationToken = default)
    {
        /* One filtered read: AgentChangeUndo.ListSql by request_id, the same rows the web page lists (#6979). */
        var rows = await new AgentChangeUndo(_dataSource, _commandTimeoutSeconds).ChangesOfRequestAsync(requestId, cancellationToken);
        var result = new List<RequestChangeRow>(rows.Count);
        foreach (var row in rows)
        {
            var target = JsonNode.Parse(row.DetailJson) is JsonObject o ? o["target"] as JsonObject : null;
            var id = target?["id"] is JsonValue i && i.TryGetValue(out string? idText) ? idText : "";
            var database = target?["database"] is JsonValue d && d.TryGetValue(out string? dbText) ? dbText : null;
            result.Add(new RequestChangeRow(row.CalledId, row.Tool ?? "", id, string.IsNullOrWhiteSpace(database) ? null : database, row.Result));
        }

        return result;
    }

    /// <summary>
    /// #6979: writes the request's report-notice mark (once key <see cref="InvestigationEvents.ReportNoticeOnceKey"/>) with
    /// <paramref name="reason"/> and answers whether THIS call wrote it. The caller sends only on true, so a report is announced
    /// at most once however often the pass reads it.
    /// </summary>
    public Task<bool> MarkReportNoticeAsync(ReportNoticeCandidate candidate, string reason, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(candidate);
        return MarkOnceAsync(candidate.RequestId, candidate.RouteId, candidate.ServerId, InvestigationEvents.ReportNoticeSent, reason,
            InvestigationEvents.ReportNoticeOnceKey(candidate.RequestId), cancellationToken);
    }
}
