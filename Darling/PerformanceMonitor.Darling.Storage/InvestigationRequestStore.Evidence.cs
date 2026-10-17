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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>A request that still needs evidence captured (#6001): open or claimed, not a practice replay, with no final
/// <c>evidence_state</c> yet.</summary>
public sealed record InvestigationEvidenceCandidate(
    Guid RequestId, int ServerId, string ServerName, string Family, string FirstMetric,
    DateTime FirstAlertAtUtc, DateTime OpenedAtUtc, bool IsPostgres);

/// <summary>One stored evidence section (config.investigation_evidence, #6001).</summary>
public sealed record InvestigationEvidenceRow(string Section, string State, string? ContentJson, DateTime? CapturedAtUtc);

/// <summary>One section the capture settles in one pass: <see cref="State"/> is <c>pending</c>, <c>ready</c> or
/// <c>not_collected</c>; <see cref="ContentJson"/> is set for <c>ready</c> only.</summary>
public sealed record InvestigationEvidenceWrite(string Section, string State, string? ContentJson);

/// <summary>What one <see cref="InvestigationRequestStore.RecordEvidenceAsync"/> call left the request at: its
/// <c>evidence_state</c> and whether THIS call wrote the one <c>evidence_ready</c> journal row.</summary>
public sealed record InvestigationEvidenceRecorded(string RequestState, bool JournaledReady);

public sealed partial class InvestigationRequestStore
{
    /// <summary>The journal's event for the moment a request's evidence is complete (D6). #6003 delivers it; this slice only
    /// writes the row, once per request.</summary>
    public const string EvidenceReadyEvent = "evidence_ready";

    /// <summary>The journal's <c>once_key</c> for a request's <see cref="EvidenceReadyEvent"/>, so it is written once.</summary>
    public static string EvidenceReadyOnceKey(Guid requestId) =>
        "evidence_ready:" + requestId.ToString("D", CultureInfo.InvariantCulture);

    /// <summary>Requests that still need evidence, newest first. $1 the oldest open time that still counts (a request older
    /// than the capture window is left as it is), $2 the row cap, $3 the first metric the capture serves.</summary>
    internal const string EvidenceCandidatesSql = @"
SELECT r.request_id, r.server_id, r.server_name, r.family, r.first_metric, r.first_alert_at, r.opened_at, s.engine_kind
FROM config.investigation_requests AS r
LEFT JOIN collect.servers AS s ON s.server_id = r.server_id
WHERE r.state IN ('open', 'claimed')
  AND r.practice_of IS NULL
  AND r.opened_at >= $1
  AND r.first_metric = $3
  AND (r.evidence_state IS NULL OR r.evidence_state = 'pending')
ORDER BY r.opened_at DESC
LIMIT $2";

    internal const string EvidenceRowsSql = @"
SELECT section, state, content::text, captured_at
FROM config.investigation_evidence
WHERE request_id = $1
ORDER BY evidence_id";

    /* A section that is no longer pending is FROZEN (D9): the upsert changes a row only while it is pending, so a second
       capture, or a later collection, never rewrites what the first froze. */
    internal const string EvidenceUpsertSql = @"
INSERT INTO config.investigation_evidence (request_id, section, state, content, captured_at)
VALUES ($1, $2, $3, $4::jsonb, $5)
ON CONFLICT (request_id, section) DO UPDATE
SET state = EXCLUDED.state, content = EXCLUDED.content, captured_at = EXCLUDED.captured_at
WHERE config.investigation_evidence.state = 'pending'";

    internal const string EvidenceTallySql = @"
SELECT count(*) FILTER (WHERE state = 'pending'), count(*) FILTER (WHERE state = 'ready')
FROM config.investigation_evidence
WHERE request_id = $1";

    internal const string EvidenceStateSql = @"
UPDATE config.investigation_requests SET evidence_state = $2 WHERE request_id = $1";

    /// <summary>The requests that still need evidence captured, opened at or after <paramref name="openedSinceUtc"/> whose first alert metric is <paramref name="firstMetric"/>.</summary>
    public async Task<IReadOnlyList<InvestigationEvidenceCandidate>> ListEvidenceCandidatesAsync(
        DateTime openedSinceUtc, int limit, string firstMetric, CancellationToken cancellationToken = default)
    {
        var rows = new List<InvestigationEvidenceCandidate>();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, EvidenceCandidatesSql, P(openedSinceUtc), P(limit), P(firstMetric));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new InvestigationEvidenceCandidate(
                reader.GetGuid(0), reader.GetInt32(1), reader.GetString(2), reader.GetString(3), reader.GetString(4),
                DateTime.SpecifyKind(reader.GetFieldValue<DateTime>(5), DateTimeKind.Utc),
                DateTime.SpecifyKind(reader.GetFieldValue<DateTime>(6), DateTimeKind.Utc),
                MonitoredEngineKind.IsPostgres(reader.IsDBNull(7) ? null : reader.GetString(7))));
        }

        return rows;
    }

    /// <summary>The evidence sections of a request, in the order they were first written.</summary>
    public async Task<IReadOnlyList<InvestigationEvidenceRow>> ReadEvidenceAsync(Guid requestId, CancellationToken cancellationToken = default)
    {
        var rows = new List<InvestigationEvidenceRow>();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, EvidenceRowsSql, P(requestId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new InvestigationEvidenceRow(
                reader.GetString(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : DateTime.SpecifyKind(reader.GetFieldValue<DateTime>(3), DateTimeKind.Utc)));
        }

        return rows;
    }

    /// <summary>
    /// Settles <paramref name="sections"/> for a request in one transaction (#6001, D9): a section that is still <c>pending</c>
    /// (or new) takes the written state and content; a section already <c>ready</c> or <c>not_collected</c> is left alone, so a
    /// repeated capture changes nothing. Then the request's <c>evidence_state</c> follows its sections (<c>pending</c> while any
    /// is pending, <c>ready</c> when none is and at least one is ready, else <c>not_collected</c>), and the moment it becomes
    /// <c>ready</c> the journal gets ONE <see cref="EvidenceReadyEvent"/> row (<see cref="EvidenceReadyOnceKey"/>, the
    /// journal's <c>once_key</c>), however often this runs.
    /// </summary>
    public async Task<InvestigationEvidenceRecorded> RecordEvidenceAsync(
        Guid requestId, int serverId, IReadOnlyList<InvestigationEvidenceWrite> sections, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sections);
        var now = Now();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        foreach (var section in sections)
        {
            await ExecAsync(connection, transaction, EvidenceUpsertSql, cancellationToken,
                P(requestId), P(section.Section), P(section.State),
                new NpgsqlParameter<string?> { TypedValue = section.State == "ready" ? section.ContentJson : null, NpgsqlDbType = NpgsqlDbType.Text },
                section.State == "pending"
                    ? new NpgsqlParameter<DateTime?> { TypedValue = null, NpgsqlDbType = NpgsqlDbType.TimestampTz }
                    : new NpgsqlParameter<DateTime?> { TypedValue = DateTime.SpecifyKind(now, DateTimeKind.Utc), NpgsqlDbType = NpgsqlDbType.TimestampTz });
        }

        long pending, ready;
        await using (var tally = Command(connection, transaction, EvidenceTallySql, P(requestId)))
        await using (var reader = await tally.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            pending = reader.GetInt64(0);
            ready = reader.GetInt64(1);
        }

        var state = pending > 0 ? "pending" : ready > 0 ? "ready" : "not_collected";
        await ExecAsync(connection, transaction, EvidenceStateSql, cancellationToken, P(requestId), P(state));
        var journaled = false;
        if (state == "ready")
        {
            var names = new JsonArray();
            foreach (var section in sections)
                names.Add(section.Section);
            var detail = new JsonObject { ["sections"] = names }.ToJsonString();
            journaled = await JournalAsync(connection, transaction, now, requestId, null, serverId, EvidenceReadyEvent, "service",
                null, detail, EvidenceReadyOnceKey(requestId), cancellationToken) > 0;
        }

        await transaction.CommitAsync(cancellationToken);
        return new InvestigationEvidenceRecorded(state, journaled);
    }
}
