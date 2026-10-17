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
/// <c>evidence_state</c> yet. <see cref="Metrics"/> (#6002) is every metric linked to the request
/// (<c>config.investigation_request_alerts</c>), each with the time of its first linked alert: a join can add a family of
/// sections to a request that opened on another metric.</summary>
public sealed record InvestigationEvidenceCandidate(
    Guid RequestId, int ServerId, string ServerName, string Family, string FirstMetric,
    DateTime FirstAlertAtUtc, DateTime OpenedAtUtc, bool IsPostgres,
    IReadOnlyDictionary<string, DateTime>? Metrics = null);

/// <summary>Which requests the capture serves (#6002): a request linked to a metric in <see cref="Keys"/>, or in a family named
/// there as <c>family:&lt;name&gt;</c>, with <see cref="Sections"/> (aligned with <see cref="Keys"/>) the sections that key
/// brings. A request still open or claimed that lacks one of its keys' sections is a candidate again, so a metric that joins
/// after the sections froze gets its own sections on the next pass.</summary>
public sealed record InvestigationEvidenceScope(IReadOnlyList<string> Keys, IReadOnlyList<string> Sections)
{
    /// <summary>The scope key of a family's sections.</summary>
    public static string FamilyKey(string family) => "family:" + family;
}

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

    /// <summary>Requests that still need evidence, OLDEST first (#6001 review: the oldest are the closest to their grace, so a
    /// backlog reaches them before a newer request). $1 the oldest open time that still counts (a request older than the
    /// capture window is left as it is), $2 the page size, $3 and $4 the scope (<see cref="InvestigationEvidenceScope"/>: the
    /// keys, each a linked metric or <c>family:&lt;name&gt;</c>, and the section each key brings), $5 and $6 the keyset cursor:
    /// the (opened_at, request_id) of the last request the previous page returned, so a request that stays pending is not
    /// returned again in the same tick. A request in scope (#6002: ANY linked metric, not only the first, or its family) is a
    /// candidate while it is open or claimed, and also in any other state (reported, expired) while one of its sections is
    /// still <c>pending</c> (#6001 round 2): a slow section's grace can outlast the request, and the section then flips to
    /// <c>ready</c> or <c>not_collected</c> on a later pass instead of staying <c>pending</c> for good. An open or claimed
    /// request whose evidence is already final is a candidate again when one of its keys' sections has no row yet (#6002): a
    /// metric that joined after the sections froze.</summary>
    internal const string EvidenceCandidatesSql = @"
SELECT r.request_id, r.server_id, r.server_name, r.family, r.first_metric, r.first_alert_at, r.opened_at, s.engine_kind,
       m.names, m.firsts
FROM config.investigation_requests AS r
LEFT JOIN collect.servers AS s ON s.server_id = r.server_id
CROSS JOIN LATERAL (
    SELECT COALESCE(array_agg(a.metric_name ORDER BY a.metric_name), '{}') AS names,
           COALESCE(array_agg(a.first_at ORDER BY a.metric_name), '{}') AS firsts
    FROM (SELECT metric_name, MIN(alert_time) AS first_at
          FROM config.investigation_request_alerts
          WHERE request_id = r.request_id
          GROUP BY metric_name) AS a
) AS m
CROSS JOIN LATERAL (
    SELECT count(*) AS sections,
           COALESCE(bool_or(NOT EXISTS (SELECT 1 FROM config.investigation_evidence AS e
                                        WHERE e.request_id = r.request_id AND e.section = k.section)), false) AS missing
    FROM unnest($3::text[], $4::text[]) AS k(key, section)
    WHERE k.key = 'family:' || r.family
       OR k.key = ANY(m.names)
) AS k
WHERE r.practice_of IS NULL
  AND r.opened_at >= $1
  AND k.sections > 0
  AND (((r.evidence_state IS NULL OR r.evidence_state = 'pending')
        AND (r.state IN ('open', 'claimed')
             OR EXISTS (SELECT 1 FROM config.investigation_evidence AS e WHERE e.request_id = r.request_id AND e.state = 'pending')))
       OR (r.state IN ('open', 'claimed') AND k.missing))
  AND (r.opened_at, r.request_id) > ($5, $6)
ORDER BY r.opened_at, r.request_id
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

    /* Every section of the request after this call's writes, so the journal names the ready ones and the not_collected ones
       whatever this call wrote (#6001 review). */
    internal const string EvidenceTallySql = @"
SELECT section, state
FROM config.investigation_evidence
WHERE request_id = $1
ORDER BY evidence_id";

    internal const string EvidenceStateSql = @"
UPDATE config.investigation_requests SET evidence_state = $2 WHERE request_id = $1";

    /// <summary>One page of the requests that still need evidence captured, OLDEST first, opened at or after
    /// <paramref name="openedSinceUtc"/> that <paramref name="scope"/> serves. <paramref name="after"/> is the candidate the
    /// previous page ended on (null for the first page).</summary>
    public async Task<IReadOnlyList<InvestigationEvidenceCandidate>> ListEvidenceCandidatesAsync(
        DateTime openedSinceUtc, int limit, InvestigationEvidenceScope scope, InvestigationEvidenceCandidate? after = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        var rows = new List<InvestigationEvidenceCandidate>();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, EvidenceCandidatesSql, P(openedSinceUtc), P(limit),
            P(scope.Keys.ToArray()), P(scope.Sections.ToArray()),
            P(after?.OpenedAtUtc ?? DateTime.UnixEpoch), P(after?.RequestId ?? Guid.Empty));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var names = reader.GetFieldValue<string[]>(8);
            var firsts = reader.GetFieldValue<DateTime[]>(9);
            var metrics = new Dictionary<string, DateTime>(StringComparer.Ordinal);
            for (var i = 0; i < names.Length && i < firsts.Length; i++)
                metrics[names[i]] = DateTime.SpecifyKind(firsts[i], DateTimeKind.Utc);
            rows.Add(new InvestigationEvidenceCandidate(
                reader.GetGuid(0), reader.GetInt32(1), reader.GetString(2), reader.GetString(3), reader.GetString(4),
                DateTime.SpecifyKind(reader.GetFieldValue<DateTime>(5), DateTimeKind.Utc),
                DateTime.SpecifyKind(reader.GetFieldValue<DateTime>(6), DateTimeKind.Utc),
                MonitoredEngineKind.IsPostgres(reader.IsDBNull(7) ? null : reader.GetString(7)),
                metrics));
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

    /// <summary>The cap <c>ck_investigation_evidence_content</c> holds <c>octet_length(content::text)</c> to; it equals the capture's
    /// <c>InvestigationToolCore.MaxEvidenceBytes</c>, which this project cannot see (a test pins the two together).</summary>
    internal const int EvidenceContentCapBytes = 32768;

    /// <summary>The bytes <paramref name="json"/> takes as PostgreSQL prints a jsonb (<c>octet_length(content::text)</c>, which
    /// the table's check holds to <see cref="EvidenceContentCapBytes"/>): a space follows every structural colon and
    /// comma, so each colon and comma is counted twice, strings included, which only over-counts. The capture's own
    /// <c>StoredBytes</c> is the same count; the Storage project cannot see the Service.</summary>
    internal static int StoredEvidenceBytes(string json)
    {
        var extra = 0;
        foreach (var c in json)
        {
            if (c == ':' || c == ',')
                extra++;
        }

        return System.Text.Encoding.UTF8.GetByteCount(json) + extra;
    }

    /// <summary>
    /// The content a <c>ready</c> section is stored with (#6002 review round 2): <paramref name="contentJson"/> through
    /// <see cref="DerivedStoreScrub.Json"/>, unless the scrub withheld it whole (null) or grew it past
    /// <see cref="EvidenceContentCapBytes"/> (the write judge replaced a short value with the placeholder after the
    /// capture filled the section to the cap). Then the section is stored as <c>status: unavailable</c> with the reason's name and
    /// no collected text, so <c>ck_investigation_evidence_content</c> never fires and no ready row has null content.
    /// </summary>
    internal static string StoredEvidenceContent(string? contentJson)
    {
        var scrubbed = contentJson is null ? null : DerivedStoreScrub.Json(contentJson);
        if (scrubbed is not null && StoredEvidenceBytes(scrubbed) <= EvidenceContentCapBytes)
            return scrubbed;

        var unavailable = new JsonObject();
        try
        {
            var original = contentJson is null ? null : JsonNode.Parse(contentJson) as JsonObject;
            unavailable["window_start"] = original?["window_start"]?.GetValue<string>();
            unavailable["window_end"] = original?["window_end"]?.GetValue<string>();
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
        {
            unavailable["window_start"] = null;
            unavailable["window_end"] = null;
        }

        unavailable["rows"] = new JsonArray();
        unavailable["status"] = "unavailable";
        unavailable["error"] = scrubbed is null ? "EvidenceWithheld" : "EvidenceTooLarge";
        return unavailable.ToJsonString();
    }

    /// <summary>
    /// Settles <paramref name="sections"/> for a request in one transaction (#6001, D9): a section that is still <c>pending</c>
    /// (or new) takes the written state and content; a section already <c>ready</c> or <c>not_collected</c> is left alone, so a
    /// repeated capture changes nothing. Then the request's <c>evidence_state</c> follows its sections (<c>pending</c> while any
    /// is pending, <c>ready</c> when none is and at least one is ready, else <c>not_collected</c>), and the moment it becomes
    /// <c>ready</c> the journal gets ONE <see cref="EvidenceReadyEvent"/> row (<see cref="EvidenceReadyOnceKey"/>, the
    /// journal's <c>once_key</c>), however often this runs.
    ///
    /// <para>Slow collectors (#6001): <paramref name="slowSections"/> are the sections whose collector runs too rarely for the
    /// base grace. Once <paramref name="baseGraceElapsed"/> and every section NOT in that set is final, the one journal row is
    /// written although a slow section is still <c>pending</c> (and at least one section is ready); its detail lists that
    /// section under <c>pending</c>. The request's <c>evidence_state</c> stays <c>pending</c> (the table allows no other state
    /// for it), so the capture keeps visiting the request until the slow section flips in place to <c>ready</c> or
    /// <c>not_collected</c>; that later flip writes no second journal row.</para>
    /// </summary>
    public Task<InvestigationEvidenceRecorded> RecordEvidenceAsync(
        Guid requestId, int serverId, IReadOnlyList<InvestigationEvidenceWrite> sections, CancellationToken cancellationToken = default) =>
        RecordEvidenceAsync(requestId, serverId, sections, null, false, cancellationToken);

    /// <inheritdoc cref="RecordEvidenceAsync(Guid, int, IReadOnlyList{InvestigationEvidenceWrite}, CancellationToken)"/>
    public async Task<InvestigationEvidenceRecorded> RecordEvidenceAsync(
        Guid requestId, int serverId, IReadOnlyList<InvestigationEvidenceWrite> sections,
        IReadOnlySet<string>? slowSections, bool baseGraceElapsed, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sections);
        var now = Now();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        foreach (var section in sections)
        {
            await ExecAsync(connection, transaction, EvidenceUpsertSql, cancellationToken,
                P(requestId), P(section.Section), P(section.State),
                /* #4348 (#6002 review): the statement filter at the write, as the finding and alert stores have it. The capture
                   already scrubbed each row before fitting it under the cap, so clean content comes back the same instance. When the
                   write's judge disagrees with the capture's, the scrub can grow the content past the table's size check (a short value
                   becomes the placeholder) or withhold it whole; either stores the section as unavailable, so the check never aborts
                   the batch and a ready row never carries null content (#6002 review round 2). */
                new NpgsqlParameter<string?> { TypedValue = section.State == "ready" ? StoredEvidenceContent(section.ContentJson) : null, NpgsqlDbType = NpgsqlDbType.Text },
                section.State == "pending"
                    ? new NpgsqlParameter<DateTime?> { TypedValue = null, NpgsqlDbType = NpgsqlDbType.TimestampTz }
                    : new NpgsqlParameter<DateTime?> { TypedValue = DateTime.SpecifyKind(now, DateTimeKind.Utc), NpgsqlDbType = NpgsqlDbType.TimestampTz });
        }

        long pending = 0, ready = 0;
        var readySections = new JsonArray();
        var notCollectedSections = new JsonArray();
        var pendingSections = new JsonArray();
        var pendingOnBaseGrace = 0;
        await using (var tally = Command(connection, transaction, EvidenceTallySql, P(requestId)))
        await using (var reader = await tally.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var name = reader.GetString(0);
                switch (reader.GetString(1))
                {
                    case "pending":
                        pending++;
                        pendingSections.Add(name);
                        if (slowSections is null || !slowSections.Contains(name))
                            pendingOnBaseGrace++;
                        break;
                    case "ready":
                        ready++;
                        readySections.Add(name);
                        break;
                    default:
                        notCollectedSections.Add(name);
                        break;
                }
            }
        }

        var state = pending > 0 ? "pending" : ready > 0 ? "ready" : "not_collected";
        await ExecAsync(connection, transaction, EvidenceStateSql, cancellationToken, P(requestId), P(state));
        var journaled = false;
        /* Every section final, or only slow ones left after the base grace with something ready to report. */
        if (state == "ready" || (state == "pending" && baseGraceElapsed && pendingOnBaseGrace == 0 && ready > 0))
        {
            /* #6001 review: `sections` names the READY sections and `not_collected` the ones that never landed, read from the
               rows the request holds now, not from this call's writes (the grace-expiry call writes only the not_collected ones). */
            var detailNode = new JsonObject { ["sections"] = readySections, ["not_collected"] = notCollectedSections };
            if (pendingSections.Count > 0)
                detailNode["pending"] = pendingSections;
            var detail = detailNode.ToJsonString();
            journaled = await JournalAsync(connection, transaction, now, requestId, null, serverId, EvidenceReadyEvent, "service",
                null, detail, EvidenceReadyOnceKey(requestId), cancellationToken) > 0;
        }

        await transaction.CommitAsync(cancellationToken);
        return new InvestigationEvidenceRecorded(state, journaled);
    }
}
