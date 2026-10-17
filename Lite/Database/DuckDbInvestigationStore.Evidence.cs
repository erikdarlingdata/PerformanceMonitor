/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Database;

/// <summary>A request that still needs evidence captured (#6010, the twin of Darling's #6001 candidate): open or claimed, or
/// holding a section that is still <c>pending</c>. <see cref="Metrics"/> is every metric linked to the request with the time
/// of its first linked alert.</summary>
public sealed record LiteEvidenceCandidate(
    Guid RequestId, int ServerId, string ServerName, string Family, string FirstMetric,
    DateTime FirstAlertAtUtc, DateTime OpenedAtUtc, IReadOnlyDictionary<string, DateTime> Metrics);

/// <summary>One stored evidence section.</summary>
public sealed record LiteEvidenceRow(string Section, string State, string? ContentJson, DateTime? CapturedAtUtc);

/// <summary>One section a capture pass settles: <see cref="State"/> is <c>pending</c>, <c>ready</c> or <c>not_collected</c>.</summary>
public sealed record LiteEvidenceWrite(string Section, string State, string? ContentJson);

/// <summary>What one <see cref="DuckDbInvestigationStore.RecordEvidenceAsync(Guid, int, IReadOnlyList{LiteEvidenceWrite}, IReadOnlySet{string}?, bool, CancellationToken)"/>
/// call left the request at, and whether THIS call wrote the one <c>evidence_ready</c> journal row.</summary>
public sealed record LiteEvidenceRecorded(string RequestState, bool JournaledReady);

/// <summary>
/// Evidence over DuckDB (#6010, the twin of Darling's #6001): frozen rows per section. A section that is no longer
/// <c>pending</c> is never rewritten (D9), and the request's <c>evidence_ready</c> journal row is written once.
/// </summary>
public sealed partial class DuckDbInvestigationStore
{
    /// <summary>The journal's event for the moment a request's evidence is complete.</summary>
    public const string EvidenceReadyEvent = "evidence_ready";

    /// <summary>The cap on one section's stored content, in bytes: Darling's table check, counted the way PostgreSQL prints a jsonb.</summary>
    internal const int EvidenceContentCapBytes = 32768;

    /// <summary>The journal's <c>once_key</c> for a request's <see cref="EvidenceReadyEvent"/>; the same text Darling writes.</summary>
    public static string EvidenceReadyOnceKey(Guid requestId) =>
        "evidence_ready:" + requestId.ToString("D", CultureInfo.InvariantCulture);

    /* Requests inside the capture horizon that are open or claimed, or whose evidence is not final. The scope (which metrics
       bring which sections) and the missing-section test are the caller's, in C#: DuckDB has no unnest-join shape worth the
       text, and the horizon keeps this to a few hundred rows at most. */
    internal const string EvidenceCandidatesSql = @"
SELECT r.request_id, r.server_id, r.server_name, r.family, r.first_metric, r.first_alert_at, r.opened_at, r.state, r.evidence_state
FROM investigation_requests AS r
WHERE r.opened_at >= $1
  AND (r.state IN ('open', 'claimed') OR r.evidence_state IS NULL OR r.evidence_state = 'pending'
       OR EXISTS (SELECT 1 FROM investigation_evidence AS e WHERE e.request_id = r.request_id AND e.state = 'pending'))
ORDER BY r.opened_at, r.request_id
LIMIT 500";

    internal const string EvidenceMetricsSql = @"
SELECT request_id, metric_name, MIN(alert_time)
FROM investigation_request_alerts
WHERE request_id IN (SELECT request_id FROM investigation_requests WHERE opened_at >= $1)
GROUP BY request_id, metric_name";

    internal const string EvidenceRowsSql = @"
SELECT section, state, content, captured_at
FROM investigation_evidence
WHERE request_id = $1
ORDER BY evidence_id";

    internal const string EvidenceAllRowsSql = @"
SELECT request_id, section, state
FROM investigation_evidence
WHERE request_id IN (SELECT request_id FROM investigation_requests WHERE opened_at >= $1)";

    /* The upsert changes a row only while it is pending, so a second capture, or a later collection, never rewrites a frozen
       section. The id is max + 1 under the write lock. */
    internal const string EvidenceInsertSql = @"
INSERT INTO investigation_evidence (evidence_id, request_id, section, state, content, captured_at, created_at)
VALUES ((SELECT coalesce(max(evidence_id), 0) + 1 FROM investigation_evidence), $1, $2, $3, $4, $5, $6)
ON CONFLICT (request_id, section) DO NOTHING";

    internal const string EvidenceUpdateSql = @"
UPDATE investigation_evidence SET state = $3, content = $4, captured_at = $5
WHERE request_id = $1 AND section = $2 AND state = 'pending'";

    internal const string EvidenceStateSql = "UPDATE investigation_requests SET evidence_state = $2 WHERE request_id = $1";

    /// <summary>Requests that need evidence, oldest first, opened at or after <paramref name="openedSinceUtc"/>, that
    /// <paramref name="keys"/> (linked metrics, or <c>family:&lt;name&gt;</c>) bring a section to. A request open or claimed
    /// whose evidence is final stays a candidate while one of its keys' sections has no row yet (a metric that joined late).</summary>
    public async Task<IReadOnlyList<LiteEvidenceCandidate>> ListEvidenceCandidatesAsync(
        DateTime openedSinceUtc, int limit, IReadOnlyDictionary<string, IReadOnlyList<string>> sectionsByKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sectionsByKey);
        using var connection = await OpenAsync(cancellationToken);
        var raw = new List<(Guid Id, int Server, string Name, string Family, string First, DateTime FirstAt, DateTime Opened, string State, string? Ev)>();
        using (var command = Command(connection, EvidenceCandidatesSql, openedSinceUtc))
        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                raw.Add((reader.GetGuid(0), reader.GetInt32(1), reader.GetString(2), reader.GetString(3), reader.GetString(4),
                    Utc(reader.GetDateTime(5)), Utc(reader.GetDateTime(6)), reader.GetString(7), reader.IsDBNull(8) ? null : reader.GetString(8)));
            }
        }

        var metrics = new Dictionary<Guid, Dictionary<string, DateTime>>();
        using (var command = Command(connection, EvidenceMetricsSql, openedSinceUtc))
        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetGuid(0);
                if (!metrics.TryGetValue(id, out var map))
                    metrics[id] = map = new Dictionary<string, DateTime>(StringComparer.Ordinal);
                map[reader.GetString(1)] = Utc(reader.GetDateTime(2));
            }
        }

        var have = new Dictionary<Guid, HashSet<string>>();
        var pending = new HashSet<Guid>();
        using (var command = Command(connection, EvidenceAllRowsSql, openedSinceUtc))
        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetGuid(0);
                if (!have.TryGetValue(id, out var set))
                    have[id] = set = new HashSet<string>(StringComparer.Ordinal);
                set.Add(reader.GetString(1));
                if (reader.GetString(2) == "pending")
                    pending.Add(id);
            }
        }

        var result = new List<LiteEvidenceCandidate>();
        foreach (var r in raw)
        {
            var map = metrics.GetValueOrDefault(r.Id) ?? new Dictionary<string, DateTime>(StringComparer.Ordinal);
            var wanted = new List<string>();
            foreach (var (key, sections) in sectionsByKey)
            {
                if (key == "family:" + r.Family || map.ContainsKey(key) || key == r.First)
                    wanted.AddRange(sections);
            }

            if (wanted.Count == 0)
                continue;
            var open = r.State is "open" or "claimed";
            var has = have.GetValueOrDefault(r.Id);
            var missing = open && wanted.Any(w => has is null || !has.Contains(w));
            var notFinal = r.Ev is null or "pending";
            if (!((notFinal && (open || pending.Contains(r.Id))) || missing))
                continue;
            result.Add(new LiteEvidenceCandidate(r.Id, r.Server, r.Name, r.Family, r.First, r.FirstAt, r.Opened, map));
            if (result.Count >= limit)
                break;
        }

        return result;
    }

    /// <summary>The evidence sections of a request, in the order they were first written.</summary>
    public async Task<IReadOnlyList<LiteEvidenceRow>> ReadEvidenceAsync(Guid requestId, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenAsync(cancellationToken);
        return await ReadEvidenceRowsAsync(connection, requestId, cancellationToken);
    }

    private static async Task<List<LiteEvidenceRow>> ReadEvidenceRowsAsync(DuckDB.NET.Data.DuckDBConnection connection, Guid requestId,
        CancellationToken cancellationToken)
    {
        var rows = new List<LiteEvidenceRow>();
        using var command = Command(connection, EvidenceRowsSql, requestId);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new LiteEvidenceRow(reader.GetString(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : Utc(reader.GetDateTime(3))));
        }

        return rows;
    }

    /// <summary>The bytes <paramref name="json"/> takes as PostgreSQL prints a jsonb: each colon and comma counts twice, so Lite
    /// holds a section to the same size Darling's table check does and the two sections read alike.</summary>
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

    /// <summary>The content a <c>ready</c> section is stored with: through <see cref="DerivedStoreScrub.Json"/>, and as
    /// <c>status: unavailable</c> when the scrub withheld it whole or grew it past the cap (Darling's #6002 rule).</summary>
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
    /// Settles <paramref name="sections"/> for a request in one transaction (D9): a section still <c>pending</c> (or new) takes
    /// the written state and content; a section already <c>ready</c> or <c>not_collected</c> is left alone. The request's
    /// <c>evidence_state</c> then follows its sections, and the moment it is <c>ready</c> (or only slow sections
    /// <paramref name="slowSections"/> are pending after the base grace, with one ready) the journal gets ONE
    /// <c>evidence_ready</c> row, however often this runs.
    /// </summary>
    public async Task<LiteEvidenceRecorded> RecordEvidenceAsync(
        Guid requestId, int serverId, IReadOnlyList<LiteEvidenceWrite> sections,
        IReadOnlySet<string>? slowSections, bool baseGraceElapsed, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sections);
        var now = Now();
        using var writeLock = _db.AcquireWriteLock(_lockBudget);
        using var connection = await OpenAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();
        foreach (var section in sections)
        {
            var content = section.State == "ready" ? StoredEvidenceContent(section.ContentJson) : null;
            DateTime? capturedAt = section.State == "pending" ? null : now;
            await ExecAsync(connection, EvidenceInsertSql, cancellationToken, requestId, section.Section, section.State, content, capturedAt, now);
            await ExecAsync(connection, EvidenceUpdateSql, cancellationToken, requestId, section.Section, section.State, content, capturedAt);
        }

        long pending = 0, ready = 0;
        var readySections = new JsonArray();
        var notCollectedSections = new JsonArray();
        var pendingSections = new JsonArray();
        var pendingOnBaseGrace = 0;
        foreach (var row in await ReadEvidenceRowsAsync(connection, requestId, cancellationToken))
        {
            switch (row.State)
            {
                case "pending":
                    pending++;
                    pendingSections.Add(row.Section);
                    if (slowSections is null || !slowSections.Contains(row.Section))
                        pendingOnBaseGrace++;
                    break;
                case "ready":
                    ready++;
                    readySections.Add(row.Section);
                    break;
                default:
                    notCollectedSections.Add(row.Section);
                    break;
            }
        }

        var state = pending > 0 ? "pending" : ready > 0 ? "ready" : "not_collected";
        await ExecAsync(connection, EvidenceStateSql, cancellationToken, requestId, state);
        var journaled = false;
        if (state == "ready" || (state == "pending" && baseGraceElapsed && pendingOnBaseGrace == 0 && ready > 0))
        {
            var detailNode = new JsonObject { ["sections"] = readySections, ["not_collected"] = notCollectedSections };
            if (pendingSections.Count > 0)
                detailNode["pending"] = pendingSections;
            journaled = await JournalAsync(connection, now, requestId, null, serverId, EvidenceReadyEvent, "service",
                null, detailNode.ToJsonString(), EvidenceReadyOnceKey(requestId), cancellationToken) > 0;
        }

        transaction.Commit();
        return new LiteEvidenceRecorded(state, journaled);
    }
}
