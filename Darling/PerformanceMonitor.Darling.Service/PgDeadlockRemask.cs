/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Brings the PostgreSQL deadlock reports, and the deadlock alerts, stored before #4005 to its rules (#4012), one
/// bounded slice per hourly store-maintenance tick, the pattern #3915 set for the store log
/// (<see cref="StoreLogSweep.RemaskStoredEventsAsync"/>).
///
/// <para><b>What is left after #4005.</b> Since #4005 the collector stores each report's queries normalized, and
/// every product read normalizes a row stored before it. The rows themselves still hold their SQL raw:
/// <c>pg_deadlocks</c> keeps <c>victim_statement</c> and <c>graph_text</c> as the old collector wrote them, with a
/// <c>deadlock_hash</c> over the raw graph (a test for the literals, #4004), for its 90-day retention; and a
/// deadlock alert fired before #4005 keeps that hash and the raw victim statement in its history row's
/// <c>context_json</c>, and in <c>detail_text</c> when it was delivered per event, for the alert log's 90 days.
/// A role with a direct <c>SELECT</c> (#4004 names mcp's) reads them as stored.</para>
///
/// <para><b>The reports.</b> A row is still raw when <see cref="PgDeadlockLogParser.RawGraphHashSql"/> is true for
/// it, and only such a row is rewritten, by the same statement that tests it, so a pass is safe to repeat and
/// safe beside the collector (which writes only #4005's form). Its graph and victim statement become what every
/// read already shows for it (<see cref="PgDeadlockLogParser.NormalizeGraph"/>,
/// <see cref="PgDeadlockLogParser.NormalizeStatement"/>, both idempotent), so the rewrite changes nothing a
/// reader sees but the identity: the row takes #4005's (<see cref="PgDeadlockLogParser.IdentityOf"/> over its
/// timestamp and normalized graph), and its sightings group with any the new collector stored of the same
/// report.</para>
///
/// <para><b>The alerts go first</b>, because they are found through the reports: an incident's key is the raw
/// hash of a report, and the report's text is what its normalized incident is built from
/// (<see cref="DarlingWorker.BuildPgDeadlockIncident"/>, the live alert's own builder). Once a report is
/// rewritten its raw hash is gone, so the report pass starts only after the alert pass reached the log's
/// end.</para>
/// </summary>
public static class PgDeadlockRemask
{
    /// <summary>How many stored report rows one slice examines. A report is rewritten with all of its sightings,
    /// so most rows a slice meets after the first sighting of each report are already in #4005's form.</summary>
    public const int MaxReportRowsPerPass = 2000;

    /// <summary>How many deadlock alert rows one slice examines.</summary>
    public const int MaxAlertRowsPerPass = 500;

    /// <summary>How long one slice may run: its own cap inside the hourly tick's budget, as #3920's review gave
    /// the store-log slice, so a slow slice gives up alone and the collector-cost flush after it still runs.</summary>
    public static readonly TimeSpan SliceBudget = TimeSpan.FromSeconds(60);

    /// <summary>The server-side <c>statement_timeout</c> every statement of a slice runs under, set LOCAL to its
    /// own transaction. Authoritative, so an overrun ends as <c>57014</c> naming the cancel.</summary>
    public const int StatementTimeoutSeconds = 15;

    /// <summary>The client-side backstop, strictly above <see cref="StatementTimeoutSeconds"/> so the server's
    /// cancel is the one that fires (the <c>ServiceCommandDeadlines.CliBudgetBackstopSeconds</c> rule).</summary>
    public const int CommandBackstopSeconds = StatementTimeoutSeconds + 5;

    /// <summary>How many times one process reads a table through again after a scan that left rows raw: an alert
    /// row that changed between its read and its write, or a report whose rewrite failed. Bounded, so a report
    /// that fails every time costs a few re-reads rather than one page an hour for the life of the process; it is
    /// left to the next process's pass, and every read normalizes it meanwhile.</summary>
    public const int MaxRescansPerProcess = 3;

    /// <summary>Where a report slice resumes: the last row it examined, in the page's order.</summary>
    public readonly record struct ReportCursor(DateTime CollectionTime, int ServerId, string DeadlockHash);

    /// <summary>
    /// One page of stored reports, in <c>(collection_time, server_id, deadlock_hash)</c> order after the cursor
    /// (<c>$1</c>..<c>$3</c>, null for the start), <c>$4</c> rows. Keyed on the partitioning column so the page
    /// walks the hypertable's chunks in order. Only a raw row's text leaves the store; a row already in #4005's
    /// form is examined by its key alone.
    /// </summary>
    public static readonly string ReportPageSql = $@"
SELECT
    p.collection_time,
    p.server_id,
    p.deadlock_hash,
    p.occurred_at,
    p.victim_pid,
    CASE WHEN r.raw_hash THEN p.victim_statement END AS victim_statement,
    CASE WHEN r.raw_hash THEN p.graph_text END       AS graph_text,
    COALESCE(r.raw_hash, false)                      AS raw_hash
FROM
(
    SELECT
        d.collection_time,
        d.server_id,
        d.deadlock_hash,
        d.occurred_at,
        d.victim_pid,
        d.victim_statement,
        d.graph_text
    FROM pg_deadlocks AS d
    WHERE d.deadlock_hash IS NOT NULL
    AND   ($1::timestamp IS NULL
           OR (d.collection_time >= $1::timestamp
               AND (d.collection_time, d.server_id, d.deadlock_hash) > ($1::timestamp, $2::integer, $3::text)))
    ORDER BY d.collection_time, d.server_id, d.deadlock_hash
    LIMIT $4
) AS p
CROSS JOIN LATERAL (SELECT {PgDeadlockLogParser.RawGraphHashSql("p.deadlock_hash", "p.graph_text")} AS raw_hash) AS r
ORDER BY p.collection_time, p.server_id, p.deadlock_hash";

    /// <summary>
    /// Rewrites every sighting of one raw report: <c>$1</c> server, <c>$2</c> its raw hash, <c>$3</c> the first
    /// sighting the page met (the pager walks <c>collection_time</c> upward and leaves nothing raw behind it, so
    /// no raw sighting of this report is older), <c>$4</c>..<c>$6</c> what the page read, <c>$7</c>..<c>$9</c> the
    /// normalized values. The raw test is part of the statement, so a row already rewritten, or stored since, is
    /// never touched.
    /// </summary>
    public static readonly string ReportUpdateSql = $@"
UPDATE pg_deadlocks AS d
SET victim_statement = $7,
    graph_text = $8,
    deadlock_hash = $9
WHERE d.server_id = $1
AND   d.deadlock_hash = $2
AND   d.collection_time >= $3
AND   d.occurred_at IS NOT DISTINCT FROM $4
AND   d.victim_pid IS NOT DISTINCT FROM $5
AND   d.victim_statement IS NOT DISTINCT FROM $6
AND   {PgDeadlockLogParser.RawGraphHashSql("d.deadlock_hash", "d.graph_text")}";

    /// <summary>One page of deadlock alert rows in physical order after <c>$1</c> (null for the start). The
    /// history table is a plain table, so its physical order is the store-log slice's cursor too.</summary>
    public const string AlertPageSql = @"
SELECT
    a.ctid::text,
    a.server_id,
    a.detail_text,
    a.context_json
FROM config_alert_log AS a
WHERE ($1::tid IS NULL OR a.ctid > $1::tid)
AND   a.metric_name = '" + AlertEngine.DeadlockWatermarkMetric + @"'
AND   a.context_json IS NOT NULL
ORDER BY a.ctid
LIMIT $2";

    /// <summary>
    /// The stored report each alert incident names, by server and hash (<c>$1</c>/<c>$2</c>, parallel arrays):
    /// whether its hash is raw, and a raw one's text. The earliest report where one hash covers two, as
    /// <see cref="DarlingPgDeadlockReader.DeadlocksSql"/> grouped them when the alert fired.
    /// </summary>
    public static readonly string AlertReportLookupSql = $@"
SELECT
    q.server_id,
    q.deadlock_hash,
    q.occurred_at,
    q.victim_pid,
    q.participant_count,
    CASE WHEN q.raw_hash THEN q.victim_statement END AS victim_statement,
    CASE WHEN q.raw_hash THEN q.graph_text END       AS graph_text,
    COALESCE(q.raw_hash, false)                      AS raw_hash
FROM
(
    SELECT DISTINCT ON (d.server_id, d.deadlock_hash)
        d.server_id,
        d.deadlock_hash,
        d.occurred_at,
        d.victim_pid,
        d.participant_count,
        d.victim_statement,
        d.graph_text,
        {PgDeadlockLogParser.RawGraphHashSql("d.deadlock_hash", "d.graph_text")} AS raw_hash
    FROM pg_deadlocks AS d
    JOIN unnest($1::integer[], $2::text[]) AS k(server_id, deadlock_hash)
      ON  d.server_id = k.server_id
      AND d.deadlock_hash = k.deadlock_hash
    ORDER BY d.server_id, d.deadlock_hash, d.occurred_at NULLS LAST, d.collection_time
) AS q";

    /// <summary>Writes one alert row's normalized context and detail back, only while the row still holds exactly
    /// what the page read, so a row changed in between is left for the next pass.</summary>
    public const string AlertUpdateSql = @"
UPDATE config_alert_log
SET context_json = $1,
    detail_text = $2
WHERE ctid = $3::tid
AND   context_json = $4
AND   detail_text IS NOT DISTINCT FROM $5";

    /// <summary>
    /// A stored report in #4005's form: the graph and victim statement every read shows for it, and the identity
    /// the collector gives a report since, over its timestamp and that graph. Pure; the same text in gives the same
    /// row out, and a row already in this form is left as it is by the slice's raw test, never by this.
    /// </summary>
    public static (string? VictimStatement, string GraphText, string DeadlockHash) RemaskReport(
        DateTime? occurredAt, string? victimStatement, string graphText)
    {
        ArgumentNullException.ThrowIfNull(graphText);

        /* The collector's own trim (PgDeadlockLogParser.FromEntry), so the identity is over the text it hashes. */
        var graph = PgDeadlockLogParser.NormalizeGraph(graphText)!.TrimEnd('\n');
        return (
            PgDeadlockLogParser.NormalizeStatement(victimStatement),
            graph,
            PgDeadlockLogParser.IdentityOf(occurredAt ?? default, graph));
    }

    /// <summary>
    /// Examines one slice of stored reports and rewrites the raw ones (#4012). Each report is its own transaction
    /// under <see cref="StatementTimeoutSeconds"/>, and one that fails (a timeout on a very long-lived report, a
    /// lock) is counted and left raw for the next process's pass rather than holding the rest of the table back.
    /// Returns the cursor to resume from, null once the table's end is reached.
    /// </summary>
    public static async Task<(ReportCursor? Next, int Examined, int Rewritten, int Failed)> RemaskStoredReportsAsync(
        NpgsqlConnection connection, ReportCursor? after, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var page = new List<(DateTime CollectionTime, int ServerId, string Hash, DateTime? OccurredAt, int? VictimPid,
            string? Victim, string? Graph, bool Raw)>();
        await using (var transaction = await BeginBoundedAsync(connection, cancellationToken))
        {
            await using (var select = new NpgsqlCommand(ReportPageSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
            {
                select.Parameters.Add(new NpgsqlParameter { Value = after is { } a ? a.CollectionTime : DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
                select.Parameters.Add(new NpgsqlParameter { Value = after is { } b ? b.ServerId : DBNull.Value, NpgsqlDbType = NpgsqlDbType.Integer });
                select.Parameters.Add(new NpgsqlParameter { Value = after is { } c ? c.DeadlockHash : DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                select.Parameters.AddWithValue(MaxReportRowsPerPass);
                await using var reader = await select.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    page.Add((
                        reader.GetDateTime(0),
                        reader.GetInt32(1),
                        reader.GetString(2),
                        reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                        reader.IsDBNull(4) ? null : reader.GetInt32(4),
                        reader.IsDBNull(5) ? null : reader.GetString(5),
                        reader.IsDBNull(6) ? null : reader.GetString(6),
                        reader.GetBoolean(7)));
                }
            }

            await transaction.CommitAsync(cancellationToken);
        }

        int rewritten = 0, failed = 0;
        var done = new HashSet<(int, string, DateTime?, int?, string?)>();
        foreach (var row in page)
        {
            /* A report is rewritten with all of its sightings at its first: the rest of them in this page are the
               same key, and are rewritten already. */
            if (!row.Raw || row.Graph is null || !done.Add((row.ServerId, row.Hash, row.OccurredAt, row.VictimPid, row.Victim)))
            {
                continue;
            }

            var (victim, graph, hash) = RemaskReport(row.OccurredAt, row.Victim, row.Graph);
            try
            {
                await using var transaction = await BeginBoundedAsync(connection, cancellationToken);
                await using (var update = new NpgsqlCommand(ReportUpdateSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
                {
                    update.Parameters.AddWithValue(row.ServerId);
                    update.Parameters.AddWithValue(row.Hash);
                    update.Parameters.Add(new NpgsqlParameter { Value = row.CollectionTime, NpgsqlDbType = NpgsqlDbType.Timestamp });
                    update.Parameters.Add(new NpgsqlParameter { Value = (object?)row.OccurredAt ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
                    update.Parameters.Add(new NpgsqlParameter { Value = (object?)row.VictimPid ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Integer });
                    update.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Victim ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = (object?)victim ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = graph, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = hash, NpgsqlDbType = NpgsqlDbType.Text });
                    rewritten += await update.ExecuteNonQueryAsync(cancellationToken);
                }

                await transaction.CommitAsync(cancellationToken);
            }
            catch (PostgresException) when (connection.State == System.Data.ConnectionState.Open)
            {
                /* Rolled back with its transaction; the connection is still usable, so the slice goes on. */
                failed++;
            }
        }

        var next = page.Count < MaxReportRowsPerPass
            ? (ReportCursor?)null
            : new ReportCursor(page[^1].CollectionTime, page[^1].ServerId, page[^1].Hash);
        return (next, page.Count, rewritten, failed);
    }

    /// <summary>What an alert incident's key names in <c>pg_deadlocks</c>.</summary>
    public enum ReportState
    {
        /// <summary>No stored report: retention dropped it, or its server's rows are gone.</summary>
        Missing,

        /// <summary>A report in #4005's form, so the incident was built from normalized text.</summary>
        Current,

        /// <summary>A report stored raw, which the incident was built from.</summary>
        Raw,
    }

    /// <summary>A key's report and, for a raw one, the incident the live alert builds from it once it is
    /// rewritten.</summary>
    public readonly record struct ResolvedReport(ReportState State, AlertIncident? Incident);

    /// <summary>The live alert's text for a report with no victim statement
    /// (<see cref="DarlingWorker.BuildPgDeadlockIncident"/>), which carries no SQL.</summary>
    private static readonly Regex s_noStatementObject = new(
        @"^victim pid -?[0-9]+, -?[0-9]+ participant\(s\)$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A report's key as a PostgreSQL deadlock alert carries it: <see cref="PgDeadlockLogParser.HashOf"/>'s
    /// shape, 32 upper-case hex characters, which the SQL Server alert's fingerprint (64, lower case) never
    /// has.</summary>
    private static readonly Regex s_reportHash = new(
        "^[0-9A-F]{32}$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>Whether an alert incident's key has a PostgreSQL deadlock report's hash shape.</summary>
    public static bool IsReportHash(string? key) => key is not null && s_reportHash.IsMatch(key);

    /// <summary>
    /// The incident the live alert builds for a raw report once it is rewritten
    /// (<see cref="DarlingWorker.BuildPgDeadlockIncident"/> over the row the read then returns): its key is the
    /// report's new identity and its object the normalized victim statement.
    /// </summary>
    public static AlertIncident RemaskedIncident(
        DateTime? occurredAt, int? victimPid, int? participantCount, string? victimStatement, string graphText)
    {
        var (victim, _, identity) = RemaskReport(occurredAt, victimStatement, graphText);
        return DarlingWorker.BuildPgDeadlockIncident(new DarlingPgDeadlockReader.PgDeadlockRow(
            occurredAt ?? default, victimPid ?? 0, participantCount ?? 0, identity, null, null,
            PgDeadlockLogParser.NormalizeStatement(victim), 0));
    }

    /// <summary>
    /// One stored deadlock alert brought to #4005's rules, or null when it already is (#4012). An incident whose key
    /// names a raw report takes the incident the live alert builds for that report once rewritten, key and object
    /// both. One whose report is gone keeps its key (nothing is left to test that hash against) and has its
    /// statement normalized as it stands; one whose report is already current, or whose key is a
    /// <see cref="PgDeadlockLogParser.ReportIdentity"/> (fired since #4005 from a normalizing read), was built from
    /// normalized text and is left as it is. A per-event row's detail item and its <c>detail_text</c> carry the
    /// incident's key and objects as rendered (<see cref="AlertIncidentRenderer.BuildItem"/>), and are rewritten
    /// where they carry them; every other member round-trips through the persisted projection unchanged. Pure.
    /// </summary>
    public static (string ContextJson, string? DetailText)? RemaskAlert(
        string contextJson, string? detailText, Func<string, ResolvedReport> resolve)
    {
        ArgumentNullException.ThrowIfNull(resolve);

        AlertContextDto? dto;
        try
        {
            dto = JsonSerializer.Deserialize<AlertContextDto>(contextJson);
        }
        catch (JsonException)
        {
            return null;
        }

        if (dto?.Incidents is not { Count: > 0 } incidents)
        {
            return null;
        }

        var replacements = new List<(string OldKey, string NewKey, string OldObjects, string NewObjects)>();
        var rewritten = new List<AlertIncidentDto>(incidents.Count);
        foreach (var incident in incidents)
        {
            if (incident is null || !IsReportHash(incident.DedupKey))
            {
                rewritten.Add(incident!);
                continue;
            }

            var objects = incident.InvolvedObjects ?? new List<string>();
            var resolved = resolve(incident.DedupKey);
            List<string> newObjects;
            var newKey = incident.DedupKey;
            if (resolved is { State: ReportState.Raw, Incident: { } built })
            {
                newKey = built.DedupKey;
                newObjects = built.InvolvedObjects.ToList();
            }
            else if (resolved.State == ReportState.Missing)
            {
                newObjects = objects.ConvertAll(o =>
                    string.IsNullOrEmpty(o) || s_noStatementObject.IsMatch(o) ? o : PgDeadlockLogParser.NormalizeStatement(o)!);
            }
            else
            {
                rewritten.Add(incident);
                continue;
            }

            if (newKey == incident.DedupKey && newObjects.SequenceEqual(objects, StringComparer.Ordinal))
            {
                rewritten.Add(incident);
                continue;
            }

            replacements.Add((incident.DedupKey, newKey, Rendered(objects), Rendered(newObjects)));
            rewritten.Add(incident with { DedupKey = newKey, InvolvedObjects = newObjects });
        }

        if (replacements.Count == 0)
        {
            return null;
        }

        /* A per-event row's detail item renders the incident's key and objects as facts
           (AlertIncidentRenderer.BuildItem); a summary row has no detail items. */
        var details = dto.Details?.ConvertAll(item => item with
        {
            Fields = item.Fields?.ConvertAll(field => field with { Value = Replace(field.Label, field.Value, replacements) }) ?? item.Fields!,
        }) ?? dto.Details!;

        var json = JsonSerializer.Serialize(dto with { Details = details, Incidents = rewritten });
        var detail = detailText;
        if (detail is not null)
        {
            foreach (var (oldKey, newKey, oldObjects, newObjects) in replacements)
            {
                if (oldObjects != newObjects)
                {
                    detail = detail.Replace(oldObjects, newObjects, StringComparison.Ordinal);
                }

                if (oldKey != newKey)
                {
                    detail = detail.Replace(oldKey, newKey, StringComparison.Ordinal);
                }
            }
        }

        return (json, detail);

        static string Rendered(List<string> objects) =>
            objects.Count > 0 ? string.Join(", ", objects) : "(unresolved)";

        static string Replace(string label, string value, List<(string OldKey, string NewKey, string OldObjects, string NewObjects)> pairs)
        {
            foreach (var (oldKey, newKey, oldObjects, newObjects) in pairs)
            {
                if (label == AlertIncidentRenderer.DedupKeyFactName && value == oldKey)
                {
                    return newKey;
                }

                if (label == "Involved Objects" && value == oldObjects)
                {
                    return newObjects;
                }
            }

            return value;
        }
    }

    /// <summary>
    /// Examines one slice of deadlock alert rows and rewrites the ones still carrying a pre-#4005 incident (#4012).
    /// Returns the cursor to resume from, null once the log's end is reached, and how many rows changed between the
    /// read and the write (a dismissal moves a row), which were left as they were and need the log read again.
    /// </summary>
    public static async Task<(string? NextCursor, int Examined, int Rewritten, int Raced)> RemaskStoredAlertsAsync(
        NpgsqlConnection connection, string? afterCursor, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var page = new List<(string Ctid, int ServerId, string? Detail, string Context)>();
        var reports = new Dictionary<(int, string), ResolvedReport>();
        await using (var transaction = await BeginBoundedAsync(connection, cancellationToken))
        {
            await using (var select = new NpgsqlCommand(AlertPageSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
            {
                select.Parameters.Add(new NpgsqlParameter { Value = (object?)afterCursor ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                select.Parameters.AddWithValue(MaxAlertRowsPerPass);
                await using var reader = await select.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    page.Add((
                        reader.GetString(0),
                        reader.GetInt32(1),
                        reader.IsDBNull(2) ? null : reader.GetString(2),
                        reader.GetString(3)));
                }
            }

            /* Every report-shaped key the page's incidents carry, looked up in one read. */
            var keys = new HashSet<(int ServerId, string Key)>();
            foreach (var row in page)
            {
                foreach (var key in ReportKeysOf(row.Context))
                {
                    keys.Add((row.ServerId, key));
                }
            }

            if (keys.Count > 0)
            {
                await using var lookup = new NpgsqlCommand(AlertReportLookupSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds };
                lookup.Parameters.Add(new NpgsqlParameter { Value = keys.Select(k => k.ServerId).ToArray(), NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer });
                lookup.Parameters.Add(new NpgsqlParameter { Value = keys.Select(k => k.Key).ToArray(), NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
                await using var reader = await lookup.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var raw = reader.GetBoolean(7);
                    reports[(reader.GetInt32(0), reader.GetString(1))] = raw && !reader.IsDBNull(6)
                        ? new ResolvedReport(ReportState.Raw, RemaskedIncident(
                            reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                            reader.IsDBNull(3) ? null : reader.GetInt32(3),
                            reader.IsDBNull(4) ? null : reader.GetInt32(4),
                            reader.IsDBNull(5) ? null : reader.GetString(5),
                            reader.GetString(6)))
                        : new ResolvedReport(ReportState.Current, null);
                }
            }

            await transaction.CommitAsync(cancellationToken);
        }

        int rewritten = 0, raced = 0;
        foreach (var row in page)
        {
            var remasked = RemaskAlert(row.Context, row.Detail, key =>
                reports.TryGetValue((row.ServerId, key), out var report) ? report : new ResolvedReport(ReportState.Missing, null));
            if (remasked is not { } alert)
            {
                continue;
            }

            await using var transaction = await BeginBoundedAsync(connection, cancellationToken);
            await using (var update = new NpgsqlCommand(AlertUpdateSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
            {
                update.Parameters.Add(new NpgsqlParameter { Value = alert.ContextJson, NpgsqlDbType = NpgsqlDbType.Text });
                update.Parameters.Add(new NpgsqlParameter { Value = (object?)alert.DetailText ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                update.Parameters.Add(new NpgsqlParameter { Value = row.Ctid, NpgsqlDbType = NpgsqlDbType.Text });
                update.Parameters.Add(new NpgsqlParameter { Value = row.Context, NpgsqlDbType = NpgsqlDbType.Text });
                update.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Detail ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                var written = await update.ExecuteNonQueryAsync(cancellationToken);
                rewritten += written;
                raced += written == 0 ? 1 : 0;
            }

            await transaction.CommitAsync(cancellationToken);
        }

        var next = page.Count < MaxAlertRowsPerPass ? null : page[^1].Ctid;
        return (next, page.Count, rewritten, raced);
    }

    /// <summary>The report-shaped keys a stored alert context's incidents carry; none for a context that does not
    /// parse, which <see cref="RemaskAlert"/> leaves as it is too.</summary>
    private static List<string> ReportKeysOf(string contextJson)
    {
        try
        {
            return (JsonSerializer.Deserialize<AlertContextDto>(contextJson)?.Incidents ?? new List<AlertIncidentDto>())
                .Where(incident => IsReportHash(incident?.DedupKey))
                .Select(incident => incident.DedupKey)
                .ToList();
        }
        catch (JsonException)
        {
            return new List<string>();
        }
    }

    /// <summary>A transaction whose statements run under <see cref="StatementTimeoutSeconds"/>, set LOCAL so
    /// nothing outlives it on the pooled connection.</summary>
    private static async Task<NpgsqlTransaction> BeginBoundedAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            await using var timeout = new NpgsqlCommand(
                $"SET LOCAL statement_timeout = '{StatementTimeoutSeconds}s'", connection, transaction)
            { CommandTimeout = CommandBackstopSeconds };
            await timeout.ExecuteNonQueryAsync(cancellationToken);
            return transaction;
        }
        catch
        {
            await transaction.DisposeAsync();
            throw;
        }
    }
}
