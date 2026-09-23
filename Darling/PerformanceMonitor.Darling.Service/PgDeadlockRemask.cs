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
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Brings the PostgreSQL deadlock reports, the deadlock alerts and the analysis findings stored before #4005 to its
/// rules (#4012), page after page within each hourly store-maintenance tick's budget (<see cref="RunAsync"/>), the
/// pattern #3915 set for the store log (<see cref="StoreLogSweep.RemaskStoredEventsAsync"/>).
///
/// <para><b>What this does not rewrite.</b> <c>pg_log_events</c> holds a deadlock's ERROR line too, and a row stored
/// before #4020 keeps its unkeyed <c>raw_line_hash</c> over the raw DETAIL. It is left as it is on purpose: #4004's
/// ruling keeps an old row's hash until retention drops it (30 days), because re-keying it would split the entry's
/// dedupe identity from the sightings stored since. So after this pass the deadlock reports, alerts and findings hold
/// no raw query or raw-graph hash, and the <c>pg_log_events</c> rows from before #4020 age out within 30 days.</para>
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
/// rewritten its raw hash is gone, so the report pass starts only after the alert pass is done (or waiting out a
/// failure), and an alert it missed takes a keyed key rather than keeping the raw one (<see cref="RemaskAlert"/>).</para>
///
/// <para><b>Stored analysis findings</b> are the third copy: a finding persists its deadlock drill-down and prose
/// for 30 days, and one written before #4005 kept its exemplars' SQL raw. They are rewritten last, by the read's own
/// normalization (<see cref="RemaskFinding"/>), so their read cannot hold the reports back (#4012's review).</para>
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

    /// <summary>How many whole walks of one table a process makes that rewrite nothing but leave rows raw: an alert
    /// row that changed between its read and its write, or a report whose rewrite failed. Bounded, so a report
    /// that fails every time costs a few re-reads rather than a walk an hour for the life of the process; it is
    /// left to the next process's pass, and every read normalizes it meanwhile. A walk that rewrote rows does not
    /// count: it is progress, and the walk after it is the one that proves the table clean.</summary>
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
    /// no raw sighting of this report is older), <c>$4</c>/<c>$5</c> what the page read, <c>$6</c>..<c>$8</c> the
    /// normalized values. The raw test is part of the statement, so a row already rewritten, or stored since, is
    /// never touched; and it is what proves the row still holds the graph the page read (the hash is over that
    /// graph), so no raw text goes back to the store as a parameter, where <c>log_min_duration_statement</c> would
    /// write it into the store's own log (#4012's review). The victim statement is the graph's own victim query,
    /// so the same graph cannot carry another.
    /// </summary>
    public static readonly string ReportUpdateSql = $@"
UPDATE pg_deadlocks AS d
SET victim_statement = $6,
    graph_text = $7,
    deadlock_hash = $8
WHERE d.server_id = $1
AND   d.deadlock_hash = $2
AND   d.collection_time >= $3
AND   d.occurred_at IS NOT DISTINCT FROM $4
AND   d.victim_pid IS NOT DISTINCT FROM $5
AND   {PgDeadlockLogParser.RawGraphHashSql("d.deadlock_hash", "d.graph_text")}";

    /// <summary>One page of deadlock alert rows in physical order after <c>$1</c> (null for the start). The
    /// history table is a plain table with no key, so its physical order is the cursor, as the store-log slice's is.
    /// An UPDATE (a dismissal) can move a row the walk has not reached behind it; that is why a stage is done only
    /// after a whole walk that rewrote nothing (<see cref="RunAsync"/>), which a row moved that way cannot survive.
    /// <c>xmin</c> is what the write checks the row by.</summary>
    public const string AlertPageSql = @"
SELECT
    a.ctid::text,
    a.server_id,
    a.detail_text,
    a.context_json,
    a.xmin
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

    /// <summary>Writes one alert row's normalized context and detail back, only while the row is still the version the
    /// page read (<c>ctid</c> and <c>xmin</c>), so a row changed in between is left for the next walk. By its version,
    /// not by comparing its text: the raw text sent back as a parameter would land in the store's own log under
    /// <c>log_min_duration_statement</c> (#4012's review).</summary>
    public const string AlertUpdateSql = @"
UPDATE config_alert_log
SET context_json = $1,
    detail_text = $2
WHERE ctid = $3::tid
AND   xmin = $4::xid";

    /// <summary>
    /// A stored report in #4005's form: the graph and victim statement every read shows for it, and the identity
    /// the collector gives a report since, over its timestamp and that graph. Pure; the same text in gives the same
    /// row out, and a row already in this form is left as it is by the slice's raw test, never by this.
    /// </summary>
    public static (string? VictimStatement, string GraphText, string DeadlockHash) RemaskReport(
        DateTime? occurredAt, string? victimStatement, string graphText)
    {
        ArgumentNullException.ThrowIfNull(graphText);

        /* The collector's own trim (PgDeadlockLogParser.FromEntry), so the identity is over the text it hashes.
           Where the stored text cannot say what the live collector read, this cannot match it (#4012's review), and
           the report's old and new sightings then keep two identities rather than merge:
           - Tabs. The collector before #4005 took every tab out of the DETAIL (93787e8c~1's FromEntry), the query's
             own included; since #4005 the assembler removes only the one PostgreSQL adds to each continuation line.
             Where a query's tabs stood is not in the stored text, so `LIMIT<tab>50` stays `LIMIT50`, one identifier
             that keeps its digits (every read already shows it so).
           - Whether the report was whole. The live collector passes DetailComplete (a HINT followed the DETAIL);
             the old collector stored a report cut at a chunk boundary as it arrived and kept no such proof. So this
             reads the fail-closed way, complete = false: after a query that does not read to its end, the ones that
             follow are withheld here where the live read keeps them. Claiming complete for a report that was cut is
             #3996's review's out-of-step read, a literal kept as a word, so it is not claimed. */
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
    /// Returns the cursor to resume from, null once the table's end is reached. <paramref name="rowDone"/> hears each
    /// row as it is finished, in the page's order, so a caller canceled mid-page resumes after the last one rather
    /// than reading the page again (#4012's review).
    /// </summary>
    public static async Task<(ReportCursor? Next, int Examined, int Rewritten, int Failed)> RemaskStoredReportsAsync(
        NpgsqlConnection connection, ReportCursor? after, Action<ReportCursor>? rowDone,
        CancellationToken cancellationToken = default)
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
                rowDone?.Invoke(new ReportCursor(row.CollectionTime, row.ServerId, row.Hash));
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

            rowDone?.Invoke(new ReportCursor(row.CollectionTime, row.ServerId, row.Hash));
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
    /// both. One whose report cannot be found has its statement normalized as it stands, and its key keyed under the
    /// store's secret (<see cref="PgLogHashKey.DeadlockAlertKey"/>, #4012's review): such a key may be the hash of a
    /// raw graph, a test for the literals (#4004), and once the report pass has rewritten the report, or retention
    /// dropped it, nothing is left to rebuild the normalized identity from. Keyed, two alerts that shared a key still
    /// do, and the value no longer has a report hash's shape, so it is not taken for one again. One whose report is
    /// already current, or whose key is a <see cref="PgDeadlockLogParser.ReportIdentity"/> (fired since #4005 from a
    /// normalizing read), was built from normalized text and is left as it is. A per-event row's detail item and its
    /// <c>detail_text</c> carry the incident's key and objects as rendered (<see cref="AlertIncidentRenderer.BuildItem"/>),
    /// and are rewritten where they carry them; every other member round-trips through the persisted projection
    /// unchanged. Pure.
    /// </summary>
    /// <param name="keyUnresolvedWith">The store's key, to key an unresolvable key with; null leaves such a key as it
    /// is for now. Null while any report may still be raw: an alert already rewritten to a raw report's new identity
    /// finds no report by it until the report stage rewrites that report, and keying it then would cut it from the
    /// report it names (<see cref="RunAsync"/> keys them only after the report stage is done).</param>
    public static (string ContextJson, string? DetailText)? RemaskAlert(
        string contextJson, string? detailText, Func<string, ResolvedReport> resolve, PgLogHashKey? keyUnresolvedWith)
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
                newKey = keyUnresolvedWith?.DeadlockAlertKey(incident.DedupKey) ?? incident.DedupKey;
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
        NpgsqlConnection connection, string? afterCursor, PgLogHashKey? keyUnresolvedWith, Action<string>? rowDone,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var page = new List<(string Ctid, int ServerId, string? Detail, string Context, uint Xmin)>();
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
                        reader.GetString(3),
                        reader.GetFieldValue<uint>(4)));
                }
            }

            /* Every report-shaped key the page's incidents carry, looked up in one read. */
            var keys = new HashSet<(int ServerId, string Key)>();
            foreach (var row in page)
            {
                foreach (var reportKey in ReportKeysOf(row.Context))
                {
                    keys.Add((row.ServerId, reportKey));
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
            var remasked = RemaskAlert(row.Context, row.Detail, reportKey =>
                reports.TryGetValue((row.ServerId, reportKey), out var report) ? report : new ResolvedReport(ReportState.Missing, null), keyUnresolvedWith);
            if (remasked is { } alert)
            {
                await using var transaction = await BeginBoundedAsync(connection, cancellationToken);
                await using (var update = new NpgsqlCommand(AlertUpdateSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
                {
                    update.Parameters.Add(new NpgsqlParameter { Value = alert.ContextJson, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = (object?)alert.DetailText ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = row.Ctid, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = row.Xmin, NpgsqlDbType = NpgsqlDbType.Xid });
                    var written = await update.ExecuteNonQueryAsync(cancellationToken);
                    rewritten += written;
                    raced += written == 0 ? 1 : 0;
                }

                await transaction.CommitAsync(cancellationToken);
            }

            rowDone?.Invoke(row.Ctid);
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

    /// <summary>How many stored analysis findings one slice examines.</summary>
    public const int MaxFindingRowsPerPass = 500;

    /// <summary>One page of stored analysis findings that carry a deadlock exemplar section, in physical order after
    /// <c>$1</c> (null for the start). Only a finding rooted where the section is attached (the deadlock rate fact or
    /// its anomaly, <see cref="PgTargetDrillDownCollector"/>) is looked into, by its <c>root_fact_key</c>, a plain
    /// column, so every other finding's drill-down is never detoasted (#4012's review measured the unfiltered read
    /// as a Seq Scan and Sort over every drill-down). A section this build wrote, or one the pass already rewrote,
    /// says <c>sql_normalized</c> and is not read.</summary>
    public const string FindingPageSql = @"
SELECT
    f.ctid::text,
    f.drill_down_json,
    f.story_text,
    f.xmin
FROM analysis_findings AS f
WHERE ($1::tid IS NULL OR f.ctid > $1::tid)
AND   f.root_fact_key IN ('" + PgTargetFactKeys.DeadlockRate + "', '" + PgTargetFactKeys.AnomalyDeadlockRate + @"')
AND   strpos(f.drill_down_json, '""" + PgTargetDrillDownCollector.DeadlockExemplarsSection + @"""') > 0
AND   strpos(f.drill_down_json, '""sql_normalized"":true') = 0
ORDER BY f.ctid
LIMIT $2";

    /// <summary>Writes one finding's normalized drill-down and prose back, only while the row is still the version
    /// the page read (<c>ctid</c> and <c>xmin</c>), never by sending its raw text back (#4012's review).</summary>
    public const string FindingUpdateSql = @"
UPDATE analysis_findings
SET drill_down_json = $1,
    story_text = $2
WHERE ctid = $3::tid
AND   xmin = $4::xid";

    /// <summary>
    /// A stored analysis finding's deadlock exemplars brought to #4005's rules and stamped so, or null when it has
    /// none to rewrite (#4012). Findings persist their drill-down and prose for 30 days, and one written before
    /// #4005 kept each exemplar's victim statement, its fingerprint and graph raw, with a hash that may be over the
    /// raw graph. This is the read's own normalization
    /// (<see cref="PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars"/>), so the rewritten finding reads
    /// exactly as it read before, and the stamp is what the read and a later pass leave it as it is by. Pure.
    /// </summary>
    public static (string DrillDownJson, string StoryText)? RemaskFinding(string drillDownJson, string storyText)
    {
        var finding = new AnalysisFinding { DrillDown = DrillDownSerializer.Deserialize(drillDownJson), StoryText = storyText };
        return PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars(finding, markNormalized: true)
            && DrillDownSerializer.Serialize(finding.DrillDown) is { } json
                ? (json, finding.StoryText)
                : null;
    }

    /// <summary>
    /// Examines one slice of stored analysis findings and rewrites the ones whose deadlock exemplars predate #4005
    /// (#4012). Returns the cursor to resume from, null once the table's end is reached, and how many rows changed
    /// between the read and the write, which were left as they were and need the table read again.
    /// </summary>
    public static async Task<(string? NextCursor, int Examined, int Rewritten, int Raced)> RemaskStoredFindingsAsync(
        NpgsqlConnection connection, string? afterCursor, Action<string>? rowDone,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var page = new List<(string Ctid, string DrillDown, string Story, uint Xmin)>();
        await using (var transaction = await BeginBoundedAsync(connection, cancellationToken))
        {
            await using (var select = new NpgsqlCommand(FindingPageSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
            {
                select.Parameters.Add(new NpgsqlParameter { Value = (object?)afterCursor ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                select.Parameters.AddWithValue(MaxFindingRowsPerPass);
                await using var reader = await select.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    page.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetFieldValue<uint>(3)));
                }
            }

            await transaction.CommitAsync(cancellationToken);
        }

        int rewritten = 0, raced = 0;
        foreach (var row in page)
        {
            if (RemaskFinding(row.DrillDown, row.Story) is { } finding)
            {
                await using var transaction = await BeginBoundedAsync(connection, cancellationToken);
                await using (var update = new NpgsqlCommand(FindingUpdateSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
                {
                    update.Parameters.Add(new NpgsqlParameter { Value = finding.DrillDownJson, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = finding.StoryText, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = row.Ctid, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = row.Xmin, NpgsqlDbType = NpgsqlDbType.Xid });
                    var written = await update.ExecuteNonQueryAsync(cancellationToken);
                    rewritten += written;
                    raced += written == 0 ? 1 : 0;
                }

                await transaction.CommitAsync(cancellationToken);
            }

            rowDone?.Invoke(row.Ctid);
        }

        var next = page.Count < MaxFindingRowsPerPass ? null : page[^1].Ctid;
        return (next, page.Count, rewritten, raced);
    }

    /// <summary>How many consecutive failures of one stage end it for this process (#4012's review). Each failure is
    /// logged, and the stage waits twice as many ticks each time (none, 1, 3) before it tries again; meanwhile, and
    /// once it gives up, the stages after it run, so a stage that errors every time (a <c>57014</c> on a very large
    /// table) neither retries forever nor holds the others back. The next process tries it again.</summary>
    public const int MaxConsecutiveStageFailures = 4;

    /// <summary>One stage's progress through its table, held in memory for the life of the process like #3915's
    /// store-log cursor (no store migration, #4012's review ruling): a restart walks the table again from its start,
    /// a no-op re-read of rows already rewritten, bounded by the tick's budget.</summary>
    public sealed class StageProgress
    {
        public StageProgress(string name) => Name = name;

        /// <summary>What the stage's log lines call a row: alert, report, finding.</summary>
        public string Name { get; }

        /// <summary>True once a whole walk of the table rewrote nothing and left nothing, or the stage gave up for
        /// this process.</summary>
        public bool Done { get; set; }

        /// <summary>True when the stage ended for this process without a clean walk (failures, or walks that only
        /// left rows): the next process tries it again.</summary>
        public bool GaveUp { get; set; }

        /// <summary>Whole walks finished this process.</summary>
        public int Walks { get; set; }

        /// <summary>Rows the current walk rewrote.</summary>
        public int WalkRewritten { get; set; }

        /// <summary>Rows the current walk left as they were: changed under the write, or a rewrite that failed.</summary>
        public int WalkLeft { get; set; }

        /// <summary>Walks that rewrote nothing but left rows, which count toward <see cref="MaxRescansPerProcess"/>.</summary>
        public int NoProgressWalks { get; set; }

        /// <summary>Failures in a row, reset by a page that succeeds.</summary>
        public int ConsecutiveFailures { get; set; }

        /// <summary>Ticks still to skip after a failure.</summary>
        public int SkipTicks { get; set; }
    }

    /// <summary>The whole re-mask's state for one process: a stage and a cursor per table.</summary>
    public sealed class RemaskProgress
    {
        public StageProgress Alerts { get; } = new("alert");

        public StageProgress Reports { get; } = new("report");

        /// <summary>The alerts walked again once every report is rewritten, to key the ones whose report cannot be
        /// found (<see cref="RemaskAlert"/>).</summary>
        public StageProgress AlertKeys { get; } = new("alert key");

        public StageProgress Findings { get; } = new("finding");

        /// <summary>The alert walk's cursor: the last row it finished.</summary>
        public string? AlertCursor { get; set; }

        /// <summary>The alert-key walk's cursor: the last row it finished.</summary>
        public string? AlertKeyCursor { get; set; }

        /// <summary>The report walk's cursor: the last row it finished.</summary>
        public ReportCursor? ReportCursor { get; set; }

        /// <summary>The finding walk's cursor: the last row it finished.</summary>
        public string? FindingCursor { get; set; }

        /// <summary>True once every stage is done for this process.</summary>
        public bool Done => Alerts.Done && Reports.Done && AlertKeys.Done && Findings.Done;
    }

    /// <summary>
    /// One tick of the re-mask (#4012): page after page until <paramref name="cancellationToken"/> ends the tick
    /// (the caller's <see cref="SliceBudget"/>) or every stage is done. #4012's review measured a page of 2000
    /// compressed rows at 62-133 ms and a report's rewrite at 18 ms, so a page an hour took about 11 days for the
    /// reports alone. A cancel keeps each walk's cursor at the last row it finished, so the next tick resumes there.
    ///
    /// <para><b>Alerts, then reports, then findings.</b> An alert is found through its report's raw hash, which the
    /// report stage replaces, so the alerts go first; findings last, so a slow findings read cannot hold the reports
    /// back. A stage starts once the one before it is done, or is waiting out a failure: a failing stage must not
    /// starve the rest (<see cref="MaxConsecutiveStageFailures"/>). Once every report is rewritten, the alerts are
    /// walked once more (<see cref="RemaskProgress.AlertKeys"/>): an alert the first walk missed (it moved behind the
    /// cursor, or that stage gave up) finds no report by its raw hash and takes a keyed key
    /// (<see cref="RemaskAlert"/>), never keeps the raw one. Not before: while a report is raw, an alert already
    /// carrying its new identity finds no report by it either, and keying it would cut it from its report.</para>
    ///
    /// <para><b>A stage is done only after a whole walk that rewrote nothing</b> (#4012's review). A walk that
    /// rewrote rows is followed by another, which catches a row an UPDATE moved behind the cursor; one that rewrote
    /// nothing but left rows counts toward <see cref="MaxRescansPerProcess"/>.</para>
    /// </summary>
    public static async Task RunAsync(
        NpgsqlConnection connection, RemaskProgress progress, PgLogHashKey key, ILogger logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(progress);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(logger);

        var stages = new[] { progress.Alerts, progress.Reports, progress.AlertKeys, progress.Findings };
        foreach (var stage in stages)
        {
            /* The alert keys wait for every report, not for a report stage merely waiting out a failure: while a
               report is raw, an alert already carrying its new identity finds nothing by it. */
            if (stage.Done || (ReferenceEquals(stage, progress.AlertKeys) && !progress.Reports.Done))
            {
                continue;
            }

            if (stage.SkipTicks > 0)
            {
                /* Waiting out a failure; the stages after it run meanwhile. */
                stage.SkipTicks--;
                continue;
            }

            int examined = 0, rewritten = 0, left = 0;
            try
            {
                while (!stage.Done)
                {
                    if (connection.State != System.Data.ConnectionState.Open)
                    {
                        /* A Broken connection must be closed before it can open again. */
                        await connection.CloseAsync();
                        await connection.OpenAsync(cancellationToken);
                    }

                    (bool End, int Examined, int Rewritten, int Left) page;
                    if (ReferenceEquals(stage, progress.Alerts))
                    {
                        var (next, e, w, r) = await RemaskStoredAlertsAsync(
                            connection, progress.AlertCursor, null, row => progress.AlertCursor = row, cancellationToken);
                        progress.AlertCursor = next;
                        page = (next is null, e, w, r);
                    }
                    else if (ReferenceEquals(stage, progress.AlertKeys))
                    {
                        var (next, e, w, r) = await RemaskStoredAlertsAsync(
                            connection, progress.AlertKeyCursor, key, row => progress.AlertKeyCursor = row, cancellationToken);
                        progress.AlertKeyCursor = next;
                        page = (next is null, e, w, r);
                    }
                    else if (ReferenceEquals(stage, progress.Reports))
                    {
                        var (next, e, w, f) = await RemaskStoredReportsAsync(
                            connection, progress.ReportCursor, row => progress.ReportCursor = row, cancellationToken);
                        progress.ReportCursor = next;
                        page = (next is null, e, w, f);
                    }
                    else
                    {
                        var (next, e, w, r) = await RemaskStoredFindingsAsync(
                            connection, progress.FindingCursor, row => progress.FindingCursor = row, cancellationToken);
                        progress.FindingCursor = next;
                        page = (next is null, e, w, r);
                    }

                    stage.ConsecutiveFailures = 0;
                    examined += page.Examined;
                    rewritten += page.Rewritten;
                    left += page.Left;
                    stage.WalkRewritten += page.Rewritten;
                    stage.WalkLeft += page.Left;
                    if (page.End)
                    {
                        EndWalk(stage, logger);
                    }
                }
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                /* The tick's budget (or shutdown) ended it: each walk's cursor stands at the last row it finished. */
                LogTick(stage, examined, rewritten, left, logger, finished: false);
                return;
            }
            catch (Exception ex)
            {
                stage.ConsecutiveFailures++;
                if (stage.ConsecutiveFailures >= MaxConsecutiveStageFailures)
                {
                    stage.Done = true;
                    stage.GaveUp = true;
                    logger.LogWarning(
                        "PostgreSQL deadlocks: re-masking stored {Stage} rows failed {Failures} times in a row, and stops for this process; every read still normalizes them, and the next process tries again: {Message}",
                        stage.Name, stage.ConsecutiveFailures, ex.Message);
                }
                else
                {
                    stage.SkipTicks = (1 << (stage.ConsecutiveFailures - 1)) - 1;
                    logger.LogWarning(
                        "PostgreSQL deadlocks: re-masking stored {Stage} rows failed ({Failures} of {Max} in a row), and is retried after {Skip} more hourly pass(es); the other stages go on: {Message}",
                        stage.Name, stage.ConsecutiveFailures, MaxConsecutiveStageFailures, stage.SkipTicks, ex.Message);
                }

                continue;
            }

            LogTick(stage, examined, rewritten, left, logger, finished: true);
        }
    }

    /// <summary>A walk reached its table's end: done when it rewrote and left nothing, otherwise walked again.</summary>
    private static void EndWalk(StageProgress stage, ILogger logger)
    {
        stage.Walks++;
        if (stage.WalkRewritten == 0 && stage.WalkLeft == 0)
        {
            stage.Done = true;
        }
        else if (stage.WalkRewritten == 0 && ++stage.NoProgressWalks > MaxRescansPerProcess)
        {
            stage.Done = true;
            stage.GaveUp = true;
            logger.LogWarning(
                "PostgreSQL deadlocks: {Left} stored {Stage} row(s) could not be re-masked in {Walks} walks, and are left for the next process; every read still normalizes them.",
                stage.WalkLeft, stage.Name, stage.Walks);
        }

        stage.WalkRewritten = 0;
        stage.WalkLeft = 0;
    }

    private static void LogTick(StageProgress stage, int examined, int rewritten, int left, ILogger logger, bool finished)
    {
        if (rewritten == 0 && left == 0)
        {
            return;
        }

        logger.LogInformation(
            "PostgreSQL deadlocks: re-masked {Rewritten} of {Examined} stored {Stage} row(s) written before #4005, so their SQL literals and raw report hashes no longer sit in the store; {Left} were left for another walk{Remaining}.",
            rewritten, examined, stage.Name, left, finished && stage.Done ? "" : "; the rest follow on the next hourly passes");
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
