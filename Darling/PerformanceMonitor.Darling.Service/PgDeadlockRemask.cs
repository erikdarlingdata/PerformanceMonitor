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
/// rewritten its raw hash is gone, so the report pass starts only after the alert pass is done (or has given up,
/// #4036's round-2 review), and an alert it missed takes a keyed key rather than keeping the raw one
/// (<see cref="RemaskAlert"/>).</para>
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

    /// <summary>
    /// Where a report slice resumes: the last collection batch it finished, one server's rows of one
    /// <c>collection_time</c>. No hash (#4035): a raw report's hash is a test for its literals (#4004), and a cursor
    /// sent back as a parameter lands in the store's own log under <c>log_min_duration_statement</c>.
    /// </summary>
    public readonly record struct ReportCursor(DateTime CollectionTime, int ServerId);

    /// <summary>
    /// Where an alert-log walk resumes (#5625): a position in <c>idx_config_alert_log_time (server_id, metric_name,
    /// alert_time)</c> order, which is the order both alert stages walk in. <see cref="AlertTime"/> is the last
    /// instant finished under <see cref="ServerId"/> and <see cref="MetricName"/> (rows tied at it are finished
    /// too); null means that server and metric are still to be read from their first row. The log is not walked in
    /// <c>alert_time</c> order across servers because no index serves that: the walk would read every row of every
    /// server on each page. Nothing depends on the global order: no UPDATE changes <c>server_id</c>,
    /// <c>metric_name</c> or <c>alert_time</c>, so a rewritten or dismissed row keeps its place in this index order, and
    /// an alert written after this build is already in its final form, wherever it lands.
    /// </summary>
    public readonly record struct AlertLogCursor(int ServerId, string MetricName, DateTime? AlertTime);

    /// <summary>The next server that has any row in <c>config_alert_log</c> at or after <c>$1</c> (a bigint, so
    /// <c>server_id + 1</c> cannot overflow): one descent of <c>idx_config_alert_log_time</c> on its leading column,
    /// not the servers registry, because the rows of a server that was removed stay in the log until their retention
    /// ends (#5625).</summary>
    public const string AlertServerProbeSql = @"
SELECT a.server_id
FROM config_alert_log AS a
WHERE a.server_id >= $1
ORDER BY a.server_id
LIMIT 1";

    /// <summary>The first server and metric of the log, in index order (#5625).</summary>
    public const string AlertPairFirstSql = @"
SELECT a.server_id, a.metric_name, a.metric_name LIKE 'Analysis: %' AS is_analysis
FROM config_alert_log AS a
ORDER BY a.server_id, a.metric_name
LIMIT 1";

    /// <summary>The next server and metric after <c>($1, $2)</c> in index order, with whether its name is an
    /// <c>Analysis: </c> one (#5625). A row comparison on the index's first two columns, so the descent lands past every
    /// row of the pair just finished: a loose scan that needs no range on <c>metric_name</c> (which could not be told
    /// from the <c>LIKE</c> under every collation a bring-your-own store can have) and no skip scan.</summary>
    public const string AlertPairNextSql = @"
SELECT a.server_id, a.metric_name, a.metric_name LIKE 'Analysis: %' AS is_analysis
FROM config_alert_log AS a
WHERE (a.server_id, a.metric_name) > ($1, $2)
ORDER BY a.server_id, a.metric_name
LIMIT 1";

    /// <summary>How many probe statements one alert page may run besides the row reads that fill it (#5625): every
    /// step to the next server (deadlock stage) or to the next server and metric (finding-alert stage) is one,
    /// whatever the metric's name and whether or not rows follow it. A page that meets no row to examine still ends,
    /// with a cursor on the place it reached, so a log whose remaining metrics hold nothing to look at costs a few
    /// pages rather than one that runs a statement per metric.</summary>
    public const int MaxAlertStepsPerPage = 100;

    /// <summary>
    /// One page of stored reports, in <c>(collection_time, server_id)</c> order after the cursor (<c>$1</c>/<c>$2</c>,
    /// null for the start): <c>$3</c> rows and every row tied with the last, so a page never splits a batch and the
    /// next resumes strictly after it. Keyed on the partitioning column so the page walks the hypertable's chunks in
    /// order, and on nothing a raw report's text decides (#4035): neither <c>ctid</c> nor <c>xmin</c> can stand in,
    /// because a compressed chunk's rows carry neither ("transparent decompression only supports tableoid system
    /// column"). Only a raw row's text leaves the store; a row already in #4005's form is examined by its key alone.
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
               AND (d.collection_time, d.server_id) > ($1::timestamp, $2::integer)))
    ORDER BY d.collection_time, d.server_id
    FETCH FIRST $3 ROWS WITH TIES
) AS p
CROSS JOIN LATERAL (SELECT {PgDeadlockLogParser.RawGraphHashSql("p.deadlock_hash", "p.graph_text")} AS raw_hash) AS r
ORDER BY p.collection_time, p.server_id";

    /// <summary>
    /// The raw hash of the report the page read, found in the store by the row's own place rather than sent back
    /// (#4035): <c>$1</c> server, <c>$2</c> its <c>collection_time</c>, <c>$3</c>/<c>$4</c> what the page read. Two raw
    /// reports can share a place (one backend the victim of two deadlocks within a second, in a store written before
    /// the upgrade), so the anchor is the place's raw hashes in order and <c>$8</c> the position of the report's own
    /// among them, read in the same transaction by <see cref="ReportRawGraphsSql"/>; the write also checks <c>$9</c>,
    /// how many there were. The set only shrinks, so the same count is the same set and the position names the same
    /// report; a set changed since writes nothing, and the report waits for the next walk.
    /// </summary>
    private static readonly string ReportAnchorSql = $@"(
          SELECT a.deadlock_hash
          FROM pg_deadlocks AS a
          WHERE a.server_id = $1
          AND   a.collection_time = $2
          AND   a.occurred_at IS NOT DISTINCT FROM $3
          AND   a.victim_pid IS NOT DISTINCT FROM $4
          AND   {PgDeadlockLogParser.RawGraphHashSql("a.deadlock_hash", "a.graph_text")}
          GROUP BY a.deadlock_hash
          ORDER BY a.deadlock_hash
          OFFSET $8 LIMIT 1)";

    /// <summary>How many raw reports the place holds, the write's check that the position <see cref="ReportAnchorSql"/>
    /// names is the one read in the same transaction.</summary>
    private static readonly string ReportRawCountSql = $@"(
          SELECT count(DISTINCT a.deadlock_hash)
          FROM pg_deadlocks AS a
          WHERE a.server_id = $1
          AND   a.collection_time = $2
          AND   a.occurred_at IS NOT DISTINCT FROM $3
          AND   a.victim_pid IS NOT DISTINCT FROM $4
          AND   {PgDeadlockLogParser.RawGraphHashSql("a.deadlock_hash", "a.graph_text")})";

    /// <summary>
    /// The graphs of the raw reports at one place, in the order <see cref="ReportAnchorSql"/> ranks them: read in the
    /// update's own transaction, so the position of the graph the page read is the one the write names. One graph
    /// per hash, because a raw hash is over its graph. Not computed by the page: a report at the same place rewritten
    /// earlier in the walk, through a sighting in an earlier batch, moves the ones ranked after it, and a position the
    /// page computed would then name another report.
    /// </summary>
    public static readonly string ReportRawGraphsSql = $@"
SELECT a.graph_text
FROM pg_deadlocks AS a
WHERE a.server_id = $1
AND   a.collection_time = $2
AND   a.occurred_at IS NOT DISTINCT FROM $3
AND   a.victim_pid IS NOT DISTINCT FROM $4
AND   {PgDeadlockLogParser.RawGraphHashSql("a.deadlock_hash", "a.graph_text")}
GROUP BY a.deadlock_hash, a.graph_text
ORDER BY a.deadlock_hash";

    /// <summary>
    /// Rewrites every sighting of one raw report: <c>$1</c> server, <c>$2</c> the sighting the page met (the pager
    /// walks <c>collection_time</c> upward and leaves nothing raw behind it, so no raw sighting of this report is
    /// older), <c>$3</c>/<c>$4</c> what the page read, <c>$5</c>..<c>$7</c> the normalized values. The report's raw
    /// hash never leaves the store (#4035): the statement takes it from the row the page read
    /// (<see cref="ReportAnchorSql"/>). The report's timestamp and victim pid are compared with <c>=</c>, which
    /// TimescaleDB can test a compressed batch by before it decompresses it; the anchor alone cannot be, and a
    /// statement keyed on it alone decompresses every batch after <c>$2</c> ("tuple decompression limit exceeded",
    /// measured on the lane's rig). A report with no timestamp or pid is rewritten in its own batch by
    /// <see cref="ReportUpdateOwnBatchSql"/>. The raw test is part of the statement, so a row already rewritten, or
    /// stored since, is never touched, and no raw text goes back to the store as a parameter, where
    /// <c>log_min_duration_statement</c> would write it into the store's own log (#4012's review). The victim
    /// statement is the graph's own victim query, so the same graph cannot carry another.
    /// </summary>
    public static readonly string ReportUpdateSql = $@"
UPDATE pg_deadlocks AS d
SET victim_statement = $5,
    graph_text = $6,
    deadlock_hash = $7
WHERE d.server_id = $1
AND   d.collection_time >= $2
AND   d.occurred_at = $3
AND   d.victim_pid = $4
AND   d.deadlock_hash = {ReportAnchorSql}
AND   {ReportRawCountSql} = $9
AND   {PgDeadlockLogParser.RawGraphHashSql("d.deadlock_hash", "d.graph_text")}";

    /// <summary>
    /// <see cref="ReportUpdateSql"/> for a report stored with no timestamp or no victim pid, which <c>=</c> cannot
    /// match: its sightings in the page's own batch only (<c>collection_time = $2</c>, which a compressed batch's
    /// range is tested by), and a later sighting when the walk reaches it.
    /// </summary>
    public static readonly string ReportUpdateOwnBatchSql = $@"
UPDATE pg_deadlocks AS d
SET victim_statement = $5,
    graph_text = $6,
    deadlock_hash = $7
WHERE d.server_id = $1
AND   d.collection_time = $2
AND   d.occurred_at IS NOT DISTINCT FROM $3
AND   d.victim_pid IS NOT DISTINCT FROM $4
AND   d.deadlock_hash = {ReportAnchorSql}
AND   {ReportRawCountSql} = $9
AND   {PgDeadlockLogParser.RawGraphHashSql("d.deadlock_hash", "d.graph_text")}";

    /// <summary>One slice of deadlock alert rows of ONE server (<c>$1</c>) in <c>alert_time</c> order after <c>$2</c>
    /// (null for the server's first row): <c>$3</c> rows and every row tied with the last, so a slice never splits one
    /// instant's rows and the next resumes strictly after it. The server and the metric are both equalities, so the
    /// read is one range of <c>idx_config_alert_log_time (server_id, metric_name, alert_time)</c> that starts at the
    /// cursor and stops at the page size, on PostgreSQL 16, 17 and 18 alike (no skip scan, #5625). A page spans servers
    /// by asking <see cref="AlertServerProbeSql"/> for the next one, never by one read over all of them: the metric
    /// is not the index's first column, so a read over every server would walk each server's other alerts too.
    /// By <c>alert_time</c>, not physical order (#4036): an UPDATE writes a new copy of a row, and a Viewer
    /// dismiss-all writes one of every row, so a copy could land behind a <c>ctid</c> cursor during an otherwise clean
    /// walk, never be read, and the stage be marked done over it. No UPDATE changes a row's <c>alert_time</c>,
    /// <c>server_id</c> or <c>metric_name</c>. <c>ctid</c> and <c>xmin</c> are only what the write checks the row by
    /// (the history table is a plain table, never compressed, so both are there).
    /// <para>The same statement resolves every report-shaped key the slice's incidents carry to its stored report
    /// (the rows after the slice's, <c>is_report</c> true): whether its hash is raw, and a raw one's text, the
    /// earliest report where one hash covers two, as <see cref="DarlingPgDeadlockReader.DeadlocksSql"/> grouped them
    /// when the alert fired. In SQL, from the slice's own <c>context_json</c>, so no raw hash leaves the store as a
    /// parameter (#4035), and in one statement, so the keys looked up are the slice's own. The keys are matched as
    /// text rather than parsed, so a context that is not JSON cannot fail the slice (<see cref="RemaskAlert"/> leaves
    /// such a row as it is).</para></summary>
    public static readonly string AlertPageSql = $@"
WITH page AS MATERIALIZED
(
    SELECT
        a.ctid,
        a.alert_time,
        a.server_id,
        a.detail_text,
        a.context_json,
        a.xmin
    FROM config_alert_log AS a
    WHERE a.server_id = $1
    AND   a.metric_name = '{AlertEngine.DeadlockWatermarkMetric}'
    AND   ($2::timestamp IS NULL OR a.alert_time > $2::timestamp)
    AND   a.context_json IS NOT NULL
    ORDER BY a.alert_time
    FETCH FIRST $3 ROWS WITH TIES
),
keys AS
(
    SELECT DISTINCT
        p.server_id,
        m.k[1] AS deadlock_hash
    FROM page AS p
    CROSS JOIN LATERAL regexp_matches(p.context_json, '""DedupKey"":""([0-9A-F]{{32}})""', 'g') AS m(k)
),
reports AS
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
    JOIN keys AS k
      ON  d.server_id = k.server_id
      AND d.deadlock_hash = k.deadlock_hash
    ORDER BY d.server_id, d.deadlock_hash, d.occurred_at NULLS LAST, d.collection_time
)
SELECT
    false          AS is_report,
    p.alert_time   AS sort_time,
    p.ctid::text   AS row_ctid,
    p.server_id,
    p.detail_text,
    p.context_json,
    p.xmin,
    NULL::text      AS deadlock_hash,
    NULL::timestamp AS occurred_at,
    NULL::integer   AS victim_pid,
    NULL::integer   AS participant_count,
    NULL::text      AS victim_statement,
    NULL::text      AS graph_text,
    false           AS raw_hash
FROM page AS p
UNION ALL
SELECT
    true,
    NULL::timestamp,
    NULL::text,
    r.server_id,
    NULL::text,
    NULL::text,
    NULL::xid,
    r.deadlock_hash,
    r.occurred_at,
    r.victim_pid,
    r.participant_count,
    CASE WHEN r.raw_hash THEN r.victim_statement END,
    CASE WHEN r.raw_hash THEN r.graph_text END,
    COALESCE(r.raw_hash, false)
FROM reports AS r
ORDER BY is_report, sort_time";

    /// <summary><see cref="AlertPageSql"/> for the repair pass: a report in #4005's form comes back with its graph too,
    /// so an incident built under an earlier rule can be rebuilt from it. Normalized text only: the graph of a report
    /// that is not raw is what every read shows for it.</summary>
    public static readonly string AlertRepairPageSql = AlertPageSql.Replace(
        "CASE WHEN r.raw_hash THEN r.graph_text END", "r.graph_text", StringComparison.Ordinal);

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
        DateTime? occurredAt, int? victimPid, string graphText)
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

        /* The victim statement is the normalized graph's own for the victim's pid, as the collector takes it from
           a report it reads live, and not the stored statement normalized on its own: the stored one, read alone,
           can keep a value the graph withholds (PgDeadlockLogParser.VictimStatementOf). */
        return (
            PgDeadlockLogParser.VictimStatementOf(graph, victimPid),
            graph,
            PgDeadlockLogParser.IdentityOf(occurredAt ?? default, graph));
    }

    /// <summary>
    /// Examines one slice of stored reports and rewrites the raw ones (#4012). Each report is its own transaction
    /// under <see cref="StatementTimeoutSeconds"/>, and one that fails (a timeout on a very long-lived report, a
    /// lock) is counted and left raw for the next process's pass rather than holding the rest of the table back.
    /// Returns the cursor to resume from, null once the table's end is reached. <paramref name="rowDone"/> hears each
    /// row's outcome as it is finished, in the page's order (#4012's review, finding 2), with the cursor to resume
    /// from once the row closes its batch (null while the batch has rows left), so a caller canceled mid-page counts
    /// every finished row and resumes after the last finished batch rather than reading the page again.
    /// </summary>
    public static async Task<(ReportCursor? Next, int Examined, int Rewritten, int Failed)> RemaskStoredReportsAsync(
        NpgsqlConnection connection, ReportCursor? after, Action<ReportCursor?, RowOutcome>? rowDone,
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
                select.Parameters.Add(new NpgsqlParameter { Value = (long)MaxReportRowsPerPass, NpgsqlDbType = NpgsqlDbType.Bigint });
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
        for (var i = 0; i < page.Count; i++)
        {
            var row = page[i];

            /* The cursor moves only once a batch is finished: the next page resumes strictly after it. */
            ReportCursor? closes = i == page.Count - 1
                || page[i + 1].CollectionTime != row.CollectionTime || page[i + 1].ServerId != row.ServerId
                    ? new ReportCursor(row.CollectionTime, row.ServerId)
                    : null;

            /* A report is rewritten with all of its sightings at its first: the rest of them in this page are the
               same key, and are rewritten already. */
            if (!row.Raw || row.Graph is null || !done.Add((row.ServerId, row.Hash, row.OccurredAt, row.VictimPid, row.Victim)))
            {
                rowDone?.Invoke(closes, RowOutcome.Unchanged);
                continue;
            }

            var (victim, graph, hash) = RemaskReport(row.OccurredAt, row.VictimPid, row.Graph);
            RowOutcome outcome;
            try
            {
                await using var transaction = await BeginBoundedAsync(connection, cancellationToken);

                /* The place's raw graphs in the anchor's order, read in this transaction (ReportRawGraphsSql): the
                   position of the graph the page read is what the write names. Not there: the report was rewritten
                   since the page was read, with an earlier batch's sighting of it, and there is nothing to do. */
                var rawGraphs = new List<string>();
                await using (var place = new NpgsqlCommand(ReportRawGraphsSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
                {
                    place.Parameters.AddWithValue(row.ServerId);
                    place.Parameters.Add(new NpgsqlParameter { Value = row.CollectionTime, NpgsqlDbType = NpgsqlDbType.Timestamp });
                    place.Parameters.Add(new NpgsqlParameter { Value = (object?)row.OccurredAt ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
                    place.Parameters.Add(new NpgsqlParameter { Value = (object?)row.VictimPid ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Integer });
                    await using var reader = await place.ExecuteReaderAsync(cancellationToken);
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        rawGraphs.Add(reader.GetString(0));
                    }
                }

                var position = rawGraphs.IndexOf(row.Graph);
                if (position < 0)
                {
                    outcome = RowOutcome.Unchanged;
                }
                else
                {
                    var sql = row.OccurredAt is not null && row.VictimPid is not null ? ReportUpdateSql : ReportUpdateOwnBatchSql;
                    await using var update = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandBackstopSeconds };
                    update.Parameters.AddWithValue(row.ServerId);
                    update.Parameters.Add(new NpgsqlParameter { Value = row.CollectionTime, NpgsqlDbType = NpgsqlDbType.Timestamp });
                    update.Parameters.Add(new NpgsqlParameter { Value = (object?)row.OccurredAt ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
                    update.Parameters.Add(new NpgsqlParameter { Value = (object?)row.VictimPid ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Integer });
                    update.Parameters.Add(new NpgsqlParameter { Value = (object?)victim ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = graph, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = hash, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = position, NpgsqlDbType = NpgsqlDbType.Integer });
                    update.Parameters.Add(new NpgsqlParameter { Value = rawGraphs.Count, NpgsqlDbType = NpgsqlDbType.Integer });
                    var written = await update.ExecuteNonQueryAsync(cancellationToken);
                    rewritten += written;
                    outcome = written > 0 ? RowOutcome.Rewritten : RowOutcome.Unchanged;
                }

                await transaction.CommitAsync(cancellationToken);
            }
            catch (PostgresException) when (connection.State == System.Data.ConnectionState.Open)
            {
                /* Rolled back with its transaction; the connection is still usable, so the slice goes on. */
                failed++;
                outcome = RowOutcome.Left;
            }

            rowDone?.Invoke(closes, outcome);
        }

        var next = page.Count < MaxReportRowsPerPass
            ? (ReportCursor?)null
            : new ReportCursor(page[^1].CollectionTime, page[^1].ServerId);
        return (next, page.Count, rewritten, failed);
    }

    /// <summary>
    /// The version of the rules the stored deadlock text is held to: reports, alerts, findings and finding alerts.
    /// The raw-hash passes find what an earlier build left raw. Rows an earlier build's pass rewrote carry a hash that
    /// is no longer over a raw graph, so those passes skip them, and their victim statements were normalized alone,
    /// which can keep a value the graph withholds (<see cref="PgDeadlockLogParser.VictimStatementOf"/>). The repair
    /// stages read every such row once per store, and the version is what says a store has had them: it is written to
    /// <c>collect.collector_state</c> (the shape <see cref="PgSettingScrub"/> uses for its marker, so no migration)
    /// when every repair stage has finished a clean walk, and a store at this version never runs them again. A change
    /// to those rules raises the number, and the stages run once more.
    /// </summary>
    public const int RulesVersion = PgTargetDrillDownCollector.DeadlockTextRulesVersion;

    /// <summary>The owner name of the version marker in <c>collect.collector_state</c>.</summary>
    public const string StateCollectorName = "pg_deadlock_remask";

    /// <summary>The marker's one key: the <see cref="RulesVersion"/> the store was last repaired under, as text.</summary>
    public const string RulesVersionStateKey = "rules_version";

    /// <summary>The marker read; one short keyed row.</summary>
    public const string RulesMarkerGetSql = @"
SELECT state_value
FROM collect.collector_state
WHERE server_id = $1 AND collector_name = $2 AND state_key = $3";

    /// <summary>The marker write.</summary>
    public const string RulesMarkerUpsertSql = @"
INSERT INTO collect.collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (server_id, collector_name, state_key)
DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXCLUDED.updated_at";

    /// <summary>
    /// <see cref="ReportPageSql"/> for the repair stage: the rows already in #4005's form come back with their
    /// statement and graph (normalized text, what every read shows), a raw row with neither, since the raw-hash stage
    /// owns it. Every row is returned, so the page's size and the cursor mean what they do there.
    /// </summary>
    public static readonly string ReportRepairPageSql = ReportPageSql
        .Replace("CASE WHEN r.raw_hash THEN p.victim_statement END", "CASE WHEN NOT COALESCE(r.raw_hash, false) THEN p.victim_statement END", StringComparison.Ordinal)
        .Replace("CASE WHEN r.raw_hash THEN p.graph_text END", "CASE WHEN NOT COALESCE(r.raw_hash, false) THEN p.graph_text END", StringComparison.Ordinal);

    /// <summary>
    /// Sets one report's victim statement to the graph's own, in the batch the page read it in (<c>collection_time = $2</c>,
    /// which a compressed batch's range is tested by): <c>$1</c> server, <c>$2</c> batch, <c>$3</c>/<c>$4</c> the
    /// report's timestamp and victim pid, <c>$5</c> the new statement, <c>$6</c> the report's identity. The identity
    /// is not a raw hash (the page only repairs a row whose hash is not over its raw graph), so it may be sent back.
    /// A row already holding the statement is not touched, so a second pass writes nothing.
    /// </summary>
    public const string ReportStatementUpdateSql = @"
UPDATE pg_deadlocks AS d
SET victim_statement = $5
WHERE d.server_id = $1
AND   d.collection_time = $2
AND   d.occurred_at IS NOT DISTINCT FROM $3
AND   d.victim_pid IS NOT DISTINCT FROM $4
AND   d.deadlock_hash = $6
AND   d.victim_statement IS DISTINCT FROM $5";

    /// <summary>
    /// The victim statement a stored report in #4005's form carries under <see cref="RulesVersion"/> 2: the normalized
    /// graph's own for the victim's pid, never the stored statement normalized alone. Null when the graph carries
    /// none. Pure; the same graph gives the same statement, and a statement this build wrote is its own result.
    /// </summary>
    public static string? RepairedVictimStatement(int? victimPid, string graphText) =>
        PgDeadlockLogParser.VictimStatementOf(PgDeadlockLogParser.NormalizeGraph(graphText)?.TrimEnd('\n'), victimPid);

    /// <summary>
    /// Examines one slice of stored reports and sets the victim statement of each one in #4005's form to the graph's
    /// own (<see cref="RepairedVictimStatement"/>), where it differs: the statement an earlier build's pass wrote was
    /// the stored one normalized alone. Same paging, cursor and per-row outcomes as
    /// <see cref="RemaskStoredReportsAsync"/>; the report's graph, hash and identity are not touched, so no dedupe
    /// identity moves.
    /// </summary>
    public static async Task<(ReportCursor? Next, int Examined, int Rewritten, int Failed)> RepairStoredReportStatementsAsync(
        NpgsqlConnection connection, ReportCursor? after, Action<ReportCursor?, RowOutcome>? rowDone,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var page = new List<(DateTime CollectionTime, int ServerId, string Hash, DateTime? OccurredAt, int? VictimPid,
            string? Victim, string? Graph, bool Raw)>();
        await using (var transaction = await BeginBoundedAsync(connection, cancellationToken))
        {
            await using (var select = new NpgsqlCommand(ReportRepairPageSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
            {
                select.Parameters.Add(new NpgsqlParameter { Value = after is { } a ? a.CollectionTime : DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
                select.Parameters.Add(new NpgsqlParameter { Value = after is { } b ? b.ServerId : DBNull.Value, NpgsqlDbType = NpgsqlDbType.Integer });
                select.Parameters.Add(new NpgsqlParameter { Value = (long)MaxReportRowsPerPass, NpgsqlDbType = NpgsqlDbType.Bigint });
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
        var done = new HashSet<(int, DateTime, string, DateTime?, int?)>();
        for (var i = 0; i < page.Count; i++)
        {
            var row = page[i];
            ReportCursor? closes = i == page.Count - 1
                || page[i + 1].CollectionTime != row.CollectionTime || page[i + 1].ServerId != row.ServerId
                    ? new ReportCursor(row.CollectionTime, row.ServerId)
                    : null;

            if (row.Raw || row.Graph is null || !done.Add((row.ServerId, row.CollectionTime, row.Hash, row.OccurredAt, row.VictimPid)))
            {
                rowDone?.Invoke(closes, RowOutcome.Unchanged);
                continue;
            }

            var statement = RepairedVictimStatement(row.VictimPid, row.Graph);
            if (string.Equals(statement, row.Victim, StringComparison.Ordinal))
            {
                rowDone?.Invoke(closes, RowOutcome.Unchanged);
                continue;
            }

            RowOutcome outcome;
            try
            {
                await using var transaction = await BeginBoundedAsync(connection, cancellationToken);
                await using (var update = new NpgsqlCommand(ReportStatementUpdateSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
                {
                    update.Parameters.AddWithValue(row.ServerId);
                    update.Parameters.Add(new NpgsqlParameter { Value = row.CollectionTime, NpgsqlDbType = NpgsqlDbType.Timestamp });
                    update.Parameters.Add(new NpgsqlParameter { Value = (object?)row.OccurredAt ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
                    update.Parameters.Add(new NpgsqlParameter { Value = (object?)row.VictimPid ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Integer });
                    update.Parameters.Add(new NpgsqlParameter { Value = (object?)statement ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = row.Hash, NpgsqlDbType = NpgsqlDbType.Text });
                    var written = await update.ExecuteNonQueryAsync(cancellationToken);
                    rewritten += written;
                    /* A statement that differs and an UPDATE that finds no row: the row went (retention) or the write
                       cannot find it. Left, so the walk is not a clean one and no marker is written over it. */
                    outcome = written > 0 ? RowOutcome.Rewritten : RowOutcome.Left;
                    failed += written > 0 ? 0 : 1;
                }

                await transaction.CommitAsync(cancellationToken);
            }
            catch (PostgresException) when (connection.State == System.Data.ConnectionState.Open)
            {
                failed++;
                outcome = RowOutcome.Left;
            }

            rowDone?.Invoke(closes, outcome);
        }

        var next = page.Count < MaxReportRowsPerPass
            ? (ReportCursor?)null
            : new ReportCursor(page[^1].CollectionTime, page[^1].ServerId);
        return (next, page.Count, rewritten, failed);
    }

    /// <summary>The <see cref="RulesVersion"/> the store was last repaired under, or null when it never was.</summary>
    public static async Task<string?> ReadRulesMarkerAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(RulesMarkerGetSql, connection) { CommandTimeout = CommandBackstopSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = RulesVersionStateKey });
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    /// <summary>Records that the store has been repaired under <see cref="RulesVersion"/>.</summary>
    public static async Task WriteRulesMarkerAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(RulesMarkerUpsertSql, connection) { CommandTimeout = CommandBackstopSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = RulesVersionStateKey });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Text,
            Value = RulesVersion.ToString(System.Globalization.CultureInfo.InvariantCulture),
        });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
        });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>What became of one row a slice examined, as its per-row callback hears it (#4012's review, finding
    /// 2): the stage counts it there, so a page cut short by a cancel or a failure loses no row's outcome.</summary>
    public enum RowOutcome
    {
        /// <summary>Already in #4005's form, or nothing of it to rewrite.</summary>
        Unchanged,

        /// <summary>Rewritten by this slice.</summary>
        Rewritten,

        /// <summary>Left as it was for another walk: changed between its read and its write, or its rewrite
        /// failed.</summary>
        Left,
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
    /// (<see cref="DarlingWorker.BuildPgDeadlockIncident"/>), which carries no SQL. The builder writes "1 participant" or
    /// "3 participants" (#6703); rows stored before that keep "2 participant(s)" for their retention, so all three
    /// spellings are the same line.</summary>
    private static readonly Regex s_noStatementObject = new(
        @"^victim pid -?[0-9]+, -?[0-9]+ participant(?:s|\(s\))?$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A report's key as a PostgreSQL deadlock alert carries it: <see cref="PgDeadlockLogParser.HashOf"/>'s
    /// shape, 32 upper-case hex characters, which the SQL Server alert's fingerprint (64, lower case) never
    /// has.</summary>
    private static readonly Regex s_reportHash = new(
        "^[0-9A-F]{32}$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>Whether an alert incident's key has a PostgreSQL deadlock report's hash shape.</summary>
    public static bool IsReportHash(string? key) => key is not null && s_reportHash.IsMatch(key);

    /// <summary>Whether an alert incident's key is one an earlier build wrote for a report it could not name by hash: a
    /// keyed hash (<see cref="PgLogHashKey.DeadlockAlertKeyTag"/>, a report retention had pruned) or a timestamp identity
    /// (<see cref="PgDeadlockLogParser.ReportIdentity"/>, a report that was still raw). The repair pass withholds the
    /// statement of such an incident.</summary>
    public static bool IsEarlierBuildKey(string? key) =>
        key is not null
        && (key.StartsWith(PgLogHashKey.DeadlockAlertKeyTag, StringComparison.Ordinal)
            || PgDeadlockLogParser.TryParseReportIdentity(key, out _, out _));

    /// <summary>Whether a statement object still holds text normalization would change: only the decision whether a row
    /// was ever rewritten (it is keyed before any report is rewritten only then), never the text a row is given.</summary>
    private static bool StillCarriesLiterals(List<string> objects) =>
        objects.Any(o => !string.IsNullOrEmpty(o) && !s_noStatementObject.IsMatch(o)
            && !string.Equals(PgDeadlockLogParser.NormalizeStatement(o), o, StringComparison.Ordinal));

    /// <summary>An incident's objects for a report that cannot be found: each statement withheld, since there is no
    /// graph to derive it from and a stored statement normalized on its own can keep a value the graph withholds
    /// (<see cref="PgDeadlockLogParser.VictimStatementOf"/>). The live alert's no-statement text, which carries no
    /// SQL, and an object already withheld stay as they are, so a second pass changes nothing.</summary>
    private static List<string> WithheldObjects(List<string> objects) =>
        objects.ConvertAll(o => string.IsNullOrEmpty(o) || s_noStatementObject.IsMatch(o) || o == PgLogTextRedactor.WithheldStatement
            ? o
            : PgLogTextRedactor.WithheldStatement);

    /// <summary>
    /// The property the pass adds to an incident it rebuilt from its raw report (#4036's round-2 review, finding 2),
    /// so no walk keys it: the rebuilt key is the report's new identity, which is 32 hex like the raw hash it
    /// replaced, and while the report is still raw it finds nothing by it, just as a raw key whose report is gone
    /// finds nothing. The marker is its own property, never part of the key, so:
    /// <list type="bullet">
    /// <item>it cannot be taken for a raw or keyed hash, and no key's value changes;</item>
    /// <item>dedupe is unchanged: the rebuilt key is the one the live alert gives the rewritten report, and the
    /// cooldown seed's anchored <c>"DedupKey":"…"</c> match (<see cref="AlertContextSerializer.BuildDedupKeyLikePattern"/>)
    /// is unaffected, because the marker follows the incident's other members;</item>
    /// <item>every reader ignores it: <see cref="AlertIncidentDto"/> does not map it, and nothing in the product
    /// deserializes an alert context with unmapped members disallowed, so it needs no store migration.</item>
    /// </list>
    /// </summary>
    public const string RebuiltIncidentMarker = "RemaskRebuilt4012";

    /// <summary>The positions of the incidents in <paramref name="contextJson"/> that carry
    /// <see cref="RebuiltIncidentMarker"/>.</summary>
    private static HashSet<int> RebuiltIncidentIndexes(string contextJson)
    {
        var indexes = new HashSet<int>();
        using var document = JsonDocument.Parse(contextJson);
        if (document.RootElement.ValueKind == JsonValueKind.Object
            && document.RootElement.TryGetProperty(nameof(AlertContextDto.Incidents), out var incidents)
            && incidents.ValueKind == JsonValueKind.Array)
        {
            var index = 0;
            foreach (var incident in incidents.EnumerateArray())
            {
                if (incident.ValueKind == JsonValueKind.Object
                    && incident.TryGetProperty(RebuiltIncidentMarker, out var marker)
                    && marker.ValueKind == JsonValueKind.True)
                {
                    indexes.Add(index);
                }

                index++;
            }
        }

        return indexes;
    }

    /// <summary><paramref name="json"/>, serialized from the DTO (which drops the marker), with
    /// <see cref="RebuiltIncidentMarker"/> on the incidents at <paramref name="rebuilt"/>, appended after their
    /// other members.</summary>
    private static string MarkRebuilt(string json, HashSet<int> rebuilt)
    {
        if (rebuilt.Count == 0)
        {
            return json;
        }

        var root = System.Text.Json.Nodes.JsonNode.Parse(json)!.AsObject();
        var incidents = root[nameof(AlertContextDto.Incidents)]!.AsArray();
        foreach (var index in rebuilt)
        {
            if (incidents[index] is System.Text.Json.Nodes.JsonObject incident)
            {
                incident[RebuiltIncidentMarker] = true;
            }
        }

        return root.ToJsonString();
    }

    /// <summary>
    /// The incident the live alert builds for a raw report once it is rewritten
    /// (<see cref="DarlingWorker.BuildPgDeadlockIncident"/> over the row the read then returns): its key is the
    /// report's new identity and its object the normalized victim statement.
    /// </summary>
    public static AlertIncident RemaskedIncident(
        DateTime? occurredAt, int? victimPid, int? participantCount, string graphText)
    {
        var (victim, _, identity) = RemaskReport(occurredAt, victimPid, graphText);
        return DarlingWorker.BuildPgDeadlockIncident(new DarlingPgDeadlockReader.PgDeadlockRow(
            occurredAt ?? default, victimPid ?? 0, participantCount ?? 0, identity, null, null, victim, 0));
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
    /// is for now. An incident this pass rebuilt from its report carries <see cref="RebuiltIncidentMarker"/> and is
    /// never keyed (#4036's round-2 review, finding 2): its key is the report's new identity, which finds no report
    /// while that report is still raw, and keying it would cut it from the report it names. So the key is passed on
    /// every walk, the first included (finding 3), and an unmarked key that finds no report is keyed in the same
    /// write that normalizes its row, rather than bound back to the store raw.</param>
    /// <param name="reportsMayBeRaw">True for the alert stage, which runs before the reports are rewritten: an
    /// unmarked key that finds no report is keyed only in a row no pass has rewritten (one with an incident that
    /// still names a raw report, or still carries a statement to normalize), where it can only be a raw hash. A row
    /// #4022's pass rewrote carries no marker, and its rebuilt key finds nothing while its report is raw; such a row
    /// has nothing else to rewrite, so it is not written at all, and the alert-key stage keys what is left once every
    /// report is rewritten.</param>
    public static (string ContextJson, string? DetailText)? RemaskAlert(
        string contextJson, string? detailText, Func<string, ResolvedReport> resolve, PgLogHashKey? keyUnresolvedWith,
        bool reportsMayBeRaw = false, bool repair = false)
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

        /* A key the pass rebuilt from its report is the report's new identity (#4036's round-2 review, finding 2):
           while the report is raw it finds nothing by it, and it is never keyed. */
        var rebuilt = RebuiltIncidentIndexes(contextJson);
        var resolvedAt = new Dictionary<int, ResolvedReport>();
        var keyedByEarlierBuild = new HashSet<int>();
        var neverRemasked = false;
        for (var index = 0; index < incidents.Count; index++)
        {
            if (repair && incidents[index] is { } earlier && IsEarlierBuildKey(earlier.DedupKey))
            {
                /* An incident an earlier build keyed (a keyed hash for a report retention had pruned, or a timestamp
                   identity for a raw report) holds a victim that build normalized alone, and there is no report to
                   derive it from by this key: it is withheld, whatever it says, and its key stays. */
                resolvedAt[index] = new ResolvedReport(ReportState.Missing, null);
                keyedByEarlierBuild.Add(index);
                continue;
            }

            if (incidents[index] is { } candidate && IsReportHash(candidate.DedupKey) && (repair || !rebuilt.Contains(index)))
            {
                var resolved = resolve(candidate.DedupKey);
                resolvedAt[index] = resolved;
                var objects = candidate.InvolvedObjects ?? new List<string>();
                neverRemasked |= resolved.State == ReportState.Raw
                    || (resolved.State == ReportState.Missing && StillCarriesLiterals(objects));
            }
        }

        /* While a report may be raw, an unmarked key that finds no report is keyed only in a row no pass has
           rewritten, where it can only be a raw hash: one that still names a raw report, or still carries a
           statement to normalize. A row #4022's pass rewrote (it marked nothing, and left no such incident) may hold
           a still-raw report's new identity, and waits for the alert-key stage, after every report is rewritten. */
        var keyWith = reportsMayBeRaw && !neverRemasked ? null : keyUnresolvedWith;
        var replacements = new List<(string OldKey, string NewKey, string OldObjects, string NewObjects)>();
        var rewritten = new List<AlertIncidentDto>(incidents.Count);
        for (var index = 0; index < incidents.Count; index++)
        {
            var incident = incidents[index];
            if (!resolvedAt.TryGetValue(index, out var resolved))
            {
                rewritten.Add(incident!);
                continue;
            }

            var objects = incident!.InvolvedObjects ?? new List<string>();
            List<string> newObjects;
            var newKey = incident.DedupKey;
            if (resolved is { State: ReportState.Raw, Incident: { } built })
            {
                newKey = built.DedupKey;
                newObjects = built.InvolvedObjects.ToList();
                rebuilt.Add(index);
            }
            else if (resolved.State == ReportState.Missing)
            {
                /* There is no graph to derive the statement from, so it is withheld whatever rule wrote it. An
                   incident a pass rebuilt already carries its report's new identity: it is never keyed, only
                   withheld. */
                newKey = rebuilt.Contains(index) || keyedByEarlierBuild.Contains(index)
                    ? incident.DedupKey
                    : keyWith?.DeadlockAlertKey(incident.DedupKey) ?? incident.DedupKey;
                newObjects = WithheldObjects(objects);
            }
            else if (repair && resolved is { State: ReportState.Current, Incident: { } current })
            {
                /* The repair pass: a report in #4005's form, read with its graph, gives the object the live alert
                   would build for it now, whichever rule wrote the stored one. The key stays as it is. */
                newObjects = current.InvolvedObjects.ToList();
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

        var json = MarkRebuilt(JsonSerializer.Serialize(dto with { Details = details, Incidents = rewritten }), rebuilt);
        /* Longest objects first: an overflow row's text carries several incidents, and where one incident's
           statement is a prefix of a later one's, replacing the shorter first rewrote the longer line's head
           and left its tail (the literals past the prefix) as it was. The longer line rewritten first no longer
           contains the shorter raw statement, whose own line is then the only place it is found. */
        var detail = detailText;
        if (detail is not null)
        {
            foreach (var (oldKey, newKey, oldObjects, newObjects) in replacements.OrderByDescending(r => r.OldObjects.Length))
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
    /// A slice reads one server at a time through <see cref="AlertPageSql"/> (a range of the alert-log index, bounded
    /// by the slice's size) and steps to the next server with <see cref="AlertServerProbeSql"/> until it holds
    /// <see cref="MaxAlertRowsPerPass"/> rows or the log ends (#5625).
    /// <paramref name="rowDone"/> hears each row's outcome, with the cursor to resume from once the row closes its
    /// <c>alert_time</c> (null while rows of that instant are left in the page), so a cancel mid-page resumes after
    /// the last instant finished and never skips a row tied with it.
    /// </summary>
    public static Task<(AlertLogCursor? NextCursor, int Examined, int Rewritten, int Raced)> RemaskStoredAlertsAsync(
        NpgsqlConnection connection, AlertLogCursor? afterCursor, PgLogHashKey? keyUnresolvedWith, bool reportsMayBeRaw,
        Action<AlertLogCursor?, RowOutcome>? rowDone, CancellationToken cancellationToken = default) =>
        RemaskStoredAlertsCoreAsync(connection, afterCursor, keyUnresolvedWith, reportsMayBeRaw, rowDone, repair: false, cancellationToken);

    /// <summary><see cref="RemaskStoredAlertsAsync"/> for the repair pass (<see cref="RulesVersion"/>): every report is read
    /// with its graph, and an incident built under an earlier rule is rebuilt from it. Keys nothing.</summary>
    public static Task<(AlertLogCursor? NextCursor, int Examined, int Rewritten, int Raced)> RepairStoredAlertsAsync(
        NpgsqlConnection connection, AlertLogCursor? afterCursor, Action<AlertLogCursor?, RowOutcome>? rowDone,
        CancellationToken cancellationToken = default) =>
        RemaskStoredAlertsCoreAsync(connection, afterCursor, null, reportsMayBeRaw: false, rowDone, repair: true, cancellationToken);

    private static async Task<(AlertLogCursor? NextCursor, int Examined, int Rewritten, int Raced)> RemaskStoredAlertsCoreAsync(
        NpgsqlConnection connection, AlertLogCursor? afterCursor, PgLogHashKey? keyUnresolvedWith, bool reportsMayBeRaw,
        Action<AlertLogCursor?, RowOutcome>? rowDone, bool repair, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var page = new List<(string Ctid, DateTime AlertTime, int ServerId, string? Detail, string Context, uint Xmin)>();
        var reports = new Dictionary<(int, string), ResolvedReport>();
        AlertLogCursor? next = null;
        await using (var transaction = await BeginBoundedAsync(connection, cancellationToken))
        {
            int? server;
            DateTime? after;
            if (afterCursor is { } resume)
            {
                server = resume.ServerId;
                after = resume.AlertTime;
            }
            else
            {
                server = await NextServerAsync(connection, transaction, long.MinValue, cancellationToken);
                after = null;
            }

            var steps = 0;
            while (server is { } current)
            {
                await using (var select = new NpgsqlCommand(repair ? AlertRepairPageSql : AlertPageSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
                {
                    select.Parameters.Add(new NpgsqlParameter { Value = current, NpgsqlDbType = NpgsqlDbType.Integer });
                    select.Parameters.Add(new NpgsqlParameter { Value = (object?)after ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
                    select.Parameters.Add(new NpgsqlParameter { Value = (long)Math.Max(1, MaxAlertRowsPerPass - page.Count), NpgsqlDbType = NpgsqlDbType.Bigint });
                    await using var reader = await select.ExecuteReaderAsync(cancellationToken);
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        if (!reader.GetBoolean(0))
                        {
                            page.Add((
                                reader.GetString(2),
                                reader.GetDateTime(1),
                                reader.GetInt32(3),
                                reader.IsDBNull(4) ? null : reader.GetString(4),
                                reader.GetString(5),
                                reader.GetFieldValue<uint>(6)));
                            continue;
                        }

                        /* A stored report one of the page's incidents names. */
                        var raw = reader.GetBoolean(13);
                        var incident = !reader.IsDBNull(12)
                            ? RemaskedIncident(
                                reader.IsDBNull(8) ? null : reader.GetDateTime(8),
                                reader.IsDBNull(9) ? null : reader.GetInt32(9),
                                reader.IsDBNull(10) ? null : reader.GetInt32(10),
                                reader.GetString(12))
                            : null;
                        reports[(reader.GetInt32(3), reader.GetString(7))] = raw && incident is not null
                            ? new ResolvedReport(ReportState.Raw, incident)
                            : new ResolvedReport(ReportState.Current, repair ? incident : null);
                    }
                }

                if (page.Count >= MaxAlertRowsPerPass)
                {
                    next = new AlertLogCursor(page[^1].ServerId, AlertEngine.DeadlockWatermarkMetric, page[^1].AlertTime);
                    break;
                }

                server = await NextServerAsync(connection, transaction, (long)current + 1, cancellationToken);
                after = null;
                if (server is { } following && ++steps >= MaxAlertStepsPerPage)
                {
                    /* A log of many servers with little to look at: end the page on the server it reached. */
                    next = new AlertLogCursor(following, AlertEngine.DeadlockWatermarkMetric, null);
                    break;
                }
            }

            await transaction.CommitAsync(cancellationToken);
        }

        int rewritten = 0, raced = 0;
        for (var i = 0; i < page.Count; i++)
        {
            var row = page[i];
            var remasked = RemaskAlert(row.Context, row.Detail, reportKey =>
                reports.TryGetValue((row.ServerId, reportKey), out var report) ? report : new ResolvedReport(ReportState.Missing, null), keyUnresolvedWith,
                reportsMayBeRaw, repair);
            var outcome = RowOutcome.Unchanged;
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
                    outcome = written > 0 ? RowOutcome.Rewritten : RowOutcome.Left;
                }

                await transaction.CommitAsync(cancellationToken);
            }

            rowDone?.Invoke(
                i == page.Count - 1 || page[i + 1].AlertTime != row.AlertTime || page[i + 1].ServerId != row.ServerId
                    ? new AlertLogCursor(row.ServerId, AlertEngine.DeadlockWatermarkMetric, row.AlertTime)
                    : null,
                outcome);
        }

        return (next, page.Count, rewritten, raced);
    }

    /// <summary>The first server at or after <paramref name="from"/> that has a row in the alert log, or null: one
    /// descent of the index's leading column (<see cref="AlertServerProbeSql"/>).</summary>
    private static async Task<int?> NextServerAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, long from, CancellationToken cancellationToken)
    {
        await using var probe = new NpgsqlCommand(AlertServerProbeSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds };
        probe.Parameters.Add(new NpgsqlParameter { Value = from, NpgsqlDbType = NpgsqlDbType.Bigint });
        await using var reader = await probe.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? reader.GetInt32(0) : null;
    }

    /// <summary>How many heap blocks of <c>analysis_findings</c> one slice reads (#5625): 2048 blocks of 8 KiB, 16 MiB.
    /// The slice is bounded by the table's physical size, not by how many findings match, so a store with none to
    /// rewrite still costs one slice a fixed read. On a 600,000-finding store (1,514 MB, none matching) the old page
    /// read all 132,357 buffers (about 1 GB) and failed twice under the 15 s <see cref="StatementTimeoutSeconds"/>
    /// on a cold or busy disk; 16 MiB is 1/64 of that, so a disk has to fall below about 1 MiB/s to time a slice
    /// out, and a walk is about 65 slices on that store.</summary>
    public const int FindingBlocksPerPass = 2048;

    /// <summary>The block count of <c>analysis_findings</c> now (#5625): where a block-range walk ends. Read for each
    /// slice, so a table that grew during the walk is walked to its new end (rows this build writes are already
    /// stamped; the ones the walk rewrites are found again by the next walk).</summary>
    public const string FindingBlockCountSql = @"
SELECT pg_catalog.pg_relation_size('analysis_findings'::regclass) / pg_catalog.current_setting('block_size')::bigint,
       pg_catalog.pg_relation_filenode('analysis_findings'::regclass)::bigint";

    /// <summary>The file the block walk is reading (#5625). A <c>VACUUM FULL</c>, <c>CLUSTER</c> or <c>pg_repack</c>
    /// of <c>analysis_findings</c> writes the live rows to a new file, packed toward block 0, so rows that sat at or
    /// after the walk's cursor can land before it. <see cref="Filenode"/> is the file the walk's cursor belongs to
    /// (null before its first page); <see cref="RemaskStoredFindingsAsync"/> reads the table's current one with the
    /// block count and, when the two differ, sends the walk back to block 0 and sets <see cref="Restarted"/>.</summary>
    public sealed class FindingWalk
    {
        /// <summary>The <c>pg_relation_filenode</c> of the table when the walk's cursor was last set.</summary>
        public long? Filenode { get; set; }

        /// <summary>True when the last page found a different file than the walk began in and returned to block 0.</summary>
        public bool Restarted { get; private set; }

        internal void Update(long? filenode, bool restarted)
        {
            Filenode = filenode;
            Restarted = restarted;
        }
    }

    /// <summary>One page of stored analysis findings that carry a deadlock exemplar section, from one physical block
    /// range of the table (#5625): <c>$1</c> is the first block's <c>(block,0)</c> and <c>$2</c> the end of the range,
    /// exclusive, so the read is a TID Range Scan of <see cref="FindingBlocksPerPass"/> blocks whatever matches. It was
    /// <c>finding_id &gt; $1 ORDER BY finding_id</c>, and the table has no index on <c>finding_id</c> (the bulk ingest
    /// goes without it), so every page was a Seq Scan and Sort of the whole table, 1 GB on a 600,000-finding store,
    /// and a page with nothing to find still read all of it. Only a finding whose chain holds a key the section is
    /// attached for (the deadlock
    /// rate fact or its anomaly, anywhere on the path, as <see cref="PgTargetDrillDownCollector"/> attaches it: #4012's
    /// review, finding 4) is looked into, by its <c>story_path</c>, a plain column holding the chain's keys joined
    /// with <c>" → "</c> (<c>InferenceEngine.BuildStory</c>), so every other finding's drill-down is never detoasted
    /// (#4012's review measured the unfiltered read as a Seq Scan and Sort over every drill-down). The chain test
    /// guards the drill-down's in a <c>CASE</c>, so it is evaluated first whatever order the planner gives the
    /// quals. A section this build wrote, or one the pass already rewrote, says <c>sql_normalized</c> and is not
    /// read.</summary>
    public const string FindingPageSql = @"
SELECT
    f.ctid::text,
    f.drill_down_json,
    f.story_text,
    f.xmin
FROM analysis_findings AS f
WHERE f.ctid >= $1::tid
AND   f.ctid <  $2::tid
AND   CASE
          WHEN string_to_array(f.story_path, ' → ') && ARRAY['" + PgTargetFactKeys.DeadlockRate + "', '" + PgTargetFactKeys.AnomalyDeadlockRate + @"']
          THEN strpos(f.drill_down_json, '""" + PgTargetDrillDownCollector.DeadlockExemplarsSection + @"""') > 0
               AND strpos(f.drill_down_json, '""sql_normalized"":true') = 0
          ELSE false
      END
ORDER BY f.ctid";

    /// <summary><see cref="FindingPageSql"/> for the repair pass, which reads the sections an earlier build stamped
    /// <c>sql_normalized</c> too (see <see cref="RulesVersion"/>), and none that says it was written under this version
    /// (<see cref="PgTargetDrillDownCollector.DeadlockTextRulesVersionKey"/>): a section this build wrote, or the pass
    /// rewrote, is not read again.</summary>
    public static readonly string FindingRepairPageSql = FindingPageSql.Replace(
        "AND strpos(f.drill_down_json, '\"sql_normalized\":true') = 0",
        "AND strpos(f.drill_down_json, '\"" + PgTargetDrillDownCollector.DeadlockTextRulesVersionKey + "\":" + RulesVersion + ",') = 0"
        + " AND strpos(f.drill_down_json, '\"" + PgTargetDrillDownCollector.DeadlockTextRulesVersionKey + "\":" + RulesVersion + "}') = 0",
        StringComparison.Ordinal);

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
    public static (string DrillDownJson, string StoryText)? RemaskFinding(string drillDownJson, string storyText, bool repair = false)
    {
        var finding = new AnalysisFinding { DrillDown = DrillDownSerializer.Deserialize(drillDownJson), StoryText = storyText };
        if (!PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars(finding, markNormalized: true, restamped: repair)
            || DrillDownSerializer.Serialize(finding.DrillDown) is not { } json)
        {
            return null;
        }

        /* The repair pass reads sections that are stamped already; one it leaves as it was is not written. */
        return repair && json == drillDownJson && finding.StoryText == storyText ? null : (json, finding.StoryText);
    }

    /// <summary>
    /// Examines one block range of stored analysis findings and rewrites the ones whose deadlock exemplars predate
    /// #4005 (#4012, #5625). <paramref name="afterCursor"/> is the first block to read (null for the start) and the
    /// returned cursor the next one, or null once the table's last block is behind it; it moves by
    /// <see cref="FindingBlocksPerPass"/> blocks whether or not anything matched. Returns how many matching findings
    /// were read, how many were rewritten, and how many changed between the read and the write, which were left as
    /// they were and need the table read again. A cancel inside a slice leaves the cursor at the slice's start: the
    /// rows it rewrote are stamped, so the next tick reads them no more. <paramref name="rowDone"/> is called with a
    /// null cursor, for the same reason.
    /// <paramref name="walk"/> carries the table's file across the walk's slices (#5625): when the file under a
    /// cursor has been replaced (a table rewrite), the slice reads nothing and returns block 0, so the walk starts
    /// again, and <see cref="FindingWalk.Restarted"/> tells the caller that walk is not a clean one.
    /// </summary>
    public static Task<(long? NextCursor, int Examined, int Rewritten, int Raced)> RemaskStoredFindingsAsync(
        NpgsqlConnection connection, long? afterCursor, Action<long?, RowOutcome>? rowDone,
        CancellationToken cancellationToken = default) =>
        RemaskStoredFindingsAsync(connection, afterCursor, rowDone, null, cancellationToken);

    /// <inheritdoc cref="RemaskStoredFindingsAsync(NpgsqlConnection, long?, Action{long?, RowOutcome}?, CancellationToken)"/>
    public static Task<(long? NextCursor, int Examined, int Rewritten, int Raced)> RemaskStoredFindingsAsync(
        NpgsqlConnection connection, long? afterCursor, Action<long?, RowOutcome>? rowDone,
        FindingWalk? walk, CancellationToken cancellationToken) =>
        RemaskStoredFindingsCoreAsync(connection, afterCursor, rowDone, walk, repair: false, cancellationToken);

    /// <summary><see cref="RemaskStoredFindingsAsync(NpgsqlConnection, long?, Action{long?, RowOutcome}?, FindingWalk?, CancellationToken)"/>
    /// for the repair pass (<see cref="RulesVersion"/>): the sections an earlier pass stamped <c>sql_normalized</c> are
    /// read too, and one that comes out as it went in is not written.</summary>
    public static Task<(long? NextCursor, int Examined, int Rewritten, int Raced)> RepairStoredFindingsAsync(
        NpgsqlConnection connection, long? afterCursor, Action<long?, RowOutcome>? rowDone,
        FindingWalk? walk, CancellationToken cancellationToken) =>
        RemaskStoredFindingsCoreAsync(connection, afterCursor, rowDone, walk, repair: true, cancellationToken);

    private static async Task<(long? NextCursor, int Examined, int Rewritten, int Raced)> RemaskStoredFindingsCoreAsync(
        NpgsqlConnection connection, long? afterCursor, Action<long?, RowOutcome>? rowDone,
        FindingWalk? walk, bool repair, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var from = afterCursor ?? 0L;
        long blocks;
        long filenode;
        var page = new List<(string Ctid, string DrillDown, string Story, uint Xmin)>();
        await using (var transaction = await BeginBoundedAsync(connection, cancellationToken))
        {
            await using (var size = new NpgsqlCommand(FindingBlockCountSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
            {
                await using var sizeReader = await size.ExecuteReaderAsync(cancellationToken);
                await sizeReader.ReadAsync(cancellationToken);
                blocks = sizeReader.GetInt64(0);
                filenode = sizeReader.GetInt64(1);
            }

            if (walk is { Filenode: { } walkFilenode } && afterCursor is not null && walkFilenode != filenode)
            {
                /* The table was rewritten under the cursor: rows may now sit before it. Nothing is read; the walk
                   begins again at block 0 and is not counted clean. */
                await transaction.CommitAsync(cancellationToken);
                walk.Update(filenode, restarted: true);
                return (0L, 0, 0, 0);
            }

            if (from < blocks)
            {
                await using var select = new NpgsqlCommand(repair ? FindingRepairPageSql : FindingPageSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds };
                select.Parameters.Add(new NpgsqlParameter { Value = string.Create(System.Globalization.CultureInfo.InvariantCulture, $"({from},0)"), NpgsqlDbType = NpgsqlDbType.Text });
                select.Parameters.Add(new NpgsqlParameter { Value = string.Create(System.Globalization.CultureInfo.InvariantCulture, $"({from + FindingBlocksPerPass},0)"), NpgsqlDbType = NpgsqlDbType.Text });
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
            var outcome = RowOutcome.Unchanged;
            if (RemaskFinding(row.DrillDown, row.Story, repair) is { } finding)
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
                    outcome = written > 0 ? RowOutcome.Rewritten : RowOutcome.Left;
                }

                await transaction.CommitAsync(cancellationToken);
            }

            rowDone?.Invoke(null, outcome);
        }

        var next = from + FindingBlocksPerPass >= blocks ? (long?)null : from + FindingBlocksPerPass;
        walk?.Update(next is null ? null : filenode, restarted: false);
        return (next, page.Count, rewritten, raced);
    }

    /// <summary>How many stored finding alerts one slice examines.</summary>
    public const int MaxFindingAlertRowsPerPass = 200;

    /// <summary>
    /// The earliest <c>alert_time</c> a finding alert with a deadlock exemplar section can carry (#4036's round-2
    /// review, finding 6): #3721 added the section (its first commit 2026-09-19 11:11 UTC, merged 11:28 UTC), so no
    /// alert recorded before then has one to rewrite. A day earlier, because <c>alert_time</c> carries no zone, so the
    /// bound holds for a store that recorded local time anywhere from UTC-12 to UTC+14. Every row the stage must reach
    /// is newer; the older ones are skipped without their context being read.
    /// </summary>
    public static readonly DateTime FindingAlertSectionSince = new(2026, 9, 18, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>What a withheld field, or a withheld victim in a finding alert's prose, says instead (#4012's review,
    /// finding 3). ASCII, so the page's text test matches it as <c>System.Text.Json</c> stores it.</summary>
    public const string WithheldBefore4005 = "(withheld: stored before #4005 with its SQL raw)";

    /// <summary>The heading the live finding alert gives the deadlock exemplar section's detail item.</summary>
    public static string FindingAlertExemplarsHeading => FindingMessageFormatter.DrillDownHeading(PgTargetDrillDownCollector.DeadlockExemplarsSection);

    /// <summary>
    /// One slice of analysis finding alerts of ONE server (<c>$1</c>) and ONE metric (<c>$2</c>), in <c>alert_time</c>
    /// order after <c>$3</c> (null for the pair's first row): the first <c>$4</c> rows recorded since
    /// <see cref="FindingAlertSectionSince"/> and every row tied with the last (#4036: not physical order, which a
    /// dismissal's new copy of a row can land behind). Server and metric are equalities and the time is a range, so the
    /// read is one range of <c>idx_config_alert_log_time (server_id, metric_name, alert_time)</c> that starts at the
    /// cursor and stops at the slice's size (#5625); an older row is never read, so its context is never detoasted
    /// (#4036). Every row read comes back, with <c>needs</c> telling whether its context holds a deadlock exemplar
    /// section flattened before #4005, so the walk's cursor moves over rows that need nothing; only the rows that need
    /// work carry their <c>ctid</c>, context and finding. A finding alert flattens its drill-down into
    /// <c>context_json</c> (<c>FindingMessageFormatter.BuildContext</c>): the section's <c>exemplars</c> as a
    /// 300-character JSON prefix that holds the raw <c>deadlock_hash</c>, its <c>note</c> naming the raw victim
    /// fingerprint, and the advice the prose was frozen with. The alert names its finding by server, category and the
    /// first eight characters of its story hash (<c>FindingMessageFormatter.MetricName</c>), which is the metric the
    /// slice reads; the finding is the newest such one analysed before the alert was recorded, within a day. A section
    /// this build flattened (its <c>Sql Normalized</c> field) or one the pass already rewrote or withheld needs
    /// nothing. Which metrics are <c>Analysis: </c> ones is the walk's loose scan (<see cref="AlertPairNextSql"/>), not
    /// a range on <c>metric_name</c>, which a store with another collation could not be shown to match.
    /// </summary>
    public static readonly string FindingAlertPageSql = $@"
WITH cand AS MATERIALIZED
(
    SELECT
        a.ctid,
        a.alert_time,
        a.xmin,
        a.context_json
    FROM config_alert_log AS a
    WHERE a.server_id = $1
    AND   a.metric_name = $2
    AND   a.alert_time >= '__SINCE__'::timestamp
    AND   ($3::timestamp IS NULL OR a.alert_time > $3::timestamp)
    ORDER BY a.alert_time
    FETCH FIRST $4 ROWS WITH TIES
),
tested AS
(
    SELECT
        c.ctid,
        c.alert_time,
        c.xmin,
        c.context_json,
        CASE
            WHEN c.context_json IS NOT NULL
            THEN strpos(c.context_json, '""Heading"":""{FindingAlertExemplarsHeading}""') > 0
                 AND strpos(c.context_json, '{{""Label"":""Sql Normalized"",""Value"":""true""}}') = 0
                 AND strpos(c.context_json, '{WithheldBefore4005}') = 0
            ELSE false
        END AS needs
    FROM cand AS c
)
SELECT
    CASE WHEN t.needs THEN t.ctid::text END,
    CASE WHEN t.needs THEN t.context_json END,
    t.xmin,
    f.drill_down_json,
    f.story_text,
    f.severity,
    f.confidence,
    f.time_range_start,
    f.time_range_end,
    t.alert_time,
    t.needs
FROM tested AS t
LEFT JOIN LATERAL
(
    SELECT
        f.drill_down_json,
        f.story_text,
        f.severity,
        f.confidence,
        f.time_range_start,
        f.time_range_end
    FROM analysis_findings AS f
    WHERE t.needs
    AND   f.server_id = $1
    AND   f.analysis_time <= t.alert_time
    AND   f.analysis_time >= t.alert_time - INTERVAL '1 day'
    AND   $2::text = 'Analysis: ' || CASE WHEN f.category = '' THEN 'finding' ELSE f.category END || ' [' || left(f.story_path_hash, 8) || ']'
    ORDER BY f.analysis_time DESC
    LIMIT 1
) AS f ON true
ORDER BY t.alert_time".Replace("__SINCE__", FindingAlertSectionSince.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal);

    /// <summary><see cref="FindingAlertPageSql"/> for the repair pass, which reads the alerts stamped <c>Sql Normalized</c>
    /// too (see <see cref="RulesVersion"/>), and none whose section carries <see cref="TextRulesVersionLabel"/> at this
    /// version: an alert flattened from a section written under these rules needs nothing.</summary>
    public static readonly string FindingAlertRepairPageSql = FindingAlertPageSql.Replace(
        "AND strpos(c.context_json, '{\"Label\":\"Sql Normalized\",\"Value\":\"true\"}') = 0",
        "AND strpos(c.context_json, '{\"Label\":\"" + TextRulesVersionLabel + "\",\"Value\":\"" + RulesVersion + "\"}') = 0",
        StringComparison.Ordinal);

    /// <summary>Writes one finding alert's context back, only while the row is still the version the page read
    /// (<c>ctid</c> and <c>xmin</c>). Its <c>detail_text</c> carries no drill-down and is not touched.</summary>
    public const string FindingAlertUpdateSql = @"
UPDATE config_alert_log
SET context_json = $1
WHERE ctid = $2::tid
AND   xmin = $3::xid";

    /* The victim fingerprint as the exemplar prose names it (PgTargetAdvice.DescribeShape): whitespace collapsed,
       at most VictimFingerprintCap characters and an ellipsis, closed by a backtick that ", seen" (the sentence) or
       ". " (the remediation) follows. Greedy within that bound, so a backtick inside the fingerprint cannot end it
       early. */
    private static readonly Regex s_proseVictim = new(
        "with the victim `[^\\n]{1," + (PgTargetDrillDownCollector.VictimFingerprintCap + 1) + "}`(?=, seen |\\. |\\.?$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// One stored finding alert's deadlock exemplar section brought to #4005's rules, or null when it has none to
    /// rewrite (#4012's review, finding 3). When the finding it was sent for is still stored
    /// (<paramref name="findingDrillDownJson"/>), the section's fields become what the live alert flattens from that
    /// finding's section once normalized (<see cref="PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars"/>,
    /// the read's own), stamped <c>Sql Normalized</c>; but only when every other field the alert stored reads the
    /// same there, the proof it is the same finding. Otherwise the fields that carry SQL (<c>Exemplars</c>,
    /// <c>Note</c>) are withheld (<see cref="WithheldBefore4005"/>). The victim the advice prose names is replaced the
    /// same way: by the normalized fingerprint, or withheld. Pure.
    /// <para>#4036's round-2 review, finding 5: the section's other fields are counts, usually all 1, and fixed text,
    /// so another stored run of the same story (an on-demand analysis, or a rerun that queued no page) passes that
    /// test as the newest one the page finds, and its exemplars would be written over the alert's for good. So the
    /// alert's Diagnosis must also read what <paramref name="findingDiagnosis"/> says: the same severity, confidence
    /// and window, as <c>FindingMessageFormatter.BuildContext</c> rendered them; otherwise the finding is not taken
    /// for the alert's, and the SQL is withheld.</para>
    /// </summary>
    public static string? RemaskFindingAlert(
        string contextJson, string? findingDrillDownJson, string? findingStoryText, FindingDiagnosis? findingDiagnosis,
        bool repair = false)
    {
        AlertContextDto? dto;
        try
        {
            dto = JsonSerializer.Deserialize<AlertContextDto>(contextJson);
        }
        catch (JsonException)
        {
            return null;
        }

        var heading = FindingAlertExemplarsHeading;
        var index = dto?.Details?.FindIndex(item => item is not null && item.Heading == heading) ?? -1;
        if (index < 0)
        {
            return null;
        }

        var item = dto!.Details[index];
        var fields = item.Fields ?? new List<FieldDto>();
        if ((!repair && fields.Any(f => f.Label == SqlNormalizedLabel && f.Value == "true"))
            || fields.Any(f => f.Value == WithheldBefore4005)
            || fields.Any(f => f.Label == TextRulesVersionLabel
                && f.Value == RulesVersion.ToString(System.Globalization.CultureInfo.InvariantCulture)))
        {
            /* Withheld already, or flattened from a section written under these rules (so its finding, if it aged out
               since, is no reason to take its text away). */
            return null;
        }

        List<FieldDto>? live = null;
        string? victim = null;
        if (findingDrillDownJson is not null && findingDiagnosis is { } diagnosis && SameDiagnosis(dto, diagnosis))
        {
            var finding = new AnalysisFinding { DrillDown = DrillDownSerializer.Deserialize(findingDrillDownJson), StoryText = findingStoryText ?? string.Empty };
            PgTargetDrillDownCollector.NormalizeStoredDeadlockExemplars(finding, markNormalized: true, restamped: repair);
            if (finding.DrillDown is not null
                && finding.DrillDown.TryGetValue(PgTargetDrillDownCollector.DeadlockExemplarsSection, out var section)
                && FindingMessageFormatter.DrillDownItem(PgTargetDrillDownCollector.DeadlockExemplarsSection, section) is { } rebuilt)
            {
                var rebuiltFields = rebuilt.Fields.Select(f => new FieldDto(f.Label, f.Value)).ToList();
                var same = fields.All(f => f.Label is ExemplarsLabel or NoteLabel
                    || rebuiltFields.Any(r => r.Label == f.Label && r.Value == f.Value));
                if (same && rebuiltFields.Any(f => f.Label == SqlNormalizedLabel && f.Value == "true"))
                {
                    live = rebuiltFields;
                    victim = TopVictimFingerprint(section);
                }
            }
        }

        /* With no finding to rebuild the fields from, the SQL they carry is withheld, in the repair pass as in the first:
           an alert an earlier build stamped holds a victim that build normalized alone, and nothing is left to check
           it against. The next walk stops at the withheld fields above. */
        var newFields = live ?? fields.ConvertAll(f => f.Label is ExemplarsLabel or NoteLabel ? f with { Value = WithheldBefore4005 } : f);
        var details = new List<AlertDetailItemDto>(dto.Details.Count);
        for (var i = 0; i < dto.Details.Count; i++)
        {
            var detail = dto.Details[i];
            if (i == index)
            {
                details.Add(detail with { Fields = newFields });
                continue;
            }

            details.Add(detail is null ? detail! : detail with
            {
                Heading = Victim(detail.Heading, victim)!,
                Body = Victim(detail.Body, victim),
            });
        }

        var rebuiltContext = JsonSerializer.Serialize(dto with { Details = details });
        return repair && rebuiltContext == contextJson ? null : rebuiltContext;

        /* An evaluator, not a replacement pattern: a normalized statement keeps its "$1" placeholders, which a
           pattern would read as a group reference. */
        static string? Victim(string? text, string? fingerprint)
        {
            if (text is null || !text.Contains("with the victim `", StringComparison.Ordinal))
            {
                return text;
            }

            var replacement = fingerprint is null
                ? "with the victim " + WithheldBefore4005
                : "with the victim `" + fingerprint + "`";
            return s_proseVictim.Replace(text, _ => replacement);
        }
    }

    /// <summary>What a stored finding's alert says about it in its Diagnosis item (#4036's round-2 review, finding 5):
    /// its <c>severity</c>, <c>confidence</c> and <c>time_range_start</c>/<c>time_range_end</c>.</summary>
    public readonly record struct FindingDiagnosis(double Severity, double Confidence, DateTime? WindowStart, DateTime? WindowEnd);

    /* Whether the alert's Diagnosis item reads as FindingMessageFormatter.BuildContext rendered it for this finding:
       Severity and Confidence as F2 (in the service's culture, so a decimal comma is taken too), and the Window as the
       two 'u' timestamps, or no Window when the finding has no range. Anything else is another finding. */
    private static bool SameDiagnosis(AlertContextDto dto, FindingDiagnosis finding)
    {
        var diagnosis = dto.Details?.FirstOrDefault(item => item is not null && item.Heading == "Diagnosis");
        if (diagnosis?.Fields is not { } fields)
        {
            return false;
        }

        string? Field(string label) => fields.Where(f => f is not null && f.Label == label).Select(f => f.Value).FirstOrDefault();
        static bool SameNumber(string? stored, double value)
        {
            var invariant = value.ToString("F2", System.Globalization.CultureInfo.InvariantCulture);
            return stored is not null && (stored == invariant || stored == invariant.Replace('.', ','));
        }

        var window = finding.WindowStart is { } start && finding.WindowEnd is { } end
            ? string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{start:u} → {end:u}")
            : null;

        return SameNumber(Field("Severity"), finding.Severity)
            && SameNumber(Field("Confidence"), finding.Confidence)
            && Field("Window") == window;
    }

    private const string ExemplarsLabel = "Exemplars";
    private const string NoteLabel = "Note";
    private const string SqlNormalizedLabel = "Sql Normalized";

    /// <summary>The field a section's <see cref="PgTargetDrillDownCollector.DeadlockTextRulesVersionKey"/> flattens to in
    /// a finding alert.</summary>
    private const string TextRulesVersionLabel = "Text Rules Version";

    /* The top exemplar's normalized fingerprint, the one the advice prose names (PgTargetAdvice.DescribeShape over
       the first exemplar). */
    private static string? TopVictimFingerprint(object section)
    {
        if (section is not JsonElement { ValueKind: JsonValueKind.Object } element
            || !element.TryGetProperty("exemplars", out var exemplars)
            || exemplars.ValueKind != JsonValueKind.Array
            || exemplars.GetArrayLength() == 0
            || exemplars[0].ValueKind != JsonValueKind.Object
            || !exemplars[0].TryGetProperty("victim_statement_fingerprint", out var fingerprint)
            || fingerprint.ValueKind != JsonValueKind.String)
        {
            return null;
        }

        return fingerprint.GetString();
    }

    /// <summary>
    /// Examines one slice of stored finding alerts and rewrites the ones whose deadlock exemplar section was
    /// flattened before #4005 (#4012). Returns the cursor to resume from, null once the log's end is reached, how
    /// many alerts were examined, and how many rows changed between the read and the write.
    /// A slice walks the alert log in index order: for each server and <c>Analysis: </c> metric (found by a loose scan,
    /// <see cref="AlertPairNextSql"/>) one range read of <see cref="FindingAlertPageSql"/>, until
    /// <see cref="MaxFindingAlertRowsPerPass"/> alerts are examined or the log ends (#5625). Alerts are examined
    /// whether or not they need work, so the cursor moves on a slice that rewrites nothing.
    /// </summary>
    public static Task<(AlertLogCursor? NextCursor, int Examined, int Rewritten, int Raced)> RemaskStoredFindingAlertsAsync(
        NpgsqlConnection connection, AlertLogCursor? afterCursor, Action<AlertLogCursor?, RowOutcome>? rowDone,
        CancellationToken cancellationToken = default) =>
        RemaskStoredFindingAlertsCoreAsync(connection, afterCursor, rowDone, repair: false, cancellationToken);

    /// <summary><see cref="RemaskStoredFindingAlertsAsync"/> for the repair pass (<see cref="RulesVersion"/>): the alerts
    /// stamped <c>Sql Normalized</c> are read too, and rebuilt from their finding.</summary>
    public static Task<(AlertLogCursor? NextCursor, int Examined, int Rewritten, int Raced)> RepairStoredFindingAlertsAsync(
        NpgsqlConnection connection, AlertLogCursor? afterCursor, Action<AlertLogCursor?, RowOutcome>? rowDone,
        CancellationToken cancellationToken = default) =>
        RemaskStoredFindingAlertsCoreAsync(connection, afterCursor, rowDone, repair: true, cancellationToken);

    private static async Task<(AlertLogCursor? NextCursor, int Examined, int Rewritten, int Raced)> RemaskStoredFindingAlertsCoreAsync(
        NpgsqlConnection connection, AlertLogCursor? afterCursor, Action<AlertLogCursor?, RowOutcome>? rowDone,
        bool repair, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var page = new List<(string Ctid, string Context, uint Xmin, string? DrillDown, string? Story, FindingDiagnosis? Diagnosis, AlertLogCursor Key)>();
        var examined = 0;
        AlertLogCursor? next = null;
        await using (var transaction = await BeginBoundedAsync(connection, cancellationToken))
        {
            (int Server, string Metric, bool IsAnalysis)? pair;
            DateTime? after;
            if (afterCursor is { } resume)
            {
                /* A step cap can end a page on a pair of any name (#5625), so the cursor's own name says whether it is
                   an Analysis one, the same test the probe's LIKE makes. */
                pair = (resume.ServerId, resume.MetricName, resume.MetricName.StartsWith("Analysis: ", StringComparison.Ordinal));
                after = resume.AlertTime;
            }
            else
            {
                pair = await NextAlertPairAsync(connection, transaction, null, cancellationToken);
                after = null;
            }

            var steps = 0;
            while (pair is { } current)
            {
                if (current.IsAnalysis)
                {
                    DateTime? last = null;
                    await using (var select = new NpgsqlCommand(repair ? FindingAlertRepairPageSql : FindingAlertPageSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
                    {
                        select.Parameters.Add(new NpgsqlParameter { Value = current.Server, NpgsqlDbType = NpgsqlDbType.Integer });
                        select.Parameters.Add(new NpgsqlParameter { Value = current.Metric, NpgsqlDbType = NpgsqlDbType.Text });
                        select.Parameters.Add(new NpgsqlParameter { Value = (object?)after ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
                        select.Parameters.Add(new NpgsqlParameter { Value = (long)Math.Max(1, MaxFindingAlertRowsPerPass - examined), NpgsqlDbType = NpgsqlDbType.Bigint });
                        await using var reader = await select.ExecuteReaderAsync(cancellationToken);
                        while (await reader.ReadAsync(cancellationToken))
                        {
                            examined++;
                            last = reader.GetDateTime(9);
                            if (!reader.GetBoolean(10))
                            {
                                continue;
                            }

                            page.Add((
                                reader.GetString(0),
                                reader.GetString(1),
                                reader.GetFieldValue<uint>(2),
                                reader.IsDBNull(3) ? null : reader.GetString(3),
                                reader.IsDBNull(4) ? null : reader.GetString(4),
                                reader.IsDBNull(5) ? null : new FindingDiagnosis(
                                    reader.GetDouble(5),
                                    reader.GetDouble(6),
                                    reader.IsDBNull(7) ? null : reader.GetDateTime(7),
                                    reader.IsDBNull(8) ? null : reader.GetDateTime(8)),
                                new AlertLogCursor(current.Server, current.Metric, last)));
                        }
                    }

                    if (examined >= MaxFindingAlertRowsPerPass)
                    {
                        next = new AlertLogCursor(current.Server, current.Metric, last);
                        break;
                    }
                }

                pair = await NextAlertPairAsync(connection, transaction, (current.Server, current.Metric), cancellationToken);
                after = null;
                if (pair is { } following && ++steps >= MaxAlertStepsPerPage)
                {
                    /* Many servers and metrics with little to look at: end the page on the pair it reached, whatever its
                       name, so every probe counts. */
                    next = new AlertLogCursor(following.Server, following.Metric, null);
                    break;
                }
            }

            await transaction.CommitAsync(cancellationToken);
        }

        int rewritten = 0, raced = 0;
        for (var i = 0; i < page.Count; i++)
        {
            var row = page[i];
            var outcome = RowOutcome.Unchanged;
            if (RemaskFindingAlert(row.Context, row.DrillDown, row.Story, row.Diagnosis, repair) is { } context)
            {
                await using var transaction = await BeginBoundedAsync(connection, cancellationToken);
                await using (var update = new NpgsqlCommand(FindingAlertUpdateSql, connection, transaction) { CommandTimeout = CommandBackstopSeconds })
                {
                    update.Parameters.Add(new NpgsqlParameter { Value = context, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = row.Ctid, NpgsqlDbType = NpgsqlDbType.Text });
                    update.Parameters.Add(new NpgsqlParameter { Value = row.Xmin, NpgsqlDbType = NpgsqlDbType.Xid });
                    var written = await update.ExecuteNonQueryAsync(cancellationToken);
                    rewritten += written;
                    raced += written == 0 ? 1 : 0;
                    outcome = written > 0 ? RowOutcome.Rewritten : RowOutcome.Left;
                }

                await transaction.CommitAsync(cancellationToken);
            }

            rowDone?.Invoke(i == page.Count - 1 || page[i + 1].Key != row.Key ? row.Key : null, outcome);
        }

        return (next, examined, rewritten, raced);
    }

    /// <summary>The first (<paramref name="after"/> null) or next server and metric of the alert log in index order, with
    /// whether the metric is an <c>Analysis: </c> one, or null at the log's end (<see cref="AlertPairFirstSql"/>,
    /// <see cref="AlertPairNextSql"/>).</summary>
    private static async Task<(int Server, string Metric, bool IsAnalysis)?> NextAlertPairAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, (int Server, string Metric)? after, CancellationToken cancellationToken)
    {
        await using var probe = new NpgsqlCommand(after is null ? AlertPairFirstSql : AlertPairNextSql, connection, transaction)
        { CommandTimeout = CommandBackstopSeconds };
        if (after is { } position)
        {
            probe.Parameters.Add(new NpgsqlParameter { Value = position.Server, NpgsqlDbType = NpgsqlDbType.Integer });
            probe.Parameters.Add(new NpgsqlParameter { Value = position.Metric, NpgsqlDbType = NpgsqlDbType.Text });
        }

        await using var reader = await probe.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? (reader.GetInt32(0), reader.GetString(1), reader.GetBoolean(2))
            : null;
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

        /// <summary>True once a failure, a cut inside a batch (<see cref="CutBySlice"/>) or a restart of the finding
        /// table's walk has interrupted the current walk, over all of its slices (#4012's review, finding 2): such a
        /// walk is never the clean one a stage is done after, however little it counted. A slice's budget that cuts
        /// the walk after a finished batch does not set it.</summary>
        public bool WalkInterrupted { get; set; }

        /// <summary>Walks that rewrote nothing but left rows or were cut short, which count toward
        /// <see cref="MaxRescansPerProcess"/>.</summary>
        public int NoProgressWalks { get; set; }

        /// <summary>Failures in a row, reset by a page that succeeds.</summary>
        public int ConsecutiveFailures { get; set; }

        /// <summary>Ticks still to skip after a failure.</summary>
        public int SkipTicks { get; set; }

        /// <summary>When the stage gave up (#4012's review, finding 5): <see cref="RetryGivenUpAfter"/> later it is
        /// tried again from the start.</summary>
        public DateTime? GaveUpUtc { get; set; }

        /// <summary>True while the page in hand has a batch open: a row was counted that did not close its batch (or,
        /// for the finding walk, the page is the batch and has rows counted), so the walk's cursor does not yet stand
        /// after that row. Cleared at each page's start, and by a slice's end.</summary>
        internal bool BatchOpen { get; set; }

        /// <summary>Counts one row's outcome toward the current walk.</summary>
        public void Count(RowOutcome outcome)
        {
            if (outcome == RowOutcome.Rewritten)
            {
                WalkRewritten++;
            }
            else if (outcome == RowOutcome.Left)
            {
                WalkLeft++;
            }
        }

        /// <summary>Counts one row's outcome, and whether the row closed its batch (the walk's cursor moved to
        /// it).</summary>
        internal void Count(RowOutcome outcome, bool batchClosed)
        {
            Count(outcome);
            BatchOpen = !batchClosed;
        }

        /// <summary>
        /// A slice's budget (or the service's shutdown) cut the stage's walk. The one rule for the main stages and the
        /// repair stages: a cut after a finished batch, or before the page's first row, leaves the cursor on a closed
        /// batch, every finished row counted, and the walk goes on there on the next tick, so it is neither unclean
        /// nor a new walk. A cut inside a batch makes the walk unclean: the batch is read again from its start. The
        /// walk's counts and flag live on the stage, so a walk that spans many slices is clean only if it rewrote
        /// nothing, left nothing and was never cut inside a batch or failed, over all of them.
        /// </summary>
        internal void CutBySlice()
        {
            if (BatchOpen)
            {
                WalkInterrupted = true;
            }

            BatchOpen = false;
        }

        /// <summary>A stage that gave up, as it was before its first walk.</summary>
        internal void Reset()
        {
            Done = false;
            GaveUp = false;
            GaveUpUtc = null;
            WalkRewritten = 0;
            WalkLeft = 0;
            WalkInterrupted = false;
            BatchOpen = false;
            NoProgressWalks = 0;
            ConsecutiveFailures = 0;
            SkipTicks = 0;
        }
    }

    /// <summary>How long a stage that gave up waits before it is tried again (#4012's review, finding 5): a day,
    /// so a service that runs for weeks retries within the rows' retention rather than leaving them raw until it
    /// restarts.</summary>
    public static readonly TimeSpan RetryGivenUpAfter = TimeSpan.FromDays(1);

    /// <summary>The whole re-mask's state for one process: a stage and a cursor per table.</summary>
    public sealed class RemaskProgress
    {
        public StageProgress Alerts { get; } = new("alert");

        public StageProgress Reports { get; } = new("report");

        /// <summary>The alerts walked again once every report is rewritten, to key the ones whose report cannot be
        /// found (<see cref="RemaskAlert"/>). The one stage that needs the store's log-hash key: without one it waits,
        /// and every other stage runs (#4012's review, finding 1).</summary>
        public StageProgress AlertKeys { get; } = new("alert key");

        public StageProgress Findings { get; } = new("finding");

        /// <summary>The analysis finding alerts whose deadlock exemplar section was flattened into their context
        /// before #4005 (#4012's review, finding 3).</summary>
        public StageProgress FindingAlerts { get; } = new("finding alert");

        /// <summary>The alert walk's cursor: the last row it finished.</summary>
        public AlertLogCursor? AlertCursor { get; set; }

        /// <summary>The alert-key walk's cursor: the last row it finished.</summary>
        public AlertLogCursor? AlertKeyCursor { get; set; }

        /// <summary>The report walk's cursor: the last batch it finished.</summary>
        public ReportCursor? ReportCursor { get; set; }

        /// <summary>The finding walk's cursor (#5625): the next heap block to read.</summary>
        public long? FindingCursor { get; set; }

        /// <summary>The file of <c>analysis_findings</c> the finding walk's cursor belongs to (#5625); a different
        /// one at the next page means the table was rewritten and the walk starts again.</summary>
        public long? FindingFilenode { get; set; }

        /// <summary>The finding-alert walk's cursor: the last row it finished.</summary>
        public AlertLogCursor? FindingAlertCursor { get; set; }

        /// <summary>True once the alert-key stage has said it waits for a log-hash key; said once per process.</summary>
        public bool NoKeyWarned { get; set; }

        /// <summary>Hears each row's outcome once its stage has counted it. The tests' hook for a cancel mid-page;
        /// null in the service.</summary>
        public Action<StageProgress, RowOutcome>? RowCounted { get; set; }

        /// <summary>The repair stages (<see cref="RulesVersion"/>), which run once per store beside the stages above, not
        /// after them: the victim statements of the reports an earlier build's pass rewrote, then the alerts,
        /// findings and finding alerts it stamped or keyed.</summary>
        public StageProgress RepairReports { get; } = new("report statement");

        public StageProgress RepairAlerts { get; } = new("repaired alert");

        public StageProgress RepairFindings { get; } = new("repaired finding");

        public StageProgress RepairFindingAlerts { get; } = new("repaired finding alert");

        /// <summary>The repair walks' cursors, as the stages above keep theirs.</summary>
        public ReportCursor? RepairReportCursor { get; set; }

        public AlertLogCursor? RepairAlertCursor { get; set; }

        public long? RepairFindingCursor { get; set; }

        public long? RepairFindingFilenode { get; set; }

        public AlertLogCursor? RepairFindingAlertCursor { get; set; }

        /// <summary>Whether the store's marker already says <see cref="RulesVersion"/>: null until the first tick
        /// reads it, true when the repair stages have nothing to do (they start done), false while they run.</summary>
        public bool? RulesCurrent { get; set; }

        /// <summary>Every stage, in the order a tick runs them.</summary>
        public IReadOnlyList<StageProgress> Stages => [Alerts, Reports, AlertKeys, Findings, FindingAlerts];

        /// <summary>The repair stages, in the order a tick runs them.</summary>
        public IReadOnlyList<StageProgress> RepairStages => [RepairReports, RepairAlerts, RepairFindings, RepairFindingAlerts];

        /// <summary>True once every stage is done for this process, a stage that gave up included. Not the worker's
        /// gate: <see cref="Pending"/> is. The repair stages are not part of it (<see cref="RepairStages"/>).</summary>
        public bool Done => Stages.All(stage => stage.Done);

        /// <summary>
        /// Whether a tick has anything to do: a stage not done yet, or one that gave up, which
        /// <see cref="RunAsync"/> tries again <see cref="RetryGivenUpAfter"/> later. The worker's gate (#4036's
        /// round-2 review, finding 1): gated on <see cref="Done"/>, which a stage that gave up satisfies, a process
        /// whose every stage had finished or given up never called <see cref="RunAsync"/> again, so the day-later
        /// retry never ran and the rows a stage gave up on stayed raw until a restart or their retention. Cheap
        /// while nothing is due: every stage is skipped without a statement.
        /// </summary>
        public bool Pending => Stages.Any(stage => !stage.Done || stage.GaveUp);

        /// <summary>Whether <see cref="RunRepairAsync"/> has anything to do: until the store's marker is known to say
        /// <see cref="RulesVersion"/>.</summary>
        public bool RepairPending => RulesCurrent != true;
    }

    /// <summary>
    /// One tick of the re-mask (#4012): page after page until <paramref name="cancellationToken"/> ends the tick
    /// (the caller's <see cref="SliceBudget"/>) or every stage is done. #4012's review measured a page of 2000
    /// compressed rows at 62-133 ms and a report's rewrite at 18 ms, so a page an hour took about 11 days for the
    /// reports alone. A cancel keeps each walk's cursor at the last row it finished, so the next tick resumes there.
    ///
    /// <para><b>Alerts, then reports, then findings, then finding alerts.</b> An alert is found through its report's
    /// raw hash, which the report stage replaces, so the alerts go first; findings and their alerts last, so a slow
    /// findings read cannot hold the reports back. The reports start once the alerts are done, by a clean walk or by
    /// giving up (#4036's round-2 review, finding 2), never while the alerts only wait out a failure, so no report is
    /// rewritten before its alert is rebuilt; the findings and finding alerts need nothing before them, so a failing
    /// stage cannot starve them (<see cref="MaxConsecutiveStageFailures"/>). Once every report is rewritten by a clean
    /// walk (not by a report stage that gave up), the alerts are walked once more
    /// (<see cref="RemaskProgress.AlertKeys"/>): an alert the
    /// first walk missed (it moved behind the cursor, or that stage gave up) finds no report by its raw hash and takes
    /// a keyed key (<see cref="RemaskAlert"/>), never keeps the raw one. Not before: while a report is raw, an alert
    /// already carrying its new identity finds no report by it either, and keying it would cut it from its
    /// report.</para>
    ///
    /// <para><b>Without a log-hash key</b> (<paramref name="key"/> null: the key file's directory or ACL is not
    /// trusted, it cannot be read, or DPAPI cannot open it on this machine) every stage runs but the alert keys, which
    /// wait for one, say so once, and never count as done (#4012's review, finding 1): the reports, alerts, findings
    /// and finding alerts do not need the key, and skipping them would leave their literals for their whole
    /// retention.</para>
    ///
    /// <para><b>A stage is done only after a whole walk that counted no rewrite and left nothing, and that no
    /// failure or cut inside a batch interrupted, over all of the slices it spans</b> (#4012's review, findings 2 and
    /// 4). A budget cut after a finished batch resumes at the cursor and interrupts nothing
    /// (<see cref="StageProgress.CutBySlice"/>, the rule <see cref="RunRepairAsync"/> follows too). Each row's outcome is counted as it is
    /// finished, through its slice's per-row callback, so a page cut short loses none. A walk that rewrote rows is
    /// followed by another, which catches a row an UPDATE moved behind the cursor; one that rewrote nothing but left
    /// rows or was cut short counts toward <see cref="MaxRescansPerProcess"/>. A stage that gave up is tried again
    /// from its start <see cref="RetryGivenUpAfter"/> later (finding 5).</para>
    /// </summary>
    public static async Task RunAsync(
        NpgsqlConnection connection, RemaskProgress progress, PgLogHashKey? key, ILogger logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(progress);
        ArgumentNullException.ThrowIfNull(logger);

        var now = DateTime.UtcNow;
        foreach (var stage in progress.Stages)
        {
            if (stage.GaveUp && stage.GaveUpUtc is { } gaveUp && now - gaveUp >= RetryGivenUpAfter)
            {
                /* A long-running service tries a stage that gave up again, from its table's start, within the
                   rows' retention. */
                stage.Reset();
                if (ReferenceEquals(stage, progress.Alerts)) progress.AlertCursor = null;
                else if (ReferenceEquals(stage, progress.Reports)) progress.ReportCursor = null;
                else if (ReferenceEquals(stage, progress.AlertKeys)) progress.AlertKeyCursor = null;
                else if (ReferenceEquals(stage, progress.Findings)) { progress.FindingCursor = null; progress.FindingFilenode = null; }
                else progress.FindingAlertCursor = null;
            }
        }

        foreach (var stage in progress.Stages)
        {
            /* #4036's round-2 review, finding 2: an alert must never be cut from its report.
               - The reports wait for the alerts to be done (a clean walk, or given up), not merely to be waiting out
                 a failure: a report rewritten before its alert is rebuilt leaves the alert naming a hash no report
                 carries any more.
               - The alert keys wait for every report to be rewritten, by a clean walk: not for a report stage
                 waiting out a failure, and not for one that gave up. While a report is raw, an alert already
                 carrying its new identity (32 hex, like a raw hash) finds nothing by it, and keying it would cut
                 it from that report for good. */
            if (stage.Done
                || (ReferenceEquals(stage, progress.Reports) && !progress.Alerts.Done)
                || (ReferenceEquals(stage, progress.AlertKeys) && (!progress.Reports.Done || progress.Reports.GaveUp)))
            {
                continue;
            }

            if (ReferenceEquals(stage, progress.AlertKeys) && key is null)
            {
                /* Pending, never done: the one stage that needs the key waits for it. */
                if (!progress.NoKeyWarned)
                {
                    progress.NoKeyWarned = true;
                    logger.LogWarning(
                        "PostgreSQL deadlocks: deadlock reports, alerts and findings stored before this build are re-masked, but a deadlock alert whose report is gone keeps its key, because this service has no log-hash key (#4004) to key it with. The service log's start-up error names the key file and why it could not be used.");
                }

                continue;
            }

            if (stage.SkipTicks > 0)
            {
                /* Waiting out a failure; the stages after it run meanwhile. */
                stage.SkipTicks--;
                continue;
            }

            int examined = 0, rewritten = 0, left = 0;
            void Counted(RowOutcome outcome, bool batchClosed)
            {
                stage.Count(outcome, batchClosed);
                rewritten += outcome == RowOutcome.Rewritten ? 1 : 0;
                left += outcome == RowOutcome.Left ? 1 : 0;
                progress.RowCounted?.Invoke(stage, outcome);
            }

            try
            {
                while (!stage.Done)
                {
                    stage.BatchOpen = false;
                    if (connection.State != System.Data.ConnectionState.Open)
                    {
                        /* A Broken connection must be closed before it can open again. */
                        await connection.CloseAsync();
                        await connection.OpenAsync(cancellationToken);
                    }

                    (bool End, int Examined) page;
                    if (ReferenceEquals(stage, progress.Alerts))
                    {
                        /* With the key (#4035, #4036's round-2 review, finding 3): an incident whose report is gone is
                           keyed in the same write that normalizes its row, so its raw key is never bound back to the
                           store; an incident this pass rebuilt carries RebuiltIncidentMarker and is never keyed. */
                        var (next, e, _, _) = await RemaskStoredAlertsAsync(
                            connection, progress.AlertCursor, key, reportsMayBeRaw: true,
                            (row, outcome) => { progress.AlertCursor = row ?? progress.AlertCursor; Counted(outcome, row is not null); }, cancellationToken);
                        progress.AlertCursor = next;
                        page = (next is null, e);
                    }
                    else if (ReferenceEquals(stage, progress.AlertKeys))
                    {
                        var (next, e, _, _) = await RemaskStoredAlertsAsync(
                            connection, progress.AlertKeyCursor, key, reportsMayBeRaw: false,
                            (row, outcome) => { progress.AlertKeyCursor = row ?? progress.AlertKeyCursor; Counted(outcome, row is not null); }, cancellationToken);
                        progress.AlertKeyCursor = next;
                        page = (next is null, e);
                    }
                    else if (ReferenceEquals(stage, progress.Reports))
                    {
                        var (next, e, _, _) = await RemaskStoredReportsAsync(
                            connection, progress.ReportCursor, (row, outcome) => { progress.ReportCursor = row ?? progress.ReportCursor; Counted(outcome, row is not null); }, cancellationToken);
                        progress.ReportCursor = next;
                        page = (next is null, e);
                    }
                    else if (ReferenceEquals(stage, progress.Findings))
                    {
                        var walk = new FindingWalk { Filenode = progress.FindingFilenode };
                        var (next, e, _, _) = await RemaskStoredFindingsAsync(
                            connection, progress.FindingCursor, (row, outcome) => { progress.FindingCursor = row ?? progress.FindingCursor; Counted(outcome, row is not null); }, walk, cancellationToken);
                        progress.FindingCursor = next;
                        progress.FindingFilenode = walk.Filenode;
                        stage.WalkInterrupted |= walk.Restarted;
                        page = (next is null, e);
                    }
                    else
                    {
                        var (next, e, _, _) = await RemaskStoredFindingAlertsAsync(
                            connection, progress.FindingAlertCursor, (row, outcome) => { progress.FindingAlertCursor = row ?? progress.FindingAlertCursor; Counted(outcome, row is not null); }, cancellationToken);
                        progress.FindingAlertCursor = next;
                        page = (next is null, e);
                    }

                    if (stage.ConsecutiveFailures > 0)
                    {
                        /* #5634: the failure warnings end at the last failure; without this line nothing says the stage came back. */
                        logger.LogInformation(
                            "PostgreSQL deadlocks: re-masking stored {Stage} rows recovered; {Failures} earlier pass(es) in a row had failed, and this one succeeded",
                            stage.Name, stage.ConsecutiveFailures);
                    }

                    stage.ConsecutiveFailures = 0;
                    stage.BatchOpen = false;
                    examined += page.Examined;
                    if (page.End)
                    {
                        EndWalk(stage, logger);
                    }
                }
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                /* The tick's budget (or shutdown) ended it: each walk's cursor stands on the last batch it finished
                   and every finished row is counted. The walk goes on there on the next tick, unclean only if the
                   cut fell inside a batch (the repair stages' rule: StageProgress.CutBySlice). */
                stage.CutBySlice();
                LogTick(stage, examined, rewritten, left, logger, finished: false);
                return;
            }
            catch (Exception ex)
            {
                stage.WalkInterrupted = true;
                stage.ConsecutiveFailures++;
                if (stage.ConsecutiveFailures >= MaxConsecutiveStageFailures)
                {
                    GiveUp(stage);
                    logger.LogWarning(
                        "PostgreSQL deadlocks: re-masking stored {Stage} rows failed {Failures} times in a row, and stops for a day; every read still normalizes them: {Message}",
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

    /// <summary>
    /// One tick of the repair (<see cref="RulesVersion"/>), run by the worker after <see cref="RunAsync"/>. It reads the
    /// store's marker once, and when the store is not at this version walks, in this order: the statements of the
    /// reports in #4005's form, the alerts built from them (an incident an earlier build keyed <c>hk-</c> or <c>at-</c>
    /// has its statement withheld), the stamped findings and the stamped finding alerts. It does not wait for the
    /// stages of <see cref="RunAsync"/>: it skips the rows those own (a raw report) and writes the same text they do for
    /// the rest, so neither needs the other done, and a slow stage there cannot hold this one back. A stage's
    /// failures and retry follow <see cref="RunAsync"/>'s rules; a walk the slice's budget cut goes on at its cursor on
    /// the next tick, and is not an unclean one. Once every stage has finished a clean walk the marker is written, and
    /// the store never runs the stages again.
    /// </summary>
    public static async Task RunRepairAsync(
        NpgsqlConnection connection, RemaskProgress progress, ILogger logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(progress);
        ArgumentNullException.ThrowIfNull(logger);

        if (progress.RulesCurrent == true)
        {
            return;
        }

        if (progress.RulesCurrent is null)
        {
            var marker = await ReadRulesMarkerAsync(connection, cancellationToken);
            progress.RulesCurrent = marker == RulesVersion.ToString(System.Globalization.CultureInfo.InvariantCulture);
            if (progress.RulesCurrent == true)
            {
                return;
            }
        }

        var now = DateTime.UtcNow;
        foreach (var stage in progress.RepairStages)
        {
            if (stage.GaveUp && stage.GaveUpUtc is { } gaveUp && now - gaveUp >= RetryGivenUpAfter)
            {
                stage.Reset();
                if (ReferenceEquals(stage, progress.RepairReports)) progress.RepairReportCursor = null;
                else if (ReferenceEquals(stage, progress.RepairAlerts)) progress.RepairAlertCursor = null;
                else if (ReferenceEquals(stage, progress.RepairFindings)) { progress.RepairFindingCursor = null; progress.RepairFindingFilenode = null; }
                else progress.RepairFindingAlertCursor = null;
            }
        }

        foreach (var stage in progress.RepairStages)
        {
            if (stage.Done)
            {
                continue;
            }

            if (stage.SkipTicks > 0)
            {
                stage.SkipTicks--;
                continue;
            }

            int examined = 0, rewritten = 0, left = 0;
            void Counted(RowOutcome outcome, bool batchClosed)
            {
                stage.Count(outcome, batchClosed);
                rewritten += outcome == RowOutcome.Rewritten ? 1 : 0;
                left += outcome == RowOutcome.Left ? 1 : 0;
                progress.RowCounted?.Invoke(stage, outcome);
            }

            try
            {
                while (!stage.Done)
                {
                    stage.BatchOpen = false;
                    if (connection.State != System.Data.ConnectionState.Open)
                    {
                        await connection.CloseAsync();
                        await connection.OpenAsync(cancellationToken);
                    }

                    (bool End, int Examined) page;
                    if (ReferenceEquals(stage, progress.RepairReports))
                    {
                        var (next, e, _, _) = await RepairStoredReportStatementsAsync(
                            connection, progress.RepairReportCursor, (row, outcome) => { progress.RepairReportCursor = row ?? progress.RepairReportCursor; Counted(outcome, row is not null); }, cancellationToken);
                        progress.RepairReportCursor = next;
                        page = (next is null, e);
                    }
                    else if (ReferenceEquals(stage, progress.RepairAlerts))
                    {
                        /* No key: the repair keys nothing. An incident whose report is gone is withheld, whatever key it
                           carries, and one this pass cannot resolve is left as it is. */
                        var (next, e, _, _) = await RepairStoredAlertsAsync(
                            connection, progress.RepairAlertCursor,
                            (row, outcome) => { progress.RepairAlertCursor = row ?? progress.RepairAlertCursor; Counted(outcome, row is not null); }, cancellationToken);
                        progress.RepairAlertCursor = next;
                        page = (next is null, e);
                    }
                    else if (ReferenceEquals(stage, progress.RepairFindings))
                    {
                        var walk = new FindingWalk { Filenode = progress.RepairFindingFilenode };
                        var (next, e, _, _) = await RepairStoredFindingsAsync(
                            connection, progress.RepairFindingCursor, (row, outcome) => { progress.RepairFindingCursor = row ?? progress.RepairFindingCursor; Counted(outcome, row is not null); },
                            walk, cancellationToken);
                        progress.RepairFindingCursor = next;
                        progress.RepairFindingFilenode = walk.Filenode;
                        stage.WalkInterrupted |= walk.Restarted;
                        page = (next is null, e);
                    }
                    else
                    {
                        var (next, e, _, _) = await RepairStoredFindingAlertsAsync(
                            connection, progress.RepairFindingAlertCursor, (row, outcome) => { progress.RepairFindingAlertCursor = row ?? progress.RepairFindingAlertCursor; Counted(outcome, row is not null); },
                            cancellationToken);
                        progress.RepairFindingAlertCursor = next;
                        page = (next is null, e);
                    }

                    stage.ConsecutiveFailures = 0;
                    stage.BatchOpen = false;
                    examined += page.Examined;
                    if (page.End)
                    {
                        EndWalk(stage, logger);
                    }
                }
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                /* The slice's budget ended it. Each walk's cursor stands on a closed batch (the last report batch, alert
                   instant or finding block range it finished), every finished row is counted, and the next tick
                   resumes there, so the walk goes on and is neither unclean nor a new one, as for the main stages
                   (StageProgress.CutBySlice): a store too big for one slice would otherwise never end a clean walk,
                   and never reach its marker. A failure, a Left row or a cut inside a batch makes the walk unclean. */
                stage.CutBySlice();
                LogTick(stage, examined, rewritten, left, logger, finished: false);
                return;
            }
            catch (Exception ex)
            {
                stage.WalkInterrupted = true;
                stage.ConsecutiveFailures++;
                if (stage.ConsecutiveFailures >= MaxConsecutiveStageFailures)
                {
                    GiveUp(stage);
                    logger.LogWarning(
                        "PostgreSQL deadlocks: repairing stored {Stage} rows failed {Failures} times in a row, and stops for a day; every read still normalizes them: {Message}",
                        stage.Name, stage.ConsecutiveFailures, ex.Message);
                }
                else
                {
                    stage.SkipTicks = (1 << (stage.ConsecutiveFailures - 1)) - 1;
                    logger.LogWarning(
                        "PostgreSQL deadlocks: repairing stored {Stage} rows failed ({Failures} of {Max} in a row), and is retried after {Skip} more hourly pass(es): {Message}",
                        stage.Name, stage.ConsecutiveFailures, MaxConsecutiveStageFailures, stage.SkipTicks, ex.Message);
                }

                continue;
            }

            LogTick(stage, examined, rewritten, left, logger, finished: true);
        }

        if (progress.RepairStages.All(stage => stage.Done && !stage.GaveUp))
        {
            await WriteRulesMarkerAsync(connection, cancellationToken);
            progress.RulesCurrent = true;
        }
    }

    private static void GiveUp(StageProgress stage)
    {
        stage.Done = true;
        stage.GaveUp = true;
        stage.GaveUpUtc = DateTime.UtcNow;
    }

    /// <summary>A walk reached its table's end: done when it counted no rewrite, left nothing and was not cut short,
    /// otherwise walked again.</summary>
    internal static void EndWalk(StageProgress stage, ILogger logger)
    {
        stage.Walks++;
        if (stage.WalkRewritten == 0 && stage.WalkLeft == 0 && !stage.WalkInterrupted)
        {
            stage.Done = true;
        }
        else if (stage.WalkRewritten == 0 && ++stage.NoProgressWalks > MaxRescansPerProcess)
        {
            GiveUp(stage);
            logger.LogWarning(
                "PostgreSQL deadlocks: {Walks} walks of the stored {Stage} rows ended without a clean one, and they are tried again in a day. The last walk left {Left} row(s) as they were (changed under the write, or a rewrite that failed), or was interrupted by a failure or by a pass cut inside a batch; every read still normalizes them.",
                stage.WalkLeft, stage.Name, stage.Walks);
        }

        stage.WalkRewritten = 0;
        stage.WalkLeft = 0;
        stage.WalkInterrupted = false;
        stage.BatchOpen = false;
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
