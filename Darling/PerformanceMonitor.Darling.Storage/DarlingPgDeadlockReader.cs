// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads the stored PostgreSQL deadlock reports (#2661).
///
/// <para><b>Once per deadlock, not once per sighting.</b> On the <c>pg_read_file</c> route the collector
/// re-reads an overlapping tail of the server log every cycle on purpose, so a report inside the window
/// arrives again and again until it falls out. Every read here groups on <c>deadlock_hash</c> and takes
/// the earliest sighting, which is also the closest to when it actually happened.</para>
///
/// <para>The grouping still earns its place on the consume-once RDS log-API route, which can re-offer a
/// window after a restart or a write that did not land (#3008). But there a report normally arrives ONCE,
/// so a low <c>times_seen</c> is the ordinary state rather than a partial count, and nothing in a row
/// says which transport produced it (#3009).</para>
///
/// <para><b>The SQL in a report is normalized on the way out as well as on the way in</b> (#4005). Since #4005
/// the collector stores each query with its literals replaced by <c>?</c>, and a query that cannot be read to
/// its end withheld (<see cref="PgDeadlockLogParser.NormalizeGraph"/>). A row stored before then holds them
/// raw, so every read here puts the victim statement and the graph through the same normalization; it is
/// idempotent, so a row stored since comes back unchanged, and the 90-day retention ages the older ones out.
/// Such a row's <c>deadlock_hash</c> is over its raw graph, which makes it a test for the literals the read
/// just normalized away (#4004), so it is never returned and never looked up by:
/// <see cref="PgDeadlockLogParser.ReportIdentity"/> names that row instead, and
/// <see cref="PgDeadlockLogParser.RawGraphHashSql"/> is how it is told apart.</para>
/// </summary>
public static class DarlingPgDeadlockReader
{
    public readonly record struct PgDeadlockRow(
        DateTime OccurredAtUtc,
        int VictimPid,
        int ParticipantCount,
        string DeadlockHash,
        string? LockModes,
        string? Resources,
        string? VictimStatement,
        int TimesSeen);

    public readonly record struct PgDeadlockDetailRow(
        DateTime OccurredAtUtc,
        int VictimPid,
        int ParticipantCount,
        string? LockModes,
        string? Resources,
        string? GraphText,
        string DeadlockHash);

    /// <summary>
    /// Deadlocks in the window, newest first, one row per distinct report.
    ///
    /// <para>Windowed on <c>occurred_at</c> rather than <c>collection_time</c>, and the difference is not
    /// cosmetic: a report is collected some minutes AFTER it happened, and can be collected repeatedly for
    /// as long as it stays in the log tail. Filtering on collection time would put a deadlock in the wrong
    /// window and would move it every cycle.</para>
    /// </summary>
    public const string DeadlocksSql = """
        SELECT
            MIN(d.occurred_at)          AS occurred_at,
            MIN(d.victim_pid)           AS victim_pid,
            MAX(d.participant_count)    AS participant_count,
            d.deadlock_hash,
            MIN(d.lock_modes)           AS lock_modes,
            MIN(d.resources)            AS resources,
            MIN(d.victim_statement)     AS victim_statement,
            /* How many times the collector saw this same report. Never a count of deadlocks: one that
               genuinely recurred carries a DIFFERENT hash each time because the pids differ, and being
               told apart from that is what this is surfaced for. On the pg_read_file route it is a
               property of the overlapping read window; on the consume-once RDS route there is no such
               window, so it is normally 1 and a low value is the ordinary state rather than a partial
               count -- see the class remarks for the two ways it can still exceed 1 there (#3009). */
            COUNT(*)::int               AS times_seen,
            /* Whether the group's hash is over its raw graph, and so must not leave this read (#4005). One
               graph per group, because the hash is over it. PgDeadlockLogParser.RawGraphHashSql, spelled out
               because this is a constant; a test holds the two equal. */
            (upper(left(encode(sha256(convert_to(MIN(d.graph_text), 'UTF8')), 'hex'), 32)) = d.deadlock_hash) AS raw_hash
        FROM pg_deadlocks AS d
        WHERE d.server_id = $1
        AND   d.occurred_at >= $2
        AND   d.occurred_at <= $3
        AND   d.deadlock_hash IS NOT NULL
        GROUP BY d.deadlock_hash
        ORDER BY MIN(d.occurred_at) DESC
        LIMIT $4
        """;

    /// <summary>
    /// Reports in full, including the graph text the collector stored.
    ///
    /// <para><b>The identity is optional</b>, and that is what lets this be a panel as well as a drill-down.
    /// With one it answers about a single report; without one it returns the most recent graphs, which is
    /// the shape the SQL Server "Deadlock Graphs" panel already has and the reason a reader does not have to
    /// call the summary first just to see a graph. <c>$2</c> is a hash, <c>$3</c>/<c>$4</c> the timestamp and
    /// victim pid of a <see cref="PgDeadlockLogParser.ReportIdentity"/> (#4005). A hash finds only a row whose
    /// hash is not over its raw graph, so a raw hash a reader computed from a guess finds nothing; the pair,
    /// which every read shows anyway, finds the report whichever build stored it.</para>
    ///
    /// <para><c>DISTINCT ON (deadlock_hash)</c> for the same reason every read here groups: on the
    /// <c>pg_read_file</c> route the collector re-reads an overlapping tail, so without it the newest few
    /// rows are frequently the same report several times over. Costless where that route is not in
    /// use.</para>
    ///
    /// <para><b>Newest first, in an outer query</b> (#4005). The DISTINCT ON key has to lead the inner sort and
    /// the LIMIT sat on that sort, so without an identity this returned the reports whose hashes sort first
    /// rather than the most recent. The raw-hash test is out there too, so it runs once per report rather than
    /// once per sighting.</para>
    /// </summary>
    public const string DeadlockDetailSql = """
        SELECT
            r.occurred_at,
            r.victim_pid,
            r.participant_count,
            r.lock_modes,
            r.resources,
            r.graph_text,
            r.deadlock_hash,
            r.raw_hash
        FROM
        (
            SELECT
                report.*,
                /* PgDeadlockLogParser.RawGraphHashSql, spelled out because this is a constant; a test holds
                   the two equal. */
                (upper(left(encode(sha256(convert_to(report.graph_text, 'UTF8')), 'hex'), 32)) = report.deadlock_hash) AS raw_hash
            FROM
            (
                SELECT DISTINCT ON (d.deadlock_hash)
                    d.occurred_at,
                    d.victim_pid,
                    d.participant_count,
                    d.lock_modes,
                    d.resources,
                    d.graph_text,
                    d.deadlock_hash
                FROM pg_deadlocks AS d
                WHERE d.server_id = $1
                AND   ($2::text IS NULL OR d.deadlock_hash = $2::text)
                AND   ($3::timestamp IS NULL OR (d.occurred_at = $3::timestamp AND d.victim_pid = $4::integer))
                AND   d.deadlock_hash IS NOT NULL
                /* The DISTINCT ON key must lead the sort; the earliest sighting of each report is the one
                   closest to when it actually happened. */
                ORDER BY d.deadlock_hash, d.collection_time
            ) AS report
        ) AS r
        WHERE ($2::text IS NULL OR NOT r.raw_hash)
        ORDER BY r.occurred_at DESC NULLS LAST, r.deadlock_hash
        LIMIT $5
        """;

    public static async Task<List<PgDeadlockRow>> GetDeadlocksAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgDeadlockRow>();
        await using var command = postgres.CreateCommand(DeadlocksSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then converts these naive columns at the store session's
           TimeZone, which silently empties the window east of UTC. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var occurredAt = reader.IsDBNull(0) ? default : reader.GetDateTime(0);
            var victimPid = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            rows.Add(new PgDeadlockRow(
                occurredAt,
                victimPid,
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                IdentityOf(reader.GetString(3), !reader.IsDBNull(8) && reader.GetBoolean(8), occurredAt, victimPid),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : PgDeadlockLogParser.NormalizeStatement(reader.GetString(6)),
                reader.IsDBNull(7) ? 0 : reader.GetInt32(7)));
        }

        return rows;
    }

    /// <param name="deadlockHash">An identity a read here returned: a hash, or a
    /// <see cref="PgDeadlockLogParser.ReportIdentity"/>. Null or blank for the most recent reports.</param>
    public static async Task<List<PgDeadlockDetailRow>> GetDeadlockDetailAsync(
        NpgsqlDataSource postgres, int serverId, string? deadlockHash, int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgDeadlockDetailRow>();
        var byReport = PgDeadlockLogParser.TryParseReportIdentity(deadlockHash, out var reportAt, out var reportPid);
        await using var command = postgres.CreateCommand(DeadlockDetailSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text,
            byReport || string.IsNullOrWhiteSpace(deadlockHash) ? DBNull.Value : (object)deadlockHash.Trim());
        /* Kind-Unspecified at the bind, per the store's naive-UTC discipline (see GetDeadlocksAsync). */
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp,
            byReport ? DateTime.SpecifyKind(reportAt, DateTimeKind.Unspecified) : (object)DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, byReport ? reportPid : (object)DBNull.Value);
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var occurredAt = reader.IsDBNull(0) ? default : reader.GetDateTime(0);
            var victimPid = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            rows.Add(new PgDeadlockDetailRow(
                occurredAt,
                victimPid,
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : PgDeadlockLogParser.NormalizeGraph(reader.GetString(5)),
                IdentityOf(reader.GetString(6), !reader.IsDBNull(7) && reader.GetBoolean(7), occurredAt, victimPid)));
        }

        return rows;
    }

    /// <summary>
    /// What a read returns as a report's identity (#4005): its stored hash, unless that hash is over the raw
    /// graph, in which case the <see cref="PgDeadlockLogParser.ReportIdentity"/> the detail read looks it up by.
    /// </summary>
    public static string IdentityOf(string storedHash, bool rawHash, DateTime occurredAt, int victimPid) =>
        rawHash ? PgDeadlockLogParser.ReportIdentity(occurredAt, victimPid) : storedHash;
}
