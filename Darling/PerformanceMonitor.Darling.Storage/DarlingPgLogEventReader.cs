/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads <c>collect.pg_log_events</c> (#3601) — the classified PostgreSQL server-log events — for
/// <c>get_pg_log_events</c> and the Viewer's log-events panel.
///
/// <para><b>Every read dedupes on <c>raw_line_hash</c>, for <see cref="DarlingPgDeadlockReader"/>'s
/// reason.</b> The <c>pg_read_file</c> transport re-reads an overlapping tail every cycle, so one event
/// arrives again and again until it falls out of the window; <c>DISTINCT ON (raw_line_hash)</c> takes the
/// earliest sighting, and <c>times_seen</c> says how many there were. On the consume-once RDS route an
/// event normally arrives once, so a low count there is the ordinary state rather than a partial one.</para>
///
/// <para><b>Windowed on <c>occurred_at</c>, bounded below on <c>collection_time</c> too.</b> The event's
/// own time is what the reader means by "last hour"; a collection-time window would put an event in the
/// wrong bucket and move it every cycle. But the hypertable is chunked on <c>collection_time</c>, and an
/// event is always collected AFTER it occurred, so <c>collection_time &gt;= window start</c> is a bound
/// that is always true of every row the window admits and lets the planner exclude every chunk older
/// than the window. Cheap, and the deadlock reader does not have it because its table is small.</para>
///
/// <para><b>The page contract (#3594, #3613).</b> <c>LIMIT</c> is the caller's, bound as a parameter;
/// the tool fetches one more than it will show and OBSERVES truncation; <c>window_total</c> is
/// <c>COUNT(*) OVER ()</c> on the same statement, after the distinct and the filters and before the
/// limit, so it counts every distinct event the window holds under the same filters, not the page.</para>
/// </summary>
public static class DarlingPgLogEventReader
{
    public readonly record struct PgLogEventRow(
        DateTime OccurredAtUtc,
        string Family,
        string Severity,
        string? SqlState,
        string? DatabaseName,
        string? UserName,
        string? ApplicationName,
        int? Pid,
        string Message,
        string? Detail,
        string? StatementFingerprint,
        string RawLineHash,
        int TimesSeen);

    /// <summary>The rows plus the window's distinct-event count under the same filters.</summary>
    public readonly record struct PgLogEventsPage(IReadOnlyList<PgLogEventRow> Rows, int WindowTotal);

    /// <summary>
    /// The seriousness ranking, as SQL. MUST agree with <c>PgLogEntry.RankOf</c> — pinned by test — because
    /// the tool turns the caller's <c>min_severity</c> label into a rank with that method and this CASE is
    /// what the rank is compared against. Seriousness order, not <c>log_min_messages</c>' order, where LOG
    /// ranks above ERROR; the tool's description says so.
    /// </summary>
    public const string SeverityRankSql = """
        CASE e.severity
            WHEN 'PANIC'   THEN 6
            WHEN 'FATAL'   THEN 5
            WHEN 'ERROR'   THEN 4
            WHEN 'WARNING' THEN 3
            WHEN 'NOTICE'  THEN 2
            WHEN 'INFO'    THEN 1
            WHEN 'LOG'     THEN 1
            ELSE 0
        END
        """;

    /// <summary>
    /// Events in the window, newest first, one row per distinct event, optionally one family and at least
    /// one severity. <c>$4</c> is the family or NULL for all; <c>$5</c> is the minimum rank (0 for all).
    /// </summary>
    public static readonly string EventsSql = $$"""
        WITH distinct_events AS (
            SELECT DISTINCT ON (e.raw_line_hash)
                e.occurred_at,
                e.family,
                e.severity,
                e.sqlstate,
                e.database_name,
                e.user_name,
                e.application_name,
                e.pid,
                e.message,
                e.detail,
                e.statement_fingerprint,
                e.raw_line_hash,
                /* Sightings of the SAME entry, never a count of events -- see the class remarks. Evaluated
                   before DISTINCT ON, so it counts every sighting the distinct then collapses. */
                COUNT(*) OVER (PARTITION BY e.raw_line_hash)::int AS times_seen
            FROM pg_log_events AS e
            WHERE e.server_id = $1
            AND   e.occurred_at >= $2
            AND   e.occurred_at <= $3
            AND   e.collection_time >= $2
            AND   e.raw_line_hash IS NOT NULL
            AND   ($4::text IS NULL OR e.family = $4)
            AND   {{SeverityRankSql}} >= $5
            ORDER BY e.raw_line_hash, e.collection_time
        )
        SELECT
            d.occurred_at,
            d.family,
            d.severity,
            d.sqlstate,
            d.database_name,
            d.user_name,
            d.application_name,
            d.pid,
            d.message,
            d.detail,
            d.statement_fingerprint,
            d.raw_line_hash,
            d.times_seen,
            /* The window's distinct-event count under the SAME filters, on the SAME statement: after the
               distinct, before the limit. The page is what LIMIT admits; this is what it was cut from. */
            COUNT(*) OVER ()::int AS window_total
        FROM distinct_events AS d
        ORDER BY d.occurred_at DESC
        LIMIT $6
        """;

    public static async Task<PgLogEventsPage> GetEventsAsync(
        NpgsqlDataSource dataSource,
        int serverId,
        DateTime startUtc,
        DateTime endUtc,
        string? family,
        int minSeverityRank,
        int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgLogEventRow>();
        var windowTotal = 0;

        await using var command = dataSource.CreateCommand(EventsSql);
        /* The MCP-read regime's deadline (#2874), chosen rather than inherited — every reader in this project
           sets one, and StorageCommandTimeoutTests sweeps for the one that does not. */
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.Add(new NpgsqlParameter<string?>
        {
            TypedValue = string.IsNullOrWhiteSpace(family) ? null : family,
            NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text,
        });
        command.Parameters.AddWithValue(minSeverityRank);
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgLogEventRow(
                OccurredAtUtc: DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                Family: reader.IsDBNull(1) ? PerformanceMonitor.Collectors.PgLogFamilies.Other : reader.GetString(1),
                Severity: reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                SqlState: reader.IsDBNull(3) ? null : reader.GetString(3),
                DatabaseName: reader.IsDBNull(4) ? null : reader.GetString(4),
                UserName: reader.IsDBNull(5) ? null : reader.GetString(5),
                ApplicationName: reader.IsDBNull(6) ? null : reader.GetString(6),
                Pid: reader.IsDBNull(7) ? null : reader.GetInt32(7),
                Message: reader.IsDBNull(8) ? string.Empty : reader.GetString(8),
                Detail: reader.IsDBNull(9) ? null : reader.GetString(9),
                StatementFingerprint: reader.IsDBNull(10) ? null : reader.GetString(10),
                RawLineHash: reader.GetString(11),
                TimesSeen: reader.GetInt32(12)));

            windowTotal = reader.GetInt32(13);
        }

        return new PgLogEventsPage(rows, windowTotal);
    }
}
