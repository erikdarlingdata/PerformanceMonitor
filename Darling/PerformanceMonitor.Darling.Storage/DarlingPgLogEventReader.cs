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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads <c>collect.pg_log_events</c> (#3601) — the classified PostgreSQL server-log events — for
/// <c>get_pg_log_events</c> and the Viewer's log-events panel, and the per-table autovacuum run history
/// (#3603) <c>get_pg_autovacuum_health</c> sets beside its catalog sample.
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
        string? Context,
        string? StatementFingerprint,
        string RawLineHash,
        int TimesSeen,
        PgLogEventMetricsRow Metrics);

    /// <summary>
    /// The V130 columns (#3602, #3603), read as the collector wrote them: every member null on a family
    /// that has none, and null on a row stored under V129 before the parsers lifted anything. The tool
    /// publishes each only when non-null, so a connection event's JSON does not carry thirteen nulls.
    /// </summary>
    public readonly record struct PgLogEventMetricsRow(
        string? RelationName,
        long? Bytes,
        long? DurationMs,
        long? PagesRemoved,
        long? PagesRemaining,
        long? TuplesRemoved,
        long? TuplesRemaining,
        long? BufferHits,
        long? BufferMisses,
        long? BufferDirtied,
        long? WalRecords,
        long? WalBytes,
        bool? IsAnalyze);

    /// <summary>The rows plus the window's distinct-event count under the same filters.</summary>
    public readonly record struct PgLogEventsPage(IReadOnlyList<PgLogEventRow> Rows, int WindowTotal);

    /// <summary>
    /// One completed autovacuum / autoanalyze run (#3603), for <c>get_pg_autovacuum_health</c>'s
    /// <c>recent_runs</c> block: the cost of a run beside the catalog's "did it run" — the two halves of
    /// the question the issue put in one place.
    /// </summary>
    public readonly record struct PgAutovacuumRunRow(
        DateTime OccurredAtUtc,
        string? DatabaseName,
        string RelationName,
        bool IsAnalyze,
        long? DurationMs,
        long? PagesRemoved,
        long? PagesRemaining,
        long? TuplesRemoved,
        long? TuplesRemaining,
        long? BufferHits,
        long? BufferMisses,
        long? BufferDirtied,
        long? WalRecords,
        long? WalBytes);

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
                e.context,
                e.statement_fingerprint,
                e.raw_line_hash,
                /* Sightings of the SAME entry, never a count of events -- see the class remarks. Evaluated
                   before DISTINCT ON, so it counts every sighting the distinct then collapses. */
                COUNT(*) OVER (PARTITION BY e.raw_line_hash)::int AS times_seen,
                /* V130 (#3602, #3603): the family's numbers, null wherever the family has none. */
                e.relation_name,
                e.bytes,
                e.duration_ms,
                e.pages_removed,
                e.pages_remaining,
                e.tuples_removed,
                e.tuples_remaining,
                e.buffer_hits,
                e.buffer_misses,
                e.buffer_dirtied,
                e.wal_records,
                e.wal_bytes,
                e.is_analyze
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
            d.context,
            d.statement_fingerprint,
            d.raw_line_hash,
            d.times_seen,
            /* The window's distinct-event count under the SAME filters, on the SAME statement: after the
               distinct, before the limit. The page is what LIMIT admits; this is what it was cut from. */
            COUNT(*) OVER ()::int AS window_total,
            d.relation_name,
            d.bytes,
            d.duration_ms,
            d.pages_removed,
            d.pages_remaining,
            d.tuples_removed,
            d.tuples_remaining,
            d.buffer_hits,
            d.buffer_misses,
            d.buffer_dirtied,
            d.wal_records,
            d.wal_bytes,
            d.is_analyze
        FROM distinct_events AS d
        ORDER BY d.occurred_at DESC
        LIMIT $6
        """;

    /// <summary>
    /// The completed autovacuum / autoanalyze runs in the window for the relations named, newest first,
    /// one row per distinct run (#3603). <c>$4</c> is the relation list (<c>schema.table</c> spellings,
    /// matched exactly against <c>relation_name</c>); <c>$5</c> caps the rows PER RELATION, applied after
    /// the dedupe and the newest-first order, so a table that ran two hundred times in the window does not
    /// starve the others of their history on one page. Same chunk-excluding <c>collection_time</c> bound
    /// as the events read, same identity dedupe, same deadline.
    /// </summary>
    public const string AutovacuumRunsSql = """
        WITH distinct_runs AS (
            SELECT DISTINCT ON (e.raw_line_hash)
                e.occurred_at,
                e.database_name,
                e.relation_name,
                e.is_analyze,
                e.duration_ms,
                e.pages_removed,
                e.pages_remaining,
                e.tuples_removed,
                e.tuples_remaining,
                e.buffer_hits,
                e.buffer_misses,
                e.buffer_dirtied,
                e.wal_records,
                e.wal_bytes
            FROM pg_log_events AS e
            WHERE e.server_id = $1
            AND   e.occurred_at >= $2
            AND   e.occurred_at <= $3
            AND   e.collection_time >= $2
            AND   e.family = 'autovacuum'
            AND   e.relation_name = ANY($4::text[])
            AND   e.raw_line_hash IS NOT NULL
            ORDER BY e.raw_line_hash, e.collection_time
        ),
        ranked AS (
            SELECT
                r.*,
                ROW_NUMBER() OVER (PARTITION BY r.database_name, r.relation_name ORDER BY r.occurred_at DESC) AS rn
            FROM distinct_runs AS r
        )
        SELECT
            occurred_at,
            database_name,
            relation_name,
            is_analyze,
            duration_ms,
            pages_removed,
            pages_remaining,
            tuples_removed,
            tuples_remaining,
            buffer_hits,
            buffer_misses,
            buffer_dirtied,
            wal_records,
            wal_bytes
        FROM ranked
        WHERE rn <= $5
        ORDER BY relation_name, occurred_at DESC
        """;

    public static async Task<IReadOnlyList<PgAutovacuumRunRow>> GetAutovacuumRunsAsync(
        NpgsqlDataSource dataSource,
        int serverId,
        DateTime startUtc,
        DateTime endUtc,
        IReadOnlyList<string> relationNames,
        int runsPerRelation,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgAutovacuumRunRow>();

        if (relationNames.Count == 0)
        {
            return rows;
        }

        await using var command = dataSource.CreateCommand(AutovacuumRunsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.Add(new NpgsqlParameter<string[]>
        {
            TypedValue = relationNames.ToArray(),
            NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text,
        });
        command.Parameters.AddWithValue(runsPerRelation);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgAutovacuumRunRow(
                OccurredAtUtc: DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                DatabaseName: reader.IsDBNull(1) ? null : reader.GetString(1),
                RelationName: reader.GetString(2),
                /* A V129-era autovacuum row has a null is_analyze; the header said "vacuum" or "analyze" but
                   nothing lifted it. Read as vacuum — the commoner run and the one the health read's
                   cost question is about — rather than dropped, so the count of runs stays honest. */
                IsAnalyze: !reader.IsDBNull(3) && reader.GetBoolean(3),
                DurationMs: reader.IsDBNull(4) ? null : reader.GetInt64(4),
                PagesRemoved: reader.IsDBNull(5) ? null : reader.GetInt64(5),
                PagesRemaining: reader.IsDBNull(6) ? null : reader.GetInt64(6),
                TuplesRemoved: reader.IsDBNull(7) ? null : reader.GetInt64(7),
                TuplesRemaining: reader.IsDBNull(8) ? null : reader.GetInt64(8),
                BufferHits: reader.IsDBNull(9) ? null : reader.GetInt64(9),
                BufferMisses: reader.IsDBNull(10) ? null : reader.GetInt64(10),
                BufferDirtied: reader.IsDBNull(11) ? null : reader.GetInt64(11),
                WalRecords: reader.IsDBNull(12) ? null : reader.GetInt64(12),
                WalBytes: reader.IsDBNull(13) ? null : reader.GetInt64(13)));
        }

        return rows;
    }

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
                Context: reader.IsDBNull(10) ? null : reader.GetString(10),
                StatementFingerprint: reader.IsDBNull(11) ? null : reader.GetString(11),
                RawLineHash: reader.GetString(12),
                TimesSeen: reader.GetInt32(13),
                Metrics: new PgLogEventMetricsRow(
                    RelationName: reader.IsDBNull(15) ? null : reader.GetString(15),
                    Bytes: reader.IsDBNull(16) ? null : reader.GetInt64(16),
                    DurationMs: reader.IsDBNull(17) ? null : reader.GetInt64(17),
                    PagesRemoved: reader.IsDBNull(18) ? null : reader.GetInt64(18),
                    PagesRemaining: reader.IsDBNull(19) ? null : reader.GetInt64(19),
                    TuplesRemoved: reader.IsDBNull(20) ? null : reader.GetInt64(20),
                    TuplesRemaining: reader.IsDBNull(21) ? null : reader.GetInt64(21),
                    BufferHits: reader.IsDBNull(22) ? null : reader.GetInt64(22),
                    BufferMisses: reader.IsDBNull(23) ? null : reader.GetInt64(23),
                    BufferDirtied: reader.IsDBNull(24) ? null : reader.GetInt64(24),
                    WalRecords: reader.IsDBNull(25) ? null : reader.GetInt64(25),
                    WalBytes: reader.IsDBNull(26) ? null : reader.GetInt64(26),
                    IsAnalyze: reader.IsDBNull(27) ? null : reader.GetBoolean(27))));

            windowTotal = reader.GetInt32(14);
        }

        return new PgLogEventsPage(rows, windowTotal);
    }
}
