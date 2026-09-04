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
/// Reads per-table autovacuum state (<c>pg_autovacuum_stats</c>), ranked by how far past its own
/// threshold each table is.
/// </summary>
public static class DarlingPgAutovacuumReader
{
    public sealed record PgAutovacuumRow(
        string? DatabaseName,
        string? SchemaName,
        string? TableName,
        DateTime MeasuredAt,
        long LiveTuples,
        long DeadTuples,
        long VacuumThreshold,
        long ModsSinceAnalyze,
        long AnalyzeThreshold,
        long InsertsSinceVacuum,
        long InsertVacuumThreshold,
        bool AutovacuumDisabled,
        long TotalBytes,
        DateTime? LastVacuum,
        DateTime? LastAutovacuum,
        DateTime? LastAnalyze,
        DateTime? LastAutoanalyze,
        long AutovacuumCount,
        long FirstDeadTuples,
        DateTime FirstSeenAt);

    /// <summary>
    /// Latest state per table, joined to that table's earliest reading in the window.
    /// <para>Ordered by the RATIO of dead tuples to that table's own threshold, not by the raw count.
    /// Ordering by count would put the biggest tables on top permanently — they always have the most dead
    /// tuples and are usually fine — and bury the small hot table that is fifty times past its line. The
    /// threshold is what makes the two comparable.</para>
    /// <para>The earliest reading answers the follow-up question: dead tuples that are climbing mean
    /// autovacuum is losing, while a flat figure at ten times the threshold usually means it is blocked or
    /// switched off. Those need different fixes.</para>
    /// <para>$1 server_id, $2/$3 window (naive UTC), $4 row limit.</para>
    /// </summary>
    public const string PgAutovacuumSql = """
        WITH latest AS (
            SELECT DISTINCT ON (database_name, schema_name, table_name)
                database_name, schema_name, table_name, collection_time,
                live_tuples, dead_tuples, vacuum_threshold, mods_since_analyze, analyze_threshold,
                inserts_since_vacuum, insert_vacuum_threshold, autovacuum_disabled, total_bytes,
                last_vacuum, last_autovacuum, last_analyze, last_autoanalyze, autovacuum_count
            FROM pg_autovacuum_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, schema_name, table_name, collection_time DESC
        ),
        earliest AS (
            SELECT DISTINCT ON (database_name, schema_name, table_name)
                database_name, schema_name, table_name,
                dead_tuples AS first_dead_tuples,
                collection_time AS first_seen_at
            FROM pg_autovacuum_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, schema_name, table_name, collection_time ASC
        )
        SELECT
            l.database_name,
            l.schema_name,
            l.table_name,
            l.collection_time,
            l.live_tuples,
            l.dead_tuples,
            l.vacuum_threshold,
            l.mods_since_analyze,
            l.analyze_threshold,
            l.inserts_since_vacuum,
            l.insert_vacuum_threshold,
            l.autovacuum_disabled,
            l.total_bytes,
            l.last_vacuum,
            l.last_autovacuum,
            l.last_analyze,
            l.last_autoanalyze,
            l.autovacuum_count,
            e.first_dead_tuples,
            e.first_seen_at
        FROM latest AS l
        JOIN earliest AS e
          ON  e.database_name IS NOT DISTINCT FROM l.database_name
          AND e.schema_name   IS NOT DISTINCT FROM l.schema_name
          AND e.table_name    IS NOT DISTINCT FROM l.table_name
        /* Ratio, not raw count — and a table with autovacuum switched off sorts to the top regardless,
           because that is a configuration finding rather than a workload one. NULLIF guards the
           never-analyzed case, where the threshold can be 0.

           The ratio is the WORSE of the two, dead-tuple and insert-only. Ranking on dead tuples alone buried
           append-only tables at ratio 0, below the LIMIT — and those are the classic wraparound route the
           collector gathers inserts_since_vacuum for in the first place: never vacuumed means relfrozenxid
           never advances. A table taking 10x its insert threshold with zero dead tuples is a finding, and it
           used to be invisible here.

           insert_vacuum_threshold carries -1 as the not-applicable sentinel on a major that has no
           autovacuum_vacuum_insert_threshold, so the CASE keeps that out of the arithmetic rather than
           producing a negative ratio. GREATEST ignores NULLs (verified on live Aurora), so a NULL dead ratio
           does not swallow a real insert ratio. */
        ORDER BY
            l.autovacuum_disabled DESC,
            GREATEST(
                l.dead_tuples::numeric / NULLIF(l.vacuum_threshold, 0),
                CASE
                    WHEN l.insert_vacuum_threshold > 0
                        THEN l.inserts_since_vacuum::numeric / l.insert_vacuum_threshold
                    ELSE 0
                END
            ) DESC NULLS LAST,
            GREATEST(l.dead_tuples, l.inserts_since_vacuum) DESC
        LIMIT $4
        """;

    public static async Task<List<PgAutovacuumRow>> GetPgAutovacuumAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgAutovacuumRow>();
        await using var command = postgres.CreateCommand(PgAutovacuumSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then resolves the comparison against these naive
           timestamp columns by converting THEM at the store session's TimeZone - east of UTC every fresh
           row falls out of the window and the read silently returns nothing. Hidden by UTC-hosted test
           stores; found by the round-2 review. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgAutovacuumRow(
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetDateTime(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? -1 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? -1 : reader.GetInt64(8),
                reader.IsDBNull(9) ? -1 : reader.GetInt64(9),
                reader.IsDBNull(10) ? -1 : reader.GetInt64(10),
                !reader.IsDBNull(11) && reader.GetBoolean(11),
                reader.IsDBNull(12) ? -1 : reader.GetInt64(12),
                reader.IsDBNull(13) ? null : reader.GetDateTime(13),
                reader.IsDBNull(14) ? null : reader.GetDateTime(14),
                reader.IsDBNull(15) ? null : reader.GetDateTime(15),
                reader.IsDBNull(16) ? null : reader.GetDateTime(16),
                reader.IsDBNull(17) ? 0 : reader.GetInt64(17),
                reader.IsDBNull(18) ? 0 : reader.GetInt64(18),
                reader.GetDateTime(19)));
        }

        return rows;
    }
}
