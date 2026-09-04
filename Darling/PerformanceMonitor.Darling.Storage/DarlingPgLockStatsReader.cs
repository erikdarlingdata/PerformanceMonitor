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
/// Reads the stored lock samples (<c>pg_lock_stats</c>, #2544) rolled up over the window, with the CAPTURE
/// COUNT as the denominator.
///
/// <para><b>These are samples, not events.</b> A minute-cadence collector sees a lock queue only if the
/// queue existed when it looked, so "three ungranted rows" means something completely different in 60
/// captures than in 4. The same rule <c>pg_blocking</c> follows, and for the same reason: without the
/// denominator a reader silently converts a sampling artefact into a rate.</para>
///
/// <para><b>Ranked by ungranted first, then by how long anyone waited.</b> A granted lock is not a finding —
/// every working server holds thousands — so the ordering puts queues at the top and, within them, the ones
/// somebody actually sat behind. <c>max_wait_ms</c> is the sharp end: a queue that appears in one capture
/// having waited four minutes is worse than one appearing in forty having waited ten milliseconds.</para>
///
/// <para><b><c>relation_name</c> may be NULL for a real relation.</b> <c>pg_locks</c> is cluster-wide while
/// <c>pg_class</c> is per-database, so a lock in another database has an OID and no name. The read returns
/// both columns and never coalesces the name to something invented — a caller showing "unknown" is correct,
/// and one showing a name from the wrong database is not.</para>
///
/// <para>Shared by the WPF tab and the MCP surface so there is one copy of this SQL, per #2530.</para>
/// </summary>
public static class DarlingPgLockStatsReader
{
    /// <param name="Captures">How many snapshots this group appeared in — the numerator.</param>
    /// <param name="TotalCaptures">How many snapshots the collector took in the window — the denominator.
    /// Present on every row so a caller never has to make a second read to interpret the first.</param>
    /// <param name="MaxBackends">The largest queue seen in any single capture, not a sum across captures:
    /// summing would multiply one persistent queue by the number of times it was observed.</param>
    /// <param name="MaxWaitMs">The longest any backend in this group was observed waiting.</param>
    public sealed record PgLockStatRow(
        string? DatabaseName,
        string? LockType,
        string? Mode,
        bool Granted,
        long? RelationOid,
        string? RelationName,
        long Captures,
        long TotalCaptures,
        long MaxBackends,
        double? MaxWaitMs,
        DateTime LastSeen);

    /* The capture denominator is computed once over the whole window and cross-joined onto every row, rather
       than counted per group - a group's own count is the numerator and cannot also be the denominator.

       MAX(backend_count), never SUM. A queue of three backends that persists across forty captures is three
       backends, not a hundred and twenty; summing would report a steady queue as an enormous one and rank it
       above a genuinely worse transient. Same trap the blocking read documents. */
    public const string PgLockStatsSql = """
        WITH captures AS (
            SELECT count(DISTINCT collection_time) AS total
            FROM pg_lock_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        )
        SELECT
            s.database_name,
            s.lock_type,
            s.mode,
            s.granted,
            s.relation_oid,
            s.relation_name,
            count(DISTINCT s.collection_time) AS captures,
            c.total                           AS total_captures,
            max(s.backend_count)              AS max_backends,
            max(s.oldest_wait_ms)             AS max_wait_ms,
            max(s.collection_time)            AS last_seen
        FROM pg_lock_stats AS s
        CROSS JOIN captures AS c
        WHERE s.server_id = $1
        AND   s.collection_time >= $2
        AND   s.collection_time <= $3
        GROUP BY s.database_name, s.lock_type, s.mode, s.granted,
                 s.relation_oid, s.relation_name, c.total
        /* Ungranted first: a granted lock is not a finding. Then by the worst wait anyone actually served,
           and only then by queue size - a long wait is the complaint, and the queue length is its shape. */
        ORDER BY s.granted,
                 max(s.oldest_wait_ms) DESC NULLS LAST,
                 max(s.backend_count) DESC
        LIMIT $4
        """;

    public static async Task<List<PgLockStatRow>> GetPgLockStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgLockStatRow>();
        await using var command = postgres.CreateCommand(PgLockStatsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the BIND, same convention as every other PostgreSQL read here: Npgsql
           does not reject Kind=Utc, it infers timestamptz, and PostgreSQL then resolves the comparison
           against these NAIVE timestamp columns at the store session's TimeZone — so east of UTC the window
           slides off the data and the read returns nothing at all. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgLockStatRow(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                LockType: reader.IsDBNull(1) ? null : reader.GetString(1),
                Mode: reader.IsDBNull(2) ? null : reader.GetString(2),
                Granted: !reader.IsDBNull(3) && reader.GetBoolean(3),
                RelationOid: reader.IsDBNull(4) ? null : reader.GetInt64(4),
                RelationName: reader.IsDBNull(5) ? null : reader.GetString(5),
                Captures: reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                TotalCaptures: reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                MaxBackends: reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                MaxWaitMs: reader.IsDBNull(9) ? null : reader.GetDouble(9),
                LastSeen: reader.IsDBNull(10)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(10), DateTimeKind.Utc)));
        }

        return rows;
    }
}
