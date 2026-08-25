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
/// Reads what is resident in shared buffers (<c>pg_buffer_usage</c>, #2544) — the LATEST snapshot, with the
/// share of the pool each relation holds computed for the caller.
///
/// <para><b>Latest, not aggregated across the window.</b> Buffer residency is a level, not a counter:
/// averaging a relation's buffer count across a day answers nothing anyone asks. What is wanted is "what is
/// in the pool now, and is the working set fitting" — and the history exists so somebody can see a relation
/// being evicted and re-read repeatedly, which is a trend read rather than this one.</para>
///
/// <para><b>The share is computed here rather than left to the caller</b>, because the denominator travels
/// on the row: the collector repeats the pool totals on every row precisely so a percentage never has to be
/// derived from a second read against a pool that has moved on.</para>
///
/// <para><b>A NULL relation name is not an unnamed relation.</b> It means the buffer belongs to another
/// database (the pool is cluster-wide, <c>pg_class</c> is not) or to a shared catalog. Those rows are real
/// occupancy and are kept — dropping them would understate how full the pool is, which is the one number
/// this exists to report.</para>
///
/// <para>Shared by the WPF tab and the MCP surface so there is one copy of this SQL, per #2530.</para>
/// </summary>
public static class DarlingPgBufferUsageReader
{
    /// <param name="RelationName">NULL for another database's relation or a shared catalog — see the type
    /// header. Not a missing name.</param>
    /// <param name="PctOfPool">This relation's share of the WHOLE pool, used and unused alike, which is the
    /// share that answers "is the working set fitting".</param>
    /// <param name="PctDirty">Share of this relation's own buffers that are dirty — what a checkpoint will
    /// have to write on its account.</param>
    public sealed record PgBufferUsageRow(
        string? DatabaseName,
        string? RelationName,
        string? RelationKind,
        long Buffers,
        long DirtyBuffers,
        double? AvgUsageCount,
        long PoolBuffersTotal,
        long PoolBuffersUsed,
        double PctOfPool,
        double PctDirty,
        DateTime CaptureTime);

    /* Only the newest collection_time in the window, taken as a whole: the pool totals are consistent within
       one snapshot and not across snapshots, so mixing rows from two of them would produce shares that do
       not add up.

       NULLIF guards both denominators. A pool total of zero cannot happen on a live server, but a division
       that produces a divide-by-zero error instead of a NULL turns an odd sample into a failed read. */
    public const string PgBufferUsageSql = """
        WITH newest AS (
            SELECT max(collection_time) AS at
            FROM pg_buffer_usage
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        )
        SELECT
            b.database_name,
            b.relation_name,
            b.relation_kind,
            b.buffers,
            b.dirty_buffers,
            b.avg_usage_count,
            b.pool_buffers_total,
            b.pool_buffers_used,
            (100.0 * b.buffers / NULLIF(b.pool_buffers_total, 0))::double precision AS pct_of_pool,
            (100.0 * b.dirty_buffers / NULLIF(b.buffers, 0))::double precision       AS pct_dirty,
            b.collection_time
        FROM pg_buffer_usage AS b
        JOIN newest AS n ON b.collection_time = n.at
        WHERE b.server_id = $1
        ORDER BY b.buffers DESC
        LIMIT $4
        """;

    public static async Task<List<PgBufferUsageRow>> GetPgBufferUsageAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgBufferUsageRow>();
        await using var command = postgres.CreateCommand(PgBufferUsageSql);
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
            rows.Add(new PgBufferUsageRow(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                RelationName: reader.IsDBNull(1) ? null : reader.GetString(1),
                RelationKind: reader.IsDBNull(2) ? null : reader.GetString(2),
                Buffers: reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                DirtyBuffers: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                AvgUsageCount: reader.IsDBNull(5) ? null : reader.GetDouble(5),
                PoolBuffersTotal: reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                PoolBuffersUsed: reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                PctOfPool: reader.IsDBNull(8) ? 0 : reader.GetDouble(8),
                PctDirty: reader.IsDBNull(9) ? 0 : reader.GetDouble(9),
                CaptureTime: reader.IsDBNull(10)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(10), DateTimeKind.Utc)));
        }

        return rows;
    }
}
