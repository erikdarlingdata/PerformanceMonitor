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
/// Reads measured b-tree index bloat (<c>pg_index_bloat</c>, #2561) — the LATEST measurement per index,
/// ranked by RECLAIMABLE BYTES rather than by density.
///
/// <para><b>Ranking by density would be wrong, and this is the whole reason the read exists.</b> A tiny
/// index at 40% density is worse-looking and worth nothing; a large one at 70% is where the space actually
/// is. The estimate of what a <c>REINDEX</c> would return is <c>index_bytes</c> scaled by how far density
/// sits below what a freshly built index of that shape achieves — and since a healthy index measures near
/// 90 rather than 100, the shortfall is computed against 90, not against a full page.</para>
///
/// <para><b>That 90 is a floor, not a constant, and the read says so.</b> Measured across freshly built
/// indexes the value ranged 89.98–91.48, and post-<c>REINDEX</c> 87.07–90.81. So the reclaimable figure is
/// an ESTIMATE derived from a measurement, and it is presented beside the raw density rather than replacing
/// it. Anyone acting on it should confirm against the index's own post-rebuild density.</para>
///
/// <para><b>Skipped indexes sort to the top, not out of sight.</b> An index too large to measure is exactly
/// the one most likely to be holding reclaimable space, so a read that filtered it out would hide the
/// biggest candidate behind a performance optimisation.</para>
///
/// <para>Shared by the WPF tab and the MCP surface so there is one copy of this SQL, per #2530.</para>
/// </summary>
public static class DarlingPgIndexBloatReader
{
    /// <param name="AvgLeafDensity">The server's raw figure. NULL when the index was skipped.</param>
    /// <param name="EstimatedReclaimableBytes">What a rebuild might return, derived from the density
    /// shortfall against a 90% healthy floor. NULL when not measured, and 0 when the index is at or above
    /// that floor.</param>
    /// <param name="SkippedReason">Non-null means this index was NOT measured — its bloat is unknown rather
    /// than zero.</param>
    public sealed record PgIndexBloatRow(
        string? DatabaseName,
        string? SchemaName,
        string? TableName,
        string? IndexName,
        long IndexBytes,
        int? TreeLevel,
        long? EmptyPages,
        long? DeletedPages,
        double? AvgLeafDensity,
        double? LeafFragmentation,
        long? EstimatedReclaimableBytes,
        string? SkippedReason,
        DateTime CaptureTime);

    /* DISTINCT ON the index identity ordered by collection_time DESC gives the newest measurement per index
       in one pass. The outer ORDER BY then ranks for reading, which the inner one cannot do.

       database_name leads the distinct key (#2599) - this collector runs once per database, and an index
       name is only unique within one, so without it the newest collection_time silently picks which
       database's copy of a shared schema the grid shows.

       The reclaimable estimate uses GREATEST(0, ...) so an index measuring ABOVE the healthy floor reports
       zero rather than a negative saving - which is a real case, since freshly built indexes measured up to
       91.48.

       NULLIF guards the division: a density of 0 is not something pgstatindex returns for a live index, but
       a divide-by-zero would turn one odd row into a failed read for the whole grid. */
    public const string PgIndexBloatSql = """
        SELECT database_name, schema_name, table_name, index_name, index_bytes, tree_level,
               empty_pages, deleted_pages, avg_leaf_density, leaf_fragmentation,
               CASE
                   WHEN avg_leaf_density IS NULL THEN NULL
                   ELSE GREATEST(
                       0,
                       (index_bytes * (90.0 - avg_leaf_density) / NULLIF(90.0, 0))::bigint)
               END AS estimated_reclaimable_bytes,
               skipped_reason,
               collection_time
        FROM (
            SELECT DISTINCT ON (database_name, schema_name, table_name, index_name)
                   database_name, schema_name, table_name, index_name, index_bytes, tree_level,
                   empty_pages, deleted_pages, avg_leaf_density, leaf_fragmentation,
                   skipped_reason, collection_time
            FROM pg_index_bloat
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, schema_name, table_name, index_name, collection_time DESC
        ) AS latest
        /* Unmeasured first - a skipped index is the likeliest big win and must not be ranked below measured
           ones by a reclaimable figure it does not have. Then by reclaimable bytes, never by density: a
           small index at 40% is worth nothing next to a large one at 70%. */
        ORDER BY (skipped_reason IS NOT NULL) DESC,
                 CASE WHEN avg_leaf_density IS NULL THEN NULL
                      ELSE GREATEST(0, (index_bytes * (90.0 - avg_leaf_density) / 90.0)::bigint)
                 END DESC NULLS LAST,
                 index_bytes DESC
        LIMIT $4
        """;

    public static async Task<List<PgIndexBloatRow>> GetPgIndexBloatAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgIndexBloatRow>();
        await using var command = postgres.CreateCommand(PgIndexBloatSql);
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
            rows.Add(new PgIndexBloatRow(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                SchemaName: reader.IsDBNull(1) ? null : reader.GetString(1),
                TableName: reader.IsDBNull(2) ? null : reader.GetString(2),
                IndexName: reader.IsDBNull(3) ? null : reader.GetString(3),
                IndexBytes: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                TreeLevel: reader.IsDBNull(5) ? null : reader.GetInt32(5),
                EmptyPages: reader.IsDBNull(6) ? null : reader.GetInt64(6),
                DeletedPages: reader.IsDBNull(7) ? null : reader.GetInt64(7),
                AvgLeafDensity: reader.IsDBNull(8) ? null : reader.GetDouble(8),
                LeafFragmentation: reader.IsDBNull(9) ? null : reader.GetDouble(9),
                EstimatedReclaimableBytes: reader.IsDBNull(10) ? null : reader.GetInt64(10),
                SkippedReason: reader.IsDBNull(11) ? null : reader.GetString(11),
                CaptureTime: reader.IsDBNull(12)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(12), DateTimeKind.Utc)));
        }

        return rows;
    }
}
