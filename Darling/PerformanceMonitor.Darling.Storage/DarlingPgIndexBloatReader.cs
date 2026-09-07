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
/// <para><b>An EMPTY index has no density, and it still gets a row (#3121).</b> <c>pgstatindex</c> returns
/// NaN for both float columns when there are no leaf pages to average over, so the read normalises NaN to
/// NULL and reports 0 reclaimable — the index is measured, and what it holds is nothing. Filtering the row
/// out instead would serve the read successfully while hiding a real index, which is the failure mode that
/// looks most like a fix.</para>
///
/// <para>Shared by the WPF tab and the MCP surface so there is one copy of this SQL, per #2530.</para>
/// </summary>
public static class DarlingPgIndexBloatReader
{
    /// <param name="AvgLeafDensity">The server's raw figure. NULL when the index was skipped, and NULL when
    /// the index is EMPTY — <c>pgstatindex</c> divides by a leaf-page count of zero and returns NaN, which
    /// is not a density and cannot be carried to any consumer here (see the SQL's own note).</param>
    /// <param name="LeafFragmentation">As above, and NaN on an empty index for the same reason.</param>
    /// <param name="EstimatedReclaimableBytes">What a rebuild might return, derived from the density
    /// shortfall against a 90% healthy floor. NULL when not measured, and 0 both when the index is at or
    /// above that floor and when it is empty — an index with no leaf pages holds nothing to reclaim, which
    /// is a measurement rather than an absence of one.</param>
    /// <param name="SkippedReason">Non-null means this index was NOT measured — its bloat is unknown rather
    /// than zero. An empty index leaves this NULL: it was measured, and the measurement is that it is
    /// empty.</param>
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
       a divide-by-zero would turn one odd row into a failed read for the whole grid.

       THE THIRD STATE OF A FLOAT COLUMN (#3121). pgstatindex reports avg_leaf_density and
       leaf_fragmentation as NaN for an EMPTY b-tree index - verified on PostgreSQL 18.6 against a real index
       on a table with no rows, which is an entirely ordinary object (a fresh partition, a table whose rows
       were all deleted, a constraint index on an unpopulated table). avg_leaf_density is double precision in
       the store, so NaN is representable, persists, and is re-read on every later call. An IS NULL guard
       alone is not enough here, and it is not enough for a reason its own vocabulary hides: "handle the
       missing case" covers two of the three states a float column has, and a NaN that reaches the ::bigint
       cast below raises 22003, which fails the WHOLE read rather than the row.

       nullif(x, 'NaN'::double precision) is the whole normalisation, and it turns on a PostgreSQL semantic
       that is the opposite of the C one: NaN compares EQUAL to itself here, so nullif catches it and
       IS NOT DISTINCT FROM would too, while an IEEE-style self-inequality check (x <> x) is false and
       catches nothing. It is identity on a real density and on NULL, so the two states that already worked
       are untouched.

       Normalising is not optional decoration on the density columns themselves: a NaN that clears the SQL
       still reaches System.Text.Json, whose default number handling REJECTS NaN, so it would take down the
       MCP read and the web page one layer up from the cast. Nothing may leave this read as NaN.

       AN EMPTY INDEX REPORTS 0 RECLAIMABLE, NOT NULL. NULL in this column means "not measured" - that is
       what a skipped row carries, and the read hoists those to the top on the strength of it. An empty
       index WAS measured, and what was measured is that it holds nothing to reclaim, so it takes the same 0
       an above-floor index takes and ranks where a 0 belongs. Reporting NULL would sort it among the
       unmeasured and claim its bloat was unknown when it is known to be nil.

       THE ESTIMATE IS COMPUTED ONCE, in the inner scope, and the outer projection and the outer ORDER BY
       both reference that one column. Two copies of the expression would be a correctness hazard rather
       than a duplication: they have to agree, and the failure of a disagreement is the ordering raising
       while the projection succeeds - a read that half works, for a reason no reader could see. One
       expression cannot drift from itself. */
    public const string PgIndexBloatSql = """
        SELECT database_name, schema_name, table_name, index_name, index_bytes, tree_level,
               empty_pages, deleted_pages,
               nullif(avg_leaf_density, 'NaN'::double precision) AS avg_leaf_density,
               nullif(leaf_fragmentation, 'NaN'::double precision) AS leaf_fragmentation,
               estimated_reclaimable_bytes,
               skipped_reason,
               collection_time
        FROM (
            SELECT DISTINCT ON (database_name, schema_name, table_name, index_name)
                   database_name, schema_name, table_name, index_name, index_bytes, tree_level,
                   empty_pages, deleted_pages, avg_leaf_density, leaf_fragmentation,
                   CASE
                       WHEN avg_leaf_density IS NULL THEN NULL
                       WHEN avg_leaf_density = 'NaN'::double precision THEN 0::bigint
                       ELSE GREATEST(
                           0,
                           (index_bytes * (90.0 - avg_leaf_density) / NULLIF(90.0, 0))::bigint)
                   END AS estimated_reclaimable_bytes,
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
                 latest.estimated_reclaimable_bytes DESC NULLS LAST,
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
