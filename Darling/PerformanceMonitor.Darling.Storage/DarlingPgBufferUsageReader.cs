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
using PerformanceMonitor.Collectors;

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
        DateTime CaptureTime,
        long RelationsInSnapshot = 0,
        long SnapshotBuffers = 0,
        string RelationBreakdown = RelationsListed)
    {
        /// <summary>
        /// True for the one row of a snapshot whose per-relation scan was skipped (#9508): it carries the pool totals and no
        /// relation. Its <see cref="Buffers"/> is 0 only because a count cannot be missing here, so a caller that lists
        /// relations leaves this row out.
        /// </summary>
        public bool IsTotalsOnly => RelationBreakdown != RelationsListed;

        /// <summary>
        /// A word for a row that has no relation name, null when it has one (#7455). The name is missing by
        /// design and not by fault, and a blank cell reads as a fault: the largest row on a quiet server is
        /// often one of these.
        /// </summary>
        public string? NameLabel => RelationLabel(DatabaseName, RelationName);
    }

    /// <summary><see cref="PgBufferUsageRow.RelationBreakdown"/> of an ordinary snapshot: the rows are the relations.</summary>
    public const string RelationsListed = "relations";

    /// <summary>
    /// The snapshot has no relation rows by design (#9508): the pool was over the collector's cap, so only the totals were
    /// read. Not a failure and not missing data.
    /// </summary>
    public const string SkippedOverCap = "skipped_over_cap";

    /// <summary>
    /// The snapshot has no relation rows and no used count (#9508): the pool was over the cap and the target has no
    /// <c>pg_buffercache_summary()</c>, so the totals are the pool size alone. <c>ALTER EXTENSION pg_buffercache UPDATE</c>
    /// is what gives the target the function.
    /// </summary>
    public const string Precondition = "precondition";

    /// <summary>The statement that gives a target <c>pg_buffercache_summary()</c> (#9508). The service never runs it.</summary>
    public const string PreconditionStatement = "ALTER EXTENSION pg_buffercache UPDATE";

    /// <summary>
    /// What a reader says about a snapshot with no relation rows (#9508), so a skip by design reads differently from a
    /// failure and from no data. Null for an ordinary snapshot.
    /// </summary>
    public static string? BreakdownNote(string relationBreakdown, long poolBuffersTotal) => relationBreakdown switch
    {
        SkippedOverCap =>
            $"The per-relation breakdown was skipped by design: the pool is {poolBuffersTotal:N0} buffers, over the "
            + $"{PgBufferUsageCollector.PoolCapBuffers:N0}-buffer cap above which the collector does not scan pg_buffercache "
            + "(the scan costs about 1 microsecond per buffer on the monitored server). The pool totals come from pg_buffercache_summary().",
        Precondition =>
            $"The per-relation breakdown was skipped by design: the pool is {poolBuffersTotal:N0} buffers, over the "
            + $"{PgBufferUsageCollector.PoolCapBuffers:N0}-buffer cap above which the collector does not scan pg_buffercache, "
            + "and this target has no pg_buffercache_summary() to read the pool totals from, so how much of the pool is in use is unknown. "
            + $"The precondition is {PreconditionStatement} in the database the collector connects to (the function arrives with "
            + "extension version 1.4, PostgreSQL 16). The collector never runs it.",
        _ => null,
    };

    /// <summary>
    /// Shown for a buffer that has a database name and no relation name. The stored row does not record which
    /// database the collector connected to, so this says only that the relation was not resolved: usually a
    /// buffer in another database, but also a relation in the connected database that <c>pg_class</c> did not
    /// resolve.
    /// </summary>
    public const string NotResolvedLabel = "(not resolved, usually another database)";

    /// <summary>Shown for a buffer whose database matches no row in <c>pg_database</c>.</summary>
    public const string SharedOrDroppedLabel = "(shared catalogs or a dropped database)";

    /// <summary>
    /// Why a name is missing, for a row without one (#7455). The pool is cluster-wide and <c>pg_class</c> is
    /// per-database, so the collector names a relation only in the database it is connected to. A row with a
    /// database name and no relation was not resolved here: usually another database's buffers, but the row
    /// does not say which database the collector connected to, so the label claims no more. A row with neither name is a
    /// buffer whose database id is in no <c>pg_database</c> row, which on a stock server is the shared
    /// catalogs (database id 0) and otherwise a database that has since been dropped. Neither is an error.
    /// </summary>
    public static string? RelationLabel(string? databaseName, string? relationName) =>
        !string.IsNullOrEmpty(relationName) ? null
        : string.IsNullOrEmpty(databaseName) ? SharedOrDroppedLabel
        : NotResolvedLabel;

    /* Only the newest collection_time in the window, taken as a whole: the pool totals are consistent within
       one snapshot and not across snapshots, so mixing rows from two of them would produce shares that do
       not add up.

       NULLIF guards both denominators. A pool total of zero cannot happen on a live server, but a division
       that produces a divide-by-zero error instead of a NULL turns an odd sample into a failed read.

       relations_in_snapshot and snapshot_buffers are windows over the whole snapshot, evaluated before LIMIT
       cuts the page (#7455), so a caller learns the real size of what the page is cut from.

       #9508: a snapshot whose per-relation scan was skipped is ONE row with buffers NULL (the relation columns are NULL
       too). count(b.buffers) leaves it out of relations_in_snapshot, NULLS LAST keeps it behind any relation row, and
       relation_breakdown says which kind of snapshot this is: pool_buffers_used NULL means the totals-only row has no
       summary to read the used count from. */
    public const string PgBufferUsageSql = """
        WITH newest AS (
            SELECT max(collection_time) AS at
            FROM pg_buffer_usage
            WHERE server_id = $1
            AND   collection_time > $2
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
            b.collection_time,
            count(b.buffers) OVER ()                                                  AS relations_in_snapshot,
            sum(b.buffers) OVER ()::bigint                                            AS snapshot_buffers,
            CASE
                WHEN b.buffers IS NOT NULL THEN 'relations'
                WHEN b.pool_buffers_used IS NOT NULL THEN 'skipped_over_cap'
                ELSE 'precondition'
            END                                                                       AS relation_breakdown
        FROM pg_buffer_usage AS b
        JOIN newest AS n ON b.collection_time = n.at
        WHERE b.server_id = $1
        ORDER BY b.buffers DESC NULLS LAST
        LIMIT $4
        """;

    public static async Task<List<PgBufferUsageRow>> GetPgBufferUsageAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgBufferUsageRow>();
        await using var command = postgres.CreateCommand(PgBufferUsageSql);
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
                    : DateTime.SpecifyKind(reader.GetDateTime(10), DateTimeKind.Utc),
                RelationsInSnapshot: reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                SnapshotBuffers: reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                RelationBreakdown: reader.IsDBNull(13) ? RelationsListed : reader.GetString(13)));
        }

        return rows;
    }
}
