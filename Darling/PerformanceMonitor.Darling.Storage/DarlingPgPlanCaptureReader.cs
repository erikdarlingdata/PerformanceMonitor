/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads <c>collect.pg_plan_capture</c> — plans captured by <c>auto_explain</c> (#2566).
///
/// <para><b>Grouped by plan, not by capture.</b> The collector reads a bounded tail of the log every cycle,
/// so the same plan is legitimately captured many times — and the overlapping window means the SAME
/// execution can be seen twice. Returning raw captures would rank by how often the reader happened to look.
/// Rows are grouped on <c>(query_id, plan_hash)</c>, which is what makes "this shape ran a lot and is slow"
/// answerable at all. The hash is of the plan's SHAPE (#5114), so captures whose estimates differ share a group, and
/// the counts and durations sum over all of them.</para>
///
/// <para><b>The plan JSON returned here is already redacted</b> — the collector strips it before storage, so
/// there is no un-redacted copy anywhere for a read to leak. Nothing here needs to re-check that, and
/// nothing here should re-derive it.</para>
///
/// <para>Ranked by TOTAL duration rather than max: a plan that takes 40 ms and runs constantly costs more
/// than one that took 900 ms once, and the second is usually a cold cache.</para>
/// </summary>
public static class DarlingPgPlanCaptureReader
{
    /// <param name="Captures">How many times this shape was seen. A count of the CAPTURES, which the
    /// overlapping tail read can inflate — treat it as a frequency signal rather than an execution count;
    /// <c>pg_statement_stats.calls</c> is the authority on that.</param>
    /// <param name="PlanJson">The JSON of the group's latest collection cycle, redacted at collection (literals and query text
    /// never reach the store). Its estimates and counters are that capture's: the hash does not cover them, so
    /// the group's other captures may carry different ones.</param>
    public sealed record PgPlanCaptureRow(
        long QueryId,
        string? PlanHash,
        string? TopNodeType,
        int NodeCount,
        long Captures,
        double TotalDurationMs,
        double MaxDurationMs,
        double AvgDurationMs,
        string? PlanJson,
        DateTime LastSeen);

    /* The hash is of the plan's SHAPE (#5114), so the JSON of one group is NOT identical across its captures: they
       differ in costs, row estimates and runtime counters. The group is ranked and cut first, without the JSON, and
       the LATERAL then fetches the JSON of the group's latest collection cycle for the page only - a real capture's
       own numbers, and kilobyte-sized text is read for the rows returned rather than aggregated over every group.
       Two captures of one plan can share a cycle's timestamp, so max() picks one of them deterministically; the
       lookup is an equality on that timestamp, bounded by the same (server_id, collection_time) index. The outer
       LIMIT repeats the inner one, because the paged-read contract wants every such read to end in its bound. top_node_type and
       node_count are shape properties, so min() and max() over a group are exact. */
    public const string PgPlanCaptureSql = """
        SELECT
            g.query_id,
            g.plan_hash,
            g.top_node_type,
            g.node_count,
            g.captures,
            g.total_duration_ms,
            g.max_duration_ms,
            g.avg_duration_ms,
            latest.plan_json,
            g.last_seen
        FROM (
            SELECT
                query_id,
                plan_hash,
                min(top_node_type)          AS top_node_type,
                max(node_count)             AS node_count,
                count(*)                    AS captures,
                sum(duration_ms)            AS total_duration_ms,
                max(duration_ms)            AS max_duration_ms,
                avg(duration_ms)            AS avg_duration_ms,
                max(collection_time)        AS last_seen
            FROM pg_plan_capture
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            /* The optional queryid pin, IN the SQL rather than filtered client-side over a page (#3533): the
               ranking is by total duration, so a cheap-but-wanted query sits arbitrarily far below the top and
               no page size makes it reachable — filtering a fetched page turned "ranked low" into "was never
               captured". NULL leaves the read as the top-duration page. */
            AND   ($4::bigint IS NULL OR query_id = $4)
            GROUP BY query_id, plan_hash
            ORDER BY sum(duration_ms) DESC
            LIMIT $5
        ) AS g
        LEFT JOIN LATERAL (
            SELECT max(c.plan_json) AS plan_json
            FROM pg_plan_capture AS c
            WHERE c.server_id = $1
            AND   c.collection_time = g.last_seen
            AND   c.query_id IS NOT DISTINCT FROM g.query_id
            AND   c.plan_hash IS NOT DISTINCT FROM g.plan_hash
        ) AS latest ON true
        ORDER BY g.total_duration_ms DESC
        LIMIT $5
        """;

    public static Task<List<PgPlanCaptureRow>> GetPgPlanCaptureAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default) =>
        GetPgPlanCaptureAsync(postgres, serverId, startUtc, endUtc, limit, queryId: null, cancellationToken);

    /// <param name="queryId">Pins the read to one statement's plans, server-side, over the whole window.
    /// Null returns the top page by total duration instead.</param>
    public static async Task<List<PgPlanCaptureRow>> GetPgPlanCaptureAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        long? queryId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgPlanCaptureRow>();
        await using var command = postgres.CreateCommand(PgPlanCaptureSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the BIND, the convention every PostgreSQL read here follows. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        /* Typed explicitly rather than through AddWithValue: DBNull carries no type for Npgsql to infer,
           so an untyped null fails at bind time — the same reason the trend reader's TextOrNull exists. */
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Bigint,
            Value = (object?)queryId ?? DBNull.Value,
        });
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgPlanCaptureRow(
                QueryId: reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                PlanHash: reader.IsDBNull(1) ? null : reader.GetString(1),
                TopNodeType: reader.IsDBNull(2) ? null : reader.GetString(2),
                NodeCount: reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                Captures: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                TotalDurationMs: reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                MaxDurationMs: reader.IsDBNull(6) ? 0 : reader.GetDouble(6),
                AvgDurationMs: reader.IsDBNull(7) ? 0 : Convert.ToDouble(reader.GetValue(7)),
                PlanJson: reader.IsDBNull(8) ? null : reader.GetString(8),
                LastSeen: reader.IsDBNull(9)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(9), DateTimeKind.Utc)));
        }

        return rows;
    }
}
