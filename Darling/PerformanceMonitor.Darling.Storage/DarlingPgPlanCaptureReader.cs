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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads <c>collect.pg_plan_capture</c> — plans captured by <c>auto_explain</c> (#2566).
///
/// <para><b>Grouped by plan, not by capture.</b> The collector reads a bounded tail of the log every cycle,
/// so the same plan is legitimately captured many times — and the overlapping window means the SAME
/// execution can be seen twice. Returning raw captures would rank by how often the reader happened to look.
/// Rows are grouped on <c>(query_id, plan_hash)</c>, which is what makes "this shape ran a lot and is slow"
/// answerable at all.</para>
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
    /// <param name="PlanJson">Redacted at collection. Literals and query text never reach the store.</param>
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

    /* DISTINCT ON inside the aggregate is not what is wanted here: the JSON is identical for a given
       plan_hash by construction (the hash is OF the redacted JSON), so any row's copy will do and min()
       avoids a second scan to pick one. */
    public const string PgPlanCaptureSql = """
        SELECT
            query_id,
            plan_hash,
            min(top_node_type)          AS top_node_type,
            max(node_count)             AS node_count,
            count(*)                    AS captures,
            sum(duration_ms)            AS total_duration_ms,
            max(duration_ms)            AS max_duration_ms,
            avg(duration_ms)            AS avg_duration_ms,
            min(plan_json)              AS plan_json,
            max(collection_time)        AS last_seen
        FROM pg_plan_capture
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY query_id, plan_hash
        ORDER BY sum(duration_ms) DESC
        LIMIT $4
        """;

    public static async Task<List<PgPlanCaptureRow>> GetPgPlanCaptureAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgPlanCaptureRow>();
        await using var command = postgres.CreateCommand(PgPlanCaptureSql);
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the BIND, the convention every PostgreSQL read here follows. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
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
