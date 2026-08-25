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
/// Reads <c>collect.pg_predicate_stats</c> — which columns are filtered on and how badly the planner
/// estimated them (#2603).
///
/// <para><b>Newest per predicate, NOT differenced.</b> Unlike the wait and kernel profiles, these counters
/// are not useful as a rate: the question is "does this column deserve an index", and the answer comes from
/// the accumulated shape of the workload rather than from what happened in the last five minutes. A delta
/// would also fight the sampler, which fires on a fraction of executions.</para>
///
/// <para><b><c>filtered_pct</c> is computed here and guarded.</b> Rows filtered over rows evaluated is the
/// index-candidate ratio, and <c>rows_evaluated</c> can be zero — a predicate sampled at the moment it
/// evaluated nothing. Dividing anyway yields a division error that fails the whole read for one odd row.</para>
///
/// <para><b><c>sample_rate</c> travels to the caller unchanged.</b> The counts are NOT scaled up by it here.
/// Scaling would manufacture precision the sampler never had — a count of 3 at a 1% rate is not 300, it is
/// "three observations, of a sample that catches roughly one in a hundred". The panel says the rate and
/// lets a reader reason about it.</para>
/// </summary>
public static class DarlingPgPredicateStatsReader
{
    /// <param name="FilteredPct">Rows discarded as a percentage of rows evaluated. High with no index is
    /// the index-candidate signal. Null when nothing was evaluated.</param>
    /// <param name="WorstEstimateErrorRatio">How far the planner's row estimate was from reality. Large
    /// means the plan was built on a wrong number, which an index alone will not fix.</param>
    /// <param name="SampleRate">Fraction of executions the extension recorded. Usually not 1 — the default
    /// is <c>1/max_connections</c>.</param>
    public sealed record PgPredicateStatRow(
        string? DatabaseName,
        string? SchemaName,
        string? TableName,
        string? ColumnName,
        string? Operator,
        long QueryId,
        long SampleCount,
        long RowsEvaluated,
        long RowsFiltered,
        double? FilteredPct,
        double WorstEstimateErrorRatio,
        double SampleRate,
        DateTime CaptureTime);

    /* DISTINCT ON includes database_name because this collector runs per database and a column name is only
       unique within one - the #2599 lesson applied at the read as well as the write. */
    public const string PgPredicateStatsSql = """
        SELECT database_name, schema_name, table_name, column_name, operator, query_id,
               sample_count, rows_evaluated, rows_filtered,
               CASE WHEN rows_evaluated > 0
                    THEN (rows_filtered::double precision / rows_evaluated) * 100.0
               END AS filtered_pct,
               worst_estimate_error_ratio, sample_rate, collection_time
        FROM (
            SELECT DISTINCT ON (database_name, schema_name, table_name, column_name, operator, query_id)
                   database_name, schema_name, table_name, column_name, operator, query_id,
                   sample_count, rows_evaluated, rows_filtered,
                   worst_estimate_error_ratio, sample_rate, collection_time
            FROM pg_predicate_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, schema_name, table_name, column_name, operator, query_id,
                     collection_time DESC
        ) AS latest
        ORDER BY rows_filtered DESC, worst_estimate_error_ratio DESC NULLS LAST
        LIMIT $4
        """;

    public static async Task<List<PgPredicateStatRow>> GetPgPredicateStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgPredicateStatRow>();
        await using var command = postgres.CreateCommand(PgPredicateStatsSql);
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the BIND, the convention every PostgreSQL read here follows. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgPredicateStatRow(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                SchemaName: reader.IsDBNull(1) ? null : reader.GetString(1),
                TableName: reader.IsDBNull(2) ? null : reader.GetString(2),
                ColumnName: reader.IsDBNull(3) ? null : reader.GetString(3),
                Operator: reader.IsDBNull(4) ? null : reader.GetString(4),
                QueryId: reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                SampleCount: reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                RowsEvaluated: reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                RowsFiltered: reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                FilteredPct: reader.IsDBNull(9) ? null : reader.GetDouble(9),
                WorstEstimateErrorRatio: reader.IsDBNull(10) ? 0 : reader.GetDouble(10),
                SampleRate: reader.IsDBNull(11) ? 1.0 : reader.GetDouble(11),
                CaptureTime: reader.IsDBNull(12)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(12), DateTimeKind.Utc)));
        }

        return rows;
    }
}
