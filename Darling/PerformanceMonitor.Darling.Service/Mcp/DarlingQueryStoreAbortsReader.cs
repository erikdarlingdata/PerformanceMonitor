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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #6073: the store read behind <c>get_query_store_aborts</c>. For one server and window it counts each query's Query Store
/// runs by outcome (Regular, Aborted for a client timeout or cancel, Exception) and ranks the queries by aborted plus exception
/// runs, then by their share of the query's own runs.
///
/// <para><b>Same source and window logic as <c>get_query_store_top</c>.</b> The raw read dedupes the open interval's repeated
/// snapshots exactly as <c>DarlingDataReader.QueryStoreTopSql</c> does (same partition, newest snapshot wins), and the table read
/// takes <c>query_store_interval_wide</c> under the same gate, the same read-only REPEATABLE READ snapshot, the same
/// <c>first_execution_time</c> floor and ceiling (#4605, #5523) and the same narrowed edge margin (#5882), so the two reads count
/// one query's runs the same way. It does NOT read the daily summary: it always counts the interval rows, so its totals are never
/// the daily summary's approximate ones.</para>
///
/// <para>Rows are grouped by (database, query_id, query_hash), so a query's plans, replica roles and outcome rows fold into one
/// line. $1 server_id, $2 window start, $3 window end (naive UTC), $4 row cap, $5 databases (<c>text[]</c>, NULL for all).</para>
/// </summary>
internal static class DarlingQueryStoreAbortsReader
{
    internal readonly record struct AbortRow(
        string DatabaseName, long QueryId, string QueryHash, long RegularRuns, long AbortedRuns, long ExceptionRuns, long TotalRuns,
        double? AvgAbortedDurationMs, DateTime? LastFailedTime, string? QueryText);

    /// <summary>The rows, the interval-table plan when that table served them (null when the raw tier did).</summary>
    internal readonly record struct AbortsRead(List<AbortRow> Rows, QueryStoreIntervalWide.WideReadPlan? Table);

    /// <summary>The raw dedupe: <c>DarlingDataReader.QueryStoreTopSql</c>'s own, with no outcome filter (the denominator needs the Regular rows).</summary>
    private const string RawPrefix = """
        WITH deduped AS (
            SELECT
                database_name, query_id, query_hash, execution_type_desc, execution_count, avg_duration_us, last_execution_time,
                ROW_NUMBER() OVER
                (
                    PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                    ORDER BY collection_time DESC, execution_count DESC
                ) AS rn
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
        ),
        """;

    /// <summary>The interval table's head: one row per interval identity already, with the top read's own first_execution_time bounds.</summary>
    private static readonly string TablePrefix = $$"""
        WITH deduped AS (
            SELECT
                database_name, query_id, query_hash, execution_type_desc, execution_count, avg_duration_us, last_execution_time,
                1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= $3 + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
        ),
        """;

    /// <summary>
    /// Everything from the per-query fold down. The averages are execution-weighted over the aborted rows only. A query with no
    /// aborted or exception run does not pass the HAVING, so a Regular-only query never appears. The page is cut before the text
    /// lookup, so the lateral runs for the rows kept only.
    /// </summary>
    private const string Tail = $$"""
        ranked AS (
            SELECT
                database_name,
                query_id,
                query_hash,
                CAST(COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Regular'), 0) AS bigint) AS regular_runs,
                CAST(COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Aborted'), 0) AS bigint) AS aborted_runs,
                CAST(COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Exception'), 0) AS bigint) AS exception_runs,
                CAST(COALESCE(SUM(execution_count), 0) AS bigint) AS total_runs,
                SUM(execution_count * CAST(avg_duration_us AS double precision)) FILTER (WHERE execution_type_desc = 'Aborted')
                    / NULLIF(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Aborted'), 0) / 1000.0 AS avg_aborted_duration_ms,
                MAX(last_execution_time) FILTER (WHERE execution_type_desc IN ('Aborted', 'Exception')) AS last_failed_time
            FROM deduped
            WHERE rn = 1
            GROUP BY database_name, query_id, query_hash
            HAVING COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc IN ('Aborted', 'Exception')), 0) > 0
        ),
        page AS (
            SELECT
                r.*,
                r.aborted_runs + r.exception_runs AS failed_runs,
                CAST(r.aborted_runs + r.exception_runs AS double precision) / NULLIF(r.total_runs, 0) AS failed_share
            FROM ranked AS r
            /* #6665: the monitor's own statement is left out before the cap, by the shared read-side predicate. Its text is only in
               query_store_text on Darling (the collector's drop never fires), and only the few queries with an aborted or failed run
               reach this point, so the key lookup is cheap. Rows from before the separate text fetch carry inline text the collector
               already filtered, so there is no inline fallback here. */
            WHERE NOT EXISTS (SELECT 1 FROM query_store_text AS x
                              WHERE x.server_id = $1
                              AND   x.database_name = r.database_name
                              AND   x.query_id = r.query_id
                              AND   {{QueryStoreSelfStatement.SelfOfStoredText}})
            ORDER BY r.aborted_runs + r.exception_runs DESC,
                     CAST(r.aborted_runs + r.exception_runs AS double precision) / NULLIF(r.total_runs, 0) DESC NULLS LAST,
                     r.database_name, r.query_id, r.query_hash
            LIMIT $4
        )
        SELECT
            p.database_name,
            p.query_id,
            p.query_hash,
            p.regular_runs,
            p.aborted_runs,
            p.exception_runs,
            p.total_runs,
            p.avg_aborted_duration_ms,
            p.last_failed_time,
            t.query_text
        FROM page AS p
        LEFT JOIN LATERAL (
            /* The top read's text lookup: the collector's text table first, then the newest inline text inside the read's own window. */
            SELECT COALESCE(
                       (
                           SELECT x.query_sql_text
                           FROM query_store_text AS x
                           WHERE x.server_id = $1
                           AND   x.database_name = p.database_name
                           AND   x.query_id = p.query_id
                       ),
                       (
                           SELECT s.query_text
                           FROM query_store_stats AS s
                           WHERE s.server_id = $1
                           AND   s.query_id = p.query_id
                           AND   s.database_name = p.database_name
                           AND   s.collection_time >= $2
                           AND   s.collection_time <= $3
                           AND   s.query_text IS NOT NULL
                           ORDER BY s.collection_time DESC, s.collection_id DESC
                           LIMIT 1
                       )
                   ) AS query_text
        ) AS t ON TRUE
        ORDER BY p.failed_runs DESC, p.failed_share DESC NULLS LAST, p.database_name, p.query_id, p.query_hash
        """;

    internal const string RawSql = RawPrefix + "\n" + Tail;

    internal static readonly string TableSql = TablePrefix + "\n" + Tail;

    /// <summary>
    /// Reads <paramref name="limit"/> rows. The caller asks for one more than it shows so a cut page is observed, not inferred.
    /// The interval table is tried first under <c>get_query_store_top</c>'s own gate (<see cref="DarlingDataReader.QueryStoreTopMayReadTable"/>
    /// and <see cref="QueryStoreIntervalWide.ResolveReadAsync"/>); a gate that says raw, or any fault before the rows are read, reads the raw tier.
    /// </summary>
    internal static async Task<AbortsRead> ReadAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit, DatabaseFilter databases,
        CancellationToken cancellationToken)
    {
        if (DarlingDataReader.QueryStoreTopMayReadTable(startUtc, endUtc))
        {
            var table = await TryReadTableAsync(postgres, serverId, startUtc, endUtc, limit, databases, cancellationToken);
            if (table is { } served)
            {
                ReadScope.NoteSource(ReadScope.SourceIntervalTable);
                ReadScope.NoteRows(served.Rows.Count);
                return served;
            }
        }

        List<AbortRow> rows;
        await using (var command = postgres.CreateCommand(RawSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            Bind(command, serverId, startUtc, endUtc, limit, databases);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            rows = await ReadRowsAsync(reader, cancellationToken);
        }

        ReadScope.NoteSource(ReadScope.SourceRaw);
        ReadScope.NoteRows(rows.Count);
        return new AbortsRead(rows, null);
    }

    private static async Task<AbortsRead?> TryReadTableAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit, DatabaseFilter databases,
        CancellationToken cancellationToken)
    {
        var gateDecided = false;
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
            await using (var readOnly = new NpgsqlCommand(DarlingDataReader.SetTransactionReadOnlySql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                await readOnly.ExecuteNonQueryAsync(cancellationToken);
            }

            var plan = await QueryStoreIntervalWide.ResolveReadAsync(
                connection, serverId, startUtc, endUtc, endUtc, DarlingDataReader.QueryStoreTopMinWindow,
                McpCommandDeadlines.ReadSeconds, ReadScope.Current?.Logger, legacyRows: QueryStoreIntervalWide.LegacyRows.Allow, cancellationToken);
            if (!plan.UseTable)
            {
                if (plan.DecisionFailed)
                {
                    ReadScope.Note(ReadFallback.GateFailed);
                }

                return null;
            }

            gateDecided = true;
            var sql = TableSql;
            var edgeMargin = await QueryStoreIntervalWide.ResolveEdgeMarginAsync(
                connection, serverId, plan.ReadStart, endUtc, McpCommandDeadlines.ReadSeconds, cancellationToken);
            if (edgeMargin < QueryStoreIntervalWide.PurgeEdgeMargin)
            {
                sql = QueryStoreIntervalWide.WithEdgeMargin(sql, QueryStoreIntervalWide.EdgeMarginSqlFor(edgeMargin));
            }

            List<AbortRow> rows;
            await using (var command = new NpgsqlCommand(sql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                Bind(command, serverId, plan.ReadStart, endUtc, limit, databases);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                rows = await ReadRowsAsync(reader, cancellationToken);
            }

            return new AbortsRead(rows, plan);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ReadScope.NoteFallback(gateDecided ? ReadFallback.FallbackRaw : ReadFallback.GateFailed, "#6073 Query Store aborts table read", ex);
            return null;
        }
    }

    private static void Bind(NpgsqlCommand command, int serverId, DateTime start, DateTime end, int limit, DatabaseFilter databases)
    {
        DarlingMcpReadParameters.AddWindow(command, serverId, start, end);
        DarlingMcpReadParameters.AddInt(command, limit);
        command.Parameters.Add(databases.Parameter());
    }

    private static async Task<List<AbortRow>> ReadRowsAsync(NpgsqlDataReader reader, CancellationToken cancellationToken)
    {
        var rows = new List<AbortRow>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new AbortRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5), reader.GetInt64(6),
                reader.IsDBNull(7) ? null : reader.GetDouble(7),
                reader.IsDBNull(8) ? null : reader.GetDateTime(8),
                reader.IsDBNull(9) ? null : reader.GetString(9)));
        }

        return rows;
    }
}
