/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// The failure half of the PLAN_REGRESSION fact (#6267): a query whose Aborted (timeout) or Exception (error) runs rose.
/// The cost read counts Regular runs only, so a plan that regresses into timeouts or errors was invisible to it, and its
/// successful runs can even look cheaper because the slow runs that hit the timeout drop out (Erik, 2026-10-21: "not
/// counting failures and not showing timeout metrics misses very important query characteristics and i would call it
/// downright misleading").
///
/// <para>Same rule as #6260's regressions read, with the same constants
/// (<see cref="PlanRegressionFailureRule.MinRecentRuns"/>, <see cref="PlanRegressionFailureRule.RateMultiple"/>,
/// <see cref="PlanRegressionFailureRule.HighMultiple"/>): a group rises when its per-hour rate in the recent window
/// is at least the multiple of the baseline's (or any at all where the baseline had none) with at least the minimum number of
/// runs in the recent window. The recent window is the last <see cref="PlanRegressionFailureRule.RecentHours"/> hours of the
/// pass, the baseline the rest of the 14-day window.</para>
///
/// <para><b>Data.</b> The daily totals and <c>query_store_interval_latest</c> hold Regular intervals only, so the read goes to
/// <c>query_store_interval_wide</c> with <see cref="QueryStoreRegressionBaseline.NonRegularPredicate"/>, the predicate of the
/// partial background index (<see cref="QueryStoreBackgroundIndexes.WideNonRegularFirstExecIndexName"/>), so the planner can
/// read only those rows. Where the wide table's gate says raw, the read dedupes the raw rows itself.</para>
/// </summary>
public sealed partial class PgFactCollector
{
    private static readonly string s_failureHead = $$"""
        WITH src AS (
            SELECT database_name, query_id, execution_type_desc, execution_count, last_execution_time
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   {{QueryStoreRegressionBaseline.NonRegularPredicate}}
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   last_execution_time >= $2
            {{PgSecondaryReplicaScope.Marker}}
        ),
        """;

    private static readonly string s_failureRawHead = $$"""
        WITH src AS (
            SELECT database_name, query_id, execution_type_desc, execution_count, last_execution_time
            FROM (
                SELECT database_name, query_id, execution_type_desc, execution_count, last_execution_time,
                       ROW_NUMBER() OVER
                       (
                           PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                           ORDER BY collection_time DESC, execution_count DESC
                       ) AS rn
                FROM query_store_stats
                WHERE server_id = $1
                AND   {{QueryStoreRegressionBaseline.NonRegularPredicate}}
                AND   collection_time >= $6
                {{PgSecondaryReplicaScope.Marker}}
            ) AS d
            WHERE rn = 1
            AND   last_execution_time >= $2
        ),
        """;

    private static readonly string s_failureTail = $$"""
        agg AS (
            SELECT database_name, query_id,
                   COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Aborted' AND last_execution_time >= $3), 0) AS recent_timeouts,
                   COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Aborted' AND last_execution_time < $3), 0) AS baseline_timeouts,
                   COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Exception' AND last_execution_time >= $3), 0) AS recent_errors,
                   COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Exception' AND last_execution_time < $3), 0) AS baseline_errors
            FROM src
            GROUP BY database_name, query_id
        ),
        flagged AS (
            SELECT *,
                   (recent_timeouts >= {{PlanRegressionFailureRule.MinRecentRuns}}
                    AND (baseline_timeouts = 0 OR recent_timeouts::double precision / $4 >= {{PlanRegressionFailureRule.RateMultiple}} * baseline_timeouts::double precision / $5)) AS timeouts_rise,
                   (recent_errors >= {{PlanRegressionFailureRule.MinRecentRuns}}
                    AND (baseline_errors = 0 OR recent_errors::double precision / $4 >= {{PlanRegressionFailureRule.RateMultiple}} * baseline_errors::double precision / $5)) AS errors_rise
            FROM agg
        )
        SELECT database_name, query_id, recent_timeouts, baseline_timeouts, recent_errors, baseline_errors,
               timeouts_rise, errors_rise,
               (timeouts_rise AND (baseline_timeouts = 0 OR recent_timeouts::double precision / $4 >= {{PlanRegressionFailureRule.HighMultiple}} * baseline_timeouts::double precision / $5)) AS timeouts_high,
               (errors_rise AND (baseline_errors = 0 OR recent_errors::double precision / $4 >= {{PlanRegressionFailureRule.HighMultiple}} * baseline_errors::double precision / $5)) AS errors_high
        FROM flagged
        WHERE timeouts_rise OR errors_rise
        ORDER BY (recent_timeouts + recent_errors) DESC, database_name, query_id
        LIMIT {{PlanRegressionFailureRule.ReadLimit}}
        """;

    /// <summary>The failure read over the wide table: $1 server, $2 window start, $3 recent start, $4 recent hours, $5 baseline hours.</summary>
    internal static readonly string PlanRegressionFailureWideSql = s_failureHead + s_failureTail;

    /// <summary>The failure read over the raw rows: the same, with $6 the collection bound.</summary>
    internal static readonly string PlanRegressionFailureRawSql = s_failureRawHead + s_failureTail;

    /// <summary>
    /// Reads the queries whose timeouts or errors rose (#6267), on the connection, as an ordinary statement (the cost read's
    /// snapshot is gone by now). The wide table when its gate says so, raw otherwise.
    /// </summary>
    private async Task<List<PlanRegressionFailureRule.Failure>> ReadPlanRegressionFailuresAsync(
        NpgsqlConnection connection, AnalysisContext context, DateTime windowStart, DateTime collectionBound)
    {
        var recentStart = AsNaive(context.TimeRangeEnd).AddHours(-PlanRegressionFailureRule.RecentHours);
        var baselineHours = Math.Max(1.0, (recentStart - windowStart).TotalHours);

        var useWide = false;
        try
        {
            (useWide, _) = await QueryStoreIntervalWide.ReadsTableAsync(
                connection, context.ServerId, windowStart, AsNaive(context.TimeRangeEnd), null,
                QueryStoreIntervalWide.TopMinWindow, FactCommandTimeoutSeconds, _logger, context.CancellationToken);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* The gate failed: the raw read is whole, just slower. */
            _logger?.LogDebug(ex, "PLAN_REGRESSION failure read: the interval table's gate failed for server {ServerId}; reading raw", context.ServerId);
        }

        using var cmd = new NpgsqlCommand(useWide ? PlanRegressionFailureWideSql : PlanRegressionFailureRawSql, connection)
        {
            CommandTimeout = FactCommandTimeoutSeconds
        };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, windowStart);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, recentStart);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Double, (double)PlanRegressionFailureRule.RecentHours);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Double, baselineHours);
        if (!useWide)
        {
            cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, collectionBound);
        }

        PgSecondaryReplicaScope.Apply(cmd, context, "PLAN_REGRESSION", "database_name");

        var rows = new List<PlanRegressionFailureRule.Failure>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            rows.Add(new PlanRegressionFailureRule.Failure(
                reader.GetString(0), reader.GetInt64(1),
                reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5),
                reader.GetBoolean(6), reader.GetBoolean(7), reader.GetBoolean(8), reader.GetBoolean(9)));
        }

        return rows;
    }
}
