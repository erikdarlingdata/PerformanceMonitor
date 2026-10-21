using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// The failure half of the PLAN_REGRESSION fact (#6267), Lite's twin of Darling's <c>PgFactCollector.PlanRegressionFailures</c>:
/// a query whose Aborted (timeout) or Exception (error) runs rose. The cost read counts Regular runs only, so a plan that
/// regresses into timeouts or errors was invisible to it. The rule and its constants are the shared
/// <see cref="PlanRegressionFailureRule"/>, the same ones Darling reads.
/// </summary>
public partial class DuckDbFactCollector
{
    /// <summary>
    /// The failure read: $1 server, $2 window start, $3 recent start, $4 recent hours, $5 baseline hours, then the
    /// secondary-database names. The interval dedup is the cost read's (the latest snapshot of each interval, per outcome),
    /// over the non-Regular rows only.
    /// </summary>
    internal static readonly string PlanRegressionFailureSql = $@"
WITH deduped AS
(
    SELECT
        database_name,
        query_id,
        execution_type_desc,
        execution_count,
        last_execution_time,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time, execution_type_desc
            ORDER BY collection_time DESC, execution_count DESC, collection_id DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   execution_type_desc <> 'Regular'
    AND   last_execution_time >= $2
    /*SEC*/
),
agg AS
(
    SELECT database_name, query_id,
           COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Aborted' AND last_execution_time >= $3), 0) AS recent_timeouts,
           COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Aborted' AND last_execution_time < $3), 0) AS baseline_timeouts,
           COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Exception' AND last_execution_time >= $3), 0) AS recent_errors,
           COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Exception' AND last_execution_time < $3), 0) AS baseline_errors
    FROM deduped
    WHERE rn = 1
    GROUP BY database_name, query_id
),
flagged AS
(
    SELECT *,
           (recent_timeouts >= {PlanRegressionFailureRule.MinRecentRuns}
            AND (baseline_timeouts = 0 OR recent_timeouts::DOUBLE / $4 >= {PlanRegressionFailureRule.RateMultiple} * baseline_timeouts::DOUBLE / $5)) AS timeouts_rise,
           (recent_errors >= {PlanRegressionFailureRule.MinRecentRuns}
            AND (baseline_errors = 0 OR recent_errors::DOUBLE / $4 >= {PlanRegressionFailureRule.RateMultiple} * baseline_errors::DOUBLE / $5)) AS errors_rise
    FROM agg
)
SELECT database_name, query_id, recent_timeouts, baseline_timeouts, recent_errors, baseline_errors,
       timeouts_rise, errors_rise,
       (timeouts_rise AND (baseline_timeouts = 0 OR recent_timeouts::DOUBLE / $4 >= {PlanRegressionFailureRule.HighMultiple} * baseline_timeouts::DOUBLE / $5)) AS timeouts_high,
       (errors_rise AND (baseline_errors = 0 OR recent_errors::DOUBLE / $4 >= {PlanRegressionFailureRule.HighMultiple} * baseline_errors::DOUBLE / $5)) AS errors_high
FROM flagged
WHERE timeouts_rise OR errors_rise
ORDER BY (recent_timeouts + recent_errors) DESC, database_name, query_id
LIMIT {PlanRegressionFailureRule.ReadLimit}";

    /// <summary>Reads the queries whose timeouts or errors rose (#6267) on the caller's open connection.</summary>
    private static async Task<List<PlanRegressionFailureRule.Failure>> ReadPlanRegressionFailuresAsync(
        DuckDBConnection connection, AnalysisContext context)
    {
        var windowStart = context.TimeRangeStart.AddDays(-14);
        var recentStart = context.TimeRangeEnd.AddHours(-PlanRegressionFailureRule.RecentHours);
        var baselineHours = Math.Max(1.0, (recentStart - windowStart).TotalHours);

        using var cmd = connection.CreateCommand();
        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = windowStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = recentStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = (double)PlanRegressionFailureRule.RecentHours });
        cmd.Parameters.Add(new DuckDBParameter { Value = baselineHours });
        cmd.CommandText = SecondaryReplicaScope.Apply(PlanRegressionFailureSql, cmd, context, "PLAN_REGRESSION", "database_name", 6);

        var rows = new List<PlanRegressionFailureRule.Failure>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            rows.Add(new PlanRegressionFailureRule.Failure(
                reader.GetString(0), ToInt64(reader.GetValue(1)),
                ToInt64(reader.GetValue(2)), ToInt64(reader.GetValue(3)),
                ToInt64(reader.GetValue(4)), ToInt64(reader.GetValue(5)),
                reader.GetBoolean(6), reader.GetBoolean(7), reader.GetBoolean(8), reader.GetBoolean(9)));
        }

        return rows;
    }
}
