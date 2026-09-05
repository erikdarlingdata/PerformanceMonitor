/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The DuckDB read behind the forced-plan-failure alert (#2157) — Darling's Postgres query ported, kept
/// deliberately shape-for-shape with it so the two apps can never disagree about what counts as a new
/// failure.
/// </summary>
public sealed partial class LocalDataService
{
    /// <summary>
    /// Forced plans whose failure counter ROSE between the two most recent collections that carried the
    /// plan. $1 server_id.
    ///
    /// <para>query_store_stats holds one row per plan PER INTERVAL per collection and the forcing columns
    /// are plan-level attributes repeated across them, so each (plan, collection_time) collapses to one
    /// value with MAX before any comparison. The two-hour window bounds the scan: a plan not collected
    /// inside it is not failing right now, and Query Store's 900s flush cadence means an active plan
    /// appears several times within it.</para>
    ///
    /// <para>The <c>&gt;</c> is what makes this a delta read — equal counters are silence, and a LOWER
    /// counter (an unforce/re-force reset) is silence rather than a negative delta.</para>
    ///
    /// <para>The window's lower bound is BOUND as <c>$2</c>, not written <c>now() - INTERVAL '2 hours'</c>.
    /// <c>collection_time</c> is a naive-UTC <c>TIMESTAMP</c> and <c>now()</c> is <c>TIMESTAMP WITH TIME
    /// ZONE</c>, so the mixed comparison resolves the naive side in the session's TimeZone — which DuckDB
    /// takes from the host — and the window silently widens west of UTC and narrows to nothing east of it.
    /// Darling's twin carries the same bound for the same reason; keeping the two shape-for-shape is what
    /// stops the apps disagreeing about what counts as a new failure.</para>
    /// </summary>
    /// <summary>How far back <see cref="ForcePlanFailuresSql"/> looks for a plan's two most recent
    /// collections. Bound as a parameter rather than written into the SQL — see that query's remarks.</summary>
    internal static readonly TimeSpan ForcePlanFailureWindow = TimeSpan.FromHours(2);

    public const string ForcePlanFailuresSql = @"
WITH per_collection AS (
    SELECT
        qs.database_name,
        qs.query_id,
        qs.plan_id,
        qs.collection_time,
        MAX(COALESCE(qs.force_failure_count, 0)) AS failures,
        MAX(CASE WHEN qs.is_forced_plan THEN 1 ELSE 0 END) AS forced,
        MAX(COALESCE(qs.plan_forcing_type, '')) AS forcing_type,
        MAX(COALESCE(qs.last_force_failure_reason, '')) AS reason
    FROM v_query_store_stats AS qs
    WHERE qs.server_id = $1
    AND   qs.collection_time > $2
    GROUP BY qs.database_name, qs.query_id, qs.plan_id, qs.collection_time
),
ranked AS (
    SELECT
        pc.*,
        ROW_NUMBER() OVER (PARTITION BY pc.database_name, pc.query_id, pc.plan_id ORDER BY pc.collection_time DESC) AS rn
    FROM per_collection AS pc
)
SELECT
    n.database_name,
    n.query_id,
    n.plan_id,
    n.forcing_type,
    n.reason,
    n.failures - p.failures AS failure_delta,
    n.failures AS total_failures
FROM ranked AS n
JOIN ranked AS p
  ON  p.database_name = n.database_name
  AND p.query_id = n.query_id
  AND p.plan_id = n.plan_id
  AND p.rn = 2
WHERE n.rn = 1
AND   n.forced = 1
AND   n.failures > p.failures
ORDER BY n.database_name, n.query_id, n.plan_id";

    /// <summary>Runs <see cref="ForcePlanFailuresSql"/> for one server.</summary>
    public async Task<List<ForcePlanFailureInfo>> GetForcePlanFailuresAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = ForcePlanFailuresSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow - ForcePlanFailureWindow });

        var items = new List<ForcePlanFailureInfo>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ForcePlanFailureInfo
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryId = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                PlanId = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                ForcingType = reader.IsDBNull(3) ? "" : reader.GetString(3),
                FailureReason = reader.IsDBNull(4) ? "" : reader.GetString(4),
                FailureDelta = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                TotalFailures = reader.IsDBNull(6) ? 0 : reader.GetInt64(6)
            });
        }

        return items;
    }
}
