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

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /* The Plan Cache readers — the Lite (DuckDB) port of the Darling viewer's ViewerDataService.PlanCache
       reads. plan_cache_stats is a point-in-time snapshot collector: one row per (cacheobjtype, objtype)
       group per collection, each stamped with the store-wide oldest_plan_create_time. The trend sums
       single-use vs multi-use size (MB) across every group per collection (the single-use bloat signal),
       the composition grid breaks the latest snapshot down per group, and the summary reads the TRUE
       total/single-use plan counts (uncapped SUM, independent of the top-30 grid). All read the
       v_plan_cache_stats archive view; collection_time is UTC (GetTimeRange). */

    /// <summary>
    /// The Plan Cache size trend: single-use vs multi-use cache size (MB) per collection, summed across
    /// every (cacheobjtype, objtype) group — the Dashboard's Plan Cache chart shape (single-use bloat).
    /// </summary>
    public async Task<List<PlanCacheTrendPoint>> GetPlanCacheTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetPlanCacheTrendAsync", "v_plan_cache_stats single/multi-use size trend");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
SELECT
    collection_time,
    CAST(SUM(single_use_size_mb) AS DOUBLE PRECISION) AS single_use_mb,
    CAST(SUM(multi_use_size_mb) AS DOUBLE PRECISION) AS multi_use_mb
FROM v_plan_cache_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
GROUP BY collection_time
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<PlanCacheTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new PlanCacheTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                SingleUseSizeMb = reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                MultiUseSizeMb = reader.IsDBNull(2) ? 0 : reader.GetDouble(2)
            });
        }

        return items;
    }

    /// <summary>
    /// The Plan Cache latest-snapshot composition grid: every (cacheobjtype, objtype) group captured at
    /// the most recent collection in the window, ordered by total cache size then plan count, capped at 30.
    /// </summary>
    public async Task<List<PlanCacheSnapshotRow>> GetPlanCacheSnapshotAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetPlanCacheSnapshotAsync", "v_plan_cache_stats latest composition");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
WITH latest AS
(
    SELECT MAX(collection_time) AS mx
    FROM v_plan_cache_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT
    cacheobjtype,
    objtype,
    total_plans,
    total_size_mb,
    single_use_plans,
    single_use_size_mb,
    multi_use_plans,
    multi_use_size_mb,
    avg_use_count,
    avg_size_kb,
    oldest_plan_create_time
FROM v_plan_cache_stats
WHERE server_id = $1
AND   collection_time = (SELECT mx FROM latest)
ORDER BY total_size_mb DESC, total_plans DESC
LIMIT 30";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<PlanCacheSnapshotRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new PlanCacheSnapshotRow
            {
                Cacheobjtype = reader.IsDBNull(0) ? "" : reader.GetString(0),
                Objtype = reader.IsDBNull(1) ? "" : reader.GetString(1),
                TotalPlans = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                TotalSizeMb = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                SingleUsePlans = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                SingleUseSizeMb = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                MultiUsePlans = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                MultiUseSizeMb = reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                AvgUseCount = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                AvgSizeKb = reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
                OldestPlanCreateTime = reader.IsDBNull(10) ? null : reader.GetDateTime(10)
            });
        }

        return items;
    }

    /// <summary>
    /// The Plan Cache summary read: the TRUE total plan count and single-use plan count (SUM over EVERY
    /// group, uncapped) plus the oldest cached plan's create time (MIN) at the most recent collection in
    /// the window. Computed independently of the top-30 grid so "Total Plans" is exact even when the grid
    /// is capped. An aggregate with no GROUP BY always returns one row, so an empty window yields (0,0,null).
    /// </summary>
    public async Task<PlanCacheSummary> GetPlanCacheSummaryAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetPlanCacheSummaryAsync", "v_plan_cache_stats summary totals");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
WITH latest AS
(
    SELECT MAX(collection_time) AS mx
    FROM v_plan_cache_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT
    COALESCE(SUM(total_plans), 0) AS total_plans,
    COALESCE(SUM(single_use_plans), 0) AS single_use_plans,
    MIN(oldest_plan_create_time) AS oldest_plan_create_time
FROM v_plan_cache_stats
WHERE server_id = $1
AND   collection_time = (SELECT mx FROM latest)";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return new PlanCacheSummary();
        }

        return new PlanCacheSummary
        {
            TotalPlans = reader.IsDBNull(0) ? 0 : ToInt64(reader.GetValue(0)),
            SingleUsePlans = reader.IsDBNull(1) ? 0 : ToInt64(reader.GetValue(1)),
            OldestPlanCreateTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2)
        };
    }

    /// <summary>
    /// The plan-cache bloat classification (single-use plan bloat), a pure client-side derivation mirroring
    /// install/47's <c>report.plan_cache_bloat</c> and the Darling viewer's
    /// <c>ViewerDataService.ClassifyPlanCacheBloat</c>. The ratio is single-use plans / total plans:
    /// &gt; 50% CRITICAL, &gt; 30% HIGH, &gt; 20% MEDIUM, else NORMAL; the recommendation flips to the
    /// "unparameterized queries / Forced Parameterization" hint at the same &gt; 20% threshold. An empty
    /// cache (totalPlans == 0) is NORMAL.
    /// </summary>
    public static PlanCacheBloat ClassifyPlanCacheBloat(long totalPlans, long singleUsePlans)
    {
        double singleUsePercent = totalPlans > 0 ? singleUsePlans * 100.0 / totalPlans : 0.0;

        var level =
            singleUsePercent > 50 ? "CRITICAL" :
            singleUsePercent > 30 ? "HIGH" :
            singleUsePercent > 20 ? "MEDIUM" :
            "NORMAL";

        var recommendation = singleUsePercent > 20
            ? "Check for unparameterized queries / Consider Forced Parameterization"
            : "Plan cache composition is healthy";

        return new PlanCacheBloat(level, recommendation);
    }
}

/// <summary>One point on the plan-cache size trend: single-use vs multi-use plan cache size (MB) at a
/// collection instant, summed across every (cacheobjtype, objtype) group.</summary>
public class PlanCacheTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public double SingleUseSizeMb { get; set; }
    public double MultiUseSizeMb { get; set; }
}

/// <summary>One row of the Plan Cache latest-snapshot composition grid: one (cacheobjtype, objtype)
/// group's plan/size breakdown at the most recent collection in the window.</summary>
public class PlanCacheSnapshotRow
{
    public string Cacheobjtype { get; set; } = "";
    public string Objtype { get; set; } = "";
    public int TotalPlans { get; set; }
    public int TotalSizeMb { get; set; }
    public int SingleUsePlans { get; set; }
    public int SingleUseSizeMb { get; set; }
    public int MultiUsePlans { get; set; }
    public int MultiUseSizeMb { get; set; }
    public double AvgUseCount { get; set; }
    public int AvgSizeKb { get; set; }
    public DateTime? OldestPlanCreateTime { get; set; }
}

/// <summary>The Plan Cache summary strip's figures for the latest snapshot in the window: the TRUE total
/// plan count and single-use plan count (both summed across every group, not just the displayed top-N)
/// and the oldest cached plan's create time.</summary>
public class PlanCacheSummary
{
    public long TotalPlans { get; set; }
    public long SingleUsePlans { get; set; }
    public DateTime? OldestPlanCreateTime { get; set; }
}

/// <summary>The plan-cache bloat classification for the summary badge: the banded severity level plus the
/// paired recommendation, mirroring install/47's report.plan_cache_bloat.</summary>
public sealed record PlanCacheBloat(string Level, string Recommendation);
