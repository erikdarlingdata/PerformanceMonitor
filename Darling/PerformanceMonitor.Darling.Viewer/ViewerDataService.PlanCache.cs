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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One point on the plan-cache size trend: single-use vs multi-use plan cache size (MB) at a
/// collection instant, summed across every (cacheobjtype, objtype) group — the single-use bloat signal
/// the Dashboard's Plan Cache chart plots.</summary>
public sealed record PlanCacheTrendPoint(DateTime CollectionTime, double SingleUseSizeMb, double MultiUseSizeMb);

/// <summary>One row of the Plan Cache latest-snapshot composition grid: one (cacheobjtype, objtype)
/// group's plan/size breakdown at the most recent collection in the window. <c>OldestPlanCreateTime</c>
/// is the store-wide MIN(creation_time) the collector stamps on every group row identically, so the
/// summary can read it off any row.</summary>
public sealed record PlanCacheSnapshotRow(
    string Cacheobjtype,
    string Objtype,
    int TotalPlans,
    int TotalSizeMb,
    int SingleUsePlans,
    int SingleUseSizeMb,
    int MultiUsePlans,
    int MultiUseSizeMb,
    decimal AvgUseCount,
    int AvgSizeKb,
    DateTime? OldestPlanCreateTime);

/// <summary>The Plan Cache summary strip's figures for the latest snapshot in the window: the TRUE total
/// plan count and single-use plan count (both summed across every group, not just the displayed top-N) and
/// the oldest cached plan's create time — matching the Dashboard, which computes these over the full latest
/// snapshot. <see cref="SingleUsePlans"/> feeds the derived bloat-level badge
/// (<see cref="ViewerDataService.ClassifyPlanCacheBloat"/>).</summary>
public sealed record PlanCacheSummary(long TotalPlans, long SingleUsePlans, DateTime? OldestPlanCreateTime);

/// <summary>The plan-cache bloat classification for the summary badge: the banded severity level plus the
/// paired recommendation, mirroring install/47's report.plan_cache_bloat.</summary>
public sealed record PlanCacheBloat(string Level, string Recommendation);

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The Plan Cache size trend read: single-use vs multi-use cache size (MB) per collection, summed
    /// across every (cacheobjtype, objtype) group, from the <c>v_plan_cache_stats</c> passthrough view —
    /// the Dashboard's Plan Cache chart shape (single-use vs multi-use bloat), windowed on the settable
    /// range. The integer MB sums are CAST to double precision for the typed reader. $1 server_id,
    /// $2 window start, $3 window end (all naive UTC).
    /// </summary>
    public const string PlanCacheTrendSql = """
        SELECT
            collection_time,
            CAST(SUM(single_use_size_mb) AS double precision) AS single_use_mb,
            CAST(SUM(multi_use_size_mb) AS double precision) AS multi_use_mb
        FROM v_plan_cache_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY collection_time
        ORDER BY collection_time
        """;

    /// <summary>
    /// The Plan Cache latest-snapshot composition read: every (cacheobjtype, objtype) group captured at
    /// the most recent collection in the window, ordered by total cache size then plan count, capped at
    /// 30 rows. $1 server_id, $2 start, $3 end (naive UTC).
    /// </summary>
    public const string PlanCacheSnapshotSql = """
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
        LIMIT 30
        """;

    /// <summary>
    /// The Plan Cache summary read: the TRUE total plan count (SUM over EVERY group, uncapped) and the
    /// oldest cached plan's create time (MIN) at the most recent collection in the window — the summary
    /// strip's two figures, computed independently of the top-30 grid read so "Total Plans" is exact even
    /// when the composition grid is capped (matching the Dashboard, which sums the full latest snapshot).
    /// An aggregate with no GROUP BY always returns one row, so an empty window yields (0, NULL).
    /// $1 server_id, $2 start, $3 end (naive UTC).
    /// </summary>
    public const string PlanCacheSummarySql = """
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
        AND   collection_time = (SELECT mx FROM latest)
        """;

    /// <summary>
    /// The plan-cache bloat classification (single-use plan bloat), a pure client-side derivation mirroring
    /// install/47's <c>report.plan_cache_bloat</c> (bloat_level + recommendation) and the Dashboard's
    /// <c>PlanCacheStatsItem.BloatLevel/Recommendation</c>. The ratio is single-use plans / total plans:
    /// &gt; 50% CRITICAL, &gt; 30% HIGH, &gt; 20% MEDIUM, else NORMAL; the recommendation flips to the
    /// "unparameterized queries / Forced Parameterization" hint at the same &gt; 20% threshold. Static + pure
    /// so it is unit-testable without Postgres. An empty cache (<paramref name="totalPlans"/> == 0) is NORMAL.
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

    /// <summary>The single-use vs multi-use plan-cache size trend over the window (empty when none).</summary>
    public async Task<List<PlanCacheTrendPoint>> GetPlanCacheTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new List<PlanCacheTrendPoint>();

        await using var command = _dataSource.CreateCommand(PlanCacheTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new PlanCacheTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2)));
        }

        return result;
    }

    /// <summary>The latest-snapshot plan-cache composition rows (top 30 by size) for the window.</summary>
    public async Task<List<PlanCacheSnapshotRow>> GetPlanCacheSnapshotAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new List<PlanCacheSnapshotRow>();

        await using var command = _dataSource.CreateCommand(PlanCacheSnapshotSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new PlanCacheSnapshotRow(
                Cacheobjtype: reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                Objtype: reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                TotalPlans: reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                TotalSizeMb: reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                SingleUsePlans: reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                SingleUseSizeMb: reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                MultiUsePlans: reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                MultiUseSizeMb: reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                AvgUseCount: reader.IsDBNull(8) ? 0m : reader.GetDecimal(8),
                AvgSizeKb: reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
                OldestPlanCreateTime: reader.IsDBNull(10) ? null : reader.GetDateTime(10)));
        }

        return result;
    }

    /// <summary>The exact total-plans + oldest-plan figures for the summary strip (uncapped), or
    /// (0, null) when the window holds no snapshot.</summary>
    public async Task<PlanCacheSummary> GetPlanCacheSummaryAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(PlanCacheSummarySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new PlanCacheSummary(0, 0, null);
        }

        return new PlanCacheSummary(
            reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
            reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
            reader.IsDBNull(2) ? null : reader.GetDateTime(2));
    }
}
