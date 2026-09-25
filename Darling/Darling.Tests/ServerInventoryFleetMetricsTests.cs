/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4227: FinOps Server Inventory's idle-database check used to run once PER SERVER (43 round trips of a raw
/// 7-day <c>v_query_stats</c> scan on a 43-server fleet). These pins hold the fleet-wide, rollup-routed
/// replacement to its shape: one statement for every server's metrics, and the idle check answered from the
/// stitched rollup plus raw only for the two edges it cannot cover.
/// </summary>
public sealed class ServerInventoryFleetMetricsTests
{
    // ── #4227: one statement, not one per server ──

    /// <summary>
    /// THE SOURCE PIN: the loader calls the fleet metrics read EXACTLY ONCE, outside any per-server loop —
    /// the old code called <c>GetServerMetricsAsync(item.ServerId)</c> once per row inside
    /// <c>lanes.Select(async lane =&gt; { foreach (var item in lane) ... })</c>, which is exactly the 43-round-trip
    /// shape #4227 measured. Reverting <c>FinOpsTab.Loaders.cs</c>'s loader to that shape fails this test: the
    /// per-server call site (<c>GetServerMetricsAsync(item.ServerId)</c>) reappears and the zero-argument fleet
    /// call disappears.
    /// </summary>
    [Fact]
    public void LoadFinOpsServerInventoryAsync_CallsTheFleetMetricsReadOnce_NotOncePerServer()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Loaders.cs");

        var loaderStart = source.IndexOf("private async Task LoadFinOpsServerInventoryAsync()", StringComparison.Ordinal);
        Assert.True(loaderStart >= 0, "LoadFinOpsServerInventoryAsync was not found — has it been renamed?");

        var loaderEnd = source.IndexOf("\n    private async void FinOpsRefreshUtilization_Click", loaderStart, StringComparison.Ordinal);
        Assert.True(loaderEnd > loaderStart, "could not find the end of LoadFinOpsServerInventoryAsync — the scan anchor has drifted.");

        var body = source[loaderStart..loaderEnd];

        Assert.DoesNotContain("GetServerMetricsAsync(item.ServerId)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("ViewerReadFanOut.Lanes(servers)", body, StringComparison.Ordinal);

        var callCount = 0;
        var searchFrom = 0;
        while (true)
        {
            var next = body.IndexOf("GetServerMetricsAsync(", searchFrom, StringComparison.Ordinal);
            if (next < 0) break;
            callCount++;
            searchFrom = next + 1;
        }

        Assert.Equal(1, callCount);
    }

    // ── ServerMetricsSql: fleet-wide, not per-server ──

    [Fact]
    public void ServerMetricsSql_IsFleetWide_NoServerIdParameter_GroupedOrLateralPerServer()
    {
        var sql = ViewerDataService.ServerMetricsSql;

        Assert.DoesNotContain("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("FROM servers s", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id", sql, StringComparison.Ordinal);
        Assert.Contains("CROSS JOIN LATERAL", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);

        /* The idle check's own predicate, unrouted: any execution in the window is active. idle_dbs is
           rooted at servers (a LEFT JOIN, not an EXCEPT grouped by server_id), so a server whose every
           known database is active still gets a row — idle_db_count 0, not a missing row that reads
           NULL through the outer LEFT JOIN (the bug the live parity test caught). */
        Assert.Contains("active_dbs AS (", sql, StringComparison.Ordinal);
        Assert.Contains("latest_dbs", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN latest_dbs ld ON ld.server_id = s.server_id", sql, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE ad.database_name IS NULL)", sql, StringComparison.Ordinal);
        Assert.Contains("delta_execution_count > 0", sql, StringComparison.Ordinal);

        /* Only two binds now: cpu cutoff, idle cutoff — server_id is gone. */
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ServerMetricsSqlFor_RoutesTheIdleCheckThroughTheStitchedRollup_PlusTwoRawEdges()
    {
        var coverage = RollupCoverage.Unknown; // no successor known here — legacy-only stitch, still exercises the composer
        var idleCutoff = new DateTime(2026, 3, 1, 4, 27, 0, DateTimeKind.Unspecified);
        var watermark = new DateTime(2026, 3, 7, 22, 0, 0, DateTimeKind.Unspecified);

        var sql = ViewerDataService.ServerMetricsSqlFor(coverage, idleCutoff, watermark);

        /* The raw-only anchor is gone, replaced by the three-arm union. */
        Assert.DoesNotContain(
            "active_dbs AS (\n    SELECT DISTINCT server_id, database_name\n    FROM v_query_stats\n    WHERE collection_time >= $2\n    AND   delta_execution_count > 0\n),",
            sql, StringComparison.Ordinal);

        Assert.Contains("execution_count_sum > 0", sql, StringComparison.Ordinal);
        Assert.Contains("bucket >=", sql, StringComparison.Ordinal);
        var unionCount = System.Text.RegularExpressions.Regex.Matches(sql, "UNION").Count;
        Assert.Equal(2, unionCount); // three arms, two UNIONs joining them

        /* The two edge literals: the ceiling hour of the idle cutoff (05:00 — 04:27 rounds UP to the next
           whole hour, since the partial hour [04:27, 05:00) is exactly the slice the rollup can't answer)
           and the watermark, verbatim. */
        Assert.Contains("TIMESTAMP '2026-03-01 05:00:00", sql, StringComparison.Ordinal);
        Assert.Contains("TIMESTAMP '2026-03-07 22:00:00", sql, StringComparison.Ordinal);

        /* Every other CTE (cpu_24h, mem_latest, grants, storage_totals, latest_dbs) is untouched. */
        Assert.Contains("FROM v_cpu_utilization_stats", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_memory_grant_stats", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ServerMetricsSqlFor_WhenTheIdleCutoffIsAlreadyHourAligned_CeilingEqualsTheCutoff()
    {
        var coverage = RollupCoverage.Unknown;
        var idleCutoff = new DateTime(2026, 3, 1, 4, 0, 0, DateTimeKind.Unspecified);
        var watermark = new DateTime(2026, 3, 7, 22, 0, 0, DateTimeKind.Unspecified);

        var sql = ViewerDataService.ServerMetricsSqlFor(coverage, idleCutoff, watermark);

        Assert.Contains("TIMESTAMP '2026-03-01 04:00:00", sql, StringComparison.Ordinal);
    }

    // ── RollupMaterializationWatermark ──

    [Fact]
    public void WatermarkSql_ReadsCaggWatermark_WithTheSameFiniteSentinelGuardAsCollectionHealth()
    {
        var sql = RollupMaterializationWatermark.WatermarkSql("query_stats_db_interval_hourly");

        Assert.Contains("_timescaledb_functions.cagg_watermark(mat_hypertable_id)", sql, StringComparison.Ordinal);
        Assert.Contains("_timescaledb_catalog.continuous_agg", sql, StringComparison.Ordinal);
        Assert.Contains("isfinite(w)", sql, StringComparison.Ordinal);
        Assert.Contains("'2000-01-01'", sql, StringComparison.Ordinal);
        Assert.Contains("user_view_name = 'query_stats_db_interval_hourly'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void FallbackMaxBucketSql_ReadsTheViewDirectly_NoCatalogAccess()
    {
        var sql = RollupMaterializationWatermark.FallbackMaxBucketSql("query_stats_db_hourly");

        Assert.Equal("SELECT max(bucket) FROM collect.query_stats_db_hourly", sql);
        Assert.DoesNotContain("_timescaledb", sql, StringComparison.Ordinal);
    }
}
