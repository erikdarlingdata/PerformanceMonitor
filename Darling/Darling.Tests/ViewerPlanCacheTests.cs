/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Plan Cache sub-tab's two reads against the Darling store contract (no live Postgres): the
/// size trend (single-use vs multi-use MB, summed across every cacheobjtype/objtype group per collection,
/// the Dashboard's single-use bloat shape) and the latest-snapshot composition read (the most recent
/// collection in the window, per group, top by size). Both run on the <c>v_plan_cache_stats</c>
/// passthrough view.
/// </summary>
public sealed class ViewerPlanCacheSqlTests
{
    [Fact]
    public void PlanCacheTrendSql_SumsSingleVsMultiUseMb_PerCollection_ThenBucketed_OrderedByBucket()
    {
        var sql = ViewerDataService.PlanCacheTrendSql;

        Assert.Contains("FROM v_plan_cache_stats", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(single_use_size_mb) AS double precision)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(multi_use_size_mb) AS double precision)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collection_time", sql, StringComparison.Ordinal);

        /* #4349: the per_collection CTE still groups by collection_time, but the outer read
         * buckets those rows by bucket_start and orders the bucketed result, not the raw rows. */
        Assert.Contains("AS bucket_start", sql, StringComparison.Ordinal);

        var normalized = sql.ReplaceLineEndings("\n");
        var outer = normalized[(normalized.LastIndexOf("\nFROM ", StringComparison.Ordinal) + 1)..];
        var groupByIndex = outer.IndexOf("GROUP BY 1", StringComparison.Ordinal);
        var orderByIndex = outer.IndexOf("ORDER BY 1", StringComparison.Ordinal);
        Assert.True(groupByIndex >= 0, "expected GROUP BY 1 in the outer read");
        Assert.True(orderByIndex > groupByIndex, "expected ORDER BY 1 to follow GROUP BY 1 in the outer read");
    }

    [Fact]
    public void PlanCacheSnapshotSql_LatestCollection_Composition_OrderedBySize_CapAt30()
    {
        var sql = ViewerDataService.PlanCacheSnapshotSql;

        Assert.Contains("FROM v_plan_cache_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT MAX(collection_time) AS mx", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time = (SELECT mx FROM latest)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY total_size_mb DESC, total_plans DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 30", sql, StringComparison.Ordinal);

        /* The composition + stability columns the grid + summary render. */
        foreach (var column in new[]
        {
            "cacheobjtype", "objtype", "total_plans", "single_use_plans", "single_use_size_mb",
            "multi_use_plans", "multi_use_size_mb", "avg_use_count", "avg_size_kb", "oldest_plan_create_time",
        })
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void PlanCacheSummarySql_SumsAllGroups_LatestCollection_Uncapped()
    {
        var sql = ViewerDataService.PlanCacheSummarySql;

        Assert.Contains("FROM v_plan_cache_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT MAX(collection_time) AS mx", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time = (SELECT mx FROM latest)", sql, StringComparison.Ordinal);
        /* TRUE total across every group + single-use total (for the bloat badge) + oldest plan; COALESCE so
           an empty window yields 0 not NULL. */
        Assert.Contains("COALESCE(SUM(total_plans), 0)", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(SUM(single_use_plans), 0)", sql, StringComparison.Ordinal);
        Assert.Contains("MIN(oldest_plan_create_time)", sql, StringComparison.Ordinal);
        /* Must NOT be capped — the summary is the exact total, independent of the grid's LIMIT 30. */
        Assert.DoesNotContain("LIMIT", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("trend")]
    [InlineData("snapshot")]
    [InlineData("summary")]
    public void PlanCacheSql_PgDialect_PositionalParams_NoBareNow_NoNLiterals(string which)
    {
        var sql = which switch
        {
            "trend" => ViewerDataService.PlanCacheTrendSql,
            "snapshot" => ViewerDataService.PlanCacheSnapshotSql,
            _ => ViewerDataService.PlanCacheSummarySql,
        };

        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
        Assert.Contains("$3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanCacheSql_ReadsColumnsThatExistInTheGeneratedTable()
    {
        Assert.Equal("plan_cache_stats", PlanCacheStatsCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(PlanCacheStatsCollector.Instance);
        foreach (var column in new[]
        {
            "collection_time", "cacheobjtype", "objtype", "total_plans", "total_size_mb",
            "single_use_plans", "single_use_size_mb", "multi_use_plans", "multi_use_size_mb",
            "avg_use_count", "avg_size_kb", "oldest_plan_create_time",
        })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void PlanCacheView_IsPinnedInTheAuthoritativeViewList()
        => Assert.Contains("v_plan_cache_stats", PgSchemaGenerator.AllPassthroughViews);
}

/// <summary>
/// Pins the derived single-use plan-cache bloat badge (<see cref="ViewerDataService.ClassifyPlanCacheBloat"/>),
/// a pure client-side CASE mirroring install/47's report.plan_cache_bloat: bloat_level banding on the
/// single-use / total plan ratio (&gt; 50 CRITICAL / &gt; 30 HIGH / &gt; 20 MEDIUM / else NORMAL) and the
/// paired forced-parameterization recommendation flipping at the same &gt; 20% threshold.
/// </summary>
public sealed class ViewerPlanCacheBloatTests
{
    [Theory]
    [InlineData(100, 60, "CRITICAL")]   // 60% > 50
    [InlineData(100, 51, "CRITICAL")]
    [InlineData(100, 50, "HIGH")]       // exactly 50 is NOT > 50 → HIGH
    [InlineData(100, 40, "HIGH")]       // 40% > 30
    [InlineData(100, 31, "HIGH")]
    [InlineData(100, 30, "MEDIUM")]     // exactly 30 → MEDIUM
    [InlineData(100, 25, "MEDIUM")]     // 25% > 20
    [InlineData(100, 21, "MEDIUM")]
    [InlineData(100, 20, "NORMAL")]     // exactly 20 → NORMAL
    [InlineData(100, 5, "NORMAL")]
    [InlineData(0, 0, "NORMAL")]        // empty cache → no divide-by-zero, NORMAL
    public void ClassifyPlanCacheBloat_BandsOnSingleUsePercent(long total, long singleUse, string expectedLevel)
        => Assert.Equal(expectedLevel, ViewerDataService.ClassifyPlanCacheBloat(total, singleUse).Level);

    [Theory]
    [InlineData(100, 25, true)]         // > 20% → forced-parameterization hint
    [InlineData(100, 21, true)]
    [InlineData(100, 20, false)]        // <= 20% → healthy
    [InlineData(100, 0, false)]
    [InlineData(0, 0, false)]
    public void ClassifyPlanCacheBloat_RecommendationFlipsAtTwentyPercent(long total, long singleUse, bool expectHint)
    {
        var rec = ViewerDataService.ClassifyPlanCacheBloat(total, singleUse).Recommendation;
        if (expectHint)
        {
            Assert.Contains("Forced Parameterization", rec, StringComparison.Ordinal);
        }
        else
        {
            Assert.Equal("Plan cache composition is healthy", rec);
        }
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the Plan Cache reads: the trend SUMs single/multi-use MB
/// across the per-collection groups, and the snapshot takes the latest collection's composition ordered by
/// size (with oldest_plan_create_time round-tripped for the summary). Shares the serialized "live-postgres"
/// collection; uses a negative sentinel server_id and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerPlanCacheLivePostgresTests
{
    private const int ServerId = -940004;
    private const string ServerName = "viewer-plancache-e2e";

    [Fact]
    public async Task Trend_SumsAcrossGroups_AndSnapshot_TakesLatestCompositionBySize_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-cache test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteAsync(connection, ServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var oldest = t1.AddDays(-2);

            /* Two groups at the same collection: single-use MB 40 + 10 = 50; multi-use 100 + 200 = 300. */
            await InsertAsync(connection, t1, "Compiled Plan", "Proc",
                totalPlans: 5, totalSizeMb: 140, singlePlans: 1, singleMb: 40, multiPlans: 4, multiMb: 100, oldest: oldest);
            await InsertAsync(connection, t1, "Compiled Plan", "Adhoc",
                totalPlans: 30, totalSizeMb: 210, singlePlans: 25, singleMb: 10, multiPlans: 5, multiMb: 200, oldest: oldest);

            var trend = await viewer.GetPlanCacheTrendAsync(ServerId, t1.AddMinutes(-1), t1.AddMinutes(1));
            Assert.Single(trend);
            Assert.Equal(50.0, trend[0].SingleUseSizeMb, precision: 3);
            Assert.Equal(300.0, trend[0].MultiUseSizeMb, precision: 3);

            var snapshot = await viewer.GetPlanCacheSnapshotAsync(ServerId, t1.AddMinutes(-1), t1.AddMinutes(1));
            Assert.Equal(2, snapshot.Count);
            /* Ordered by total_size_mb DESC → the Adhoc group (210) first. */
            Assert.Equal("Adhoc", snapshot[0].Objtype);
            Assert.Equal(210, snapshot[0].TotalSizeMb);
            Assert.Equal(oldest.Ticks, snapshot[0].OldestPlanCreateTime!.Value.Ticks);

            /* Summary sums total_plans across BOTH groups (5 + 30 = 35, uncapped) and takes MIN(oldest). */
            var summary = await viewer.GetPlanCacheSummaryAsync(ServerId, t1.AddMinutes(-1), t1.AddMinutes(1));
            Assert.Equal(35L, summary.TotalPlans);
            Assert.Equal(oldest.Ticks, summary.OldestPlanCreateTime!.Value.Ticks);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, ServerId, cleanupCt));
        }
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc, string cacheobjtype, string objtype,
        int totalPlans, int totalSizeMb, int singlePlans, int singleMb, int multiPlans, int multiMb, DateTime oldest)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO plan_cache_stats
    (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype,
     total_plans, total_size_mb, single_use_plans, single_use_size_mb,
     multi_use_plans, multi_use_size_mb, avg_use_count, avg_size_kb, oldest_plan_create_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 1.5, 64, $13)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(cacheobjtype);
        command.Parameters.AddWithValue(objtype);
        command.Parameters.AddWithValue(totalPlans);
        command.Parameters.AddWithValue(totalSizeMb);
        command.Parameters.AddWithValue(singlePlans);
        command.Parameters.AddWithValue(singleMb);
        command.Parameters.AddWithValue(multiPlans);
        command.Parameters.AddWithValue(multiMb);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(oldest, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM plan_cache_stats WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
