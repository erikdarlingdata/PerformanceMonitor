/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7445: Lite's plan cache bloat badge and get_plan_cache_bloat verdict come from the one shared rule. An idle
/// 49-entry, 11 MB cache that is 20% single use, most of the entries view parse trees, read MEDIUM and
/// recommended Forced Parameterization. The summary read now carries the compiled-plan figures the rule needs,
/// and a cache whose single-use plans take under 100 MB is NORMAL whatever the ratio.
/// </summary>
public sealed class LitePlanCacheBloatTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 7445;
    private readonly DuckDbInitializer _duckDb;

    public LitePlanCacheBloatTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task SmallCache_WithParseTrees_IsNormal_AndSaysTheCacheIsTooSmallToJudge()
    {
        var ct = TestContext.Current.CancellationToken;
        var collected = DateTime.UtcNow.AddMinutes(-5);
        await InsertGroupAsync(collected, "Compiled Plan", "Adhoc", totalPlans: 10, totalMb: 9, singlePlans: 8, singleMb: 6, ct);
        await InsertGroupAsync(collected, "Parse Tree", "View", totalPlans: 38, totalMb: 1, singlePlans: 2, singleMb: 1, ct);
        await InsertGroupAsync(collected, "Extended Proc", "Proc", totalPlans: 1, totalMb: 1, singlePlans: 1, singleMb: 1, ct);

        var summary = await new LocalDataService(_duckDb).GetPlanCacheSummaryAsync(ServerId);

        Assert.Equal(49L, summary.TotalPlans);
        Assert.Equal(11L, summary.TotalSizeMb);
        Assert.Equal(10L, summary.CompiledPlans);
        Assert.Equal(8L, summary.CompiledSingleUsePlans);
        Assert.Equal(6L, summary.CompiledSingleUseSizeMb);

        var bloat = LocalDataService.ClassifyPlanCacheBloat(summary);
        Assert.Equal("NORMAL", bloat.Level);
        Assert.Contains("too small to judge", bloat.Recommendation, StringComparison.Ordinal);
        Assert.Contains("49 plans using 11 MB", bloat.Recommendation, StringComparison.Ordinal);
    }

    /// <summary>#7445: 20,000 compiled plans in 3,000 MB with 50 MB single use is the normal state of a well-parameterized
    /// server. It is below the floor, but the cache is not small, so the text must not say it is too small to judge.</summary>
    [Fact]
    public async Task LargeCache_UnderTheFloor_IsNormal_AndIsNotCalledTooSmallToJudge()
    {
        var ct = TestContext.Current.CancellationToken;
        var collected = DateTime.UtcNow.AddMinutes(-5);
        await InsertGroupAsync(collected, "Compiled Plan", "Adhoc", totalPlans: 20000, totalMb: 3000, singlePlans: 1000, singleMb: 50, ct);

        var bloat = LocalDataService.ClassifyPlanCacheBloat(await new LocalDataService(_duckDb).GetPlanCacheSummaryAsync(ServerId));

        Assert.Equal("NORMAL", bloat.Level);
        Assert.DoesNotContain("too small", bloat.Recommendation, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("50 MB", bloat.Recommendation, StringComparison.Ordinal);
        Assert.Contains("3,000 MB", bloat.Recommendation, StringComparison.Ordinal);
        Assert.Contains("no action is needed", bloat.Recommendation, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LargeCache_OfSingleUseCompiledPlans_IsCritical_AndAdvisesAdHocWorkloadsFirst()
    {
        var ct = TestContext.Current.CancellationToken;
        var collected = DateTime.UtcNow.AddMinutes(-5);
        await InsertGroupAsync(collected, "Compiled Plan", "Adhoc", totalPlans: 1000, totalMb: 800, singlePlans: 600, singleMb: 400, ct);
        await InsertGroupAsync(collected, "Parse Tree", "View", totalPlans: 4000, totalMb: 40, singlePlans: 0, singleMb: 0, ct);

        var bloat = LocalDataService.ClassifyPlanCacheBloat(await new LocalDataService(_duckDb).GetPlanCacheSummaryAsync(ServerId));

        Assert.Equal("CRITICAL", bloat.Level);
        Assert.StartsWith("Optimize for ad hoc workloads", bloat.Recommendation, StringComparison.Ordinal);
        Assert.Contains("Forced Parameterization", bloat.Recommendation, StringComparison.Ordinal);
    }

    private async Task InsertGroupAsync(
        DateTime collectionTime, string cacheObjType, string objType,
        int totalPlans, int totalMb, int singlePlans, int singleMb, System.Threading.CancellationToken ct)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(ct);
        using var command = connection.CreateCommand();
        command.CommandText = $@"
INSERT INTO plan_cache_stats
    (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype,
     total_plans, total_size_mb, single_use_plans, single_use_size_mb,
     multi_use_plans, multi_use_size_mb, avg_use_count, avg_size_kb, oldest_plan_create_time)
VALUES
    ((SELECT COALESCE(MAX(collection_id), 0) + 1 FROM plan_cache_stats), TIMESTAMP '{collectionTime:yyyy-MM-dd HH:mm:ss}', {ServerId}, 'lite-7445',
     '{cacheObjType}', '{objType}', {totalPlans}, {totalMb}, {singlePlans}, {singleMb},
     {totalPlans - singlePlans}, {totalMb - singleMb}, 1.5, 64, NULL)";
        await command.ExecuteNonQueryAsync(ct);
    }
}
