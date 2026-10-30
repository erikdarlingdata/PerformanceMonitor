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
using PerformanceMonitor.Darling.Service.ChangeWatch;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.ChangeWatchLiveTests;
using Plan = PerformanceMonitor.Darling.Service.ChangeWatch.ChangeWatchEvaluator.PlanRuntime;

namespace Darling.Tests;

/* #6555 */
public sealed class ChangeWatchPlanMixTests
{
    private static Dictionary<string, Plan> Plans(params (string Plan, long Executions, double AvgUs)[] plans)
    {
        var result = new Dictionary<string, Plan>(StringComparer.Ordinal);
        foreach (var (plan, executions, avgUs) in plans)
        {
            result[plan] = new Plan(executions, executions * avgUs);
        }

        return result;
    }

    private static double Avg(Dictionary<string, Plan> plans)
    {
        double executions = 0, elapsed = 0;
        foreach (var p in plans.Values)
        {
            executions += p.Executions;
            elapsed += p.ElapsedUs;
        }

        return elapsed / executions;
    }

    [Fact]
    public void TwoStablePlans_WhoseMixChanges_AreNotARegression_ThoughTheWholeQueryAverageMovedFivefold()
    {
        var before = Plans(("fast", 90, 1_000), ("slow", 10, 10_000));   /* 1.9 ms */
        var after = Plans(("fast", 10, 1_000), ("slow", 90, 10_000));    /* 9.1 ms */

        Assert.True(ChangeWatchEvaluator.DurationMoved(Avg(before), Avg(after)));
        Assert.False(ChangeWatchEvaluator.QueryDurationMoved(Avg(before), Avg(after), null, before, after, Plans(), out var note));
        Assert.Equal(1_900, ChangeWatchEvaluator.AdjustAfterToMix(before, after), 3);
        Assert.Contains("plan mix", note, StringComparison.Ordinal);
    }

    [Fact]
    public void ASlowerPlan_IsStillARegression_WhateverTheMix()
    {
        var before = Plans(("fast", 90, 1_000), ("slow", 10, 10_000));
        var after = Plans(("fast", 90, 4_000), ("slow", 10, 10_000));    /* the fast plan itself got four times slower */
        Assert.True(ChangeWatchEvaluator.QueryDurationMoved(Avg(before), Avg(after), null, before, after, Plans(), out _));
    }

    [Fact]
    public void ASwappedPlan_IsStillARegression_BecauseNoPlanIsShared()
    {
        var before = Plans(("old", 100, 1_000));
        var after = Plans(("new", 100, 5_000));
        Assert.True(ChangeWatchEvaluator.QueryDurationMoved(Avg(before), Avg(after), null, before, after, Plans(), out var note));
        Assert.Null(note);
    }

    [Fact]
    public void ANewSlowPlan_NextToAStableOne_IsStillARegression()
    {
        var before = Plans(("fast", 100, 1_000));
        var after = Plans(("fast", 50, 1_000), ("new", 50, 9_000));
        Assert.True(ChangeWatchEvaluator.QueryDurationMoved(Avg(before), Avg(after), null, before, after, Plans(), out _));
    }

    /* The reviewer's case (#6580 round 1): plan A was 10% of the executions before and 1 ms; after, it is 90% and ten times slower. Weighted by its OLD share
       the whole query reads 1.9 ms against 1.0 ms, under the factor, so the mix adjustment alone called it flat. */
    [Fact]
    public void AMinorityPlan_ThatGotSlower_AndTookOverTheWorkload_IsStillARegression()
    {
        var before = Plans(("a", 10, 1_000), ("b", 90, 1_000));
        var after = Plans(("a", 90, 10_000), ("b", 10, 1_000));

        Assert.True(ChangeWatchEvaluator.DurationMoved(Avg(before), Avg(after)));
        Assert.False(ChangeWatchEvaluator.DurationMoved(Avg(before), ChangeWatchEvaluator.AdjustAfterToMix(before, after)));
        Assert.True(ChangeWatchEvaluator.QueryDurationMoved(Avg(before), Avg(after), null, before, after, Plans(), out var note));
        Assert.Contains("plan a: 10% at 1 ms before, 90% at 10 ms after", note, StringComparison.Ordinal);
    }

    [Fact]
    public void ASlowerPlan_CountsOnlyWhenItRanEnoughInBothWindows()
    {
        var before = Plans(("a", 5, 1_000), ("b", 95, 1_000));      /* plan a ran 5 times: its average is not evidence */
        var after = Plans(("a", 90, 10_000), ("b", 10, 1_000));
        Assert.False(ChangeWatchEvaluator.AnySharedPlanMoved(before, after));
        Assert.True(ChangeWatchEvaluator.AnySharedPlanMoved(Plans(("a", 10, 1_000), ("b", 90, 1_000)), after));
    }

    private static async Task SeedPlanAsync(NpgsqlConnection c, DateTime windowStart, long planId, long executionsPerInterval, long avgUs, CancellationToken ct)
    {
        for (var interval = 0; interval < 6; interval++)
        {
            var start = windowStart.AddMinutes(interval * 10);
            await SeedAsync(c, "query_store_stats", new Dictionary<string, object>
            {
                ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = ChangeWatchDetectionLiveTests.Naive(start.AddMinutes(10)), ["database_name"] = "Sales",
                ["query_id"] = 42L, ["plan_id"] = planId, ["runtime_stats_interval_id"] = (long)interval + 1,
                ["first_execution_time"] = ChangeWatchDetectionLiveTests.Naive(start), ["execution_type_desc"] = "Regular", ["replica_role"] = "PRIMARY",
                ["interval_start_time_utc"] = ChangeWatchDetectionLiveTests.Naive(start), ["interval_end_time_utc"] = ChangeWatchDetectionLiveTests.Naive(start.AddMinutes(10)),
                ["execution_count"] = executionsPerInterval, ["avg_duration_us"] = (double)avgUs,
            }, ct);
        }
    }

    private static async Task<string?> RunWatchAsync(Func<NpgsqlConnection, DateTime, CancellationToken, Task> seed, Func<Harness, string?, CancellationToken, Task> check)
    {
        string? result = null;
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await seed(h.Connection, change, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName,
                new[] { new ChangeWatchChange("plan_force:action:1", "plan_force", change, "plan 7 of query 42 in Sales: force by the plan-force ledger", "Sales", 42) }, ct);
            await h.NewPass().EvaluateDueAsync(ct);
            result = await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct);
            await check(h, await TextAsync(h.Connection, "SELECT result_json FROM config.change_watches", ct), ct);
        });
        return result;
    }

    /* The Query Store read, end to end: plan 1 (1 ms) and plan 2 (10 ms) ran in both windows; only their share changed, 9:1 before and 1:9 after. */
    [Fact]
    public async Task AQueryStoreQuery_WhosePlanMixChanged_OpensNoRequest_AndTheResultSaysSo()
    {
        var result = await RunWatchAsync(
            async (c, change, ct) =>
            {
                await SeedPlanAsync(c, change.AddMinutes(-60), 1, 9, 1_000, ct);
                await SeedPlanAsync(c, change.AddMinutes(-60), 2, 1, 10_000, ct);
                await SeedPlanAsync(c, change, 1, 1, 1_000, ct);
                await SeedPlanAsync(c, change, 2, 9, 10_000, ct);
            },
            (h, json, ct) =>
            {
                Assert.DoesNotContain("\"moved\":true", json, StringComparison.Ordinal);
                Assert.Contains("only with the plan mix", json, StringComparison.Ordinal);
                return Task.CompletedTask;
            });
        Assert.NotEqual("request_opened", result);
    }

    /* The same query with plan 1 itself four times slower after the change is still a regression, with the plan counts in the note. */
    [Fact]
    public async Task AQueryStoreQuery_WhoseSharedPlanGotSlower_StillOpensARequest()
    {
        var result = await RunWatchAsync(
            async (c, change, ct) =>
            {
                await SeedPlanAsync(c, change.AddMinutes(-60), 1, 9, 1_000, ct);
                await SeedPlanAsync(c, change.AddMinutes(-60), 2, 1, 10_000, ct);
                await SeedPlanAsync(c, change, 1, 9, 4_000, ct);
                await SeedPlanAsync(c, change, 2, 1, 10_000, ct);
            },
            (h, json, ct) =>
            {
                Assert.Contains("query_avg_duration_ms:Sales:42", json, StringComparison.Ordinal);
                Assert.Contains("2 plan(s) before, 2 after", json, StringComparison.Ordinal);
                return Task.CompletedTask;
            });
        Assert.Equal("request_opened", result);
    }

    private static Task SeedStatsAsync(NpgsqlConnection c, DateTime at, string planHash, long executions, long avgUs, CancellationToken ct, string database = "Sales") =>
        SeedAsync(c, "query_stats", new Dictionary<string, object>
        {
            ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = ChangeWatchDetectionLiveTests.Naive(at), ["database_name"] = database,
            ["query_hash"] = "0xABC", ["query_plan_hash"] = planHash, ["delta_execution_count"] = executions, ["delta_elapsed_time"] = executions * avgUs,
        }, ct);

    /* The plan-cache read, end to end: a configuration change, so the top queries by total duration are measured (query_stats, per query hash). */
    [Theory]
    [InlineData(false, "request_opened")]
    [InlineData(true, "flat")]
    public async Task APlanCacheQuery_IsComparedPerPlan_WhenItRanTwoPlans(bool mixOnly, string expected)
    {
        var result = await RunWatchWithConfigChangeAsync(
            async (c, change, ct) =>
            {
                for (var i = 0; i < 6; i++)
                {
                    var before = change.AddMinutes(-55 + (i * 8));
                    var after = change.AddMinutes(5 + (i * 8));
                    await SeedStatsAsync(c, before, "0xP1", 9, 1_000, ct);
                    await SeedStatsAsync(c, before, "0xP2", 1, 10_000, ct);
                    /* mixOnly: the same two plans at the same speed with the share reversed; otherwise the fast plan itself got four times slower. */
                    await SeedStatsAsync(c, after, "0xP1", mixOnly ? 1 : 9, mixOnly ? 1_000 : 4_000, ct);
                    await SeedStatsAsync(c, after, "0xP2", mixOnly ? 9 : 1, 10_000, ct);
                }
            },
            (json) =>
            {
                Assert.Equal(!mixOnly, json!.Contains("\"moved\":true", StringComparison.Ordinal));
                Assert.Equal(mixOnly, json.Contains("only with the plan mix", StringComparison.Ordinal));
            });
        Assert.Equal(expected, result);
    }

    /* A plan_force watch whose Query Store holds nothing falls back to the top queries of the CHANGED databases. The same query hash also runs in another
       database (a procedure deployed per tenant) fast and in volume; every read of the hash, the plan reads included, is limited to Sales, so that other
       database cannot pull Sales' 1 ms -> 5 ms back under the factor. */
    [Fact]
    public async Task AFallbackQuery_IsMeasuredInTheChangedDatabaseOnly_NotInAnotherThatRunsTheSameHash()
    {
        var result = await RunWatchAsync(
            async (c, change, ct) =>
            {
                for (var i = 0; i < 6; i++)
                {
                    var before = change.AddMinutes(-55 + (i * 8));
                    var after = change.AddMinutes(5 + (i * 8));
                    await SeedStatsAsync(c, before, "0xP1", 20, 1_000, ct);
                    await SeedStatsAsync(c, after, "0xP1", 20, 5_000, ct);
                    await SeedStatsAsync(c, before, "0xP1", 5_000, 1_000, ct, "Tenant2");
                    await SeedStatsAsync(c, after, "0xP1", 5_000, 1_000, ct, "Tenant2");
                }
            },
            (h, json, ct) =>
            {
                Assert.Contains("\"moved\":true", json, StringComparison.Ordinal);
                return Task.CompletedTask;
            });
        Assert.Equal("request_opened", result);
    }

    private static async Task<string?> RunWatchWithConfigChangeAsync(Func<NpgsqlConnection, DateTime, CancellationToken, Task> seed, Action<string?> check)
    {
        string? result = null;
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await seed(h.Connection, change, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName,
                new[] { new ChangeWatchChange("server_config:max dop:1", "server_config", change, "server setting max degree of parallelism: 0 -> 4") }, ct);
            await h.NewPass().EvaluateDueAsync(ct);
            result = await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct);
            check(await TextAsync(h.Connection, "SELECT result_json FROM config.change_watches", ct));
        });
        return result;
    }
}
