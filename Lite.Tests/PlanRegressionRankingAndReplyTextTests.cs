/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7417: the plan regression card is ranked by the CPU the regression lost and not by the factor alone, a statement whose
/// regression lost too little is left out, the force-plan state goes out in snake_case, and the MAXDOP text uses the
/// server's own scheduler count.
/// </summary>
public class PlanRegressionRankingAndReplyTextTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -7417_02;
    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -1;

    public PlanRegressionRankingAndReplyTextTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _conn?.Dispose();

    private async Task ExecAsync(string sql, params object[] args)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_conn is null)
        {
            _conn = _duckDb.CreateConnection();
            await _conn.OpenAsync();
        }
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var arg in args)
            cmd.Parameters.Add(new DuckDBParameter { Value = arg });
        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedPlanAsync(long queryId, long planId, string hash, long execs, double cpuUs, DateTime lastExec, double? durUs = null) => ExecAsync(
        @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
            execution_type_desc, first_execution_time, last_execution_time, query_text, query_hash, query_plan_hash, execution_count,
            avg_cpu_time_us, avg_duration_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
            runtime_stats_interval_id, interval_start_time_utc, is_forced_plan, force_failure_count)
          VALUES ($1, $2, $3, 'TestServer', 'AppDb', $4, $5, 'Regular', $6, $6, $7, $8, $9, $10, $11, $12, 1, 1, 1, $5, $6, false, 0)",
        _nextId--, lastExec, ServerId, queryId, planId, lastExec.AddMinutes(-10), "SELECT " + queryId, "0xQ" + queryId, hash, execs, cpuUs, durUs ?? cpuUs);

    [Fact]
    public async Task ACheapStatementWithTheBiggestFactorDoesNotLeadTheCard_AndOneThatLostTooLittleIsLeftOut()
    {
        var now = DateTime.UtcNow;
        var window = now.AddHours(-4);
        var earlier = now.AddDays(-5);
        /* Query 1: 21 us to 455 us (21.7x) over 30,000 runs: loses 13 CPU-seconds. */
        await SeedPlanAsync(1, 11, "A1", 30_000, 21, earlier);
        await SeedPlanAsync(1, 12, "B1", 30_000, 455, now.AddMinutes(-20));
        /* Query 2: 74 ms to 1.2 s (16.2x) over 30 runs: loses 33.8 CPU-seconds. */
        await SeedPlanAsync(2, 21, "A2", 30, 74_000, earlier);
        await SeedPlanAsync(2, 22, "B2", 30, 1_200_000, now.AddMinutes(-30));
        /* Query 3: 40 us to 100 us (2.5x) over 100,000 runs: the latest plan burned 10 CPU-seconds and lost 6. Under the floor. */
        await SeedPlanAsync(3, 31, "A3", 100_000, 40, earlier);
        await SeedPlanAsync(3, 32, "B3", 100_000, 100, now.AddMinutes(-40));

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(
            new AnalysisContext { ServerId = ServerId, ServerName = "TestServer", TimeRangeStart = window, TimeRangeEnd = now });

        var fact = Assert.Single(facts, f => f.Key == "PLAN_REGRESSION");
        Assert.Equal(2.0, fact.Metadata["offender_count"]);
        Assert.Equal(2.0, fact.Metadata["worst_query_id"]);
        Assert.Equal(1_200_000.0, fact.Metadata["latest_cpu_per_exec_us"], 3);
        Assert.Equal(1_200_000.0 / 74_000.0, fact.Value, 6);
    }

    /// <summary>The pass the service runs: the fact, then the drill-down on the same context, then the force targets taken
    /// from the drill-down's rows in their order.</summary>
    private async Task<(Fact Fact, IReadOnlyList<ForcePlanTarget> Targets)> RunPassAsync(DateTime window, DateTime now)
    {
        var context = new AnalysisContext { ServerId = ServerId, ServerName = "TestServer", TimeRangeStart = window, TimeRangeEnd = now };
        var fact = Assert.Single(await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context), f => f.Key == "PLAN_REGRESSION");
        var finding = new AnalysisFinding { RootFactKey = "PLAN_REGRESSION", StoryPath = "PLAN_REGRESSION", PathKeys = ["PLAN_REGRESSION"], Severity = 1.0 };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);
        return (fact, FactRemediation.ExtractPlanRegressionTargets(finding));
    }

    [Fact]
    public async Task TheForceTargetsLeadWithTheQueryThatLostTheMostCpu_NotTheHighestFactor()
    {
        var now = DateTime.UtcNow;
        var earlier = now.AddDays(-5);
        /* Query 1: 21.7x, loses 13 CPU-seconds. Query 2: 16.2x, loses 33.8 CPU-seconds. */
        await SeedPlanAsync(1, 11, "A1", 30_000, 21, earlier);
        await SeedPlanAsync(1, 12, "B1", 30_000, 455, now.AddMinutes(-20));
        await SeedPlanAsync(2, 21, "A2", 30, 74_000, earlier);
        await SeedPlanAsync(2, 22, "B2", 30, 1_200_000, now.AddMinutes(-30));

        var (fact, targets) = await RunPassAsync(now.AddHours(-4), now);

        Assert.Equal(2.0, fact.Metadata["worst_query_id"]);
        Assert.Equal([2L, 1L], targets.Select(t => t.QueryId));
    }

    [Fact]
    public async Task WithMoreThanFiveOffenders_TheLeadIsStillAForceTarget_AndIsFirst()
    {
        var now = DateTime.UtcNow;
        var earlier = now.AddDays(-5);
        /* Queries 1 to 6: big factors (29x down to 24x) but small losses (14 to 11.5 CPU-seconds). */
        for (long q = 1; q <= 6; q++)
        {
            await SeedPlanAsync(q, q * 10 + 1, "A" + q, 500, 1_000, earlier);
            await SeedPlanAsync(q, q * 10 + 2, "B" + q, 500, 1_000 * (30 - q), now.AddMinutes(-20));
        }

        /* Query 7: only 2.5x, but 1,000 runs lose 150 CPU-seconds. By factor alone it is the seventh. */
        await SeedPlanAsync(7, 71, "A7", 1_000, 100_000, earlier);
        await SeedPlanAsync(7, 72, "B7", 1_000, 250_000, now.AddMinutes(-20));

        var (fact, targets) = await RunPassAsync(now.AddHours(-4), now);

        Assert.Equal(7.0, fact.Metadata["offender_count"]);
        Assert.Equal(7.0, fact.Metadata["worst_query_id"]);
        Assert.Equal(5, targets.Count);
        Assert.Equal(7L, targets[0].QueryId);
        Assert.Equal(2.5, fact.Metadata["lead_regression_factor"], 6);
        Assert.Equal(fact.Metadata["lead_regression_factor"], fact.Value, 6);
    }

    [Fact]
    public async Task ADurationPathRow_KeepsTheOldBar_AndRanksBelowTheLargerCpuLoss()
    {
        var now = DateTime.UtcNow;
        var earlier = now.AddDays(-5);
        /* Query 2 (the CPU path): 16.2x, loses 33.8 CPU-seconds. */
        await SeedPlanAsync(2, 21, "A2", 30, 74_000, earlier);
        await SeedPlanAsync(2, 22, "B2", 30, 1_200_000, now.AddMinutes(-30));
        /* Query 4 (the duration path): 10x slower in duration, CPU only 1.3x. The latest plan burned 39 CPU-seconds and
           lost 9, which is under the lost-CPU floor. A statement slowed by waits is not a CPU loss and must stay. */
        await SeedPlanAsync(4, 41, "A4", 300, 100_000, earlier, durUs: 100_000);
        await SeedPlanAsync(4, 42, "B4", 300, 130_000, now.AddMinutes(-30), durUs: 1_000_000);

        var (fact, targets) = await RunPassAsync(now.AddHours(-4), now);

        Assert.Equal(2.0, fact.Metadata["offender_count"]);
        Assert.Equal(2.0, fact.Metadata["worst_query_id"]);
        Assert.Equal([2L, 4L], targets.Select(t => t.QueryId));
        Assert.Equal(5.0, targets[1].RegressionFactor, 6);
    }

    [Fact]
    public async Task ACpuPathRowThatLostTooLittle_IsLeftOutOfTheDrillDownToo()
    {
        var now = DateTime.UtcNow;
        var earlier = now.AddDays(-5);
        await SeedPlanAsync(2, 21, "A2", 30, 74_000, earlier);
        await SeedPlanAsync(2, 22, "B2", 30, 1_200_000, now.AddMinutes(-30));
        /* 2.5x over 100,000 runs: the latest plan burned 10 CPU-seconds and lost 6. */
        await SeedPlanAsync(3, 31, "A3", 100_000, 40, earlier);
        await SeedPlanAsync(3, 32, "B3", 100_000, 100, now.AddMinutes(-40));

        var (_, targets) = await RunPassAsync(now.AddHours(-4), now);

        Assert.Equal([2L], targets.Select(t => t.QueryId));
    }

    [Fact]
    public void LostCpu_IsThePerExecutionDifferenceTimesTheLatestPlansExecutions()
    {
        Assert.Equal(13_020_000.0, PlanRegressionRanking.LostCpuMicroseconds(455, 21, 30_000), 3);
        Assert.Equal(10_000_000L, PlanRegressionRanking.MinimumLostCpuMicroseconds);
    }

    [Fact]
    public void ForcePlanTargetState_GoesOutInSnakeCase_IncludingTheDerivedMembers()
    {
        var state = new ForcePlanTargetState(
            null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, "ON", DateTime.UtcNow);

        var names = JsonDocument.Parse(JsonSerializer.Serialize(state)).RootElement.EnumerateObject().Select(p => p.Name).ToList();

        Assert.Contains("is_empty", names);
        Assert.Contains("apc_is_on", names);
        Assert.DoesNotContain(names, n => n.Any(char.IsUpper));
    }

    private static Fact Hardware(double cpuCount) => new() { Source = "config", Key = "SERVER_HARDWARE", Value = cpuCount, Metadata = new Dictionary<string, double> { ["cpu_count"] = cpuCount, ["cores_per_socket"] = cpuCount } };

    private static Dictionary<string, Fact> Facts(params Fact[] facts) => facts.ToDictionary(f => f.Key, f => f);

    private static Fact Maxdop0 => new() { Source = "config", Key = "CONFIG_MAXDOP", Value = 0 };

    [Theory]
    [InlineData(2, "every scheduler (2 on this server)")]
    [InlineData(128, "every scheduler (up to 64 of this server's 128)")]
    public void MaxdopZeroHeadline_UsesTheServersOwnSchedulerCount(int cpus, string expected)
    {
        var advice = FactAdvice.Compose("CONFIG_MAXDOP", Facts(Hardware(cpus), Maxdop0))!;

        Assert.Contains(expected, advice.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("(up to 64)", advice.Headline, StringComparison.Ordinal);
    }

    [Fact]
    public void MaxdopZeroHeadline_WithNoHardwareFact_CarriesNoNumber()
    {
        var advice = FactAdvice.Compose("CONFIG_MAXDOP", Facts(Maxdop0))!;

        Assert.Equal("MAXDOP is 0 — a single query can fan out across every scheduler", advice.Headline);
    }
}
