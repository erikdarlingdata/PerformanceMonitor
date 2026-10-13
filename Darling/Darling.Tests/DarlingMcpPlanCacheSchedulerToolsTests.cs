/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the plan-cache + CPU-scheduler snapshot MCP slice — get_plan_cache_bloat, get_cpu_scheduler_pressure
/// over the Postgres store. Ungated: tool surface, the param contracts, the latest-snapshot read SQL pins
/// (v_plan_cache_stats / v_cpu_scheduler_stats), the reproduced #1410 classifications (plan-cache bloat_level,
/// cpu-scheduler pressure_level / recommendation), and the Gemini-clean advertised schema.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class DarlingMcpPlanCacheSchedulerToolsSurfaceAndSqlTests
{
    private static readonly string[] PlanCacheSchedulerToolSurface =
    {
        "get_cpu_scheduler_pressure",
        "get_plan_cache_bloat",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpPlanCacheSchedulerTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheTwoTools()
    {
        var names = ToolMethods()
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(PlanCacheSchedulerToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpPlanCacheSchedulerTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(ToolMethods(), m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(ToolMethods(), m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    private static (string Name, bool Optional)[] McpParams(string toolName)
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == toolName);
        return method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name!, p.HasDefaultValue))
            .ToArray();
    }

    [Fact]
    public void ParamContract_PlanCacheBloat_ServerHours()
    {
        var p = McpParams("get_plan_cache_bloat");
        Assert.Equal(new[] { "server_name", "hours_back", "as_of" }, p.Select(x => x.Name).ToArray());
        Assert.All(p, x => Assert.True(x.Optional));
    }

    /// <summary>#3541 A10: the same (server_name, hours_back, as_of) surface Lite's twin has always had. The
    /// tool took server_name alone here — two parameter surfaces under one tool name, on the one tool whose
    /// answer is a CRITICAL/HIGH/MEDIUM/NORMAL verdict. <c>McpLatestSnapshotStampTests</c> pins the two SKUs'
    /// descriptions equal; this pins the shape.</summary>
    [Fact]
    public void ParamContract_CpuSchedulerPressure_ServerHoursAsOf_MatchesLite()
    {
        var p = McpParams("get_cpu_scheduler_pressure");
        Assert.Equal(new[] { "server_name", "hours_back", "as_of" }, p.Select(x => x.Name).ToArray());
        Assert.All(p, x => Assert.True(x.Optional));
    }

    [Fact]
    public void PlanCacheBloatSql_LatestSnapshot_AllGroups()
    {
        var sql = DarlingPlanCacheSchedulerReader.PlanCacheBloatSql;
        Assert.Contains("FROM v_plan_cache_stats", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("cacheobjtype", sql, StringComparison.Ordinal);
        Assert.Contains("single_use_plans", sql, StringComparison.Ordinal);
        Assert.Contains("multi_use_plans", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY total_size_mb DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void CpuSchedulerPressureSql_LatestRow()
    {
        var sql = DarlingPlanCacheSchedulerReader.CpuSchedulerPressureSql;
        Assert.Contains("FROM v_cpu_scheduler_stats", sql, StringComparison.Ordinal);
        Assert.Contains("total_runnable_tasks_count", sql, StringComparison.Ordinal);
        Assert.Contains("max_workers_count", sql, StringComparison.Ordinal);
        Assert.Contains("worker_thread_exhaustion_warning", sql, StringComparison.Ordinal);
        /* #5710: the newest collection_time is found first and ALL of its rows are combined into one. */
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id, collection_time", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT 1", sql, StringComparison.Ordinal);
        /* #3541 A10: the newest row IN THE WINDOW, as Lite reads it — not the newest row the store ever held. */
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingPlanCacheSchedulerReader.PlanCacheBloatSql))]
    [InlineData(nameof(DarlingPlanCacheSchedulerReader.CpuSchedulerPressureSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName == nameof(DarlingPlanCacheSchedulerReader.PlanCacheBloatSql)
            ? DarlingPlanCacheSchedulerReader.PlanCacheBloatSql
            : DarlingPlanCacheSchedulerReader.CpuSchedulerPressureSql;
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTables()
    {
        var pc = PgSchemaGenerator.CreateTable(PlanCacheStatsCollector.Instance);
        Assert.Equal("plan_cache_stats", PlanCacheStatsCollector.Instance.TargetTable);
        Assert.Contains("cacheobjtype", pc, StringComparison.Ordinal);
        Assert.Contains("single_use_plans", pc, StringComparison.Ordinal);
        Assert.Contains("multi_use_plans", pc, StringComparison.Ordinal);

        var cpu = PgSchemaGenerator.CreateTable(CpuSchedulerStatsCollector.Instance);
        Assert.Equal("cpu_scheduler_stats", CpuSchedulerStatsCollector.Instance.TargetTable);
        Assert.Contains("total_runnable_tasks_count", cpu, StringComparison.Ordinal);
        Assert.Contains("max_workers_count", cpu, StringComparison.Ordinal);
        Assert.Contains("total_current_workers_count", cpu, StringComparison.Ordinal);
        Assert.Contains("worker_thread_exhaustion_warning", cpu, StringComparison.Ordinal);

        Assert.Contains("v_plan_cache_stats", PgSchemaGenerator.AllPassthroughViews);
        Assert.Contains("v_cpu_scheduler_stats", PgSchemaGenerator.AllPassthroughViews);
    }

    /* ─────────────── #1410 classification thresholds ─────────────── */

    [Theory]
    [InlineData(100L, 60L, "CRITICAL")]   /* 60% > 50 */
    [InlineData(100L, 40L, "HIGH")]       /* 40% > 30 */
    [InlineData(100L, 25L, "MEDIUM")]     /* 25% > 20 */
    [InlineData(100L, 10L, "NORMAL")]     /* 10% <= 20 */
    [InlineData(0L, 0L, "NORMAL")]        /* empty cache */
    public void ClassifyPlanCacheBloat_MatchesThresholds(long total, long singleUse, string expected)
    {
        var (level, recommendation) = DarlingPlanCacheSchedulerReader.ClassifyPlanCacheBloat(total, singleUse);
        Assert.Equal(expected, level);
        if (expected == "NORMAL")
            Assert.Equal("Plan cache composition is healthy", recommendation);
        else
            Assert.Equal("Check for unparameterized queries / Consider Forced Parameterization", recommendation);
    }

    [Fact]
    public void ClassifyCpuSchedulerPressure_RunnableTaskBands()
    {
        Assert.Equal("CRITICAL - High runnable task queue", Level(60, 0, 100, 0, false, false, false));
        Assert.Equal("HIGH - Moderate runnable task queue", Level(30, 0, 100, 0, false, false, false));
        Assert.Equal("MEDIUM - Some runnable tasks queued", Level(15, 0, 100, 0, false, false, false));
        Assert.Equal("NORMAL", Level(0, 0, 100, 0, false, false, false));

        static string Level(int runnable, int workers, int maxWorkers, int queued, bool ex, bool rt, bool qr) =>
            DarlingPlanCacheSchedulerReader.ClassifyCpuSchedulerPressure(runnable, workers, maxWorkers, queued, ex, rt, qr).Level;
    }

    [Fact]
    public void ClassifyCpuSchedulerPressure_WorkerUtilizationAndWarningFallbacks()
    {
        /* worker utilization > 90% (95/100) with low runnable tasks escalates to HIGH */
        Assert.Equal("HIGH - Worker thread exhaustion",
            DarlingPlanCacheSchedulerReader.ClassifyCpuSchedulerPressure(5, 95, 100, 0, false, false, false).Level);

        /* warning-flag fallbacks (below the worker-util threshold) */
        Assert.Equal("CRITICAL - Worker thread exhaustion warning",
            DarlingPlanCacheSchedulerReader.ClassifyCpuSchedulerPressure(5, 10, 100, 0, true, false, false).Level);
        Assert.Equal("HIGH - Runnable tasks warning",
            DarlingPlanCacheSchedulerReader.ClassifyCpuSchedulerPressure(5, 10, 100, 0, false, true, false).Level);
        Assert.Equal("MEDIUM - Queued requests warning",
            DarlingPlanCacheSchedulerReader.ClassifyCpuSchedulerPressure(5, 10, 100, 0, false, false, true).Level);

        /* recommendation branches */
        Assert.Equal("CPU pressure detected - check for CPU-intensive queries, consider adding CPU cores",
            DarlingPlanCacheSchedulerReader.ClassifyCpuSchedulerPressure(30, 10, 100, 0, false, false, false).Recommendation);
        Assert.Equal("Requests queued for execution - CPU or worker thread pressure",
            DarlingPlanCacheSchedulerReader.ClassifyCpuSchedulerPressure(5, 10, 100, 3, false, false, false).Recommendation);
        Assert.Equal("No CPU scheduler pressure detected",
            DarlingPlanCacheSchedulerReader.ClassifyCpuSchedulerPressure(0, 10, 100, 0, false, false, false).Recommendation);
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpPlanCacheSchedulerTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_NoRequiredParams()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(2, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        foreach (var t in tools)
            Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(t.InputSchema));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the plan-cache + cpu-scheduler tools. Plants two plan-cache
/// groups and a cpu-scheduler snapshot, then asserts each tool returns its data-bearing envelope with the
/// derived classification; an empty store returns the miss.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpPlanCacheSchedulerToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-plancache-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task PlanCacheSchedulerTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-cache/scheduler-tools test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);

            foreach (var (cacheType, objType, total, single) in new[] { ("Compiled Plan", "Adhoc", 800, 700), ("Compiled Plan", "Proc", 200, 10) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO plan_cache_stats (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype, total_plans, total_size_mb, single_use_plans, single_use_size_mb, multi_use_plans, multi_use_size_mb, avg_use_count, avg_size_kb, oldest_plan_create_time)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, cacheType, objType, total, 500, single, 400, total - single, 100, 3.5m, 64, t);
            }

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO cpu_scheduler_stats (collection_id, collection_time, server_id, server_name, max_workers_count, scheduler_count, cpu_count, total_runnable_tasks_count, total_work_queue_count, total_current_workers_count, avg_runnable_tasks_count, total_active_request_count, total_queued_request_count, total_blocked_task_count, total_active_parallel_thread_count, runnable_percent, worker_thread_exhaustion_warning, runnable_tasks_warning, blocked_tasks_warning, queued_requests_warning, total_physical_memory_kb, available_physical_memory_kb, physical_memory_pressure_warning, total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, 512, 8, 8, 60, 5L, 100, 7.5m, 40, 12, 2, 20L, 12.5m, false, true, false, true, 65536000L, 32768000L, false, 1, 1, 0, false);

            var planCache = await DarlingMcpPlanCacheSchedulerTools.GetPlanCacheBloat(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(planCache, ServerName, "cache_types");
            Assert.Contains("bloat_level", planCache, StringComparison.Ordinal);
            Assert.Contains("CRITICAL", planCache, StringComparison.Ordinal);   /* 710/1000 single-use = 71% */

            var cpu = await DarlingMcpPlanCacheSchedulerTools.GetCpuSchedulerPressure(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(cpu, ServerName, "pressure_level");
            Assert.Contains("CRITICAL - High runnable task queue", cpu, StringComparison.Ordinal);   /* runnable 60 > 50 */

            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpPlanCacheSchedulerTools.GetPlanCacheBloat(postgres, ServerName)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #5710: a collection written before the collector fix can be split across several cpu_scheduler_stats rows that
    /// share one collection_time and each cover some of the schedulers (every row still says scheduler_count = 4). The
    /// pressure read combines them: the runnable total is the SUM, the per-scheduler average is that total over
    /// scheduler_count (a one-row collection keeps its stored average), the two sum-based warning flags are recomputed
    /// from the combined counts, and the single-row DMV columns take their MAX. Three collections, each read through its
    /// own window: one exact row, a split, and an all-zero split. The higher collection_id goes in FIRST so a
    /// physical-order coincidence cannot pass this. This replaces the #3936 same-instant tiebreak test: two rows under one
    /// collection_time are now ONE collection, not two competing snapshots.
    /// </summary>
    [Fact]
    public async Task GetCpuSchedulerPressure_RowsOfOneCollectionTime_AreCombinedIntoOneSnapshot()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-cache/scheduler-tools test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var baseTime = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-30);
            var exactTime = baseTime;
            var splitTime = baseTime.AddMinutes(10);
            var zeroTime = baseTime.AddMinutes(20);

            const string insertSql = @"INSERT INTO cpu_scheduler_stats (collection_id, collection_time, server_id, server_name, max_workers_count, scheduler_count, cpu_count, total_runnable_tasks_count, total_work_queue_count, total_current_workers_count, avg_runnable_tasks_count, total_active_request_count, total_queued_request_count, total_blocked_task_count, total_active_parallel_thread_count, runnable_percent, worker_thread_exhaustion_warning, runnable_tasks_warning, blocked_tasks_warning, queued_requests_warning, total_physical_memory_kb, available_physical_memory_kb, physical_memory_pressure_warning, total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)";

            /* ONE row: the stored average (5.25) is kept, not recomputed. */
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9100L, exactTime, ServerId, ServerName, 100, 4, 4, 21, 2L, 60, 5.25m, 7, 1, 0, 3L, 12.5m, false, true, false, false, 65536000L, 32768000L, false, 1, 1, 0, false);

            /* A split of 4 schedulers: 3 + 2 runnable tasks, 50 + 45 workers. Each row's own flags are FALSE (3 < 4
               CPUs, 50 < 90 workers); the whole server's are TRUE (5 >= 4, 95 >= 90). The request columns differ
               between the two pieces, as the old collector's re-read per scheduler row made them. */
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9202L, splitTime, ServerId, ServerName, 100, 4, 4, 2, 0L, 45, 2.00m, 9, 1, 0, 3L, 20.0m, false, false, false, false, 65536000L, 30000000L, false, 1, 1, 0, false);
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9201L, splitTime, ServerId, ServerName, 100, 4, 4, 3, 0L, 50, 3.00m, 8, 1, 0, 3L, 10.0m, false, false, false, false, 65536000L, 31000000L, false, 1, 1, 0, false);

            /* An all-zero 1 + 1 + 1 + 1 split at idle. */
            for (var i = 0; i < 4; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                    9300L + i, zeroTime, ServerId, ServerName, 100, 4, 4, 0, 0L, 10, 0.00m, 0, 0, 0, 0L, 0m, false, false, false, false, 65536000L, 60000000L, false, 1, 1, 0, false);
            }

            var exact = await DarlingPlanCacheSchedulerReader.GetCpuSchedulerPressureAsync(
                postgres, ServerId, exactTime.AddMinutes(-1), exactTime.AddMinutes(1), ct);
            Assert.NotNull(exact);
            Assert.Equal(21, exact!.TotalRunnableTasksCount);
            Assert.Equal(5.25m, exact.AvgRunnableTasksCount);
            Assert.True(exact.RunnableTasksWarning);

            var split = await DarlingPlanCacheSchedulerReader.GetCpuSchedulerPressureAsync(
                postgres, ServerId, splitTime.AddMinutes(-1), splitTime.AddMinutes(1), ct);
            Assert.NotNull(split);
            Assert.Equal(splitTime, split!.CollectionTime);
            Assert.Equal(4, split.SchedulerCount);
            Assert.Equal(5, split.TotalRunnableTasksCount);
            Assert.Equal(1.25m, split.AvgRunnableTasksCount);
            Assert.Equal(95, split.TotalCurrentWorkersCount);
            Assert.Equal(100, split.MaxWorkersCount);
            Assert.Equal(9, split.TotalActiveRequestCount);
            Assert.Equal(20.0m, split.RunnablePercent);
            Assert.True(split.RunnableTasksWarning);
            Assert.True(split.WorkerThreadExhaustionWarning);

            var zero = await DarlingPlanCacheSchedulerReader.GetCpuSchedulerPressureAsync(
                postgres, ServerId, zeroTime.AddMinutes(-1), zeroTime.AddMinutes(1), ct);
            Assert.NotNull(zero);
            Assert.Equal(0, zero!.TotalRunnableTasksCount);
            Assert.Equal(0.00m, zero.AvgRunnableTasksCount);
            Assert.Equal(40, zero.TotalCurrentWorkersCount);
            Assert.False(zero.RunnableTasksWarning);

            /* The tool's own verdict over the combined row (#5712 round 1): the read above is the reader, this is the
               tool the MCP client calls, so the pressure_level text it builds over a combined row is checked live too.
               The split collection is the newest one inside a window that ends a minute after it. Neither piece alone
               reaches the bar (50 and 45 workers of 100); the combined 95 does: the worker-utilization band, not NORMAL. */
            var asOf = splitTime.AddMinutes(1).ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", System.Globalization.CultureInfo.InvariantCulture);
            var toolSplit = await DarlingMcpPlanCacheSchedulerTools.GetCpuSchedulerPressure(postgres, ServerName, hours_back: 1, as_of: asOf, cancellationToken: ct);
            DarlingMcpTestData.AssertEnvelope(toolSplit, ServerName, "pressure_level");
            Assert.Contains("HIGH - Worker thread exhaustion", toolSplit, StringComparison.Ordinal);
            Assert.Contains("Worker thread exhaustion - check max worker threads setting", toolSplit, StringComparison.Ordinal);

            /* #5712 review: the memory group comes from ONE row, the one with the LEAST available memory. One piece stored TRUE at
               6,000,000 KB (below 10 % of 65,536,000), the other FALSE at 20,000,000 KB: the read shows the 6,000,000 KB row and the
               flag is recomputed from that row's pair, TRUE. */
            var memHighTime = baseTime.AddMinutes(25);
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9401L, memHighTime, ServerId, ServerName, 100, 4, 4, 1, 0L, 10, 1.00m, 1, 0, 0, 0L, 1m, false, false, false, false, 65536000L, 6000000L, true, 1, 1, 0, false);
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9402L, memHighTime, ServerId, ServerName, 100, 4, 4, 1, 0L, 10, 1.00m, 1, 0, 0, 0L, 1m, false, false, false, false, 65536000L, 20000000L, false, 1, 1, 0, false);
            var memHigh = await DarlingPlanCacheSchedulerReader.GetCpuSchedulerPressureAsync(
                postgres, ServerId, memHighTime.AddMinutes(-1), memHighTime.AddMinutes(1), ct);
            Assert.NotNull(memHigh);
            Assert.True(memHigh!.PhysicalMemoryPressureWarning);

            /* Both pieces stored FALSE; the least available value, 5,000,000 KB, is below 6,553,600 KB: TRUE. */
            var memLowTime = baseTime.AddMinutes(26);
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9411L, memLowTime, ServerId, ServerName, 100, 4, 4, 1, 0L, 10, 1.00m, 1, 0, 0, 0L, 1m, false, false, false, false, 65536000L, 5000000L, false, 1, 1, 0, false);
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9412L, memLowTime, ServerId, ServerName, 100, 4, 4, 1, 0L, 10, 1.00m, 1, 0, 0, 0L, 1m, false, false, false, false, 65536000L, 6000000L, false, 1, 1, 0, false);
            var memLow = await DarlingPlanCacheSchedulerReader.GetCpuSchedulerPressureAsync(
                postgres, ServerId, memLowTime.AddMinutes(-1), memLowTime.AddMinutes(1), ct);
            Assert.NotNull(memLow);
            Assert.True(memLow!.PhysicalMemoryPressureWarning);

            /* L5: 1 runnable task over 8 schedulers is exactly 0.125, rounded in the SQL to 0.13 on both engines. */
            var halfTime = baseTime.AddMinutes(27);
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9421L, halfTime, ServerId, ServerName, 100, 8, 8, 1, 0L, 10, 0.50m, 1, 0, 0, 0L, 1m, false, false, false, false, 65536000L, 30000000L, false, 1, 1, 0, false);
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9422L, halfTime, ServerId, ServerName, 100, 8, 8, 0, 0L, 10, 0.00m, 1, 0, 0, 0L, 1m, false, false, false, false, 65536000L, 30000000L, false, 1, 1, 0, false);
            var half = await DarlingPlanCacheSchedulerReader.GetCpuSchedulerPressureAsync(
                postgres, ServerId, halfTime.AddMinutes(-1), halfTime.AddMinutes(1), ct);
            Assert.NotNull(half);
            Assert.Equal(0.13m, half!.AvgRunnableTasksCount);

            /* The divisor is scheduler_count (it IS the online count, measured on a SQL Server 2019 VM): 6 runnable tasks over 8 is 0.75,
               even with offline_cpu_count 4 stored (no offline subtraction). */
            var offlineTime = baseTime.AddMinutes(28);
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9431L, offlineTime, ServerId, ServerName, 100, 8, 8, 4, 0L, 10, 1.00m, 1, 0, 0, 0L, 1m, false, false, false, false, 65536000L, 30000000L, false, 1, 1, 4, true);
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9432L, offlineTime, ServerId, ServerName, 100, 8, 8, 2, 0L, 10, 1.00m, 1, 0, 0, 0L, 1m, false, false, false, false, 65536000L, 30000000L, false, 1, 1, 4, true);
            var offline = await DarlingPlanCacheSchedulerReader.GetCpuSchedulerPressureAsync(
                postgres, ServerId, offlineTime.AddMinutes(-1), offlineTime.AddMinutes(1), ct);
            Assert.NotNull(offline);
            Assert.Equal(0.75m, offline!.AvgRunnableTasksCount);

            /* The measured affinity case: scheduler_count 1 with offline_cpu_count 3 (4 CPUs, AFFINITY CPU = 0). The old divisor
               (scheduler_count less offline_cpu_count, floored at 0) was NULL here; the average is 5 / 1 = 5.00. */
            var affinityTime = baseTime.AddMinutes(29);
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9441L, affinityTime, ServerId, ServerName, 100, 1, 4, 3, 0L, 10, 1.00m, 1, 0, 0, 0L, 1m, false, false, false, false, 65536000L, 30000000L, false, 1, 1, 3, true);
            await DarlingMcpTestData.ExecAsync(connection, ct, insertSql,
                9442L, affinityTime, ServerId, ServerName, 100, 1, 4, 2, 0L, 10, 1.00m, 1, 0, 0, 0L, 1m, false, false, false, false, 65536000L, 30000000L, false, 1, 1, 3, true);
            var affinity = await DarlingPlanCacheSchedulerReader.GetCpuSchedulerPressureAsync(
                postgres, ServerId, affinityTime.AddMinutes(-1), affinityTime.AddMinutes(1), ct);
            Assert.NotNull(affinity);
            Assert.Equal(5.00m, affinity!.AvgRunnableTasksCount);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, bool keepServer = false)
    {
        var sql = string.Join(" ", new[] { "plan_cache_stats", "cpu_scheduler_stats" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"));
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
