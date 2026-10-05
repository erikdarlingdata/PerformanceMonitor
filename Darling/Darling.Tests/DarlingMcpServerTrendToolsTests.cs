/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shape of <c>get_server_trend</c> (#4843): one tool, a closed <c>metric</c> switch, the standard trend
/// parameters, a hand-registered web read, and one shared SQL text for the viewer's chart reads and the tool.
/// </summary>
public sealed class DarlingMcpServerTrendToolsTests
{
    [Fact]
    public void TheTool_IsRegistered_WithTheStandardTrendParameters()
    {
        var method = typeof(DarlingMcpServerTrendTools).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "get_server_trend");
        var names = method.GetParameters().Select(p => p.Name).ToArray();

        Assert.Equal(["postgres", "metric", "server_name", "hours_back", "as_of", "bucket_minutes", "clerk_types", "names", "cancellationToken"], names);
        Assert.Contains("DarlingMcpServerTrendTools", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheMetricList_IsTheEightShipped_AndEveryOneIsNamedInTheDescription()
    {
        Assert.Equal(["total_waits", "cpu_scheduler", "memory_clerks", "plan_cache", "latch", "spinlock", "session_stats", "collector_duration"], DarlingMcpServerTrendTools.Metrics);
        var served = McpToolGuideTests.Served("get_server_trend");
        foreach (var metric in DarlingMcpServerTrendTools.Metrics)
        {
            Assert.Contains(metric, served.Served, StringComparison.Ordinal);
        }

        Assert.Contains(BaselineDiscontinuities.DescriptionSentence, served.Tail! + served.Served, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnUnknownMetric_IsRefused_BeforeAnythingIsRead()
    {
        var answer = await DarlingMcpServerTrendTools.GetServerTrend(null!, "tempdb_size", null, 24, null, null, null, null, TrendBudget.Mcp(DarlingMcpServerTrendTools.MaxPoints));
        Assert.True(McpHelpers.IsRefusalEnvelope(answer));
        Assert.Contains("total_waits, cpu_scheduler, memory_clerks, plan_cache, latch, spinlock, session_stats, collector_duration", answer, StringComparison.Ordinal);

        var missing = await DarlingMcpServerTrendTools.GetServerTrend(null!, null, null, 24, null, null, null, null, TrendBudget.Mcp(DarlingMcpServerTrendTools.MaxPoints));
        Assert.True(McpHelpers.IsRefusalEnvelope(missing));
    }

    [Fact]
    public async Task ClerkTypes_OnAnotherMetric_OrOverTheCap_AreRefused()
    {
        var wrongMetric = await DarlingMcpServerTrendTools.GetServerTrend(null!, "plan_cache", null, 24, null, null, "MEMORYCLERK_SQLBUFFERPOOL", null, TrendBudget.Mcp(DarlingMcpServerTrendTools.MaxPoints));
        Assert.True(McpHelpers.IsRefusalEnvelope(wrongMetric));

        var tooMany = string.Join(",", Enumerable.Range(0, DarlingMcpServerTrendTools.MaxClerkCount + 1).Select(i => "C" + i));
        var over = await DarlingMcpServerTrendTools.GetServerTrend(null!, "memory_clerks", null, 24, null, null, tooMany, null, TrendBudget.Mcp(DarlingMcpServerTrendTools.MaxPoints));
        Assert.True(McpHelpers.IsRefusalEnvelope(over));
    }

    [Fact]
    public async Task Names_OnAMetricThatTakesNone_OrOverTheCap_AreRefused_AndClerkTypesStayMemoryClerksOnly()
    {
        var budget = TrendBudget.Mcp(DarlingMcpServerTrendTools.MaxPoints);
        foreach (var metric in new[] { "total_waits", "cpu_scheduler", "memory_clerks", "plan_cache", "session_stats" })
        {
            var wrong = await DarlingMcpServerTrendTools.GetServerTrend(null!, metric, null, 24, null, null, null, "A", budget);
            Assert.True(McpHelpers.IsRefusalEnvelope(wrong), metric);
        }

        var tooMany = string.Join(",", Enumerable.Range(0, DarlingMcpServerTrendTools.MaxClerkCount + 1).Select(i => "N" + i));
        foreach (var metric in new[] { "latch", "spinlock", "collector_duration" })
        {
            Assert.True(McpHelpers.IsRefusalEnvelope(await DarlingMcpServerTrendTools.GetServerTrend(null!, metric, null, 24, null, null, null, tooMany, budget)), metric);
            Assert.True(McpHelpers.IsRefusalEnvelope(await DarlingMcpServerTrendTools.GetServerTrend(null!, metric, null, 24, null, null, "CLERK", null, budget)), metric);
        }
    }

    [Fact]
    public void EveryFieldNameANoteMentions_IsAFieldThatMetricEmits()
    {
        var allowedParameters = new[] { "bucket_minutes", "hours_back" };
        foreach (var metric in DarlingMcpServerTrendTools.Metrics)
        {
            var emitted = DarlingMcpServerTrendTools.Fields(metric);
            Assert.NotEmpty(emitted);
            var note = DarlingMcpServerTrendTools.Note(metric, 5, false, 120);
            var words = System.Text.RegularExpressions.Regex.Matches(note, "[a-z]+(?:_[a-z]+)+").Select(m => m.Value).Distinct().ToArray();
            Assert.DoesNotContain(words, w => w.StartsWith("peak_", StringComparison.Ordinal) || w.StartsWith("worst_", StringComparison.Ordinal));
            Assert.All(words, w => Assert.True(emitted.Contains(w) || allowedParameters.Contains(w), $"{metric}: the note names '{w}', which the metric does not emit"));
        }

        Assert.Contains("wait_time_ms_per_second", DarlingMcpServerTrendTools.Note("total_waits", 5, false, 120), StringComparison.Ordinal);
        Assert.Contains("averages as 0", DarlingMcpServerTrendTools.Note("cpu_scheduler", 5, false, 120), StringComparison.Ordinal);
        Assert.Contains("collisions_per_second", DarlingMcpServerTrendTools.Note("spinlock", 5, false, 120), StringComparison.Ordinal);
        Assert.Contains("max_duration_ms", DarlingMcpServerTrendTools.Note("collector_duration", 5, false, 120), StringComparison.Ordinal);
        Assert.Contains("top_host_name", DarlingMcpServerTrendTools.Note("session_stats", 5, false, 120), StringComparison.Ordinal);
    }

    [Fact]
    public void TheDescription_SaysWhereTheUnmatchedNamesAreReported()
    {
        var method = typeof(DarlingMcpServerTrendTools).GetMethods().First(m => m.Name == "GetServerTrend" && m.IsPublic);
        var text = method.GetCustomAttribute<System.ComponentModel.DescriptionAttribute>()!.Description;
        Assert.Contains("missing_names", text, StringComparison.Ordinal);
        var param = method.GetParameters().First(p => p.Name == "names").GetCustomAttribute<System.ComponentModel.DescriptionAttribute>()!.Description;
        Assert.Contains("exact case", param, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLatchAndSpinlockReads_BindTheNamesThenTheWidth_AndKeepTheViewersRateRule()
    {
        foreach (var sql in new[] { ServerTrendSql.LatchWaits(3), ServerTrendSql.SpinlockCollisions(3), ServerTrendSql.CollectorDurations(3) })
        {
            Assert.Contains("IN ($4, $5, $6)", sql, StringComparison.Ordinal);
            Assert.Contains("CAST($7 AS integer)", sql, StringComparison.Ordinal);
        }

        Assert.Contains("NULLIF(sample_interval_seconds, 0)", ServerTrendSql.LatchWaits(1), StringComparison.Ordinal);
        Assert.Contains("PARTITION BY spinlock_name", ServerTrendSql.SpinlockCollisions(1), StringComparison.Ordinal);
        Assert.Contains("status = 'SUCCESS'", ServerTrendSql.CollectorDurations(1), StringComparison.Ordinal);
    }

    [Fact]
    public void TheDescription_SaysClerkNamesAreExact_AndWhereTheUnmatchedOnesAreReported()
    {
        var method = typeof(DarlingMcpServerTrendTools).GetMethods().First(m => m.Name == "GetServerTrend" && m.IsPublic);
        var text = method.GetCustomAttribute<System.ComponentModel.DescriptionAttribute>()!.Description;
        Assert.Contains("missing_clerk_types", text, StringComparison.Ordinal);
        Assert.Contains("matched exactly", text, StringComparison.Ordinal);
        var param = method.GetParameters().First(p => p.Name == "clerk_types").GetCustomAttribute<System.ComponentModel.DescriptionAttribute>()!.Description;
        Assert.Contains("exact case", param, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseClerks_TrimsAndDeduplicates_InGivenOrder()
    {
        Assert.Equal(["B", "A"], DarlingMcpServerTrendTools.ParseClerks(" B, A ,,B"));
        Assert.Empty(DarlingMcpServerTrendTools.ParseClerks("  "));
    }

    [Fact]
    public void TheViewerCharts_AndTheTool_ShareOneSqlText()
    {
        Assert.Equal(ServerTrendSql.TotalWaits, ViewerDataService_TotalWaitSql());
        Assert.Equal(ServerTrendSql.CpuScheduler, ViewerCpuSql());
        Assert.Equal(ServerTrendSql.PlanCache, ViewerPlanCacheSql());
        Assert.Equal(ServerTrendSql.MemoryClerks(3), ViewerMemoryClerksSql(3));
        Assert.Equal(ServerTrendSql.SessionSummary, PerformanceMonitor.Darling.Viewer.ViewerDataService.SessionStatsSql);
    }

    private static string ViewerDataService_TotalWaitSql() => PerformanceMonitor.Darling.Viewer.ViewerDataService.TotalWaitTrendSql;
    private static string ViewerCpuSql() => PerformanceMonitor.Darling.Viewer.ViewerDataService.CpuSchedulerTrendSql;
    private static string ViewerPlanCacheSql() => PerformanceMonitor.Darling.Viewer.ViewerDataService.PlanCacheTrendSql;
    private static string ViewerMemoryClerksSql(int n) => PerformanceMonitor.Darling.Viewer.ViewerDataService.MemoryClerkTrendsSql(n);

    [Fact]
    public void TheWebRead_IsHandRegistered_InTheCatalogAndTheDispatch()
    {
        var web = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        Assert.Contains("[\"get_server_trend\"] = R(CatTrends,", web, StringComparison.Ordinal);
        Assert.Contains("[\"get_server_trend\"] = (c, pg, an) => RequireText(c, \"metric\"", web, StringComparison.Ordinal);
        Assert.Contains("DarlingMcpServerTrendTools.GetServerTrend(pg, serverTrendMetric, Server(c), Hours(c, 24), AsOf(c), bucketMinutes, Str(c, \"clerk_types\"), Str(c, \"names\"), TrendBudget.Chart", web, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryStoreCommand_SetsAnExplicitDeadline()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpServerTrendTools.cs");
        Assert.Equal(
            System.Text.RegularExpressions.Regex.Matches(source, @"postgres\.CreateCommand\(").Count,
            System.Text.RegularExpressions.Regex.Matches(source, @"CommandTimeout = McpCommandDeadlines\.ReadSeconds").Count);
    }
}

/// <summary>
/// One live round-trip per metric against a local TimescaleDB store (set <c>DARLING_TEST_PG</c>): the numbers
/// planted come back, a null column is omitted, an empty window says empty, a server that never collected says
/// unavailable, and the 168-hour answers stay under the 32 KB response target.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpServerTrendToolsLiveTests
{
    private const string ServerName = "server-trend-live";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly string[] Tables = ["wait_stats", "cpu_scheduler_stats", "memory_clerks", "plan_cache_stats", "latch_stats", "spinlock_stats", "session_summary_stats", "collection_log"];

    [Fact]
    public async Task EveryMetric_ReturnsThePlantedSeries_AndTheEmptyAndUnavailableWordsHold()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live server-trend test.");

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

            /* Nothing collected yet: every metric says so outright, not "quiet". */
            foreach (var metric in DarlingMcpServerTrendTools.Metrics.Take(4))
            {
                Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpServerTrendTools.GetServerTrend(postgres, metric, ServerName)));
            }

            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-1);
            var asOf = end.ToString("o", System.Globalization.CultureInfo.InvariantCulture);

            /* Two collections 60 s apart; a 1-hour window sizes to 1-minute buckets, so each is its own point. */
            for (var i = 0; i < 2; i++)
            {
                var t = DarlingMcpTestData.Naive(end.AddMinutes(-2 + i));
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "LCK_M_S", 5L, 30000L, 60);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "CXPACKET", 5L, 30000L, 60);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO cpu_scheduler_stats (collection_id, collection_time, server_id, server_name, max_workers_count, scheduler_count, cpu_count, total_runnable_tasks_count, total_work_queue_count, total_current_workers_count, avg_runnable_tasks_count, total_active_request_count, total_queued_request_count, total_blocked_task_count, total_active_parallel_thread_count, runnable_request_count, total_request_count, runnable_percent, worker_thread_exhaustion_warning, runnable_tasks_warning, blocked_tasks_warning, queued_requests_warning, total_physical_memory_kb, available_physical_memory_kb, physical_memory_pressure_warning, total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, 512, 8, 8, 4 + 2 * i, 5L, 100, 7.5m, 40, 3, 6, 2, 20L, 12, 12.5m, false, false, false, false, 65536000L, 32768000L, false, 1, 1, 0, false);
                foreach (var (clerk, mb) in new[] { ("CLERK_BIG", 40000m), ("CLERK_MID", 900m), ("CLERK_SMALL", 5m) })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        "INSERT INTO memory_clerks (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb) VALUES ($1,$2,$3,$4,$5,$6)",
                        CollectionIdGenerator.Next(), t, ServerId, ServerName, clerk, mb + i);
                }

                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO plan_cache_stats (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype, total_plans, total_size_mb, single_use_plans, single_use_size_mb, multi_use_plans, multi_use_size_mb, avg_use_count, avg_size_kb, oldest_plan_create_time)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "Compiled Plan", "Adhoc", 100, 500, 80, 400 + i, 20, 100, 3.5m, 64, t);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO plan_cache_stats (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype, total_plans, total_size_mb, single_use_plans, single_use_size_mb, multi_use_plans, multi_use_size_mb, avg_use_count, avg_size_kb, oldest_plan_create_time)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "Compiled Plan", "Proc", 100, 500, 10, 50, 90, 450, 3.5m, 64, t);
            }

            // total_waits: two wait types x 30000 ms over 60 s = 1000 ms per second.
            var waits = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "total_waits", ServerName, 1, asOf)).RootElement;
            Assert.Equal("total_waits", waits.GetProperty("metric").GetString());
            Assert.Equal("1 minute", waits.GetProperty("bucket").GetString());
            var wps = waits.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(2, wps.Length);
            Assert.All(wps, wp => Assert.Equal(1000.0, wp.GetProperty("wait_time_ms_per_second").GetDouble(), 2));
            Assert.Equal(0, waits.GetProperty("discontinuities").GetArrayLength());

            // cpu_scheduler: runnable 4 then 6, blocked 6, queued 3.
            var cpu = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "cpu_scheduler", ServerName, 1, asOf)).RootElement;
            var cps = cpu.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal([4.0, 6.0], cps.Select(p => p.GetProperty("runnable_tasks").GetDouble()).ToArray());
            Assert.All(cps, cp => Assert.Equal(6.0, cp.GetProperty("blocked_tasks").GetDouble(), 2));
            Assert.All(cps, cp => Assert.Equal(3.0, cp.GetProperty("queued_requests").GetDouble(), 2));

            // memory_clerks: the default is the heaviest clerks, biggest first; a named clerk narrows it.
            var clerks = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "memory_clerks", ServerName, 1, asOf)).RootElement;
            Assert.Equal(["CLERK_BIG", "CLERK_MID", "CLERK_SMALL"], clerks.GetProperty("series").EnumerateArray().Select(s => s.GetProperty("clerk_type").GetString()).ToArray());
            Assert.Equal([40000.0, 40001.0], clerks.GetProperty("series")[0].GetProperty("trend").EnumerateArray().Select(p => p.GetProperty("memory_mb").GetDouble()).ToArray());
            var one = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "memory_clerks", ServerName, 1, asOf, clerk_types: "CLERK_MID")).RootElement;
            Assert.Single(one.GetProperty("series").EnumerateArray());
            Assert.False(one.TryGetProperty("missing_clerk_types", out _));

            // A named type the window never recorded is reported, and the types that were found still come back.
            var partial = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "memory_clerks", ServerName, 1, asOf, clerk_types: "CLERK_MID, clerk_mid, CLERK_NOPE")).RootElement;
            Assert.Equal(["CLERK_MID"], partial.GetProperty("series").EnumerateArray().Select(sr => sr.GetProperty("clerk_type").GetString()).ToArray());
            Assert.Equal(["clerk_mid", "CLERK_NOPE"], partial.GetProperty("missing_clerk_types").EnumerateArray().Select(x => x.GetString()).ToArray());

            // None found: not "quiet" - a distinct note, the misses, and the heaviest types the window did record.
            var none = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "memory_clerks", ServerName, 1, asOf, clerk_types: "CLERK_NOPE")).RootElement;
            Assert.Equal("empty", none.GetProperty("status").GetString());
            var noneMessage = none.GetProperty("message").GetString()!;
            Assert.Contains("None of the named clerk types", noneMessage, StringComparison.Ordinal);
            Assert.DoesNotContain("genuinely quiet", noneMessage, StringComparison.Ordinal);
            Assert.Equal(["CLERK_NOPE"], none.GetProperty("hints").GetProperty("missing_clerk_types").EnumerateArray().Select(x => x.GetString()).ToArray());
            Assert.Equal(["CLERK_BIG", "CLERK_MID", "CLERK_SMALL"], none.GetProperty("hints").GetProperty("heaviest_clerk_types").EnumerateArray().Select(x => x.GetString()).ToArray());

            // plan_cache: single-use is 400 + 50 (Adhoc + Proc), +1 on the second collection; multi-use 100 + 450.
            var plan = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "plan_cache", ServerName, 1, asOf)).RootElement;
            var pps = plan.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal([450.0, 451.0], pps.Select(p => p.GetProperty("single_use_mb").GetDouble()).ToArray());
            Assert.All(pps, pp => Assert.Equal(550.0, pp.GetProperty("multi_use_mb").GetDouble(), 2));

            // A window with nothing in it, on a server that HAS collected, says empty.
            var quiet = await DarlingMcpServerTrendTools.GetServerTrend(postgres, "plan_cache", ServerName, 1, end.AddDays(-3).ToString("o", System.Globalization.CultureInfo.InvariantCulture));
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(quiet));

            // A null column is omitted, not written as null.
            await DarlingMcpTestData.ExecAsync(connection, ct, $"UPDATE plan_cache_stats SET single_use_size_mb = NULL, multi_use_size_mb = NULL WHERE server_id = {ServerId}");
            var nulls = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "plan_cache", ServerName, 1, asOf)).RootElement;
            Assert.All(nulls.GetProperty("trend").EnumerateArray(), p => Assert.False(p.TryGetProperty("single_use_mb", out _)));

            // A week of one-minute collections: the default answer stays under the 32 KB response target, and the
            // widest explicit answer a caller can ask for is measured (reported, and bounded by the point cap).
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM wait_stats WHERE server_id = {ServerId}; DELETE FROM cpu_scheduler_stats WHERE server_id = {ServerId}; DELETE FROM memory_clerks WHERE server_id = {ServerId}; DELETE FROM plan_cache_stats WHERE server_id = {ServerId};");
            var weekStart = DarlingMcpTestData.Naive(end.AddMinutes(-(7 * 24 * 60 - 5)));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds)
SELECT 1000000 + g, $1::timestamp + g * INTERVAL '1 minute', $2, $3, w, 5, 12345.678, 60 FROM generate_series(0, 10070) g, unnest(ARRAY['LCK_M_S','CXPACKET']) w",
                weekStart, ServerId, ServerName);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO cpu_scheduler_stats (collection_id, collection_time, server_id, server_name, max_workers_count, scheduler_count, cpu_count, total_runnable_tasks_count, total_work_queue_count, total_current_workers_count, avg_runnable_tasks_count, total_active_request_count, total_queued_request_count, total_blocked_task_count, total_active_parallel_thread_count, runnable_request_count, total_request_count, runnable_percent, worker_thread_exhaustion_warning, runnable_tasks_warning, blocked_tasks_warning, queued_requests_warning, total_physical_memory_kb, available_physical_memory_kb, physical_memory_pressure_warning, total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
SELECT 2000000 + g, $1::timestamp + g * INTERVAL '1 minute', $2, $3, 512, 8, 8, g % 17, 5, 100, 7.5, 40, g % 5, g % 7, 2, 20, 12, 12.5, false, false, false, false, 65536000, 32768000, false, 1, 1, 0, false FROM generate_series(0, 10070) g",
                weekStart, ServerId, ServerName);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO memory_clerks (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
SELECT 3000000 + g * 20 + c, $1::timestamp + g * INTERVAL '1 minute', $2, $3, 'MEMORYCLERK_TYPE_' || c, 1000 + g % 977 + c * 31.37 FROM generate_series(0, 10070) g, generate_series(1, 12) c",
                weekStart, ServerId, ServerName);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO plan_cache_stats (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype, total_plans, total_size_mb, single_use_plans, single_use_size_mb, multi_use_plans, multi_use_size_mb, avg_use_count, avg_size_kb, oldest_plan_create_time)
SELECT 4000000 + g * 2 + o, $1::timestamp + g * INTERVAL '1 minute', $2, $3, 'Compiled Plan', 'T' || o, 100, 500, 80, 400 + g % 313 + 0.37, 20, 100 + g % 91, 3.5, 64, $1::timestamp FROM generate_series(0, 10070) g, generate_series(1, 2) o",
                weekStart, ServerId, ServerName);

            var sizes = new System.Collections.Generic.List<string>();
            foreach (var metric in DarlingMcpServerTrendTools.Metrics.Take(4))
            {
                var week = await DarlingMcpServerTrendTools.GetServerTrend(postgres, metric, ServerName, 168, asOf);
                var bytes = System.Text.Encoding.UTF8.GetByteCount(week);
                sizes.Add($"{metric} default 168h: {bytes} bytes");
                Assert.True(bytes <= McpResponseBudget.DefaultBytes, $"{metric}: {bytes} bytes over the 32 KB target");

                var series = metric == "memory_clerks" ? DarlingMcpServerTrendTools.DefaultClerkCount : 1;
                var width = TrendBuckets.NarrowestFitting(7 * 24 * 60, series, DarlingMcpServerTrendTools.MaxPoints);
                var widest = await DarlingMcpServerTrendTools.GetServerTrend(postgres, metric, ServerName, 168, asOf, bucket_minutes: width);
                var widestBytes = System.Text.Encoding.UTF8.GetByteCount(widest);
                sizes.Add($"{metric} widest 168h at {width} min: {widestBytes} bytes");
                Assert.True(widestBytes <= 320 * 1024, $"{metric}: widest {widestBytes} bytes");
            }

            TestContext.Current.TestOutputHelper?.WriteLine(string.Join(Environment.NewLine, sizes));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task LatchSpinlockSessionStatsAndCollectorDuration_ReturnThePlantedSeries()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live server-trend test.");

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
            var later = new[] { "latch", "spinlock", "session_stats", "collector_duration" };
            foreach (var metric in later)
            {
                Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpServerTrendTools.GetServerTrend(postgres, metric, ServerName)));
            }

            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-1);
            var asOf = end.ToString("o", System.Globalization.CultureInfo.InvariantCulture);

            for (var i = 0; i < 2; i++)
            {
                var t = DarlingMcpTestData.Naive(end.AddMinutes(-2 + i));
                foreach (var (latchClass, delta) in new[] { ("LATCH_BIG", 6000L), ("LATCH_MID", 600L), ("LATCH_SMALL", 6L) })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        "INSERT INTO latch_stats (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms, delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms, sample_interval_seconds) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::integer)",
                        CollectionIdGenerator.Next(), t, ServerId, ServerName, latchClass, 1000L, 20000L, 50L, 10L, delta, 5L, 60);
                }

                /* A restart sample: the stored interval is the 0 marker, so it is not a point and is not drawn as 0. */
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO latch_stats (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms, delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms, sample_interval_seconds) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::integer)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "LATCH_RESTART", 1000L, 20000L, 50L, 0L, 0L, 0L, 0);

                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO spinlock_stats (collection_id, collection_time, server_id, server_name, spinlock_name, collisions, spins, spins_per_collision, sleep_time, backoffs, delta_collisions, delta_spins, delta_sleep_time, delta_backoffs, sample_interval_seconds) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::integer)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "LOCK_HASH", 900000L, 5000000L, 5.5d, 100L, 200L, 1200L, 4000L, 3L, 7L, 60);

                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO session_summary_stats (collection_id, collection_time, server_id, server_name, total_sessions, running_sessions, sleeping_sessions, background_sessions, dormant_sessions, idle_sessions_over_30min, sessions_waiting_for_memory, databases_with_connections, top_application_name, top_application_connections, top_host_name, top_host_connections)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, 100 + 20 * i, 5, 90, 3, 2, 1, 0, 4, "app-" + i, 50 + i, "host-" + i, 40 + i);

                foreach (var (collector, ms) in new[] { ("slow_collector", 9000), ("quick_collector", 120) })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status) VALUES ($1,$2,$3,$4,$5,$6,'SUCCESS')",
                        CollectionIdGenerator.Next(), ServerId, ServerName, collector, t, ms + 100 * i);
                }

                /* A failed run is not a duration. */
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status) VALUES ($1,$2,$3,$4,$5,$6,'ERROR')",
                    CollectionIdGenerator.Next(), ServerId, ServerName, "failing_collector", t, 77777);
            }

            // latch: the default is the heaviest classes, biggest first; 6000 ms over 60 s = 100 ms/s; the restart row is no point and no series.
            var latch = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "latch", ServerName, 1, asOf, bucket_minutes: 1)).RootElement;
            Assert.Equal(["LATCH_BIG", "LATCH_MID", "LATCH_SMALL"], latch.GetProperty("series").EnumerateArray().Select(s => s.GetProperty("latch_class").GetString()).ToArray());
            var big = latch.GetProperty("series")[0].GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(2, big.Length);
            Assert.All(big, p => Assert.Equal(100.0, p.GetProperty("wait_time_ms_per_second").GetDouble(), 2));
            Assert.False(latch.TryGetProperty("missing_names", out _));
            Assert.Equal(0, latch.GetProperty("discontinuities").GetArrayLength());

            var named = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "latch", ServerName, 1, asOf, bucket_minutes: 1, names: "LATCH_MID, latch_mid, LATCH_NOPE")).RootElement;
            Assert.Equal(["LATCH_MID"], named.GetProperty("series").EnumerateArray().Select(s => s.GetProperty("latch_class").GetString()).ToArray());
            Assert.Equal(["latch_mid", "LATCH_NOPE"], named.GetProperty("missing_names").EnumerateArray().Select(x => x.GetString()).ToArray());

            var none = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "latch", ServerName, 1, asOf, names: "LATCH_NOPE")).RootElement;
            Assert.Equal("empty", none.GetProperty("status").GetString());
            Assert.Contains("None of the named latch classes", none.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal(["LATCH_NOPE"], none.GetProperty("hints").GetProperty("missing_names").EnumerateArray().Select(x => x.GetString()).ToArray());
            Assert.Equal(["LATCH_BIG", "LATCH_MID", "LATCH_SMALL", "LATCH_RESTART"], none.GetProperty("hints").GetProperty("top_names").EnumerateArray().Select(x => x.GetString()).ToArray());

            // spinlock: 1200 collisions over 60 s = 20 per second.
            var spin = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "spinlock", ServerName, 1, asOf, bucket_minutes: 1)).RootElement;
            Assert.Equal("LOCK_HASH", spin.GetProperty("series")[0].GetProperty("spinlock_name").GetString());
            Assert.All(spin.GetProperty("series")[0].GetProperty("trend").EnumerateArray(), p => Assert.Equal(20.0, p.GetProperty("collisions_per_second").GetDouble(), 2));

            // session_stats: the count is averaged per bucket (100 then 120); the attribution is the bucket's newest collection's own.
            var sessions = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "session_stats", ServerName, 1, asOf, bucket_minutes: 1)).RootElement;
            var sps = sessions.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal([100.0, 120.0], sps.Select(p => p.GetProperty("total_sessions").GetDouble()).ToArray());
            Assert.Equal(["app-0", "app-1"], sps.Select(p => p.GetProperty("top_application_name").GetString()).ToArray());
            Assert.Equal([40.0, 41.0], sps.Select(p => p.GetProperty("top_host_connections").GetDouble()).ToArray());
            Assert.All(sps, p => Assert.Equal(4.0, p.GetProperty("databases_with_connections").GetDouble(), 2));

            // collector_duration: the slowest collector first, success runs only, the failed one is never a series.
            var durations = JsonDocument.Parse(await DarlingMcpServerTrendTools.GetServerTrend(postgres, "collector_duration", ServerName, 1, asOf, bucket_minutes: 1)).RootElement;
            Assert.Equal(["slow_collector", "quick_collector"], durations.GetProperty("series").EnumerateArray().Select(s => s.GetProperty("collector_name").GetString()).ToArray());
            var slow = durations.GetProperty("series")[0].GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal([9000.0, 9100.0], slow.Select(p => p.GetProperty("max_duration_ms").GetDouble()).ToArray());
            Assert.All(slow, p => Assert.Equal(1.0, p.GetProperty("run_count").GetDouble(), 2));

            // A quiet window on a server that HAS collected says empty.
            var quiet = end.AddDays(-3).ToString("o", System.Globalization.CultureInfo.InvariantCulture);
            foreach (var metric in later)
            {
                Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpServerTrendTools.GetServerTrend(postgres, metric, ServerName, 1, quiet)));
            }

            // A week of one-minute collections: the default answer stays under the 32 KB target; the widest is measured.
            await DarlingMcpTestData.ExecAsync(connection, ct, string.Join(" ", new[] { "latch_stats", "spinlock_stats", "session_summary_stats", "collection_log" }.Select(tb => $"DELETE FROM {tb} WHERE server_id = {ServerId};")));
            var weekStart = DarlingMcpTestData.Naive(end.AddMinutes(-(7 * 24 * 60 - 5)));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO latch_stats (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms, delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms, sample_interval_seconds)
SELECT 5000000 + g * 8 + c, $1::timestamp + g * INTERVAL '1 minute', $2, $3, 'LATCH_' || c, 1000, 20000, 50, 10, 1234 + g % 977 + c * 31, 5, 60 FROM generate_series(0, 10070) g, generate_series(1, 8) c",
                weekStart, ServerId, ServerName);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO spinlock_stats (collection_id, collection_time, server_id, server_name, spinlock_name, collisions, spins, spins_per_collision, sleep_time, backoffs, delta_collisions, delta_spins, delta_sleep_time, delta_backoffs, sample_interval_seconds)
SELECT 6000000 + g * 8 + c, $1::timestamp + g * INTERVAL '1 minute', $2, $3, 'SPIN_' || c, 900000, 5000000, 5.5, 100, 200, 1200 + g % 311 + c * 17, 4000, 3, 7, 60 FROM generate_series(0, 10070) g, generate_series(1, 8) c",
                weekStart, ServerId, ServerName);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO session_summary_stats (collection_id, collection_time, server_id, server_name, total_sessions, running_sessions, sleeping_sessions, background_sessions, dormant_sessions, idle_sessions_over_30min, sessions_waiting_for_memory, databases_with_connections, top_application_name, top_application_connections, top_host_name, top_host_connections)
SELECT 7000000 + g, $1::timestamp + g * INTERVAL '1 minute', $2, $3, 100 + g % 53, 5 + g % 7, 90, 3, 2, 1, 0, 4, 'Microsoft SQL Server Management Studio', 50 + g % 9, 'WORKSTATION-' || g % 13, 40 + g % 5 FROM generate_series(0, 10070) g",
                weekStart, ServerId, ServerName);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
SELECT 8000000 + g * 12 + c, $2, $3, 'collector_' || c, $1::timestamp + g * INTERVAL '1 minute', 100 + g % 997 + c * 13, 'SUCCESS' FROM generate_series(0, 10070) g, generate_series(1, 12) c",
                weekStart, ServerId, ServerName);

            var sizes = new System.Collections.Generic.List<string>();
            foreach (var metric in later)
            {
                var week = await DarlingMcpServerTrendTools.GetServerTrend(postgres, metric, ServerName, 168, asOf);
                var bytes = System.Text.Encoding.UTF8.GetByteCount(week);
                sizes.Add($"{metric} default 168h: {bytes} bytes");
                Assert.True(bytes <= McpResponseBudget.DefaultBytes, $"{metric}: {bytes} bytes over the 32 KB target");

                var series = DarlingMcpServerTrendTools.SeriesOf(metric) is null ? (metric == "session_stats" ? DarlingMcpServerTrendTools.SessionPointWeight : 1) : DarlingMcpServerTrendTools.DefaultClerkCount;
                var width = TrendBuckets.NarrowestFitting(7 * 24 * 60, series, DarlingMcpServerTrendTools.MaxPoints);
                var widest = await DarlingMcpServerTrendTools.GetServerTrend(postgres, metric, ServerName, 168, asOf, bucket_minutes: width);
                var widestBytes = System.Text.Encoding.UTF8.GetByteCount(widest);
                sizes.Add($"{metric} widest 168h at {width} min: {widestBytes} bytes");
                Assert.True(widestBytes <= 320 * 1024, $"{metric}: widest {widestBytes} bytes");
            }

            TestContext.Current.TestOutputHelper?.WriteLine(string.Join(Environment.NewLine, sizes));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var sql = string.Join(" ", Tables.Select(t => $"DELETE FROM {t} WHERE server_id = {ServerId};")) + $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
