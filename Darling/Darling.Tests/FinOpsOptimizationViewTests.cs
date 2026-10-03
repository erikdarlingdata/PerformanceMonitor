/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

public sealed class FinOpsOptimizationViewTests
{
    [Fact]
    public void ServedHead_NamesTheView_AndStaysUnderTheTarget()
    {
        var served = McpToolGuideTests.Served("get_finops");
        Assert.Contains("optimization:", served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"served head is {served.Served.Length}");
        Assert.True(DarlingMcpFinOpsTools.OptimizationViewLine.Length <= 80);
    }

    [Fact]
    public void GuideTail_CarriesTheViewsKeyFacts()
    {
        var tail = McpToolGuideTests.Served("get_finops").Tail;
        Assert.NotNull(tail);
        Assert.Contains("window_days 7; hours_back does not move it", tail, StringComparison.Ordinal);
        Assert.Contains("the monitored server's own clock, not UTC", tail, StringComparison.Ordinal);
        Assert.Contains("printed yyyy-MM-ddTHH:mm:ss with no Z", tail, StringComparison.Ordinal);
        Assert.Contains("ordered by total_size_mb descending then database_name", tail, StringComparison.Ordinal);
        Assert.Contains("up to 500 rows with database_count and truncated", tail, StringComparison.Ordinal);
        Assert.Contains("a fixed 24 hours (window_hours 24)", tail, StringComparison.Ordinal);
        Assert.Contains("ordered by total_wait_time_ms descending then category", tail, StringComparison.Ordinal);
        Assert.Contains("ordered by total_cpu_ms descending then database_name then query_preview", tail, StringComparison.Ordinal);
        Assert.Contains("limit is the top-N here (1-50, default 10; the desktop shows 20)", tail, StringComparison.Ordinal);
        Assert.Contains("which of several statements tied at the top-N cut is returned is the engine's choice", tail, StringComparison.Ordinal);
        Assert.Contains("so for expensive_queries it depends on limit", tail, StringComparison.Ordinal);
        Assert.Contains("rounded to 2 places", tail, StringComparison.Ordinal);
        Assert.Contains("'monthly cost not set'", tail, StringComparison.Ordinal);
        Assert.Contains("get_plan_xml", tail, StringComparison.Ordinal);
        Assert.Contains("ordered by day ascending", tail, StringComparison.Ordinal);
    }

    [Fact]
    public void ViewsAllowList_ContainsTheView() =>
        Assert.Contains("optimization", DarlingMcpFinOpsTools.Views);

    private static WaitCategorySummary Wait(string category, long total) => new(category, total, 1, 1m, "W", total);

    private static ExpensiveQuery Query(string db, string preview, long cpu) =>
        new(db, cpu, 1m, 1, 1m, 1, preview, preview, null);

    [Fact]
    public void OrderWaitCategories_BreaksATieByCategory_FromAReversedInput()
    {
        var expected = new[] { Wait("Cpu", 900), Wait("Locks", 700), Wait("Memory", 700), Wait("Other", 100) };
        var ordered = DarlingMcpFinOpsTools.OrderWaitCategories(expected.Reverse());
        Assert.Equal(new[] { "Cpu", "Locks", "Memory", "Other" }, ordered.Select(r => r.Category).ToArray());
    }

    [Fact]
    public void OrderExpensiveQueries_BreaksTiesByDatabase_ThenByPreview_FromAReversedInput()
    {
        /* Alpha/zz and Beta/aa tie on CPU and differ by database (the preview order would reverse them); Gamma/aa and Gamma/bb tie on both. */
        var expected = new[]
        {
            Query("AlphaDb", "zz", 5000), Query("BetaDb", "aa", 5000),
            Query("GammaDb", "aa", 4000), Query("GammaDb", "bb", 4000),
            Query("AlphaDb", "aa", 10),
        };
        var ordered = DarlingMcpFinOpsTools.OrderExpensiveQueries(expected.Reverse());
        Assert.Equal(new[] { "AlphaDb/zz", "BetaDb/aa", "GammaDb/aa", "GammaDb/bb", "AlphaDb/aa" },
            ordered.Select(r => r.DatabaseName + "/" + r.QueryPreview).ToArray());
    }

    [Fact]
    public void OrderIdleDatabases_BreaksASizeTieByName_FromAReversedInput()
    {
        var expected = new[]
        {
            new IdleDatabase("Zeta", 900m, 1, null), new IdleDatabase("Alpha", 500m, 1, null), new IdleDatabase("Bravo", 500m, 1, null),
        };
        var ordered = DarlingMcpFinOpsTools.OrderIdleDatabases(expected.Reverse());
        Assert.Equal(new[] { "Zeta", "Alpha", "Bravo" }, ordered.Select(r => r.DatabaseName).ToArray());
    }

    [Fact]
    public void CostShare_IsTheDesktopsFigure_AgainstLiteralDollars()
    {
        /* 1000 a month over 24 hours is a 32.876712... budget: 9000 of 24000 is 12.33, 6000 is 8.22, 4000 is 5.48. */
        Assert.Equal(12.33m, DarlingMcpFinOpsTools.OptimizationCostShare(9000, 24000, 1000m, 24));
        Assert.Equal(8.22m, DarlingMcpFinOpsTools.OptimizationCostShare(6000, 24000, 1000m, 24));
        Assert.Equal(5.48m, DarlingMcpFinOpsTools.OptimizationCostShare(4000, 24000, 1000m, 24));
        /* A midpoint rounds away from zero: half of a 0.05 budget is 0.025, which is 0.03 (to even it would be 0.02). */
        Assert.Equal(0.03m, DarlingMcpFinOpsTools.OptimizationCostShare(1, 2, 0.05m, 730));
        /* No cost, a zero cost, or rows that total 0 give no figure. */
        Assert.Null(DarlingMcpFinOpsTools.OptimizationCostShare(9000, 24000, 0m, 24));
        Assert.Null(DarlingMcpFinOpsTools.OptimizationCostShare(0, 0, 1000m, 24));
    }

    [Fact]
    public void WaitCategoryRows_CarryTheShareOfTheReturnedRows_AndNullWithoutACost()
    {
        var rows = new[] { Wait("Locks", 700), Wait("Cpu", 2300) };
        var withCost = System.Text.Json.JsonSerializer.Serialize(DarlingMcpFinOpsTools.WaitCategoryRows(rows, 1000m, 24));
        using (var doc = JsonDocument.Parse(withCost))
        {
            var first = doc.RootElement[0];
            Assert.Equal("Cpu", first.GetProperty("category").GetString());
            /* 2300 of 3000 of the 32.876712... budget. */
            Assert.Equal(25.21m, first.GetProperty("est_cost_usd").GetDecimal());
            Assert.Equal(7.67m, doc.RootElement[1].GetProperty("est_cost_usd").GetDecimal());
        }
        var noCost = System.Text.Json.JsonSerializer.Serialize(DarlingMcpFinOpsTools.WaitCategoryRows(rows, 0m, 24));
        using (var doc = JsonDocument.Parse(noCost))
            Assert.All(doc.RootElement.EnumerateArray(), r => Assert.Equal(JsonValueKind.Null, r.GetProperty("est_cost_usd").ValueKind));
    }
}

/* #1776 own-store: each fact seeds its own scratch database, so nothing here shares rows with another test. */
public sealed class FinOpsOptimizationViewLiveTests
{
    private const string ServerName = "darling-finops-optview-a";
    private const string EmptyServerName = "darling-finops-optview-b";
    private const string PostgresServerName = "darling-finops-optview-c";
    private const string TempdbOnlyServerName = "darling-finops-optview-d";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly int EmptyServerId = ServerIdHelper.GetDeterministicHashCode(EmptyServerName);
    private static readonly int PostgresServerId = ServerIdHelper.GetDeterministicHashCode(PostgresServerName);
    private static readonly int TempdbOnlyServerId = ServerIdHelper.GetDeterministicHashCode(TempdbOnlyServerName);

    private static string? Cs()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live optimization view test.");
        return cs;
    }

    private async Task<ScratchPostgres> SeedAsync(string cs, CancellationToken ct, decimal? monthlyCost = 1000m)
    {
        /* The memory-grant days are yesterday and today (UTC); keep clear of the last minutes of the day. */
        Assert.SkipWhen(DateTime.UtcNow.TimeOfDay > new TimeSpan(23, 45, 0), "The seeded grant days need the clock before 23:45 UTC.");
        var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, ServerId, ServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, EmptyServerId, EmptyServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, TempdbOnlyServerId, TempdbOnlyServerName, ct);
        await PgTargetFactCollectorTests.RegisterServerAsync(c, PostgresServerId, PostgresServerName, MonitoredEngineKind.Postgres, 16, ct);
        await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE servers SET monthly_cost_usd = $2 WHERE server_id = $1", ServerId, (object?)monthlyCost ?? DBNull.Value);
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);

        /* Sizes: the latest snapshot. IdleBig has two files; master is a system database and is left out. */
        await Size(c, ct, now.AddHours(-3), "IdleBig", 300.5m);
        await Size(c, ct, now.AddHours(-3), "IdleBig", 200.25m);
        await Size(c, ct, now.AddHours(-3), "IdleSmall", 64m);
        await Size(c, ct, now.AddHours(-3), "BusyDb", 900m);
        await Size(c, ct, now.AddHours(-3), "master", 5000m);

        /* Activity: BusyDb ran in the window. IdleBig has rows that ran nothing, and a server-local last execution well before the 7 days. */
        await Query(c, ct, now.AddDays(-2), "BusyDb", "0xB0", "SELECT busy", 1_000_000, 5, 100, new DateTime(2026, 10, 1, 9, 0, 0), null);
        await Query(c, ct, now.AddDays(-2), "IdleBig", "0xB1", "SELECT idle", 0, 0, 0, new DateTime(2026, 9, 20, 3, 4, 5), null);

        /* Expensive statements. Beta and Gamma tie on CPU; Alpha has a plan; Delta's text is 260 characters. */
        await Query(c, ct, now.AddHours(-2), "AlphaDb", "0xE1", "SELECT alpha", 9_000_000, 30, 1000, null, "<ShowPlanXML />");
        await Query(c, ct, now.AddHours(-2), "DeltaDb", "0xE4", "SELECT " + new string('d', 260), 6_000_000, 3, 50, null, null);
        await Query(c, ct, now.AddHours(-2), "BetaDb", "0xE2", "SELECT beta", 4_000_000, 8, 300, null, null);
        await Query(c, ct, now.AddHours(-2), "GammaDb", "0xE3", "SELECT gamma", 4_000_000, 8, 200, null, null);

        /* tempdb: the 30-hour-old sample is outside the 24-hour peak. */
        await Tempdb(c, ct, ServerId, ServerName, now.AddHours(-30), 9000m, 9000m, 9000m, 27000m);
        await Tempdb(c, ct, ServerId, ServerName, now.AddHours(-10), 1500.5m, 300m, 2500m, 4300.5m);
        await Tempdb(c, ct, ServerId, ServerName, now.AddHours(-1), 800m, 1200.25m, 100m, 2100.25m);
        await Tempdb(c, ct, TempdbOnlyServerId, TempdbOnlyServerName, now.AddHours(-1), 800m, 1200.25m, 100m, 2100.25m);

        /* Waits: Locks and Memory tie on 700. Other holds 100 inside 24 hours and 5100 inside 48. */
        await Wait(c, ct, now.AddHours(-2), "CXPACKET", 1000, 10);
        await Wait(c, ct, now.AddHours(-3), "CXPACKET ", 500, 5);
        await Wait(c, ct, now.AddHours(-2), "SOS_SCHEDULER_YIELD", 800, 20);
        await Wait(c, ct, now.AddHours(-2), "PAGEIOLATCH_SH", 700, 7);
        await Wait(c, ct, now.AddHours(-2), "WRITELOG", 300, 3);
        await Wait(c, ct, now.AddHours(-2), "LCK_M_X", 700, 7);
        await Wait(c, ct, now.AddHours(-2), "RESOURCE_SEMAPHORE", 700, 9);
        await Wait(c, ct, now.AddHours(-2), "BROKER_TASK_STOP", 100, 1);
        await Wait(c, ct, now.AddHours(-30), "OUTSIDE_WAIT", 5000, 50);

        /* Memory grants: two samples yesterday, one today. */
        var today = now.Date;
        await Grant(c, ct, today.AddMinutes(-10), 2000m, 1500m, 3, 1, 2, 1);
        await Grant(c, ct, today.AddMinutes(-5), 1000m, 500m, 3, 2, 3, 0);
        await Grant(c, ct, today.AddMinutes(5), 800m, 200m, 4, 0, 0, 7);
        return scratch;
    }

    private static Task Size(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, decimal sizeMb) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, total_size_mb) VALUES ($1, $2, $3, $4, $5, $6)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, db, sizeMb);

    private static Task Query(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, string handle, string text,
        long cpuUs, long exec, long logical, DateTime? lastExecution, string? planXml) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                query_text, delta_worker_time, delta_execution_count, delta_logical_reads, sample_interval_seconds,
                last_execution_time, query_plan_xml)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, db, "0xQ" + handle, handle, text, cpuUs, exec, logical,
            60, lastExecution, planXml);

    private static Task Tempdb(NpgsqlConnection c, CancellationToken ct, int serverId, string serverName, DateTime at, decimal user,
        decimal internalObjects, decimal versionStore, decimal total) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, user_object_reserved_mb,
                internal_object_reserved_mb, version_store_reserved_mb, total_reserved_mb)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            CollectionIdGenerator.Next(), at, serverId, serverName, user, internalObjects, versionStore, total);

    private static Task Wait(NpgsqlConnection c, CancellationToken ct, DateTime at, string type, long waitMs, long tasks) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_wait_time_ms, delta_waiting_tasks)
              VALUES ($1,$2,$3,$4,$5,$6,$7)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, type, waitMs, tasks);

    private static Task Grant(NpgsqlConnection c, CancellationToken ct, DateTime at, decimal granted, decimal used,
        int grantees, int waiters, long timeoutDelta, long forcedDelta) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id, target_memory_mb, max_target_memory_mb, total_memory_mb, available_memory_mb, granted_memory_mb, used_memory_mb, grantee_count, waiter_count, timeout_error_count, forced_grant_count, timeout_error_count_delta, forced_grant_count_delta)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, (short)0, 2, 8000m, 12000m, 8000m, 8000m - granted, granted, used,
            grantees, waiters, 4L, 2L, timeoutDelta, forcedDelta);

    private static async Task<JsonDocument> ToolAsync(NpgsqlDataSource ds, string server, int hours, int limit, CancellationToken ct) =>
        JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "optimization", server, hours, limit, cancellationToken: ct));

    private static JsonElement Section(JsonDocument doc, string name) => doc.RootElement.GetProperty(name);

    [Fact]
    public async Task OneRowPerSection_ReadsFieldByField_AgainstLiteralValues()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);
        var before = DateTime.UtcNow;
        using var tool = await ToolAsync(ds, ServerName, 24, 10, ct);
        var after = DateTime.UtcNow;
        var root = tool.RootElement;

        Assert.Equal(ServerName, root.GetProperty("server").GetString());
        Assert.Equal(24, root.GetProperty("hours_back").GetInt32());
        Assert.Equal(1000m, root.GetProperty("monthly_cost_usd").GetDecimal());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("cost_reason").ValueKind);

        var idle = Section(tool, "idle_databases");
        Assert.Equal("ok", idle.GetProperty("status").GetString());
        Assert.Equal(7, idle.GetProperty("window_days").GetInt32());
        Assert.Equal(2, idle.GetProperty("database_count").GetInt32());
        Assert.False(idle.GetProperty("truncated").GetBoolean());
        var big = idle.GetProperty("rows")[0];
        Assert.Equal("IdleBig", big.GetProperty("database_name").GetString());
        Assert.Equal(500.75m, big.GetProperty("total_size_mb").GetDecimal());
        Assert.Equal(2, big.GetProperty("file_count").GetInt32());
        Assert.Equal("2026-09-20T03:04:05", big.GetProperty("last_execution_server_local").GetString());
        Assert.Equal("IdleSmall", idle.GetProperty("rows")[1].GetProperty("database_name").GetString());
        Assert.Equal(JsonValueKind.Null, idle.GetProperty("rows")[1].GetProperty("last_execution_server_local").ValueKind);

        var tempdb = Section(tool, "tempdb_pressure");
        Assert.Equal("ok", tempdb.GetProperty("status").GetString());
        Assert.Equal(24, tempdb.GetProperty("window_hours").GetInt32());
        Assert.Equal(new[] { "User Objects", "Internal Objects", "Version Store", "Total Reserved" },
            tempdb.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("metric").GetString()).ToArray());
        var version = tempdb.GetProperty("rows")[2];
        Assert.Equal("Version Store", version.GetProperty("metric").GetString());
        Assert.Equal(100m, version.GetProperty("current_mb").GetDecimal());
        Assert.Equal(2500m, version.GetProperty("peak_24h_mb").GetDecimal());
        Assert.Equal("Version store pressure \u2014 check long-running transactions", version.GetProperty("warning").GetString());
        Assert.Equal("", tempdb.GetProperty("rows")[3].GetProperty("warning").GetString());

        var waits = Section(tool, "wait_categories");
        Assert.Equal("ok", waits.GetProperty("status").GetString());
        Assert.Equal(24, waits.GetProperty("window_hours").GetInt32());
        Assert.Equal(new[] { "CPU", "Storage", "Locks", "Memory", "Other" },
            waits.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("category").GetString()).ToArray());
        var cpu = waits.GetProperty("rows")[0];
        Assert.Equal(2300L, cpu.GetProperty("total_wait_time_ms").GetInt64());
        Assert.Equal(35L, cpu.GetProperty("waiting_tasks").GetInt64());
        Assert.Equal(47.9m, cpu.GetProperty("pct_of_total").GetDecimal());
        Assert.Equal("CXPACKET", cpu.GetProperty("top_wait_type").GetString());
        Assert.Equal(1500L, cpu.GetProperty("top_wait_time_ms").GetInt64());
        Assert.Equal(15.75m, cpu.GetProperty("est_cost_usd").GetDecimal());
        Assert.Equal(4.79m, waits.GetProperty("rows")[2].GetProperty("est_cost_usd").GetDecimal());

        var queries = Section(tool, "expensive_queries");
        Assert.Equal("ok", queries.GetProperty("status").GetString());
        Assert.Equal(24, queries.GetProperty("window_hours").GetInt32());
        var start = queries.GetProperty("effective_start").GetString()!;
        Assert.EndsWith("Z", start, StringComparison.Ordinal);
        var startInstant = DateTime.Parse(start, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.InRange(startInstant, before.AddHours(-24), after.AddHours(-24));
        Assert.Equal(McpHelpers.FormatEffectiveStart(startInstant), start);
        Assert.Equal(4, queries.GetProperty("rows").GetArrayLength());
        var alpha = queries.GetProperty("rows")[0];
        Assert.Equal("AlphaDb", alpha.GetProperty("database_name").GetString());
        Assert.Equal(9000L, alpha.GetProperty("total_cpu_ms").GetInt64());
        Assert.Equal(300m, alpha.GetProperty("avg_cpu_ms_per_exec").GetDecimal());
        Assert.Equal(1000L, alpha.GetProperty("total_reads").GetInt64());
        Assert.Equal(33m, alpha.GetProperty("avg_reads_per_exec").GetDecimal());
        Assert.Equal(30L, alpha.GetProperty("executions").GetInt64());
        Assert.Equal("SELECT alpha", alpha.GetProperty("query_preview").GetString());
        Assert.True(alpha.GetProperty("has_plan").GetBoolean());
        Assert.Equal(12.86m, alpha.GetProperty("est_cost_usd").GetDecimal());
        Assert.False(alpha.TryGetProperty("full_query_text", out _));
        Assert.False(alpha.TryGetProperty("query_plan_xml", out _));
        var delta = queries.GetProperty("rows")[1];
        Assert.Equal(200, delta.GetProperty("query_preview").GetString()!.Length);
        Assert.False(delta.GetProperty("has_plan").GetBoolean());
        Assert.Equal(8.58m, delta.GetProperty("est_cost_usd").GetDecimal());
        Assert.Equal(new[] { "BetaDb", "GammaDb" },
            new[] { queries.GetProperty("rows")[2], queries.GetProperty("rows")[3] }.Select(r => r.GetProperty("database_name").GetString()).ToArray());

        var grants = Section(tool, "memory_grant_efficiency");
        Assert.Equal("ok", grants.GetProperty("status").GetString());
        Assert.Equal(24, grants.GetProperty("window_hours").GetInt32());
        Assert.Equal(2, grants.GetProperty("rows").GetArrayLength());
        var yesterday = grants.GetProperty("rows")[0];
        Assert.Equal(DateTime.UtcNow.Date.AddDays(-1).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), yesterday.GetProperty("day").GetString());
        Assert.Equal(1500m, yesterday.GetProperty("avg_granted_mb").GetDecimal());
        Assert.Equal(1000m, yesterday.GetProperty("avg_used_mb").GetDecimal());
        Assert.Equal(66.7m, yesterday.GetProperty("efficiency_pct").GetDecimal());
        Assert.Equal(2000m, yesterday.GetProperty("peak_granted_mb").GetDecimal());
        Assert.Equal(500m, yesterday.GetProperty("wasted_mb").GetDecimal());
        Assert.Equal(6L, yesterday.GetProperty("total_grantees").GetInt64());
        Assert.Equal(3L, yesterday.GetProperty("total_waiters").GetInt64());
        Assert.Equal(5L, yesterday.GetProperty("timeout_errors").GetInt64());
        Assert.Equal(1L, yesterday.GetProperty("forced_grants").GetInt64());
        Assert.Equal(7L, grants.GetProperty("rows")[1].GetProperty("forced_grants").GetInt64());
    }

    [Fact]
    public async Task Sections_EqualTheReadersRows_ThroughTheRowFunctions()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);
        using var tool = await ToolAsync(ds, ServerName, 24, 10, ct);
        var now = DateTime.UtcNow;
        var (textStart, _) = RetentionTierRouter.ClampToTextHorizon(now, now.AddHours(-24));

        var idle = await DarlingFinOpsOptimizationReader.GetIdleDatabasesAsync(ds, ServerId, now.AddDays(-7), 60, ct);
        var tempdb = await DarlingFinOpsOptimizationReader.GetTempdbSummaryAsync(ds, ServerId, now.AddHours(-24), 60, ct);
        var waits = await DarlingFinOpsOptimizationReader.GetWaitCategorySummaryAsync(ds, ServerId, now.AddHours(-24), 60, ct);
        var queries = await DarlingFinOpsOptimizationReader.GetExpensiveQueriesAsync(ds, ServerId, textStart, 10, 60, ct);
        var grants = await DarlingFinOpsUtilizationReader.GetMemoryGrantEfficiencyAsync(ds, ServerId, 24, 60, ct);

        string Json(object o) => JsonSerializer.Serialize(o, McpHelpers.JsonOptions);
        Assert.Equal(Json(DarlingMcpFinOpsTools.OrderIdleDatabases(idle).Select(DarlingMcpFinOpsTools.IdleDatabaseRow).ToList()), Section(tool, "idle_databases").GetProperty("rows").GetRawText());
        Assert.Equal(Json(tempdb.Select(DarlingMcpFinOpsTools.TempdbRow).ToList()), Section(tool, "tempdb_pressure").GetProperty("rows").GetRawText());
        Assert.Equal(Json(DarlingMcpFinOpsTools.WaitCategoryRows(waits, 1000m, 24)), Section(tool, "wait_categories").GetProperty("rows").GetRawText());
        Assert.Equal(Json(DarlingMcpFinOpsTools.ExpensiveQueryRows(queries, 1000m, 24)), Section(tool, "expensive_queries").GetProperty("rows").GetRawText());
        Assert.Equal(Json(DarlingMcpFinOpsTools.MemoryGrantRows(grants)), Section(tool, "memory_grant_efficiency").GetProperty("rows").GetRawText());
    }

    [Fact]
    public async Task Limit_CapsOnlyTheExpensiveQueries_AndMovesTheirCostShare()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);
        using var tool = await ToolAsync(ds, ServerName, 24, 2, ct);

        var queries = Section(tool, "expensive_queries");
        Assert.Equal(2, queries.GetProperty("rows").GetArrayLength());
        Assert.Equal("AlphaDb", queries.GetProperty("rows")[0].GetProperty("database_name").GetString());
        Assert.Equal("DeltaDb", queries.GetProperty("rows")[1].GetProperty("database_name").GetString());
        /* 9000 and 6000 of the two returned rows' 15000 ms. */
        Assert.Equal(19.73m, queries.GetProperty("rows")[0].GetProperty("est_cost_usd").GetDecimal());
        Assert.Equal(13.15m, queries.GetProperty("rows")[1].GetProperty("est_cost_usd").GetDecimal());
        Assert.Equal(2, Section(tool, "idle_databases").GetProperty("rows").GetArrayLength());
        Assert.Equal(5, Section(tool, "wait_categories").GetProperty("rows").GetArrayLength());
    }

    [Fact]
    public async Task HoursBack_WidensWaitsAndQueries_AndLeavesTheFixedSectionsAlone()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);
        using var day = await ToolAsync(ds, ServerName, 24, 10, ct);
        using var two = await ToolAsync(ds, ServerName, 48, 10, ct);

        long Other(JsonDocument d) => Section(d, "wait_categories").GetProperty("rows").EnumerateArray()
            .Single(r => r.GetProperty("category").GetString() == "Other").GetProperty("total_wait_time_ms").GetInt64();
        Assert.Equal(100L, Other(day));
        Assert.Equal(5100L, Other(two));
        Assert.Equal(48, Section(two, "wait_categories").GetProperty("window_hours").GetInt32());
        Assert.Equal(48, Section(two, "expensive_queries").GetProperty("window_hours").GetInt32());
        Assert.Equal(Section(day, "idle_databases").GetRawText(), Section(two, "idle_databases").GetRawText());
        Assert.Equal(Section(day, "tempdb_pressure").GetRawText(), Section(two, "tempdb_pressure").GetRawText());
        Assert.Equal(Section(day, "memory_grant_efficiency").GetRawText(), Section(two, "memory_grant_efficiency").GetRawText());

        /* A week asks for more query text than is retained: the start is clamped to the retention horizon (3 days). */
        var before = DateTime.UtcNow;
        using var week = await ToolAsync(ds, ServerName, 168, 10, ct);
        var after = DateTime.UtcNow;
        var start = DateTime.Parse(Section(week, "expensive_queries").GetProperty("effective_start").GetString()!,
            CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.InRange(start, before.AddDays(-3), after.AddDays(-3));
        Assert.Equal(168, Section(week, "expensive_queries").GetProperty("window_hours").GetInt32());
    }

    [Fact]
    public async Task ServerWithNoRows_IsExactlyEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        var body = await DarlingMcpFinOpsTools.GetFinOps(ds, "optimization", EmptyServerName, 24, 10, cancellationToken: ct);
        Assert.Equal(
            McpHelpers.Status("empty", "No idle-database, tempdb, wait, query or memory-grant data was found for this server in the windows read, so there is no optimization data to show."),
            body);
    }

    [Fact]
    public async Task PostgresServer_IsNotCollected_OnlyBecauseEverySectionIsGated()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        var body = await DarlingMcpFinOpsTools.GetFinOps(ds, "optimization", PostgresServerName, 24, 10, cancellationToken: ct);
        using var doc = JsonDocument.Parse(body);
        Assert.Equal("not_collected", doc.RootElement.GetProperty("status").GetString());
        Assert.False(doc.RootElement.TryGetProperty("idle_databases", out _));
        Assert.Equal(
            await DarlingEngineCapability.NotCollectedStatusAsync(ds, PostgresServerId, PostgresServerName, "database_size_stats", ct),
            body);
    }

    [Fact]
    public async Task ServerWithOnlyTempdbRows_ReturnsThePayload_WithTheOtherSectionsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var tool = await ToolAsync(ds, TempdbOnlyServerName, 24, 10, ct);
        Assert.Equal("ok", Section(tool, "tempdb_pressure").GetProperty("status").GetString());
        Assert.Equal(4, Section(tool, "tempdb_pressure").GetProperty("rows").GetArrayLength());
        foreach (var name in new[] { "idle_databases", "wait_categories", "expensive_queries", "memory_grant_efficiency" })
        {
            Assert.Equal("empty", Section(tool, name).GetProperty("status").GetString());
            Assert.Equal(0, Section(tool, name).GetProperty("rows").GetArrayLength());
        }
        Assert.Equal(JsonValueKind.Null, tool.RootElement.GetProperty("monthly_cost_usd").ValueKind);
        Assert.Equal("monthly cost not set", tool.RootElement.GetProperty("cost_reason").GetString());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NoCostSet_LeavesEveryCostNull_AndSaysWhy(bool zero)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct, zero ? 0m : null);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var tool = await ToolAsync(ds, ServerName, 24, 10, ct);
        Assert.Equal(JsonValueKind.Null, tool.RootElement.GetProperty("monthly_cost_usd").ValueKind);
        Assert.Equal("monthly cost not set", tool.RootElement.GetProperty("cost_reason").GetString());
        var rows = Section(tool, "wait_categories").GetProperty("rows").EnumerateArray()
            .Concat(Section(tool, "expensive_queries").GetProperty("rows").EnumerateArray()).ToList();
        Assert.Equal(9, rows.Count);
        Assert.All(rows, r => Assert.Equal(JsonValueKind.Null, r.GetProperty("est_cost_usd").ValueKind));
    }

    [Fact]
    public async Task ReadRoute_ReturnsTheToolsBody()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(ds);
        await using var app = builder.Build();
        DarlingWebEndpoints.MapAll(app, ds, new CollectorRuntimeState(), new CapturingTestLogger());
        await app.StartAsync(ct);
        using var server = app.GetTestServer();
        var context = await server.SendAsync(request =>
        {
            request.Request.Method = "GET";
            request.Request.Path = "/api/read/get_finops";
            request.Request.QueryString = new QueryString($"?server={ServerName}&view=optimization&hours=24");
            request.Request.Headers.Host = "localhost";
        });
        using var reader = new StreamReader(context.Response.Body);
        var body = await reader.ReadToEndAsync(ct);
        /* The tool is read after the route, so a clock tick between the two cannot move a window start past the route's. */
        var tool = await DarlingMcpFinOpsTools.GetFinOps(ds, "optimization", ServerName, 24, 10, cancellationToken: ct);

        Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
        using var expected = JsonDocument.Parse(tool);
        using var actual = JsonDocument.Parse(body);
        /* effective_start carries the read's own clock, so it is compared to the second and the rest exactly. */
        var a = actual.RootElement.GetProperty("expensive_queries").GetProperty("effective_start").GetString()!;
        var e = expected.RootElement.GetProperty("expensive_queries").GetProperty("effective_start").GetString()!;
        Assert.True((DateTime.Parse(e, CultureInfo.InvariantCulture) - DateTime.Parse(a, CultureInfo.InvariantCulture)).Duration() < TimeSpan.FromSeconds(10));
        Assert.Equal(StripStart(expected.RootElement.GetRawText()), StripStart(actual.RootElement.GetRawText()));
    }

    private static string StripStart(string json) =>
        System.Text.RegularExpressions.Regex.Replace(json, "\"effective_start\":\"[^\"]+\"", "\"effective_start\":\"-\"");
}
