/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
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

using Reader = PerformanceMonitor.Darling.Service.Mcp.DarlingHealthReader;

namespace Darling.Tests;

/// <summary>
/// Pins the health-overview MCP slice — get_server_summary (one-shot per-server health), get_daily_summary
/// (the daily rollup folded through the SHARED DailyHealthBandCalculator) and get_daily_summary_range (#2484:
/// the same rollup across a span of days, the Performance Calendar's month grid) over the Postgres store.
/// Ungated: the tool surface is EXACTLY the three names (all static, on a [McpServerToolType] class, returning
/// Task&lt;string&gt;); each param contract matches Lite's / the Dashboard's; the read SQL is Postgres-dialect,
/// positional-param; the daily band wires through the shared calculator; and the advertised tools/list schema
/// is Gemini-clean.
/// </summary>
public sealed class DarlingMcpHealthToolsSurfaceAndSqlTests
{
    private static readonly string[] HealthToolSurface =
    {
        "get_daily_summary",
        "get_daily_summary_range",
        "get_server_summary",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpHealthTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheThreeHealthTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(HealthToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpHealthTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    private static (string Name, bool Optional)[] McpParams(string toolName)
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == toolName);
        return method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name!, p.HasDefaultValue))
            .ToArray();
    }

    [Theory]
    [InlineData("get_server_summary", "server_name")]
    [InlineData("get_daily_summary", "server_name,summary_date")]
    /* The range read is a SIBLING rather than a wider get_daily_summary: the single-day tool returns a flat
       object of scalars and this returns rows, and the Overview tab reads both, which it could not do if they
       were one read (no tab may fetch a read twice). Its span is in DAYS, so it carries the day-grained
       anchor description rather than the hours one. */
    [InlineData("get_daily_summary_range", "server_name,days_back,as_of")]
    public void ParamContract_MatchesContract(string toolName, string expectedCsv)
    {
        Assert.Equal(expectedCsv.Split(','), McpParams(toolName).Select(p => p.Name).ToArray());
    }

    [Fact]
    public void ParamContract_EveryDescribedParamIsOptional()
    {
        foreach (var tool in HealthToolSurface)
            Assert.All(McpParams(tool), p => Assert.True(p.Optional, $"{tool}.{p.Name} must be optional"));
    }

    /* ---------------- read SQL pins ---------------- */

    [Fact]
    public void ServerSummarySql_LatestCpuMemory_HourWindowBlockingDeadlock()
    {
        Assert.Contains("FROM v_cpu_utilization_stats", Reader.ServerSummaryCpuSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY sample_time DESC", Reader.ServerSummaryCpuSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", Reader.ServerSummaryCpuSql, StringComparison.Ordinal);

        Assert.Contains("total_server_memory_mb", Reader.ServerSummaryMemorySql, StringComparison.Ordinal);

        /* Blocking: both sources, so the caller can apply the XE-preferred / DMV-fallback rule. */
        Assert.Contains("v_blocked_process_reports", Reader.ServerSummaryBlockingSql, StringComparison.Ordinal);
        Assert.Contains("v_dmv_blocking_snapshots", Reader.ServerSummaryBlockingSql, StringComparison.Ordinal);

        Assert.Contains("FROM v_deadlocks", Reader.ServerSummaryDeadlockSql, StringComparison.Ordinal);
        Assert.Contains("deadlock_time >= $2", Reader.ServerSummaryDeadlockSql, StringComparison.Ordinal);

        Assert.Contains("MAX(collection_time)", Reader.ServerSummaryLastCollectionSql, StringComparison.Ordinal);
    }

    [Fact]
    public void DailySummarySql_DayBucketed_AllSources()
    {
        var sql = Reader.DailySummaryRangeSql;
        Assert.Contains("date_trunc('day'", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_wait_stats", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_deadlocks", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_memory_pressure_events", sql, StringComparison.Ordinal);
        Assert.Contains("FROM config_alert_log", sql, StringComparison.Ordinal);
        Assert.Contains("day_spine", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(Reader.ServerSummaryCpuSql))]
    [InlineData(nameof(Reader.ServerSummaryBlockingSql))]
    [InlineData(nameof(Reader.ServerSummaryDeadlockSql))]
    [InlineData(nameof(Reader.DailySummaryRangeSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(Reader.ServerSummaryCpuSql) => Reader.ServerSummaryCpuSql,
            nameof(Reader.ServerSummaryBlockingSql) => Reader.ServerSummaryBlockingSql,
            nameof(Reader.ServerSummaryDeadlockSql) => Reader.ServerSummaryDeadlockSql,
            _ => Reader.DailySummaryRangeSql,
        };
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    /* ---------------- shared daily-health band wiring (no live PG) ---------------- */

    [Fact]
    public void DailySummaryRow_BandsThroughSharedCalculator()
    {
        var date = new DateTime(2026, 7, 9, 0, 0, 0, DateTimeKind.Unspecified);

        /* A deadlock day is Critical (the shared calculator's rule). */
        var critical = new Reader.DailySummaryReadRow(date, 0m, "", 0, DeadlockCount: 1, 0, 0, 0, 0, 0, 0, 0, HasData: true);
        Assert.Equal(DailyHealthBand.Critical, critical.HealthBand);
        Assert.Equal("Critical", critical.OverallHealth);

        /* A collected-but-quiet day is Healthy. */
        var healthy = new Reader.DailySummaryReadRow(date, 12m, "CXPACKET", 3, 0, 0, 0, 0, 0, 0, 0, 0, HasData: true);
        Assert.Equal(DailyHealthBand.Healthy, healthy.HealthBand);

        /* No collection is No Data. */
        var noData = new Reader.DailySummaryReadRow(date, 0m, "", 0, 0, 0, 0, 0, 0, 0, 0, 0, HasData: false);
        Assert.Equal(DailyHealthBand.NoData, noData.HealthBand);
        Assert.Equal("No Data", noData.OverallHealth);
    }

    /* ---------------- advertised MCP schema ---------------- */

    private static System.Collections.Generic.List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpHealthTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    /// <summary>
    /// The anchor pinned by IDENTITY, not merely by name: <c>get_daily_summary_range</c>'s <c>as_of</c> must
    /// carry the DAY-grained shared constant. Not the hours one, which names <c>hours_back</c> in its own text
    /// — a description naming a parameter the tool does not have is worse than a generic one, because an
    /// unknown query key is ignored rather than rejected and the caller never learns their span was dropped.
    /// </summary>
    [Fact]
    public void DailySummaryRange_AnchorCarriesTheDayGrainedSharedDescription()
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "get_daily_summary_range");
        var asOf = method.GetParameters().Single(p => p.Name == "as_of");

        Assert.Equal(McpHelpers.AsOfDaysDescription, asOf.GetCustomAttribute<DescriptionAttribute>()!.Description);
        Assert.NotEqual(McpHelpers.AsOfDescription, McpHelpers.AsOfDaysDescription);
        Assert.DoesNotContain("hours_back", McpHelpers.AsOfDaysDescription, StringComparison.Ordinal);
        Assert.Contains("days_back", McpHelpers.AsOfDaysDescription, StringComparison.Ordinal);
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForEveryTool_NoRequiredParams()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(3, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        foreach (var t in tools)
            Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(t.InputSchema));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the health tools. Plants a day's worth of CPU / memory / wait /
/// deadlock / collection-log rows, then asserts get_server_summary reports the current metrics and
/// get_daily_summary bands the day Critical (a deadlock fired).
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpHealthToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-health-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The #1736 boundary, simulated rather than waited for. The sibling test above only exercises the
    /// midnight case if CI happens to run between 00:00 and 00:05 UTC — which is precisely why the bug
    /// survived: it was unobservable 99.7% of the day. This plants at an ABSOLUTE timestamp three minutes
    /// before a UTC midnight, so the "row is on the previous day" condition holds on every run at every
    /// hour, and asserts the explicit-date call still finds it.
    ///
    /// <para>It also pins the mechanism, not just the fix: the same rows queried WITHOUT a date return the
    /// empty envelope, because "today" is not the day they belong to. That asymmetry is the bug, and a
    /// regression that reintroduced an implicit-today call would fail here on any run rather than on the
    /// 0.3% of runs that straddle midnight.</para>
    /// </summary>
    [Fact]
    public async Task DailySummary_ExplicitDate_FindsRowsPlantedJustBeforeUtcMidnight()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live health-tools test.");

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

            /* 23:57 on a fixed past day: the 00:02-equivalent, reachable on demand. Past and absolute so it
               never collides with the sibling test's UtcNow-relative rows. */
            var boundary = new DateTime(2026, 7, 20, 23, 57, 0, DateTimeKind.Utc);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(boundary), ServerId, ServerName,
                DarlingMcpTestData.Naive(boundary), "process9z9z", "DELETE FROM dbo.Boundary", "<deadlock/>");

            /* The fix: ask for the day the rows carry. */
            var onItsOwnDay = await DarlingMcpHealthTools.GetDailySummary(
                postgres, ServerName, boundary.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));

            DarlingMcpTestData.AssertEnvelope(onItsOwnDay, ServerName, "overall_health");
            Assert.Contains("Critical", onItsOwnDay, StringComparison.Ordinal);
            Assert.Contains("2026-07-20", onItsOwnDay, StringComparison.Ordinal);

            /* The bug: the same rows are invisible to an implicit "today", which is what the sibling test
               used to do and what made it fail only in the five minutes after midnight. */
            var onToday = await DarlingMcpHealthTools.GetDailySummary(postgres, ServerName);
            using (var doc = JsonDocument.Parse(onToday))
            {
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            }

            Assert.DoesNotContain("2026-07-20", onToday, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task HealthTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live health-tools test.");

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
            /* Within both today's UTC day (for the daily rollup) and the last hour (for the summary windows). */
            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1,$2,$3,$4,$5,$6,$7)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, DarlingMcpTestData.Naive(when), 85, 10);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, plan_cache_mb, system_memory_state, sql_memory_model)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, 65536m, 16384m, 49152m, 49152m, 40000m, 5000m, "Available physical memory is high", "CONVENTIONAL");

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, "CXPACKET", 5000L, 500L, 50L);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, DarlingMcpTestData.Naive(when), "process1a2b", "DELETE FROM dbo.Votes", "<deadlock/>");

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, "wait_stats", "SUCCESS", 42, 100);

            var summary = await DarlingMcpHealthTools.GetServerSummary(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(summary, ServerName, "cpu_percent");
            Assert.Contains("deadlock_count", summary, StringComparison.Ordinal);
            Assert.Contains("last_collection", summary, StringComparison.Ordinal);

            /* Ask for the day the rows were actually PLANTED on, not the implicit "today" (#1736). The rows
               land at UtcNow-5min, so between 00:00 and 00:05 UTC they carry yesterday's date while "today"
               has already rolled over — the tool then correctly returns its empty envelope and this test
               failed deterministically in that five-minute window, turning any darling-pg run that landed
               there into a red required check that looked like a regression. The product default is right
               and is deliberately NOT widened; the test just stops assuming the two dates agree. */
            var daily = await DarlingMcpHealthTools.GetDailySummary(
                postgres, ServerName, when.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
            DarlingMcpTestData.AssertEnvelope(daily, ServerName, "overall_health");
            Assert.Contains("Critical", daily, StringComparison.Ordinal);   /* the deadlock makes the day Critical */

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var sql = string.Join(" ", new[] { "cpu_utilization_stats", "memory_stats", "wait_stats", "deadlocks", "collection_log" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"))
            + $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
