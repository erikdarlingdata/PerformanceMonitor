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
/// Pins the session diagnostic-depth MCP slice — get_session_stats, get_active_queries, get_waiting_tasks
/// over the Postgres store. Ungated: tool surface, per-tool param contract (matching Lite), the read SQL
/// pins (v_session_stats latest snapshot; query_snapshots windowed with the WAITFOR trim; waiting_tasks base
/// table — no v_ view), and the Gemini-clean advertised schema.
/// </summary>
public sealed class DarlingMcpSessionToolsSurfaceAndSqlTests
{
    private static readonly string[] SessionToolSurface =
    {
        "get_active_queries",
        "get_session_stats",
        "get_waiting_tasks",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpSessionTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheThreeSessionTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(SessionToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpSessionTools).GetCustomAttribute<McpServerToolTypeAttribute>());
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
    [InlineData("get_session_stats", "server_name")]
    [InlineData("get_active_queries", "server_name,hours_back,database_name,blocking_only,limit,as_of")]
    [InlineData("get_waiting_tasks", "server_name,hours_back,limit,as_of")]
    public void ParamContract_MatchesLite(string toolName, string expectedCsv)
    {
        Assert.Equal(expectedCsv.Split(','), McpParams(toolName).Select(p => p.Name).ToArray());
    }

    [Fact]
    public void ParamContract_ServerNameAlwaysOptional()
    {
        foreach (var tool in SessionToolSurface)
            Assert.True(McpParams(tool).Single(x => x.Name == "server_name").Optional, $"{tool}.server_name must be optional");
    }

    [Fact]
    public void SessionStatsSql_LatestSnapshot_PerApplication()
    {
        var sql = DarlingSessionReader.LatestSessionStatsSql;
        Assert.Contains("FROM v_session_stats", sql, StringComparison.Ordinal);
        Assert.Contains("program_name", sql, StringComparison.Ordinal);
        Assert.Contains("connection_count", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY connection_count DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ActiveQueriesSql_ReadsBaseTable_WindowedWithWaitforTrim()
    {
        var sql = DarlingSessionReader.ActiveQueriesSql;
        Assert.Contains("FROM query_snapshots", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_query_snapshots", sql, StringComparison.Ordinal);
        Assert.Contains("granted_query_memory_gb", sql, StringComparison.Ordinal);
        Assert.Contains("blocking_session_id", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("NOT LIKE 'WAITFOR%'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitingTasksSql_ReadsBaseTable_NoView_Windowed()
    {
        var sql = DarlingSessionReader.WaitingTasksSql;
        Assert.Contains("FROM waiting_tasks", sql, StringComparison.Ordinal);   /* no v_waiting_tasks view exists */
        Assert.DoesNotContain("v_waiting_tasks", sql, StringComparison.Ordinal);
        Assert.Contains("wait_duration_ms", sql, StringComparison.Ordinal);
        Assert.Contains("resource_description", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingSessionReader.LatestSessionStatsSql))]
    [InlineData(nameof(DarlingSessionReader.ActiveQueriesSql))]
    [InlineData(nameof(DarlingSessionReader.WaitingTasksSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(DarlingSessionReader.LatestSessionStatsSql) => DarlingSessionReader.LatestSessionStatsSql,
            nameof(DarlingSessionReader.ActiveQueriesSql) => DarlingSessionReader.ActiveQueriesSql,
            _ => DarlingSessionReader.WaitingTasksSql,
        };
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
        var ss = PgSchemaGenerator.CreateTable(SessionStatsCollector.Instance);
        Assert.Equal("session_stats", SessionStatsCollector.Instance.TargetTable);
        Assert.Contains("program_name", ss, StringComparison.Ordinal);
        Assert.Contains("connection_count", ss, StringComparison.Ordinal);
        Assert.Contains("total_logical_reads", ss, StringComparison.Ordinal);

        var qs = PgSchemaGenerator.CreateTable(QuerySnapshotsCollector.Instance);
        Assert.Equal("query_snapshots", QuerySnapshotsCollector.Instance.TargetTable);
        Assert.Contains("granted_query_memory_gb", qs, StringComparison.Ordinal);
        Assert.Contains("elapsed_time_formatted", qs, StringComparison.Ordinal);
        Assert.Contains("blocking_session_id", qs, StringComparison.Ordinal);

        var wt = PgSchemaGenerator.CreateTable(WaitingTasksCollector.Instance);
        Assert.Equal("waiting_tasks", WaitingTasksCollector.Instance.TargetTable);
        Assert.Contains("wait_duration_ms", wt, StringComparison.Ordinal);
        Assert.Contains("resource_description", wt, StringComparison.Ordinal);

        /* waiting_tasks deliberately has NO v_ passthrough view (the base reads confirm it). */
        Assert.DoesNotContain("v_waiting_tasks", string.Join(",", PgSchemaGenerator.AllPassthroughViews));
        Assert.Contains("v_session_stats", PgSchemaGenerator.AllPassthroughViews);
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpSessionTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForAllThreeTools_NoRequiredParams()
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
/// Gated (DARLING_TEST_PG) live round-trips for the session tools. Plants a session_stats snapshot, a couple
/// of query_snapshots (one blocking), and a waiting_tasks row, then asserts each tool returns its data-bearing
/// envelope, the blocking_only/database filters narrow without erroring, and an empty store returns the miss.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpSessionToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-session-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task SessionTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live session-tools test.");

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

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count, running_count, sleeping_count, dormant_count, total_cpu_time_ms, total_logical_reads)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, "SQLCMD", 12L, 3, 9, 0, 4200L, 99999L);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, 55, Db, "00 00:00:05.000", "SELECT * FROM Posts", "running", 60, "CXPACKET", 500L, 5000L, 5000L, 100L, 0L, 2000L, 0.5m, "Read Committed", 4, 3, "sa", "APP01", "SSMS", 1, 0);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, 55, "LCK_M_X", 3000L, 60, null, Db);

            DarlingMcpTestData.AssertEnvelope(await DarlingMcpSessionTools.GetSessionStats(postgres, ServerName), ServerName, "applications");
            var aq = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(aq, ServerName, "queries");
            Assert.Contains("SELECT * FROM Posts", aq, StringComparison.Ordinal);
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 1, Db, true), ServerName, "queries");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpSessionTools.GetWaitingTasks(postgres, ServerName), ServerName, "tasks");

            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpSessionTools.GetSessionStats(postgres, ServerName)));
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpSessionTools.GetWaitingTasks(postgres, ServerName)));

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
        var sql = string.Join(" ", new[] { "session_stats", "query_snapshots", "waiting_tasks" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"));
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
