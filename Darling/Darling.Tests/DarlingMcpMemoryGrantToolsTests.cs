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
/// Pins the memory-grant MCP slice — get_resource_semaphore (Dashboard semaphore/ceiling shape) and
/// get_memory_grants (Lite per-pool shape) over the Postgres store. Ungated: tool surface, the param contracts,
/// the latest-snapshot read SQL pins (v_memory_grant_stats, the ceiling columns, per-pool SUM), and the
/// Gemini-clean advertised schema.
/// </summary>
public sealed class DarlingMcpMemoryGrantToolsSurfaceAndSqlTests
{
    private static readonly string[] MemoryGrantToolSurface =
    {
        "get_memory_grants",
        "get_memory_pressure_events",
        "get_resource_semaphore",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpMemoryGrantTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheThreeMemoryGrantTools()
    {
        var names = ToolMethods()
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(MemoryGrantToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpMemoryGrantTools).GetCustomAttribute<McpServerToolTypeAttribute>());
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

    [Theory]
    [InlineData("get_resource_semaphore")]
    [InlineData("get_memory_grants")]
    [InlineData("get_memory_pressure_events")]
    public void ParamContract_ServerHours_AllOptional(string toolName)
    {
        var p = McpParams(toolName);
        Assert.Equal(new[] { "server_name", "hours_back", "as_of" }, p.Select(x => x.Name).ToArray());
        Assert.All(p, x => Assert.True(x.Optional, $"{toolName}.{x.Name} must be optional"));
    }

    [Fact]
    public void ResourceSemaphoreLatestSql_LatestSnapshot_CarriesCeilingColumns()
    {
        var sql = DarlingMemoryGrantReader.ResourceSemaphoreLatestSql;
        Assert.Contains("FROM v_memory_grant_stats", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("target_memory_mb", sql, StringComparison.Ordinal);
        Assert.Contains("max_target_memory_mb", sql, StringComparison.Ordinal);
        Assert.Contains("resource_semaphore_id", sql, StringComparison.Ordinal);
        Assert.Contains("timeout_error_count_delta", sql, StringComparison.Ordinal);
        Assert.Contains("forced_grant_count_delta", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryGrantsLatestSql_LatestSnapshot_SumsPerPool()
    {
        var sql = DarlingMemoryGrantReader.MemoryGrantsLatestSql;
        Assert.Contains("FROM v_memory_grant_stats", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(granted_memory_mb)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collection_time, pool_id", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryPressureEventsSql_WindowsOnSampleTime_PostgresDialect()
    {
        var sql = DarlingMemoryGrantReader.MemoryPressureEventsSql;
        Assert.Contains("FROM v_memory_pressure_events", sql, StringComparison.Ordinal);
        Assert.Contains("memory_notification", sql, StringComparison.Ordinal);
        Assert.Contains("memory_indicators_process", sql, StringComparison.Ordinal);
        Assert.Contains("sample_time >= $2", sql, StringComparison.Ordinal);   /* windowed on the payload clock */
        Assert.Contains("ORDER BY sample_time", sql, StringComparison.Ordinal);
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingMemoryGrantReader.ResourceSemaphoreLatestSql))]
    [InlineData(nameof(DarlingMemoryGrantReader.MemoryGrantsLatestSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName == nameof(DarlingMemoryGrantReader.ResourceSemaphoreLatestSql)
            ? DarlingMemoryGrantReader.ResourceSemaphoreLatestSql
            : DarlingMemoryGrantReader.MemoryGrantsLatestSql;
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTable()
    {
        var mg = PgSchemaGenerator.CreateTable(MemoryGrantsCollector.Instance);
        Assert.Equal("memory_grant_stats", MemoryGrantsCollector.Instance.TargetTable);
        Assert.Contains("target_memory_mb", mg, StringComparison.Ordinal);
        Assert.Contains("max_target_memory_mb", mg, StringComparison.Ordinal);
        Assert.Contains("total_memory_mb", mg, StringComparison.Ordinal);
        Assert.Contains("available_memory_mb", mg, StringComparison.Ordinal);
        Assert.Contains("granted_memory_mb", mg, StringComparison.Ordinal);
        Assert.Contains("used_memory_mb", mg, StringComparison.Ordinal);
        Assert.Contains("resource_semaphore_id", mg, StringComparison.Ordinal);
        Assert.Contains("pool_id", mg, StringComparison.Ordinal);
        Assert.Contains("timeout_error_count_delta", mg, StringComparison.Ordinal);
        Assert.Contains("forced_grant_count_delta", mg, StringComparison.Ordinal);

        Assert.Contains("v_memory_grant_stats", PgSchemaGenerator.AllPassthroughViews);
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpMemoryGrantTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_NoRequiredParams()
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
/// Gated (DARLING_TEST_PG) live round-trips for the memory-grant tools. Plants two resource pools at one
/// snapshot, then asserts get_resource_semaphore surfaces the ceiling columns and get_memory_grants surfaces
/// the per-pool detail; an empty store returns the miss.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpMemoryGrantToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-grants-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task MemoryGrantTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live memory-grant-tools test.");

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

            foreach (var (semaphore, pool) in new[] { ((short)0, 1), ((short)0, 2) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id, target_memory_mb, max_target_memory_mb, total_memory_mb, available_memory_mb, granted_memory_mb, used_memory_mb, grantee_count, waiter_count, timeout_error_count, forced_grant_count, timeout_error_count_delta, forced_grant_count_delta)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, semaphore, pool, 8000m, 12000m, 8000m, 6000m, 2000m, 1500m, 3, 1, 4L, 2L, 1L, 0L);
            }

            var semaphoreJson = await DarlingMcpMemoryGrantTools.GetResourceSemaphore(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(semaphoreJson, ServerName, "grants");
            Assert.Contains("max_target_memory_mb", semaphoreJson, StringComparison.Ordinal);

            var grantsJson = await DarlingMcpMemoryGrantTools.GetMemoryGrants(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(grantsJson, ServerName, "grants");
            Assert.Contains("granted_memory_mb", grantsJson, StringComparison.Ordinal);

            /* Memory pressure events — a severe RESOURCE_MEMPHYSICAL_LOW sample surfaces. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO memory_pressure_events (collection_id, collection_time, server_id, server_name, sample_time, memory_notification, memory_indicators_process, memory_indicators_system)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, t, "RESOURCE_MEMPHYSICAL_LOW", 3, 1);

            var pressureJson = await DarlingMcpMemoryGrantTools.GetMemoryPressureEvents(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(pressureJson, ServerName, "events");
            Assert.Contains("RESOURCE_MEMPHYSICAL_LOW", pressureJson, StringComparison.Ordinal);

            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpMemoryGrantTools.GetResourceSemaphore(postgres, ServerName)));

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
        var sql = $"DELETE FROM memory_grant_stats WHERE server_id = {ServerId};"
            + $" DELETE FROM memory_pressure_events WHERE server_id = {ServerId};";
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
