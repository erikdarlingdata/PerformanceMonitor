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
/// Pins the latch / spinlock resource-contention MCP slice — get_latch_stats, get_spinlock_stats over the
/// Postgres store. Ungated: tool surface, the (server_name, hours_back, top) param contract, the top-N read SQL
/// pins (v_latch_stats / v_spinlock_stats, LAG per-second, DISTINCT ON latest), the reproduced Dashboard CASE
/// enrichment (severity / description / recommendation), and the Gemini-clean advertised schema.
/// </summary>
public sealed class DarlingMcpLatchSpinlockToolsSurfaceAndSqlTests
{
    private static readonly string[] LatchSpinlockToolSurface =
    {
        "get_latch_stats",
        "get_spinlock_stats",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpLatchSpinlockTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheTwoLatchSpinlockTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(LatchSpinlockToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpLatchSpinlockTools).GetCustomAttribute<McpServerToolTypeAttribute>());
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
    [InlineData("get_latch_stats")]
    [InlineData("get_spinlock_stats")]
    public void ParamContract_MatchesDashboard_ServerHoursTop_AllOptional(string toolName)
    {
        var p = McpParams(toolName);
        Assert.Equal(new[] { "server_name", "hours_back", "top", "as_of" }, p.Select(x => x.Name).ToArray());
        Assert.All(p, x => Assert.True(x.Optional, $"{toolName}.{x.Name} must be optional"));
    }

    [Fact]
    public void LatchStatsTopNSql_TopByWaitTime_PerSecondFromLag_LatestSnapshot()
    {
        var sql = DarlingLatchSpinlockReader.LatchStatsTopNSql;
        Assert.Contains("FROM v_latch_stats", sql, StringComparison.Ordinal);
        Assert.Contains("delta_wait_time_ms", sql, StringComparison.Ordinal);
        Assert.Contains("delta_waiting_requests_count", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (latch_class)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY a.total_delta_wait_time_ms DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SpinlockStatsTopNSql_TopByCollisions_PerSecondFromLag()
    {
        var sql = DarlingLatchSpinlockReader.SpinlockStatsTopNSql;
        Assert.Contains("FROM v_spinlock_stats", sql, StringComparison.Ordinal);
        Assert.Contains("delta_collisions", sql, StringComparison.Ordinal);
        Assert.Contains("delta_spins", sql, StringComparison.Ordinal);
        Assert.Contains("delta_backoffs", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (spinlock_name)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY a.total_delta_collisions DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingLatchSpinlockReader.LatchStatsTopNSql))]
    [InlineData(nameof(DarlingLatchSpinlockReader.SpinlockStatsTopNSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName == nameof(DarlingLatchSpinlockReader.LatchStatsTopNSql)
            ? DarlingLatchSpinlockReader.LatchStatsTopNSql
            : DarlingLatchSpinlockReader.SpinlockStatsTopNSql;
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
        var latch = PgSchemaGenerator.CreateTable(LatchStatsCollector.Instance);
        Assert.Equal("latch_stats", LatchStatsCollector.Instance.TargetTable);
        Assert.Contains("latch_class", latch, StringComparison.Ordinal);
        Assert.Contains("delta_wait_time_ms", latch, StringComparison.Ordinal);
        Assert.Contains("delta_waiting_requests_count", latch, StringComparison.Ordinal);

        var spin = PgSchemaGenerator.CreateTable(SpinlockStatsCollector.Instance);
        Assert.Equal("spinlock_stats", SpinlockStatsCollector.Instance.TargetTable);
        Assert.Contains("spinlock_name", spin, StringComparison.Ordinal);
        Assert.Contains("delta_collisions", spin, StringComparison.Ordinal);
        Assert.Contains("delta_spins", spin, StringComparison.Ordinal);
        Assert.Contains("delta_backoffs", spin, StringComparison.Ordinal);

        Assert.Contains("v_latch_stats", PgSchemaGenerator.AllPassthroughViews);
        Assert.Contains("v_spinlock_stats", PgSchemaGenerator.AllPassthroughViews);
    }

    [Theory]
    [InlineData(0L, "LOW")]
    [InlineData(5000L, "LOW")]
    [InlineData(5001L, "MEDIUM")]
    [InlineData(10000L, "MEDIUM")]
    [InlineData(10001L, "HIGH")]
    public void LatchSeverity_MatchesDashboardBands(long latestDeltaWaitMs, string expected)
    {
        Assert.Equal(expected, DarlingLatchSpinlockReader.LatchSeverity(latestDeltaWaitMs));
    }

    [Fact]
    public void LatchDescription_KnownAndDefault()
    {
        Assert.Equal("Synchronize short term access to database pages.", DarlingLatchSpinlockReader.LatchDescription("BUFFER"));
        Assert.Equal("Internal SQL Server synchronization.", DarlingLatchSpinlockReader.LatchDescription("SOMETHING_ELSE"));
    }

    [Theory]
    [InlineData("PAGEIOLATCH_SH", "I/O bottleneck - check disk latency, add memory")]
    [InlineData("PAGELATCH_EX", "Page contention - check for hot pages, tempdb issues")]
    [InlineData("BUFFER", "Buffer pool contention - check for memory pressure")]
    [InlineData("ACCESS_METHODS_DATASET_PARENT", "Index/heap access contention")]
    [InlineData("ALLOC_FREESPACE_CACHE", "Allocation contention - consider pre-sizing files")]
    [InlineData("LOG_MANAGER", "Log contention - check log disk")]
    [InlineData("WHATEVER", "Review latch class documentation")]
    public void LatchRecommendation_MatchesDashboardCase(string latchClass, string expected)
    {
        Assert.Equal(expected, DarlingLatchSpinlockReader.LatchRecommendation(latchClass));
    }

    [Fact]
    public void SpinlockDescription_KnownAndDefault()
    {
        Assert.Equal("Lock manager hash table access.", DarlingLatchSpinlockReader.SpinlockDescription("LOCK_HASH"));
        Assert.Equal("Internal use only.", DarlingLatchSpinlockReader.SpinlockDescription("NOT_A_REAL_SPINLOCK"));
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpLatchSpinlockTools>();
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
/// Gated (DARLING_TEST_PG) live round-trips for the latch/spinlock tools. Plants two snapshots (for the LAG
/// per-second interval) of a latch class and a spinlock, then asserts each tool returns its data-bearing
/// envelope and an empty store returns the miss.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpLatchSpinlockToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-latch-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task LatchSpinlockTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live latch/spinlock-tools test.");

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
            var older = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-4);
            var newer = older.AddMinutes(2);

            foreach (var (t, delta) in new[] { (older, 4000L), (newer, 6000L) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO latch_stats (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms, delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "ACCESS_METHODS_DATASET_PARENT", 1000L, 20000L, 50L, 100L, delta, 5L);

                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO spinlock_stats (collection_id, collection_time, server_id, server_name, spinlock_name, collisions, spins, spins_per_collision, sleep_time, backoffs, delta_collisions, delta_spins, delta_sleep_time, delta_backoffs)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "LOCK_HASH", 900000L, 5000000L, 5.5d, 100L, 200L, delta * 10, delta * 50, 3L, 7L);
            }

            var latch = await DarlingMcpLatchSpinlockTools.GetLatchStats(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(latch, ServerName, "latches");
            Assert.Contains("ACCESS_METHODS_DATASET_PARENT", latch, StringComparison.Ordinal);
            Assert.Contains("Index/heap access contention", latch, StringComparison.Ordinal);

            var spin = await DarlingMcpLatchSpinlockTools.GetSpinlockStats(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(spin, ServerName, "spinlocks");
            Assert.Contains("Lock manager hash table access.", spin, StringComparison.Ordinal);

            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpLatchSpinlockTools.GetLatchStats(postgres, ServerName)));

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
        var sql = string.Join(" ", new[] { "latch_stats", "spinlock_stats" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"));
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
