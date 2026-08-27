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
/// Pins the index / object diagnostic-depth MCP slice — get_table_index_sizes, get_index_usage,
/// get_object_locking, get_database_sizes over the Postgres store. Ungated: tool surface, the single
/// server_name param each, the latest-snapshot read SQL pins (v_index_object_stats / v_database_size_stats),
/// and the Gemini-clean advertised schema.
/// </summary>
public sealed class DarlingMcpObjectStatsToolsSurfaceAndSqlTests
{
    private static readonly string[] ObjectStatsToolSurface =
    {
        "get_database_sizes",
        "get_index_usage",
        "get_object_locking",
        "get_table_index_sizes",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpObjectStatsTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheFourObjectStatsTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(ObjectStatsToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpObjectStatsTools).GetCustomAttribute<McpServerToolTypeAttribute>());
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
    [InlineData("get_table_index_sizes")]
    [InlineData("get_object_locking")]
    [InlineData("get_database_sizes")]
    public void ParamContract_ServerNameOnly_Optional(string toolName)
    {
        var p = McpParams(toolName);
        Assert.Equal(new[] { "server_name" }, p.Select(x => x.Name).ToArray());
        Assert.True(p.Single().Optional);
    }

    /// <summary>
    /// #2636 moved <c>get_index_usage</c> off the server-name-only contract, and it had to.
    ///
    /// <para>Rows sort unused-first across the WHOLE server behind a cap, so on an instance with enough
    /// unused indexes in one database the entire answer comes from that database and every Active index
    /// elsewhere is invisible. A field report hit exactly that: healthy collection, full retention, zero
    /// returned rows for the database asked about, and nothing in the answer to distinguish "not returned"
    /// from "not collected". <c>database_name</c> is what makes the question askable and <c>limit</c> is
    /// what makes the cap the caller's; both stay OPTIONAL, so every existing caller behaves as before.</para>
    /// </summary>
    [Fact]
    public void IndexUsage_TakesADatabaseFilterAndALimit_BothOptional()
    {
        var p = McpParams("get_index_usage");

        Assert.Equal(new[] { "server_name", "database_name", "limit" }, p.Select(x => x.Name).ToArray());
        Assert.All(p, x => Assert.True(x.Optional, $"{x.Name} must stay optional — existing callers pass neither"));
    }

    [Fact]
    public void ObjectSizeGrowthSql_RollsUpPerTable_ComputesGrowth()
    {
        var sql = DarlingObjectStatsReader.ObjectSizeGrowthSql;
        Assert.Contains("FROM v_index_object_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(reserved_mb)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY database_name, schema_name, table_name", sql, StringComparison.Ordinal);
        Assert.Contains("growth_7d_mb", sql, StringComparison.Ordinal);
        Assert.Contains("growth_30d_mb", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY l.current_reserved_mb DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void IndexUsageSql_LatestSnapshot_ClassifiesUnusedWriteOnlyActive()
    {
        var sql = DarlingObjectStatsReader.IndexUsageSql;
        Assert.Contains("FROM v_index_object_stats", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("'Unused'", sql, StringComparison.Ordinal);
        Assert.Contains("'Write-only'", sql, StringComparison.Ordinal);
        Assert.Contains("'Active'", sql, StringComparison.Ordinal);
        Assert.Contains("user_seeks", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void IndexLockingSql_PerDatabaseLatest_NonzeroWaits_ContendedFirst()
    {
        var sql = DarlingObjectStatsReader.IndexLockingSql;
        Assert.Contains("FROM v_index_object_stats", sql, StringComparison.Ordinal);
        Assert.Contains("row_lock_wait_in_ms", sql, StringComparison.Ordinal);
        Assert.Contains("page_io_latch_wait_in_ms", sql, StringComparison.Ordinal);
        Assert.Contains("index_lock_promotion_count", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseSizeLatestSql_LatestSnapshot_CarriesSizeAndVolume()
    {
        var sql = DarlingObjectStatsReader.DatabaseSizeLatestSql;
        Assert.Contains("FROM v_database_size_stats", sql, StringComparison.Ordinal);
        Assert.Contains("total_size_mb", sql, StringComparison.Ordinal);
        Assert.Contains("max_size_mb", sql, StringComparison.Ordinal);
        Assert.Contains("volume_free_mb", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingObjectStatsReader.ObjectSizeGrowthSql))]
    [InlineData(nameof(DarlingObjectStatsReader.IndexUsageSql))]
    [InlineData(nameof(DarlingObjectStatsReader.IndexLockingSql))]
    [InlineData(nameof(DarlingObjectStatsReader.DatabaseSizeLatestSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(DarlingObjectStatsReader.ObjectSizeGrowthSql) => DarlingObjectStatsReader.ObjectSizeGrowthSql,
            nameof(DarlingObjectStatsReader.IndexUsageSql) => DarlingObjectStatsReader.IndexUsageSql,
            nameof(DarlingObjectStatsReader.IndexLockingSql) => DarlingObjectStatsReader.IndexLockingSql,
            _ => DarlingObjectStatsReader.DatabaseSizeLatestSql,
        };
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);   /* CAST(... AS double precision), not T-SQL CONVERT */
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);     /* COALESCE, not ISNULL */
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTables()
    {
        var ios = PgSchemaGenerator.CreateTable(IndexObjectStatsCollector.Instance);
        Assert.Equal("index_object_stats", IndexObjectStatsCollector.Instance.TargetTable);
        Assert.Contains("reserved_mb", ios, StringComparison.Ordinal);
        Assert.Contains("user_seeks", ios, StringComparison.Ordinal);
        Assert.Contains("row_lock_wait_in_ms", ios, StringComparison.Ordinal);
        Assert.Contains("index_lock_promotion_count", ios, StringComparison.Ordinal);
        Assert.Contains("page_io_latch_wait_in_ms", ios, StringComparison.Ordinal);

        var dss = PgSchemaGenerator.CreateTable(DatabaseSizeStatsCollector.Instance);
        Assert.Equal("database_size_stats", DatabaseSizeStatsCollector.Instance.TargetTable);
        Assert.Contains("total_size_mb", dss, StringComparison.Ordinal);
        Assert.Contains("max_size_mb", dss, StringComparison.Ordinal);
        Assert.Contains("volume_free_mb", dss, StringComparison.Ordinal);

        Assert.Contains("v_index_object_stats", PgSchemaGenerator.AllPassthroughViews);
        Assert.Contains("v_database_size_stats", PgSchemaGenerator.AllPassthroughViews);
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpObjectStatsTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForAllFourTools_NoRequiredParams()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(4, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        foreach (var t in tools)
            Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(t.InputSchema));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the object-stats tools. Plants an index_object_stats row (with
/// usage + locking counters) and a database_size_stats row, then asserts each tool returns its data-bearing
/// envelope and an empty store returns the miss.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpObjectStatsToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-objectstats-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ObjectStatsTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live object-stats-tools test.");

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
                @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, Db, "dbo", 100, "Posts", 1, "PK_Posts", "CLUSTERED", 5000m, 4800m, 1000000L, 500L, 10L, 20L, 300L, 40L, 12000L, 5L, 800L, 3L, 60L, 2L, 30L);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb, max_size_mb, volume_mount_point, volume_total_mb, volume_free_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, Db, 5, 1, "ROWS", "so_data", "D:\\so.mdf", 100000m, 90000m, -1m, "D:\\", 500000m, 250000m);

            var sizes = await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(sizes, ServerName, "tables");
            Assert.Contains("Posts", sizes, StringComparison.Ordinal);

            DarlingMcpTestData.AssertEnvelope(await DarlingMcpObjectStatsTools.GetIndexUsage(postgres, ServerName), ServerName, "indexes");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, ServerName), ServerName, "objects");
            var dbSizes = await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(dbSizes, ServerName, "databases");
            Assert.Contains("volume_free_mb", dbSizes, StringComparison.Ordinal);

            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, ServerName)));

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
        var sql = string.Join(" ", new[] { "index_object_stats", "database_size_stats" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"));
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
