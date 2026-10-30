/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

using Reader = PerformanceMonitor.Darling.Service.Mcp.DarlingCurrentConfigReader;

namespace Darling.Tests;

/// <summary>
/// Pins the current-config snapshot MCP slice — get_server_config, get_database_config, get_trace_flags over
/// the Postgres store (the "what is it set to right now" companion to the *_changes diff tools). Ungated: the
/// tool surface is EXACTLY the three names (all static, on a [McpServerToolType] class, returning
/// Task&lt;string&gt;); each param contract matches Lite's; every latest-snapshot read is Postgres-dialect,
/// positional-param, reads the config passthrough view via <c>MAX(capture_time)</c>; and the advertised
/// tools/list schema is Gemini-clean.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class DarlingMcpConfigToolsSurfaceAndSqlTests
{
    private static readonly string[] ConfigToolSurface =
    {
        "get_database_config",
        "get_server_config",
        "get_trace_flags",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpConfigTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheThreeConfigTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(ConfigToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpConfigTools).GetCustomAttribute<McpServerToolTypeAttribute>());
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
    [InlineData("get_server_config", "server_name")]
    [InlineData("get_database_config", "server_name,database_name")]
    [InlineData("get_trace_flags", "server_name")]
    public void ParamContract_MatchesLite(string toolName, string expectedCsv)
    {
        Assert.Equal(expectedCsv.Split(','), McpParams(toolName).Select(p => p.Name).ToArray());
    }

    [Fact]
    public void ParamContract_ServerNameAlwaysOptional()
    {
        foreach (var tool in ConfigToolSurface)
            Assert.True(McpParams(tool).Single(x => x.Name == "server_name").Optional, $"{tool}.server_name must be optional");
    }

    /* ---------------- latest-snapshot read SQL pins ---------------- */

    [Fact]
    public void ServerConfigSql_LatestSnapshot()
    {
        var sql = Reader.ServerConfigSql;
        Assert.Contains("FROM v_server_config", sql, StringComparison.Ordinal);
        Assert.Contains("value_configured", sql, StringComparison.Ordinal);
        Assert.Contains("value_in_use", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(capture_time)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseConfigSql_LatestSnapshot_28ColumnOrder()
    {
        var sql = Reader.DatabaseConfigSql;
        Assert.Contains("FROM v_database_config", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(capture_time)", sql, StringComparison.Ordinal);
        Assert.Contains("is_read_committed_snapshot_on", sql, StringComparison.Ordinal);
        Assert.Contains("is_optimized_locking_on", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TraceFlagsSql_LatestSnapshot()
    {
        var sql = Reader.TraceFlagsSql;
        Assert.Contains("FROM v_trace_flags", sql, StringComparison.Ordinal);
        Assert.Contains("trace_flag", sql, StringComparison.Ordinal);
        Assert.Contains("is_global", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(capture_time)", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(Reader.ServerConfigSql))]
    [InlineData(nameof(Reader.DatabaseConfigSql))]
    [InlineData(nameof(Reader.TraceFlagsSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(Reader.ServerConfigSql) => Reader.ServerConfigSql,
            nameof(Reader.DatabaseConfigSql) => Reader.DatabaseConfigSql,
            _ => Reader.TraceFlagsSql,
        };
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    /* ---------------- advertised MCP schema ---------------- */

    private static System.Collections.Generic.List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpConfigTools>();
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
/// Gated (DARLING_TEST_PG) live round-trips for the current-config tools. Plants one latest-capture snapshot
/// in each config view (server_config, database_config, trace_flags), then asserts each tool round-trips its
/// data-bearing envelope.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpConfigToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-currentconfig-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ConfigTools_ReadPlantedSnapshots_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live current-config test.");

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
            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                CollectionIdGenerator.Next(), when, ServerId, ServerName, "max degree of parallelism", 8L, 8L, true, true);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc, compatibility_level, collation_name, recovery_model,
    is_read_only, is_auto_close_on, is_auto_shrink_on, is_auto_create_stats_on, is_auto_update_stats_on, is_auto_update_stats_async_on,
    is_read_committed_snapshot_on, snapshot_isolation_state, is_parameterization_forced, is_query_store_on, is_encrypted, is_trustworthy_on,
    is_db_chaining_on, is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on, log_reuse_wait_desc, page_verify_option,
    target_recovery_time_seconds, delayed_durability, is_accelerated_database_recovery_on, is_memory_optimized_enabled, is_optimized_locking_on)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32)",
                CollectionIdGenerator.Next(), when, ServerId, ServerName, Db, "ONLINE", 160, "SQL_Latin1_General_CP1_CI_AS", "FULL",
                false, false, false, true, true, false,
                true, "OFF", false, true, false, false,
                false, true, false, false, "NOTHING", "CHECKSUM",
                60, "DISABLED", false, false, false);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO trace_flags (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), when, ServerId, ServerName, 3226, true, true, false);

            var serverConfig = await DarlingMcpConfigTools.GetServerConfig(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(serverConfig, ServerName, "settings");
            Assert.Contains("max degree of parallelism", serverConfig, StringComparison.Ordinal);

            var dbConfig = await DarlingMcpConfigTools.GetDatabaseConfig(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(dbConfig, ServerName, "databases");
            Assert.Contains("FULL", dbConfig, StringComparison.Ordinal);
            Assert.DoesNotContain("\"note\"", dbConfig, StringComparison.Ordinal);

            /* A filter that matches nothing in a snapshot that holds databases keeps the data shape (count 0, no databases) and says why. */
            var missed = System.Text.Json.JsonDocument.Parse(await DarlingMcpConfigTools.GetDatabaseConfig(postgres, ServerName, "NoSuchDb")).RootElement;
            Assert.Equal(0, missed.GetProperty("database_count").GetInt32());
            Assert.False(missed.TryGetProperty("status", out _));
            Assert.Contains("master, model and msdb are not collected", missed.GetProperty("note").GetString(), StringComparison.Ordinal);

            var traceFlags = await DarlingMcpConfigTools.GetTraceFlags(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(traceFlags, ServerName, "trace_flags");
            Assert.Contains("3226", traceFlags, StringComparison.Ordinal);

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
        var sql = string.Join(" ", new[] { "server_config", "database_config", "trace_flags" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"))
            + $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
