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

using Reader = PerformanceMonitor.Darling.Service.Mcp.DarlingConfigHistoryReader;

namespace Darling.Tests;

/// <summary>
/// Pins the config / trace-flag diagnostic-depth MCP slice — get_server_config_changes,
/// get_database_config_changes, get_trace_flag_changes, and the latest-snapshot pair
/// get_database_scoped_config + get_query_store_health (#2319) over the Postgres store.
/// The Dashboard reads pre-materialized report.*_changes tables Darling does not have, so the change tools
/// diff the store's append-only config snapshots IN C#; the bulk of these tests exercise that diff directly
/// (no live PG). Also pins the tool surface, param contracts, snapshot-read SQL, the 27-setting database-config
/// column list, and the Gemini-clean advertised schema.
/// </summary>
public sealed class DarlingMcpConfigHistoryToolsSurfaceAndSqlTests
{
    private static readonly string[] ConfigToolSurface =
    {
        "get_database_config_changes",
        "get_database_scoped_config",
        "get_query_store_health",
        "get_server_config_changes",
        "get_trace_flag_changes",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpConfigHistoryTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheFiveConfigTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(ConfigToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpConfigHistoryTools).GetCustomAttribute<McpServerToolTypeAttribute>());
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
    [InlineData("get_server_config_changes", "server_name,hours_back,as_of")]
    [InlineData("get_database_config_changes", "server_name,hours_back,as_of")]
    [InlineData("get_trace_flag_changes", "server_name,hours_back,as_of")]
    [InlineData("get_database_scoped_config", "server_name,database_name")]
    [InlineData("get_query_store_health", "server_name,database_name")]
    public void ParamContract_MatchesContract(string toolName, string expectedCsv)
    {
        Assert.Equal(expectedCsv.Split(','), McpParams(toolName).Select(p => p.Name).ToArray());
    }

    [Fact]
    public void ParamContract_ServerNameAlwaysOptional()
    {
        foreach (var tool in ConfigToolSurface)
            Assert.True(McpParams(tool).Single(x => x.Name == "server_name").Optional, $"{tool}.server_name must be optional");
    }

    /* ---------------- snapshot-read SQL pins ---------------- */

    [Fact]
    public void ServerConfigSnapshotsSql_ReadsView_OrderedForPerNameDiff()
    {
        var sql = Reader.ServerConfigSnapshotsSql;
        Assert.Contains("FROM v_server_config", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time", sql, StringComparison.Ordinal);
        Assert.Contains("value_configured", sql, StringComparison.Ordinal);
        Assert.Contains("value_in_use", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY configuration_name, capture_time", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseConfigSnapshotsSql_ReadsView_CastsSettingsToText()
    {
        var sql = Reader.DatabaseConfigSnapshotsSql;
        Assert.Contains("FROM v_database_config", sql, StringComparison.Ordinal);
        Assert.Contains("is_read_committed_snapshot_on::text", sql, StringComparison.Ordinal);
        Assert.Contains("compatibility_level::text", sql, StringComparison.Ordinal);
        Assert.Contains("is_optimized_locking_on::text", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TraceFlagSnapshotsSql_ReadsView()
    {
        var sql = Reader.TraceFlagSnapshotsSql;
        Assert.Contains("FROM v_trace_flags", sql, StringComparison.Ordinal);
        Assert.Contains("trace_flag", sql, StringComparison.Ordinal);
        Assert.Contains("is_global", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseScopedConfigSql_LatestSnapshot()
    {
        var sql = Reader.DatabaseScopedConfigSql;
        Assert.Contains("FROM v_database_scoped_config", sql, StringComparison.Ordinal);
        Assert.Contains("value_for_secondary", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(capture_time)", sql, StringComparison.Ordinal);
    }

    /// <summary>The read must be the same latest-snapshot shape as the scoped-config sibling, over the
    /// passthrough view, selecting the reader's ten ordinals in the collector's payload order.</summary>
    [Fact]
    public void QueryStoreHealthSql_LatestSnapshot_SelectsPayloadOrder()
    {
        var sql = Reader.QueryStoreHealthSql;
        Assert.Contains("FROM v_query_store_health", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(capture_time)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY database_name", sql, StringComparison.Ordinal);
        Assert.Contains(
            "database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes",
            sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(Reader.ServerConfigSnapshotsSql))]
    [InlineData(nameof(Reader.DatabaseConfigSnapshotsSql))]
    [InlineData(nameof(Reader.TraceFlagSnapshotsSql))]
    [InlineData(nameof(Reader.DatabaseScopedConfigSql))]
    [InlineData(nameof(Reader.QueryStoreHealthSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(Reader.ServerConfigSnapshotsSql) => Reader.ServerConfigSnapshotsSql,
            nameof(Reader.DatabaseConfigSnapshotsSql) => Reader.DatabaseConfigSnapshotsSql,
            nameof(Reader.TraceFlagSnapshotsSql) => Reader.TraceFlagSnapshotsSql,
            nameof(Reader.DatabaseScopedConfigSql) => Reader.DatabaseScopedConfigSql,
            _ => Reader.QueryStoreHealthSql,
        };
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);   /* ::text casts, not T-SQL CONVERT */
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseConfigSettingNames_Match27CollectorColumns_InOrder()
    {
        var table = PgSchemaGenerator.CreateTable(DatabaseConfigCollector.Instance);
        Assert.Equal(27, ConfigChangeDiff.DatabaseConfigChangeSettingNames.Count);
        foreach (var name in ConfigChangeDiff.DatabaseConfigChangeSettingNames)
            Assert.Contains(name, table, StringComparison.Ordinal);

        /* The change-diff walks these positionally, so their order must match the SELECT's column order. */
        Assert.Equal("state_desc", ConfigChangeDiff.DatabaseConfigChangeSettingNames[0]);
        Assert.Equal("is_optimized_locking_on", ConfigChangeDiff.DatabaseConfigChangeSettingNames[^1]);
    }

    /* ---------------- C# snapshot-diff logic (no live PG) ---------------- */

    private static readonly DateTime T0 = new(2026, 7, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime T1 = new(2026, 7, 2, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime T2 = new(2026, 7, 3, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void ServerConfigChanges_DetectsValueMove_NewestFirst()
    {
        var snapshots = new List<ConfigChangeDiff.ServerConfigSnapshot>
        {
            new(T0, "max degree of parallelism", 0, 0, true, true),
            new(T1, "max degree of parallelism", 4, 4, true, true),   /* changed */
            new(T0, "cost threshold for parallelism", 5, 5, true, true),
            new(T1, "cost threshold for parallelism", 5, 5, true, true), /* unchanged */
        };

        var changes = ConfigChangeDiff.DiffServerConfigChanges(snapshots, T0, DateTime.MaxValue);
        var change = Assert.Single(changes);
        Assert.Equal("max degree of parallelism", change.ConfigurationName);
        Assert.Equal(0L, change.OldValueConfigured);
        Assert.Equal(4L, change.NewValueConfigured);
        Assert.Equal(T1, change.ChangeTime);
    }

    [Fact]
    public void ServerConfigChanges_SingleSnapshot_YieldsNothing_AndWindowFilters()
    {
        var single = new List<ConfigChangeDiff.ServerConfigSnapshot> { new(T0, "max server memory (MB)", 8000, 8000, true, true) };
        Assert.Empty(ConfigChangeDiff.DiffServerConfigChanges(single, T0, DateTime.MaxValue));

        var twoOld = new List<ConfigChangeDiff.ServerConfigSnapshot>
        {
            new(T0, "max server memory (MB)", 8000, 8000, true, true),
            new(T1, "max server memory (MB)", 16000, 16000, true, true),
        };
        /* The change is at T1 — a window starting at T2 excludes it. */
        Assert.Empty(ConfigChangeDiff.DiffServerConfigChanges(twoOld, T2, DateTime.MaxValue));
        Assert.Single(ConfigChangeDiff.DiffServerConfigChanges(twoOld, T0, DateTime.MaxValue));
    }

    [Fact]
    public void TraceFlagChanges_DetectsEnableDisableModify()
    {
        var snapshots = new List<ConfigChangeDiff.TraceFlagSnapshot>
        {
            /* T0: 3226 global on. */
            new(T0, 3226, true, true, false),
            /* T1: 3226 still on, 1117 appears (enabled), 3226 scope unchanged. */
            new(T1, 3226, true, true, false),
            new(T1, 1117, true, true, false),
            /* T2: 1117 disappears (disabled), 3226 becomes session-scoped (modified). */
            new(T2, 3226, true, false, true),
        };

        var changes = ConfigChangeDiff.DiffTraceFlagChanges(snapshots, T0, DateTime.MaxValue);
        Assert.Equal(3, changes.Count);
        Assert.Contains(changes, c => c.TraceFlag == 1117 && c.ChangeType == "enabled" && c.ChangeTime == T1);
        Assert.Contains(changes, c => c.TraceFlag == 1117 && c.ChangeType == "disabled" && c.ChangeTime == T2);
        Assert.Contains(changes, c => c.TraceFlag == 3226 && c.ChangeType == "modified" && c.ChangeTime == T2);
    }

    [Fact]
    public void DatabaseConfigChanges_UnpivotsWideRow_NamesTheChangedColumn()
    {
        const int rcsiIndex = 10; /* is_read_committed_snapshot_on */
        Assert.Equal("is_read_committed_snapshot_on", ConfigChangeDiff.DatabaseConfigChangeSettingNames[rcsiIndex]);

        var snapshots = new List<ConfigChangeDiff.DatabaseConfigSnapshot>
        {
            new(T0, "StackOverflow", DbValues()),
            new(T1, "StackOverflow", DbValues((rcsiIndex, "true"))),  /* RCSI flipped on */
        };

        var changes = ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, T0, DateTime.MaxValue);
        var change = Assert.Single(changes);
        Assert.Equal("StackOverflow", change.DatabaseName);
        Assert.Equal("is_read_committed_snapshot_on", change.SettingName);
        Assert.Equal("false", change.OldValue);
        Assert.Equal("true", change.NewValue);
        Assert.Equal(T1, change.ChangeTime);
    }

    [Fact]
    public void DatabaseConfigChanges_PerDatabase_UnchangedYieldsNothing()
    {
        var snapshots = new List<ConfigChangeDiff.DatabaseConfigSnapshot>
        {
            new(T0, "DbA", DbValues()),
            new(T1, "DbA", DbValues()),          /* no change */
            new(T0, "DbB", DbValues((3, "FULL"))),
            new(T1, "DbB", DbValues((3, "SIMPLE"))),  /* recovery_model changed */
        };

        var changes = ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, T0, DateTime.MaxValue);
        var change = Assert.Single(changes);
        Assert.Equal("DbB", change.DatabaseName);
        Assert.Equal("recovery_model", change.SettingName);
        Assert.Equal("FULL", change.OldValue);
        Assert.Equal("SIMPLE", change.NewValue);
    }

    private static string?[] DbValues(params (int Index, string? Value)[] overrides)
    {
        var values = new string?[ConfigChangeDiff.DatabaseConfigChangeSettingNames.Count];
        for (var i = 0; i < values.Length; i++)
            values[i] = "false";
        foreach (var (index, value) in overrides)
            values[index] = value;
        return values;
    }

    /* ---------------- advertised MCP schema ---------------- */

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpConfigHistoryTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForAllFiveTools_NoRequiredParams()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(5, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        foreach (var t in tools)
            Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(t.InputSchema));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the config tools. Plants two server_config snapshots (one
/// setting changed), two trace-flag snapshots (a flag disappears), a database_scoped_config snapshot, then
/// asserts the change tools surface the change and the scoped-config tool round-trips.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpConfigHistoryToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-confighist-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ConfigTools_ReadPlantedSnapshots_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live config-tools test.");

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
            var older = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddHours(-2);
            var newer = older.AddHours(1);

            foreach (var (t, configured) in new[] { (older, 0L), (newer, 4L) })
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "max degree of parallelism", configured, configured, true, true);

            /* Trace flag 1117 present at the older capture, absent at the newer → 'disabled'. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO trace_flags (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), older, ServerId, ServerName, 1117, true, true, false);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO trace_flags (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), newer, ServerId, ServerName, 3226, true, true, false);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_scoped_config (config_id, capture_time, server_id, server_name, database_name, configuration_name, value, value_for_secondary)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), newer, ServerId, ServerName, Db, "MAXDOP", "8", null);

            /* Query Store health: the cap-hit shape the tool exists to surface — desired READ_WRITE,
               actual READ_ONLY, readonly_reason 65536 (storage cap reached). */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                CollectionIdGenerator.Next(), newer, ServerId, ServerName, Db, "READ_ONLY", "READ_WRITE", 65536, 1000L, 1000L, "AUTO", 30L, 200L, 60L);

            var serverChanges = await DarlingMcpConfigHistoryTools.GetServerConfigChanges(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(serverChanges, ServerName, "changes");
            Assert.Contains("max degree of parallelism", serverChanges, StringComparison.Ordinal);

            var tfChanges = await DarlingMcpConfigHistoryTools.GetTraceFlagChanges(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(tfChanges, ServerName, "changes");
            Assert.Contains("disabled", tfChanges, StringComparison.Ordinal);

            var scoped = await DarlingMcpConfigHistoryTools.GetDatabaseScopedConfig(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(scoped, ServerName, "databases");
            Assert.Contains("MAXDOP", scoped, StringComparison.Ordinal);

            var qsh = await DarlingMcpConfigHistoryTools.GetQueryStoreHealth(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(qsh, ServerName, "databases");
            JsonAssert.Contains("\"state_matches_desired\": false", qsh);
            Assert.Contains("storage cap reached", qsh, StringComparison.Ordinal);
            JsonAssert.Contains("\"pct_of_cap\": 100", qsh);

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
        var sql = string.Join(" ", new[] { "server_config", "database_config", "trace_flags", "database_scoped_config", "query_store_health" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"))
            + $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
