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
using System.Text.Json;
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
/// Pins the windowed-trend MCP slice — get_memory_trend / get_perfmon_trend / get_file_io_trend /
/// get_query_trend / get_query_duration_trend / get_procedure_duration_trend /
/// get_query_store_duration_trend over the Postgres store, the same names Lite and (for four of
/// the seven) the Dashboard expose. Ungated: the exact seven-tool surface, each tool's MCP parameter contract
/// (server_name optional; counter_name required on get_perfmon_trend; query_hash + database_name required on
/// get_query_trend), the read-SQL pins (Postgres dialect, positional params, the collector columns the schema
/// generator emits, BOTH-sides collection_time window), and the Gemini-clean advertised schema (#1074).
/// </summary>
public sealed class DarlingMcpTrendToolsSurfaceAndSqlTests
{
    private static readonly string[] TrendToolSurface =
    {
        "get_file_io_trend",
        "get_memory_trend",
        "get_perfmon_trend",
        "get_procedure_duration_trend",
        "get_query_duration_trend",
        "get_query_store_duration_trend",
        "get_query_trend",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpTrendTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheSevenTrendTools()
    {
        var names = ToolMethods()
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(TrendToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpTrendTools).GetCustomAttribute<McpServerToolTypeAttribute>());
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
    [InlineData("get_memory_trend", "server_name,hours_back,as_of")]
    [InlineData("get_perfmon_trend", "counter_name,server_name,hours_back,as_of")]
    [InlineData("get_file_io_trend", "server_name,hours_back,as_of")]
    [InlineData("get_query_trend", "query_hash,database_name,server_name,hours_back,as_of")]
    [InlineData("get_query_duration_trend", "server_name,hours_back,as_of")]
    [InlineData("get_procedure_duration_trend", "server_name,hours_back,as_of")]
    [InlineData("get_query_store_duration_trend", "server_name,hours_back,as_of")]
    public void ParamContract_MatchesLite(string toolName, string expectedCsv)
    {
        Assert.Equal(expectedCsv.Split(','), McpParams(toolName).Select(p => p.Name).ToArray());
    }

    [Fact]
    public void ParamContract_ServerNameOptional_RequiredKeysAreNot()
    {
        foreach (var tool in TrendToolSurface)
        {
            var p = McpParams(tool);
            Assert.True(p.Single(x => x.Name == "server_name").Optional, $"{tool}.server_name must be optional");
        }

        Assert.False(McpParams("get_perfmon_trend").Single(x => x.Name == "counter_name").Optional);
        Assert.False(McpParams("get_query_trend").Single(x => x.Name == "query_hash").Optional);
        Assert.False(McpParams("get_query_trend").Single(x => x.Name == "database_name").Optional);
    }

    [Fact]
    public void MemoryTrendSql_WindowedBothSides_CastsNumericToDouble()
    {
        var sql = DarlingTrendReader.MemoryTrendSql;
        Assert.Contains("FROM v_memory_stats", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(total_server_memory_mb AS double precision)", sql, StringComparison.Ordinal);
        Assert.Contains("buffer_pool_mb", sql, StringComparison.Ordinal);
        Assert.Contains("plan_cache_mb", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PerfmonTrendSql_SingleCounter_SumsInstances_CastsBigint()
    {
        var sql = DarlingTrendReader.PerfmonTrendSql;
        Assert.Contains("FROM v_perfmon_stats", sql, StringComparison.Ordinal);
        Assert.Contains("counter_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(cntr_value) AS bigint)", sql, StringComparison.Ordinal);       /* PG SUM(bigint) is numeric */
        Assert.Contains("CAST(SUM(delta_cntr_value) AS bigint)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collection_time", sql, StringComparison.Ordinal);

        var distinct = DarlingTrendReader.DistinctPerfmonCountersSql;
        Assert.Contains("SELECT DISTINCT counter_name", distinct, StringComparison.Ordinal);
        Assert.Contains("FROM v_perfmon_stats", distinct, StringComparison.Ordinal);
    }

    [Fact]
    public void FileIoLatencyTrendSql_TopFiles_StallPerOp_WindowedBothSides()
    {
        var sql = DarlingTrendReader.FileIoLatencyTrendSql;
        Assert.Contains("FROM v_file_io_stats", sql, StringComparison.Ordinal);
        Assert.Contains("top_files", sql, StringComparison.Ordinal);                              /* 10 busiest files */
        Assert.Contains("delta_stall_read_ms", sql, StringComparison.Ordinal);
        Assert.Contains("delta_stall_write_ms", sql, StringComparison.Ordinal);
        Assert.Contains("avg_read_latency_ms", sql, StringComparison.Ordinal);
        Assert.Contains("f.collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("f.collection_time <= $3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryDurationTrendSql_PerSecondRate_ReadsBaseTable()
    {
        var sql = DarlingTrendReader.QueryDurationTrendSql;
        Assert.Contains("FROM query_stats", sql, StringComparison.Ordinal);                       /* base table, like the merged data reader */
        Assert.DoesNotContain("v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_elapsed_time)", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("elapsed_ms_per_second", sql, StringComparison.Ordinal);
        Assert.Contains("executions_per_second", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2484: the procedure trend is the query trend over a DIFFERENT table, and that is the whole reason it
    /// exists. If this ever reads query_stats it has silently become a second name for its sibling.
    /// </summary>
    [Fact]
    public void ProcedureDurationTrendSql_SameRate_OverProcedureStats()
    {
        var sql = DarlingTrendReader.ProcedureDurationTrendSql;
        Assert.Contains("FROM procedure_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_procedure_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_elapsed_time)", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("elapsed_ms_per_second", sql, StringComparison.Ordinal);
        Assert.Contains("executions_per_second", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2484: the Query Store trend carries the #1841 tier-2 interval placement, copied from the viewer's
    /// read rather than rewritten. Both arms are pinned because losing either one changes the numbers: drop
    /// arm 1 and every open interval is charged to each cycle that fetched it; drop arm 2 and rows collected
    /// before the fix vanish from the chart entirely.
    /// </summary>
    [Fact]
    public void QueryStoreDurationTrendSql_KeepsBothIntervalArms_AndPlacesWorkWhenItRan()
    {
        var sql = DarlingTrendReader.QueryStoreDurationTrendSql;
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_query_store_stats", sql, StringComparison.Ordinal);

        /* Arm 1: dedup to the interval's final snapshot, placed at the hour the work RAN. */
        Assert.Contains("ROW_NUMBER() OVER", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc AS point_time", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc IS NOT NULL", sql, StringComparison.Ordinal);

        /* Arm 2: the legacy rows, split on the same column so the two arms partition with no overlap. */
        Assert.Contains("UNION ALL", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time AS point_time", sql, StringComparison.Ordinal);

        Assert.Contains("duration_ms_per_second", sql, StringComparison.Ordinal);
        Assert.Contains("executions_per_second", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2484: each probe must read the SAME table its trend reads. A probe on a different source could
    /// report a server as sampled for rows the trend can never see — the wrong branch in exactly the case
    /// the probe exists to get right. They are windowless by design: a time bound would make the probe
    /// answer the same question the read just did.
    /// </summary>
    [Theory]
    [InlineData(nameof(DarlingTrendReader.HasAnyQueryStatSql), "query_stats")]
    [InlineData(nameof(DarlingTrendReader.HasAnyProcedureStatSql), "procedure_stats")]
    [InlineData(nameof(DarlingTrendReader.HasAnyQueryStoreStatSql), "query_store_stats")]
    public void TrendProbes_ReadTheirOwnTable_WindowlessAndLimited(string sqlName, string table)
    {
        var sql = SqlByName(sqlName);
        Assert.Contains("FROM " + table, sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_time", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryHistorySql_OneQuery_CarriesDeltas_ReadsBaseTable()
    {
        var sql = DarlingTrendReader.QueryHistorySql;
        Assert.Contains("FROM query_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("database_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("query_hash = $3", sql, StringComparison.Ordinal);
        Assert.Contains("delta_execution_count", sql, StringComparison.Ordinal);
        Assert.Contains("delta_worker_time", sql, StringComparison.Ordinal);
        Assert.Contains("query_plan_hash", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $4", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $5", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingTrendReader.MemoryTrendSql))]
    [InlineData(nameof(DarlingTrendReader.PerfmonTrendSql))]
    [InlineData(nameof(DarlingTrendReader.DistinctPerfmonCountersSql))]
    [InlineData(nameof(DarlingTrendReader.FileIoLatencyTrendSql))]
    [InlineData(nameof(DarlingTrendReader.QueryDurationTrendSql))]
    [InlineData(nameof(DarlingTrendReader.ProcedureDurationTrendSql))]
    [InlineData(nameof(DarlingTrendReader.QueryStoreDurationTrendSql))]
    [InlineData(nameof(DarlingTrendReader.QueryStoreDurationTrendRollupSql))]
    [InlineData(nameof(DarlingTrendReader.HasAnyQueryStatSql))]
    [InlineData(nameof(DarlingTrendReader.HasAnyProcedureStatSql))]
    [InlineData(nameof(DarlingTrendReader.HasAnyQueryStoreStatSql))]
    [InlineData(nameof(DarlingTrendReader.QueryHistorySql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = SqlByName(sqlName);
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    private static string SqlByName(string name) => name switch
    {
        nameof(DarlingTrendReader.MemoryTrendSql) => DarlingTrendReader.MemoryTrendSql,
        nameof(DarlingTrendReader.PerfmonTrendSql) => DarlingTrendReader.PerfmonTrendSql,
        nameof(DarlingTrendReader.DistinctPerfmonCountersSql) => DarlingTrendReader.DistinctPerfmonCountersSql,
        nameof(DarlingTrendReader.FileIoLatencyTrendSql) => DarlingTrendReader.FileIoLatencyTrendSql,
        nameof(DarlingTrendReader.QueryDurationTrendSql) => DarlingTrendReader.QueryDurationTrendSql,
        nameof(DarlingTrendReader.ProcedureDurationTrendSql) => DarlingTrendReader.ProcedureDurationTrendSql,
        nameof(DarlingTrendReader.QueryStoreDurationTrendSql) => DarlingTrendReader.QueryStoreDurationTrendSql,
        nameof(DarlingTrendReader.QueryStoreDurationTrendRollupSql) => DarlingTrendReader.QueryStoreDurationTrendRollupSql,
        nameof(DarlingTrendReader.HasAnyQueryStatSql) => DarlingTrendReader.HasAnyQueryStatSql,
        nameof(DarlingTrendReader.HasAnyProcedureStatSql) => DarlingTrendReader.HasAnyProcedureStatSql,
        nameof(DarlingTrendReader.HasAnyQueryStoreStatSql) => DarlingTrendReader.HasAnyQueryStoreStatSql,
        _ => DarlingTrendReader.QueryHistorySql,
    };

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTables()
    {
        var mem = PgSchemaGenerator.CreateTable(MemoryStatsCollector.Instance);
        Assert.Contains("total_server_memory_mb", mem, StringComparison.Ordinal);
        Assert.Contains("target_server_memory_mb", mem, StringComparison.Ordinal);
        Assert.Contains("buffer_pool_mb", mem, StringComparison.Ordinal);
        Assert.Contains("plan_cache_mb", mem, StringComparison.Ordinal);

        var perfmon = PgSchemaGenerator.CreateTable(PerfmonStatsCollector.Instance);
        Assert.Contains("counter_name", perfmon, StringComparison.Ordinal);
        Assert.Contains("cntr_value", perfmon, StringComparison.Ordinal);
        Assert.Contains("delta_cntr_value", perfmon, StringComparison.Ordinal);

        var io = PgSchemaGenerator.CreateTable(FileIoStatsCollector.Instance);
        Assert.Contains("delta_reads", io, StringComparison.Ordinal);
        Assert.Contains("delta_stall_read_ms", io, StringComparison.Ordinal);
        Assert.Contains("delta_stall_write_ms", io, StringComparison.Ordinal);

        var qs = PgSchemaGenerator.CreateTable(QueryStatsCollector.Instance);
        Assert.Contains("delta_execution_count", qs, StringComparison.Ordinal);
        Assert.Contains("delta_elapsed_time", qs, StringComparison.Ordinal);
        Assert.Contains("delta_worker_time", qs, StringComparison.Ordinal);
        Assert.Contains("query_hash", qs, StringComparison.Ordinal);
        Assert.Contains("query_plan_hash", qs, StringComparison.Ordinal);

        var procs = PgSchemaGenerator.CreateTable(ProcedureStatsCollector.Instance);
        Assert.Contains("delta_execution_count", procs, StringComparison.Ordinal);
        Assert.Contains("delta_elapsed_time", procs, StringComparison.Ordinal);

        /* The Query Store trend places its points at interval_start_time_utc, so the column has to exist
           on the collected table or arm 1 silently returns nothing and arm 2 quietly serves everything. */
        var store = PgSchemaGenerator.CreateTable(QueryStoreCollector.Instance);
        Assert.Contains("interval_start_time_utc", store, StringComparison.Ordinal);
        Assert.Contains("execution_count", store, StringComparison.Ordinal);
        Assert.Contains("avg_duration_us", store, StringComparison.Ordinal);
        Assert.Contains("runtime_stats_interval_id", store, StringComparison.Ordinal);
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpTrendTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_RequiredParamsMatchLite()
    {
        var tools = BuildToolSchemas();
        /* Derived from the pinned name list rather than restated, so a new trend tool cannot land with
           this literal left describing the old surface. */
        Assert.Equal(TrendToolSurface.Length, tools.Count);

        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));

        var byName = tools.ToDictionary(t => t.Name, t => t.InputSchema);
        Assert.Equal(new[] { "counter_name" }, DarlingMcpSchemaAssert.RequiredOf(byName["get_perfmon_trend"]));
        Assert.Equal(new[] { "query_hash", "database_name" }, DarlingMcpSchemaAssert.RequiredOf(byName["get_query_trend"]));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(byName["get_memory_trend"]));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(byName["get_file_io_trend"]));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(byName["get_query_duration_trend"]));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(byName["get_procedure_duration_trend"]));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(byName["get_query_store_duration_trend"]));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the trend tools. Plants two collection cycles in each source
/// table so the LAG-based per-second rates and the single-query history have data, then asserts each tool
/// returns its data-bearing "trend" envelope; an empty store returns the #1224 miss, and get_perfmon_trend's
/// miss vocabulary (unknown counter, PLE) is exercised.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpTrendToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-trend-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TrendTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trend-tools test.");

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
            var older = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-3);
            var newer = older.AddMinutes(1);

            foreach (var (t, v) in new[] { (older, 40000m), (newer, 41000m) })
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, plan_cache_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", CollectionIdGenerator.Next(), t, ServerId, ServerName, v, 49152m, v - 5000m, 5000m);

            foreach (var (t, cv, dv) in new[] { (older, 100000L, 0L), (newer, 123456L, 23456L) })
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", CollectionIdGenerator.Next(), t, ServerId, ServerName, "SQLServer:SQL Statistics", "Batch Requests/sec", "", cv, dv);

            foreach (var (t, dr) in new[] { (older, 500L), (newer, 800L) })
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)", CollectionIdGenerator.Next(), t, ServerId, ServerName, Db, "so.mdf", "ROWS", "D:\\so.mdf", 100000m, dr, 200L, 4096000L, 1024000L, dr * 5L, 400L);

            foreach (var (t, dc) in new[] { (older, 10L), (newer, 30L) })
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills, min_dop, max_dop)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, Db, "0xTRENDHASH", "0xPLANHASH", "0xSQLH", "0xPLANH", "SELECT * FROM Posts",
                    dc, dc * 1000L, dc * 2000L, dc * 500L, 0L, dc * 5L, dc * 100L, 0L, 1, 4);

            DarlingMcpTestData.AssertEnvelope(await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName), ServerName, "trend");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpTrendTools.GetPerfmonTrend(postgres, "Batch Requests/sec", ServerName), ServerName, "trend");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName), ServerName, "trend");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName), ServerName, "trend");

            var qt = await DarlingMcpTrendTools.GetQueryTrend(postgres, "0xTRENDHASH", Db, ServerName);
            DarlingMcpTestData.AssertEnvelope(qt, ServerName, "trend");
            Assert.Contains("0xPLANHASH", qt, StringComparison.Ordinal);

            /* perfmon miss vocabulary: an unknown counter is not_collected + lists the collected ones; PLE is intentionally not collected. */
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(await DarlingMcpTrendTools.GetPerfmonTrend(postgres, "No Such Counter", ServerName)));
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(await DarlingMcpTrendTools.GetPerfmonTrend(postgres, "Page life expectancy", ServerName)));

            /* an unknown server resolves to the listing error. */
            Assert.StartsWith("Could not resolve server.", await DarlingMcpTrendTools.GetMemoryTrend(postgres, "darling-no-such-server"), StringComparison.Ordinal);

            /* an EMPTY store returns the miss, not a throw. */
            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName)));
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName)));
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpTrendTools.GetQueryTrend(postgres, "0xTRENDHASH", Db, ServerName)));

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
        var tables = new[] { "memory_stats", "perfmon_stats", "file_io_stats", "query_stats" };
        var sql = string.Join(" ", tables.Select(t => $"DELETE FROM {t} WHERE server_id = {ServerId};"));
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
