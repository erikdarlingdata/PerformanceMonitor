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
/// Pins the core data-read MCP slice — the eighteen resource-metric / query-performance /
/// discovery-health tools over the Postgres store, the same names the Dashboard and Lite expose.
/// Ungated: the tool surface is EXACTLY the pinned names (all static, on a [McpServerToolType]
/// class, returning the string envelope); each tool's MCP parameter contract matches Lite's (server_name
/// optional / sole-server auto-select, hours_back / top / limit windows, wait_type required on
/// get_wait_trend); every read SQL is Postgres-dialect, positional-param, reads the collector columns the
/// schema generator emits, and windows on the naive-UTC collection_time; and the advertised tools/list
/// schema is Gemini-clean (#1074 — no union types, no default keyword). Gated on DARLING_TEST_PG: register
/// a server the worker's way, plant rows across the collector tables, call the tool METHODS directly and
/// assert each read round-trips its data-bearing envelope and an empty store returns the #1224 miss.
/// </summary>
public sealed class DarlingMcpDataToolsSurfaceAndSqlTests
{
    /* ---------------- ungated: tool-surface pin ---------------- */

    /// <summary>The eighteen data-read tool names, ordinal-sorted — the same names Lite and the Dashboard
    /// expose, so MCP clients see one consistent product.</summary>
    private static readonly string[] DataToolSurface =
    {
        "get_blocking_stats",
        "get_collection_health",
        "get_collection_log",
        "get_cpu_utilization",
        "get_current_waits_trend",
        "get_file_io_stats",
        "get_memory_clerks",
        "get_memory_stats",
        "get_perfmon_stats",
        "get_query_store_top",
        "get_server_properties",
        "get_tempdb_trend",
        "get_top_procedures_by_cpu",
        "get_top_queries_by_cpu",
        "get_wait_stats",
        "get_wait_trend",
        "get_wait_types",
        "list_servers",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpDataTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheEighteenDataTools()
    {
        var toolMethods = ToolMethods();

        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(DataToolSurface, names);

        Assert.NotNull(typeof(DarlingMcpDataTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static for WithGeminiCompatibleTools"));
        Assert.All(toolMethods, m => Assert.True(
            m.ReturnType == typeof(Task<string>),
            $"{m.Name} must return Task<string>"));
    }

    /* ---------------- ungated: per-tool MCP parameter contract ---------------- */

    /// <summary>The MCP input parameters of a tool are those decorated with [Description]; the injected
    /// DI services (NpgsqlDataSource) carry no [Description] and are not part of the schema.</summary>
    private static (string Name, bool Optional)[] McpParams(string toolName)
    {
        var method = ToolMethods().Single(m =>
            m.GetCustomAttribute<McpServerToolAttribute>()!.Name == toolName);
        return method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name!, p.HasDefaultValue))
            .ToArray();
    }

    /// <summary>
    /// Lite's parameter contract, pinned as an ORDERED SUBSEQUENCE of Darling's (#2235, widened by #2495).
    ///
    /// <para>This asserted the full list, which was the same thing until <c>get_top_queries_by_cpu</c> gained a
    /// trailing optional <c>group_by</c> that Lite does not have; it then asserted a PREFIX, which held only
    /// while every parameter Lite gained after that landed before <c>group_by</c>. #2495 appended <c>as_of</c>
    /// to BOTH SKUs — last on each, which is the convention <c>group_by</c> itself established — so Lite's
    /// list is no longer a prefix of Darling's: Darling reads <c>…, min_dop, group_by, as_of</c> while Lite
    /// reads <c>…, min_dop, as_of</c>.</para>
    ///
    /// <para>The guarantee that matters was never "the lists are identical", and it is not positional either:
    /// MCP invokes by NAME, and the only C# call sites (the <c>/api/read</c> dispatch) do not pass Darling's
    /// extra optionals at all. It is that <b>a client written against Lite's contract still calls Darling
    /// correctly</b>, which needs exactly two things — every Lite name present in Lite's relative order, and
    /// every Darling-only extra OPTIONAL so the client never has to supply one. Both are asserted here, so a
    /// dropped parameter, a REORDERING, or a required Darling-only addition all still fail.</para>
    /// </summary>
    [Theory]
    [InlineData("get_cpu_utilization", "server_name,hours_back,as_of")]
    [InlineData("get_wait_stats", "server_name,hours_back,limit,as_of")]
    [InlineData("get_wait_trend", "wait_type,server_name,hours_back,as_of")]
    [InlineData("get_wait_types", "server_name,hours_back,as_of")]
    [InlineData("get_memory_stats", "server_name")]
    [InlineData("get_memory_clerks", "server_name")]
    [InlineData("get_file_io_stats", "server_name")]
    [InlineData("get_tempdb_trend", "server_name,hours_back,as_of")]
    [InlineData("get_perfmon_stats", "server_name,counter_name,instance_name")]
    [InlineData("get_top_queries_by_cpu", "server_name,hours_back,top,database_name,parallel_only,min_dop,as_of")]
    [InlineData("get_top_procedures_by_cpu", "server_name,hours_back,top,database_name,as_of")]
    [InlineData("get_query_store_top", "server_name,hours_back,top,database_name,as_of")]
    [InlineData("get_collection_health", "server_name")]
    [InlineData("get_server_properties", "server_name")]
    public void ParamContract_MatchesLite(string toolName, string expectedCsv)
    {
        var expected = expectedCsv.Split(',');
        var actual = McpParams(toolName);
        var actualNames = actual.Select(p => p.Name).ToArray();

        Assert.True(actual.Length >= expected.Length,
            $"{toolName} dropped a parameter Lite has: [{string.Join(",", actualNames)}]");

        /* Every Lite name, in Lite's order, with Darling's own additions filtered out. */
        Assert.Equal(expected, actualNames.Where(n => expected.Contains(n, StringComparer.Ordinal)).ToArray());

        /* And nothing Darling adds on top may be required, or a Lite-shaped call could not be made at all. */
        foreach (var extra in actual.Where(p => !expected.Contains(p.Name, StringComparer.Ordinal)))
            Assert.True(extra.Optional, $"{toolName}.{extra.Name} is Darling-only and must be optional");
    }

    /// <summary>
    /// #2235's <c>group_by</c>, pinned as an APPENDED OPTIONAL on <c>get_top_queries_by_cpu</c> and absent from
    /// its siblings.
    ///
    /// <para>It was pinned as the LAST parameter until #2495 appended <c>as_of</c> to both SKUs behind it.
    /// Moving <c>group_by</c> to keep it last would have relocated an already-shipped parameter to preserve a
    /// property (position) that no caller of an MCP tool can observe, so what is pinned now is what the
    /// property was always standing in for: <c>group_by</c> is optional, and it sits AFTER every parameter Lite
    /// has, so a Lite-shaped call never meets it. Absent on <c>get_top_procedures_by_cpu</c> because procedure stats are
    /// ALREADY keyed on the object — the rollup would be a no-op there — and absent on
    /// <c>get_query_store_top</c> because Query Store keys on <c>query_id</c>, which does not fragment the way a
    /// shape hash does, so the same option would imply a grouping that surface cannot perform.</para>
    /// </summary>
    [Fact]
    public void ParamContract_GroupByIsAnAppendedOptionalOnTopQueriesOnly()
    {
        var names = McpParams("get_top_queries_by_cpu").Select(p => p.Name).ToArray();

        Assert.Contains("group_by", names);
        Assert.True(
            McpParams("get_top_queries_by_cpu").Single(p => p.Name == "group_by").Optional,
            "group_by must be optional so Lite-shaped calls still work");
        Assert.True(
            Array.IndexOf(names, "group_by") > Array.IndexOf(names, "min_dop"),
            "group_by must sit after every parameter Lite has, so a Lite-shaped call never meets it");

        foreach (var tool in new[] { "get_top_procedures_by_cpu", "get_query_store_top" })
            Assert.DoesNotContain("group_by", McpParams(tool).Select(p => p.Name));
    }

    [Fact]
    public void ParamContract_ListServers_TakesNoInputParameters()
    {
        /* Only the injected NpgsqlDataSource, which is not [Description]-decorated — so the tool
           advertises an empty input schema (the Dashboard's / Lite's list_servers contract). */
        Assert.Empty(McpParams("list_servers"));
    }

    [Fact]
    public void ParamContract_ServerNameAlwaysOptional_WaitTypeRequired()
    {
        /* server_name is optional everywhere (sole-server auto-select); wait_type is the one required
           data param (get_wait_trend needs an exact wait type — Lite's contract). */
        foreach (var tool in DataToolSurface.Where(t => t != "list_servers"))
        {
            var p = McpParams(tool);
            if (p.Any(x => x.Name == "server_name"))
                Assert.True(p.Single(x => x.Name == "server_name").Optional, $"{tool}.server_name must be optional");
        }

        Assert.False(McpParams("get_wait_trend").Single(x => x.Name == "wait_type").Optional);
    }

    /* ---------------- ungated: read SQL pins ---------------- */

    [Fact]
    public void CpuSql_ReadsBaseTable_DeSkewsSampleTime_WindowsOnCollectionTime()
    {
        var sql = DarlingDataReader.CpuUtilizationSql;
        Assert.Contains("FROM cpu_utilization_stats", sql, StringComparison.Ordinal);   /* base table for the de-skew window fn */
        Assert.Contains("sqlserver_cpu_utilization", sql, StringComparison.Ordinal);
        Assert.Contains("other_process_cpu_utilization", sql, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY server_id, collection_time", sql, StringComparison.Ordinal); /* #1262 per-batch de-skew */
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);        /* window on the reliable clock */
    }

    /// <summary>#2320: the attribution denominator windows on collection_time — the SAME bounds the
    /// rankings use, so numerator and denominator share collection gaps — and aggregates rather than
    /// pulling sample rows.</summary>
    [Fact]
    public void CpuWindowAggregateSql_WindowsOnCollectionTime_BothEdges()
    {
        var sql = DarlingDataReader.CpuWindowAggregateSql;
        Assert.Contains("FROM cpu_utilization_stats", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(sqlserver_cpu_utilization)", sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitStatsSql_AggregatesDeltas_HeaviestFirst()
    {
        var sql = DarlingDataReader.WaitStatsSql;
        Assert.Contains("FROM v_wait_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_wait_time_ms)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_signal_wait_time_ms)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_waiting_tasks)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY wait_type", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(delta_wait_time_ms) DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitTrendSql_PerSecondRate_ForOneType()
    {
        var sql = DarlingDataReader.WaitTrendSql;
        Assert.Contains("FROM v_wait_stats", sql, StringComparison.Ordinal);
        Assert.Contains("wait_type = $2", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("wait_time_ms_per_second", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void MemorySql_LatestSnapshot_CastsNumericToDouble()
    {
        var sql = DarlingDataReader.LatestMemoryStatsSql;
        Assert.Contains("FROM v_memory_stats", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(buffer_pool_mb AS double precision)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);

        var clerks = DarlingDataReader.LatestMemoryClerksSql;
        Assert.Contains("FROM v_memory_clerks", clerks, StringComparison.Ordinal);
        Assert.Contains("clerk_type", clerks, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", clerks, StringComparison.Ordinal);      /* latest snapshot */
    }

    [Fact]
    public void FileIoSql_LatestSnapshot_CarriesRawDeltas()
    {
        var sql = DarlingDataReader.LatestFileIoStatsSql;
        Assert.Contains("FROM v_file_io_stats", sql, StringComparison.Ordinal);
        Assert.Contains("delta_stall_read_ms", sql, StringComparison.Ordinal);
        Assert.Contains("delta_stall_write_ms", sql, StringComparison.Ordinal);
        Assert.Contains("delta_reads", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TempDbSql_WindowedTrend()
    {
        var sql = DarlingDataReader.TempDbTrendSql;
        Assert.Contains("FROM v_tempdb_stats", sql, StringComparison.Ordinal);
        Assert.Contains("version_store_reserved_mb", sql, StringComparison.Ordinal);
        Assert.Contains("total_sessions_using_tempdb", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PerfmonSql_LatestSnapshot_ValueAndDelta()
    {
        var sql = DarlingDataReader.LatestPerfmonStatsSql;
        Assert.Contains("FROM v_perfmon_stats", sql, StringComparison.Ordinal);
        Assert.Contains("cntr_value", sql, StringComparison.Ordinal);
        Assert.Contains("delta_cntr_value", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TopQueriesSql_AggregatesDeltas_OptionalDbFilter_WaitforTrim_BaseTableAggregate_ViewForText()
    {
        var sql = DarlingDataReader.TopQueriesSql;

        /* Two relations, deliberately (#1767). The ranked CTE projects no text and keeps reading the BASE
           table; the latest-text LATERAL reads v_query_stats, which resolves the payload dimension — the
           base table's inline query_text is NULL on every row written since. ("FROM v_query_stats" does not
           contain "FROM query_stats", so these two assertions name two different relations.) */
        var rankedRead = sql.IndexOf("FROM query_stats", StringComparison.Ordinal);
        var lateral = sql.IndexOf("LEFT JOIN LATERAL", StringComparison.Ordinal);
        var textRead = sql.IndexOf("FROM v_query_stats", StringComparison.Ordinal);
        Assert.True(rankedRead >= 0, "the ranked CTE must aggregate the base query_stats table");
        Assert.True(textRead > lateral && lateral > rankedRead,
            "the base-table aggregate comes first; the resolving view is read by the latest-text LATERAL");

        Assert.Contains("SUM(delta_worker_time)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_elapsed_time)", sql, StringComparison.Ordinal);
        /* #2012 stage 2: host_object_name joins the key (INSERT...EXEC callers split; NULL ad-hoc
           hosts still collapse) and pins the LATERAL to the group's own rows. */
        Assert.Contains("GROUP BY database_name, query_hash, host_object_name", sql, StringComparison.Ordinal);
        Assert.Contains("host_object_name IS NOT DISTINCT FROM r.host_object_name", sql, StringComparison.Ordinal);
        Assert.Contains("$5::text IS NULL OR database_name = $5", sql, StringComparison.Ordinal); /* optional db filter */
        Assert.Contains("NOT LIKE 'WAITFOR%'", sql, StringComparison.Ordinal);          /* over-fetch + trim */
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TopProceduresSql_AggregatesDeltas_OptionalDbFilter_ReadsBaseTable()
    {
        var sql = DarlingDataReader.TopProceduresSql;
        Assert.Contains("FROM procedure_stats", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY database_name, schema_name, object_name, object_type", sql, StringComparison.Ordinal);
        Assert.Contains("$5::text IS NULL OR database_name = $5", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_elapsed_time) DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreSql_AveragesIntervals_OptionalDbFilter_ReadsBaseTable()
    {
        var sql = DarlingDataReader.QueryStoreTopSql;
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(CAST(avg_duration_us AS double precision))", sql, StringComparison.Ordinal);
        /* replica_role is a grouping key: an AG's shared Query Store (2022+) would otherwise report
           primary and secondary workload blended into one row. */
        Assert.Contains("GROUP BY database_name, query_id, plan_id, query_hash, replica_role", sql, StringComparison.Ordinal);
        Assert.Contains("$5::text IS NULL OR database_name = $5", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(execution_count)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreSql_DedupsPerIntervalBeforeAggregating()
    {
        /* #1841. query_store_stats rows are CUMULATIVE per-interval snapshots and the collector re-fetches
           the OPEN interval every cycle, so SUM(execution_count) over the raw rows reports 10 + 25 + 40
           for an interval that reached 40, and the AVG(avg_*) columns become an avg-of-avgs weighted by
           re-collection frequency. This surface feeds BOTH the MCP tool and the REST route, so inflated
           numbers would reach an agent's reasoning as readily as the web dashboard. replica_role is in the
           key because the aggregate GROUPs BY it — the dedup must never drop a row the read must return. */
        var sql = DarlingDataReader.QueryStoreTopSql;
        Assert.Contains(
            "PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role",
            sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE rn = 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ServerListSql_EnabledServers_WithLastCollection()
    {
        var sql = DarlingDataReader.ServerListSql;
        Assert.Contains("FROM servers", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE s.is_enabled", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(cl.collection_time)", sql, StringComparison.Ordinal);       /* freshness source */
        Assert.Contains("FROM v_collection_log cl", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void CollectionHealthSql_SevenDayAggregate_SkippedIsHealthy()
    {
        var sql = DarlingDataReader.CollectionHealthSql;
        Assert.Contains("FROM v_collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("collector_name", sql, StringComparison.Ordinal);
        Assert.Contains("status IN ('SUCCESS', 'SKIPPED')", sql, StringComparison.Ordinal); /* skipped counts as healthy */
        Assert.Contains("status = 'PERMISSIONS'", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collector_name", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ServerPropertiesSql_LatestSnapshot_ReadsBaseTable_NoView()
    {
        var sql = DarlingDataReader.LatestServerPropertiesSql;
        /* The store has NO v_server_properties view (not in AllPassthroughViews) — read the base table,
           exactly like the viewer's UTC-offset read. */
        Assert.Contains("FROM server_properties", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_server_properties", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_server_properties", PgSchemaGenerator.AllPassthroughViews.Aggregate("", (a, v) => a + v));
        Assert.Contains("engine_edition", sql, StringComparison.Ordinal);
        Assert.Contains("cores_per_socket", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingDataReader.CpuUtilizationSql))]
    [InlineData(nameof(DarlingDataReader.WaitStatsSql))]
    [InlineData(nameof(DarlingDataReader.WaitTrendSql))]
    [InlineData(nameof(DarlingDataReader.DistinctWaitTypesSql))]
    [InlineData(nameof(DarlingDataReader.LatestMemoryStatsSql))]
    [InlineData(nameof(DarlingDataReader.LatestMemoryClerksSql))]
    [InlineData(nameof(DarlingDataReader.LatestFileIoStatsSql))]
    [InlineData(nameof(DarlingDataReader.TempDbTrendSql))]
    [InlineData(nameof(DarlingDataReader.LatestPerfmonStatsSql))]
    [InlineData(nameof(DarlingDataReader.TopQueriesSql))]
    [InlineData(nameof(DarlingDataReader.TopProceduresSql))]
    [InlineData(nameof(DarlingDataReader.QueryStoreTopSql))]
    [InlineData(nameof(DarlingDataReader.ServerListSql))]
    [InlineData(nameof(DarlingDataReader.CollectionHealthSql))]
    [InlineData(nameof(DarlingDataReader.LatestServerPropertiesSql))]
    [InlineData(nameof(DarlingDataReader.CpuWindowAggregateSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = SqlByName(sqlName);
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);   /* no T-SQL binary CONVERT */
        Assert.DoesNotContain("top (", lower);      /* LIMIT, not TOP */
        Assert.DoesNotContain("isnull(", lower);    /* COALESCE, not ISNULL */
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    private static string SqlByName(string name) => name switch
    {
        nameof(DarlingDataReader.CpuUtilizationSql) => DarlingDataReader.CpuUtilizationSql,
        nameof(DarlingDataReader.WaitStatsSql) => DarlingDataReader.WaitStatsSql,
        nameof(DarlingDataReader.WaitTrendSql) => DarlingDataReader.WaitTrendSql,
        nameof(DarlingDataReader.DistinctWaitTypesSql) => DarlingDataReader.DistinctWaitTypesSql,
        nameof(DarlingDataReader.LatestMemoryStatsSql) => DarlingDataReader.LatestMemoryStatsSql,
        nameof(DarlingDataReader.LatestMemoryClerksSql) => DarlingDataReader.LatestMemoryClerksSql,
        nameof(DarlingDataReader.LatestFileIoStatsSql) => DarlingDataReader.LatestFileIoStatsSql,
        nameof(DarlingDataReader.TempDbTrendSql) => DarlingDataReader.TempDbTrendSql,
        nameof(DarlingDataReader.LatestPerfmonStatsSql) => DarlingDataReader.LatestPerfmonStatsSql,
        nameof(DarlingDataReader.TopQueriesSql) => DarlingDataReader.TopQueriesSql,
        nameof(DarlingDataReader.TopProceduresSql) => DarlingDataReader.TopProceduresSql,
        nameof(DarlingDataReader.QueryStoreTopSql) => DarlingDataReader.QueryStoreTopSql,
        nameof(DarlingDataReader.ServerListSql) => DarlingDataReader.ServerListSql,
        nameof(DarlingDataReader.CollectionHealthSql) => DarlingDataReader.CollectionHealthSql,
        nameof(DarlingDataReader.CpuWindowAggregateSql) => DarlingDataReader.CpuWindowAggregateSql,
        _ => DarlingDataReader.LatestServerPropertiesSql,
    };

    /* ---------------- ungated: read columns exist in the generated collector tables ---------------- */

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTables()
    {
        var cpu = PgSchemaGenerator.CreateTable(CpuUtilizationCollector.Instance);
        Assert.Contains("sqlserver_cpu_utilization", cpu, StringComparison.Ordinal);
        Assert.Contains("other_process_cpu_utilization", cpu, StringComparison.Ordinal);

        var wait = PgSchemaGenerator.CreateTable(WaitStatsCollector.Instance);
        Assert.Contains("delta_wait_time_ms", wait, StringComparison.Ordinal);
        Assert.Contains("delta_signal_wait_time_ms", wait, StringComparison.Ordinal);
        Assert.Contains("delta_waiting_tasks", wait, StringComparison.Ordinal);

        var mem = PgSchemaGenerator.CreateTable(MemoryStatsCollector.Instance);
        Assert.Contains("buffer_pool_mb", mem, StringComparison.Ordinal);
        Assert.Contains("plan_cache_mb", mem, StringComparison.Ordinal);
        Assert.Contains("total_server_memory_mb", mem, StringComparison.Ordinal);

        var clerks = PgSchemaGenerator.CreateTable(MemoryClerksCollector.Instance);
        Assert.Contains("clerk_type", clerks, StringComparison.Ordinal);
        Assert.Contains("memory_mb", clerks, StringComparison.Ordinal);

        var io = PgSchemaGenerator.CreateTable(FileIoStatsCollector.Instance);
        Assert.Contains("delta_stall_read_ms", io, StringComparison.Ordinal);
        Assert.Contains("delta_stall_write_ms", io, StringComparison.Ordinal);
        Assert.Contains("physical_name", io, StringComparison.Ordinal);

        var tempdb = PgSchemaGenerator.CreateTable(TempDbStatsCollector.Instance);
        Assert.Contains("version_store_reserved_mb", tempdb, StringComparison.Ordinal);
        Assert.Contains("total_sessions_using_tempdb", tempdb, StringComparison.Ordinal);

        var perfmon = PgSchemaGenerator.CreateTable(PerfmonStatsCollector.Instance);
        Assert.Contains("counter_name", perfmon, StringComparison.Ordinal);
        Assert.Contains("cntr_value", perfmon, StringComparison.Ordinal);

        var qs = PgSchemaGenerator.CreateTable(QueryStatsCollector.Instance);
        Assert.Contains("delta_worker_time", qs, StringComparison.Ordinal);
        Assert.Contains("delta_elapsed_time", qs, StringComparison.Ordinal);
        Assert.Contains("min_worker_time", qs, StringComparison.Ordinal);
        Assert.Contains("query_plan_hash", qs, StringComparison.Ordinal);

        var proc = PgSchemaGenerator.CreateTable(ProcedureStatsCollector.Instance);
        Assert.Contains("schema_name", proc, StringComparison.Ordinal);
        Assert.Contains("object_name", proc, StringComparison.Ordinal);
        Assert.Contains("delta_worker_time", proc, StringComparison.Ordinal);

        var store = PgSchemaGenerator.CreateTable(QueryStoreCollector.Instance);
        Assert.Contains("avg_duration_us", store, StringComparison.Ordinal);
        Assert.Contains("avg_cpu_time_us", store, StringComparison.Ordinal);
        Assert.Contains("query_id", store, StringComparison.Ordinal);
        Assert.Contains("plan_id", store, StringComparison.Ordinal);

        var props = PgSchemaGenerator.CreateTable(ServerPropertiesCollector.Instance);
        Assert.Equal("server_properties", ServerPropertiesCollector.Instance.TargetTable);
        Assert.Contains("edition", props, StringComparison.Ordinal);
        Assert.Contains("engine_edition", props, StringComparison.Ordinal);
        Assert.Contains("cores_per_socket", props, StringComparison.Ordinal);
    }

    /* ---------------- ungated: advertised MCP schema (the #1074 Gemini contract) ---------------- */

    private static List<ModelContextProtocol.Protocol.Tool> BuildDataToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpDataTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    private static List<string> SchemaViolations(string toolName, JsonElement schema)
    {
        var violations = new List<string>();
        Walk(schema, toolName);
        return violations;

        void Walk(JsonElement element, string path)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                    foreach (var property in element.EnumerateObject())
                    {
                        if (property.NameEquals("type") && property.Value.ValueKind == JsonValueKind.Array)
                            violations.Add($"{path}: union type {property.Value.GetRawText()}");
                        if (property.NameEquals("default"))
                            violations.Add($"{path}: default = {property.Value.GetRawText()}");
                        Walk(property.Value, $"{path}.{property.Name}");
                    }
                    break;
                case JsonValueKind.Array:
                    var i = 0;
                    foreach (var item in element.EnumerateArray())
                        Walk(item, $"{path}[{i++}]");
                    break;
            }
        }
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForAllEighteenDataTools()
    {
        var tools = BuildDataToolSchemas();
        /* Derived from the pinned name list rather than restated. This literal was 15 while the list
           beside it held 16 and the method name said Sixteen -- a count kept by hand in four places
           drifts in whichever one the next person forgets. */
        Assert.Equal(DataToolSurface.Length, tools.Count);

        var violations = tools.SelectMany(t => SchemaViolations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0,
            "Gemini-incompatible schema keywords leaked into tools/list:\n" + string.Join("\n", violations));
    }

    [Fact]
    public void AdvertisedSchema_RequiredParams_MatchLite()
    {
        var tools = BuildDataToolSchemas().ToDictionary(t => t.Name, t => t.InputSchema);

        /* get_wait_trend requires the exact wait_type; every other data tool's params are optional
           (server_name auto-selects, windows have defaults). list_servers has no params at all. */
        Assert.Equal(new[] { "wait_type" }, RequiredOf(tools["get_wait_trend"]));
        foreach (var tool in DataToolSurface.Where(t => t != "get_wait_trend"))
            Assert.Empty(RequiredOf(tools[tool]));
    }

    private static string[] RequiredOf(JsonElement schema) =>
        schema.TryGetProperty("required", out var req) && req.ValueKind == JsonValueKind.Array
            ? req.EnumerateArray().Select(e => e.GetString()!).ToArray()
            : Array.Empty<string>();
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the data-read tools. Registers a sentinel server the
/// worker's way, plants a row (or two, for the delta/aggregate reads) in each collector table, then calls
/// the tool methods and asserts each returns its data-bearing envelope (not an error, not a miss) with the
/// expected server + key fields — and that an unknown server name returns the listing error. Shares the
/// serialized "live-postgres" collection and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpDataToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-data-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task DataTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live data-tools test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            var older = TruncateToSeconds(DateTime.UtcNow).AddMinutes(-3);
            var newer = older.AddMinutes(1);

            await PlantCpuAsync(connection, newer, ct);
            await PlantWaitStatsAsync(connection, older, newer, ct);
            await PlantMemoryStatsAsync(connection, newer, ct);
            await PlantMemoryClerksAsync(connection, newer, ct);
            await PlantFileIoAsync(connection, newer, ct);
            await PlantTempDbAsync(connection, newer, ct);
            await PlantPerfmonAsync(connection, newer, ct);
            await PlantQueryStatsAsync(connection, older, newer, ct);
            await PlantProcedureStatsAsync(connection, older, newer, ct);
            await PlantQueryStoreAsync(connection, newer, ct);
            await PlantServerPropertiesAsync(connection, newer, ct);
            await PlantCollectionLogAsync(connection, newer, ct);

            /* ---- resource metrics ---- */
            AssertServerEnvelope(await DarlingMcpDataTools.GetCpuUtilization(postgres, ServerName), "samples");
            AssertServerEnvelope(await DarlingMcpDataTools.GetWaitStats(postgres, ServerName), "waits");
            AssertServerEnvelope(await DarlingMcpDataTools.GetWaitTrend(postgres, "CXPACKET", ServerName), "trend");
            AssertServerEnvelope(await DarlingMcpDataTools.GetWaitTypes(postgres, ServerName), "wait_types");
            AssertServerEnvelope(await DarlingMcpDataTools.GetMemoryStats(postgres, ServerName), "buffer_pool_mb");
            AssertServerEnvelope(await DarlingMcpDataTools.GetMemoryClerks(postgres, ServerName), "clerks");
            AssertServerEnvelope(await DarlingMcpDataTools.GetFileIoStats(postgres, ServerName), "files");
            AssertServerEnvelope(await DarlingMcpDataTools.GetTempDbTrend(postgres, ServerName), "trend");
            AssertServerEnvelope(await DarlingMcpDataTools.GetPerfmonStats(postgres, ServerName), "counters");

            /* ---- query performance ---- */
            var q = await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName);
            AssertServerEnvelope(q, "queries");
            Assert.Contains("0xE2EDATAHASH", q, StringComparison.Ordinal);   /* the planted query surfaced */
            AssertServerEnvelope(await DarlingMcpDataTools.GetTopProceduresByCpu(postgres, ServerName), "procedures");
            AssertServerEnvelope(await DarlingMcpDataTools.GetQueryStoreTop(postgres, ServerName), "queries");

            /* database_name filter narrows without erroring. */
            AssertServerEnvelope(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 24, 20, Db), "queries");

            /* ---- discovery / health ---- */
            var list = await DarlingMcpDataTools.ListServers(postgres);
            using (var doc = JsonDocument.Parse(list))
                Assert.Contains(ServerName, list, StringComparison.Ordinal);

            AssertServerEnvelope(await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName), "collectors");
            var props = await DarlingMcpDataTools.GetServerProperties(postgres, ServerName);
            AssertServerEnvelope(props, "edition");
            Assert.Contains("Enterprise", props, StringComparison.Ordinal);

            /* ---- server resolution flows through: an unknown name returns the listing error. */
            var unknown = await DarlingMcpDataTools.GetMemoryStats(postgres, "darling-no-such-server");
            Assert.StartsWith("Could not resolve server.", unknown, StringComparison.Ordinal);

            /* ---- an EMPTY store for a tool returns the #1224 miss, not a throw. */
            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("unavailable", StatusOf(await DarlingMcpDataTools.GetCpuUtilization(postgres, ServerName)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static void AssertServerEnvelope(string json, string expectedKey)
    {
        Assert.False(json.StartsWith("Error during", StringComparison.Ordinal), $"tool returned an error: {json}");
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal(ServerName, root.GetProperty("server").GetString());
        Assert.True(json.Contains(expectedKey, StringComparison.Ordinal), $"expected '{expectedKey}' in: {json}");
    }

    private static string StatusOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("status").GetString()!;
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Naive(DateTime.UtcNow));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct, params object?[] values)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var v in values)
            command.Parameters.AddWithValue(v ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantCpuAsync(NpgsqlConnection c, DateTime t, System.Threading.CancellationToken ct) =>
        await Exec(c, @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1,$2,$3,$4,$5,$6,$7)", ct, CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, Naive(t), 40, 10);

    private static async Task PlantWaitStatsAsync(NpgsqlConnection c, DateTime t1, DateTime t2, System.Threading.CancellationToken ct)
    {
        foreach (var (t, dw) in new[] { (t1, 1000L), (t2, 2500L) })
            await Exec(c, @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", ct, CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, "CXPACKET", dw, dw / 10, 50L);
    }

    private static async Task PlantMemoryStatsAsync(NpgsqlConnection c, DateTime t, System.Threading.CancellationToken ct) =>
        await Exec(c, @"INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, plan_cache_mb, system_memory_state, sql_memory_model)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", ct, CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, 65536m, 16384m, 49152m, 49152m, 40000m, 5000m, "Available physical memory is high", "CONVENTIONAL");

    private static async Task PlantMemoryClerksAsync(NpgsqlConnection c, DateTime t, System.Threading.CancellationToken ct) =>
        await Exec(c, @"INSERT INTO memory_clerks (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
VALUES ($1,$2,$3,$4,$5,$6)", ct, CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, "MEMORYCLERK_SQLBUFFERPOOL", 40000m);

    private static async Task PlantFileIoAsync(NpgsqlConnection c, DateTime t, System.Threading.CancellationToken ct) =>
        await Exec(c, @"INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)", ct, CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, Db, "StackOverflow.mdf", "ROWS", "D:\\data\\so.mdf", 100000m, 500L, 200L, 4096000L, 1024000L, 2500L, 400L);

    private static async Task PlantTempDbAsync(NpgsqlConnection c, DateTime t, System.Threading.CancellationToken ct) =>
        await Exec(c, @"INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, user_object_reserved_mb, internal_object_reserved_mb, version_store_reserved_mb, total_reserved_mb, unallocated_mb, total_sessions_using_tempdb, top_session_id, top_session_tempdb_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", ct, CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, 100m, 50m, 25m, 175m, 825m, 7L, 55, 12m);

    private static async Task PlantPerfmonAsync(NpgsqlConnection c, DateTime t, System.Threading.CancellationToken ct) =>
        await Exec(c, @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", ct, CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, "SQLServer:SQL Statistics", "Batch Requests/sec", "", 123456L, 789L);

    private static async Task PlantQueryStatsAsync(NpgsqlConnection c, DateTime t1, DateTime t2, System.Threading.CancellationToken ct)
    {
        foreach (var (t, dc) in new[] { (t1, 10L), (t2, 30L) })
            await Exec(c, @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills, min_dop, max_dop, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)",
                ct, CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, Db, "0xE2EDATAHASH", "0xPLANHASH", "0xSQLH", "0xPLANH",
                "SELECT * FROM Posts", dc, dc * 1000L, dc * 2000L, dc * 500L, 0L, dc * 5L, dc * 100L, 0L, 1, 4, 800L, 1500L, 1600L, 3000L);
    }

    private static async Task PlantProcedureStatsAsync(NpgsqlConnection c, DateTime t1, DateTime t2, System.Threading.CancellationToken ct)
    {
        foreach (var (t, dc) in new[] { (t1, 5L), (t2, 15L) })
            await Exec(c, @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_spills, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)",
                ct, CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, Db, "dbo", "usp_GetPosts", "SQL_STORED_PROCEDURE", "0xPROCH", "0xPROCPLANH",
                dc, dc * 1000L, dc * 2000L, dc * 400L, 0L, dc * 4L, 0L, 900L, 1200L, 1800L, 2400L);
    }

    private static async Task PlantQueryStoreAsync(NpgsqlConnection c, DateTime t, System.Threading.CancellationToken ct) =>
        await Exec(c, @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, min_dop, max_dop, last_execution_time)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)",
            ct, CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, Db, 42L, 7L, "0xE2EDATAHASH", "0xPLANHASH", "SELECT * FROM Users",
            100L, 5000L, 4000L, 800L, 0L, 40L, 250L, 1L, 4L, Naive(t));

    private static async Task PlantServerPropertiesAsync(NpgsqlConnection c, DateTime t, System.Threading.CancellationToken ct) =>
        await Exec(c, @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket, is_hadr_enabled, is_clustered)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
            ct, CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, "Enterprise Edition (64-bit)", "15.0.4322.2", "RTM", 3, 16, 4, 65536L, 2, 8, false, false);

    private static async Task PlantCollectionLogAsync(NpgsqlConnection c, DateTime t, System.Threading.CancellationToken ct) =>
        await Exec(c, @"INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", ct, CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, "wait_stats", "SUCCESS", 42, 100);

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, bool keepServer = false)
    {
        var tables = new[]
        {
            "cpu_utilization_stats", "wait_stats", "memory_stats", "memory_clerks", "file_io_stats",
            "tempdb_stats", "perfmon_stats", "query_stats", "procedure_stats", "query_store_stats",
            "server_properties", "collection_log",
        };
        var sql = string.Join(" ", tables.Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"));
        if (!keepServer)
            sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
