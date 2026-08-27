/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The five plan-analysis MCP tools — the SAME tool surface the Dashboard and Lite expose
/// (analyze_query_plan, analyze_procedure_plan, analyze_query_store_plan, analyze_plan_xml,
/// get_plan_xml), served over Darling's Postgres store. Each tool body mirrors the Dashboard's
/// <c>McpPlanTools</c> field-for-field (same tool names, same required parameters, the #1224 miss
/// vocabulary via <see cref="McpHelpers.Status"/>, and the SHARED
/// <see cref="McpPlanAnalysisFormatter"/> projection both apps already call) so an MCP client sees one
/// consistent product across all three SKUs.
///
/// <para>
/// THE SEAM: where the Dashboard fetches the plan XML from its SQL-Server store (query_hash → binary
/// CONVERT, most-recent by last_execution_time) and Lite from DuckDB, Darling reads the plan text the
/// collectors persisted into Postgres via <see cref="DarlingStoredPlanReader"/> — a STORED-plan read
/// (no live monitored-server hit), keyed by the same object identity the Dashboard's tools use, with
/// server resolution through the Postgres <c>servers</c> registry (<see cref="DarlingServerResolver"/>)
/// instead of the Dashboard's ServerManager. Two tools additionally accept the finer store key the
/// viewer's grids carry (database_name on the query-stats read, plan_id on the query-store read) as an
/// OPTIONAL refinement; omitting it reproduces the Dashboard's coarse-key behavior exactly, so a client
/// relying on the Dashboard contract works identically here.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPlanTools
{
    [McpServerTool(Name = "analyze_query_plan"), Description(
        "Analyzes a stored execution plan captured from query stats by query_hash. " +
        "Use after get_top_queries_by_cpu to understand why a query is expensive. " +
        "Returns warnings, missing indexes, parameters, memory grants, and top operators.")]
    public static async Task<string> AnalyzeQueryPlan(
        NpgsqlDataSource postgres,
        [Description("The query_hash value from get_top_queries_by_cpu.")] string query_hash,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Optional database name to disambiguate the same query_hash across databases. Omit for the most recently captured plan.")] string? database_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var xml = await DarlingStoredPlanReader.GetQueryStatsPlanXmlByHashAsync(
                postgres, resolved.ServerId, query_hash, database_name);
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats")
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No stored plan found for query_hash '{query_hash}'{DbSuffix(database_name)}. The plan collector may not have captured a plan for this query, or the row has aged out of the store.");

            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.ServerName, "query_stats", query_hash);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("analyze_query_plan", ex);
        }
    }

    [McpServerTool(Name = "analyze_procedure_plan"), Description(
        "Analyzes a stored execution plan captured from procedure stats by sql_handle. " +
        "Use after get_top_procedures_by_cpu to understand why a procedure is expensive. " +
        "Returns warnings, missing indexes, parameters, memory grants, and top operators.")]
    public static async Task<string> AnalyzeProcedurePlan(
        NpgsqlDataSource postgres,
        [Description("The sql_handle value from get_top_procedures_by_cpu.")] string sql_handle,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var xml = await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(
                postgres, resolved.ServerId, sql_handle);
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "procedure_stats")
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No stored plan found for sql_handle '{sql_handle}'. The plan collector may not have captured a plan for this procedure, or the row has aged out of the store.");

            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.ServerName, "procedure_stats", sql_handle);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("analyze_procedure_plan", ex);
        }
    }

    [McpServerTool(Name = "analyze_query_store_plan"), Description(
        "Analyzes a stored Query Store execution plan by database name and query ID. " +
        "Use after get_query_store_top to understand why a query is expensive. " +
        "Returns warnings, missing indexes, parameters, memory grants, and top operators.")]
    public static async Task<string> AnalyzeQueryStorePlan(
        NpgsqlDataSource postgres,
        [Description("The database_name from get_query_store_top.")] string database_name,
        [Description("The query_id from get_query_store_top.")] long query_id,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Optional plan_id to pin one specific compiled plan. Omit for the most recently captured plan for the query.")] long? plan_id = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var xml = await DarlingStoredPlanReader.GetQueryStorePlanTextAsync(
                postgres, resolved.ServerId, database_name, query_id, plan_id);
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store")
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No stored Query Store plan found for query_id {query_id} in database '{database_name}'{PlanSuffix(plan_id)}. Query Store plan capture may be disabled for this database, or the plan has been purged.");

            var identifier = plan_id is null ? $"{database_name}:{query_id}" : $"{database_name}:{query_id}:{plan_id}";
            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.ServerName, "query_store", identifier);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("analyze_query_store_plan", ex);
        }
    }

    [McpServerTool(Name = "analyze_plan_xml"), Description(
        "Analyzes raw showplan XML directly. Use when you have plan XML from any source " +
        "(clipboard, file, another tool). Returns warnings, missing indexes, parameters, " +
        "memory grants, and top operators.")]
    public static string AnalyzePlanXml(
        [Description("Raw showplan XML content.")] string plan_xml)
    {
        if (string.IsNullOrWhiteSpace(plan_xml))
            return "No plan XML provided.";

        try
        {
            return McpPlanAnalysisFormatter.BuildAnalysisResult(plan_xml, null, "xml", null);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("analyze_plan_xml", ex);
        }
    }

    [McpServerTool(Name = "get_plan_xml"), Description(
        "Returns the raw stored showplan XML for a query identified by query_hash. " +
        "Use when you need to inspect plan details not captured in the structured analysis. " +
        "Truncated at 500KB.")]
    public static async Task<string> GetPlanXml(
        NpgsqlDataSource postgres,
        [Description("The query_hash value from get_top_queries_by_cpu.")] string query_hash,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Optional database name to disambiguate the same query_hash across databases. Omit for the most recently captured plan.")] string? database_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var xml = await DarlingStoredPlanReader.GetQueryStatsPlanXmlByHashAsync(
                postgres, resolved.ServerId, query_hash, database_name);
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats")
                    ?? McpHelpers.Status("unavailable", $"No stored plan found for query_hash '{query_hash}'{DbSuffix(database_name)}.");

            return McpHelpers.Truncate(xml, 512_000) ?? "No plan XML available.";
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_plan_xml", ex);
        }
    }

    /// <summary>" in database 'X'" when a database was supplied, else empty — keeps the miss message honest
    /// about the coarse-vs-fine key the caller actually used.</summary>
    private static string DbSuffix(string? databaseName) =>
        string.IsNullOrEmpty(databaseName) ? "" : $" in database '{databaseName}'";

    /// <summary>" (plan_id N)" when a plan_id was supplied, else empty.</summary>
    private static string PlanSuffix(long? planId) =>
        planId is null ? "" : $" (plan_id {planId})";
}
