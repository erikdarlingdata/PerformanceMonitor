using System;
using System.ComponentModel;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpPlanTools
{
    [McpServerTool(Name = "analyze_query_plan"), Description(
        "Analyzes an execution plan from query stats (plan cache) by query_hash. " +
        "Use after get_top_queries_by_cpu to understand why a query is expensive. " +
        "Returns warnings, missing indexes, parameters, memory grants, and top operators.")]
    public static async Task<string> AnalyzeQueryPlan(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("The query_hash value from get_top_queries_by_cpu.")] string query_hash,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var xml = await resolved.Value.Service.GetPlanXmlByQueryHashAsync(query_hash);
            if (string.IsNullOrEmpty(xml))
                return McpHelpers.Status(
                    "unavailable",
                    $"No plan found for query_hash '{query_hash}'. The query may have been evicted from the plan cache since the last collection.");

            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.Value.ServerName, "query_stats", query_hash);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("analyze_query_plan", ex);
        }
    }

    [McpServerTool(Name = "analyze_procedure_plan"), Description(
        "Analyzes an execution plan from procedure stats by sql_handle. " +
        "Use after get_top_procedures_by_cpu to understand why a procedure is expensive. " +
        "Returns warnings, missing indexes, parameters, memory grants, and top operators.")]
    public static async Task<string> AnalyzeProcedurePlan(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("The sql_handle value from get_top_procedures_by_cpu.")] string sql_handle,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var xml = await resolved.Value.Service.GetProcedurePlanXmlBySqlHandleAsync(sql_handle);
            if (string.IsNullOrEmpty(xml))
                return McpHelpers.Status(
                    "unavailable",
                    $"No plan found for sql_handle '{sql_handle}'. The procedure may have been evicted from the plan cache since the last collection.");

            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.Value.ServerName, "procedure_stats", sql_handle);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("analyze_procedure_plan", ex);
        }
    }

    [McpServerTool(Name = "analyze_query_store_plan"), Description(
        "Analyzes an execution plan from Query Store by database name and query ID. " +
        "Use after get_query_store_top to understand why a query is expensive. " +
        "Returns warnings, missing indexes, parameters, memory grants, and top operators.")]
    public static async Task<string> AnalyzeQueryStorePlan(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("The database_name from get_query_store_top.")] string database_name,
        [Description("The query_id from get_query_store_top.")] long query_id,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var xml = await resolved.Value.Service.GetQueryStorePlanXmlAsync(database_name, query_id);
            if (string.IsNullOrEmpty(xml))
                return McpHelpers.Status(
                    "unavailable",
                    $"No plan found for query_id {query_id} in database '{database_name}'. Query Store may not be enabled or the query may have been purged.");

            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.Value.ServerName, "query_store", $"{database_name}:{query_id}");
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
        "Returns the raw showplan XML for a query identified by query_hash. " +
        "Use when you need to inspect plan details not captured in the structured analysis. " +
        "Truncated at 500KB.")]
    public static async Task<string> GetPlanXml(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("The query_hash value from get_top_queries_by_cpu.")] string query_hash,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var xml = await resolved.Value.Service.GetPlanXmlByQueryHashAsync(query_hash);
            if (string.IsNullOrEmpty(xml))
                return McpHelpers.Status("unavailable", $"No plan found for query_hash '{query_hash}'.");

            return McpHelpers.Truncate(xml, 512_000) ?? "No plan XML available.";
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_plan_xml", ex);
        }
    }

}
