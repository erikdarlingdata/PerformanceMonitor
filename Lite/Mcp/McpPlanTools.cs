using System.ComponentModel;
using ModelContextProtocol.Server;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpPlanTools
{
    [McpServerTool(Name = "analyze_query_plan"), Description(
        "Analyzes an execution plan from the plan cache by query_hash. " +
        "Use after get_top_queries_by_cpu to understand why a query is expensive. " +
        "Returns warnings, missing indexes, parameters, memory grants, and top operators.")]
    public static async Task<string> AnalyzeQueryPlan(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The query_hash value from get_top_queries_by_cpu.")] string query_hash,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var xml = await dataService.GetCachedQueryPlanAsync(resolved.Value.ServerId, query_hash);
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
        "Analyzes an execution plan from procedure stats by plan_handle. " +
        "Use after get_top_procedures_by_cpu to understand why a procedure is expensive. " +
        "Returns warnings, missing indexes, parameters, memory grants, and top operators.")]
    public static async Task<string> AnalyzeProcedurePlan(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The plan_handle value from get_top_procedures_by_cpu.")] string plan_handle,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var xml = await dataService.GetCachedProcedurePlanAsync(resolved.Value.ServerId, plan_handle);
            if (string.IsNullOrEmpty(xml))
                return McpHelpers.Status(
                    "unavailable",
                    $"No plan found for plan_handle '{plan_handle}'. The procedure may have been evicted from the plan cache since the last collection.");

            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.Value.ServerName, "procedure_stats", plan_handle);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("analyze_procedure_plan", ex);
        }
    }

    [McpServerTool(Name = "analyze_query_store_plan"), Description(
        "Analyzes an execution plan from Query Store by database name and plan ID. " +
        "Fetches the plan on-demand from the monitored SQL Server instance. " +
        "Use after get_query_store_top to understand why a query is expensive.")]
    public static async Task<string> AnalyzeQueryStorePlan(
        ServerManager serverManager,
        [Description("The database_name from get_query_store_top.")] string database_name,
        [Description("The plan_id from get_query_store_top.")] long plan_id,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            /* Find the server connection to build a connection string */
            var server = serverManager.GetEnabledServers().Find(s =>
            {
                var storageName = RemoteCollectorService.GetServerNameForStorage(s);
                return string.Equals(storageName, resolved.Value.ServerName, StringComparison.OrdinalIgnoreCase);
            });

            if (server == null)
                return $"Could not find connection details for server '{resolved.Value.ServerName}'.";

            var connectionString = serverManager.CredentialResolver.GetConnectionString(server);
            var xml = await LocalDataService.FetchQueryStorePlanAsync(connectionString, database_name, plan_id);

            if (string.IsNullOrEmpty(xml))
                return McpHelpers.Status(
                    "unavailable",
                    $"No plan found for plan_id {plan_id} in database '{database_name}'. Query Store may not be enabled or the plan may have been purged.");

            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.Value.ServerName, "query_store", $"{database_name}:{plan_id}");
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
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The query_hash value from get_top_queries_by_cpu.")] string query_hash,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var xml = await dataService.GetCachedQueryPlanAsync(resolved.Value.ServerId, query_hash);
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
