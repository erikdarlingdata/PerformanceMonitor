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
        "Analyzes query_hash's latest plan. No plan: not_collected if the engine can't collect query_stats, else unavailable. " +
        "Per statement: warnings; missing_indexes labelled impact_basis, with create_statement — the optimizer's suggested CREATE INDEX for this statement: " +
        "corroboration for a statement already measured slow, never a diagnosis; every row carries the fixed caveat (regression risk for other plans, write cost); " +
        "parameters; memory_grant; top_operators by operators_ranked_by, with operators_returned / total_operators / truncated. " +
        "actual_* are null, not 0, without runtime stats. " +
        "<<GUIDE>> " +
        "Analyzes an execution plan from the plan cache by query_hash. Use after get_top_queries_by_cpu to understand why a query is expensive. Returns warnings, missing indexes (column lists, the optimizer's statement-scoped impact estimate labelled impact_basis, and create_statement — the optimizer's suggested CREATE INDEX for this statement: corroboration for a statement already measured slow, never a diagnosis, and every row carries the fixed caveat — the estimate is per-statement, an index is a per-table commitment with write cost and regression risk for other plans, so test it), parameters, memory grants, and top_operators — a stated cut of the operators_cap most expensive operators per statement, with operators_returned / total_operators / truncated, ranked by operators_ranked_by: measured actual_elapsed_ms when the plan has runtime statistics, otherwise the optimizer's cost_percent estimate.")]
    public static async Task<string> AnalyzeQueryPlan(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The query_hash value from get_top_queries_by_cpu.")] string query_hash,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var xml = await dataService.GetCachedQueryPlanAsync(resolved.ServerId, query_hash);
            if (string.IsNullOrEmpty(xml))
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_stats")
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No plan found for query_hash '{query_hash}'. The query may have been evicted from the plan cache since the last collection.");

            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.ServerName, "query_stats", query_hash);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("analyze_query_plan", ex);
        }
    }

    [McpServerTool(Name = "analyze_procedure_plan"), Description(
        "Analyzes an execution plan from procedure stats by plan_handle. " +
        "Use after get_top_procedures_by_cpu to understand why a procedure is expensive. " +
        "Returns warnings, missing indexes (column lists, the optimizer's statement-scoped impact estimate labelled impact_basis, and create_statement — the optimizer's suggested CREATE INDEX for this statement: corroboration for a statement already measured slow, never a diagnosis, and every row carries the fixed caveat — the estimate is per-statement, an index is a per-table commitment with write cost and regression risk for other plans, so test it), parameters, memory grants, and top_operators — a stated cut of the operators_cap most expensive operators per statement, with operators_returned / total_operators / truncated, ranked by operators_ranked_by: measured actual_elapsed_ms when the plan has runtime statistics, otherwise the optimizer's cost_percent estimate.")]
    public static async Task<string> AnalyzeProcedurePlan(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The plan_handle value from get_top_procedures_by_cpu.")] string plan_handle,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var xml = await dataService.GetCachedProcedurePlanAsync(resolved.ServerId, plan_handle);
            if (string.IsNullOrEmpty(xml))
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "procedure_stats")
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No plan found for plan_handle '{plan_handle}'. The procedure may have been evicted from the plan cache since the last collection.");

            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.ServerName, "procedure_stats", plan_handle);
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
        /* Injected for the engine-capability miss alone (#2532): unlike its three siblings this tool
           fetches the plan LIVE from the monitored instance rather than from the store, so the store
           handle is not otherwise needed here — but the capability question is the same question, and
           Darling's twin asks it, so the two SKUs must not answer differently. */
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The database_name from get_query_store_top.")] string database_name,
        [Description("The plan_id from get_query_store_top.")] long plan_id,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            /* Find the server connection to build a connection string */
            var server = serverManager.GetEnabledServers().Find(s =>
            {
                var storageName = RemoteCollectorService.GetServerNameForStorage(s);
                return string.Equals(storageName, resolved.ServerName, StringComparison.OrdinalIgnoreCase);
            });

            if (server == null)
                return $"Could not find connection details for server '{resolved.ServerName}'.";

            var connectionString = serverManager.CredentialResolver.GetConnectionString(server);
            var xml = await LocalDataService.FetchQueryStorePlanAsync(connectionString, database_name, plan_id);

            if (string.IsNullOrEmpty(xml))
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_store")
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No plan found for plan_id {plan_id} in database '{database_name}'. Query Store may not be enabled or the plan may have been purged.");

            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.ServerName, "query_store", $"{database_name}:{plan_id}");
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("analyze_query_store_plan", ex);
        }
    }

    [McpServerTool(Name = "analyze_plan_xml"), Description(
        "Analyzes raw showplan XML you provide (not a stored plan). Per statement: warnings; missing_indexes " +
        "labelled impact_basis, with create_statement — the optimizer's suggested CREATE INDEX for this statement: " +
        "corroboration for a statement already measured slow, never a diagnosis; every row carries the fixed caveat: " +
        "regression risk for other plans and write cost, so test it; parameters; memory_grant; top_operators, ranked " +
        "by operators_ranked_by, with operators_returned / total_operators / truncated. Malformed or non-plan XML " +
        "doesn't error: statement_count 0. Blank plan_xml is refused. " +
        "<<GUIDE>> " +
        "Analyzes raw showplan XML directly. Use when you have plan XML from any source " +
        "(clipboard, file, another tool). " +
        "Returns warnings, missing indexes (column lists, the optimizer's statement-scoped impact estimate labelled impact_basis, and create_statement — the optimizer's suggested CREATE INDEX for this statement: corroboration for a statement already measured slow, never a diagnosis, and every row carries the fixed caveat — the estimate is per-statement, an index is a per-table commitment with write cost and regression risk for other plans, so test it), parameters, memory grants, and top_operators — a stated cut of the operators_cap most expensive operators per statement, with operators_returned / total_operators / truncated, ranked by operators_ranked_by: measured actual_elapsed_ms when the plan has runtime statistics, otherwise the optimizer's cost_percent estimate.")]
    public static string AnalyzePlanXml(
        [Description("Raw showplan XML content.")] string plan_xml)
    {
        if (string.IsNullOrWhiteSpace(plan_xml))
            return McpHelpers.Refusal("plan_xml", "No plan XML provided.");

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
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var xml = await dataService.GetCachedQueryPlanAsync(resolved.ServerId, query_hash);
            if (string.IsNullOrEmpty(xml))
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_stats")
                    ?? McpHelpers.Status("unavailable", $"No plan found for query_hash '{query_hash}'.");

            return McpHelpers.Truncate(xml, 512_000) ?? "No plan XML available.";
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_plan_xml", ex);
        }
    }

}
