using System;
using System.ComponentModel;
using System.Threading;
using ModelContextProtocol.Server;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

using PerformanceMonitorLite;
namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpPlanTools
{
    /// <summary>
    /// #4348: said when the statement filter withheld a plan whole, so the analysis is skipped instead of reading the
    /// placeholder as XML and reporting a parse error. Worded once for the four analysis tools.
    /// </summary>
    internal const string WithheldPlanMessage = "This plan was withheld by the statement filter (#4348), so it was not analysed.";

    /// <summary>
    /// Where the <c>get_plan_xml</c> tool cuts a plan for transport (500 KB of characters, the "Truncated at 500KB" in its
    /// description). #4348: the same number goes to the plan read, so the statement filter stops walking a very large plan
    /// once nothing before the cut can change, and <see cref="McpHelpers.Truncate"/> then cuts the FILTERED plan, never the
    /// raw one (a cut plan is not well-formed XML, and a half-cut statement is no longer one the filter can name).
    /// </summary>
    internal const int PlanXmlOutputChars = 512_000;

    /// <summary>
    /// #4348, Layer 1: the one place a stored plan is read for an MCP tool. Every plan read in this file goes through
    /// one of the three methods below, so the plan a tool holds is the FILTERED plan whatever it does next (return it,
    /// cut it, analyse it); nothing else in this file calls the plan reads on <see cref="LocalDataService"/> (a source
    /// scan pins that). The reads on <see cref="LocalDataService"/> stay raw, because Lite's own windows show the user's
    /// own data. Mirrors Darling's <c>DarlingStoredPlanReader</c>. <paramref name="maxOutputChars"/> is where the caller
    /// will cut the plan, so the filter stops there.
    /// </summary>
    internal static async Task<string?> ReadQueryStatsPlanAsync(
        LocalDataService dataService, int serverId, string queryHash, int maxOutputChars = int.MaxValue) =>
        FilterStoredPlan(await dataService.GetCachedQueryPlanAsync(serverId, queryHash), maxOutputChars);

    /// <summary>The procedure twin of <see cref="ReadQueryStatsPlanAsync"/> (see there).</summary>
    internal static async Task<string?> ReadProcedurePlanAsync(
        LocalDataService dataService, int serverId, string planHandle, int maxOutputChars = int.MaxValue) =>
        FilterStoredPlan(await dataService.GetCachedProcedurePlanAsync(serverId, planHandle), maxOutputChars);

    /// <summary>The live Query Store fetch of <see cref="ReadQueryStatsPlanAsync"/>'s family (see there).</summary>
    internal static async Task<string?> ReadQueryStorePlanAsync(
        string connectionString, string databaseName, long planId, int maxOutputChars = int.MaxValue) =>
        FilterStoredPlan(await LocalDataService.FetchQueryStorePlanAsync(connectionString, databaseName, planId), maxOutputChars);

    /// <summary>
    /// The filter at the seam: the statement filter's XML judge over a plan read. A missing plan stays missing (the
    /// callers' "no plan" answers still apply), and a plan the judge cannot finish comes back as the placeholder, never
    /// as the raw text.
    /// </summary>
    internal static string? FilterStoredPlan(string? xml, int maxOutputChars = int.MaxValue) =>
        string.IsNullOrEmpty(xml) ? xml : SensitiveStatements.Xml(xml, maxOutputChars) ?? SensitiveStatements.PlaceholderText;

    /// <summary>
    /// #4348: the one way every plan-analysis tool here turns plan XML into its result. The statement filter runs on
    /// the XML first, because the analysis lifts parameter values and statement text out of it into fields the JSON
    /// sweep cannot pair with their statement. A plan the filter withholds whole is answered with
    /// <see cref="WithheldPlanMessage"/>. Kept out of <c>McpPlanAnalysisFormatter.BuildAnalysisResult</c>, which the
    /// desktop Dashboard also calls.
    /// </summary>
    internal static string AnalyzeFilteredPlan(
        string xml,
        string? serverName,
        string source,
        string? identifier,
        AnalyzerConfig? analyzerConfig,
        ServerMetadata? metadata,
        CancellationToken cancellationToken)
    {
        xml = SensitiveStatements.Xml(xml) ?? SensitiveStatements.PlaceholderText;
        if (xml == SensitiveStatements.PlaceholderText)
            return McpHelpers.Status("unavailable", WithheldPlanMessage);
        return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, serverName, source, identifier, analyzerConfig, metadata, cancellationToken);
    }

    /// <summary>
    /// What every Lite plan read answers when it finds no plan text. Lite never captures plans:
    /// <c>CollectorContext.CapturePlanXml</c> defaults to false and Lite never sets it (Darling does), so
    /// <c>query_stats.query_plan_xml</c> is NULL on every row Lite collects. "No plan found" is therefore the
    /// permanent state of a Lite store, not a fact about the monitored server's plan cache, and the answer says
    /// so instead of blaming an eviction. Worded once, so the tools cannot drift apart and the tests can hold the
    /// descriptions to the same way out.
    /// </summary>
    internal const string PlansNotKeptMessage =
        "Lite does not keep query plans. Pass the plan XML to analyze_plan_xml, or use Darling, which keeps them.";

    /// <summary>
    /// The miss answer for a plan read over a column Lite never fills: <c>not_collected</c> with
    /// <see cref="PlansNotKeptMessage"/>. It holds no capability call on purpose. Each tool asks
    /// <c>McpEngineCapability.NotCollectedStatusAsync</c> itself, with its own literal collector, and falls back
    /// to this only when the engine CAN collect the read: the engine's own answer (it names a permanent gap in
    /// the engine's own words) stays first, and the capability-wiring source guards can map every gate call to
    /// the tool it sits in. Called only after the read found nothing, so a store that does hold plan text is
    /// still served its plan.
    /// </summary>
    internal static string PlansNotKept() => McpHelpers.Status("not_collected", PlansNotKeptMessage);

    [McpServerTool(Name = "analyze_query_plan"), Description(
        "Analyzes query_hash's latest plan. No plan: not_collected if the engine can't collect query_stats, else unavailable. " +
        "Per statement: warnings; missing_indexes labelled impact_basis, with create_statement — the optimizer's suggested CREATE INDEX for this statement: " +
        "corroboration for a statement already measured slow, never a diagnosis; every row carries the fixed caveat (regression risk for other plans, write cost); " +
        "parameters; memory_grant; top_operators by operators_ranked_by, with operators_returned / total_operators / truncated. " +
        "actual_* are null, not 0, without runtime stats. " +
        "<<GUIDE>> " +
        "Analyzes an execution plan from the plan cache by query_hash. Use after get_top_queries_by_cpu to understand why a query is expensive. Returns warnings, missing indexes (column lists, the optimizer's statement-scoped impact estimate labelled impact_basis, and create_statement — the optimizer's suggested CREATE INDEX for this statement: corroboration for a statement already measured slow, never a diagnosis, and every row carries the fixed caveat — the estimate is per-statement, an index is a per-table commitment with write cost and regression risk for other plans, so test it), parameters, memory grants, and top_operators — a stated cut of the operators_cap most expensive operators per statement, with operators_returned / total_operators / truncated, ranked by operators_ranked_by: measured actual_elapsed_ms when the plan has runtime statistics, otherwise the optimizer's cost_percent estimate. " +
        "Lite never captures plans, so on Lite a miss is always not_collected and never a sign the plan was evicted from the cache: pass the plan XML to analyze_plan_xml, or use Darling, which keeps plans.")]
    public static async Task<string> AnalyzeQueryPlan(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The query_hash value from get_top_queries_by_cpu.")] string query_hash,
        [Description("Server name or display name.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var xml = await ReadQueryStatsPlanAsync(dataService, resolved.ServerId, query_hash);
            /* Lite never fills query_stats.query_plan_xml, so a miss here is not "evicted from the plan cache":
               it is "Lite does not keep plans". The engine-capability answer still comes first. */
            if (string.IsNullOrEmpty(xml))
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_stats")
                    ?? PlansNotKept();

            // #4530: one store read per call so rule 38 can see the server's edition/MAXDOP.
            var metadata = await dataService.GetServerMetadataForPlanAnalysisAsync(resolved.ServerId);
            return AnalyzeFilteredPlan(xml, resolved.ServerName, "query_stats", query_hash, App.AnalyzerConfig, metadata, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("analyze_query_plan", ex);
        }
    }

    [McpServerTool(Name = "analyze_procedure_plan"), Description(
        "Analyzes a procedure's stored plan by sql_handle (Darling) or plan_handle (Lite). No plan: not_collected if the engine can't collect procedure_stats, else unavailable. " +
        "Per statement: warnings; missing_indexes labelled impact_basis, with create_statement — the optimizer's suggested CREATE INDEX for this statement: " +
        "corroboration for a statement already measured slow, never a diagnosis; every row carries the fixed caveat (regression risk for other plans, write cost); " +
        "parameters; memory_grant; top_operators by operators_ranked_by, with operators_returned / total_operators / truncated. " +
        "<<GUIDE>> " +
        "Analyzes an execution plan from procedure stats by plan_handle. " +
        "Use after get_top_procedures_by_cpu to understand why a procedure is expensive. " +
        "Returns warnings, missing indexes (column lists, the optimizer's statement-scoped impact estimate labelled impact_basis, and create_statement — the optimizer's suggested CREATE INDEX for this statement: corroboration for a statement already measured slow, never a diagnosis, and every row carries the fixed caveat — the estimate is per-statement, an index is a per-table commitment with write cost and regression risk for other plans, so test it), parameters, memory grants, and top_operators — a stated cut of the operators_cap most expensive operators per statement, with operators_returned / total_operators / truncated, ranked by operators_ranked_by: measured actual_elapsed_ms when the plan has runtime statistics, otherwise the optimizer's cost_percent estimate. " +
        "Lite never captures plans, so on Lite a miss is always not_collected and never a sign the plan was evicted from the cache: pass the plan XML to analyze_plan_xml, or use Darling, which keeps plans.")]
    public static async Task<string> AnalyzeProcedurePlan(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The plan_handle value from get_top_procedures_by_cpu.")] string plan_handle,
        [Description("Server name or display name.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var xml = await ReadProcedurePlanAsync(dataService, resolved.ServerId, plan_handle);
            /* GetCachedProcedurePlanAsync reads the same query_stats.query_plan_xml (matched on plan_handle), which
               Lite never fills, so this miss is "Lite does not keep plans" too. The engine answer stays first. */
            if (string.IsNullOrEmpty(xml))
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "procedure_stats")
                    ?? PlansNotKept();

            // #4530: one store read per call so rule 38 can see the server's edition/MAXDOP.
            var metadata = await dataService.GetServerMetadataForPlanAnalysisAsync(resolved.ServerId);
            return AnalyzeFilteredPlan(xml, resolved.ServerName, "procedure_stats", plan_handle, App.AnalyzerConfig, metadata, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
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
        [Description("Server name or display name.")] string? server_name = null,
        CancellationToken cancellationToken = default)
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

            /* Deliberately NOT PlansNotKept: this tool reads no stored plan column. It fetches the plan from
               Query Store on the monitored instance, so "no plan found" here is a statement about that instance
               (Query Store off, plan purged) and stays true. */
            var xml = await ReadQueryStorePlanAsync(connectionString, database_name, plan_id);

            if (string.IsNullOrEmpty(xml))
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_store")
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No plan found for plan_id {plan_id} in database '{database_name}'. Query Store may not be enabled or the plan may have been purged.");

            // #4530: one store read per call so rule 38 can see the server's edition/MAXDOP.
            var metadata = await dataService.GetServerMetadataForPlanAnalysisAsync(resolved.ServerId);
            return AnalyzeFilteredPlan(xml, resolved.ServerName, "query_store", $"{database_name}:{plan_id}", App.AnalyzerConfig, metadata, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
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
        [Description("Raw showplan XML content.")] string plan_xml,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(plan_xml))
            return McpHelpers.Refusal("plan_xml", "No plan XML provided.");

        try
        {
            // #4348: the caller's XML has no stored-plan read to filter it, so AnalyzeFilteredPlan runs the statement
            // filter before the analysis lifts parameter values and statement text out of it into fields the JSON sweep
            // cannot pair.
            return AnalyzeFilteredPlan(plan_xml, null, "xml", null, App.AnalyzerConfig, null, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("analyze_plan_xml", ex);
        }
    }

    [McpServerTool(Name = "get_plan_xml"), Description(
        "Returns the raw showplan XML for a query identified by query_hash. " +
        "Lite does not keep plans, so a miss is not_collected; use analyze_plan_xml. " +
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
            var xml = await ReadQueryStatsPlanAsync(dataService, resolved.ServerId, query_hash, PlanXmlOutputChars);
            if (string.IsNullOrEmpty(xml))
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_stats")
                    ?? PlansNotKept();

            return McpHelpers.Truncate(xml, PlanXmlOutputChars) ?? "No plan XML available.";
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_plan_xml", ex);
        }
    }

}
