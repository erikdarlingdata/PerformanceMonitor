/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
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
        "Analyzes query_hash's latest plan. No plan: not_collected if the engine can't collect query_stats, else unavailable. " +
        "Per statement: warnings; missing_indexes labelled impact_basis, with create_statement — the optimizer's suggested CREATE INDEX for this statement: " +
        "corroboration for a statement already measured slow, never a diagnosis; every row carries the fixed caveat (regression risk for other plans, write cost); " +
        "parameters; memory_grant; top_operators by operators_ranked_by, with operators_returned / total_operators / truncated. " +
        "actual_* are null, not 0, without runtime stats. " +
        "<<GUIDE>> " +
        "Analyzes a stored execution plan captured from query stats by query_hash. Use after get_top_queries_by_cpu to understand why a query is expensive. Returns warnings, missing indexes (column lists, the optimizer's statement-scoped impact estimate labelled impact_basis, and create_statement — the optimizer's suggested CREATE INDEX for this statement: corroboration for a statement already measured slow, never a diagnosis, and every row carries the fixed caveat — the estimate is per-statement, an index is a per-table commitment with write cost and regression risk for other plans, so test it), parameters, memory grants, and top_operators — a stated cut of the operators_cap most expensive operators per statement, with operators_returned / total_operators / truncated, ranked by operators_ranked_by: measured actual_elapsed_ms when the plan has runtime statistics, otherwise the optimizer's cost_percent estimate.")]
    public static async Task<string> AnalyzeQueryPlan(
        NpgsqlDataSource postgres,
        [Description("The query_hash value from get_top_queries_by_cpu.")] string query_hash,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Optional database name to disambiguate the same query_hash across databases. Omit for the most recently captured plan.")] string? database_name = null,
        AnalyzerConfig? analyzerConfig = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var xml = await DarlingStoredPlanReader.GetQueryStatsPlanXmlByHashAsync(
                postgres, resolved.ServerId, query_hash, database_name, cancellationToken);
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats", cancellationToken)
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No stored plan found for query_hash '{query_hash}'{DbSuffix(database_name)}. The plan collector may not have captured a plan for this query, or the row has aged out of the store.");

            // #4530/#4597: one store read per call so rule 38 can see the server's edition/MAXDOP, and the
            // Server Context card can see cost threshold/max memory/database. Non-fatal (null on a miss or a
            // read failure) — the analyzer then falls back to its Info branch.
            var metadata = await DarlingServerMetadataReader.ReadAsync(postgres, resolved.ServerId, database_name, cancellationToken);
            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.ServerName, "query_stats", query_hash, analyzerConfig, metadata, cancellationToken);
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
        "Analyzes a stored execution plan captured from procedure stats by sql_handle. " +
        "Use after get_top_procedures_by_cpu to understand why a procedure is expensive. " +
        "Returns warnings, missing indexes (column lists, the optimizer's statement-scoped impact estimate labelled impact_basis, and create_statement — the optimizer's suggested CREATE INDEX for this statement: corroboration for a statement already measured slow, never a diagnosis, and every row carries the fixed caveat — the estimate is per-statement, an index is a per-table commitment with write cost and regression risk for other plans, so test it), parameters, memory grants, and top_operators — a stated cut of the operators_cap most expensive operators per statement, with operators_returned / total_operators / truncated, ranked by operators_ranked_by: measured actual_elapsed_ms when the plan has runtime statistics, otherwise the optimizer's cost_percent estimate.")]
    public static async Task<string> AnalyzeProcedurePlan(
        NpgsqlDataSource postgres,
        [Description("The sql_handle value from get_top_procedures_by_cpu.")] string sql_handle,
        [Description("Server name or display name.")] string? server_name = null,
        AnalyzerConfig? analyzerConfig = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var xml = await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(
                postgres, resolved.ServerId, sql_handle, cancellationToken);
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "procedure_stats", cancellationToken)
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No stored plan found for sql_handle '{sql_handle}'. The plan collector may not have captured a plan for this procedure, or the row has aged out of the store.");

            // #4530: one store read per call so rule 38 can see the server's edition/MAXDOP. No database
            // name is available for a procedure looked up by sql_handle alone, so Database stays null (#4597).
            var metadata = await DarlingServerMetadataReader.ReadAsync(postgres, resolved.ServerId, cancellationToken: cancellationToken);
            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.ServerName, "procedure_stats", sql_handle, analyzerConfig, metadata, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("analyze_procedure_plan", ex);
        }
    }

    [McpServerTool(Name = "analyze_query_store_plan"), Description(
        "Analyzes a stored Query Store execution plan by database name and query ID. " +
        "Use after get_query_store_top to understand why a query is expensive. " +
        "Returns warnings, missing indexes (column lists, the optimizer's statement-scoped impact estimate labelled impact_basis, and create_statement — the optimizer's suggested CREATE INDEX for this statement: corroboration for a statement already measured slow, never a diagnosis, and every row carries the fixed caveat — the estimate is per-statement, an index is a per-table commitment with write cost and regression risk for other plans, so test it), parameters, memory grants, and top_operators — a stated cut of the operators_cap most expensive operators per statement, with operators_returned / total_operators / truncated, ranked by operators_ranked_by: measured actual_elapsed_ms when the plan has runtime statistics, otherwise the optimizer's cost_percent estimate.")]
    public static async Task<string> AnalyzeQueryStorePlan(
        NpgsqlDataSource postgres,
        [Description("The database_name from get_query_store_top.")] string database_name,
        [Description("The query_id from get_query_store_top.")] long query_id,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Optional plan_id to pin one specific compiled plan. Omit for the most recently captured plan for the query.")] long? plan_id = null,
        AnalyzerConfig? analyzerConfig = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var read = await DarlingStoredPlanReader.ResolveQueryStorePlanAsync(
                postgres, resolved.ServerId, database_name, query_id, plan_id, cancellationToken: cancellationToken);
            var xml = read?.PlanXml;
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store", cancellationToken)
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No stored Query Store plan found for query_id {query_id} in database '{database_name}'{PlanSuffix(plan_id)}. The plan may not have been collected yet, the database's Query Store may be off or its capture mode may exclude this query, or the plan has been purged.");

            /* #5257: the identifier always names the plan that was analysed. Without a plan_id the read picks the
               newest plan that has content, which is not necessarily the newest plan the query ran under (a plan
               not yet fetched is skipped), so the caller needs the plan_id to tell. */
            var identifier = $"{database_name}:{query_id}:{read!.PlanId}";
            // #4530/#4597: one store read per call so rule 38 can see the server's edition/MAXDOP, and the
            // Server Context card can see cost threshold/max memory/database.
            var metadata = await DarlingServerMetadataReader.ReadAsync(postgres, resolved.ServerId, database_name, cancellationToken);
            return McpPlanAnalysisFormatter.BuildAnalysisResult(xml, resolved.ServerName, "query_store", identifier, analyzerConfig, metadata, cancellationToken);
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
        AnalyzerConfig? analyzerConfig = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(plan_xml))
            return McpHelpers.Refusal("plan_xml", "No plan XML provided.");

        try
        {
            return McpPlanAnalysisFormatter.BuildAnalysisResult(plan_xml, null, "xml", null, analyzerConfig, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
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
        [Description("Optional database name to disambiguate the same query_hash across databases. Omit for the most recently captured plan.")] string? database_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var xml = await DarlingStoredPlanReader.GetQueryStatsPlanXmlByHashAsync(
                postgres, resolved.ServerId, query_hash, database_name, cancellationToken);
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", $"No stored plan found for query_hash '{query_hash}'{DbSuffix(database_name)}.");

            return McpHelpers.Truncate(xml, 512_000) ?? "No plan XML available.";
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_plan_xml", ex);
        }
    }

    [McpServerTool(Name = "get_query_store_plan_xml"), Description(
        "Returns the raw stored Query Store showplan XML for database_name + query_id (optional plan_id). Use after get_query_store_top. Truncated at 500KB.")]
    public static async Task<string> GetQueryStorePlanXml(
        NpgsqlDataSource postgres,
        [Description("The database_name from get_query_store_top.")] string database_name,
        [Description("The query_id from get_query_store_top.")] long query_id,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Optional plan_id to pin one compiled plan. Omit for the most recently captured plan.")] long? plan_id = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var xml = await DarlingStoredPlanReader.GetQueryStorePlanTextAsync(
                postgres, resolved.ServerId, database_name, query_id, plan_id, cancellationToken: cancellationToken);
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store", cancellationToken)
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No stored Query Store plan found for query_id {query_id} in database '{database_name}'{PlanSuffix(plan_id)}.");

            return McpHelpers.Truncate(xml, 512_000) ?? "No plan XML available.";
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_store_plan_xml", ex);
        }
    }

    [McpServerTool(Name = "get_procedure_plan_xml"), Description(
        "Returns the raw stored showplan XML for a procedure identified by sql_handle. Use after get_top_procedures_by_cpu. Truncated at 500KB.")]
    public static async Task<string> GetProcedurePlanXml(
        NpgsqlDataSource postgres,
        [Description("The sql_handle value from get_top_procedures_by_cpu.")] string sql_handle,
        [Description("Server name or display name.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsSqlHandle(sql_handle))
            return McpHelpers.Refusal("sql_handle", "Expected a hex sql_handle such as 0x0300..., as get_top_procedures_by_cpu returned it.");

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var xml = await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(
                postgres, resolved.ServerId, sql_handle, cancellationToken);
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "procedure_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", $"No stored plan found for sql_handle '{sql_handle}'.");

            return McpHelpers.Truncate(xml, 512_000) ?? "No plan XML available.";
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_procedure_plan_xml", ex);
        }
    }

    /// <summary>The ISO 8601 round-trip forms get_active_queries emits: 7 fractional digits at most, with or without a
    /// trailing Z or an offset. Anything else (a bare date, "3/4/2026") is refused, never guessed at.</summary>
    private static readonly string[] CollectionTimeFormats =
    {
        "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm:sszzz",
        "yyyy-MM-dd'T'HH:mm:ss.FFFFFFF", "yyyy-MM-dd'T'HH:mm:ss.FFFFFFF'Z'", "yyyy-MM-dd'T'HH:mm:ss.FFFFFFFzzz",
    };

    private static readonly System.Text.RegularExpressions.Regex SqlHandleShape =
        new("^0x[0-9A-Fa-f]+$", System.Text.RegularExpressions.RegexOptions.CultureInvariant);

    /// <summary>A sql_handle is hex with a 0x prefix. get_procedure_plan_xml refuses anything else; analyze_procedure_plan
    /// predates this check and still takes the text as given.</summary>
    internal static bool IsSqlHandle(string? text) => text != null && SqlHandleShape.IsMatch(text);

    internal static bool TryParseCollectionTime(string? text, out DateTime utc) =>
        DateTime.TryParseExact(text, CollectionTimeFormats, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out utc);

    [McpServerTool(Name = "get_active_query_plan_xml"), Description(
        "Returns the raw showplan XML captured for one get_active_queries row (collection_time + session_id); live=true gives the live plan. Truncated at 500KB.")]
    public static async Task<string> GetActiveQueryPlanXml(
        NpgsqlDataSource postgres,
        [Description("The row's collection_time, exactly as get_active_queries returned it.")] string collection_time,
        [Description("The row's session_id.")] int session_id,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("The row's request_id; omit for 0.")] int request_id = 0,
        [Description("True for the live (actual) plan; false for the estimated plan.")] bool live = false,
        CancellationToken cancellationToken = default)
    {
        /* Parsed as an instant in UTC with every fractional digit kept: the store compares collection_time
           for equality, so a value rounded to the second would match nothing. */
        if (!TryParseCollectionTime(collection_time, out var collectionTimeUtc))
            return McpHelpers.Refusal("collection_time", "Expected the collection_time exactly as get_active_queries returned it (ISO 8601, UTC).");

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var xml = await DarlingStoredPlanReader.GetQuerySnapshotPlanXmlAsync(
                postgres, resolved.ServerId, collectionTimeUtc, session_id, request_id, live, cancellationToken);
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_snapshots", cancellationToken)
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No stored {(live ? "live " : "")}plan found for session_id {session_id} (request_id {request_id}) at collection_time '{collection_time}'.");

            return McpHelpers.Truncate(xml, 512_000) ?? "No plan XML available.";
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_active_query_plan_xml", ex);
        }
    }

    private static readonly JsonSerializerOptions ReproJson = new() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };

    [McpServerTool(Name = "get_query_repro_script"), Description(
        "Builds a paste-ready T-SQL repro script from a stored query's text and plan. kind: query_hash, query_store or active_snapshot, with that kind's key. Store-only.")]
    public static async Task<string> GetQueryReproScript(
        NpgsqlDataSource postgres,
        [Description("query_hash, query_store or active_snapshot.")] string kind,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("The row's database_name (required for query_store).")] string? database_name = null,
        [Description("The row's query_hash (kind query_hash).")] string? query_hash = null,
        [Description("The row's query_id (kind query_store).")] long? query_id = null,
        [Description("The row's plan_id (kind query_store); omit for the newest plan.")] long? plan_id = null,
        [Description("The row's collection_time (kind active_snapshot).")] string? collection_time = null,
        [Description("The row's session_id (kind active_snapshot).")] int? session_id = null,
        [Description("The row's request_id (kind active_snapshot); omit for 0.")] int request_id = 0,
        CancellationToken cancellationToken = default)
    {
        /* Every refusal comes before the store is touched. */
        DateTime snapshotTime = default;
        switch (kind)
        {
            case DarlingReproScript.KindQueryHash:
                if (string.IsNullOrEmpty(query_hash)) return McpHelpers.Refusal("query_hash", "kind query_hash needs the row's query_hash.");
                break;
            case DarlingReproScript.KindQueryStore:
                if (string.IsNullOrEmpty(database_name)) return McpHelpers.Refusal("database_name", "kind query_store needs the row's database_name.");
                if (query_id is null) return McpHelpers.Refusal("query_id", "kind query_store needs the row's query_id.");
                break;
            case DarlingReproScript.KindActiveSnapshot:
                if (string.IsNullOrEmpty(collection_time)) return McpHelpers.Refusal("collection_time", "kind active_snapshot needs the row's collection_time.");
                if (session_id is null) return McpHelpers.Refusal("session_id", "kind active_snapshot needs the row's session_id.");
                if (!TryParseCollectionTime(collection_time, out snapshotTime))
                    return McpHelpers.Refusal("collection_time", "Expected the collection_time exactly as get_active_queries returned it (ISO 8601, UTC).");
                break;
            case "procedure":
                return McpHelpers.Refusal("kind", "No query text is kept for a procedure, so there is no repro script. Use kind query_hash, query_store or active_snapshot.");
            default:
                return McpHelpers.Refusal("kind", "Expected query_hash, query_store or active_snapshot.");
        }

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            string? text, db, planXml, isolation = null, collector;
            switch (kind)
            {
                case DarlingReproScript.KindQueryHash:
                    collector = "query_stats";
                    db = database_name;
                    text = await DarlingReproScript.ReadQueryStatsTextAsync(postgres, resolved.ServerId, query_hash!, database_name, cancellationToken);
                    planXml = text == null ? null : await DarlingStoredPlanReader.GetQueryStatsPlanXmlByHashAsync(
                        postgres, resolved.ServerId, query_hash!, database_name, cancellationToken);
                    break;
                case DarlingReproScript.KindQueryStore:
                    collector = "query_store";
                    db = database_name;
                    text = await DarlingReproScript.ReadQueryStoreTextAsync(postgres, resolved.ServerId, database_name!, query_id!.Value, cancellationToken);
                    planXml = text == null ? null : await DarlingStoredPlanReader.GetQueryStorePlanTextAsync(
                        postgres, resolved.ServerId, database_name!, query_id.Value, plan_id, cancellationToken: cancellationToken);
                    break;
                default:
                    collector = "query_snapshots";
                    (text, isolation, db) = await DarlingReproScript.ReadSnapshotTextAsync(
                        postgres, resolved.ServerId, snapshotTime, session_id!.Value, request_id, cancellationToken);
                    planXml = text == null ? null : await DarlingStoredPlanReader.GetQuerySnapshotPlanXmlAsync(
                        postgres, resolved.ServerId, snapshotTime, session_id.Value, request_id, live: false, cancellationToken);
                    break;
            }

            var script = DarlingReproScript.Build(kind, text, db, planXml, isolation);
            if (script == null)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, collector, cancellationToken)
                    ?? McpHelpers.Status("unavailable", $"No stored query text found for this {kind} key, so no repro script can be built.");

            return JsonSerializer.Serialize(new
            {
                kind,
                /* Only the kind's own key fields are echoed, never stray caller params. */
                query_hash = kind == DarlingReproScript.KindQueryHash ? query_hash : null,
                database_name = kind != DarlingReproScript.KindActiveSnapshot ? database_name : null,
                query_id = kind == DarlingReproScript.KindQueryStore ? query_id : null,
                plan_id = kind == DarlingReproScript.KindQueryStore ? plan_id : null,
                collection_time = kind == DarlingReproScript.KindActiveSnapshot ? collection_time : null,
                session_id = kind == DarlingReproScript.KindActiveSnapshot ? session_id : null,
                request_id = kind == DarlingReproScript.KindActiveSnapshot ? (int?)request_id : null,
                plan_found = !string.IsNullOrEmpty(planXml),
                script,
            }, ReproJson);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_repro_script", ex);
        }
    }

    /* The two values side takes on get_blocking_plan_xml (#5236). */
    private const string BlockedSide = "blocked";
    private const string BlockingSide = "blocking";

    [McpServerTool(Name = "get_blocking_plan_xml"), Description(
        "Returns the raw showplan XML stored with one get_blocking row (event_time + both spids); side=blocking gives the blocker's plan. Truncated at 500KB.")]
    public static async Task<string> GetBlockingPlanXml(
        NpgsqlDataSource postgres,
        [Description("The row's event_time, exactly as get_blocking returned it.")] string event_time,
        [Description("The row's blocked_spid.")] int blocked_spid,
        [Description("The row's blocking_spid.")] int blocking_spid,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("The row's blocked_ecid; omit for 0.")] int blocked_ecid = 0,
        [Description("The row's blocking_ecid; omit for 0.")] int blocking_ecid = 0,
        [Description("blocked (default) for the blocked session's plan; blocking for the blocker's.")] string? side = BlockedSide,
        [Description("The row's database_name; omit to match any database.")] string? database_name = null,
        CancellationToken cancellationToken = default)
    {
        /* #5236: an absent side is the default, and only a value that is neither side is refused — the plan of the
           wrong process is worse than none, so it is never guessed. */
        var blockingSide = string.Equals(side, BlockingSide, StringComparison.OrdinalIgnoreCase);
        if (!blockingSide && !string.IsNullOrEmpty(side) && !string.Equals(side, BlockedSide, StringComparison.OrdinalIgnoreCase))
            return McpHelpers.Refusal("side", "Expected blocked or blocking.");

        /* The same exact-instant parse as get_active_query_plan_xml: event_time is the XE timestamp as get_blocking
           printed it ("o", naive UTC, microseconds kept), and the store compares it for equality. */
        if (!TryParseCollectionTime(event_time, out var eventTimeUtc))
            return McpHelpers.Refusal("event_time", "Expected the event_time exactly as get_blocking returned it (ISO 8601, UTC).");

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var xml = await DarlingStoredPlanReader.GetBlockingPlanXmlAsync(
                postgres, resolved.ServerId, eventTimeUtc, blocked_spid, blocked_ecid, blocking_spid, blocking_ecid, blockingSide, database_name, cancellationToken);
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "blocked_process_report", cancellationToken)
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No stored {(blockingSide ? BlockingSide : BlockedSide)} plan found for blocked_spid {blocked_spid} and blocking_spid {blocking_spid} at event_time '{event_time}'{DbSuffix(database_name)}. " +
                        "The collector captures these plans best-effort, only while the statement is still in the plan cache, so many reports have none.");

            return McpHelpers.Truncate(xml, 512_000) ?? "No plan XML available.";
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_blocking_plan_xml", ex);
        }
    }

    [McpServerTool(Name = "get_deadlock_plan_xml"), Description(
        "Returns the raw showplan XML stored for the victim of one get_deadlocks row (collection_time + deadlock_time). Truncated at 500KB.")]
    public static async Task<string> GetDeadlockPlanXml(
        NpgsqlDataSource postgres,
        [Description("The row's collection_time, exactly as get_deadlocks returned it.")] string collection_time,
        [Description("The row's deadlock_time, exactly as get_deadlocks returned it.")] string deadlock_time,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("The row's victim_process_id; picks the right deadlock when two share both times.")] string? victim_process_id = null,
        [Description("The row's database_name; omit to match any database.")] string? database_name = null,
        CancellationToken cancellationToken = default)
    {
        /* Both stamps are the row's own and compared for equality, so each is parsed as an exact instant, never rounded. */
        if (!TryParseCollectionTime(collection_time, out var collectionTimeUtc))
            return McpHelpers.Refusal("collection_time", "Expected the collection_time exactly as get_deadlocks returned it (ISO 8601, UTC).");
        if (!TryParseCollectionTime(deadlock_time, out var deadlockTimeUtc))
            return McpHelpers.Refusal("deadlock_time", "Expected the deadlock_time exactly as get_deadlocks returned it (ISO 8601, UTC).");

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var read = await DarlingStoredPlanReader.GetDeadlockVictimPlanXmlAsync(
                postgres, resolved.ServerId, collectionTimeUtc, deadlockTimeUtc, victim_process_id, database_name, cancellationToken);

            /* Deadlocks with the same stamps that name more than one victim (whether or not each captured a plan), and no victim
               named: the plan of the wrong deadlock is worse than none (as with side above), so none is returned and the caller is
               told what picks one. */
            if (read.Ambiguous)
                return McpHelpers.Refusal("victim_process_id",
                    "More than one deadlock shares this collection_time and deadlock_time and they name different victims. Pass the row's victim_process_id to pick one.");

            var xml = read.PlanXml;
            if (string.IsNullOrEmpty(xml))
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "deadlocks", cancellationToken)
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No stored victim plan found for the deadlock at deadlock_time '{deadlock_time}' (collection_time '{collection_time}'){DbSuffix(database_name)}. " +
                        "The collector captures the victim's plan best-effort, only while the statement is still in the plan cache, so many deadlocks have none.");

            return McpHelpers.Truncate(xml, 512_000) ?? "No plan XML available.";
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_deadlock_plan_xml", ex);
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
