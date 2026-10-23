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
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #5632 step 1b: <c>get_forced_plans</c>, whether each Query Store force works. The collector (<c>query_store_forced_plans</c>, every
/// five minutes) reads <c>sys.query_store_plan</c> itself, so a forced plan that never runs, or that the engine ignores, is still
/// stored. This read joins that capture to the per-plan runs in <c>query_store_stats</c> (<see cref="DarlingForcedPlanStateReader"/>).
/// Store-only and read-only: it never connects to a monitored server.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpForcedPlanTools
{
    /// <summary>The most forces one answer lists, worst state first; <c>truncated</c> marks a cut.</summary>
    internal const int MaxListed = 200;

    /// <summary>The most other plans one force lists (the busiest first).</summary>
    internal const int MaxOtherPlans = 5;

    internal static string StateName(ForcedPlanState state) => state switch
    {
        ForcedPlanState.Failing => "FAILING",
        ForcedPlanState.Ignored => "IGNORED",
        ForcedPlanState.Removed => "REMOVED",
        ForcedPlanState.NotExercised => "NOT_EXERCISED",
        ForcedPlanState.UnknownPlanNotCollected => "UNKNOWN_PLAN_NOT_COLLECTED",
        _ => "WORKING",
    };

    /// <summary>Worst first, so a cut keeps the forces that need a person.</summary>
    internal static int StateRank(ForcedPlanState state) => state switch
    {
        ForcedPlanState.Failing => 0,
        ForcedPlanState.Ignored => 1,
        ForcedPlanState.UnknownPlanNotCollected => 2,
        ForcedPlanState.NotExercised => 3,
        ForcedPlanState.Removed => 4,
        _ => 5,
    };

    private static string? Format(DateTime? t) => t?.ToString("o", CultureInfo.InvariantCulture);

    internal const string FirstSeenNote =
        "first_seen is the first capture in the window that shows the force. Query Store keeps no force time, so the real force can be up to one collector pass earlier (5 minutes by default). " +
        "Runs are counted from first_seen, and only from the Query Store intervals the collector stored.";

    [McpServerTool(Name = "get_forced_plans"), Description(
        "Whether each Query Store force works: one entry per plan forced in its database's newest capture in the window, with a state and the evidence. " +
        "States: FAILING (the engine counts a force failure or names a reason), IGNORED (another plan of the query ran after the force was first seen), WORKING (the forced plan ran, or a variant of it did, and no other plan did), " +
        "NOT_EXERCISED (no plan of the query ran after the force was first seen), UNKNOWN_PLAN_NOT_COLLECTED (a later plan's XML not stored), REMOVED (forced in an earlier capture in the window, absent from the database's newest). <<GUIDE>> " +
        "UNKNOWN_PLAN_NOT_COLLECTED: a later plan of the query ran after the force, but its plan XML is not in the store yet (the Query Store collector has not fetched it, or more later plans ran than one read looks up), so it is not known whether it is a variant of the force (WORKING) or another plan (IGNORED); unknown_plans and unknown_reason say which, and the state resolves on its own once the XML is stored. " +
        "Reads the forced-plan collector (every 5 minutes) and the stored Query Store runs; never a monitored server. A database is judged by its own newest capture. " +
        "first_seen is the first capture that shows the force, so the real force can be up to one pass (5 minutes by default) earlier. " +
        "Evidence per force: forced_plan executions and last run since first_seen, the other plans' executions and last run since first_seen, failure count and reason. " +
        "variant_plans lists plans of the query whose stored plan (read from the plan XML the Query Store collector stored, never from the monitored server) carries UsePlan=\"1\" and no plan guide name: SQL Server recompiled the force under a new plan id, so their runs count as the forced plan running, not as another plan " +
        "(an OPTION (USE PLAN ...) hint in the query's own text also sets UsePlan, which is rare on a forced query). " +
        "plan_guide names a plan guide that covers the forced query (name, enabled, scope), found from the query's object (an OBJECT-scoped guide) or a guide name in the stored XML of a later plan of the query; " +
        "it sits beside the state and never changes it: a FAILING force with a guide may still be running fine. " +
        "An empty answer says why: no capture in the window (the collector has not run, or Query Store is off for every database), or no forced plans in the newest captures. " +
        "forced_plans lists the worst states first, at most limit (default and most 200) per call: when has_more is true, call again with offset = next_offset. " +
        "query_id and plan_id narrow the read itself, so a force is found even on a server with more forces than one read holds (2000); with either, matched_forces, total_forces and state_counts count only the forces that match. Without them total_forces is capped at 2000. " +
        "plan_forcing_type AUTO means SQL Server forced the plan itself (automatic plan correction), so force and unforce churn on it is normal; MANUAL is a person or a tool.")]
    public static async Task<string> GetForcedPlans(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of captures ending at as_of (default 24).")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        [Description("Only the force(s) of this Query Store query_id. Omit for all.")] long? query_id = null,
        [Description("Only the force of this Query Store plan_id (it names one plan per database). Omit for all.")] long? plan_id = null,
        [Description("Maximum forces to list per call, worst state first. Default and most 200.")] int limit = MaxListed,
        [Description("Forces to skip before listing (a page start). Default 0; use next_offset when has_more is true.")] int offset = 0,
        CancellationToken cancellationToken = default)
        => await GetForcedPlans(postgres, server_name, hours_back, as_of, DatabaseFilter.One(database_name), query_id, plan_id, limit, offset, cancellationToken);

    /// <summary>The read over a LIST of databases: the MCP tool passes <c>DatabaseFilter.One</c>, the web route the repeated keys.</summary>
    internal static Task<string> GetForcedPlans(
        NpgsqlDataSource postgres, string? server_name, int hours_back, string? as_of, DatabaseFilter databases, CancellationToken cancellationToken) =>
        GetForcedPlans(postgres, server_name, hours_back, as_of, databases, null, null, MaxListed, 0, cancellationToken);

    /// <summary>
    /// #6560: the same read with a page and a plan filter. <paramref name="queryId"/> and <paramref name="planId"/> narrow the READ (before the reader's
    /// <see cref="DarlingForcedPlanStateReader.MaxForces"/> cap, so a force past it is still found), so with either set <c>total_forces</c> and
    /// <c>state_counts</c> describe the matching forces; without them <c>total_forces</c> is the count under that cap. <paramref name="limit"/> (1 to <see cref="MaxListed"/>) and <paramref name="offset"/> page the ordered list.
    /// </summary>
    internal static async Task<string> GetForcedPlans(
        NpgsqlDataSource postgres, string? server_name, int hours_back, string? as_of, DatabaseFilter databases,
        long? queryId, long? planId, int limit, int offset, CancellationToken cancellationToken)
    {
        if (limit < 1 || limit > MaxListed)
            return McpHelpers.Refusal("limit", $"Invalid limit value '{limit}'. Must be 1-{MaxListed}.");
        if (offset < 0)
            return McpHelpers.Refusal("offset", $"Invalid offset value '{offset}'. Must be 0 or more.");

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        var start = windowEnd.AddHours(-hours_back);

        try
        {
            var read = await DarlingForcedPlanStateReader.ReadAsync(
                postgres, resolved.ServerId, databases, queryId, start, windowEnd, null, McpCommandDeadlines.ReadSeconds, planId, cancellationToken);
            ReadScope.NoteSource(ReadScope.SourceRaw);

            var scope = databases.Describe();
            var where = scope is null ? "" : $" for {(databases.Names.Count == 1 ? "database '" + scope + "'" : scope)}";
            if (read.Captures.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "query_store_forced_plans", cancellationToken)
                    ?? McpHelpers.Status("empty",
                        $"No forced-plan capture{where} in the {hours_back}-hour window ending {Format(windowEnd)}. Either the query_store_forced_plans collector has not run in it, " +
                        "or Query Store is off, in error or a readable secondary in every database (such a database writes no capture).");
            }

            if (read.Plans.Count == 0)
            {
                return JsonSerializer.Serialize(new Dictionary<string, object?>
                {
                    ["server"] = resolved.DisplayName,
                    ["status"] = "empty",
                    ["message"] = queryId is not null || planId is not null
                        ? $"No force{where} in the newest captures matches query_id/plan_id. Captured databases are listed."
                        : $"No plan is forced{where} in the newest capture of each database in the window. Captured databases are listed.",
                    ["as_of"] = Format(windowEnd),
                    ["hours_back"] = hours_back,
                    ["captures"] = read.Captures.Select(CaptureNode),
                }, McpHelpers.JsonOptions);
            }

            var ordered = read.Plans
                .OrderBy(p => StateRank(p.State)).ThenBy(p => p.DatabaseName, StringComparer.Ordinal).ThenBy(p => p.PlanId).ThenBy(p => p.QueryId)
                .ToList();
            var narrowed = queryId is not null || planId is not null;
            /* The reader applied query_id and plan_id before its cap, so the list is already the matching forces. */
            var matched = ordered;
            var listed = matched.Skip(offset).Take(limit).ToList();
            var hasMore = offset + listed.Count < matched.Count;
            string? pageNote = null;
            if (hasMore)
                pageNote = $"Listed {listed.Count} of {matched.Count} forces from offset {offset}. Call again with offset {offset + listed.Count} for the next page, or filter by database_name, query_id or plan_id.";
            else if (narrowed && matched.Count == 0)
                pageNote = "No force in the newest captures matches query_id/plan_id.";
            else if (offset >= matched.Count && matched.Count > 0)
                pageNote = $"offset {offset} is past the {matched.Count} matching forces.";
            return JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["server"] = resolved.DisplayName,
                ["as_of"] = Format(windowEnd),
                ["hours_back"] = hours_back,
                ["total_forces"] = ordered.Count,
                ["state_counts"] = ordered.GroupBy(p => StateName(p.State)).OrderBy(g => g.Key, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.Count()),
                ["matched_forces"] = matched.Count,
                ["offset"] = offset,
                ["limit"] = limit,
                ["forced_plans"] = listed.Select(ForceNode),
                ["truncated"] = hasMore,
                ["has_more"] = hasMore,
                ["next_offset"] = hasMore ? offset + listed.Count : null,
                ["page_note"] = pageNote,
                ["captures"] = read.Captures.Select(CaptureNode),
                ["stats_complete"] = read.StatsComplete,
                ["note"] = FirstSeenNote + (read.StatsComplete
                    ? ""
                    : " The stored Query Store runs start after the oldest first_seen here (or are absent), so NOT_EXERCISED and WORKING say what the store holds, not what the server ran."),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_forced_plans", ex);
        }
    }

    [McpServerTool(Name = "get_query_store_hints"), Description(
        "Which Query Store hints are in place: one entry per hint in its database's newest capture in the window, with the query, the hint text, where it came from, whether the engine records a failure, and the query's text. <<GUIDE>> " +
        "A Query Store hint (sys.sp_query_store_set_hints, SQL Server 2022 and later, Azure SQL) changes a query's plan as a forced plan does, and nothing else Darling reads shows one set or cleared. " +
        "state: FAILING (the engine names a failure reason or counts a failure, last_query_hint_failure_reason and query_hint_failure_count say which) or NO_FAILURE (the engine records no failure; the view has no applied column, so this is the absence of a failure, not proof the hint took effect). " +
        "Reads the query_store_hints collector (every 5 minutes), never a monitored server. A database is judged by its own newest capture, and a hint absent from it was cleared and is not listed. " +
        "first_seen is the first capture in the window that shows the hint, so the real set time can be up to one pass (5 minutes by default) earlier, and a hint set before the window reads as first seen at the window's first capture. " +
        "query_text is the query's stored text cut to 1000 characters (query_text_truncated says when it was cut); it is null when the Query Store collector has not stored that query. " +
        "Status words: empty (databases were captured and none has a hint), unavailable (no capture in the window: the collector has not run in it, or Query Store is off, in error or a readable secondary in every database), " +
        "not_collected (the server is older than SQL Server 2022, which has no hints view, or is not a SQL Server). " +
        "query_store_hints lists FAILING hints first, at most limit (default and most 200) per call: when has_more is true, call again with offset = next_offset. query_id narrows the read itself.")]
    public static async Task<string> GetQueryStoreHints(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of captures ending at as_of (default 24).")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        [Description("Only the hints of this Query Store query_id. Omit for all.")] long? query_id = null,
        [Description("Maximum hints to list per call, failing first. Default and most 200.")] int limit = MaxListed,
        [Description("Hints to skip before listing (a page start). Default 0; use next_offset when has_more is true.")] int offset = 0,
        CancellationToken cancellationToken = default)
        => await GetQueryStoreHints(postgres, server_name, hours_back, as_of, DatabaseFilter.One(database_name), query_id, limit, offset, cancellationToken);

    /// <summary>The read over a LIST of databases: the MCP tool passes <c>DatabaseFilter.One</c>, the web route the repeated keys.</summary>
    internal static Task<string> GetQueryStoreHints(
        NpgsqlDataSource postgres, string? server_name, int hours_back, string? as_of, DatabaseFilter databases, CancellationToken cancellationToken) =>
        GetQueryStoreHints(postgres, server_name, hours_back, as_of, databases, null, MaxListed, 0, cancellationToken);

    internal static async Task<string> GetQueryStoreHints(
        NpgsqlDataSource postgres, string? server_name, int hours_back, string? as_of, DatabaseFilter databases,
        long? queryId, int limit, int offset, CancellationToken cancellationToken)
    {
        if (limit < 1 || limit > MaxListed)
            return McpHelpers.Refusal("limit", $"Invalid limit value '{limit}'. Must be 1-{MaxListed}.");
        if (offset < 0)
            return McpHelpers.Refusal("offset", $"Invalid offset value '{offset}'. Must be 0 or more.");

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        var start = windowEnd.AddHours(-hours_back);

        try
        {
            var read = await DarlingQueryStoreHintReader.ReadAsync(
                postgres, resolved.ServerId, databases, queryId, start, windowEnd, McpCommandDeadlines.ReadSeconds, cancellationToken);
            ReadScope.NoteSource(ReadScope.SourceRaw);

            var scope = databases.Describe();
            var where = scope is null ? "" : $" for {(databases.Names.Count == 1 ? "database '" + scope + "'" : scope)}";
            if (read.Captures.Count == 0)
            {
                return await HintsNotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, cancellationToken)
                    ?? McpHelpers.Status("unavailable",
                        $"No Query Store hint capture{where} in the {hours_back}-hour window ending {Format(windowEnd)}. Either the query_store_hints collector has not run in it, " +
                        "or Query Store is off, in error or a readable secondary in every database (such a database writes no capture).");
            }

            if (read.Hints.Count == 0)
            {
                return JsonSerializer.Serialize(new Dictionary<string, object?>
                {
                    ["server"] = resolved.DisplayName,
                    ["status"] = "empty",
                    ["message"] = queryId is not null
                        ? $"No hint{where} in the newest captures is on query_id {queryId}. Captured databases are listed."
                        : $"No Query Store hint is in place{where} in the newest capture of each database in the window. Captured databases are listed.",
                    ["as_of"] = Format(windowEnd),
                    ["hours_back"] = hours_back,
                    ["captures"] = read.Captures.Select(HintCaptureNode),
                }, McpHelpers.JsonOptions);
            }

            var listed = read.Hints.Skip(offset).Take(limit).ToList();
            var hasMore = offset + listed.Count < read.Hints.Count;
            var withText = await DarlingQueryStoreHintReader.WithQueryTextAsync(
                postgres, resolved.ServerId, listed, start, windowEnd, McpCommandDeadlines.ReadSeconds, cancellationToken);
            string? pageNote = null;
            if (hasMore)
                pageNote = $"Listed {listed.Count} of {read.Hints.Count} hints from offset {offset}. Call again with offset {offset + listed.Count} for the next page, or filter by database_name or query_id.";
            else if (offset >= read.Hints.Count)
                pageNote = $"offset {offset} is past the {read.Hints.Count} hints.";
            return JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["server"] = resolved.DisplayName,
                ["as_of"] = Format(windowEnd),
                ["hours_back"] = hours_back,
                ["total_hints"] = read.Hints.Count,
                ["failing_hints"] = read.Hints.Count(h => h.Failing),
                ["offset"] = offset,
                ["limit"] = limit,
                ["query_store_hints"] = withText.Select(HintNode),
                ["truncated"] = hasMore,
                ["has_more"] = hasMore,
                ["next_offset"] = hasMore ? offset + listed.Count : null,
                ["page_note"] = pageNote,
                ["captures"] = read.Captures.Select(HintCaptureNode),
                ["note"] = "first_seen is the first capture in the window that shows the hint. Query Store keeps no set time, so the real one can be up to one collector pass earlier (5 minutes by default).",
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_store_hints", ex);
        }
    }

    /// <summary>$1 server_id. The registry facts the hints collector's gate asks about: edition, SQL major, engine kind.</summary>
    internal const string HintsTargetFactsSql = "SELECT sql_engine_edition, sql_major_version, engine_kind FROM servers WHERE server_id = $1";

    /// <summary>
    /// The <c>not_collected</c> envelope when the hints collector does not run on this server, or <c>null</c> when it does (or when the
    /// registry cannot be read, so the caller keeps its own answer). The engine and readable-secondary reasons come from
    /// <see cref="DarlingEngineCapability.NotCollectedStatusAsync"/>; the version floor is asked of the collector itself
    /// (<see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/>), which is the gate that stops it running.
    /// </summary>
    internal static async Task<string?> HintsNotCollectedStatusAsync(NpgsqlDataSource postgres, int serverId, string serverName, CancellationToken cancellationToken)
    {
        var engine = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, serverId, serverName, "query_store_hints", cancellationToken);
        if (engine is not null)
            return engine;

        try
        {
            await using var command = postgres.CreateCommand(HintsTargetFactsSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.AddWithValue(serverId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                return null;
            var edition = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
            var major = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            var kind = reader.IsDBNull(2) ? null : reader.GetString(2);
            var target = new CollectorTargetInfo
            {
                Engine = MonitoredEngineKind.EngineOf(kind) ?? CollectorTargetEngine.SqlServer,
                IsAzureSqlDb = edition == CollectorEngineCapability.AzureSqlDatabaseEngineEdition,
                IsAzureManagedInstance = edition == CollectorEngineCapability.AzureManagedInstanceEngineEdition,
                SqlMajorVersion = major,
            };
            if (CollectorCatalog.AppliesTo(QueryStoreHintsCollector.Instance, target))
                return null;

            return McpHelpers.Status("not_collected",
                $"{serverName} runs SQL Server major version {major}. Query Store hints (sys.query_store_query_hints) exist from SQL Server 2022 (major version {QueryStoreHintsCollector.FirstMajorVersion}), " +
                "so the query_store_hints collector does not run on this server, and no hint can be set or read there. " +
                "This is a permanent version gap until the server is upgraded, not a collection outage: checking collection health or enabling a collector cannot change it.");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    private static object HintCaptureNode(QueryStoreHintCapture c) => new
    {
        database_name = c.DatabaseName,
        newest_capture = Format(c.NewestCapture),
        hint_count = c.HintCount,
    };

    internal static object HintNode(QueryStoreHintFact h) => new
    {
        database_name = h.DatabaseName,
        query_id = h.QueryId,
        query_hint_id = h.HintId,
        state = h.Failing ? "FAILING" : "NO_FAILURE",
        query_hint_text = h.HintText,
        source = h.Source,
        last_query_hint_failure_reason = h.FailureReason,
        query_hint_failure_count = h.FailureCount,
        first_seen = Format(h.FirstSeen),
        last_seen = Format(h.LastSeen),
        query_text = h.QueryText,
        query_text_truncated = h.QueryTextTruncated,
    };

    private static object CaptureNode(ForcedPlanCapture c) => new
    {
        database_name = c.DatabaseName,
        newest_capture = Format(c.NewestCapture),
        forced_plan_count = c.ForcedPlanCount,
    };

    internal static object ForceNode(ForcedPlanFact p) => new
    {
        database_name = p.DatabaseName,
        query_id = p.QueryId,
        plan_id = p.PlanId,
        state = StateName(p.State),
        first_seen = Format(p.FirstSeen),
        last_seen = Format(p.LastSeen),
        forced_at = Format(p.ForcedAt),
        plan_forcing_type = p.PlanForcingType,
        force_failure_count = p.FailureCount,
        last_force_failure_reason = p.FailureReason,
        forced_plan = new { execution_count = p.ForcedExecutions, last_execution_time = Format(p.ForcedLastExecution) },
        variant_plans = p.VariantPlans.Take(MaxOtherPlans).Select(o => new { plan_id = o.PlanId, execution_count = o.Executions, last_execution_time = Format(o.LastExecution) }),
        variant_plans_total = p.VariantPlans.Count,
        other_plans = p.OtherPlans.Take(MaxOtherPlans).Select(o => new { plan_id = o.PlanId, execution_count = o.Executions, last_execution_time = Format(o.LastExecution) }),
        other_plans_total = p.OtherPlans.Count,
        unknown_plans = p.UnknownPlans.Take(MaxOtherPlans).Select(o => new { plan_id = o.PlanId, execution_count = o.Executions, last_execution_time = Format(o.LastExecution) }),
        unknown_plans_total = p.UnknownPlans.Count,
        unknown_reason = p.UnknownReason,
        plan_guide = p.PlanGuide is { } guide ? new { name = guide.Name, enabled = guide.Enabled, scope = guide.Scope } : null,
    };
}
