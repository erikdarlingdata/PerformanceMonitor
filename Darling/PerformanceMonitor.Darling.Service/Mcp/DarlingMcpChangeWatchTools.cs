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
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #6234: <c>get_change_watches</c>, the read of the change watch windows a recorded change opens. It lists a server's watches, open and
/// closed, newest first, over <see cref="ChangeWatchStore.ListAsync"/>: the changes each watch names, the measures its evaluation read
/// (the three windows' numbers and whether each moved), and its result (<c>flat</c>, <c>request_opened</c>, <c>moved_no_request</c>,
/// <c>moved_agent_change</c> or <c>no_data</c>, with the request id when one opened; a watch that failed repeatedly has state and result <c>failed</c>). A read of the monitoring store only; the investigation agent may call it.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpChangeWatchTools
{
    /// <summary>The <c>state</c> value that means no filter: what the answer echoes when none was given (#6561).</summary>
    internal const string AllStates = "all";

    /// <summary>What <c>state</c> accepts: a watch's state, or the result it closed with (the store matches either).</summary>
    internal static readonly string[] StateFilters =
    {
        "open", "evaluating", "closed", "failed",
        ChangeWatchStore.ResultFlat, ChangeWatchStore.ResultRequestOpened, ChangeWatchStore.ResultMovedNoRequest,
        ChangeWatchStore.ResultAgentChange, ChangeWatchStore.ResultAutoPlanCorrection, ChangeWatchStore.ResultNoData,
    };

    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_change_watches"), Description(
        "Lists a server's change watches, open and closed, newest first: a recorded change (a configuration, trace flag, plan or " +
        "extension change) opens a 60 minute watch, and at its end Darling compares wait time per second, average CPU and query " +
        "duration with the hour before and the same hour a week earlier. Each watch carries its changes, its measures and its result " +
        "(flat, request_opened, moved_no_request, moved_agent_change, moved_auto_plan_correction, no_data), or its failed state. One watch: pass watch_id. From a request_id: state=request_opened, then match it. Stored read. <<GUIDE>> " +
        "Reads the change watch windows (config.change_watches) for SQL Server and PostgreSQL targets. A SQL Server change is a server, database or " +
        "database-scoped setting, a trace flag, a forced or unforced Query Store plan, or a Query Store hint set, changed or cleared (kind query_store_hint, SQL Server 2022 and later); a PostgreSQL change is a server setting, a per-database " +
        "or per-role setting, or an extension install, drop or version change. Every change Darling records opens a watch of 60 " +
        "minutes for its server, or extends the open one; when the window ends, the watch is evaluated and closes. Darling looks for " +
        "changes once an hour per server, so a change may be recorded up to an hour after it happened. The window still runs from the " +
        "change's own time, and a later change inside the window is still added before the watch is evaluated. Because of that delay a " +
        "watch is often due as soon as it is recorded, so an open watch is rarely seen. Each watch lists its " +
        "changes (time, kind, database, query id and what changed), and a closed watch lists its measures: total wait time per second, " +
        "average CPU and each query whose average duration at least doubled with 10 or more executions in both windows, each with the " +
        "hour before, the watch window and the same window a week earlier. result says how it closed: flat (nothing moved), " +
        "request_opened (something moved and ONE investigation request opened, request_id names it), moved_no_request (something moved and " +
        "no investigation could open: investigations are off or no agent route matched), moved_agent_change (something moved, but every " +
        "change in the watch was the investigation agent's own plan force or unforce made while it handled a change watch request, so it " +
        "is recorded and measured and opens no further request), moved_auto_plan_correction (something moved, but every change was a plan that SQL Server's own " +
        "automatic plan correction forced or unforced, so it is recorded and measured and opens no request), or no_data (the before or after hour had no " +
        "collected rows, so nothing was compared). state says where a watch is: open (still collecting its window), evaluating (its " +
        "window ended and a Darling pass is reading its measures or opening its request; a watch stuck here is retried), closed (done, " +
        "with a result) or failed (its evaluation or request failed repeatedly and Darling gave up; result_detail holds the last error). " +
        "result_detail.notes says when a plan-change watch could not read the query from Query " +
        "Store and measured the top 20 queries instead; a plan-change query's after-window holds only Query Store intervals that start at or " +
        "after the change, so a watch evaluated soon after the change may fall back to the top 20 queries and lists the plan measure in " +
        "not_measured. A PostgreSQL target reads waits from pg_wait_sampling, or from the Aurora wait " +
        "collector where that is its source, CPU from Performance Insights samples and statements from pg_stat_statements; a measure it has no source for is listed in not_measured with the reason " +
        "(for example no CPU source), never as flat. An open watch has no measures yet: it is still collecting its window. Pass state " +
        "open, evaluating, closed or failed to see only watches in that state, or a result (flat, request_opened, moved_no_request, " +
        "moved_agent_change, moved_auto_plan_correction, no_data) to see only watches that closed with it. A closed watch that measured many queries can run to about 20 KB, " +
        "so ask for a small limit (3 to 5), or pass watch_id to read one watch in any state. The answer is held to the 32,768-byte response budget, whole watches only, newest first: budget_truncated and budget_note say how many watches fit, and a single watch over the budget on its own keeps its newest changes and says changes_left_out. measures, not_measured and " +
        "changes are sent once, at the top of the watch; result_detail holds the rest of the stored result (had_data, notes, window). " +
        "A read over the monitoring store; no monitored server is touched.")]
    public static Task<string> GetChangeWatches(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Only watches in this state (open, evaluating, closed, failed) or with this result (flat, request_opened, moved_no_request, moved_agent_change, moved_auto_plan_correction, no_data). Omit it, or pass all, for every state.")] string? state = null,
        [Description("Most watches to return, newest first. Default 20 (1-1000). A small limit gives the newest, not a chosen watch: use watch_id.")] int limit = 20,
        [Description("Read one watch by its watch_id, in any state; state still applies if you give it. A watch_id that is not on this server answers not_found.")] string? watch_id = null,
        CancellationToken cancellationToken = default) =>
        GetChangeWatches(postgres, server_name, state, limit, watch_id, McpResponseBudget.DefaultBytes, cancellationToken);

    /// <summary>
    /// #8901: the read behind <c>get_change_watches</c>. Under a <paramref name="responseByteBudget"/> the answer is held to it (see
    /// <see cref="FitToBudget"/>). The web route names no budget and keeps the whole answer, as the other web reads do: the viewer must not be
    /// shrunk by the MCP cut.
    /// </summary>
    internal static async Task<string> GetChangeWatches(
        NpgsqlDataSource postgres, string? server_name, string? state, int limit, string? watch_id, int? responseByteBudget,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        /* #6561: the answer says "state": "all" when no filter was given, so a caller that copies that value back means no filter. */
        if (string.Equals(state, AllStates, StringComparison.OrdinalIgnoreCase))
        {
            state = null;
        }

        if (state is not null && Array.IndexOf(StateFilters, state) < 0)
        {
            return McpHelpers.Refusal("state", $"Invalid state '{state}'. Use {string.Join(", ", StateFilters)}, {AllStates}, or omit it for all.");
        }

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return limitError;

        /* #6591: a watch_id that is not a GUID cannot match a watch; it answers like an id nobody holds. */
        Guid? watchId = null;
        if (!string.IsNullOrWhiteSpace(watch_id))
        {
            if (!Guid.TryParse(watch_id, out var parsed))
            {
                return McpHelpers.Status("not_found", $"watch_id '{watch_id}' is not a change watch id.");
            }

            watchId = parsed;
        }

        try
        {
            var store = new ChangeWatchStore(postgres, McpCommandDeadlines.ReadSeconds);
            var watches = await store.ListAsync(resolved.ServerId, state, limit, watchId, cancellationToken);
            if (watches.Count == 0 && watchId is not null)
            {
                return McpHelpers.Status("not_found", $"No change watch{(state is null ? "" : $" matching {state}")} with that watch_id is recorded for {resolved.DisplayName}.");
            }

            if (watches.Count == 0)
            {
                /* The change kinds belong to the target's engine: a PostgreSQL target records settings and extensions, never trace flags or plans. */
                var (_, engineKind) = await DarlingEngineCapability.PostgresTargetFactsAsync(postgres, resolved.ServerId, cancellationToken);
                var kinds = MonitoredEngineKind.IsPostgres(engineKind)
                    ? "a server setting, a per-database or per-role setting, or an extension install, drop or version change"
                    : "a server, database or database-scoped configuration change, a trace flag change, a Query Store plan force or unforce or a Query Store hint change";
                return McpHelpers.Status(
                    "empty",
                    $"No change watch{(state is null ? "" : $" matching {state}")} is recorded for {resolved.DisplayName}. A watch opens when {kinds} " +
                    "is recorded; none has been since change watching began, or the 90 day retention removed it.");
            }

            var payload = JsonSerializer.SerializeToNode(new
            {
                server = resolved.DisplayName,
                state = state ?? "all",
                watch_count = watches.Count,
                watches = watches.Select(w =>
                {
                    /* #8815: the stored averages are doubles (109.81369347172361); the reply writes them by the one reply rounding rule. */
                    var detail = McpHelpers.RoundFloatNoise(ParseDetail(w.ResultJson));
                    return new
                    {
                        watch_id = w.WatchId,
                        state = w.State,
                        first_change_at = w.FirstChangeUtc.ToString("o"),
                        last_change_at = w.LastChangeUtc.ToString("o"),
                        window_ends_at = w.WindowEndsUtc.ToString("o"),
                        closed_at = w.ClosedUtc?.ToString("o"),
                        result = w.Result,
                        request_id = w.RequestId,
                        changes = w.Changes.Select(c => new
                        {
                            change_time = c.ChangeTimeUtc.ToString("o"),
                            kind = c.Kind,
                            database = c.DatabaseName,
                            query_id = c.QueryId,
                            plan_forcing_type = c.PlanForcingType,
                            what = c.Description,
                        }),
                        measures = detail?["measures"],
                        not_measured = detail?["not_measured"],
                        result_detail = WithoutRepeats(detail),
                    };
                }),
            }, McpHelpers.JsonOptions)!.AsObject();

            /* #8901: held to the shared response budget, whole watches only, newest first. */
            return responseByteBudget is int budget ? FitToBudget(payload, budget) : payload.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_change_watches", ex);
        }
    }

    /// <summary>The sentence <c>budget_note</c> ends with when watches were left off: how one is read in full, and how the rest are listed.</summary>
    private const string WatchesCutRemedy = "Pass watch_id to read one watch in full, or state to list only the watches in one state.";

    /// <summary>
    /// #8901: holds the answer to <paramref name="budgetBytes"/> by the rule the #8568 reads use. Whole watches are kept newest first and the
    /// rest are left off (<c>budget_truncated</c>, <c>budget_note</c>, and <c>watch_count</c> rewritten to the watches kept). When the newest watch
    /// is over the budget by itself, it is the only one kept and its <c>changes</c> are cut to the newest that fit: the watch says
    /// <c>changes_left_out</c>, and the note says how many. A watch's changes are stored oldest first, so the newest are the last ones.
    /// </summary>
    internal static string FitToBudget(JsonObject payload, int budgetBytes)
    {
        if (payload["watches"] is not JsonArray watches || watches.Count == 0)
        {
            return payload.ToJsonString(McpHelpers.JsonOptions);
        }

        var inScope = watches.Count;
        var first = watches[0]!.AsObject();
        var changes = (first["changes"] as JsonArray)?.ToList() ?? new List<JsonNode?>();

        string Build(int keptChanges)
        {
            var node = payload.DeepClone().AsObject();
            var watch = first.DeepClone().AsObject();
            var leftOut = changes.Count - keptChanges;
            watch["changes"] = new JsonArray(changes.Skip(leftOut).Select(c => c?.DeepClone()).ToArray());
            watch["changes_left_out"] = leftOut;
            node["watches"] = new JsonArray(watch);
            node["watch_count"] = 1;
            node["budget_truncated"] = true;
            var cutText = string.Create(System.Globalization.CultureInfo.InvariantCulture,
                $"{leftOut:#,0} of its {changes.Count:#,0} changes were left out and the newest {keptChanges:#,0} are kept.");
            node["budget_note"] = inScope > 1
                ? McpResponseBudget.PageCutNote(inScope, 1, "newest first", "watches", "The newest watch is over the budget by itself, so " + cutText + " " + WatchesCutRemedy)
                : string.Create(System.Globalization.CultureInfo.InvariantCulture,
                    $"TRUNCATED: this watch is over the {budgetBytes:#,0}-byte response budget by itself, so {cutText}");
            return node.ToJsonString(McpHelpers.JsonOptions);
        }

        /* The newest watch alone, whole: when it fits, whole watches are the unit and the shared cut applies. The shared cut adds its note
           (and rewrites watch_count), so its answer is measured as written: a newest watch that fits only without the note is still cut by changes. */
        var oneWatch = payload.DeepClone().AsObject();
        oneWatch["watches"] = new JsonArray(first.DeepClone());
        if (changes.Count <= 1 || Encoding.UTF8.GetByteCount(oneWatch.ToJsonString(McpHelpers.JsonOptions)) <= budgetBytes)
        {
            var capped = McpResponseBudget.CapArray(
                payload.DeepClone().AsObject(), "watches", "newest first", "watches", _ => WatchesCutRemedy, budgetBytes,
                "budget_truncated", "budget_note", (node, kept) => node["watch_count"] = kept);
            if (changes.Count <= 1 || Encoding.UTF8.GetByteCount(capped) <= budgetBytes)
            {
                return capped;
            }
        }

        var fit = McpResponseBudget.RowsThatFit(changes.Count, n => Encoding.UTF8.GetByteCount(Build(n)), budgetBytes);
        return Build(fit);
    }

    /// <summary>
    /// #6591: the stored result minus the keys the watch already carries at its top (<c>measures</c>, <c>not_measured</c>, <c>changes</c>), so
    /// each fact is sent once. What is left (<c>had_data</c>, <c>notes</c>, <c>window</c>, or a failed watch's <c>error</c>) is only here.
    /// </summary>
    private static JsonNode? WithoutRepeats(JsonNode? detail)
    {
        if (detail is JsonObject stored)
        {
            var rest = new JsonObject();
            foreach (var (key, value) in stored)
            {
                if (key is "measures" or "not_measured" or "changes")
                {
                    continue;
                }

                rest[key] = key == "notes" ? WithoutNotMeasuredLines(value, stored["not_measured"]) : value?.DeepClone();
            }

            return rest;
        }

        return detail;
    }

    /// <summary>
    /// #8815: the stored notes minus the "not measured: {measure}: {reason}" lines that older results folded in from <c>not_measured</c>, which the
    /// watch already carries at its top. A note that is not one of those lines stays.
    /// </summary>
    private static JsonNode? WithoutNotMeasuredLines(JsonNode? notes, JsonNode? notMeasured)
    {
        if (notes is not JsonArray stored)
        {
            return notes?.DeepClone();
        }

        var repeated = new HashSet<string>(StringComparer.Ordinal);
        if (notMeasured is JsonArray entries)
        {
            foreach (var entry in entries)
            {
                repeated.Add($"not measured: {(string?)entry?["measure"]}: {(string?)entry?["reason"]}");
            }
        }

        var kept = new JsonArray();
        foreach (var note in stored)
        {
            if (note is JsonValue value && value.TryGetValue<string>(out var text) && repeated.Contains(text))
            {
                continue;
            }

            kept.Add(note?.DeepClone());
        }

        return kept;
    }

    /// <summary>The stored result as a JSON node, or null when the watch is still open or its text is not JSON.</summary>
    private static JsonNode? ParseDetail(string? resultJson)
    {
        if (string.IsNullOrWhiteSpace(resultJson))
        {
            return null;
        }

        try
        {
            return JsonNode.Parse(resultJson);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
