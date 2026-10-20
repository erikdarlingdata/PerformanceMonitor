/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
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
/// (the three windows' numbers and whether each moved), and its result (<c>flat</c>, <c>request_opened</c>, <c>moved_no_request</c> or
/// <c>no_data</c>, with the request id when one opened). A read of the monitoring store only; the investigation agent may call it.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpChangeWatchTools
{
    [McpServerTool(Name = "get_change_watches"), Description(
        "Lists a server's change watches, open and closed, newest first: a recorded change (a configuration, trace flag, plan or " +
        "extension change) opens a 60 minute watch, and at its end Darling compares wait time per second, average CPU and query " +
        "duration with the hour before and the same hour a week earlier. Each watch carries its changes, its measures and its result " +
        "(flat, request_opened, moved_no_request, no_data). Stored read. <<GUIDE>> " +
        "Reads the change watch windows (config.change_watches) for SQL Server and PostgreSQL targets. A SQL Server change is a server, database or " +
        "database-scoped setting, a trace flag, or a forced or unforced Query Store plan; a PostgreSQL change is a server setting, a per-database " +
        "or per-role setting, or an extension install, drop or version change. Every change Darling records opens a watch of 60 " +
        "minutes for its server, or extends the open one; when the window ends, the watch is evaluated and closes. Each watch lists its " +
        "changes (time, kind, database, query id and what changed), and a closed watch lists its measures: total wait time per second, " +
        "average CPU and each query whose average duration at least doubled with 10 or more executions in both windows, each with the " +
        "hour before, the watch window and the same window a week earlier. result says how it closed: flat (nothing moved), " +
        "request_opened (something moved and ONE investigation request opened, request_id names it), moved_no_request (something moved and " +
        "no investigation could open: investigations are off or no agent route matched), or no_data (the before or after hour had no " +
        "collected rows, so nothing was compared). result_detail.notes says when a plan-change watch could not read the query from Query " +
        "Store and measured the top 20 queries instead. A PostgreSQL target reads waits from pg_wait_sampling, or from the Aurora wait " +
        "collector where that is its source, CPU from Performance Insights samples and statements from pg_stat_statements; a measure it has no source for is listed in not_measured with the reason " +
        "(for example no CPU source), never as flat. An open watch has no measures yet: it is still collecting its window. Pass state " +
        "open or closed to see only those. A read over the monitoring store; no monitored server is touched.")]
    public static async Task<string> GetChangeWatches(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Only watches in this state: open or closed. Omit for both.")] string? state = null,
        [Description("Most watches to return, newest first. Default 20 (1-1000).")] int limit = 20,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        if (state is not null && state != "open" && state != "closed")
        {
            return McpHelpers.Refusal("state", $"Invalid state '{state}'. Use open or closed, or omit it for both.");
        }

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return limitError;

        try
        {
            var store = new ChangeWatchStore(postgres, McpCommandDeadlines.ReadSeconds);
            var watches = await store.ListAsync(resolved.ServerId, state, limit, cancellationToken);
            if (watches.Count == 0)
            {
                /* The change kinds belong to the target's engine: a PostgreSQL target records settings and extensions, never trace flags or plans. */
                var (_, engineKind) = await DarlingEngineCapability.PostgresTargetFactsAsync(postgres, resolved.ServerId, cancellationToken);
                var kinds = MonitoredEngineKind.IsPostgres(engineKind)
                    ? "a server setting, a per-database or per-role setting, or an extension install, drop or version change"
                    : "a server, database or database-scoped configuration change, a trace flag change or a Query Store plan force or unforce";
                return McpHelpers.Status(
                    "empty",
                    $"No change watch{(state is null ? "" : $" in state {state}")} is recorded for {resolved.DisplayName}. A watch opens when {kinds} " +
                    "is recorded; none has been since change watching began, or the 90 day retention removed it.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                state = state ?? "open and closed",
                watch_count = watches.Count,
                watches = watches.Select(w =>
                {
                    var detail = ParseDetail(w.ResultJson);
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
                            what = c.Description,
                        }),
                        measures = detail?["measures"],
                        not_measured = detail?["not_measured"],
                        result_detail = detail,
                    };
                }),
            });
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_change_watches", ex);
        }
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
