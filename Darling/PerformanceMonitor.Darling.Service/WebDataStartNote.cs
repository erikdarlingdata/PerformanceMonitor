/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Where the data starts, for the server page's grids (#4966). A grid or a list over a window hides a short history:
/// a server added two days ago shows two days of rows under "last 7 days" with nothing saying so. This asks
/// <see cref="DataWindowFloor"/> where the read's own table starts covering the server and, when that is after the
/// window's start, adds the same three fields the tools that already carry the window floor use
/// (<c>window_truncated</c>, <c>effective_start</c>, <c>truncation_note</c>) to the response the page reads.
///
/// <para><b>Why in the web layer.</b> The server page reads <c>/api/read/{tool}</c>, which returns the tool's own
/// payload. The tools are shared with the MCP host, which gets its own notices later; this wraps the web mirror only,
/// so no tool's payload changes. A read the tool already answers with <c>window_truncated</c> (the Queries tab's
/// grids) is left as the tool wrote it.</para>
///
/// <para><b>Which reads.</b> Only a grid over one collector table whose rows are stamped with the collection time:
/// <see cref="TableByRead"/>. A chart whose time axis spans the asked range already shows the empty span, a read of
/// the newest snapshot has no window to cut, and an event surface (blocked process reports, deadlocks, system health
/// events, the default trace) filters on the event's own time, which can reach before the first collection, so a
/// coverage start could name a time later than the history it shows. Those are not in this list.</para>
///
/// <para><b>When it says nothing.</b> The window is covered (whether or not it holds rows: a quiet start is not a
/// cut), nothing in scope holds a row or logged a run in it, the answer is an envelope (empty, unavailable, invalid)
/// or an error rather than rows, the read cannot be resolved, or the probe fails. A failed probe costs the grid its
/// notice, never its rows.</para>
/// </summary>
internal static class WebDataStartNote
{
    /// <summary>
    /// The grid reads that carry the notice, each over the one collector table it reads. A closed list: each name is
    /// a read <c>BuildReadDispatch</c> serves, each table one <see cref="DataWindowFloor.Source.TryForCollectorTable"/>
    /// can probe, and a test holds both.
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, string> TableByRead = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["get_waiting_tasks"] = "waiting_tasks",
        ["get_latch_stats"] = "latch_stats",
        ["get_spinlock_stats"] = "spinlock_stats",
        ["get_wait_stats"] = "wait_stats",
        ["get_pg_wait_stats"] = "pg_wait_stats",
        ["get_pg_wait_sampling"] = "pg_wait_sampling",
        ["get_pg_kernel_stats"] = "pg_kernel_stats",
        ["get_pg_lock_stats"] = "pg_lock_stats",
        ["get_pg_predicate_stats"] = "pg_predicate_stats",
    };

    /// <summary>
    /// <paramref name="result"/> with the notice fields added when <paramref name="tool"/> is a listed grid read whose
    /// window starts before its table's coverage; otherwise <paramref name="result"/> itself, untouched.
    /// <paramref name="hoursBack"/> is the window the page asked for (null or below 1: none was asked), and
    /// <paramref name="asOf"/> the request's window anchor.
    /// </summary>
    internal static async Task<string> AddAsync(
        NpgsqlDataSource postgres, string tool, string? server, int? hoursBack, string? asOf, string result,
        ILogger? logger, CancellationToken cancellationToken)
    {
        if (!TableByRead.TryGetValue(tool, out var table)
            || string.IsNullOrWhiteSpace(server)
            || hoursBack is not int hours
            || hours < 1
            || !DataWindowFloor.Source.TryForCollectorTable(table, out var source))
        {
            return result;
        }

        JsonObject? payload;
        try
        {
            payload = JsonNode.Parse(result) as JsonObject;
        }
        catch (System.Text.Json.JsonException)
        {
            return result;
        }

        /* Rows only: an envelope (status), an error, or a tool that already reports its own window floor is left
           as it is. An empty answer says so itself, and the probe would count a server by its logged runs. */
        if (payload is null
            || payload.ContainsKey("status")
            || payload.ContainsKey("error")
            || payload.ContainsKey("window_truncated"))
        {
            return result;
        }

        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server, cancellationToken);
            if (error is not null || McpHelpers.ValidateWindow(hours, asOf, out var windowEnd) is not null)
            {
                return result;
            }

            var windowStart = windowEnd.AddHours(-hours);
            var dataStart = await DataWindowFloor.GetAsync(
                postgres, [source], [resolved.ServerName], windowStart, windowEnd, StorageCommandDeadlines.McpReadSeconds, cancellationToken);
            if (!RawWindowFloor.IsTruncated(dataStart, windowStart))
            {
                return result;
            }

            payload["window_truncated"] = true;
            payload["effective_start"] = dataStart!.Value.ToString("o", CultureInfo.InvariantCulture);
            payload["truncation_note"] = ComposeStoreAvailability.BuildDataStartNotice(dataStart.Value, windowStart, windowEnd);
            return payload.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Debug, like the Custom Views runner's probe: the grid is still answered, and this runs once per grid
               refresh, so a standing fault at Warning would put a line in the log on every one. */
            logger?.LogDebug(ex, "The data-start probe for {Tool} failed; the grid is answered without a partial-window notice.", tool);
            return result;
        }
    }
}
