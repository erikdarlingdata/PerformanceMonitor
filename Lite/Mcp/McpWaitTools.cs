using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpWaitTools
{
    [McpServerTool(Name = "get_wait_stats"), Description("Gets the top SQL Server wait types aggregated over a time period, heaviest total wait first. Wait stats reveal what SQL Server spends time waiting on — high signal waits indicate CPU pressure, high resource waits indicate I/O or lock contention. Use this first to identify the dominant wait category, then drill into specific tools based on the wait type. THE PAGE IS BOUNDED BY limit: wait_types_returned is how many wait types you got and truncated says the window observed more than limit — the rows you have are the heaviest, and the ones past the cap are lighter, but a sum over the page is a sum over the page rather than over the server.")]
    public static async Task<string> GetWaitStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum wait types to return, heaviest first. Default 20. This is what bounds the page — read truncated to know whether the window observed more.")] int limit = 20,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            /* #3541 A3: the caller's limit + 1 as the fetch, the extra row as the observed truncation
               signal. The reader's LIMIT 50 sat under a limit this tool accepts up to 1,000. */
            var rows = await dataService.GetWaitStatsAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd, limit: limit + 1);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "wait_stats")
                    ?? McpHelpers.Status("unavailable", "No wait stats data available for the specified time range.");
            }

            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            var result = page.Select(r => new
            {
                wait_type = r.WaitType,
                total_wait_time_ms = r.TotalWaitTimeMs,
                total_signal_wait_ms = r.TotalSignalWaitTimeMs,
                resource_wait_ms = r.ResourceWaitTimeMs,
                waiting_tasks = r.TotalWaitingTasks,
                signal_wait_pct = Math.Round(r.SignalWaitPercent, 1)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #3541 A3: the page described as a page, on Darling's names. No time bounds here — the rows
                   are per-type aggregates over the whole window, so there is no page reach to report, only a
                   cap. */
                wait_types_returned = page.Count,
                truncated,
                order = "total_wait_time_ms_desc",
                waits = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_wait_stats", ex);
        }
    }

    [McpServerTool(Name = "get_wait_types"), Description("Lists the distinct wait types observed on a server in the given time period. Useful for discovering what wait types to drill into with get_wait_trend.")]
    public static async Task<string> GetWaitTypes(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var types = await dataService.GetDistinctWaitTypesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

            if (types.Count == 0)
            {
                /*
                    An empty list said nothing about which nothing this is. A server that collected and was
                    quiet in THIS window wants the window widened; a server nothing has been stored for
                    wants somebody to look at collection, and widening will never fill it. Same words as
                    Darling's twin.
                */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "wait_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await dataService.HasAnyWaitStatAsync(resolved.ServerId)
                    ? McpHelpers.Status(
                        "empty",
                        $"No wait types recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected wait stats before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No wait stats have EVER been recorded for {resolved.ServerName}. This is not an empty window — nothing has been stored for this server at all. Delta wait stats need a SECOND collection cycle before the first row exists, so on a newly added server this clears itself; otherwise check that collection is running and that the server is enabled.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                wait_types = types
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_wait_types", ex);
        }
    }

    [McpServerTool(Name = "get_wait_trend"), Description("Gets a time-series trend for a specific wait type, showing how wait time changes over time. Use get_wait_types first to discover available wait types.")]
    public static async Task<string> GetWaitTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The exact wait type name, e.g. CXPACKET, PAGEIOLATCH_SH.")] string wait_type,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var points = await dataService.GetWaitStatsTrendAsync(resolved.ServerId, wait_type, hours_back, asOfUtc: windowEnd);
            if (points.Count == 0)
            {
                /* The engine question comes BEFORE the distinct-values probe, not after it. Both are on
                   the miss path, so either order keeps the property that matters — but a permanently gated
                   engine takes this branch on every call, forever, and the probe below could never tell it
                   anything. Asking first makes that case one query instead of two. */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "wait_stats");
                if (gated != null)
                {
                    return gated;
                }

                /* Same shape as get_perfmon_trend: tell the caller whether the wait type is just
                   unknown here vs. nothing collected at all, and hand back the ones that do have data. */
                var collected = await dataService.GetDistinctWaitTypesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
                if (collected.Count == 0)
                    return McpHelpers.Status(
                        "unavailable",
                        $"No trend data for wait type '{wait_type}'. No wait stats have been collected for this server in the last {hours_back}h yet " +
                        "(the collector may not have run, or delta wait stats need a second collection cycle).");

                return McpHelpers.Status(
                    "not_collected",
                    $"No trend data for wait type '{wait_type}'. It may not be a wait type observed on this server in this window — see hints.collected_wait_types for the {collected.Count} that have data.",
                    new { collected_wait_types = collected });
            }

            var result = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                wait_time_ms_per_second = p.WaitTimeMsPerSecond,
                signal_wait_time_ms_per_second = p.SignalWaitTimeMsPerSecond
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                wait_type,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_wait_trend", ex);
        }
    }

    [McpServerTool(Name = "get_waiting_tasks"), Description("Gets recently captured waiting tasks — queries that were actively waiting on a resource at collection time — NEWEST CAPTURE FIRST, longest wait first within a capture. Shows session ID, wait type, duration, blocking session, and database. Complements get_wait_stats by showing individual waiting queries rather than aggregated stats. THE PAGE IS BOUNDED BY limit, NOT BY hours_back: tasks_returned is how many rows you got, truncated says the window held more than limit, and oldest_returned_collection_time / newest_returned_collection_time bound the page — under newest-first ordering the oldest stamp IS how far back this read reached, and one busy capture can fill the whole page by itself. Raise limit or narrow hours_back when truncated is true.")]
    public static async Task<string> GetWaitingTasks(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 1.")] int hours_back = 1,
        [Description("Maximum rows to return, newest capture first. Default 30. This is what bounds the page — read truncated to know whether the window held more.")] int limit = 30,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            /* #3541 A3: the caller's limit + 1 as the fetch, the extra row as the observed truncation
               signal. The read was UNBOUNDED with a Take(limit) on top, and the envelope stated no bound. */
            var rows = await dataService.GetWaitingTasksAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd, limit: limit + 1);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "waiting_tasks")
                    ?? McpHelpers.Status("empty", "No waiting tasks captured in the specified time range.");
            }

            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            var result = page.Select(r => new
            {
                session_id = r.SessionId,
                wait_type = r.WaitType,
                wait_duration_ms = r.WaitDurationMs,
                blocking_session_id = r.BlockingSessionId,
                database_name = r.DatabaseName,
                resource_description = r.ResourceDescription,
                collection_time = r.CollectionTime.ToString("o")
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3541 A3: the envelope was bare — server and rows, no window, no count, no bound. Now the
                   span requested, the page described as a page, and the span the page covers, on Darling's
                   names. */
                hours_back,
                tasks_returned = page.Count,
                truncated,
                oldest_returned_collection_time = page.Min(r => r.CollectionTime).ToString("o"),
                newest_returned_collection_time = page.Max(r => r.CollectionTime).ToString("o"),
                order = "collection_time_desc",
                tasks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_waiting_tasks", ex);
        }
    }
}
