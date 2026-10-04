using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The long-query completion MCP tool (#1496) — get_long_query_completions — over Lite's DuckDB store. The
/// Lite twin of Darling's <see cref="PerformanceMonitor.Darling.Service.Mcp.DarlingMcpLongQueryTools"/>,
/// field-for-field, so the cross-app MCP inventory stays in parity. Returns the longest completed queries
/// (rpc/batch over the trace's duration threshold) plus attentions (cancels/timeouts) from the opt-in
/// PerformanceMonitor_Lite_{id}_LongQueryCompletions XE session this install makes, ordered by duration DESC.
/// </summary>
[McpServerToolType]
public sealed class McpLongQueryTools
{
    [McpServerTool(Name = "get_long_query_completions"), Description("Gets the SLOWEST long-running query completions in the window: rpc/batch completions over the trace's duration threshold, plus attentions (cancels/timeouts), ranked duration DESC, attentions (no duration) last. THE PAGE IS THE window's limit SLOWEST, NOT ITS NEWEST: truncated means the window held more, none slower than the page; oldest/newest_returned_event_time bound the slowest runs' ages, NOT how far the read reached. Collector is opt-in, OFF by default: empty can mean none in the window, or the collector was off — enable 'long_query_completions' in the schedule. <<GUIDE>> Gets the SLOWEST long-running query completions in the window, captured by the opt-in long-query trace: rpc/batch completions whose duration exceeded the trace threshold, plus attentions (client cancels / query timeouts), ranked by duration DESC with attentions (no duration) last. Shows duration, CPU, reads/writes, row count, result (OK/Error/Abort — Abort means the long query was cancelled), the statement text, and the calling session/app/login. THE PAGE IS THE window's limit SLOWEST, NOT ITS NEWEST: completions_returned is how many rows you got and truncated says the window held more than limit. Because the page is duration-RANKED, oldest_returned_event_time / newest_returned_event_time tell you how old the slowest runs are and say NOTHING about how far back the read reached — every row in the window was a candidate, so a truncated page still holds the window's slowest. Both SKUs keep the same population. The collector is OFF by default; if it returns empty, enable the 'long_query_completions' collector in the schedule.")]
    public static async Task<string> GetLongQueryCompletions(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return, slowest first. Default 30. This is what bounds the page — read truncated to know whether the window held more.")] int limit = 30,
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

            /*
                #3541 A3, and the SKU-parity half of it. This tool used to read the grid's NEWEST 200 rows
                and re-rank them by duration here, while Darling ranked by duration in SQL over the whole
                window — so on a busy trace Lite could omit the window's slowest run entirely, under the same
                tool name. A completions tool sorted by duration keeps the SLOWEST; that is the one truth both
                SKUs now serve, from a duration-ranked read with the caller's limit + 1 as the fetch and the
                extra row as the observed truncation signal.
            */
            var rows = await dataService.GetSlowestLongQueryCompletionsAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd, limit: limit + 1);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "long_query_completions")
                    /* #2546: this collector is opt-in, so the fall-through below already sends the reader to
                       the schedule — which is the wrong place when the collector IS enabled and its session
                       is missing. The precondition answer names that state instead of quietly blaming a knob
                       that is already switched on. */
                    ?? await McpRuntimePrecondition.StatusAsync(dataService, resolved.ServerId, resolved.ServerName, "long_query_completions")
                    ?? McpHelpers.Status("empty", "No long-running query completions found in the specified time range. The long_query_completions collector is opt-in (default OFF) — enable it in the collector schedule to capture data.",
                        (await McpQueryTools.EventWindowNoticeAsync(
                            () => dataService.GetQueryWindowFloorAsync(QueryWindowRelation.LongQueryCompletions, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd),
                            null, windowEnd.AddHours(-hours_back), windowEnd, "long_query_completions", emptyAnswer: true)).AsHints());
            }

            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            /* #4966: where this server's long_query_completions data starts for the window. An event list, so the floor is the
               earlier of the coverage probe and the oldest event the page shows. The page is the slowest N, so its oldest
               event depends on limit; that cannot make a false notice, as the helper only moves the floor earlier and a
               first run reads back 10 minutes, well inside the 90-minute slack. */
            var requestedStart = windowEnd.AddHours(-hours_back);
            var notice = await McpQueryTools.EventWindowNoticeAsync(
                () => dataService.GetQueryWindowFloorAsync(QueryWindowRelation.LongQueryCompletions, resolved.ServerId, requestedStart, windowEnd),
                page.Min(r => r.EventTime), requestedStart, windowEnd, "long_query_completions");

            var result = page
                .Select(r => new
                {
                    event_time = r.EventTime?.ToString("o"),
                    event_type = r.EventType,
                    duration_ms = r.DurationMicroseconds.HasValue ? r.DurationMicroseconds.Value / 1000.0 : (double?)null,
                    cpu_ms = r.CpuTimeMicroseconds.HasValue ? r.CpuTimeMicroseconds.Value / 1000.0 : (double?)null,
                    logical_reads = r.LogicalReads,
                    physical_reads = r.PhysicalReads,
                    writes = r.Writes,
                    row_count = r.RowCountValue,
                    result = r.Result,
                    database_name = r.DatabaseName,
                    object_name = r.ObjectName,
                    statement = McpHelpers.Truncate(r.StatementText, 2000),
                    session_id = r.SessionId,
                    client_app_name = r.ClientAppName,
                    client_pid = r.ClientPid,
                    nt_username = r.NtUserName,
                    server_principal_name = r.ServerPrincipalName,
                    query_hash = r.QueryHash
                });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #4966: where the store's data starts, always present (false and null when the store covered the
                   window). The floor is the earlier of the coverage probe and the oldest event this page shows: a
                   first run of the collector can store events from before itself. No effective_hours_back: this
                   payload carries a page `truncated`, and the census holds that key apart for the window floor. */
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                /* #3541 A3: the page described as a page, on Darling's names. Under a duration RANKING the
                   two stamps bound the slowest runs, not the reach — the description says so. */
                completions_returned = page.Count,
                truncated,
                oldest_returned_event_time = McpHelpers.FormatEffectiveStart(page.Min(r => r.EventTime)),
                newest_returned_event_time = McpHelpers.FormatEffectiveStart(page.Max(r => r.EventTime)),
                order = "duration_ms_desc",
                completions = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_long_query_completions", ex);
        }
    }
}
