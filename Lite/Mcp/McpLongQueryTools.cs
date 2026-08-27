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
/// PerformanceMonitor_LongQueryCompletions XE session, ordered by duration DESC.
/// </summary>
[McpServerToolType]
public sealed class McpLongQueryTools
{
    [McpServerTool(Name = "get_long_query_completions"), Description("Gets long-running query completions captured by the opt-in long-query trace: rpc/batch completions whose duration exceeded the trace threshold, plus attentions (client cancels / query timeouts). Shows duration, CPU, reads/writes, row count, result (OK/Error/Abort — Abort means the long query was cancelled), the statement text, and the calling session/app/login. The collector is OFF by default; if it returns empty, enable the 'long_query_completions' collector in the collector schedule.")]
    public static async Task<string> GetLongQueryCompletions(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 30.")] int limit = 30,
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

            var rows = await dataService.GetRecentLongQueryCompletionsAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "long_query_completions")
                    /* #2546: this collector is opt-in, so the fall-through below already sends the reader to
                       the schedule — which is the wrong place when the collector IS enabled and its session
                       is missing. The precondition answer names that state instead of quietly blaming a knob
                       that is already switched on. */
                    ?? await McpRuntimePrecondition.StatusAsync(dataService, resolved.ServerId, resolved.ServerName, "long_query_completions")
                    ?? McpHelpers.Status("empty", "No long-running query completions found in the specified time range. The long_query_completions collector is opt-in (default OFF) — enable it in the collector schedule to capture data.");
            }

            var result = rows
                .OrderByDescending(r => r.DurationMicroseconds ?? long.MinValue)
                .Take(limit)
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
                total_completions = rows.Count,
                completions = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_long_query_completions", ex);
        }
    }
}
