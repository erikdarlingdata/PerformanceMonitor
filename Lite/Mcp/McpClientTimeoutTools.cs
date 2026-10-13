using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The Client Timeouts read (#5640) — get_client_timeouts — over Lite's DuckDB store. The Lite twin of Darling's
/// <c>DarlingMcpClientTimeoutTools</c>: both run the one shared <see cref="AttentionWindowSql"/> text and judge it with the one
/// shared <see cref="ClientTimeoutsRead"/>, which applies the alert's own freshness and span rule, so the tool and the alert cannot
/// disagree about how many timeouts a window held.
/// </summary>
[McpServerToolType]
public sealed class McpClientTimeoutTools
{
    private const string Collector = "attention";

    [McpServerTool(Name = "get_client_timeouts"), Description("Gets client timeouts (a client cancelled or timed out a query) for one server over a time range: the EXACT count and rate per minute from the session's event_counter, plus the top statements (fingerprint and one sample text), databases and client applications, and the newest sampled events. count and rate_per_minute are exact; the statements, databases, applications and sampled_events come from sampled ring-buffer events, which are a subset, never the total (samples_note says so). status empty = the collector ran across the range and counted none; unavailable = no recent successful collector run, so no count is claimed; precondition = the capture session is missing or denied.")]
    public static async Task<string> GetClientTimeouts(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 1.")] int hours_back = 1,
        [Description("Maximum sampled events to return, newest first. Default 20. Bounds sampled_events only; the count and the top lists are never cut.")] int limit = 20,
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

            var unmet = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, Collector);
            if (unmet != null)
            {
                return unmet;
            }

            var windowStart = windowEnd.AddHours(-hours_back);
            var (window, samples) = await dataService.GetAttentionRangeAsync(resolved.ServerId, windowStart, windowEnd, limit);
            var (kind, reading) = ClientTimeoutsRead.Classify(window, windowEnd);

            if (kind == ClientTimeoutsKind.NotObserved)
            {
                /* A collector that recorded a denial or a missing session on its latest run has a specific sentence, which is better
                   evidence than the generic one. */
                return await McpRuntimePrecondition.StatusAsync(dataService, resolved.ServerId, resolved.ServerName, Collector)
                    ?? McpHelpers.Status("unavailable", ClientTimeoutsRead.UnavailableMessage(resolved.ServerName, window, windowEnd, hours_back));
            }

            if (kind == ClientTimeoutsKind.Quiet)
            {
                return McpHelpers.Status("empty", ClientTimeoutsRead.EmptyMessage(resolved.ServerName, reading!, hours_back));
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                client_timeouts = ClientTimeoutsRead.Payload(reading!, window, samples, limit, 2000),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_client_timeouts", ex);
        }
    }
}
