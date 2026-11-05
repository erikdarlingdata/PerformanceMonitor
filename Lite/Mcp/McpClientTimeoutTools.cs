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

    [McpServerTool(Name = "get_client_timeouts"), Description("Gets client timeouts (queries a client cancelled or timed out) for one server over a time range: exact count and rate per minute, plus sampled top statements, databases, client apps and recent events. Samples are a subset, never the total. empty = collector ran, none counted; unavailable = no recent run; not_collected = engine without the collector; precondition = capture session missing. hours_back is whole hours (minimum 1): for a short alert window set as_of to its end and read sampled_events' event_time. <<GUIDE>> Reading guide for get_client_timeouts. count is exact: it is the sum of the attention collector's event_counter deltas, one per run, so it holds every client cancel or timeout the capture session counted, however many events the ring buffer kept. top_statements, top_databases, top_client_apps and sampled_events come from the ring buffer, which keeps at most 1,000 events and can miss some: they are a subset, they show what the timeouts looked like and never how many there were (samples_note says so on every answer). limit cuts only sampled_events (sampled_events_truncated says it did), never count or the top lists. rate_per_minute is count divided by minutes_covered, the minutes between the oldest and newest collector run the read used, not the whole hours_back window, so minutes the collector did not cover do not dilute the rate. Status words: empty means the collector ran across the window and its counter did not move, a true zero; unavailable means no recent successful run (the newest is outside the freshness window, or fewer than two runs), so no count is claimed, because an absent row is not a zero, and get_collection_health shows why; not_collected means the collector does not run on this server's engine, which is permanent and needs no fix; precondition means the collector's latest run recorded a missing or denied capture session, and the message names the fix. An attention is a client cancel or timeout, so a high rate is a client-side symptom (the client gave up waiting), not proof the server failed: look at top_statements and their waits next (get_wait_stats, get_waiting_tasks, get_active_queries).")]
    public static async Task<string> GetClientTimeouts(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Whole hours, minimum 1, no fractions. Default 1. For an alert's few minutes set as_of to its end and read event_time in sampled_events.")] int hours_back = 1,
        [Description("Sampled events to return, newest first. Default 20. Never cuts the count or top lists.")] int limit = 20,
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

            var unmet = await McpEngineCapability.NotCollectedStatusAsync(dataService, serverManager, resolved.ServerId, resolved.DisplayName, Collector);
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
                /* #7005: the window notice (an unavailable answer only; a precondition says its own cause). */
                return await McpQueryTools.WithEmptyWindowNoticeAsync(
                    await McpRuntimePrecondition.StatusAsync(dataService, resolved.ServerId, resolved.DisplayName, Collector)
                        ?? McpHelpers.Status("unavailable", ClientTimeoutsRead.UnavailableMessage(resolved.DisplayName, window, windowEnd, hours_back)),
                    dataService, QueryWindowRelation.AttentionEvents, resolved.ServerId, windowStart, windowEnd, "attention_events");
            }

            if (kind == ClientTimeoutsKind.Quiet)
            {
                return await McpQueryTools.WithEmptyWindowNoticeAsync(
                    McpHelpers.Status("empty", ClientTimeoutsRead.EmptyMessage(resolved.DisplayName, reading!, hours_back)),
                    dataService, QueryWindowRelation.AttentionEvents, resolved.ServerId, windowStart, windowEnd, "attention_events");
            }

            return await McpQueryTools.WithDataWindowNoticeAsync(JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                hours_back,
                client_timeouts = ClientTimeoutsRead.Payload(reading!, window, samples, limit, 2000),
            }, McpHelpers.JsonOptions),
                dataService, QueryWindowRelation.AttentionEvents, resolved.ServerId, windowStart, windowEnd, "attention_events");
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_client_timeouts", ex);
        }
    }
}
