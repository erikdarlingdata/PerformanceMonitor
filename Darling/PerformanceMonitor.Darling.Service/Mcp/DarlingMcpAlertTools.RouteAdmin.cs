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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The alert admin writes an operator used to do only in the Viewer or the web dashboard (#6231 part C): create and edit a
/// notification route, dismiss alert history, and the restore that undoes a dismiss. A route is written through the store's own
/// <c>config.create_notification_route</c> and <c>config.update_notification_route</c> functions, because the <c>mcp</c> login holds
/// no INSERT on the table and no read or write on a destination column. MCP never sets or changes where a route sends: create makes a
/// route with every channel empty (it sends through the channels configured in Settings) and update changes the match, scope, enabled
/// flag, resolution override and fallback. An answer shows a route the way <c>get_notification_routes</c> does (channels by name). Every tool here is in
/// <see cref="DarlingMcpWritePolicy.WriteTools"/> (served only with <c>mcp.allowWrites</c> and a token) and in
/// <see cref="DarlingMcpWritePolicy.NeverAgentTools"/> (an agent working an alert can never call it).
/// </summary>
public sealed partial class DarlingMcpAlertTools
{
    private const string MatchHelp = "a family (performance, self-monitor, reports, agent-jobs, external), an analysis category ('Analysis: <category>'), an exact metric name, or * (every metric, scoped routes only)";

    /// <summary>The sentence both route tools carry: MCP never sets or changes where a route sends (#6231).</summary>
    private const string DestinationsInViewer = "Neither route tool takes or changes a destination (a webhook URL, a routing key or email recipients): those are set in the Viewer's Settings window (Notifications, routes).";

    [McpServerTool(Name = "create_notification_route"), Description(
        "Creates a notification route that sends through the channels configured in Settings (Notifications): the route is made with every channel empty, and an empty channel inherits the parent's. It takes no webhook URL, key or email recipients; the route's own destinations are set in the Viewer. Changes shared alert configuration the service delivers on, on its next sweep. Returns created (the route as get_notification_routes shows it) with the undo call, or invalid. It cannot set the agent channel: agent routes are changed in the Viewer." +
        "<<GUIDE>>" +
        "Creates a route that decides WHERE one family's alerts go and nothing about the destinations themselves: the route is stored with every destination empty, " +
        "so it sends to the channels configured in Settings > Notifications (the parent set), exactly as get_notification_routes explains inherit. " +
        DestinationsInViewer + " " +
        "What it can set: metric_match (" + MatchHelp + "), a scope of one server or one tag (not both; * only with a scope), enabled, " +
        "send_resolutions (inherit, always or never: the clear message to the channels this route resolves to) and fallback_route_id (an existing route). " +
        "The database role this tool runs as has no INSERT on the routes table and no read or write on a destination column: the row is written by the store's own " +
        "config.create_notification_route function, which never takes a destination. " +
        "scope_server_id and scope_tag_id are ids from get_notification_routes or the server and tag reads; an id that names nothing is invalid. " +
        "Returns {status:\"created\", route:{...}, sends_to, undo:{tool:\"delete_notification_route\", arguments:{route_id}}}; check it with get_notification_routes.")]
    public static async Task<string> CreateNotificationRoute(
        NpgsqlDataSource postgres,
        [Description("What the route matches: " + MatchHelp + ".")] string metric_match,
        [Description("true (default) puts the route in force; false creates it disabled.")] bool enabled = true,
        [Description("Scope the route to this registry server_id. Omit for fleet-wide. Not with scope_tag_id.")] int? scope_server_id = null,
        [Description("Scope the route to this server tag id and every tag under it. Omit for fleet-wide. Not with scope_server_id.")] int? scope_tag_id = null,
        [Description("inherit (default: each channel's own setting), always (send the clear message to the channels this route resolves to) or never.")] string? send_resolutions = null,
        [Description("The route_id of an existing route an expired investigation request goes to first. Omit for none.")] int? fallback_route_id = null,
        ModelContextProtocol.Server.McpServer? server = null,
        Microsoft.AspNetCore.Http.IHttpContextAccessor? accessor = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (ParseSendResolutions(send_resolutions, out var resolutions) is { } badResolutions)
            {
                return Outcome("invalid", badResolutions);
            }

            var match = NormalizeRouteMatch(metric_match);
            int routeId;
            try
            {
                await using var command = postgres.CreateCommand("SELECT config.create_notification_route($1, $2, $3, $4, $5, $6)");
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = match });
                command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabled });
                NotificationRouteWriter.AddNullableInt(command, scope_server_id);
                NotificationRouteWriter.AddNullableInt(command, scope_tag_id);
                command.Parameters.Add(resolutions.HasValue
                    ? new NpgsqlParameter<bool> { TypedValue = resolutions.Value }
                    : new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean });
                NotificationRouteWriter.AddNullableInt(command, fallback_route_id);
                routeId = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
            }
            catch (PostgresException ex) when (RouteFunctionOutcome(ex) is { } refusal)
            {
                return refusal;
            }

            LogRouteWrite(logger, "create_notification_route", CallerOf(server, accessor), routeId, "metric_match, enabled, scope, send_resolutions, fallback_route_id");
            var stored = (await new PgNotificationRouteStore(postgres).LoadSummariesAsync(cancellationToken)).FirstOrDefault(r => r.RouteId == routeId);
            return JsonSerializer.Serialize(new
            {
                status = "created",
                route = stored is null ? null : RoutePayload(stored),
                sends_to = "The channels configured in Settings > Notifications (the route's own destinations are empty, so it inherits them). Set the route's own destinations in the Viewer.",
                undo = new { tool = "delete_notification_route", arguments = new { route_id = routeId } },
                note = "The service picks the route up on its next collection sweep.",
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("create_notification_route", ex);
        }
    }

    [McpServerTool(Name = "update_notification_route"), Description(
        "Edits a notification route by route_id: only the fields you pass change, and never its destinations (webhook URLs, keys, email recipients), which are set in the Viewer. Changes shared alert configuration the service delivers on, on its next sweep. Returns updated (the route as get_notification_routes shows it), previous (the route before) and the undo call, or unchanged, invalid, or not_found. An agent route (agent_level 1 or more) is refused: it is changed in the Viewer." +
        "<<GUIDE>>" +
        "Edits what a route matches and applies to, never where it sends. " + DestinationsInViewer + " " +
        "Fields: metric_match (" + MatchHelp + "), enabled, scope (scope_server_id or scope_tag_id, the other is cleared; fleet_wide true clears both), " +
        "send_resolutions (inherit, always or never) and fallback_route_id (an existing route; clear_fallback true removes it). A field left out keeps its stored value. " +
        "The same rules the Viewer's route window applies hold: * needs a scope, a route is scoped to one server or one tag, a route is not its own fallback. " +
        "The database role this tool runs as can neither read nor write a destination column; the row is changed by the store's own config.update_notification_route function, " +
        "which re-reads the row under its lock and refuses an agent route inside the same transaction. " +
        "Use set_notification_route_enabled to turn a route off and on without the rest. " +
        "The answer's previous is the route before the change and undo is the update_notification_route call that puts the fields back. " +
        "Returns {status:\"updated\", route, previous, undo}, or {status:\"unchanged\"} when nothing differs.")]
    public static async Task<string> UpdateNotificationRoute(
        NpgsqlDataSource postgres,
        [Description("The route_id to edit (from get_notification_routes).")] int route_id,
        [Description("New match: " + MatchHelp + ". Omit to keep.")] string? metric_match = null,
        [Description("Put the route in force (true) or skip it (false). Omit to keep.")] bool? enabled = null,
        [Description("Scope the route to this registry server_id (clears a tag scope). Omit to keep.")] int? scope_server_id = null,
        [Description("Scope the route to this server tag id (clears a server scope). Omit to keep.")] int? scope_tag_id = null,
        [Description("true makes the route fleet-wide (clears the server or tag scope).")] bool? fleet_wide = null,
        [Description("inherit, always or never. Omit to keep.")] string? send_resolutions = null,
        [Description("The route_id of an existing route an expired investigation request goes to first. Omit to keep.")] int? fallback_route_id = null,
        [Description("true removes the fallback route.")] bool? clear_fallback = null,
        ModelContextProtocol.Server.McpServer? server = null,
        Microsoft.AspNetCore.Http.IHttpContextAccessor? accessor = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var store = new PgNotificationRouteStore(postgres);
            var existing = (await store.LoadSummariesAsync(cancellationToken)).FirstOrDefault(r => r.RouteId == route_id);
            if (existing is null)
            {
                return Outcome("not_found", $"No notification route with route_id {route_id}.");
            }

            if (AgentRouteRefusal(existing, "update_notification_route") is { } agentRefusal)
            {
                return agentRefusal;
            }

            if (scope_server_id.HasValue && scope_tag_id.HasValue)
            {
                return Outcome("invalid", "A route is scoped to one server or one tag, not both.");
            }

            var columns = new List<string>();
            var match = existing.MetricMatch;
            if (!string.IsNullOrWhiteSpace(metric_match) && NormalizeRouteMatch(metric_match) is var wanted && wanted != existing.MetricMatch)
            {
                match = wanted;
                columns.Add("metric_match");
            }

            var enabledNow = existing.Enabled;
            if (enabled.HasValue && enabled.Value != existing.Enabled)
            {
                enabledNow = enabled.Value;
                columns.Add("enabled");
            }

            var serverScope = existing.ScopeServerId;
            var tagScope = existing.ScopeTagId;
            if (fleet_wide == true)
            {
                serverScope = null;
                tagScope = null;
            }

            if (scope_server_id.HasValue)
            {
                serverScope = scope_server_id;
                tagScope = null;
            }
            else if (scope_tag_id.HasValue)
            {
                tagScope = scope_tag_id;
                serverScope = null;
            }

            if (serverScope != existing.ScopeServerId || tagScope != existing.ScopeTagId)
            {
                columns.Add("scope");
            }

            var resolutions = existing.SendResolutions;
            if (send_resolutions is not null)
            {
                if (ParseSendResolutions(send_resolutions, out var parsed) is { } badResolutions)
                {
                    return Outcome("invalid", badResolutions);
                }

                if (parsed != existing.SendResolutions)
                {
                    resolutions = parsed;
                    columns.Add("send_resolutions");
                }
            }

            var fallback = existing.FallbackRouteId;
            if (clear_fallback == true)
            {
                fallback = null;
            }
            else if (fallback_route_id.HasValue)
            {
                fallback = fallback_route_id;
            }

            if (fallback != existing.FallbackRouteId)
            {
                columns.Add("fallback_route_id");
            }

            if (columns.Count == 0)
            {
                return JsonSerializer.Serialize(new { status = "unchanged", route = RoutePayload(existing) }, McpHelpers.JsonOptions);
            }

            try
            {
                await using var command = postgres.CreateCommand("SELECT config.update_notification_route($1, $2, $3, $4, $5, $6, $7, $8)");
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = route_id });
                command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = columns.ToArray() });
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = match });
                command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabledNow });
                NotificationRouteWriter.AddNullableInt(command, serverScope);
                NotificationRouteWriter.AddNullableInt(command, tagScope);
                command.Parameters.Add(resolutions.HasValue
                    ? new NpgsqlParameter<bool> { TypedValue = resolutions.Value }
                    : new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean });
                NotificationRouteWriter.AddNullableInt(command, fallback);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (PostgresException ex) when (RouteFunctionOutcome(ex) is { } refusal)
            {
                return refusal;
            }

            LogRouteWrite(logger, "update_notification_route", CallerOf(server, accessor), route_id, string.Join(", ", columns));
            var stored = (await store.LoadSummariesAsync(cancellationToken)).FirstOrDefault(r => r.RouteId == route_id);
            return JsonSerializer.Serialize(new
            {
                status = "updated",
                route = stored is null ? null : RoutePayload(stored),
                previous = RoutePayload(existing),
                changed = columns,
                undo = new { tool = "update_notification_route", arguments = UndoArguments(existing) },
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("update_notification_route", ex);
        }
    }

    /// <summary>The refusal a route function raised (its own DL400, DL403 or DL404 sentence), or null for any other error.</summary>
    private static string? RouteFunctionOutcome(PostgresException ex) => ex.SqlState switch
    {
        "DL400" or "DL404" => Outcome("invalid", ex.MessageText),
        "DL403" => Outcome("refused", ex.MessageText),
        _ => null,
    };

    /// <summary>The caller of a route write: the MCP client's own name and version, else the request's User-Agent token, else unknown.</summary>
    private static string CallerOf(ModelContextProtocol.Server.McpServer? server, Microsoft.AspNetCore.Http.IHttpContextAccessor? accessor)
    {
        try
        {
            var clientInfo = server?.ClientInfo;
            string? userAgent = null;
            if (string.IsNullOrWhiteSpace(clientInfo?.Name))
            {
                userAgent = accessor?.HttpContext?.Request.Headers.UserAgent.ToString();
            }

            return ReadCaller.OfMcpClient(clientInfo?.Name, clientInfo?.Version, userAgent);
        }
        catch (Exception)
        {
            return ReadCaller.Unknown;
        }
    }

    /// <summary>One line per route write (#6231): the tool, the caller, the route and the names of the fields changed. Never a value.</summary>
    internal static void LogRouteWrite(ILogger? logger, string tool, string caller, int routeId, string fields) =>
        logger?.LogInformation("MCP route write {Tool} by {Caller}: route {RouteId}, fields changed: {Fields}", tool, caller, routeId, fields);

    /// <summary>The update call that puts a route's fields back to <paramref name="existing"/>'s values.</summary>
    private static Dictionary<string, object?> UndoArguments(PgNotificationRouteStore.RouteSummary existing)
    {
        var args = new Dictionary<string, object?>
        {
            ["route_id"] = existing.RouteId,
            ["metric_match"] = existing.MetricMatch,
            ["enabled"] = existing.Enabled,
            ["send_resolutions"] = existing.SendResolutions switch { true => "always", false => "never", _ => "inherit" },
        };
        if (existing.ScopeServerId.HasValue)
        {
            args["scope_server_id"] = existing.ScopeServerId.Value;
        }
        else if (existing.ScopeTagId.HasValue)
        {
            args["scope_tag_id"] = existing.ScopeTagId.Value;
        }
        else
        {
            args["fleet_wide"] = true;
        }

        if (existing.FallbackRouteId.HasValue)
        {
            args["fallback_route_id"] = existing.FallbackRouteId.Value;
        }
        else
        {
            args["clear_fallback"] = true;
        }

        return args;
    }

    /// <summary>The match as the Viewer's dialog stores it: an exact name that is really a family spelled by hand is the family.</summary>
    private static string NormalizeRouteMatch(string? match)
    {
        var text = (match ?? "").Trim();
        return AlertFamily.NormalizeFamily(text) ?? text;
    }

    private static string? ParseSendResolutions(string? text, out bool? value)
    {
        value = null;
        switch ((text ?? "").Trim().ToLowerInvariant())
        {
            case "":
            case "inherit":
                return null;
            case "always":
                value = true;
                return null;
            case "never":
                value = false;
                return null;
            default:
                return "send_resolutions is inherit, always or never.";
        }
    }

    // ── Alert history dismiss ──

    [McpServerTool(Name = "dismiss_alert_history"), Description(
        "Dismisses alert history rows: they leave get_alert_history's default read (include_dismissed true still shows them) and nothing else changes. The rows stay stored. Pass the rows to dismiss as alert_time, server_id and metric_name exactly as get_alert_history returned them. Returns dismissed (counts: requested, dismissed, already_dismissed, unknown) with the undo call, restore_alert_history. Dismissing is a flag, not a delete, so it is fully undone." +
        "<<GUIDE>>" +
        "Dismisses alert history rows the way the web dashboard's Dismiss does (the same store call): sets the dismissed flag on each named row and no other column. " +
        "alerts_json is a JSON array of up to 1000 objects {\"alert_time\":\"...\",\"server_id\":1,\"metric_name\":\"...\"}, copied from get_alert_history rows. " +
        "A key that names no row counts under unknown and is not an error; a row already dismissed counts under already_dismissed. " +
        "Nothing is deleted. The undo is restore_alert_history with the rows this call actually dismissed (rows that were already dismissed are not in it, so the undo does not bring back a dismissal somebody else made). " +
        "Check with get_alert_history (the rows are gone from the default read) and include_dismissed true.")]
    public static async Task<string> DismissAlertHistory(
        NpgsqlDataSource postgres,
        [Description("JSON array of {alert_time, server_id, metric_name} copied from get_alert_history rows. At most 1000.")] string alerts_json,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (ParseAlertKeys(alerts_json, out var keys) is { } refusal)
            {
                return Outcome("invalid", refusal);
            }

            var willChange = await AlertDismissStore.KeysInStateAsync(postgres, keys, dismissed: false, McpCommandDeadlines.ReadSeconds, cancellationToken);
            var outcome = await AlertDismissStore.DismissKeysAsync(postgres, keys, McpCommandDeadlines.ReadSeconds, cancellationToken);
            LogAlertWrite(logger, "dismiss_alert_history", outcome.Dismissed);
            return JsonSerializer.Serialize(new
            {
                status = "dismissed",
                requested = outcome.Requested,
                dismissed = outcome.Dismissed,
                already_dismissed = Math.Max(0, outcome.Known - outcome.Dismissed),
                unknown = Math.Max(0, outcome.Requested - outcome.Known),
                undo = new { tool = "restore_alert_history", arguments = new { alerts_json = KeysJson(willChange) } },
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("dismiss_alert_history", ex);
        }
    }

    [McpServerTool(Name = "restore_alert_history"), Description(
        "Brings dismissed alert history rows back into get_alert_history's default read: the undo of dismiss_alert_history. Pass the rows as alert_time, server_id and metric_name. Returns restored (counts: requested, restored, not_dismissed_or_unknown) with the call that dismisses them again." +
        "<<GUIDE>>" +
        "Clears the dismissed flag on each named row and no other column. alerts_json has the same shape dismiss_alert_history takes, " +
        "and its answer's undo carries exactly the rows to pass. A row that was not dismissed, or that names no row, is counted and left alone. " +
        "Check with get_alert_history.")]
    public static async Task<string> RestoreAlertHistory(
        NpgsqlDataSource postgres,
        [Description("JSON array of {alert_time, server_id, metric_name}. At most 1000.")] string alerts_json,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (ParseAlertKeys(alerts_json, out var keys) is { } refusal)
            {
                return Outcome("invalid", refusal);
            }

            var willChange = await AlertDismissStore.KeysInStateAsync(postgres, keys, dismissed: true, McpCommandDeadlines.ReadSeconds, cancellationToken);
            var outcome = await AlertDismissStore.RestoreKeysAsync(postgres, keys, McpCommandDeadlines.ReadSeconds, cancellationToken);
            LogAlertWrite(logger, "restore_alert_history", outcome.Dismissed);
            return JsonSerializer.Serialize(new
            {
                status = "restored",
                requested = outcome.Requested,
                restored = outcome.Dismissed,
                not_dismissed_or_unknown = Math.Max(0, outcome.Requested - outcome.Dismissed),
                undo = new { tool = "dismiss_alert_history", arguments = new { alerts_json = KeysJson(willChange) } },
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("restore_alert_history", ex);
        }
    }

    /// <summary>One Information line per dismiss or restore call: the tool, who called, and how many rows changed.</summary>
    private static void LogAlertWrite(ILogger? logger, string tool, int rows) =>
        logger?.LogInformation("MCP write {Tool} by {Caller}: {Rows} alert history rows changed",
            tool, ReadCaller.AmbientMcpCaller ?? ReadCaller.Unknown, rows);

    /// <summary>Parses the keys with the web dismiss route's own parser, so the two surfaces refuse a body in the same words.</summary>
    private static string? ParseAlertKeys(string? alertsJson, out List<AlertDismissKey> keys)
    {
        keys = new List<AlertDismissKey>();
        JsonNode? node;
        try
        {
            node = JsonNode.Parse(alertsJson ?? "");
        }
        catch (JsonException)
        {
            return "alerts_json must be a JSON array of {alert_time, server_id, metric_name}.";
        }

        if (node is not JsonArray array)
        {
            return "alerts_json must be a JSON array of {alert_time, server_id, metric_name}.";
        }

        var body = new JsonObject { ["alerts"] = array.DeepClone() }.ToJsonString();
        return DarlingWebEndpoints.ParseDismissBody(body, out keys, out var refusal) ? null : refusal;
    }

    private static string KeysJson(IEnumerable<AlertDismissKey> keys) =>
        JsonSerializer.Serialize(
            keys.Select(k => new { alert_time = k.AlertTime.ToString("o", System.Globalization.CultureInfo.InvariantCulture), server_id = k.ServerId, metric_name = k.MetricName }),
            McpHelpers.JsonOptions);
}
