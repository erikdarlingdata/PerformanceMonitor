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
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The alert admin writes an operator used to do only in the Viewer or the web dashboard (#6231 part C): create and edit a
/// notification route, dismiss alert history, and the restore that undoes a dismiss. A route is saved by
/// <see cref="NotificationRouteWriter"/>, the same code the Viewer's route window runs, so a route stored here is the same row and
/// is refused with the same sentences. A webhook value is sealed with the service's own key and is never echoed: an answer shows a
/// route the way <c>get_notification_routes</c> does (channels by name, no URL or key). Every tool here is in
/// <see cref="DarlingMcpWritePolicy.WriteTools"/> (served only with <c>mcp.allowWrites</c> and a token) and in
/// <see cref="DarlingMcpWritePolicy.NeverAgentTools"/> (an agent working an alert can never call it).
/// </summary>
public sealed partial class DarlingMcpAlertTools
{
    private const string MatchHelp = "a family (performance, self-monitor, reports, agent-jobs, external), an analysis category ('Analysis: <category>'), an exact metric name, or * (every metric, scoped routes only)";

    [McpServerTool(Name = "create_notification_route"), Description(
        "Creates a notification route: where one family of alerts posts (Teams, Slack, generic webhook, PagerDuty, email). Changes shared alert configuration the service delivers on, on its next sweep. A webhook URL or key is a secret: it is sealed for storage and never shown again, here or anywhere. Returns created (the route as get_notification_routes shows it, no URL or key) with the undo call, or invalid or refused with the reason. It cannot set the agent channel: agent routes are changed in the Viewer." +
        "<<GUIDE>>" +
        "Creates a notification route with the same validation the Viewer's route window runs (the same code): a match, at least one destination, " +
        "a scope of one server or one tag (not both), and * only with a scope. metric_match is " + MatchHelp + ". " +
        "A destination left out inherits the parent channel's (get_notification_routes explains resolution). " +
        "The webhook values (teams_url, slack_url, generic_url, pagerduty_routing_key) are bearer secrets: they are sealed with the service's password key " +
        "exactly as the Viewer seals them, so the running service can open them and nothing can read them back, and no answer or log carries one. " +
        "If the service has no usable password key the call is refused and nothing is written. A URL is not probed or sent to: " +
        "this tool adds no outbound request and the same checks the Viewer makes apply (it makes none on the URL's shape; a bad URL fails when the route sends). " +
        "scope_server_id and scope_tag_id are ids from get_notification_routes or the server and tag reads; an id that names nothing is invalid. " +
        "send_resolutions is inherit (the channel's own setting), always or never. fallback_route_id must be an existing route. " +
        "Returns {status:\"created\", route:{...}, undo:{tool:\"delete_notification_route\", arguments:{route_id}}}; check it with get_notification_routes.")]
    public static async Task<string> CreateNotificationRoute(
        NpgsqlDataSource postgres,
        [Description("What the route matches: " + MatchHelp + ".")] string metric_match,
        [Description("Teams webhook URL (a secret: sealed, never shown). Omit to inherit the parent channel's.")] string? teams_url = null,
        [Description("Slack webhook URL (a secret: sealed, never shown). Omit to inherit.")] string? slack_url = null,
        [Description("Generic webhook URL (a secret: sealed, never shown). Omit to inherit.")] string? generic_url = null,
        [Description("PagerDuty routing key (a secret: sealed, never shown). Omit to inherit.")] string? pagerduty_routing_key = null,
        [Description("Email recipients, comma separated (not a secret: shown by get_notification_routes). Omit to inherit.")] string? smtp_recipients = null,
        [Description("true (default) puts the route in force; false creates it disabled.")] bool enabled = true,
        [Description("Scope the route to this registry server_id. Omit for fleet-wide. Not with scope_tag_id.")] int? scope_server_id = null,
        [Description("Scope the route to this server tag id and every tag under it. Omit for fleet-wide. Not with scope_server_id.")] int? scope_tag_id = null,
        [Description("inherit (default: each channel's own setting), always (send the clear message to this route's destinations) or never.")] string? send_resolutions = null,
        [Description("The route_id of an existing route an expired investigation request goes to first. Omit for none.")] int? fallback_route_id = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var draft = new NotificationRouteDraft
            {
                MetricMatch = NormalizeRouteMatch(metric_match),
                TeamsUrl = (teams_url ?? "").Trim(),
                SlackUrl = (slack_url ?? "").Trim(),
                GenericUrl = (generic_url ?? "").Trim(),
                PagerDutyRoutingKey = (pagerduty_routing_key ?? "").Trim(),
                SmtpRecipients = (smtp_recipients ?? "").Trim(),
                Enabled = enabled,
                ScopeServerId = scope_server_id,
                ScopeTagId = scope_tag_id,
                FallbackRouteId = fallback_route_id,
            };
            if (ParseSendResolutions(send_resolutions, out var resolutions) is { } badResolutions)
            {
                return Outcome("invalid", badResolutions);
            }

            draft.SendResolutions = resolutions;
            if (await ScopeRefusalAsync(postgres, draft, cancellationToken) is { } scopeRefusal)
            {
                return Outcome("invalid", scopeRefusal);
            }

            var routeId = 0;
            NotificationRouteRefusedException? refusedWrite = null;
            try
            {
                routeId = await SaveRouteAsync(postgres, draft, null, cancellationToken);
            }
            catch (NotificationRouteRefusedException ex)
            {
                refusedWrite = ex;
            }

            if (refusedWrite is not null)
            {
                return Outcome(refusedWrite.Kind == NotificationRouteRefusalKind.Value ? "refused" : "invalid", refusedWrite.Message);
            }

            var stored = (await new PgNotificationRouteStore(postgres).LoadSummariesAsync(cancellationToken)).FirstOrDefault(r => r.RouteId == routeId);
            return JsonSerializer.Serialize(new
            {
                status = "created",
                route = stored is null ? null : RoutePayload(stored),
                undo = new { tool = "delete_notification_route", arguments = new { route_id = routeId } },
                note = "The service picks the route up on its next collection sweep. Destination values are sealed and are not shown.",
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("create_notification_route", ex);
        }
    }

    [McpServerTool(Name = "update_notification_route"), Description(
        "Edits a notification route by route_id: only the fields you pass change. Changes shared alert configuration the service delivers on, on its next sweep. A webhook URL or key you pass replaces the saved one (sealed, never shown again); pass - to remove one. Returns updated (the route as get_notification_routes shows it, no URL or key), previous (the route before, same masking) and the undo call, or invalid, refused, or not_found. An agent route (agent_level 1 or more) is refused: it is changed in the Viewer." +
        "<<GUIDE>>" +
        "Edits a notification route with the same validation the Viewer's route window runs. A field left out keeps its stored value, including a saved " +
        "destination (those are sealed and are never read back). A webhook value passed is sealed and replaces the saved one; the text - removes it. " +
        "smtp_recipients - clears the address list. To move a route to the whole fleet pass fleet_wide true; pass scope_server_id or scope_tag_id to scope it (the other is cleared). " +
        "A route whose generic URL has a signing key cannot have that URL changed: remove the key in the Viewer first (the Viewer says the same). " +
        "Use set_notification_route_enabled to turn a route off and on, and set_notification_route_fallback for its fallback. " +
        "The answer's previous is the route before the change; undo is the update_notification_route call that puts the non-secret fields back. " +
        "A destination you replaced or removed is a secret and cannot be restored by a call: undo says so and you must pass the old value again. " +
        "Returns {status:\"updated\", route, previous, undo}, or {status:\"unchanged\"} when nothing differs.")]
    public static async Task<string> UpdateNotificationRoute(
        NpgsqlDataSource postgres,
        [Description("The route_id to edit (from get_notification_routes).")] int route_id,
        [Description("New match: " + MatchHelp + ". Omit to keep.")] string? metric_match = null,
        [Description("Teams webhook URL (a secret: sealed, never shown). Omit to keep the saved one, - to remove it.")] string? teams_url = null,
        [Description("Slack webhook URL (a secret). Omit to keep, - to remove.")] string? slack_url = null,
        [Description("Generic webhook URL (a secret). Omit to keep, - to remove.")] string? generic_url = null,
        [Description("PagerDuty routing key (a secret). Omit to keep, - to remove.")] string? pagerduty_routing_key = null,
        [Description("Email recipients, comma separated. Omit to keep, - to clear.")] string? smtp_recipients = null,
        [Description("Put the route in force (true) or skip it (false). Omit to keep.")] bool? enabled = null,
        [Description("Scope the route to this registry server_id (clears a tag scope). Omit to keep.")] int? scope_server_id = null,
        [Description("Scope the route to this server tag id (clears a server scope). Omit to keep.")] int? scope_tag_id = null,
        [Description("true makes the route fleet-wide (clears the server or tag scope).")] bool? fleet_wide = null,
        [Description("inherit, always or never. Omit to keep.")] string? send_resolutions = null,
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

            var before = await NotificationRouteWriter.ReadStoredAsync(postgres, route_id, McpCommandDeadlines.ReadSeconds, cancellationToken);
            if (before is null)
            {
                return Outcome("not_found", $"No notification route with route_id {route_id}.");
            }

            var draft = before.Clone();
            if (!string.IsNullOrWhiteSpace(metric_match))
            {
                draft.MetricMatch = NormalizeRouteMatch(metric_match);
            }

            draft.TeamsUrl = Replace(teams_url, before.TeamsUrl);
            draft.SlackUrl = Replace(slack_url, before.SlackUrl);
            draft.GenericUrl = Replace(generic_url, before.GenericUrl);
            draft.PagerDutyRoutingKey = Replace(pagerduty_routing_key, before.PagerDutyRoutingKey);
            if (smtp_recipients is not null && smtp_recipients.Trim().Length > 0)
            {
                draft.SmtpRecipients = smtp_recipients.Trim() == NotificationRouteWriter.ClearMarker ? "" : smtp_recipients.Trim();
            }

            if (enabled.HasValue)
            {
                draft.Enabled = enabled.Value;
            }

            if (fleet_wide == true)
            {
                draft.ScopeServerId = null;
                draft.ScopeTagId = null;
            }

            if (scope_server_id.HasValue)
            {
                draft.ScopeServerId = scope_server_id;
                draft.ScopeTagId = scope_tag_id;
            }
            else if (scope_tag_id.HasValue)
            {
                draft.ScopeTagId = scope_tag_id;
                draft.ScopeServerId = null;
            }

            if (send_resolutions is not null)
            {
                if (ParseSendResolutions(send_resolutions, out var resolutions) is { } badResolutions)
                {
                    return Outcome("invalid", badResolutions);
                }

                draft.SendResolutions = resolutions;
            }

            if (await ScopeRefusalAsync(postgres, draft, cancellationToken) is { } scopeRefusal)
            {
                return Outcome("invalid", scopeRefusal);
            }

            NotificationRouteRefusedException? refusedWrite = null;
            try
            {
                await SaveRouteAsync(postgres, draft, before, cancellationToken);
            }
            catch (NotificationRouteRefusedException ex)
            {
                refusedWrite = ex;
            }

            if (refusedWrite is not null)
            {
                return Outcome(refusedWrite.Kind == NotificationRouteRefusalKind.Value ? "refused" : "invalid", refusedWrite.Message);
            }

            var stored = (await store.LoadSummariesAsync(cancellationToken)).FirstOrDefault(r => r.RouteId == route_id);
            var secretChanged = !string.IsNullOrWhiteSpace(teams_url) || !string.IsNullOrWhiteSpace(slack_url)
                || !string.IsNullOrWhiteSpace(generic_url) || !string.IsNullOrWhiteSpace(pagerduty_routing_key);
            return JsonSerializer.Serialize(new
            {
                status = "updated",
                route = stored is null ? null : RoutePayload(stored),
                previous = RoutePayload(existing),
                undo = new
                {
                    tool = "update_notification_route",
                    arguments = UndoArguments(existing, before),
                    caveat = secretChanged
                        ? "A destination changed here is a secret and is not kept: this call restores the non-secret fields only. Pass the old URL or key again to restore it."
                        : null,
                },
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("update_notification_route", ex);
        }
    }

    /// <summary>An omitted or blank value keeps the stored text; anything else (including the clear marker) is the new value.</summary>
    private static string Replace(string? typed, string stored) => string.IsNullOrWhiteSpace(typed) ? stored : typed.Trim();

    /// <summary>The update call that puts a route's non-secret fields back to <paramref name="existing"/>'s values.</summary>
    private static Dictionary<string, object?> UndoArguments(PgNotificationRouteStore.RouteSummary existing, NotificationRouteDraft before)
    {
        var args = new Dictionary<string, object?>
        {
            ["route_id"] = existing.RouteId,
            ["metric_match"] = existing.MetricMatch,
            ["enabled"] = existing.Enabled,
            ["smtp_recipients"] = string.IsNullOrWhiteSpace(before.SmtpRecipients) ? NotificationRouteWriter.ClearMarker : before.SmtpRecipients,
            ["send_resolutions"] = before.SendResolutions switch { true => "always", false => "never", _ => "inherit" },
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

    /// <summary>A scope id that names no server or tag would store an orphan route that matches nothing: said before the write.</summary>
    private static async Task<string?> ScopeRefusalAsync(NpgsqlDataSource postgres, NotificationRouteDraft draft, CancellationToken cancellationToken)
    {
        if (draft.ScopeServerId is { } serverId && draft.ScopeTagId is null)
        {
            await using var command = postgres.CreateCommand("SELECT 1 FROM config_monitored_servers WHERE server_id = $1");
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            if (await command.ExecuteScalarAsync(cancellationToken) is null)
            {
                return $"No monitored server has server_id {serverId}.";
            }
        }

        if (draft.ScopeTagId is { } tagId && draft.ScopeServerId is null)
        {
            await using var command = postgres.CreateCommand("SELECT 1 FROM server_tags WHERE id = $1");
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
            if (await command.ExecuteScalarAsync(cancellationToken) is null)
            {
                return $"No server tag has id {tagId}.";
            }
        }

        return null;
    }

    /// <summary>A test's own key ring for the calls in its async flow (the tools take the service's <see cref="DarlingPasswordKey.Current"/>
    /// otherwise). Never set outside tests.</summary>
    internal static readonly AsyncLocal<IPasswordKeyRing?> RingForTests = new();

    /// <summary>Saves the draft through the shared writer, sealing webhook values with the service's own key.</summary>
    internal static async Task<int> SaveRouteAsync(
        NpgsqlDataSource postgres, NotificationRouteDraft draft, NotificationRouteDraft? stored, CancellationToken cancellationToken,
        IPasswordKeyRing? ring = null)
    {
        ring ??= RingForTests.Value ?? DarlingPasswordKey.Current;
        var status = ring.Status;
        Func<string, PasswordBinding, string>? seal = status.CanSeal
            ? (text, binding) => NotificationRouteWriter.SealValue(text, binding, ring.Seal)
            : null;
        var parent = await NotificationRouteWriter.ReadProxiesAsync(postgres, McpCommandDeadlines.ReadSeconds, cancellationToken);
        return await NotificationRouteWriter.SaveAsync(
            postgres, draft, stored, parent, seal, status.Reason ?? NotificationRouteWriter.NoKeyText, McpCommandDeadlines.ReadSeconds, cancellationToken);
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
