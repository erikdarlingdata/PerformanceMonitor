/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// #9639: create and edit a notification route over MCP, the twins of Darling's <c>create_notification_route</c> and
/// <c>update_notification_route</c>. Both save through <see cref="DuckDbNotificationRouteStore.InsertAsync"/> and
/// <see cref="DuckDbNotificationRouteStore.UpdateAsync"/>, the calls the Notification Routes window's Save makes, so the store's own
/// rules (a match, one scope, * only with a scope), the Credential Manager writes and the route cache reload are the window's.
/// A destination (webhook URL or routing key) is stored in Credential Manager and is never in an answer or a log: an answer says
/// whether each channel is saved. The agent settings (level, limits) are not arguments: Lite has no operator token, a route with an
/// agent level of 1 or more is changed only in Settings (<see cref="AgentRouteRefusal"/>), and an edit keeps an agent level 0 route's
/// agent settings as they are.
/// </summary>
public sealed partial class McpAlertTools
{
    private const string RouteMatchHelp = "a family (performance, self-monitor, reports, agent-jobs, external), 'Analysis: <category>', 'Analysis: *' (every finding), an exact metric name, or * (scoped routes only)";

    private const string RouteDestinationsHelp =
        "A route's own destinations: teams_webhook_url, slack_webhook_url, generic_webhook_url (each an absolute http or https URL), " +
        "pagerduty_routing_key and email_recipients (comma separated addresses). A webhook URL or key is kept in Windows Credential Manager and cannot be read back: " +
        "an answer says whether each channel is saved and never shows the value; the recipient list is shown in full.";

    /// <summary>The destination values a call carries. Null or blank leaves a channel alone (on a create: none), <c>-</c> removes it (update only).</summary>
    internal sealed record RouteDestinationArgs(string? Teams, string? Slack, string? Generic, string? PagerDuty, string? Recipients)
    {
        internal static readonly RouteDestinationArgs None = new(null, null, null, null, null);
    }

    internal const string ClearMarker = "-";

    [McpServerTool(Name = "create_notification_route"), Description(
        "Write: creates a notification route, optionally with its own destinations (Teams, Slack, generic webhook, PagerDuty, email). With none, each channel goes where it went before (the next matching route, else Settings). Changes shared alert configuration; the next alert resolves against it. Returns created (the route, and for each destination set whether it is saved, never a URL or key) with the undo call, or invalid, or unavailable. Served only when mcp_allow_writes is true.<<GUIDE>>" +
        "Creates a route that decides WHICH alerts it covers and, when you pass them, where they go, through the same save as Settings > Manage Notification Routes. " + RouteDestinationsHelp + " " +
        "With no destination argument the route is stored with every destination empty, so it has none of its own and each channel goes where it went before: to the next less specific matching route that sets that channel, else to the Settings > Notifications channels. " +
        "What it can set: metric_match (" + RouteMatchHelp + "), a scope of one server or one tag (not both; * only with a scope), enabled, and the destinations. " +
        "scope_server_id and scope_tag_id are ids from get_notification_routes or list_servers and the tag reads; an id that names nothing is invalid. " +
        "Not set here: the agent channel (agent level and limits) and a generic webhook's signing key are set only in Settings > Manage Notification Routes, and Darling's send_resolutions, fallback_route_id, severities, report_notice and slack_bot_channel have no field in Lite. " +
        "Returns {status:\"created\", route:{...}, previous (null: there was no route before), destinations:{changed, now}, undo:{tool:\"delete_notification_route\", arguments:{route_id}}}, {status:\"invalid\", message} naming the argument, or {status:\"unavailable\"} while an earlier database reset's saved-settings restore is pending (it finishes at the next Lite start). Check it with get_notification_routes.")]
    public static async Task<string> CreateNotificationRoute(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("What the route matches: " + RouteMatchHelp + ".")] string metric_match,
        [Description("true (default) puts the route in force; false creates it disabled.")] bool enabled = true,
        [Description("Scope the route to this server's id (as get_notification_routes reports it). Omit for fleet-wide. Not with scope_tag_id.")] int? scope_server_id = null,
        [Description("Scope the route to this server tag id and every tag under it. Omit for fleet-wide. Not with scope_server_id.")] int? scope_tag_id = null,
        [Description("The route's own Teams webhook URL (absolute http or https). Kept in Windows Credential Manager; never shown back. Omit for none.")] string? teams_webhook_url = null,
        [Description("The route's own Slack webhook URL (absolute http or https). Kept in Windows Credential Manager; never shown back. Omit for none.")] string? slack_webhook_url = null,
        [Description("The route's own generic webhook URL (absolute http or https). Kept in Windows Credential Manager; never shown back. Omit for none.")] string? generic_webhook_url = null,
        [Description("The route's own PagerDuty Events API v2 routing key. Kept in Windows Credential Manager; never shown back. Omit for none.")] string? pagerduty_routing_key = null,
        [Description("The route's own email recipients, comma separated. Omit for none.")] string? email_recipients = null)
    {
        try
        {
            return await CreateRouteAsync(
                CreateRouteStore(dataService, serverManager),
                serverManager.GetAllServers(),
                await dataService.GetServerTagsAsync(),
                metric_match, enabled, scope_server_id, scope_tag_id,
                new RouteDestinationArgs(teams_webhook_url, slack_webhook_url, generic_webhook_url, pagerduty_routing_key, email_recipients));
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("create_notification_route", ex);
        }
    }

    internal static async Task<string> CreateRouteAsync(
        DuckDbNotificationRouteStore store,
        IReadOnlyCollection<ServerConnection> servers,
        IReadOnlyCollection<ServerTag> tags,
        string? metric_match,
        bool enabled,
        int? scope_server_id,
        int? scope_tag_id,
        RouteDestinationArgs destinations)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(metric_match))
            {
                return McpHelpers.Refusal("metric_match", "metric_match is required: " + RouteMatchHelp + ".");
            }

            var match = NotificationRouteKinds.MatchToSave(null, metric_match);
            if (RouteScopeRefusal(match, scope_server_id, scope_tag_id, servers, tags) is { } scopeRefusal)
            {
                return scopeRefusal;
            }

            if (ChangedDestinations(destinations, includeClears: true).FirstOrDefault(c => c.Typed == ClearMarker) is { Argument: not null } clear)
            {
                return McpHelpers.Refusal(clear.Argument, $"{clear.Argument} is - but a new route has no destination to remove: pass a value or omit it.");
            }

            foreach (var (value, argument) in DestinationUrls(destinations))
            {
                if (RouteUrlRefusal(value, argument) is { } urlRefusal)
                {
                    return urlRefusal;
                }
            }

            var route = new NotificationRoute(
                0,
                match,
                (destinations.Teams ?? "").Trim(),
                (destinations.Slack ?? "").Trim(),
                (destinations.Generic ?? "").Trim(),
                (destinations.PagerDuty ?? "").Trim(),
                (destinations.Recipients ?? "").Trim(),
                enabled)
            {
                ScopeServerId = scope_server_id,
                ScopeTagId = scope_tag_id,
            };

            int routeId;
            try
            {
                routeId = await store.InsertAsync(route);
            }
            catch (ArgumentException ex)
            {
                return McpHelpers.Refusal("route", RouteArgumentMessage(ex));
            }
            catch (RouteSecretException ex)
            {
                return McpHelpers.Refusal("destinations", ex.Message);
            }

            var stored = await store.GetAsync(routeId);
            var changed = ChangedDestinations(destinations, includeClears: false);
            return JsonSerializer.Serialize(new
            {
                status = "created",
                route = stored is null ? null : RoutePayload(stored, ServerNames(servers), tags.ToDictionary(t => t.Id, t => t.Name)),
                previous = (object?)null,
                destinations = changed.Count == 0 ? null : DestinationsPayload(changed, route, null),
                undo = UndoCall("delete_notification_route", new { route_id = routeId }),
            }, McpHelpers.JsonOptions);
        }
        catch (PendingRestoreException ex)
        {
            return McpHelpers.Status("unavailable", ex.Message);
        }
    }

    [McpServerTool(Name = "update_notification_route"), Description(
        "Write: edits a notification route by route_id: only the arguments you pass change: its match, scope, enabled flag and own destinations (Teams, Slack, generic webhook, PagerDuty, email). Changes shared alert configuration; the next alert resolves against it. Returns updated (the route, and each destination changed as saved or none, never a URL or key), previous and the undo call, or unchanged, invalid, not_found, refused or unavailable. A route with an agent level of 1 or more is refused. Served only when mcp_allow_writes is true.<<GUIDE>>" +
        "Edits what a route matches and applies to, and where it sends, through the same save as Settings > Manage Notification Routes. " + RouteDestinationsHelp + " " +
        "Omit an argument to keep it. Pass - in a destination argument to remove that destination. " +
        "Arguments: metric_match (" + RouteMatchHelp + "), enabled, scope (scope_server_id or scope_tag_id, the other is cleared; fleet_wide true clears both), plus the destination arguments. " +
        "The same rules the window applies hold: * needs a scope, a route is scoped to one server or one tag, a webhook is an absolute http or https URL. " +
        "A route with an agent level of 1 or more (it opens investigation requests) is changed only in Settings > Manage Notification Routes, so this tool refuses it, as set_notification_route_enabled does; an agent level 0 route keeps its agent settings. " +
        "A generic webhook's signing key is managed in Settings; Darling's send_resolutions, fallback_route_id, severities, report_notice and slack_bot_channel have no field in Lite. " +
        "Use set_notification_route_enabled to turn a route off and on without the rest. " +
        "The answer's previous is the route before the change and undo is the update_notification_route call that puts the match, scope and enabled flag back (and the email recipients, when changed). A webhook URL or key is never shown, so to put one back pass its earlier value again in its argument; destinations.previous says whether one was saved. " +
        "Returns {status:\"updated\", route, previous, destinations:{changed, now, previous}, undo}, {status:\"unchanged\", route, previous, undo} when nothing differs, {status:\"not_found\"}, {status:\"invalid\", message} naming the argument, {status:\"refused\"} for an agent route, or {status:\"unavailable\"} while an earlier database reset's saved-settings restore is pending (it finishes at the next Lite start).")]
    public static async Task<string> UpdateNotificationRoute(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The route_id to edit (from get_notification_routes).")] int route_id,
        [Description("New match: " + RouteMatchHelp + ". Omit to keep.")] string? metric_match = null,
        [Description("Put the route in force (true) or skip it (false). Omit to keep.")] bool? enabled = null,
        [Description("Scope the route to this server's id (clears a tag scope). Omit to keep.")] int? scope_server_id = null,
        [Description("Scope the route to this server tag id (clears a server scope). Omit to keep.")] int? scope_tag_id = null,
        [Description("true makes the route fleet-wide (clears the server or tag scope).")] bool? fleet_wide = null,
        [Description("The route's own Teams webhook URL (absolute http or https). Kept in Windows Credential Manager; never shown back. Omit to keep; - removes it.")] string? teams_webhook_url = null,
        [Description("The route's own Slack webhook URL (absolute http or https). Kept in Windows Credential Manager; never shown back. Omit to keep; - removes it.")] string? slack_webhook_url = null,
        [Description("The route's own generic webhook URL (absolute http or https). Kept in Windows Credential Manager; never shown back. Omit to keep; - removes it.")] string? generic_webhook_url = null,
        [Description("The route's own PagerDuty Events API v2 routing key. Kept in Windows Credential Manager; never shown back. Omit to keep; - removes it.")] string? pagerduty_routing_key = null,
        [Description("The route's own email recipients, comma separated. Omit to keep; - removes them.")] string? email_recipients = null)
    {
        try
        {
            return await UpdateRouteAsync(
                CreateRouteStore(dataService, serverManager),
                serverManager.GetAllServers(),
                await dataService.GetServerTagsAsync(),
                route_id, metric_match, enabled, scope_server_id, scope_tag_id, fleet_wide,
                new RouteDestinationArgs(teams_webhook_url, slack_webhook_url, generic_webhook_url, pagerduty_routing_key, email_recipients));
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("update_notification_route", ex);
        }
    }

    internal static async Task<string> UpdateRouteAsync(
        DuckDbNotificationRouteStore store,
        IReadOnlyCollection<ServerConnection> servers,
        IReadOnlyCollection<ServerTag> tags,
        int route_id,
        string? metric_match,
        bool? enabled,
        int? scope_server_id,
        int? scope_tag_id,
        bool? fleet_wide,
        RouteDestinationArgs destinations)
    {
        try
        {
            var existing = await store.GetAsync(route_id);
            if (existing is null)
            {
                return McpHelpers.Status("not_found", $"No notification route with route_id {route_id}.");
            }

            if (AgentRouteRefusal(existing, "update_notification_route") is { } agentRefusal)
            {
                return agentRefusal;
            }

            if (scope_server_id.HasValue && scope_tag_id.HasValue)
            {
                return McpHelpers.Refusal("scope_server_id", "Pass scope_server_id or scope_tag_id, not both.");
            }

            if (fleet_wide == true && (scope_server_id.HasValue || scope_tag_id.HasValue))
            {
                return McpHelpers.Refusal("fleet_wide", "fleet_wide true clears the scope: do not pass scope_server_id or scope_tag_id with it.");
            }

            var match = string.IsNullOrWhiteSpace(metric_match) ? existing.MetricMatch : NotificationRouteKinds.MatchToSave(null, metric_match);
            var serverScope = existing.ScopeServerId;
            var tagScope = existing.ScopeTagId;
            if (fleet_wide == true)
            {
                serverScope = null;
                tagScope = null;
            }
            else if (scope_server_id.HasValue)
            {
                serverScope = scope_server_id;
                tagScope = null;
            }
            else if (scope_tag_id.HasValue)
            {
                serverScope = null;
                tagScope = scope_tag_id;
            }

            /* Only a scope the call sets is checked against the known servers and tags: a route whose scope was removed since stays editable. */
            if (RouteScopeRefusal(match, serverScope, tagScope, servers, tags, checkExists: scope_server_id.HasValue || scope_tag_id.HasValue) is { } scopeRefusal)
            {
                return scopeRefusal;
            }

            foreach (var (value, argument) in DestinationUrls(destinations))
            {
                if (RouteUrlRefusal(value, argument) is { } urlRefusal)
                {
                    return urlRefusal;
                }
            }

            /* A channel the call leaves alone keeps what the window would pass back: the saved value, and the off flag of one that could
               not be read. A channel it sets or removes drops that flag. */
            var off = new HashSet<string>(existing.OffChannels ?? [], StringComparer.Ordinal);
            string Resolve(string? typed, string stored, string channel)
            {
                var text = (typed ?? "").Trim();
                if (text.Length == 0)
                {
                    return stored;
                }

                off.Remove(channel);
                if (text == ClearMarker)
                {
                    return string.Empty;
                }

                return text;
            }

            var teams = Resolve(destinations.Teams, existing.TeamsUrl, NotificationRouter.TeamsChannel);
            var slack = Resolve(destinations.Slack, existing.SlackUrl, NotificationRouter.SlackChannel);
            var generic = Resolve(destinations.Generic, existing.GenericUrl, NotificationRouter.GenericChannel);
            var pager = Resolve(destinations.PagerDuty, existing.PagerDutyRoutingKey, NotificationRouter.PagerDutyChannel);
            var recipientsText = (destinations.Recipients ?? "").Trim();
            var recipients = recipientsText.Length == 0 ? existing.SmtpRecipients : recipientsText == ClearMarker ? "" : recipientsText;

            var updated = existing with
            {
                MetricMatch = match,
                TeamsUrl = teams,
                SlackUrl = slack,
                GenericUrl = generic,
                PagerDutyRoutingKey = pager,
                SmtpRecipients = recipients,
                Enabled = enabled ?? existing.Enabled,
                ScopeServerId = serverScope,
                ScopeTagId = tagScope,
                OffChannels = off.Count == 0 ? null : off.ToList(),
            };

            var serverNames = ServerNames(servers);
            var tagNames = tags.ToDictionary(t => t.Id, t => t.Name);
            var undo = UndoCall("update_notification_route", RouteUndoArguments(existing, recipientsChanged: recipients != existing.SmtpRecipients));
            var changed = ChangedDestinations(destinations, includeClears: true);
            var same = updated.MetricMatch == existing.MetricMatch && updated.Enabled == existing.Enabled
                && updated.ScopeServerId == existing.ScopeServerId && updated.ScopeTagId == existing.ScopeTagId
                && updated.TeamsUrl == existing.TeamsUrl && updated.SlackUrl == existing.SlackUrl
                && updated.GenericUrl == existing.GenericUrl && updated.PagerDutyRoutingKey == existing.PagerDutyRoutingKey
                && updated.SmtpRecipients == existing.SmtpRecipients
                && (updated.OffChannels?.Count ?? 0) == (existing.OffChannels?.Count ?? 0);
            if (same)
            {
                return JsonSerializer.Serialize(new
                {
                    status = "unchanged",
                    route = RoutePayload(existing, serverNames, tagNames),
                    previous = RoutePayload(existing, serverNames, tagNames),
                    undo,
                }, McpHelpers.JsonOptions);
            }

            try
            {
                if (!await store.UpdateAsync(updated))
                {
                    return McpHelpers.Status("not_found", $"No notification route with route_id {route_id}.");
                }
            }
            catch (ArgumentException ex)
            {
                return McpHelpers.Refusal("route", RouteArgumentMessage(ex));
            }
            catch (RouteSecretException ex)
            {
                return McpHelpers.Refusal("destinations", ex.Message);
            }

            /* Re-read AFTER the write, so the reported row is the stored one. */
            var stored = await store.GetAsync(route_id);
            return JsonSerializer.Serialize(new
            {
                status = "updated",
                route = stored is null ? null : RoutePayload(stored, serverNames, tagNames),
                previous = RoutePayload(existing, serverNames, tagNames),
                destinations = changed.Count == 0 ? null : DestinationsPayload(changed, updated, existing),
                undo,
            }, McpHelpers.JsonOptions);
        }
        catch (PendingRestoreException ex)
        {
            return McpHelpers.Status("unavailable", ex.Message);
        }
    }

    /// <summary>The scope and match rules, in words that name the argument (the store's own checks run again at the save).</summary>
    private static string? RouteScopeRefusal(
        string match, int? serverId, int? tagId,
        IReadOnlyCollection<ServerConnection> servers, IReadOnlyCollection<ServerTag> tags, bool checkExists = true)
    {
        if (serverId.HasValue && tagId.HasValue)
        {
            return McpHelpers.Refusal("scope_server_id", "Pass scope_server_id or scope_tag_id, not both.");
        }

        if (NotificationRouteKinds.IsWildcard(match) && !serverId.HasValue && !tagId.HasValue)
        {
            return McpHelpers.Refusal("metric_match", "metric_match * needs scope_server_id or scope_tag_id: a fleet-wide route must name a metric or a family.");
        }

        if (!checkExists)
        {
            return null;
        }

        if (serverId is { } sid && !ServerNames(servers).ContainsKey(sid))
        {
            return McpHelpers.Refusal("scope_server_id", $"scope_server_id {sid} names no monitored server (see get_notification_routes or list_servers).");
        }

        if (tagId is { } tid && tags.All(t => t.Id != tid))
        {
            return McpHelpers.Refusal("scope_tag_id", $"scope_tag_id {tid} names no server tag.");
        }

        return null;
    }

    private static IEnumerable<(string? Value, string Argument)> DestinationUrls(RouteDestinationArgs d)
    {
        yield return (d.Teams, "teams_webhook_url");
        yield return (d.Slack, "slack_webhook_url");
        yield return (d.Generic, "generic_webhook_url");
    }

    /// <summary>A webhook argument must be an absolute http or https URL (or blank to leave it, or - to remove it). The message names the argument, never the value.</summary>
    private static string? RouteUrlRefusal(string? value, string argument)
    {
        var text = (value ?? "").Trim();
        if (text.Length == 0 || text == ClearMarker)
        {
            return null;
        }

        return Uri.TryCreate(text, UriKind.Absolute, out var uri) && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps)
            ? null
            : McpHelpers.Refusal(argument, $"{argument} must be an absolute http or https URL.");
    }

    /// <summary>The store's <see cref="ArgumentException"/> message without the "(Parameter 'route')" suffix the runtime appends.</summary>
    private static string RouteArgumentMessage(ArgumentException ex) =>
        ex.Message.Replace($" (Parameter '{ex.ParamName}')", "", StringComparison.Ordinal);

    /// <summary>The destination arguments the call carried: (argument name, channel). A clear (<c>-</c>) counts only on an update.</summary>
    private static List<(string Argument, string Channel, string Typed)> ChangedDestinations(RouteDestinationArgs d, bool includeClears)
    {
        var all = new (string Argument, string Channel, string? Typed)[]
        {
            ("teams_webhook_url", NotificationRouter.TeamsChannel, d.Teams),
            ("slack_webhook_url", NotificationRouter.SlackChannel, d.Slack),
            ("generic_webhook_url", NotificationRouter.GenericChannel, d.Generic),
            ("pagerduty_routing_key", NotificationRouter.PagerDutyChannel, d.PagerDuty),
            ("email_recipients", NotificationRouter.EmailChannel, d.Recipients),
        };
        return all
            .Where(a => !string.IsNullOrWhiteSpace(a.Typed) && (includeClears || a.Typed!.Trim() != ClearMarker))
            .Select(a => (a.Argument, a.Channel, a.Typed!.Trim()))
            .ToList();
    }

    /// <summary>What a destination is after a write, without showing it: "none", "saved", or the recipient list for email.</summary>
    private static string DescribeDestination(string channel, NotificationRoute route) => channel switch
    {
        _ when channel == NotificationRouter.TeamsChannel => route.TeamsUrl.Length > 0 ? "saved" : "none",
        _ when channel == NotificationRouter.SlackChannel => route.SlackUrl.Length > 0 ? "saved" : "none",
        _ when channel == NotificationRouter.GenericChannel => route.GenericUrl.Length > 0 ? "saved" : "none",
        _ when channel == NotificationRouter.PagerDutyChannel => route.PagerDutyRoutingKey.Length > 0 ? "saved" : "none",
        _ => route.SmtpRecipients.Length > 0 ? route.SmtpRecipients : "none",
    };

    private static object DestinationsPayload(List<(string Argument, string Channel, string Typed)> changed, NotificationRoute now, NotificationRoute? previous) => new
    {
        changed = changed.Select(c => c.Argument).ToList(),
        /* Darling's keys: teams, slack, generic, pagerduty, email. */
        now = changed.ToDictionary(c => c.Channel.ToLowerInvariant(), c => DescribeDestination(c.Channel, now)),
        previous = previous is null ? null : changed.ToDictionary(c => c.Channel.ToLowerInvariant(), c => DescribeDestination(c.Channel, previous)),
        note = "A webhook URL or key is kept in Windows Credential Manager and never shown: the answer says saved or none. The recipient list is shown in full.",
    };

    /// <summary>The update call that puts the match, scope and enabled flag back. A destination secret is not in it (it cannot be read back); the recipient list is, when the call changed it.</summary>
    private static Dictionary<string, object?> RouteUndoArguments(NotificationRoute before, bool recipientsChanged)
    {
        var args = new Dictionary<string, object?>
        {
            ["route_id"] = before.RouteId,
            ["metric_match"] = before.MetricMatch,
            ["enabled"] = before.Enabled,
        };
        if (before.ScopeServerId is { } sid)
        {
            args["scope_server_id"] = sid;
        }
        else if (before.ScopeTagId is { } tid)
        {
            args["scope_tag_id"] = tid;
        }
        else
        {
            args["fleet_wide"] = true;
        }

        if (recipientsChanged)
        {
            args["email_recipients"] = before.SmtpRecipients.Length == 0 ? ClearMarker : before.SmtpRecipients;
        }

        return args;
    }
}
