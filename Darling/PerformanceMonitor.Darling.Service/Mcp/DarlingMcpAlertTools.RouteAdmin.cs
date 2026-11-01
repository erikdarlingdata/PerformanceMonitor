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
/// notification route, dismiss alert history, and the restore that undoes a dismiss. A route is written through the store's own
/// <c>config.create_notification_route</c>, <c>config.update_notification_route</c> and <c>config.set_notification_route_destinations</c>
/// functions, because the <c>mcp</c> login holds no INSERT on the table and no read or write on a destination column. Both tools can set
/// a route's own destinations (Slack, Teams, a generic webhook, PagerDuty, email recipients), with the Viewer's own rules
/// (<see cref="NotificationRouteWriter"/>): the service seals each webhook value for the route's own row, in the transaction that creates
/// or edits the route. Update changes the match or scope only on a route with no explicit agent setting; the store function refuses the
/// rest, in plain words. An answer shows a route the way <c>get_notification_routes</c> does (channels by name) and a destination as saved
/// or none (with the host for Teams and Slack only) and the recipient list in full, never a URL or key. Every tool here is in
/// <see cref="DarlingMcpWritePolicy.WriteTools"/> (served only with <c>mcp.allowWrites</c> and a token) and in
/// <see cref="DarlingMcpWritePolicy.NeverAgentTools"/> (an agent working an alert can never call it).
/// </summary>
public sealed partial class DarlingMcpAlertTools
{
    private const string MatchHelp = "a family (performance, self-monitor, reports, agent-jobs, external), 'Analysis: <category>', an exact metric name, or * (scoped routes only)";

    [McpServerTool(Name = "create_notification_route"), Description(
        "Creates a notification route, optionally with its own destinations (Slack, Teams, generic webhook, PagerDuty, email). With none, each channel goes where it went before (the next matching route, else Settings). Changes shared alert configuration; the service delivers on it at its next sweep. Returns created (the route with its recipient list in full, and for each destination set whether it is saved: the host for Teams and Slack, never a URL or key) with the undo call, or invalid. Only an operator token sets the agent fields (levels 0 to 3)." +
        "<<GUIDE>>" +
        "Creates a route that decides WHICH alerts it covers and, when you pass them, where they go. " + DestinationsHelp + " " +
        "With no destination field the route is stored with every destination empty, so it has none of its own and each channel goes where it went before: to the next less specific matching route that sets that channel, else to the Settings > Notifications channels. " +
        "What it can set: metric_match (" + MatchHelp + "), a scope of one server or one tag (not both; * only with a scope), enabled, " +
        "send_resolutions (inherit, always or never: the clear message to the channels this route resolves to), fallback_route_id (an existing route), severities (" + SeveritiesHelp + ") and report_notice (" + ReportNoticeHelp + "). " +
        "The database role this tool runs as has no INSERT on the routes table and no read or write on a destination column: the route is written by the store's own " +
        "config.create_notification_route function and its destinations by config.set_notification_route_destinations, in one transaction, so a destination that is refused leaves no route behind. " +
        "A destination is sealed for this route's own row, so it cannot be copied to another route. scope_server_id and scope_tag_id are ids from get_notification_routes or the server and tag reads; an id that names nothing is invalid. " +
        "Returns {status:\"created\", route:{...}, previous (null: there was no route before), destinations:{changed, now}, sends_to, undo:{tool:\"delete_notification_route\", arguments:{route_id}}}; check it with get_notification_routes.")]
    public static Task<string> CreateNotificationRoute(
        NpgsqlDataSource postgres,
        [Description("What the route matches: " + MatchHelp + ".")] string metric_match,
        [Description("true (default) puts the route in force; false creates it disabled.")] bool enabled = true,
        [Description("Scope the route to this registry server_id. Omit for fleet-wide. Not with scope_tag_id.")] int? scope_server_id = null,
        [Description("Scope the route to this server tag id and every tag under it. Omit for fleet-wide. Not with scope_server_id.")] int? scope_tag_id = null,
        [Description("inherit (default: each channel's own setting), always (send the clear message to the channels this route resolves to) or never.")] string? send_resolutions = null,
        [Description("The route_id of an existing route an expired investigation request goes to first. Omit for none.")] int? fallback_route_id = null,
        [Description("Which alerts the route takes by severity: critical, warning or both (default, case-insensitive). An alert that is not Critical counts as Warning.")] string? severities = null,
        [Description("The route's own Teams webhook URL (absolute http or https). Sealed for this route; never shown back. Omit for none.")] string? teams_webhook_url = null,
        [Description("The route's own Slack webhook URL (absolute http or https). Sealed for this route; never shown back. Omit for none.")] string? slack_webhook_url = null,
        [Description("The route's own generic webhook URL (absolute http or https). Sealed for this route; never shown back. Omit for none.")] string? generic_webhook_url = null,
        [Description("The route's own PagerDuty Events API v2 routing key. Sealed for this route; never shown back. Omit for none.")] string? pagerduty_routing_key = null,
        [Description("The route's own email recipients, comma separated plain addresses. Omit for none.")] string? email_recipients = null,
        [Description("Operator token only: the agent level, 0 (Off), 1 (Investigate), 2 (Act) or 3 (Act on servers). Omit to keep.")] int? agent_level = null,
        [Description("Operator token only: requests per hour, 1 to 60. Omit to keep.")] int? agent_max_per_hour = null,
        [Description("Operator token only: requests per day, 1 to 500. Omit to keep.")] int? agent_max_per_day = null,
        [Description("Operator token only: lease minutes, 5 to 120. Omit to keep.")] int? agent_lease_minutes = null,
        [Description("Operator token only: expiry minutes, 15 to 1440. Omit to keep.")] int? agent_expiry_minutes = null,
        [Description("Whether the route posts a chat notice when an agent files a report: off (default), needs_person_or_change or all (case-insensitive).")] string? report_notice = null,
        DarlingMcpServedPolicy? served = null,
        ILogger? logger = null,
        PostgresConfig? postgresConfig = null,
        CancellationToken cancellationToken = default) =>
        CreateNotificationRouteAsync(
            postgres, metric_match, enabled, scope_server_id, scope_tag_id, send_resolutions, fallback_route_id,
            new RouteDestinationRequest(teams_webhook_url, slack_webhook_url, generic_webhook_url, pagerduty_routing_key, email_recipients),
            DarlingPasswordKey.Current, logger, cancellationToken,
            new RouteAgentRequest(agent_level, agent_max_per_hour, agent_max_per_day, agent_lease_minutes, agent_expiry_minutes),
            served?.Policy.OperatorActor, StoreRolesAreRunByOperator(postgresConfig), severities, report_notice);

    /// <summary>The create: the route and its destinations in one transaction (a sealed value is bound to the route's id, so the id comes first).</summary>
    internal static async Task<string> CreateNotificationRouteAsync(
        NpgsqlDataSource postgres, string metric_match, bool enabled, int? scope_server_id, int? scope_tag_id, string? send_resolutions,
        int? fallback_route_id, RouteDestinationRequest destinations, IPasswordKeyRing ring, ILogger? logger, CancellationToken cancellationToken,
        RouteAgentRequest? agent = null, string? operatorActor = null, bool selfManagedStore = true, string? severities = null, string? reportNotice = null)
    {
        try
        {
            if (ParseSendResolutions(send_resolutions, out var resolutions) is { } badResolutions)
            {
                return Outcome("invalid", badResolutions);
            }

            if (ParseSeverities(severities, out var severitiesWord) is { } badSeverities)
            {
                return Outcome("invalid", badSeverities);
            }

            if (ParseReportNotice(reportNotice, out var reportNoticeWord) is { } badReportNotice)
            {
                return Outcome("invalid", badReportNotice);
            }

            var match = NormalizeRouteMatch(metric_match);
            agent ??= RouteAgentRequest.None;
            if (agent.Any)
            {
                var draft = new NotificationRouteDraft
                {
                    MetricMatch = match,
                    ScopeServerId = scope_server_id,
                    ScopeTagId = scope_tag_id,
                    FallbackRouteId = fallback_route_id,
                    TeamsUrl = DestinationStandIn,
                };
                if (AgentFieldRefusal(agent, draft, operatorActor, "create_notification_route") is { } agentRefusal)
                {
                    return agentRefusal;
                }
            }

            int routeId;
            RouteDestinationChange? change = null;
            try
            {
                var proxies = destinations.Any
                    ? await NotificationRouteWriter.ReadProxiesAsync(postgres, McpCommandDeadlines.ReadSeconds, cancellationToken)
                    : new WebhookProxies("", "", "", "");
                await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
                const string createSql = "SELECT config.create_notification_route($1, $2, $3, $4, $5, $6, $7, $8)";
                await using (var command = new NpgsqlCommand(createSql, connection) { Transaction = transaction })
                {
                    command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                    command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = match });
                    command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabled });
                    NotificationRouteWriter.AddNullableInt(command, scope_server_id);
                    NotificationRouteWriter.AddNullableInt(command, scope_tag_id);
                    command.Parameters.Add(resolutions.HasValue
                        ? new NpgsqlParameter<bool> { TypedValue = resolutions.Value }
                        : new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean });
                    NotificationRouteWriter.AddNullableInt(command, fallback_route_id);
                    command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = severitiesWord });
                    command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = reportNoticeWord });
                    routeId = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
                }

                if (destinations.Any)
                {
                    change = await ApplyRouteDestinationsAsync(connection, transaction, routeId, destinations, proxies, ring, cancellationToken, operatorActor);
                }

                if (agent.Any)
                {
                    await SetRouteAgentAsync(connection, transaction, routeId, operatorActor!, agent, null, created: true, cancellationToken);
                }

                await transaction.CommitAsync(cancellationToken);
            }
            catch (PostgresException ex) when (RouteFunctionOutcome(ex, selfManagedStore) is { } refusal)
            {
                return refusal;
            }
            catch (NotificationRouteRefusedException ex)
            {
                return Outcome("invalid", ex.Message);
            }

            var fields = "metric_match, enabled, scope, send_resolutions, fallback_route_id, severities, report_notice" + (change is { Fields.Count: > 0 } ? ", " + DestinationFieldNames(change) : "")
                + (agent.Any ? ", " + string.Join(", ", agent.Columns) : "");
            LogRouteWrite(logger, "create_notification_route", CallerOf(), routeId, fields);
            var stored = (await new PgNotificationRouteStore(postgres).LoadSummariesAsync(cancellationToken)).FirstOrDefault(r => r.RouteId == routeId);
            return JsonSerializer.Serialize(new
            {
                status = "created",
                route = RouteWritePayload(stored),
                previous = (object?)null,
                destinations = DestinationsPayload(change),
                sends_to = change is { Fields.Count: > 0 }
                    ? "The route sends to the destinations set on it. A channel it sets nothing for goes where it went before: to the next matching route that sets that channel, else to the channels configured in Settings > Notifications."
                    : "The route has no destinations of its own, so each channel goes where it went before: to the next matching route that sets that channel, else to the channels configured in Settings > Notifications. Set the route's own destinations with update_notification_route or in the Viewer.",
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
        "Edits a notification route by route_id: only the fields you pass change: its match and scope, enabled flag, resolution override, fallback and own destinations (Slack, Teams, generic webhook, PagerDuty, email). Changes shared alert configuration the service delivers on. Returns updated (the route with its recipient list in full, and each destination changed as saved or none, with the host for Teams and Slack and never a URL or key), previous and the undo call, or unchanged, invalid, not_found or refused. An operator token changes any route, agent fields (levels 0 to 3) included." +
        "<<GUIDE>>" +
        "Edits what a route matches and applies to, and where it sends. " + DestinationsHelp + " " +
        "An operator token changes any route, an agent route included: its match, scope, enabled flag, send_resolutions, fallback, destinations and agent fields. A change to the agent fields, or to the match, scope or enabled flag of a route with an agent setting, is journaled as route_agent_changed under operator: and the token's name. " +
        "A route's own destinations do not stop a change to what it matches. " +
        "Fields: metric_match (" + MatchHelp + "), enabled, scope (scope_server_id or scope_tag_id, the other is cleared; fleet_wide true clears both), " +
        "send_resolutions (inherit, always or never), severities (" + SeveritiesHelp + "), report_notice (" + ReportNoticeHelp + ") and fallback_route_id (an existing route; clear_fallback true removes it), plus the destination fields. A field left out keeps its stored value. " +
        "An operator token sets agent_level 0 to 3, or clear_agent_level true to put the level back to Inherit (null: the next level decides), not both. Clearing the level keeps the four limits, as the Viewer's Inherit does. " +
        "The same rules the Viewer's route window applies hold: * needs a scope, a route is scoped to one server or one tag, a route is not its own fallback, a webhook is an absolute http or https URL, recipients are plain addresses. " +
        "A route whose generic webhook has a signing key keeps its generic URL until the key is removed (remove_webhook_signing_key, or the Viewer). " +
        "The database role this tool runs as can neither read nor write a destination column; the row is changed by the store's own config.update_notification_route and config.set_notification_route_destinations functions in one transaction, " +
        "which re-read the row under its lock and refuse a change to an agent route from any caller that is not an operator token. " +
        "Use set_notification_route_enabled to turn a route off and on without the rest. " +
        "The answer's previous is the route before the change and undo is the update_notification_route call that puts the match, scope, enabled flag, resolution override and fallback back. " +
        "For an operator token undo puts the agent fields back too: agent_level and the four limits, or clear_agent_level true when the level was Inherit. " +
        "A webhook URL or key is never shown, so to put one back pass its earlier value again in its field; destinations.previous says whether one was saved (the host for Teams and Slack). The earlier recipient list is shown, and undo.arguments carries it as email_recipients. " +
        "Returns {status:\"updated\", route, previous, destinations:{changed, now, previous}, undo}, or {status:\"unchanged\", route, previous, undo} when nothing differs.")]
    public static Task<string> UpdateNotificationRoute(
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
        [Description("Which alerts the route takes by severity: critical, warning or both (case-insensitive). Omit to keep.")] string? severities = null,
        [Description("The route's own Teams webhook URL (absolute http or https). Sealed for this route; never shown back. Omit to keep; - removes it.")] string? teams_webhook_url = null,
        [Description("The route's own Slack webhook URL (absolute http or https). Sealed for this route; never shown back. Omit to keep; - removes it.")] string? slack_webhook_url = null,
        [Description("The route's own generic webhook URL (absolute http or https). Sealed for this route; never shown back. Omit to keep; - removes it.")] string? generic_webhook_url = null,
        [Description("The route's own PagerDuty Events API v2 routing key. Sealed for this route; never shown back. Omit to keep; - removes it.")] string? pagerduty_routing_key = null,
        [Description("The route's own email recipients, comma separated plain addresses. Omit to keep; - removes them.")] string? email_recipients = null,
        [Description("Operator token only: the agent level, 0 (Off), 1 (Investigate), 2 (Act) or 3 (Act on servers). Omit to keep.")] int? agent_level = null,
        [Description("Operator token only: requests per hour, 1 to 60. Omit to keep.")] int? agent_max_per_hour = null,
        [Description("Operator token only: requests per day, 1 to 500. Omit to keep.")] int? agent_max_per_day = null,
        [Description("Operator token only: lease minutes, 5 to 120. Omit to keep.")] int? agent_lease_minutes = null,
        [Description("Operator token only: expiry minutes, 15 to 1440. Omit to keep.")] int? agent_expiry_minutes = null,
        [Description("Operator token only: true puts agent_level back to Inherit (null), so the next level decides. The limits are kept. Not with agent_level.")] bool? clear_agent_level = null,
        [Description("Whether the route posts a chat notice when an agent files a report: off, needs_person_or_change or all (case-insensitive). Omit to keep.")] string? report_notice = null,
        DarlingMcpServedPolicy? served = null,
        ILogger? logger = null,
        PostgresConfig? postgresConfig = null,
        CancellationToken cancellationToken = default) =>
        UpdateNotificationRouteAsync(
            postgres, route_id, metric_match, enabled, scope_server_id, scope_tag_id, fleet_wide, send_resolutions, fallback_route_id, clear_fallback,
            new RouteDestinationRequest(teams_webhook_url, slack_webhook_url, generic_webhook_url, pagerduty_routing_key, email_recipients),
            DarlingPasswordKey.Current, logger, cancellationToken,
            new RouteAgentRequest(agent_level, agent_max_per_hour, agent_max_per_day, agent_lease_minutes, agent_expiry_minutes, clear_agent_level == true),
            served?.Policy.OperatorActor, StoreRolesAreRunByOperator(postgresConfig), severities, report_notice);

    /// <summary>The edit: the match, scope and flag columns and the destinations in one transaction.</summary>
    internal static async Task<string> UpdateNotificationRouteAsync(
        NpgsqlDataSource postgres, int route_id, string? metric_match, bool? enabled, int? scope_server_id, int? scope_tag_id, bool? fleet_wide,
        string? send_resolutions, int? fallback_route_id, bool? clear_fallback, RouteDestinationRequest destinations, IPasswordKeyRing ring,
        ILogger? logger, CancellationToken cancellationToken, RouteAgentRequest? agent = null, string? operatorActor = null,
        bool selfManagedStore = true, string? severities = null, string? reportNotice = null)
    {
        try
        {
            var store = new PgNotificationRouteStore(postgres);
            var existing = (await store.LoadSummariesAsync(cancellationToken)).FirstOrDefault(r => r.RouteId == route_id);
            if (existing is null)
            {
                return Outcome("not_found", $"No notification route with route_id {route_id}.");
            }

            agent ??= RouteAgentRequest.None;
            if (AgentRouteRefusal(existing, "update_notification_route", operatorActor) is { } agentRefusal)
            {
                return agentRefusal;
            }

            if (agent.Any && operatorActor is null)
            {
                return AgentFieldsNeedOperator("update_notification_route");
            }

            if (agent.ClearLevel && agent.Level.HasValue)
            {
                return Outcome("invalid", "Pass agent_level to set a level, or clear_agent_level true to put it back to Inherit, not both.");
            }

            /* Only the agent fields that differ from the stored row are written, so an undo that carries all five (L5 of the #6593 review)
               changes nothing it does not have to, and a call that changes nothing still answers unchanged. */
            agent = agent.Narrowed(existing);

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

            var severitiesNow = existing.Severities;
            if (severities is not null)
            {
                if (ParseSeverities(severities, out var parsedSeverities) is { } badSeverities)
                {
                    return Outcome("invalid", badSeverities);
                }

                if (parsedSeverities != existing.Severities)
                {
                    severitiesNow = parsedSeverities;
                    columns.Add("severities");
                }
            }

            var reportNoticeNow = existing.ReportNotice;
            if (reportNotice is not null)
            {
                if (ParseReportNotice(reportNotice, out var parsedReportNotice) is { } badReportNotice)
                {
                    return Outcome("invalid", badReportNotice);
                }

                if (parsedReportNotice != existing.ReportNotice)
                {
                    reportNoticeNow = parsedReportNotice;
                    columns.Add("report_notice");
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

            /* #6542 part 2: config.update_notification_route changes an agent route for an operator-class actor, so the fallback always goes
               through it, with the match, scope and flags. */
            if (fallback != existing.FallbackRouteId)
            {
                columns.Add("fallback_route_id");
            }

            /* The Viewer journals a match, scope or enabled change to a route with an agent setting (NotificationRouteWriter.AgentChange), so an
               operator's change to one is journaled too, by the agent function, with the before row the update function returns. */
            var journalsMatchOrFlag = operatorActor is not null && existing.AgentLevel is not null
                && columns.Any(c => c is "metric_match" or "scope" or "enabled" or "severities" or "report_notice");

            if (agent.Any)
            {
                var draft = new NotificationRouteDraft
                {
                    RouteId = route_id,
                    MetricMatch = match,
                    ScopeServerId = serverScope,
                    ScopeTagId = tagScope,
                    FallbackRouteId = fallback,
                    AgentLevel = existing.AgentLevel,
                    AgentMaxPerHour = existing.AgentMaxPerHour,
                    AgentMaxPerDay = existing.AgentMaxPerDay,
                    AgentLeaseMinutes = existing.AgentLeaseMinutes,
                    AgentExpiryMinutes = existing.AgentExpiryMinutes,
                    TeamsUrl = DestinationStandIn,
                };
                if (AgentFieldRefusal(agent, draft, operatorActor, "update_notification_route") is { } agentFieldRefusal)
                {
                    return agentFieldRefusal;
                }
            }

            if (columns.Count == 0 && !destinations.Any && !agent.Any)
            {
                return JsonSerializer.Serialize(new
                {
                    status = "unchanged",
                    route = RouteWritePayload(existing),
                    previous = RouteWritePayload(existing),
                    undo = UndoCall("update_notification_route", UndoArguments(existing, null, operatorActor is not null)),
                }, McpHelpers.JsonOptions);
            }

            RouteDestinationChange? change = null;
            try
            {
                var proxies = destinations.Any
                    ? await NotificationRouteWriter.ReadProxiesAsync(postgres, McpCommandDeadlines.ReadSeconds, cancellationToken)
                    : new WebhookProxies("", "", "", "");
                await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
                string? before = null;
                if (columns.Count > 0)
                {
                    const string updateSql = "SELECT config.update_notification_route($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)::text";
                    await using var command = new NpgsqlCommand(updateSql, connection) { Transaction = transaction };
                    command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                    command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = route_id });
                    command.Parameters.Add(ActorParameter(operatorActor));
                    command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = columns.ToArray() });
                    command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = match });
                    command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabledNow });
                    NotificationRouteWriter.AddNullableInt(command, serverScope);
                    NotificationRouteWriter.AddNullableInt(command, tagScope);
                    command.Parameters.Add(resolutions.HasValue
                        ? new NpgsqlParameter<bool> { TypedValue = resolutions.Value }
                        : new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean });
                    NotificationRouteWriter.AddNullableInt(command, fallback);
                    command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = severitiesNow });
                    command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = reportNoticeNow });
                    before = await command.ExecuteScalarAsync(cancellationToken) as string;
                }

                if (destinations.Any)
                {
                    change = await ApplyRouteDestinationsAsync(connection, transaction, route_id, destinations, proxies, ring, cancellationToken, operatorActor);
                }

                /* After the update, with the row it returned as the journal's before: one call that changes metric_match and agent_level journals
                   "changed metric_match, agent_level", as the Viewer's save does. */
                if (agent.Any || journalsMatchOrFlag)
                {
                    await SetRouteAgentAsync(connection, transaction, route_id, operatorActor!, agent, fallback, created: false, cancellationToken, before);
                }

                await transaction.CommitAsync(cancellationToken);
            }
            catch (PostgresException ex) when (RouteFunctionOutcome(ex, selfManagedStore) is { } refusal)
            {
                return refusal;
            }
            catch (NotificationRouteRefusedException ex)
            {
                return Outcome("invalid", ex.Message);
            }

            var changedFields = columns.Concat(change is { Fields.Count: > 0 } ? change.Fields : Array.Empty<string>()).Concat(agent.Columns).ToList();
            LogRouteWrite(logger, "update_notification_route", CallerOf(), route_id, string.Join(", ", changedFields));
            var stored = (await store.LoadSummariesAsync(cancellationToken)).FirstOrDefault(r => r.RouteId == route_id);
            return JsonSerializer.Serialize(new
            {
                status = "updated",
                route = RouteWritePayload(stored),
                previous = RouteWritePayload(existing),
                changed = changedFields,
                destinations = DestinationsPayload(change),
                undo = new
                {
                    tool = "update_notification_route",
                    arguments = UndoArguments(existing, change, operatorActor is not null),
                    destinations = change is { Fields.Count: > 0 }
                        ? "A webhook URL or key is never shown. To put one back, pass its earlier value again in the same field (destinations.previous says whether one was saved). The earlier recipient list is in the arguments as email_recipients."
                        : null,
                },
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("update_notification_route", ex);
        }
    }

    /// <summary>The agent fields a route write carries (#6542 part 2): each null keeps the stored value. The fallback goes through
    /// config.update_notification_route with the match, scope and flags. <paramref name="ClearLevel"/> (L5 of the #6593 review) puts the
    /// level back to Inherit (NULL) and keeps the limits, as the Viewer's route window saves an Inherit.</summary>
    internal sealed record RouteAgentRequest(int? Level, int? PerHour, int? PerDay, int? LeaseMinutes, int? ExpiryMinutes, bool ClearLevel = false)
    {
        internal static readonly RouteAgentRequest None = new(null, null, null, null, null);

        internal bool HasAgentField => Level.HasValue || ClearLevel || PerHour.HasValue || PerDay.HasValue || LeaseMinutes.HasValue || ExpiryMinutes.HasValue;

        /// <summary>This request without the fields that already hold the stored value.</summary>
        internal RouteAgentRequest Narrowed(PgNotificationRouteStore.RouteSummary stored) => new(
            Level == stored.AgentLevel ? null : Level,
            PerHour == stored.AgentMaxPerHour ? null : PerHour,
            PerDay == stored.AgentMaxPerDay ? null : PerDay,
            LeaseMinutes == stored.AgentLeaseMinutes ? null : LeaseMinutes,
            ExpiryMinutes == stored.AgentExpiryMinutes ? null : ExpiryMinutes,
            ClearLevel && stored.AgentLevel is not null);

        internal bool Any => HasAgentField;

        /// <summary>The columns this request names, in the agent function's words.</summary>
        internal IReadOnlyList<string> Columns
        {
            get
            {
                var names = new List<string>();
                if (Level.HasValue || ClearLevel) names.Add("agent_level");
                if (PerHour.HasValue) names.Add("agent_max_per_hour");
                if (PerDay.HasValue) names.Add("agent_max_per_day");
                if (LeaseMinutes.HasValue) names.Add("agent_lease_minutes");
                if (ExpiryMinutes.HasValue) names.Add("agent_expiry_minutes");
                return names;
            }
        }
    }

    /// <summary>A destination stand-in for <see cref="NotificationRouteWriter.Validate"/>: a route made over MCP may have no destination of its
    /// own (each channel goes where it went before), so the Viewer's "at least one destination" rule does not apply here. The destinations are
    /// checked by their own path.</summary>
    private const string DestinationStandIn = "(checked separately)";

    /// <summary>The refusal for agent fields from a caller that is not a named operator token (the legacy token keeps today's rule).</summary>
    internal static string AgentFieldsNeedOperator(string tool) =>
        Outcome("refused", $"{tool} sets a route's agent fields only for an operator token (a named operator token, or the single token with mcp.allowWrites). Set them in the Viewer's Settings window or on the web dashboard.");

    /// <summary>
    /// The check an operator token's agent fields get before anything is written (#6542 part 2): a named operator token only (MCP is full
    /// admin, so it sets levels 0 to 3 and changes a route at any level), then <see cref="NotificationRouteWriter.Validate"/> (the Viewer's save rules) over the route as it would be.
    /// Null when the write may go on. The agent function checks the same again under the row's lock.
    /// </summary>
    internal static string? AgentFieldRefusal(RouteAgentRequest agent, NotificationRouteDraft draft, string? operatorActor, string tool)
    {
        if (operatorActor is null)
        {
            return AgentFieldsNeedOperator(tool);
        }

        draft.AgentLevel = agent.ClearLevel ? null : agent.Level ?? draft.AgentLevel;
        draft.AgentMaxPerHour = agent.PerHour ?? draft.AgentMaxPerHour;
        draft.AgentMaxPerDay = agent.PerDay ?? draft.AgentMaxPerDay;
        draft.AgentLeaseMinutes = agent.LeaseMinutes ?? draft.AgentLeaseMinutes;
        draft.AgentExpiryMinutes = agent.ExpiryMinutes ?? draft.AgentExpiryMinutes;
        return NotificationRouteWriter.Validate(draft) is { } invalid ? Outcome("invalid", invalid) : null;
    }

    /// <summary>Writes the agent fields through <c>config.set_notification_route_agent</c> on the write's own transaction. The function
    /// journals <c>route_agent_changed</c> as <c>operator:&lt;token name&gt;</c> in the same transaction (never the token's value).</summary>
    /// <para><paramref name="before"/> is the row <c>config.update_notification_route</c> returned in the same transaction (null when it was not
    /// called), so the journal's before and its reason cover a match, scope or enabled change made in the same call (#6542 part 2, L2).</para>
    private static async Task SetRouteAgentAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int routeId, string operatorActor,
        RouteAgentRequest agent, int? fallback, bool created, CancellationToken cancellationToken, string? before = null)
    {
        const string sql = "SELECT config.set_notification_route_agent($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)";
        await using var command = new NpgsqlCommand(sql, connection) { Transaction = transaction };
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = operatorActor });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = agent.Columns.ToArray() });
        NotificationRouteWriter.AddNullableInt(command, agent.Level);
        NotificationRouteWriter.AddNullableInt(command, agent.PerHour);
        NotificationRouteWriter.AddNullableInt(command, agent.PerDay);
        NotificationRouteWriter.AddNullableInt(command, agent.LeaseMinutes);
        NotificationRouteWriter.AddNullableInt(command, agent.ExpiryMinutes);
        NotificationRouteWriter.AddNullableInt(command, fallback);
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = created });
        command.Parameters.Add(before is null
            ? new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text }
            : new NpgsqlParameter<string> { TypedValue = before });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>The actor a route function is passed: the operator-class token's <c>operator:</c> actor, or SQL NULL for any other caller,
    /// which the function holds to the person-only rules for a route with an agent setting.</summary>
    private static NpgsqlParameter ActorParameter(string? operatorActor) => operatorActor is null
        ? new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text }
        : new NpgsqlParameter<string> { TypedValue = operatorActor };


    /// <summary>The route's recipient list as stored now, read on the write's own transaction (the <c>mcp</c> login may read that column).</summary>
    private static async Task<string> ReadRecipientsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int routeId, CancellationToken cancellationToken)
    {
        const string readSql = "SELECT smtp_recipients FROM config.config_notification_routes WHERE route_id = $1";
        await using var command = new NpgsqlCommand(readSql, connection) { Transaction = transaction };
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        return await command.ExecuteScalarAsync(cancellationToken) as string ?? "";
    }

    /// <summary>The fixed answer when the store has no route, signing-key or default-channel function the calling role may run: a
    /// self-managed store whose roles were provisioned by an older build (3.12 added and changed these functions, and only
    /// provision-roles.sql creates them). Never PostgreSQL's own text. The web dashboard's route save does not use it: it writes
    /// the table on its own login and maps a 42501 itself (<c>DarlingWebAgentSetup</c>).</summary>
    internal const string RouteStoreNeedsRolesText =
        "This store's roles predate this route, signing key or notification channel write: re-run " + DarlingManagedRoles.ProvisionRolesScriptPath + " on the store, then try again.";

    /// <summary>Whether the operator runs provision-roles.sql on this store: false on the managed store and on the compose
    /// distribution's own store (the MCP host connects as a role the service provisioned, this start or an earlier one), where
    /// the script is never run by hand. True on a bring-your-own store, in a container or not, and for a host with no config.</summary>
    internal static bool StoreRolesAreRunByOperator(PostgresConfig? postgresConfig) =>
        RolesAreRunByOperator(
            DarlingWebStoreWrites.IsManaged(postgresConfig), DarlingStoreLogins.McpLoginIsComposeStoreRole());

    /// <summary>The decision, pure. The compose store is told by the login the MCP host resolved, not by whether this start
    /// provisioned the roles: provisioning that failed or was refused still leaves the host on the role's credential from an
    /// earlier start, and nobody runs the script on that store either.</summary>
    internal static bool RolesAreRunByOperator(bool managedStore, bool mcpLoginIsComposeStoreRole) =>
        !managedStore && !mcpLoginIsComposeStoreRole;

    /// <summary>The refusal a route function raised (its own DL400, DL403 or DL404 sentence), the re-run sentence when the store lacks
    /// the function (42883) or, on a self-managed store, the role may not run it (42501), or null for any other error. A 42501 on a managed
    /// or compose store gets no re-run advice (nobody runs the script there) and keeps the answer the tools gave before.</summary>
    private static string? RouteFunctionOutcome(PostgresException ex, bool selfManagedStore = true) => ex.SqlState switch
    {
        "DL400" or "DL404" => Outcome("invalid", ex.MessageText),
        "DL403" => Outcome("refused", ex.MessageText),
        "42883" => Outcome("refused", RouteStoreNeedsRolesText),
        "42501" when selfManagedStore => Outcome("refused", RouteStoreNeedsRolesText),
        _ => null,
    };

    /// <summary>Who made a route write, for the log: the token's server-side identity, then the name the client reported for itself
    /// (<see cref="ReadCaller.WriteIdentity"/>). The tool-call filter sets both once per call.</summary>
    private static string CallerOf() => ReadCaller.WriteIdentity();

    /// <summary>One line per route write (#6231): the tool, the token and client identity, the route and the names of the fields changed. Never a value.</summary>
    internal static void LogRouteWrite(ILogger? logger, string tool, string caller, int routeId, string fields) =>
        logger?.LogInformation("MCP route write {Tool} by {Identity}: route {RouteId}, fields changed: {Fields}", tool, caller, routeId, fields);

    /// <summary>The update call that puts a route's fields back to <paramref name="existing"/>'s values, and the recipient list back when the call changed it (<c>-</c> removes a list that was empty).
    /// With <paramref name="agentFields"/> (an operator token, the only caller that may set them) it puts the agent level and the four limits back too,
    /// with clear_agent_level true when the level was Inherit (L5 of the #6593 review). Any other caller cannot change them, so its undo leaves them out.</summary>
    private static Dictionary<string, object?> UndoArguments(PgNotificationRouteStore.RouteSummary existing, RouteDestinationChange? change, bool agentFields)
    {
        var args = new Dictionary<string, object?>
        {
            ["route_id"] = existing.RouteId,
            ["metric_match"] = existing.MetricMatch,
            ["enabled"] = existing.Enabled,
            ["send_resolutions"] = existing.SendResolutions switch { true => "always", false => "never", _ => "inherit" },
            ["severities"] = existing.Severities,
            ["report_notice"] = existing.ReportNotice,
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

        if (agentFields)
        {
            if (existing.AgentLevel is { } level)
            {
                args["agent_level"] = level;
            }
            else
            {
                args["clear_agent_level"] = true;
            }

            args["agent_max_per_hour"] = existing.AgentMaxPerHour;
            args["agent_max_per_day"] = existing.AgentMaxPerDay;
            args["agent_lease_minutes"] = existing.AgentLeaseMinutes;
            args["agent_expiry_minutes"] = existing.AgentExpiryMinutes;
        }

        if (change?.PreviousRecipients is { } previousRecipients)
        {
            args["email_recipients"] = previousRecipients.Trim().Length == 0 ? NotificationRouteWriter.ClearMarker : previousRecipients;
        }

        return args;
    }

    /// <summary>The match as the Viewer's dialog stores it: an exact name that is really a family spelled by hand is the family.</summary>
    private static string NormalizeRouteMatch(string? match)
    {
        var text = (match ?? "").Trim();
        return AlertFamily.NormalizeFamily(text) ?? text;
    }

    private const string SeveritiesHelp = "critical, warning or both; both is the default and takes every alert. An alert that is not Critical counts as Warning. A route whose filter excludes an alert does not match it, and matching goes on to the next route and ends at the default channels";

    /// <summary>The severity filter word (#6928): critical, warning or both, any case. Blank is both. Anything else is refused with the sentence returned.</summary>
    private static string? ParseSeverities(string? text, out string word)
    {
        word = NotificationRouteKinds.SeveritiesBoth;
        var trimmed = (text ?? "").Trim().ToLowerInvariant();
        if (trimmed.Length == 0)
        {
            return null;
        }

        if (trimmed is NotificationRouteKinds.SeveritiesBoth or NotificationRouteKinds.SeveritiesCritical or NotificationRouteKinds.SeveritiesWarning)
        {
            word = trimmed;
            return null;
        }

        return "severities is critical, warning or both.";
    }

    private const string ReportNoticeHelp = "off, needs_person_or_change or all; off is the default and posts nothing. needs_person_or_change posts a Slack or Teams notice for a report with the needs_person verdict or whose request made at least one agent change, and all posts one for every report";

    /// <summary>The report notice word (#6979): off, needs_person_or_change or all, any case. Blank is off. Anything else is refused with the sentence returned.</summary>
    private static string? ParseReportNotice(string? text, out string word)
    {
        word = NotificationRouteKinds.ReportNoticeOff;
        var trimmed = (text ?? "").Trim().ToLowerInvariant();
        if (trimmed.Length == 0)
        {
            return null;
        }

        if (trimmed is NotificationRouteKinds.ReportNoticeOff or NotificationRouteKinds.ReportNoticeNeedsPersonOrChange or NotificationRouteKinds.ReportNoticeAll)
        {
            word = trimmed;
            return null;
        }

        return "report_notice is off, needs_person_or_change or all.";
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
        "Dismisses alert history rows: they leave get_alert_history's default read (include_dismissed true still shows them) and nothing else changes. The rows stay stored. Pass the rows to dismiss as alert_time, server_id and metric_name exactly as get_alert_history returned them. Returns status dismissed (counts: requested, dismissed, already_dismissed, unknown) with the undo call, restore_alert_history; status not_found when no key names a row, unchanged when every named row was dismissed already. Dismissing is a flag, not a delete, so it is fully undone." +
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
                status = DismissStatus(outcome),
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
        "Brings dismissed alert history rows back into get_alert_history's default read: the undo of dismiss_alert_history. Pass the rows as alert_time, server_id and metric_name. Returns status restored (counts: requested, restored, not_dismissed_or_unknown) with the call that dismisses them again; status not_found when no key names a row, unchanged when the named rows were not dismissed." +
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
                status = RestoreStatus(outcome),
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

    /// <summary>
    /// The status a dismiss answers with (#6455): <c>dismissed</c> only when it dismissed a row. A call whose keys name no row is
    /// <c>not_found</c>, and one whose rows were all dismissed already is <c>unchanged</c>, so a success word never sits on a call that
    /// changed nothing.
    /// </summary>
    internal static string DismissStatus(AlertDismissOutcome outcome) =>
        outcome.Dismissed > 0 ? "dismissed" : outcome.Known == 0 ? "not_found" : "unchanged";

    /// <summary>The status a restore answers with (#6455): <c>restored</c> only when it brought a row back; keys that name no row are
    /// <c>not_found</c>, and rows that were not dismissed are <c>unchanged</c>.</summary>
    internal static string RestoreStatus(AlertDismissOutcome outcome) =>
        outcome.Dismissed > 0 ? "restored" : outcome.Known == 0 ? "not_found" : "unchanged";

    /// <summary>One Information line per dismiss or restore call: the tool, who called, and how many rows changed.</summary>
    private static void LogAlertWrite(ILogger? logger, string tool, int rows) =>
        logger?.LogInformation("MCP write {Tool} by {Identity}: {Rows} alert history rows changed",
            tool, ReadCaller.WriteIdentity(), rows);

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
