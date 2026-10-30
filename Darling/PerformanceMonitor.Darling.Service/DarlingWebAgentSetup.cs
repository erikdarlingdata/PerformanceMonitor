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
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The web dashboard's agent setup writes (#6264): the agent channel of a notification route (level, requests per hour and per day, lease and
/// expiry minutes), the route's match, scope, enabled flag, resolution override and fallback, its deletion, and the environment profiles. The web
/// stands level with the desktop Viewer here, and it shares the Viewer's code, not a copy: a route is saved and deleted by
/// <see cref="NotificationRouteWriter"/> (the validation, the fallback check, the one transaction) and a profile by
/// <see cref="InvestigationRequestStore.SaveProfileAsync"/> (the rules, the write and the <c>profile_saved</c> / <c>profile_removed</c> journal row).
/// A person's change to a route's agent level, limits, scope or match writes a <c>route_agent_changed</c> journal row (before and after, the
/// store's time), the same row the Viewer's save writes; the write is also logged with the seat that made it.
/// <para>Who may write (#6274): only a signed-in admin (<see cref="Hosting.DarlingWebSeat.IsSignedInAdmin"/>). A dashboard with no sign-in
/// registers no seat, and a request with no seat reads as the reads-only seat; it is refused 403 with
/// <see cref="DarlingWebEndpoints.InvestigationWriteSignInMessage"/> and nothing is written. The routes in <see cref="DarlingWebEndpoints"/> are also born gated by the host's method
/// gate (a read-only seat is refused every unsafe method), and the store decides the rest: a login with no write grant gets SQLSTATE 42501,
/// answered 403 with a fixed sentence. The agent's token is the MCP listener's credential and is not a web credential, so it reaches none of
/// these routes.</para>
/// <para>No lost updates (#6274): an edit carries <c>expected_modified_at</c>, the route's <c>modified_at_utc</c> the page read. A route saved or
/// removed by anyone since is refused 409 with <see cref="NotificationRouteWriter.ConflictText"/> and nothing is written.</para>
/// <para>Destinations (the same fields the MCP route tools take, sealed by the same code): <c>teams_webhook_url</c>, <c>slack_webhook_url</c>,
/// <c>generic_webhook_url</c> (absolute http or https URLs), <c>pagerduty_routing_key</c> and <c>email_recipients</c> (comma separated plain
/// addresses). A webhook URL or key is sealed for the route (<see cref="DarlingMcpAlertTools.RouteSealer"/>, then
/// <see cref="NotificationRouteWriter.SaveAsync"/>) and never answered: the page learns only whether each is saved, from the route read's
/// <c>configured_channels</c>. A field left out, null or blank keeps what the route has; <c>-</c> removes it. A refused value is named by its
/// field and never echoed.</para>
/// </summary>
internal static class DarlingWebAgentSetup
{
    /// <summary>The 403 sentence a refused route or profile write answers: the web host runs on whatever login <c>postgres.webConnectionString</c>
    /// names, and a managed or compose store connects it as the <c>viewer</c> role.</summary>
    internal static string RefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("change agent routes or environment profiles", managedStore, adminWritesOff: adminWritesOff);

    /// <summary>The body keys a route write accepts. Anything else is refused by name (never its value).</summary>
    private static readonly string[] s_routeKeys =
    [
        "metric_match", "enabled", "scope_server_id", "scope_tag_id", "agent_level", "agent_max_per_hour", "agent_max_per_day",
        "agent_lease_minutes", "agent_expiry_minutes", "send_resolutions", "fallback_route_id", "severities",
        "teams_webhook_url", "slack_webhook_url", "generic_webhook_url", "pagerduty_routing_key", "email_recipients",
        "expected_modified_at",
    ];

    /// <summary>The body keys a profile write accepts (#6274). <c>modified_by</c> is not one: the signed-in person is the author.</summary>
    private static readonly string[] s_profileKeys = ["scope_server_id", "scope_tag_id", "profile_text", "expected_modified_at"];

    /// <summary>Whether this store login can write routes and profiles: the routes table and the profile table for INSERT and UPDATE, and the
    /// journal for INSERT. The viewer role cannot, and the page then lists only.</summary>
    internal static async Task<JsonObject> AccessAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(@"
SELECT has_table_privilege(current_user, 'config_notification_routes', 'INSERT')
       AND has_table_privilege(current_user, 'config_notification_routes', 'UPDATE')
       AND has_table_privilege(current_user, 'config_notification_routes', 'DELETE')
       AND has_any_column_privilege(current_user, 'config.investigation_journal', 'INSERT'),
       has_table_privilege(current_user, 'config.agent_environment_profiles', 'INSERT')
       AND has_table_privilege(current_user, 'config.agent_environment_profiles', 'UPDATE')
       AND has_any_column_privilege(current_user, 'config.investigation_journal', 'INSERT')");
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new JsonObject
        {
            ["can_edit_routes"] = !reader.IsDBNull(0) && reader.GetBoolean(0),
            ["can_edit_profiles"] = !reader.IsDBNull(1) && reader.GetBoolean(1),
        };
    }

    /// <summary>What a caller that is not a signed-in admin sees (#6274): the write flags false and the sentence that says how to get them. A
    /// signed-in admin's answer is returned as it is.</summary>
    internal static JsonObject ForSeat(JsonObject answer, bool signedInAdmin)
    {
        if (signedInAdmin)
        {
            return answer;
        }

        foreach (var key in new[] { "can_edit_routes", "can_edit_profiles", "can_edit" })
        {
            if (answer.ContainsKey(key))
            {
                answer[key] = false;
            }
        }

        answer["sign_in_message"] = DarlingWebEndpoints.InvestigationWriteSignInMessage;
        return answer;
    }

    /// <summary>Creates a route (<paramref name="routeId"/> null) or edits one. A body key left out keeps the saved value on an edit and
    /// takes the Viewer's default on a create.</summary>
    internal static async Task<(int Status, JsonNode Body)> SaveRouteAsync(
        NpgsqlDataSource postgres, ILogger logger, int? routeId, JsonObject body, string editor, IPasswordKeyRing ring, CancellationToken ct,
        bool managedStore = false, bool adminWritesOff = false)
    {
        if (body.Select(p => p.Key).FirstOrDefault(k => !s_routeKeys.Contains(k, StringComparer.Ordinal)) is { } unknown)
        {
            return Error(400, "Unknown field: " + (unknown.Length > 40 ? unknown[..40] : unknown) + ".");
        }

        try
        {
            NotificationRouteDraft? stored = null;
            if (routeId is { } id)
            {
                if (body["expected_modified_at"] is not JsonValue expectedNode || expectedNode.GetValueKind() != JsonValueKind.String
                    || !DateTime.TryParse(expectedNode.GetValue<string>(), CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var expected)
                    || expected == default)
                {
                    return Error(400, "expected_modified_at is required: send the modified_at_utc the page read for this route.");
                }

                stored = await NotificationRouteWriter.ReadStoredAsync(postgres, id, McpCommandDeadlines.ReadSeconds, ct);
                if (stored is null)
                {
                    return Error(404, "That route is gone.");
                }

                /* The version the page read, not the one just read: a save over a newer row is refused by the writer (#6274). */
                stored.ModifiedAtUtc = expected;
            }

            var draft = stored?.Clone() ?? new NotificationRouteDraft();
            if ((ApplyBody(draft, body) ?? ApplyDestinations(draft, body)) is { } bodyError)
            {
                return Error(400, bodyError);
            }

            var proxies = await NotificationRouteWriter.ReadProxiesAsync(postgres, McpCommandDeadlines.ReadSeconds, ct);
            var (seal, refusal) = DarlingMcpAlertTools.RouteSealer(ring);
            var saved = await NotificationRouteWriter.SaveAsync(
                postgres, draft, stored, proxies, seal, refusal, McpCommandDeadlines.ReadSeconds, ct, editor);
            logger.LogInformation("Web route write: {Action} route {RouteId} by {Editor}, fields {Fields}",
                routeId is null ? "created" : "updated", saved, editor, string.Join(", ", body.Select(p => p.Key)));
            return (200, new JsonObject { ["status"] = routeId is null ? "created" : "updated", ["route_id"] = saved });
        }
        catch (NotificationRouteRefusedException ex) when (ex.Kind == NotificationRouteRefusalKind.Conflict)
        {
            return Error(409, ex.Message);
        }
        catch (NotificationRouteRefusedException ex)
        {
            return Error(400, ex.Message);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return Error(403, RefusedMessage(managedStore, adminWritesOff));
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ForeignKeyViolation)
        {
            return Error(400, "That server or tag does not exist.");
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.CheckViolation)
        {
            return Error(400, "The store refused those values: check the match, the scope and the agent numbers.");
        }
    }

    internal static async Task<(int Status, JsonNode Body)> DeleteRouteAsync(
        NpgsqlDataSource postgres, ILogger logger, int routeId, string editor, CancellationToken ct, bool managedStore = false, bool adminWritesOff = false)
    {
        try
        {
            /* The editor is the person: deleting a route set to an agent level journals route_agent_changed (#6274). */
            if (!await NotificationRouteWriter.DeleteAsync(postgres, routeId, McpCommandDeadlines.ReadSeconds, ct, editor))
            {
                return Error(404, "That route is gone.");
            }

            logger.LogInformation("Web route write: deleted route {RouteId} by {Editor}", routeId, editor);
            return (200, new JsonObject { ["status"] = "deleted", ["route_id"] = routeId });
        }
        catch (NotificationRouteRefusedException ex)
        {
            return Error(400, ex.Message);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return Error(403, RefusedMessage(managedStore, adminWritesOff));
        }
    }

    /// <summary>The profiles, fleet first, with whether this login can change them.</summary>
    internal static async Task<JsonObject> ProfilesBodyAsync(NpgsqlDataSource postgres, CancellationToken ct, NpgsqlDataSource? accessSource = null)
    {
        var rows = await new InvestigationRequestStore(postgres, McpCommandDeadlines.ReadSeconds).ListProfilesAsync(ct);
        var profiles = new JsonArray();
        foreach (var row in rows)
        {
            profiles.Add(new JsonObject
            {
                ["profile_id"] = row.ProfileId,
                ["scope_server_id"] = row.ScopeServerId,
                ["scope_tag_id"] = row.ScopeTagId,
                ["profile_text"] = row.ProfileText,
                ["modified_at_utc"] = row.ModifiedAtUtc.ToString("o", CultureInfo.InvariantCulture),
                ["modified_by"] = row.ModifiedBy,
            });
        }

        return new JsonObject
        {
            ["profiles"] = profiles,
            ["max_length"] = InvestigationProfiles.MaxLength,
            ["can_edit"] = (await AccessAsync(accessSource ?? postgres, ct))["can_edit_profiles"]!.GetValue<bool>(),
        };
    }

    /// <summary>Saves (or, blank, removes) the profile of one scope: <c>{scope_server_id?, scope_tag_id?, profile_text}</c>, both scopes
    /// absent for the fleet's. Stamped <c>person:&lt;editor&gt;</c>, with the Viewer's journal row.</summary>
    internal static async Task<(int Status, JsonNode Body)> SaveProfileAsync(
        NpgsqlDataSource postgres, JsonObject body, string editor, CancellationToken ct, bool managedStore = false, bool adminWritesOff = false)
    {
        if (!TryInt(body, "scope_server_id", out _, out var server, out var error)
            || !TryInt(body, "scope_tag_id", out _, out var tag, out error))
        {
            return Error(400, error!);
        }

        var text = body["profile_text"];
        if (text is not null && text.GetValueKind() is not (JsonValueKind.String or JsonValueKind.Null))
        {
            return Error(400, "profile_text must be text.");
        }

        /* Only the page's own fields: who saved is the signed-in person, never a field of the body. */
        if (body.Select(p => p.Key).FirstOrDefault(k => !s_profileKeys.Contains(k, StringComparer.Ordinal)) is { } unknown)
        {
            return Error(400, "Unknown field: " + (unknown.Length > 40 ? unknown[..40] : unknown) + ".");
        }

        /* The version the page read for this scope; absent or null when it showed no profile (#6274). */
        DateTime? expected = null;
        if (body["expected_modified_at"] is { } expectedNode && expectedNode.GetValueKind() != JsonValueKind.Null)
        {
            if (expectedNode.GetValueKind() != JsonValueKind.String
                || !DateTime.TryParse(expectedNode.GetValue<string>(), CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
            {
                return Error(400, "expected_modified_at must be the modified_at_utc the page read for this profile.");
            }

            expected = parsed;
        }

        var profileText = text?.GetValueKind() == JsonValueKind.String ? text.GetValue<string>() : null;
        try
        {
            var changed = await new InvestigationRequestStore(postgres, McpCommandDeadlines.ReadSeconds)
                .SaveProfileAsync(server, tag, profileText, editor, expected, ct);
            var status = !changed ? "unchanged" : string.IsNullOrWhiteSpace(profileText) ? "removed" : "saved";
            return (200, new JsonObject { ["status"] = status, ["scope_server_id"] = server, ["scope_tag_id"] = tag });
        }
        catch (InvestigationProfileConflictException ex)
        {
            return Error(409, ex.Message);
        }
        catch (ArgumentException ex)
        {
            return Error(400, ex.Message.Split('\n')[0].Replace(" (Parameter 'profileText')", "", StringComparison.Ordinal));
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return Error(403, RefusedMessage(managedStore, adminWritesOff));
        }
    }

    /// <summary>Puts the body's fields on the draft; the sentence of the first field that is not the right kind, or null.</summary>
    private static string? ApplyBody(NotificationRouteDraft draft, JsonObject body)
    {
        if (body.ContainsKey("metric_match"))
        {
            if (body["metric_match"] is not JsonValue match || match.GetValueKind() != JsonValueKind.String)
            {
                return "metric_match must be text.";
            }

            draft.MetricMatch = match.GetValue<string>();
        }

        if (body.ContainsKey("enabled"))
        {
            if (body["enabled"]?.GetValueKind() is not (JsonValueKind.True or JsonValueKind.False))
            {
                return "enabled must be true or false.";
            }

            draft.Enabled = body["enabled"]!.GetValue<bool>();
        }

        if (body.ContainsKey("send_resolutions"))
        {
            var kind = body["send_resolutions"]?.GetValueKind() ?? JsonValueKind.Null;
            if (kind is not (JsonValueKind.True or JsonValueKind.False or JsonValueKind.Null))
            {
                return "send_resolutions must be true, false or null.";
            }

            draft.SendResolutions = kind == JsonValueKind.Null ? null : body["send_resolutions"]!.GetValue<bool>();
        }

        /* #6928: the severity filter. A word other than both, critical or warning is refused by the writer's validation with its sentence. */
        if (body.ContainsKey("severities"))
        {
            if (body["severities"] is not JsonValue severities || severities.GetValueKind() != JsonValueKind.String)
            {
                return "severities must be text.";
            }

            draft.Severities = severities.GetValue<string>();
        }

        if (!TryInt(body, "scope_server_id", out var hasServer, out var server, out var error)
            || !TryInt(body, "scope_tag_id", out var hasTag, out var tag, out error)
            || !TryInt(body, "agent_level", out var hasLevel, out var level, out error)
            || !TryInt(body, "fallback_route_id", out var hasFallback, out var fallback, out error)
            || !TryInt(body, "agent_max_per_hour", out var hasHour, out var perHour, out error)
            || !TryInt(body, "agent_max_per_day", out var hasDay, out var perDay, out error)
            || !TryInt(body, "agent_lease_minutes", out var hasLease, out var lease, out error)
            || !TryInt(body, "agent_expiry_minutes", out var hasExpiry, out var expiry, out error))
        {
            return error;
        }

        if (hasServer) draft.ScopeServerId = server;
        if (hasTag) draft.ScopeTagId = tag;
        if (hasLevel) draft.AgentLevel = level;
        if (hasFallback) draft.FallbackRouteId = fallback;
        if (hasHour) draft.AgentMaxPerHour = perHour ?? NotificationRoute.DefaultAgentMaxPerHour;
        if (hasDay) draft.AgentMaxPerDay = perDay ?? NotificationRoute.DefaultAgentMaxPerDay;
        if (hasLease) draft.AgentLeaseMinutes = lease ?? NotificationRoute.DefaultAgentLeaseMinutes;
        if (hasExpiry) draft.AgentExpiryMinutes = expiry ?? NotificationRoute.DefaultAgentExpiryMinutes;
        return null;
    }

    /// <summary>Puts the body's destinations on the draft (the MCP route tools' fields). A field left out, null or blank keeps what the draft holds
    /// (an edit's saved value, a create's none); <c>-</c> removes it. The sentence of the first field that is not text, or that is saved text
    /// instead of a value, or null. A value is shaped, sealed and checked by <see cref="NotificationRouteWriter.SaveAsync"/>, which never echoes it.</summary>
    private static string? ApplyDestinations(NotificationRouteDraft draft, JsonObject body)
    {
        (string Key, string Label, Action<string> Set, bool Webhook)[] fields =
        [
            ("teams_webhook_url", "Teams webhook URL", v => draft.TeamsUrl = v, true),
            ("slack_webhook_url", "Slack webhook URL", v => draft.SlackUrl = v, true),
            ("generic_webhook_url", "generic webhook URL", v => draft.GenericUrl = v, true),
            ("pagerduty_routing_key", "PagerDuty routing key", v => draft.PagerDutyRoutingKey = v, true),
            ("email_recipients", "email recipients", v => draft.SmtpRecipients = v == NotificationRouteWriter.ClearMarker ? "" : v, false),
        ];
        foreach (var (key, label, set, webhook) in fields)
        {
            if (!body.ContainsKey(key) || body[key] is null)
            {
                continue;
            }

            if (body[key] is not JsonValue node || node.GetValueKind() != JsonValueKind.String)
            {
                return key + " must be text.";
            }

            var text = node.GetValue<string>().Trim();
            if (text.Length == 0)
            {
                continue;
            }

            if (webhook && PasswordSeal.IsSealed(text))
            {
                return $"The {label} must be the value itself, not saved text.";
            }

            set(text);
        }

        return null;
    }

    /// <summary>Reads an integer or null; absent is <paramref name="present"/> false. Any other kind is the sentence in <paramref name="error"/>.</summary>
    private static bool TryInt(JsonObject body, string key, out bool present, out int? value, out string? error)
    {
        value = null;
        error = null;
        present = body.ContainsKey(key);
        if (!present || body[key] is null)
        {
            return true;
        }

        if (body[key] is JsonValue node && node.GetValueKind() == JsonValueKind.Number && node.TryGetValue<int>(out var parsed))
        {
            value = parsed;
            return true;
        }

        error = key + " must be a whole number or null.";
        return false;
    }

    private static (int Status, JsonNode Body) Error(int status, string message) =>
        (status, new JsonObject { ["error"] = message });
}
