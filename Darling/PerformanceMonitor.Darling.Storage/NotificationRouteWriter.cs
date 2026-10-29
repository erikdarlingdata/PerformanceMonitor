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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>Why a route write was refused: <see cref="Validation"/> is a field the store would not accept (said in words before
/// the statement runs), <see cref="Value"/> is a webhook value that cannot be sealed or kept.</summary>
public enum NotificationRouteRefusalKind
{
    Validation,
    Value,

    /// <summary>The route was saved or removed by someone else since the writer's screen read it (#6274); nothing was written.</summary>
    Conflict,
}

/// <summary>A route write refused with a sentence that never carries a secret (#6231).</summary>
public sealed class NotificationRouteRefusedException(string message, NotificationRouteRefusalKind kind)
    : Exception(message)
{
    public NotificationRouteRefusalKind Kind { get; } = kind;
}

/// <summary>The proxy text of each webhook channel on the notification settings row: a route's sealed value is bound to its
/// channel's proxy there (#5366).</summary>
public sealed record WebhookProxies(string? TeamsProxy, string? SlackProxy, string? GenericProxy, string? PagerDutyProxy);

/// <summary>A <c>config_notification_routes</c> row as a writer states it (#6231): the one shape the Viewer and the MCP tools both
/// hand to <see cref="NotificationRouteWriter"/>. A webhook value is a plaintext the caller typed, the sealed text stored for it,
/// or empty (inherit).</summary>
public sealed class NotificationRouteDraft
{
    public int RouteId { get; set; }
    public string MetricMatch { get; set; } = "";
    public string TeamsUrl { get; set; } = "";
    public string SlackUrl { get; set; } = "";
    public string GenericUrl { get; set; } = "";
    public string PagerDutyRoutingKey { get; set; } = "";
    public string SmtpRecipients { get; set; } = "";
    public bool Enabled { get; set; } = true;
    public int? ScopeServerId { get; set; }
    public int? ScopeTagId { get; set; }
    public int? AgentLevel { get; set; }
    public int AgentMaxPerHour { get; set; } = NotificationRoute.DefaultAgentMaxPerHour;
    public int AgentMaxPerDay { get; set; } = NotificationRoute.DefaultAgentMaxPerDay;
    public int AgentLeaseMinutes { get; set; } = NotificationRoute.DefaultAgentLeaseMinutes;
    public int AgentExpiryMinutes { get; set; } = NotificationRoute.DefaultAgentExpiryMinutes;
    public bool? SendResolutions { get; set; }
    public int? FallbackRouteId { get; set; }

    /// <summary>The severity filter (V229, #6928): <c>both</c>, <c>critical</c> or <c>warning</c>.</summary>
    public string Severities { get; set; } = NotificationRouteKinds.SeveritiesBoth;

    /// <summary>When the route was last saved, as the writer's screen read it (the store's <c>modified_at</c>, UTC). On a <c>stored</c> draft
    /// handed to <see cref="NotificationRouteWriter.SaveAsync"/> it is the version the save expects: a route saved by anyone since is refused
    /// as a conflict instead of overwritten (#6274). Null or <c>default</c> skips the check.</summary>
    public DateTime? ModifiedAtUtc { get; set; }

    public bool HasAnyDestination =>
        !string.IsNullOrWhiteSpace(TeamsUrl) || !string.IsNullOrWhiteSpace(SlackUrl)
        || !string.IsNullOrWhiteSpace(GenericUrl) || !string.IsNullOrWhiteSpace(PagerDutyRoutingKey)
        || !string.IsNullOrWhiteSpace(SmtpRecipients);

    public NotificationRouteDraft Clone() => (NotificationRouteDraft)MemberwiseClone();
}

/// <summary>
/// The one copy of the notification-route write (#3598, #5366, #5998, #6036, #6231): the SQL, the validation said in words, the
/// sealing of the webhook values and the signing-key guard. The Viewer's route window and the MCP
/// <c>create_notification_route</c> / <c>update_notification_route</c> tools both call this, so a route stored either way is the
/// same row and refused with the same sentences. The V131 trigger bumps <c>config_version</c> on every statement, so the running
/// service re-reads the routes on its next sweep.
/// </summary>
public static class NotificationRouteWriter
{
    /// <summary>The five destination columns in the SAME order the service reads them (StoreConfigProvider).</summary>
    public const string DestinationColumns = "teams_url, slack_url, generic_url, pagerduty_routing_key, smtp_recipients";

    /// <summary>The agent channel's five columns (V184, #5632), in the SAME order the service reads them.</summary>
    public const string AgentColumns = "agent_level, agent_max_per_hour, agent_max_per_day, agent_lease_minutes, agent_expiry_minutes";

    /// <summary>Inserts one route. <c>route_id</c> is <c>GENERATED ALWAYS AS IDENTITY</c>, so it is never in the column list.</summary>
    public const string InsertSql = @"
INSERT INTO config_notification_routes
    (metric_match, " + DestinationColumns + @", enabled, scope_server_id, scope_tag_id, " + AgentColumns + @", send_resolutions, fallback_route_id, severities, modified_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, (now() AT TIME ZONE 'UTC'))
RETURNING route_id";

    public const string UpdateSql = @"
UPDATE config_notification_routes SET
    metric_match = $2,
    teams_url = $3,
    slack_url = $4,
    generic_url = $5,
    pagerduty_routing_key = $6,
    smtp_recipients = $7,
    enabled = $8,
    scope_server_id = $9,
    scope_tag_id = $10,
    agent_level = $11,
    agent_max_per_hour = $12,
    agent_max_per_day = $13,
    agent_lease_minutes = $14,
    agent_expiry_minutes = $15,
    send_resolutions = $16,
    fallback_route_id = $17,
    severities = $18,
    modified_at = (now() AT TIME ZONE 'UTC')
WHERE route_id = $1";

    /// <summary>The column has no foreign key (V193), so a delete clears the references to the deleted route first (#6036).</summary>
    public const string ClearFallbackSql =
        "UPDATE config_notification_routes SET fallback_route_id = NULL, modified_at = (now() AT TIME ZONE 'UTC') WHERE fallback_route_id = $1";

    public const string DeleteSql = "DELETE FROM config_notification_routes WHERE route_id = $1";

    /// <summary>Clears every other route's fallback reference to the route, then deletes it, in one transaction (#6036; #6264: the one
    /// copy the Viewer and the web page share). False when the route is gone. A store error (SQLSTATE 42501 for a login with no write
    /// grant) propagates. #6274: when <paramref name="login"/> names the person deleting and the route had an agent level (an Off route
    /// stops the lookup, so deleting it can hand its servers to a broader agent route), a <c>route_agent_changed</c> journal row is
    /// written in the same transaction.</summary>
    public static async Task<bool> DeleteAsync(
        NpgsqlDataSource dataSource, int routeId, int timeoutSeconds, CancellationToken cancellationToken, string? login = null)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        if (login is not null && InvestigationPersonWrites.ValidateLogin(login) is { } loginError)
        {
            throw new NotificationRouteRefusedException(loginError, NotificationRouteRefusalKind.Validation);
        }

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var before = login is null ? null : await TryLockAsync(connection, transaction, routeId, timeoutSeconds, cancellationToken);
        await using (var clear = new NpgsqlCommand(ClearFallbackSql, connection, transaction))
        {
            clear.CommandTimeout = timeoutSeconds;
            clear.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
            await clear.ExecuteNonQueryAsync(cancellationToken);
        }

        int deleted;
        await using (var delete = new NpgsqlCommand(DeleteSql, connection, transaction))
        {
            delete.CommandTimeout = timeoutSeconds;
            delete.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
            deleted = await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        if (deleted > 0 && login is not null && AgentChange(before, null) is { } change)
        {
            await InvestigationPersonJournal.InsertAsync(
                connection, transaction, null, routeId, before!.ScopeServerId, AgentChangedEvent,
                InvestigationPersonWrites.PersonActor(login), change.Reason, change.Detail, timeoutSeconds, cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return deleted > 0;
    }

    public const string SetEnabledSql =
        "UPDATE config_notification_routes SET enabled = $2, modified_at = (now() AT TIME ZONE 'UTC') WHERE route_id = $1";

    /// <summary>Turns a route on or off (#6274: the one copy the Viewer's toggle shares). False when the route is gone. When
    /// <paramref name="login"/> names the person and the route has an agent level, the change is journaled as
    /// <c>route_agent_changed</c> in the same transaction: a disabled route is dropped from the lookup, so turning an Off route off
    /// can hand its servers to a broader agent route.</summary>
    public static async Task<bool> SetEnabledAsync(
        NpgsqlDataSource dataSource, int routeId, bool enabled, int timeoutSeconds, CancellationToken cancellationToken, string? login = null)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        if (login is not null && InvestigationPersonWrites.ValidateLogin(login) is { } loginError)
        {
            throw new NotificationRouteRefusedException(loginError, NotificationRouteRefusalKind.Validation);
        }

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var before = login is null ? null : await TryLockAsync(connection, transaction, routeId, timeoutSeconds, cancellationToken);
        int updated;
        await using (var update = new NpgsqlCommand(SetEnabledSql, connection, transaction))
        {
            update.CommandTimeout = timeoutSeconds;
            update.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
            update.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabled });
            updated = await update.ExecuteNonQueryAsync(cancellationToken);
        }

        if (updated > 0 && login is not null && before is not null)
        {
            var after = before.Clone();
            after.Enabled = enabled;
            if (AgentChange(before, after) is { } change)
            {
                await InvestigationPersonJournal.InsertAsync(
                    connection, transaction, null, routeId, before.ScopeServerId, AgentChangedEvent,
                    InvestigationPersonWrites.PersonActor(login), change.Reason, change.Detail, timeoutSeconds, cancellationToken);
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return updated > 0;
    }

    /// <summary>The columns an update needs to carry a route's stored values forward: the four sealed destinations (or legacy
    /// plaintext) and the address list. Only a writer with the service's login reads them.</summary>
    public const string SelectStoredSql =
        "SELECT route_id, metric_match, teams_url, slack_url, generic_url, pagerduty_routing_key, smtp_recipients, enabled, "
        + "scope_server_id, scope_tag_id, " + AgentColumns + ", send_resolutions, fallback_route_id, modified_at, severities "
        + "FROM config_notification_routes WHERE route_id = $1";

    /// <summary>The proxy of each webhook channel on the settings row (the row id 1 the Viewer reads).</summary>
    public const string SelectProxiesSql =
        "SELECT teams_proxy, slack_proxy, generic_proxy, pagerduty_proxy FROM config_notification WHERE id = 1";

    /// <summary>A box holding only this text removes the saved value.</summary>
    public const string ClearMarker = "-";

    /// <summary>What a save says when it would change the URL or proxy a signing key is tied to.</summary>
    public const string RemoveKeyFirstText =
        "A signing key is saved for this destination, and it is tied to the generic webhook URL and proxy. Remove the key first, "
        + "save the change, then generate a new key.";

    /// <summary>The refusal when a value needs sealing and no sealing key is available.</summary>
    public const string NoKeyText =
        "Passwords cannot be saved: the Darling service has not published a password key for this store yet. Start the service, then try again.";

    /// <summary>
    /// The write-side validation the edit dialog also runs, so a row the dialog accepts is a row the store accepts: a non-blank
    /// match (the V131 CHECK's twin), at least one destination or the agent channel, no route scoped to both a server and a tag, no
    /// every-metric match without a scope (V181 CHECKs), no route as its own fallback, and the agent channel's bounds (V184).
    /// </summary>
    public static string? Validate(NotificationRouteDraft row)
    {
        ArgumentNullException.ThrowIfNull(row);
        if (string.IsNullOrWhiteSpace(row.MetricMatch))
        {
            return "Choose a family or enter an exact metric name.";
        }

        if (row.ScopeServerId.HasValue && row.ScopeTagId.HasValue)
        {
            return "A route is scoped to one server or one tag, not both.";
        }

        if (row.MetricMatch.Trim() == NotificationRoute.WildcardMatch && !row.ScopeServerId.HasValue && !row.ScopeTagId.HasValue)
        {
            return "Every metric (*) needs a server or a tag scope. Pick one, or choose a family or an exact metric.";
        }

        if (!row.HasAnyDestination && !row.AgentLevel.HasValue)
        {
            return "Set at least one destination or the agent channel — a route with every channel empty inherits everything and routes nothing.";
        }

        if (RecipientsRefusal(row.SmtpRecipients) is { } recipients)
        {
            return recipients;
        }

        if (row.FallbackRouteId.HasValue && row.RouteId != 0 && row.FallbackRouteId == row.RouteId)
        {
            return "A route cannot be its own fallback route.";
        }

        if (SeveritiesRefusal(row.Severities) is { } severities)
        {
            return severities;
        }

        if (row.AgentLevel is { } level && level is not (NotificationRoute.AgentLevelOff or NotificationRoute.AgentLevelInvestigate or NotificationRoute.AgentLevelAct or NotificationRoute.AgentLevelServerAct))
        {
            return "The agent channel is Inherit, Off, Investigate, Act or Act on servers.";
        }

        return ValidateAgentLimit("Requests per hour", row.AgentMaxPerHour, NotificationRoute.MinAgentMaxPerHour, NotificationRoute.MaxAgentMaxPerHour)
            ?? ValidateAgentLimit("Requests per day", row.AgentMaxPerDay, NotificationRoute.MinAgentMaxPerDay, NotificationRoute.MaxAgentMaxPerDay)
            ?? ValidateAgentLimit("Lease minutes", row.AgentLeaseMinutes, NotificationRoute.MinAgentLeaseMinutes, NotificationRoute.MaxAgentLeaseMinutes)
            ?? ValidateAgentLimit("Expiry minutes", row.AgentExpiryMinutes, NotificationRoute.MinAgentExpiryMinutes, NotificationRoute.MaxAgentExpiryMinutes);
    }

    /// <summary>The most addresses one route's recipient list holds.</summary>
    public const int MaxRecipients = 50;

    /// <summary>
    /// The refusal for a recipient list, or null when it is fine or blank (#6231). The list is comma separated, as the email send
    /// splits it, and each entry must be one plain address (no display name, no group). One copy for the Viewer's route window and
    /// the MCP route tools. The text never carries an address.
    /// </summary>
    public static string? RecipientsRefusal(string? recipients)
    {
        if (string.IsNullOrWhiteSpace(recipients))
        {
            return null;
        }

        var entries = recipients.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (entries.Length > MaxRecipients)
        {
            return string.Create(CultureInfo.InvariantCulture, $"A route holds at most {MaxRecipients} email recipients.");
        }

        foreach (var entry in entries)
        {
            if (!System.Net.Mail.MailAddress.TryCreate(entry, out var address) || !string.Equals(address.Address, entry, StringComparison.Ordinal)
                || !string.IsNullOrEmpty(address.DisplayName))
            {
                return "The email recipients are a comma separated list of plain addresses such as name@example.com. (The value is not echoed.)";
            }
        }

        return null;
    }

    /// <summary>
    /// Stores sealed destinations through <c>config.set_notification_route_destinations</c> on the caller's transaction (#6231): the
    /// function the <c>mcp</c> login writes a route's destinations through. <paramref name="values"/> holds the teams, slack, generic,
    /// PagerDuty and recipients values in that order (a webhook value sealed for the route's own row, or empty), and
    /// <paramref name="columns"/> names the ones to change. Returns, in the same order, whether a value was saved before for each column named (null for one not named; never a stored value, so a
    /// webhook value, sealed or not, never leaves the function). A database refusal propagates as a <see cref="PostgresException"/>.
    /// </summary>
    /// <para>#6542 part 2: <paramref name="actor"/> is the operator-class MCP token's journal actor (<c>operator:</c> and its name), or null.
    /// The function changes an agent route's destinations only for an operator actor.</para>
    public static async Task<bool?[]> SetRouteDestinationsAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int routeId, string? actor, IReadOnlyList<string> columns, IReadOnlyList<string> values,
        int timeoutSeconds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(values);
        await using var command = new NpgsqlCommand(
            "SELECT * FROM config.set_notification_route_destinations($1, $2, $3, $4, $5, $6, $7, $8)", connection, transaction);
        command.CommandTimeout = timeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        command.Parameters.Add(actor is null
            ? new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text }
            : new NpgsqlParameter<string> { TypedValue = actor });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = columns.ToArray() });
        for (var i = 0; i < 5; i++)
        {
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = i < values.Count ? values[i] ?? "" : "" });
        }

        var previous = new bool?[5];
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            for (var i = 0; i < 5; i++)
            {
                previous[i] = reader.IsDBNull(i) ? null : reader.GetBoolean(i);
            }
        }

        return previous;
    }

    /// <summary>The recipient count and the distinct domains of a list, for an answer that must not carry the addresses.</summary>
    public static (int Count, string[] Domains) DescribeRecipients(string? recipients)
    {
        var entries = (recipients ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var domains = entries.Select(e => e.Contains('@', StringComparison.Ordinal) ? e[(e.LastIndexOf('@') + 1)..] : "")
            .Where(d => d.Length > 0).Distinct(StringComparer.OrdinalIgnoreCase).Order(StringComparer.OrdinalIgnoreCase).ToArray();
        return (entries.Length, domains);
    }

    private static string? ValidateAgentLimit(string name, int value, int min, int max) =>
        value < min || value > max
            ? string.Create(CultureInfo.InvariantCulture, $"{name} must be from {min} to {max}.")
            : null;

    /// <summary>Refuses a fallback route id that names no route (#6036): the column has no foreign key, so the check is here.
    /// Runs on the caller's transaction when it has one. Null when the fallback is fine or absent.</summary>
    public static async Task<string?> FallbackRefusalAsync(
        NotificationRouteDraft row, NpgsqlConnection connection, NpgsqlTransaction? transaction, int timeoutSeconds,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(connection);
        if (row.FallbackRouteId is not { } fallbackId)
        {
            return null;
        }

        await using var command = new NpgsqlCommand("SELECT 1 FROM config_notification_routes WHERE route_id = $1", connection, transaction);
        command.CommandTimeout = timeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = fallbackId });
        return await command.ExecuteScalarAsync(cancellationToken) is null ? $"Fallback route {fallbackId} does not exist." : null;
    }

    /// <summary>The sentence for a severity filter that is not <c>both</c>, <c>critical</c> or <c>warning</c> (case-insensitive), else null (#6928).</summary>
    public static string? SeveritiesRefusal(string? severities) =>
        severities is not null && NotificationRouteKinds.NormalizeSeverities(severities) != severities.Trim().ToLowerInvariant()
            ? "severities is critical, warning or both."
            : null;

    /// <summary>Binds the agent columns, the resolution override and the fallback, in the order every insert and update takes them.</summary>
    public static void AddAgentColumns(NpgsqlCommand command, NotificationRouteDraft row)
    {
        ArgumentNullException.ThrowIfNull(command);
        ArgumentNullException.ThrowIfNull(row);
        command.Parameters.Add(row.AgentLevel.HasValue
            ? new NpgsqlParameter<short> { TypedValue = (short)row.AgentLevel.Value }
            : new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Smallint });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.AgentMaxPerHour });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.AgentMaxPerDay });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.AgentLeaseMinutes });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.AgentExpiryMinutes });
        /* #5825 (V187): NULL is "use the channel's setting". */
        command.Parameters.Add(row.SendResolutions.HasValue
            ? new NpgsqlParameter<bool> { TypedValue = row.SendResolutions.Value }
            : new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean });
        /* #6036: the fallback route, bound last. NULL is none. */
        AddNullableInt(command, row.FallbackRouteId);
        /* #6928 (V229): the severity filter, bound after the fallback. */
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = NotificationRouteKinds.NormalizeSeverities(row.Severities) });
    }

    /// <summary>Binds an <c>integer</c> that may be SQL NULL.</summary>
    public static void AddNullableInt(NpgsqlCommand command, int? value) =>
        command.Parameters.Add(value.HasValue
            ? new NpgsqlParameter<int> { TypedValue = value.Value }
            : new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });

    /// <summary>The value a blank box stands for: the sealed text already stored, or what was typed.</summary>
    public static string CarryKept(string? typed, string? stored)
    {
        var text = typed?.Trim() ?? "";
        return text.Length == 0 && PasswordSeal.IsSealed(stored) ? stored! : text;
    }

    /// <summary>
    /// Replaces the webhook destinations on a route with what is stored: kept sealed text, a new sealed value, or empty. A route has
    /// no proxy of its own: each value is bound to its channel's proxy on the settings row. <paramref name="seal"/> seals a typed
    /// value for a binding and throws <see cref="NotificationRouteRefusedException"/> (kind Value) when it cannot.
    /// </summary>
    public static void ResolveRoute(
        NotificationRouteDraft row, NotificationRouteDraft? stored, WebhookProxies parent, int routeId,
        Func<string, PasswordBinding, string>? seal, string? refusal)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(parent);
        var where = PasswordBinding.WebhookRouteRow(routeId);

        row.TeamsUrl = ResolveValue(row.TeamsUrl, stored?.TeamsUrl, "teams", where, parent.TeamsProxy, parent.TeamsProxy, "Teams webhook URL", seal, refusal);
        row.SlackUrl = ResolveValue(row.SlackUrl, stored?.SlackUrl, "slack", where, parent.SlackProxy, parent.SlackProxy, "Slack webhook URL", seal, refusal);
        row.GenericUrl = ResolveValue(row.GenericUrl, stored?.GenericUrl, "generic", where, parent.GenericProxy, parent.GenericProxy, "generic webhook URL", seal, refusal);
        row.PagerDutyRoutingKey = ResolveValue(row.PagerDutyRoutingKey, stored?.PagerDutyRoutingKey, "pagerduty", where, parent.PagerDutyProxy, parent.PagerDutyProxy, "PagerDuty routing key", seal, refusal);
    }

    /// <summary>
    /// Shapes and seals one NEW webhook value for a route's own row (#6231): the URL shape and address refusal for a URL slot, then the seal bound to
    /// <c>route:&lt;id&gt;</c> and the channel's proxy. The MCP route tools use it per field, because they hold no stored value to keep.
    /// <paramref name="slot"/> is teams, slack, generic or pagerduty. Throws <see cref="NotificationRouteRefusedException"/>.
    /// </summary>
    public static string SealNewRouteValue(
        string slot, string plaintext, int routeId, string? proxy, string label, Func<string, PasswordBinding, string>? seal, string? refusal) =>
        PasswordSeal.IsSealed(plaintext?.Trim())
            ? throw new NotificationRouteRefusedException($"The {label} must be the value itself, not saved text.", NotificationRouteRefusalKind.Value)
            : ResolveValue(plaintext, null, slot, PasswordBinding.WebhookRouteRow(routeId), proxy, proxy, label, seal, refusal);

    /// <summary>
    /// Seals a new webhook value for the SETTINGS row (#6392): the Teams, Slack or generic URL, the PagerDuty routing key
    /// (<paramref name="slot"/> teams, slack, generic or pagerduty) or the generic headers (<c>generic_headers</c>, bound to
    /// <paramref name="boundUrl"/>, the generic URL text as stored). The shape rule and the refusal words are the ones the route values use.
    /// Throws <see cref="NotificationRouteRefusedException"/>.
    /// </summary>
    public static string SealNewSettingsValue(
        string slot, string plaintext, string? proxy, string? boundUrl, string label, Func<string, PasswordBinding, string>? seal, string? refusal)
    {
        var text = plaintext?.Trim() ?? "";
        if (PasswordSeal.IsSealed(text))
        {
            throw new NotificationRouteRefusedException($"The {label} must be the value itself, not saved text.", NotificationRouteRefusalKind.Value);
        }

        if (slot is "teams" or "slack" or "generic" && WebhookUrlShape.Refusal(text, label) is { } shapeRefusal)
        {
            throw new NotificationRouteRefusedException(shapeRefusal, NotificationRouteRefusalKind.Value);
        }

        if (seal is null)
        {
            throw new NotificationRouteRefusedException(refusal ?? NoKeyText, NotificationRouteRefusalKind.Value);
        }

        try
        {
            return seal(text, PasswordBinding.ForWebhook(slot, PasswordBinding.WebhookSettingsRow, proxy, boundUrl));
        }
        catch (NotificationRouteRefusedException ex)
        {
            throw new NotificationRouteRefusedException($"The {label}: {ex.Message}", NotificationRouteRefusalKind.Value);
        }
    }

    private static string ResolveValue(
        string? value, string? stored, string slot, string row, string? proxy, string? storedProxy, string label,
        Func<string, PasswordBinding, string>? seal, string? refusal)
    {
        var text = value?.Trim() ?? "";
        if (text.Length == 0 || text == ClearMarker)
        {
            return "";
        }

        if (PasswordSeal.IsSealed(text) && string.Equals(text, stored, StringComparison.Ordinal))
        {
            /* The saved value stays only while what it was sealed for is unchanged: a changed proxy needs it again. */
            if (!string.Equals(proxy ?? "", storedProxy ?? "", StringComparison.Ordinal))
            {
                throw new NotificationRouteRefusedException($"Enter the {label} again: its proxy changed.", NotificationRouteRefusalKind.Value);
            }

            return text;
        }

        /* #6231: a URL that cannot be sent to is refused at save, with the same words in every writer. A routing key is not a URL. */
        if (slot is "teams" or "slack" or "generic" && !PasswordSeal.IsSealed(text)
            && WebhookUrlShape.Refusal(text, label) is { } shapeRefusal)
        {
            throw new NotificationRouteRefusedException(shapeRefusal, NotificationRouteRefusalKind.Value);
        }

        if (seal is null)
        {
            throw new NotificationRouteRefusedException(refusal ?? NoKeyText, NotificationRouteRefusalKind.Value);
        }

        try
        {
            return seal(text, PasswordBinding.ForWebhook(slot, row, proxy, null));
        }
        catch (NotificationRouteRefusedException ex)
        {
            throw new NotificationRouteRefusedException($"The {label}: {ex.Message}", NotificationRouteRefusalKind.Value);
        }
    }

    private static readonly System.Text.UTF8Encoding s_strictUtf8 = new(false, true);

    /// <summary>
    /// Seals one webhook value (a URL, a routing key) for a webhook binding with <paramref name="seal"/>, the key's seal. The one copy
    /// of the sentences a value that cannot be stored is refused with: the Viewer's sealer and the MCP route tools both call it.
    /// Throws <see cref="NotificationRouteRefusedException"/> (kind Value); the text never carries the value.
    /// </summary>
    public static string SealValue(string plaintext, PasswordBinding binding, Func<string, PasswordBinding, string> seal)
    {
        ArgumentNullException.ThrowIfNull(plaintext);
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(seal);
        if (!string.Equals(binding.Purpose, PasswordBinding.WebhookPurpose, StringComparison.Ordinal))
        {
            throw new ArgumentException("A webhook value is sealed with a webhook binding.", nameof(binding));
        }

        const string BadCharacters = "The value contains characters that cannot be stored.";
        try
        {
            _ = s_strictUtf8.GetByteCount(plaintext);
        }
        catch (System.Text.EncoderFallbackException)
        {
            throw new NotificationRouteRefusedException(BadCharacters, NotificationRouteRefusalKind.Value);
        }

        try
        {
            return seal(plaintext, binding);
        }
        catch (PasswordSealException)
        {
            throw new NotificationRouteRefusedException(
                s_strictUtf8.GetByteCount(plaintext) > PasswordSeal.MaxPlaintextBytes ? "The value is longer than can be stored." : BadCharacters,
                NotificationRouteRefusalKind.Value);
        }
        catch (ArgumentException)
        {
            throw new NotificationRouteRefusedException(BadCharacters, NotificationRouteRefusalKind.Value);
        }
    }

    /// <summary>Reads the proxy of each webhook channel from the settings row; empty proxies when the row is absent.</summary>
    public static async Task<WebhookProxies> ReadProxiesAsync(NpgsqlDataSource dataSource, int timeoutSeconds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        await using var command = dataSource.CreateCommand(SelectProxiesSql);
        command.CommandTimeout = timeoutSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new WebhookProxies("", "", "", "");
        }

        return new WebhookProxies(
            reader.IsDBNull(0) ? "" : reader.GetString(0), reader.IsDBNull(1) ? "" : reader.GetString(1),
            reader.IsDBNull(2) ? "" : reader.GetString(2), reader.IsDBNull(3) ? "" : reader.GetString(3));
    }

    /// <summary>One route as stored, destinations included (sealed text, or legacy plaintext), or null when there is no such route.
    /// The service's own login reads it; the <c>viewer</c> role cannot.</summary>
    public static async Task<NotificationRouteDraft?> ReadStoredAsync(
        NpgsqlDataSource dataSource, int routeId, int timeoutSeconds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        await using var command = dataSource.CreateCommand(SelectStoredSql);
        command.CommandTimeout = timeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return ReadStoredRow(reader);
    }

    private static NotificationRouteDraft ReadStoredRow(NpgsqlDataReader reader)
    {
        return new NotificationRouteDraft
        {
            RouteId = reader.GetInt32(0),
            MetricMatch = reader.GetString(1),
            TeamsUrl = reader.GetString(2),
            SlackUrl = reader.GetString(3),
            GenericUrl = reader.GetString(4),
            PagerDutyRoutingKey = reader.GetString(5),
            SmtpRecipients = reader.GetString(6),
            Enabled = reader.GetBoolean(7),
            ScopeServerId = reader.IsDBNull(8) ? null : reader.GetInt32(8),
            ScopeTagId = reader.IsDBNull(9) ? null : reader.GetInt32(9),
            AgentLevel = reader.IsDBNull(10) ? null : reader.GetInt16(10),
            AgentMaxPerHour = reader.GetInt32(11),
            AgentMaxPerDay = reader.GetInt32(12),
            AgentLeaseMinutes = reader.GetInt32(13),
            AgentExpiryMinutes = reader.GetInt32(14),
            SendResolutions = reader.IsDBNull(15) ? null : reader.GetBoolean(15),
            FallbackRouteId = reader.IsDBNull(16) ? null : reader.GetInt32(16),
            ModifiedAtUtc = reader.IsDBNull(17) ? null : DateTime.SpecifyKind(reader.GetDateTime(17), DateTimeKind.Utc),
            Severities = reader.GetString(18),
        };
    }

    /// <summary>
    /// Saves a route with its webhook destinations sealed (#5366) and returns its <c>route_id</c>. A new route is inserted with
    /// every destination empty to get its id, because a sealed value is bound to the row it is stored in, then updated with the
    /// sealed values, in one transaction: a refusal rolls the insert back. <paramref name="stored"/> is the row as read, for the
    /// values a blank box keeps; <paramref name="parent"/> holds each channel's proxy. Throws
    /// <see cref="NotificationRouteRefusedException"/> for every refusal; a store error propagates.
    /// <para>#6274: an edit locks the route row first and refuses it as <see cref="NotificationRouteRefusalKind.Conflict"/> when the
    /// row is gone or <paramref name="stored"/> carries a <see cref="NotificationRouteDraft.ModifiedAtUtc"/> that is not the row's
    /// (a change made elsewhere since the screen loaded is not overwritten). When <paramref name="login"/> names the person saving,
    /// a change to the route's agent level, limits, scope or match writes a <c>route_agent_changed</c> journal row, in the same
    /// transaction, with the values before and after and the store's time.</para>
    /// </summary>
    public static async Task<int> SaveAsync(
        NpgsqlDataSource dataSource, NotificationRouteDraft typed, NotificationRouteDraft? stored, WebhookProxies parent,
        Func<string, PasswordBinding, string>? seal, string? refusal, int timeoutSeconds, CancellationToken cancellationToken,
        string? login = null)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(typed);
        ArgumentNullException.ThrowIfNull(parent);

        if (Validate(typed) is { } early)
        {
            throw new NotificationRouteRefusedException(early, NotificationRouteRefusalKind.Validation);
        }

        if (login is not null && InvestigationPersonWrites.ValidateLogin(login) is { } loginError)
        {
            throw new NotificationRouteRefusedException(loginError, NotificationRouteRefusalKind.Validation);
        }

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var routeId = typed.RouteId;
        NotificationRouteDraft? before = null;
        if (routeId != 0)
        {
            before = await LockAndCheckAsync(connection, transaction, routeId, stored?.ModifiedAtUtc, timeoutSeconds, cancellationToken);
        }

        if (routeId == 0)
        {
            await using var insert = new NpgsqlCommand(InsertSql, connection, transaction);
            insert.CommandTimeout = timeoutSeconds;
            insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = typed.MetricMatch.Trim() });
            for (var i = 0; i < 5; i++)
            {
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "" });
            }

            insert.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = typed.Enabled });
            /* The scope goes in with the insert: a scoped "*" route would fail the V181 CHECK with no scope. */
            AddNullableInt(insert, typed.ScopeServerId);
            AddNullableInt(insert, typed.ScopeTagId);
            /* The agent columns too: an agent-only route has no destination for the update to add. */
            AddAgentColumns(insert, typed);
            routeId = Convert.ToInt32(await insert.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        }

        var resolved = typed.Clone();
        resolved.RouteId = routeId;
        ResolveRoute(resolved, stored, parent, routeId, seal, refusal);
        if (Validate(resolved) is { } error)
        {
            throw new NotificationRouteRefusedException(error, NotificationRouteRefusalKind.Validation);
        }

        if (await FallbackRefusalAsync(resolved, connection, transaction, timeoutSeconds, cancellationToken) is { } fallback)
        {
            throw new NotificationRouteRefusedException(fallback, NotificationRouteRefusalKind.Validation);
        }

        /* #5998: a signing key is sealed for the route's stored URL text, so a save that changes that text would leave the key
           unreadable to the service. A new route has no key. */
        if (typed.RouteId != 0 && await KeyWouldBreakAsync(connection, transaction, routeId, resolved.GenericUrl, timeoutSeconds, cancellationToken))
        {
            throw new NotificationRouteRefusedException(RemoveKeyFirstText, NotificationRouteRefusalKind.Value);
        }

        await using var update = new NpgsqlCommand(UpdateSql, connection, transaction);
        update.CommandTimeout = timeoutSeconds;
        update.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });                          // $1
        update.Parameters.Add(new NpgsqlParameter<string> { TypedValue = resolved.MetricMatch.Trim() });    // $2
        update.Parameters.Add(new NpgsqlParameter<string> { TypedValue = resolved.TeamsUrl });              // $3
        update.Parameters.Add(new NpgsqlParameter<string> { TypedValue = resolved.SlackUrl });              // $4
        update.Parameters.Add(new NpgsqlParameter<string> { TypedValue = resolved.GenericUrl });            // $5
        update.Parameters.Add(new NpgsqlParameter<string> { TypedValue = resolved.PagerDutyRoutingKey });   // $6
        update.Parameters.Add(new NpgsqlParameter<string> { TypedValue = resolved.SmtpRecipients.Trim() }); // $7
        update.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = resolved.Enabled });                 // $8
        AddNullableInt(update, resolved.ScopeServerId);                                                      // $9
        AddNullableInt(update, resolved.ScopeTagId);                                                         // $10
        AddAgentColumns(update, resolved);                                                                   // $11-$18
        await update.ExecuteNonQueryAsync(cancellationToken);
        if (login is not null && AgentChange(before, resolved) is { } change)
        {
            await InvestigationPersonJournal.InsertAsync(
                connection, transaction, null, routeId, resolved.ScopeServerId, AgentChangedEvent,
                InvestigationPersonWrites.PersonActor(login), change.Reason, change.Detail, timeoutSeconds, cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return routeId;
    }

    /// <summary>The journal event a person's change to a route's agent level, limits, scope or match writes (#6274).</summary>
    public const string AgentChangedEvent = "route_agent_changed";

    /// <summary>The sentence a save answers when the route changed since the writer's screen read it.</summary>
    public const string ConflictText = "This route was changed or removed since you opened it. Reload it and make the change again.";

    private const string LockSql = SelectStoredSql + " FOR UPDATE";

    /// <summary>Locks the route row for the rest of the transaction and returns it as it is now. Throws a conflict when the route is gone
    /// or its <c>modified_at</c> is not <paramref name="expected"/> (when one was given).</summary>
    private static async Task<NotificationRouteDraft> LockAndCheckAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int routeId, DateTime? expected, int timeoutSeconds,
        CancellationToken cancellationToken)
    {
        var row = await TryLockAsync(connection, transaction, routeId, timeoutSeconds, cancellationToken);
        if (row is null || (expected is { } want && want != default && row.ModifiedAtUtc?.Ticks != want.Ticks))
        {
            throw new NotificationRouteRefusedException(ConflictText, NotificationRouteRefusalKind.Conflict);
        }

        return row;
    }

    /// <summary>Locks the route row for the rest of the transaction and returns it as it is now, or null when the route is gone.</summary>
    private static async Task<NotificationRouteDraft?> TryLockAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int routeId, int timeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(LockSql, connection, transaction);
        command.CommandTimeout = timeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadStoredRow(reader) : null;
    }

    /// <summary>The reason and the before and after JSON of a change to the agent level, limits, scope or match, or to whether the route
    /// is enabled, or null when none of them changed. A new route (<paramref name="before"/> null) journals only when it sets an agent
    /// level; a deleted one (<paramref name="after"/> null) only when it had one. A disabled route is dropped from the agent lookup (an
    /// Off route included), so enabling or disabling a route that has or had an agent level moves servers between agent routes.</summary>
    private static (string Reason, string Detail)? AgentChange(NotificationRouteDraft? before, NotificationRouteDraft? after)
    {
        if ((before is null && after?.AgentLevel is null) || (after is null && before?.AgentLevel is null))
        {
            return null;
        }

        var names = new List<string>();
        void Compare(string name, object? was, object? now)
        {
            if (!Equals(was, now))
            {
                names.Add(name);
            }
        }

        Compare("metric_match", before?.MetricMatch.Trim(), after?.MetricMatch.Trim());
        Compare("scope_server_id", before?.ScopeServerId, after?.ScopeServerId);
        Compare("scope_tag_id", before?.ScopeTagId, after?.ScopeTagId);
        Compare("agent_level", before?.AgentLevel, after?.AgentLevel);
        Compare("agent_max_per_hour", before?.AgentMaxPerHour, after?.AgentMaxPerHour);
        Compare("agent_max_per_day", before?.AgentMaxPerDay, after?.AgentMaxPerDay);
        Compare("agent_lease_minutes", before?.AgentLeaseMinutes, after?.AgentLeaseMinutes);
        Compare("agent_expiry_minutes", before?.AgentExpiryMinutes, after?.AgentExpiryMinutes);
        if (before?.AgentLevel is not null || after?.AgentLevel is not null)
        {
            Compare("enabled", before?.Enabled, after?.Enabled);
        }

        if (names.Count == 0)
        {
            return null;
        }

        static System.Text.Json.Nodes.JsonObject? Describe(NotificationRouteDraft? row) => row is null ? null : new System.Text.Json.Nodes.JsonObject
        {
            ["metric_match"] = row.MetricMatch.Trim(),
            ["scope_server_id"] = row.ScopeServerId,
            ["scope_tag_id"] = row.ScopeTagId,
            ["agent_level"] = row.AgentLevel,
            ["agent_max_per_hour"] = row.AgentMaxPerHour,
            ["agent_max_per_day"] = row.AgentMaxPerDay,
            ["agent_lease_minutes"] = row.AgentLeaseMinutes,
            ["agent_expiry_minutes"] = row.AgentExpiryMinutes,
            ["enabled"] = row.Enabled,
        };

        var detail = new System.Text.Json.Nodes.JsonObject { ["before"] = Describe(before), ["after"] = Describe(after) };
        return (after is null ? "deleted route" : "changed " + string.Join(", ", names), detail.ToJsonString());
    }

    /// <summary>Whether the route holds a signing key sealed for a stored URL text other than <paramref name="newUrlText"/> (#5998).
    /// The read takes the row's lock (<c>FOR UPDATE</c>) and the caller's transaction keeps it to the commit, so a Generate in another
    /// viewer waits, then finds the URL changed and is refused: no key is left tied to the old URL.</summary>
    public static async Task<bool> KeyWouldBreakAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int routeId, string newUrlText, int timeoutSeconds,
        CancellationToken cancellationToken)
    {
        await using var keyRead = new NpgsqlCommand(
            "SELECT signing_key <> '', generic_url FROM config_notification_routes WHERE route_id = $1 FOR UPDATE", connection, transaction);
        keyRead.CommandTimeout = timeoutSeconds;
        keyRead.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        await using var keyReader = await keyRead.ExecuteReaderAsync(cancellationToken);
        return await keyReader.ReadAsync(cancellationToken) && keyReader.GetBoolean(0)
            && !string.Equals(newUrlText, keyReader.GetString(1), StringComparison.Ordinal);
    }
}
