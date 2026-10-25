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
/// <c>update_notification_channels</c> (#6392): the default notification channels the Viewer sets in Settings > Notifications (SMTP,
/// Teams, Slack, the generic webhook and PagerDuty), which the routes fall back to. A secret is sealed here by the same code the Viewer
/// uses (<see cref="SmtpPasswordSealing"/> and <see cref="NotificationRouteWriter.SealNewSettingsValue"/>, over the password key ring) and
/// written through the store's <c>config.set_notification_channels</c> function: the <c>mcp</c> login holds no read or write on a secret
/// column, and the function stores only sealed text and makes the Viewer's refusals on the stored row. An answer never shows a secret: it
/// says set or not set, and shows every other setting in full.
/// </summary>
public sealed partial class DarlingMcpAlertTools
{
    /// <summary>The values a call carries: null (or blank) leaves a setting alone, <c>-</c> clears a text setting.</summary>
    internal sealed record NotificationChannelsRequest(
        string? SmtpHost, int? SmtpPort, bool? SmtpSsl, string? SmtpUser, string? SmtpPassword, string? SmtpFrom, string? SmtpRecipients,
        string? Teams, string? TeamsProxy, string? Slack, string? SlackProxy,
        string? Generic, string? GenericHeaders, string? GenericBody, string? GenericProxy,
        string? PagerDuty, bool? PagerDutyEu, string? PagerDutyProxy, bool? PagerDutyAutoResolve);

    /// <summary>The settings of the notification row the <c>mcp</c> login may read (everything but the secrets).</summary>
    private sealed record ChannelSettings(
        string SmtpHost, int SmtpPort, bool SmtpSsl, string SmtpFrom, string SmtpRecipients, string TeamsProxy, string SlackProxy,
        string GenericBody, string GenericProxy, bool PagerDutyEu, string PagerDutyProxy, bool PagerDutyAutoResolve);

    [McpServerTool(Name = "update_notification_channels"), Description(
        "Sets the default notification channels (the Viewer's Settings > Notifications) that routes fall back to: SMTP, Teams, Slack, the generic webhook and PagerDuty. Only the fields you pass change; - clears a text field. Changes shared alert configuration. Returns updated with each setting before and after (a secret as set or not set, never its value) and an undo for the non-secret settings, or invalid or refused." +
        "<<GUIDE>>" +
        "Sets the channels that alerts go to when no route covers them. A field left out keeps its value; pass - to remove a text field (the SMTP password, user, host, recipients, a webhook URL, the headers, the body template, a proxy). " +
        "Secrets (smtp_user, smtp_password, teams_webhook_url, slack_webhook_url, generic_webhook_url, generic_headers, pagerduty_routing_key) are sealed by the service the way the Viewer seals them and can never be read back: the answer says set or not set. " +
        "The SMTP password is sealed for the host, port, SSL flag and user name, so smtp_password needs smtp_user in the same call (- for none); changing the host, port, SSL flag or user while a password is saved needs the password again. " +
        "A webhook value is sealed for its proxy: changing a proxy while its URL or key is saved needs the URL or key again. The generic headers are sealed for the generic URL as stored, so generic_headers goes with generic_webhook_url in the same call and a new generic URL needs the headers again; a saved signing key blocks a generic URL or proxy change until it is removed in the Viewer. " +
        "A URL must be an absolute http or https URL; a link-local or cloud instance address is refused. Recipients are comma separated plain addresses, at most 50. generic_headers is a JSON object, generic_body_template a template; the Viewer's checks apply. " +
        "If the service has no password key the secret fields are refused with the same words the Viewer shows; nothing is written. " +
        "Needs mcp.allowWrites and a token. The database role this tool runs as can neither read nor write a secret column: the row is changed by the store's own config.set_notification_channels function, which re-reads the row under its lock. " +
        "Returns {status:\"updated\", changed, settings:{field:{before, after}}, undo:{tool, arguments, note}}, or {status:\"unchanged\"}. undo puts the non-secret settings back; a secret cannot be restored by undo, pass it again.")]
    public static Task<string> UpdateNotificationChannels(
        NpgsqlDataSource postgres,
        [Description("SMTP server host name. Omit to keep; - removes it.")] string? smtp_host = null,
        [Description("SMTP port, 1 to 65535. Omit to keep.")] int? smtp_port = null,
        [Description("Use SSL/TLS for SMTP. Omit to keep.")] bool? smtp_ssl = null,
        [Description("SMTP user name. Secret: shown as set or not set. Omit to keep; - removes it.")] string? smtp_user = null,
        [Description("SMTP password. Secret: sealed, never shown. Needs smtp_user in the same call. Omit to keep; - removes it.")] string? smtp_password = null,
        [Description("The From address of alert email. Omit to keep; - removes it.")] string? smtp_from = null,
        [Description("Default email recipients, comma separated plain addresses (at most 50). Omit to keep; - removes them.")] string? smtp_recipients = null,
        [Description("Teams webhook URL (absolute http or https). Secret. Omit to keep; - removes it.")] string? teams_webhook_url = null,
        [Description("Proxy address for Teams. Omit to keep; - removes it.")] string? teams_proxy = null,
        [Description("Slack webhook URL (absolute http or https). Secret. Omit to keep; - removes it.")] string? slack_webhook_url = null,
        [Description("Proxy address for Slack. Omit to keep; - removes it.")] string? slack_proxy = null,
        [Description("Generic webhook URL (absolute http or https). Secret. Omit to keep; - removes it.")] string? generic_webhook_url = null,
        [Description("Generic webhook headers as a JSON object. Secret. Needs generic_webhook_url in the same call. Omit to keep; - removes them.")] string? generic_headers = null,
        [Description("Generic webhook body template. Omit to keep; - returns to the built-in default.")] string? generic_body_template = null,
        [Description("Proxy address for the generic webhook. Omit to keep; - removes it.")] string? generic_proxy = null,
        [Description("PagerDuty Events API v2 routing key. Secret. Omit to keep; - removes it.")] string? pagerduty_routing_key = null,
        [Description("Send PagerDuty events to the EU region. Omit to keep.")] bool? pagerduty_eu_region = null,
        [Description("Proxy address for PagerDuty. Omit to keep; - removes it.")] string? pagerduty_proxy = null,
        [Description("Resolve the PagerDuty incident when the alert clears. Omit to keep.")] bool? pagerduty_auto_resolve = null,
        ILogger? logger = null,
        PostgresConfig? postgresConfig = null,
        CancellationToken cancellationToken = default) =>
        UpdateNotificationChannelsAsync(
            postgres,
            new NotificationChannelsRequest(
                smtp_host, smtp_port, smtp_ssl, smtp_user, smtp_password, smtp_from, smtp_recipients, teams_webhook_url, teams_proxy,
                slack_webhook_url, slack_proxy, generic_webhook_url, generic_headers, generic_body_template, generic_proxy,
                pagerduty_routing_key, pagerduty_eu_region, pagerduty_proxy, pagerduty_auto_resolve),
            DarlingPasswordKey.Current, logger, cancellationToken, StoreRolesAreRunByOperator(postgresConfig));

    /// <summary>The settings write: read the stored settings, validate and seal, call the store function, read them again, in one transaction.</summary>
    internal static async Task<string> UpdateNotificationChannelsAsync(
        NpgsqlDataSource postgres, NotificationChannelsRequest request, IPasswordKeyRing ring, ILogger? logger, CancellationToken cancellationToken,
        bool selfManagedStore = true)
    {
        try
        {
            ArgumentNullException.ThrowIfNull(postgres);
            ArgumentNullException.ThrowIfNull(request);
            ArgumentNullException.ThrowIfNull(ring);

            string[] columns;
            object?[] values;
            ChannelSettings before;
            ChannelSettings after;
            bool?[] previousSaved;
            var secretNow = new Dictionary<string, string>(StringComparer.Ordinal);
            try
            {
                await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
                before = await ReadChannelSettingsAsync(connection, transaction, cancellationToken)
                    ?? throw new NotificationRouteRefusedException("The notification settings row does not exist yet.", NotificationRouteRefusalKind.Validation);
                (columns, values) = BuildChannelWrite(request, before, ring, secretNow);
                if (columns.Length == 0)
                {
                    return JsonSerializer.Serialize(
                        new { status = "unchanged", message = "Nothing differs from the stored channel settings.", settings = SettingsPayload(before) }, McpHelpers.JsonOptions);
                }

                previousSaved = await CallSetChannelsAsync(connection, transaction, columns, values, cancellationToken);
                after = await ReadChannelSettingsAsync(connection, transaction, cancellationToken) ?? before;
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

            var changed = columns.ToList();
            LogChannelsWrite(logger, CallerOf(), string.Join(", ", changed));
            return JsonSerializer.Serialize(new
            {
                status = "updated",
                changed,
                settings = ChannelDiff(before, after, columns),
                secrets = SecretPayload(previousSaved, secretNow),
                undo = new
                {
                    tool = "update_notification_channels",
                    arguments = ChannelUndoArguments(before, columns),
                    note = "undo puts the non-secret settings back. A secret cannot be restored by undo: pass it again in update_notification_channels. A proxy that changed back needs its URL or key again if one is saved.",
                },
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("update_notification_channels", ex);
        }
    }

    /// <summary>Reads the settings row under its row lock (#6392), so a save from the Viewer cannot land between this read and the store
    /// function's own lock and leave a secret sealed for settings that have since changed. The <c>mcp</c> role's UPDATE on a column of
    /// the row is what <c>FOR UPDATE</c> needs.</summary>
    private static async Task<ChannelSettings?> ReadChannelSettingsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        const string sql =
            "SELECT smtp_host, smtp_port, smtp_use_ssl, smtp_from_address, smtp_recipients, teams_proxy, slack_proxy, generic_body_template, generic_proxy, "
            + "pagerduty_use_eu_region, pagerduty_proxy, pagerduty_auto_resolve FROM config.config_notification WHERE id = 1 FOR UPDATE";
        await using var command = new NpgsqlCommand(sql, connection) { Transaction = transaction };
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ChannelSettings(
            reader.GetString(0), reader.GetInt32(1), reader.GetBoolean(2), reader.GetString(3), reader.GetString(4), reader.GetString(5),
            reader.GetString(6), reader.GetString(7), reader.GetString(8), reader.GetBoolean(9), reader.GetString(10), reader.GetBoolean(11));
    }

    /// <summary>
    /// Validates the request against the stored settings and seals each secret. Returns the store columns to change and their values in the
    /// function's parameter order. Throws <see cref="NotificationRouteRefusedException"/> for anything the Viewer's checks refuse; nothing is
    /// written then. <paramref name="secretNow"/> receives each secret field given, as set or not set.
    /// </summary>
    private static (string[] Columns, object?[] Values) BuildChannelWrite(
        NotificationChannelsRequest r, ChannelSettings stored, IPasswordKeyRing ring, Dictionary<string, string> secretNow)
    {
        var (seal, refusal) = RouteSealer(ring);
        var columns = new List<string>();
        var v = new object?[19];

        // A text setting: null or blank leaves it, - clears it, anything else is the new value (trimmed).
        string? Text(string? typed) => string.IsNullOrWhiteSpace(typed) ? null : typed.Trim() == NotificationRouteWriter.ClearMarker ? "" : typed.Trim();

        void Plain(string? typed, string column, int index, string current)
        {
            if (Text(typed) is { } value && !string.Equals(value, current, StringComparison.Ordinal))
            {
                if (value.Length > 0 && !SmtpPasswordSealing.IsValidText(value))
                {
                    throw new NotificationRouteRefusedException(SmtpPasswordSealing.FieldCharactersText(column), NotificationRouteRefusalKind.Validation);
                }

                columns.Add(column);
                v[index] = value;
            }
        }

        var port = stored.SmtpPort;
        if (r.SmtpPort is { } newPort && newPort != stored.SmtpPort)
        {
            if (newPort is < 1 or > 65535)
            {
                throw new NotificationRouteRefusedException("The SMTP port is between 1 and 65535.", NotificationRouteRefusalKind.Validation);
            }

            port = newPort;
            columns.Add("smtp_port");
            v[1] = newPort;
        }

        var ssl = stored.SmtpSsl;
        if (r.SmtpSsl is { } newSsl && newSsl != stored.SmtpSsl)
        {
            ssl = newSsl;
            columns.Add("smtp_use_ssl");
            v[2] = newSsl;
        }

        Plain(r.SmtpHost, "smtp_host", 0, stored.SmtpHost);
        var host = v[0] as string ?? stored.SmtpHost;

        if (Text(r.SmtpFrom) is { Length: > 0 } from && !from.Contains('@', StringComparison.Ordinal))
        {
            throw new NotificationRouteRefusedException("The From address must be a valid email address.", NotificationRouteRefusalKind.Validation);
        }

        Plain(r.SmtpFrom, "smtp_from_address", 5, stored.SmtpFrom);
        if (Text(r.SmtpRecipients) is { Length: > 0 } recipients && NotificationRouteWriter.RecipientsRefusal(recipients) is { } badRecipients)
        {
            throw new NotificationRouteRefusedException(badRecipients, NotificationRouteRefusalKind.Validation);
        }

        Plain(r.SmtpRecipients, "smtp_recipients", 6, stored.SmtpRecipients);

        // The SMTP user name and password. The user name is bound into the password's seal and cannot be read back, so it comes with the password.
        string? user = null;
        if (Text(r.SmtpUser) is { } typedUser)
        {
            if (!SmtpPasswordSealing.IsValidText(typedUser))
            {
                throw new NotificationRouteRefusedException(SmtpPasswordSealing.FieldCharactersText("SMTP user name"), NotificationRouteRefusalKind.Validation);
            }

            user = typedUser;
            columns.Add("smtp_username");
            v[3] = typedUser;
            secretNow["smtp_user"] = typedUser.Length > 0 ? "set" : "not set";
        }

        if (Text(r.SmtpPassword) is { } typedPassword)
        {
            columns.Add("smtp_encrypted_password");
            if (typedPassword.Length == 0)
            {
                v[4] = "";
            }
            else
            {
                if (Text(r.SmtpUser) is null)
                {
                    throw new NotificationRouteRefusedException(
                        "The SMTP password is sealed for the user name, which cannot be read back: pass smtp_user in the same call (- for none).",
                        NotificationRouteRefusalKind.Validation);
                }

                if (seal is null)
                {
                    throw new NotificationRouteRefusedException(refusal ?? NotificationRouteWriter.NoKeyText, NotificationRouteRefusalKind.Value);
                }

                v[4] = SmtpPasswordSealing.Seal(typedPassword, host, port, ssl, user is { Length: > 0 } ? user : null, (text, binding) => ring.Seal(text, binding));
            }

            secretNow["smtp_password"] = typedPassword.Length > 0 ? "set" : "not set";
        }

        // The webhooks. Each secret is sealed for the proxy it will have; the generic headers for the generic URL text as it will be stored.
        string? Secret(string? typed, string slot, string column, int index, string field, string? proxy, string? boundUrl, string label)
        {
            if (Text(typed) is not { } value)
            {
                return null;
            }

            columns.Add(column);
            v[index] = value.Length == 0 ? "" : NotificationRouteWriter.SealNewSettingsValue(slot, value, proxy, boundUrl, label, seal, refusal);
            secretNow[field] = value.Length > 0 ? "set" : "not set";
            return v[index] as string;
        }

        // #6392: the send connects to the proxy through the connect guard, so a proxy written as a refused IP literal is refused here.
        void Proxy(string? typed, string column, int index, string current)
        {
            if (Text(typed) is { Length: > 0 } value && WebhookUrlShape.ProxyRefusal(value, column) is { } refused)
            {
                throw new NotificationRouteRefusedException(refused, NotificationRouteRefusalKind.Validation);
            }

            Plain(typed, column, index, current);
        }

        Proxy(r.TeamsProxy, "teams_proxy", 8, stored.TeamsProxy);
        Proxy(r.SlackProxy, "slack_proxy", 10, stored.SlackProxy);
        Proxy(r.GenericProxy, "generic_proxy", 14, stored.GenericProxy);
        Proxy(r.PagerDutyProxy, "pagerduty_proxy", 17, stored.PagerDutyProxy);
        Secret(r.Teams, "teams", "teams_url", 7, "teams_webhook_url", v[8] as string ?? stored.TeamsProxy, null, "Teams webhook URL");
        Secret(r.Slack, "slack", "slack_url", 9, "slack_webhook_url", v[10] as string ?? stored.SlackProxy, null, "Slack webhook URL");
        var genericProxy = v[14] as string ?? stored.GenericProxy;
        var sealedGeneric = Secret(r.Generic, "generic", "generic_url", 11, "generic_webhook_url", genericProxy, null, "generic webhook URL");

        if (Text(r.GenericBody) is not null || Text(r.GenericHeaders) is not null)
        {
            var headersText = Text(r.GenericHeaders) ?? "";
            var bodyText = Text(r.GenericBody) ?? stored.GenericBody;
            if (WebhookAlertService.ValidateGenericConfig(headersText, bodyText) is { } configError)
            {
                throw new NotificationRouteRefusedException(configError, NotificationRouteRefusalKind.Validation);
            }
        }

        Plain(r.GenericBody, "generic_body_template", 13, stored.GenericBody);
        Secret(r.GenericHeaders, "generic_headers", "generic_headers", 12, "generic_headers", genericProxy, sealedGeneric, "generic webhook headers");
        Secret(r.PagerDuty, "pagerduty", "pagerduty_routing_key", 15, "pagerduty_routing_key", v[17] as string ?? stored.PagerDutyProxy, null, "PagerDuty routing key");

        if (r.PagerDutyEu is { } eu && eu != stored.PagerDutyEu)
        {
            columns.Add("pagerduty_use_eu_region");
            v[16] = eu;
        }

        if (r.PagerDutyAutoResolve is { } auto && auto != stored.PagerDutyAutoResolve)
        {
            columns.Add("pagerduty_auto_resolve");
            v[18] = auto;
        }

        return (columns.ToArray(), v);
    }

    private static async Task<bool?[]> CallSetChannelsAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, string[] columns, object?[] values, CancellationToken cancellationToken)
    {
        const string sql = "SELECT * FROM config.set_notification_channels($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)";
        await using var command = new NpgsqlCommand(sql, connection) { Transaction = transaction };
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = columns });
        // Values are in the function's order after the column list: text, integer, boolean, text x4, text...; a null stays null (the column is not in the list).
        var types = new[]
        {
            NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Integer, NpgsqlTypes.NpgsqlDbType.Boolean,
            NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Text,
            NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Text,
            NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Text,
            NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Boolean, NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Boolean,
        };
        for (var i = 0; i < values.Length; i++)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = types[i], Value = values[i] ?? DBNull.Value });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var saved = new bool?[7];
        if (await reader.ReadAsync(cancellationToken))
        {
            for (var i = 0; i < saved.Length; i++)
            {
                saved[i] = reader.IsDBNull(i) ? null : reader.GetBoolean(i);
            }
        }

        return saved;
    }

    private static Dictionary<string, object?> SettingsPayload(ChannelSettings s) => new Dictionary<string, object?>
    {
        ["smtp_host"] = s.SmtpHost, ["smtp_port"] = s.SmtpPort, ["smtp_ssl"] = s.SmtpSsl, ["smtp_from"] = s.SmtpFrom, ["smtp_recipients"] = s.SmtpRecipients,
        ["teams_proxy"] = s.TeamsProxy, ["slack_proxy"] = s.SlackProxy, ["generic_body_template"] = s.GenericBody, ["generic_proxy"] = s.GenericProxy,
        ["pagerduty_eu_region"] = s.PagerDutyEu, ["pagerduty_proxy"] = s.PagerDutyProxy, ["pagerduty_auto_resolve"] = s.PagerDutyAutoResolve,
    };

    private static readonly Dictionary<string, string> s_columnToField = new(StringComparer.Ordinal)
    {
        ["smtp_host"] = "smtp_host", ["smtp_port"] = "smtp_port", ["smtp_use_ssl"] = "smtp_ssl", ["smtp_from_address"] = "smtp_from",
        ["smtp_recipients"] = "smtp_recipients", ["teams_proxy"] = "teams_proxy", ["slack_proxy"] = "slack_proxy",
        ["generic_body_template"] = "generic_body_template", ["generic_proxy"] = "generic_proxy", ["pagerduty_use_eu_region"] = "pagerduty_eu_region",
        ["pagerduty_proxy"] = "pagerduty_proxy", ["pagerduty_auto_resolve"] = "pagerduty_auto_resolve",
    };

    /// <summary>Each non-secret setting that was named, before and after.</summary>
    private static Dictionary<string, object?> ChannelDiff(ChannelSettings before, ChannelSettings after, string[] columns)
    {
        var was = SettingsPayload(before);
        var now = SettingsPayload(after);
        var diff = new Dictionary<string, object?>();
        foreach (var column in columns)
        {
            if (s_columnToField.TryGetValue(column, out var field))
            {
                diff[field] = new { before = was[field], after = now[field] };
            }
        }

        return diff;
    }

    /// <summary>Each secret field named in the call, as set or not set before and after. Never a value.</summary>
    private static Dictionary<string, object?> SecretPayload(bool?[] previousSaved, Dictionary<string, string> secretNow)
    {
        // previousSaved is in the function's return order: smtp user, smtp password, teams, slack, generic URL, generic headers, PagerDuty key.
        var fields = new[] { "smtp_user", "smtp_password", "teams_webhook_url", "slack_webhook_url", "generic_webhook_url", "generic_headers", "pagerduty_routing_key" };
        var payload = new Dictionary<string, object?>();
        for (var i = 0; i < fields.Length; i++)
        {
            if (secretNow.TryGetValue(fields[i], out var now))
            {
                payload[fields[i]] = new { before = previousSaved[i] == true ? "set" : "not set", after = now };
            }
        }

        return payload;
    }

    /// <summary>The call that puts the non-secret settings named in this write back. A cleared text setting is passed as -.</summary>
    private static Dictionary<string, object?> ChannelUndoArguments(ChannelSettings before, string[] columns)
    {
        var was = SettingsPayload(before);
        var arguments = new Dictionary<string, object?>();
        foreach (var column in columns)
        {
            if (s_columnToField.TryGetValue(column, out var field))
            {
                arguments[field] = was[field] is string text && text.Length == 0 ? NotificationRouteWriter.ClearMarker : was[field];
            }
        }

        return arguments;
    }

    /// <summary>One line per channels write: the tool, the token and client identity and the names of the fields changed. Never a value.</summary>
    internal static void LogChannelsWrite(ILogger? logger, string caller, string fields) =>
        logger?.LogInformation("MCP channels write update_notification_channels by {Identity}: fields changed: {Fields}", caller, fields);
}
