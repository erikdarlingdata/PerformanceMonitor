/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's control-plane writes to <c>config.config_notification</c> — the single-row (id=1) desired
/// state for SMTP + Teams/Slack alert delivery the Stage-1 <c>StoreConfigProvider</c> reads and hot-swaps into
/// the running service's <c>SmtpConfig</c>/<c>WebhooksConfig</c>. This is the Stage-3b rewire that replaces the
/// severed viewer-local SMTP/webhook block (formerly <see cref="ViewerAppSettings"/> + Windows Credential
/// Manager): saving the Settings window's Email + Teams/Slack sections now drives the running service's
/// delivery.
///
/// <para>Same discipline as the other write partials: public-const SQL (Darling.Tests pin the dialect +
/// column parity with <c>StoreConfigProvider.ReadNotificationAsync</c>), bound <c>$N</c> parameters, routed
/// through <see cref="ExecuteWriteAsync"/> so a read-only seat degrades to <see cref="ViewerReadOnlyException"/>.
/// The single global row is <c>id = 1</c>; <c>modified_at</c> is server-side naive-UTC.</para>
///
/// <para><b>The SMTP secret.</b> <c>smtp_encrypted_password</c> is NEVER plaintext in Postgres — it is the
/// DPAPI-LocalMachine blob <see cref="ViewerServerSecret.Protect"/> produces, the SAME format the service's
/// <c>DarlingAlertSettings.GetSmtpPassword</c> reads back with <c>DarlingSecrets.Unprotect</c> (identical
/// entropy + scope, pinned by a Darling.Tests round-trip). The managed single-box deploy (viewer + service on
/// one host) shares the machine DPAPI key, so the service decrypts what the viewer sealed. The webhook URLs
/// carry as plain text in <c>teams_url</c>/<c>slack_url</c> exactly as darling.json holds them (the service
/// treats a channel as enabled by a non-empty URL — so a disabled channel writes an EMPTY url, matching the
/// service's <c>TeamsWebhookEnabled =&gt; !IsNullOrWhiteSpace(TeamsUrl)</c> derivation).</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /* The 19 SmtpConfig + WebhooksConfig columns in the SAME order the service reads them
       (StoreConfigProvider.ReadNotificationAsync), so the parity test pins one list against both ends. */
    private const string NotificationColumns =
        "smtp_host, smtp_port, smtp_use_ssl, smtp_username, smtp_encrypted_password, smtp_from_address, " +
        "smtp_recipients, email_cooldown_minutes, teams_url, teams_proxy, slack_url, slack_proxy, " +
        "generic_url, generic_headers, generic_body_template, generic_proxy, " +
        "pagerduty_routing_key, pagerduty_use_eu_region, pagerduty_proxy";

    /// <summary>The single global notification row (id=1), for the Settings window prefill + migrate-in check.</summary>
    public const string NotificationSelectSql =
        "SELECT " + NotificationColumns + " FROM config_notification WHERE id = 1";

    /* The non-secret subset of NotificationColumns a read-only viewer role CAN read. The seven secret
       columns (smtp_encrypted_password, smtp_username, teams_url, slack_url, generic_url, pagerduty_routing_key
       — webhook URLs and routing keys are bearer secrets — and generic_headers, which holds the Authorization
       token itself) are column-REVOKEd from the viewer role (#1262 / DarlingManagedRoles.ViewerRestrictedConfigTables),
       so selecting them raises SQLSTATE 42501 for a read-only seat and — in the Settings load — one such
       throw would blank every later section. */
    private const string NotificationNonSecretColumns =
        "smtp_host, smtp_port, smtp_use_ssl, smtp_from_address, smtp_recipients, " +
        "email_cooldown_minutes, teams_proxy, slack_proxy, generic_body_template, generic_proxy, " +
        "pagerduty_use_eu_region, pagerduty_proxy";

    /// <summary>The secret-free notification projection a read-only <c>viewer</c> seat reads (D7 degradation):
    /// the four carved secret columns are omitted, so it never 42501s for a viewer. The secret fields come back
    /// null/empty (the viewer can't author them anyway); the non-secret fields prefill the Settings window.</summary>
    public const string NotificationSelectNoSecretSql =
        "SELECT " + NotificationNonSecretColumns + " FROM config_notification WHERE id = 1";

    /// <summary>Upserts the single global notification row (Settings window Save). ON CONFLICT rewrites every
    /// column and bumps <c>config_version</c> via the V17 trigger. $1..$19 in <see cref="NotificationColumns"/> order.</summary>
    public const string NotificationUpsertSql = @"
INSERT INTO config_notification (id, " + NotificationColumns + @", modified_at)
VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, (now() AT TIME ZONE 'UTC'))
ON CONFLICT (id) DO UPDATE SET
    smtp_host = EXCLUDED.smtp_host,
    smtp_port = EXCLUDED.smtp_port,
    smtp_use_ssl = EXCLUDED.smtp_use_ssl,
    smtp_username = EXCLUDED.smtp_username,
    smtp_encrypted_password = EXCLUDED.smtp_encrypted_password,
    smtp_from_address = EXCLUDED.smtp_from_address,
    smtp_recipients = EXCLUDED.smtp_recipients,
    email_cooldown_minutes = EXCLUDED.email_cooldown_minutes,
    teams_url = EXCLUDED.teams_url,
    teams_proxy = EXCLUDED.teams_proxy,
    slack_url = EXCLUDED.slack_url,
    slack_proxy = EXCLUDED.slack_proxy,
    generic_url = EXCLUDED.generic_url,
    generic_headers = EXCLUDED.generic_headers,
    generic_body_template = EXCLUDED.generic_body_template,
    generic_proxy = EXCLUDED.generic_proxy,
    pagerduty_routing_key = EXCLUDED.pagerduty_routing_key,
    pagerduty_use_eu_region = EXCLUDED.pagerduty_use_eu_region,
    pagerduty_proxy = EXCLUDED.pagerduty_proxy,
    modified_at = (now() AT TIME ZONE 'UTC')";

    /// <summary>Reads the single global notification row, or null when the store has not seeded it yet. A
    /// read-only <c>viewer</c> seat is column-denied the four secret columns, so it degrades to the secret-free
    /// <see cref="NotificationSelectNoSecretSql"/> projection (D7) instead of failing the read with 42501 —
    /// which, in the Settings load, would otherwise blank the later sections.</summary>
    public async Task<NotificationRow?> GetNotificationAsync(CancellationToken cancellationToken = default)
    {
        if (IsReadOnly)
        {
            await using var noSecretCommand = _dataSource.CreateCommand(NotificationSelectNoSecretSql);
            noSecretCommand.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            await using var noSecretReader = await noSecretCommand.ExecuteReaderAsync(cancellationToken);
            return await noSecretReader.ReadAsync(cancellationToken) ? ReadNotificationRowNoSecret(noSecretReader) : null;
        }

        await using var command = _dataSource.CreateCommand(NotificationSelectSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadNotificationRow(reader) : null;
    }

    /// <summary>Upserts the single global notification row (Settings window Save). Read-only seats throw
    /// <see cref="ViewerReadOnlyException"/> via <see cref="ExecuteWriteAsync"/>.</summary>
    public async Task UpsertNotificationAsync(NotificationRow row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);

        await using var command = _dataSource.CreateCommand(NotificationUpsertSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.SmtpHost });            // $1
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.SmtpPort });               // $2
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = row.SmtpUseSsl });            // $3
        AddNullableText(command, row.SmtpUsername);                                                    // $4
        AddNullableText(command, row.SmtpEncryptedPassword);                                           // $5
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.SmtpFromAddress });     // $6
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.SmtpRecipients });      // $7
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = row.EmailCooldownMinutes });   // $8
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.TeamsUrl });            // $9
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.TeamsProxy });          // $10
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.SlackUrl });            // $11
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.SlackProxy });          // $12
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.GenericUrl });          // $13
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.GenericHeaders });      // $14
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.GenericBodyTemplate }); // $15
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.GenericProxy });        // $16
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.PagerDutyRoutingKey }); // $17
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = row.PagerDutyUseEuRegion });  // $18
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = row.PagerDutyProxy ?? "" });  // $19
        await ExecuteWriteAsync(command, cancellationToken);
    }

    private static NotificationRow ReadNotificationRow(NpgsqlDataReader reader) => new()
    {
        SmtpHost = reader.GetString(0),
        SmtpPort = reader.GetInt32(1),
        SmtpUseSsl = reader.GetBoolean(2),
        SmtpUsername = reader.IsDBNull(3) ? null : reader.GetString(3),
        SmtpEncryptedPassword = reader.IsDBNull(4) ? null : reader.GetString(4),
        SmtpFromAddress = reader.GetString(5),
        SmtpRecipients = reader.GetString(6),
        EmailCooldownMinutes = reader.GetInt32(7),
        TeamsUrl = reader.GetString(8),
        TeamsProxy = reader.GetString(9),
        SlackUrl = reader.GetString(10),
        SlackProxy = reader.GetString(11),
        GenericUrl = reader.GetString(12),
        GenericHeaders = reader.GetString(13),
        GenericBodyTemplate = reader.GetString(14),
        GenericProxy = reader.GetString(15),
        PagerDutyRoutingKey = reader.GetString(16),
        PagerDutyUseEuRegion = reader.GetBoolean(17),
        PagerDutyProxy = reader.GetString(18),
    };

    /// <summary>
    /// Reads a <see cref="NotificationRow"/> from the secret-free projection
    /// (<see cref="NotificationSelectNoSecretSql"/>) a read-only <c>viewer</c> seat uses (D7): identical to
    /// <see cref="ReadNotificationRow"/> but the seven carved secret columns (<c>smtp_username</c>,
    /// <c>smtp_encrypted_password</c>, <c>teams_url</c>, <c>slack_url</c>, <c>generic_url</c>,
    /// <c>generic_headers</c>, <c>pagerduty_routing_key</c>) are not selected, so they stay null/empty and
    /// every column after each shifts ordinal. The viewer can't author those secrets on a read-only seat
    /// anyway, so the empty values are harmless — the non-secret fields still prefill.
    /// </summary>
    private static NotificationRow ReadNotificationRowNoSecret(NpgsqlDataReader reader) => new()
    {
        SmtpHost = reader.GetString(0),
        SmtpPort = reader.GetInt32(1),
        SmtpUseSsl = reader.GetBoolean(2),
        SmtpUsername = null,          /* carved — not selected for a read-only viewer (#1262) */
        SmtpEncryptedPassword = null, /* carved */
        SmtpFromAddress = reader.GetString(3),
        SmtpRecipients = reader.GetString(4),
        EmailCooldownMinutes = reader.GetInt32(5),
        TeamsUrl = "",                /* carved */
        TeamsProxy = reader.GetString(6),
        SlackUrl = "",                /* carved */
        SlackProxy = reader.GetString(7),
        GenericUrl = "",              /* carved */
        GenericHeaders = "",          /* carved — holds the Authorization bearer token (#1506) */
        GenericBodyTemplate = reader.GetString(8),
        GenericProxy = reader.GetString(9),
        PagerDutyRoutingKey = "",     /* carved — bearer secret like webhook URLs */
        PagerDutyUseEuRegion = reader.GetBoolean(10),
        PagerDutyProxy = reader.GetString(11),
    };
}

/// <summary>
/// A <c>config.config_notification</c> row (the single id=1 global row) as the viewer authors + reads it —
/// the desired-state twin of the service's <c>SmtpConfig</c> + <c>WebhooksConfig</c>. There is no per-channel
/// "enabled" column (the service derives enablement from non-empty fields: SMTP from host+from+to, a webhook
/// from its URL); the Settings window maps its enable toggles onto that by clearing the channel's fields when
/// unchecked. <see cref="SmtpEncryptedPassword"/> is a DPAPI-LocalMachine blob, never plaintext. Defaults
/// mirror the V17 DDL so <see cref="Defaults"/> equals a freshly-seeded row.
/// </summary>
public sealed class NotificationRow
{
    public string SmtpHost { get; set; } = "";
    public int SmtpPort { get; set; } = 587;
    public bool SmtpUseSsl { get; set; } = true;
    public string? SmtpUsername { get; set; }

    /// <summary>DPAPI-LocalMachine base64 blob (<see cref="ViewerServerSecret.Protect"/>), or null when unset.</summary>
    public string? SmtpEncryptedPassword { get; set; }

    public string SmtpFromAddress { get; set; } = "";
    public string SmtpRecipients { get; set; } = "";
    public int EmailCooldownMinutes { get; set; } = 15;
    public string TeamsUrl { get; set; } = "";
    public string TeamsProxy { get; set; } = "";
    public string SlackUrl { get; set; } = "";
    public string SlackProxy { get; set; } = "";

    /* Generic webhook (#1506 / V26). GenericUrl and GenericHeaders are secrets — the headers JSON carries
       the Authorization bearer token — and are column-REVOKEd from the read-only viewer role. */
    public string GenericUrl { get; set; } = "";
    public string GenericHeaders { get; set; } = "";
    public string GenericBodyTemplate { get; set; } = "";
    public string GenericProxy { get; set; } = "";

    /* PagerDuty webhook (V40). PagerDutyRoutingKey is a bearer secret — column-REVOKEd from the read-only
       viewer role alongside the webhook URLs. */
    public string PagerDutyRoutingKey { get; set; } = "";
    public bool PagerDutyUseEuRegion { get; set; }
    public string PagerDutyProxy { get; set; } = "";

    /// <summary>A row equal to the V17 seed defaults — the migrate-in "is the service section still at defaults?" baseline.</summary>
    public static NotificationRow Defaults() => new();

    /// <summary>Value-equality against another row — the migrate-in uses it to detect an untouched (default)
    /// store section and a genuine viewer customization. The DPAPI blob is compared ordinally (it is opaque).</summary>
    public bool ValueEquals(NotificationRow other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return string.Equals(SmtpHost, other.SmtpHost, StringComparison.Ordinal)
            && SmtpPort == other.SmtpPort
            && SmtpUseSsl == other.SmtpUseSsl
            && string.Equals(SmtpUsername ?? "", other.SmtpUsername ?? "", StringComparison.Ordinal)
            && string.Equals(SmtpEncryptedPassword ?? "", other.SmtpEncryptedPassword ?? "", StringComparison.Ordinal)
            && string.Equals(SmtpFromAddress, other.SmtpFromAddress, StringComparison.Ordinal)
            && string.Equals(SmtpRecipients, other.SmtpRecipients, StringComparison.Ordinal)
            && EmailCooldownMinutes == other.EmailCooldownMinutes
            && string.Equals(TeamsUrl, other.TeamsUrl, StringComparison.Ordinal)
            && string.Equals(TeamsProxy, other.TeamsProxy, StringComparison.Ordinal)
            && string.Equals(SlackUrl, other.SlackUrl, StringComparison.Ordinal)
            && string.Equals(SlackProxy, other.SlackProxy, StringComparison.Ordinal)
            && string.Equals(GenericUrl, other.GenericUrl, StringComparison.Ordinal)
            && string.Equals(GenericHeaders, other.GenericHeaders, StringComparison.Ordinal)
            && string.Equals(GenericBodyTemplate, other.GenericBodyTemplate, StringComparison.Ordinal)
            && string.Equals(GenericProxy, other.GenericProxy, StringComparison.Ordinal)
            && string.Equals(PagerDutyRoutingKey, other.PagerDutyRoutingKey, StringComparison.Ordinal)
            && PagerDutyUseEuRegion == other.PagerDutyUseEuRegion
            && PagerDutyProxy == other.PagerDutyProxy;
    }
}
