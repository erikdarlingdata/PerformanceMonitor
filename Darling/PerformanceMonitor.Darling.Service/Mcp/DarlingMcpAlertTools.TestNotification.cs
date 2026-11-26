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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>send_test_notification</c> (#9636): the fixed sample the Viewer's Settings test buttons send, sent from the service through
/// <see cref="WebhookAlertService"/> and <see cref="EmailSendCore"/>, the delivery code real alerts use, with the channel settings
/// the service holds. An answer never carries a webhook URL, a header value, a routing key or the SMTP login: the exception text is
/// stripped of them before it leaves, and the log line carries only the channel, the status and the exception's type.
/// </summary>
public sealed partial class DarlingMcpAlertTools
{
    private static readonly Regex s_urlText = new(@"[a-zA-Z][a-zA-Z0-9+.\-]*://\S+", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex s_hostPortText = new(@"\(\s*[^()\s]+:\d+\s*\)", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "send_test_notification"), Description(
        "Sends the Viewer's fixed test message to one notification channel from the service, so a channel set up over MCP is proven from the machine that sends real alerts. Needs a named operator token, or a token with mcp.allowWrites. " +
        "<<GUIDE>>" +
        "channel is teams, slack, pagerduty, webhook (the generic webhook) or email. The send uses the default channel settings the service has loaded (the ones update_notification_channels and the Viewer's Settings > Notifications change), through the same delivery code real alerts use, with the same timeouts: 10 seconds for a webhook post, 30 seconds for email. " +
        "A channel that is not set up answers status not_configured and sends nothing. A test send skips mute rules, quiet windows, routes and digests, as the Viewer's test button does, so it always goes to the default channel and is not recorded in the alert history. " +
        "PagerDuty gets one trigger event under a throwaway dedup key, which the test does not resolve afterwards (the Viewer's test does the same), so resolve it in PagerDuty if it opens an incident. The generic webhook is signed when it has a signing key. " +
        "A change saved a moment ago can take up to about 15 seconds to reach the service, so a call right after update_notification_channels can still see the old values. " +
        "Never returns a URL, header value, routing key or SMTP login: the failure text has them removed. " +
        "Returns {status:\"delivered\"|\"failed\"|\"not_configured\"|\"invalid\"|\"not_ready\", channel, delivered, http_status (webhook channels, null when no response came back), smtp_status (email), exception_type and message on a failure}.")]
    public static async Task<string> SendTestNotification(
        [Description("teams, slack, pagerduty, webhook or email.")] string channel,
        NotificationTestSeat? seat = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var settings = seat?.Read();
            var name = (channel ?? "").Trim().ToLowerInvariant();
            if (name == "generic")
            {
                name = "webhook";
            }

            if (name is not ("teams" or "slack" or "pagerduty" or "webhook" or "email"))
            {
                return Outcome("invalid", "channel must be teams, slack, pagerduty, webhook or email.");
            }

            if (settings is null)
            {
                return Outcome("not_ready", "The service has not loaded its notification settings yet. Try again in a moment.");
            }

            var (result, secrets) = await RunTestSendAsync(name, settings, cancellationToken);
            var answer = TestSendAnswer(name, result, secrets);
            /* Channel, outcome and the exception's type only: the exception's text can name the destination. */
            logger?.LogInformation(
                "MCP test notification send_test_notification by {Identity}: channel {Channel}, delivered {Delivered}, http status {HttpStatus}, exception {ExceptionType}",
                CallerOf(), name, result.Delivered, result.HttpStatus, result.ExceptionType ?? "none");
            return answer;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("send_test_notification", ex);
        }
    }

    /// <summary>Sends the channel's test through the delivery code with the settings, and names the secret values of that channel so the
    /// answer can be stripped of them.</summary>
    internal static async Task<(TestSendResult Result, IReadOnlyList<string?> Secrets)> RunTestSendAsync(
        string channel, DarlingAlertSettings settings, CancellationToken cancellationToken)
    {
        var branding = DarlingAlertDeliverer.Branding;
        switch (channel)
        {
            case "teams":
                return (await WebhookAlertService.SendTestTeamsDetailedAsync(
                    settings.TeamsWebhookUrl, NullIfBlank(settings.TeamsProxyAddress), branding, includeBody: false, cancellationToken),
                    UrlSecrets(settings.TeamsWebhookUrl));
            case "slack":
                return (await WebhookAlertService.SendTestSlackDetailedAsync(
                    settings.SlackWebhookUrl, NullIfBlank(settings.SlackProxyAddress), branding, includeBody: false, cancellationToken),
                    UrlSecrets(settings.SlackWebhookUrl));
            case "pagerduty":
                return (await WebhookAlertService.SendTestPagerDutyDetailedAsync(
                    settings.PagerDutyRoutingKey, settings.PagerDutyUseEuRegion, branding, NullIfBlank(settings.PagerDutyProxyAddress), includeBody: false, cancellationToken),
                    new List<string?> { settings.PagerDutyRoutingKey });
            case "webhook":
                var secrets = UrlSecrets(settings.GenericWebhookUrl);
                secrets.AddRange(HeaderValues(settings.GenericWebhookHeadersJson));
                return (await WebhookAlertService.SendTestGenericDetailedAsync(
                    settings.GenericWebhookUrl, NullIfBlank(settings.GenericWebhookHeadersJson), NullIfBlank(settings.GenericWebhookBodyTemplate),
                    NullIfBlank(settings.GenericWebhookProxyAddress), branding, settings.GenericWebhookSigning, includeBody: false, cancellationToken),
                    secrets);
            default:
                return (await EmailSendCore.SendTestEmailDetailedAsync(settings, branding),
                    new List<string?> { settings.SmtpUsername, settings.GetSmtpPassword() });
        }
    }

    internal static string TestSendAnswer(string channel, TestSendResult result, IReadOnlyList<string?> secrets)
    {
        var status = result.Delivered ? "delivered" : result.NotConfigured ? "not_configured" : "failed";
        return JsonSerializer.Serialize(new
        {
            status,
            channel,
            delivered = result.Delivered,
            http_status = result.HttpStatus,
            smtp_status = result.SmtpStatus,
            exception_type = result.ExceptionType,
            message = result.Delivered ? "The test notification was sent." : StripSecrets(result.Error, secrets),
        }, McpHelpers.JsonOptions);
    }

    private static string? NullIfBlank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

    /// <summary>A URL's secret parts: the whole text and every piece an exception could quote (host, path, query, login).</summary>
    private static List<string?> UrlSecrets(string? url)
    {
        var parts = new List<string?> { url };
        if (Uri.TryCreate(url, UriKind.Absolute, out var uri))
        {
            parts.Add(uri.Host);
            parts.Add(uri.UserInfo);
            parts.Add(uri.Query);
            parts.Add(uri.AbsolutePath);
        }

        return parts;
    }

    private static List<string?> HeaderValues(string? headersJson)
    {
        var values = new List<string?>();
        if (string.IsNullOrWhiteSpace(headersJson))
        {
            return values;
        }

        try
        {
            using var doc = JsonDocument.Parse(headersJson);
            if (doc.RootElement.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in doc.RootElement.EnumerateObject())
                {
                    if (property.Value.ValueKind == JsonValueKind.String)
                    {
                        values.Add(property.Value.GetString());
                    }
                }
            }
        }
        catch (JsonException)
        {
            /* Headers that are not JSON never sent: the send's own refusal says so, and there is no value to strip. */
        }

        return values;
    }

    /// <summary>The failure text with every URL, every <c>(host:port)</c> and every named secret value removed, cut to 240 characters.</summary>
    internal static string? StripSecrets(string? text, IEnumerable<string?> secrets)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        var clean = text;
        foreach (var secret in secrets)
        {
            if (!string.IsNullOrEmpty(secret) && secret.Length >= 3)
            {
                clean = clean.Replace(secret, "[hidden]", StringComparison.OrdinalIgnoreCase);
            }
        }

        clean = s_urlText.Replace(clean, "[url hidden]");
        clean = s_hostPortText.Replace(clean, "(address hidden)");
        return clean.Length <= 240 ? clean : clean[..240] + "...";
    }
}
