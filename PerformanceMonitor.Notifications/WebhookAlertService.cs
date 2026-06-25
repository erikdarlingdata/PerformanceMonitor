/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Sends alert notifications to Microsoft Teams and/or Slack via incoming webhooks.
/// Color-coded accent bars match the existing email alert severity mapping.
/// <para>
/// Shared between Lite and Dashboard (Plan E E3c). Per-app branding (edition label +
/// optional snooze hint) arrives via <see cref="AlertBranding"/>; credentials live in
/// each app's settings adapter (<see cref="IAlertSettings"/>) — this service carries no
/// credential storage of its own. The Dashboard-only <c>Current</c> handle and the
/// MCP/health surfaces stay app-side: Dashboard keeps a reference to the injected
/// instance.
/// </para>
/// </summary>
public class WebhookAlertService
{
    /* Webhooks do not deliver the copy-paste T-SQL (too large, wrong channel) — they point
       at the email / in-app dialog instead. */
    private const string TsqlWebhookHint = "See email or in-app Alert Details for the copy-paste T-SQL.";
    private static readonly JsonSerializerOptions s_jsonOptions = new() { PropertyNamingPolicy = null };

    /* #1154: per-incident-fingerprint cooldown (was a per-(serverId, metricName)
       ConcurrentDictionary). Keyed per #1140 dedup fingerprint so a distinct incident in the
       window is delivered; falls back to the metric-level key when an alert carries no
       fingerprintable incident. */
    private readonly IncidentCooldown _cooldown;
    private readonly IAlertSettings _settings;
    private readonly AlertBranding _branding;
    private readonly ILogger<WebhookAlertService> _logger;

    private int _consecutiveTeamsFailures;
    private string? _lastTeamsError;
    private int _consecutiveSlackFailures;
    private string? _lastSlackError;

    /// <param name="historyStore">
    /// Optional alert-history store used to seed the per-fingerprint webhook cooldown across an app
    /// restart (#1145, mirroring the email seed #981). When null the cooldown is purely in-memory
    /// (the pre-#1145 behavior, seeding disabled) — the test call sites pass null.
    /// </param>
    public WebhookAlertService(
        IAlertSettings settings,
        AlertBranding branding,
        ILogger<WebhookAlertService> logger,
        IAlertHistoryStore? historyStore = null)
    {
        _settings = settings;
        _branding = branding;
        _logger = logger;
        _cooldown = new IncidentCooldown(
            keyPrefix: "webhook:",
            // Null store -> null seed delegate -> no restart seeding (preserves the pre-#1145 in-memory path).
            seedLastSentUtc: historyStore is null
                ? null
                : (serverId, metricName, dedupKey) =>
                    historyStore.GetLastWebhookSentUtcAsync(serverId, metricName, dedupKey));
    }

    /// <summary>
    /// Sends webhook alerts to all configured channels (Teams and/or Slack).
    /// Respects the email cooldown setting for throttling. Never throws.
    /// </summary>
    public async Task<bool> TrySendWebhookAlertsAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        string serverId = "",
        AlertContext? context = null)
    {
        try
        {
            /* #1154: per-fingerprint cooldown. Post if any incident in this alert is outside its
               window (a distinct fingerprint is not throttled by an unrelated prior incident); stamp
               every candidate key only after a successful post. Seeds the webhook last-sent time from
               the alert log on first touch per key (#1145), unless the store is null (no seeding). No
               incidents -> the metric-level fallback key (today's behavior). */
            var decision = await _cooldown.EvaluateAsync(
                serverId, metricName, context?.Incidents,
                TimeSpan.FromMinutes(_settings.EmailCooldownMinutes));

            if (!decision.ShouldSend)
            {
                return false;
            }

            bool sent = false;

            if (_settings.TeamsWebhookEnabled && !string.IsNullOrWhiteSpace(_settings.TeamsWebhookUrl))
            {
                sent |= await TrySendTeamsAlertAsync(metricName, serverName, currentValue, thresholdValue, context);
            }

            if (_settings.SlackWebhookEnabled && !string.IsNullOrWhiteSpace(_settings.SlackWebhookUrl))
            {
                sent |= await TrySendSlackAlertAsync(metricName, serverName, currentValue, thresholdValue, context);
            }

            if (sent)
            {
                _cooldown.Stamp(decision);
            }

            return sent;
        }
        catch (Exception ex)
        {
            _logger.LogError($"TrySendWebhookAlertsAsync outer error: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Sends a test notification to Microsoft Teams. Returns null on success, error message on failure.
    /// </summary>
    public static async Task<string?> SendTestTeamsAsync(string webhookUrl, string? proxyAddress, AlertBranding branding)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(webhookUrl))
                return "Teams webhook URL is not configured.";

            var payload = BuildTeamsPayload("Test Notification", "", "Webhook configuration verified", "", branding, isTest: true);
            return await PostWebhookAsync(webhookUrl, payload, proxyAddress);
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    /// <summary>
    /// Sends a test notification to Slack. Returns null on success, error message on failure.
    /// </summary>
    public static async Task<string?> SendTestSlackAsync(string webhookUrl, string? proxyAddress, AlertBranding branding)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(webhookUrl))
                return "Slack webhook URL is not configured.";

            var payload = BuildSlackPayload("Test Notification", "", "Webhook configuration verified", "", branding, isTest: true);
            return await PostWebhookAsync(webhookUrl, payload, proxyAddress);
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    public (int ConsecutiveFailures, string? LastError) GetTeamsHealth() =>
        (_consecutiveTeamsFailures, _lastTeamsError);

    public (int ConsecutiveFailures, string? LastError) GetSlackHealth() =>
        (_consecutiveSlackFailures, _lastSlackError);

    #region Teams

    private async Task<bool> TrySendTeamsAlertAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertContext? context)
    {
        try
        {
            var payload = BuildTeamsPayload(metricName, serverName, currentValue, thresholdValue, _branding, context: context);
            var error = await PostWebhookAsync(_settings.TeamsWebhookUrl, payload, _settings.TeamsProxyAddress);

            if (error != null)
            {
                _consecutiveTeamsFailures++;
                _lastTeamsError = error;

                if (_consecutiveTeamsFailures <= 3)
                    _logger.LogError($"TEAMS WEBHOOK FAILED ({_consecutiveTeamsFailures}x): {error}");
                else if (_consecutiveTeamsFailures % 50 == 0)
                    _logger.LogError($"TEAMS WEBHOOK STILL FAILING: {_consecutiveTeamsFailures} failures. Last: {error}");

                return false;
            }

            if (_consecutiveTeamsFailures > 0)
                _logger.LogInformation($"Teams webhook recovered after {_consecutiveTeamsFailures} failure(s)");

            _consecutiveTeamsFailures = 0;
            _lastTeamsError = null;
            _logger.LogInformation($"Teams webhook sent for {metricName} on {serverName}");
            return true;
        }
        catch (Exception ex)
        {
            _consecutiveTeamsFailures++;
            _lastTeamsError = ex.Message;
            _logger.LogError($"Teams webhook error: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Builds an O365 MessageCard payload for Teams incoming webhooks.
    /// The themeColor property renders as a colored accent bar at the top of the card.
    /// </summary>
    internal static string BuildTeamsPayload(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertBranding branding,
        bool isTest = false,
        AlertContext? context = null)
    {
        var (hexColor, badgeText, emoji) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var themeColor = hexColor.TrimStart('#');
        var utcNow = DateTime.UtcNow;
        var localNow = DateTime.Now;

        var facts = new List<object>();

        if (isTest)
        {
            facts.Add(new { name = "Status", value = "Webhook configuration is working correctly" });
            facts.Add(new { name = "Sent at", value = localNow.ToString("yyyy-MM-dd HH:mm:ss") });
        }
        else
        {
            facts.Add(new { name = "Server", value = serverName });
            facts.Add(new { name = "Current Value", value = currentValue });
            facts.Add(new { name = "Threshold", value = thresholdValue });
            facts.Add(new { name = "Time (UTC)", value = utcNow.ToString("yyyy-MM-dd HH:mm:ss") });
            facts.Add(new { name = "Time (Local)", value = localNow.ToString("yyyy-MM-dd HH:mm:ss") });
        }

        if (context?.Details != null)
        {
            foreach (var detail in context.Details)
            {
                if (detail.IsCodeBlock)
                {
                    /* Remediation T-SQL: point at the email / in-app dialog, never inline it. */
                    facts.Add(new { name = detail.Heading, value = TsqlWebhookHint });
                    continue;
                }

                if (!string.IsNullOrEmpty(detail.Body))
                {
                    /* Advice prose: one "Advice" fact for the headline, then a fact per
                       Investigation/Remediation paragraph (split on the blank line).
                       Body is exclusive with Fields, matching the email and Slack surfaces
                       which skip Fields when Body is present. */
                    facts.Add(new { name = "Advice", value = detail.Heading });
                    foreach (var para in detail.Body.Split("\n\n", StringSplitOptions.RemoveEmptyEntries))
                    {
                        var (label, text) = SplitProseLabel(para);
                        facts.Add(new { name = label, value = text });
                    }
                    continue;
                }

                foreach (var (label, value) in detail.Fields)
                {
                    facts.Add(new { name = label, value });
                }
            }
        }

        var title = isTest
            ? $"{emoji} TEST — {metricName}"
            : $"{emoji} {badgeText} — {metricName}";

        var sections = new List<object>
        {
            new
            {
                activityTitle = title,
                activitySubtitle = isTest ? branding.EditionName : $"{branding.EditionName} — {serverName}",
                facts,
                markdown = true
            }
        };

        if (!isTest && branding.SnoozeHint is not null)
        {
            sections.Add(new { text = branding.SnoozeHint });
        }

        var card = new
        {
            @type = "MessageCard",
            @context = "http://schema.org/extensions",
            themeColor,
            summary = isTest
                ? "[SQL Monitor] Test Notification"
                : $"[SQL Monitor] {badgeText}: {metricName} on {serverName}",
            sections
        };

        return JsonSerializer.Serialize(card, s_jsonOptions);
    }

    #endregion

    #region Slack

    private async Task<bool> TrySendSlackAlertAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertContext? context)
    {
        try
        {
            var payload = BuildSlackPayload(metricName, serverName, currentValue, thresholdValue, _branding, context: context);
            var error = await PostWebhookAsync(_settings.SlackWebhookUrl, payload, _settings.SlackProxyAddress);

            if (error != null)
            {
                _consecutiveSlackFailures++;
                _lastSlackError = error;

                if (_consecutiveSlackFailures <= 3)
                    _logger.LogError($"SLACK WEBHOOK FAILED ({_consecutiveSlackFailures}x): {error}");
                else if (_consecutiveSlackFailures % 50 == 0)
                    _logger.LogError($"SLACK WEBHOOK STILL FAILING: {_consecutiveSlackFailures} failures. Last: {error}");

                return false;
            }

            if (_consecutiveSlackFailures > 0)
                _logger.LogInformation($"Slack webhook recovered after {_consecutiveSlackFailures} failure(s)");

            _consecutiveSlackFailures = 0;
            _lastSlackError = null;
            _logger.LogInformation($"Slack webhook sent for {metricName} on {serverName}");
            return true;
        }
        catch (Exception ex)
        {
            _consecutiveSlackFailures++;
            _lastSlackError = ex.Message;
            _logger.LogError($"Slack webhook error: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Builds a Slack incoming webhook payload with a colored attachment sidebar.
    /// Uses Slack Block Kit for rich formatting.
    /// </summary>
    internal static string BuildSlackPayload(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertBranding branding,
        bool isTest = false,
        AlertContext? context = null)
    {
        var (hexColor, badgeText, emoji) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var utcNow = DateTime.UtcNow;
        var localNow = DateTime.Now;

        var title = isTest
            ? $"{emoji} TEST — {metricName}"
            : $"{emoji} {badgeText} — {metricName}";

        var blocks = new List<object>
        {
            new
            {
                type = "header",
                text = new { type = "plain_text", text = title, emoji = true }
            }
        };

        var fields = new List<object>();

        if (isTest)
        {
            fields.Add(new { type = "mrkdwn", text = "*Status:*\nWebhook configuration is working correctly" });
            fields.Add(new { type = "mrkdwn", text = $"*Sent at:*\n{localNow:yyyy-MM-dd HH:mm:ss}" });
        }
        else
        {
            fields.Add(new { type = "mrkdwn", text = $"*Server:*\n{serverName}" });
            fields.Add(new { type = "mrkdwn", text = $"*Current Value:*\n{currentValue}" });
            fields.Add(new { type = "mrkdwn", text = $"*Threshold:*\n{thresholdValue}" });
            fields.Add(new { type = "mrkdwn", text = $"*Time (UTC):*\n{utcNow:yyyy-MM-dd HH:mm:ss}" });
            fields.Add(new { type = "mrkdwn", text = $"*Time (Local):*\n{localNow:yyyy-MM-dd HH:mm:ss}" });
        }

        blocks.Add(new { type = "section", fields });

        if (context?.Details != null)
        {
            foreach (var detail in context.Details)
            {
                blocks.Add(new { type = "divider" });

                if (detail.IsCodeBlock)
                {
                    /* Remediation T-SQL: point at the email / in-app dialog, never inline it. */
                    blocks.Add(new { type = "section", text = new { type = "mrkdwn", text = $"*{detail.Heading}*\n{TsqlWebhookHint}" } });
                    continue;
                }

                if (!string.IsNullOrEmpty(detail.Body))
                {
                    /* Advice prose flows as a single mrkdwn section; the synthesized Body is
                       "Investigation: ...\n\nRemediation: ..." which Slack renders verbatim. */
                    blocks.Add(new { type = "section", text = new { type = "mrkdwn", text = $"*{detail.Heading}*\n{detail.Body}" } });
                    continue;
                }

                var detailFields = new List<object>();
                detailFields.Add(new { type = "mrkdwn", text = $"*{detail.Heading}*" });

                foreach (var (label, value) in detail.Fields)
                {
                    detailFields.Add(new { type = "mrkdwn", text = $"*{label}:*\n{value}" });
                }

                blocks.Add(new { type = "section", fields = detailFields });
            }
        }

        var contextElements = new List<object>
        {
            new { type = "mrkdwn", text = $"Sent by {branding.EditionName}" }
        };
        if (!isTest && branding.SnoozeHint is not null)
        {
            contextElements.Add(new { type = "mrkdwn", text = branding.SnoozeHint });
        }

        blocks.Add(new
        {
            type = "context",
            elements = contextElements
        });

        var payload = new
        {
            attachments = new object[]
            {
                new { color = hexColor, blocks }
            }
        };

        return JsonSerializer.Serialize(payload, s_jsonOptions);
    }

    #endregion

    #region Shared

    /// <summary>
    /// Splits a synthesized advice paragraph ("Investigation: ..." / "Remediation: ...") into a
    /// (label, value) pair on the first ": ". Falls back to ("Detail", paragraph) when there is no
    /// leading label. Depends on advice prose containing no interior blank-line break so each
    /// Body chunk is a single labelled paragraph (audited safe for the current FactAdvice blocks);
    /// a future block with a paragraph break degrades to the "Detail" fallback rather than breaking.
    /// </summary>
    private static (string Label, string Value) SplitProseLabel(string paragraph)
    {
        var idx = paragraph.IndexOf(": ", StringComparison.Ordinal);
        if (idx > 0)
        {
            return (paragraph.Substring(0, idx), paragraph.Substring(idx + 2));
        }
        return ("Detail", paragraph);
    }

    /* Reuse HttpClients instead of newing one (plus a handler) per send. A fresh
       HttpClient/handler per request leaks sockets into TIME_WAIT and can exhaust
       the ephemeral port range when many servers alert. One pooled client covers the
       no-proxy case; proxied clients are cached per proxy address. */
    private static readonly HttpClient s_defaultClient =
        new(new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(5) })
        { Timeout = TimeSpan.FromSeconds(30) };

    private static readonly ConcurrentDictionary<string, HttpClient> s_proxyClients = new();

    private static HttpClient GetHttpClient(string? proxyAddress)
    {
        if (string.IsNullOrWhiteSpace(proxyAddress))
            return s_defaultClient;

        return s_proxyClients.GetOrAdd(proxyAddress, addr =>
            new HttpClient(new SocketsHttpHandler
            {
                Proxy = new WebProxy(addr),
                UseProxy = true,
                PooledConnectionLifetime = TimeSpan.FromMinutes(5)
            })
            { Timeout = TimeSpan.FromSeconds(30) });
    }

    /// <summary>
    /// Posts a JSON payload to a webhook URL. Returns null on success, error message on failure.
    /// </summary>
    private static async Task<string?> PostWebhookAsync(string webhookUrl, string jsonPayload, string? proxyAddress)
    {
        var client = GetHttpClient(proxyAddress);
        using var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");

        var response = await client.PostAsync(webhookUrl, content);

        if (response.IsSuccessStatusCode)
            return null;

        var body = await response.Content.ReadAsStringAsync();
        return $"HTTP {(int)response.StatusCode}: {body}";
    }

    #endregion
}
