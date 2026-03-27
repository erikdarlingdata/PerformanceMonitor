/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Sends alert notifications to Microsoft Teams and/or Slack via incoming webhooks.
/// Color-coded accent bars match the existing email alert severity mapping.
/// </summary>
public class WebhookAlertService
{
    private const string EditionName = "Performance Monitor Lite";
    private static readonly JsonSerializerOptions s_jsonOptions = new() { PropertyNamingPolicy = null };

    private readonly ConcurrentDictionary<string, DateTime> _cooldowns = new();

    private int _consecutiveTeamsFailures;
    private string? _lastTeamsError;
    private int _consecutiveSlackFailures;
    private string? _lastSlackError;

    /// <summary>
    /// Sends webhook alerts to all configured channels (Teams and/or Slack).
    /// Respects the email cooldown setting for throttling. Never throws.
    /// </summary>
    public async Task TrySendWebhookAlertsAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        int serverId = 0,
        AlertContext? context = null)
    {
        try
        {
            var cooldownKey = $"webhook:{serverId}:{metricName}";
            if (_cooldowns.TryGetValue(cooldownKey, out var lastSent) &&
                DateTime.UtcNow - lastSent < TimeSpan.FromMinutes(App.EmailCooldownMinutes))
            {
                return;
            }

            bool sent = false;

            if (App.TeamsWebhookEnabled && !string.IsNullOrWhiteSpace(App.TeamsWebhookUrl))
            {
                sent |= await TrySendTeamsAlertAsync(metricName, serverName, currentValue, thresholdValue, context);
            }

            if (App.SlackWebhookEnabled && !string.IsNullOrWhiteSpace(App.SlackWebhookUrl))
            {
                sent |= await TrySendSlackAlertAsync(metricName, serverName, currentValue, thresholdValue, context);
            }

            if (sent)
            {
                _cooldowns[cooldownKey] = DateTime.UtcNow;
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("Webhook", $"TrySendWebhookAlertsAsync outer error: {ex.Message}");
        }
    }

    /// <summary>
    /// Sends a test notification to Microsoft Teams. Returns null on success, error message on failure.
    /// </summary>
    public static async Task<string?> SendTestTeamsAsync(string webhookUrl, string? proxyAddress)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(webhookUrl))
                return "Teams webhook URL is not configured.";

            var payload = BuildTeamsPayload("Test Notification", "", "Webhook configuration verified", "", isTest: true);
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
    public static async Task<string?> SendTestSlackAsync(string webhookUrl, string? proxyAddress)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(webhookUrl))
                return "Slack webhook URL is not configured.";

            var payload = BuildSlackPayload("Test Notification", "", "Webhook configuration verified", "", isTest: true);
            return await PostWebhookAsync(webhookUrl, payload, proxyAddress);
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

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
            var payload = BuildTeamsPayload(metricName, serverName, currentValue, thresholdValue, context: context);
            var error = await PostWebhookAsync(App.TeamsWebhookUrl, payload, App.TeamsProxyAddress);

            if (error != null)
            {
                _consecutiveTeamsFailures++;
                _lastTeamsError = error;

                if (_consecutiveTeamsFailures <= 3)
                    AppLogger.Error("Webhook", $"TEAMS WEBHOOK FAILED ({_consecutiveTeamsFailures}x): {error}");
                else if (_consecutiveTeamsFailures % 50 == 0)
                    AppLogger.Error("Webhook", $"TEAMS WEBHOOK STILL FAILING: {_consecutiveTeamsFailures} failures. Last: {error}");

                return false;
            }

            if (_consecutiveTeamsFailures > 0)
                AppLogger.Info("Webhook", $"Teams webhook recovered after {_consecutiveTeamsFailures} failure(s)");

            _consecutiveTeamsFailures = 0;
            _lastTeamsError = null;
            AppLogger.Info("Webhook", $"Teams webhook sent for {metricName} on {serverName}");
            return true;
        }
        catch (Exception ex)
        {
            _consecutiveTeamsFailures++;
            _lastTeamsError = ex.Message;
            AppLogger.Error("Webhook", $"Teams webhook error: {ex.Message}");
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
        bool isTest = false,
        AlertContext? context = null)
    {
        var (hexColor, badgeText, emoji) = GetSeverity(metricName);
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
                foreach (var (label, value) in detail.Fields)
                {
                    facts.Add(new { name = label, value });
                }
            }
        }

        var title = isTest
            ? $"{emoji} TEST \u2014 {metricName}"
            : $"{emoji} {badgeText} \u2014 {metricName}";

        var sections = new List<object>
        {
            new
            {
                activityTitle = title,
                activitySubtitle = isTest ? EditionName : $"{EditionName} \u2014 {serverName}",
                facts,
                markdown = true
            }
        };

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
            var payload = BuildSlackPayload(metricName, serverName, currentValue, thresholdValue, context: context);
            var error = await PostWebhookAsync(App.SlackWebhookUrl, payload, App.SlackProxyAddress);

            if (error != null)
            {
                _consecutiveSlackFailures++;
                _lastSlackError = error;

                if (_consecutiveSlackFailures <= 3)
                    AppLogger.Error("Webhook", $"SLACK WEBHOOK FAILED ({_consecutiveSlackFailures}x): {error}");
                else if (_consecutiveSlackFailures % 50 == 0)
                    AppLogger.Error("Webhook", $"SLACK WEBHOOK STILL FAILING: {_consecutiveSlackFailures} failures. Last: {error}");

                return false;
            }

            if (_consecutiveSlackFailures > 0)
                AppLogger.Info("Webhook", $"Slack webhook recovered after {_consecutiveSlackFailures} failure(s)");

            _consecutiveSlackFailures = 0;
            _lastSlackError = null;
            AppLogger.Info("Webhook", $"Slack webhook sent for {metricName} on {serverName}");
            return true;
        }
        catch (Exception ex)
        {
            _consecutiveSlackFailures++;
            _lastSlackError = ex.Message;
            AppLogger.Error("Webhook", $"Slack webhook error: {ex.Message}");
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
        bool isTest = false,
        AlertContext? context = null)
    {
        var (hexColor, badgeText, emoji) = GetSeverity(metricName);
        var utcNow = DateTime.UtcNow;
        var localNow = DateTime.Now;

        var title = isTest
            ? $"{emoji} TEST \u2014 {metricName}"
            : $"{emoji} {badgeText} \u2014 {metricName}";

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

                var detailFields = new List<object>();
                detailFields.Add(new { type = "mrkdwn", text = $"*{detail.Heading}*" });

                foreach (var (label, value) in detail.Fields)
                {
                    detailFields.Add(new { type = "mrkdwn", text = $"*{label}:*\n{value}" });
                }

                blocks.Add(new { type = "section", fields = detailFields });
            }
        }

        blocks.Add(new
        {
            type = "context",
            elements = new object[]
            {
                new { type = "mrkdwn", text = $"Sent by {EditionName}" }
            }
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

    private static (string HexColor, string BadgeText, string Emoji) GetSeverity(string metricName) => metricName switch
    {
        "Blocking Detected" => ("#D97706", "ALERT", "\U0001F7E0"),
        "Deadlocks Detected" => ("#DC2626", "ALERT", "\U0001F534"),
        "High CPU" => ("#F59E0B", "WARNING", "\U0001F7E1"),
        "Poison Wait" => ("#DC2626", "CRITICAL", "\U0001F534"),
        "Long-Running Query" => ("#D97706", "WARNING", "\U0001F7E0"),
        "TempDB Space" => ("#D97706", "WARNING", "\U0001F7E0"),
        "Long-Running Job" => ("#D97706", "WARNING", "\U0001F7E0"),
        "Server Unreachable" => ("#DC2626", "CRITICAL", "\U0001F534"),
        "Server Restored" => ("#16A34A", "RESOLVED", "\U0001F7E2"),
        _ => ("#2eaef1", "INFO", "\U0001F535")
    };

    /// <summary>
    /// Posts a JSON payload to a webhook URL. Returns null on success, error message on failure.
    /// </summary>
    private static async Task<string?> PostWebhookAsync(string webhookUrl, string jsonPayload, string? proxyAddress)
    {
        var handler = new HttpClientHandler();
        if (!string.IsNullOrWhiteSpace(proxyAddress))
        {
            handler.Proxy = new WebProxy(proxyAddress);
            handler.UseProxy = true;
        }

        using var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(30) };
        using var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");

        var response = await client.PostAsync(webhookUrl, content);

        if (response.IsSuccessStatusCode)
            return null;

        var body = await response.Content.ReadAsStringAsync();
        return $"HTTP {(int)response.StatusCode}: {body}";
    }

    #endregion
}
