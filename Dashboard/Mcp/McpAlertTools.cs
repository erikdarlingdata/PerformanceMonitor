/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpAlertTools
{
    [McpServerTool(Name = "get_alert_settings"), Description("Gets the current alert notification and SMTP email configuration settings.")]
    public static Task<string> GetAlertSettings(
        UserPreferencesService prefsService)
    {
        try
        {
            var prefs = prefsService.GetPreferences();

            var emailHealth = EmailAlertService.Current?.GetEmailHealth();

            var settings = new
            {
                notifications_enabled = prefs.NotificationsEnabled,
                notify_connection_lost = prefs.NotifyOnConnectionLost,
                notify_connection_restored = prefs.NotifyOnConnectionRestored,
                cpu = new
                {
                    enabled = prefs.NotifyOnHighCpu,
                    threshold_percent = prefs.CpuThresholdPercent
                },
                blocking = new
                {
                    enabled = prefs.NotifyOnBlocking,
                    threshold_seconds = prefs.BlockingThresholdSeconds
                },
                deadlocks = new
                {
                    enabled = prefs.NotifyOnDeadlock,
                    threshold = prefs.DeadlockThreshold
                },
                smtp = new
                {
                    enabled = prefs.SmtpEnabled,
                    server = prefs.SmtpServer,
                    port = prefs.SmtpPort,
                    use_ssl = prefs.SmtpUseSsl,
                    username = prefs.SmtpUsername,
                    from_address = prefs.SmtpFromAddress,
                    recipients = prefs.SmtpRecipients,
                    password_configured = !string.IsNullOrEmpty(EmailAlertService.GetSmtpPassword()),
                    consecutive_failures = emailHealth?.ConsecutiveFailures ?? 0,
                    last_error = emailHealth?.LastError
                }
            };

            return Task.FromResult(JsonSerializer.Serialize(settings, McpHelpers.JsonOptions));
        }
        catch (Exception ex)
        {
            return Task.FromResult(McpHelpers.FormatError("get_alert_settings", ex));
        }
    }

    [McpServerTool(Name = "get_alert_history"), Description("Gets recent alert history showing what alerts fired, when, notification type, and whether email was sent successfully.")]
    public static Task<string> GetAlertHistory(
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 50.")] int limit = 50)
    {
        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return Task.FromResult(hoursError);

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return Task.FromResult(limitError);

            var service = EmailAlertService.Current;
            if (service == null)
            {
                return Task.FromResult("Alert service not initialized. Connect to a server first.");
            }

            var alerts = service.GetAlertHistory(hours_back, limit);

            if (alerts.Count == 0)
            {
                return Task.FromResult("No alerts found in the specified time range.");
            }

            var result = new
            {
                hours_back,
                total_alerts = alerts.Count,
                note = "Alert history is in-memory and resets when the application restarts.",
                alerts = alerts.Select(a => new
                {
                    alert_time = a.AlertTime.ToString("o"),
                    server_id = a.ServerId,
                    server_name = a.ServerName,
                    metric_name = a.MetricName,
                    current_value = a.CurrentValue,
                    threshold_value = a.ThresholdValue,
                    alert_sent = a.AlertSent,
                    notification_type = a.NotificationType,
                    send_error = a.SendError,
                    muted = a.Muted,
                    detail_text = a.DetailText
                })
            };

            return Task.FromResult(JsonSerializer.Serialize(result, McpHelpers.JsonOptions));
        }
        catch (Exception ex)
        {
            return Task.FromResult(McpHelpers.FormatError("get_alert_history", ex));
        }
    }

    [McpServerTool(Name = "get_mute_rules"), Description("Gets the configured alert mute rules. Mute rules suppress specific recurring alerts while still logging them.")]
    public static Task<string> GetMuteRules(
        MuteRuleService muteRuleService,
        [Description("Include only enabled rules. Default true.")] bool enabled_only = true)
    {
        try
        {
            var rules = muteRuleService.GetRules();
            if (enabled_only)
                rules = rules.Where(r => r.Enabled && (r.ExpiresAtUtc == null || r.ExpiresAtUtc > DateTime.UtcNow)).ToList();

            var result = new
            {
                mute_rules = rules.Select(r => new
                {
                    id = r.Id,
                    enabled = r.Enabled,
                    created_at_utc = r.CreatedAtUtc.ToString("o"),
                    expires_at_utc = r.ExpiresAtUtc?.ToString("o"),
                    reason = r.Reason,
                    server_name = r.ServerName,
                    metric_name = r.MetricName,
                    database_pattern = r.DatabasePattern,
                    query_text_pattern = r.QueryTextPattern,
                    wait_type_pattern = r.WaitTypePattern,
                    job_name_pattern = r.JobNamePattern,
                    summary = r.Summary
                }).ToArray(),
                total_count = rules.Count
            };

            return Task.FromResult(JsonSerializer.Serialize(result, McpHelpers.JsonOptions));
        }
        catch (Exception ex)
        {
            return Task.FromResult(McpHelpers.FormatError("get_mute_rules", ex));
        }
    }
}
