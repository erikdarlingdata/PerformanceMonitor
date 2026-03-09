using System.ComponentModel;
using System.Text.Json;
using DuckDB.NET.Data;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpAlertTools
{
    [McpServerTool(Name = "get_alert_history"), Description("Gets recent alert history from the alert log. Shows what alerts fired, when, and whether email was sent successfully.")]
    public static async Task<string> GetAlertHistory(
        LocalDataService dataService,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 50.")] int limit = 50)
    {
        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            using var connection = await dataService.OpenConnectionAsync();

            using var command = connection.CreateCommand();
            command.CommandText = @"
SELECT
    alert_time,
    server_id,
    server_name,
    metric_name,
    current_value,
    threshold_value,
    alert_sent,
    notification_type,
    send_error
FROM config_alert_log
WHERE alert_time >= $1
ORDER BY alert_time DESC
LIMIT $2";

            command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-hours_back) });
            command.Parameters.Add(new DuckDBParameter { Value = limit });

            var alerts = new List<object>();
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                alerts.Add(new
                {
                    alert_time = reader.GetDateTime(0).ToString("o"),
                    server_id = reader.GetInt32(1),
                    server_name = reader.GetString(2),
                    metric_name = reader.GetString(3),
                    current_value = reader.GetDouble(4),
                    threshold_value = reader.GetDouble(5),
                    alert_sent = reader.GetBoolean(6),
                    notification_type = reader.GetString(7),
                    send_error = reader.IsDBNull(8) ? null : reader.GetString(8)
                });
            }

            if (alerts.Count == 0)
            {
                return "No alerts found in the specified time range.";
            }

            return JsonSerializer.Serialize(new
            {
                hours_back,
                total_alerts = alerts.Count,
                alerts
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_alert_history", ex);
        }
    }

    [McpServerTool(Name = "get_alert_settings"), Description("Gets the current alert and SMTP email configuration settings.")]
    public static Task<string> GetAlertSettings()
    {
        try
        {
            var settings = new
            {
                alerts_enabled = App.AlertsEnabled,
                notify_connection_changes = App.NotifyConnectionChanges,
                cpu = new
                {
                    enabled = App.AlertCpuEnabled,
                    threshold = App.AlertCpuThreshold
                },
                blocking = new
                {
                    enabled = App.AlertBlockingEnabled,
                    threshold = App.AlertBlockingThreshold
                },
                deadlocks = new
                {
                    enabled = App.AlertDeadlockEnabled,
                    threshold = App.AlertDeadlockThreshold
                },
                smtp = new
                {
                    enabled = App.SmtpEnabled,
                    server = App.SmtpServer,
                    port = App.SmtpPort,
                    use_ssl = App.SmtpUseSsl,
                    username = App.SmtpUsername,
                    from_address = App.SmtpFromAddress,
                    recipients = App.SmtpRecipients,
                    password_configured = !string.IsNullOrEmpty(App.GetSmtpPassword())
                }
            };

            return Task.FromResult(JsonSerializer.Serialize(settings, McpHelpers.JsonOptions));
        }
        catch (Exception ex)
        {
            return Task.FromResult(McpHelpers.FormatError("get_alert_settings", ex));
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
