/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Mail;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// SMTP email sending service with per-metric cooldown.
/// Uses System.Net.Mail.SmtpClient (no new NuGet packages needed).
/// </summary>
public class EmailAlertService
{
    private readonly ConcurrentDictionary<string, DateTime> _cooldowns = new();
    private static readonly TimeSpan CooldownPeriod = TimeSpan.FromMinutes(15);
    private readonly DuckDbInitializer? _duckDb;

    /* Failure tracking for louder logging */
    private int _consecutiveSmtpFailures;
    private string? _lastSmtpError;
    private int _consecutiveLogFailures;

    public EmailAlertService(DuckDbInitializer? duckDb = null)
    {
        _duckDb = duckDb;
    }

    /// <summary>
    /// Attempts to send an alert email if SMTP is enabled and cooldown has elapsed.
    /// Never throws — logs errors via AppLogger.
    /// </summary>
    public async Task TrySendAlertEmailAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        int serverId = 0,
        AlertContext? context = null)
    {
        try
        {
            string? sendError = null;
            bool sent = false;
            var notificationType = "tray";

            /* Attempt email delivery if SMTP is fully configured */
            if (App.SmtpEnabled &&
                !string.IsNullOrWhiteSpace(App.SmtpServer) &&
                !string.IsNullOrWhiteSpace(App.SmtpFromAddress) &&
                !string.IsNullOrWhiteSpace(App.SmtpRecipients))
            {
                var cooldownKey = $"{serverId}:{metricName}";
                var withinCooldown = _cooldowns.TryGetValue(cooldownKey, out var lastSent) &&
                    DateTime.UtcNow - lastSent < CooldownPeriod;

                if (!withinCooldown)
                {
                    notificationType = "email";

                    var subject = $"[SQL Monitor Alert] {metricName} on {serverName}";
                    var (htmlBody, plainTextBody) = EmailTemplateBuilder.BuildAlertEmail(
                        metricName, serverName, currentValue, thresholdValue, context);

                    try
                    {
                        await SendEmailAsync(subject, htmlBody, plainTextBody, context);
                        sent = true;
                        _cooldowns[cooldownKey] = DateTime.UtcNow;

                        if (_consecutiveSmtpFailures > 0)
                        {
                            AppLogger.Info("EmailAlert", $"Email delivery recovered after {_consecutiveSmtpFailures} failure(s)");
                        }
                        _consecutiveSmtpFailures = 0;
                        _lastSmtpError = null;

                        AppLogger.Info("EmailAlert", $"Alert email sent for {metricName} on {serverName}");
                    }
                    catch (Exception ex)
                    {
                        sendError = ex.Message;
                        _consecutiveSmtpFailures++;
                        _lastSmtpError = ex.Message;

                        if (_consecutiveSmtpFailures <= 3)
                        {
                            AppLogger.Error("EmailAlert", $"ALERT EMAIL FAILED ({_consecutiveSmtpFailures}x): {ex.GetType().Name}: {ex.Message}");
                        }
                        else if (_consecutiveSmtpFailures % 50 == 0)
                        {
                            AppLogger.Error("EmailAlert", $"ALERT EMAIL STILL FAILING: {_consecutiveSmtpFailures} consecutive failures. Last error: {ex.Message}");
                        }
                    }
                }
            }

            /* Always log the alert to DuckDB, regardless of email status */
            await LogAlertAsync(serverId, serverName, metricName,
                double.TryParse(currentValue.TrimEnd('%'), out var cv) ? cv : 0,
                double.TryParse(thresholdValue.TrimEnd('%'), out var tv) ? tv : 0,
                sent, notificationType, sendError);
        }
        catch (Exception ex)
        {
            AppLogger.Error("EmailAlert", $"TrySendAlertEmailAsync outer error: {ex.Message}");
        }
    }

    /// <summary>
    /// Sends a test email to verify SMTP configuration.
    /// Returns null on success, or the error message on failure.
    /// </summary>
    public static async Task<string?> SendTestEmailAsync()
    {
        try
        {
            if (string.IsNullOrWhiteSpace(App.SmtpServer))
                return "SMTP server is not configured.";

            if (string.IsNullOrWhiteSpace(App.SmtpFromAddress))
                return "From address is not configured.";

            if (string.IsNullOrWhiteSpace(App.SmtpRecipients))
                return "No recipients configured.";

            var (htmlBody, plainTextBody) = EmailTemplateBuilder.BuildTestEmail();
            await SendEmailAsync("[SQL Monitor] Test Email", htmlBody, plainTextBody);
            return null;
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    /// <summary>
    /// Shared SMTP send helper with multipart/alternative (HTML + plain text).
    /// </summary>
    private static async Task SendEmailAsync(string subject, string htmlBody, string plainTextBody, AlertContext? context = null)
    {
        using var smtpClient = new SmtpClient(App.SmtpServer, App.SmtpPort)
        {
            EnableSsl = App.SmtpUseSsl,
            DeliveryMethod = SmtpDeliveryMethod.Network,
            Timeout = 30000
        };

        if (!string.IsNullOrWhiteSpace(App.SmtpUsername))
        {
            var password = App.GetSmtpPassword();
            smtpClient.Credentials = new NetworkCredential(App.SmtpUsername, password ?? "");
        }

        using var message = new MailMessage
        {
            From = new MailAddress(App.SmtpFromAddress),
            Subject = subject
        };

        /* Multipart/alternative: plain text + HTML */
        var plainView = AlternateView.CreateAlternateViewFromString(plainTextBody, null, MediaTypeNames.Text.Plain);
        var htmlView = AlternateView.CreateAlternateViewFromString(htmlBody, null, MediaTypeNames.Text.Html);
        message.AlternateViews.Add(plainView);
        message.AlternateViews.Add(htmlView);

        /* XML attachment (deadlock graph, blocked process report) */
        if (!string.IsNullOrEmpty(context?.AttachmentXml) && !string.IsNullOrEmpty(context?.AttachmentFileName))
        {
            var xmlBytes = Encoding.UTF8.GetBytes(context.AttachmentXml);
            var stream = new MemoryStream(xmlBytes);
            message.Attachments.Add(new Attachment(stream, context.AttachmentFileName, "application/xml"));
        }

        foreach (var recipient in App.SmtpRecipients.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            message.To.Add(recipient);
        }

        await smtpClient.SendMailAsync(message);
    }

    /// <summary>
    /// Logs an alert to the config_alert_log table in DuckDB.
    /// Reuses the injected DuckDbInitializer instead of creating a new one each time.
    /// </summary>
    private async Task LogAlertAsync(int serverId, string serverName, string metricName,
        double currentValue, double thresholdValue, bool alertSent, string notificationType, string? sendError)
    {
        try
        {
            /* Use injected initializer, fall back to creating one from App.DatabasePath */
            var duckDb = _duckDb;
            if (duckDb == null)
            {
                var dbPath = App.DatabasePath;
                if (string.IsNullOrEmpty(dbPath)) return;
                duckDb = new DuckDbInitializer(dbPath);
            }

            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = @"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";

            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = DateTime.UtcNow });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverName });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = metricName });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = currentValue });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = thresholdValue });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = alertSent });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = notificationType });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = sendError ?? (object)DBNull.Value });

            await command.ExecuteNonQueryAsync();

            /* Reset log failure counter on success */
            if (_consecutiveLogFailures > 0)
            {
                AppLogger.Info("EmailAlert", $"Alert logging recovered after {_consecutiveLogFailures} failure(s)");
            }
            _consecutiveLogFailures = 0;
        }
        catch (Exception ex)
        {
            _consecutiveLogFailures++;
            if (_consecutiveLogFailures <= 3)
            {
                AppLogger.Error("EmailAlert", $"Failed to log alert ({_consecutiveLogFailures}x): {ex.Message}");
            }
            else if (_consecutiveLogFailures % 50 == 0)
            {
                AppLogger.Error("EmailAlert", $"Alert logging STILL broken: {_consecutiveLogFailures} failures. Last: {ex.Message}");
            }
        }
    }
}
