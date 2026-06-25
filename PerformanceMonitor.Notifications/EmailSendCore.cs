/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net;
using System.Net.Mail;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The shared send core for alert email + webhook fan-out (Plan E E3c, Approach B).
/// Owns the SMTP send, the per-(serverId, metricName) email cooldown (seed-from-history +
/// in-memory check), the webhook fan-out, and the consecutive-failure counters + health
/// getter. It does <b>not</b> record alert-history rows — that cadence stays in each app's
/// per-app <c>EmailAlertService</c> orchestrator shell, which calls <see cref="TrySendAsync"/>
/// and then writes its own rows (Lite: one combined row; Dashboard: per-channel + tray
/// fallback). See plan §4.6.
/// </summary>
public sealed class EmailSendCore
{
    private readonly IAlertSettings _settings;
    private readonly WebhookAlertService _webhookAlertService;
    private readonly AlertBranding _branding;
    private readonly ILogger _logger;

    /* #1154: per-incident-fingerprint cooldown (was a per-(serverId, metricName)
       ConcurrentDictionary). Keyed per #1140 dedup fingerprint so a distinct incident in
       the window is delivered; falls back to the metric-level key when an alert carries no
       fingerprintable incident. Seeds the email last-sent time from the alert log (#981). */
    private readonly IncidentCooldown _cooldown;

    /* Failure tracking for louder logging + the health getter (MIN-4: counters stay
       co-located with GetEmailHealth on the shared core). */
    private int _consecutiveFailures;
    private string? _lastFailureError;

    public EmailSendCore(
        IAlertSettings settings,
        IAlertHistoryStore historyStore,
        WebhookAlertService webhookAlertService,
        AlertBranding branding,
        ILogger logger)
    {
        _settings = settings;
        _webhookAlertService = webhookAlertService;
        _branding = branding;
        _logger = logger;
        _cooldown = new IncidentCooldown(
            keyPrefix: "",
            seedLastSentUtc: (serverId, metricName, dedupKey) =>
                historyStore.GetLastEmailSentUtcAsync(serverId, metricName, dedupKey));
    }

    /// <summary>
    /// Attempts email delivery (if SMTP is configured and outside the per-metric cooldown)
    /// and the webhook fan-out, and reports what happened so the caller can record its
    /// app-specific alert-history rows. Never throws.
    /// </summary>
    /// <param name="attemptChannels">
    /// When false (Lite's muted case) neither email nor webhook is attempted; the caller
    /// still records its row from the all-false result.
    /// </param>
    public async Task<EmailFanoutResult> TrySendAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        string serverId,
        AlertContext? context,
        bool attemptChannels)
    {
        bool emailAttempted = false;
        bool emailSent = false;
        string? sendError = null;

        /* Attempt email delivery if SMTP is fully configured */
        if (attemptChannels &&
            _settings.SmtpEnabled &&
            !string.IsNullOrWhiteSpace(_settings.SmtpServer) &&
            !string.IsNullOrWhiteSpace(_settings.SmtpFromAddress) &&
            !string.IsNullOrWhiteSpace(_settings.SmtpRecipients))
        {
            /* #1154: per-fingerprint cooldown. Send if any incident in this alert is outside its
               window (a distinct fingerprint is not throttled by an unrelated prior incident);
               stamp every candidate key only after a successful send. Seeds from the alert log on
               first touch per key (#981). No incidents -> the metric-level fallback key (today's behavior). */
            var decision = await _cooldown.EvaluateAsync(
                serverId, metricName, context?.Incidents,
                TimeSpan.FromMinutes(_settings.EmailCooldownMinutes));

            if (decision.ShouldSend)
            {
                emailAttempted = true;

                var subject = $"[SQL Monitor Alert] {metricName} on {serverName}";
                var (htmlBody, plainTextBody) = EmailTemplateBuilder.BuildAlertEmail(
                    metricName, serverName, currentValue, thresholdValue, _settings.EmailCooldownMinutes, _branding, context);

                try
                {
                    await SendEmailAsync(_settings, subject, htmlBody, plainTextBody, context);
                    emailSent = true;
                    _cooldown.Stamp(decision);

                    if (_consecutiveFailures > 0)
                    {
                        _logger.LogInformation($"Alert email delivery recovered after {_consecutiveFailures} failure(s)");
                    }
                    _consecutiveFailures = 0;
                    _lastFailureError = null;

                    _logger.LogInformation($"Alert email sent for {metricName} on {serverName}");
                }
                catch (Exception ex)
                {
                    sendError = ex.Message;
                    _consecutiveFailures++;
                    _lastFailureError = ex.Message;

                    if (_consecutiveFailures <= 3)
                    {
                        _logger.LogError($"ALERT EMAIL FAILED ({_consecutiveFailures}x): {ex.GetType().Name}: {ex.Message}");
                    }
                    else if (_consecutiveFailures % 50 == 0)
                    {
                        _logger.LogError($"ALERT EMAIL STILL FAILING: {_consecutiveFailures} consecutive failures. Last error: {ex.Message}");
                    }
                }
            }
        }

        /* Webhook notifications (Teams / Slack) — independent of email */
        bool webhookSent = false;
        if (attemptChannels)
        {
            webhookSent = await _webhookAlertService.TrySendWebhookAlertsAsync(
                metricName, serverName, currentValue, thresholdValue, serverId, context);
        }

        return new EmailFanoutResult(emailAttempted, emailSent, sendError, webhookSent);
    }

    /// <summary>Gets email delivery health summary (consecutive failures + last error).</summary>
    public (int ConsecutiveFailures, string? LastError) GetEmailHealth() =>
        (_consecutiveFailures, _lastFailureError);

    /// <summary>
    /// Sends a test email to verify SMTP configuration. Returns null on success, or the error
    /// message on failure. Static — the test-send path supplies its own <see cref="IAlertSettings"/>
    /// (Lite: <c>new AppAlertSettings()</c>; Dashboard: the transient form-values adapter, MOD-1).
    /// </summary>
    public static async Task<string?> SendTestEmailAsync(IAlertSettings settings, AlertBranding branding)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(settings.SmtpServer))
                return "SMTP server is not configured.";

            if (string.IsNullOrWhiteSpace(settings.SmtpFromAddress))
                return "From address is not configured.";

            if (string.IsNullOrWhiteSpace(settings.SmtpRecipients))
                return "No recipients configured.";

            var (htmlBody, plainTextBody) = EmailTemplateBuilder.BuildTestEmail(branding);
            await SendEmailAsync(settings, "[SQL Monitor] Test Email", htmlBody, plainTextBody);
            return null;
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    /// <summary>
    /// Shared SMTP send helper with multipart/alternative (HTML + plain text) and an optional
    /// XML attachment (deadlock graph / blocked process report).
    /// </summary>
    private static async Task SendEmailAsync(IAlertSettings settings, string subject, string htmlBody, string plainTextBody, AlertContext? context = null)
    {
        using var smtpClient = new SmtpClient(settings.SmtpServer, settings.SmtpPort)
        {
            EnableSsl = settings.SmtpUseSsl,
            DeliveryMethod = SmtpDeliveryMethod.Network,
            Timeout = 30000
        };

        if (!string.IsNullOrWhiteSpace(settings.SmtpUsername))
        {
            var password = settings.GetSmtpPassword();
            smtpClient.Credentials = new NetworkCredential(settings.SmtpUsername, password ?? "");
        }

        using var message = new MailMessage
        {
            From = new MailAddress(settings.SmtpFromAddress),
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
            var stream = new MemoryStream(xmlBytes); /* Disposed by MailMessage.Dispose() via Attachment chain */
            message.Attachments.Add(new Attachment(stream, context.AttachmentFileName, "application/xml"));
        }

        foreach (var recipient in settings.SmtpRecipients.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            message.To.Add(recipient);
        }

        await smtpClient.SendMailAsync(message);
    }
}

/// <summary>
/// What <see cref="EmailSendCore.TrySendAsync"/> did, so the per-app shell can record its
/// alert-history rows: whether email was attempted (configured + outside cooldown), whether
/// it actually sent, any send error, and whether a webhook was delivered.
/// </summary>
public readonly record struct EmailFanoutResult(
    bool EmailAttempted,
    bool EmailSent,
    string? SendError,
    bool WebhookSent);
