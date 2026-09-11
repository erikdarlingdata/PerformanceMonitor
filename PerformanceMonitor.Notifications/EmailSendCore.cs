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
    /// <param name="detailText">
    /// #3297: the alert's flat prose detail — the same text the caller records as the history row's
    /// <c>detail_text</c> and the viewer, the MCP reader and the triage page all show. Passed on to the
    /// email template and the webhook fan-out so the channels carry the alert's remedy and not only its
    /// number. Optional so the deprecated Dashboard shell, which has no detail text to give, keeps
    /// compiling; every live caller has one.
    /// </param>
    public async Task<EmailFanoutResult> TrySendAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        string serverId,
        AlertContext? context,
        bool attemptChannels,
        string? detailText = null)
    {
        bool emailAttempted = false;
        bool emailSent = false;
        string? sendError = null;

        /* The SMTP gate, hoisted so the same expression both decides whether email is attempted and
           answers "is any channel configured at all". A restatement of it somewhere else would be free to
           drift; a reader of the result needs the answer from the code that consults the settings. */
        var smtpConfigured =
            _settings.SmtpEnabled &&
            !string.IsNullOrWhiteSpace(_settings.SmtpServer) &&
            !string.IsNullOrWhiteSpace(_settings.SmtpFromAddress) &&
            !string.IsNullOrWhiteSpace(_settings.SmtpRecipients);

        var anyChannelConfigured = smtpConfigured || _webhookAlertService.AnyWebhookConfigured;

        /* Attempt email delivery if SMTP is fully configured */
        if (attemptChannels && smtpConfigured)
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
                    metricName, serverName, currentValue, thresholdValue, _settings.EmailCooldownMinutes, _branding, context,
                    detailText);

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
                metricName, serverName, currentValue, thresholdValue, serverId, context, detailText);
        }

        return new EmailFanoutResult(emailAttempted, emailSent, sendError, webhookSent, anyChannelConfigured);
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
/// it actually sent, any send error, whether a webhook was delivered, and whether any channel
/// was configured to attempt in the first place.
/// </summary>
/// <param name="AnyChannelConfigured">
/// Whether SMTP or at least one webhook is configured on this deployment. Reported by the send core
/// rather than derived by the caller because it comes from the very gates that decide what gets
/// attempted, and it is the only thing that separates "nothing is set up" from "something is set up and
/// this alert did not go out" — two states that otherwise both arrive as an all-false result with a null
/// error. Note it is answered from configuration, so <c>attemptChannels: false</c> (a muted alert) still
/// reports it truthfully.
/// </param>
public readonly record struct EmailFanoutResult(
    bool EmailAttempted,
    bool EmailSent,
    string? SendError,
    bool WebhookSent,
    bool AnyChannelConfigured);
