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

    /* #3430: the per-metric ceiling on REPEAT sends, the email twin of the webhook fan-out's. Its own
       instance, so an email that was folded is not governed by the webhook's send history — the same reason
       the two channels hold separate IncidentCooldown key spaces. */
    private readonly RepeatDeliveryBudget _repeatBudget = new();

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
    /// <param name="displayName">
    /// Human-facing name to render in the subject/body/webhook titles in place of
    /// <paramref name="metricName"/> (a custom alert rule's name). Null/empty keeps
    /// <paramref name="metricName"/> — built-in alerts render exactly as before. The cooldown key,
    /// severity, and dedup all stay on <paramref name="metricName"/>.
    /// </param>
    /// <param name="deliveryMode">
    /// #3430: the effective <see cref="AlertNotificationMode"/> for this alert's server, resolved by the
    /// caller (both SKUs' deliverers already do, for the #1141 split) and passed straight through to the
    /// webhook fan-out as well, so one alert's two channels agree about whether it may be aggregated.
    /// <see cref="AlertNotificationMode.Summary"/> permits the per-metric repeat ceiling;
    /// <see cref="AlertNotificationMode.PerEvent"/> and <c>null</c> do not — see
    /// <see cref="RepeatDeliveryBudget.Evaluate"/> for why an unstated mode declines rather than defaults.
    /// </param>
    public async Task<EmailFanoutResult> TrySendAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        string serverId,
        AlertContext? context,
        bool attemptChannels,
        string? detailText = null,
        string? displayName = null,
        AlertNotificationMode? deliveryMode = null)
    {
        var emailOutcome = AlertChannelOutcome.NotAttempted;
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
               first touch per key (#981). No incidents -> the metric-level fallback key (today's behavior).
               WHETHER to send only; #3313's filter below decides WHICH incidents the email contains. */
            var window = TimeSpan.FromMinutes(_settings.EmailCooldownMinutes);
            var decision = await _cooldown.EvaluateAsync(
                serverId, metricName, context?.Incidents, window);

            /* #3430: the per-metric repeat ceiling, on the email channel's own budget and the cooldown's own
               instant. Consulted only once the cooldown has already said this alert sends — an alert inside
               its own fingerprint's window was never a candidate, so folding it would put an entry on the
               roster for something that is not owed a delivery at all. A first notice is exempt; a repeat
               that finds the metric's window already spent is folded and named by the next email that does
               go out. Throttled and folded are separate AlertChannelOutcome values (#3427): both attempt
               nothing, and collapsing them onto one "nothing was attempted" left the alert log unable to say
               whether a non-delivery was a spent window or a roster entry on another server's send. */
            var budget = decision.ShouldSend
                ? _repeatBudget.Evaluate(
                    metricName, serverName, decision, window,
                    aggregateRepeats: deliveryMode == AlertNotificationMode.Summary,
                    incidents: context?.Incidents)
                : null;

            if (budget is not null && budget.ShouldSend)
            {

                /* #3313: render only the incidents outside their own window. Email keys off THIS path's own
                   cooldown, not the webhook's: the two channels hold separate key spaces (see the keyPrefix
                   on each IncidentCooldown), so an email that failed to send last cycle left its key
                   unstamped and its incident is still owed a delivery even where the webhook's is not. */
                var render = IncidentDeliveryFilter.ForDelivery(
                    context, detailText, decision.DeliverableDedupKeys, budget.Roster);

                var titleName = string.IsNullOrEmpty(displayName) ? metricName : displayName;
                var subject = $"[SQL Monitor Alert] {titleName} on {serverName}";
                var (htmlBody, plainTextBody) = EmailTemplateBuilder.BuildAlertEmail(
                    metricName, serverName, currentValue, thresholdValue, _settings.EmailCooldownMinutes, _branding,
                    render.Context, render.Prose, displayName);

                try
                {
                    await SendEmailAsync(_settings, subject, htmlBody, plainTextBody, render.Context);
                    emailOutcome = AlertChannelOutcome.Delivered;
                    _cooldown.Stamp(decision);

                    /* #3430: clear only the roster entries this email named, so anything folded while the
                       SMTP send was in flight is named by the next one. */
                    _repeatBudget.Commit(budget);
                    if (budget.RosterEntryCount > 0)
                    {
                        _logger.LogInformation(
                            $"Alert email for {metricName} on {serverName} carried {budget.RosterEntryCount} already-reported incident(s) from other servers");
                    }

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
                    /* The send threw, so this email named nothing and must not have spent the metric's
                       window — the same rule the cooldown applies by stamping only on success. */
                    _repeatBudget.Release(budget);

                    emailOutcome = AlertChannelOutcome.Failed;
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
            else if (budget is not null)
            {
                emailOutcome = AlertChannelOutcome.Folded;

                /* Debug, not Information: a fold happens on every sweep of every co-affected server, which
                   is the volume this issue is about. The aggregate worth a log line is the carrier's roster
                   size above, which happens once per window. The alert-log row says `folded` either way, so
                   a store's fold count no longer depends on the service log's level (#3427). */
                _logger.LogDebug(
                    $"Alert email for {metricName} on {serverName} folded into the metric's roster ({budget.RosterEntryCount} entr(ies) pending)");
            }
            else
            {
                /* decision.ShouldSend was false, so the budget was never consulted: a cooldown window is
                   still open on every candidate key. */
                emailOutcome = AlertChannelOutcome.Throttled;
            }
        }

        /* Webhook notifications (Teams / Slack) — independent of email, and handed the UNFILTERED context:
           it owns its own cooldown key space and applies its own #3313 filter from its own decision. Passing
           email's filtered copy would make one channel's send history govern the other's card. The delivery
           mode goes through unchanged (#3430): one alert's two channels must agree about whether it may be
           aggregated, and this is the only place that holds the answer for both. */
        var webhook = WebhookFanoutResult.NotAttempted;
        if (attemptChannels)
        {
            webhook = await _webhookAlertService.TrySendWebhookAlertsAsync(
                metricName, serverName, currentValue, thresholdValue, serverId, context, detailText, displayName,
                deliveryMode);
        }

        return new EmailFanoutResult(
            emailOutcome, sendError, webhook.Outcome, webhook.SendError, anyChannelConfigured);
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
/// What <see cref="EmailSendCore.TrySendAsync"/> did, so the per-app shell can record its alert-history
/// rows: one <see cref="AlertChannelOutcome"/> per channel, each channel's error, and whether any channel
/// was configured to attempt in the first place.
///
/// <para><b>One outcome per channel, not three bools.</b> Attempted / sent / errored cannot express WHY a
/// channel attempted nothing, so a working cooldown, a #3430 fold and a failed webhook post all arrived
/// here identically and <see cref="AlertDelivery.FromFanout"/> collapsed them onto one
/// <c>notification_type</c>. The three bools this replaces are still exposed below, derived, so the domain
/// is exactly the outcome cross-product and the two representations cannot disagree.</para>
/// </summary>
/// <param name="EmailOutcome">What happened on the email channel.</param>
/// <param name="SendError">The SMTP exception's message when <paramref name="EmailOutcome"/> is
/// <see cref="AlertChannelOutcome.Failed"/>; null otherwise.</param>
/// <param name="WebhookOutcome">What the webhook fan-out did, over all four of its channels.</param>
/// <param name="WebhookSendError">The first failing webhook channel's error, named with its channel —
/// see <see cref="WebhookFanoutResult"/>.</param>
/// <param name="AnyChannelConfigured">
/// Whether SMTP or at least one webhook is configured on this deployment. Reported by the send core
/// rather than derived by the caller because it comes from the very gates that decide what gets
/// attempted, and it is the only thing that separates "nothing is set up" from "something is set up and
/// this alert did not go out". Note it is answered from configuration, so <c>attemptChannels: false</c>
/// (a muted alert) still reports it truthfully — which is why it is not derived from the two outcomes,
/// both of which read <see cref="AlertChannelOutcome.NotAttempted"/> for a muted alert on a fully
/// configured store.
/// </param>
public readonly record struct EmailFanoutResult(
    AlertChannelOutcome EmailOutcome,
    string? SendError,
    AlertChannelOutcome WebhookOutcome,
    string? WebhookSendError,
    bool AnyChannelConfigured)
{
    /// <summary>Whether an SMTP send was attempted — configured, outside its cooldown, and not folded.</summary>
    public bool EmailAttempted =>
        EmailOutcome is AlertChannelOutcome.Delivered or AlertChannelOutcome.Failed;

    /// <summary>Whether the SMTP send succeeded.</summary>
    public bool EmailSent => EmailOutcome == AlertChannelOutcome.Delivered;

    /// <summary>Whether at least one webhook channel delivered.</summary>
    public bool WebhookSent => WebhookOutcome == AlertChannelOutcome.Delivered;
}
