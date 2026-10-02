/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// Dashboard's per-app alert orchestrator shell (Plan E E3c, Approach B). The shared SMTP
    /// send, cooldown, webhook fan-out, and failure counters live in <see cref="EmailSendCore"/>;
    /// this shell owns Dashboard's record cadence: a per-channel <c>email</c> row and/or
    /// <c>webhook</c> row written via <see cref="JsonAlertHistoryStore"/>, plus the analysis-path
    /// no-channel "tray" fallback (in <see cref="SendFindingAlertAsync"/>).
    /// <para>
    /// The Dashboard-only history-management API (GetAlertHistory / Hide* / SaveAlertLog) lives
    /// on <see cref="JsonAlertHistoryStore"/>; its consumers (AlertsHistoryContent, McpAlertTools,
    /// MainWindow) reach it directly via the store's <c>Current</c> (E3c Phase 6).
    /// </para>
    /// </summary>
    public class EmailAlertService : IFindingAlertSender
    {
        private static readonly AlertBranding s_branding = new("Performance Monitor Dashboard", null);

        /// <summary>Test seam: the branding this app feeds the shared email/template renderer.</summary>
        internal static AlertBranding Branding => s_branding;

        private readonly IAlertSettings _settings;
        private readonly JsonAlertHistoryStore _historyStore;
        private readonly EmailSendCore _core;
        private readonly ILogger<EmailAlertService> _logger;

        /// <summary>
        /// The current instance, set when MainWindow creates the service.
        /// Used by MCP tools and the Alerts history UI to reach the service.
        /// </summary>
        public static EmailAlertService? Current { get; private set; }

        public EmailAlertService(IAlertSettings settings, JsonAlertHistoryStore historyStore, WebhookAlertService webhookAlertService, ILogger<EmailAlertService> logger)
        {
            _settings = settings;
            _historyStore = historyStore;
            _logger = logger;
            _core = new EmailSendCore(settings, historyStore, webhookAlertService, s_branding, logger);
            Current = this;
        }

        /// <summary>
        /// Attempts to send alert notifications (email, Teams, Slack) via the shared core and
        /// records Dashboard's per-channel rows: an <c>email</c> row when email is attempted
        /// (configured + outside cooldown) and a <c>webhook</c> row when a webhook is delivered.
        /// Each channel operates independently. Never throws.
        /// <para>#3916: returns the most-delivered disposition among the rows it recorded (Sent if any
        /// channel sent), or null when it recorded no row or caught. The threshold callers discard it.</para>
        /// </summary>
        public async Task<AlertDelivery?> TrySendAlertEmailAsync(
            string metricName,
            string serverName,
            string currentValue,
            string thresholdValue,
            string serverId = "",
            AlertContext? context = null)
        {
            try
            {
                var result = await _core.TrySendAsync(
                    metricName, serverName, currentValue, thresholdValue, serverId, context, attemptChannels: true);

                AlertDelivery? recorded = null;

                if (result.EmailAttempted)
                {
                    var emailContextJson = context is not null ? AlertContextSerializer.Serialize(context) : null;
                    recorded = RecordAlert(serverId, serverName, metricName, currentValue, thresholdValue, result.EmailSent, "email", result.SendError, contextJson: emailContextJson);
                }

                if (result.WebhookSent)
                {
                    var webhookContextJson = context is not null ? AlertContextSerializer.Serialize(context) : null;
                    var webhook = RecordAlert(serverId, serverName, metricName, currentValue, thresholdValue, true, "webhook", contextJson: webhookContextJson);

                    /* Per-channel rows: the most-delivered one speaks for the send (a sent webhook beats a
                       failed email row). */
                    if (recorded is null || !recorded.Sent) recorded = webhook;
                }

                return recorded;
            }
            catch (Exception ex)
            {
                _logger.LogError($"TrySendAlertEmailAsync outer error: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Records an alert (tray notification or email) to the alert-history store.
        /// Thin forwarder over <see cref="JsonAlertHistoryStore.RecordAlertAsync"/>. The store
        /// completes synchronously (in-memory + trim), so this stays a sync method to preserve
        /// the existing call sites' shape (MainWindow threshold alerts call it directly).
        /// <para>#3916: returns the disposition it recorded; the threshold call sites discard it.</para>
        /// </summary>
        public AlertDelivery RecordAlert(string serverId, string serverName, string metricName,
            string currentValue, string thresholdValue, bool alertSent,
            string notificationType, string? sendError = null, bool muted = false, string? detailText = null,
            string? contextJson = null)
        {
            /* This SKU is deprecated and its 26 callers each hand-compute the alertSent/notificationType
               pair, so the disposition arrives already decided rather than derived. FromLegacyStoredColumns
               is internal and exists for exactly this call. */
#pragma warning disable CS0618
            var delivery = AlertDelivery.FromLegacyStoredColumns(alertSent, notificationType, sendError);
#pragma warning restore CS0618
            _historyStore.RecordAlertAsync(new AlertHistoryRecord(
                serverId, serverName, metricName,
                currentValue, thresholdValue,
                null, null,
                delivery,
                muted, detailText, contextJson)).GetAwaiter().GetResult();
            return delivery;
        }

        /// <summary>
        /// <see cref="IFindingAlertSender"/>: latest delivered-page time for (serverId, metricName) —
        /// seeds the shared AnalysisNotificationService #2054 hold across restarts (#3916). On this SKU
        /// every page row is a delivered page (the tray balloon rides the shared service's sink), so the
        /// store answers with any row. Thin forwarder over the store.
        /// </summary>
        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName)
            => _historyStore.GetLastDeliveredPageUtcAsync(serverId, metricName);

        /// <summary>
        /// <see cref="IFindingAlertSender"/>: dispatches a composed analysis-finding alert.
        /// Dashboard's cadence — per-channel email/webhook rows from
        /// <see cref="TrySendAlertEmailAsync"/>, plus a "tray" fallback row when no channel is
        /// configured to log (so the Alerts history tab still shows the finding). The fallback
        /// lives here, not in <see cref="TrySendAlertEmailAsync"/>, because the threshold-alert
        /// path records its own "tray" row separately — this fallback is analysis-path only.
        /// <para>#3916: returns the most-delivered disposition recorded. The shared service counts a
        /// Dashboard page as delivered through its wired tray sink regardless, so this value only matters
        /// to a caller without one.</para>
        /// </summary>
        public async Task<AlertDelivery?> SendFindingAlertAsync(FindingAlert alert)
        {
            if (alert.Route == FindingRoute.Digest)
            {
                /* #3712: the corroboration gate routed this finding to the digest — no channel is consulted.
                   The deprecated SKU has no digest document; the row is the whole record here, written with the
                   shared disposition so its grid and MCP read label it the way the live SKUs do. */
                return RecordAlert(
                    alert.ServerId,
                    alert.ServerName,
                    alert.MetricName,
                    alert.CurrentValue,
                    alert.ThresholdValue,
                    alertSent: false,
                    notificationType: AlertDelivery.ChannelDigest,
                    muted: false,
                    detailText: alert.DetailText,
                    contextJson: AlertContextSerializer.Serialize(alert.Context));
            }

            var recorded = await TrySendAlertEmailAsync(
                alert.MetricName,
                alert.ServerName,
                alert.CurrentValue,
                alert.ThresholdValue,
                alert.ServerId,
                alert.Context);

            // Round-2/3 review carry-over: require both the enable flag AND the URL for a
            // channel to count as "attempted", so the fallback fires when no channel can send.
            var emailWouldLog =
                _settings.SmtpEnabled
                && !string.IsNullOrWhiteSpace(_settings.SmtpServer)
                && !string.IsNullOrWhiteSpace(_settings.SmtpFromAddress)
                && !string.IsNullOrWhiteSpace(_settings.SmtpRecipients);
            var webhooksAttempted =
                (_settings.TeamsWebhookEnabled && !string.IsNullOrWhiteSpace(_settings.TeamsWebhookUrl))
             || (_settings.SlackWebhookEnabled && !string.IsNullOrWhiteSpace(_settings.SlackWebhookUrl));

            if (!emailWouldLog && !webhooksAttempted)
            {
                recorded = RecordAlert(
                    alert.ServerId,
                    alert.ServerName,
                    alert.MetricName,
                    alert.CurrentValue,
                    alert.ThresholdValue,
                    alertSent: false,
                    notificationType: "tray",
                    muted: false,
                    detailText: alert.DetailText,
                    contextJson: AlertContextSerializer.Serialize(alert.Context));
            }

            /* A configured channel that recorded nothing (email inside its cooldown, webhook throttled)
               delivered nothing; say so rather than return null, which the contract reserves for a catch. */
#pragma warning disable CS0618
            return recorded ?? AlertDelivery.FromLegacyStoredColumns(false, AlertDelivery.ChannelUndelivered, null);
#pragma warning restore CS0618
        }

        /// <summary>
        /// <see cref="IFindingAlertSender"/> (#3916): ONE message naming every held page over the cap, then one
        /// row per named page under its own metric name carrying the summary's delivery. The shared service
        /// raises ONE tray balloon for the summary through its wired sink, so the rows say the tray showed.
        /// Never throws.
        /// </summary>
        public async Task<AlertDelivery?> SendFindingSummaryAsync(IReadOnlyList<FindingAlert> named)
        {
            if (named is null || named.Count == 0)
                return null;
            try
            {
                var (serverName, currentValue, context) = FindingSummary.Compose(named);
                var result = await _core.TrySendAsync(
                    FindingSummary.MetricName, serverName, currentValue, named.Count.ToString(),
                    named[0].ServerId, context, attemptChannels: true);
                var delivery = AlertDelivery.FromFanout(result, muted: false, trayChannelPresent: true);
                foreach (var alert in named)
                {
                    await _historyStore.RecordAlertAsync(new AlertHistoryRecord(
                        alert.ServerId, alert.ServerName, alert.MetricName,
                        alert.CurrentValue, alert.ThresholdValue,
                        null, null,
                        delivery,
                        false, FindingSummary.RowDetailText(alert, named.Count),
                        AlertContextSerializer.Serialize(alert.Context)));
                }
                return delivery;
            }
            catch (Exception ex)
            {
                _logger.LogError($"SendFindingSummaryAsync error: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Gets email delivery health summary (from the shared send core).
        /// </summary>
        public (int ConsecutiveFailures, string? LastError) GetEmailHealth()
            => _core.GetEmailHealth();
    }
}
