/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
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
        /// </summary>
        public async Task TrySendAlertEmailAsync(
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

                if (result.EmailAttempted)
                {
                    var emailContextJson = context is not null ? AlertContextSerializer.Serialize(context) : null;
                    RecordAlert(serverId, serverName, metricName, currentValue, thresholdValue, result.EmailSent, "email", result.SendError, contextJson: emailContextJson);
                }

                if (result.WebhookSent)
                {
                    var webhookContextJson = context is not null ? AlertContextSerializer.Serialize(context) : null;
                    RecordAlert(serverId, serverName, metricName, currentValue, thresholdValue, true, "webhook", contextJson: webhookContextJson);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"TrySendAlertEmailAsync outer error: {ex.Message}");
            }
        }

        /// <summary>
        /// Records an alert (tray notification or email) to the alert-history store.
        /// Thin forwarder over <see cref="JsonAlertHistoryStore.RecordAlertAsync"/>. The store
        /// completes synchronously (in-memory + trim), so this stays a sync method to preserve
        /// the existing call sites' shape (MainWindow threshold alerts call it directly).
        /// </summary>
        public void RecordAlert(string serverId, string serverName, string metricName,
            string currentValue, string thresholdValue, bool alertSent,
            string notificationType, string? sendError = null, bool muted = false, string? detailText = null,
            string? contextJson = null)
        {
            _historyStore.RecordAlertAsync(new AlertHistoryRecord(
                serverId, serverName, metricName,
                currentValue, thresholdValue,
                null, null,
                alertSent, notificationType, sendError,
                muted, detailText, contextJson)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// <see cref="IFindingAlertSender"/>: latest alert_log time for (serverId, metricName),
        /// any channel/result — seeds the shared AnalysisNotificationService cooldown across
        /// restarts. Thin forwarder over the store.
        /// </summary>
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName)
            => _historyStore.GetLastAlertTimeAsync(serverId, metricName);

        /// <summary>
        /// <see cref="IFindingAlertSender"/>: dispatches a composed analysis-finding alert.
        /// Dashboard's cadence — per-channel email/webhook rows from
        /// <see cref="TrySendAlertEmailAsync"/>, plus a "tray" fallback row when no channel is
        /// configured to log (so the Alerts history tab still shows the finding). The fallback
        /// lives here, not in <see cref="TrySendAlertEmailAsync"/>, because the threshold-alert
        /// path records its own "tray" row separately — this fallback is analysis-path only.
        /// </summary>
        public async Task SendFindingAlertAsync(FindingAlert alert)
        {
            await TrySendAlertEmailAsync(
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
                RecordAlert(
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
        }

        /// <summary>
        /// Gets email delivery health summary (from the shared send core).
        /// </summary>
        public (int ConsecutiveFailures, string? LastError) GetEmailHealth()
            => _core.GetEmailHealth();
    }
}
