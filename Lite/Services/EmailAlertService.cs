/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Lite's per-app alert orchestrator shell (Plan E E3c, Approach B). The shared SMTP send,
/// cooldown, webhook fan-out, and failure counters live in <see cref="EmailSendCore"/>; this
/// shell owns Lite's record cadence: one combined <c>config_alert_log</c> row per call (with
/// the muted / detailText / numeric params), written via the injected
/// <see cref="IAlertHistoryStore"/>.
/// </summary>
public class EmailAlertService : IFindingAlertSender
{
    private static readonly AlertBranding s_branding = new(
        "Performance Monitor Lite",
        "To silence this alert: open Performance Monitor Lite → Settings → Manage Mute Rules");

    /// <summary>Test seam: the branding this app feeds the shared email/template renderer.</summary>
    internal static AlertBranding Branding => s_branding;

    private readonly IAlertHistoryStore _historyStore;
    private readonly EmailSendCore _core;
    private readonly ILogger<EmailAlertService> _logger;

    public EmailAlertService(
        IAlertSettings settings,
        IAlertHistoryStore historyStore,
        WebhookAlertService webhookAlertService,
        ILogger<EmailAlertService> logger)
    {
        _historyStore = historyStore;
        _logger = logger;
        _core = new EmailSendCore(settings, historyStore, webhookAlertService, s_branding, logger);
    }

    /// <summary>
    /// Attempts to send an alert (email + webhook via the shared core) and writes Lite's single
    /// combined <c>config_alert_log</c> row, regardless of email status. Never throws.
    /// </summary>
    public async Task TrySendAlertEmailAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        int serverId = 0,
        AlertContext? context = null,
        double? numericCurrentValue = null,
        double? numericThresholdValue = null,
        bool muted = false,
        string? detailText = null)
    {
        try
        {
            var result = await _core.TrySendAsync(
                metricName, serverName, currentValue, thresholdValue, serverId.ToString(), context, attemptChannels: !muted);

            /* Combine the channel outcomes into Lite's single-row notification_type taxonomy:
               muted → "muted"; email attempted → "email"; webhook delivery upgrades to
               "email+webhook"/"webhook"; otherwise "tray". */
            var notificationType = muted ? "muted" : "tray";
            if (result.EmailAttempted)
                notificationType = "email";

            var sent = result.EmailSent;
            if (result.WebhookSent)
            {
                notificationType = notificationType == "email" ? "email+webhook" : "webhook";
                sent = true;
            }

            /* Always log the alert, regardless of email status. The numeric current/threshold
               resolution happens in the store (it owns the DuckDB DOUBLE columns); the record
               carries both the display text and the optional numerics so no data is lost. The
               structured context is persisted as JSON alongside the flat detail_text so the
               in-app dialog can render the same advice / T-SQL / drill-down shape email and
               webhooks show (and survive purge of the source findings). */
            string? contextJson = context is not null ? AlertContextSerializer.Serialize(context) : null;
            await _historyStore.RecordAlertAsync(new AlertHistoryRecord(
                serverId.ToString(), serverName, metricName,
                currentValue, thresholdValue,
                numericCurrentValue, numericThresholdValue,
                sent, notificationType, result.SendError,
                muted, detailText, contextJson));
        }
        catch (Exception ex)
        {
            _logger.LogError($"TrySendAlertEmailAsync outer error: {ex.Message}");
        }
    }

    /// <summary>
    /// <see cref="IFindingAlertSender"/>: latest alert_log time for (serverId, metricName),
    /// any channel/result — seeds the shared AnalysisNotificationService cooldown across
    /// restarts (the analysis cooldown is stamped unconditionally, so the persisted equivalent
    /// is the latest row for that metric_name). Delegates to the injected store.
    /// </summary>
    public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName)
        => _historyStore.GetLastAlertTimeAsync(serverId, metricName);

    /// <summary>
    /// <see cref="IFindingAlertSender"/>: dispatches a composed analysis-finding alert.
    /// Lite's cadence — one combined <c>config_alert_log</c> row written unconditionally by
    /// <see cref="TrySendAlertEmailAsync"/>, which already carries the muted/detailText/numeric
    /// params — so no separate fallback row is needed.
    /// </summary>
    public Task SendFindingAlertAsync(FindingAlert alert)
    {
        var serverId = int.TryParse(alert.ServerId, out var sid) ? sid : 0;
        return TrySendAlertEmailAsync(
            alert.MetricName,
            alert.ServerName,
            alert.CurrentValue,
            alert.ThresholdValue,
            serverId,
            alert.Context,
            numericCurrentValue: alert.Severity,
            numericThresholdValue: alert.NotifyThreshold,
            muted: false,
            detailText: alert.DetailText);
    }
}
