/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Darling's <see cref="IFindingAlertSender"/> — the seam the shared
/// <see cref="AnalysisNotificationService"/> dispatches composed analysis-finding alerts
/// through. Lite's <c>EmailAlertService.SendFindingAlertAsync</c> cadence transplanted onto
/// the same <see cref="EmailSendCore"/>/webhook flow <see cref="DarlingAlertDeliverer"/>
/// already uses (same branding, same settings, same PG history store): the core attempts
/// email + fans out to Teams/Slack, then ONE combined <c>config_alert_log</c> row is written
/// per finding alert regardless of channel outcome (no channel configured → the row states
/// <see cref="AlertDelivery.ChannelNoneConfigured"/>), with the finding's numeric
/// severity/threshold and detail text persisted like Lite's row. Never throws.
/// <see cref="AlertDelivery.FromFanout"/> is called with <c>trayChannelPresent: false</c> for the same
/// reason <see cref="DarlingAlertDeliverer"/> does: this service is headless and has no tray (#3169).
///
/// <para>
/// A separate class from <see cref="DarlingAlertDeliverer"/> (where Lite folds both surfaces
/// into one EmailAlertService) because the two seams belong to different engines — the alert
/// engine's <c>IAlertDeliverer</c> and the analysis pipeline's <c>IFindingAlertSender</c>.
/// Each holds its own <see cref="EmailSendCore"/>; the per-fingerprint email cooldown state
/// is keyed by metric name and the two namespaces are disjoint ("Analysis: …" vs the engine
/// metrics), and both cores seed cooldowns from the SAME shared history store across
/// restarts (#1145), so the split changes no delivery behavior.
/// </para>
/// </summary>
public sealed class DarlingFindingAlertSender : IFindingAlertSender
{
    private readonly IAlertHistoryStore _historyStore;
    private readonly EmailSendCore _core;
    private readonly ILogger _logger;

    public DarlingFindingAlertSender(
        IAlertSettings settings,
        IAlertHistoryStore historyStore,
        WebhookAlertService webhookAlertService,
        ILogger logger)
    {
        _historyStore = historyStore ?? throw new ArgumentNullException(nameof(historyStore));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _core = new EmailSendCore(settings, historyStore, webhookAlertService, DarlingAlertDeliverer.Branding, logger);
    }

    /// <summary>
    /// <see cref="IFindingAlertSender"/>: latest alert_log time for (serverId, metricName),
    /// any channel/result — seeds the shared AnalysisNotificationService cooldown across
    /// restarts (the analysis cooldown is stamped unconditionally, so the persisted
    /// equivalent is the latest row for that metric_name). Delegates to the PG store.
    /// </summary>
    public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName)
        => _historyStore.GetLastAlertTimeAsync(serverId, metricName);

    /// <summary>
    /// <see cref="IFindingAlertSender"/>: dispatches a composed analysis-finding alert.
    /// Lite's cadence — one combined alert-history row written unconditionally, carrying the
    /// numeric severity/threshold and the detail text — so no separate fallback row is needed.
    /// </summary>
    public async Task SendFindingAlertAsync(FindingAlert alert)
    {
        if (alert is null)
        {
            return;
        }

        try
        {
            /* Findings are never muted here — the pipeline's mute filter dropped muted
               stories before they became findings (Lite passes muted: false identically). */
            var result = await _core.TrySendAsync(
                alert.MetricName, alert.ServerName, alert.CurrentValue, alert.ThresholdValue,
                alert.ServerId, alert.Context, attemptChannels: true, detailText: alert.DetailText);

            /* trayChannelPresent: false — the headless service has no tray; see DarlingAlertDeliverer. */
            var delivery = AlertDelivery.FromFanout(result, muted: false, trayChannelPresent: false);

            /* Always log the alert, regardless of channel status — the structured context
               persists as JSON alongside the flat detail_text, the numeric severity/threshold
               in the double columns (Lite's shape). */
            string? contextJson = alert.Context is not null ? AlertContextSerializer.Serialize(alert.Context) : null;
            await _historyStore.RecordAlertAsync(new AlertHistoryRecord(
                alert.ServerId, alert.ServerName, alert.MetricName,
                alert.CurrentValue, alert.ThresholdValue,
                alert.Severity, alert.NotifyThreshold,
                delivery,
                false, alert.DetailText, contextJson));
        }
        catch (Exception ex)
        {
            _logger.LogError("Finding alert delivery failed for {Metric} on {Server}: {Message}",
                alert.MetricName, alert.ServerName, ex.Message);
        }
    }
}
