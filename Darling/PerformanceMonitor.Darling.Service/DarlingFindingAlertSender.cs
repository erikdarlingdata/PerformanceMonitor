/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Alerting;
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
    private readonly InvestigationDispatcher? _investigations;

    public DarlingFindingAlertSender(
        IAlertSettings settings,
        IAlertHistoryStore historyStore,
        WebhookAlertService webhookAlertService,
        ILogger logger,
        InvestigationDispatcher? investigationDispatcher = null)
    {
        /* #5632 Phase A: optional; null dispatches nothing. A single page and each held page the summary names open or
           join an investigation request after their rows are written; a digest finding never does. */
        _investigations = investigationDispatcher;
        _historyStore = historyStore ?? throw new ArgumentNullException(nameof(historyStore));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _core = new EmailSendCore(settings, historyStore, webhookAlertService, DarlingAlertDeliverer.Branding, logger);
    }

    /// <summary>
    /// <see cref="IFindingAlertSender"/>: latest DELIVERED-page time for (serverId, metricName) —
    /// seeds the shared AnalysisNotificationService #2054 hold across restarts. #3916: a hold is
    /// earned by a delivery, so the seed reads only rows that reached someone. Delegates to the PG store.
    /// </summary>
    public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName)
        => _historyStore.GetLastDeliveredPageUtcAsync(serverId, metricName);

    /// <summary>
    /// <see cref="IFindingAlertSender"/> (#3916): ONE message naming every held page over the cap, then one
    /// row per named page under its own metric name carrying the summary's delivery, so the restart seed
    /// finds every named story. Never throws; null only when this method caught.
    /// </summary>
    public async Task<AlertDelivery?> SendFindingSummaryAsync(IReadOnlyList<FindingAlert> named)
    {
        if (named is null || named.Count == 0)
            return null;
        try
        {
        /* #5320 (part of #4348): the statement filter, before anything is composed, sent or written. The
           context, its attachment and the prose are judged once here, so the digest row, the channels and
           the history row all carry the filtered values. */
            named = AlertStatementFilter.Apply(named);
            var (serverName, currentValue, context) = FindingSummary.Compose(named);
            var result = await _core.TrySendAsync(
                FindingSummary.MetricName, serverName, currentValue, named.Count.ToString(CultureInfo.InvariantCulture),
                named[0].ServerId, context, attemptChannels: true);
            var delivery = AlertDelivery.FromFanout(result, muted: false, trayChannelPresent: false);
            foreach (var alert in named)
            {
                var record = new AlertHistoryRecord(
                    alert.ServerId, alert.ServerName, alert.MetricName,
                    alert.CurrentValue, alert.ThresholdValue,
                    alert.Severity, alert.NotifyThreshold,
                    delivery,
                    false, FindingSummary.RowDetailText(alert, named.Count),
                    alert.Context is not null ? AlertContextSerializer.Serialize(alert.Context) : null)
                {
                    AlertTimeUtc = AlertHistoryEscalationSeed.TruncateToMicroseconds(DateTime.UtcNow),
                };
                await _historyStore.RecordAlertAsync(record);
                /* #5632 Phase A (ruling R1): each held page the summary names opens or joins a request, after its row. */
                if (_investigations is not null)
                {
                    await _investigations.DispatchAsync(record, alert.Context, alert.StoryPathHash,
                        recurringAtThisHour: alert.RecurringAtThisHour);
                }
            }
            return delivery;
        }
        catch (Exception ex)
        {
            _logger.LogError("Finding summary delivery failed for {Count} pages: {Message}", named.Count, ex.Message);
            return null;
        }
    }

    /// <summary>
    /// <see cref="IFindingAlertSender"/>: dispatches a composed analysis-finding alert.
    /// Lite's cadence — one combined alert-history row written unconditionally, carrying the
    /// numeric severity/threshold and the detail text — so no separate fallback row is needed.
    /// <para>The channels get <see cref="FindingAlert.DeliveredProse"/> and the row gets
    /// <c>DetailText</c>: two destinations, two values. See <see cref="FindingAlert"/>.</para>
    /// <para>#3916: returns the delivery the row recorded; null only when this method caught.</para>
    /// </summary>
    public async Task<AlertDelivery?> SendFindingAlertAsync(FindingAlert alert)
    {
        if (alert is null)
        {
            return null;
        }

        try
        {
        /* #5320 (part of #4348): the statement filter, before anything is composed, sent or written. The
           context, its attachment and the prose are judged once here, so the digest row, the channels and
           the history row all carry the filtered values. */
            alert = AlertStatementFilter.Apply(alert);
            var context = alert.Context;
            var alertTimeUtc = AlertHistoryEscalationSeed.TruncateToMicroseconds(DateTime.UtcNow);
            AlertDelivery delivery;

            if (alert.Route == FindingRoute.Digest)
            {
                /* #3712: the corroboration gate routed this finding to the daily digest. NO channel is
                   consulted — not the send core, not the webhook fan-out — so neither cooldown is spent and
                   the row states the route as its disposition. The context already carries the routing
                   record (AlertContext.Routing) with the reason; the row below persists it with the full
                   detail text and the drill-down, so the web surface, the MCP history read and the digest
                   reader see everything a page would have carried. */
                delivery = AlertDelivery.RoutedToDigest();
            }
            else
            {
                /* Findings are never muted here — the pipeline's mute filter dropped muted
                   stories before they became findings (Lite passes muted: false identically).
                   DeliveredProse, not DetailText: an analysis finding's prose restates the structured
                   context the channels already render, so delivering both prints the Diagnosis facts
                   twice. The persisted value below is unaffected. */
                /* #5821: a single page opens or joins its investigation request BEFORE the channels post, so the generic
                   webhook's {{investigation_json}} can carry it, at the instant its row below is stamped with, and its link rows
                   carry the values that row will store, so a claim made while the page is in flight reads them. */
                var investigationJson = _investigations is null
                    ? null
                    : await _investigations.OpenForWebhookAsync(
                        alert.ServerId, alert.ServerName, alert.MetricName, muted: false, alertTimeUtc, context,
                        alert.StoryPathHash,
                        InvestigationDispatcher.Facts(
                            alert.CurrentValue, alert.Severity, alert.ThresholdValue, alert.NotifyThreshold, muted: false,
                            alert.DetailText, context is not null ? AlertContextSerializer.Serialize(context) : null),
                        recurringAtThisHour: alert.RecurringAtThisHour);
                var result = await _core.TrySendAsync(
                    alert.MetricName, alert.ServerName, alert.CurrentValue, alert.ThresholdValue,
                    alert.ServerId, alert.Context, attemptChannels: true, detailText: alert.DeliveredProse,
                    investigationJson: investigationJson);

                /* trayChannelPresent: false — the headless service has no tray; see DarlingAlertDeliverer. */
                delivery = AlertDelivery.FromFanout(result, muted: false, trayChannelPresent: false);

                /* #3598: where the posts went, on the finding's context exactly as DarlingAlertDeliverer records it
                   for engine alerts — an "Analysis: …" finding is a performance-family alert and routes like one,
                   so its history row must say so too. A finding always carries a context, so nothing is created
                   here; null when no channel reached resolution. #4750: the record carries each channel's
                   outcome too, as DarlingAlertDeliverer explains. */
                if (result.Route is { } route)
                {
                    context ??= new AlertContext();
                    context.Route = route.ToDto(result.ChannelOutcomes, result.GenericDedupKey, result.PagerDutyDedupKey);
                }
            }

            /* Always log the alert, regardless of channel status — the structured context
               persists as JSON alongside the flat detail_text, the numeric severity/threshold
               in the double columns (Lite's shape). DetailText in full here whatever the channels
               were handed: this column is what the MCP reader, the triage endpoint and the Viewer's
               detail pane render, and what the mute pre-fill parses. */

            string? contextJson = context is not null ? AlertContextSerializer.Serialize(context) : null;
            var record = new AlertHistoryRecord(
                alert.ServerId, alert.ServerName, alert.MetricName,
                alert.CurrentValue, alert.ThresholdValue,
                alert.Severity, alert.NotifyThreshold,
                delivery,
                false, alert.DetailText, contextJson)
            {
                AlertTimeUtc = alertTimeUtc,
            };
            await _historyStore.RecordAlertAsync(record);
            /* #5632 Phase A (ruling R1): a single page opened or joined its investigation request above, before the send
               (#5821); a digest finding is a batch, not an incident, and never does. */

            return delivery;
        }
        catch (Exception ex)
        {
            _logger.LogError("Finding alert delivery failed for {Metric} on {Server}: {Message}",
                alert.MetricName, alert.ServerName, ex.Message);
            return null;
        }
    }
}
