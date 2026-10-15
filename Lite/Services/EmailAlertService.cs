/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
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

    /// <summary>#5750: how long an alert waits for the first load of the notification routes before it reads what is loaded.
    /// The wait is an await, so it holds no thread, and it ends at once when the routes are already loaded.</summary>
    private static readonly TimeSpan s_routeFirstLoadWait = TimeSpan.FromSeconds(5);

    private readonly IAlertHistoryStore _historyStore;
    private readonly EmailSendCore _core;
    private readonly ILogger<EmailAlertService> _logger;
    private readonly InvestigationDispatchCore? _investigations;
    private readonly Func<Guid, CancellationToken, Task<(InvestigationRequestRow Row, string? Brief)?>>? _readInvestigation;
    private readonly Func<McpDataDetail> _investigationDataDetail;

    /// <param name="investigations">
    /// #5820: the shared investigation dispatch core over Lite's investigation store. When set, an alert an agent route
    /// covers opens or joins an investigation request: after its history row is written, or before the channels post when
    /// the generic webhook's body template carries <c>{{investigation_json}}</c>. Null (the default) opens nothing.
    /// </param>
    /// <param name="readInvestigation">The store's read of one request and its brief, for the webhook token; null reads nothing.</param>
    /// <param name="investigationDataDetail">
    /// The <c>mcp_data_detail</c> level the webhook token's brief is masked at, read per alert; null is the setting's
    /// default, <see cref="McpDataDetail.Full"/>.
    /// </param>
    public EmailAlertService(
        IAlertSettings settings,
        IAlertHistoryStore historyStore,
        WebhookAlertService webhookAlertService,
        ILogger<EmailAlertService> logger,
        InvestigationDispatchCore? investigations = null,
        Func<Guid, CancellationToken, Task<(InvestigationRequestRow Row, string? Brief)?>>? readInvestigation = null,
        Func<McpDataDetail>? investigationDataDetail = null)
    {
        _historyStore = historyStore;
        _logger = logger;
        _core = new EmailSendCore(settings, historyStore, webhookAlertService, s_branding, logger);
        _investigations = investigations;
        _readInvestigation = readInvestigation;
        _investigationDataDetail = investigationDataDetail ?? (static () => McpDataDetail.Full);
    }

    /// <summary>
    /// Attempts to send an alert (email + webhook via the shared core) and writes Lite's single
    /// combined <c>config_alert_log</c> row, regardless of email status. Never throws, except for the caller's
    /// own cancellation (<paramref name="cancellationToken"/>).
    /// </summary>
    /// <param name="detailText">
    /// The alert's prose. PERSISTED on the row either way; delivered to the channels only when
    /// <paramref name="deliverProse"/>.
    /// </param>
    /// <param name="deliverProse">
    /// Whether the channels render <paramref name="detailText"/>. True for every threshold and self
    /// alert — that is the #3297 fix, and a self-alert's prose is the only thing it carries. The
    /// analysis-finding path passes false: its prose restates the structured context the channels
    /// already render (see <see cref="FindingAlert"/>).
    /// <para>Defaults to TRUE so a caller that never considers it delivers the prose. The two failure
    /// directions are not symmetric: delivering a redundant paragraph is visible and cosmetic, while
    /// withholding one discards an operator's only copy of the remedy, which is the #3296 defect.</para>
    /// </param>
    /// <param name="deliveryMode">
    /// #3430: the effective <see cref="AlertNotificationMode"/> for this server, which the caller resolved
    /// through <see cref="AlertDeliveryModeResolver"/>. Governs the shared send core's per-metric repeat
    /// ceiling only — it selects nothing about this method's own behaviour, because the #1141 split has
    /// already happened upstream by the time a message reaches here.
    /// <para>Defaults to null, meaning "the caller stated no mode", which declines aggregation. The analysis
    /// finding path (<see cref="FindingAlert"/>) and the dialog test-send take that default deliberately:
    /// findings carry their own <c>analysis_notify_cooldown_minutes</c> throttle and were not part of what
    /// #3430 measured.</para>
    /// </param>
    /// <param name="trayShown">
    /// #3916: whether the caller raised Lite's tray balloon for this alert on the same call. Defaults to
    /// TRUE — the engine path (<c>LiteAlertDeliverer.DeliverAsync</c>) shows a styled balloon for every
    /// non-muted alert. The analysis finding path passes FALSE: Lite wires no tray sink into
    /// <c>AnalysisNotificationService</c>, so no toast is raised for a finding, and a row stored
    /// <c>tray</c> there would read "Shown" for a toast nobody saw.
    /// </param>
    /// <param name="cancellationToken">
    /// #4752: handed to the webhook posts, so a caller that is stopping (the alert engine's token, through
    /// <see cref="LiteAlertDeliverer"/>) does not wait out an endpoint that never answers. A cancel from this
    /// token comes out of here as the <see cref="OperationCanceledException"/> it is, before any row is
    /// written: a delivery the caller abandoned is not a delivery that failed, so it leaves no history row and
    /// does not count against the channel. A post that only times out is still that channel's failure, and its
    /// row is still written. Optional, so every caller that has no token to give (the finding path, the
    /// connection-edge and availability-group alerts) compiles and behaves as before.
    /// </param>
    /// <param name="storyPathHash">
    /// #5820: an analysis finding's full story path hash, for the investigation request it opens (the metric name holds only
    /// eight characters of it). Null for an engine alert.
    /// </param>
    public async Task<AlertDelivery?> TrySendAlertEmailAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        int serverId = 0,
        AlertContext? context = null,
        double? numericCurrentValue = null,
        double? numericThresholdValue = null,
        bool muted = false,
        string? detailText = null,
        bool deliverProse = true,
        AlertNotificationMode? deliveryMode = null,
        bool trayShown = true,
        string? storyPathHash = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await NotificationRouteCache.WaitForFirstLoadAsync(s_routeFirstLoadWait);

            /* #5820: the row's time is stamped once, here, so the investigation request names this exact row (Darling's
               DarlingAlertDeliverer does the same). The context is kept as the caller gave it: the route record added
               below belongs to the history row, not to the incident keys the request links. */
            var alertTimeUtc = AlertHistoryEscalationSeed.TruncateToMicroseconds(DateTime.UtcNow);
            var investigationContext = context;

            /* #5820 (#5821's token): when the generic webhook this alert posts to carries {{investigation_json}}, the request
               is opened or joined BEFORE the channels post, so the post can carry it; the link rows carry the values the
               history row below will store. Every other alert opens or joins after its row is written. The core never
               throws: a store failure or a cancel posts the token empty and the delivery goes on. */
            string? investigationJson = null;
            var investigationAsked = false;
            if (_investigations is not null && !muted && _investigations.GenericPostCarriesToken(serverId.ToString(), metricName))
            {
                investigationAsked = true;
                investigationJson = await _investigations.OpenForWebhookAsync(
                    serverId.ToString(), serverName, metricName, muted, alertTimeUtc, context, storyPathHash,
                    InvestigationDispatchCore.Facts(
                        currentValue, numericCurrentValue, thresholdValue, numericThresholdValue, muted, detailText,
                        context is not null ? AlertContextSerializer.Serialize(context) : null),
                    _readInvestigation, _investigationDataDetail(), cancellationToken);
            }

            var result = await _core.TrySendAsync(
                metricName, serverName, currentValue, thresholdValue, serverId.ToString(), context, attemptChannels: !muted,
                detailText: deliverProse ? detailText : null, deliveryMode: deliveryMode,
                cancellationToken: cancellationToken, investigationJson: investigationJson);

            /* trayChannelPresent: trayShown, true on the engine path — LiteAlertDeliverer.DeliverAsync shows a styled balloon for every
               non-muted alert on the same call that reaches here, so a stored "tray" really does mean a
               toast was shown. This is Lite's half of the deliberate per-SKU divergence; the headless
               Darling service passes false because it has no tray at all. Lite's stored values are
               unchanged by the shared derivation. #3916: the value is trayShown, which the analysis finding
               path clears — no toast is raised for a finding on Lite, so its row must not claim one.
               Still the named argument, so the answer cannot be flipped by a positional edit. */
            var delivery = AlertDelivery.FromFanout(result, muted, trayChannelPresent: trayShown);

            /* #3598 / #4750: the row records where the posts went and what each channel's send did, exactly as
               the headless service's DarlingAlertDeliverer does. The record lists the channels the firing resolved
               to, each with the route that supplied it (route_id; source "Exact", "Family" or "Wildcard", or "Default"
               for the parent settings since #5750) and "delivered", "failed" or "not attempted" — an alert that reached one webhook and failed on another used to read as one
               delivery. Only the outcome word is stored: a webhook error can carry its endpoint's URL, so the
               reason stays in send_error. Null when no channel reached resolution (throttled, folded, muted,
               unconfigured), and the context is created here if the alert had none. */
            if (result.Route is { } route)
            {
                context ??= new AlertContext();
                context.Route = route.ToDto(result.ChannelOutcomes);
            }

            /* Always log the alert, regardless of email status. The numeric current/threshold
               resolution happens in the store (it owns the DuckDB DOUBLE columns); the record
               carries both the display text and the optional numerics so no data is lost. The
               structured context is persisted as JSON alongside the flat detail_text so the
               in-app dialog can render the same advice / T-SQL / drill-down shape email and
               webhooks show (and survive purge of the source findings). detailText in full here
               whatever the channels were handed — deliverProse governs delivery only, so the
               column the Alerts tab, the MCP reader and the mute pre-fill read is unaffected. */
            string? contextJson = context is not null ? AlertContextSerializer.Serialize(context) : null;
            var record = new AlertHistoryRecord(
                serverId.ToString(), serverName, metricName,
                currentValue, thresholdValue,
                numericCurrentValue, numericThresholdValue,
                delivery,
                muted, detailText, contextJson)
            {
                AlertTimeUtc = alertTimeUtc,
            };
            await _historyStore.RecordAlertAsync(record);

            /* #5820 (#5632 Phase A, ruling R1): after its row, an alert an agent route covers opens or joins a request,
               unless the webhook path above already asked for this one. A muted row, a report, a recovery and an
               unrouted alert open nothing. The core never throws, so a store failure or a cancel leaves the delivery
               as recorded. */
            if (_investigations is not null && !investigationAsked)
            {
                await _investigations.DispatchAsync(record, investigationContext, storyPathHash, cancellationToken);
            }

            /* #3916: the recorded disposition, for the analysis path's hold (a hold is earned by a
               delivery). The engine callers discard it. */
            return delivery;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* #4752: the caller's own cancel is a stop request, not a failed send. Passed on before the
               catch-all below, which would log it as an error and hand back null, so the deliverer would
               never see that its caller had stopped. The `when` filter leaves every other exception, a
               cancel this method did not ask for included, on the old path. */
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError($"TrySendAlertEmailAsync outer error: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// <see cref="IFindingAlertSender"/>: latest DELIVERED-page time for (serverId, metricName) —
    /// seeds the shared AnalysisNotificationService #2054 hold across restarts. #3916: a hold is
    /// earned by a delivery, so the seed reads only rows that reached someone. Delegates to the
    /// injected store.
    /// </summary>
    public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName)
        => _historyStore.GetLastDeliveredPageUtcAsync(serverId, metricName);

    /// <summary>
    /// <see cref="IFindingAlertSender"/>: dispatches a composed analysis-finding alert.
    /// Lite's cadence — one combined <c>config_alert_log</c> row written unconditionally by
    /// <see cref="TrySendAlertEmailAsync"/>, which already carries the muted/detailText/numeric
    /// params — so no separate fallback row is needed.
    /// <para>The row persists <c>DetailText</c>; the channels render it only if the producer says
    /// to. Darling's <c>DarlingFindingAlertSender</c> reads the same declaration.</para>
    /// <para>#3916: returns the delivery the row recorded; null only when the send path caught.</para>
    /// </summary>
    public Task<AlertDelivery?> SendFindingAlertAsync(FindingAlert alert)
    {
        /* #5320 (part of #4348): the statement filter, before anything is composed, sent or written, so the
           digest row, the channels and the history row all carry the filtered context and prose. */
        alert = AlertStatementFilter.Apply(alert);
        var serverId = int.TryParse(alert.ServerId, out var sid) ? sid : 0;

        if (alert.Route == FindingRoute.Digest)
        {
            /* #3712: the corroboration gate routed this finding to the digest — no channel is consulted, so
               this does NOT go through TrySendAlertEmailAsync (which would spend the send core's cooldown and
               stamp the row `tray`, a toast that was never shown). One row, disposition `digest`, the routing
               record already on the context; the Alerts tab, the MCP history read and the Recommendations grid
               all read it from there. */
            return RecordDigestRoutedFindingAsync(alert, serverId);
        }

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
            detailText: alert.DetailText,
            deliverProse: alert.DeliverDetailText,
            /* #3916: Lite raises NO toast for an analysis finding (MainWindow wires no tray sink into
               AnalysisNotificationService), so an unconfigured finding's row is "unconfigured", not a
               "tray" row that reads Shown. */
            trayShown: false,
            /* #5820: the request records the finding's full story path hash, not the metric name's eight characters. */
            storyPathHash: alert.StoryPathHash);
    }

    /// <summary>
    /// <see cref="IFindingAlertSender"/> (#3916): ONE message naming every held page over the cap, then one
    /// row per named page under its own metric name carrying the summary's delivery. No toast (Lite wires no
    /// analysis tray sink), so trayChannelPresent is false as on the single-page path. Never throws.
    /// </summary>
    public async Task<AlertDelivery?> SendFindingSummaryAsync(IReadOnlyList<FindingAlert> named)
    {
        if (named is null || named.Count == 0)
            return null;
        try
        {
        /* #5320 (part of #4348): the statement filter, before anything is composed, sent or written, so the
           digest row, the channels and the history row all carry the filtered context and prose. */
            named = AlertStatementFilter.Apply(named);
            var (serverName, currentValue, context) = FindingSummary.Compose(named);
            await NotificationRouteCache.WaitForFirstLoadAsync(s_routeFirstLoadWait);
            var result = await _core.TrySendAsync(
                FindingSummary.MetricName, serverName, currentValue, named.Count.ToString(),
                named[0].ServerId, context, attemptChannels: true);
            /* No toast for a summary on Lite, the same answer the single-finding arm states: the local keeps
               the one declared producer shape (trayChannelPresent: trayShown) the tray-channel pin reads. */
            const bool trayShown = false;
            var delivery = AlertDelivery.FromFanout(result, muted: false, trayChannelPresent: trayShown);
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

                /* #5820 (#5632 Phase A, ruling R1): each held page the summary names opens or joins a request after its
                   row, with its own full story path hash, as Darling's DarlingFindingAlertSender does. Never throws. */
                if (_investigations is not null)
                {
                    await _investigations.DispatchAsync(record, alert.Context, alert.StoryPathHash);
                }
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
    /// The digest arm of <see cref="SendFindingAlertAsync"/> (#3712): Lite's one combined
    /// <c>config_alert_log</c> row, written with <see cref="AlertDelivery.RoutedToDigest"/> and nothing
    /// attempted. Mirrors the row <see cref="TrySendAlertEmailAsync"/> writes column for column — the same
    /// numeric severity/threshold, the full detail text, the serialized context — so every reader of the
    /// table sees one shape. Never throws.
    /// </summary>
    private async Task<AlertDelivery?> RecordDigestRoutedFindingAsync(FindingAlert alert, int serverId)
    {
        try
        {
            string? contextJson = alert.Context is not null ? AlertContextSerializer.Serialize(alert.Context) : null;
            var delivery = AlertDelivery.RoutedToDigest();
            await _historyStore.RecordAlertAsync(new AlertHistoryRecord(
                serverId.ToString(), alert.ServerName, alert.MetricName,
                alert.CurrentValue, alert.ThresholdValue,
                alert.Severity, alert.NotifyThreshold,
                delivery,
                false, alert.DetailText, contextJson));
            return delivery;
        }
        catch (Exception ex)
        {
            _logger.LogError($"RecordDigestRoutedFindingAsync error: {ex.Message}");
            return null;
        }
    }
}
