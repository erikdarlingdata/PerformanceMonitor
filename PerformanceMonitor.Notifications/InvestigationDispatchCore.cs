/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The product-neutral core of the Level 1 investigation dispatch hook (#5632 Phase A, plan v2 section 4, ruling R1; shared
/// by #5820): for an alert that is about to be sent, or whose history row was just written, it resolves the alert's agent
/// route and hands the alert to the store's open-or-join (<see cref="IInvestigationRequestStore.OpenOrJoinAsync"/>), which
/// opens a request, joins the alert to an open or claimed one of the same server and family opened within the last
/// 30 minutes, or refuses at the route's limit. Each product wraps it with its own store, its own open signal and its own
/// webhook token (Darling's wrapper is <c>InvestigationDispatcher</c>).
///
/// <para><b>What never starts one.</b> A muted alert; an alert whose metric has no family in
/// <see cref="InvestigationFamily.Of"/> (the daily reports and the recovery alerts), read from that map and not re-derived here;
/// and an alert no route sends to an agent (<see cref="NotificationRouter.ResolveAgent"/> answers null).</para>
///
/// <para><b>The link.</b> The request's alert row carries the history row's own <see cref="AlertHistoryRecord.AlertTimeUtc"/>
/// (the store writes that instant into <c>config_alert_log.alert_time</c>), its server and metric, the values the history row
/// stores (<see cref="InvestigationAlertFacts"/>, #5821), and the PagerDuty-style
/// dedup key the send path derives (<see cref="WebhookAlertService.DerivePagerDutyDedupKeyForSend"/>) followed by every
/// incident's own dedup key, so a request names the exact alert rows it grew from. The keys are the link only: they never
/// decide a join (#5632 review round 1, M2), since the send path's key is the constant <c>{server}:{metric}</c> for an alert
/// with no incidents.</para>
///
/// <para><b>The open signal (#5822).</b> When the store answers <see cref="InvestigationOpenOutcome.Opened"/>, and only
/// then (a join is not a new request, and a limit opens nothing), the core calls its <c>opened</c> delegate with the route
/// id.</para>
///
/// <para><b>Never throws.</b> A failure, a cancellation included, is logged and the delivery that called it goes on: a
/// caller's catch would turn a throw into a null delivery, which would misreport a page that was sent.</para>
/// </summary>
public sealed class InvestigationDispatchCore
{
    private readonly Func<InvestigationAlert, CancellationToken, Task<InvestigationOpenResult>> _openOrJoin;
    private readonly IAlertSettings _settings;
    private readonly ILogger _logger;
    private readonly Action<int>? _opened;

    /// <param name="openOrJoin">The store's open-or-join (<see cref="IInvestigationRequestStore.OpenOrJoinAsync"/>).</param>
    /// <param name="settings">The live alert settings: the route list and the PagerDuty auto-resolve flag are read per call.</param>
    /// <param name="logger">Where a failure is logged.</param>
    /// <param name="opened">Called with the route id when a request opens (#5822); null calls nothing.</param>
    public InvestigationDispatchCore(
        Func<InvestigationAlert, CancellationToken, Task<InvestigationOpenResult>> openOrJoin,
        IAlertSettings settings,
        ILogger logger,
        Action<int>? opened = null)
    {
        _openOrJoin = openOrJoin ?? throw new ArgumentNullException(nameof(openOrJoin));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _opened = opened;
    }

    /// <summary>
    /// Opens or joins an investigation request for one written alert row, when its route sends it to an agent. Call it after
    /// <c>RecordAlertAsync</c> with the record that call wrote and the context the row was written from. Never throws.
    /// The link rows carry the record's values (<see cref="InvestigationAlertFacts"/>).
    /// A finding page passes its lead finding's full story path hash (the metric name holds only eight characters of it);
    /// an engine alert has none and passes null.
    /// </summary>
    /// <returns>What the store did, or null when nothing was asked of it (muted, no family, no agent route) or it failed.</returns>
    public async Task<InvestigationOpenOutcome?> DispatchAsync(
        AlertHistoryRecord record, AlertContext? context, string? storyPathHash = null, CancellationToken cancellationToken = default)
    {
        if (record is null)
        {
            return null;
        }

        var result = await OpenOrJoinAsync(
            record.ServerId, record.ServerName, record.MetricName, record.Muted,
            record.AlertTimeUtc ?? AlertHistoryEscalationSeed.TruncateToMicroseconds(DateTime.UtcNow),
            context, storyPathHash,
            Facts(record.CurrentValueText, record.NumericCurrentValue, record.ThresholdValueText, record.NumericThresholdValue,
                record.Muted, record.DetailText, record.ContextJson),
            cancellationToken);
        return result?.Outcome;
    }

    /// <summary>
    /// #5821: the values an alert's history row stores, for its link rows: the two numbers resolved from the numeric value or
    /// its display text exactly as the history store resolves them (<see cref="AlertValueParser.ResolveStoredValue"/>).
    /// </summary>
    public static InvestigationAlertFacts Facts(
        string currentValueText, double? numericCurrentValue, string thresholdValueText, double? numericThresholdValue,
        bool muted, string? detailText, string? contextJson) =>
        new(AlertValueParser.ResolveStoredValue(numericCurrentValue, currentValueText),
            AlertValueParser.ResolveStoredValue(numericThresholdValue, thresholdValueText),
            muted, detailText, contextJson);

    /// <summary>
    /// #5821: whether the generic webhook this alert posts to has <c>{{investigation_json}}</c> in its body template, resolved
    /// through the same router the send uses. Only then is the request read back: the token is in no shipped template.
    /// </summary>
    public bool GenericPostCarriesToken(string serverId, string metricName) =>
        WebhookAlertService.TemplateCarriesInvestigationToken(_settings.GenericWebhookBodyTemplate)
        && NotificationRouter.Resolve(metricName, _settings.NotificationRoutes, _settings, NotificationRouter.ParseServerId(serverId))
            .Generic.Destination is not null;

    /// <summary>
    /// Opens or joins the request for one alert when its route sends it to an agent, and calls the open signal when the
    /// store opened one. Never throws.
    /// </summary>
    /// <returns>What the store answered, or null when nothing was asked of it (muted, no family, no agent route) or it failed.</returns>
    public async Task<InvestigationOpenResult?> OpenOrJoinAsync(
        string serverId, string serverName, string metricName, bool muted, DateTime alertTimeUtc, AlertContext? context,
        string? storyPathHash, InvestigationAlertFacts? facts, CancellationToken cancellationToken)
    {
        if (muted)
        {
            return null;
        }

        try
        {
            var family = InvestigationFamily.Of(metricName);
            if (family is null)
            {
                return null;
            }

            var routeServerId = NotificationRouter.ParseServerId(serverId);
            var route = NotificationRouter.ResolveAgent(metricName, _settings.NotificationRoutes, routeServerId);
            if (route is null)
            {
                return null;
            }

            var alert = new InvestigationAlert(
                route.RouteId,
                AlertHistoryServerIdentity.StorageId(serverId),
                serverName,
                family,
                metricName,
                alertTimeUtc,
                DedupKeys(serverId, serverName, metricName, context, _settings),
                storyPathHash,
                route.AgentMaxPerHour,
                route.AgentMaxPerDay,
                route.AgentLeaseMinutes,
                route.AgentExpiryMinutes,
                facts);

            var result = await _openOrJoin(alert, cancellationToken);
            switch (result.Outcome)
            {
                case InvestigationOpenOutcome.Opened:
                    _logger.LogInformation(
                        "Investigation request {RequestId} opened for {Metric} on {Server} (family {Family}, route {RouteId})",
                        result.RequestId, metricName, serverName, family, route.RouteId);
                    _opened?.Invoke(route.RouteId);
                    break;
                case InvestigationOpenOutcome.LimitReached:
                    _logger.LogWarning(
                        "Investigation not opened for {Metric} on {Server}: route {RouteId} reached its limit ({PerHour} an hour, {PerDay} a day)",
                        metricName, serverName, route.RouteId, route.AgentMaxPerHour, route.AgentMaxPerDay);
                    break;
                default:
                    _logger.LogDebug(
                        "Alert {Metric} on {Server} joined investigation request {RequestId}",
                        metricName, serverName, result.RequestId);
                    break;
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Investigation dispatch failed for {Metric} on {Server}; the alert's delivery and record go on: {Message}",
                metricName, serverName, ex.Message);
            return null;
        }
    }

    /// <summary>
    /// The PagerDuty-style dedup key the send path derives for this row, then every incident's own key: the store records a
    /// link row per distinct key, so the request names each incident of the alert and not only the first.
    /// </summary>
    internal static IReadOnlyList<string> DedupKeys(AlertHistoryRecord record, AlertContext? context, IAlertSettings settings) =>
        DedupKeys(record.ServerId, record.ServerName, record.MetricName, context, settings);

    private static List<string> DedupKeys(
        string serverId, string serverName, string metricName, AlertContext? context, IAlertSettings settings)
    {
        var keyServer = string.IsNullOrEmpty(serverId) ? serverName : serverId;
        var keys = new List<string>
        {
            WebhookAlertService.DerivePagerDutyDedupKeyForSend(keyServer, metricName, context, settings),
        };

        if (context?.Incidents is { Count: > 0 } incidents)
        {
            foreach (var incident in incidents)
            {
                if (!string.IsNullOrEmpty(incident.DedupKey) && !keys.Contains(incident.DedupKey))
                {
                    keys.Add(incident.DedupKey);
                }
            }
        }

        return keys;
    }

    /// <summary>
    /// One pass of the expiry sweep (#5632 Phase A, ruling R4): <see cref="IInvestigationRequestStore.SweepAsync"/> expires
    /// lapsed leases and old requests, and a pass that changed anything is logged. The caller's loop owns the interval, the
    /// pause and the failure handling: this throws what the store throws.
    /// </summary>
    public static async Task<InvestigationSweepResult> SweepOnceAsync(
        IInvestigationRequestStore store, ILogger logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(logger);
        var swept = await store.SweepAsync(cancellationToken);
        if (swept.LeasesLapsed > 0 || swept.Expired > 0)
        {
            logger.LogInformation(
                "Investigation sweep: {Lapsed} lapsed lease(s) journaled, {Expired} request(s) expired",
                swept.LeasesLapsed, swept.Expired);
        }

        return swept;
    }
}
