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
    private readonly Func<string, string, CancellationToken, Task<bool>>? _agentGate;
    private readonly IPseudonymKeySource? _keys;
    private readonly Func<InvestigationAlert, CancellationToken, Task>? _recurringSkipped;

    /// <param name="openOrJoin">The store's open-or-join (<see cref="IInvestigationRequestStore.OpenOrJoinAsync"/>).</param>
    /// <param name="settings">The live alert settings: the route list and the PagerDuty auto-resolve flag are read per call.</param>
    /// <param name="logger">Where a failure is logged.</param>
    /// <param name="opened">Called with the route id when a request opens (#5822); null calls nothing.</param>
    /// <param name="keys">#6066: the store's pseudonym key, read when a webhook token's brief is masked at <c>metadataOnly</c>; null numbers per answer.</param>
    /// <param name="recurringSkipped">#6596: the store's journal write for an alert whose story is recurring at this hour, which
    /// opens no request; null journals nothing (the skip is still logged).</param>
    public InvestigationDispatchCore(
        Func<InvestigationAlert, CancellationToken, Task<InvestigationOpenResult>> openOrJoin,
        IAlertSettings settings,
        ILogger logger,
        Action<int>? opened = null,
        Func<string, string, CancellationToken, Task<bool>>? agentGate = null,
        IPseudonymKeySource? keys = null,
        Func<InvestigationAlert, CancellationToken, Task>? recurringSkipped = null)
    {
        _agentGate = agentGate;
        _recurringSkipped = recurringSkipped;
        _keys = keys;
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
        AlertHistoryRecord record, AlertContext? context, string? storyPathHash = null, bool recurringAtThisHour = false,
        CancellationToken cancellationToken = default)
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
            cancellationToken, recurringAtThisHour: recurringAtThisHour);
        return result?.Outcome;
    }

    /// <summary>
    /// #6003: the alert's severity word for the request's <c>max_severity</c>: the tier graded at the fire site when it carries
    /// one, else the metric's own tier (red is critical; every other tier is warning).
    /// </summary>
    public static string SeverityOf(string metricName, AlertContext? context) =>
        context?.SeverityOverride switch
        {
            AlertSeverityLevel.Critical => InvestigationEvents.Critical,
            AlertSeverityLevel.Warning => InvestigationEvents.Warning,
            _ => AlertSeverity.ForMetric(metricName).HexColor == "#DC2626" ? InvestigationEvents.Critical : InvestigationEvents.Warning,
        };

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
    public bool GenericPostCarriesToken(string serverId, string metricName, AlertSeverityLevel? severity = null) =>
        WebhookAlertService.TemplateCarriesInvestigationToken(_settings.GenericWebhookBodyTemplate)
        && NotificationRouter.Resolve(metricName, _settings.NotificationRoutes, _settings, NotificationRouter.ParseServerId(serverId), severity)
            .Generic.Destination is not null;

    /// <summary>
    /// Opens or joins the request for one alert when its route sends it to an agent, and calls the open signal when the
    /// store opened one. Never throws.
    /// </summary>
    /// <param name="recurringAtThisHour">#6596: the analysis pass labelled the story that raised this alert "recurring at this
    /// hour" (<c>RecurrenceLabeler</c>). Such an alert opens and joins no request: the skip is journaled through the
    /// <c>recurringSkipped</c> delegate instead. The alert itself is the caller's and is unchanged.</param>
    /// <returns>What the store answered, or null when nothing was asked of it (muted, no family, no agent route, recurring at
    /// this hour) or it failed.</returns>
    public async Task<InvestigationOpenResult?> OpenOrJoinAsync(
        string serverId, string serverName, string metricName, bool muted, DateTime alertTimeUtc, AlertContext? context,
        string? storyPathHash, InvestigationAlertFacts? facts, CancellationToken cancellationToken,
        string triggerKind = InvestigationTriggerKinds.Alert, IReadOnlyList<string>? dedupKeys = null, Action<Exception>? failed = null,
        bool recurringAtThisHour = false)
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
            var route = NotificationRouter.ResolveAgent(metricName, _settings.NotificationRoutes, routeServerId,
                AlertSeverity.RouteLevelOf(metricName, context?.SeverityOverride));
            if (route is null)
            {
                return null;
            }

            /* #6005, D13: a host's agent gate (Darling's deadlock gate; null in Lite) may hold an agent request back. It sits
               after the route is resolved and before the store is asked, so it touches only the agent request: the page
               (the alert row and the send) is written by the caller whatever this answers. */
            if (_agentGate is not null && !await _agentGate(serverId, metricName, cancellationToken))
            {
                _logger.LogDebug("Investigation request for {Metric} on {Server} held back by the agent gate", metricName, serverName);
                return null;
            }

            var alert = new InvestigationAlert(
                route.RouteId,
                AlertHistoryServerIdentity.StorageId(serverId),
                serverName,
                family,
                metricName,
                alertTimeUtc,
                dedupKeys ?? DedupKeys(serverId, serverName, metricName, context, _settings),
                storyPathHash,
                route.AgentMaxPerHour,
                route.AgentMaxPerDay,
                route.AgentLeaseMinutes,
                route.AgentExpiryMinutes,
                facts)
            {
                /* #5912: the route's level is stamped on the request; the store lowers it to 1 for an alert of a custom rule
                   an agent last wrote, and to the lowest level on a join. */
                AutonomyLevel = route.AgentLevel ?? NotificationRoute.AgentLevelInvestigate,
                /* #6003, D6: a join above the request's max_severity is an event. */
                Severity = SeverityOf(metricName, context),
                /* #6234: a change watch's request goes through this same route, gate and store call; only its kind differs. */
                TriggerKind = triggerKind,
            };

            /* #6596 (Erik's option 1, for the request only): a story the analysis pass labelled "recurring at this hour" fired
               in this hour and weekday slot for three weeks running, which is a scheduled job's signature. An agent would spend
               a claim and about ten tool calls to report that, every week. So no request is opened or joined. The skip is
               journaled so a person can see it. The alert row, its severity and its send are the caller's and stay as they are:
               ruling Q3 (#3653) is "label, not discount". */
            if (recurringAtThisHour)
            {
                _logger.LogInformation(
                    "Investigation request for {Metric} on {Server} not opened: its story is recurring at this hour (route {RouteId})",
                    metricName, serverName, route.RouteId);
                if (_recurringSkipped is not null)
                {
                    await _recurringSkipped(alert, cancellationToken);
                }

                return null;
            }

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
                    /* #6003: a join that brought a new metric or a higher severity is an event, and wakes the long-poll. */
                    if (result.Evented)
                    {
                        _opened?.Invoke(route.RouteId);
                    }

                    break;
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Investigation dispatch failed for {Metric} on {Server}; the alert's delivery and record go on: {Message}",
                metricName, serverName, ex.Message);
            failed?.Invoke(ex);
            return null;
        }
    }

    /// <summary>
    /// #6234: opens the one investigation request of a change watch whose measures moved. It is the alert path with a different
    /// trigger: the same agent route (the metric <see cref="InvestigationTriggerKinds.ChangeWatchMetric"/> resolves like any
    /// other, so a route that sends everything, or this metric, to an agent turns it on), the same agent gate, the same store
    /// call and the same wake-up. Null when nothing was opened (no agent route, held by the gate, or a failure).
    /// <paramref name="requestKey"/> is the request's dedup key, the watch's id: a unique index admits one request per key, so a replay
    /// cannot open a second. <paramref name="failed"/> is called with the exception when the open threw, which tells a failure from "no route".
    /// </summary>
    public Task<InvestigationOpenResult?> OpenChangeWatchAsync(
        string serverId, string serverName, DateTime alertTimeUtc, InvestigationAlertFacts facts, CancellationToken cancellationToken,
        string? requestKey = null, Action<Exception>? failed = null) =>
        OpenOrJoinAsync(serverId, serverName, InvestigationTriggerKinds.ChangeWatchMetric, false, alertTimeUtc, null, null, facts,
            cancellationToken, InvestigationTriggerKinds.ChangeWatch, requestKey is null ? null : new[] { requestKey }, failed);

    /// <summary>
    /// #5821, shared by #5820: opens or joins the request for an alert that is about to be sent and recorded, and returns the
    /// generic webhook's <c>{{investigation_json}}</c> value for it (<see cref="InvestigationWebhookToken"/>), or null when no
    /// request was opened or joined (muted, no family, no agent route, the route's limit, a failure). The request is read
    /// back through <paramref name="readRequest"/> only when it is not null and the generic webhook this alert posts to has
    /// the token in its body template (<see cref="GenericPostCarriesToken"/>); otherwise the value names the request id only
    /// and no read is made. Never throws: a failed read-back, or a brief that cannot be rendered, still renders the request id.
    /// </summary>
    /// <param name="readRequest">The product's read of one request and its stored brief; null reads nothing.</param>
    /// <param name="dataDetail">The <c>mcp.dataDetail</c> level the token's brief is masked at.</param>
    public async Task<string?> OpenForWebhookAsync(
        string serverId, string serverName, string metricName, bool muted, DateTime alertTimeUtc, AlertContext? context,
        string? storyPathHash, InvestigationAlertFacts? facts,
        Func<Guid, CancellationToken, Task<(InvestigationRequestRow Row, string? Brief)?>>? readRequest,
        McpDataDetail dataDetail, bool recurringAtThisHour = false, CancellationToken cancellationToken = default)
    {
        var result = await OpenOrJoinAsync(
            serverId, serverName, metricName, muted, alertTimeUtc, context, storyPathHash, facts, cancellationToken,
            recurringAtThisHour: recurringAtThisHour);
        if (result is not { RequestId: Guid requestId } || result.Outcome == InvestigationOpenOutcome.LimitReached)
        {
            return null;
        }

        var opened = result.Outcome == InvestigationOpenOutcome.Opened;
        try
        {
            (InvestigationRequestRow Row, string? Brief)? read = null;
            if (readRequest is not null && GenericPostCarriesToken(serverId, metricName, AlertSeverity.RouteLevelOf(metricName, context?.SeverityOverride)))
            {
                try
                {
                    read = await readRequest(requestId, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        "Investigation request {RequestId} could not be read back for the webhook; it carries the id only: {Message}",
                        requestId, ex.Message);
                }
            }

            var key = await PseudonymKeySources.KeyForAsync(_keys, dataDetail, cancellationToken);
            return InvestigationWebhookToken.Render(requestId, opened, read?.Row, read?.Brief, dataDetail, key);
        }
        catch (Exception ex)
        {
            /* The render parses and masks the stored brief; whatever fails there, the page still goes, carrying the id. The
               id-only render parses nothing. */
            _logger.LogWarning(
                "Investigation request {RequestId} could not be rendered for the webhook; it carries the id only: {Message}",
                requestId, ex.Message);
            return InvestigationWebhookToken.Render(requestId, opened, null, null, dataDetail);
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
        if (swept.LeasesLapsed > 0 || swept.Expired > 0 || swept.Resolved > 0 || swept.FollowUps > 0)
        {
            logger.LogInformation(
                "Investigation sweep: {Lapsed} lapsed lease(s) journaled, {Expired} request(s) expired, {Resolved} request(s) resolved, {FollowUps} follow-up(s) opened",
                swept.LeasesLapsed, swept.Expired, swept.Resolved, swept.FollowUps);
        }

        return swept;
    }
}
