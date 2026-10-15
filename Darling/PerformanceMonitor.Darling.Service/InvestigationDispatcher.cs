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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The dispatch hook of Level 1 investigations (#5632 Phase A, plan v2 section 4, ruling R1): for an alert that is about to be
/// sent, or whose history row was just written, it resolves the alert's agent route and hands the alert to
/// <see cref="InvestigationRequestStore.OpenOrJoinAsync"/>,
/// which opens a request, joins the alert to an open or claimed one of the same server and family opened within the last
/// 30 minutes, or refuses at the route's limit.
///
/// <para><b>Where it is called.</b> Three places write an alert row that can start an investigation, and each opens or joins
/// once per row. <see cref="DarlingAlertDeliverer"/> per row (a Per-event split writes a row per incident, so each incident is
/// linked) and <see cref="DarlingFindingAlertSender.SendFindingAlertAsync"/> for a single page (never for a finding routed to
/// the digest: a digest is a batch, not an incident) call <see cref="OpenForWebhookAsync"/> BEFORE the send and before
/// <c>RecordAlertAsync</c>, so an agent-routed alert's page waits on the store (see #5821 below).
/// <see cref="DarlingFindingAlertSender.SendFindingSummaryAsync"/> calls <see cref="DispatchAsync"/> per held page it names,
/// after its row is written. The other <c>RecordAlertAsync</c> callers (the worker's two resolution writers and
/// <c>ExternalAlertDrain</c>'s resolve) write resolution rows only, which are not alerts: they have no hook.</para>
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
/// <para><b>#5821: before the send when a webhook can carry it.</b> <see cref="DarlingAlertDeliverer"/> and a single finding
/// page call <see cref="OpenForWebhookAsync"/> BEFORE their channels post and before the row is written, with the instant the
/// row will carry, so the generic webhook's <c>{{investigation_json}}</c> token can name the request the alert opened or
/// joined. Only an alert with an agent route waits on the store there: every other alert answers before any store call. The
/// wait is the open or join, plus one read-back of the request only when the generic webhook this alert posts to has the
/// token in its body template; each is one command under the store's command timeout. The history row is written after the
/// send, so the link rows carry its values and an agent that claims while the page is in flight still reads them. The
/// finding summary still dispatches after its row: it is one page naming several alerts, so no one request is its own.</para>
///
/// <para><b>The brief is not composed here.</b> It is built when an agent claims the request
/// (<see cref="InvestigationRequestStore.SetBriefAsync"/> needs the claim id), so the dispatcher leaves <c>brief</c> NULL.</para>
///
/// <para><b>The long-poll's wake-up (#5822).</b> When the store answers <see cref="InvestigationOpenOutcome.Opened"/>, and only
/// then (a join is not a new request, and a limit opens nothing), the dispatcher fires <see cref="InvestigationOpenSignal.Opened"/>
/// for the route, which wakes every <c>wait_for_investigation_requests</c> call waiting on it. The signal carries nothing: the
/// woken call reads the committed request from the store. With nobody waiting it costs a lock and an empty loop.</para>
///
/// <para><b>Never throws.</b> A failure, a cancellation included, is logged and the delivery that called it goes on: the
/// deliverer's and the finding sender's catch turn a throw into a null delivery, which would misreport a page that was sent.
/// Its store connections carry a 5-second <c>lock_timeout</c> (<see cref="WithLockTimeout"/>), so a held lock costs an alert
/// pass at most that long per statement; a slow store is bounded by the command timeout instead.</para>
/// </summary>
public sealed class InvestigationDispatcher
{
    /// <summary>The <c>lock_timeout</c> on the dispatcher's store connections, in milliseconds.</summary>
    public const int LockTimeoutMilliseconds = 5000;

    private readonly Func<InvestigationAlert, CancellationToken, Task<InvestigationOpenResult>> _openOrJoin;
    private readonly Func<Guid, CancellationToken, Task<(InvestigationRequestRow Row, string? Brief)?>>? _readRequest;
    private readonly McpDataDetail _dataDetail;
    private readonly IAlertSettings _settings;
    private readonly ILogger _logger;
    private readonly InvestigationOpenSignal? _openSignal;

    /// <param name="store">The store, over a data source built from <see cref="WithLockTimeout"/>.</param>
    /// <param name="settings">The live alert settings: the route list and the PagerDuty auto-resolve flag are read per call.</param>
    /// <param name="logger">Where a failure is logged.</param>
    /// <param name="dataDetail">
    /// #5821: the <c>mcp.dataDetail</c> level the webhook token's brief is masked at, the level the MCP tools serve. The
    /// default is the strictest, so a caller that names none never sends more than any level allows.
    /// </param>
    /// <param name="openSignal">Fired when a request opens (#5822); null fires nothing.</param>
    public InvestigationDispatcher(InvestigationRequestStore store, IAlertSettings settings, ILogger logger,
        McpDataDetail dataDetail = McpDataDetail.MetadataOnly, InvestigationOpenSignal? openSignal = null)
        : this((store ?? throw new ArgumentNullException(nameof(store))).OpenOrJoinAsync, settings, logger,
            store.ReadRequestAsync, dataDetail, openSignal)
    {
    }

    /// <summary>The seam a test uses to stand a failing store in.</summary>
    internal InvestigationDispatcher(
        Func<InvestigationAlert, CancellationToken, Task<InvestigationOpenResult>> openOrJoin,
        IAlertSettings settings,
        ILogger logger,
        Func<Guid, CancellationToken, Task<(InvestigationRequestRow Row, string? Brief)?>>? readRequest = null,
        McpDataDetail dataDetail = McpDataDetail.MetadataOnly,
        InvestigationOpenSignal? openSignal = null)
    {
        _openOrJoin = openOrJoin ?? throw new ArgumentNullException(nameof(openOrJoin));
        _readRequest = readRequest;
        _dataDetail = dataDetail;
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _openSignal = openSignal;
    }

    /// <summary>
    /// <paramref name="connectionString"/> with <c>-c lock_timeout=5000</c> appended to its <c>Options</c>, for the data source
    /// the dispatcher and the expiry sweep share. A connection-level setting, so it holds on every pooled connection of that
    /// data source and on no other.
    /// </summary>
    public static string WithLockTimeout(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var option = "-c lock_timeout=" + LockTimeoutMilliseconds.ToString(CultureInfo.InvariantCulture);
        builder.Options = string.IsNullOrWhiteSpace(builder.Options) ? option : builder.Options + " " + option;
        return builder.ConnectionString;
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
    /// #5821: opens or joins the request for an alert that is about to be sent and recorded, and returns the generic webhook's
    /// <c>{{investigation_json}}</c> value for it (<see cref="InvestigationWebhookToken"/>), or null when no request was
    /// opened or joined (muted, no family, no agent route, the route's limit, a failure). Call it before the send, with the
    /// exact <paramref name="alertTimeUtc"/> the history row will carry and <paramref name="facts"/> holding the values it will
    /// store, and do not also call <see cref="DispatchAsync"/> for that row. The request is read back only when the generic
    /// webhook this alert posts to has the token in its body template
    /// (<see cref="WebhookAlertService.TemplateCarriesInvestigationToken"/>); otherwise the value names the request id only and
    /// no read is made. Never throws: a failed read-back, or a brief that cannot be rendered, still renders the request id.
    /// </summary>
    public async Task<string?> OpenForWebhookAsync(
        string serverId, string serverName, string metricName, bool muted, DateTime alertTimeUtc, AlertContext? context,
        string? storyPathHash = null, InvestigationAlertFacts? facts = null, CancellationToken cancellationToken = default)
    {
        var result = await OpenOrJoinAsync(
            serverId, serverName, metricName, muted, alertTimeUtc, context, storyPathHash, facts, cancellationToken);
        if (result is not { RequestId: Guid requestId } || result.Outcome == InvestigationOpenOutcome.LimitReached)
        {
            return null;
        }

        var opened = result.Outcome == InvestigationOpenOutcome.Opened;
        try
        {
            (InvestigationRequestRow Row, string? Brief)? read = null;
            if (_readRequest is not null && GenericPostCarriesToken(serverId, metricName))
            {
                try
                {
                    read = await _readRequest(requestId, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        "Investigation request {RequestId} could not be read back for the webhook; it carries the id only: {Message}",
                        requestId, ex.Message);
                }
            }

            return InvestigationWebhookToken.Render(requestId, opened, read?.Row, read?.Brief, _dataDetail);
        }
        catch (Exception ex)
        {
            /* The render parses and masks the stored brief; whatever fails there, the page still goes, carrying the id. The
               id-only render parses nothing. */
            _logger.LogWarning(
                "Investigation request {RequestId} could not be rendered for the webhook; it carries the id only: {Message}",
                requestId, ex.Message);
            return InvestigationWebhookToken.Render(requestId, opened, null, null, _dataDetail);
        }
    }

    /// <summary>
    /// #5821: whether the generic webhook this alert posts to has <c>{{investigation_json}}</c> in its body template, resolved
    /// through the same router the send uses. Only then is the request read back: the token is in no shipped template.
    /// </summary>
    private bool GenericPostCarriesToken(string serverId, string metricName) =>
        WebhookAlertService.TemplateCarriesInvestigationToken(_settings.GenericWebhookBodyTemplate)
        && NotificationRouter.Resolve(metricName, _settings.NotificationRoutes, _settings, NotificationRouter.ParseServerId(serverId))
            .Generic.Destination is not null;

    private async Task<InvestigationOpenResult?> OpenOrJoinAsync(
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
                    _openSignal?.Opened(route.RouteId);
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
}
