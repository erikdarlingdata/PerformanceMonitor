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
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The dispatch hook of Level 1 investigations (#5632 Phase A, plan v2 section 4, ruling R1): after an alert's history row is
/// written, it resolves the alert's agent route and hands the alert to <see cref="InvestigationRequestStore.OpenOrJoinAsync"/>,
/// which opens a request, joins the alert to an open or claimed one of the same server and family opened within the last
/// 30 minutes, or refuses at the route's limit.
///
/// <para><b>Where it is called.</b> Three places write an alert row that can start an investigation, and each calls
/// <see cref="DispatchAsync"/> once per row, after <c>RecordAlertAsync</c>, so the pages go out first and a store wait never
/// delays one: <see cref="DarlingAlertDeliverer"/> per row (a Per-event split writes a row per incident, so each incident is
/// linked); <see cref="DarlingFindingAlertSender.SendFindingAlertAsync"/> for a single page, never for a finding routed to the
/// digest (a digest is a batch, not an incident); and <see cref="DarlingFindingAlertSender.SendFindingSummaryAsync"/> per
/// held page it names. The other <c>RecordAlertAsync</c> callers (the worker's two resolution writers and
/// <c>ExternalAlertDrain</c>'s resolve) write resolution rows only, which are not alerts: they have no hook.</para>
///
/// <para><b>What never starts one.</b> A muted alert; an alert whose metric has no family in
/// <see cref="InvestigationFamily.Of"/> (the daily reports and the recovery alerts), read from that map and not re-derived here;
/// and an alert no route sends to an agent (<see cref="NotificationRouter.ResolveAgent"/> answers null).</para>
///
/// <para><b>The link.</b> The request's alert row carries the history row's own <see cref="AlertHistoryRecord.AlertTimeUtc"/>
/// (the store writes that instant into <c>config_alert_log.alert_time</c>), its server and metric, and the PagerDuty-style
/// dedup key the send path derives (<see cref="WebhookAlertService.DerivePagerDutyDedupKeyForSend"/>) followed by every
/// incident's own dedup key, so a request names the exact alert rows it grew from. The keys are the link only: they never
/// decide a join (#5632 review round 1, M2), since the send path's key is the constant <c>{server}:{metric}</c> for an alert
/// with no incidents.</para>
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
/// pass at most that long.</para>
/// </summary>
public sealed class InvestigationDispatcher
{
    /// <summary>The <c>lock_timeout</c> on the dispatcher's store connections, in milliseconds.</summary>
    public const int LockTimeoutMilliseconds = 5000;

    private readonly Func<InvestigationAlert, CancellationToken, Task<InvestigationOpenResult>> _openOrJoin;
    private readonly IAlertSettings _settings;
    private readonly ILogger _logger;
    private readonly InvestigationOpenSignal? _openSignal;

    /// <param name="store">The store, over a data source built from <see cref="WithLockTimeout"/>.</param>
    /// <param name="settings">The live alert settings: the route list and the PagerDuty auto-resolve flag are read per call.</param>
    /// <param name="logger">Where a failure is logged.</param>
    /// <param name="openSignal">Fired when a request opens (#5822); null fires nothing.</param>
    public InvestigationDispatcher(InvestigationRequestStore store, IAlertSettings settings, ILogger logger,
        InvestigationOpenSignal? openSignal = null)
        : this((store ?? throw new ArgumentNullException(nameof(store))).OpenOrJoinAsync, settings, logger, openSignal)
    {
    }

    /// <summary>The seam a test uses to stand a failing store in.</summary>
    internal InvestigationDispatcher(
        Func<InvestigationAlert, CancellationToken, Task<InvestigationOpenResult>> openOrJoin,
        IAlertSettings settings,
        ILogger logger,
        InvestigationOpenSignal? openSignal = null)
    {
        _openOrJoin = openOrJoin ?? throw new ArgumentNullException(nameof(openOrJoin));
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
    /// A finding page passes its lead finding's full story path hash (the metric name holds only eight characters of it);
    /// an engine alert has none and passes null.
    /// </summary>
    /// <returns>What the store did, or null when nothing was asked of it (muted, no family, no agent route) or it failed.</returns>
    public async Task<InvestigationOpenOutcome?> DispatchAsync(
        AlertHistoryRecord record, AlertContext? context, string? storyPathHash = null, CancellationToken cancellationToken = default)
    {
        if (record is null || record.Muted)
        {
            return null;
        }

        try
        {
            var family = InvestigationFamily.Of(record.MetricName);
            if (family is null)
            {
                return null;
            }

            var routeServerId = NotificationRouter.ParseServerId(record.ServerId);
            var route = NotificationRouter.ResolveAgent(record.MetricName, _settings.NotificationRoutes, routeServerId);
            if (route is null)
            {
                return null;
            }

            var alert = new InvestigationAlert(
                route.RouteId,
                AlertHistoryServerIdentity.StorageId(record.ServerId),
                record.ServerName,
                family,
                record.MetricName,
                record.AlertTimeUtc ?? AlertHistoryEscalationSeed.TruncateToMicroseconds(DateTime.UtcNow),
                DedupKeys(record, context, _settings),
                storyPathHash,
                route.AgentMaxPerHour,
                route.AgentMaxPerDay,
                route.AgentLeaseMinutes,
                route.AgentExpiryMinutes);

            var result = await _openOrJoin(alert, cancellationToken);
            switch (result.Outcome)
            {
                case InvestigationOpenOutcome.Opened:
                    _logger.LogInformation(
                        "Investigation request {RequestId} opened for {Metric} on {Server} (family {Family}, route {RouteId})",
                        result.RequestId, record.MetricName, record.ServerName, family, route.RouteId);
                    _openSignal?.Opened(route.RouteId);
                    break;
                case InvestigationOpenOutcome.LimitReached:
                    _logger.LogWarning(
                        "Investigation not opened for {Metric} on {Server}: route {RouteId} reached its limit ({PerHour} an hour, {PerDay} a day)",
                        record.MetricName, record.ServerName, route.RouteId, route.AgentMaxPerHour, route.AgentMaxPerDay);
                    break;
                default:
                    _logger.LogDebug(
                        "Alert {Metric} on {Server} joined investigation request {RequestId}",
                        record.MetricName, record.ServerName, result.RequestId);
                    break;
            }

            return result.Outcome;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Investigation dispatch failed for {Metric} on {Server}; the alert was delivered and recorded: {Message}",
                record.MetricName, record.ServerName, ex.Message);
            return null;
        }
    }

    /// <summary>
    /// The PagerDuty-style dedup key the send path derives for this row, then every incident's own key: the store records a
    /// link row per distinct key, so the request names each incident of the alert and not only the first.
    /// </summary>
    internal static IReadOnlyList<string> DedupKeys(AlertHistoryRecord record, AlertContext? context, IAlertSettings settings)
    {
        var keyServer = string.IsNullOrEmpty(record.ServerId) ? record.ServerName : record.ServerId;
        var keys = new List<string>
        {
            WebhookAlertService.DerivePagerDutyDedupKeyForSend(keyServer, record.MetricName, context, settings),
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
