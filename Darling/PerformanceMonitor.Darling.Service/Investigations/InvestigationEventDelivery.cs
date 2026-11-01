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
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// The sends one request-event pass makes (#6003): the signed POST of an event to an agent route, a notice to human
/// channels, and the self-monitor alert. The service's is <see cref="ServiceInvestigationEventSends"/>; a test stands its own in.
/// </summary>
public interface IInvestigationEventSends
{
    /// <summary>Posts <paramref name="payload"/> to the route's generic URL, signed, with <paramref name="eventId"/> as
    /// <c>webhook-id</c>. Null on success, else the error.</summary>
    Task<string?> PostEventAsync(int routeId, string url, string payload, string eventId, CancellationToken cancellationToken);

    /// <summary>Sends a notice to each target; answers the channels that took it.</summary>
    Task<IReadOnlyList<string>> SendNoticeAsync(
        IReadOnlyList<(string Channel, string Destination)> targets, string title, string serverName, string currentValue,
        string detail, CancellationToken cancellationToken);

    /// <summary>Raises the self-monitor alert (<see cref="AlertFamily.InvestigationUnansweredMetric"/>); answers whether a
    /// channel took it.</summary>
    Task<bool> RaiseSelfAlertAsync(Guid requestId, string serverName, int serverId, string shortMessage, string detail, CancellationToken cancellationToken);
}

/// <summary>The service's sends: the webhook service, the deliverer and the self-alert evaluator it already runs.</summary>
internal sealed class ServiceInvestigationEventSends(
    WebhookAlertService webhooks, DarlingAlertDeliverer deliverer, Func<DarlingSelfAlertEvaluator?> selfAlerts) : IInvestigationEventSends
{
    public Task<string?> PostEventAsync(int routeId, string url, string payload, string eventId, CancellationToken cancellationToken) =>
        webhooks.TrySendInvestigationEventAsync(routeId, url, payload, eventId, cancellationToken);

    public Task<IReadOnlyList<string>> SendNoticeAsync(
        IReadOnlyList<(string Channel, string Destination)> targets, string title, string serverName, string currentValue,
        string detail, CancellationToken cancellationToken) =>
        deliverer.SendNoticeAsync(targets, title, serverName, currentValue, detail, cancellationToken);

    public async Task<bool> RaiseSelfAlertAsync(Guid requestId, string serverName, int serverId, string shortMessage, string detail, CancellationToken cancellationToken) =>
        selfAlerts() is { } evaluator
        && (await evaluator.RaiseInvestigationUnansweredAsync(requestId, serverName, serverId, shortMessage, detail, cancellationToken))?.Sent == true;
}

/// <summary>
/// One pass of the request-event readers' service side (#6003, D5 and D8 of #5632 Phase C), run after each expiry sweep.
///
/// <para><b>Fallback (D8).</b> A request that expired with no agent holding it goes, in this order, to its agent route's
/// <c>fallback_route_id</c> (an enabled route with a human channel), else to the agent route's own human channels (email,
/// Teams, Slack), else to the self-monitor alert. The request's fallback mark is written first and the send made only when
/// this pass wrote it, so a fallback goes out at most once.</para>
///
/// <para><b>Signed POST (D5).</b> Every event (<see cref="InvestigationEvents.Kinds"/>) journaled in the last
/// <see cref="Lookback"/> without a delivery mark is posted to its request's route's own generic URL, signed with that route's
/// keys, with the event's <c>event_id</c> as <c>webhook-id</c>. The mark is written first, under the once key
/// <c>delivered:&lt;event_id&gt;</c>, and the post made only when this pass wrote it: a replayed event id is never delivered
/// twice, and a post that fails is logged and not retried (the receiver can catch up through the MCP long-poll, which reads
/// the same journal). A route with no generic URL gets the mark with the reason and no post.</para>
///
/// <para><b>Report notice (#6979).</b> A report made in the last <see cref="ReportNoticeWindow"/> whose route's
/// <c>report_notice</c> is not <c>off</c>, and that has no notice mark, goes to the route's Slack and Teams destinations when
/// the setting takes it: <c>all</c> takes every report, <c>needs_person_or_change</c> a <c>needs_person</c> verdict or a request
/// with an agent change. The mark (<c>report_notice:&lt;request_id&gt;</c>) is written first and the send made only when this pass
/// wrote it; a failed send is logged and not retried. The destinations resolve as an alert's do (<see cref="NotificationRouter"/>,
/// the settings' own channel when the route has none); a disabled route sends nothing. The root cause and each change's object are
/// masked at <c>mcp.dataDetail</c>.</para>
///
/// <para><b>Wake.</b> A pass that found an undelivered event wakes every long-poll waiter, so a <c>resolved</c>,
/// <c>expired</c>, <c>claims_exhausted</c> or <c>evidence_ready</c> event reaches a waiting agent within one pass; the
/// dispatcher wakes the route at once for <c>opened</c> and an evented <c>joined</c>.</para>
/// </summary>
public sealed class InvestigationEventDelivery
{
    /// <summary>How far back a pass reads events and expiries: a service down longer than this does not post stale events.</summary>
    public static readonly TimeSpan Lookback = TimeSpan.FromHours(1);

    /// <summary>The most events, and the most expiries, one pass takes.</summary>
    public const int BatchSize = 100;

    /// <summary>#6979: how far back a pass reads reports for a notice. A route switched to <c>all</c> does not post the history.</summary>
    public static readonly TimeSpan ReportNoticeWindow = TimeSpan.FromMinutes(30);

    /// <summary>The notice's title (the self-alert's metric name).</summary>
    public const string NoticeTitle = AlertFamily.InvestigationUnansweredMetric;

    private readonly InvestigationRequestStore _store;
    private readonly Func<IReadOnlyList<NotificationRoute>> _routes;
    private readonly IInvestigationEventSends _sends;
    private readonly InvestigationOpenSignal? _signal;
    private readonly ILogger _logger;
    private readonly Func<DateTime> _utcNow;
    private readonly IReportNoticeSends? _reportNotices;
    private readonly IAlertSettings? _defaults;
    private readonly McpDataDetail _dataDetail;
    private readonly Func<string> _baseUrl;

    /// <param name="reportNotices">#6979: sends a report notice to one Slack or Teams destination; null sends none.</param>
    /// <param name="defaults">#6979: the settings' channels, the parent default a route with no channel of its own falls to.</param>
    /// <param name="dataDetail">#6979: the <c>mcp.dataDetail</c> level the notice's text is masked at; the strictest unless told.</param>
    /// <param name="baseUrl">#6979: the web dashboard's public base URL (empty for no links), read per notice.</param>
    public InvestigationEventDelivery(InvestigationRequestStore store, Func<IReadOnlyList<NotificationRoute>> routes,
        IInvestigationEventSends sends, InvestigationOpenSignal? signal, ILogger logger, Func<DateTime>? utcNow = null,
        IReportNoticeSends? reportNotices = null, IAlertSettings? defaults = null,
        McpDataDetail dataDetail = McpDataDetail.MetadataOnly, Func<string>? baseUrl = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _routes = routes ?? throw new ArgumentNullException(nameof(routes));
        _sends = sends ?? throw new ArgumentNullException(nameof(sends));
        _signal = signal;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _reportNotices = reportNotices;
        _defaults = defaults;
        _dataDetail = dataDetail;
        _baseUrl = baseUrl ?? (() => "");
    }

    /// <summary>One pass: fallbacks, then event posts, then report notices. Answers how many fallbacks, posts and report notices
    /// this pass sent. Throws what the store throws; a send's failure is logged.</summary>
    public async Task<(int Fallbacks, int Posted, int Notices)> RunOnceAsync(CancellationToken cancellationToken)
    {
        var since = DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc) - Lookback;
        var routes = _routes();
        var fallbacks = 0;
        foreach (var expiry in await _store.UnclaimedExpiriesAsync(since, BatchSize, cancellationToken))
        {
            if (await FallBackAsync(expiry, routes, cancellationToken))
            {
                fallbacks++;
            }
        }

        var pending = await _store.PendingDeliveriesAsync(since, BatchSize, cancellationToken);
        /* The long-poll reads the journal, not the posts, so its waiters wake now rather than after every POST's timeout. */
        if (pending.Count > 0)
        {
            _signal?.WakeAll();
        }

        var posted = 0;
        foreach (var row in pending)
        {
            if (await DeliverAsync(row, routes, cancellationToken))
            {
                posted++;
            }
        }

        return (fallbacks, posted, await NoticePassAsync(routes, cancellationToken));
    }

    private async Task<bool> FallBackAsync(
        InvestigationUnclaimedExpiry expiry, IReadOnlyList<NotificationRoute> routes, CancellationToken cancellationToken)
    {
        var route = routes.FirstOrDefault(r => r.RouteId == expiry.RouteId);
        var fallback = route?.FallbackRouteId is int fallbackId ? routes.FirstOrDefault(r => r.RouteId == fallbackId) : null;
        var fallbackTargets = fallback is null ? [] : DarlingAlertDeliverer.NoticeTargets(fallback);
        var routeTargets = route is null ? [] : DarlingAlertDeliverer.NoticeTargets(route);
        var (target, targets) = fallbackTargets.Count > 0
            ? ("fallback_route:" + fallback!.RouteId.ToString(CultureInfo.InvariantCulture), fallbackTargets)
            : routeTargets.Count > 0
                ? ("route_channels", routeTargets)
                : ("self_alert", (IReadOnlyList<(string Channel, string Destination)>)[]);

        if (!await _store.MarkFallbackAsync(expiry, target, cancellationToken))
        {
            return false;
        }

        var shortMessage = string.Create(CultureInfo.InvariantCulture,
            $"Investigation request {expiry.RequestId} on {expiry.ServerName} expired with no agent working on it");
        var detail = string.Create(CultureInfo.InvariantCulture,
            $"{shortMessage}. It opened at {expiry.OpenedAtUtc:yyyy-MM-dd HH:mm:ss} UTC for {expiry.FirstMetric} (family {expiry.Family}) on route {expiry.RouteId} and expired at {expiry.ExpiredAtUtc:yyyy-MM-dd HH:mm:ss} UTC. Look at the alert yourself.");
        try
        {
            if (targets.Count > 0)
            {
                var sent = await _sends.SendNoticeAsync(targets, NoticeTitle, expiry.ServerName, "expired", detail, cancellationToken);
                if (sent.Count == 0)
                {
                    _logger.LogWarning("Investigation request {RequestId} expired unclaimed; its fallback ({Target}) reached no channel",
                        expiry.RequestId, target);
                }
                else
                {
                    _logger.LogInformation("Investigation request {RequestId} expired unclaimed; its fallback ({Target}) reached {Channels}",
                        expiry.RequestId, target, string.Join(",", sent));
                }
            }
            else
            {
                /* #6003 review: the last fallback reaching no channel is a warning; nothing else says this request went unanswered. */
                if (!await _sends.RaiseSelfAlertAsync(expiry.RequestId, expiry.ServerName, expiry.ServerId, shortMessage, detail, cancellationToken))
                {
                    _logger.LogWarning("Investigation request {RequestId} expired unclaimed; its self-alert fallback reached no channel",
                        expiry.RequestId);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Investigation request {RequestId} fallback ({Target}) failed: {Message}", expiry.RequestId, target, ex.Message);
        }

        return true;
    }

    private async Task<bool> DeliverAsync(InvestigationEventRow row, IReadOnlyList<NotificationRoute> routes, CancellationToken cancellationToken)
    {
        var route = routes.FirstOrDefault(r => r.RouteId == row.RouteId);
        if (route is null || !route.Enabled || string.IsNullOrWhiteSpace(route.GenericUrl)
            || route.IsOffFor(NotificationRouter.GenericChannel))
        {
            await _store.MarkDeliveredAsync(row, "not sent: the route has no generic URL", cancellationToken);
            return false;
        }

        if (!await _store.MarkDeliveredAsync(row, null, cancellationToken))
        {
            return false;
        }

        string? error;
        try
        {
            error = await _sends.PostEventAsync(route.RouteId, route.GenericUrl, Payload(row), MessageId(row.EventId), cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            error = ex.Message;
        }

        if (error is not null)
        {
            _logger.LogWarning("Investigation event {Event} {EventId} was not delivered to route {RouteId}: {Error}",
                row.Event, row.EventId, route.RouteId, error);
            return false;
        }

        return true;
    }

    private const string NeedsPersonVerdict = "needs_person";

    /// <summary>#6979: the report notices this pass sends. Answers how many reports reached at least one channel.</summary>
    private async Task<int> NoticePassAsync(IReadOnlyList<NotificationRoute> routes, CancellationToken cancellationToken)
    {
        if (_reportNotices is null || _defaults is null)
        {
            return 0;
        }

        var since = DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc) - ReportNoticeWindow;
        var sent = 0;
        foreach (var candidate in await _store.PendingReportNoticesAsync(since, BatchSize, cancellationToken))
        {
            if (await NoticeAsync(candidate, routes, _reportNotices, _defaults, cancellationToken))
            {
                sent++;
            }
        }

        return sent;
    }

    private async Task<bool> NoticeAsync(ReportNoticeCandidate candidate, IReadOnlyList<NotificationRoute> routes,
        IReportNoticeSends sends, IAlertSettings defaults, CancellationToken cancellationToken)
    {
        if (!ReportNoticeQualifies(candidate))
        {
            return false;
        }

        /* A route that is gone or disabled, or that has nowhere to send, writes no mark: the report stays a candidate for the rest of
           its window, so a destination set within it still gets the notice, and no mark claims a notice that was never sent. */
        var route = routes.FirstOrDefault(r => r.RouteId == candidate.RouteId);
        if (route is null || !route.Enabled)
        {
            return false;
        }

        var targets = ReportNoticeTargets(route, candidate.Metric, candidate.ServerId, defaults);
        if (targets.Count == 0)
        {
            return false;
        }

        /* The changes are read before the mark, so a read that fails is tried again next pass instead of losing the notice. */
        var changes = candidate.HasChange ? await _store.ChangesForRequestAsync(candidate.RequestId, cancellationToken) : [];
        if (!await _store.MarkReportNoticeAsync(candidate, candidate.ReportNotice, cancellationToken))
        {
            return false;
        }

        var content = new ReportNoticeContent(
            candidate.RequestId, candidate.Verdict, candidate.ServerName, candidate.Metric,
            RootCauseFirstLine(candidate.RootCause, _dataDetail),
            changes.Select(c => new ReportNoticeChange(c.Tool, ChangeObject(c.TargetId, c.Database, _dataDetail), ChangeResultWord(c.Result))).ToList(),
            _baseUrl() ?? "");
        var reached = 0;
        foreach (var (channel, destination) in targets)
        {
            string? error;
            try
            {
                error = await sends.TrySendReportNoticeAsync(channel, destination, content, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                error = ex.Message;
            }

            if (error is null)
            {
                reached++;
            }
            else
            {
                /* Not retried: the mark is written, and the report stays on the Investigations page. */
                _logger.LogWarning("Report notice for investigation request {RequestId} was not delivered to {Channel}: {Error}",
                    candidate.RequestId, channel, error);
            }
        }

        return reached > 0;
    }

    /// <summary>#6979: whether the route's setting takes this report: <c>all</c> every report, <c>needs_person_or_change</c> a
    /// <c>needs_person</c> verdict or a request with at least one agent change, <c>off</c> (or a word nobody knows) none.</summary>
    internal static bool ReportNoticeQualifies(ReportNoticeCandidate candidate)
    {
        ArgumentNullException.ThrowIfNull(candidate);
        return NotificationRouteKinds.NormalizeReportNotice(candidate.ReportNotice) switch
        {
            NotificationRouteKinds.ReportNoticeAll => true,
            NotificationRouteKinds.ReportNoticeNeedsPersonOrChange =>
                candidate.HasChange || string.Equals(candidate.Verdict, NeedsPersonVerdict, StringComparison.Ordinal),
            _ => false,
        };
    }

    /// <summary>
    /// #6979: the Slack and Teams destinations of a report notice, resolved as an alert's are (<see cref="NotificationRouter.Resolve"/>
    /// over the request's route alone, <paramref name="defaults"/> as the parent), so a route with no channel of its own sends to the
    /// settings' channel. Nothing for a disabled route, and nothing for a channel the route turns off (<see cref="NotificationRoute.IsOffFor"/>),
    /// even when the metric no longer matches the route and the resolver would reach the parent. Email, the generic URL and
    /// PagerDuty carry no notice.
    /// </summary>
    internal static IReadOnlyList<(string Channel, string Destination)> ReportNoticeTargets(
        NotificationRoute route, string metric, int serverId, IAlertSettings defaults)
    {
        ArgumentNullException.ThrowIfNull(route);
        ArgumentNullException.ThrowIfNull(defaults);
        var targets = new List<(string Channel, string Destination)>();
        if (!route.Enabled)
        {
            return targets;
        }

        var decision = NotificationRouter.Resolve(metric, [route], defaults, serverId);
        foreach (var routed in new[] { decision.Teams, decision.Slack })
        {
            if (routed.IsDelivered && !route.IsOffFor(routed.Channel))
            {
                targets.Add((routed.Channel, routed.Destination!.Trim()));
            }
        }

        return targets;
    }

    /// <summary>
    /// #6979: the first non-blank line of the root cause at <paramref name="mode"/>. <c>maskLiterals</c> masks the whole text before the
    /// line is cut, because a quoted literal that runs over a line break is only recognised whole; <c>metadataOnly</c> leaves the root
    /// cause out (null), as the MCP tools withhold a report's root cause there.
    /// </summary>
    internal static string? RootCauseFirstLine(string? rootCause, McpDataDetail mode)
    {
        if (mode == McpDataDetail.MetadataOnly || string.IsNullOrWhiteSpace(rootCause))
        {
            return null;
        }

        var text = mode == McpDataDetail.MaskLiterals ? McpDataDetailMasker.MaskLiterals(rootCause) : rootCause;
        foreach (var line in text.Split('\n'))
        {
            var trimmed = line.Trim();
            if (trimmed.Length > 0)
            {
                return trimmed;
            }
        }

        return null;
    }

    /// <summary>
    /// #6979: a change's object at <paramref name="mode"/>: the target id, with the database when the tool named one. Object and
    /// database names stay in the notice at every level (the MCP tools keep them, <c>McpDataDetail</c>), but a name can carry a
    /// literal (a plan guide's name, a threshold key), so both go through <see cref="McpDataDetailMasker.MaskLiterals"/> at
    /// <c>maskLiterals</c> and at <c>metadataOnly</c>, which never hands out more than <c>maskLiterals</c> does. An all-digit id
    /// (a rule, plan or session id the store or the server assigned) is kept: it is no literal of collected data, and masked it
    /// would read <c>?</c>.
    /// </summary>
    internal static string ChangeObject(string targetId, string? database, McpDataDetail mode)
    {
        string Mask(string value) => mode == McpDataDetail.Full || value.All(char.IsAsciiDigit) ? value : McpDataDetailMasker.MaskLiterals(value);
        var id = Mask(targetId ?? "");
        return string.IsNullOrWhiteSpace(database) ? id : id + " in " + Mask(database);
    }

    private static string ChangeResultWord(AgentChangeResult result) => result switch
    {
        AgentChangeResult.Done => "done",
        AgentChangeResult.Failed => "failed",
        _ => "result unknown",
    };

    /// <summary>The <c>webhook-id</c> of an event: its journal row's <c>event_id</c> (D6), so a receiver can drop a repeat.</summary>
    public static string MessageId(Guid eventId) => eventId.ToString("D");

    /// <summary>
    /// The event's POST body: its id, kind and time, the request and route it is about, and the journal row's detail. An
    /// <c>evidence_ready</c> detail names the sections (<c>sections</c>, <c>not_collected</c>, <c>pending</c>) the agent
    /// re-reads with the request id.
    /// </summary>
    public static string Payload(InvestigationEventRow row)
    {
        ArgumentNullException.ThrowIfNull(row);
        JsonNode? detail = null;
        if (!string.IsNullOrWhiteSpace(row.DetailJson))
        {
            try
            {
                detail = JsonNode.Parse(row.DetailJson);
            }
            catch (JsonException)
            {
                detail = null;
            }
        }

        var node = new JsonObject
        {
            ["type"] = "investigation." + row.Event,
            ["event_id"] = row.EventId.ToString("D"),
            ["event"] = row.Event,
            ["occurred_at"] = DateTime.SpecifyKind(row.EventTimeUtc, DateTimeKind.Utc).ToString("O", CultureInfo.InvariantCulture),
            ["request_id"] = row.RequestId.ToString("D"),
            ["route_id"] = row.RouteId,
            ["server_id"] = row.ServerId,
            ["server_name"] = row.ServerName,
            ["family"] = row.Family,
            ["reason"] = row.Reason,
            ["detail"] = detail,
        };
        return node.ToJsonString();
    }
}
