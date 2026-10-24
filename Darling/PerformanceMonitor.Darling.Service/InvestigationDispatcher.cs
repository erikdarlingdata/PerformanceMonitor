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
/// which opens a request, joins the alert to an open or claimed one of the same server and family while its alerts keep
/// firing (within 30 minutes of its latest alert, up to 4 hours after its first), or refuses at the route's limit.
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
/// <para><b>The long-poll's wake-up (#5822, #6003).</b> When the store answers <see cref="InvestigationOpenOutcome.Opened"/>, or a
/// join it journaled as a <c>joined</c> event (a new metric or a higher severity; a quiet join and a limit wake nothing), the dispatcher fires <see cref="InvestigationOpenSignal.Opened"/>
/// for the route (the core calls it through the <c>opened</c> delegate it is given), which wakes every
/// <c>wait_for_investigation_requests</c> call waiting on it. The signal carries nothing: the woken call reads the committed
/// request from the store. With nobody waiting it costs a lock and an empty loop.</para>
///
/// <para><b>Never throws.</b> A failure, a cancellation included, is logged and the delivery that called it goes on: the
/// deliverer's and the finding sender's catch turn a throw into a null delivery, which would misreport a page that was sent.
/// Its store connections carry a 5-second <c>lock_timeout</c> (<see cref="WithLockTimeout"/>), so a held lock costs an alert
/// pass at most that long per statement; a slow store is bounded by the command timeout instead.</para>
///
/// <para><b>Shared core (#5820).</b> The open-or-join, the muted and family rules, the agent route, the dedup keys and the open
/// signal live in <see cref="InvestigationDispatchCore"/> in <c>PerformanceMonitor.Notifications</c>, so Lite runs the same
/// rules. This wrapper adds what is Darling's: the PostgreSQL store, the lock timeout, the process's open signal, and the
/// generic webhook's token with its read-back.</para>
/// </summary>
public sealed class InvestigationDispatcher
{
    /// <summary>The <c>lock_timeout</c> on the dispatcher's store connections, in milliseconds.</summary>
    public const int LockTimeoutMilliseconds = 5000;

    private readonly InvestigationDispatchCore _core;
    private readonly Func<Guid, CancellationToken, Task<(InvestigationRequestRow Row, string? Brief)?>>? _readRequest;
    private readonly McpDataDetail _dataDetail;
    private readonly ILogger _logger;

    /// <param name="store">The store, over a data source built from <see cref="WithLockTimeout"/>.</param>
    /// <param name="settings">The live alert settings: the route list and the PagerDuty auto-resolve flag are read per call.</param>
    /// <param name="logger">Where a failure is logged.</param>
    /// <param name="dataDetail">
    /// #5821: the <c>mcp.dataDetail</c> level the webhook token's brief is masked at, the level the MCP tools serve. The
    /// default is the strictest, so a caller that names none never sends more than any level allows.
    /// </param>
    /// <param name="openSignal">Fired when a request opens (#5822); null fires nothing.</param>
    /// <param name="keys">#6066: the store's pseudonym key source, read over the owner connection when the token's brief is masked at <c>metadataOnly</c>; null numbers per answer.</param>
    /// <param name="agentGate">
    /// #6005: asked with the server id and metric before an agent request is opened or joined; false holds the agent request
    /// back and nothing else (the page is the caller's). Null (every caller but the worker) holds nothing back.
    /// </param>
    public InvestigationDispatcher(InvestigationRequestStore store, IAlertSettings settings, ILogger logger,
        McpDataDetail dataDetail = McpDataDetail.MetadataOnly, InvestigationOpenSignal? openSignal = null,
        Func<string, string, CancellationToken, Task<bool>>? agentGate = null, IPseudonymKeySource? keys = null)
        : this((store ?? throw new ArgumentNullException(nameof(store))).OpenOrJoinAsync, settings, logger,
            store.ReadRequestAsync, dataDetail, openSignal, agentGate, keys, store.RecordRecurringSkipAsync)
    {
    }

    /// <summary>The seam a test uses to stand a failing store in.</summary>
    internal InvestigationDispatcher(
        Func<InvestigationAlert, CancellationToken, Task<InvestigationOpenResult>> openOrJoin,
        IAlertSettings settings,
        ILogger logger,
        Func<Guid, CancellationToken, Task<(InvestigationRequestRow Row, string? Brief)?>>? readRequest = null,
        McpDataDetail dataDetail = McpDataDetail.MetadataOnly,
        InvestigationOpenSignal? openSignal = null,
        Func<string, string, CancellationToken, Task<bool>>? agentGate = null,
        IPseudonymKeySource? keys = null,
        Func<InvestigationAlert, CancellationToken, Task>? recurringSkipped = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _core = new InvestigationDispatchCore(openOrJoin, settings, logger, openSignal is null ? null : openSignal.Opened, agentGate, keys,
            recurringSkipped);
        _readRequest = readRequest;
        _dataDetail = dataDetail;
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
    public Task<InvestigationOpenOutcome?> DispatchAsync(
        AlertHistoryRecord record, AlertContext? context, string? storyPathHash = null, bool recurringAtThisHour = false,
        CancellationToken cancellationToken = default) =>
        _core.DispatchAsync(record, context, storyPathHash, recurringAtThisHour, cancellationToken);

    /// <summary>The shared core, for the change watch's pass (#6234).</summary>
    internal InvestigationDispatchCore Core => _core;

    /// <summary>
    /// #5821: the values an alert's history row stores, for its link rows (<see cref="InvestigationDispatchCore.Facts"/>).
    /// </summary>
    public static InvestigationAlertFacts Facts(
        string currentValueText, double? numericCurrentValue, string thresholdValueText, double? numericThresholdValue,
        bool muted, string? detailText, string? contextJson) =>
        InvestigationDispatchCore.Facts(
            currentValueText, numericCurrentValue, thresholdValueText, numericThresholdValue, muted, detailText, contextJson);

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
    public Task<string?> OpenForWebhookAsync(
        string serverId, string serverName, string metricName, bool muted, DateTime alertTimeUtc, AlertContext? context,
        string? storyPathHash = null, InvestigationAlertFacts? facts = null, bool recurringAtThisHour = false,
        CancellationToken cancellationToken = default)
    {
        return _core.OpenForWebhookAsync(
            serverId, serverName, metricName, muted, alertTimeUtc, context, storyPathHash, facts,
            _readRequest, _dataDetail, recurringAtThisHour, cancellationToken);
    }
}
