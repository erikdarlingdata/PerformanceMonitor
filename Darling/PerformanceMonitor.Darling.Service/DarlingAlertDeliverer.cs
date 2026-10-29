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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Darling's <see cref="IAlertDeliverer"/> — Lite's <c>EmailAlertService.TrySendAlertEmailAsync</c>
/// record-and-send cadence transplanted: the shared <see cref="EmailSendCore"/> attempts email
/// (when SMTP is configured and outside the per-fingerprint cooldown) and fans out to the shared
/// <see cref="WebhookAlertService"/> (Teams/Slack), then ONE combined <c>config_alert_log</c> row
/// is written per fired alert regardless of channel outcome — including muted alerts (flagged
/// muted, channels skipped) and alerts with no channel configured at all, whose row states
/// <see cref="AlertDelivery.ChannelNoneConfigured"/> (the headless smoke asserts the row exists).
/// Throws only the caller's own cancellation — a dead SMTP server or Postgres store must not abort the
/// engine's sweep, but a service that is stopping must not be told its abandoned delivery failed.
///
/// <para>The row's disposition comes from <see cref="AlertDelivery.FromFanout"/> with
/// <c>trayChannelPresent: false</c>. This service is headless: it has no tray icon and no toast code, so
/// Lite's <c>tray</c> fallback — which is truthful there, where the deliverer really does show a balloon —
/// would assert a UI event that cannot occur here (#3169).</para>
///
/// <para>Delivery-mode fan-out (Lite/Dashboard parity): the effective mode is the shared
/// <see cref="AlertDeliveryModeResolver"/> of a per-server override (#1236,
/// <see cref="MonitoredServer.AlertDeliveryModeOverride"/> via the injected resolver) against the global
/// <see cref="DarlingAlertSettings.DeliveryMode"/>. In Per-event mode an alert carrying incidents is split by
/// the shared <see cref="PerEventNotification.Split"/> into one send+row per distinct incident (capped at
/// <see cref="DarlingAlertSettings.PerEventMax"/> with a trailing "+N more"); Summary mode — or any alert
/// without incidents (CPU, low-disk, jobs) — takes the single combined send unchanged.</para>
/// </summary>
public sealed class DarlingAlertDeliverer : IAlertDeliverer, IAlertResolutionSender
{
    private static readonly AlertBranding s_branding = new(
        "Performance Monitor Darling",
        /* Headless: no snooze-hint footer (Lite's points at its Settings window). */
        null);

    /// <summary>The branding this service feeds the shared email/template/webhook renderers.</summary>
    public static AlertBranding Branding => s_branding;

    private readonly IAlertHistoryStore _historyStore;
    private readonly EmailSendCore _core;
    private readonly WebhookAlertService _webhooks;
    private readonly ILogger _logger;
    private readonly DarlingAlertSettings _settings;
    private readonly Func<string, AlertNotificationMode?> _resolveServerOverride;
    private readonly InvestigationDispatcher? _investigations;
    private readonly Func<AlertMuteContext, bool>? _isAlertMuted;
    private readonly Func<DateTime> _utcNow;

    /* #5825: where each open alert's delivered fires went, keyed (server key, firing metric name), so its clear goes to
       those destinations and nowhere else. Written after a fire reaches at least one channel, removed by the clear. The
       fast path only: a clear this memory knows nothing of (a service restart since the fire) reads the alert's delivered
       fire rows from history instead (ReadDeliveredFireAsync). Guarded by its own lock. */
    private readonly Dictionary<(string ServerKey, string MetricName), DeliveredFire> _deliveredFires = new();

    /// <param name="resolveServerOverride">
    /// Maps a fired alert's <see cref="AlertOutcome.ServerKey"/> to that server's
    /// <see cref="MonitoredServer.AlertDeliveryModeOverride"/> (or null to inherit the global mode). Optional —
    /// null (the default) means every server inherits the global <see cref="DarlingAlertSettings.DeliveryMode"/>,
    /// which is also correct for incident-free self-alerts. The service passes a resolver over its live server set.
    /// </param>
    /// <param name="investigationDispatcher">
    /// #5632 Phase A: called once per history row, so an alert whose route sends it to an agent opens or joins an
    /// investigation request; since #5821 before the channels post, so the generic webhook can carry the request. Optional: null (the default, and every test that predates it) dispatches nothing.
    /// </param>
    /// <param name="isAlertMuted">
    /// #5825: the mute check a resolution consults before it is sent, the same rule set that mutes fires. Optional: null
    /// (the default) mutes no resolution.
    /// </param>
    /// <param name="utcNow">#5825: the clock a fire's and its clear's times are read from. Optional: null is the system clock.</param>
    public DarlingAlertDeliverer(
        DarlingAlertSettings settings,
        IAlertHistoryStore historyStore,
        WebhookAlertService webhookAlertService,
        ILogger logger,
        Func<string, AlertNotificationMode?>? resolveServerOverride = null,
        InvestigationDispatcher? investigationDispatcher = null,
        Func<AlertMuteContext, bool>? isAlertMuted = null,
        Func<DateTime>? utcNow = null)
    {
        _isAlertMuted = isAlertMuted;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _investigations = investigationDispatcher;
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _historyStore = historyStore ?? throw new ArgumentNullException(nameof(historyStore));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _resolveServerOverride = resolveServerOverride ?? (_ => null);
        _core = new EmailSendCore(settings, historyStore, webhookAlertService, s_branding, logger);
        _webhooks = webhookAlertService;
    }

    /// <summary>
    /// Closes the PagerDuty incident an external alert's fire opened (#5654), and posts nowhere else. See
    /// <see cref="WebhookAlertService.TrySendPagerDutyResolveAsync"/>: it sends only with PagerDuty auto-resolve on.
    /// </summary>
    public Task<AlertChannelOutcome> TryResolvePagerDutyAsync(
        string firingMetric, string serverName, string serverKey, string? detailText,
        IReadOnlyCollection<AlertSeverityLevel?> firedSeverities, CancellationToken cancellationToken = default) =>
        _webhooks.TrySendPagerDutyResolveAsync(firingMetric, serverName, serverKey, detailText, firedSeverities, cancellationToken);

    public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default) =>
        DeliverAndReportAsync(outcome, cancellationToken);

    /// <summary>
    /// The delivery, reporting its disposition (#3580). Every send goes through here —
    /// <see cref="DeliverAsync"/> is this with the answer discarded — so there is one delivery path and
    /// not a reporting one beside a silent one.
    ///
    /// <para><b>What comes back.</b> On the combined send (Summary mode, or any alert without incidents,
    /// which is every self-alert) the exact <see cref="AlertDelivery"/> the history row was written with.
    /// On a Per-event split there are N sends and N rows, and this returns the one
    /// <see cref="FailedSendBackoff.ReportForSplit"/> picks (#4822): a delivery that reached a channel, else a
    /// failed one so the engine tries the alert again, else <c>null</c> — "unreported" — when no send was
    /// attempted; the callers that read the answer (the three daily documents) carry structured
    /// <see cref="AlertContext.Details"/> since #3834 but no
    /// <see cref="AlertContext.Incidents"/>, so the Per-event branch below — which is gated on incidents,
    /// not on a context existing — remains unreachable for them and their disposition is always reported.
    /// That is a property of what a report IS rather than an accident of how it fires: an incident-carrying
    /// context would enter these documents into per-event splitting and the incident delivery filter, which
    /// are paging mechanisms a once-a-day report stays outside of (#3834 states this at each builder). The
    /// belt-and-suspenders catch below also answers <c>null</c>: both <c>TrySendAsync</c> and
    /// <c>RecordAlertAsync</c> are failure-isolated themselves, so a throw here is something outside the
    /// channels and says nothing about whether they delivered.</para>
    /// </summary>
    public async Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
    {
        if (outcome is null)
        {
            throw new ArgumentNullException(nameof(outcome));
        }

        /* #5320 (part of #4348): the statement filter runs at this choke point, so a caller that hands an outcome
           straight here (the PostgreSQL families, the self alerts, the custom alert rules) is filtered like an
           engine alert. An outcome AlertEngine.FireAsync already filtered is remembered by the filter and passes
           through untouched, so nothing is judged twice. Never throws. */
        outcome = AlertStatementFilter.Apply(outcome);

        try
        {
            /* #1236/#1141: a per-server override wins over the global mode; Per-event splits an
               incident-carrying alert into one send+row per distinct incident (capped, "+N more"). */
            var mode = AlertDeliveryModeResolver.Resolve(_resolveServerOverride(outcome.ServerKey), _settings.DeliveryMode);
            if (mode == AlertNotificationMode.PerEvent && outcome.Context?.Incidents is { Count: > 0 })
            {
                var deliveries = new List<AlertDelivery?>();
                foreach (var message in PerEventNotification.Split(outcome.Context, _settings.PerEventMax))
                {
                    /* Per-incident card: msg.CurrentValue is the incident's occurrence count, and
                       msg.NumericValue carries it as a number for the history row (#1830 — the
                       overflow message's "+N more" text is unparseable, so the store's text fallback
                       silently recorded 0). Threshold is the outcome's, unchanged. Matches Lite's
                       per-event sends; detail text rebuilt from the split context. */
                    deliveries.Add(await SendAndRecordAsync(
                        outcome, message.CurrentValue, message.Context,
                        AlertContextBuilders.ContextToDetailText(message.Context),
                        numericCurrentValue: message.NumericValue, numericThresholdValue: outcome.NumericThresholdValue,
                        deliveryMode: mode, cancellationToken: cancellationToken));
                }

                /* N sends, N rows: the split reports the one delivery the engine acts on (#4822). */
                return FailedSendBackoff.ReportForSplit(deliveries);
            }

            /* Summary mode, or an alert with no incidents (CPU/low-disk/jobs): one combined send+row, unchanged. */
            return await SendAndRecordAsync(
                outcome, outcome.CurrentValue, outcome.Context, outcome.DetailText,
                outcome.NumericCurrentValue, outcome.NumericThresholdValue,
                deliveryMode: mode, cancellationToken: cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("Alert delivery failed for {Metric} on {Server}: {Message}",
                outcome.MetricName, outcome.ServerName, ex.Message);
            return null;
        }
    }

    /// <summary>
    /// Sends one alert message (email + webhook fan-out via the shared core) and writes exactly one combined
    /// <c>config_alert_log</c> row for it — Lite's record-and-send cadence. Called once for a Summary alert and
    /// once per split incident in Per-event mode, so every send is paired with its own history row (the
    /// structured context persists as JSON alongside the flat detail_text). Muted alerts skip both channels but
    /// still record (flagged muted).
    /// </summary>
    /// <param name="deliveryMode">
    /// The mode <see cref="DeliverAndReportAsync"/> resolved for this server, forwarded to the shared send
    /// core for #3430's per-metric repeat ceiling. Passed rather than re-resolved so one alert's two channels
    /// and its history row all describe the same decision, and passed FAITHFULLY on the Per-event split —
    /// those messages must not be aggregated, which is that mode's own contract.
    /// </param>
    /// <param name="cancellationToken">
    /// #4752: the token <see cref="DeliverAndReportAsync"/> received, handed to the webhook posts so a service
    /// that is stopping does not wait out an endpoint that never answers. A cancel from this token comes out of
    /// here as the <see cref="OperationCanceledException"/> it is, before any row is written:
    /// <see cref="DeliverAndReportAsync"/> lets it through and records nothing for a delivery the service
    /// abandoned. A post that merely times out is still that channel's failure, and the row is still written.
    /// </param>
    /// <returns>The disposition the history row was written with — the same value, so what the caller is
    /// told and what the operator later reads in the alert log cannot disagree (#3580).</returns>
    private async Task<AlertDelivery> SendAndRecordAsync(
        AlertOutcome outcome, string currentValue, AlertContext? context, string? detailText,
        double? numericCurrentValue, double? numericThresholdValue, AlertNotificationMode deliveryMode,
        CancellationToken cancellationToken)
    {
        /* #2090: the fire site's severity rode AlertOutcome.Severity but the channel builders read
           only Context.SeverityOverride — so every self-alert (fired with Context: null) rendered
           INFO-blue in Teams/Slack/PagerDuty/webhooks while its log line said Critical. Fold the
           outcome's severity into the context here, once, upstream of every channel; ??= so an
           explicit override set by a context builder still wins. The context serializes into alert
           history below, and since #3539 A8e the serializer carries this property as the row's Severity
           member — so the history grids and get_alert_history read the tier the alert fired at (before
           that the projection dropped it, and this comment's "replays keep the severity" was not true). */
        if (outcome.Severity is not null)
        {
            context ??= new AlertContext();
            context.SeverityOverride ??= outcome.Severity;
        }

        /* #5642: the sample-age line rides the history row's detail_text and every channel's prose exactly
           once, whichever way the engine built the text; the context JSON below carries the typed field. */
        detailText = AlertDetailText.WithSampleAge(detailText, context);

        /* #5632 Phase A: the row's time is stamped once, here, so the investigation link names this exact row.
           #5821: the request is opened or joined BEFORE the channels post, so the generic webhook's {{investigation_json}}
           can carry it; per row (a Per-event split comes through here once per incident). A muted alert, a report, a
           recovery or an alert with no agent route opens nothing and waits on no store call; the dispatcher never throws.
           The history row is written after the send, so the link rows carry the values it will store: an agent that claims
           while the page is in flight reads them in its brief. */
        var alertTimeUtc = AlertHistoryEscalationSeed.TruncateToMicroseconds(DateTime.UtcNow);
        var investigationJson = _investigations is null
            ? null
            : await _investigations.OpenForWebhookAsync(
                outcome.ServerKey, outcome.ServerName, outcome.MetricName, outcome.Muted, alertTimeUtc, context,
                facts: InvestigationDispatcher.Facts(
                    currentValue, numericCurrentValue, outcome.ThresholdValue, numericThresholdValue, outcome.Muted, detailText,
                    context is not null ? AlertContextSerializer.Serialize(context) : null),
                cancellationToken: cancellationToken);

        /* Lite's EmailAlertService.cs:65-66 — muted alerts skip both channels but still record below.
           outcome.DisplayName (a custom rule's human name, or null for built-ins) renders in the
           subject/body/webhook titles in place of the "Custom:<id>" metric name; the metric name stays the
           cooldown/history/mute key. detailText is the same prose this method records as the history row's
           detail_text (#3297) — the channels get the alert's remedy, not only its number. */
        var result = await _core.TrySendAsync(
            outcome.MetricName, outcome.ServerName, currentValue, outcome.ThresholdValue,
            outcome.ServerKey, context, attemptChannels: !outcome.Muted, detailText: detailText,
            displayName: outcome.DisplayName, deliveryMode: deliveryMode, cancellationToken: cancellationToken,
            investigationJson: investigationJson);

        /* trayChannelPresent: false — this is the HEADLESS service. It has no tray icon and no toast
           code, so the taxonomy's "tray" fallback (which is Lite's, and truthful there) would assert a UI
           event that cannot occur here. Without a channel configured a fired alert is reported as
           "unconfigured", which is the state an operator can act on. */
        var delivery = AlertDelivery.FromFanout(result, outcome.Muted, trayChannelPresent: false);

        /* #5825: remember where a delivered fire went, so its clear can follow it there. */
        NoteDeliveredFire(outcome, currentValue, context, detailText, result);

        /* #3598 (design point 3): the ledger says WHERE the post went. The send core reports the routing
           decision the fan-out actually used — resolved once, after the cooldown, so it is the decision and
           not a re-derivation that could disagree with it if the route list reloaded in between — and it
           rides the row's context_json as the trailing Route member, beside #3539 A8e's Severity, with no
           schema change. Null when no channel reached resolution (throttled, folded, muted, unconfigured):
           a row that consulted no destination records none. The context is created here if the alert had
           none (every self-alert fires with Context: null), exactly as #2090 does for the severity above;
           the Viewer's detail window falls back to detail_text for a context with no detail items, so an
           empty-Details context carrying only provenance costs the operator nothing.

           #4750: the record also says what each channel's send DID. One success used to stand for the whole
           fan-out, so a channel that failed beside a channel that delivered read as delivered on the row and
           nowhere else. Only the outcome word is stored: a webhook error can carry its endpoint's URL, so the
           reason stays in send_error and out of the context. */
        if (result.Route is { } route)
        {
            context ??= new AlertContext();
            /* #6718: the generic post's rendered {{dedup_key}} rides its destination, so a resolution after a restart closes
               the key the receiver opened. The row's incidents are unfiltered (#3313) and cannot give that key back. */
            context.Route = route.ToDto(result.ChannelOutcomes, result.GenericDedupKey, result.PagerDutyDedupKey);
        }

        /* Always log the alert, regardless of channel status (EmailAlertService.cs:82-94). */
        string? contextJson = context is not null ? AlertContextSerializer.Serialize(context) : null;
        var record = new AlertHistoryRecord(
            outcome.ServerKey, outcome.ServerName, outcome.MetricName,
            currentValue, outcome.ThresholdValue,
            numericCurrentValue, numericThresholdValue,
            delivery,
            outcome.Muted, detailText, contextJson)
        {
            AlertTimeUtc = alertTimeUtc,
        };
        await _historyStore.RecordAlertAsync(record);

        return delivery;
    }

    /// <summary>The channels in fan-out order, so a resolution's channel list reads the same way every time.</summary>
    private static readonly string[] s_channelOrder =
    {
        NotificationRouter.TeamsChannel, NotificationRouter.SlackChannel, NotificationRouter.GenericChannel,
        NotificationRouter.PagerDutyChannel, NotificationRouter.EmailChannel,
    };

    /// <summary>One open alert's delivered fires (#5825): when the first one went out, the last value sent, the triage link
    /// the first carried, each channel's destination and the route that supplied it, and the PagerDuty incidents opened.</summary>
    private sealed class DeliveredFire
    {
        public DeliveredFire(DateTime firedAtUtc) => FiredAtUtc = firedAtUtc;

        public DateTime FiredAtUtc { get; }

        public string LastValue { get; set; } = "";

        public string? TriageUrl { get; set; }

        /// <summary>The newest delivered fire's detail text: the Database, Wait Type, Job and Query a mute rule is judged
        /// against, read the way the viewer reads a row (<see cref="AlertMuteContext.PopulateFromDetailText"/>).</summary>
        public string? DetailText { get; set; }

        /// <summary>Every destination each channel was fired to during the episode, each once (#6928). A severity filter can send a
        /// Warning fire and the Critical fire after it to different routes, and the clear owes each of them its close.</summary>
        public Dictionary<string, List<(string Destination, int? RouteId)>> Channels { get; } = new(StringComparer.Ordinal);

        public void AddDestination(string channel, string destination, int? routeId)
        {
            if (!Channels.TryGetValue(channel, out var list))
            {
                Channels[channel] = list = new List<(string Destination, int? RouteId)>();
            }

            if (!list.Contains((destination, routeId)))
            {
                list.Add((destination, routeId));
            }
        }

        public HashSet<string> PagerDutyDedupKeys { get; } = new(StringComparer.Ordinal);

        /// <summary>The <c>{{dedup_key}}</c> the generic webhook's fires rendered (#6718), which its resolution renders again so
        /// the receiver closes what the fire opened. Not the PagerDuty keys: the generic token ignores the auto-resolve flag.</summary>
        public HashSet<string> GenericDedupKeys { get; } = new(StringComparer.Ordinal);

        /// <summary>Channels a fire read back from history reached through a route that is gone or disabled now: no
        /// destination to send to, and the row's outcome says so.</summary>
        public List<string> RouteGoneChannels { get; } = new();
    }

    /// <summary>
    /// Records where a fire that reached at least one channel went (#5825). A closing edge of a recovery pair ("Server
    /// Restored", "AG Replica Reconnected") is not a fire: it is delivered as an alert of its own and closes nothing here.
    /// Repeats of an open alert add their channels; the episode keeps its first fire's time and triage link.
    /// </summary>
    private void NoteDeliveredFire(
        AlertOutcome outcome, string currentValue, AlertContext? context, string? detailText, EmailFanoutResult result)
    {
        if (outcome.Muted || result.Route is not { } route || result.ChannelOutcomes is not { } outcomes
            || AlertFamily.RecoveryPairs.ContainsKey(outcome.MetricName))
        {
            return;
        }

        var delivered = new List<(string Channel, string Destination, int? RouteId)>();
        foreach (var destination in route.All)
        {
            if (!outcomes.TryGetValue(destination.Channel, out var sent) || sent != AlertChannelOutcome.Delivered)
            {
                continue;
            }

            /* Email falls back to the parent's list when no route names recipients, as the send itself does. */
            var where = destination.Destination
                ?? (destination.Channel == NotificationRouter.EmailChannel ? _settings.SmtpRecipients : null);
            if (!string.IsNullOrWhiteSpace(where))
            {
                delivered.Add((destination.Channel, where, destination.RouteId));
            }
        }

        if (delivered.Count == 0)
        {
            return;
        }

        var now = _utcNow();
        var serverId = string.IsNullOrEmpty(outcome.ServerKey) ? outcome.ServerName : outcome.ServerKey;
        /* The key the PagerDuty trigger carried, from the filtered context, so the resolve closes the incident it opened and
           the triage link names it. Derived from this unfiltered context only when PagerDuty was not sent, for the link. */
        var dedupKey = result.PagerDutyDedupKey
            ?? WebhookAlertService.DerivePagerDutyDedupKeyForSend(serverId, outcome.MetricName, context, _settings);
        /* #6718: the key the generic post rendered, from the filtered context, not one derived again from this unfiltered one. */
        var genericDedupKey = result.GenericDedupKey;
        lock (_deliveredFires)
        {
            var key = (outcome.ServerKey, outcome.MetricName);
            if (!_deliveredFires.TryGetValue(key, out var fire))
            {
                fire = new DeliveredFire(now)
                {
                    TriageUrl = TriageLink.Build(_settings.TriageBaseUrl, outcome.ServerName, outcome.MetricName, now, dedupKey),
                };
                _deliveredFires[key] = fire;
            }

            fire.LastValue = currentValue;
            fire.DetailText = detailText;
            foreach (var (channel, where, routeId) in delivered)
            {
                fire.AddDestination(channel, where, routeId);
                if (channel == NotificationRouter.PagerDutyChannel)
                {
                    fire.PagerDutyDedupKeys.Add(dedupKey);
                }
                else if (channel == NotificationRouter.GenericChannel && genericDedupKey is not null)
                {
                    fire.GenericDedupKeys.Add(genericDedupKey);
                }
            }
        }
    }

    /// <summary>
    /// Sends an alert's clear to the destinations its delivered fire reached, then writes the resolution row (#5825).
    ///
    /// <para><b>Who gets it.</b> Only a channel a delivered fire of the same alert (server key and firing metric name)
    /// reached, and only where <see cref="NotificationRouteResolutions.SendsResolutions"/> says yes for the route that
    /// supplied that destination. PagerDuty follows <c>pagerduty_auto_resolve</c> alone and gets a <c>resolve</c> on each
    /// incident the fire opened. No delivered fire (muted, digest, throttled, unconfigured) sends nothing, and the row is
    /// today's <c>none</c> row. No minimum episode length: a delivered fire always gets its clear.</para>
    ///
    /// <para><b>Mute rules.</b> A rule that matches the alert stops the clear: nothing is sent, and the row says
    /// <c>muted</c>. <b>The two recovery pairs</b> ("Server Unreachable" and "AG Replica Disconnected") close through their
    /// own recovery alert, so their clear sends nothing more.</para>
    /// </summary>
    public async Task DeliverResolutionAsync(
        AlertResolution resolution, AlertResolutionSendOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(resolution);
        options ??= AlertResolutionSendOptions.Default;
        /* #6003: the row's context carries the firing metric (Resolves) beside the caller's own context, on every path:
           sent, muted, digested, throttled or with resolutions off. The investigation resolved detector reads it. */
        var record = DarlingSelfAlertEvaluator.BuildResolutionRecord(resolution, options.ContextJson);

        try
        {
            record = await SendResolutionAsync(resolution, record, options, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("Alert resolution send failed for {Metric} on {Server}: {Message}",
                resolution.MetricName, resolution.ServerName, ex.Message);
        }

        await _historyStore.RecordAlertAsync(record);
    }

    /// <summary>
    /// #6003, D8: the human channels of <paramref name="route"/> a notice can go to: Teams, Slack and email, each where the
    /// route has a destination and does not turn the channel off. The generic webhook is the agent's channel and PagerDuty
    /// would open an incident nothing resolves, so neither carries a notice.
    /// </summary>
    public static IReadOnlyList<(string Channel, string Destination)> NoticeTargets(NotificationRoute route)
    {
        ArgumentNullException.ThrowIfNull(route);
        var targets = new List<(string Channel, string Destination)>();
        if (!route.Enabled)
        {
            return targets;
        }

        void Add(string channel, string destination)
        {
            if (!string.IsNullOrWhiteSpace(destination) && !route.IsOffFor(channel))
            {
                targets.Add((channel, destination));
            }
        }

        Add(NotificationRouter.EmailChannel, route.SmtpRecipients);
        Add(NotificationRouter.TeamsChannel, route.TeamsUrl);
        Add(NotificationRouter.SlackChannel, route.SlackUrl);
        return targets;
    }

    /// <summary>
    /// #6003, D8: sends one notice to each of <paramref name="targets"/> and answers the channels that took it. Never throws
    /// for a channel's failure, which is logged.
    /// </summary>
    public async Task<IReadOnlyList<string>> SendNoticeAsync(
        IReadOnlyList<(string Channel, string Destination)> targets, string title, string serverName, string currentValue,
        string detail, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(targets);
        var delivered = new List<string>();
        foreach (var (channel, destination) in targets)
        {
            var error = channel == NotificationRouter.EmailChannel
                ? await _core.TrySendNoticeEmailAsync(destination, title, serverName, currentValue, detail)
                : await _webhooks.TrySendNoticeAsync(channel, destination, title, serverName, currentValue, detail, cancellationToken);
            if (error is null)
            {
                delivered.Add(channel);
            }
            else
            {
                _logger.LogWarning("Notice {Title} on {Server} was not sent to {Channel}: {Error}", title, serverName, channel, error);
            }
        }

        return delivered;
    }

    private async Task<AlertHistoryRecord> SendResolutionAsync(
        AlertResolution resolution, AlertHistoryRecord record, AlertResolutionSendOptions options,
        CancellationToken cancellationToken)
    {
        if (AlertFamily.RecoveryPairs.Values.Contains(resolution.MetricName, StringComparer.Ordinal))
        {
            lock (_deliveredFires)
            {
                _deliveredFires.Remove((resolution.ServerKey, resolution.MetricName));
            }

            return record;
        }

        DeliveredFire? fire;
        lock (_deliveredFires)
        {
            _deliveredFires.Remove((resolution.ServerKey, resolution.MetricName), out fire);
        }

        /* Memory is the fast path. A clear it knows nothing of (the service restarted since the fire, which every nightly
           install does) reads the open episode's delivered fire rows from history. */
        fire ??= await ReadDeliveredFireAsync(resolution, cancellationToken);
        if (fire is null)
        {
            return record;
        }

        var flags = _settings.ResolutionFlags;
        var routes = _settings.NotificationRoutes;
        var targets = new List<(string Channel, string Destination)>();
        var routeGone = new List<string>();
        foreach (var channel in s_channelOrder)
        {
            if (!fire.Channels.TryGetValue(channel, out var reachedList))
            {
                continue;
            }

            /* #6928: a severity filter can fire one channel to different destinations during an episode, so each is closed. */
            foreach (var reached in reachedList)
            {
                /* The route that carried the fire, as it is now. Deleted or disabled since the fire: the same answer as the
                   history path below (#5855 review), so the clear goes nowhere on that destination and the row says why. */
                var route = RouteNow(reached.RouteId, routes, out var gone);
                if (gone)
                {
                    if (!fire.RouteGoneChannels.Contains(channel))
                    {
                        fire.RouteGoneChannels.Add(channel);
                    }

                    continue;
                }

                if (channel == NotificationRouter.PagerDutyChannel
                    && (!options.ClosePagerDuty || fire.PagerDutyDedupKeys.Count == 0))
                {
                    continue;
                }

                if (NotificationRouteResolutions.SendsResolutions(route, channel, flags)
                    && !targets.Contains((channel, reached.Destination)))
                {
                    targets.Add((channel, reached.Destination));
                }
            }
        }

        /* A channel whose route was deleted or disabled since the fire (found above from memory, or read back from history)
           has no destination now. Nothing goes there, and the row says why, where the channel's own setting would have sent
           the clear. */
        foreach (var channel in s_channelOrder)
        {
            if (fire.RouteGoneChannels.Contains(channel)
                && (channel != NotificationRouter.PagerDutyChannel || options.ClosePagerDuty)
                && NotificationRouteResolutions.SendsResolutions(null, channel, flags))
            {
                routeGone.Add(channel);
            }
        }

        if (targets.Count == 0 && routeGone.Count == 0)
        {
            return record;
        }

        var channels = string.Join(",", s_channelOrder.Where(c => targets.Any(t => t.Channel == c) || routeGone.Contains(c)));
        if (IsMuted(resolution, options, fire.DetailText))
        {
            _logger.LogInformation("Alert resolution for {Metric} on {Server} muted by a mute rule",
                resolution.MetricName, resolution.ServerName);
            return record with
            {
                Delivery = AlertDelivery.FromFanout(
                    new EmailFanoutResult(AlertChannelOutcome.NotAttempted, null, AlertChannelOutcome.NotAttempted, null, true),
                    muted: true, trayChannelPresent: false),
                Muted = true,
                ResolutionChannels = channels,
                ResolutionOutcome = "muted",
            };
        }

        var clearedAt = _utcNow();
        var facts = new AlertResolutionFacts(
            resolution.Title, resolution.MetricName, resolution.ServerName, fire.FiredAtUtc, clearedAt, fire.LastValue,
            resolution.Message, fire.TriageUrl);
        var detail = facts.DetailText();
        var serverId = string.IsNullOrEmpty(resolution.ServerKey) ? resolution.ServerName : resolution.ServerKey;
        var outcomes = new Dictionary<string, AlertChannelOutcome>(StringComparer.Ordinal);
        string? emailError = null;
        string? webhookError = null;
        foreach (var (channel, destination) in targets)
        {
            string? error;
            if (channel == NotificationRouter.EmailChannel)
            {
                error = await _core.TrySendResolutionEmailAsync(destination, facts);
                emailError ??= error;
            }
            else if (channel == NotificationRouter.PagerDutyChannel)
            {
                error = null;
                foreach (var dedupKey in fire.PagerDutyDedupKeys)
                {
                    error ??= await _webhooks.TrySendPagerDutyResolveAsync(
                        destination, dedupKey, resolution.MetricName, resolution.ServerName, serverId, detail, cancellationToken);
                }
            }
            else if (channel == NotificationRouter.GenericChannel && fire.GenericDedupKeys.Count > 0)
            {
                /* #6718: one resolution post per key the fire's posts rendered, as the PagerDuty close does per incident. Usually one. */
                error = null;
                foreach (var dedupKey in fire.GenericDedupKeys)
                {
                    error ??= await _webhooks.TrySendResolutionAsync(
                        channel, destination, facts, serverId, dedupKey, cancellationToken);
                }
            }
            else
            {
                /* Teams and Slack carry no key. A generic fire with no recorded key lands here and the send reports it. */
                error = await _webhooks.TrySendResolutionAsync(channel, destination, facts, serverId, genericDedupKey: null, cancellationToken);
            }

            /* A channel with several destinations is Failed when any of them failed. */
            if (error is not null || !outcomes.ContainsKey(channel))
            {
                outcomes[channel] = error is null ? AlertChannelOutcome.Delivered : AlertChannelOutcome.Failed;
            }

            if (error is not null && channel != NotificationRouter.EmailChannel)
            {
                webhookError ??= $"{channel}: {error}";
            }
        }

        var webhookOutcomes = outcomes.Where(o => o.Key != NotificationRouter.EmailChannel).Select(o => o.Value).ToList();
        var fanout = new EmailFanoutResult(
            outcomes.GetValueOrDefault(NotificationRouter.EmailChannel, AlertChannelOutcome.NotAttempted), emailError,
            webhookOutcomes.Contains(AlertChannelOutcome.Delivered) ? AlertChannelOutcome.Delivered
                : webhookOutcomes.Contains(AlertChannelOutcome.Failed) ? AlertChannelOutcome.Failed
                : AlertChannelOutcome.NotAttempted,
            webhookError, AnyChannelConfigured: true);
        var failed = outcomes.Where(o => o.Value == AlertChannelOutcome.Failed).Select(o => o.Key).ToList();
        /* Channel names only: a webhook's error text can carry its URL, so the reason stays in send_error. */
        var outcome = new List<string>();
        if (failed.Count > 0)
        {
            outcome.Add("failed: " + string.Join(",", failed));
        }

        if (routeGone.Count > 0)
        {
            outcome.Add("route deleted or disabled: " + string.Join(",", routeGone));
        }

        return record with
        {
            Delivery = targets.Count == 0
                ? AlertDelivery.NoChannelApplies()
                : AlertDelivery.FromFanout(fanout, muted: false, trayChannelPresent: false),
            ResolutionSentAtUtc = outcomes.ContainsValue(AlertChannelOutcome.Delivered)
                ? AlertHistoryEscalationSeed.TruncateToMicroseconds(clearedAt)
                : null,
            ResolutionChannels = channels,
            ResolutionOutcome = outcome.Count == 0 ? "sent" : string.Join("; ", outcome),
        };
    }

    /// <summary>
    /// The open episode's delivered fires, read back from alert history (#5825): the clear's fallback when this
    /// deliverer's memory has no record of the alert, which is every clear after a service restart. The fire rows'
    /// <c>Route</c> records name each channel that delivered and the route that supplied it; the destination is that
    /// route's CURRENT one (a URL is a secret, so the row never holds it), or the channel's current default when no route
    /// supplied it. A route deleted or disabled since the fire gives that channel no destination. Null when the store
    /// cannot read history, no delivered fire is open, or the read fails (logged): the clear then sends nothing.
    /// </summary>
    private async Task<DeliveredFire?> ReadDeliveredFireAsync(AlertResolution resolution, CancellationToken cancellationToken)
    {
        if (_historyStore is not IDeliveredFireReader reader)
        {
            return null;
        }

        DeliveredFireHistory? history;
        try
        {
            history = await reader.GetOpenDeliveredFiresAsync(
                resolution.ServerKey, resolution.MetricName, resolution.Title, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not read the delivered fires of {Metric} on {Server} for its resolution: {Message}",
                resolution.MetricName, resolution.ServerName, ex.Message);
            return null;
        }

        if (history is null || history.Rows.Count == 0)
        {
            return null;
        }

        var routes = _settings.NotificationRoutes;
        var serverId = string.IsNullOrEmpty(resolution.ServerKey) ? resolution.ServerName : resolution.ServerKey;
        var newest = history.Rows[0];
        var fire = new DeliveredFire(history.FirstFiredAtUtc)
        {
            LastValue = newest.CurrentValue is { } value ? value.ToString("0.##", CultureInfo.InvariantCulture) : "",
            DetailText = newest.DetailText,
        };
        string? firstDedupKey = null;

        /* Every delivered fire of the episode adds its destination, as the memory path does (#6928). */
        foreach (var row in history.Rows)
        {
            if (AlertContextSerializer.TryReadRoute(row.ContextJson) is not { } route)
            {
                continue;
            }

            AlertContextSerializer.TryDeserialize(row.ContextJson, out var context);
            /* The key the PagerDuty trigger carried, stored on its destination as sent. A row written before that was stored,
               or one that did not send PagerDuty, carries none, and the key is derived from the row's incidents, which
               matches the trigger whenever the #3313 filter dropped none of them. The triage link takes the same key. */
            var dedupKey = route.Destinations.FirstOrDefault(d => d.Channel == NotificationRouter.PagerDutyChannel)?.SentKey
                ?? WebhookAlertService.DerivePagerDutyDedupKeyForSend(serverId, resolution.MetricName, context, _settings);
            firstDedupKey = dedupKey;
            foreach (var destination in route.Destinations)
            {
                /* #6718: the key the generic post rendered, stored on its destination as sent. A row written before that was
                   stored carries none, and the key is derived from the row's incidents, which matches the post whenever the
                   #3313 filter dropped none of them. */
                var genericDedupKey = destination.SentKey
                    ?? WebhookAlertService.DeriveGenericDedupKey(resolution.ServerKey, resolution.ServerName, resolution.MetricName, context);
                /* #6928: every delivered fire's destination counts, not only the newest, because a severity filter can send the
                   Warning fire and the Critical fire of one episode to different routes, and each owes its close. */
                if (destination.Outcome != AlertRouteOutcomes.Delivered)
                {
                    continue;
                }

                if (destination.Channel == NotificationRouter.PagerDutyChannel)
                {
                    fire.PagerDutyDedupKeys.Add(dedupKey);
                }
                else if (destination.Channel == NotificationRouter.GenericChannel)
                {
                    fire.GenericDedupKeys.Add(genericDedupKey);
                }

                var where = CurrentDestination(destination.Channel, destination.RouteId, routes, out var gone);
                if (gone)
                {
                    if (!fire.RouteGoneChannels.Contains(destination.Channel))
                    {
                        fire.RouteGoneChannels.Add(destination.Channel);
                    }

                    continue;
                }

                if (string.IsNullOrWhiteSpace(where))
                {
                    continue;
                }

                fire.AddDestination(destination.Channel, where, destination.RouteId);
            }
        }

        if (fire.Channels.Count == 0 && fire.RouteGoneChannels.Count == 0)
        {
            return null;
        }

        /* The triage link the episode's first fire carried: its time and dedup key. */
        fire.TriageUrl = TriageLink.Build(
            _settings.TriageBaseUrl, resolution.ServerName, resolution.MetricName, history.FirstFiredAtUtc, firstDedupKey);
        return fire;
    }

    /// <summary>The route that supplied a channel at the fire, as it is now (#5825): the one place a clear decides whether that
    /// route is gone, for the memory path and the history path alike. Null with <paramref name="routeGone"/> false when no
    /// route supplied the channel; null with it true when the route was deleted, or disabled (a disabled route is skipped at
    /// resolution exactly as if it did not exist, <see cref="NotificationRoute.Enabled"/>).</summary>
    private static NotificationRoute? RouteNow(int? routeId, IReadOnlyList<NotificationRoute> routes, out bool routeGone)
    {
        routeGone = false;
        if (routeId is not { } id)
        {
            return null;
        }

        var route = routes.FirstOrDefault(r => r.RouteId == id);
        if (route is null || !route.Enabled)
        {
            routeGone = true;
            return null;
        }

        return route;
    }

    /// <summary>A channel's destination now (#5825): the route's own when a route supplied it at the fire, else the
    /// channel's current default. <paramref name="routeGone"/> is true when that route is deleted or disabled.</summary>
    private string? CurrentDestination(string channel, int? routeId, IReadOnlyList<NotificationRoute> routes, out bool routeGone)
    {
        var route = RouteNow(routeId, routes, out routeGone);
        if (routeGone)
        {
            return null;
        }

        if (route is not null)
        {
            var own = channel switch
            {
                NotificationRouter.TeamsChannel => route.TeamsUrl,
                NotificationRouter.SlackChannel => route.SlackUrl,
                NotificationRouter.GenericChannel => route.GenericUrl,
                NotificationRouter.PagerDutyChannel => route.PagerDutyRoutingKey,
                NotificationRouter.EmailChannel => route.SmtpRecipients,
                _ => null,
            };

            /* Email falls back to the parent's list when the route names no recipients, as the send itself does. */
            return string.IsNullOrWhiteSpace(own) && channel == NotificationRouter.EmailChannel ? _settings.SmtpRecipients : own;
        }

        return channel switch
        {
            NotificationRouter.TeamsChannel => _settings.TeamsWebhookEnabled ? _settings.TeamsWebhookUrl : null,
            NotificationRouter.SlackChannel => _settings.SlackWebhookEnabled ? _settings.SlackWebhookUrl : null,
            NotificationRouter.GenericChannel => _settings.GenericWebhookEnabled ? _settings.GenericWebhookUrl : null,
            NotificationRouter.PagerDutyChannel => _settings.PagerDutyEnabled ? _settings.PagerDutyRoutingKey : null,
            NotificationRouter.EmailChannel => _settings.SmtpEnabled ? _settings.SmtpRecipients : null,
            _ => null,
        };
    }

    private bool IsMuted(AlertResolution resolution, AlertResolutionSendOptions options, string? fireDetailText)
    {
        if (_isAlertMuted is null)
        {
            return false;
        }

        try
        {
            /* The fire's scope as the viewer reads an alert row (ViewerAlertRow.ToMuteContext): the Database, Wait Type,
               Job and Query lines of the fire's detail text, parsed against the FIRING metric name, so a rule scoped to a
               database or a query matches the clear exactly when it matches the fire. */
            var context = new AlertMuteContext
            {
                ServerName = resolution.ServerName,
                ServerId = int.TryParse(resolution.ServerKey, NumberStyles.Integer, CultureInfo.InvariantCulture, out var id) ? id : null,
                ExternalSource = options.ExternalSource,
                MetricName = resolution.MetricName,
            };
            context.PopulateFromDetailText(fireDetailText, resolution.MetricName);
            return _isAlertMuted(context);
        }
        catch (Exception ex)
        {
            /* A mute check that throws mutes nothing, as on the fire side. */
            _logger.LogWarning("Mute check for the resolution of {Metric} on {Server} failed: {Message}",
                resolution.MetricName, resolution.ServerName, ex.Message);
            return false;
        }
    }
}
