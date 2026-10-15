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
       those destinations and nowhere else. Written after a fire reaches at least one channel, removed by the clear. In
       memory only: a restart forgets an open alert, so a clear after a restart sends nothing and writes its history row
       as before, which is the quiet direction. Guarded by its own lock. */
    private readonly Dictionary<(string ServerKey, string MetricName), DeliveredFire> _deliveredFires = new();

    /// <param name="resolveServerOverride">
    /// Maps a fired alert's <see cref="AlertOutcome.ServerKey"/> to that server's
    /// <see cref="MonitoredServer.AlertDeliveryModeOverride"/> (or null to inherit the global mode). Optional —
    /// null (the default) means every server inherits the global <see cref="DarlingAlertSettings.DeliveryMode"/>,
    /// which is also correct for incident-free self-alerts. The service passes a resolver over its live server set.
    /// </param>
    /// <param name="investigationDispatcher">
    /// #5632 Phase A: called after each history row is written, so an alert whose route sends it to an agent opens or joins
    /// an investigation request. Optional: null (the default, and every test that predates it) dispatches nothing.
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
        CancellationToken cancellationToken = default) =>
        _webhooks.TrySendPagerDutyResolveAsync(firingMetric, serverName, serverKey, detailText, cancellationToken);

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

        /* Lite's EmailAlertService.cs:65-66 — muted alerts skip both channels but still record below.
           outcome.DisplayName (a custom rule's human name, or null for built-ins) renders in the
           subject/body/webhook titles in place of the "Custom:<id>" metric name; the metric name stays the
           cooldown/history/mute key. detailText is the same prose this method records as the history row's
           detail_text (#3297) — the channels get the alert's remedy, not only its number. */
        var result = await _core.TrySendAsync(
            outcome.MetricName, outcome.ServerName, currentValue, outcome.ThresholdValue,
            outcome.ServerKey, context, attemptChannels: !outcome.Muted, detailText: detailText,
            displayName: outcome.DisplayName, deliveryMode: deliveryMode, cancellationToken: cancellationToken);

        /* trayChannelPresent: false — this is the HEADLESS service. It has no tray icon and no toast
           code, so the taxonomy's "tray" fallback (which is Lite's, and truthful there) would assert a UI
           event that cannot occur here. Without a channel configured a fired alert is reported as
           "unconfigured", which is the state an operator can act on. */
        var delivery = AlertDelivery.FromFanout(result, outcome.Muted, trayChannelPresent: false);

        /* #5825: remember where a delivered fire went, so its clear can follow it there. */
        NoteDeliveredFire(outcome, currentValue, context, result);

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
            context.Route = route.ToDto(result.ChannelOutcomes);
        }

        /* Always log the alert, regardless of channel status (EmailAlertService.cs:82-94). */
        string? contextJson = context is not null ? AlertContextSerializer.Serialize(context) : null;
        /* #5632 Phase A: the row's time is stamped here, so the investigation link row below names this exact row. */
        var record = new AlertHistoryRecord(
            outcome.ServerKey, outcome.ServerName, outcome.MetricName,
            currentValue, outcome.ThresholdValue,
            numericCurrentValue, numericThresholdValue,
            delivery,
            outcome.Muted, detailText, contextJson)
        {
            AlertTimeUtc = AlertHistoryEscalationSeed.TruncateToMicroseconds(DateTime.UtcNow),
        };
        await _historyStore.RecordAlertAsync(record);
        /* #5632 Phase A: after the row, per row (a Per-event split comes through here once per incident). A muted row, a
           report, a recovery or an alert with no agent route dispatches nothing; the dispatcher never throws. */
        if (_investigations is not null)
        {
            await _investigations.DispatchAsync(record, context, cancellationToken: cancellationToken);
        }

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

        public Dictionary<string, (string Destination, int? RouteId)> Channels { get; } = new(StringComparer.Ordinal);

        public HashSet<string> PagerDutyDedupKeys { get; } = new(StringComparer.Ordinal);
    }

    /// <summary>
    /// Records where a fire that reached at least one channel went (#5825). A closing edge of a recovery pair ("Server
    /// Restored", "AG Replica Reconnected") is not a fire: it is delivered as an alert of its own and closes nothing here.
    /// Repeats of an open alert add their channels; the episode keeps its first fire's time and triage link.
    /// </summary>
    private void NoteDeliveredFire(AlertOutcome outcome, string currentValue, AlertContext? context, EmailFanoutResult result)
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
        var dedupKey = WebhookAlertService.DerivePagerDutyDedupKeyForSend(serverId, outcome.MetricName, context, _settings);
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
            foreach (var (channel, where, routeId) in delivered)
            {
                fire.Channels[channel] = (where, routeId);
                if (channel == NotificationRouter.PagerDutyChannel)
                {
                    fire.PagerDutyDedupKeys.Add(dedupKey);
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
        var record = DarlingSelfAlertEvaluator.BuildResolutionRecord(resolution);
        if (options.ContextJson is not null)
        {
            record = record with { ContextJson = options.ContextJson };
        }

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

    private async Task<AlertHistoryRecord> SendResolutionAsync(
        AlertResolution resolution, AlertHistoryRecord record, AlertResolutionSendOptions options,
        CancellationToken cancellationToken)
    {
        DeliveredFire? fire;
        lock (_deliveredFires)
        {
            _deliveredFires.Remove((resolution.ServerKey, resolution.MetricName), out fire);
        }

        if (fire is null || AlertFamily.RecoveryPairs.Values.Contains(resolution.MetricName, StringComparer.Ordinal))
        {
            return record;
        }

        var flags = _settings.ResolutionFlags;
        var routes = _settings.NotificationRoutes;
        var targets = new List<(string Channel, string Destination)>();
        foreach (var channel in s_channelOrder)
        {
            if (!fire.Channels.TryGetValue(channel, out var reached))
            {
                continue;
            }

            if (channel == NotificationRouter.PagerDutyChannel
                && (!options.ClosePagerDuty || fire.PagerDutyDedupKeys.Count == 0))
            {
                continue;
            }

            var route = reached.RouteId is { } routeId ? routes.FirstOrDefault(r => r.RouteId == routeId) : null;
            if (NotificationRouteResolutions.SendsResolutions(route, channel, flags))
            {
                targets.Add((channel, reached.Destination));
            }
        }

        if (targets.Count == 0)
        {
            return record;
        }

        var channels = string.Join(",", targets.Select(t => t.Channel));
        if (IsMuted(resolution, options))
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
        var detail = ResolutionDetailText(resolution, fire, clearedAt);
        var serverId = string.IsNullOrEmpty(resolution.ServerKey) ? resolution.ServerName : resolution.ServerKey;
        var outcomes = new Dictionary<string, AlertChannelOutcome>(StringComparer.Ordinal);
        string? emailError = null;
        string? webhookError = null;
        foreach (var (channel, destination) in targets)
        {
            string? error;
            if (channel == NotificationRouter.EmailChannel)
            {
                error = await _core.TrySendResolutionEmailAsync(destination, resolution.Title, resolution.ServerName, detail, fire.TriageUrl);
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
            else
            {
                error = await _webhooks.TrySendResolutionAsync(
                    channel, destination, resolution.Title, resolution.ServerName, serverId, detail, fire.TriageUrl, cancellationToken);
            }

            outcomes[channel] = error is null ? AlertChannelOutcome.Delivered : AlertChannelOutcome.Failed;
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
        return record with
        {
            Delivery = AlertDelivery.FromFanout(fanout, muted: false, trayChannelPresent: false),
            ResolutionSentAtUtc = outcomes.ContainsValue(AlertChannelOutcome.Delivered)
                ? AlertHistoryEscalationSeed.TruncateToMicroseconds(clearedAt)
                : null,
            ResolutionChannels = channels,
            ResolutionOutcome = failed.Count == 0 ? "sent" : "failed: " + string.Join(",", failed),
        };
    }

    private bool IsMuted(AlertResolution resolution, AlertResolutionSendOptions options)
    {
        if (_isAlertMuted is null)
        {
            return false;
        }

        try
        {
            return _isAlertMuted(new AlertMuteContext
            {
                ServerName = resolution.ServerName,
                ServerId = int.TryParse(resolution.ServerKey, NumberStyles.Integer, CultureInfo.InvariantCulture, out var id) ? id : null,
                ExternalSource = options.ExternalSource,
                MetricName = resolution.MetricName,
            });
        }
        catch (Exception ex)
        {
            /* A mute check that throws mutes nothing, as on the fire side. */
            _logger.LogWarning("Mute check for the resolution of {Metric} on {Server} failed: {Message}",
                resolution.MetricName, resolution.ServerName, ex.Message);
            return false;
        }
    }

    /// <summary>The clear's prose (#5825): the resolution's own line, then when the alert fired and cleared, how long it
    /// lasted and the last value its fire sent.</summary>
    internal static string ResolutionDetailText(AlertResolution resolution, DateTime firedAtUtc, DateTime clearedAtUtc, string lastValue)
    {
        var text = new StringBuilder(resolution.Message);
        text.Append(CultureInfo.InvariantCulture, $"\nFired: {firedAtUtc:yyyy-MM-dd HH:mm:ss} UTC");
        text.Append(CultureInfo.InvariantCulture, $"\nCleared: {clearedAtUtc:yyyy-MM-dd HH:mm:ss} UTC");
        text.Append(CultureInfo.InvariantCulture, $"\nLasted: {DurationText(clearedAtUtc - firedAtUtc)}");
        if (!string.IsNullOrWhiteSpace(lastValue))
        {
            text.Append(CultureInfo.InvariantCulture, $"\nLast value: {lastValue}");
        }

        return text.ToString();
    }

    private static string ResolutionDetailText(AlertResolution resolution, DeliveredFire fire, DateTime clearedAtUtc) =>
        ResolutionDetailText(resolution, fire.FiredAtUtc, clearedAtUtc, fire.LastValue);

    private static string DurationText(TimeSpan lasted)
    {
        if (lasted < TimeSpan.Zero)
        {
            lasted = TimeSpan.Zero;
        }

        return lasted.TotalHours >= 1
            ? string.Create(CultureInfo.InvariantCulture, $"{(int)lasted.TotalHours}h {lasted.Minutes}m")
            : lasted.TotalMinutes >= 1
                ? string.Create(CultureInfo.InvariantCulture, $"{lasted.Minutes}m {lasted.Seconds}s")
                : string.Create(CultureInfo.InvariantCulture, $"{lasted.Seconds}s");
    }
}
