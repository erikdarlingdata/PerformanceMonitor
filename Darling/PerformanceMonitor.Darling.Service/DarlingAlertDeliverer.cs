/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// Never throws — a dead SMTP server or Postgres store must not abort the engine's sweep.
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
public sealed class DarlingAlertDeliverer : IAlertDeliverer
{
    private static readonly AlertBranding s_branding = new(
        "Performance Monitor Darling",
        /* Headless: no snooze-hint footer (Lite's points at its Settings window). */
        null);

    /// <summary>The branding this service feeds the shared email/template/webhook renderers.</summary>
    public static AlertBranding Branding => s_branding;

    private readonly IAlertHistoryStore _historyStore;
    private readonly EmailSendCore _core;
    private readonly ILogger _logger;
    private readonly DarlingAlertSettings _settings;
    private readonly Func<string, AlertNotificationMode?> _resolveServerOverride;

    /// <param name="resolveServerOverride">
    /// Maps a fired alert's <see cref="AlertOutcome.ServerKey"/> to that server's
    /// <see cref="MonitoredServer.AlertDeliveryModeOverride"/> (or null to inherit the global mode). Optional —
    /// null (the default) means every server inherits the global <see cref="DarlingAlertSettings.DeliveryMode"/>,
    /// which is also correct for incident-free self-alerts. The service passes a resolver over its live server set.
    /// </param>
    public DarlingAlertDeliverer(
        DarlingAlertSettings settings,
        IAlertHistoryStore historyStore,
        WebhookAlertService webhookAlertService,
        ILogger logger,
        Func<string, AlertNotificationMode?>? resolveServerOverride = null)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _historyStore = historyStore ?? throw new ArgumentNullException(nameof(historyStore));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _resolveServerOverride = resolveServerOverride ?? (_ => null);
        _core = new EmailSendCore(settings, historyStore, webhookAlertService, s_branding, logger);
    }

    public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default) =>
        DeliverAndReportAsync(outcome, cancellationToken);

    /// <summary>
    /// The delivery, reporting its disposition (#3580). Every send goes through here —
    /// <see cref="DeliverAsync"/> is this with the answer discarded — so there is one delivery path and
    /// not a reporting one beside a silent one.
    ///
    /// <para><b>What comes back.</b> On the combined send (Summary mode, or any alert without incidents,
    /// which is every self-alert) the exact <see cref="AlertDelivery"/> the history row was written with.
    /// On a Per-event split there are N sends and N rows and no single disposition describes them, so this
    /// returns <c>null</c> — "unreported" — rather than electing one; the callers that read the answer (the
    /// three daily documents) carry structured <see cref="AlertContext.Details"/> since #3834 but no
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

        try
        {
            /* #1236/#1141: a per-server override wins over the global mode; Per-event splits an
               incident-carrying alert into one send+row per distinct incident (capped, "+N more"). */
            var mode = AlertDeliveryModeResolver.Resolve(_resolveServerOverride(outcome.ServerKey), _settings.DeliveryMode);
            if (mode == AlertNotificationMode.PerEvent && outcome.Context?.Incidents is { Count: > 0 })
            {
                foreach (var message in PerEventNotification.Split(outcome.Context, _settings.PerEventMax))
                {
                    /* Per-incident card: msg.CurrentValue is the incident's occurrence count, and
                       msg.NumericValue carries it as a number for the history row (#1830 — the
                       overflow message's "+N more" text is unparseable, so the store's text fallback
                       silently recorded 0). Threshold is the outcome's, unchanged. Matches Lite's
                       per-event sends; detail text rebuilt from the split context. */
                    await SendAndRecordAsync(
                        outcome, message.CurrentValue, message.Context,
                        AlertContextBuilders.ContextToDetailText(message.Context),
                        numericCurrentValue: message.NumericValue, numericThresholdValue: outcome.NumericThresholdValue,
                        deliveryMode: mode);
                }

                return null;
            }

            /* Summary mode, or an alert with no incidents (CPU/low-disk/jobs): one combined send+row, unchanged. */
            return await SendAndRecordAsync(
                outcome, outcome.CurrentValue, outcome.Context, outcome.DetailText,
                outcome.NumericCurrentValue, outcome.NumericThresholdValue,
                deliveryMode: mode);
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
    /// <returns>The disposition the history row was written with — the same value, so what the caller is
    /// told and what the operator later reads in the alert log cannot disagree (#3580).</returns>
    private async Task<AlertDelivery> SendAndRecordAsync(
        AlertOutcome outcome, string currentValue, AlertContext? context, string? detailText,
        double? numericCurrentValue, double? numericThresholdValue, AlertNotificationMode deliveryMode)
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

        /* Lite's EmailAlertService.cs:65-66 — muted alerts skip both channels but still record below.
           outcome.DisplayName (a custom rule's human name, or null for built-ins) renders in the
           subject/body/webhook titles in place of the "Custom:<id>" metric name; the metric name stays the
           cooldown/history/mute key. detailText is the same prose this method records as the history row's
           detail_text (#3297) — the channels get the alert's remedy, not only its number. */
        var result = await _core.TrySendAsync(
            outcome.MetricName, outcome.ServerName, currentValue, outcome.ThresholdValue,
            outcome.ServerKey, context, attemptChannels: !outcome.Muted, detailText: detailText,
            displayName: outcome.DisplayName, deliveryMode: deliveryMode);

        /* trayChannelPresent: false — this is the HEADLESS service. It has no tray icon and no toast
           code, so the taxonomy's "tray" fallback (which is Lite's, and truthful there) would assert a UI
           event that cannot occur here. Without a channel configured a fired alert is reported as
           "unconfigured", which is the state an operator can act on. */
        var delivery = AlertDelivery.FromFanout(result, outcome.Muted, trayChannelPresent: false);

        /* #3598 (design point 3): the ledger says WHERE the post went. The send core reports the routing
           decision the fan-out actually used — resolved once, after the cooldown, so it is the decision and
           not a re-derivation that could disagree with it if the route list reloaded in between — and it
           rides the row's context_json as the trailing Route member, beside #3539 A8e's Severity, with no
           schema change. Null when no channel reached resolution (throttled, folded, muted, unconfigured):
           a row that consulted no destination records none. The context is created here if the alert had
           none (every self-alert fires with Context: null), exactly as #2090 does for the severity above;
           the Viewer's detail window falls back to detail_text for a context with no detail items, so an
           empty-Details context carrying only provenance costs the operator nothing. */
        if (result.Route is { } route)
        {
            context ??= new AlertContext();
            context.Route = route.ToDto();
        }

        /* Always log the alert, regardless of channel status (EmailAlertService.cs:82-94). */
        string? contextJson = context is not null ? AlertContextSerializer.Serialize(context) : null;
        await _historyStore.RecordAlertAsync(new AlertHistoryRecord(
            outcome.ServerKey, outcome.ServerName, outcome.MetricName,
            currentValue, outcome.ThresholdValue,
            numericCurrentValue, numericThresholdValue,
            delivery,
            outcome.Muted, detailText, contextJson));

        return delivery;
    }
}
