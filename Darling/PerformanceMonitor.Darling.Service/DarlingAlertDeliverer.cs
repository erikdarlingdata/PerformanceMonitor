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

    public async Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
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
                        numericCurrentValue: message.NumericValue, numericThresholdValue: outcome.NumericThresholdValue);
                }

                return;
            }

            /* Summary mode, or an alert with no incidents (CPU/low-disk/jobs): one combined send+row, unchanged. */
            await SendAndRecordAsync(
                outcome, outcome.CurrentValue, outcome.Context, outcome.DetailText,
                outcome.NumericCurrentValue, outcome.NumericThresholdValue);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("Alert delivery failed for {Metric} on {Server}: {Message}",
                outcome.MetricName, outcome.ServerName, ex.Message);
        }
    }

    /// <summary>
    /// Sends one alert message (email + webhook fan-out via the shared core) and writes exactly one combined
    /// <c>config_alert_log</c> row for it — Lite's record-and-send cadence. Called once for a Summary alert and
    /// once per split incident in Per-event mode, so every send is paired with its own history row (the
    /// structured context persists as JSON alongside the flat detail_text). Muted alerts skip both channels but
    /// still record (flagged muted).
    /// </summary>
    private async Task SendAndRecordAsync(
        AlertOutcome outcome, string currentValue, AlertContext? context, string? detailText,
        double? numericCurrentValue, double? numericThresholdValue)
    {
        /* #2090: the fire site's severity rode AlertOutcome.Severity but the channel builders read
           only Context.SeverityOverride — so every self-alert (fired with Context: null) rendered
           INFO-blue in Teams/Slack/PagerDuty/webhooks while its log line said Critical. Fold the
           outcome's severity into the context here, once, upstream of every channel; ??= so an
           explicit override set by a context builder still wins. The context also serializes into
           alert history, so replays keep the severity too. */
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
            displayName: outcome.DisplayName);

        /* trayChannelPresent: false — this is the HEADLESS service. It has no tray icon and no toast
           code, so the taxonomy's "tray" fallback (which is Lite's, and truthful there) would assert a UI
           event that cannot occur here. Without a channel configured a fired alert is reported as
           "unconfigured", which is the state an operator can act on. */
        var delivery = AlertDelivery.FromFanout(result, outcome.Muted, trayChannelPresent: false);

        /* Always log the alert, regardless of channel status (EmailAlertService.cs:82-94). */
        string? contextJson = context is not null ? AlertContextSerializer.Serialize(context) : null;
        await _historyStore.RecordAlertAsync(new AlertHistoryRecord(
            outcome.ServerKey, outcome.ServerName, outcome.MetricName,
            currentValue, outcome.ThresholdValue,
            numericCurrentValue, numericThresholdValue,
            delivery,
            outcome.Muted, detailText, contextJson));
    }
}
