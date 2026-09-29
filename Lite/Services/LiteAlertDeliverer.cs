/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Threading;
using Hardcodet.Wpf.TaskbarNotification;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using AlertContextBuilders = PerformanceMonitor.Alerting.AlertContextBuilders;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Lite's <see cref="IAlertDeliverer"/> (Phase-5 forwarding): the pre-forwarding loop's per-fire
/// delivery block moved behind the engine seam, verbatim —
/// <list type="number">
/// <item>the TRAY TOAST first (skipped when muted, exactly the old <c>if (!isMuted)</c> gates):
/// title = the metric name, message = <c>"{server}: {ShortMessage}"</c> (the engine carries each
/// metric's exact old toast body), icon = Error for "Deadlocks Detected"/"Poison Wait" and Warning
/// for everything else, always the snoozable balloon — marshaled to the WPF dispatcher because the
/// tray icon is a UI element;</item>
/// <item>then the email/webhook/history send: "Blocking Detected"/"Deadlocks Detected" go through
/// the old <c>SendDetectedAlertAsync</c> semantics — the #1236 per-server delivery-mode override
/// (from <c>servers.json</c> via <see cref="ServerManager"/>) resolved against the global
/// <c>App.AlertDeliveryMode</c>, #1141 Per-event mode splitting the context into one send per
/// distinct incident (capped at <c>App.AlertPerEventMaxPerCycle</c> with a "+N more" trailer),
/// Summary mode (or no incidents) falling back to ONE combined send, and — like the old loop —
/// no numeric values on these two metrics; every other metric is the old direct
/// <see cref="EmailAlertService.TrySendAlertEmailAsync"/> call WITH the outcome's numerics.</item>
/// </list>
/// <see cref="EmailAlertService.TrySendAlertEmailAsync"/> keeps Lite's history-row semantics
/// unchanged: one combined <c>config_alert_log</c> row per send, written even when muted
/// (flagged muted, channels skipped). Throws only the caller's own cancellation (#4752) — a broken
/// toast or send must not abort the engine's sweep, but an engine that is stopping must not be told
/// its abandoned delivery failed, and the row for it is not written. Resolution ("Resolved/Cleared") toasts deliberately do NOT flow through here;
/// they ride the engine's resolution callback because Lite records no history row for them.
/// </summary>
public sealed class LiteAlertDeliverer : IAlertDeliverer
{
    /* Test seams — the production ctor wires the real tray/email/servers.json paths. */
    internal delegate void ShowToast(string title, string message, BalloonIcon icon, string serverName, string metricName);
    /* #4752: the send answers the delivery the email service recorded its history row with (null when it could not
       say), so the deliverer can pass on whether any channel received the alert. */
    internal delegate Task<AlertDelivery?> SendAlert(
        string metricName, string serverName, string currentValue, string thresholdValue,
        int serverId, AlertContext? context, double? numericCurrentValue, double? numericThresholdValue,
        bool muted, string? detailText, AlertNotificationMode deliveryMode, CancellationToken cancellationToken);

    private readonly ShowToast _showToast;
    private readonly SendAlert _sendAlert;
    private readonly Func<int, AlertNotificationMode?> _resolveServerDeliveryOverride;

    public LiteAlertDeliverer(
        EmailAlertService emailAlertService,
        MuteRuleService muteRuleService,
        ServerManager serverManager,
        Func<SystemTrayService?> trayService,
        Dispatcher dispatcher)
    {
        ArgumentNullException.ThrowIfNull(emailAlertService);
        ArgumentNullException.ThrowIfNull(muteRuleService);
        ArgumentNullException.ThrowIfNull(serverManager);
        ArgumentNullException.ThrowIfNull(trayService);
        ArgumentNullException.ThrowIfNull(dispatcher);

        _showToast = (title, message, icon, serverName, metricName) =>
        {
            var tray = trayService();
            if (tray == null)
            {
                return;
            }

            /* The tray icon is a WPF element; the engine's continuations normally resume on the
               dispatcher already (no ConfigureAwait(false) anywhere in the chain), but marshal
               defensively so a pool-thread caller can't crash the balloon. */
            if (dispatcher.CheckAccess())
            {
                tray.ShowSnoozableNotification(title, message, icon, serverName, metricName, muteRuleService);
            }
            else
            {
                dispatcher.Invoke(() =>
                    tray.ShowSnoozableNotification(title, message, icon, serverName, metricName, muteRuleService));
            }
        };

        _sendAlert = (metricName, serverName, currentValue, thresholdValue, serverId, context,
                numericCurrentValue, numericThresholdValue, muted, detailText, deliveryMode, cancellationToken) =>
            emailAlertService.TrySendAlertEmailAsync(
                metricName, serverName, currentValue, thresholdValue, serverId, context,
                numericCurrentValue: numericCurrentValue, numericThresholdValue: numericThresholdValue,
                muted: muted, detailText: detailText, deliveryMode: deliveryMode,
                cancellationToken: cancellationToken);

        /* #1236: the per-server delivery-mode override for serverId, or null to inherit the global
           App.AlertDeliveryMode. serverId is the deterministic hash of the storage name (the same mapping
           the alert engine keys on), so the lookup back to the ServerConnection is a scan — and it lives on
           ServerManager rather than here because the connection-edge and AG alerts, which bypass this
           deliverer entirely, have to reach the same answer. */
        _resolveServerDeliveryOverride = serverManager.ResolveAlertDeliveryModeOverride;
    }

    /// <summary>Test ctor: injected toast/send/override seams, no WPF or SMTP dependencies.</summary>
    internal LiteAlertDeliverer(
        ShowToast showToast,
        SendAlert sendAlert,
        Func<int, AlertNotificationMode?> resolveServerDeliveryOverride)
    {
        _showToast = showToast;
        _sendAlert = sendAlert;
        _resolveServerDeliveryOverride = resolveServerDeliveryOverride;
    }

    /// <summary>
    /// <see cref="DeliverAndReportAsync"/> with the answer discarded, so there is one delivery path and not a
    /// reporting one beside a silent one.
    /// </summary>
    public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default) =>
        DeliverAndReportAsync(outcome, cancellationToken);

    /// <summary>
    /// The delivery, reporting what the channels did (#4752). Written by hand rather than inherited, because
    /// the interface declares the member REQUIRED (CONTRIBUTING, Two-Store Parity: a defaulted member leaves the
    /// implementer you forgot quietly doing nothing).
    ///
    /// <para><b>What comes back.</b> On a combined send (the direct road every metric but the three that carry
    /// incidents takes, and Summary mode for those three) the exact <see cref="AlertDelivery"/>
    /// <c>EmailAlertService.TrySendAlertEmailAsync</c> wrote the history row with, so the engine can tell "every
    /// channel failed" (<see cref="FailedSendBackoff.EveryChannelFailed"/>) from a delivery and try the failed
    /// alert again sooner than its cooldown. With no email or webhook configured that delivery is the tray
    /// toast's, and a muted alert attempts nothing: neither is a failure, and neither is retried. On a Per-event
    /// split there are N sends and N rows, and the answer is the one <see cref="FailedSendBackoff.ReportForSplit"/>
    /// picks (#4822), as Darling's deliverer answers it: a delivery that reached a channel, else a failed one so
    /// the engine tries the alert again, else <c>null</c> when no send was attempted. <c>null</c> is
    /// "unreported", never "failed": the engine reads it as delivered, and it is also the answer when the send
    /// threw anything but the caller's cancel, or could not say what it did.</para>
    ///
    /// <para>The caller's own cancel leaves as the <see cref="OperationCanceledException"/> it is, before any
    /// history row is written (see <see cref="EmailAlertService.TrySendAlertEmailAsync"/>).</para>
    /// </summary>
    public async Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
    {
        if (outcome is null)
        {
            throw new ArgumentNullException(nameof(outcome));
        }

        try
        {
            var serverId = int.Parse(outcome.ServerKey, CultureInfo.InvariantCulture);

            /* Toast BEFORE the send, matching the old loop's order in every branch. */
            if (!outcome.Muted)
            {
                _showToast(
                    outcome.MetricName,
                    $"{outcome.ServerName}: {outcome.ShortMessage}",
                    ToastIconFor(outcome.MetricName),
                    outcome.ServerName,
                    outcome.MetricName);
            }

            /* #1236: a per-server override (Manage Servers -> Edit) wins over the global delivery mode;
               null inherits App.AlertDeliveryMode. Resolved HERE, for every metric, because #3430's
               per-metric repeat ceiling in the shared send core turns on it — and the fan-out that ceiling
               bounds is at its worst on the metrics that never had an incident to split (one CPU threshold
               breached on fifteen servers is fifteen metric-level cooldown keys). The #1141 split below
               still only applies to the three metrics that carry incidents. */
            var deliveryMode = AlertDeliveryModeResolver.Resolve(
                _resolveServerDeliveryOverride(serverId), App.AlertDeliveryMode);

            /* Only blocking/deadlocks ever routed through SendDetectedAlertAsync's #1141 split in
               the old loop; every other metric was a direct single send with its numerics. */
            /* #1839's "Blocking Wait Time" joins them: it carries the same blocked-process incident
               context, so it splits per-event the same way. */
            if (outcome.MetricName is "Blocking Detected" or "Blocking Wait Time" or "Deadlocks Detected")
            {
                return await SendDetectedAlertAsync(
                    outcome.MetricName, outcome.ServerName, outcome.CurrentValue, outcome.ThresholdValue,
                    serverId, outcome.Context, outcome.NumericCurrentValue, outcome.NumericThresholdValue,
                    outcome.Muted, outcome.DetailText, deliveryMode, cancellationToken);
            }

            return await _sendAlert(
                outcome.MetricName, outcome.ServerName, outcome.CurrentValue, outcome.ThresholdValue,
                serverId, outcome.Context, outcome.NumericCurrentValue, outcome.NumericThresholdValue,
                outcome.Muted, outcome.DetailText, deliveryMode, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* #4752: the token now reaches the webhook posts, so a cancel can land in the middle of one, and
               EmailAlertService lets it through before it writes a history row. It leaves as the cancellation
               it is: the engine that asked to stop is not told its abandoned delivery failed. */
            throw;
        }
        catch (Exception ex)
        {
            AppLogger.Error("Alerts", $"Alert delivery failed for {outcome.MetricName} on {outcome.ServerName}: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// The old loop's toast icon table: Error for the two data-corruption-adjacent conditions
    /// (deadlocks, poison waits), Warning for everything else.
    /// </summary>
    internal static BalloonIcon ToastIconFor(string metricName) =>
        metricName is "Deadlocks Detected" or "Poison Wait" ? BalloonIcon.Error : BalloonIcon.Warning;

    /* #1141: in Per-event mode, deliver one notification per distinct incident (capped at
       AlertPerEventMaxPerCycle, with a trailing "+N more" that still carries the remaining
       fingerprints) instead of one batched summary card. Falls back to the single summary send in
       Summary mode or when there are no incidents. The engine's edge-triggered gating already
       decided whether to fire; this only shapes delivery. Moved verbatim from the pre-forwarding
       MainWindow.SendDetectedAlertAsync. */
    private async Task<AlertDelivery?> SendDetectedAlertAsync(
        string metricName, string serverName, string summaryCurrentValue, string thresholdValue,
        int serverId, AlertContext? context, double? numericCurrentValue, double? numericThresholdValue,
        bool isMuted, string? summaryDetailText, AlertNotificationMode deliveryMode,
        CancellationToken cancellationToken)
    {
        if (deliveryMode == AlertNotificationMode.PerEvent && context?.Incidents is { Count: > 0 })
        {
            var deliveries = new List<AlertDelivery?>();
            foreach (var msg in PerEventNotification.Split(context, App.AlertPerEventMaxPerCycle))
            {
                /* Each per-event row stores its OWN numeric (#1830): the overflow message's
                   "+N more incident(s)" text is unparseable, and the history store's text fallback
                   silently recorded 0 for it. The threshold is the outcome's, unchanged. */
                deliveries.Add(await _sendAlert(
                    metricName, serverName, msg.CurrentValue, thresholdValue, serverId,
                    msg.Context, msg.NumericValue, numericThresholdValue, isMuted,
                    AlertContextBuilders.ContextToDetailText(msg.Context), deliveryMode, cancellationToken));
            }

            /* N sends, N rows: the split reports the one delivery the engine acts on (#4822). */
            return FailedSendBackoff.ReportForSplit(deliveries);
        }

        return await _sendAlert(
            metricName, serverName, summaryCurrentValue, thresholdValue, serverId,
            context, numericCurrentValue, numericThresholdValue, isMuted, summaryDetailText, deliveryMode,
            cancellationToken);
    }
}
