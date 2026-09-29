/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4752, Lite's half: an alert that no email or webhook channel received is reported as not sent, so the shared
/// engine tries it again. The engine's own retry (a minute, then two, capped at the cooldown) is covered by the
/// Darling family tests; what is pinned here is that Lite's REAL deliverer hands the engine the answer to act on.
/// Before, <see cref="LiteAlertDeliverer.DeliverAndReportAsync"/> returned null, which the engine reads as
/// "delivered", so a Lite alert whose every channel failed was stamped as sent and stayed silent for the whole
/// cooldown. The deliveries below are the shapes <c>EmailAlertService.TrySendAlertEmailAsync</c> produces on the
/// engine path, where <c>trayChannelPresent</c> is true because the deliverer shows the toast first.
/// </summary>
public partial class LiteAlertForwardingTests
{
    private static EmailFanoutResult Fanout(
        AlertChannelOutcome email, string? emailError, AlertChannelOutcome webhook, string? webhookError,
        bool anyChannelConfigured = true) =>
        new(EmailOutcome: email, SendError: emailError,
            WebhookOutcome: webhook, WebhookSendError: webhookError, AnyChannelConfigured: anyChannelConfigured);

    /// <summary>Email failed with its own error and nothing else went out.</summary>
    private static AlertDelivery FailedByEmail() => AlertDelivery.FromFanout(
        Fanout(AlertChannelOutcome.Failed, "SMTP: 535 authentication failed", AlertChannelOutcome.NotAttempted, null),
        muted: false, trayChannelPresent: true);

    /// <summary>A webhook-only fan-out whose every post failed (an HTTP 429, say).</summary>
    private static AlertDelivery FailedByWebhook() => AlertDelivery.FromFanout(
        Fanout(AlertChannelOutcome.NotAttempted, null, AlertChannelOutcome.Failed, "Slack: 429 Too Many Requests"),
        muted: false, trayChannelPresent: true);

    /// <summary>The webhook delivered.</summary>
    private static AlertDelivery DeliveredByWebhook() => AlertDelivery.FromFanout(
        Fanout(AlertChannelOutcome.NotAttempted, null, AlertChannelOutcome.Delivered, null),
        muted: false, trayChannelPresent: true);

    /// <summary>A PARTIAL failure: the email failed, the webhook delivered.</summary>
    private static AlertDelivery FailedByEmailButDeliveredByWebhook() => AlertDelivery.FromFanout(
        Fanout(AlertChannelOutcome.Failed, "SMTP: 535 authentication failed", AlertChannelOutcome.Delivered, null),
        muted: false, trayChannelPresent: true);

    /// <summary>No email and no webhook is configured: the tray toast is the whole delivery.</summary>
    private static AlertDelivery NoExternalChannelConfigured() => AlertDelivery.FromFanout(
        Fanout(AlertChannelOutcome.NotAttempted, null, AlertChannelOutcome.NotAttempted, null, anyChannelConfigured: false),
        muted: false, trayChannelPresent: true);

    /// <summary>
    /// The real <see cref="LiteAlertDeliverer"/> on its test seams, its send answering whatever
    /// <paramref name="answer"/> returns at the time of the call, and recording every toast and send.
    /// </summary>
    private static (LiteAlertDeliverer Deliverer, List<ToastCall> Toasts, List<SendCall> Sends) BuildReportingDeliverer(
        Func<AlertDelivery?> answer, AlertNotificationMode? serverOverride = null)
    {
        var toasts = new List<ToastCall>();
        var sends = new List<SendCall>();
        var deliverer = new LiteAlertDeliverer(
            (title, message, icon, serverName, metricName) => toasts.Add(new ToastCall(title, message, icon, serverName, metricName)),
            (metricName, serverName, currentValue, thresholdValue, serverId, context, numCur, numThr, muted, detailText, deliveryMode, _) =>
            {
                sends.Add(new SendCall(
                    metricName, serverName, currentValue, thresholdValue, serverId, context, numCur, numThr,
                    muted, detailText, deliveryMode));
                return Task.FromResult(answer());
            },
            _ => serverOverride);
        return (deliverer, toasts, sends);
    }

    private static AlertContext TwoBlockedIncidents() => new()
    {
        Incidents = new List<AlertIncident> { new("a", new[] { "dbo.Users" }), new("b", new[] { "dbo.Posts" }) }
    };

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Cpu_EveryConfiguredChannelFailed_LiteReportsIt_AndTheEngineTriesAgain(bool webhookOnly)
    {
        DisableAllChecks();
        App.AlertCpuEnabled = true;
        var h = new Harness();
        AlertDelivery? answer = webhookOnly ? FailedByWebhook() : FailedByEmail();
        var (deliverer, toasts, sends) = BuildReportingDeliverer(() => answer);
        var engine = h.Build(deliverer);

        var at = await DriveCpuAsync(engine, sqlCpu: 70, totalCpu: 95, samples: AlertEngine.CpuBreachSamples, from: Harness.SampleBase);
        Assert.Single(sends);
        Assert.Single(toasts);

        async Task<int> SweepAfterAsync(TimeSpan wait)
        {
            h.Now = h.Now.Add(wait);
            at = await DriveCpuAsync(engine, sqlCpu: 70, totalCpu: 95, samples: 1, from: at);
            return sends.Count;
        }

        /* First failure: the retry waits a minute, not the five-minute cooldown the stamp would have held it for. */
        Assert.Equal(1, await SweepAfterAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal(2, await SweepAfterAsync(TimeSpan.FromSeconds(31)));

        /* A retry shows the tray notice again: Lite keeps no memory of what it already toasted, and the engine's
           backoff is what bounds how often. */
        Assert.Equal(2, toasts.Count);

        /* Second failure in a row: two minutes. */
        Assert.Equal(2, await SweepAfterAsync(TimeSpan.FromSeconds(119)));

        /* The channel works again. The third fire delivers, and that ends the streak. */
        answer = DeliveredByWebhook();
        Assert.Equal(3, await SweepAfterAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal(3, toasts.Count);

        /* A delivered fire waits the whole cooldown, as it always did. */
        Assert.Equal(3, await SweepAfterAsync(TimeSpan.FromSeconds(61)));
        Assert.Equal(3, await SweepAfterAsync(TimeSpan.FromSeconds(238)));
        Assert.Equal(4, await SweepAfterAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task Cpu_OneChannelDeliversAndAnotherFails_IsStampedAsDelivered_AndNotSentAgain()
    {
        /* A partial failure has Sent true: a retry would send the alert a second time down the channel that
           worked, so the alert waits out its cooldown like any delivered one. */
        DisableAllChecks();
        App.AlertCpuEnabled = true;
        var h = new Harness();
        var (deliverer, toasts, sends) = BuildReportingDeliverer(FailedByEmailButDeliveredByWebhook);
        var engine = h.Build(deliverer);

        var at = await DriveCpuAsync(engine, sqlCpu: 70, totalCpu: 95, samples: AlertEngine.CpuBreachSamples, from: Harness.SampleBase);
        Assert.Single(sends);

        async Task<int> SweepAfterAsync(TimeSpan wait)
        {
            h.Now = h.Now.Add(wait);
            at = await DriveCpuAsync(engine, sqlCpu: 70, totalCpu: 95, samples: 1, from: at);
            return sends.Count;
        }

        Assert.Equal(1, await SweepAfterAsync(TimeSpan.FromSeconds(61)));
        Assert.Equal(1, await SweepAfterAsync(TimeSpan.FromSeconds(238)));
        Assert.Equal(2, await SweepAfterAsync(TimeSpan.FromSeconds(1)));
        Assert.Equal(2, toasts.Count);
    }

    [Fact]
    public async Task Cpu_NoExternalChannelConfigured_TheToastIsTheDelivery_AndTheAlertIsStamped()
    {
        /* With no email and no webhook configured there is nothing to fail: a delivery with no attempts is not
           "every channel failed", so the alert is stamped and waits its cooldown, exactly as it did before. */
        DisableAllChecks();
        App.AlertCpuEnabled = true;
        var h = new Harness();
        var (deliverer, toasts, sends) = BuildReportingDeliverer(NoExternalChannelConfigured);
        var engine = h.Build(deliverer);

        var at = await DriveCpuAsync(engine, sqlCpu: 70, totalCpu: 95, samples: AlertEngine.CpuBreachSamples, from: Harness.SampleBase);
        Assert.Single(sends);
        Assert.Single(toasts);

        async Task<int> SweepAfterAsync(TimeSpan wait)
        {
            h.Now = h.Now.Add(wait);
            at = await DriveCpuAsync(engine, sqlCpu: 70, totalCpu: 95, samples: 1, from: at);
            return sends.Count;
        }

        Assert.Equal(1, await SweepAfterAsync(TimeSpan.FromSeconds(61)));
        Assert.Equal(1, await SweepAfterAsync(TimeSpan.FromSeconds(238)));
        Assert.Equal(2, await SweepAfterAsync(TimeSpan.FromSeconds(1)));
        Assert.Equal(2, toasts.Count);
    }

    [Fact]
    public async Task Deliverer_DeliverAndReportAsync_ReturnsTheDeliveryTheSendAnswered_OnTheDirectAndSummaryRoads()
    {
        var failed = FailedByEmail();
        var (deliverer, _, sends) = BuildReportingDeliverer(() => failed);

        /* The direct road: every metric but the three that carry incidents. */
        Assert.Same(failed, await deliverer.DeliverAndReportAsync(Outcome("High CPU")));

        /* The Summary road: an incident-carrying alert sends once, combined. */
        App.AlertDeliveryMode = AlertNotificationMode.Summary;
        Assert.Same(failed, await deliverer.DeliverAndReportAsync(Outcome("Blocking Detected", context: TwoBlockedIncidents())));

        Assert.Equal(2, sends.Count);
        Assert.True(FailedSendBackoff.EveryChannelFailed(failed));
    }

    [Fact]
    public async Task Deliverer_DeliverAndReportAsync_ReportsNothing_ForAPerEventSplit()
    {
        /* N sends, N rows: no single delivery describes them, so the answer is "unreported" (null), which the
           engine reads as delivered. Darling's deliverer answers the same way. */
        App.AlertDeliveryMode = AlertNotificationMode.PerEvent;
        var (deliverer, _, sends) = BuildReportingDeliverer(FailedByEmail);

        var delivery = await deliverer.DeliverAndReportAsync(Outcome("Blocking Detected", context: TwoBlockedIncidents()));

        Assert.Null(delivery);
        Assert.True(sends.Count >= 2, "the per-event road sends once per incident");
    }

    [Fact]
    public async Task Deliverer_ACallersCancel_LeavesAsTheCancellation_NotAsAFailedSend()
    {
        using var cts = new CancellationTokenSource();
        var deliverer = new LiteAlertDeliverer(
            (_, _, _, _, _) => { },
            (_, _, _, _, _, _, _, _, _, _, _, token) =>
            {
                cts.Cancel();
                token.ThrowIfCancellationRequested();
                return Task.FromResult<AlertDelivery?>(FailedByEmail());
            },
            _ => null);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => deliverer.DeliverAndReportAsync(Outcome("High CPU"), cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => deliverer.DeliverAsync(Outcome("High CPU"), cts.Token));
    }

    [Fact]
    public async Task Deliverer_ASendThatFailsOnItsOwn_IsUnreported_NotAStop()
    {
        /* A cancel the caller did not ask for (an HTTP client's own timeout surfacing as a cancellation) and any
           other exception stay what they were: logged, swallowed, and answered as "unreported". */
        var deliverer = new LiteAlertDeliverer(
            (_, _, _, _, _) => { },
            (_, _, _, _, _, _, _, _, _, _, _, _) => throw new OperationCanceledException("the send's own timeout"),
            _ => null);
        var broken = new LiteAlertDeliverer(
            (_, _, _, _, _) => { },
            (_, _, _, _, _, _, _, _, _, _, _, _) => throw new InvalidOperationException("boom"),
            _ => null);

        Assert.Null(await deliverer.DeliverAndReportAsync(Outcome("High CPU")));
        Assert.Null(await broken.DeliverAndReportAsync(Outcome("High CPU")));
    }
}
