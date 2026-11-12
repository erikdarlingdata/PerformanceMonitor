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
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8246, Darling's half: a deliverer that throws outside its channels reports every channel failed, not delivered. The
/// deliverer answered null for a throw, which <see cref="FailedSendBackoff.EveryChannelFailed"/> reads as not failed: the
/// engine cleared the family's failure streak and left its cooldown stamp at now, so the alert stayed silent for the whole
/// cooldown with one log line, and the external alert drain closed the row as not to be sent again. The throw here is the
/// per-server delivery-mode lookup, which runs inside the deliverer before any channel is reached.
/// </summary>
public sealed class DelivererThrowReportsFailureTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ADelivererThatThrows_ReportsEveryChannelFailed_WithTheReason()
    {
        var settings = new DarlingAlertSettings(new DarlingConfig());
        var history = new CapturingHistoryStore();
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var deliverer = new DarlingAlertDeliverer(
            settings, history, webhooks, NullLogger.Instance,
            resolveServerOverride: _ => throw new InvalidOperationException("the delivery-mode override could not be read"));
        var outcome = new AlertOutcome(
            "srv-1", "SQL01", "High CPU", "97%", "90%", Context: null, DetailText: "CPU is high",
            NumericCurrentValue: 97, NumericThresholdValue: 90, Muted: false, Severity: null, ShortMessage: "CPU 97%");

        var delivery = await deliverer.DeliverAndReportAsync(outcome, Ct);

        Assert.NotNull(delivery);
        Assert.False(delivery!.Sent);
        Assert.Equal(AlertDelivery.ChannelFailed, delivery.Channel);
        Assert.Contains("could not be read", delivery.SendError, StringComparison.Ordinal);
        Assert.True(FailedSendBackoff.EveryChannelFailed(delivery));
        Assert.Empty(history.Records);
    }
}
