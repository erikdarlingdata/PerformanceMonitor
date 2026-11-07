/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8246, Lite's half: a send that throws is reported as every channel failed, so the engine tries the alert again, and
/// the Per-event split applies to any alert that carries incidents, by the one rule Darling's deliverer reads. The
/// deliverer answered null for a throw, which the engine reads as delivered: the alert was stamped as sent and stayed
/// silent for the whole cooldown, with one log line. The split was an allow-list of three metric names here and "any
/// incidents" in Darling, so the first other family to gain incidents would have paged per incident in one app and as
/// one card in the other.
/// </summary>
public partial class LiteAlertForwardingTests
{
    private static AlertOutcome CpuOutcome() => new(
        Key, Name, "High CPU", "97%", "90%", Context: null, DetailText: "CPU is high",
        NumericCurrentValue: 97, NumericThresholdValue: 90, Muted: false, Severity: null, ShortMessage: "CPU 97%");

    [Fact]
    public async Task ASendThatThrows_IsReportedAsEveryChannelFailed_NotAsDelivered()
    {
        var (deliverer, toasts, sends) = BuildReportingDeliverer(() => throw new InvalidOperationException("the template could not render"));

        var delivery = await deliverer.DeliverAndReportAsync(CpuOutcome());

        Assert.NotNull(delivery);
        Assert.False(delivery!.Sent);
        Assert.Equal(AlertDelivery.ChannelFailed, delivery.Channel);
        Assert.Contains("could not render", delivery.SendError, StringComparison.Ordinal);
        Assert.True(FailedSendBackoff.EveryChannelFailed(delivery));
        Assert.Single(sends);
        Assert.Single(toasts);
    }

    [Fact]
    public async Task Cpu_ASendThatThrows_IsTriedAgainAfterAMinute_LikeAChannelThatFailed()
    {
        DisableAllChecks();
        App.AlertCpuEnabled = true;
        var h = new Harness();
        var throwing = true;
        var (deliverer, _, sends) = BuildReportingDeliverer(
            () => throwing ? throw new InvalidOperationException("the template could not render") : DeliveredByWebhook());
        var engine = h.Build(deliverer);

        var at = await DriveCpuAsync(engine, sqlCpu: 70, totalCpu: 95, samples: AlertEngine.SqlCpuBreachSamples, from: Harness.SampleBase);
        Assert.Single(sends);

        async Task<int> SweepAfterAsync(TimeSpan wait)
        {
            h.Now = h.Now.Add(wait);
            at = await DriveCpuAsync(engine, sqlCpu: 70, totalCpu: 95, samples: 1, from: at);
            return sends.Count;
        }

        /* First failure: the retry waits a minute, not the cooldown the stamp would have held it for. */
        Assert.Equal(1, await SweepAfterAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal(2, await SweepAfterAsync(TimeSpan.FromSeconds(31)));

        /* Second failure in a row: two minutes. */
        Assert.Equal(2, await SweepAfterAsync(TimeSpan.FromSeconds(119)));

        /* The send works again: the third fire delivers, and that ends the streak. */
        throwing = false;
        Assert.Equal(3, await SweepAfterAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task PerEventMode_SplitsAnyAlertThatCarriesIncidents_NotOnlyBlockingAndDeadlocks()
    {
        var (deliverer, _, sends) = BuildReportingDeliverer(DeliveredByWebhook, serverOverride: AlertNotificationMode.PerEvent);
        var outcome = new AlertOutcome(
            Key, Name, "Long Running Queries", "2", "1", Context: TwoBlockedIncidents(), DetailText: "two queries",
            NumericCurrentValue: 2, NumericThresholdValue: 1, Muted: false, Severity: null, ShortMessage: "2 queries");

        var delivery = await deliverer.DeliverAndReportAsync(outcome);

        Assert.NotNull(delivery);
        Assert.True(delivery!.Sent);
        Assert.Equal(2, sends.Count);
        Assert.All(sends, s => Assert.Equal(AlertNotificationMode.PerEvent, s.DeliveryMode));
        Assert.All(sends, s => Assert.Single(s.Context!.Incidents!));
        Assert.Equal(new[] { "a", "b" }, sends.Select(s => s.Context!.Incidents![0].DedupKey).OrderBy(k => k, StringComparer.Ordinal));
    }

    [Fact]
    public async Task SummaryMode_SendsOneCard_ForAnAlertThatCarriesIncidents()
    {
        var (deliverer, _, sends) = BuildReportingDeliverer(DeliveredByWebhook, serverOverride: AlertNotificationMode.Summary);
        var outcome = new AlertOutcome(
            Key, Name, "Long Running Queries", "2", "1", Context: TwoBlockedIncidents(), DetailText: "two queries",
            NumericCurrentValue: 2, NumericThresholdValue: 1, Muted: false, Severity: null, ShortMessage: "2 queries");

        await deliverer.DeliverAndReportAsync(outcome);

        var send = Assert.Single(sends);
        Assert.Equal(2, send.Context!.Incidents!.Count);
        Assert.Equal(2, send.NumericCurrentValue);
    }
}
