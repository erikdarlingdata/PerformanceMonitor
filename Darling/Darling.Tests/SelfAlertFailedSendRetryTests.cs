/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4795: a Darling self-alert whose every channel failed is tried again a minute later while its condition
/// still holds, not after the whole cooldown. The self-alerts stamp their cooldown BEFORE they send and used to
/// throw away what the send reported, so a failed send looked like a delivered one. The arms now keep the
/// delivery and hand it to <see cref="DarlingWorker.AfterPgFireCore"/> (through the evaluator's own step), the
/// same step the PostgreSQL families use (#4795) and the SQL Server engine's twin of it (#4752).
///
/// <para>Driven through the real arms with a scripted deliverer and a controllable clock: Collection Stopped,
/// Capture Down, Agent Not Running and Collector Cost Regression. The cost regression is the arm with a second
/// "already reported" marker (the metric time it last reported), which the retry must not find advanced.</para>
/// </summary>
public sealed class SelfAlertFailedSendRetryTests
{
    private const int ServerId = 424242;
    private const string Name = "SELF-ALERT-SRV";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly TimeSpan Cooldown = TimeSpan.FromMinutes(5);
    private static readonly DateTime Start = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime FirstDataPoint = new(2026, 7, 1, 11, 0, 0, DateTimeKind.Utc);

    public enum Arm
    {
        CollectionStopped,
        CaptureDown,
        AgentDown,
        CostRegression,
    }

    public static TheoryData<Arm> Arms => new()
    {
        Arm.CollectionStopped, Arm.CaptureDown, Arm.AgentDown, Arm.CostRegression,
    };

    /* The delivery shapes the channels really produce, as PgAlertFailedSendRetryTests and AlertEngineTests build
       them: the deliverer answers these same values for a self-alert. */
    private static AlertDelivery FailedByWebhook() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Failed, WebhookSendError: "Slack: 429 Too Many Requests", AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    private static AlertDelivery DeliveredByWebhook() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Delivered, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    /* A PARTIAL failure: the email failed, the webhook delivered. Sent is true, and a retry would send the alert
       a second time down the channel that worked. */
    private static AlertDelivery FailedByEmailButDeliveredByWebhook() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.Failed, SendError: "SMTP: 535 authentication failed",
            WebhookOutcome: AlertChannelOutcome.Delivered, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    /// <summary>Answers every send with whatever <see cref="Answer"/> holds, and records what it was asked to send.</summary>
    private sealed class ScriptedDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        public AlertDelivery? Answer { get; set; }

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }

        public Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.FromResult(Answer);
        }
    }

    /// <summary>One evaluator, a scripted deliverer and a clock the test moves.</summary>
    private sealed class Rig
    {
        private DateTime _dataPoint = FirstDataPoint;

        public Rig()
        {
            Evaluator = new DarlingSelfAlertEvaluator(
                Settings, Deliverer, new DarlingSelfAlertTests.FakeHistoryStore(), _ => false,
                logger: new DarlingSelfAlertTests.CapturingLogger(), utcNow: () => Now);
        }

        public DarlingSelfAlertTests.FakeSettings Settings { get; } = new() { CooldownMinutes = 5 };

        public ScriptedDeliverer Deliverer { get; } = new();

        public DateTime Now { get; set; } = Start;

        public DarlingSelfAlertEvaluator Evaluator { get; }

        /// <summary>The cost reader stamps each regression with the newest metric row it folded in. A new row moves
        /// this forward; a retry against the same rows does not.</summary>
        public void NewCostDataPoint() => _dataPoint = _dataPoint.AddHours(1);

        public int Fires => Deliverer.Outcomes.Count;

        /// <summary>One sweep's evaluation of the arm, with its condition holding.</summary>
        public Task SweepAsync(Arm arm) => SweepAsync(arm, _dataPoint);

        public Task SweepAsync(Arm arm, DateTime costDataPoint) => arm switch
        {
            Arm.CollectionStopped => Evaluator.ApplyCollectionStoppedAsync(ServerId, Name, stopped: true, "no recent collection", Ct),
            Arm.CaptureDown => Evaluator.ApplyCaptureDownAsync(ServerId, Name, new[] { "Blocking" }, Ct),
            Arm.AgentDown => Evaluator.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: true, Ct),
            Arm.CostRegression => Evaluator.ApplyCostRegressionsAsync(new[] { Regression(costDataPoint) }, Ct),
            _ => throw new ArgumentOutOfRangeException(nameof(arm)),
        };
    }

    private static DarlingCollectorCostReader.CostRegression Regression(DateTime latestMetricTime) =>
        new(7, "prod-multi-19", "query_store", 8000, 2000.0, latestMetricTime, 100, 80.0, 20.0, 20.0);

    [Theory]
    [MemberData(nameof(Arms))]
    public async Task AFireWhoseEveryChannelFailed_FiresAgainAMinuteLater_NotAfterTheCooldown(Arm arm)
    {
        var rig = new Rig { Deliverer = { Answer = FailedByWebhook() } };

        await rig.SweepAsync(arm);
        Assert.Equal(1, rig.Fires);

        /* The same condition, the same metric rows: 59 seconds on it is still closed. */
        rig.Now = Start.AddSeconds(59);
        await rig.SweepAsync(arm);
        Assert.Equal(1, rig.Fires);

        rig.Now = Start.AddSeconds(60);
        await rig.SweepAsync(arm);
        Assert.Equal(2, rig.Fires);
    }

    [Theory]
    [MemberData(nameof(Arms))]
    public async Task EachFurtherFailure_DoublesTheWait_AndNeverPassesTheCooldown(Arm arm)
    {
        var rig = new Rig { Deliverer = { Answer = FailedByWebhook() } };

        await rig.SweepAsync(arm);
        var fires = rig.Fires;
        var fireTime = Start;

        foreach (var expectedMinutes in new[] { 1, 2, 4, 5, 5 })
        {
            /* One second short of the delay: closed. */
            rig.Now = fireTime.AddMinutes(expectedMinutes).AddSeconds(-1);
            await rig.SweepAsync(arm);
            Assert.Equal(fires, rig.Fires);

            /* On the delay: it fires again, and that fire failed too. */
            rig.Now = fireTime.AddMinutes(expectedMinutes);
            await rig.SweepAsync(arm);
            fires++;
            Assert.Equal(fires, rig.Fires);
            fireTime = rig.Now;
        }
    }

    [Theory]
    [MemberData(nameof(Arms))]
    public async Task ADeliveredFire_WaitsTheFullCooldown(Arm arm)
    {
        var rig = new Rig { Deliverer = { Answer = DeliveredByWebhook() } };

        await rig.SweepAsync(arm);
        Assert.Equal(1, rig.Fires);

        rig.NewCostDataPoint();
        rig.Now = Start + Cooldown - TimeSpan.FromSeconds(1);
        await rig.SweepAsync(arm);
        Assert.Equal(1, rig.Fires);

        rig.Now = Start + Cooldown;
        await rig.SweepAsync(arm);
        Assert.Equal(2, rig.Fires);
    }

    [Theory]
    [MemberData(nameof(Arms))]
    public async Task ARetryThatIsDelivered_WaitsTheFullCooldown_AndTheNextFailureStartsAtAMinute(Arm arm)
    {
        var rig = new Rig { Deliverer = { Answer = FailedByWebhook() } };

        await rig.SweepAsync(arm);
        Assert.Equal(1, rig.Fires);

        /* The retry, a minute on, is delivered. */
        rig.Deliverer.Answer = DeliveredByWebhook();
        rig.Now = Start.AddMinutes(1);
        await rig.SweepAsync(arm);
        Assert.Equal(2, rig.Fires);

        /* A delivered fire waits the whole cooldown from where it fired. */
        rig.NewCostDataPoint();
        rig.Now = Start.AddMinutes(1) + Cooldown - TimeSpan.FromSeconds(1);
        await rig.SweepAsync(arm);
        Assert.Equal(2, rig.Fires);

        rig.Deliverer.Answer = FailedByWebhook();
        rig.Now = Start.AddMinutes(1) + Cooldown;
        await rig.SweepAsync(arm);
        Assert.Equal(3, rig.Fires);

        /* The delivery ended the streak, so this failure waits a minute again, not the two the streak would have
           reached had the delivered fire not counted. */
        rig.Now = Start.AddMinutes(1) + Cooldown + TimeSpan.FromSeconds(59);
        await rig.SweepAsync(arm);
        Assert.Equal(3, rig.Fires);

        rig.Now = Start.AddMinutes(1) + Cooldown + TimeSpan.FromSeconds(60);
        await rig.SweepAsync(arm);
        Assert.Equal(4, rig.Fires);
    }

    [Theory]
    [MemberData(nameof(Arms))]
    public async Task APartlyDeliveredFire_IsNotTried_AgainSooner(Arm arm)
    {
        var rig = new Rig { Deliverer = { Answer = FailedByEmailButDeliveredByWebhook() } };

        await rig.SweepAsync(arm);
        Assert.Equal(1, rig.Fires);

        rig.NewCostDataPoint();
        rig.Now = Start.AddMinutes(1);
        await rig.SweepAsync(arm);
        Assert.Equal(1, rig.Fires);
    }

    [Fact]
    public async Task ACostRegressionMarker_GoesBackToItsPriorValue_NotToNothing_WhenTheFireFailed()
    {
        var rig = new Rig { Deliverer = { Answer = DeliveredByWebhook() } };

        /* The first data point is reported and delivered: the marker holds it. */
        await rig.SweepAsync(Arm.CostRegression, FirstDataPoint);
        Assert.Equal(1, rig.Fires);

        /* A newer data point arrives after the cooldown, and its fire fails. */
        var newer = FirstDataPoint.AddHours(1);
        rig.Deliverer.Answer = FailedByWebhook();
        rig.Now = Start + Cooldown;
        await rig.SweepAsync(Arm.CostRegression, newer);
        Assert.Equal(2, rig.Fires);

        /* The retry sees the newer data point as not yet reported, and fires it. */
        rig.Now = Start + Cooldown + TimeSpan.FromMinutes(1);
        await rig.SweepAsync(Arm.CostRegression, newer);
        Assert.Equal(3, rig.Fires);

        /* The marker went back to the first data point, not to nothing: that older point stays reported. Had the
           failed fire removed the marker, this ask would fire it. */
        rig.Deliverer.Answer = DeliveredByWebhook();
        rig.Now = Start + Cooldown + TimeSpan.FromMinutes(1) + Cooldown;
        await rig.SweepAsync(Arm.CostRegression, FirstDataPoint);
        Assert.Equal(3, rig.Fires);
    }
}
