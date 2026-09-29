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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4795: Policy Job Failing advances its failure-count baseline on every pass, before the cooldown check. When
/// the alert's send reached no channel the baseline is put back so the retry still sees the failures as new; a
/// pass that lands inside the retry delay (which can be longer than the hourly check) must not advance it again.
/// A delivered alert keeps advancing the baseline as before.
/// </summary>
public sealed class PolicyJobFailingRetryTests
{
    private const int JobId = 77;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly DateTime Start = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    private static AlertDelivery Failed() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Failed, WebhookSendError: "Slack: 429 Too Many Requests", AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    private static AlertDelivery Delivered() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Delivered, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

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

    private sealed class Rig
    {
        public Rig(AlertDelivery answer)
        {
            Deliverer.Answer = answer;
            Evaluator = new DarlingSelfAlertEvaluator(
                new DarlingSelfAlertTests.FakeSettings { CooldownMinutes = 180 },
                Deliverer, new DarlingSelfAlertTests.FakeHistoryStore(), _ => false,
                logger: new DarlingSelfAlertTests.CapturingLogger(), utcNow: () => Now);
        }

        public ScriptedDeliverer Deliverer { get; } = new();

        public DateTime Now { get; set; } = Start;

        public DarlingSelfAlertEvaluator Evaluator { get; }

        public Task PassAsync(TimeSpan afterStart, long totalFailures)
        {
            Now = Start + afterStart;
            var reading = new StorePolicyJobHealth(
                Array.Empty<StuckPolicyJob>(),
                new[]
                {
                    new PolicyJobRunReading(
                        JobId, TimescaleSupport.StorePolicyJobFamily.Compression, "wait_stats",
                        Held: false, LastRunStatus: "Failed", TotalFailures: totalFailures),
                });
            return Evaluator.ApplyPolicyJobsStuckAsync(reading, _ => Task.FromResult(true), Ct);
        }
    }

    [Fact]
    public async Task APendingRetry_KeepsTheBaseline_SoASweepInsideTheDelayDoesNotEatTheFailures()
    {
        var rig = new Rig(Failed());

        await rig.PassAsync(TimeSpan.Zero, 0);
        await rig.PassAsync(TimeSpan.FromHours(1), 3);
        Assert.Single(rig.Deliverer.Outcomes);

        /* A sweep inside the one-minute retry delay: nothing to send, and the baseline must stay at 0. */
        await rig.PassAsync(TimeSpan.FromHours(1) + TimeSpan.FromSeconds(30), 3);
        Assert.Single(rig.Deliverer.Outcomes);

        /* The retry is due and still reports the three failures. */
        await rig.PassAsync(TimeSpan.FromHours(1) + TimeSpan.FromSeconds(60), 3);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
        Assert.Equal("3 new failure(s), 3 total", rig.Deliverer.Outcomes[1].CurrentValue);
    }

    [Fact]
    public async Task ADeliveredAlert_AdvancesTheBaselineAsBefore()
    {
        var rig = new Rig(Delivered());

        await rig.PassAsync(TimeSpan.Zero, 0);
        await rig.PassAsync(TimeSpan.FromHours(1), 3);
        Assert.Single(rig.Deliverer.Outcomes);

        /* Nothing new inside the cooldown, and the baseline moved on to 3 ... */
        await rig.PassAsync(TimeSpan.FromHours(1) + TimeSpan.FromSeconds(30), 3);
        await rig.PassAsync(TimeSpan.FromHours(2), 3);
        Assert.Single(rig.Deliverer.Outcomes);

        /* ... so when the cooldown is over, two more failures read as two, not five. */
        await rig.PassAsync(TimeSpan.FromHours(4), 5);
        Assert.Equal(2, rig.Deliverer.Outcomes.Count);
        Assert.Equal("2 new failure(s), 5 total", rig.Deliverer.Outcomes[1].CurrentValue);
    }
}
