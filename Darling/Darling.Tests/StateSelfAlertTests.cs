/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5493: a self-alert for a LASTING state sends one alert per occurrence, not one per cooldown. Collection Stopped
/// took the "Server Unreachable" shape in #5489 (<see cref="ConnectionAlertPolicy"/>: one alert on entry, a repeat only
/// per <c>connection_refire_minutes</c>, a send again only for an alert no channel took); these are the other alerts
/// whose condition stays until someone acts. Driven through the real arms with a scripted deliverer and a clock the
/// test moves, one [Theory] over the alerts so each gets the same four cases:
/// one alert across many cooldowns with repeats off, a repeat per interval with repeats on, a new alert after the
/// condition ended and started again, and a send again for an alert no channel took.
/// </summary>
public sealed class StateSelfAlertTests
{
    private const int ServerId = 424242;
    private const string Name = "STATE-ALERT-SRV";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly DateTime Start = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    public enum State
    {
        CaptureDown,
        AgentNotRunning,
        AgSyncBehind,
        CustomRuleHealth,
        FleetGate,
        StoreJobOverCadence,
        RetentionHeld,
        RawPurgeOverHorizon,
        PolicyJobStuckEscalated,
    }

    public static TheoryData<State> States
    {
        get
        {
            var data = new TheoryData<State>();
            foreach (var state in Enum.GetValues<State>())
            {
                data.Add(state);
            }

            return data;
        }
    }

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

    private sealed class ScriptedDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        public AlertDelivery? Answer { get; set; } = DeliveredByWebhook();

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
        public Rig(State state)
        {
            State = state;
            Evaluator = new DarlingSelfAlertEvaluator(
                Settings, Deliverer, History, _ => false,
                logger: new DarlingSelfAlertTests.CapturingLogger(), utcNow: () => Now,
                connectionRefireMinutes: () => ConnectionRefireMinutes);
        }

        public State State { get; }

        public DarlingSelfAlertTests.FakeHistoryStore History { get; } = new();

        public int ConnectionRefireMinutes { get; set; }

        public DarlingSelfAlertTests.FakeSettings Settings { get; } = new() { CooldownMinutes = 5 };

        public ScriptedDeliverer Deliverer { get; } = new();

        public DateTime Now { get; set; } = Start;

        public DarlingSelfAlertEvaluator Evaluator { get; }

        public int Fires => Deliverer.Outcomes.Count;

        public void At(int minute) => Now = Start.AddMinutes(minute);

        /// <summary>One check with the condition holding.</summary>
        public Task HoldAsync() => State switch
        {
            State.CaptureDown => Evaluator.ApplyCaptureDownAsync(ServerId, Name, new[] { "Blocking" }, Ct),
            State.AgentNotRunning => Evaluator.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: true, Ct),
            State.AgSyncBehind => Evaluator.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { AgDatabase(lagSeconds: 900) }, Ct),
            State.CustomRuleHealth => Evaluator.ApplyCustomRuleHealthAsync(UnhealthyRules(), Ct),
            State.FleetGate => Evaluator.ApplyFleetGateAsync(GateReport(skipped: 500), Ct),
            State.StoreJobOverCadence => Evaluator.ApplyStoreJobCadenceAsync(new[] { new StoreJobCadenceReading(1028, "policy_compression t", 3_600_000, 3_600_000) }, Ct),
            State.RetentionHeld => Evaluator.ApplyRetentionHoldsAsync(new[] { Retention(armed: false) }, Ct),
            State.RawPurgeOverHorizon => Evaluator.ApplyRawPurgeOverHorizonAsync(new[] { new RawPurgeOverHorizonReading(1, "t", "4 days", 4.5, null) }, Ct),
            State.PolicyJobStuckEscalated => Evaluator.ApplyPolicyJobsStuckAsync(StuckJob(), RearmFails, Ct),
            _ => throw new ArgumentOutOfRangeException(nameof(State)),
        };

        /// <summary>One check with the condition gone (the fleet gate also needs its quiet hour, so that one
        /// moves the clock past it).</summary>
        public async Task EndAsync()
        {
            switch (State)
            {
                case State.CaptureDown:
                    await Evaluator.ApplyCaptureDownAsync(ServerId, Name, Array.Empty<string>(), Ct);
                    break;
                case State.AgentNotRunning:
                    await Evaluator.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: true, agentEverSeenRunning: true, Ct);
                    break;
                case State.AgSyncBehind:
                    await Evaluator.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { AgDatabase(lagSeconds: 0) }, Ct);
                    break;
                case State.CustomRuleHealth:
                    await Evaluator.ApplyCustomRuleHealthAsync(new CustomAlertHealthReport(
                        Array.Empty<CustomAlertRuleHealthIssue>(), Array.Empty<CustomAlertRuleHealthIssue>()), Ct);
                    break;
                case State.FleetGate:
                    /* Caught up starts the quiet clock; a whole quiet hour later it resolves. */
                    await Evaluator.ApplyFleetGateAsync(GateReport(skipped: 0), Ct);
                    Now += DarlingSelfAlertEvaluator.FleetGateQuietHold;
                    await Evaluator.ApplyFleetGateAsync(GateReport(skipped: 0), Ct);
                    break;
                case State.StoreJobOverCadence:
                    await Evaluator.ApplyStoreJobCadenceAsync(new[] { new StoreJobCadenceReading(1028, "policy_compression t", 60_000, 3_600_000) }, Ct);
                    break;
                case State.RetentionHeld:
                    await Evaluator.ApplyRetentionHoldsAsync(new[] { Retention(armed: true) }, Ct);
                    break;
                case State.RawPurgeOverHorizon:
                    await Evaluator.ApplyRawPurgeOverHorizonAsync(
                        new[] { new RawPurgeOverHorizonReading(1, "t", "4 days", 0.5, new RawLastPurgeRecord(Now, "ran", null, null)) }, Ct);
                    break;
                case State.PolicyJobStuckEscalated:
                    await Evaluator.ApplyPolicyJobsStuckAsync(StorePolicyJobHealth.Empty, RearmFails, Ct);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(State));
            }
        }

        private static Task<bool> RearmFails(long jobId) => Task.FromResult(false);

        private static AgDatabaseReading AgDatabase(int lagSeconds) =>
            new("AG1", "Sales", "NODE2", SecondaryLagSeconds: lagSeconds, RedoQueueSizeKb: 0, IsSuspended: false, SuspendReasonDesc: null);

        private static CustomAlertHealthReport UnhealthyRules() => new(
            new[] { new CustomAlertRuleHealthIssue(1, "broken 1", "invalid metric: unknown measure 'x'") },
            Array.Empty<CustomAlertRuleHealthIssue>());

        private DarlingSelfAlertEvaluator.FleetGateReport GateReport(long skipped) =>
            new(Run: 500, Skipped: skipped, QueueWaits: 0, QueueWaitTotal: TimeSpan.Zero, QueueWaitMax: TimeSpan.Zero,
                GateWidth: 4, WindowEndUtc: Now);

        private static RetentionHoldReading Retention(bool armed) =>
            new(1, "t", armed, "4 days", 19, 1_561_449, 345_600);

        private static StorePolicyJobHealth StuckJob()
        {
            var stuck = new[]
            {
                new StuckPolicyJob(
                    7, "wait_stats", "next_start is -infinity — the scheduler will never run it again",
                    Arm: StuckPolicyJobArm.NextStartNegativeInfinity),
            };
            return new StorePolicyJobHealth(
                stuck,
                new[] { new PolicyJobRunReading(7, stuck[0].Family, "wait_stats", Held: false, LastRunStatus: null, TotalFailures: 0) });
        }
    }

    [Theory]
    [MemberData(nameof(States))]
    public async Task AStandingCondition_AcrossManyCooldowns_WithRefireOff_AlertsExactlyOnce(State state)
    {
        var rig = new Rig(state);

        /* 12 cooldowns (an hour) of a standing condition, one check a minute. */
        for (var minute = 0; minute <= 60; minute++)
        {
            rig.At(minute);
            await rig.HoldAsync();
        }

        Assert.Equal(1, rig.Fires);
    }

    [Theory]
    [MemberData(nameof(States))]
    public async Task AStandingCondition_WithRefireOn_RepeatsOnTheRefireInterval_NotTheCooldown(State state)
    {
        var rig = new Rig(state) { ConnectionRefireMinutes = 30 };

        var firedAtMinute = new List<int>();
        for (var minute = 0; minute <= 95; minute++)
        {
            rig.At(minute);
            var before = rig.Fires;
            await rig.HoldAsync();
            if (rig.Fires > before)
            {
                firedAtMinute.Add(minute);
            }
        }

        Assert.Equal(new[] { 0, 30, 60, 90 }, firedAtMinute.ToArray());
    }

    [Theory]
    [MemberData(nameof(States))]
    public async Task TheConditionEndingAndStartingAgain_IsANewOccurrenceWithItsOwnAlert_AndOneResolution(State state)
    {
        var rig = new Rig(state);

        await rig.HoldAsync();
        Assert.Equal(1, rig.Fires);

        rig.At(1);
        await rig.EndAsync();
        Assert.Single(rig.History.Records);

        /* Back a minute later, inside the cooldown: a new occurrence, a new alert, and again no repeats. */
        rig.Now += TimeSpan.FromMinutes(1);
        await rig.HoldAsync();
        Assert.Equal(2, rig.Fires);

        rig.Now += TimeSpan.FromMinutes(60);
        await rig.HoldAsync();
        Assert.Equal(2, rig.Fires);
    }

    [Theory]
    [MemberData(nameof(States))]
    public async Task AnAlertNoChannelTook_IsSentAgainAfterTheBackOff_ThenStopsOnceDelivered_WithRefireOff(State state)
    {
        var rig = new Rig(state) { Deliverer = { Answer = FailedByWebhook() } };

        await rig.HoldAsync();
        Assert.Equal(1, rig.Fires);

        rig.Now = Start.AddSeconds(59);
        await rig.HoldAsync();
        Assert.Equal(1, rig.Fires);

        rig.Deliverer.Answer = DeliveredByWebhook();
        rig.Now = Start.AddSeconds(60);
        await rig.HoldAsync();
        Assert.Equal(2, rig.Fires);

        for (var minute = 2; minute <= 60; minute++)
        {
            rig.At(minute);
            await rig.HoldAsync();
        }

        Assert.Equal(2, rig.Fires);
    }
}
