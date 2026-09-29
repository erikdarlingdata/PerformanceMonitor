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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4795: a connection or availability group alert whose every channel failed is due again after the failed-send
/// delay (a minute, doubling, never more than the alert cooldown) while the server stays down, even with re-fire
/// off. A delivered one follows the old rules exactly, and the Restored and Reconnected notices are not retried.
/// Driven through the real arms of <see cref="DarlingSelfAlertEvaluator"/> with a scripted deliverer and a clock
/// the test moves.
/// </summary>
public sealed class ConnectionAlertFailedSendRetryTests
{
    private const int ServerId = 515151;
    private const string Name = "CONN-ALERT-SRV";
    private const string Ag = "AG1";
    private const string Replica = "NODE2";
    private const string Db = "Sales";

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
        public Rig(int connectionRefireMinutes = 0, int agRefireMinutes = 0)
        {
            Evaluator = new DarlingSelfAlertEvaluator(
                new DarlingSelfAlertTests.FakeSettings { CooldownMinutes = 5 },
                Deliverer, new DarlingSelfAlertTests.FakeHistoryStore(), _ => false,
                logger: new DarlingSelfAlertTests.CapturingLogger(), utcNow: () => Now,
                notifyConnectionChanges: () => true,
                notifyConnectionDownAtStartup: () => false,
                connectionRefireMinutes: () => connectionRefireMinutes,
                notifyAgHealth: () => true,
                agDisconnectRefireMinutes: () => agRefireMinutes);
        }

        public ScriptedDeliverer Deliverer { get; } = new();

        public AlertDelivery? Answer
        {
            get => Deliverer.Answer;
            set => Deliverer.Answer = value;
        }

        public DateTime Now { get; set; } = Start;

        public DarlingSelfAlertEvaluator Evaluator { get; }

        public int Fires => Deliverer.Outcomes.Count;

        public AlertOutcome Last => Deliverer.Outcomes[^1];

        public Task ConnectionAsync(bool online) =>
            Evaluator.ApplyConnectionOutcomeAsync(ServerId, Name, online, online ? null : "no route", Ct);

        public Task ReplicaAsync(string? role = "SECONDARY", string connected = "CONNECTED") =>
            Evaluator.ApplyAgReplicaHealthAsync(
                ServerId, Name, new[] { new AgReplicaReading(Ag, Replica, role, connected, null) }, Ct);

        public Task DatabaseAsync(bool suspended) =>
            Evaluator.ApplyAgDatabaseHealthAsync(
                ServerId, Name,
                new[] { new AgDatabaseReading(Ag, Db, Replica, 0, 0, suspended, suspended ? "SUSPEND_FROM_USER" : null) }, Ct);

        public async Task AtAsync(TimeSpan afterStart, Func<Task> sweep)
        {
            Now = Start + afterStart;
            await sweep();
        }
    }

    /* ---------------- connection ---------------- */

    [Fact]
    public async Task ALostWhoseEveryChannelFailed_IsSentAgainAMinuteLater_WithRefireOff_AndTheWaitDoubles()
    {
        var rig = new Rig { Answer = Failed() };

        await rig.ConnectionAsync(true);
        await rig.ConnectionAsync(false);
        Assert.Equal(1, rig.Fires);
        Assert.Equal("Server Unreachable", rig.Last.MetricName);

        await rig.AtAsync(TimeSpan.FromSeconds(59), () => rig.ConnectionAsync(false));
        Assert.Equal(1, rig.Fires);

        await rig.AtAsync(TimeSpan.FromSeconds(60), () => rig.ConnectionAsync(false));
        Assert.Equal(2, rig.Fires);
        Assert.Equal("Server Unreachable", rig.Last.MetricName);
        Assert.Contains("reached no channel", rig.Last.DetailText, StringComparison.Ordinal);

        /* The second failure waits two minutes. */
        await rig.AtAsync(TimeSpan.FromSeconds(60 + 119), () => rig.ConnectionAsync(false));
        Assert.Equal(2, rig.Fires);
        await rig.AtAsync(TimeSpan.FromSeconds(60 + 120), () => rig.ConnectionAsync(false));
        Assert.Equal(3, rig.Fires);

        /* A retry that gets through ends it: nothing more while the server stays down. */
        rig.Answer = Delivered();
        await rig.AtAsync(TimeSpan.FromSeconds(60 + 120 + 240), () => rig.ConnectionAsync(false));
        Assert.Equal(4, rig.Fires);
        await rig.AtAsync(TimeSpan.FromHours(3), () => rig.ConnectionAsync(false));
        Assert.Equal(4, rig.Fires);
    }

    [Fact]
    public async Task ADeliveredLost_IsNotSentAgain_WithRefireOff()
    {
        var rig = new Rig { Answer = Delivered() };

        await rig.ConnectionAsync(true);
        await rig.ConnectionAsync(false);
        Assert.Equal(1, rig.Fires);

        foreach (var minutes in new[] { 1, 2, 10, 60, 24 * 60 })
        {
            await rig.AtAsync(TimeSpan.FromMinutes(minutes), () => rig.ConnectionAsync(false));
            Assert.Equal(1, rig.Fires);
        }
    }

    [Fact]
    public async Task AnUnreportedLost_CountsAsDelivered_AndIsNotSentAgain()
    {
        var rig = new Rig { Answer = null };

        await rig.ConnectionAsync(true);
        await rig.ConnectionAsync(false);
        await rig.AtAsync(TimeSpan.FromMinutes(30), () => rig.ConnectionAsync(false));
        Assert.Equal(1, rig.Fires);
    }

    [Fact]
    public async Task ARestoredIsNotRetried_AndItEndsThePendingRetryOfTheOutageBeforeIt()
    {
        var rig = new Rig { Answer = Failed() };

        await rig.ConnectionAsync(true);
        await rig.ConnectionAsync(false);
        Assert.Equal(1, rig.Fires);

        /* Back up before the retry came due: the notice goes out once, fails, and is left alone. */
        await rig.AtAsync(TimeSpan.FromSeconds(30), () => rig.ConnectionAsync(true));
        Assert.Equal(2, rig.Fires);
        Assert.Equal("Server Restored", rig.Last.MetricName);
        await rig.AtAsync(TimeSpan.FromMinutes(10), () => rig.ConnectionAsync(true));
        Assert.Equal(2, rig.Fires);

        /* The next outage announces itself once. The first outage's retry time must not make its second poll a
           StillDown. */
        await rig.AtAsync(TimeSpan.FromMinutes(11), () => rig.ConnectionAsync(false));
        Assert.Equal(3, rig.Fires);
        await rig.AtAsync(TimeSpan.FromMinutes(11), () => rig.ConnectionAsync(false));
        Assert.Equal(3, rig.Fires);
    }

    [Fact]
    public async Task WithRefireOn_AFailedLost_IsRetriedAtTheFailedSendDelay_WithTheUsualRefireText()
    {
        var rig = new Rig(connectionRefireMinutes: 10) { Answer = Failed() };

        await rig.ConnectionAsync(true);
        await rig.ConnectionAsync(false);
        Assert.Equal(1, rig.Fires);

        await rig.AtAsync(TimeSpan.FromSeconds(59), () => rig.ConnectionAsync(false));
        Assert.Equal(1, rig.Fires);

        await rig.AtAsync(TimeSpan.FromSeconds(60), () => rig.ConnectionAsync(false));
        Assert.Equal(2, rig.Fires);
        Assert.Contains("re-alerting every 10 min", rig.Last.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("reached no channel", rig.Last.DetailText, StringComparison.Ordinal);
    }

    /* ---------------- availability group ---------------- */

    [Fact]
    public async Task AnAgDisconnectWhoseEveryChannelFailed_IsSentAgainAMinuteLater_WithRefireOff()
    {
        var rig = new Rig { Answer = Failed() };

        await rig.ReplicaAsync(connected: "CONNECTED");
        await rig.ReplicaAsync(connected: "DISCONNECTED");
        Assert.Equal(1, rig.Fires);
        Assert.Equal(AgAlertPolicy.ReplicaDisconnectedMetric, rig.Last.MetricName);

        await rig.AtAsync(TimeSpan.FromSeconds(59), () => rig.ReplicaAsync(connected: "DISCONNECTED"));
        Assert.Equal(1, rig.Fires);

        await rig.AtAsync(TimeSpan.FromSeconds(60), () => rig.ReplicaAsync(connected: "DISCONNECTED"));
        Assert.Equal(2, rig.Fires);
        Assert.Equal(AgAlertPolicy.ReplicaDisconnectedMetric, rig.Last.MetricName);
        Assert.Contains("reached no channel", rig.Last.DetailText, StringComparison.Ordinal);

        rig.Answer = Delivered();
        await rig.AtAsync(TimeSpan.FromSeconds(60 + 120), () => rig.ReplicaAsync(connected: "DISCONNECTED"));
        Assert.Equal(3, rig.Fires);
        await rig.AtAsync(TimeSpan.FromHours(3), () => rig.ReplicaAsync(connected: "DISCONNECTED"));
        Assert.Equal(3, rig.Fires);
    }

    [Fact]
    public async Task ADeliveredAgDisconnect_IsNotSentAgain_WithRefireOff()
    {
        var rig = new Rig { Answer = Delivered() };

        await rig.ReplicaAsync(connected: "CONNECTED");
        await rig.ReplicaAsync(connected: "DISCONNECTED");
        Assert.Equal(1, rig.Fires);

        await rig.AtAsync(TimeSpan.FromMinutes(10), () => rig.ReplicaAsync(connected: "DISCONNECTED"));
        await rig.AtAsync(TimeSpan.FromHours(3), () => rig.ReplicaAsync(connected: "DISCONNECTED"));
        Assert.Equal(1, rig.Fires);
    }

    [Fact]
    public async Task WithRefireOn_AFailedAgDisconnect_DoesNotConsumeTheWindow_AndADeliveredOneOpensIt()
    {
        var rig = new Rig(agRefireMinutes: 10) { Answer = Failed() };

        await rig.ReplicaAsync(connected: "CONNECTED");
        await rig.ReplicaAsync(connected: "DISCONNECTED");
        Assert.Equal(1, rig.Fires);

        /* Held to the failed-send delay, not sent on every sweep and not left waiting for the 10 minute window. */
        await rig.AtAsync(TimeSpan.FromSeconds(30), () => rig.ReplicaAsync(connected: "DISCONNECTED"));
        Assert.Equal(1, rig.Fires);
        rig.Answer = Delivered();
        await rig.AtAsync(TimeSpan.FromSeconds(60), () => rig.ReplicaAsync(connected: "DISCONNECTED"));
        Assert.Equal(2, rig.Fires);

        /* Delivered now, so the re-fire window runs from here. */
        await rig.AtAsync(TimeSpan.FromMinutes(1) + TimeSpan.FromMinutes(9), () => rig.ReplicaAsync(connected: "DISCONNECTED"));
        Assert.Equal(2, rig.Fires);
        await rig.AtAsync(TimeSpan.FromMinutes(1) + TimeSpan.FromMinutes(10), () => rig.ReplicaAsync(connected: "DISCONNECTED"));
        Assert.Equal(3, rig.Fires);
    }

    [Fact]
    public async Task AnAgReconnectedIsNotRetried_AndItEndsThePendingRetry()
    {
        var rig = new Rig { Answer = Failed() };

        await rig.ReplicaAsync(connected: "CONNECTED");
        await rig.ReplicaAsync(connected: "DISCONNECTED");
        await rig.AtAsync(TimeSpan.FromSeconds(30), () => rig.ReplicaAsync(connected: "CONNECTED"));
        Assert.Equal(2, rig.Fires);
        Assert.Equal(AgAlertPolicy.ReplicaReconnectedMetric, rig.Last.MetricName);

        await rig.AtAsync(TimeSpan.FromMinutes(10), () => rig.ReplicaAsync(connected: "CONNECTED"));
        Assert.Equal(2, rig.Fires);

        await rig.AtAsync(TimeSpan.FromMinutes(11), () => rig.ReplicaAsync(connected: "DISCONNECTED"));
        Assert.Equal(3, rig.Fires);
        await rig.AtAsync(TimeSpan.FromMinutes(11), () => rig.ReplicaAsync(connected: "DISCONNECTED"));
        Assert.Equal(3, rig.Fires);
    }

    [Fact]
    public async Task AFailoverWhoseEveryChannelFailed_IsSentAgainAMinuteLater_AndADeliveredOneIsNot()
    {
        var rig = new Rig { Answer = Failed() };

        await rig.ReplicaAsync(role: "SECONDARY");
        await rig.ReplicaAsync(role: "PRIMARY");
        Assert.Equal(1, rig.Fires);
        Assert.Equal(AgAlertPolicy.FailoverMetric, rig.Last.MetricName);

        await rig.AtAsync(TimeSpan.FromSeconds(59), () => rig.ReplicaAsync(role: "PRIMARY"));
        Assert.Equal(1, rig.Fires);

        rig.Answer = Delivered();
        await rig.AtAsync(TimeSpan.FromSeconds(60), () => rig.ReplicaAsync(role: "PRIMARY"));
        Assert.Equal(2, rig.Fires);
        Assert.Equal(AgAlertPolicy.FailoverMetric, rig.Last.MetricName);
        Assert.Equal("PRIMARY", rig.Last.CurrentValue);
        Assert.Equal("SECONDARY", rig.Last.ThresholdValue);

        await rig.AtAsync(TimeSpan.FromHours(1), () => rig.ReplicaAsync(role: "PRIMARY"));
        Assert.Equal(2, rig.Fires);
    }

    [Fact]
    public async Task ADataMovementSuspendedWhoseEveryChannelFailed_IsSentAgainAMinuteLater_AndADeliveredOneIsNot()
    {
        var rig = new Rig { Answer = Failed() };

        await rig.DatabaseAsync(suspended: false);
        await rig.DatabaseAsync(suspended: true);
        Assert.Equal(1, rig.Fires);
        Assert.Equal(AgAlertPolicy.DatabaseSuspendedMetric, rig.Last.MetricName);

        await rig.AtAsync(TimeSpan.FromSeconds(59), () => rig.DatabaseAsync(suspended: true));
        Assert.Equal(1, rig.Fires);

        rig.Answer = Delivered();
        await rig.AtAsync(TimeSpan.FromSeconds(60), () => rig.DatabaseAsync(suspended: true));
        Assert.Equal(2, rig.Fires);
        Assert.Equal(AgAlertPolicy.DatabaseSuspendedMetric, rig.Last.MetricName);

        await rig.AtAsync(TimeSpan.FromHours(1), () => rig.DatabaseAsync(suspended: true));
        Assert.Equal(2, rig.Fires);
    }
}
