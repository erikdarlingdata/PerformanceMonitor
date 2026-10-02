/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4795: an Availability Group alert whose every channel failed is tried again after the failed-send delay
/// (a minute, doubling, never more than the cooldown) while the condition holds. The sweep hands each send's
/// answer to <see cref="AgAlertEvaluator.NoteSent"/>, which opens the re-fire window only for a send that
/// reached a channel (or had none to reach) and holds a failed alert back until its retry is due. The WPF window
/// that sends cannot be built in a test, so the logic the window calls is pinned here.
/// </summary>
public sealed class AgAlertFailedSendRetryTests
{
    private const int ServerId = 4242;
    private static readonly TimeSpan Cooldown = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan Refire = TimeSpan.FromMinutes(10);
    private static readonly DateTime Start = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    private DateTime _now = Start;

    private AgAlertEvaluator Evaluator() => new(() => _now);

    private void At(TimeSpan afterStart) => _now = Start + afterStart;

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

    private static AgReplicaReading Replica(string? role = "SECONDARY", string? connected = "CONNECTED") =>
        new("AG1", "NODE2", role, connected);

    private static AgDatabaseReading Database(
        bool? suspended = false, long? lagSeconds = 0, string? suspendReason = null) =>
        new("AG1", "Sales", "NODE2", lagSeconds, 0, suspended, suspendReason);

    private static AgAlert Single(System.Collections.Generic.List<AgAlert> alerts) => Assert.Single(alerts);

    /* ---------------- disconnect ---------------- */

    [Fact]
    public void ADisconnectWhoseEveryChannelFailed_DoesNotOpenTheWindow_AndIsHeldUntilTheRetryIsDue()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, Refire);
        var lost = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        e.NoteSent(lost, Failed(), Cooldown);

        /* Not on every sweep: a lasting channel failure is tried at 1, 2, 4 ... minutes. */
        At(TimeSpan.FromSeconds(30));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));

        At(TimeSpan.FromSeconds(60));
        var again = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        Assert.Equal(AgAlertPolicy.ReplicaDisconnectedMetric, again.MetricName);
        e.NoteSent(again, Failed(), Cooldown);

        /* The second failure waits two minutes. */
        At(TimeSpan.FromSeconds(60 + 119));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        At(TimeSpan.FromSeconds(60 + 120));
        var third = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));

        /* Delivered at last: the window opens on this send and runs its full length from here. */
        e.NoteSent(third, Delivered(), Cooldown);
        At(TimeSpan.FromSeconds(60 + 120) + Refire - TimeSpan.FromSeconds(1));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        At(TimeSpan.FromSeconds(60 + 120) + Refire);
        Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
    }

    [Fact]
    public void WithRefireOff_AFailedDisconnectIsTriedAgainAMinuteLater_AndADeliveredOneIsNever()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") });
        var lost = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
        e.NoteSent(lost, Failed(), Cooldown);

        At(TimeSpan.FromSeconds(59));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));

        At(TimeSpan.FromSeconds(60));
        var again = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
        Assert.Contains("reached no channel", again.DetailText, StringComparison.Ordinal);
        e.NoteSent(again, Delivered(), Cooldown);

        At(TimeSpan.FromHours(6));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
    }

    [Fact]
    public void WithRefireOff_ADeliveredDisconnectIsNeverSentAgain()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") });
        var lost = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
        e.NoteSent(lost, Delivered(), Cooldown);

        foreach (var minutes in new[] { 1, 5, 60, 24 * 60 })
        {
            At(TimeSpan.FromMinutes(minutes));
            Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
        }
    }

    [Fact]
    public void AnUnreportedSend_CountsAsDelivered_AndOpensTheWindowLikeBefore()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, Refire);
        var lost = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        e.NoteSent(lost, null, Cooldown);

        At(TimeSpan.FromMinutes(2));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        At(Refire);
        Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
    }

    [Fact]
    public void AReconnectEndsThePendingRetry_SoTheNextOutageStartsClean()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") });
        e.NoteSent(Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") })), Failed(), Cooldown);

        At(TimeSpan.FromSeconds(30));
        Assert.True(Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") })).IsResolution);

        At(TimeSpan.FromMinutes(10));
        var lost = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
        e.NoteSent(lost, Delivered(), Cooldown);
        At(TimeSpan.FromMinutes(11));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
    }

    /* ---------------- #4732: a wall clock that stepped backwards ---------------- */

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ADisconnectRetryStampedBeforeAnHourLongBackwardClockStep_IsSentAtTheNextSweep_WhateverTheRefireSays(bool refireOn)
    {
        TimeSpan? refire = refireOn ? Refire : null;
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, refire);
        var lost = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, refire));
        e.NoteSent(lost, Failed(), Cooldown);

        /* The retry is stamped a minute out under the 5 minute cooldown cap. The clock steps back an hour, so the stamp is
           over an hour ahead of it, which no wait can be: the retry is due now. Left raw it waited out the hour, and
           while it was pending the re-fire window was not consulted either. */
        At(TimeSpan.FromHours(-1));
        var again = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, refire));
        Assert.Equal(AgAlertPolicy.ReplicaDisconnectedMetric, again.MetricName);
        e.NoteSent(again, Delivered(), Cooldown);

        /* Delivered, so the retry is over. */
        At(TimeSpan.FromHours(-1) + TimeSpan.FromMinutes(1));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
    }

    [Fact]
    public void ADisconnectRetryStampedInsideTheCapAheadOfTheClock_StillWaits_AfterASmallBackwardStep()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, Refire);
        var lost = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        e.NoteSent(lost, Failed(), Cooldown);

        /* The stamp is at 1 minute. Three minutes back it is 4 minutes ahead, and at 4 minutes back it is exactly the
           5 minute cap ahead: both are waits, not steps. */
        At(TimeSpan.FromMinutes(-3));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        At(TimeSpan.FromMinutes(-4));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));

        /* And it is still sent when its time comes. */
        At(TimeSpan.FromMinutes(1));
        Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
    }

    [Fact]
    public void TheDisconnectDecision_ReadsTheClockOnce_AndGivesThatSameValueToThePolicyAndToTheRetryDueTime()
    {
        var source = Regex.Replace(ParitySource.ReadFile("Lite/Services/AgAlertEvaluator.cs"), @"\s+", " ");

        Assert.Contains(
            "var nowUtc = _utcNow(); var decision = AgAlertPolicy.DecideConnection(", source, StringComparison.Ordinal);
        Assert.Contains(
            "? lastDisconnect : null, nowUtc, _retries.DueUtc(disconnectRetryKey, nowUtc));", source, StringComparison.Ordinal);
        Assert.DoesNotContain("StampedDueUtc(", source, StringComparison.Ordinal);
    }

    /* ---------------- failover, data movement suspended, sync fell behind ---------------- */

    [Fact]
    public void AFailoverWhoseEveryChannelFailed_IsTriedAgainAMinuteLater_AndADeliveredOneIsNot()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(role: "SECONDARY") });
        var failover = Single(e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") }));
        e.NoteSent(failover, Failed(), Cooldown);

        At(TimeSpan.FromSeconds(59));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") }));

        At(TimeSpan.FromSeconds(60));
        var again = Single(e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") }));
        Assert.Equal(AgAlertPolicy.FailoverMetric, again.MetricName);
        Assert.Equal("SECONDARY", again.ThresholdValue);
        e.NoteSent(again, Delivered(), Cooldown);

        At(TimeSpan.FromHours(1));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") }));
    }

    [Fact]
    public void ADataMovementSuspendedWhoseEveryChannelFailed_IsTriedAgainAMinuteLater_AndADeliveredOneIsNot()
    {
        var e = Evaluator();
        e.EvaluateDatabases(ServerId, new[] { Database(suspended: false) }, 300, 0, Cooldown);
        var suspended = Single(e.EvaluateDatabases(ServerId, new[] { Database(suspended: true) }, 300, 0, Cooldown));
        Assert.Equal(AgAlertPolicy.DatabaseSuspendedMetric, suspended.MetricName);
        e.NoteSent(suspended, Failed(), Cooldown);

        At(TimeSpan.FromSeconds(59));
        Assert.DoesNotContain(
            e.EvaluateDatabases(ServerId, new[] { Database(suspended: true) }, 300, 0, Cooldown),
            a => a.MetricName == AgAlertPolicy.DatabaseSuspendedMetric);

        At(TimeSpan.FromSeconds(60));
        var again = e.EvaluateDatabases(ServerId, new[] { Database(suspended: true) }, 300, 0, Cooldown)
            .Single(a => a.MetricName == AgAlertPolicy.DatabaseSuspendedMetric);
        e.NoteSent(again, Delivered(), Cooldown);

        At(TimeSpan.FromHours(1));
        Assert.DoesNotContain(
            e.EvaluateDatabases(ServerId, new[] { Database(suspended: true) }, 300, 0, Cooldown),
            a => a.MetricName == AgAlertPolicy.DatabaseSuspendedMetric);
    }

    [Fact]
    public void ASyncFellBehindWhoseEveryChannelFailed_IsTriedAgainAMinuteLater_NotAfterTheCooldown()
    {
        var e = Evaluator();
        var behind = new[] { Database(lagSeconds: 900) };
        var first = Single(e.EvaluateDatabases(ServerId, behind, 300, 0, Cooldown));
        Assert.Equal(AgAlertPolicy.SyncFellBehindMetric, first.MetricName);
        e.NoteSent(first, Failed(), Cooldown);

        At(TimeSpan.FromSeconds(59));
        Assert.Empty(e.EvaluateDatabases(ServerId, behind, 300, 0, Cooldown));

        At(TimeSpan.FromSeconds(60));
        var again = Single(e.EvaluateDatabases(ServerId, behind, 300, 0, Cooldown));
        e.NoteSent(again, Delivered(), Cooldown);

        /* Delivered, so it waits out the whole cooldown from here. */
        At(TimeSpan.FromSeconds(60) + Cooldown - TimeSpan.FromSeconds(1));
        Assert.Empty(e.EvaluateDatabases(ServerId, behind, 300, 0, Cooldown));
        At(TimeSpan.FromSeconds(60) + Cooldown);
        Single(e.EvaluateDatabases(ServerId, behind, 300, 0, Cooldown));
    }

    /* ---------------- a server removed from monitoring ---------------- */

    [Fact]
    public void AForgottenServer_DropsItsPendingRetry_SoAReAddedOneWithTheReplicaStillDisconnectedTakesTheSilentBaseline()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") });
        e.NoteSent(Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") })), Failed(), Cooldown);

        /* Removed with a retry pending, then added again with the replica still down: like any first sighting, a
           silent baseline. The retry belonged to the outage the removed server had, not to this one. */
        e.Forget(ServerId);
        At(TimeSpan.FromMinutes(2));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
        At(TimeSpan.FromMinutes(3));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
    }

    [Fact]
    public void AForgottenServer_StartsItsFailedSendStreakOver()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") });
        e.NoteSent(Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") })), Failed(), Cooldown);
        At(TimeSpan.FromSeconds(60));
        e.NoteSent(Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") })), Failed(), Cooldown);

        /* Removed after two failures in a row, added again, and a new outage whose first send fails. */
        e.Forget(ServerId);
        At(TimeSpan.FromMinutes(10));
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") });
        e.NoteSent(Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") })), Failed(), Cooldown);

        /* It is the first failure of that outage, so it waits a minute, not the four the old streak had earned. */
        At(TimeSpan.FromMinutes(10) + TimeSpan.FromSeconds(60));
        Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
    }

    [Fact]
    public void AForgottenServer_DropsTheMarkersItsUnsentAlertsWouldHavePutBack()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(role: "SECONDARY") });

        /* Decided but never sent (the server is acknowledged or silenced): the put-back waits for a NoteSent that
           does not come. */
        Single(e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") }));
        var putBack = (System.Collections.IDictionary)typeof(AgAlertEvaluator)
            .GetField("_putBack", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(e)!;
        Assert.Single(putBack);

        e.Forget(ServerId);

        Assert.Empty(putBack);
    }

    [Fact]
    public void ForgettingOneServer_LeavesAnotherServersPendingRetryAlone()
    {
        /* "42" is a prefix of "4242": what keeps the two apart is the separator after the id. */
        const int shorterId = 42;
        var e = Evaluator();
        foreach (var id in new[] { ServerId, shorterId })
        {
            e.EvaluateReplicas(id, new[] { Replica(connected: "CONNECTED") });
            e.NoteSent(Single(e.EvaluateReplicas(id, new[] { Replica(connected: "DISCONNECTED") })), Failed(), Cooldown);
        }

        e.Forget(shorterId);

        At(TimeSpan.FromSeconds(60));
        Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
    }

    /* ---------------- a send that answers after its server was removed ---------------- */

    [Fact]
    public void AFailedAnswerThatArrivesAfterTheServerWasForgotten_RecordsNoRetry_SoAReAddedOneTakesTheSilentBaseline()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") });
        var lost = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));

        /* The send is still running when the server is removed, and its answer arrives afterwards. */
        e.Forget(ServerId);
        At(TimeSpan.FromSeconds(10));
        e.NoteSent(lost, Failed(), Cooldown);

        /* Added again with the replica still down: a first sighting, silent. The answer belonged to the outage
           the removed server had, so it must not leave a retry that comes due here. */
        At(TimeSpan.FromMinutes(2));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
        At(TimeSpan.FromMinutes(3));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
    }

    [Fact]
    public void ADeliveredAnswerThatArrivesAfterTheServerWasForgotten_OpensNoRefireWindow_SoAReAddedOneStillAnnouncesItsStandingOutage()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, Refire);
        var lost = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));

        e.Forget(ServerId);
        At(TimeSpan.FromSeconds(10));
        e.NoteSent(lost, Delivered(), Cooldown);

        /* With re-fire on, a replica already down at its first sighting announces. The window the removed server's
           answer would have opened must not swallow that. */
        At(TimeSpan.FromSeconds(20));
        var announced = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        Assert.Equal(AgAlertPolicy.ReplicaDisconnectedMetric, announced.MetricName);
    }

    [Fact]
    public void NoteDelivered_IgnoresAnAlertThatWasDecidedBeforeItsServerWasForgotten()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, Refire);
        var lost = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));

        e.Forget(ServerId);
        At(TimeSpan.FromSeconds(10));
        e.NoteDelivered(lost);

        At(TimeSpan.FromSeconds(20));
        Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
    }

    [Fact]
    public void AFailedSyncBehindAnswerThatArrivesAfterTheServerWasForgotten_DoesNotHoldBackTheReAddedServersFirstAlert()
    {
        var e = Evaluator();
        var behind = new[] { Database(lagSeconds: 900) };
        var first = Single(e.EvaluateDatabases(ServerId, behind, 300, 0, Cooldown));

        e.Forget(ServerId);
        e.NoteSent(first, Failed(), Cooldown);

        /* A standing condition has no silent baseline: the re-added server's first sweep announces it, and a retry
           the removed server's answer left behind would hold that back for the failed-send delay. */
        At(TimeSpan.FromSeconds(10));
        Single(e.EvaluateDatabases(ServerId, behind, 300, 0, Cooldown));
    }

    [Fact]
    public void AnOldAnswer_DoesNotConsumeThePutBackOfTheReAddedServersOwnAlertForTheSameGrain()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(role: "SECONDARY") });
        var old = Single(e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") }));

        /* Removed and added again while that send runs, and the re-added server sees the same change, so its alert is
           registered under the same key. Checking whether the grain still has state cannot tell the two apart. */
        e.Forget(ServerId);
        e.EvaluateReplicas(ServerId, new[] { Replica(role: "SECONDARY") });
        var current = Single(e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") }));

        e.NoteSent(old, Delivered(), Cooldown);
        e.NoteSent(current, Failed(), Cooldown);

        /* The current alert's send failed, so its marker went back and the change is announced again once due. */
        At(TimeSpan.FromSeconds(60));
        Single(e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") }));
    }

    [Fact]
    public void AnAlertDecidedAfterTheForget_StillRecordsItsFailedSend()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") });
        e.Forget(ServerId);

        /* The server is added again: its own baseline, its own outage, its own send. */
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") });
        var lost = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
        e.NoteSent(lost, Failed(), Cooldown);

        At(TimeSpan.FromSeconds(59));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
        At(TimeSpan.FromSeconds(60));
        Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }));
    }

    [Fact]
    public void AnAlertDecidedAfterTheForget_StillOpensItsRefireWindowWhenDelivered()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, Refire);
        e.Forget(ServerId);

        e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }, Refire);
        var lost = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        e.NoteSent(lost, Delivered(), Cooldown);

        At(Refire - TimeSpan.FromSeconds(1));
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        At(Refire);
        Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
    }

    /* ---------------- a sweep that was reading when its server was removed ---------------- */

    [Fact]
    public void TheGeneration_StartsAtZero_AndMovesOnlyForTheServerThatWasForgotten()
    {
        var e = Evaluator();
        Assert.Equal(0, e.GenerationOf(ServerId));

        e.Forget(ServerId);
        e.Forget(ServerId);

        Assert.Equal(2, e.GenerationOf(ServerId));
        Assert.Equal(0, e.GenerationOf(ServerId + 1));
    }

    [Fact]
    public void AReplicaSweepThatWasReadingWhenTheServerWasForgotten_RecordsNoRole_SoAReAddedServersFirstSweepTakesTheSilentBaseline()
    {
        var e = Evaluator();
        var generation = e.GenerationOf(ServerId);

        /* The sweep took the generation, then waited on its read. The server was removed meanwhile, and the rows come
           back for a server that is gone. */
        e.Forget(ServerId);
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(role: "SECONDARY") }, sweepGeneration: generation));

        /* Added again with its replica PRIMARY: a first sighting, silent. The role the stale sweep saw must not stand
           in as the previous one and page a failover nobody had. */
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(role: "PRIMARY") }));
    }

    [Fact]
    public void AReplicaSweepThatWasReadingWhenTheServerWasForgotten_RecordsNoConnectionState_SoAReAddedServersFirstSweepReportsNoReconnect()
    {
        var e = Evaluator();
        var generation = e.GenerationOf(ServerId);

        e.Forget(ServerId);
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, sweepGeneration: generation));

        /* Added again with the replica connected: a first sighting. Had the stale sweep left DISCONNECTED behind, this
           would read as a reconnect and send a notice for an outage the re-added server never had. */
        Assert.Empty(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "CONNECTED") }));
    }

    [Fact]
    public void AReplicaSweepThatWasReadingWhenTheServerWasForgotten_AnnouncesNothingEvenWithRefireOn_SoAReAddedServersFirstSweepAnnouncesForItself()
    {
        var e = Evaluator();
        var generation = e.GenerationOf(ServerId);

        /* With re-fire on, a replica already down at first sighting announces, so the stale sweep would have returned
           an alert for a server that is gone, and started the re-fire clock the re-added server would then wait out. */
        e.Forget(ServerId);
        Assert.Empty(e.EvaluateReplicas(
            ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire, sweepGeneration: generation));

        var announced = Single(e.EvaluateReplicas(ServerId, new[] { Replica(connected: "DISCONNECTED") }, Refire));
        Assert.Equal(AgAlertPolicy.ReplicaDisconnectedMetric, announced.MetricName);
    }

    [Fact]
    public void ADatabaseSweepThatWasReadingWhenTheServerWasForgotten_RecordsNoSuspension_SoAReAddedServersFirstSweepReportsNoResume()
    {
        var e = Evaluator();
        var generation = e.GenerationOf(ServerId);

        e.Forget(ServerId);
        Assert.Empty(e.EvaluateDatabases(
            ServerId, new[] { Database(suspended: true, suspendReason: "SUSPEND_FROM_USER") }, 300, 0, Cooldown,
            sweepGeneration: generation));

        /* Added again with the database moving data: a first sighting, silent. Had the stale sweep left "suspended"
           behind, this would read as data movement resuming, a notice for a suspension the re-added server never had. */
        Assert.Empty(e.EvaluateDatabases(ServerId, new[] { Database(suspended: false) }, 300, 0, Cooldown));
    }

    [Fact]
    public void ADatabaseSweepThatWasReadingWhenTheServerWasForgotten_AnnouncesNothingAndStartsNoCooldown_SoAReAddedServerStillAnnouncesItsFirstSyncBehind()
    {
        var e = Evaluator();
        var behind = new[] { Database(lagSeconds: 900) };
        var generation = e.GenerationOf(ServerId);

        e.Forget(ServerId);
        Assert.Empty(e.EvaluateDatabases(ServerId, behind, 300, 0, Cooldown, sweepGeneration: generation));

        /* A standing condition has no silent baseline: the re-added server's first sweep announces it. A cooldown the
           stale sweep had started would hold that back, and the stale sweep's own alert would have gone out for a
           server that is gone. */
        At(TimeSpan.FromSeconds(10));
        var announced = Single(e.EvaluateDatabases(ServerId, behind, 300, 0, Cooldown));
        Assert.Equal(AgAlertPolicy.SyncFellBehindMetric, announced.MetricName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void ASweepThatPassesTheServersCurrentGeneration_EvaluatesLikeOneThatPassesNone(int timesForgotten)
    {
        var behind = new[] { Database(lagSeconds: 900) };

        foreach (var pass in new[] { "current generation", "no generation" })
        {
            var e = Evaluator();
            for (var i = 0; i < timesForgotten; i++)
            {
                e.Forget(ServerId);
            }

            /* Another server's removal moves that server on, not this one. */
            e.Forget(ServerId + 1);
            int? sweepGeneration = pass == "current generation" ? e.GenerationOf(ServerId) : null;

            e.EvaluateReplicas(ServerId, new[] { Replica(role: "SECONDARY") }, sweepGeneration: sweepGeneration);
            var failover = Single(e.EvaluateReplicas(
                ServerId, new[] { Replica(role: "PRIMARY") }, sweepGeneration: sweepGeneration));
            Assert.Equal(AgAlertPolicy.FailoverMetric, failover.MetricName);

            var syncBehind = Single(e.EvaluateDatabases(ServerId, behind, 300, 0, Cooldown, sweepGeneration: sweepGeneration));
            Assert.Equal(AgAlertPolicy.SyncFellBehindMetric, syncBehind.MetricName);
        }
    }
}
