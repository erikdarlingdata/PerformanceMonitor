/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4732: an Availability Group alert's re-fire window and cooldown after the wall clock steps backward. The
/// stamp of the last announcement is then AHEAD of the clock; the first sweep that sees it stores that sweep's
/// clock reading in its place, so the repeat is due one window after that sweep (not the step plus the window),
/// and never at once. The WPF window that sends cannot be built in a test, so the evaluator it calls is pinned
/// here with a fixed clock.
/// </summary>
public sealed class AgAlertClockStepTests
{
    private const int ServerId = 4242;
    private static readonly TimeSpan Cooldown = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan Refire = TimeSpan.FromMinutes(10);
    private static readonly DateTime Start = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    private DateTime _now = Start;

    private AgAlertEvaluator Evaluator() => new(() => _now);

    private static AlertDelivery Delivered() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Delivered, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    private static AgReplicaReading Replica(string connected) => new("AG1", "NODE2", "SECONDARY", connected);

    private static AgDatabaseReading Database(long lagSeconds) => new("AG1", "Sales", "NODE2", lagSeconds, 0, false, null);

    private List<AgAlert> Down(AgAlertEvaluator e) =>
        e.EvaluateReplicas(ServerId, new[] { Replica("DISCONNECTED") }, Refire);

    private List<AgAlert> Behind(AgAlertEvaluator e) =>
        e.EvaluateDatabases(ServerId, new[] { Database(lagSeconds: 900) }, 300, 0, Cooldown);

    /// <summary>A disconnect announced at <see cref="Start"/> and delivered, so the re-fire window is open.</summary>
    private AgAlertEvaluator DisconnectAnnounced()
    {
        var e = Evaluator();
        e.EvaluateReplicas(ServerId, new[] { Replica("CONNECTED") }, Refire);
        var lost = Assert.Single(Down(e));
        e.NoteSent(lost, Delivered(), Cooldown);
        return e;
    }

    [Fact]
    public void ADisconnectRefire_AfterA10MinuteStepBack_IsDueOneWindowFromTheFirstSweepThatSeesTheStep()
    {
        var e = DisconnectAnnounced();

        _now = Start.AddMinutes(-10);
        Assert.Empty(Down(e));

        _now = Start.AddMinutes(-10) + Refire - TimeSpan.FromSeconds(1);
        Assert.Empty(Down(e));

        _now = Start.AddMinutes(-10) + Refire;
        Assert.Single(Down(e));
    }

    [Fact]
    public void ADisconnectRefire_After50MillisecondsBack_IsNotSentAtOnce()
    {
        var e = DisconnectAnnounced();

        _now = Start.AddMilliseconds(-50);
        Assert.Empty(Down(e));

        _now = Start.AddMilliseconds(-50) + Refire - TimeSpan.FromMilliseconds(1);
        Assert.Empty(Down(e));

        _now = Start.AddMilliseconds(-50) + Refire;
        Assert.Single(Down(e));
    }

    [Fact]
    public void ADisconnectRefire_OnAForwardClock_IsDueExactlyOneWindowAfterTheSend()
    {
        var e = DisconnectAnnounced();

        _now = Start + Refire - TimeSpan.FromTicks(1);
        Assert.Empty(Down(e));

        _now = Start + Refire;
        Assert.Single(Down(e));
    }

    [Fact]
    public void TheFirstSweepAfterAStepBack_StoresItsOwnClockValueAsTheDisconnectStamp()
    {
        var e = DisconnectAnnounced();
        var stamps = (Dictionary<string, DateTime>)typeof(AgAlertEvaluator)
            .GetField("_lastDisconnectAlert", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(e)!;
        Assert.Equal(Start, Assert.Single(stamps).Value);

        _now = Start.AddMinutes(-10);
        Down(e);

        Assert.Equal(Start.AddMinutes(-10), Assert.Single(stamps).Value);
    }

    [Fact]
    public void ASyncBehindRepeat_AfterA10MinuteStepBack_IsHeldOneCooldownFromTheFirstSweepThatSeesTheStep()
    {
        var e = Evaluator();
        Assert.Single(Behind(e));

        _now = Start.AddMinutes(-10);
        Assert.Empty(Behind(e));

        _now = Start.AddMinutes(-10) + Cooldown - TimeSpan.FromSeconds(1);
        Assert.Empty(Behind(e));

        _now = Start.AddMinutes(-10) + Cooldown;
        Assert.Single(Behind(e));
    }

    [Fact]
    public void ASyncBehindRepeat_OnAForwardClock_IsDueExactlyOneCooldownAfterTheSend()
    {
        var e = Evaluator();
        Assert.Single(Behind(e));

        _now = Start + Cooldown - TimeSpan.FromTicks(1);
        Assert.Empty(Behind(e));

        _now = Start + Cooldown;
        Assert.Single(Behind(e));
    }
}
