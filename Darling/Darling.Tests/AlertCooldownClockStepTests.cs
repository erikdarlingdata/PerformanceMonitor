/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4732: the rule for a stored last-fired stamp that is AHEAD of the clock (the wall clock stepped back since it
/// was written). The first check that sees it stores now in its place and counts the elapsed time from there, so a
/// repeat is due one window after that check: not the step plus the window, and never at once.
/// </summary>
public sealed class LastFiredStampTests
{
    private static readonly DateTime Now = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void WithNoStamp_TheKeyIsReportedAbsent_AndNothingIsWritten()
    {
        var stamps = new ConcurrentDictionary<string, DateTime>();

        Assert.False(LastFiredStamp.TryGet(stamps, "k", Now, out _));
        Assert.Empty(stamps);
    }

    [Fact]
    public void AStampAtOrBeforeNow_IsReturnedAsStored_AndLeftAlone()
    {
        var stamps = new ConcurrentDictionary<string, DateTime>();
        stamps["before"] = Now.AddMinutes(-3);
        stamps["equal"] = Now;

        Assert.True(LastFiredStamp.TryGet(stamps, "before", Now, out var before));
        Assert.True(LastFiredStamp.TryGet(stamps, "equal", Now, out var equal));

        Assert.Equal(Now.AddMinutes(-3), before);
        Assert.Equal(Now, equal);
        Assert.Equal(Now.AddMinutes(-3), stamps["before"]);
        Assert.Equal(Now, stamps["equal"]);
    }

    [Fact]
    public void AStampAheadOfNow_IsReplacedByNowInTheDictionary_AndReturnedAsNow()
    {
        var stamps = new Dictionary<int, DateTime> { [7] = Now.AddMinutes(10) };

        Assert.True(LastFiredStamp.TryGet(stamps, 7, Now, out var last));

        Assert.Equal(Now, last);
        Assert.Equal(Now, stamps[7]);
    }

    [Fact]
    public void Settle_KeepsAnEarlierTime_AndCapsALaterOneAtNow()
    {
        Assert.Equal(Now.AddSeconds(-1), LastFiredStamp.Settle(Now.AddSeconds(-1), Now));
        Assert.Equal(Now, LastFiredStamp.Settle(Now, Now));
        Assert.Equal(Now, LastFiredStamp.Settle(Now.AddMilliseconds(50), Now));
    }
}

/// <summary>
/// #4732: <c>AlertEngine.CooldownElapsed</c>, the cooldown gate every SQL Server alert family shares (13 callers,
/// used by Lite and Darling alike), after the wall clock steps backward. It is private, so it is called through
/// reflection with a fixed clock; the stamps are the caller's dictionary, so the write-back can be read.
/// </summary>
public sealed class AlertEngineCooldownClockStepTests
{
    private static readonly DateTime FiredAt = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Cooldown = TimeSpan.FromMinutes(5);
    private const string Key = "server|cpu";

    private static readonly MethodInfo Gate = typeof(AlertEngine)
        .GetMethod("CooldownElapsed", BindingFlags.NonPublic | BindingFlags.Static)!
        .MakeGenericMethod(typeof(string));

    private static bool CooldownElapsed(ConcurrentDictionary<string, DateTime> stamps, DateTime now, TimeSpan cooldown) =>
        (bool)Gate.Invoke(null, new object[] { stamps, Key, now, cooldown })!;

    private static ConcurrentDictionary<string, DateTime> FiredOnce() => new() { [Key] = FiredAt };

    [Fact]
    public void ARepeatAfterA10MinuteStepBack_IsHeldOneCooldownFromTheFirstCheckThatSeesTheStep()
    {
        var stamps = FiredOnce();
        var afterStep = FiredAt.AddMinutes(-10);

        Assert.False(CooldownElapsed(stamps, afterStep, Cooldown));
        Assert.False(CooldownElapsed(stamps, afterStep + Cooldown - TimeSpan.FromSeconds(1), Cooldown));
        Assert.True(CooldownElapsed(stamps, afterStep + Cooldown, Cooldown));
    }

    [Fact]
    public void A50MillisecondStepBack_DoesNotSendTheRepeatAtOnce()
    {
        var stamps = FiredOnce();
        var afterStep = FiredAt.AddMilliseconds(-50);

        Assert.False(CooldownElapsed(stamps, afterStep, Cooldown));
        Assert.False(CooldownElapsed(stamps, afterStep + Cooldown - TimeSpan.FromMilliseconds(1), Cooldown));
        Assert.True(CooldownElapsed(stamps, afterStep + Cooldown, Cooldown));
    }

    [Fact]
    public void AForwardClock_DecidesExactlyAsBefore_AndNeverTouchesTheStamp()
    {
        var stamps = FiredOnce();

        Assert.False(CooldownElapsed(stamps, FiredAt, Cooldown));
        Assert.False(CooldownElapsed(stamps, FiredAt + Cooldown - TimeSpan.FromTicks(1), Cooldown));
        Assert.True(CooldownElapsed(stamps, FiredAt + Cooldown, Cooldown));
        Assert.Equal(FiredAt, stamps[Key]);

        Assert.True(CooldownElapsed(new ConcurrentDictionary<string, DateTime>(), FiredAt, Cooldown));
    }

    [Fact]
    public void TheFirstCheckThatSeesTheStep_StoresItsOwnClockValueAsTheStamp()
    {
        var stamps = FiredOnce();
        var afterStep = FiredAt.AddMinutes(-10);

        CooldownElapsed(stamps, afterStep, Cooldown);
        Assert.Equal(afterStep, stamps[Key]);

        /* A later check is not ahead of anything, so the stamp stays where the first check put it. */
        CooldownElapsed(stamps, afterStep.AddMinutes(1), Cooldown);
        Assert.Equal(afterStep, stamps[Key]);
    }

    [Fact]
    public void AZeroCooldown_NeverHolds_EvenRightAfterAStepBack()
    {
        var stamps = FiredOnce();

        Assert.True(CooldownElapsed(stamps, FiredAt.AddMinutes(-10), TimeSpan.Zero));
        Assert.True(CooldownElapsed(stamps, FiredAt.AddMinutes(-10), TimeSpan.Zero));
    }
}

/// <summary>
/// #4732 through the real arms of <see cref="DarlingSelfAlertEvaluator"/> with a recording deliverer and a clock
/// the test moves: the shared cooldown gate, the connection re-fire window, and a daily document's delivery stamp
/// seeded from the store by a restart.
/// </summary>
public sealed class DarlingSelfAlertClockStepTests
{
    private const int ServerId = 424242;
    private const string Key = "424242";
    private const string Name = "SELF-ALERT-SRV";
    private static readonly TimeSpan Cooldown = TimeSpan.FromMinutes(5);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static DarlingSelfAlertTests.Harness Rig(int connectionRefireMinutes = 0)
    {
        var h = new DarlingSelfAlertTests.Harness { ConnectionRefireMinutes = connectionRefireMinutes };
        h.Settings.CooldownMinutes = 5;
        return h;
    }

    private static Task StoppedAsync(DarlingSelfAlertEvaluator e) =>
        e.ApplyCollectionStoppedAsync(ServerId, Name, stopped: true, "no recent collection", Ct);

    private static int Fires(DarlingSelfAlertTests.Harness h, string metric) =>
        h.Deliverer.Outcomes.Count(o => o.MetricName == metric);

    [Fact]
    public async Task ASharedCooldownRepeat_AfterA10MinuteStepBack_IsHeldOneCooldownFromTheFirstCheckThatSeesTheStep()
    {
        var h = Rig();
        var e = h.Build();
        var firedAt = h.Now;

        await StoppedAsync(e);
        Assert.Equal(1, Fires(h, "Collection Stopped"));

        h.Now = firedAt.AddMinutes(-10);
        await StoppedAsync(e);
        Assert.Equal(1, Fires(h, "Collection Stopped"));

        h.Now = firedAt.AddMinutes(-10) + Cooldown - TimeSpan.FromSeconds(1);
        await StoppedAsync(e);
        Assert.Equal(1, Fires(h, "Collection Stopped"));

        h.Now = firedAt.AddMinutes(-10) + Cooldown;
        await StoppedAsync(e);
        Assert.Equal(2, Fires(h, "Collection Stopped"));
    }

    [Fact]
    public async Task ASharedCooldownRepeat_After50MillisecondsBack_IsNotSentAtOnce()
    {
        var h = Rig();
        var e = h.Build();
        var firedAt = h.Now;

        await StoppedAsync(e);
        h.Now = firedAt.AddMilliseconds(-50);
        await StoppedAsync(e);
        Assert.Equal(1, Fires(h, "Collection Stopped"));

        h.Now = firedAt.AddMilliseconds(-50) + Cooldown - TimeSpan.FromMilliseconds(1);
        await StoppedAsync(e);
        Assert.Equal(1, Fires(h, "Collection Stopped"));

        h.Now = firedAt.AddMilliseconds(-50) + Cooldown;
        await StoppedAsync(e);
        Assert.Equal(2, Fires(h, "Collection Stopped"));
    }

    [Fact]
    public async Task ASharedCooldownRepeat_OnAForwardClock_IsDueExactlyOneCooldownAfterTheFire()
    {
        var h = Rig();
        var e = h.Build();
        var firedAt = h.Now;

        await StoppedAsync(e);
        h.Now = firedAt + Cooldown - TimeSpan.FromTicks(1);
        await StoppedAsync(e);
        Assert.Equal(1, Fires(h, "Collection Stopped"));

        h.Now = firedAt + Cooldown;
        await StoppedAsync(e);
        Assert.Equal(2, Fires(h, "Collection Stopped"));
    }

    [Fact]
    public async Task TheFirstSharedCooldownCheckAfterAStepBack_StoresItsOwnClockValueAsTheStamp()
    {
        var h = Rig();
        var e = h.Build();
        var firedAt = h.Now;
        var stamps = (ConcurrentDictionary<string, DateTime>)typeof(DarlingSelfAlertEvaluator)
            .GetField("_lastCollectionStoppedAlert", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(e)!;

        await StoppedAsync(e);
        Assert.Equal(firedAt, stamps[Key]);

        h.Now = firedAt.AddMinutes(-10);
        await StoppedAsync(e);

        Assert.Equal(firedAt.AddMinutes(-10), stamps[Key]);
    }

    [Fact]
    public async Task AConnectionRefire_AfterA10MinuteStepBack_IsDueOneWindowFromTheFirstCheckThatSeesTheStep()
    {
        var h = Rig(connectionRefireMinutes: 10);
        var e = h.Build();
        var window = TimeSpan.FromMinutes(10);

        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, null, Ct);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, "no route", Ct);
        var downAt = h.Now;
        Assert.Equal(1, Fires(h, "Server Unreachable"));

        h.Now = downAt.AddMinutes(-10);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, "no route", Ct);
        Assert.Equal(1, Fires(h, "Server Unreachable"));

        h.Now = downAt.AddMinutes(-10) + window - TimeSpan.FromSeconds(1);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, "no route", Ct);
        Assert.Equal(1, Fires(h, "Server Unreachable"));

        h.Now = downAt.AddMinutes(-10) + window;
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, "no route", Ct);
        Assert.Equal(2, Fires(h, "Server Unreachable"));
    }

    [Fact]
    public async Task AConnectionRefire_After50MillisecondsBack_IsNotSentAtOnce_AndAForwardClockIsUnchanged()
    {
        var h = Rig(connectionRefireMinutes: 10);
        var e = h.Build();

        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, null, Ct);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, "no route", Ct);
        var downAt = h.Now;

        h.Now = downAt.AddMilliseconds(-50);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, "no route", Ct);
        Assert.Equal(1, Fires(h, "Server Unreachable"));

        /* A forward clock from a fresh outage: due exactly one window after the send, as before. */
        var forward = Rig(connectionRefireMinutes: 10);
        var fe = forward.Build();
        await fe.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, null, Ct);
        await fe.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, "no route", Ct);
        var sentAt = forward.Now;
        forward.Now = sentAt.AddMinutes(10) - TimeSpan.FromTicks(1);
        await fe.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, "no route", Ct);
        Assert.Equal(1, Fires(forward, "Server Unreachable"));
        forward.Now = sentAt.AddMinutes(10);
        await fe.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, "no route", Ct);
        Assert.Equal(2, Fires(forward, "Server Unreachable"));
    }

    /* ---------------- a stamp seeded from the store by a restart ---------------- */

    private sealed class MemoryStampStore : ISelfAlertDeliveryStampStore
    {
        public Dictionary<string, DateTime> Stamps { get; } = new(StringComparer.Ordinal);

        public Task<DateTime?> GetDeliveredAtUtcAsync(string stateKey, CancellationToken cancellationToken) =>
            Task.FromResult(Stamps.TryGetValue(stateKey, out var at) ? at : (DateTime?)null);

        public Task RecordDeliveredAtUtcAsync(string stateKey, DateTime deliveredAtUtc, CancellationToken cancellationToken)
        {
            Stamps[stateKey] = deliveredAtUtc;
            return Task.CompletedTask;
        }
    }

    private static AnalysisSinglesDigestReader.DigestRoutedRow[] OneSingle(DateTime at)
    {
        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem { Heading = "Diagnosis" });
        context.Routing = new AlertRoutingDto(FindingRouting.DigestText, "uncorroborated: a lone fact.");
        return new[]
        {
            new AnalysisSinglesDigestReader.DigestRoutedRow(
                at.AddHours(-2), 1, "server-1", "Analysis: anomaly [aaaaaaaa]", 1.62, AlertContextSerializer.Serialize(context)),
        };
    }

    [Fact]
    public async Task ARestartRightAfterAStepBack_SeededWithADeliveryTimeAheadOfNow_HoldsTheDailyDocumentForOneInterval()
    {
        var now = new DateTime(2026, 9, 19, 12, 0, 0, DateTimeKind.Utc);
        var stamps = new MemoryStampStore();
        stamps.Stamps[PgSelfAlertDeliveryStampStore.AnalysisSinglesDigestStateKey] = now.AddHours(3);
        var deliverer = new DarlingSelfAlertTests.RecordingDeliverer();
        var clock = now;
        var e = new DarlingSelfAlertEvaluator(
            new DarlingSelfAlertTests.FakeSettings { CooldownMinutes = 5 }, deliverer,
            new DarlingSelfAlertTests.FakeHistoryStore(), _ => false,
            logger: null, utcNow: () => clock, deliveryStamps: stamps);
        var interval = DarlingSelfAlertEvaluator.AnalysisSinglesDigestInterval;

        await e.ApplyAnalysisSinglesDigestAsync(OneSingle(clock), clock - interval, clock, Ct);
        Assert.Empty(deliverer.Outcomes);

        clock = now + interval - TimeSpan.FromSeconds(1);
        await e.ApplyAnalysisSinglesDigestAsync(OneSingle(clock), clock - interval, clock, Ct);
        Assert.Empty(deliverer.Outcomes);

        clock = now + interval;
        await e.ApplyAnalysisSinglesDigestAsync(OneSingle(clock), clock - interval, clock, Ct);
        Assert.Single(deliverer.Outcomes);
    }

    [Fact]
    public async Task ARestartSeededWithADeliveryTimeBeforeNow_DecidesExactlyAsBefore()
    {
        var now = new DateTime(2026, 9, 19, 12, 0, 0, DateTimeKind.Utc);
        var stamps = new MemoryStampStore();
        stamps.Stamps[PgSelfAlertDeliveryStampStore.AnalysisSinglesDigestStateKey] = now.AddHours(-2);
        var deliverer = new DarlingSelfAlertTests.RecordingDeliverer();
        var clock = now;
        var e = new DarlingSelfAlertEvaluator(
            new DarlingSelfAlertTests.FakeSettings { CooldownMinutes = 5 }, deliverer,
            new DarlingSelfAlertTests.FakeHistoryStore(), _ => false,
            logger: null, utcNow: () => clock, deliveryStamps: stamps);
        var interval = DarlingSelfAlertEvaluator.AnalysisSinglesDigestInterval;

        await e.ApplyAnalysisSinglesDigestAsync(OneSingle(clock), clock - interval, clock, Ct);
        Assert.Empty(deliverer.Outcomes);

        clock = now.AddHours(-2) + interval;
        await e.ApplyAnalysisSinglesDigestAsync(OneSingle(clock), clock - interval, clock, Ct);
        Assert.Single(deliverer.Outcomes);
    }
}
